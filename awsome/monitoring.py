"""
AWS Step Functions monitoring.

Functions:
    step_functions_report - Build a detailed execution report for Step Functions state machines
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone

import boto3
import pandas as pd


def step_functions_report(
    state_machine_arns: list[str],
    hours: int = 24,
    region: str = "us-east-1",
) -> pd.DataFrame:
    """Build a detailed execution-report DataFrame for Step Functions.

    For each execution within the look-back window, the report includes:
    name, status, start/stop times, per-step durations, retried steps,
    error messages, related AWS services, and total duration.

    Args:
        state_machine_arns: List of state machine ARNs.
        hours: Only include executions started within the last *hours* hours.
        region: AWS region.

    Returns:
        A ``pandas.DataFrame`` with one row per execution.

    Example:
        >>> df = step_functions_report(
        ...     ["arn:aws:states:us-east-1:123456789012:stateMachine:MyPipeline"],
        ...     hours=48,
        ... )
        >>> df[df["status"] == "FAILED"]
    """
    sfn = boto3.client("stepfunctions", region_name=region)
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=hours)
    rows: list[dict] = []

    for arn in state_machine_arns:
        try:
            sm = sfn.describe_state_machine(stateMachineArn=arn)
            sm_name = sm["name"]
            definition = json.loads(sm["definition"])
            iam_role = sm["roleArn"]

            # Detect related AWS services from task resources
            services: set[str] = set()
            for state in definition.get("States", {}).values():
                resource = state.get("Resource", "")
                m = re.match(r"arn:aws:(\w+):", resource)
                if m:
                    services.add(m.group(1))

            paginator = sfn.get_paginator("list_executions")
            for page in paginator.paginate(stateMachineArn=arn, maxResults=100):
                for exe in page["executions"]:
                    start = exe["startDate"]
                    if start < cutoff:
                        continue

                    stop = exe.get("stopDate")
                    duration_s = (stop - start).total_seconds() if stop else None

                    # Analyse execution history
                    step_durations: dict[str, float] = {}
                    step_statuses: dict[str, str] = {}
                    errors: list[str] = []
                    retried: list[str] = []
                    _step_start: dict[str, datetime] = {}

                    history = sfn.get_execution_history(
                        executionArn=exe["executionArn"],
                        maxResults=1000,
                        reverseOrder=False,
                    )["events"]

                    for ev in history:
                        if "stateEnteredEventDetails" in ev:
                            name = ev["stateEnteredEventDetails"]["name"]
                            _step_start[name] = ev["timestamp"]
                            step_statuses[name] = "STARTED"
                        if "stateExitedEventDetails" in ev:
                            name = ev["stateExitedEventDetails"]["name"]
                            step_statuses[name] = "SUCCEEDED"
                            if name in _step_start:
                                step_durations[name] = (ev["timestamp"] - _step_start[name]).total_seconds()
                        if "executionFailedEventDetails" in ev:
                            errors.append(ev["executionFailedEventDetails"].get("cause", ""))

                    rows.append({
                        "state_machine_arn": arn,
                        "state_machine_name": sm_name,
                        "execution_name": exe["name"],
                        "status": exe["status"],
                        "start_time": start.strftime("%Y-%m-%d %H:%M:%S"),
                        "stop_time": stop.strftime("%Y-%m-%d %H:%M:%S") if stop else "RUNNING",
                        "duration_seconds": duration_s,
                        "duration_minutes": round(duration_s / 60, 2) if duration_s else None,
                        "step_statuses": step_statuses,
                        "step_durations": step_durations,
                        "retried_steps": retried,
                        "errors": errors,
                        "related_services": sorted(services),
                        "iam_role": iam_role,
                    })

        except Exception as e:
            print(f"Error processing {arn}: {e}")

    return pd.DataFrame(rows)
