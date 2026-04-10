"""
AWS Glue Job management helpers.

Functions:
    list_jobs             - List Glue job names, optionally filtered by substring
    estimate_job_cost     - Estimate the dollar cost of a single Glue job run
    glue_job_audit_report - Build a DataFrame with run details & costs for a list of jobs
    log                   - Log a message through the Glue logger

Catalog operations (write, read, register, partitions) have moved to
``awsome.catalog``.  Imports from this module still work for backward
compatibility::

    # preferred
    from awsome.catalog import write_dataframe, read_catalog_table

    # still works
    from awsome.glue import write_dataframe, read_catalog_table
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import boto3
import pandas as pd

# ---------------------------------------------------------------------------
# Re-export catalog symbols so ``from awsome.glue import …`` keeps working
# ---------------------------------------------------------------------------
from awsome.catalog._helpers import _get_or_create_spark, _get_or_create_glue_context  # noqa: F401
from awsome.catalog.write import write_dataframe  # noqa: F401
from awsome.catalog.read import read_catalog_table  # noqa: F401
from awsome.catalog.register import register_table  # noqa: F401
from awsome.catalog.partitions import get_latest_partition, get_last_partition_spark  # noqa: F401


# ---------------------------------------------------------------------------
# Job listing / cost estimation
# ---------------------------------------------------------------------------

# DPU cost per hour by worker type (us-east-1 pricing)
_GLUE_DPU_COSTS = {
    "G.1X": 0.44,
    "G.2X": 0.88,
    "G.4X": 1.76,
    "G.8X": 3.52,
    "G.16X": 7.04,
    "G.32X": 14.08,
    "Standard": 0.44,
    "PythonShell": 0.44,
}


def list_jobs(
    name_filter: str = "*",
    region: str = "us-east-1",
) -> list[str]:
    """List AWS Glue job names, optionally filtered by a substring.

    Args:
        name_filter: Substring to match (case-insensitive). ``"*"`` returns all jobs.
        region: AWS region.

    Returns:
        Sorted list of matching job names.

    Example:
        >>> list_jobs("etl")
        ['etl-customers', 'etl-orders']
    """
    glue = boto3.client("glue", region_name=region)
    jobs: list[str] = []
    next_token = None

    while True:
        kwargs = {"MaxResults": 200}
        if next_token:
            kwargs["NextToken"] = next_token
        resp = glue.list_jobs(**kwargs)
        jobs.extend(resp.get("JobNames", []))
        next_token = resp.get("NextToken")
        if not next_token:
            break

    if name_filter.strip() == "*":
        return sorted(jobs)

    needle = name_filter.strip().lower()
    return sorted(j for j in jobs if needle in j.lower())


def estimate_job_cost(
    job_name: str,
    run_id: str,
    region: str = "us-east-1",
) -> float:
    """Estimate the dollar cost of a single AWS Glue job run.

    Args:
        job_name: Name of the Glue job.
        run_id: Job run ID.
        region: AWS region.

    Returns:
        Estimated cost in USD.

    Example:
        >>> estimate_job_cost("etl-orders", "jr_abc123")
        0.37
    """
    glue = boto3.client("glue", region_name=region)
    run = glue.get_job_run(JobName=job_name, RunId=run_id)["JobRun"]
    hours = run.get("ExecutionTime", 0) / 3600
    dpu = run.get("MaxCapacity", 0)
    worker = run.get("WorkerType", "Standard")
    return round(_GLUE_DPU_COSTS.get(worker, 0.44) * dpu * hours, 4)


def glue_job_audit_report(
    job_names: list[str],
    region: str = "us-east-1",
    inactive_days: int = 31,
) -> pd.DataFrame:
    """Build an audit-report DataFrame for a list of AWS Glue jobs.

    For each job run found, the report includes: job name, creation date,
    start/end time, status, worker type, DPU, runtime, estimated cost,
    trigger name, and error message (if any).

    Args:
        job_names: List of Glue job names.
        region: AWS region.
        inactive_days: Jobs with no run in this many days are flagged inactive.

    Returns:
        A ``pandas.DataFrame`` with one row per job run.

    Example:
        >>> df = glue_job_audit_report(["etl-orders", "etl-customers"])
        >>> df[df["status"] == "FAILED"]
    """
    glue = boto3.client("glue", region_name=region)
    now = datetime.now(timezone.utc)
    rows: list[dict] = []

    for name in job_names:
        try:
            job_def = glue.get_job(JobName=name)["Job"]
            created = job_def["CreatedOn"]
            timeout = job_def.get("Timeout")
            glue_version = job_def.get("GlueVersion")
            exec_class = job_def.get("ExecutionClass", "Standard")

            # Find trigger
            trigger_name = None
            try:
                for trig in glue.get_triggers()["Triggers"]:
                    if name in [a["JobName"] for a in trig.get("Actions", [])]:
                        trigger_name = trig["Name"]
                        break
            except Exception:
                pass

            for run in glue.get_job_runs(JobName=name)["JobRuns"]:
                start = run["StartedOn"]
                end = run.get("CompletedOn")
                hours = run.get("ExecutionTime", 0) / 3600
                dpu = run.get("MaxCapacity", 0)
                worker = run.get("WorkerType", "Standard")
                cost = _GLUE_DPU_COSTS.get(worker, 0.44) * dpu * hours

                rows.append({
                    "job_name": name,
                    "created_date": created.strftime("%Y-%m-%d"),
                    "run_date": start.strftime("%Y-%m-%d"),
                    "start_time": start.strftime("%Y-%m-%d %H:%M:%S"),
                    "end_time": end.strftime("%Y-%m-%d %H:%M:%S") if end else None,
                    "status": run["JobRunState"],
                    "active": (now - start) <= timedelta(days=inactive_days),
                    "worker_type": worker,
                    "dpu": dpu,
                    "runtime_hours": round(hours, 4),
                    "execution_class": exec_class,
                    "glue_version": glue_version,
                    "timeout_min": timeout,
                    "cost_usd": round(cost, 4),
                    "error_message": run.get("ErrorMessage"),
                    "trigger_name": trigger_name,
                })

        except Exception as e:
            print(f"Error processing job {name}: {e}")

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def log(message: str, glue_context=None) -> None:
    """Log a message via the AWS Glue logger.

    If no *glue_context* is provided, one is created from the active
    ``SparkContext``.

    Args:
        message: Log message text.
        glue_context: Optional ``GlueContext``.

    Example:
        >>> log("ETL step 3 completed — 1.2 M rows written.")
    """
    gc = _get_or_create_glue_context(glue_context)
    gc.get_logger().info(f"[AWSome] {message}")
