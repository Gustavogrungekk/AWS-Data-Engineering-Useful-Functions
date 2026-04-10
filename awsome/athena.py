"""
Amazon Athena query execution and result retrieval.

Functions:
    run_query            - Execute an Athena query and wait for it to finish
    fetch_query_results  - Retrieve results of a completed Athena query as a DataFrame
    athena_audit_report  - Build a cost/usage report for recent Athena queries

Note:
    To run an Athena query and write the result directly to the Glue Catalog,
    use ``awsome.catalog.write_dataframe(sql=..., mode="new")`` instead.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from time import sleep
from typing import Optional

import boto3
import pandas as pd


# ---------------------------------------------------------------------------
# Core execution
# ---------------------------------------------------------------------------

def run_query(
    query: str,
    *,
    database: Optional[str] = None,
    workgroup: str = "primary",
    output_location: Optional[str] = None,
    region: str = "us-east-1",
    poll_seconds: int = 3,
) -> str:
    """Execute an Athena SQL query and block until it completes.

    Args:
        query: SQL query string.
        database: Default database context for the query.
        workgroup: Athena workgroup name.
        output_location: S3 path for query results. If ``None``, the
            workgroup default is used.
        region: AWS region.
        poll_seconds: Seconds between status polls.

    Returns:
        The Athena ``QueryExecutionId``.

    Raises:
        RuntimeError: If the query fails or is cancelled.

    Example:
        >>> qid = run_query("SELECT count(*) FROM my_db.events", database="my_db")
    """
    athena = boto3.client("athena", region_name=region)
    kwargs: dict = {
        "QueryString": query,
        "WorkGroup": workgroup,
    }
    if database:
        kwargs["QueryExecutionContext"] = {"Database": database}
    if output_location:
        kwargs["ResultConfiguration"] = {"OutputLocation": output_location}

    qid = athena.start_query_execution(**kwargs)["QueryExecutionId"]

    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        sleep(poll_seconds)

    if state != "SUCCEEDED":
        reason = status["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
        raise RuntimeError(f"Athena query {state} (id={qid}): {reason}")

    return qid


# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------

def fetch_query_results(
    query_execution_id: str,
    region: str = "us-east-1",
) -> pd.DataFrame:
    """Fetch the results of a *completed* Athena query into a DataFrame.

    Handles pagination automatically.

    Args:
        query_execution_id: The Athena ``QueryExecutionId``.
        region: AWS region.

    Returns:
        A ``pandas.DataFrame`` with column names from the result set.

    Example:
        >>> qid = run_query("SELECT * FROM my_db.events LIMIT 10")
        >>> df = fetch_query_results(qid)
    """
    athena = boto3.client("athena", region_name=region)
    rows: list[dict] = []
    columns: list[str] = []
    next_token: Optional[str] = None
    first_page = True

    while True:
        kwargs = {"QueryExecutionId": query_execution_id}
        if next_token:
            kwargs["NextToken"] = next_token

        resp = athena.get_query_results(**kwargs)
        if not columns:
            columns = [
                col["Name"]
                for col in resp["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
            ]

        page_rows = resp["ResultSet"]["Rows"]
        # The first page includes the header row — skip it
        start = 1 if first_page else 0
        first_page = False

        for row in page_rows[start:]:
            values = [field.get("VarCharValue", "") for field in row["Data"]]
            rows.append(dict(zip(columns, values)))

        next_token = resp.get("NextToken")
        if not next_token:
            break

    return pd.DataFrame(rows, columns=columns)




# ---------------------------------------------------------------------------
# Audit report
# ---------------------------------------------------------------------------

def athena_audit_report(
    workgroups: list[str],
    hours: int = 24,
    region: str = "us-east-1",
) -> pd.DataFrame:
    """Build a usage/cost report for recent Athena queries.

    Scans queries submitted within the last *hours* hours across the given
    workgroups.  Cost is estimated at **$5 per TB** scanned.

    Args:
        workgroups: List of Athena workgroup names.
        hours: Look-back window in hours.
        region: AWS region.

    Returns:
        A ``pandas.DataFrame`` with one row per query execution.

    Example:
        >>> df = athena_audit_report(["primary", "analytics"], hours=48)
        >>> df.groupby("workgroup")["cost_usd"].sum()
    """
    athena = boto3.client("athena", region_name=region)
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=hours)
    rows: list[dict] = []

    for wg in workgroups:
        try:
            ids = athena.list_query_executions(WorkGroup=wg).get("QueryExecutionIds", [])
        except Exception as e:
            print(f"Error listing queries for workgroup {wg}: {e}")
            continue

        for qid in ids:
            try:
                qe = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
                submitted = qe["Status"]["SubmissionDateTime"]
                if not (start_time <= submitted <= end_time):
                    continue

                completed = qe["Status"].get("CompletionDateTime")
                scanned = qe.get("Statistics", {}).get("DataScannedInBytes", 0)
                scanned_gb = scanned / (1024 ** 3)
                cost = round((scanned / (1024 ** 4)) * 5, 6)  # $5/TB

                rows.append({
                    "query_id": qid,
                    "workgroup": wg,
                    "database": qe.get("QueryExecutionContext", {}).get("Database"),
                    "status": qe["Status"]["State"],
                    "submitted": submitted.strftime("%Y-%m-%d %H:%M:%S"),
                    "completed": completed.strftime("%Y-%m-%d %H:%M:%S") if completed else None,
                    "scanned_bytes": scanned,
                    "scanned_gb": round(scanned_gb, 4),
                    "cost_usd": cost,
                    "query": qe.get("Query", "")[:500],  # truncated for readability
                    "output_location": qe.get("ResultConfiguration", {}).get("OutputLocation"),
                })
            except Exception as e:
                print(f"Error fetching query {qid}: {e}")

    return pd.DataFrame(rows)
