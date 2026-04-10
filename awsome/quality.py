"""
Data quality checks via Athena.

Functions:
    run_quality_check - Count records (per partition) for a list of Glue Catalog tables
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

import boto3
import pandas as pd

from awsome.athena import run_query, fetch_query_results


def run_quality_check(
    tables: list[str],
    *,
    output_location: str = "s3://aws-athena-query-results/quality/",
    workgroup: str = "primary",
    region: str = "us-east-1",
) -> pd.DataFrame:
    """Run record-count quality checks on a list of Glue Catalog tables.

    For partitioned tables the count is broken down by partition.  Results
    are returned as a single consolidated DataFrame.

    Args:
        tables: List of fully-qualified table names
            (``"database.table"`` format).
        output_location: S3 path for Athena query results.
        workgroup: Athena workgroup.
        region: AWS region.

    Returns:
        A ``pandas.DataFrame`` with columns: ``check_date``, ``database``,
        ``table_name``, ``partition``, ``record_count``.

    Example:
        >>> df = run_quality_check(["analytics.events", "analytics.users"])
        >>> df[df["record_count"] == 0]  # find empty partitions
    """
    glue = boto3.client("glue", region_name=region)
    all_results: list[pd.DataFrame] = []
    check_date = datetime.now().strftime("%Y-%m-%d")

    for fqn in tables:
        try:
            database, table_name = fqn.split(".", 1)
        except ValueError:
            print(f"Skipping {fqn!r} — expected 'database.table' format.")
            continue

        try:
            table_meta = glue.get_table(DatabaseName=database, Name=table_name)["Table"]
        except Exception as e:
            print(f"Cannot read metadata for {fqn}: {e}")
            continue

        partitions = table_meta.get("PartitionKeys", [])

        if partitions:
            part_cols = [p["Name"] for p in partitions]
            concat_parts = ", '/', ".join(
                f"'{col}=', CAST({col} AS VARCHAR)" for col in part_cols
            )
            partition_expr = f"CONCAT({concat_parts}) AS partition_key"
            group_by = "GROUP BY " + ", ".join(part_cols)
        else:
            partition_expr = "CAST(NULL AS VARCHAR) AS partition_key"
            group_by = ""

        query = f"""
            SELECT
                '{check_date}'  AS check_date,
                '{database}'    AS database,
                '{table_name}'  AS table_name,
                {partition_expr},
                COUNT(1)        AS record_count
            FROM "{database}"."{table_name}"
            {group_by}
        """

        try:
            qid = run_query(
                query,
                database=database,
                workgroup=workgroup,
                output_location=output_location,
                region=region,
            )
            df = fetch_query_results(qid, region=region)
            all_results.append(df)
        except Exception as e:
            print(f"Quality check failed for {fqn}: {e}")

    if all_results:
        return pd.concat(all_results, ignore_index=True)
    return pd.DataFrame(columns=["check_date", "database", "table_name", "partition_key", "record_count"])
