"""
Partition discovery helpers for AWS Glue Data Catalog tables.

Usage:
    >>> from awsome.catalog import get_latest_partition, get_last_partition_spark
    >>> from awsome.catalog.partitions import get_latest_partition   # equivalent
"""

from __future__ import annotations

from time import sleep
from typing import Optional

import boto3

from awsome.catalog._helpers import _get_or_create_spark


def get_latest_partition(
    database: str,
    table_name: str,
    athena_workgroup: str = "primary",
    region: str = "us-east-1",
) -> str:
    """Return the latest partition clause for a partitioned Glue table.

    Uses Athena's ``$partitions`` virtual table to determine the most recent
    partition, then returns a SQL WHERE clause like
    ``"year=2024 AND month=06"``.

    Args:
        database: Glue Catalog database name.
        table_name: Table name.
        athena_workgroup: Athena workgroup to use.
        region: AWS region.

    Returns:
        A SQL-ready partition clause string.

    Example:
        >>> clause = get_latest_partition("analytics", "events")
        >>> df = spark.sql(f"SELECT * FROM analytics.events WHERE {clause}")
    """
    glue = boto3.client("glue", region_name=region)
    athena = boto3.client("athena", region_name=region)

    # Discover partition keys
    table_meta = glue.get_table(DatabaseName=database, Name=table_name)["Table"]
    partition_keys = table_meta.get("PartitionKeys", [])
    if not partition_keys:
        raise ValueError(f"Table {database}.{table_name} has no partition keys.")

    order_clause = ", ".join(f'"{pk["Name"]}" DESC' for pk in partition_keys)
    query = (
        f'SELECT * FROM "{database}"."{table_name}$partitions" '
        f"ORDER BY {order_clause} LIMIT 1"
    )
    output_loc = f"s3://aws-athena-query-results-{region}/awsome/"

    resp = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        WorkGroup=athena_workgroup,
        ResultConfiguration={"OutputLocation": output_loc},
    )
    qid = resp["QueryExecutionId"]

    # Wait for completion
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        sleep(2)

    if state != "SUCCEEDED":
        reason = status["QueryExecution"]["Status"].get("StateChangeReason", "")
        raise RuntimeError(f"Athena query {state}: {reason}")

    result = athena.get_query_results(QueryExecutionId=qid)
    rows = result["ResultSet"]["Rows"]
    if len(rows) < 2:
        raise ValueError(f"No partitions found for {database}.{table_name}")

    headers = [c["VarCharValue"] for c in rows[0]["Data"]]
    values = [c["VarCharValue"] for c in rows[1]["Data"]]

    pk_types = {pk["Name"]: pk["Type"].lower() for pk in partition_keys}
    numeric_types = {"int", "bigint", "smallint", "tinyint", "float", "double", "decimal"}

    conditions = []
    for col, val in zip(headers, values):
        col_type = pk_types.get(col, "string")
        if any(nt in col_type for nt in numeric_types):
            conditions.append(f"{col}={val}")
        else:
            conditions.append(f"{col}='{val}'")

    return " AND ".join(conditions)


def get_last_partition_spark(table: str, spark=None) -> Optional[str]:
    """Return the last partition of a Hive/Glue table using Spark SQL.

    Args:
        table: Fully qualified table name (``"database.table"``).
        spark: Existing ``SparkSession``.

    Returns:
        A partition clause like ``"year=2024 and month=06"``, or ``None``.

    Example:
        >>> get_last_partition_spark("analytics.events")
        'year=2024 and month=06'
    """
    spark = _get_or_create_spark(spark)
    partitions = (
        spark.sql(f"SHOW PARTITIONS {table}")
        .rdd.flatMap(lambda x: x)
        .collect()
    )
    if not partitions:
        return None
    return sorted(partitions)[-1].replace("/", " AND ")
