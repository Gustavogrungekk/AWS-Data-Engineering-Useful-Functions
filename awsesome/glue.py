"""
AWS Glue Catalog and Job helpers.

Functions:
    read_catalog_table    - Read a Glue Catalog table into a Spark DataFrame (optionally create a temp view)
    register_table        - Infer schema from S3 data and create/update a Glue Catalog table
    get_latest_partition  - Fetch the latest partition of a partitioned table via Athena
    get_last_partition_spark - Fetch the last partition using Spark SQL
    list_jobs             - List Glue job names, optionally filtered by substring
    estimate_job_cost     - Estimate the dollar cost of a single Glue job run
    glue_job_audit_report - Build a DataFrame with run details & costs for a list of jobs
    log                   - Log a message through the Glue logger
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from time import sleep
from typing import Optional

import boto3
import pandas as pd


# ---------------------------------------------------------------------------
# Catalyst: Spark / GlueContext lazy helpers
# ---------------------------------------------------------------------------

def _get_or_create_spark(spark=None):
    """Return an existing SparkSession or create a new one."""
    if spark is not None:
        return spark
    from pyspark.sql import SparkSession
    return SparkSession.builder.getOrCreate()


def _get_or_create_glue_context(glue_context=None, spark=None):
    """Return an existing GlueContext or create one from *spark*."""
    if glue_context is not None:
        return glue_context
    from awsglue.context import GlueContext
    spark = _get_or_create_spark(spark)
    return GlueContext(spark.sparkContext)


# ---------------------------------------------------------------------------
# Catalog I/O
# ---------------------------------------------------------------------------

def read_catalog_table(
    database: str,
    table_name: str,
    *,
    view: Optional[str] = None,
    additional_options: Optional[dict] = None,
    spark=None,
    glue_context=None,
):
    """Read a table from the AWS Glue Data Catalog into a Spark DataFrame.

    Optionally registers the DataFrame as a Spark SQL temporary view so you
    can query it with ``spark.sql("SELECT … FROM <view>")``.

    Args:
        database: Glue Catalog database name.
        table_name: Glue Catalog table name.
        view: If provided, register the DataFrame as a temp view with this name.
        additional_options: Extra options forwarded to ``create_dynamic_frame.from_catalog``.
        spark: Existing ``SparkSession`` (created automatically if omitted).
        glue_context: Existing ``GlueContext`` (created automatically if omitted).

    Returns:
        A Spark ``DataFrame``.

    Example:
        >>> df = read_catalog_table("analytics", "events", view="events")
        >>> spark.sql("SELECT count(*) FROM events").show()
    """
    gc = _get_or_create_glue_context(glue_context, spark)
    opts = additional_options or {}
    dyf = gc.create_dynamic_frame.from_catalog(
        database=database,
        table_name=table_name,
        transformation_ctx=f"read_{database}_{table_name}",
        **opts,
    )
    df = dyf.toDF()
    if view:
        df.createOrReplaceTempView(view)
    return df


def register_table(
    database: str,
    table_name: str,
    s3_path: str,
    file_format: str = "parquet",
    region: str = "us-east-1",
    spark=None,
) -> str:
    """Read data from S3, infer schema, and create or update a Glue Catalog table.

    This is a lightweight alternative to running an AWS Glue Crawler.

    Args:
        database: Glue Catalog database name.
        table_name: Table name to create or update.
        s3_path: S3 location of the data files.
        file_format: ``"parquet"`` or ``"csv"``.
        region: AWS region for the Glue client.
        spark: Existing ``SparkSession`` (created automatically if omitted).

    Returns:
        A status string.

    Example:
        >>> register_table("analytics", "events", "s3://bucket/events/", "parquet")
    """
    spark = _get_or_create_spark(spark)
    glue = boto3.client("glue", region_name=region)

    if file_format == "parquet":
        df = spark.read.parquet(s3_path)
    elif file_format == "csv":
        df = spark.read.csv(s3_path, header=True, inferSchema=True)
    else:
        raise ValueError(f"Unsupported file_format: {file_format!r}. Use 'parquet' or 'csv'.")

    # Map Spark types → Glue/Hive types
    _type_map = {
        "string": "string", "integer": "int", "long": "bigint",
        "double": "double", "float": "float", "boolean": "boolean",
        "date": "date", "timestamp": "timestamp", "binary": "binary",
        "short": "smallint", "byte": "tinyint",
    }
    columns = []
    for field in df.schema.fields:
        hive_type = _type_map.get(field.dataType.simpleString(), field.dataType.simpleString())
        columns.append({"Name": field.name, "Type": hive_type})

    serde_map = {
        "parquet": {
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            },
        },
        "csv": {
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                "Parameters": {"field.delim": ",", "serialization.format": ","},
            },
        },
    }

    table_input = {
        "Name": table_name,
        "StorageDescriptor": {
            "Columns": columns,
            "Location": s3_path,
            "Compressed": file_format == "parquet",
            **serde_map[file_format],
        },
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": file_format},
    }

    try:
        glue.get_table(DatabaseName=database, Name=table_name)
        glue.update_table(DatabaseName=database, TableInput=table_input)
        return f"Table {database}.{table_name} updated."
    except glue.exceptions.EntityNotFoundException:
        glue.create_table(DatabaseName=database, TableInput=table_input)
        return f"Table {database}.{table_name} created."


# ---------------------------------------------------------------------------
# Partition helpers
# ---------------------------------------------------------------------------

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
    output_loc = f"s3://aws-athena-query-results-{region}/awsesome/"

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
    gc.get_logger().info(f"[AWSesome] {message}")
