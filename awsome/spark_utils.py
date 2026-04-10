"""
Spark & Glue DataFrame utility functions.

Functions:
    estimate_df_size       - Estimate the serialized size of a Spark DataFrame
    cdp_to_s3              - Transfer data from a CDP Hive JDBC source to S3 (Parquet)
    process_s3_files_to_catalog - Read files from S3 and register them in the Glue Catalog
"""

from __future__ import annotations

from typing import Optional

from awsome.utils import convert_bytes


# ---------------------------------------------------------------------------
# Size estimation
# ---------------------------------------------------------------------------

def estimate_df_size(df, spark=None) -> str:
    """Estimate the serialized byte-size of a Spark DataFrame.

    Requires the ``repartipy`` library.

    Args:
        df: A Spark ``DataFrame``.
        spark: Existing ``SparkSession`` (auto-detected from *df* if omitted).

    Returns:
        A human-readable size string, e.g. ``"1.34 GB"``.

    Example:
        >>> size = estimate_df_size(df)
        >>> print(size)
        '256.7 MB'
    """
    import repartipy

    if spark is None:
        spark = df.sparkSession

    with repartipy.SizeEstimator(spark=spark, df=df) as se:
        size_bytes = se.estimate()

    return convert_bytes(int(size_bytes))


# ---------------------------------------------------------------------------
# CDP ➜ S3 transfer
# ---------------------------------------------------------------------------

def cdp_to_s3(
    jdbc_url: str,
    query: str,
    s3_output: str,
    *,
    username: str,
    password: str,
    write_mode: str = "overwrite",
    partition_by: Optional[str | list[str]] = None,
    catalog_db: Optional[str] = None,
    catalog_table: Optional[str] = None,
    spark=None,
) -> str:
    """Transfer data from a CDP (Cloudera) Hive source to S3 via JDBC.

    Optionally registers the output as a Glue Catalog table.

    Args:
        jdbc_url: JDBC URL for the Hive server.
        query: SQL query to execute on the source.
        s3_output: Destination S3 path for Parquet output.
        username: JDBC username.
        password: JDBC password.
        write_mode: Spark write mode (``"overwrite"``, ``"append"``).
        partition_by: Column name(s) to partition by.
        catalog_db: Glue database name (required together with *catalog_table*).
        catalog_table: Glue table name.
        spark: Existing ``SparkSession``.

    Returns:
        Status message.

    Example:
        >>> cdp_to_s3(
        ...     jdbc_url="jdbc:hive2://server:10000/default",
        ...     query="SELECT * FROM events",
        ...     s3_output="s3://bucket/events/",
        ...     username="user",
        ...     password="pass",
        ... )
    """
    from pyspark.sql import SparkSession

    spark = spark or SparkSession.builder.appName("cdp_to_s3").enableHiveSupport().getOrCreate()

    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("query", query)
        .option("user", username)
        .option("password", password)
        .option("driver", "org.apache.hive.jdbc.HiveDriver")
        .load()
    )

    writer = df.write.mode(write_mode)
    if partition_by:
        cols = [partition_by] if isinstance(partition_by, str) else partition_by
        writer = writer.partitionBy(*cols)
    writer.parquet(s3_output)

    if catalog_db and catalog_table:
        if not catalog_db or not catalog_table:
            raise ValueError("Both catalog_db and catalog_table must be provided.")
        save_opts = {"path": s3_output, "format": "parquet", "mode": write_mode}
        if partition_by:
            cols = [partition_by] if isinstance(partition_by, str) else partition_by
            save_opts["partitionBy"] = cols
        df.write.mode(write_mode).options(**save_opts).saveAsTable(f"{catalog_db}.{catalog_table}")

    return f"Data transferred to {s3_output}"


# ---------------------------------------------------------------------------
# S3 file ➜ Glue Catalog
# ---------------------------------------------------------------------------

def process_s3_files_to_catalog(
    s3_bucket: str,
    s3_prefix: str,
    object_names: list[str],
    database: str,
    *,
    region: str = "us-east-1",
    table_prefix: str = "",
    days_threshold: int = 1,
    glue_context=None,
) -> list[str]:
    """Read files from S3 and register each as a Glue Catalog table.

    Supports CSV, Parquet, JSON, and Excel files.  Files older than
    *days_threshold* days are skipped.

    Args:
        s3_bucket: S3 bucket name.
        s3_prefix: Key prefix under which the files live.
        object_names: File names (keys relative to *s3_prefix*).
        database: Target Glue Catalog database.
        region: AWS region.
        table_prefix: Optional string prepended to each table name.
        days_threshold: Skip files not modified within this many days.
        glue_context: Existing ``GlueContext``.

    Returns:
        List of table names that were registered.

    Example:
        >>> process_s3_files_to_catalog(
        ...     "data-bucket", "raw/", ["sales.csv", "products.parquet"],
        ...     database="analytics",
        ... )
    """
    import boto3
    from datetime import datetime
    from awsglue.context import GlueContext
    from awsglue.dynamicframe import DynamicFrame
    from pyspark.context import SparkContext

    if glue_context is None:
        glue_context = GlueContext(SparkContext.getOrCreate())
    spark = glue_context.spark_session
    s3 = boto3.client("s3", region_name=region)

    registered: list[str] = []

    for name in object_names:
        key = f"{s3_prefix.rstrip('/')}/{name}"
        s3_path = f"s3://{s3_bucket}/{key}"

        # Check freshness
        try:
            meta = s3.head_object(Bucket=s3_bucket, Key=key)
            last_mod = meta["LastModified"]
            age_days = (datetime.now(last_mod.tzinfo) - last_mod).days
            if age_days > days_threshold:
                continue
        except Exception as e:
            print(f"Skipping {name}: {e}")
            continue

        ext = name.rsplit(".", 1)[-1].lower()
        if ext == "csv":
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(s3_path)
        elif ext == "parquet":
            df = spark.read.parquet(s3_path)
        elif ext == "json":
            df = spark.read.json(s3_path)
        elif ext in ("xls", "xlsx"):
            df = (
                spark.read.format("com.crealytics.spark.excel")
                .option("useHeader", "true")
                .option("inferSchema", "true")
                .load(s3_path)
            )
        else:
            print(f"Unsupported file type: {ext} (file: {name})")
            continue

        table_name = f"{table_prefix}{name.rsplit('.', 1)[0]}"
        dyf = DynamicFrame.fromDF(df, glue_context, table_name)
        glue_context.write_dynamic_frame.from_catalog(
            frame=dyf,
            database=database,
            table_name=table_name,
            transformation_ctx=f"write_{table_name}",
        )
        registered.append(table_name)

    return registered
