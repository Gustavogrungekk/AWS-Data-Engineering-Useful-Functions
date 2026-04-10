"""
Universal write engine — write a Spark DataFrame (or Athena SQL result)
to S3 and register it in the AWS Glue Data Catalog.
When the source is an Athena SQL query, the function uses ``UNLOAD`` to
export results as **Parquet** so column types are preserved natively (no
CSV parsing / schema inference).
Every pipeline step is logged through the Glue logger **and** Python's
standard ``logging`` so output always appears in CloudWatch and the console.

Usage:
    >>> from awsome.catalog import write_dataframe
    >>> from awsome.catalog.write import write_dataframe   # equivalent
"""

from __future__ import annotations

from typing import Optional

from awsome.catalog._helpers import (
    _get_or_create_spark,
    _get_or_create_glue_context,
    _resolve_dataframe,
    _estimate_df_bytes,
    _compact_small_files,
    _PipelineLogger,
    _BYTES_PER_MB,
)

_VALID_MODES = ("new", "insert", "insert_update", "merge", "overwrite_partitions", "dataframe")


def write_dataframe(
    # ── source ────────────────────────────────────────────────────────────
    dataframe=None,
    sql: Optional[str] = None,

    # ── target ────────────────────────────────────────────────────────────
    database: Optional[str] = None,
    table: Optional[str] = None,
    path: Optional[str] = None,

    # ── write mode ────────────────────────────────────────────────────────
    mode: str = "insert",

    # ── partitioning ──────────────────────────────────────────────────────
    partitions: Optional[list[str]] = None,
    dynamic_partition: bool = True,

    # ── merge ─────────────────────────────────────────────────────────────
    merge_keys: Optional[list[str]] = None,
    merge_condition: Optional[str] = None,
    delete_not_matched: bool = False,

    # ── schema ────────────────────────────────────────────────────────────
    drop_columns: Optional[list[str]] = None,
    cast_columns: Optional[dict[str, str]] = None,
    enforce_schema: bool = False,
    evolve_schema: bool = True,

    # ── file sizing / layout ──────────────────────────────────────────────
    target_file_size_mb: int = 256,
    max_records_per_file: Optional[int] = None,
    repartition: Optional[int | list[str]] = None,
    coalesce: Optional[int] = None,
    sort_within_partitions: Optional[list[str]] = None,

    # ── compaction / optimisation ─────────────────────────────────────────
    auto_compact: bool = False,
    compact_only_touched_partitions: bool = True,
    optimize_after_write: bool = False,
    zorder: Optional[list[str]] = None,

    # ── incremental / watermark ───────────────────────────────────────────
    watermark_column: Optional[str] = None,
    watermark_value=None,

    # ── athena source ─────────────────────────────────────────────────────
    athena_output: Optional[str] = None,
    athena_workgroup: str = "primary",
    athena_database: Optional[str] = None,
    region: str = "us-east-1",

    # ── catalog options ───────────────────────────────────────────────────
    table_properties: Optional[dict[str, str]] = None,
    table_comment: Optional[str] = None,
    column_comments: Optional[dict[str, str]] = None,
    create_database: bool = True,
    create_table: bool = True,

    # ── operational hooks ─────────────────────────────────────────────────
    temp_view: Optional[str] = None,
    pre_sql: Optional[str | list[str]] = None,
    post_sql: Optional[str | list[str]] = None,
    dry_run: bool = False,
    repair_table: bool = True,

    # ── file format ───────────────────────────────────────────────────────
    file_format: str = "parquet",
    compression: str = "snappy",

    # ── return ────────────────────────────────────────────────────────────
    return_metrics: bool = True,

    # ── runtime ───────────────────────────────────────────────────────────
    spark=None,
    glue_context=None,
):
    """Universal write engine: write a Spark DataFrame (or Athena SQL result)
    to S3 and register it in the AWS Glue Data Catalog.

    This is the **single entry-point** for every catalog write pattern you
    need in Glue jobs.  Feed it a Spark ``DataFrame`` **or** a raw SQL string
    (executed on Athena under the hood) and choose a write *mode*.

    Every pipeline step is logged to CloudWatch / the console through the
    Glue logger so you can follow execution in real time.

    Modes
    -----
    +---------------------------+------------------------------------------------------+
    | Mode                      | Behaviour                                            |
    +===========================+======================================================+
    | ``"new"``                 | Drop & recreate the table (full overwrite).          |
    +---------------------------+------------------------------------------------------+
    | ``"insert"``              | Append rows.                                         |
    +---------------------------+------------------------------------------------------+
    | ``"insert_update"``       | Dynamic-partition overwrite — only replaces the      |
    |                           | partitions present in the incoming DataFrame.         |
    +---------------------------+------------------------------------------------------+
    | ``"overwrite_partitions"``| Alias for ``"insert_update"``.                       |
    +---------------------------+------------------------------------------------------+
    | ``"merge"``               | Upsert: update matched rows, insert new ones.        |
    |                           | Optionally delete unmatched target rows.              |
    +---------------------------+------------------------------------------------------+
    | ``"dataframe"``           | Return the DataFrame without writing.                |
    +---------------------------+------------------------------------------------------+

    Args:
        dataframe: Source Spark ``DataFrame``.  Mutually exclusive with *sql*.
        sql: Athena SQL query.  Executed on Athena via ``UNLOAD`` so the
            result is written as **Parquet** (preserving native types — no
            CSV parsing or schema inference).  Mutually exclusive with
            *dataframe*.
        database: Glue Catalog database name.
        table: Target table name.
        path: S3 location for data files
            (e.g. ``"s3://bucket/warehouse/my_table/"``).
        mode: Write mode (see table above).
        partitions: Column names to partition by.
        dynamic_partition: If ``True`` (default), enables Spark's dynamic
            partition overwrite mode for ``"insert_update"``.
        merge_keys: Columns forming the composite key for ``"merge"`` mode.
        merge_condition: Optional custom SQL merge condition
            (e.g. ``"src.id = tgt.id AND src.region = tgt.region"``).
            If omitted, built automatically from *merge_keys*.
        delete_not_matched: In ``"merge"`` mode, delete target rows that have
            no match in the source DataFrame.
        drop_columns: Columns to drop before writing.
        cast_columns: Dict ``{column: spark_type}`` for explicit casts,
            e.g. ``{"amount": "decimal(18,2)", "event_date": "date"}``.
        enforce_schema: If ``True``, select only target-table columns
            (drops extra, errors on missing).
        evolve_schema: If ``True`` (default), enables
            ``mergeSchema`` / ``overwriteSchema`` so new columns are
            added automatically.
        target_file_size_mb: Desired output file size; used to compute the
            ideal number of output partitions when no explicit
            *repartition* / *coalesce* is given.
        max_records_per_file: If set, adds
            ``spark.sql.files.maxRecordsPerFile``.
        repartition: Explicit repartition — an ``int`` (number of partitions)
            or a ``list[str]`` (column names).
        coalesce: Reduce partitions without a full shuffle.
        sort_within_partitions: Columns to sort by inside each partition
            (useful for merge locality and read performance).
        auto_compact: If ``True``, re-read and re-write the output path after
            the main write to consolidate small files.
        compact_only_touched_partitions: Only compact partitions that were
            written to (not the full table).  Default ``True``.
        optimize_after_write: If ``True``, run ``ALTER TABLE … COMPACT`` (if
            supported by the table format) after writing.
        zorder: Columns to Z-order by (only effective with Delta / Iceberg).
        watermark_column: Column used for incremental filtering.
        watermark_value: Only include rows where
            ``watermark_column > watermark_value``.  If ``None`` and a
            watermark column is set, the function tries to read the current
            max from the target table.
        athena_output: S3 path for Athena query results (when *sql* is used).
        athena_workgroup: Athena workgroup.
        athena_database: Default Athena database for the SQL query.
        region: AWS region.
        table_properties: Dict of Spark table properties
            (e.g. ``{"parquet.compression": "ZSTD"}``).
        table_comment: Description stored in the catalog.
        column_comments: Dict ``{column: comment}`` for column descriptions.
        create_database: Auto-create the database if it doesn't exist.
        create_table: Auto-create the table on first write (default ``True``).
        temp_view: If set, register the final DataFrame as a temporary SQL
            view with this name *before* writing.
        pre_sql: Spark SQL statement(s) to run **before** the write.
        post_sql: Spark SQL statement(s) to run **after** the write.
        dry_run: If ``True``, prepare the DataFrame and return it + metrics
            without performing any write.
        repair_table: If ``True``, run ``MSCK REPAIR TABLE`` after writing
            partitioned data.
        file_format: Output format — ``"parquet"`` (default), ``"orc"``, etc.
        compression: Compression codec (``"snappy"``, ``"gzip"``, ``"zstd"``).
        return_metrics: If ``True``, return a dict with write metrics.
            If ``False``, return a status string.
        spark: Existing ``SparkSession``.
        glue_context: Existing ``GlueContext`` (used for logging).

    Returns:
        Depends on *mode* and *return_metrics*:

        - ``"dataframe"`` → the Spark ``DataFrame``.
        - ``dry_run=True`` → ``dict`` with ``df`` and ``metrics``.
        - ``return_metrics=True`` → ``dict`` with write metrics.
        - Otherwise → a status ``str``.

    Examples:
        **New table from Athena SQL:**

        >>> write_dataframe(
        ...     sql="SELECT * FROM raw.events WHERE dt='2024-06-01'",
        ...     database="analytics", table="daily_events",
        ...     path="s3://lake/warehouse/daily_events/",
        ...     mode="new",
        ...     partitions=["dt"],
        ...     athena_output="s3://lake/tmp/athena/",
        ...     athena_database="raw",
        ... )

        **Dynamic-partition overwrite:**

        >>> write_dataframe(
        ...     dataframe=my_df,
        ...     database="analytics", table="daily_events",
        ...     path="s3://lake/warehouse/daily_events/",
        ...     mode="insert_update",
        ...     partitions=["dt"],
        ... )

        **Merge / upsert:**

        >>> write_dataframe(
        ...     dataframe=incremental_df,
        ...     database="analytics", table="dim_customers",
        ...     path="s3://lake/warehouse/dim_customers/",
        ...     mode="merge",
        ...     merge_keys=["customer_id"],
        ...     delete_not_matched=True,
        ... )
    """
    import time as _time
    from pyspark.sql import functions as F

    t0 = _time.time()

    # ── Normalise alias ───────────────────────────────────────────────
    if mode == "overwrite_partitions":
        mode = "insert_update"

    if mode not in _VALID_MODES:
        raise ValueError(f"Invalid mode {mode!r}. Choose from: {', '.join(_VALID_MODES)}")
    if mode == "merge" and not merge_keys and not merge_condition:
        raise ValueError("merge_keys (or merge_condition) is required when mode='merge'.")
    if mode != "dataframe" and (database is None or table is None or path is None):
        raise ValueError("database, table, and path are required for all write modes.")

    spark = _get_or_create_spark(spark)
    fqn = f"{database}.{table}" if database and table else None
    parts = partitions or []
    metrics: dict = {"mode": mode}

    # ── Initialise pipeline logger ────────────────────────────────────
    try:
        gc = _get_or_create_glue_context(glue_context, spark)
    except Exception:
        gc = None
    log = _PipelineLogger(gc)

    # ==================================================================
    #  START
    # ==================================================================
    log.header(f"write_dataframe  |  mode={mode}  |  target={fqn or 'dataframe'}")
    log.step(0, "START", f"Pipeline initiated at {_time.strftime('%Y-%m-%d %H:%M:%S')}")
    log.sub(f"Mode            : {mode}")
    log.sub(f"Target table    : {fqn or '(none — dataframe mode)'}")
    log.sub(f"Target path     : {path or '(none)'}")
    log.sub(f"File format     : {file_format}  |  compression: {compression}")
    log.sub(f"Partitions      : {parts or '(none)'}")
    if merge_keys:
        log.sub(f"Merge keys      : {merge_keys}")
    if dry_run:
        log.sub("Dry run         : YES — no data will be written")
    log.separator()

    # ==================================================================
    #  1. SOURCE
    # ==================================================================
    source_type = "Athena SQL (UNLOAD → Parquet)" if sql else "Spark DataFrame"
    log.step(1, "SOURCE", f"Loading source data ({source_type})")
    if sql:
        log.sub(f"SQL: {sql[:200]}{'...' if len(sql) > 200 else ''}")
        log.sub("Athena UNLOAD will export as Parquet (native types preserved)")

    df = _resolve_dataframe(
        df=dataframe,
        sql=sql,
        athena_workgroup=athena_workgroup,
        athena_output=athena_output,
        athena_database=athena_database or database,
        region=region,
        spark=spark,
        compression=compression.upper(),
    )
    metrics["source"] = "sql" if sql else "dataframe"
    log.sub(f"Columns loaded  : {len(df.columns)}")
    log.sub(f"Column list     : {', '.join(df.columns[:20])}{'...' if len(df.columns) > 20 else ''}")
    log.sub("Source loaded successfully")

    # ── PRE-SQL HOOK ──────────────────────────────────────────────────
    if pre_sql:
        stmts = [pre_sql] if isinstance(pre_sql, str) else pre_sql
        log.sub(f"Executing {len(stmts)} pre-SQL statement(s)")
        for i, stmt in enumerate(stmts, 1):
            log.sub(f"  pre-SQL [{i}]: {stmt[:120]}{'...' if len(stmt) > 120 else ''}")
            spark.sql(stmt)
        log.sub("Pre-SQL hooks completed")

    log.separator()

    # ==================================================================
    #  2. SCHEMA
    # ==================================================================
    log.step(2, "SCHEMA", "Applying schema transforms")
    schema_changed = False

    if drop_columns:
        cols_to_drop = [c for c in drop_columns if c in df.columns]
        if cols_to_drop:
            df = df.drop(*cols_to_drop)
            log.sub(f"Dropped columns : {cols_to_drop}")
            schema_changed = True
        else:
            log.sub(f"Drop requested for {drop_columns} but none found in DataFrame")

    if cast_columns:
        casted = []
        for col_name, col_type in cast_columns.items():
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast(col_type))
                casted.append(f"{col_name}->{col_type}")
        if casted:
            log.sub(f"Cast columns    : {', '.join(casted)}")
            schema_changed = True

    if enforce_schema and fqn:
        try:
            target_cols = [f.name for f in spark.table(fqn).schema.fields]
            before = set(df.columns)
            df = df.select(*[c for c in target_cols if c in df.columns])
            dropped = before - set(df.columns)
            if dropped:
                log.sub(f"Schema enforced : dropped extra columns {dropped}")
            else:
                log.sub("Schema enforced : all columns match target")
            schema_changed = True
        except Exception:
            log.sub("Schema enforce skipped — target table does not exist yet")

    if evolve_schema:
        log.sub("Schema evolution : ENABLED (mergeSchema)")

    # ── WATERMARK / INCREMENTAL FILTER ────────────────────────────────
    if watermark_column:
        if watermark_value is None and fqn:
            try:
                row = spark.sql(f"SELECT MAX({watermark_column}) AS wm FROM {fqn}").first()
                watermark_value = row["wm"] if row else None
                log.sub(f"Watermark auto-detected: {watermark_column} > {watermark_value}")
            except Exception:
                watermark_value = None
                log.sub(f"Watermark column '{watermark_column}' — could not read max from target")
        if watermark_value is not None:
            df = df.filter(F.col(watermark_column) > F.lit(watermark_value))
            log.sub(f"Watermark filter : {watermark_column} > {watermark_value}")
            metrics["watermark_applied"] = str(watermark_value)
            schema_changed = True

    if not schema_changed:
        log.sub("No schema transforms applied — pass-through")

    # ── REGISTER TEMP VIEW ────────────────────────────────────────────
    if temp_view:
        df.createOrReplaceTempView(temp_view)
        log.sub(f"Temp view registered: {temp_view}")

    log.sub(f"Final schema    : {len(df.columns)} columns")
    log.separator()

    # ==================================================================
    #  3. DATAFRAME MODE — return early
    # ==================================================================
    if mode == "dataframe":
        log.step(3, "RETURN", "Mode is 'dataframe' — returning DataFrame without writing")
        elapsed = round(_time.time() - t0, 2)
        log.header(f"END  |  mode=dataframe  |  elapsed={elapsed}s")
        return df

    # ==================================================================
    #  4. REPARTITION
    # ==================================================================
    log.step(3, "REPARTITION", "Computing file layout")

    if max_records_per_file:
        spark.conf.set("spark.sql.files.maxRecordsPerFile", str(max_records_per_file))
        log.sub(f"maxRecordsPerFile = {max_records_per_file}")

    if repartition is not None:
        if isinstance(repartition, int):
            df = df.repartition(repartition)
            log.sub(f"Repartitioned to {repartition} partition(s) (shuffle)")
        elif isinstance(repartition, list):
            df = df.repartition(*[F.col(c) for c in repartition])
            log.sub(f"Repartitioned by columns: {repartition}")
    elif coalesce is not None:
        df = df.coalesce(coalesce)
        log.sub(f"Coalesced to {coalesce} partition(s) (no shuffle)")
    elif target_file_size_mb and mode != "merge":
        estimated = _estimate_df_bytes(df, spark)
        if estimated > 0:
            ideal = max(1, estimated // (target_file_size_mb * _BYTES_PER_MB))
            df = df.coalesce(int(ideal))
            metrics["auto_coalesce"] = int(ideal)
            est_mb = round(estimated / _BYTES_PER_MB, 1)
            log.sub(f"Estimated size  : {est_mb} MB")
            log.sub(f"Auto-coalesce   : {int(ideal)} file(s)  (target {target_file_size_mb} MB each)")
        else:
            log.sub("Could not estimate size — skipping auto-coalesce")
    else:
        log.sub("No repartition/coalesce configured")

    if sort_within_partitions:
        df = df.sortWithinPartitions(*sort_within_partitions)
        log.sub(f"Sort within partitions by: {sort_within_partitions}")

    log.separator()

    # ==================================================================
    #  5. DRY RUN — return without writing
    # ==================================================================
    if dry_run:
        log.step(4, "DRY RUN", "Computing preview metrics (no write)")
        row_count = df.count()
        metrics["rows"] = row_count
        metrics["columns"] = len(df.columns)
        metrics["schema"] = df.schema.simpleString()
        log.sub(f"Rows            : {row_count:,}")
        log.sub(f"Columns         : {len(df.columns)}")
        elapsed = round(_time.time() - t0, 2)
        log.header(f"END  |  DRY RUN  |  {row_count:,} rows  |  elapsed={elapsed}s")
        return {"df": df, "metrics": metrics}

    # ==================================================================
    #  6. CATALOG PREP
    # ==================================================================
    log.step(4, "CATALOG", "Preparing catalog")

    if create_database and database:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
        log.sub(f"Database ensured : {database}")

    schema_option = {}
    if evolve_schema:
        schema_option["mergeSchema"] = "true"

    log.separator()

    # ==================================================================
    #  7. WRITE / MERGE
    # ==================================================================
    if mode == "merge":
        log.step(5, "MERGE", f"Executing merge/upsert into {fqn}")
        if merge_keys:
            log.sub(f"Merge keys      : {merge_keys}")
        if merge_condition:
            log.sub(f"Merge condition : {merge_condition}")
        log.sub(f"Delete unmatched: {delete_not_matched}")

        # ── Read existing data ────────────────────────────────────────
        try:
            existing = spark.read.format(file_format).load(path)
            table_exists = True
            log.sub("Existing target data loaded from path")
        except Exception:
            existing = None
            table_exists = False
            log.sub("No existing data at target path — first write")

        if existing is not None:
            src_alias = "__write_df_src"
            tgt_alias = "__write_df_tgt"
            src = df.alias(src_alias)
            tgt = existing.alias(tgt_alias)

            if merge_condition:
                join_expr = F.expr(
                    merge_condition
                    .replace("src.", f"{src_alias}.")
                    .replace("tgt.", f"{tgt_alias}.")
                )
            else:
                join_expr = F.lit(True)
                for k in merge_keys:
                    join_expr = join_expr & (F.col(f"{src_alias}.{k}") == F.col(f"{tgt_alias}.{k}"))

            kept = tgt.join(src, on=merge_keys or list(cast_columns or {}.keys()), how="left_anti")

            if delete_not_matched:
                merged = src
                log.sub("Strategy: REPLACE — only source rows kept (delete_not_matched=True)")
            else:
                merged = kept.unionByName(src, allowMissingColumns=True)
                log.sub("Strategy: UPSERT — target anti-join + union source rows")
        else:
            merged = df

        # Re-compute file sizing for merged result
        estimated = _estimate_df_bytes(merged, spark)
        if estimated > 0 and target_file_size_mb:
            ideal = max(1, estimated // (target_file_size_mb * _BYTES_PER_MB))
            merged = merged.coalesce(int(ideal))
            log.sub(f"Merged result coalesced to {int(ideal)} file(s)")

        log.sub(f"Writing merged result to {path}")
        writer = (
            merged.write.mode("overwrite")
            .format(file_format)
            .option("compression", compression)
            .option("path", path)
        )
        if parts:
            writer = writer.partitionBy(*parts)
        writer.saveAsTable(fqn)

        metrics["action"] = "merged"
        metrics["merge_keys"] = merge_keys
        metrics["delete_not_matched"] = delete_not_matched
        log.sub(f"Merge completed into {fqn}")

    else:
        # ── STANDARD WRITE (new / insert / insert_update) ─────────────
        log.step(5, "WRITE", f"Writing data  |  mode={mode}  |  format={file_format}")

        if mode == "new":
            writer = (
                df.write.mode("overwrite")
                .format(file_format)
                .option("compression", compression)
                .option("path", path)
            )
            if evolve_schema:
                writer = writer.option("overwriteSchema", "true")
            if parts:
                writer = writer.partitionBy(*parts)
            writer.saveAsTable(fqn)
            metrics["action"] = "created"
            log.sub(f"Table CREATED (full overwrite): {fqn}")

        elif mode == "insert":
            writer = (
                df.write.mode("append")
                .format(file_format)
                .option("compression", compression)
                .option("path", path)
            )
            for k, v in schema_option.items():
                writer = writer.option(k, v)
            if parts:
                writer = writer.partitionBy(*parts)
            writer.saveAsTable(fqn)
            metrics["action"] = "appended"
            log.sub(f"Rows APPENDED to: {fqn}")

        elif mode == "insert_update":
            if not parts:
                raise ValueError("insert_update / overwrite_partitions requires at least one partition column.")
            if dynamic_partition:
                spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
                log.sub("Dynamic partition overwrite ENABLED")
            writer = (
                df.write.mode("overwrite")
                .format(file_format)
                .option("compression", compression)
                .option("path", path)
                .partitionBy(*parts)
            )
            for k, v in schema_option.items():
                writer = writer.option(k, v)
            writer.saveAsTable(fqn)
            metrics["action"] = "dynamic_partition_overwrite"
            log.sub(f"Partitions OVERWRITTEN in: {fqn}")
            log.sub(f"Partition columns: {parts}")

        log.sub(f"Path: {path}")

    log.separator()

    # ==================================================================
    #  8. COMPACTION
    # ==================================================================
    compaction_ran = False

    if auto_compact and path:
        log.step(6, "COMPACTION", "Post-write small-file compaction")
        try:
            _compact_small_files(
                spark, path, file_format, target_file_size_mb,
                parts if not compact_only_touched_partitions else None,
            )
            metrics["compacted"] = True
            compaction_ran = True
            log.sub(f"Compaction completed  (target {target_file_size_mb} MB/file)")
        except Exception as e:
            metrics["compaction_error"] = str(e)
            log.warn(f"Compaction failed: {e}")

    if optimize_after_write and fqn:
        log.step(6, "COMPACTION", f"Running OPTIMIZE on {fqn}")
        try:
            spark.sql(f"OPTIMIZE {fqn}")
            if zorder:
                spark.sql(f"OPTIMIZE {fqn} ZORDER BY ({', '.join(zorder)})")
                log.sub(f"OPTIMIZE + ZORDER BY ({', '.join(zorder)})")
            else:
                log.sub("OPTIMIZE completed")
            metrics["optimized"] = True
            compaction_ran = True
        except Exception as e:
            metrics["optimize_error"] = str(e)
            log.warn(f"OPTIMIZE failed: {e}")

    if not compaction_ran:
        log.step(6, "COMPACTION", "Skipped (not requested)")

    log.separator()

    # ==================================================================
    #  9. CATALOG (post-write metadata)
    # ==================================================================
    log.step(7, "CATALOG", "Updating catalog metadata")
    catalog_updated = False

    # ── REPAIR PARTITIONS ─────────────────────────────────────────────
    if repair_table and parts:
        try:
            spark.sql(f"MSCK REPAIR TABLE {fqn}")
            log.sub(f"MSCK REPAIR TABLE {fqn} — partitions synced")
            catalog_updated = True
        except Exception as e:
            metrics["repair_error"] = str(e)
            log.warn(f"MSCK REPAIR failed: {e}")

    # ── TABLE COMMENT ─────────────────────────────────────────────────
    if table_comment and fqn:
        try:
            safe_comment = table_comment.replace("'", "\\'")
            spark.sql(f"ALTER TABLE {fqn} SET TBLPROPERTIES ('comment' = '{safe_comment}')")
            log.sub(f"Table comment set: '{table_comment[:60]}{'...' if len(table_comment) > 60 else ''}'")
            catalog_updated = True
        except Exception:
            log.warn("Failed to set table comment")

    # ── TABLE PROPERTIES ──────────────────────────────────────────────
    if table_properties and fqn:
        try:
            props = ", ".join(f"'{k}' = '{v}'" for k, v in table_properties.items())
            spark.sql(f"ALTER TABLE {fqn} SET TBLPROPERTIES ({props})")
            log.sub(f"Table properties: {list(table_properties.keys())}")
            catalog_updated = True
        except Exception:
            log.warn("Failed to set table properties")

    # ── COLUMN COMMENTS ───────────────────────────────────────────────
    if column_comments and fqn:
        success_count = 0
        for col_name, comment in column_comments.items():
            try:
                safe = comment.replace("'", "\\'")
                spark.sql(f"ALTER TABLE {fqn} CHANGE COLUMN {col_name} COMMENT '{safe}'")
                success_count += 1
            except Exception:
                log.warn(f"Failed to set comment on column '{col_name}'")
        log.sub(f"Column comments : {success_count}/{len(column_comments)} set")
        catalog_updated = True

    if not catalog_updated:
        log.sub("No catalog metadata updates needed")

    log.separator()

    # ==================================================================
    #  10. POST-SQL HOOK
    # ==================================================================
    if post_sql:
        stmts = [post_sql] if isinstance(post_sql, str) else post_sql
        log.step(8, "POST-SQL", f"Executing {len(stmts)} post-SQL statement(s)")
        for i, stmt in enumerate(stmts, 1):
            log.sub(f"  post-SQL [{i}]: {stmt[:120]}{'...' if len(stmt) > 120 else ''}")
            spark.sql(stmt)
        log.sub("Post-SQL hooks completed")
        log.separator()

    # ==================================================================
    #  11. METRICS
    # ==================================================================
    elapsed = round(_time.time() - t0, 2)
    metrics["table"] = fqn
    metrics["path"] = path
    metrics["file_format"] = file_format
    metrics["compression"] = compression
    metrics["partitions"] = parts
    metrics["elapsed_seconds"] = elapsed

    try:
        row_count = df.count()
        metrics["rows_written"] = row_count
    except Exception:
        row_count = "unknown"
        metrics["rows_written"] = row_count

    log.step(9, "METRICS", "Pipeline summary")
    log.sub(f"Table           : {fqn}")
    log.sub(f"Path            : {path}")
    log.sub(f"Mode            : {mode}")
    log.sub(f"Action          : {metrics.get('action', 'N/A')}")
    log.sub(f"Rows written    : {row_count:,}" if isinstance(row_count, int) else f"Rows written    : {row_count}")
    log.sub(f"File format     : {file_format}  |  compression: {compression}")
    log.sub(f"Partitions      : {parts or '(none)'}")
    if "auto_coalesce" in metrics:
        log.sub(f"Output files    : {metrics['auto_coalesce']}  (auto-coalesced)")
    if "watermark_applied" in metrics:
        log.sub(f"Watermark       : {metrics['watermark_applied']}")
    if "compacted" in metrics:
        log.sub("Post-compaction : YES")
    if "optimized" in metrics:
        log.sub("Optimized       : YES")
    log.sub(f"Elapsed         : {elapsed}s")

    # ==================================================================
    #  END
    # ==================================================================
    log.header(f"END  |  {mode} on {fqn}  |  {row_count if isinstance(row_count, int) else '?':,} rows  |  {elapsed}s" if isinstance(row_count, int) else f"END  |  {mode} on {fqn}  |  {row_count} rows  |  {elapsed}s")

    if return_metrics:
        return metrics

    return f"{mode} on {fqn} completed in {elapsed}s"
