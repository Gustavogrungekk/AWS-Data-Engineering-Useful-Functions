"""Shared helpers for the ``awsome.catalog`` subpackage."""

from __future__ import annotations

import logging
from typing import Optional

import boto3

# ---------------------------------------------------------------------------
# Module-level Python logger (works everywhere: Glue, EMR, local)
# ---------------------------------------------------------------------------
_logger = logging.getLogger("awsome.catalog")
if not _logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(message)s"))
    _logger.addHandler(_handler)
    _logger.setLevel(logging.INFO)


_TAG = "AWSome"
_SEPARATOR = "=" * 72


class _PipelineLogger:
    """Structured logger for ``write_dataframe`` pipeline steps.

    Emits messages both to the Glue logger (when available) *and* to
    Python's standard ``logging`` so they always appear in CloudWatch /
    the local console.
    """

    def __init__(self, glue_context=None):
        self._gc = glue_context
        self._glue_logger = None
        if self._gc is not None:
            try:
                self._glue_logger = self._gc.get_logger()
            except Exception:
                pass

    # ── helpers ───────────────────────────────────────────────────────
    def _emit(self, msg: str) -> None:
        """Write to both Glue logger and Python logger."""
        formatted = f"[{_TAG}] {msg}"
        if self._glue_logger:
            self._glue_logger.info(formatted)
        _logger.info(formatted)

    # ── public API ────────────────────────────────────────────────────
    def header(self, title: str) -> None:
        self._emit(_SEPARATOR)
        self._emit(f"  {title}")
        self._emit(_SEPARATOR)

    def step(self, step_num: int, stage: str, detail: str = "") -> None:
        tag = f"[Step {step_num:>2}] [{stage}]"
        if detail:
            self._emit(f"{tag}  {detail}")
        else:
            self._emit(tag)

    def sub(self, message: str) -> None:
        self._emit(f"         -> {message}")

    def warn(self, message: str) -> None:
        self._emit(f"         ⚠  {message}")

    def separator(self) -> None:
        self._emit("-" * 72)


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


_BYTES_PER_MB = 1024 * 1024


def _resolve_dataframe(
    df=None,
    sql: Optional[str] = None,
    athena_workgroup: str = "primary",
    athena_output: Optional[str] = None,
    athena_database: Optional[str] = None,
    region: str = "us-east-1",
    spark=None,
):
    """Return a Spark DataFrame — either the one passed in, or one built
    by executing *sql* on Athena and reading the resulting CSV with Spark."""
    if df is not None and sql is not None:
        raise ValueError("Provide either 'dataframe' or 'sql', not both.")
    if df is None and sql is None:
        raise ValueError("You must provide either 'dataframe' (Spark DF) or 'sql' (Athena query).")

    if df is not None:
        return df

    from awsome.athena import run_query as _run_athena

    qid = _run_athena(
        sql,
        database=athena_database,
        workgroup=athena_workgroup,
        output_location=athena_output,
        region=region,
    )

    athena_client = boto3.client("athena", region_name=region)
    qe = athena_client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
    csv_s3_path = qe["ResultConfiguration"]["OutputLocation"]

    spark = _get_or_create_spark(spark)
    return spark.read.option("header", "true").option("inferSchema", "true").csv(csv_s3_path)


def _estimate_df_bytes(df, spark) -> int:
    """Best-effort estimate of in-memory size; falls back to row-count heuristic."""
    try:
        import repartipy
        with repartipy.SizeEstimator(spark=spark, df=df) as se:
            return int(se.estimate())
    except Exception:
        try:
            return df.count() * len(df.columns) * 50
        except Exception:
            return 0


def _compact_small_files(spark, path: str, file_format: str, target_mb: int, partitions: list[str] | None):
    """Re-read and re-write a path to consolidate small files."""
    reader = spark.read.format(file_format)
    compacted = reader.load(path)
    size_bytes = _estimate_df_bytes(compacted, spark)
    ideal = max(1, size_bytes // (target_mb * _BYTES_PER_MB))
    compacted = compacted.coalesce(int(ideal))
    writer = compacted.write.mode("overwrite").format(file_format)
    if partitions:
        writer = writer.partitionBy(*partitions)
    writer.save(path)
