"""
Read tables from the AWS Glue Data Catalog into Spark DataFrames.

Usage:
    >>> from awsome.catalog import read_catalog_table
    >>> from awsome.catalog.read import read_catalog_table   # equivalent
"""

from __future__ import annotations

from typing import Optional

from awsome.catalog._helpers import _get_or_create_glue_context


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
