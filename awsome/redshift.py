"""
Amazon Redshift data-loading helpers.

Functions:
    copy_from_s3 - COPY Parquet data from S3 into an existing or new Redshift table
"""

from __future__ import annotations

import re
from typing import Optional

import psycopg2


def copy_from_s3(
    s3_path: str,
    table: str,
    *,
    host: str,
    database: str,
    user: str,
    password: str,
    iam_role: str,
    port: int = 5439,
    truncate_first: bool = True,
    create_if_missing: bool = True,
    spark=None,
) -> None:
    """COPY Parquet data from S3 into a Redshift table.

    If the table does not exist and *create_if_missing* is ``True``, the
    function reads the S3 data with Spark to infer the schema, creates an
    empty table via JDBC, then runs the ``COPY`` command.

    Args:
        s3_path: S3 URI pointing to Parquet files
            (e.g. ``"s3://bucket/warehouse/table/"``).
        table: Fully-qualified Redshift table name
            (e.g. ``"public.my_table"``).
        host: Redshift cluster endpoint.
        database: Redshift database name.
        user: Database user.
        password: Database password.
        iam_role: IAM role ARN used by the ``COPY`` command.
        port: Redshift port (default 5439).
        truncate_first: If ``True``, truncate the table before loading.
        create_if_missing: If ``True`` and the table doesn't exist, create it
            from the Parquet schema.
        spark: Existing ``SparkSession`` (only needed if *create_if_missing*
            is ``True``).

    Example:
        >>> copy_from_s3(
        ...     "s3://gold-lake/events/",
        ...     "analytics.events",
        ...     host="cluster.abc.us-east-1.redshift.amazonaws.com",
        ...     database="warehouse",
        ...     user="admin",
        ...     password="***",
        ...     iam_role="arn:aws:iam::123456789012:role/RedshiftCopyRole",
        ... )
    """
    jdbc_url = f"jdbc:redshift://{host}:{port}/{database}?user={user}&password={password}"

    conn = psycopg2.connect(
        dbname=database, user=user, password=password, host=host, port=port
    )
    conn.autocommit = False
    cursor = conn.cursor()

    try:
        # Ensure the table exists
        cursor.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema || '.' || table_name = %s",
            (table.lower(),),
        )
        table_exists = cursor.fetchone() is not None
        conn.commit()

        if not table_exists:
            if not create_if_missing:
                raise ValueError(f"Table {table} does not exist and create_if_missing is False.")
            # Create from Spark schema via JDBC
            from pyspark.sql import SparkSession

            spark = spark or SparkSession.builder.getOrCreate()
            df = spark.read.parquet(s3_path).limit(0)
            df.write.format("jdbc").option("url", jdbc_url).option("dbtable", table).mode(
                "overwrite"
            ).save()

        if truncate_first and table_exists:
            cursor.execute(f"TRUNCATE {table}")
            conn.commit()

        # Strip wildcard / partition segments from the path for COPY
        clean_path = re.sub(r"/?\*.*$", "/", s3_path)

        copy_sql = f"""
            COPY {table}
            FROM '{clean_path}'
            IAM_ROLE '{iam_role}'
            ACCEPTINVCHARS
            PARQUET;
        """
        cursor.execute(copy_sql)
        conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()
