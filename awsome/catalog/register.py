"""
Register (create / update) tables in the AWS Glue Data Catalog by inferring
schema from S3 data — a lightweight alternative to running a Crawler.

Usage:
    >>> from awsome.catalog import register_table
    >>> from awsome.catalog.register import register_table   # equivalent
"""

from __future__ import annotations

import boto3

from awsome.catalog._helpers import _get_or_create_spark


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
