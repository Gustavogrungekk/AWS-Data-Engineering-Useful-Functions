"""
AWSome Catalog — Glue Data Catalog read/write operations.

Sub-modules:
    write       - write_dataframe: universal write engine for Spark → Catalog
    read        - read_catalog_table: read a catalog table into a Spark DataFrame
    register    - register_table: infer schema from S3 and create/update catalog entries
    partitions  - get_latest_partition, get_last_partition_spark

Usage:
    >>> from awsome.catalog import write_dataframe, read_catalog_table
    >>> from awsome.catalog import register_table
    >>> from awsome.catalog import get_latest_partition, get_last_partition_spark
"""

from awsome.catalog.write import write_dataframe
from awsome.catalog.read import read_catalog_table
from awsome.catalog.register import register_table
from awsome.catalog.partitions import get_latest_partition, get_last_partition_spark

__all__ = [
    "write_dataframe",
    "read_catalog_table",
    "register_table",
    "get_latest_partition",
    "get_last_partition_spark",
]
