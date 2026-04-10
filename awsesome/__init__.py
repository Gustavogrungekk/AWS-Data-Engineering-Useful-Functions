"""
AWSesome — A Python toolkit for AWS Data Engineering with Spark & Glue.

Modules:
    s3          - S3 operations (sync, upload, download, list, check, etc.)
    glue        - AWS Glue Catalog operations (tables, crawlers, jobs)
    athena      - Athena query execution and result retrieval
    redshift    - Redshift COPY and data loading
    spark_utils - Spark/Glue DataFrame helpers
    monitoring  - Step Functions & Glue job audit reporting
    quality     - Data quality checks via Athena
    utils       - General-purpose helpers (dates, strings, bytes, etc.)

Quick Start:
    >>> from awsesome.s3 import s3_exists, s3_list_size, s3_sync
    >>> from awsesome.glue import read_catalog_table, register_table
    >>> from awsesome.athena import run_query, fetch_query_results
    >>> from awsesome.utils import parse_date, clean_string, business_days
"""

__version__ = "2.0.0"
__author__ = "Gustavo Barreto"
