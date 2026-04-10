"""
AWSome — A Python toolkit for AWS Data Engineering with Spark & Glue.

Modules:
    catalog     - Glue Data Catalog operations (write, read, register, partitions)
    glue        - Glue job management (list, cost, audit, logging)
    s3          - S3 operations (sync, upload, download, list, check, etc.)
    athena      - Athena query execution and result retrieval
    redshift    - Redshift COPY and data loading
    spark_utils - Spark/Glue DataFrame helpers
    monitoring  - Step Functions & Glue job audit reporting
    quality     - Data quality checks via Athena
    utils       - General-purpose helpers (dates, strings, bytes, etc.)

Quick Start:
    >>> from awsome.catalog import write_dataframe, read_catalog_table, register_table
    >>> from awsome.s3 import s3_exists, s3_list_size, s3_sync
    >>> from awsome.glue import list_jobs, estimate_job_cost, log
    >>> from awsome.athena import run_query, fetch_query_results
    >>> from awsome.utils import parse_date, clean_string, business_days
"""

__version__ = "2.1.0"
__author__ = "Gustavo Barreto"
