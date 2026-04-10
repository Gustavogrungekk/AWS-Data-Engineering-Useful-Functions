# AWSesome — AWS Data Engineering Toolkit

A modular Python library of battle-tested helper functions for **AWS Glue**, **Spark**, **Athena**, **S3**, **Redshift**, and **Step Functions**. Drop it into any Glue job, EMR notebook, or local Spark session and get productive immediately.

## Package Structure

```
awsesome/
├── __init__.py        # Package metadata & convenient re-exports
├── s3.py              # S3 operations (sync, upload, download, list, restore …)
├── glue.py            # Glue Catalog I/O, table registration, job audit reports
├── athena.py          # Athena query execution & result retrieval
├── redshift.py        # Redshift COPY from S3
├── spark_utils.py     # Spark DataFrame helpers (size estimation, CDP transfer)
├── monitoring.py      # Step Functions execution reporting
├── quality.py         # Data quality record-count checks via Athena
└── utils.py           # General utilities (dates, strings, byte conversion …)
```

## Installation

```bash
pip install -r requirements.txt
```

> **Note:** `pyspark` and `awsglue` are pre-installed in AWS Glue environments. You don't need to install them locally unless you're testing outside Glue.

### Using in AWS Glue Jobs

1. **Zip the package** and upload to S3:
   ```bash
   cd /path/to/this/repo
   zip -r awsesome.zip awsesome/
   aws s3 cp awsesome.zip s3://your-bucket/libs/awsesome.zip
   ```

2. **Configure the Glue job**: In the job's **Advanced properties → Libraries → Python library path**, add:
   ```
   s3://your-bucket/libs/awsesome.zip
   ```

3. **Import in your Glue script**:
   ```python
   from awsesome.glue import read_catalog_table, log
   from awsesome.s3 import s3_exists, s3_put_success_flag
   ```

---

## Quick Reference

### S3 Operations (`awsesome.s3`)

```python
from awsesome.s3 import (
    s3_exists, s3_sync, s3_upload, s3_download,
    s3_list_size, s3_put_success_flag, s3_check_success_flag,
    s3_last_modified, s3_clear_prefix, s3_restore_deleted,
    s3_object_details, s3_upload_local_files,
)

# Check if a prefix contains data
s3_exists("s3://my-bucket/warehouse/events/")  # → True

# Sync an entire prefix to a local directory
files = s3_sync("s3://my-bucket/exports/", "/tmp/exports")

# Upload / download single files
s3_upload("/tmp/data.parquet", "s3://bucket/raw/data.parquet")
s3_download("s3://bucket/raw/data.parquet", "/tmp/data.parquet")

# Get total size under a prefix
info = s3_list_size("s3://my-bucket/warehouse/")
print(info["total_human"])  # '2.34 GB'

# Success flag management
s3_put_success_flag("s3://bucket/table/dt=2024-06-01/")
s3_check_success_flag("s3://bucket/table/dt=2024-06-01/")  # → True

# Most recent modification timestamp
s3_last_modified("s3://bucket/data/")

# Delete all objects under a prefix
s3_clear_prefix("s3://bucket/tmp/athena-results/")

# Restore deleted objects (versioned buckets)
s3_restore_deleted("my-bucket", prefix="data/", hours=24)

# Batch-upload local files with format conversion
s3_upload_local_files(
    local_dir="/data/exports",
    s3_bucket="analytics-bucket",
    s3_prefix="raw/",
    file_extension="csv",
    output_format="parquet",
)
```

### Glue Catalog (`awsesome.glue`)

```python
from awsesome.glue import (
    read_catalog_table, register_table,
    get_latest_partition, get_last_partition_spark,
    list_jobs, estimate_job_cost, glue_job_audit_report, log,
)

# Read a catalog table into a Spark DataFrame (+ temp view)
df = read_catalog_table("analytics", "events", view="events")
spark.sql("SELECT count(*) FROM events").show()

# Register S3 data as a Glue table (like a mini-crawler)
register_table("analytics", "events", "s3://bucket/events/", "parquet")

# Get the latest partition clause
clause = get_latest_partition("analytics", "events")
# → "year=2024 AND month=06"

# List Glue jobs by name pattern
list_jobs("etl")  # → ['etl-customers', 'etl-orders']

# Estimate cost of a specific job run
estimate_job_cost("etl-orders", "jr_abc123")  # → 0.37

# Full audit report for multiple jobs
df = glue_job_audit_report(["etl-orders", "etl-customers"])

# Log from a Glue job
log("Step 3 complete — 1.2M rows written.")
```

### Athena (`awsesome.athena`)

```python
from awsesome.athena import (
    run_query, fetch_query_results,
    save_query_to_s3, athena_audit_report,
)

# Run a query and get the results as a DataFrame
qid = run_query("SELECT count(*) FROM analytics.events", database="analytics")
df = fetch_query_results(qid)

# Run a query and save results as Parquet in S3
save_query_to_s3(
    "SELECT * FROM analytics.events WHERE year='2024'",
    "s3://bucket/warehouse/events_2024/",
    athena_output="s3://bucket/athena-results/",
)

# Audit report: queries, bytes scanned, cost
df = athena_audit_report(["primary", "analytics"], hours=48)
```

### Redshift (`awsesome.redshift`)

```python
from awsesome.redshift import copy_from_s3

copy_from_s3(
    "s3://gold-lake/events/",
    "analytics.events",
    host="cluster.abc.us-east-1.redshift.amazonaws.com",
    database="warehouse",
    user="admin",
    password="***",
    iam_role="arn:aws:iam::123456789012:role/RedshiftCopyRole",
)
```

### Spark Utilities (`awsesome.spark_utils`)

```python
from awsesome.spark_utils import estimate_df_size, cdp_to_s3

# Estimate DataFrame size
print(estimate_df_size(df))  # '256.7 MB'

# Transfer data from CDP Hive to S3
cdp_to_s3(
    jdbc_url="jdbc:hive2://server:10000/default",
    query="SELECT * FROM events",
    s3_output="s3://bucket/events/",
    username="user",
    password="pass",
)
```

### Monitoring (`awsesome.monitoring`)

```python
from awsesome.monitoring import step_functions_report

df = step_functions_report(
    ["arn:aws:states:us-east-1:123456789012:stateMachine:MyPipeline"],
    hours=48,
)
```

### Data Quality (`awsesome.quality`)

```python
from awsesome.quality import run_quality_check

df = run_quality_check(["analytics.events", "analytics.users"])
df[df["record_count"] == 0]  # find empty partitions
```

### General Utilities (`awsesome.utils`)

```python
from awsesome.utils import (
    convert_bytes, parse_s3_uri, parse_date,
    clean_string, business_days,
)

convert_bytes(1_610_612_736)   # → '1.5 GB'
parse_s3_uri("s3://bucket/key/")  # → ('bucket', 'key/')
parse_date("15/06/2024")       # → '2024-06-15'
clean_string("Açúcar!")        # → 'Acucar!'
business_days("BR", "2024-12-23", "2024-12-31")
```

---

## Backward Compatibility

The legacy `AWSesome_modules.py` file still works. It re-exports everything from the new package so existing `from AWSesome_modules import log` imports continue to work.

---

## Contributing

Pull requests and issue reports are welcome! If you have a useful AWS data engineering function, feel free to contribute.

## Author

**Gustavo Barreto** — [GitHub](https://github.com/gustavogrungekk)