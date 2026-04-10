"""
AWSesome Modules — Backward-compatibility shim.

This file re-exports every public function from the ``awsesome`` package so
that existing imports like ``from AWSesome_modules import log`` keep working.

**New code should import directly from the sub-modules:**

    from awsesome.s3 import s3_exists
    from awsesome.glue import read_catalog_table
    from awsesome.athena import run_query
    from awsesome.utils import parse_date, clean_string
"""

# ── Utilities ─────────────────────────────────────────────────────────────
from awsesome.utils import (
    convert_bytes,
    parse_s3_uri as get_bucket,
    parse_date as try_date,
    clean_string as clear_string,
    business_days as get_business_days,
)

# ── S3 ────────────────────────────────────────────────────────────────────
from awsesome.s3 import (
    s3_exists as s3_contains,
    s3_sync as sync_s3_bucket,
    s3_upload,
    s3_download,
    s3_list_size as list_s3_size,
    s3_put_success_flag as success,
    s3_check_success_flag as check_success,
    s3_last_modified as get_last_modified_date,
    s3_clear_prefix as clear_s3_bucket,
    s3_restore_deleted as restore_deleted_objects_S3,
    s3_object_details as get_s3_objects_details,
    s3_upload_local_files as list_and_upload_files,
)

# ── Glue ──────────────────────────────────────────────────────────────────
from awsesome.glue import (
    read_catalog_table as get_table,
    register_table as create_update_table,
    get_latest_partition as get_calatog_latest_partition,
    get_last_partition_spark as get_partition,
    list_jobs,
    estimate_job_cost,
    glue_job_audit_report as get_glue_job_audit_report,
    log,
)

# ── Athena ────────────────────────────────────────────────────────────────
from awsesome.athena import (
    run_query as run_athena_query,
    save_query_to_s3 as save_athena_results,
    athena_audit_report as athena_query_audit_report,
)

# ── Redshift ──────────────────────────────────────────────────────────────
from awsesome.redshift import copy_from_s3 as copy_redshift

# ── Spark utilities ───────────────────────────────────────────────────────
from awsesome.spark_utils import (
    estimate_df_size as estimate_size,
    cdp_to_s3,
    process_s3_files_to_catalog as process_local_files,
)

# ── Monitoring ────────────────────────────────────────────────────────────
from awsesome.monitoring import step_functions_report as monitor_state_machines

# ── Quality ───────────────────────────────────────────────────────────────
from awsesome.quality import run_quality_check
