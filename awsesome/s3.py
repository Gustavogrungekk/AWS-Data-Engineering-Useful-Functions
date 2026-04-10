"""
Amazon S3 helper functions.

Functions:
    s3_exists               - Check whether an S3 prefix/key contains any objects
    s3_sync                 - Download an entire S3 prefix to a local directory
    s3_upload / s3_download - Single-file upload or download
    s3_list_size            - Summarise total size and object count under a prefix
    s3_object_details       - Detailed metadata for every object in a bucket list
    s3_put_success_flag     - Write a ``_SUCCESS`` marker file
    s3_check_success_flag   - Check if the ``_SUCCESS`` marker exists
    s3_last_modified        - Get the most recent ``LastModified`` timestamp
    s3_clear_prefix         - Delete all objects under a prefix
    s3_restore_deleted      - Restore delete-marked objects (versioned buckets)
    s3_upload_local_files   - Batch-upload local files filtered by extension/name
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import boto3
import pandas as pd
import pyarrow.parquet as pq
import pytz
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

from awsesome.utils import convert_bytes, parse_s3_uri


# ---------------------------------------------------------------------------
# Existence / metadata
# ---------------------------------------------------------------------------

def s3_exists(s3_uri: str) -> bool:
    """Check whether *any* object exists under an S3 URI (prefix or key).

    Args:
        s3_uri: Full S3 URI, e.g. ``"s3://bucket/prefix/"``.

    Returns:
        ``True`` if at least one object is found.

    Example:
        >>> s3_exists("s3://my-bucket/warehouse/table/")
        True
    """
    bucket, prefix = parse_s3_uri(s3_uri)
    s3 = boto3.client("s3")
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return "Contents" in resp


def s3_last_modified(s3_uri: str, region: str = "us-east-1") -> Optional[datetime]:
    """Return the most recent ``LastModified`` timestamp under an S3 prefix.

    Args:
        s3_uri: S3 URI to scan.
        region: AWS region.

    Returns:
        A timezone-aware ``datetime``, or ``None`` if no objects found.

    Example:
        >>> s3_last_modified("s3://bucket/data/")
        datetime.datetime(2024, 6, 1, 12, 30, 0, tzinfo=...)
    """
    bucket, prefix = parse_s3_uri(s3_uri)
    s3 = boto3.client("s3", region_name=region)
    paginator = s3.get_paginator("list_objects_v2")
    latest = None
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if latest is None or obj["LastModified"] > latest:
                latest = obj["LastModified"]
    return latest


# ---------------------------------------------------------------------------
# Success flag helpers
# ---------------------------------------------------------------------------

def s3_put_success_flag(s3_uri: str) -> None:
    """Write an empty ``_SUCCESS`` marker object at the given S3 prefix.

    Args:
        s3_uri: S3 prefix where the marker should be placed.

    Example:
        >>> s3_put_success_flag("s3://bucket/warehouse/table/dt=2024-06-01/")
    """
    bucket, prefix = parse_s3_uri(s3_uri)
    key = prefix.rstrip("/") + "/_SUCCESS"
    boto3.client("s3").put_object(Body=b"", Bucket=bucket, Key=key)


def s3_check_success_flag(s3_uri: str) -> bool:
    """Check whether a ``_SUCCESS`` marker exists at the given S3 prefix.

    Args:
        s3_uri: S3 prefix to check.

    Returns:
        ``True`` if the marker exists.

    Example:
        >>> s3_check_success_flag("s3://bucket/warehouse/table/dt=2024-06-01/")
        True
    """
    bucket, prefix = parse_s3_uri(s3_uri)
    key = prefix.rstrip("/") + "/_SUCCESS"
    try:
        boto3.client("s3").head_object(Bucket=bucket, Key=key)
        return True
    except boto3.client("s3").exceptions.ClientError:
        return False


# ---------------------------------------------------------------------------
# Sync / upload / download
# ---------------------------------------------------------------------------

def s3_sync(s3_uri: str, local_dir: str) -> list[str]:
    """Download all objects under an S3 prefix to a local directory tree.

    Mirrors the directory structure found in S3 into *local_dir*.

    Args:
        s3_uri: S3 prefix to sync from.
        local_dir: Local destination directory.

    Returns:
        List of local file paths that were downloaded.

    Raises:
        ValueError: If no objects are found under *s3_uri*.

    Example:
        >>> files = s3_sync("s3://my-bucket/exports/", "/tmp/exports")
    """
    bucket, prefix = parse_s3_uri(s3_uri)
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    downloaded: list[str] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue  # skip directory markers
            relative = key[len(prefix) :].lstrip("/")
            local_path = os.path.join(local_dir, relative)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            s3.download_file(Bucket=bucket, Key=key, Filename=local_path)
            downloaded.append(local_path)

    if not downloaded:
        raise ValueError(f"No objects found under {s3_uri}")
    return downloaded


def s3_upload(local_path: str, s3_uri: str) -> None:
    """Upload a single local file to S3.

    Args:
        local_path: Path to the local file.
        s3_uri: Destination S3 URI (must include the object key/filename).

    Example:
        >>> s3_upload("/tmp/data.parquet", "s3://bucket/raw/data.parquet")
    """
    bucket, key = parse_s3_uri(s3_uri)
    boto3.client("s3").upload_file(local_path, bucket, key)


def s3_download(s3_uri: str, local_path: str) -> None:
    """Download a single S3 object to a local file.

    Args:
        s3_uri: Source S3 URI.
        local_path: Local destination path.

    Example:
        >>> s3_download("s3://bucket/raw/data.parquet", "/tmp/data.parquet")
    """
    bucket, key = parse_s3_uri(s3_uri)
    boto3.client("s3").download_file(bucket, key, local_path)


# ---------------------------------------------------------------------------
# Listing / sizing
# ---------------------------------------------------------------------------

def s3_list_size(s3_uri: str, verbose: bool = False) -> dict:
    """Calculate total size and object count under an S3 prefix.

    Args:
        s3_uri: S3 URI to scan.
        verbose: If ``True``, print each object's key and size.

    Returns:
        Dict with keys ``total_bytes``, ``total_objects``, and ``total_human``.

    Example:
        >>> info = s3_list_size("s3://my-bucket/warehouse/")
        >>> print(info["total_human"])
        '2.34 GB'
    """
    bucket, prefix = parse_s3_uri(s3_uri)
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    total_size = 0
    total_objects = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            total_size += obj["Size"]
            total_objects += 1
            if verbose:
                print(f"  {obj['Key']}: {convert_bytes(obj['Size'])}")

    return {
        "total_bytes": total_size,
        "total_objects": total_objects,
        "total_human": convert_bytes(total_size),
    }


def s3_object_details(bucket_names: list[str]) -> pd.DataFrame:
    """Return a DataFrame with detailed metadata for objects in given buckets.

    Columns include: ``bucket_name``, ``object_key``, ``file_extension``,
    ``size_bytes``, ``size_human``, ``last_modified``, ``age_days``,
    ``encryption``, ``tags``.

    Args:
        bucket_names: List of S3 bucket names (without ``s3://``).

    Returns:
        A ``pandas.DataFrame`` with one row per object.

    Example:
        >>> df = s3_object_details(["my-analytics-bucket"])
    """
    s3 = boto3.client("s3")
    now = datetime.now(pytz.UTC)
    rows: list[dict] = []

    for bucket_name in bucket_names:
        paginator = s3.get_paginator("list_objects_v2")
        try:
            for page in paginator.paginate(Bucket=bucket_name):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    modified = obj["LastModified"].astimezone(pytz.UTC)
                    size = obj["Size"]
                    ext = key.rsplit(".", 1)[-1] if "." in key else "unknown"
                    age = (now - modified).days

                    # Object tags
                    try:
                        tag_resp = s3.get_object_tagging(Bucket=bucket_name, Key=key)
                        tags = {t["Key"]: t["Value"] for t in tag_resp.get("TagSet", [])}
                    except Exception:
                        tags = {}

                    rows.append({
                        "bucket_name": bucket_name,
                        "object_key": key,
                        "file_extension": ext,
                        "size_bytes": size,
                        "size_human": convert_bytes(size),
                        "last_modified": modified.strftime("%Y-%m-%d %H:%M:%S"),
                        "age_days": age,
                        "encryption": obj.get("ServerSideEncryption", "None"),
                        "tags": tags,
                    })
        except Exception as e:
            print(f"Error listing bucket {bucket_name}: {e}")

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

def s3_clear_prefix(s3_uri: str) -> int:
    """Delete all objects under an S3 prefix.

    Args:
        s3_uri: S3 URI whose objects will be deleted.

    Returns:
        Number of objects deleted.

    Example:
        >>> s3_clear_prefix("s3://bucket/tmp/athena-results/")
        42
    """
    bucket, prefix = parse_s3_uri(s3_uri)
    s3 = boto3.resource("s3")
    objects = list(s3.Bucket(bucket).objects.filter(Prefix=prefix))
    for obj in objects:
        obj.delete()
    return len(objects)


def s3_restore_deleted(
    bucket_name: str,
    prefix: str = "",
    hours: Optional[int] = None,
) -> int:
    """Restore delete-marked objects in a *versioned* S3 bucket.

    Copies the most recent non-deleted version back over each delete marker.

    Args:
        bucket_name: S3 bucket name (without ``s3://``).
        prefix: Only process objects matching this prefix.
        hours: If set, only restore objects deleted within the last *hours* hours.

    Returns:
        Number of objects restored.

    Example:
        >>> s3_restore_deleted("my-bucket", prefix="data/", hours=24)
        5
    """
    s3 = boto3.client("s3")
    now = datetime.now(timezone.utc)
    restored = 0

    paginator = s3.get_paginator("list_object_versions")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        markers = page.get("DeleteMarkers", [])
        versions = page.get("Versions", [])

        for marker in markers:
            if not marker.get("IsLatest", False):
                continue
            key = marker["Key"]
            if hours is not None:
                delta = now - marker["LastModified"]
                if delta > timedelta(hours=hours):
                    continue

            # Find most recent non-deleted version
            for ver in versions:
                if ver["Key"] == key:
                    s3.copy_object(
                        Bucket=bucket_name,
                        CopySource={"Bucket": bucket_name, "Key": key, "VersionId": ver["VersionId"]},
                        Key=key,
                    )
                    restored += 1
                    break

    return restored


# ---------------------------------------------------------------------------
# Batch upload local files
# ---------------------------------------------------------------------------

def _convert_df_to_format(df: pd.DataFrame, fmt: str) -> bytes:
    """Serialize a pandas DataFrame to bytes in the requested format."""
    if fmt == "parquet":
        return df.to_parquet(engine="pyarrow", index=False)
    elif fmt == "csv":
        return df.to_csv(index=False).encode("utf-8")
    else:
        raise ValueError(f"Unsupported output format: {fmt}")


def s3_upload_local_files(
    local_dir: str,
    s3_bucket: str,
    s3_prefix: str = "",
    file_extension: Optional[str] = None,
    file_names: Optional[list[str]] = None,
    output_format: str = "parquet",
    max_files: int = 100,
) -> list[str]:
    """Scan a local directory for files, convert them, and upload to S3.

    Supports CSV, Excel (``.xlsx``), and Parquet input files.  Files are
    converted to *output_format* before uploading.

    Args:
        local_dir: Local directory to scan.
        s3_bucket: Destination S3 bucket name.
        s3_prefix: Key prefix inside the bucket.
        file_extension: Only include files with this extension.
        file_names: Only include files whose name contains one of these strings.
        output_format: Convert to ``"parquet"`` or ``"csv"`` before upload.
        max_files: Maximum number of files to process.

    Returns:
        List of S3 keys that were uploaded.

    Example:
        >>> keys = s3_upload_local_files(
        ...     local_dir="/data/exports",
        ...     s3_bucket="analytics-bucket",
        ...     s3_prefix="raw/",
        ...     file_extension="csv",
        ...     output_format="parquet",
        ... )
    """
    s3 = boto3.client("s3")
    uploaded_keys: list[str] = []
    count = 0

    for root, _, filenames in os.walk(local_dir):
        for fname in filenames:
            if count >= max_files:
                break
            if file_names and not any(fn in fname for fn in file_names):
                continue
            if file_extension and not fname.endswith(f".{file_extension}"):
                continue

            full_path = os.path.join(root, fname)
            base_key = os.path.join(s3_prefix, fname).replace("\\", "/")

            try:
                if fname.endswith(".csv"):
                    df = pd.read_csv(full_path)
                    body = _convert_df_to_format(df, output_format)
                    key = base_key.rsplit(".", 1)[0] + f".{output_format}"

                elif fname.endswith(".xlsx"):
                    sheets = pd.read_excel(full_path, sheet_name=None)
                    for sheet_name, df in sheets.items():
                        body = _convert_df_to_format(df, output_format)
                        key = f"{base_key.rsplit('.', 1)[0]}_{sheet_name}.{output_format}"
                        s3.put_object(Bucket=s3_bucket, Key=key, Body=body)
                        uploaded_keys.append(key)
                    count += 1
                    continue

                elif fname.endswith(".parquet"):
                    df = pq.read_table(full_path).to_pandas()
                    body = _convert_df_to_format(df, output_format)
                    key = base_key.rsplit(".", 1)[0] + f".{output_format}"

                else:
                    # Upload binary as-is
                    with open(full_path, "rb") as fh:
                        s3.upload_fileobj(fh, s3_bucket, base_key)
                    uploaded_keys.append(base_key)
                    count += 1
                    continue

                s3.put_object(Bucket=s3_bucket, Key=key, Body=body)
                uploaded_keys.append(key)
                count += 1

            except Exception as e:
                print(f"Error uploading {full_path}: {e}")

    return uploaded_keys
