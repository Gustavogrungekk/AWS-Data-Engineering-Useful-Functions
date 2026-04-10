"""
General-purpose utility functions.

Functions:
    convert_bytes   - Convert byte count to human-readable string (KB, MB, GB, …)
    parse_s3_uri    - Split an S3 URI into (bucket, key) tuple
    parse_date      - Parse a date string in many common formats
    clean_string    - Remove accents, special characters, and emojis from text
    business_days   - List business days between two dates for a given country
"""

from __future__ import annotations

import math
import re
import unicodedata
from datetime import datetime
from typing import Optional

import holidays
import pandas as pd


# ---------------------------------------------------------------------------
# Byte conversions
# ---------------------------------------------------------------------------

def convert_bytes(num_bytes: int) -> str:
    """Convert a byte count to a human-readable string using binary prefixes.

    Args:
        num_bytes: Number of bytes (must be >= 0).

    Returns:
        A string like ``"1.5 GB"`` or ``"0 B"``.

    Example:
        >>> convert_bytes(1_610_612_736)
        '1.5 GB'
    """
    if num_bytes == 0:
        return "0 B"
    units = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(num_bytes, 1024)))
    p = math.pow(1024, i)
    value = round(num_bytes / p, 2)
    return f"{value} {units[i]}"


# ---------------------------------------------------------------------------
# S3 URI parsing
# ---------------------------------------------------------------------------

def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    """Parse an S3 URI into its bucket name and key (prefix).

    Args:
        s3_uri: Full S3 URI, e.g. ``"s3://my-bucket/path/to/data/"``.

    Returns:
        A ``(bucket, key)`` tuple.

    Raises:
        ValueError: If *s3_uri* does not start with ``s3://``.

    Example:
        >>> parse_s3_uri("s3://my-bucket/warehouse/table/")
        ('my-bucket', 'warehouse/table/')
    """
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI (must start with 's3://'): {s3_uri}")
    without_scheme = s3_uri[5:]
    bucket = without_scheme.split("/", 1)[0]
    key = without_scheme[len(bucket) :].lstrip("/")
    return bucket, key


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------

_DATE_FORMATS = [
    "%Y-%m-%d",
    "%Y%m%d",
    "%Y/%m/%d",
    "%d-%m-%Y",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%m-%d-%Y",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S.%f%z",
    "%Y-%m-%d %H:%M:%S.%f %Z",
    "%Y-%m-%d %H:%M:%SZ",
    "%d-%b-%Y",       # 01-Jan-2024
    "%d-%B-%Y",       # 01-January-2024
    "%b %d, %Y",      # Jan 01, 2024
    "%B %d, %Y",      # January 01, 2024
    "%d %B, %Y",      # 1 January, 2024
    "%d-%m-%y",
    "%m-%d-%y",
    "%d/%m/%y",
    "%y-%m-%d",
    "%y/%m/%d",
]


def parse_date(date_str: str, output_format: str = "%Y-%m-%d") -> str:
    """Try to parse a date string using many common formats.

    The function attempts every known format in order and returns the first
    successful parse formatted with *output_format*.  It also handles
    Unix-epoch timestamps (10-digit seconds, 13-digit milliseconds).

    Args:
        date_str: Raw date string.
        output_format: ``strftime`` format for the returned string.

    Returns:
        Formatted date string.

    Raises:
        ValueError: If no format matched.

    Examples:
        >>> parse_date("2024-06-15")
        '2024-06-15'
        >>> parse_date("15/06/2024", "%d/%m/%Y")
        '15/06/2024'
        >>> parse_date("1718409600")   # Unix timestamp
        '2024-06-15'
    """
    cleaned = date_str.strip()

    # Handle numeric timestamps (epoch seconds / milliseconds)
    if re.fullmatch(r"\d{10}", cleaned):
        return datetime.utcfromtimestamp(int(cleaned)).strftime(output_format)
    if re.fullmatch(r"\d{13}", cleaned):
        return datetime.utcfromtimestamp(int(cleaned) / 1000).strftime(output_format)

    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt).strftime(output_format)
        except ValueError:
            continue

    raise ValueError(f"Unable to parse date string: {date_str!r}")


# ---------------------------------------------------------------------------
# String cleaning
# ---------------------------------------------------------------------------

def clean_string(text: str, remove: Optional[set[str]] = None) -> str:
    """Remove accents, non-printable characters, and optionally specified chars.

    Args:
        text: Input text.
        remove: Optional set of individual characters to strip.

    Returns:
        Cleaned text (ASCII-safe).

    Example:
        >>> clean_string("Açúcar Canção", remove={"#", "@"})
        'Acucar Cancao'
    """
    if not text:
        return text
    # Decompose Unicode, strip combining marks (accents), strip non-printable
    nfd = unicodedata.normalize("NFD", text)
    ascii_text = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    clean = "".join(c for c in ascii_text if unicodedata.category(c)[0] != "C")
    if remove:
        clean = "".join(c for c in clean if c not in remove)
    return clean


# ---------------------------------------------------------------------------
# Business days
# ---------------------------------------------------------------------------

def business_days(country: str, start_date: str, end_date: str) -> list[str]:
    """Return business days between two dates, excluding country holidays.

    Uses the ``holidays`` library for country-specific public holidays.

    Args:
        country: ISO country code, e.g. ``"BR"``, ``"US"``, ``"GB"``.
        start_date: Start date in ``YYYY-MM-DD`` format (inclusive).
        end_date: End date in ``YYYY-MM-DD`` format (inclusive).

    Returns:
        List of date strings in ``YYYY-MM-DD`` format.

    Example:
        >>> days = business_days("US", "2024-12-23", "2024-12-27")
        >>> # Returns weekdays that are not US holidays
    """
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    all_days = pd.date_range(start=start, end=end, freq="D")
    country_holidays = holidays.country_holidays(country)
    return [
        day.strftime("%Y-%m-%d")
        for day in all_days
        if day.weekday() < 5 and day not in country_holidays
    ]
