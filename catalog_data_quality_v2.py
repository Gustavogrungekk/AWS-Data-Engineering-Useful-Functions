"""
Catalog Data Quality — Backward-compatibility shim.

New code should use: ``from awsesome.quality import run_quality_check``
"""

from awsesome.quality import run_quality_check  # noqa: F401
