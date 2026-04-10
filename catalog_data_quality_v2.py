"""
Catalog Data Quality — Backward-compatibility shim.

New code should use: ``from awsome.quality import run_quality_check``
"""

from awsome.quality import run_quality_check  # noqa: F401
