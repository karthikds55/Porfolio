"""Pipeline package for raw ingestion and downstream transforms.

The package uses lazy imports so ``import pipelines`` stays lightweight while
``pipelines.ingest_orders`` and other public helpers remain available.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "RAW_DIR",
    "STAGING_DIR",
    "MARTS_DIR",
    "ingest_orders",
    "save_staging",
    "load_staging",
    "build_daily_summary",
    "build_category_summary",
    "save_mart",
]

_EXPORTS = {
    "RAW_DIR": ("pipelines.ingest", "RAW_DIR"),
    "STAGING_DIR": ("pipelines.ingest", "STAGING_DIR"),
    "MARTS_DIR": ("pipelines.transform", "MARTS_DIR"),
    "ingest_orders": ("pipelines.ingest", "ingest_orders"),
    "save_staging": ("pipelines.ingest", "save_staging"),
    "load_staging": ("pipelines.transform", "load_staging"),
    "build_daily_summary": ("pipelines.transform", "build_daily_summary"),
    "build_category_summary": ("pipelines.transform", "build_category_summary"),
    "save_mart": ("pipelines.transform", "save_mart"),
}


def __getattr__(name: str) -> Any:
    """Resolve public pipeline helpers only when they are first requested."""

    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module_name, attr_name = _EXPORTS[name]
    module = import_module(module_name)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value
