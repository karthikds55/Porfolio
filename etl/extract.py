"""Extract step: read the raw CSV and perform schema/structural validation."""
from pathlib import Path
from typing import Dict, List

import pandas as pd
from loguru import logger

from etl import config


# ── Public API ─────────────────────────────────────────────────────────────────

def extract(file_path=None) -> pd.DataFrame:
    """Read the raw CSV file and run structural validation.

    Args:
        file_path: Path to the raw CSV.  Defaults to config.RAW_FILE.

    Returns:
        Raw DataFrame (no transforms applied yet).

    Raises:
        FileNotFoundError: If the source file does not exist.
        ValueError: If the file is empty or critical columns are missing.
    """
    file_path = Path(file_path) if file_path is not None else config.RAW_FILE
    logger.info(f"[EXTRACT] Starting extraction from: {file_path}")

    _assert_file_exists(file_path)

    df = _read_csv(file_path)
    _validate_schema(df)
    _log_extraction_summary(df, file_path)

    return df


# ── Private helpers ────────────────────────────────────────────────────────────

def _assert_file_exists(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Raw data file not found: {path}")
    logger.debug(f"File found: {path} ({path.stat().st_size / 1024:.1f} KB)")


def _read_csv(path: Path) -> pd.DataFrame:
    """Read CSV with basic type hints; keep raw strings for transform step."""
    try:
        df = pd.read_csv(
            path,
            dtype=str,
            na_values=["", "NA", "N/A", "null", "NULL", "None", "none", "NaN"],
            keep_default_na=True,
        )
        logger.info(f"[EXTRACT] Loaded {len(df):,} rows × {len(df.columns)} columns")
        if df.empty:
            raise ValueError("The source CSV is empty.")
        return df
    except pd.errors.ParserError as exc:
        raise ValueError(f"Could not parse CSV: {exc}") from exc


def _validate_schema(df: pd.DataFrame) -> None:
    """Check that all expected columns are present."""
    missing = [c for c in config.EXPECTED_COLUMNS if c not in df.columns]
    extra = [c for c in df.columns if c not in config.EXPECTED_COLUMNS]

    if missing:
        raise ValueError(
            f"[EXTRACT] Schema validation failed – missing columns: {missing}"
        )
    if extra:
        logger.warning(f"[EXTRACT] Unexpected extra columns (will be ignored): {extra}")

    logger.info("[EXTRACT] Schema validation passed.")


def _log_extraction_summary(df: pd.DataFrame, path: Path) -> Dict:
    """Log key stats about the raw extract."""
    null_counts = df.isnull().sum()
    null_pct = (null_counts / len(df) * 100).round(2)

    logger.info("[EXTRACT] ── Null counts per column ──────────────────────")
    for col in config.EXPECTED_COLUMNS:
        if col in null_counts and null_counts[col] > 0:
            logger.warning(f"  {col}: {null_counts[col]} nulls ({null_pct[col]}%)")
        else:
            logger.debug(f"  {col}: 0 nulls")

    duplicate_ids = df.duplicated(subset=["order_id"]).sum()
    if duplicate_ids:
        logger.warning(f"[EXTRACT] Duplicate order_ids detected: {duplicate_ids}")
    else:
        logger.info("[EXTRACT] No duplicate order_ids found.")

    return {
        "rows": len(df),
        "columns": len(df.columns),
        "nulls_total": int(null_counts.sum()),
        "duplicate_ids": int(duplicate_ids),
    }
