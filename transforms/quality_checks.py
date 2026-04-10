"""
Data quality checks for staged/mart tables.
Run: python -m transforms.quality_checks
"""

import sys
import pandas as pd
from pathlib import Path

from monitoring.azure_monitor import get_logger, track_quality_check, track_metric

STAGING_DIR = Path(__file__).parent.parent / "data" / "staging"

logger = get_logger("quality_checks")


def check_no_nulls(df: pd.DataFrame, columns: list[str]) -> list[str]:
    """Return list of columns that have null values."""
    return [col for col in columns if df[col].isnull().any()]


def check_no_duplicates(df: pd.DataFrame, key: str) -> bool:
    """Return True if no duplicate key values exist."""
    return not df[key].duplicated().any()


def check_value_range(df: pd.DataFrame, column: str, min_val: float, max_val: float) -> bool:
    """Return True if all values in column fall within [min_val, max_val]."""
    return df[column].between(min_val, max_val).all()


def run_checks(df: pd.DataFrame) -> bool:
    """Run all quality checks, emit Azure Monitor events, and return True if all pass."""
    logger.info("[quality] Running data quality checks on %d rows", len(df))
    print("[quality] Running data quality checks...")

    all_passed = True

    # ── Null checks ──────────────────────────────────────────────────────────
    required_cols = ["order_id", "order_date", "order_value", "order_status"]
    nulls = check_no_nulls(df, required_cols)
    if nulls:
        detail = f"Null values found in: {nulls}"
        print(f"  [FAIL] {detail}")
        track_quality_check("null_check_required_columns", result="FAIL", details=detail)
        all_passed = False
    else:
        detail = "No nulls in required columns"
        print(f"  [PASS] {detail}")
        track_quality_check("null_check_required_columns", result="PASS", details=detail)

    # Emit per-column null-rate metrics for the alert threshold rule
    for col in required_cols:
        if col in df.columns:
            null_rate = df[col].isnull().mean() * 100
            track_metric("null_rate_percent", value=round(null_rate, 4), properties={"column": col})

    # ── Duplicate check ──────────────────────────────────────────────────────
    if check_no_duplicates(df, "order_id"):
        detail = "No duplicate order_ids"
        print(f"  [PASS] {detail}")
        track_quality_check("duplicate_check_order_id", result="PASS", details=detail)
    else:
        dup_count = df["order_id"].duplicated().sum()
        detail = f"{dup_count} duplicate order_ids detected"
        print(f"  [FAIL] {detail}")
        track_quality_check("duplicate_check_order_id", result="FAIL", details=detail)
        all_passed = False

    # ── Range checks ─────────────────────────────────────────────────────────
    if check_value_range(df, "customer_rating", 0, 5):
        detail = "customer_rating in [0, 5]"
        print(f"  [PASS] {detail}")
        track_quality_check("range_check_customer_rating", result="PASS", details=detail)
    else:
        oob = (~df["customer_rating"].between(0, 5)).sum()
        detail = f"{oob} rows with customer_rating outside [0, 5]"
        print(f"  [FAIL] {detail}")
        track_quality_check("range_check_customer_rating", result="FAIL", details=detail)
        all_passed = False

    if check_value_range(df, "order_value", 0, float("inf")):
        detail = "order_value non-negative"
        print(f"  [PASS] {detail}")
        track_quality_check("range_check_order_value", result="PASS", details=detail)
    else:
        neg = (df["order_value"] < 0).sum()
        detail = f"{neg} rows with negative order_value"
        print(f"  [FAIL] {detail}")
        track_quality_check("range_check_order_value", result="FAIL", details=detail)
        all_passed = False

    overall = "PASS" if all_passed else "FAIL"
    logger.info("[quality] Overall result: %s", overall)
    print(f"[quality] Done. Overall: {overall}")
    return all_passed


if __name__ == "__main__":
    path = STAGING_DIR / "orders_staging.csv"
    df = pd.read_csv(path, parse_dates=["order_date"])
    passed = run_checks(df)
    sys.exit(0 if passed else 1)
