"""
Data quality checks for staged/mart tables.
Run: python -m transforms.quality_checks
"""

import pandas as pd
from pathlib import Path


STAGING_DIR = Path(__file__).parent.parent / "data" / "staging"


def check_no_nulls(df: pd.DataFrame, columns: list[str]) -> list[str]:
    """Return list of columns that have null values."""
    return [col for col in columns if df[col].isnull().any()]


def check_no_duplicates(df: pd.DataFrame, key: str) -> bool:
    """Return True if no duplicate key values exist."""
    return not df[key].duplicated().any()


def check_value_range(df: pd.DataFrame, column: str, min_val: float, max_val: float) -> bool:
    """Return True if all values in column fall within [min_val, max_val]."""
    return df[column].between(min_val, max_val).all()


def run_checks(df: pd.DataFrame) -> None:
    print("[quality] Running data quality checks...")

    required_cols = ["order_id", "order_date", "order_value", "order_status"]
    nulls = check_no_nulls(df, required_cols)
    if nulls:
        print(f"  [FAIL] Null values found in: {nulls}")
    else:
        print("  [PASS] No nulls in required columns")

    if check_no_duplicates(df, "order_id"):
        print("  [PASS] No duplicate order_ids")
    else:
        print("  [FAIL] Duplicate order_ids detected")

    if check_value_range(df, "customer_rating", 0, 5):
        print("  [PASS] customer_rating in [0, 5]")
    else:
        print("  [FAIL] customer_rating outside [0, 5]")

    if check_value_range(df, "order_value", 0, float("inf")):
        print("  [PASS] order_value non-negative")
    else:
        print("  [FAIL] Negative order_value detected")

    print("[quality] Done.")


if __name__ == "__main__":
    path = STAGING_DIR / "orders_staging.csv"
    df = pd.read_csv(path, parse_dates=["order_date"])
    run_checks(df)
