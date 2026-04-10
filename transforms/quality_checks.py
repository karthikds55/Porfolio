"""
Data quality checks for staged/mart tables.
Run: python -m transforms.quality_checks

Azure Monitor telemetry is emitted when the APPLICATIONINSIGHTS_CONNECTION_STRING
and AZURE_LOG_ANALYTICS_* environment variables are set (see monitoring/README.md).
Each [PASS] / [FAIL] result is written to QualityCheckResults_CL in Log Analytics,
replacing the CloudWatch metric filter on /ecommerce/pipeline/quality.
"""

import pandas as pd
from pathlib import Path

try:
    from monitoring.pipeline_telemetry import PipelineRun, quality_check_telemetry
    _TELEMETRY_AVAILABLE = True
except ImportError:
    _TELEMETRY_AVAILABLE = False


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


def run_checks(df: pd.DataFrame, run: "PipelineRun | None" = None) -> None:
    """
    Run all data quality checks and print results.

    When a PipelineRun is provided, each result is also sent to Azure Monitor
    (QualityCheckResults_CL), replacing the CloudWatch metric filter on
    /ecommerce/pipeline/quality.
    """
    print("[quality] Running data quality checks...")

    def _report(check_name: str, passed: bool, pass_msg: str, fail_msg: str, details: str = "") -> None:
        if passed:
            print(f"  [PASS] {pass_msg}")
        else:
            print(f"  [FAIL] {fail_msg}")
        if run is not None and _TELEMETRY_AVAILABLE:
            quality_check_telemetry(run, check_name, passed, details)

    required_cols = ["order_id", "order_date", "order_value", "order_status"]
    nulls = check_no_nulls(df, required_cols)
    _report(
        "no_nulls_in_required_columns",
        not nulls,
        "No nulls in required columns",
        f"Null values found in: {nulls}",
        details=str(nulls),
    )

    _report(
        "no_duplicate_order_ids",
        check_no_duplicates(df, "order_id"),
        "No duplicate order_ids",
        "Duplicate order_ids detected",
    )

    _report(
        "customer_rating_in_range",
        check_value_range(df, "customer_rating", 0, 5),
        "customer_rating in [0, 5]",
        "customer_rating outside [0, 5]",
    )

    _report(
        "order_value_non_negative",
        check_value_range(df, "order_value", 0, float("inf")),
        "order_value non-negative",
        "Negative order_value detected",
    )

    print("[quality] Done.")


if __name__ == "__main__":
    path = STAGING_DIR / "orders_staging.csv"
    df = pd.read_csv(path, parse_dates=["order_date"])

    if _TELEMETRY_AVAILABLE:
        with PipelineRun() as run:
            run_checks(df, run=run)
    else:
        run_checks(df)
