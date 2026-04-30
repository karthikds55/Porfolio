"""
Transform pipeline: applies business logic to staged data and writes mart tables.

Local mode (default):
    python -m pipelines.transform

Cloud mode (reads staged Parquet from S3):
    S3_STAGING_BUCKET=my-parquet-bucket S3_STAGING_KEY=orders_staging.parquet \
        python -m pipelines.transform

Azure Monitor telemetry is emitted when the APPLICATIONINSIGHTS_CONNECTION_STRING
and AZURE_LOG_ANALYTICS_* environment variables are set (see monitoring/README.md).
Runs normally without them – telemetry is silently skipped.
"""

import os
import tempfile
from pathlib import Path

import pandas as pd

try:
    from monitoring.pipeline_telemetry import PipelineRun, stage_telemetry
    _TELEMETRY_AVAILABLE = True
except ImportError:
    _TELEMETRY_AVAILABLE = False


STAGING_DIR = Path(__file__).parent.parent / "data" / "staging"
MARTS_DIR = Path(__file__).parent.parent / "data" / "marts"
DAILY_SUMMARY_COLUMNS = {
    "order_id",
    "order_date",
    "order_value",
    "customer_rating",
    "order_status",
}
CATEGORY_SUMMARY_COLUMNS = {
    "order_id",
    "product_category",
    "order_value",
    "customer_rating",
}


def _validate_columns(df: pd.DataFrame, required_columns: set[str], context: str) -> None:
    """Raise a readable error instead of letting pandas fail deep in a groupby."""

    missing_columns = sorted(required_columns.difference(df.columns))
    if missing_columns:
        raise ValueError(
            f"{context} is missing required columns: {', '.join(missing_columns)}"
        )


def load_staging(name: str = "orders_staging") -> pd.DataFrame:
    path = STAGING_DIR / f"{name}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Staging file not found: {path}")
    df = pd.read_csv(path, parse_dates=["order_date"])
    return df


def build_daily_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate orders by date: total revenue, order count, avg rating."""
    _validate_columns(df, DAILY_SUMMARY_COLUMNS, "Daily summary input")
    summary = (
        df.groupby("order_date")
        .agg(
            total_orders=("order_id", "count"),
            total_revenue=("order_value", "sum"),
            avg_rating=("customer_rating", "mean"),
            delivered=("order_status", lambda s: (s == "Delivered").sum()),
        )
        .reset_index()
        .sort_values("order_date")
    )
    summary["delivery_rate"] = summary["delivered"] / summary["total_orders"]
    return summary


def build_category_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate orders by product category."""
    _validate_columns(df, CATEGORY_SUMMARY_COLUMNS, "Category summary input")
    summary = (
        df.groupby("product_category")
        .agg(
            total_orders=("order_id", "count"),
            total_revenue=("order_value", "sum"),
            avg_order_value=("order_value", "mean"),
            avg_rating=("customer_rating", "mean"),
        )
        .reset_index()
        .sort_values("total_revenue", ascending=False)
    )
    return summary


def save_mart(df: pd.DataFrame, name: str) -> Path:
    MARTS_DIR.mkdir(parents=True, exist_ok=True)
    out = MARTS_DIR / f"{name}.csv"
    df.to_csv(out, index=False)
    print(f"[transform] Saved {len(df):,} rows → {out}")
    return out


def load_staging_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """Download a Parquet staging file from S3 and return as a DataFrame."""
    from pipelines.s3_utils import download_from_s3

    with tempfile.TemporaryDirectory() as tmp:
        local = Path(tmp) / Path(key).name
        download_from_s3(bucket, key, local)
        df = pd.read_parquet(local)
    print(f"[transform] Loaded {len(df):,} rows from s3://{bucket}/{key}")
    return df


if __name__ == "__main__":
    s3_bucket = os.environ.get("S3_STAGING_BUCKET")
    s3_key = os.environ.get("S3_STAGING_KEY")

    if s3_bucket and s3_key:
        df = load_staging_from_s3(s3_bucket, s3_key)
    else:
        df = load_staging()

    daily = build_daily_summary(df)
    category = build_category_summary(df)

    if _TELEMETRY_AVAILABLE:
        with PipelineRun() as run:
            with stage_telemetry(run, "transform") as ctx:
                save_mart(daily, "daily_summary")
                save_mart(category, "category_summary")
                ctx.set_row_count(len(df))
    else:
        save_mart(daily, "daily_summary")
        save_mart(category, "category_summary")
