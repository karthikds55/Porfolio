"""
Transform pipeline: applies business logic to staged data and writes mart tables.
Run: python -m pipelines.transform

Azure Monitor telemetry is emitted when the APPLICATIONINSIGHTS_CONNECTION_STRING
and AZURE_LOG_ANALYTICS_* environment variables are set (see monitoring/README.md).
Runs normally without them – telemetry is silently skipped.
"""

import pandas as pd
from pathlib import Path

try:
    from monitoring.pipeline_telemetry import PipelineRun, stage_telemetry
    _TELEMETRY_AVAILABLE = True
except ImportError:
    _TELEMETRY_AVAILABLE = False


STAGING_DIR = Path(__file__).parent.parent / "data" / "staging"
MARTS_DIR = Path(__file__).parent.parent / "data" / "marts"


def load_staging(name: str = "orders_staging") -> pd.DataFrame:
    path = STAGING_DIR / f"{name}.csv"
    df = pd.read_csv(path, parse_dates=["order_date"])
    return df


def build_daily_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate orders by date: total revenue, order count, avg rating."""
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


if __name__ == "__main__":
    if _TELEMETRY_AVAILABLE:
        with PipelineRun() as run:
            with stage_telemetry(run, "transform") as ctx:
                df = load_staging()
                daily = build_daily_summary(df)
                category = build_category_summary(df)
                save_mart(daily, "daily_summary")
                save_mart(category, "category_summary")
                ctx.set_row_count(len(df))
    else:
        df = load_staging()
        daily = build_daily_summary(df)
        category = build_category_summary(df)
        save_mart(daily, "daily_summary")
        save_mart(category, "category_summary")
