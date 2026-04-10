"""
Transform pipeline: applies business logic to staged data and writes mart tables.
Run: python -m pipelines.transform
"""

import pandas as pd
from pathlib import Path

from monitoring.azure_monitor import get_logger, pipeline_span

STAGING_DIR = Path(__file__).parent.parent / "data" / "staging"
MARTS_DIR = Path(__file__).parent.parent / "data" / "marts"

logger = get_logger("transform")


def load_staging(name: str = "orders_staging") -> pd.DataFrame:
    path = STAGING_DIR / f"{name}.csv"
    logger.info("Loading staging file: %s", path)
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
    logger.info("[transform] Saved %d rows → %s", len(df), out)
    print(f"[transform] Saved {len(df):,} rows → {out}")
    return out


if __name__ == "__main__":
    with pipeline_span("transform") as ctx:
        df = load_staging()
        daily = build_daily_summary(df)
        category = build_category_summary(df)
        save_mart(daily, "daily_summary")
        save_mart(category, "category_summary")
        ctx["record_count"] = len(daily) + len(category)
