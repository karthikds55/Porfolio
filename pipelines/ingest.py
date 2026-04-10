"""
Ingestion pipeline: loads raw CSV data into a staging layer.
Run: python -m pipelines.ingest
"""

import pandas as pd
from pathlib import Path

from monitoring.azure_monitor import get_logger, pipeline_span, track_metric

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
STAGING_DIR = Path(__file__).parent.parent / "data" / "staging"

logger = get_logger("ingest")


def ingest_orders(raw_path: Path = RAW_DIR / "daily_ecommerce_orders.csv") -> pd.DataFrame:
    """Load raw ecommerce orders CSV and apply basic dtype coercions."""
    logger.info("Reading raw CSV: %s", raw_path)
    df = pd.read_csv(raw_path)
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["order_value"] = pd.to_numeric(df["order_value"], errors="coerce")
    df["delivery_time_days"] = pd.to_numeric(df["delivery_time_days"], errors="coerce")
    df["customer_rating"] = pd.to_numeric(df["customer_rating"], errors="coerce")
    df["discount_applied"] = df["discount_applied"].map({"Yes": True, "No": False})

    null_rate = df["order_value"].isnull().mean() * 100
    track_metric("null_rate_percent", value=round(null_rate, 4), properties={"column": "order_value"})
    logger.debug("null_rate_percent[order_value] = %.4f%%", null_rate)

    return df


def save_staging(df: pd.DataFrame, name: str = "orders_staging") -> Path:
    """Persist staged DataFrame as a CSV for downstream transforms."""
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    out = STAGING_DIR / f"{name}.csv"
    df.to_csv(out, index=False)
    logger.info("[ingest] Saved %d rows → %s", len(df), out)
    print(f"[ingest] Saved {len(df):,} rows → {out}")
    return out


if __name__ == "__main__":
    with pipeline_span("ingest") as ctx:
        orders = ingest_orders()
        ctx["record_count"] = len(orders)
        save_staging(orders)
