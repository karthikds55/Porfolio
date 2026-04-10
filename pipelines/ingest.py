"""
Ingestion pipeline: loads raw CSV data into a staging layer.
Run: python -m pipelines.ingest

Azure Monitor telemetry is emitted when the APPLICATIONINSIGHTS_CONNECTION_STRING
and AZURE_LOG_ANALYTICS_* environment variables are set (see monitoring/README.md).
Runs normally without them – telemetry is silently skipped.
"""

import pandas as pd
from pathlib import Path

# Optional Azure Monitor integration (no-op if env vars / SDK not present)
try:
    from monitoring.pipeline_telemetry import PipelineRun, stage_telemetry
    _TELEMETRY_AVAILABLE = True
except ImportError:
    _TELEMETRY_AVAILABLE = False


RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
STAGING_DIR = Path(__file__).parent.parent / "data" / "staging"


def ingest_orders(raw_path: Path = RAW_DIR / "daily_ecommerce_orders.csv") -> pd.DataFrame:
    """Load raw ecommerce orders CSV and apply basic dtype coercions."""
    df = pd.read_csv(raw_path)
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["order_value"] = pd.to_numeric(df["order_value"], errors="coerce")
    df["delivery_time_days"] = pd.to_numeric(df["delivery_time_days"], errors="coerce")
    df["customer_rating"] = pd.to_numeric(df["customer_rating"], errors="coerce")
    df["discount_applied"] = df["discount_applied"].map({"Yes": True, "No": False})
    return df


def save_staging(df: pd.DataFrame, name: str = "orders_staging") -> Path:
    """Persist staged DataFrame as a CSV for downstream transforms."""
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    out = STAGING_DIR / f"{name}.csv"
    df.to_csv(out, index=False)
    print(f"[ingest] Saved {len(df):,} rows → {out}")
    return out


if __name__ == "__main__":
    if _TELEMETRY_AVAILABLE:
        with PipelineRun() as run:
            with stage_telemetry(run, "ingest") as ctx:
                orders = ingest_orders()
                save_staging(orders)
                ctx.set_row_count(len(orders))
    else:
        orders = ingest_orders()
        save_staging(orders)
