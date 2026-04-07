"""
Ingestion pipeline: loads raw CSV data into a staging layer.
Run: python -m pipelines.ingest

Azure Monitor telemetry is emitted when the APPLICATIONINSIGHTS_CONNECTION_STRING
and AZURE_LOG_ANALYTICS_* environment variables are set (see monitoring/README.md).
Runs normally without them – telemetry is silently skipped.
"""

from pathlib import Path

import pandas as pd

# Optional Azure Monitor integration (no-op if env vars / SDK not present)
try:
    from monitoring.pipeline_telemetry import PipelineRun, stage_telemetry
    _TELEMETRY_AVAILABLE = True
except ImportError:
    _TELEMETRY_AVAILABLE = False


RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
STAGING_DIR = Path(__file__).parent.parent / "data" / "staging"
REQUIRED_COLUMNS = {
    "order_id",
    "order_date",
    "order_value",
    "discount_applied",
    "delivery_time_days",
    "customer_rating",
}


def _validate_input_schema(df: pd.DataFrame) -> None:
    """Fail fast with a clear error when the source CSV shape changes."""

    missing_columns = sorted(REQUIRED_COLUMNS.difference(df.columns))
    if missing_columns:
        raise ValueError(
            "Raw orders file is missing required columns: "
            + ", ".join(missing_columns)
        )


def _coerce_discount_flag(value: object) -> object:
    """Normalize Yes/No style values into booleans without hiding bad data."""

    if pd.isna(value):
        return pd.NA

    normalized = str(value).strip().lower()
    if normalized in {"yes", "true", "1"}:
        return True
    if normalized in {"no", "false", "0"}:
        return False
    return pd.NA


def ingest_orders(raw_path: Path = RAW_DIR / "daily_ecommerce_orders.csv") -> pd.DataFrame:
    """Load raw ecommerce orders CSV and apply basic dtype coercions."""
    if not raw_path.exists():
        raise FileNotFoundError(f"Raw orders file not found: {raw_path}")

    df = pd.read_csv(raw_path)
    _validate_input_schema(df)
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["order_value"] = pd.to_numeric(df["order_value"], errors="coerce")
    df["delivery_time_days"] = pd.to_numeric(df["delivery_time_days"], errors="coerce")
    df["customer_rating"] = pd.to_numeric(df["customer_rating"], errors="coerce")
    df["discount_applied"] = df["discount_applied"].map(_coerce_discount_flag).astype("boolean")
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
