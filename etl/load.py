"""Load step: persist clean data to CSV files and a SQLite database."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

import pandas as pd
from loguru import logger
from sqlalchemy import create_engine

from etl import config


# ── Public API ─────────────────────────────────────────────────────────────────

def load(
    clean_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    run_metadata: Dict | None = None,
) -> Dict[str, str]:
    """Persist transformed data to all configured destinations.

    Destinations:
      - data/processed/orders_cleaned.csv
      - data/processed/orders_rejected.csv
      - data/processed/orders_summary.csv
      - data/processed/orders_by_category.csv
      - data/processed/orders_by_payment.csv
      - data/db/ecommerce.db  (SQLite)
    """
    _ensure_dirs()
    outputs: Dict[str, str] = {}

    outputs["cleaned_csv"] = _write_csv(clean_df, config.PROCESSED_FILE, "cleaned orders")

    if not rejected_df.empty:
        rejected_path = config.PROCESSED_DIR / "orders_rejected.csv"
        outputs["rejected_csv"] = _write_csv(rejected_df, rejected_path, "rejected orders")

    daily_summary = _build_daily_summary(clean_df)
    outputs["summary_csv"] = _write_csv(daily_summary, config.SUMMARY_FILE, "daily summary")

    category_summary = _build_category_summary(clean_df)
    outputs["category_csv"] = _write_csv(
        category_summary, config.CATEGORY_SUMMARY_FILE, "category summary"
    )

    payment_summary = _build_payment_summary(clean_df)
    outputs["payment_csv"] = _write_csv(
        payment_summary, config.PAYMENT_SUMMARY_FILE, "payment summary"
    )

    engine = _get_engine()
    _write_table(engine, clean_df, config.DB_TABLE_ORDERS, "orders")
    _write_table(engine, category_summary, config.DB_TABLE_CATEGORY_SUMMARY, "category_summary")
    _write_table(engine, payment_summary, config.DB_TABLE_PAYMENT_SUMMARY, "payment_summary")
    _log_pipeline_run(engine, clean_df, rejected_df, run_metadata)

    outputs["sqlite_db"] = str(config.DB_FILE)
    logger.info(f"[LOAD] All outputs written. DB: {config.DB_FILE}")
    return outputs


# ── Aggregations ───────────────────────────────────────────────────────────────

def _build_daily_summary(df: pd.DataFrame) -> pd.DataFrame:
    summary = (
        df.groupby("order_date")
        .agg(
            total_orders=("order_id", "count"),
            total_revenue=("order_value", "sum"),
            avg_order_value=("order_value", "mean"),
            avg_rating=("customer_rating", "mean"),
            avg_delivery_days=("delivery_time_days", "mean"),
            delivered_orders=("is_delivered", "sum"),
            discounted_orders=("is_discounted", "sum"),
        )
        .reset_index()
    )
    summary["delivery_rate_pct"] = (
        summary["delivered_orders"] / summary["total_orders"] * 100
    ).round(2)
    summary["discount_rate_pct"] = (
        summary["discounted_orders"] / summary["total_orders"] * 100
    ).round(2)
    summary["avg_order_value"] = summary["avg_order_value"].round(2)
    summary["avg_rating"] = summary["avg_rating"].round(2)
    summary["avg_delivery_days"] = summary["avg_delivery_days"].round(1)
    return summary.sort_values("order_date")


def _build_category_summary(df: pd.DataFrame) -> pd.DataFrame:
    summary = (
        df.groupby("product_category")
        .agg(
            total_orders=("order_id", "count"),
            total_revenue=("order_value", "sum"),
            avg_order_value=("order_value", "mean"),
            avg_rating=("customer_rating", "mean"),
            delivered_orders=("is_delivered", "sum"),
            returned_orders=("order_status", lambda x: (x.str.upper() == "RETURNED").sum()),
            cancelled_orders=("order_status", lambda x: (x.str.upper() == "CANCELLED").sum()),
        )
        .reset_index()
    )
    summary["delivery_rate_pct"] = (
        summary["delivered_orders"] / summary["total_orders"] * 100
    ).round(2)
    summary["return_rate_pct"] = (
        summary["returned_orders"] / summary["total_orders"] * 100
    ).round(2)
    summary["avg_order_value"] = summary["avg_order_value"].round(2)
    summary["avg_rating"] = summary["avg_rating"].round(2)
    return summary.sort_values("total_revenue", ascending=False)


def _build_payment_summary(df: pd.DataFrame) -> pd.DataFrame:
    summary = (
        df.groupby("payment_method")
        .agg(
            total_orders=("order_id", "count"),
            total_revenue=("order_value", "sum"),
            avg_order_value=("order_value", "mean"),
            avg_rating=("customer_rating", "mean"),
        )
        .reset_index()
    )
    summary["pct_of_orders"] = (
        summary["total_orders"] / summary["total_orders"].sum() * 100
    ).round(2)
    summary["avg_order_value"] = summary["avg_order_value"].round(2)
    summary["avg_rating"] = summary["avg_rating"].round(2)
    return summary.sort_values("total_orders", ascending=False)


# ── I/O helpers ────────────────────────────────────────────────────────────────

def _ensure_dirs() -> None:
    config.PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    config.DB_DIR.mkdir(parents=True, exist_ok=True)


def _write_csv(df: pd.DataFrame, path: Path, label: str) -> str:
    df.to_csv(path, index=False)
    logger.info(f"[LOAD] Written {label}: {path} ({len(df):,} rows)")
    return str(path)


def _get_engine():
    config.DB_DIR.mkdir(parents=True, exist_ok=True)
    return create_engine(f"sqlite:///{config.DB_FILE}")


def _write_table(engine, df: pd.DataFrame, table_name: str, label: str) -> None:
    df_copy = df.copy()
    for col in df_copy.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
        df_copy[col] = df_copy[col].astype(str)
    for col in df_copy.select_dtypes(include=["Int64", "boolean"]).columns:
        df_copy[col] = df_copy[col].astype(object)
    df_copy.to_sql(table_name, engine, if_exists="replace", index=False)
    logger.info(f"[LOAD] SQLite table '{table_name}' written ({len(df_copy):,} rows)")


def _log_pipeline_run(
    engine,
    clean_df: pd.DataFrame,
    rejected_df: pd.DataFrame,
    metadata: Dict | None,
) -> None:
    run_record = {
        "run_ts": datetime.now(tz=timezone.utc).isoformat(),
        "clean_rows": len(clean_df),
        "rejected_rows": len(rejected_df),
        "metadata": json.dumps(metadata or {}),
    }
    pd.DataFrame([run_record]).to_sql(
        config.DB_TABLE_RUN_LOG, engine, if_exists="append", index=False
    )
    logger.info(f"[LOAD] Pipeline run logged to '{config.DB_TABLE_RUN_LOG}'.")
