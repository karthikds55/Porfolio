"""Unit tests for the ETL load step."""
import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from etl.load import (
    load,
    _build_daily_summary,
    _build_category_summary,
    _build_payment_summary,
)


class TestBuildDailySummary:
    def test_columns_present(self, clean_df):
        summary = _build_daily_summary(clean_df)
        for col in ("total_orders", "total_revenue", "avg_order_value", "delivery_rate_pct"):
            assert col in summary.columns

    def test_total_orders_sums_correctly(self, clean_df):
        summary = _build_daily_summary(clean_df)
        assert summary["total_orders"].sum() == len(clean_df)

    def test_sorted_by_date(self, clean_df):
        summary = _build_daily_summary(clean_df)
        assert (summary["order_date"].diff().dropna() >= pd.Timedelta(0)).all()


class TestBuildCategorySummary:
    def test_columns_present(self, clean_df):
        summary = _build_category_summary(clean_df)
        for col in ("product_category", "return_rate_pct", "delivery_rate_pct"):
            assert col in summary.columns

    def test_one_row_per_category(self, clean_df):
        summary = _build_category_summary(clean_df)
        assert summary["product_category"].is_unique


class TestBuildPaymentSummary:
    def test_columns_present(self, clean_df):
        summary = _build_payment_summary(clean_df)
        for col in ("payment_method", "pct_of_orders"):
            assert col in summary.columns

    def test_pct_sums_to_100(self, clean_df):
        summary = _build_payment_summary(clean_df)
        assert abs(summary["pct_of_orders"].sum() - 100.0) < 0.1


class TestLoadEndToEnd:
    def test_csvs_written(self, tmp_path, clean_df, monkeypatch):
        import etl.config as cfg
        monkeypatch.setattr(cfg, "PROCESSED_DIR", tmp_path / "processed")
        monkeypatch.setattr(cfg, "DB_DIR", tmp_path / "db")
        monkeypatch.setattr(cfg, "PROCESSED_FILE", tmp_path / "processed" / "orders_cleaned.csv")
        monkeypatch.setattr(cfg, "SUMMARY_FILE", tmp_path / "processed" / "orders_summary.csv")
        monkeypatch.setattr(cfg, "CATEGORY_SUMMARY_FILE", tmp_path / "processed" / "cat.csv")
        monkeypatch.setattr(cfg, "PAYMENT_SUMMARY_FILE", tmp_path / "processed" / "pay.csv")
        monkeypatch.setattr(cfg, "DB_FILE", tmp_path / "db" / "ecommerce.db")

        outputs = load(clean_df, pd.DataFrame())
        assert Path(outputs["cleaned_csv"]).exists()
        assert Path(outputs["summary_csv"]).exists()

    def test_sqlite_tables_created(self, tmp_path, clean_df, monkeypatch):
        import etl.config as cfg
        monkeypatch.setattr(cfg, "PROCESSED_DIR", tmp_path / "processed")
        monkeypatch.setattr(cfg, "DB_DIR", tmp_path / "db")
        monkeypatch.setattr(cfg, "PROCESSED_FILE", tmp_path / "processed" / "orders_cleaned.csv")
        monkeypatch.setattr(cfg, "SUMMARY_FILE", tmp_path / "processed" / "orders_summary.csv")
        monkeypatch.setattr(cfg, "CATEGORY_SUMMARY_FILE", tmp_path / "processed" / "cat.csv")
        monkeypatch.setattr(cfg, "PAYMENT_SUMMARY_FILE", tmp_path / "processed" / "pay.csv")
        monkeypatch.setattr(cfg, "DB_FILE", tmp_path / "db" / "ecommerce.db")

        load(clean_df, pd.DataFrame())

        conn = sqlite3.connect(tmp_path / "db" / "ecommerce.db")
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        conn.close()

        for expected in ("orders", "category_summary", "payment_summary", "pipeline_run_log"):
            assert expected in tables
