"""
Unit tests for ingestion and transform pipelines.
Run: pytest tests/
"""

import pandas as pd
import pytest
from pathlib import Path

from pipelines.ingest import ingest_orders
from pipelines.transform import build_daily_summary, build_category_summary
from transforms.quality_checks import check_no_nulls, check_no_duplicates, check_value_range


RAW_ORDERS = Path(__file__).parent.parent / "data" / "raw" / "daily_ecommerce_orders.csv"


# ---------------------------------------------------------------------------
# Ingestion tests
# ---------------------------------------------------------------------------

def test_ingest_orders_returns_dataframe():
    df = ingest_orders(RAW_ORDERS)
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0


def test_ingest_orders_columns():
    df = ingest_orders(RAW_ORDERS)
    expected = {"order_id", "order_date", "order_value", "order_status", "customer_rating"}
    assert expected.issubset(df.columns)


def test_ingest_orders_date_dtype():
    df = ingest_orders(RAW_ORDERS)
    assert pd.api.types.is_datetime64_any_dtype(df["order_date"])


def test_ingest_orders_discount_bool():
    df = ingest_orders(RAW_ORDERS)
    assert df["discount_applied"].dtype == bool or df["discount_applied"].isnull().all()


# ---------------------------------------------------------------------------
# Transform tests
# ---------------------------------------------------------------------------

def _sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "order_id": [1, 2, 3, 4],
            "order_date": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02"]),
            "order_value": [100.0, 200.0, 150.0, 50.0],
            "customer_rating": [4.5, 3.0, 5.0, 2.0],
            "order_status": ["Delivered", "Cancelled", "Delivered", "Delivered"],
            "product_category": ["Electronics", "Books", "Electronics", "Books"],
        }
    )


def test_build_daily_summary_row_count():
    df = _sample_df()
    summary = build_daily_summary(df)
    assert len(summary) == 2  # two distinct dates


def test_build_daily_summary_revenue():
    df = _sample_df()
    summary = build_daily_summary(df)
    total = summary["total_revenue"].sum()
    assert total == pytest.approx(500.0)


def test_build_category_summary_categories():
    df = _sample_df()
    summary = build_category_summary(df)
    assert set(summary["product_category"]) == {"Electronics", "Books"}


# ---------------------------------------------------------------------------
# Quality check tests
# ---------------------------------------------------------------------------

def test_check_no_nulls_passes():
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    assert check_no_nulls(df, ["a", "b"]) == []


def test_check_no_nulls_fails():
    df = pd.DataFrame({"a": [1, None], "b": [3, 4]})
    assert "a" in check_no_nulls(df, ["a", "b"])


def test_check_no_duplicates_passes():
    df = pd.DataFrame({"id": [1, 2, 3]})
    assert check_no_duplicates(df, "id")


def test_check_no_duplicates_fails():
    df = pd.DataFrame({"id": [1, 1, 3]})
    assert not check_no_duplicates(df, "id")


def test_check_value_range_passes():
    df = pd.DataFrame({"rating": [1.0, 2.5, 5.0]})
    assert check_value_range(df, "rating", 0, 5)


def test_check_value_range_fails():
    df = pd.DataFrame({"rating": [1.0, 6.0]})
    assert not check_value_range(df, "rating", 0, 5)
