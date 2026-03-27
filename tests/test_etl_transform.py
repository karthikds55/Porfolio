"""Unit tests for the ETL transform step."""
import io

import pandas as pd
import pytest

from etl.transform import (
    transform,
    _cast_dtypes,
    _parse_dates,
    _strip_strings,
    _standardise_categoricals,
    _apply_quality_rules,
    _engineer_features,
)


class TestCastDtypes:
    def test_numeric_cast(self, raw_df):
        df = _cast_dtypes(raw_df.copy())
        assert pd.api.types.is_float_dtype(df["order_value"])
        assert pd.api.types.is_float_dtype(df["customer_rating"])

    def test_bad_numeric_becomes_nan(self):
        df = pd.DataFrame({
            "order_id": ["1", "abc"],
            "customer_age": ["30", "xyz"],
            "order_value": ["100.0", "bad"],
            "delivery_time_days": ["5", "5"],
            "customer_rating": ["4.0", "4.0"],
        })
        out = _cast_dtypes(df)
        assert pd.isna(out.loc[1, "order_value"])


class TestParseDates:
    def test_valid_dates_parsed(self, raw_df):
        df = _cast_dtypes(raw_df.copy())
        df = _parse_dates(df)
        assert pd.api.types.is_datetime64_any_dtype(df["order_date"])
        assert df["order_date"].notna().all()

    def test_invalid_date_becomes_nat(self):
        df = pd.DataFrame({"order_date": ["2024-01-01", "not-a-date", "2024-13-01"]})
        out = _parse_dates(df)
        assert pd.isna(out.loc[1, "order_date"])
        assert pd.isna(out.loc[2, "order_date"])


class TestStripStrings:
    def test_whitespace_stripped(self):
        df = pd.DataFrame({
            "product_category": ["  Electronics  ", " Books"],
            "order_status": ["Delivered ", " Cancelled "],
        })
        out = _strip_strings(df)
        assert out["product_category"].tolist() == ["Electronics", "Books"]
        assert out["order_status"].tolist() == ["Delivered", "Cancelled"]


class TestStandardiseCategoricals:
    def test_title_case(self):
        df = pd.DataFrame({
            "product_category": ["electronics", "BOOKS"],
            "payment_method": ["card", "COD"],
            "order_status": ["delivered", "CANCELLED"],
            "discount_applied": ["yes", "no"],
        })
        out = _standardise_categoricals(df)
        assert out["order_status"].tolist() == ["Delivered", "Cancelled"]
        assert out["product_category"].tolist() == ["Electronics", "Books"]

    def test_payment_method_abbreviations_preserved(self):
        df = pd.DataFrame({
            "product_category": ["Electronics"],
            "payment_method": ["upi"],
            "order_status": ["Delivered"],
            "discount_applied": ["Yes"],
        })
        out = _standardise_categoricals(df)
        assert out["payment_method"].iloc[0] == "UPI"

    def test_discount_normalisation(self):
        df = pd.DataFrame({
            "product_category": ["Electronics"],
            "payment_method": ["Card"],
            "order_status": ["Delivered"],
            "discount_applied": ["1"],
        })
        out = _standardise_categoricals(df)
        assert out["discount_applied"].iloc[0] == "Yes"


class TestApplyQualityRules:
    def test_clean_rows_pass(self, raw_df):
        df = _cast_dtypes(raw_df.copy())
        df = _parse_dates(df)
        df = _strip_strings(df)
        df = _standardise_categoricals(df)
        clean, rejected = _apply_quality_rules(df)
        assert len(rejected) == 0
        assert len(clean) == len(raw_df)

    def test_negative_value_rejected(self, bad_df):
        df = _cast_dtypes(bad_df.copy())
        df = _parse_dates(df)
        df = _strip_strings(df)
        df = _standardise_categoricals(df)
        _, rejected = _apply_quality_rules(df)
        assert "invalid_order_value" in rejected["rejection_reason"].str.cat()

    def test_underage_customer_rejected(self, bad_df):
        df = _cast_dtypes(bad_df.copy())
        df = _parse_dates(df)
        df = _strip_strings(df)
        df = _standardise_categoricals(df)
        _, rejected = _apply_quality_rules(df)
        assert "invalid_customer_age" in rejected["rejection_reason"].str.cat()

    def test_out_of_range_rating_rejected(self, bad_df):
        df = _cast_dtypes(bad_df.copy())
        df = _parse_dates(df)
        df = _strip_strings(df)
        df = _standardise_categoricals(df)
        _, rejected = _apply_quality_rules(df)
        assert "invalid_customer_rating" in rejected["rejection_reason"].str.cat()

    def test_duplicate_order_id_rejected(self):
        data = (
            "order_id,order_date,customer_age,product_category,order_value,"
            "discount_applied,payment_method,delivery_time_days,customer_rating,order_status\n"
            "1,2024-01-01,30,Electronics,500.00,Yes,Card,3,4.5,Delivered\n"
            "1,2024-01-02,28,Books,300.00,No,COD,5,3.5,Cancelled\n"
        )
        df = pd.read_csv(io.StringIO(data), dtype=str)
        df = _cast_dtypes(df)
        df = _parse_dates(df)
        df = _strip_strings(df)
        df = _standardise_categoricals(df)
        clean, rejected = _apply_quality_rules(df)
        assert len(rejected) == 1
        assert "duplicate_order_id" in rejected["rejection_reason"].iloc[0]


class TestEngineerFeatures:
    def test_derived_columns_created(self, clean_df):
        expected = [
            "order_year", "order_month", "order_quarter",
            "order_day_of_week", "age_group", "order_value_tier",
            "is_discounted", "is_delivered", "rating_bucket", "delivery_speed",
        ]
        for col in expected:
            assert col in clean_df.columns, f"Missing column: {col}"

    def test_is_delivered_flag(self, clean_df):
        delivered = clean_df["order_status"].str.upper() == "DELIVERED"
        assert (clean_df.loc[delivered, "is_delivered"] == True).all()
        assert (clean_df.loc[~delivered, "is_delivered"] == False).all()

    def test_is_discounted_flag(self, clean_df):
        disc = clean_df["discount_applied"].str.upper() == "YES"
        assert (clean_df.loc[disc, "is_discounted"] == True).all()

    def test_delivery_speed_express(self, clean_df):
        express = clean_df["delivery_time_days"].astype(float) <= 3
        assert (clean_df.loc[express, "delivery_speed"] == "Express").all()


class TestTransformEndToEnd:
    def test_returns_two_dataframes(self, raw_df):
        result = transform(raw_df)
        assert len(result) == 2

    def test_no_nulls_in_critical_columns(self, raw_df):
        clean, _ = transform(raw_df)
        for col in ("order_date", "order_value", "order_status", "payment_method"):
            assert clean[col].notna().all(), f"Unexpected nulls in {col}"


def test_real_file_smoke():
    """Smoke-test against the actual raw CSV (skipped if file absent)."""
    from pathlib import Path
    raw_path = Path("data/raw/daily_ecommerce_orders.csv")
    if not raw_path.exists():
        pytest.skip("Raw data file not found")
    from etl.extract import extract
    raw = extract(raw_path)
    clean, rejected = transform(raw)
    assert len(clean) > 0
    assert len(clean) + len(rejected) == len(raw)
