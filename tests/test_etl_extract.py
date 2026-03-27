"""Unit tests for the ETL extract step."""
import pandas as pd
import pytest

from etl.extract import extract, _validate_schema
from etl import config


class TestExtractHappyPath:
    def test_returns_dataframe(self, tmp_path):
        csv = tmp_path / "orders.csv"
        csv.write_text(
            "order_id,order_date,customer_age,product_category,order_value,"
            "discount_applied,payment_method,delivery_time_days,customer_rating,order_status\n"
            "1,2024-01-01,30,Electronics,500.00,Yes,Card,3,4.5,Delivered\n"
        )
        df = extract(csv)
        assert isinstance(df, pd.DataFrame)

    def test_row_count(self, tmp_path):
        csv = tmp_path / "orders.csv"
        header = (
            "order_id,order_date,customer_age,product_category,order_value,"
            "discount_applied,payment_method,delivery_time_days,customer_rating,order_status\n"
        )
        rows = "".join(
            f"{i},2024-01-0{i},30,Electronics,500.00,Yes,Card,3,4.5,Delivered\n"
            for i in range(1, 6)
        )
        csv.write_text(header + rows)
        df = extract(csv)
        assert len(df) == 5

    def test_columns_present(self, tmp_path):
        csv = tmp_path / "orders.csv"
        csv.write_text(
            "order_id,order_date,customer_age,product_category,order_value,"
            "discount_applied,payment_method,delivery_time_days,customer_rating,order_status\n"
            "1,2024-01-01,30,Electronics,500.00,Yes,Card,3,4.5,Delivered\n"
        )
        df = extract(csv)
        for col in config.EXPECTED_COLUMNS:
            assert col in df.columns


class TestExtractErrors:
    def test_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            extract(tmp_path / "nonexistent.csv")

    def test_empty_file_raises(self, tmp_path):
        csv = tmp_path / "empty.csv"
        csv.write_text(
            "order_id,order_date,customer_age,product_category,order_value,"
            "discount_applied,payment_method,delivery_time_days,customer_rating,order_status\n"
        )
        with pytest.raises(ValueError, match="empty"):
            extract(csv)

    def test_missing_columns_raises(self, tmp_path):
        csv = tmp_path / "bad_schema.csv"
        csv.write_text("order_id,order_date\n1,2024-01-01\n")
        with pytest.raises(ValueError, match="missing columns"):
            extract(csv)


class TestValidateSchema:
    def test_valid_schema_passes(self):
        df = pd.DataFrame(columns=config.EXPECTED_COLUMNS)
        _validate_schema(df)

    def test_missing_column_raises(self):
        df = pd.DataFrame(columns=["order_id", "order_date"])
        with pytest.raises(ValueError):
            _validate_schema(df)
