"""Shared pytest fixtures for the ETL test suite."""
import io
import textwrap

import pandas as pd
import pytest


RAW_CSV = textwrap.dedent("""\
    order_id,order_date,customer_age,product_category,order_value,discount_applied,payment_method,delivery_time_days,customer_rating,order_status
    1,2024-01-15,30,Electronics,1500.00,Yes,Card,3,4.5,Delivered
    2,2024-01-16,25,Books,200.50,No,COD,7,3.0,Cancelled
    3,2024-01-17,45,Sports,750.00,Yes,Wallet,5,4.0,Delivered
    4,2024-01-18,35,Home & Kitchen,3200.00,No,Card,2,5.0,Delivered
    5,2024-01-19,60,Electronics,900.00,Yes,UPI,10,2.5,Returned
    6,2024-01-20,22,Books,150.00,No,COD,6,3.5,Delivered
    7,2024-01-21,50,Sports,1100.00,Yes,Card,4,4.2,Delivered
    8,2024-01-22,28,Home & Kitchen,2800.00,No,Wallet,3,4.8,Delivered
""")

BAD_ROWS_CSV = textwrap.dedent("""\
    order_id,order_date,customer_age,product_category,order_value,discount_applied,payment_method,delivery_time_days,customer_rating,order_status
    1,2024-02-01,25,Electronics,500.00,Yes,Card,3,4.0,Delivered
    2,2024-02-02,10,Books,300.00,No,COD,5,3.5,Cancelled
    3,2024-02-03,35,Sports,-50.00,Yes,Wallet,4,4.2,Delivered
    4,2024-02-04,40,Electronics,700.00,No,Card,3,7.0,Delivered
    5,2024-02-05,55,Books,200.00,Yes,COD,3,3.0,Delivered
""")


@pytest.fixture
def raw_df() -> pd.DataFrame:
    """8-row clean raw DataFrame (all strings, as extract() produces)."""
    return pd.read_csv(io.StringIO(RAW_CSV), dtype=str)


@pytest.fixture
def bad_df() -> pd.DataFrame:
    """5-row DataFrame with known-bad rows (age=10, value=-50, rating=7)."""
    return pd.read_csv(io.StringIO(BAD_ROWS_CSV), dtype=str)


@pytest.fixture
def clean_df(raw_df) -> pd.DataFrame:
    """Fully transformed clean DataFrame from the clean raw fixture."""
    from etl.transform import transform
    clean, _ = transform(raw_df)
    return clean
