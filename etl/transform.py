"""Transform step: clean, cast, validate, and enrich the raw DataFrame."""
from typing import Tuple

import numpy as np
import pandas as pd
from loguru import logger

from etl import config


# ── Public API ─────────────────────────────────────────────────────────────────

def transform(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Run the full transformation chain on the raw extracted DataFrame.

    Steps applied in order:
      1. Select & reorder expected columns
      2. Cast data types
      3. Parse / validate dates
      4. Strip whitespace from string columns
      5. Standardise categorical values
      6. Apply business-rule quality checks (flag bad rows)
      7. Impute / drop nulls
      8. Feature engineering (derived columns)

    Args:
        df: Raw DataFrame from the extract step.

    Returns:
        A tuple of (clean_df, rejected_df) where rejected_df holds rows
        that failed hard quality rules.
    """
    logger.info("[TRANSFORM] Starting transformation pipeline.")

    df = df[config.EXPECTED_COLUMNS].copy()

    df = _cast_dtypes(df)
    df = _parse_dates(df)
    df = _strip_strings(df)
    df = _standardise_categoricals(df)
    df, rejected = _apply_quality_rules(df)
    df = _impute_nulls(df)
    df = _engineer_features(df)

    logger.info(
        f"[TRANSFORM] Complete. Clean rows: {len(df):,} | Rejected rows: {len(rejected):,}"
    )
    return df, rejected


# ── Step 1 – Type casting ──────────────────────────────────────────────────────

def _cast_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Cast columns to their target types; coerce errors to NaN."""
    numeric_cols = {
        "order_id": "Int64",
        "customer_age": "Int64",
        "order_value": "float64",
        "delivery_time_days": "Int64",
        "customer_rating": "float64",
    }
    for col, dtype in numeric_cols.items():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(dtype)

    logger.debug("[TRANSFORM] Data types cast successfully.")
    return df


# ── Step 2 – Date parsing ──────────────────────────────────────────────────────

def _parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Parse order_date to datetime; invalid dates become NaT."""
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    n_bad = df["order_date"].isna().sum()
    if n_bad:
        logger.warning(f"[TRANSFORM] {n_bad} rows have unparseable order_date → NaT.")
    return df


# ── Step 3 – String whitespace ─────────────────────────────────────────────────

def _strip_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Strip leading/trailing whitespace from all object/string columns."""
    str_cols = df.select_dtypes(include=["object", "str"]).columns
    for col in str_cols:
        df[col] = df[col].str.strip()
    return df


# ── Step 4 – Standardise categoricals ─────────────────────────────────────────

def _standardise_categoricals(df: pd.DataFrame) -> pd.DataFrame:
    """Title-case and validate categorical columns; flag unexpected values."""
    for col in ("product_category", "order_status", "discount_applied"):
        if col in df.columns:
            df[col] = df[col].str.title()

    # Normalise payment method: preserve known abbreviations
    payment_map = {
        "cod": "COD", "upi": "UPI", "card": "Card", "wallet": "Wallet",
        "net banking": "Net Banking",
    }
    if "payment_method" in df.columns:
        df["payment_method"] = (
            df["payment_method"]
            .str.strip()
            .str.lower()
            .map(payment_map)
            .fillna(df["payment_method"].str.title())
        )

    # Normalise discount_applied → Yes/No
    df["discount_applied"] = df["discount_applied"].replace(
        {"Yes": "Yes", "No": "No", "True": "Yes", "False": "No",
         "1": "Yes", "0": "No", "Y": "Yes", "N": "No"}
    )

    invalid_status = (
        ~df["order_status"].isin(config.VALID_ORDER_STATUSES) & df["order_status"].notna()
    )
    if invalid_status.any():
        logger.warning(
            f"[TRANSFORM] {invalid_status.sum()} rows have unrecognised order_status: "
            f"{df.loc[invalid_status, 'order_status'].unique().tolist()}"
        )

    invalid_payment = (
        ~df["payment_method"].isin(config.VALID_PAYMENT_METHODS) & df["payment_method"].notna()
    )
    if invalid_payment.any():
        logger.warning(
            f"[TRANSFORM] {invalid_payment.sum()} rows have unrecognised payment_method: "
            f"{df.loc[invalid_payment, 'payment_method'].unique().tolist()}"
        )

    return df


# ── Step 5 – Quality rules ─────────────────────────────────────────────────────

def _apply_quality_rules(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Flag and quarantine rows that violate hard business rules."""
    reasons = pd.Series([""] * len(df), index=df.index)

    bad = df["order_value"].notna() & (df["order_value"] <= config.MIN_ORDER_VALUE)
    reasons[bad] += "invalid_order_value;"

    bad = df["customer_age"].notna() & (
        (df["customer_age"] < config.MIN_CUSTOMER_AGE) |
        (df["customer_age"] > config.MAX_CUSTOMER_AGE)
    )
    reasons[bad] += "invalid_customer_age;"

    bad = df["customer_rating"].notna() & (
        (df["customer_rating"] < config.MIN_RATING) |
        (df["customer_rating"] > config.MAX_RATING)
    )
    reasons[bad] += "invalid_customer_rating;"

    bad = df["delivery_time_days"].notna() & (
        (df["delivery_time_days"] < config.MIN_DELIVERY_DAYS) |
        (df["delivery_time_days"] > config.MAX_DELIVERY_DAYS)
    )
    reasons[bad] += "invalid_delivery_days;"

    dupes = df.duplicated(subset=["order_id"], keep="first")
    reasons[dupes] += "duplicate_order_id;"

    reject_mask = reasons.str.len() > 0
    rejected = df[reject_mask].copy()
    rejected["rejection_reason"] = reasons[reject_mask]
    clean = df[~reject_mask].copy()

    logger.info(
        f"[TRANSFORM] Quality check: {len(clean):,} clean, {len(rejected):,} rejected."
    )
    if len(rejected):
        reason_counts = rejected["rejection_reason"].str.split(";").explode()
        reason_counts = reason_counts[reason_counts != ""].value_counts()
        for reason, cnt in reason_counts.items():
            logger.warning(f"  {reason}: {cnt}")

    return clean, rejected


# ── Step 6 – Null imputation ───────────────────────────────────────────────────

def _impute_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Fill or drop remaining nulls after quality rules."""
    before = len(df)
    df = df.dropna(subset=["order_date"])
    dropped = before - len(df)
    if dropped:
        logger.warning(f"[TRANSFORM] Dropped {dropped} rows with null order_date.")

    for col in ("customer_age", "customer_rating", "delivery_time_days"):
        if col in df.columns and df[col].isna().any():
            median_val = df[col].median()
            df[col] = df[col].fillna(median_val)
            logger.info(f"[TRANSFORM] Imputed {col} nulls with median={median_val:.2f}")

    if df["order_value"].isna().any():
        median_val = df["order_value"].median()
        df["order_value"] = df["order_value"].fillna(median_val)
        logger.info(f"[TRANSFORM] Imputed order_value nulls with median={median_val:.2f}")

    for col in ("product_category", "payment_method", "order_status", "discount_applied"):
        if df[col].isna().any():
            df[col] = df[col].fillna("Unknown")

    return df


# ── Step 7 – Feature engineering ──────────────────────────────────────────────

def _engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Derive analytical columns from the cleaned data."""
    df["order_year"] = df["order_date"].dt.year.astype("Int64")
    df["order_month"] = df["order_date"].dt.month.astype("Int64")
    df["order_quarter"] = df["order_date"].dt.quarter.astype("Int64")
    df["order_day_of_week"] = df["order_date"].dt.day_name()
    df["order_month_name"] = df["order_date"].dt.month_name()
    df["order_quarter_label"] = "Q" + df["order_quarter"].astype(str)

    age_bins = [0, 24, 34, 49, 64, 120]
    age_labels = ["18-24", "25-34", "35-49", "50-64", "65+"]
    df["age_group"] = pd.cut(
        df["customer_age"].astype(float),
        bins=age_bins,
        labels=age_labels,
        right=True,
    ).astype(str)

    quantiles = df["order_value"].quantile([0.25, 0.50, 0.75])
    q1, q2, q3 = quantiles[0.25], quantiles[0.50], quantiles[0.75]
    conditions = [
        df["order_value"] <= q1,
        (df["order_value"] > q1) & (df["order_value"] <= q2),
        (df["order_value"] > q2) & (df["order_value"] <= q3),
        df["order_value"] > q3,
    ]
    df["order_value_tier"] = np.select(
        conditions, ["Low", "Medium", "High", "Premium"], default="Unknown"
    )

    df["is_discounted"] = df["discount_applied"].str.upper() == "YES"
    df["is_delivered"] = df["order_status"].str.upper() == "DELIVERED"

    rating_conditions = [
        df["customer_rating"] < 2.0,
        (df["customer_rating"] >= 2.0) & (df["customer_rating"] < 3.5),
        (df["customer_rating"] >= 3.5) & (df["customer_rating"] < 4.5),
        df["customer_rating"] >= 4.5,
    ]
    df["rating_bucket"] = np.select(
        rating_conditions, ["Poor", "Fair", "Good", "Excellent"], default="Unknown"
    )

    delivery_conditions = [
        df["delivery_time_days"].astype(float) <= 3,
        (df["delivery_time_days"].astype(float) > 3) & (df["delivery_time_days"].astype(float) <= 7),
        df["delivery_time_days"].astype(float) > 7,
    ]
    df["delivery_speed"] = np.select(
        delivery_conditions, ["Express", "Standard", "Slow"], default="Unknown"
    )

    logger.info(
        f"[TRANSFORM] Feature engineering complete. "
        f"DataFrame now has {len(df.columns)} columns."
    )
    return df
