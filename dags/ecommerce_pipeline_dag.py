"""
Airflow DAG: ecommerce_pipeline
Orchestrates the full pipeline: ingest → transform → quality_checks.
Schedule: daily at midnight.

Prerequisites:
    pip install apache-airflow
    export AIRFLOW_HOME=~/airflow
    airflow db init
    airflow dags list
"""

from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

from pipelines.ingest import ingest_orders, save_staging
from pipelines.transform import load_staging, build_daily_summary, build_category_summary, save_mart
from transforms.quality_checks import run_checks


# ── Task callables ────────────────────────────────────────────────────────────

def task_ingest() -> None:
    """Load raw CSV, validate schema, coerce dtypes, write staging CSV."""
    df = ingest_orders()
    save_staging(df)


def task_transform() -> None:
    """Read staging CSV, build daily and category summaries, write mart CSVs."""
    df      = load_staging()
    daily   = build_daily_summary(df)
    category = build_category_summary(df)
    save_mart(daily,    "daily_summary")
    save_mart(category, "category_summary")


def task_quality_checks() -> None:
    """Run null, duplicate, and range checks against the staging layer."""
    staging = Path(__file__).parent.parent / "data" / "staging" / "orders_staging.csv"
    df = pd.read_csv(staging, parse_dates=["order_date"])
    run_checks(df)


# ── DAG definition ────────────────────────────────────────────────────────────

default_args = {
    "owner":          "karthik",
    "retries":        1,
    "retry_delay":    timedelta(minutes=5),
    "email_on_retry": False,
}

with DAG(
    dag_id="ecommerce_pipeline",
    default_args=default_args,
    description="Ingest → Transform → Quality Checks for daily ecommerce orders",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["data-engineering", "ecommerce", "portfolio"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest_orders",
        python_callable=task_ingest,
    )

    transform = PythonOperator(
        task_id="transform_orders",
        python_callable=task_transform,
    )

    quality = PythonOperator(
        task_id="quality_checks",
        python_callable=task_quality_checks,
    )

    ingest >> transform >> quality
