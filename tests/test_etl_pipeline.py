"""Integration test: full ETL pipeline run against the real raw CSV."""
from pathlib import Path

import pytest

from etl import config


@pytest.mark.skipif(
    not config.RAW_FILE.exists(),
    reason="Raw data file not found – skipping integration test",
)
def test_full_pipeline_runs_successfully(tmp_path, monkeypatch):
    """Run the complete ETL pipeline against the real CSV in a temp directory."""
    import etl.config as cfg

    monkeypatch.setattr(cfg, "PROCESSED_DIR", tmp_path / "processed")
    monkeypatch.setattr(cfg, "DB_DIR", tmp_path / "db")
    monkeypatch.setattr(cfg, "PROCESSED_FILE", tmp_path / "processed" / "orders_cleaned.csv")
    monkeypatch.setattr(cfg, "SUMMARY_FILE", tmp_path / "processed" / "orders_summary.csv")
    monkeypatch.setattr(cfg, "CATEGORY_SUMMARY_FILE", tmp_path / "processed" / "cat.csv")
    monkeypatch.setattr(cfg, "PAYMENT_SUMMARY_FILE", tmp_path / "processed" / "pay.csv")
    monkeypatch.setattr(cfg, "DB_FILE", tmp_path / "db" / "ecommerce.db")
    monkeypatch.setattr(cfg, "LOGS_DIR", tmp_path / "logs")

    from etl.pipeline import run_pipeline

    result = run_pipeline(cfg.RAW_FILE)

    assert result["status"] == "SUCCESS"
    assert result["raw_rows"] > 0
    assert result["clean_rows"] > 0
    assert result["clean_rows"] + result["rejected_rows"] == result["raw_rows"]
    assert result["rejection_rate_pct"] < 20.0

    assert Path(result["outputs"]["cleaned_csv"]).exists()
    assert Path(result["outputs"]["sqlite_db"]).exists()
