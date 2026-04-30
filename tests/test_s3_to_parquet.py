"""
Unit tests for the S3-to-Parquet Lambda handler.
All S3 calls are mocked — no real AWS credentials required.
"""

import json
import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pyarrow.parquet as pq
import pytest

from pipelines.s3_to_parquet import convert, handler, read_file, write_parquet
from pipelines.s3_utils import derive_output_key


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture()
def sample_df() -> pd.DataFrame:
    return pd.DataFrame({
        "order_id":    [1, 2, 3],
        "order_date":  pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
        "order_value": [100.0, 200.0, 150.0],
    })


@pytest.fixture()
def tmp_dir():
    d = tempfile.mkdtemp()
    yield Path(d)
    shutil.rmtree(d, ignore_errors=True)


def _s3_event(bucket: str, key: str) -> dict:
    return {
        "Records": [{
            "s3": {
                "bucket": {"name": bucket},
                "object": {"key": key},
            }
        }]
    }


# ── derive_output_key ─────────────────────────────────────────────────────────

def test_derive_output_key_strips_raw_prefix():
    assert derive_output_key("raw/orders.csv") == "orders.parquet"


def test_derive_output_key_preserves_subdirectory():
    assert derive_output_key("raw/2024/01/orders.csv") == "2024/01/orders.parquet"


def test_derive_output_key_no_matching_prefix():
    assert derive_output_key("uploads/data.json") == "uploads/data.parquet"


def test_derive_output_key_custom_strip_prefix():
    assert derive_output_key("landing/file.xlsx", strip_prefix="landing/") == "file.parquet"


def test_derive_output_key_no_directory():
    assert derive_output_key("orders.csv", strip_prefix="raw/") == "orders.parquet"


# ── read_file ─────────────────────────────────────────────────────────────────

def test_read_csv(tmp_dir: Path, sample_df: pd.DataFrame):
    p = tmp_dir / "orders.csv"
    sample_df.to_csv(p, index=False)
    df = read_file(p)
    assert list(df.columns) == list(sample_df.columns)
    assert len(df) == 3


def test_read_tsv(tmp_dir: Path, sample_df: pd.DataFrame):
    p = tmp_dir / "orders.tsv"
    sample_df.to_csv(p, sep="\t", index=False)
    df = read_file(p)
    assert len(df) == 3


def test_read_json_standard(tmp_dir: Path, sample_df: pd.DataFrame):
    p = tmp_dir / "orders.json"
    sample_df.to_json(p, orient="records", indent=2)
    df = read_file(p)
    assert len(df) == 3


def test_read_jsonl(tmp_dir: Path, sample_df: pd.DataFrame):
    p = tmp_dir / "orders.jsonl"
    sample_df.to_json(p, orient="records", lines=True)
    df = read_file(p)
    assert len(df) == 3


def test_read_excel_xlsx(tmp_dir: Path, sample_df: pd.DataFrame):
    p = tmp_dir / "orders.xlsx"
    sample_df.to_excel(p, index=False, engine="openpyxl")
    df = read_file(p)
    assert len(df) == 3


def test_read_parquet_passthrough(tmp_dir: Path, sample_df: pd.DataFrame):
    p = tmp_dir / "orders.parquet"
    write_parquet(sample_df, p)
    df = read_file(p)
    assert len(df) == 3


def test_read_file_unsupported_raises(tmp_dir: Path):
    p = tmp_dir / "data.xyz"
    p.write_text("something")
    with pytest.raises(ValueError, match="Unsupported file format"):
        read_file(p)


# ── write_parquet ─────────────────────────────────────────────────────────────

def test_write_parquet_creates_valid_file(tmp_dir: Path, sample_df: pd.DataFrame):
    out = tmp_dir / "out.parquet"
    write_parquet(sample_df, out)
    assert out.exists()
    result = pd.read_parquet(out)
    assert len(result) == len(sample_df)
    assert list(result.columns) == list(sample_df.columns)


def test_write_parquet_uses_snappy_compression(tmp_dir: Path, sample_df: pd.DataFrame):
    out = tmp_dir / "out.parquet"
    write_parquet(sample_df, out)
    meta = pq.read_metadata(str(out))
    assert meta.row_group(0).column(0).compression == "SNAPPY"


# ── convert (S3 calls mocked) ─────────────────────────────────────────────────

def _make_fake_download(source_file: Path):
    """Returns a side_effect function that copies source_file to local_path."""
    def _download(bucket, key, local_path):
        shutil.copy(source_file, local_path)
        return local_path
    return _download


@patch("pipelines.s3_to_parquet.OUTPUT_BUCKET", "parquet-bucket")
@patch("pipelines.s3_to_parquet.upload_to_s3", return_value="s3://parquet-bucket/orders.parquet")
@patch("pipelines.s3_to_parquet.download_from_s3")
def test_convert_csv_returns_metadata(mock_dl, mock_ul, tmp_dir, sample_df):
    csv_file = tmp_dir / "orders.csv"
    sample_df.to_csv(csv_file, index=False)
    mock_dl.side_effect = _make_fake_download(csv_file)

    result = convert("raw-bucket", "raw/orders.csv")

    assert result["rows"] == 3
    assert result["destination"] == "s3://parquet-bucket/orders.parquet"
    assert result["output_key"] == "orders.parquet"
    assert "order_id" in result["columns"]
    mock_ul.assert_called_once()


@patch("pipelines.s3_to_parquet.OUTPUT_BUCKET", "parquet-bucket")
@patch("pipelines.s3_to_parquet.upload_to_s3", return_value="s3://parquet-bucket/orders.parquet")
@patch("pipelines.s3_to_parquet.download_from_s3")
def test_convert_jsonl(mock_dl, mock_ul, tmp_dir, sample_df):
    jsonl_file = tmp_dir / "orders.jsonl"
    sample_df.to_json(jsonl_file, orient="records", lines=True)
    mock_dl.side_effect = _make_fake_download(jsonl_file)

    result = convert("raw-bucket", "raw/orders.jsonl")

    assert result["rows"] == 3


@patch("pipelines.s3_to_parquet.OUTPUT_BUCKET", "parquet-bucket")
@patch("pipelines.s3_to_parquet.upload_to_s3", return_value="s3://parquet-bucket/orders.parquet")
@patch("pipelines.s3_to_parquet.download_from_s3")
def test_convert_excel(mock_dl, mock_ul, tmp_dir, sample_df):
    xlsx_file = tmp_dir / "orders.xlsx"
    sample_df.to_excel(xlsx_file, index=False, engine="openpyxl")
    mock_dl.side_effect = _make_fake_download(xlsx_file)

    result = convert("raw-bucket", "raw/orders.xlsx")

    assert result["rows"] == 3


@patch("pipelines.s3_to_parquet.OUTPUT_BUCKET", "")
def test_convert_raises_when_output_bucket_unset():
    with pytest.raises(EnvironmentError, match="OUTPUT_BUCKET"):
        convert("raw-bucket", "raw/orders.csv")


# ── handler (S3 calls mocked) ─────────────────────────────────────────────────

@patch("pipelines.s3_to_parquet.OUTPUT_BUCKET", "parquet-bucket")
@patch("pipelines.s3_to_parquet.upload_to_s3", return_value="s3://parquet-bucket/orders.parquet")
@patch("pipelines.s3_to_parquet.download_from_s3")
def test_handler_returns_200_on_success(mock_dl, mock_ul, tmp_dir, sample_df):
    csv_file = tmp_dir / "orders.csv"
    sample_df.to_csv(csv_file, index=False)
    mock_dl.side_effect = _make_fake_download(csv_file)

    response = handler(_s3_event("raw-bucket", "raw/orders.csv"), {})

    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert body[0]["status"] == "ok"
    assert body[0]["rows"] == 3


@patch("pipelines.s3_to_parquet.OUTPUT_BUCKET", "parquet-bucket")
@patch("pipelines.s3_to_parquet.download_from_s3")
def test_handler_captures_error_without_raising(mock_dl, tmp_dir):
    mock_dl.side_effect = Exception("S3 connection refused")

    response = handler(_s3_event("raw-bucket", "raw/bad.csv"), {})

    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert body[0]["status"] == "error"
    assert "S3 connection refused" in body[0]["error"]


def test_handler_empty_event_returns_200():
    response = handler({"Records": []}, {})
    assert response["statusCode"] == 200
    assert json.loads(response["body"]) == []
