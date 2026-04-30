"""
AWS Lambda handler: converts any supported file landing in S3 to Parquet.
Triggered by s3:ObjectCreated:* events on the raw input bucket.

Supported formats: CSV, TSV, JSON, JSONL, Excel (.xlsx/.xls), Parquet (passthrough).

Environment variables:
    OUTPUT_BUCKET  - S3 bucket to write Parquet files to (required)
    STRIP_PREFIX   - S3 key prefix to strip before building the output key (default: "raw/")
"""

import json
import logging
import os
import tempfile
from pathlib import Path
from urllib.parse import unquote_plus

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

try:
    from pipelines.s3_utils import derive_output_key, download_from_s3, upload_to_s3
except ImportError:
    from s3_utils import derive_output_key, download_from_s3, upload_to_s3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

OUTPUT_BUCKET: str = os.environ.get("OUTPUT_BUCKET", "")
STRIP_PREFIX: str = os.environ.get("STRIP_PREFIX", "raw/")


# ── File readers ──────────────────────────────────────────────────────────────

def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def _read_tsv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, sep="\t")


def _read_json(path: Path) -> pd.DataFrame:
    # JSONL / NDJSON first; fall back to standard JSON array
    try:
        return pd.read_json(path, lines=True)
    except ValueError:
        return pd.read_json(path)


def _read_excel(path: Path) -> pd.DataFrame:
    engine = "openpyxl" if path.suffix.lower() == ".xlsx" else "xlrd"
    return pd.read_excel(path, engine=engine)


def _read_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)


_READERS = {
    ".csv":     _read_csv,
    ".tsv":     _read_tsv,
    ".json":    _read_json,
    ".jsonl":   _read_json,
    ".ndjson":  _read_json,
    ".xlsx":    _read_excel,
    ".xls":     _read_excel,
    ".parquet": _read_parquet,
}


def read_file(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    reader = _READERS.get(suffix)
    if reader is None:
        supported = ", ".join(sorted(_READERS))
        raise ValueError(
            f"Unsupported file format: '{suffix}'. Supported formats: {supported}"
        )
    return reader(path)


# ── Parquet writer ────────────────────────────────────────────────────────────

def write_parquet(df: pd.DataFrame, path: Path) -> None:
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, str(path), compression="snappy")


# ── Core conversion ───────────────────────────────────────────────────────────

def convert(bucket: str, key: str) -> dict:
    """Download one S3 object, convert to Parquet, upload to OUTPUT_BUCKET."""
    if not OUTPUT_BUCKET:
        raise EnvironmentError("OUTPUT_BUCKET environment variable is not set")

    output_key = derive_output_key(key, strip_prefix=STRIP_PREFIX)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        local_input = tmp_dir / Path(key).name
        local_output = tmp_dir / (Path(key).stem + ".parquet")

        logger.info("Downloading s3://%s/%s", bucket, key)
        download_from_s3(bucket, key, local_input)

        df = read_file(local_input)
        logger.info(
            "Read %d rows x %d columns from %s", len(df), len(df.columns), key
        )

        write_parquet(df, local_output)
        logger.info(
            "Wrote Parquet (%d bytes) → %s",
            local_output.stat().st_size,
            local_output.name,
        )

        s3_uri = upload_to_s3(local_output, OUTPUT_BUCKET, output_key)
        logger.info("Uploaded → %s", s3_uri)

    return {
        "source": f"s3://{bucket}/{key}",
        "destination": s3_uri,
        "output_key": output_key,
        "rows": len(df),
        "columns": list(df.columns),
    }


# ── Lambda entry point ────────────────────────────────────────────────────────

def handler(event: dict, context: object) -> dict:
    """Process one or more S3 ObjectCreated records from an S3 event notification."""
    results = []

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])

        try:
            result = convert(bucket, key)
            results.append({"status": "ok", **result})
        except Exception as exc:
            logger.error(
                "Failed to convert s3://%s/%s: %s", bucket, key, exc, exc_info=True
            )
            results.append({
                "status": "error",
                "source": f"s3://{bucket}/{key}",
                "error": str(exc),
            })

    return {"statusCode": 200, "body": json.dumps(results)}
