"""
S3 helper utilities shared by the Lambda handler and the local pipeline.
"""

import boto3
from pathlib import Path


def get_s3_client():
    return boto3.client("s3")


def download_from_s3(bucket: str, key: str, local_path: Path) -> Path:
    local_path.parent.mkdir(parents=True, exist_ok=True)
    get_s3_client().download_file(bucket, key, str(local_path))
    return local_path


def upload_to_s3(local_path: Path, bucket: str, key: str) -> str:
    get_s3_client().upload_file(str(local_path), bucket, key)
    return f"s3://{bucket}/{key}"


def derive_output_key(input_key: str, strip_prefix: str = "raw/") -> str:
    """Convert an S3 key to a Parquet output key in the destination bucket.

    Examples:
        raw/orders.csv            -> orders.parquet
        raw/2024/01/orders.csv   -> 2024/01/orders.parquet
        uploads/data.json         -> uploads/data.parquet  (no matching prefix)
    """
    key = input_key.lstrip("/")
    if key.startswith(strip_prefix):
        key = key[len(strip_prefix):]
    if "/" in key:
        dir_part, filename = key.rsplit("/", 1)
        stem = filename.rsplit(".", 1)[0] if "." in filename else filename
        return f"{dir_part}/{stem}.parquet"
    else:
        stem = key.rsplit(".", 1)[0] if "." in key else key
        return f"{stem}.parquet"
