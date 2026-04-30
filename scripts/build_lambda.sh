#!/usr/bin/env bash
# Build the Lambda deployment package: code + Python dependencies → dist/lambda_package.zip
# Run from the repo root: bash scripts/build_lambda.sh
#
# Prerequisites: Python 3.12, pip

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="$ROOT_DIR/dist/lambda_build"
ZIP_FILE="$ROOT_DIR/dist/lambda_package.zip"

echo "==> Cleaning build directory"
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

echo "==> Installing Python dependencies into build directory"
pip install \
  --quiet \
  --target "$BUILD_DIR" \
  boto3 \
  pyarrow \
  pandas \
  openpyxl \
  "xlrd>=2.0"

echo "==> Copying Lambda source files"
cp "$ROOT_DIR/pipelines/s3_to_parquet.py" "$BUILD_DIR/"
cp "$ROOT_DIR/pipelines/s3_utils.py"      "$BUILD_DIR/"

echo "==> Creating deployment ZIP"
mkdir -p "$ROOT_DIR/dist"
cd "$BUILD_DIR"
zip -r -q "$ZIP_FILE" .

SIZE=$(du -sh "$ZIP_FILE" | cut -f1)
echo "==> Done: $ZIP_FILE ($SIZE)"
echo ""
echo "Next steps:"
echo "  cd terraform"
echo "  cp terraform.tfvars.example terraform.tfvars   # fill in bucket names"
echo "  terraform init"
echo "  terraform plan"
echo "  terraform apply"
