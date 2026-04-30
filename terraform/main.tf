terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

locals {
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# ── S3: raw input bucket ──────────────────────────────────────────────────────

resource "aws_s3_bucket" "raw" {
  bucket = var.raw_bucket_name
  tags   = local.common_tags
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    id     = "expire-raw-files"
    status = "Enabled"

    filter {
      prefix = var.raw_prefix
    }

    expiration {
      days = var.raw_retention_days
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── S3: Parquet output bucket ─────────────────────────────────────────────────

resource "aws_s3_bucket" "parquet" {
  bucket = var.parquet_bucket_name
  tags   = local.common_tags
}

resource "aws_s3_bucket_versioning" "parquet" {
  bucket = aws_s3_bucket.parquet.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "parquet" {
  bucket = aws_s3_bucket.parquet.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "parquet" {
  bucket                  = aws_s3_bucket.parquet.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── Lambda ────────────────────────────────────────────────────────────────────

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${var.project_name}-s3-to-parquet"
  retention_in_days = var.lambda_log_retention_days
  tags              = local.common_tags
}

resource "aws_lambda_function" "s3_to_parquet" {
  function_name    = "${var.project_name}-s3-to-parquet"
  description      = "Converts CSV/JSON/Excel/Parquet files landing in S3 to Parquet format"
  filename         = "${path.module}/../dist/lambda_package.zip"
  source_code_hash = filebase64sha256("${path.module}/../dist/lambda_package.zip")
  handler          = "s3_to_parquet.handler"
  runtime          = "python3.12"
  role             = aws_iam_role.lambda_exec.arn
  timeout          = var.lambda_timeout_seconds
  memory_size      = var.lambda_memory_mb

  # /tmp is used for staging downloaded files; allow up to 2 GB for large inputs
  ephemeral_storage {
    size = 2048
  }

  environment {
    variables = {
      OUTPUT_BUCKET = aws_s3_bucket.parquet.bucket
      STRIP_PREFIX  = var.raw_prefix
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.lambda,
    aws_iam_role_policy_attachment.lambda_basic,
    aws_iam_role_policy_attachment.lambda_s3,
  ]

  tags = local.common_tags
}

# Allow S3 to invoke the Lambda function
resource "aws_lambda_permission" "allow_s3" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.s3_to_parquet.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.raw.arn
}

# ── S3 event notification → Lambda ────────────────────────────────────────────

resource "aws_s3_bucket_notification" "raw_trigger" {
  bucket = aws_s3_bucket.raw.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.s3_to_parquet.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = var.raw_prefix
  }

  depends_on = [aws_lambda_permission.allow_s3]
}
