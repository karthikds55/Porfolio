variable "aws_region" {
  description = "AWS region to deploy all resources"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Prefix applied to every resource name"
  type        = string
  default     = "data-engineering"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "raw_bucket_name" {
  description = "Globally unique name for the S3 raw input bucket"
  type        = string
}

variable "parquet_bucket_name" {
  description = "Globally unique name for the S3 Parquet output bucket"
  type        = string
}

variable "raw_prefix" {
  description = "S3 key prefix that triggers the Lambda (must end with /)"
  type        = string
  default     = "raw/"
}

variable "raw_retention_days" {
  description = "Days before raw input files are automatically expired"
  type        = number
  default     = 30
}

variable "lambda_timeout_seconds" {
  description = "Lambda function timeout — allow headroom for large files"
  type        = number
  default     = 300
}

variable "lambda_memory_mb" {
  description = "Lambda memory in MB — PyArrow benefits from at least 512 MB"
  type        = number
  default     = 1024
}

variable "lambda_log_retention_days" {
  description = "CloudWatch log retention for the Lambda log group"
  type        = number
  default     = 14
}
