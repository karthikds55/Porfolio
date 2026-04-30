output "raw_bucket_name" {
  description = "Name of the raw input S3 bucket"
  value       = aws_s3_bucket.raw.bucket
}

output "raw_bucket_arn" {
  description = "ARN of the raw input S3 bucket"
  value       = aws_s3_bucket.raw.arn
}

output "parquet_bucket_name" {
  description = "Name of the Parquet output S3 bucket"
  value       = aws_s3_bucket.parquet.bucket
}

output "parquet_bucket_arn" {
  description = "ARN of the Parquet output S3 bucket"
  value       = aws_s3_bucket.parquet.arn
}

output "lambda_function_name" {
  description = "Name of the S3-to-Parquet Lambda function"
  value       = aws_lambda_function.s3_to_parquet.function_name
}

output "lambda_function_arn" {
  description = "ARN of the S3-to-Parquet Lambda function"
  value       = aws_lambda_function.s3_to_parquet.arn
}

output "lambda_log_group" {
  description = "CloudWatch log group for the Lambda function"
  value       = aws_cloudwatch_log_group.lambda.name
}

output "upload_command" {
  description = "Example AWS CLI command to upload a file and trigger the pipeline"
  value       = "aws s3 cp <your-file> s3://${aws_s3_bucket.raw.bucket}/${var.raw_prefix}"
}
