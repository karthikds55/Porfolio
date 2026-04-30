data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda_exec" {
  name               = "${var.project_name}-s3-to-parquet-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = local.common_tags
}

# Allow Lambda to write CloudWatch logs
resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "lambda_s3" {
  statement {
    sid     = "ReadRawBucket"
    effect  = "Allow"
    actions = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.raw.arn}/*"]
  }

  statement {
    sid     = "WriteParquetBucket"
    effect  = "Allow"
    actions = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.parquet.arn}/*"]
  }
}

resource "aws_iam_policy" "lambda_s3" {
  name   = "${var.project_name}-lambda-s3-policy"
  policy = data.aws_iam_policy_document.lambda_s3.json
  tags   = local.common_tags
}

resource "aws_iam_role_policy_attachment" "lambda_s3" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.lambda_s3.arn
}
