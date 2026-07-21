# ===========================================================================
# S3: raw data bucket (component = data-storage)
# ===========================================================================
# S3 bucket that stores the batched raw train-position data delivered by
# Firehose under the raw/ prefix. Named dynamically from the account ID to
# keep it globally unique without hardcoding an account.
resource "aws_s3_bucket" "data" {
  bucket = "cta-train-tracker-prod-bucket-east-${local.account_id}"

  tags = {
    component = "data-storage"
  }
}

# Keep historical versions so an accidental overwrite or delete is recoverable.
resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Encrypt objects at rest with S3-managed keys (SSE-S3), matching the posture
# of the encrypted Terraform state bucket.
resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# This bucket is private; block every avenue of public access.
resource "aws_s3_bucket_public_access_block" "data" {
  bucket = aws_s3_bucket.data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ===========================================================================
# Firehose: delivery stream + IAM role (component = data-processing)
# ===========================================================================
# Kinesis Data Firehose delivery stream that buffers the raw train-position
# records the Lambda sends and flushes them to S3 in batches, plus the IAM
# role and log group the stream needs.

# CloudWatch log group/stream Firehose writes delivery errors to.
resource "aws_cloudwatch_log_group" "firehose" {
  name              = "/aws/kinesisfirehose/chicago-el-train-locations"
  retention_in_days = 14

  tags = {
    component = "data-processing"
  }
}

resource "aws_cloudwatch_log_stream" "firehose_s3_delivery" {
  name           = "S3Delivery"
  log_group_name = aws_cloudwatch_log_group.firehose.name
}

# Trust policy allowing the Firehose service to assume the delivery role.
data "aws_iam_policy_document" "firehose_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "firehose" {
  name               = "chicago-el-firehose-role"
  assume_role_policy = data.aws_iam_policy_document.firehose_assume_role.json

  tags = {
    component = "data-processing"
  }
}

# Permissions the stream needs to write objects to the data bucket and to log
# delivery errors.
data "aws_iam_policy_document" "firehose" {
  # Bucket-level actions act on the bucket ARN itself.
  statement {
    sid    = "S3Bucket"
    effect = "Allow"
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
    ]
    resources = [aws_s3_bucket.data.arn]
  }

  # Object-level actions act on objects within the bucket.
  statement {
    sid    = "S3Objects"
    effect = "Allow"
    actions = [
      "s3:AbortMultipartUpload",
      "s3:GetObject",
      "s3:PutObject",
    ]
    resources = ["${aws_s3_bucket.data.arn}/*"]
  }

  statement {
    sid       = "CloudWatchLogs"
    effect    = "Allow"
    actions   = ["logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.firehose.arn}:*"]
  }
}

resource "aws_iam_role_policy" "firehose" {
  name   = "chicago-el-firehose-policy"
  role   = aws_iam_role.firehose.id
  policy = data.aws_iam_policy_document.firehose.json
}

resource "aws_kinesis_firehose_delivery_stream" "train_locations" {
  name        = "chicago-el-train-locations"
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose.arn
    bucket_arn = aws_s3_bucket.data.arn

    # Land batched data under raw/; delivery failures go under errors/.
    prefix              = "raw/"
    error_output_prefix = "errors/"

    # Flush every 15 minutes. The size cap is set high so that for this
    # low-volume stream the time interval is what triggers each flush.
    buffering_interval = 900
    buffering_size     = 128
    compression_format = "GZIP"

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose.name
      log_stream_name = aws_cloudwatch_log_stream.firehose_s3_delivery.name
    }
  }

  tags = {
    component = "data-processing"
  }
}

# ===========================================================================
# Lambda + EventBridge schedule (component = data-fetch)
# ===========================================================================
# The train-locations Lambda, its IAM role, log group, and the EventBridge
# schedule that invokes it every minute.

locals {
  lambda_function_name = "chicago-el-train-locations"
  lambda_zip           = "${path.module}/../build/lambdas/train_locations.zip"
}

# Trust policy allowing the Lambda service to assume the execution role.
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

resource "aws_iam_role" "lambda" {
  name               = "chicago-el-train-locations-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json

  tags = {
    component = "data-fetch"
  }
}

# CloudWatch Logs write access for the function.
resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Allow the function to stream records to the Firehose delivery stream.
data "aws_iam_policy_document" "lambda_firehose" {
  statement {
    effect = "Allow"
    actions = [
      "firehose:PutRecord",
      "firehose:PutRecordBatch",
    ]
    resources = [aws_kinesis_firehose_delivery_stream.train_locations.arn]
  }
}

resource "aws_iam_role_policy" "lambda_firehose" {
  name   = "chicago-el-train-locations-firehose"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda_firehose.json
}

# Declare the log group explicitly so its retention is managed (Lambda would
# otherwise create it lazily with never-expire retention).
resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${local.lambda_function_name}"
  retention_in_days = 14

  tags = {
    component = "data-fetch"
  }
}

resource "aws_lambda_function" "train_locations" {
  function_name = local.lambda_function_name
  role          = aws_iam_role.lambda.arn

  # Runtime targets must match scripts/package_lambdas.sh (x86_64 / py3.13).
  runtime       = "python3.13"
  architectures = ["x86_64"]
  handler       = "train_locations.handler"

  filename         = local.lambda_zip
  source_code_hash = filebase64sha256(local.lambda_zip)

  timeout     = 30
  memory_size = 256

  environment {
    variables = {
      CTA_API_KEY     = var.cta_api_key
      FIREHOSE_STREAM = aws_kinesis_firehose_delivery_stream.train_locations.name
    }
  }

  tags = {
    component = "data-fetch"
  }

  # Ensure the log group's retention is in place before the function can log.
  depends_on = [
    aws_cloudwatch_log_group.lambda,
    aws_iam_role_policy_attachment.lambda_basic_execution,
  ]
}

# Invoke the Lambda once a minute via EventBridge.
resource "aws_cloudwatch_event_rule" "train_locations_schedule" {
  name                = "chicago-el-train-locations-schedule"
  description         = "Invoke the train-locations Lambda every minute."
  schedule_expression = "rate(1 minute)"

  tags = {
    component = "data-fetch"
  }
}

resource "aws_cloudwatch_event_target" "train_locations" {
  rule      = aws_cloudwatch_event_rule.train_locations_schedule.name
  target_id = "train-locations-lambda"
  arn       = aws_lambda_function.train_locations.arn
}

resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.train_locations.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.train_locations_schedule.arn
}
