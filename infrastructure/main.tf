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

# ⚠️ TEMPORARY — TESTING PHASE ONLY. Expire objects under raw/ (bronze),
# silver/, and gold/ after 5 days to keep storage/cost near zero while the
# pipeline is under test. REMOVE (or raise the retention on) this resource
# before any real/production use, or historical bronze, silver, and gold data is
# silently lost after 5 days. Note this also expires EXISTING raw data from the
# running fetch Lambda, not just new silver output.
#
# Versioning is enabled on this bucket, so a plain `expiration` only writes a
# delete marker over the current version while the bytes persist (and keep
# costing money) as noncurrent versions. To actually reclaim storage each rule
# also expires noncurrent versions after 1 day, and a final catch-all rule
# sweeps the leftover delete markers.
resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  # Versioning must exist before lifecycle rules that reference noncurrent
  # versions can be evaluated.
  depends_on = [aws_s3_bucket_versioning.data]

  rule {
    id     = "expire-raw"
    status = "Enabled"

    filter {
      prefix = "raw/"
    }

    expiration {
      days = 5
    }

    noncurrent_version_expiration {
      noncurrent_days = 1
    }
  }

  rule {
    id     = "expire-silver"
    status = "Enabled"

    filter {
      prefix = "silver/"
    }

    expiration {
      days = 5
    }

    noncurrent_version_expiration {
      noncurrent_days = 1
    }
  }

  rule {
    id     = "expire-gold"
    status = "Enabled"

    filter {
      prefix = "gold/"
    }

    expiration {
      days = 5
    }

    noncurrent_version_expiration {
      noncurrent_days = 1
    }
  }

  # Sweep the delete markers the expiration rules leave behind once every
  # noncurrent version under them is gone.
  rule {
    id     = "sweep-expired-delete-markers"
    status = "Enabled"

    filter {}

    expiration {
      expired_object_delete_marker = true
    }
  }
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

# ===========================================================================
# Silver transform Lambda + EventBridge schedule (component = data-transform)
# ===========================================================================
# The daily transform Lambda that reads the prior UTC day's raw/ (bronze)
# partition, deduplicates train observations, and writes date-partitioned
# Parquet to the silver/ prefix. Its IAM role, log group, and the EventBridge
# schedule that invokes it once a day.

locals {
  transform_lambda_function_name = "chicago-el-silver-layer"
  transform_lambda_zip           = "${path.module}/../build/lambdas/silver_layer.zip"
}

resource "aws_iam_role" "transform" {
  name               = "${local.transform_lambda_function_name}-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json

  tags = {
    component = "data-transform"
  }
}

# CloudWatch Logs write access for the function.
resource "aws_iam_role_policy_attachment" "transform_basic_execution" {
  role       = aws_iam_role.transform.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# S3 access: list and read the raw/ (bronze) partition it consumes, and write
# the silver/ output. That is all the role needs — no Glue or other permissions.
data "aws_iam_policy_document" "transform_s3" {
  # ListBucket is a bucket-level action; scope it to the raw/ prefix so the
  # role can only enumerate the bronze layer.
  statement {
    sid       = "ListRaw"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.data.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["raw/*"]
    }
  }

  statement {
    sid       = "ReadRaw"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.data.arn}/raw/*"]
  }

  statement {
    sid       = "WriteSilver"
    effect    = "Allow"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.data.arn}/silver/*"]
  }
}

resource "aws_iam_role_policy" "transform_s3" {
  name   = "${local.transform_lambda_function_name}-s3"
  role   = aws_iam_role.transform.id
  policy = data.aws_iam_policy_document.transform_s3.json
}

# Declare the log group explicitly so its retention is managed (Lambda would
# otherwise create it lazily with never-expire retention).
resource "aws_cloudwatch_log_group" "transform" {
  name              = "/aws/lambda/${local.transform_lambda_function_name}"
  retention_in_days = 14

  tags = {
    component = "data-transform"
  }
}

resource "aws_lambda_function" "transform_silver_train_locations" {
  function_name = local.transform_lambda_function_name
  role          = aws_iam_role.transform.arn

  # Runtime targets must match scripts/package_lambdas.sh (x86_64 / py3.13).
  runtime       = "python3.13"
  architectures = ["x86_64"]
  handler       = "silver_layer.handler"

  filename         = local.transform_lambda_zip
  source_code_hash = filebase64sha256(local.transform_lambda_zip)

  # Reads and rewrites a full day of raw objects; give it headroom in both time
  # and memory for the pyarrow Parquet write.
  timeout     = 300
  memory_size = 1024

  # pyarrow/pandas come from the AWS-managed layer rather than the deployment
  # zip (its Linux wheel would push the zip past the direct-upload size limit).
  layers = [var.aws_sdk_pandas_layer_arn]

  environment {
    variables = {
      DATA_BUCKET = aws_s3_bucket.data.bucket
    }
  }

  tags = {
    component = "data-transform"
  }

  # Ensure the log group's retention is in place before the function can log.
  depends_on = [
    aws_cloudwatch_log_group.transform,
    aws_iam_role_policy_attachment.transform_basic_execution,
  ]
}

# Invoke the transform Lambda once a day at 06:00 UTC — after the prior day's
# final Firehose buffer (15 min) has flushed to the raw/ partition.
resource "aws_cloudwatch_event_rule" "transform_schedule" {
  name                = "${local.transform_lambda_function_name}-schedule"
  description         = "Invoke the silver-transform Lambda daily at 06:00 UTC."
  schedule_expression = "cron(0 6 * * ? *)"

  tags = {
    component = "data-transform"
  }
}

resource "aws_cloudwatch_event_target" "transform" {
  rule      = aws_cloudwatch_event_rule.transform_schedule.name
  target_id = "transform-silver-train-locations-lambda"
  arn       = aws_lambda_function.transform_silver_train_locations.arn
}

resource "aws_lambda_permission" "allow_eventbridge_transform" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.transform_silver_train_locations.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.transform_schedule.arn
}

# ===========================================================================
# Gold total_trains Lambda + EventBridge schedule (component = gold)
# ===========================================================================
# The daily gold-writer that reads the silver/ Parquet covering a Central
# service day, counts distinct run numbers per line, and publishes the
# total_trains JSON (daily + weekly/monthly/yearly rollups) to gold/. It then
# invalidates the CloudFront distribution so the dashboard sees the refresh.
# Its IAM role, log group, and the EventBridge schedule that invokes it daily.

locals {
  gold_lambda_function_name = "chicago-el-gold-layer"
  gold_lambda_zip           = "${path.module}/../build/lambdas/gold_layer.zip"
}

resource "aws_iam_role" "gold" {
  name               = "${local.gold_lambda_function_name}-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json

  tags = {
    component = "gold"
  }
}

# CloudWatch Logs write access for the function.
resource "aws_iam_role_policy_attachment" "gold_basic_execution" {
  role       = aws_iam_role.gold.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Access: list/read the silver/ input, read+write the gold/ output (it reads
# back its own daily fact to accumulate history), and invalidate the gold
# distribution's cache after each refresh.
data "aws_iam_policy_document" "gold_s3" {
  # ListBucket is a bucket-level action; scope it to the two prefixes the
  # function enumerates.
  statement {
    sid       = "ListSilverAndGold"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.data.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["silver/*", "gold/*"]
    }
  }

  statement {
    sid       = "ReadSilver"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.data.arn}/silver/*"]
  }

  statement {
    sid       = "ReadWriteGold"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:PutObject"]
    resources = ["${aws_s3_bucket.data.arn}/gold/*"]
  }

  statement {
    sid       = "InvalidateGoldDistribution"
    effect    = "Allow"
    actions   = ["cloudfront:CreateInvalidation"]
    resources = [aws_cloudfront_distribution.gold.arn]
  }
}

resource "aws_iam_role_policy" "gold_s3" {
  name   = "${local.gold_lambda_function_name}-access"
  role   = aws_iam_role.gold.id
  policy = data.aws_iam_policy_document.gold_s3.json
}

# Declare the log group explicitly so its retention is managed (Lambda would
# otherwise create it lazily with never-expire retention).
resource "aws_cloudwatch_log_group" "gold" {
  name              = "/aws/lambda/${local.gold_lambda_function_name}"
  retention_in_days = 14

  tags = {
    component = "gold"
  }
}

resource "aws_lambda_function" "gold_total_trains" {
  function_name = local.gold_lambda_function_name
  role          = aws_iam_role.gold.arn

  # Runtime targets must match scripts/package_lambdas.sh (x86_64 / py3.13).
  runtime       = "python3.13"
  architectures = ["x86_64"]
  handler       = "gold_layer.handler"

  filename         = local.gold_lambda_zip
  source_code_hash = filebase64sha256(local.gold_lambda_zip)

  # Reads up to two full days of silver Parquet; give it headroom in both time
  # and memory for the pyarrow reads.
  timeout     = 300
  memory_size = 1024

  # pyarrow comes from the AWS-managed layer rather than the deployment zip (its
  # Linux wheel would push the zip past the direct-upload size limit).
  layers = [var.aws_sdk_pandas_layer_arn]

  environment {
    variables = {
      DATA_BUCKET                = aws_s3_bucket.data.bucket
      CLOUDFRONT_DISTRIBUTION_ID = aws_cloudfront_distribution.gold.id
    }
  }

  tags = {
    component = "gold"
  }

  # Ensure the log group's retention is in place before the function can log.
  depends_on = [
    aws_cloudwatch_log_group.gold,
    aws_iam_role_policy_attachment.gold_basic_execution,
  ]
}

# Invoke the gold-writer once a day at 07:00 UTC — after the silver transform's
# 06:00 UTC run has produced the partitions a completed service day needs.
resource "aws_cloudwatch_event_rule" "gold_schedule" {
  name                = "${local.gold_lambda_function_name}-schedule"
  description         = "Invoke the gold total_trains Lambda daily at 07:00 UTC."
  schedule_expression = "cron(0 7 * * ? *)"

  tags = {
    component = "gold"
  }
}

resource "aws_cloudwatch_event_target" "gold" {
  rule      = aws_cloudwatch_event_rule.gold_schedule.name
  target_id = "gold-total-trains-lambda"
  arn       = aws_lambda_function.gold_total_trains.arn
}

resource "aws_lambda_permission" "allow_eventbridge_gold" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.gold_total_trains.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.gold_schedule.arn
}

# ===========================================================================
# CloudFront: gold-layer delivery (component = delivery)
# ===========================================================================
# CloudFront distribution that serves the gold/ layer of the data bucket to the
# React dashboard. The distribution is scoped to gold/ two ways: the origin
# path (/gold) means CloudFront can only ever request keys under that prefix,
# and the bucket policy grants read on only gold/*. The bucket stays fully
# private (public-access-block untouched) — CloudFront reads it via an Origin
# Access Control (OAC) identity, not public objects. Uses the default
# *.cloudfront.net domain (no custom domain / ACM cert).

# OAC identity CloudFront uses to sign requests to the private S3 origin.
resource "aws_cloudfront_origin_access_control" "gold" {
  name                              = "chicago-el-train-tracker-oac"
  description                       = "OAC for the gold-layer CloudFront distribution to read the private data bucket."
  origin_access_control_origin_type = "s3"
  signing_behavior                  = "always"
  signing_protocol                  = "sigv4"
}

# CORS headers CloudFront attaches to responses so the browser-based React
# dashboard can fetch gold objects. Allowed origins come from the
# cors_allowed_origins variable (defaults to "*").
resource "aws_cloudfront_response_headers_policy" "gold_cors" {
  name    = "chicago-el-train-tracker-cors"
  comment = "CORS for the gold-layer distribution consumed by the React dashboard."

  cors_config {
    access_control_allow_credentials = false
    origin_override                  = true

    access_control_allow_headers {
      items = ["*"]
    }

    access_control_allow_methods {
      items = ["GET", "HEAD", "OPTIONS"]
    }

    access_control_allow_origins {
      items = var.cors_allowed_origins
    }
  }
}

# The distribution itself. Serves gold/ over HTTPS with the CachingOptimized
# managed cache policy; freshness after a gold refresh comes from an
# invalidation issued by the (future) gold-writer, not a short TTL.
resource "aws_cloudfront_distribution" "gold" {
  enabled = true
  comment = "Serves the gold/ layer of the data bucket to the React dashboard."

  origin {
    domain_name              = aws_s3_bucket.data.bucket_regional_domain_name
    origin_id                = "gold-s3"
    origin_access_control_id = aws_cloudfront_origin_access_control.gold.id
    # Prepended to every request, so the distribution can only reach gold/*.
    origin_path = "/gold"
  }

  default_cache_behavior {
    target_origin_id       = "gold-s3"
    viewer_protocol_policy = "redirect-to-https"
    compress               = true
    # OPTIONS is allowed so CloudFront can answer CORS preflight requests.
    allowed_methods            = ["GET", "HEAD", "OPTIONS"]
    cached_methods             = ["GET", "HEAD"]
    cache_policy_id            = data.aws_cloudfront_cache_policy.caching_optimized.id
    response_headers_policy_id = aws_cloudfront_response_headers_policy.gold_cors.id
  }

  # North America + Europe only — cheapest price class; the audience is local.
  price_class = "PriceClass_100"

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  # Default *.cloudfront.net certificate; no custom domain configured.
  viewer_certificate {
    cloudfront_default_certificate = true
  }

  tags = {
    component = "delivery"
  }
}

# Bucket policy granting the distribution's OAC read access to gold/* only. The
# AWS:SourceArn condition scopes it to this distribution, so S3 does not treat
# the policy as public and it coexists with block_public_policy = true.
data "aws_iam_policy_document" "data_bucket" {
  statement {
    sid       = "AllowCloudFrontGoldRead"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.data.arn}/gold/*"]

    principals {
      type        = "Service"
      identifiers = ["cloudfront.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "AWS:SourceArn"
      values   = [aws_cloudfront_distribution.gold.arn]
    }
  }
}

resource "aws_s3_bucket_policy" "data" {
  bucket = aws_s3_bucket.data.id
  policy = data.aws_iam_policy_document.data_bucket.json
}
