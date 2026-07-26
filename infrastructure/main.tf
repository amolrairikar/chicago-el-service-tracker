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

# ⚠️ TEMPORARY — TESTING PHASE ONLY. Expire objects under raw/ (bronze) and
# silver/ after 5 days to keep storage/cost near zero while the pipeline is
# under test. REMOVE (or raise the retention on) this resource before any
# real/production use, or historical bronze and silver data is silently lost
# after 5 days. Note this also expires EXISTING raw data from the running fetch
# Lambda, not just new silver output.
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

  # Sweep the delete markers the two expiration rules leave behind once every
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
# the silver/ output. Partition projection means the writer never touches the
# Glue catalog, so no Glue permissions are needed here.
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
# Glue Data Catalog: silver database + table (component = data-catalog)
# ===========================================================================
# Registers the silver layer so it is queryable in Athena and provides a schema
# contract for the future gold (calculated-metrics) layer. Partition projection
# keeps new daily partitions queryable the moment their Parquet lands — no
# crawler, no MSCK REPAIR, and no Glue permissions for the writer.

# Shared database: silver lives here now; the future gold table will too.
resource "aws_glue_catalog_database" "cta" {
  name        = "cta_train_tracker"
  description = "CTA 'L' train tracker data catalog: curated silver (and future gold) layers."
}

resource "aws_glue_catalog_table" "silver_train_locations" {
  name          = "silver_train_locations"
  database_name = aws_glue_catalog_database.cta.name
  table_type    = "EXTERNAL_TABLE"
  description   = "Curated per-train-observation silver layer for CTA 'L' live positions, deduplicated daily from the raw/bronze feed; partitioned by service date."

  # Athena surfaces these column comments and the table description via
  # DESCRIBE and information_schema, which is the metadata a text-to-SQL / AI
  # query assistant reads to choose columns and joins. Keep them accurate.
  parameters = {
    "classification"                = "parquet"
    "projection.enabled"            = "true"
    "projection.date.type"          = "date"
    "projection.date.format"        = "yyyy-MM-dd"
    "projection.date.range"         = "2026-01-01,NOW"
    "projection.date.interval"      = "1"
    "projection.date.interval.unit" = "DAYS"
    "storage.location.template"     = "s3://${aws_s3_bucket.data.bucket}/silver/date=$${date}"
  }

  partition_keys {
    name    = "date"
    type    = "string"
    comment = "Service date of the observation (YYYY-MM-DD, UTC), from the Hive date= prefix; use this to prune scans."
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data.bucket}/silver/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "request_timestamp"
      type    = "string"
      comment = "Time the CTA API generated this position snapshot (America/Chicago local time, no zone offset)."
    }
    columns {
      name    = "route"
      type    = "string"
      comment = "CTA 'L' line code for this train: one of red, blue, brn, g, org, p, pink, y."
    }
    columns {
      name    = "run_number"
      type    = "string"
      comment = "Run number = a vehicle paired with an operator, not a single trip; one run makes several trips (distinct trip_ids) per shift. Unique only within CTA's scheduling day (~03:00-03:00 local), so it can recur across the UTC date partition near the 3am cutover."
    }
    columns {
      name    = "destination_station"
      type    = "string"
      comment = "Station ID (staId, 4xxxx) of the train's final destination."
    }
    columns {
      name    = "destination_station_name"
      type    = "string"
      comment = "Human-readable destination name shown on the train (e.g. O'Hare, 95th/Dan Ryan)."
    }
    columns {
      name    = "train_direction"
      type    = "string"
      comment = "Service direction code (1 or 5); the two running directions of the route. Not a compass bearing (see heading)."
    }
    columns {
      name    = "next_station_id"
      type    = "string"
      comment = "Station ID (staId, 4xxxx) of the next station the train will reach; join key to a stations reference."
    }
    columns {
      name    = "next_stop_id"
      type    = "string"
      comment = "Stop ID (stpId, 3xxxx) of the next stop -- a direction-specific platform within next_station_id."
    }
    columns {
      name    = "next_station_name"
      type    = "string"
      comment = "Human-readable name of the next station (e.g. Irving Park)."
    }
    columns {
      name    = "prediction_time"
      type    = "string"
      comment = "Time this specific arrival prediction was generated (Chicago local); generally differs from the snapshot time (request_timestamp)."
    }
    columns {
      name    = "predicted_arrival_time"
      type    = "string"
      comment = "Predicted arrival time at next_station_id (Chicago local). Seconds-to-arrival = predicted_arrival_time - prediction_time."
    }
    columns {
      name    = "is_approaching"
      type    = "boolean"
      comment = "true when the train is within approach distance of next_station_id (about to arrive)."
    }
    columns {
      name    = "is_delayed"
      type    = "boolean"
      comment = "true when the train has not moved for several minutes and CTA flags it 'Delayed'."
    }
    columns {
      name    = "flags"
      type    = "string"
      comment = "Reserved CTA status flags; currently unused and almost always null."
    }
    columns {
      name    = "latitude"
      type    = "double"
      comment = "Train's current GPS latitude (WGS84 decimal degrees). Null if CTA omitted a position."
    }
    columns {
      name    = "longitude"
      type    = "double"
      comment = "Train's current GPS longitude (WGS84 decimal degrees). Null if CTA omitted a position."
    }
    columns {
      name    = "heading"
      type    = "smallint"
      comment = "Compass bearing of travel in degrees, 0-359 (0=N, 90=E, 180=S, 270=W). Distinct from train_direction."
    }
  }
}
