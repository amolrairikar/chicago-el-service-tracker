# Account ID is fetched dynamically and can be referenced elsewhere as
# data.aws_caller_identity.current.account_id.
data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

# AWS-managed "CachingOptimized" CloudFront cache policy used by the gold-layer
# distribution. Default TTL 1 day, max 1 year, and honors origin Cache-Control
# headers. A long TTL is safe here because the gold-writer invalidates the
# distribution on each refresh (see the CloudFront section in main.tf).
data "aws_cloudfront_cache_policy" "caching_optimized" {
  name = "Managed-CachingOptimized"
}

locals {
  account_id = data.aws_caller_identity.current.account_id

  # The state bucket name, derived dynamically from the account ID. Useful for
  # referencing the bucket in other resources; the backend itself is
  # configured at init time (see backend.tf).
  state_bucket_name = "terraform-state-bucket-east-${local.account_id}"
}
