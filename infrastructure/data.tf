# Account ID is fetched dynamically and can be referenced elsewhere as
# data.aws_caller_identity.current.account_id.
data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id

  # The state bucket name, derived dynamically from the account ID. Useful for
  # referencing the bucket in other resources; the backend itself is
  # configured at init time (see backend.tf).
  state_bucket_name = "terraform-state-bucket-east-${local.account_id}"
}
