# API key for the CTA Train Tracker API. Marked sensitive so Terraform never
# prints it. Supply the value out-of-band via a gitignored terraform.tfvars
# file (see infrastructure/README.md) or the TF_VAR_cta_api_key environment
# variable; it is injected into the Lambda as the CTA_API_KEY env var.
variable "cta_api_key" {
  type        = string
  sensitive   = true
  description = "CTA Train Tracker API key, injected into the Lambda as CTA_API_KEY."
}

# ARN of the AWS-managed "AWS SDK for pandas" Lambda layer (pyarrow + pandas),
# attached to the silver-transform Lambda instead of vendoring pyarrow into its
# deployment zip. The ARN is region/account/architecture/runtime-specific: this
# default targets us-east-1, x86_64, Python 3.13. Bump the trailing version
# number as AWS publishes newer layer builds (see
# https://aws-sdk-pandas.readthedocs.io/en/stable/layers.html).
variable "aws_sdk_pandas_layer_arn" {
  type        = string
  description = "ARN of the AWS-managed AWS SDK for pandas Lambda layer (pyarrow/pandas) for the silver-transform Lambda; region/arch/runtime-specific (us-east-1, x86_64, py3.13)."
  default     = "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python313:14"
}
