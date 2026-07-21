# API key for the CTA Train Tracker API. Marked sensitive so Terraform never
# prints it. Supply the value out-of-band via a gitignored terraform.tfvars
# file (see infrastructure/README.md) or the TF_VAR_cta_api_key environment
# variable; it is injected into the Lambda as the CTA_API_KEY env var.
variable "cta_api_key" {
  type        = string
  sensitive   = true
  description = "CTA Train Tracker API key, injected into the Lambda as CTA_API_KEY."
}
