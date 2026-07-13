# S3 backend for remote Terraform state.
#
# NOTE: Terraform evaluates the backend block before the resource graph is
# built, so it cannot reference variables or data sources (e.g. the account
# ID). The bucket name is therefore supplied at init time via partial
# configuration. See infrastructure/README.md for the exact command.
terraform {
  backend "s3" {
    key     = "chicago-el-tracker/terraform.tfstate"
    region  = "us-east-1"
    encrypt = true
  }
}
