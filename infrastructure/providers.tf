terraform {
  required_version = ">= 1.15.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = "us-east-1"

  # Applied to every taggable resource managed by this provider. The per-
  # resource "component" tag is set on each resource individually.
  default_tags {
    tags = {
      project      = "cta-train-tracker"
      environment  = "prod"
      "managed-by" = "terraform"
    }
  }
}
