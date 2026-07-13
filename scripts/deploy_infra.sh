#!/usr/bin/env bash
#
# Runs terraform init, plan, and apply against the infrastructure/ directory.
# The plan is saved to a file and that same file is fed to apply so the applied
# changes exactly match what was reviewed.
set -euo pipefail

# Resolve the infrastructure directory relative to this script so the script
# works regardless of the current working directory.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="${SCRIPT_DIR}/../infrastructure"

PLAN_FILE="tfplan"

cd "${INFRA_DIR}"

# The S3 backend bucket name contains the AWS account ID, which the backend
# block cannot reference directly, so it is supplied at init time via partial
# configuration. See infrastructure/README.md.
terraform init \
  -backend-config="bucket=terraform-state-bucket-east-$(aws sts get-caller-identity --query Account --output text)"

# Save the plan so apply operates on the exact reviewed changes.
terraform plan -out="${PLAN_FILE}"

# Apply the saved plan.
terraform apply "${PLAN_FILE}"
