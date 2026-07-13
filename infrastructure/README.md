# Infrastructure

Terraform configuration for the Chicago El Service Tracker.

## State

Remote state lives in an S3 bucket named
`terraform-state-bucket-east-<account_id>` under the key
`chicago-el-tracker/terraform.tfstate` in `us-east-1`.

Because Terraform's backend block cannot reference variables or data sources,
the bucket name (which contains the account ID) is supplied at init time via
partial configuration:

```sh
terraform init \
  -backend-config="bucket=terraform-state-bucket-east-$(aws sts get-caller-identity --query Account --output text)"
```

After the first init, Terraform caches the backend config in
`.terraform/`, so subsequent commands (`plan`, `apply`) need no extra flags.
