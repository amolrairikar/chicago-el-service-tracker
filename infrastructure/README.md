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

## Pipeline

This configuration provisions the raw train-position ingestion pipeline:

- **Lambda** (`chicago-el-train-locations`) runs the `train_locations` source. An
  **EventBridge** rule invokes it every minute (`rate(1 minute)`).
- The Lambda streams each route's raw JSON to a **Kinesis Data Firehose**
  delivery stream, which buffers for **15 minutes** (900s) and flushes batched,
  GZIP-compressed objects to S3.
- Batched data lands in the **S3 bucket**
  `cta-train-tracker-prod-bucket-east-<account_id>` under the `raw/` prefix
  (delivery failures under `errors/`).

The Lambda deployment artifact is built by `scripts/package_lambdas.sh` into
`build/lambdas/train_locations.zip`; `scripts/deploy_infra.sh` runs the
packaging step before Terraform so the zip exists when `apply` reads it. The
Lambda runtime (`python3.13` / `x86_64`) must stay in sync with that script's
packaging targets.

## Variables

| Variable      | Description                                                          |
| ------------- | ------------------------------------------------------------------- |
| `cta_api_key` | CTA Train Tracker API key, injected into the Lambda as `CTA_API_KEY`. Sensitive. |

`scripts/deploy_infra.sh` runs `plan`/`apply` non-interactively, so the value
must be supplied without a prompt. Either export it as an environment variable:

```sh
export TF_VAR_cta_api_key="your-cta-api-key"
```

or create `infrastructure/terraform.tfvars` (gitignored, so the secret is never
committed):

```hcl
cta_api_key = "your-cta-api-key"
```
