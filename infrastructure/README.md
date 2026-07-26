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

This configuration provisions a two-stage medallion pipeline over CTA train
positions.

### Ingestion — bronze layer

- **Lambda** (`chicago-el-train-locations`) runs the `train_locations` source. An
  **EventBridge** rule invokes it every minute (`rate(1 minute)`).
- The Lambda streams each route's raw JSON to a **Kinesis Data Firehose**
  delivery stream, which buffers for **15 minutes** (900s) and flushes batched,
  GZIP-compressed objects to S3.
- Batched data lands in the **S3 bucket**
  `cta-train-tracker-prod-bucket-east-<account_id>` under the `raw/` prefix — the
  **bronze layer** (append-only, as-delivered by Firehose; delivery failures go
  under `errors/`).

### Transform — silver layer

- **Lambda** (`chicago-el-silver-layer`) runs the
  `silver_layer` source. An **EventBridge** rule invokes it once a day
  at **06:00 UTC** (`cron(0 6 * * ? *)`), after the prior day's final Firehose
  buffer has flushed.
- It reads the previous UTC day's `raw/` (bronze) partition, explodes the nested
  `ctatt.route[].train[]` arrays into one row per train observation, drops exact
  duplicates (SHA-256 of the raw train dict plus route), and writes typed,
  date-partitioned Parquet to the **silver layer** under
  `silver/date=YYYY-MM-DD/<route>.parquet`. Re-running a date overwrites its
  output, so the transform is idempotent. `pyarrow`/`pandas` come from the
  AWS-managed **AWS SDK for pandas** Lambda layer (see the
  `aws_sdk_pandas_layer_arn` variable) rather than the deployment zip.
- The silver table is registered in the **Glue Data Catalog** database
  `cta_train_tracker` (table `silver_train_locations`) with **partition
  projection** on `date`, so new daily partitions are queryable in Athena the
  moment their Parquet lands — no crawler or `MSCK REPAIR`.

> ⚠️ **Temporary testing-phase guardrail:** an S3 lifecycle configuration on the
> data bucket **expires objects under `raw/` and `silver/` after 5 days** (and
> sweeps their noncurrent versions and delete markers) to keep storage cost near
> zero during testing. Remove or raise this retention before any production use,
> or historical bronze and silver data is silently lost after 5 days.

Both Lambda deployment artifacts are built by `scripts/package_lambdas.sh` into
`build/lambdas/*.zip`; `scripts/deploy_infra.sh` runs the packaging step before
Terraform so the zips exist when `apply` reads them. The Lambda runtime
(`python3.13` / `x86_64`) must stay in sync with that script's packaging
targets.

## Variables

| Variable      | Description                                                          |
| ------------- | ------------------------------------------------------------------- |
| `cta_api_key` | CTA Train Tracker API key, injected into the Lambda as `CTA_API_KEY`. Sensitive. |
| `aws_sdk_pandas_layer_arn` | ARN of the AWS-managed AWS SDK for pandas Lambda layer (pyarrow/pandas) attached to the silver-transform Lambda. Region/arch/runtime-specific; defaults to a pinned us-east-1 / x86_64 / py3.13 version. |

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
