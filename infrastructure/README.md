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
  at **01:00 UTC** (`cron(0 1 * * ? *)`), after the prior UTC day's final Firehose
  buffer has flushed. The time is DST-independent — UTC partition completion is
  fixed at 00:00 UTC, so the buffer holds year-round.
- It reads the previous UTC day's `raw/` (bronze) partition, explodes the nested
  `ctatt.route[].train[]` arrays into one row per train observation, drops exact
  duplicates (SHA-256 of the raw train dict plus route), and writes typed,
  date-partitioned Parquet to the **silver layer** under
  `silver/date=YYYY-MM-DD/<route>.parquet`. Re-running a date overwrites its
  output, so the transform is idempotent. `pyarrow`/`pandas` come from the
  AWS-managed **AWS SDK for pandas** Lambda layer (see the
  `aws_sdk_pandas_layer_arn` variable) rather than the deployment zip.
- The silver layer is an internal staging layer: it is read directly from S3 by
  the (future) gold-layer Lambda and is not registered in any query catalog.

> ⚠️ **Temporary testing-phase guardrail:** an S3 lifecycle configuration on the
> data bucket **expires objects under `raw/`, `silver/`, and `gold/` after 5 days**
> (and sweeps their noncurrent versions and delete markers) to keep storage cost
> near zero during testing. Remove or raise this retention before any production
> use, or historical bronze, silver, and gold data is silently lost after 5 days.

### Delivery — gold layer via CloudFront

- A **CloudFront distribution** fronts the data bucket and serves the **gold
  layer** over HTTPS on the default `*.cloudfront.net` domain
  (`terraform output cloudfront_domain_name`). The dashboard — an Observable
  Framework app hosted on **GitHub Pages** (see the repo root `README.md`) —
  fetches gold objects from this domain **cross-origin**. The bucket stays fully
  private — CloudFront reads it through an **Origin Access Control (OAC)**
  identity, not public objects.
- The distribution is **scoped to `gold/` only** two ways: the origin path
  (`/gold`) means a browser request for `/routes.json` resolves to
  `gold/routes.json` and nothing outside the prefix is reachable, and the bucket
  policy grants the OAC `s3:GetObject` on only `gold/*` (conditioned on the
  distribution's `AWS:SourceArn`).
- Because the dashboard is served from a different origin (GitHub Pages), a
  **CORS** response-headers policy lets the browser fetch gold objects. Allowed
  origins come from the `cors_allowed_origins` variable (defaults to `*`); tighten
  it to the Pages origin (`https://amolrairikar.github.io`) once the dashboard
  starts fetching data.
- Caching uses the AWS-managed **CachingOptimized** policy (long TTL). Freshness
  after the ~24h gold refresh is meant to come from a **CloudFront invalidation**
  issued by the (future) gold-writer, which will need
  `cloudfront:CreateInvalidation` scoped to `cloudfront_distribution_arn`.

> **Note:** the `gold/` layer and its writer do not exist yet, and the current
> dashboard is the default Observable Framework scaffold that does not fetch gold
> data yet — this distribution is ready to serve gold objects the moment they
> start landing under `gold/`.

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
| `cors_allowed_origins` | Origins allowed by the gold-layer CloudFront CORS policy (`Access-Control-Allow-Origin`). List of strings; defaults to `["*"]`. |

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
