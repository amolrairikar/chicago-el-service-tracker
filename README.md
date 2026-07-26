# chicago-el-service-tracker

Dashboard to track service metrics for Chicago's "El" trains.

## About

This project fetches live train location data from the CTA Train Tracker API (`lapi.transitchicago.com/api/1.0/ttpositions.aspx`) and surfaces it through a React dashboard, giving users an at-a-glance view of CTA "El" service metrics.

Data flows through a medallion architecture in S3. A per-minute Lambda streams raw API responses via Kinesis Firehose to the **bronze layer** (the append-only `raw/` prefix). A second Lambda runs daily to build the **silver layer** (`silver/date=YYYY-MM-DD/<route>.parquet`): it reads the prior day's bronze partition, explodes each response into one row per train observation, drops exact duplicates, and writes typed Parquet registered in the Glue Data Catalog for Athena. See [`infrastructure/README.md`](infrastructure/README.md) for the full pipeline.

## Development

Development tooling is managed with [pipenv](https://pipenv.pypa.io/). After cloning the repo, install the dev dependencies and register the git hooks:

```sh
pipenv install --dev
pipenv run pre-commit install
```

[pre-commit](https://pre-commit.com/) runs the following checks automatically on `git commit`:

- [ShellCheck](https://www.shellcheck.net/) lints shell scripts.
- [Ruff](https://docs.astral.sh/ruff/) lints Python (autofixing what it can) and formats it. Configuration lives in `ruff.toml` (line length 100).
- [Bandit](https://bandit.readthedocs.io/) is a static analysis (SAST) tool that scans Python source for common security issues. Tests are excluded since their asserts are expected.
- [Gitleaks](https://github.com/gitleaks/gitleaks) scans changes for committed secrets such as API keys and tokens.

To run every check against all files on demand:

```sh
pipenv run pre-commit run --all-files
```

### Tests

Lambda code is unit tested with [pytest](https://docs.pytest.org/). The suite runs with branch coverage and fails below 100% coverage of the Lambda source (see `pytest.ini`):

```sh
pipenv run pytest
```
