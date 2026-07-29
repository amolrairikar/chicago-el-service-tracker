# chicago-el-service-tracker

Dashboard to track service metrics for Chicago's "El" trains.

## About

This project fetches live train location data from the CTA Train Tracker API (`lapi.transitchicago.com/api/1.0/ttpositions.aspx`) and surfaces it through a React dashboard, giving users an at-a-glance view of CTA "El" service metrics.

Data flows through a medallion architecture in S3. A per-minute Lambda streams raw API responses via Kinesis Firehose to the **bronze layer** (the append-only `raw/` prefix). A second Lambda runs daily to build the **silver layer** (`silver/date=YYYY-MM-DD/<route>.parquet`): it reads the prior day's bronze partition, explodes each response into one row per train observation, drops exact duplicates, and writes typed Parquet. See [`infrastructure/README.md`](infrastructure/README.md) for the full pipeline.

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

## Frontend

The dashboard is an [Observable Framework](https://observablehq.com/framework/) app in `frontend/` — chosen for its first-class date-range / category inputs and reactive charts, a good fit for filtering El service metrics by date and train line. Install dependencies and run the preview server:

```sh
cd frontend
npm install
npm run dev
```

Build the static site (emitted to `frontend/dist/`):

```sh
npm run build
```

### Deployment

The app is hosted on **GitHub Pages** at
<https://amolrairikar.github.io/chicago-el-service-tracker/>. Pushing to `main`
with changes under `frontend/` triggers the
[`deploy-frontend`](.github/workflows/deploy-frontend.yml) GitHub Actions
workflow, which builds the app and publishes `frontend/dist/` to Pages (no manual
deploy step).

> **One-time setup:** in the repository, go to **Settings → Pages → Build and
> deployment** and set **Source** to **GitHub Actions**, or the workflow's deploy
> step will fail.

Because Pages serves a project site under a subpath, `base` is set to
`/chicago-el-service-tracker/` in `frontend/observablehq.config.js`. When the
dashboard later reads pipeline data, it will fetch it cross-origin from the gold
CloudFront distribution (see [`infrastructure/README.md`](infrastructure/README.md)).
