# chicago-el-service-tracker

Dashboard to track service metrics for Chicago's "El" trains.

## About

This project fetches live train location data from the CTA Train Tracker API (`lapi.transitchicago.com/api/1.0/ttpositions.aspx`) and surfaces it through a React dashboard, giving users an at-a-glance view of CTA "El" service metrics.

## Development

Development tooling is managed with [pipenv](https://pipenv.pypa.io/). After cloning the repo, install the dev dependencies and register the git hooks:

```sh
pipenv install --dev
pipenv run pre-commit install
```

[pre-commit](https://pre-commit.com/) runs [ShellCheck](https://www.shellcheck.net/) against shell scripts automatically on `git commit`. To lint every file on demand:

```sh
pipenv run pre-commit run --all-files
```
