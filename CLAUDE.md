# CLAUDE.md

Guidance for Claude Code when working in this repository.

## Development workflow

- When a new pre-commit check is added to `.pre-commit-config.yaml`, update the **Development** section of `README.md` to document the new check (what it does and, if relevant, how to run it).
- Keep the package lists in `Pipfile` (`[packages]` and `[dev-packages]`) alphabetized when adding or removing dependencies.

## Lambda functions

- Each Lambda lives in its own directory under `src/lambdas/<name>/` and MUST include a `requirements.txt` listing its third-party Python dependencies. If a Lambda needs nothing beyond the standard library and the packages the Lambda runtime already provides (e.g. `boto3`), leave `requirements.txt` empty rather than omitting it. `scripts/package_lambdas.sh` installs these dependencies into the deployment artifact and treats a missing `requirements.txt` as an error.
- The packaging script installs dependencies for the Lambda runtime's platform (Linux) rather than the machine building the artifact, so packages with native C extensions get compatible wheels. The target architecture and Python version default to `x86_64` / `3.13` and are overridable via the `LAMBDA_ARCH` and `LAMBDA_PYTHON_VERSION` environment variables; keep these in sync with the deployed function's `architecture` and `runtime`.
