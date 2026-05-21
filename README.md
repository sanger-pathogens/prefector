# Prefector

[![Tests](https://github.com/sanger-pathogens/prefector/actions/workflows/test.yml/badge.svg)](https://github.com/sanger-pathogens/prefector/actions/workflows/test.yml)

Reusable CLI helpers for deploying Prefect blocks and deployments from downstream
project specs.

## Install

Install `prefector` into the same Python environment as the block specs, flow
modules, and Prefect collection packages it needs to import.

```bash
pip install prefector
```

## Usage

```bash
prefector blocks list --blocks-dir path/to/block/specs
prefector blocks deploy --blocks-dir path/to/block/specs --api-url "$PREFECT_API_URL"

prefector deployments list --deployments-dir path/to/deployment/specs

prefector deployments deploy \
  --deployments-dir path/to/deployment/specs \
  --images-manifest path/to/images.yaml \
  --api-url "$PREFECT_API_URL" \
  --work-pool default \
  --image-prefix ghcr.io/example
```

Block spec modules must expose `BLOCKS: list[prefector.BlockSpec]`.
Deployment specs are YAML files loaded as `prefector.DeploymentSpec`.

## Deployment spec

Each deployment is a YAML file. All fields except `name`, `flow`, and `image_key` are optional.

```yaml
name: my_deployment
flow: flows.my_module:my_flow        # <module>:<function> format
image_key: flow_runtime              # key from images manifest

cron: "0 6 * * *"                   # standard cron expression
tags:
  - project_name
  - bronze
parameters:
  retries: 3
  bucket:
    block: my-s3-bucket              # load a Prefect block by name at run time
env:
  ENVIRONMENT: ${ENVIRONMENT}        # resolved from the environment at deploy time
  LOG_LEVEL: INFO
```

### Environment variable substitution

Values in the form `${VAR_NAME}` are replaced with the corresponding environment
variable when the spec is loaded. This happens at deploy time (e.g. in CI), not
at flow run time.

```yaml
env:
  COMMIT_SHA: ${CI_COMMIT_SHORT_SHA}
  PROJECT: ${PROJECT_NAME}
```

All referenced variables must be set when `prefector deployments deploy` runs, or
the command will exit with an error naming the missing variable.

**Using environment variables in the deployment spec:**

- Only `${VAR}` brace syntax is supported. A bare `$VAR` is left as-is.
- Substitution happens on the raw text before YAML parsing. If a variable value
  contains YAML special characters (`:`, `{`, `}`, `#`), it can produce invalid
  YAML. Quote the value to be safe:
  ```yaml
  env:
    LABEL: "${MY_LABEL}"
  ```
- Resolved values are stored in Prefect as `job_variables` and are visible in the
  Prefect UI. Avoid substituting secrets this way; use Prefect blocks instead.
- Environment variables are resolved only for deployments that are actually being
  deployed. Untargeted deployments (filtered by `--target`) and the `list`
  command do not require any variables to be set.


## Development

Setup local environment

Install project dependencies:

```bash
poetry env use 3.12
source .venv/bin/activate
poetry install --with dev
```

Set up pre-commit hooks and linting:

```bash
pre-commit install
```

This will run pre-commit hooks on every commit. To run pre-commit manually, use

```bash
pre commit run -a
```

Run tests with:

```bash
pytest
```

With coverage:

```bash
pytest --cov=src/prefector
```
