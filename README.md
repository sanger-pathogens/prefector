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

## Block specs

Each block spec is a Python module in the `--blocks-dir` directory. A module
must expose a `BLOCKS` list of `BlockSpec` objects, each pairing a
`pydantic_settings.BaseSettings` subclass (which reads field values from the
environment) with a Prefect `Block` subclass.

```python
# blocks/trino.py
from pydantic_settings import BaseSettings, SettingsConfigDict
from prefect_sqlalchemy import DatabaseCredentials, SyncDriver
from prefector.blocks.base import BlockSpec

class TrinoSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TRINO_")
    user: str
    password: str
    host: str
    port: int = 8080

class TrinoBlock(DatabaseCredentials):
    ...

BLOCKS = [BlockSpec(name="trino-credentials", settings_cls=TrinoSettings, block_cls=TrinoBlock)]
```

When `prefector blocks deploy` runs, it instantiates `TrinoSettings()` — which
reads `TRINO_USER`, `TRINO_PASSWORD`, etc. from the environment — and passes the
values to the block.

## Block sources

A `block-sources.yaml` file lets different teams use the same block spec modules
while sourcing secret values from different backends (environment variables or
Keeper Secrets Manager) and with different field naming conventions.

**It is not required.** Blocks that already define their own `settings_cls` with
the right env prefix will continue to work exactly as before. Only add a
block-sources entry when you need to override where values come from.

### Loading

Provide the file explicitly:

```bash
prefector blocks deploy --blocks-dir path/to/specs --sources path/to/block-sources.yaml
```

Or place it at `block-sources.yaml` inside `--blocks-dir` and it will be picked
up automatically with no extra flags needed.

### File format

Three equivalent YAML shapes are accepted:

**Flat mapping** (simplest):
```yaml
trino-credentials:
  source: env
  env_var_prefix: TRINO_
```

**List** (useful when ordering matters or you prefer the list style):
```yaml
- trino-credentials:
    source: env
    env_var_prefix: TRINO_
- other-block:
    source: keeper
    record_title: trino-credentials
```

**`blocks:` wrapper** (same list, under an explicit key):
```yaml
blocks:
  - trino-credentials:
      source: env
      env_var_prefix: TRINO_
```

### Environment variable source

Reads block field values from environment variables.

```yaml
trino-credentials:
  source: env
  env_var_prefix: TRINO_          # env vars are read as <prefix><field>
  fields:                          # optional: override individual field names
    user: USERNAME                 # reads TRINO_USERNAME into field `user`
    password: PASSWORD             # reads TRINO_PASSWORD into field `password`
    # unlisted fields use the field name as-is: `host` -> TRINO_host
```

The `fields` mapping is optional. Without it, each block field is read from
`<env_var_prefix><field_name>` (case-insensitive). Only add `fields` entries
when the env var suffix differs from the field name.

If a required env var is missing, the command exits with a clear error naming
the variable that needs to be set.

### Keeper Secrets Manager source

Reads block field values from a record in Keeper Secrets Manager.

```yaml
trino-credentials:
  source: keeper
  record_title: trino-credentials  # required: base record title
  record_prefix: dlh               # optional: prepended before title
  record_suffix: ${ENVIRONMENT}    # optional: appended after title
  separator: ":"                   # optional: joins the parts (default: ":"); must be quoted in YAML
  ksm_token: ${KSM_TOKEN}          # optional: one-time token; falls back to KSM_CONFIG env var
  fields:                          # optional: map block field -> KSM field title
    user: login                    # reads KSM field "login" into block field `user`
    # unlisted fields use the field name as-is
```

The full record title is assembled as
`<record_prefix><separator><record_title><separator><record_suffix>`, with any
absent components skipped cleanly (no leading or trailing separator):

The Keeper SDK (`keeper-secrets-manager-core`) must be installed separately:

```bash
pip install keeper-secrets-manager-core
```

### Environment variable substitution

Any string value in `block-sources.yaml` may use `${VAR_NAME}` syntax.
Substitution happens at build time (when `prefector blocks deploy` runs), so
you can parameterise record names, prefixes, or tokens from CI environment
variables:

```yaml
trino-credentials:
  source: keeper
  record_title: trino-credentials
  record_suffix: ${ENVIRONMENT}   # e.g. resolves to "prod" or "staging"
  ksm_token: ${KSM_TOKEN}
```

All referenced variables must be set at deploy time, or the command will exit
with an error naming the missing variable.

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
