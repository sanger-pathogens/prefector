# Prefector

[![Tests](https://github.com/sanger-pathogens/prefector/actions/workflows/test.yml/badge.svg)](https://github.com/sanger-pathogens/prefector/actions/workflows/test.yml)

Reusable CLI helpers for deploying Prefect blocks and deployments from downstream
project specs. Provides a CI-first approach to managing Prefect resources as code, stored alongside
flows and data pipelines.

For more detailed documentation, visit the [project wiki](https://github.com/sanger-pathogens/prefector/wiki)

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
`pydantic_settings.BaseSettings` subclass with a Prefect `Block` subclass.

Block classes are source-agnostic: the same `TrinoBlock` can be sourced from
environment variables in one project and from Keeper Secrets Manager in
another — the spec file decides, not the block class. `prefector` provides two
factories that build a `BaseSettings` subclass directly from a block's own
fields, so you don't hand-write one per block:

```python
# blocks/trino.py
from prefect_sqlalchemy import DatabaseCredentials, SyncDriver
from prefector import env_settings_model_for_block
from prefector.blocks.base import BlockSpec

class TrinoBlock(DatabaseCredentials):
    ...

BLOCKS = [
    BlockSpec(
        name="trino-credentials",
        block_cls=TrinoBlock,
        settings_cls=env_settings_model_for_block(TrinoBlock, env_prefix="TRINO_"),
    ),
]
```

When `prefector blocks deploy` runs, it instantiates the settings class and
passes the resolved values to the block.

### Sourcing from the environment

`env_settings_model_for_block(block_cls, *, env_prefix="", field_types=None, field_aliases=None)`
builds a settings class that reads each field from `<env_prefix><FIELD_NAME>`:

```python
settings_cls = env_settings_model_for_block(TrinoBlock, env_prefix="TRINO_")
# reads TRINO_USER, TRINO_PASSWORD, TRINO_HOST, TRINO_PORT
```

If a required env var is missing, `prefector blocks deploy` exits with a clear
error naming the variable that needs to be set.

To read a field from a specific full env var name instead (bypassing
`env_prefix` for just that field), use `field_aliases` — this works for
third-party blocks (e.g. `prefect_aws.AwsCredentials`) without subclassing them:

```python
settings_cls = env_settings_model_for_block(
    AwsCredentials,
    field_aliases={"aws_access_key_id": "AWS_ACCESS_KEY_ID_OVERRIDE"},
)
```

A dotted key (`"<field>.<subfield>"`) populates one sub-field of a nested model
field instead — for example Prefect's built-in
`AwsCredentials.aws_client_parameters.endpoint_url` — since a plain field name
can never contain a `.`, this is unambiguous and can be mixed freely with flat
renames in the same dict:

```python
settings_cls = env_settings_model_for_block(
    AwsCredentials,
    env_prefix="AWS_",
    field_aliases={"aws_client_parameters.endpoint_url": "AWS_HOSTNAME"},
)
```

Only the mapped sub-fields are set; any sub-field of `aws_client_parameters`
not listed keeps its own default.

### Sourcing from Keeper Secrets Manager

`keeper_settings_model_for_block(block_cls, *, record_title, record_prefix="", record_suffix="", separator=":", ksm_token=None, field_types=None, field_aliases=None)`
builds a settings class that reads each field from a Keeper record instead:

```python
from prefector import keeper_settings_model_for_block

settings_cls = keeper_settings_model_for_block(
    TrinoBlock,
    record_title="trino-credentials",
    record_prefix="dlh",
    record_suffix="prod",
)
```

The full record title is assembled as
`<record_prefix><separator><record_title><separator><record_suffix>`, with any
absent components skipped cleanly (no leading or trailing separator).

#### Providing the Keeper token

A token is required to connect to Keeper. It's resolved in this order:

1. The `ksm_token` argument, if given explicitly.
2. The `KSM_CONFIG` environment variable, if `ksm_token` is omitted.

If neither is set, an error is raised.

```python
# Explicit token
settings_cls = keeper_settings_model_for_block(
    TrinoBlock, record_title="trino-credentials", ksm_token=os.environ["MY_KEEPER_TOKEN"],
)

# Or omit ksm_token and set KSM_CONFIG in the environment instead (e.g. in CI)
settings_cls = keeper_settings_model_for_block(TrinoBlock, record_title="trino-credentials")
```

Fields are matched to the record by field name, checking standard fields
(matched by type) then custom fields (matched by label). To read from a
differently-named record field, use `field_aliases` — no block subclass
required, so it works for third-party blocks like `prefect_aws.AwsCredentials`:

```python
settings_cls = keeper_settings_model_for_block(
    AwsCredentials,
    record_title="aws-credentials",
    field_aliases={"aws_access_key_id": "access_key", "aws_secret_access_key": "secret_key"},
)
```

If you own the block class, giving the field a pydantic `validation_alias`
directly works too — `field_aliases` overrides it if both are set:

```python
from pydantic import Field

class TrinoBlock(DatabaseCredentials):
    user: str = Field(validation_alias="login")
```

`KeeperSettingsSource` only matches top-level fields by name/alias — it can't
reach into a nested model's sub-fields on its own. A dotted `field_aliases` key
(`"<field>.<subfield>"`) reaches one sub-field instead — since a plain field
name can never contain a `.`, this is unambiguous and can be mixed freely with
flat renames in the same dict:

```python
settings_cls = keeper_settings_model_for_block(
    AwsCredentials,
    record_title="aws-credentials",
    field_aliases={"aws_client_parameters.endpoint_url": "hostname"},
)
```

Only the mapped sub-field is set from the record; every other sub-field of
`aws_client_parameters` keeps its own default.

The Keeper SDK (`keeper-secrets-manager-core`) must be installed to use this
source. The extra `prefector[keeper]` provides it.

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
concurrency_limit: 1                # max concurrent flow runs for this deployment
collision_strategy: CANCEL_NEW      # ENQUEUE (default) or CANCEL_NEW
version: "1.2.0"                    # free-form version label, shown in the Prefect UI
description: "Loads records into the bronze layer"
```

`collision_strategy` requires `concurrency_limit` to be set, and controls what
happens to a new run submitted while the limit is already reached: `ENQUEUE`
(default) waits for a free slot, `CANCEL_NEW` cancels the incoming run
immediately.

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
