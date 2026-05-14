# prefector

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

prefector deployments \
  list \
  --deployments-dir path/to/deployment/specs

prefector deployments \
  deploy \
  --deployments-dir path/to/deployment/specs \
  --images-manifest path/to/images.yaml \
  --api-url "$PREFECT_API_URL" \
  --work-pool default \
  --image-prefix ghcr.io/example
```

Block spec modules must expose `BLOCKS: list[prefector.blocks.base.BlockSpec]`.
Deployment specs are YAML files loaded as `prefector.deployments.base.DeploymentSpec`.

## Development

Setup local environment

```bash
python3 -m venv venv
source venv/bin/activate
python -m pip install --upgrade pip
pip install -e ".[dev]"
```

Run tests with:
```bash
pytest
```

With coverage:

```bash
pytest --cov=src/prefector
```


Run linter with:
```bash
ruff check .
```

Apply auto-fixes where available:

```bash
ruff check . --fix
```
