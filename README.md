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
