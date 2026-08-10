import functools
from dataclasses import dataclass
from pathlib import Path

import click
from click_option_group import optgroup


@dataclass
class DeploymentOptions:
    deployments_dir: Path


@dataclass
class DeploymentDeployOptions:
    work_pool: str
    work_queue: str
    target: tuple[str]
    images_manifest: Path
    image_prefix: str
    image_tag: str
    dry_run: bool


_OPTION_KEYS = {f.name for f in DeploymentOptions.__dataclass_fields__.values()}
_DEPLOY_OPTION_KEYS = {f.name for f in DeploymentDeployOptions.__dataclass_fields__.values()}


def deployment_options(f):
    @optgroup.group("Deployments")
    @optgroup.option(
        "--deployments-dir",
        type=click.Path(exists=True, file_okay=False, path_type=Path),
        help="Directory containing deployment YAML.",
        required=True,
    )
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        deployment_opts = DeploymentOptions(**{k: kwargs.pop(k) for k in _OPTION_KEYS})
        return f(*args, deployment_opts=deployment_opts, **kwargs)

    return wrapper


def deployment_deploy_options(f):
    @optgroup("Deploy")
    @optgroup.option("--work-pool", help="Prefect work pool.", default="default", show_default=True)
    @optgroup.option("--work-queue", help="Prefect work queue", default="default", show_default=True)
    @optgroup.option("--target", help="Target flow to deploy. Omit to deploy all flows.", multiple=True)
    @optgroup.option(
        "--images-manifest",
        type=click.Path(path_type=Path, dir_okay=False, exists=True),
        help="Path to images manifest.",
        required=True,
    )
    @optgroup.option("--image-prefix", help="Image prefix/registry, e.g. ghcr.io/org.", required=True)
    @optgroup.option("--image-tag", help="Image tag for deployment.", default="latest", show_default=True)
    @optgroup.option(
        "--dry-run", help="Print only the proposed actions without executing.", is_flag=True, default=False
    )
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        deployment_deploy_opts = DeploymentDeployOptions(**{k: kwargs.pop(k) for k in _DEPLOY_OPTION_KEYS})
        return f(*args, deployment_deploy_opts=deployment_deploy_opts, **kwargs)

    return wrapper
