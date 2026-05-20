import functools
from dataclasses import dataclass
from pathlib import Path

from click_option_group import optgroup


@dataclass
class DeploymentOptions:
    deployments_dir: Path


_OPTION_KEYS = {f.name for f in DeploymentOptions.__dataclass_fields__.values()}


def deployment_options(f):
    @optgroup.group("Deployments")
    @optgroup.option("--deployments-dir", type=Path, default=Path.cwd(), help="Directory containing deployment YAML.")
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        deployment_opts = DeploymentOptions(**{k: kwargs.pop(k) for k in _OPTION_KEYS})
        return f(*args, deployment_opts=deployment_opts, **kwargs)

    return wrapper
