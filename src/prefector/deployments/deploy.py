import contextlib
import importlib
import io
import logging
from collections.abc import Iterable
from typing import Any

import click
from click import UsageError
from prefect.client.schemas.objects import ConcurrencyLimitConfig
from prefect.exceptions import ParameterTypeError
from prefect.settings import temporary_settings
from prefect.types.entrypoint import EntrypointType
from rich.console import Console

from prefector.deployments.base import (
    DeploymentSpec,
    _resolve_env_dict,
    load_deployments,
    load_image_manifest,
    print_deployment_header,
)
from prefector.deployments.options import (
    DeploymentDeployOptions,
    DeploymentOptions,
    deployment_deploy_options,
    deployment_options,
)
from prefector.errors import handle_errors
from prefector.prefect_connection.connection import generate_prefect_settings
from prefector.prefect_connection.options import PrefectConnectionArgs, prefect_connection_options

CONSOLE = Console()


def _select_targets(selected: Iterable[str], deployments: list[DeploymentSpec]) -> list[DeploymentSpec]:
    if not selected:
        return deployments

    selected = set(selected)
    index = {deployment.name: deployment for deployment in deployments if deployment.name in selected}

    missing = sorted(selected - set(index))
    if missing:
        available = ", ".join(sorted(deployment.name for deployment in deployments))
        raise UsageError(f"Unknown deployment name(s): {', '.join(missing)}. Available: {available}")

    return list(index.values())


def _resolve_flow(spec: DeploymentSpec):
    module = importlib.import_module(spec.module)
    return getattr(module, spec.function)


def _validate_parameters(flow_obj: Any, spec: DeploymentSpec) -> None:
    logger = logging.getLogger("prefect.flows")
    original_level = logger.level
    logger.setLevel(logging.CRITICAL)
    try:
        flow_obj.validate_parameters(spec.parameters)
    except ParameterTypeError as exc:
        details = str(exc).replace("Flow run received invalid parameters:", "").strip()
        raise ValueError(f"Invalid parameters for deployment '{spec.name}':\n{details}") from None
    finally:
        logger.setLevel(original_level)


def _build_image_name(*, image_prefix: str, image_name: str, image_tag: str) -> str:
    prefix = image_prefix.rstrip("/")
    base_name = f"{prefix}/{image_name}" if prefix else image_name

    has_tag = ":" in base_name.rsplit("/", maxsplit=1)[-1]
    if has_tag:
        return base_name
    return f"{base_name}:{image_tag}" if image_tag else base_name


def deploy_target(  # noqa: PLR0913
    spec: DeploymentSpec,
    *,
    work_pool_name: str,
    work_queue_name: str | None,
    image: str,
    dry_run: bool,
):
    print_deployment_header(spec)
    CONSOLE.print("[1/3] Preparing deployment")

    flow_obj = _resolve_flow(spec)

    CONSOLE.print("[2/3] Validating parameters")
    _validate_parameters(flow_obj, spec)

    kwargs: dict[str, Any] = {
        "name": spec.name,
        "work_pool_name": work_pool_name,
        "image": image,
        "build": False,
        "push": False,
        "print_next_steps": False,
        "job_variables": {},
        "entrypoint_type": EntrypointType.MODULE_PATH,
    }

    if work_queue_name is not None:
        kwargs["work_queue_name"] = work_queue_name
    if spec.tags:
        kwargs["tags"] = spec.tags
    if spec.parameters:
        kwargs["parameters"] = spec.parameters
    if spec.env:
        kwargs["job_variables"]["env"] = _resolve_env_dict(spec.env, spec.name)
    if spec.cron:
        kwargs["cron"] = spec.cron
    if spec.concurrency_limit is not None:
        concurrency_kwargs: dict[str, Any] = {"limit": spec.concurrency_limit}
        if spec.collision_strategy:
            concurrency_kwargs["collision_strategy"] = spec.collision_strategy
        kwargs["concurrency_limit"] = ConcurrencyLimitConfig(**concurrency_kwargs)
    if dry_run:
        CONSOLE.print("[green][✓][/green] Done (dry run)")
        return

    CONSOLE.print("[3/3] Creating deployment")
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        deployment_id = flow_obj.deploy(**kwargs)

    CONSOLE.print(f"[green][✓][/green] Done (ID: {deployment_id})")


@click.command()
@prefect_connection_options
@deployment_options
@deployment_deploy_options
def deploy(
    connection: PrefectConnectionArgs,
    deployment_opts: DeploymentOptions,
    deployment_deploy_opts: DeploymentDeployOptions,
):
    """Deploy Prefect deployments"""
    with handle_errors():
        deployments = load_deployments(deployment_opts.deployments_dir)
        images = load_image_manifest(deployment_deploy_opts.images_manifest)
        prefect_settings = generate_prefect_settings(connection)
        targets = _select_targets(deployment_deploy_opts.target, deployments)

        with temporary_settings(updates=prefect_settings):
            for index, target in enumerate(targets):
                deploy_target(
                    spec=target,
                    work_pool_name=deployment_deploy_opts.work_pool,
                    work_queue_name=deployment_deploy_opts.work_queue,
                    image=_build_image_name(
                        image_prefix=deployment_deploy_opts.image_prefix,
                        image_name=images.get(target.image_key).name,
                        image_tag=deployment_deploy_opts.image_tag,
                    ),
                    dry_run=deployment_deploy_opts.dry_run,
                )
                if index < len(targets) - 1:
                    CONSOLE.print()
