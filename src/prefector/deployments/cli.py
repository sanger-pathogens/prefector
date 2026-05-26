import contextlib
import importlib
import io
import logging
import time
from typing import Any, Iterable

import anyio
import click
from click import UsageError
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import DeploymentFilter, DeploymentFilterName, DeploymentFilterTags
from prefect.exceptions import ObjectNotFound, ParameterTypeError
from prefect.settings import temporary_settings
from prefect.types.entrypoint import EntrypointType
from rich.console import Console

from prefector.deployments.base import DeploymentSpec, _resolve_env_dict, load_deployments, load_image_manifest
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


def _print_deployment_header(spec: DeploymentSpec) -> None:
    CONSOLE.print("[blue]──[/blue]")
    CONSOLE.print(f"Deployment: [bold]{spec.name}[/bold]")
    CONSOLE.print(f"[dim]Flow:[/dim] {spec.function}")
    CONSOLE.print(f"[dim]Image:[/dim] {spec.image_key}")


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
    _print_deployment_header(spec)
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

    if dry_run:
        CONSOLE.print("[green][✓][/green] Done (dry run)")
        return

    CONSOLE.print("[3/3] Creating deployment")
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        deployment_id = flow_obj.deploy(**kwargs)

    CONSOLE.print(f"[green][✓][/green] Done (ID: {deployment_id})")


def print_deployments(deployments: Iterable[DeploymentSpec]) -> None:
    for deployment in deployments:
        _print_deployment_header(deployment)


@click.group(name="deployments")
def deployments_command():
    """List or deploy Prefect deployments"""
    pass


@deployments_command.command()
@prefect_connection_options
@deployment_options
@deployment_deploy_options
def deploy(
    connection: PrefectConnectionArgs,
    deployment_opts: DeploymentOptions,
    deployment_deploy_opts: DeploymentDeployOptions,
):
    """Deploy Prefect deployments"""
    deployments = load_deployments(deployment_opts.deployments_dir)
    images = load_image_manifest(deployment_deploy_opts.images_manifest)
    prefect_settings = generate_prefect_settings(connection)
    targets = _select_targets(deployment_deploy_opts.target, deployments)

    with handle_errors():
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


def _prefect(coro_fn):
    async def _run():
        async with get_client() as client:
            return await coro_fn(client)

    return anyio.run(_run)


def _deployment_label(d) -> str:
    flow = getattr(d, "flow_name", None)
    return f"{flow}/{d.name}" if flow else d.name


async def _find_deployment(client, name: str):
    if "/" in name:
        try:
            return await client.read_deployment_by_name(name)
        except ObjectNotFound:
            raise ValueError(f"Deployment '{name}' not found") from None
    matches = await client.read_deployments(deployment_filter=DeploymentFilter(name=DeploymentFilterName(any_=[name])))
    if not matches:
        all_deployments = await client.read_deployments()
        available = ", ".join(f"'{_deployment_label(d)}'" for d in all_deployments) or "none"
        raise ValueError(f"No deployment named '{name}'. Available: {available}")
    if len(matches) > 1:
        options = ", ".join(f"'{_deployment_label(m)}'" for m in matches)
        raise ValueError(f"Multiple deployments named '{name}': {options}. Use flow/deployment format.")
    return matches[0]


async def _find_deployments_by_tags(client, tags: list[str]):
    matches = await client.read_deployments(deployment_filter=DeploymentFilter(tags=DeploymentFilterTags(all_=tags)))
    if not matches:
        raise ValueError(f"No deployments found with tags: {', '.join(tags)}")
    return matches


def _run_deployment(deployment, watch: bool) -> None:
    flow_run = _prefect(lambda c: c.create_flow_run_from_deployment(deployment.id))
    CONSOLE.print(f"[green][✓][/green] Flow run created (ID: {flow_run.id})")

    if watch:
        while flow_run.state is None or not flow_run.state.is_final():
            time.sleep(5)
            flow_run = _prefect(lambda c, run_id=flow_run.id: c.read_flow_run(run_id))
        state = flow_run.state
        color = "green" if state.is_completed() else "red"
        CONSOLE.print(f"Final state: [{color}]{state.name}[/{color}]")
        if not state.is_completed():
            raise click.ClickException(f"Flow run ended in state: {state.name}")


@deployments_command.command(name="run")
@prefect_connection_options
@click.argument("deployment_name", required=False, default=None)
@click.option("--tag", "tags", multiple=True, help="Run all deployments with this tag (repeatable).")
@click.option("--watch", is_flag=True, default=False, help="Wait for each flow run to complete.")
def run_flow(connection: PrefectConnectionArgs, deployment_name: str | None, tags: tuple[str, ...], watch: bool):
    """Trigger one or more Prefect deployments by name or tag"""
    if not deployment_name and not tags:
        raise click.UsageError("Provide a deployment name or --tag.")
    if deployment_name and tags:
        raise click.UsageError("Provide a deployment name or --tag, not both.")

    prefect_settings = generate_prefect_settings(connection)

    with handle_errors():
        with temporary_settings(updates=prefect_settings):
            if deployment_name:
                deployments = [_prefect(lambda c: _find_deployment(c, deployment_name))]
            else:
                deployments = _prefect(lambda c: _find_deployments_by_tags(c, list(tags)))

            for index, deployment in enumerate(deployments):
                CONSOLE.print(f"Triggering deployment [bold]{_deployment_label(deployment)}[/bold]")
                _run_deployment(deployment, watch)
                if index < len(deployments) - 1:
                    CONSOLE.print()


@deployments_command.command(name="list")
@deployment_options
def list_deployments(deployment_opts: DeploymentOptions):
    """List Prefect deployments"""
    with handle_errors():
        deployments = load_deployments(deployment_opts.deployments_dir)
        print_deployments(deployments)
