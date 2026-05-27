import time
from typing import Any

import anyio
import click
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import DeploymentFilter, DeploymentFilterName, DeploymentFilterTags
from prefect.exceptions import ObjectNotFound
from prefect.settings import temporary_settings
from rich.console import Console

from prefector.errors import handle_errors
from prefector.prefect_connection.connection import generate_prefect_settings
from prefector.prefect_connection.options import PrefectConnectionArgs, prefect_connection_options

CONSOLE = Console()


def _prefect(coro_fn):
    async def _run():
        async with get_client() as client:
            return await coro_fn(client)

    return anyio.run(_run)


def _deployment_label(d) -> str:
    flow = getattr(d, "flow_name", None)
    return f"{flow}/{d.name}" if flow else d.name


async def _find_deployments(client, name: str) -> list:
    if "/" in name:
        try:
            return [await client.read_deployment_by_name(name)]
        except ObjectNotFound:
            raise ValueError(f"Deployment '{name}' not found") from None
    matches = await client.read_deployments(deployment_filter=DeploymentFilter(name=DeploymentFilterName(any_=[name])))
    if not matches:
        all_deployments = await client.read_deployments()
        available = ", ".join(f"'{_deployment_label(d)}'" for d in all_deployments) or "none"
        raise ValueError(f"No deployment named '{name}'. Available: {available}")
    return matches


async def _find_deployments_by_tags(client, tags: list[str]):
    matches = await client.read_deployments(deployment_filter=DeploymentFilter(tags=DeploymentFilterTags(all_=tags)))
    if not matches:
        raise ValueError(f"No deployments found with tags: {', '.join(tags)}")
    return matches


def _watch_flow_runs(labeled_runs: list[tuple[str, Any]]) -> None:
    pending = {fr.id: (label, fr) for label, fr in labeled_runs}
    failed = []

    while pending:
        time.sleep(5)
        for run_id in list(pending):
            label, _ = pending[run_id]
            fr = _prefect(lambda c, rid=run_id: c.read_flow_run(rid))
            if fr.state is not None and fr.state.is_final():
                del pending[run_id]
                color = "green" if fr.state.is_completed() else "red"
                CONSOLE.print(f"[{color}]{label}: {fr.state.name}[/{color}]")
                if not fr.state.is_completed():
                    failed.append(label)

    if failed:
        raise click.ClickException(f"Flow run(s) did not complete: {', '.join(failed)}")


@click.command(name="run")
@prefect_connection_options
@click.argument("deployment_name", required=False, default=None)
@click.option("--tag", "tags", multiple=True, help="Run all deployments with this tag (repeatable).")
@click.option("--watch", is_flag=True, default=False, help="Wait for all flow runs to complete.")
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
                deployments = _prefect(lambda c: _find_deployments(c, deployment_name))
            else:
                deployments = _prefect(lambda c: _find_deployments_by_tags(c, list(tags)))

            ui_base = connection.api_url.removesuffix("/api")
            labeled_runs = []
            for deployment in deployments:
                label = _deployment_label(deployment)
                CONSOLE.print(f"Triggering [bold]{label}[/bold]")
                flow_run = _prefect(lambda c, did=deployment.id: c.create_flow_run_from_deployment(did))
                CONSOLE.print(f"[green][✓][/green] {ui_base}/runs/flow-run/{flow_run.id}")
                labeled_runs.append((label, flow_run))

            if watch:
                _watch_flow_runs(labeled_runs)
