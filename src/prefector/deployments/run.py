from uuid import UUID

import anyio
import click
from prefect.client.orchestration import PrefectClient, get_client
from prefect.client.schemas.filters import (
    DeploymentFilter,
    DeploymentFilterName,
    DeploymentFilterTags,
    FlowFilter,
    FlowFilterId,
    FlowRunFilter,
    FlowRunFilterId,
)
from prefect.client.schemas.objects import FlowRun
from prefect.client.schemas.responses import DeploymentResponse
from prefect.exceptions import ObjectNotFound
from prefect.settings import temporary_settings
from rich.console import Console

from prefector.deployments.options import RunOptions, run_options
from prefector.errors import handle_errors
from prefector.prefect_connection.connection import generate_prefect_settings
from prefector.prefect_connection.options import PrefectConnectionArgs, prefect_connection_options

CONSOLE = Console(soft_wrap=True)


async def _flow_names_by_id(client: PrefectClient, deployments: list[DeploymentResponse]) -> dict[UUID, str]:
    flow_ids = {d.flow_id for d in deployments}
    flows = await client.read_flows(flow_filter=FlowFilter(id=FlowFilterId(any_=list(flow_ids))))
    return {flow.id: flow.name for flow in flows}


def _deployment_label(flow_name: str, d: DeploymentResponse) -> str:
    return f"{flow_name}/{d.name}"


async def _find_deployments(client: PrefectClient, name: str) -> list[DeploymentResponse]:
    if "/" in name:
        try:
            return [await client.read_deployment_by_name(name)]
        except ObjectNotFound:
            raise ValueError(f"Deployment '{name}' not found") from None
    matches = await client.read_deployments(deployment_filter=DeploymentFilter(name=DeploymentFilterName(any_=[name])))
    if not matches:
        all_deployments = await client.read_deployments()
        flow_names = await _flow_names_by_id(client, all_deployments)
        available = ", ".join(f"'{_deployment_label(flow_names[d.flow_id], d)}'" for d in all_deployments) or "none"
        raise ValueError(f"No deployment named '{name}'. Available: {available}")
    return matches


async def _find_deployments_by_tags(client: PrefectClient, tags: list[str]) -> list[DeploymentResponse]:
    matches = await client.read_deployments(deployment_filter=DeploymentFilter(tags=DeploymentFilterTags(all_=tags)))
    if not matches:
        raise ValueError(f"No deployments found with tags: {', '.join(tags)}")
    return matches


async def _watch_flow_runs(client: PrefectClient, labeled_runs: list[tuple[str, FlowRun]]) -> None:
    pending = {fr.id: (label, fr) for label, fr in labeled_runs}
    failed = []

    while pending:
        await anyio.sleep(5)
        current_runs = await client.read_flow_runs(
            flow_run_filter=FlowRunFilter(id=FlowRunFilterId(any_=list(pending)))
        )
        for fr in current_runs:
            if fr.state is not None and fr.state.is_final():
                label, _ = pending.pop(fr.id)
                color = "green" if fr.state.is_completed() else "red"
                CONSOLE.print(f"[{color}]{label}: {fr.state.name}[/{color}]")
                if not fr.state.is_completed():
                    failed.append(label)

    if failed:
        raise click.ClickException(f"Flow run(s) did not complete: {', '.join(failed)}")


async def _run_flow_async(
    connection: PrefectConnectionArgs, deployment_name: str | None, tags: tuple[str, ...], watch: bool
) -> None:
    async with get_client() as client:
        if deployment_name:
            deployments = await _find_deployments(client, deployment_name)
        else:
            deployments = await _find_deployments_by_tags(client, list(tags))

        flow_names = await _flow_names_by_id(client, deployments)

        ui_base = connection.api_url.removesuffix("/api")
        labeled_runs: list[tuple[str, FlowRun]] = []
        for deployment in deployments:
            label = _deployment_label(flow_names[deployment.flow_id], deployment)
            CONSOLE.print(f"Triggering [bold]{label}[/bold]")
            flow_run = await client.create_flow_run_from_deployment(deployment.id)
            CONSOLE.print(f"[green][✓][/green] {ui_base}/runs/flow-run/{flow_run.id}")
            labeled_runs.append((label, flow_run))

        if watch:
            await _watch_flow_runs(client, labeled_runs)


@click.command(name="run")
@prefect_connection_options
@run_options
def run_flow(connection: PrefectConnectionArgs, run_opts: RunOptions):
    """Trigger one or more Prefect deployments by name or tag"""
    if not run_opts.deployment_name and not run_opts.tags:
        raise click.UsageError("Provide a deployment name or --tag.")
    if run_opts.deployment_name and run_opts.tags:
        raise click.UsageError("Provide a deployment name or --tag, not both.")

    with handle_errors():
        prefect_settings = generate_prefect_settings(connection)

        with temporary_settings(updates=prefect_settings):
            anyio.run(_run_flow_async, connection, run_opts.deployment_name, run_opts.tags, run_opts.watch)
