from collections.abc import Iterable

import click
from rich.console import Console

from prefector.deployments.base import DeploymentSpec, load_deployments
from prefector.deployments.options import DeploymentOptions, deployment_options
from prefector.errors import handle_errors

CONSOLE = Console()


def _print_deployment_header(spec: DeploymentSpec) -> None:
    CONSOLE.print("[blue]──[/blue]")
    CONSOLE.print(f"Deployment: [bold]{spec.name}[/bold]")
    CONSOLE.print(f"[dim]Flow:[/dim] {spec.function}")
    CONSOLE.print(f"[dim]Image:[/dim] {spec.image_key}")


def print_deployments(deployments: Iterable[DeploymentSpec]) -> None:
    for deployment in deployments:
        _print_deployment_header(deployment)


@click.command(name="list")
@deployment_options
def list_deployments(deployment_opts: DeploymentOptions):
    """List Prefect deployments"""
    with handle_errors():
        deployments = load_deployments(deployment_opts.deployments_dir)
        print_deployments(deployments)
