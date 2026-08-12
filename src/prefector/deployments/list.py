from collections.abc import Iterable

import click

from prefector.deployments.base import DeploymentSpec, load_deployments, print_deployment_header
from prefector.deployments.options import DeploymentOptions, deployment_options
from prefector.errors import handle_errors


def print_deployments(deployments: Iterable[DeploymentSpec]) -> None:
    for deployment in deployments:
        print_deployment_header(deployment)


@click.command(name="list")
@deployment_options
def list_deployments(deployment_opts: DeploymentOptions):
    """List Prefect deployments"""
    with handle_errors():
        deployments = load_deployments(deployment_opts.deployments_dir)
        print_deployments(deployments)
