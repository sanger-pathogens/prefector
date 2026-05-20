import click

from prefector.click.deployments.options import DeploymentOptions, deployment_options
from prefector.click.prefect.options import PrefectConnectionArgs, prefect_connection_options


@click.group()
def deployments():
    """List or deploy Prefect deployments"""
    pass


@deployments.command()
@prefect_connection_options
@deployment_options
def deploy(connection: PrefectConnectionArgs, deployment_opts: DeploymentOptions):
    """Deploy Prefect deployments"""
    click.echo("Deploying deployments")


# noinspection PyShadowingBuiltins
@deployments.command()
@prefect_connection_options
@deployment_options
def list(connection: PrefectConnectionArgs, deployment_opts: DeploymentOptions):
    """List Prefect deployments"""
    click.echo("Listing deployments")
