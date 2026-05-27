import click

from prefector.deployments.deploy import deploy
from prefector.deployments.list import list_deployments
from prefector.deployments.run import run_flow


@click.group(name="deployments")
def deployments_command():
    """List or deploy Prefect deployments"""
    pass


deployments_command.add_command(deploy)
deployments_command.add_command(run_flow)
deployments_command.add_command(list_deployments)
