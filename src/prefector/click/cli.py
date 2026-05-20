import click

from prefector.click.blocks.cli import blocks_command
from prefector.click.deployments.cli import deployments_command


@click.group()
def cli():
    """Manage Prefect resources from reusable specs."""


cli.add_command(blocks_command)
cli.add_command(deployments_command)

if __name__ == "__main__":
    cli()
