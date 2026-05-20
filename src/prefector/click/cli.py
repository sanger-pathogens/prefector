import click

from prefector.click.blocks.cli import blocks
from prefector.click.deployments.cli import deployments


@click.group()
def cli():
    """Manage Prefect resources from reusable specs."""


cli.add_command(blocks)
cli.add_command(deployments)

if __name__ == "__main__":
    cli()
