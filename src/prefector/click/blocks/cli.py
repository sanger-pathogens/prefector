import click

from prefector.click.blocks.options import BlockOptions, block_options
from prefector.click.prefect.options import PrefectConnectionArgs, prefect_connection_options


@click.group()
def blocks():
    """List or deploy Prefect blocks"""
    pass


@blocks.command()
@prefect_connection_options
@block_options
def deploy(connection: PrefectConnectionArgs, block_opts: BlockOptions):
    """Deploy Prefect blocks"""
    click.echo("Deploying blocks")


# noinspection PyShadowingBuiltins
@blocks.command()
@prefect_connection_options
@block_options
def list(connection: PrefectConnectionArgs, block_opts: BlockOptions):
    """List Prefect blocks"""
    click.echo("Listing blocks")
