import click

from prefector.blocks.deploy import deploy
from prefector.blocks.list import list_blocks


@click.group(name="blocks")
def blocks_command():
    """List or deploy Prefect blocks"""
    pass


blocks_command.add_command(deploy)
blocks_command.add_command(list_blocks)
