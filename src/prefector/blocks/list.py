from collections.abc import Iterable

import click

from prefector.blocks.base import BlockSpec, load_blocks, print_block_header
from prefector.blocks.options import BlockOptions, block_options
from prefector.errors import handle_errors


def print_blocks(specs: Iterable[BlockSpec]) -> None:
    for spec in specs:
        print_block_header(spec)


@click.command(name="list")
@block_options
def list_blocks(block_opts: BlockOptions):
    """List Prefect blocks"""
    with handle_errors():
        blocks = load_blocks(block_opts.blocks_dir)
        print_blocks(blocks)
