from collections.abc import Iterable

import click
from rich.console import Console

from prefector.blocks.base import BlockSpec, load_blocks
from prefector.blocks.options import BlockOptions, block_options
from prefector.errors import handle_errors

CONSOLE = Console()


def _print_block_header(spec: BlockSpec) -> None:
    CONSOLE.print("[blue]──[/blue]")
    CONSOLE.print(f"Block: [bold]{spec.name}[/bold]")
    CONSOLE.print(f"[dim]Type: [/dim] {spec.block_cls.__name__}")


def print_blocks(specs: Iterable[BlockSpec]) -> None:
    for spec in specs:
        _print_block_header(spec)


@click.command(name="list")
@block_options
def list_blocks(block_opts: BlockOptions):
    """List Prefect blocks"""
    with handle_errors():
        blocks = load_blocks(block_opts.blocks_dir)
        print_blocks(blocks)
