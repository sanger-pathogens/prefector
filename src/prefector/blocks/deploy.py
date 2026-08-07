import click
from prefect.settings import temporary_settings
from rich.console import Console

from prefector.blocks.base import BlockSpec, load_blocks, select_targets
from prefector.blocks.options import BlockOptions, block_options
from prefector.errors import handle_errors
from prefector.prefect_connection.connection import generate_prefect_settings
from prefector.prefect_connection.options import PrefectConnectionArgs, prefect_connection_options

CONSOLE = Console()


def _print_block_header(spec: BlockSpec) -> None:
    CONSOLE.print("[blue]──[/blue]")
    CONSOLE.print(f"Block: [bold]{spec.name}[/bold]")
    CONSOLE.print(f"[dim]Type: [/dim] {spec.block_cls.__name__}")


def deploy_block(spec: BlockSpec) -> None:
    _print_block_header(spec)
    CONSOLE.print("[1/2] Preparing block")
    block = spec.build()
    CONSOLE.print("[2/2] Saving block")
    block.save(spec.name, overwrite=True)
    CONSOLE.print("[green][✓][/green] Done")


@click.command()
@prefect_connection_options
@block_options
def deploy(connection: PrefectConnectionArgs, block_opts: BlockOptions):
    """Deploy Prefect blocks"""
    with handle_errors():
        blocks_to_deploy = load_blocks(block_opts.blocks_dir)
        prefect_settings = generate_prefect_settings(connection)
        targets = select_targets(block_opts.target, blocks_to_deploy)

        with temporary_settings(updates=prefect_settings):
            for index, target in enumerate(targets):
                deploy_block(target)
                if index < len(targets) - 1:
                    CONSOLE.print()
