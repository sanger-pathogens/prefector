from pathlib import Path

import click
from prefect.settings import temporary_settings
from rich.console import Console

from prefector.blocks.base import BlockSpec, load_blocks, select_targets
from prefector.blocks.options import BlockOptions, block_options
from prefector.blocks.sources import BlockSourcesConfig, build_block_from_source, load_block_sources, source_step_label
from prefector.errors import handle_errors
from prefector.prefect_connection.connection import generate_prefect_settings
from prefector.prefect_connection.options import PrefectConnectionArgs, prefect_connection_options

CONSOLE = Console()


def _print_block_header(spec: BlockSpec) -> None:
    CONSOLE.print("[blue]──[/blue]")
    CONSOLE.print(f"Block: [bold]{spec.name}[/bold]")
    CONSOLE.print(f"[dim]Type: [/dim] {spec.block_cls.__name__}")


def _resolve_sources(block_opts: BlockOptions) -> tuple[BlockSourcesConfig | None, Path | None]:
    path = block_opts.sources or block_opts.blocks_dir / "block-sources.yaml"
    if path and path.is_file():
        return load_block_sources(path), path
    return None, None


def deploy_block(
    spec: BlockSpec,
    sources: BlockSourcesConfig | None = None,
    sources_path: Path | None = None,
) -> None:
    _print_block_header(spec)
    source_entry = sources.root.get(spec.name) if sources is not None else None

    label = source_step_label(source_entry, sources_path) if source_entry is not None else "Reading from environment"
    CONSOLE.print(f"[1/3] {label}")
    CONSOLE.print("[2/3] Preparing block")
    if source_entry is not None:
        block = build_block_from_source(spec.name, spec.block_cls, source_entry, sources_path)
    else:
        block = spec.build()
    CONSOLE.print("[3/3] Saving block")
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
        sources, sources_path = _resolve_sources(block_opts)

        targets = select_targets(block_opts.target, blocks_to_deploy)

        with temporary_settings(updates=prefect_settings):
            for index, target in enumerate(targets):
                deploy_block(target, sources=sources, sources_path=sources_path)
                if index < len(targets) - 1:
                    CONSOLE.print()
