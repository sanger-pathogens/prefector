import hashlib
import importlib.util
import sys
from collections import Counter
from pathlib import Path
from types import ModuleType
from typing import Iterable

import click
from prefect.settings import temporary_settings
from rich.console import Console

from prefector.blocks.base import BlockBuildError, BlockSpec
from prefector.blocks.options import BlockOptions, block_options
from prefector.blocks.sources import BlockSourcesConfig, build_block_from_source, load_block_sources
from prefector.prefect_connection.connection import generate_prefect_settings
from prefector.prefect_connection.options import PrefectConnectionArgs, prefect_connection_options

CONSOLE = Console()


def _print_block_header(spec: BlockSpec) -> None:
    CONSOLE.print("[blue]──[/blue]")
    CONSOLE.print(f"Block: [bold]{spec.name}[/bold]")
    CONSOLE.print(f"[dim]Type: [/dim] {spec.block_cls.__name__}")


def _already_loaded_module(module_name: str, module_path: Path):
    module = sys.modules.get(module_name)
    module_file = getattr(module, "__file__", None) if module is not None else None
    if not module_file:
        return None
    return module if Path(module_file).resolve() == module_path.resolve() else None


def _load_specs_module(module_path: Path):
    resolved_path = module_path.resolve()
    namespace = hashlib.sha256(str(resolved_path.parent).encode("utf-8")).hexdigest()[:12]
    module_name = f"prefector_external_blocks.{namespace}.{module_path.stem}"
    existing = _already_loaded_module(module_name, module_path)
    if existing is not None:
        return existing

    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ValueError(f"Unable to load module from: {module_path}")
    _ensure_synthetic_package("prefector_external_blocks")
    _ensure_synthetic_package(f"prefector_external_blocks.{namespace}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except (ModuleNotFoundError, ImportError) as exc:
        sys.modules.pop(module_name, None)
        raise ValueError(_missing_dependency_message(module_path, exc)) from exc
    return module


def _ensure_synthetic_package(module_name: str) -> None:
    if module_name in sys.modules:
        return

    module = ModuleType(module_name)
    module.__path__ = []
    sys.modules[module_name] = module


def _missing_dependency_message(module_path: Path, exc: ModuleNotFoundError | ImportError) -> str:
    missing_name = getattr(exc, "name", None)
    if not missing_name:
        return f"Block spec '{module_path}' could not be imported: {exc}"

    if missing_name.startswith("prefect_"):
        package_name = missing_name.replace("_", "-")
        return (
            f"Block spec '{module_path}' requires missing Python module '{missing_name}'. "
            f"Install the Prefect collection package: {package_name}"
        )

    return (
        f"Block spec '{module_path}' requires missing Python module '{missing_name}'. "
        "Install it in the current environment and try again."
    )


def _validate_specs(specs: list[BlockSpec]) -> None:
    name_counts = Counter(spec.name for spec in specs)
    duplicates = sorted(name for name, count in name_counts.items() if count > 1)
    if duplicates:
        raise ValueError(f"Duplicate block name(s): {', '.join(duplicates)}")


def load_blocks(spec_dir: Path) -> list[BlockSpec]:
    specs: list[BlockSpec] = []

    for path in sorted(spec_dir.glob("*.py")):
        if path.name == "__init__.py" or path.name.startswith("_"):
            continue

        module = _load_specs_module(path)
        module_specs = getattr(module, "BLOCKS", None)
        if not isinstance(module_specs, list):
            raise ValueError(f"Spec module must expose BLOCKS as list: {path}")
        if not all(isinstance(spec, BlockSpec) for spec in module_specs):
            raise ValueError(f"Spec module has non-BlockSpec entries in BLOCKS: {path}")

        specs.extend(module_specs)

    _validate_specs(specs)
    return specs


def select_targets(selected: Iterable[str], specs: list[BlockSpec]) -> list[BlockSpec]:
    if not selected:
        return specs

    selected = set(selected)
    index = {spec.name: spec for spec in specs if spec.name in selected}
    missing = sorted(selected - set(index))
    if missing:
        available = ", ".join(sorted(spec.name for spec in specs))
        raise ValueError(f"Unknown block name(s): {', '.join(missing)}. Available: {available}")
    return list(index.values())


def print_blocks(specs: Iterable[BlockSpec]) -> None:
    for spec in specs:
        _print_block_header(spec)


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
    CONSOLE.print("[1/2] Preparing block")

    try:
        if sources is not None and spec.name in sources.root:
            block = build_block_from_source(spec.name, spec.block_cls, sources.root[spec.name], sources_path)
        else:
            block = spec.build()
    except (BlockBuildError, ValueError) as e:
        raise SystemExit(str(e)) from None

    CONSOLE.print("[2/2] Saving block")
    block.save(spec.name, overwrite=True)
    CONSOLE.print("[green][✓][/green] Done")


@click.group(name="blocks")
def blocks_command():
    """List or deploy Prefect blocks"""
    pass


@blocks_command.command()
@prefect_connection_options
@block_options
def deploy(connection: PrefectConnectionArgs, block_opts: BlockOptions):
    """Deploy Prefect blocks"""
    blocks_to_deploy = load_blocks(block_opts.blocks_dir)
    prefect_settings = generate_prefect_settings(connection)
    sources, sources_path = _resolve_sources(block_opts)

    targets = select_targets(block_opts.target, blocks_to_deploy)

    with temporary_settings(updates=prefect_settings):
        for index, target in enumerate(targets):
            deploy_block(target, sources=sources, sources_path=sources_path)
            if index < len(targets) - 1:
                CONSOLE.print()


@blocks_command.command(name="list")
@block_options
def list_blocks(block_opts: BlockOptions):
    """List Prefect blocks"""
    blocks = load_blocks(block_opts.blocks_dir)
    print_blocks(blocks)
