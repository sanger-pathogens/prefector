import functools
from dataclasses import dataclass, field
from pathlib import Path

import click
from click_option_group import optgroup


@dataclass
class BlockOptions:
    blocks_dir: Path
    target: tuple[str]
    sources: Path | None = field(default=None)


_OPTION_KEYS = {f.name for f in BlockOptions.__dataclass_fields__.values()}


def block_options(f):
    @optgroup.group("Blocks")
    @optgroup.option(
        "--blocks-dir",
        type=click.Path(exists=True, file_okay=False, path_type=Path),
        default=".",
        help="Directory containing blocks.",
    )
    @optgroup.option("--target", help="Target blocks to list/deploy", multiple=True)
    @optgroup.option(
        "--sources",
        type=click.Path(exists=True, dir_okay=False, path_type=Path),
        default=None,
        help=("Path to block-sources.yaml. If omitted, falls back to block-sources.yaml in --blocks-dir if present."),
    )
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        block_opts = BlockOptions(**{k: kwargs.pop(k) for k in _OPTION_KEYS})
        return f(*args, block_opts=block_opts, **kwargs)

    return wrapper
