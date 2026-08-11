import functools
from dataclasses import dataclass
from pathlib import Path

import click
from click_option_group import optgroup


@dataclass
class BlockOptions:
    blocks_dir: Path
    target: tuple[str, ...]


_OPTION_KEYS = {f.name for f in BlockOptions.__dataclass_fields__.values()}


def block_options(f):
    @optgroup.group("Blocks")
    @optgroup.option(
        "--blocks-dir",
        type=click.Path(exists=True, file_okay=False, path_type=Path),
        help="Directory containing blocks.",
        required=True,
    )
    @optgroup.option("--target", help="Target blocks to list/deploy", multiple=True)
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        block_opts = BlockOptions(**{k: kwargs.pop(k) for k in _OPTION_KEYS})
        return f(*args, block_opts=block_opts, **kwargs)

    return wrapper
