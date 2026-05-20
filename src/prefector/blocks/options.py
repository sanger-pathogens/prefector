import functools
from dataclasses import dataclass
from pathlib import Path

from click_option_group import optgroup


@dataclass
class BlockOptions:
    blocks_dir: Path


_OPTION_KEYS = {f.name for f in BlockOptions.__dataclass_fields__.values()}


def block_options(f):
    @optgroup.group("Blocks")
    @optgroup.option("--blocks-dir", type=Path, default=Path.cwd(), help="Directory containing blocks.")
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        block_opts = BlockOptions(**{k: kwargs.pop(k) for k in _OPTION_KEYS})
        return f(*args, block_opts=block_opts, **kwargs)

    return wrapper
