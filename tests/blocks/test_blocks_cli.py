import pytest
from prefect.blocks.core import Block
from pydantic_settings import BaseSettings

from prefector.blocks import cli
from prefector.blocks.base import BlockSpec


class DummySettings(BaseSettings):
    value: int = 1


class DummyBlock(Block):
    value: int


def _spec(name: str) -> BlockSpec:
    return BlockSpec(name=name, settings_cls=DummySettings, block_cls=DummyBlock)


def test_select_targets_returns_all_when_empty():
    specs = [_spec("a"), _spec("b")]
    assert cli.select_targets([], specs) == specs


def test_select_targets_filters_requested_names():
    specs = [_spec("a"), _spec("b")]
    selected = cli.select_targets(["b"], specs)
    assert [spec.name for spec in selected] == ["b"]


def test_select_targets_rejects_unknown_name():
    specs = [_spec("a")]
    with pytest.raises(ValueError, match="Unknown block name\\(s\\): missing"):
        cli.select_targets(["missing"], specs)


def test_print_blocks_uses_block_header(monkeypatch):
    from io import StringIO

    from rich.console import Console

    import prefector.blocks.cli as cli_module

    buf = StringIO()
    monkeypatch.setattr(cli_module, "CONSOLE", Console(file=buf, highlight=False))
    cli_module.print_blocks([_spec("a")])
    out = buf.getvalue()
    assert "a" in out
    assert "DummyBlock" in out
