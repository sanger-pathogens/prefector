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
    assert cli._select_targets([], specs) == specs


def test_select_targets_filters_requested_names():
    specs = [_spec("a"), _spec("b")]
    selected = cli._select_targets(["b"], specs)
    assert [spec.name for spec in selected] == ["b"]


def test_select_targets_rejects_unknown_name():
    specs = [_spec("a")]
    with pytest.raises(ValueError, match="Unknown block name\\(s\\): missing"):
        cli._select_targets(["missing"], specs)


def test_print_blocks_uses_block_header(capsys):
    cli._print_blocks([_spec("a")])
    out = capsys.readouterr().out

    assert "Block: a" in out
    assert "Type:  DummyBlock" in out
