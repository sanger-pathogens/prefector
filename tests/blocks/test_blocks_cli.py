from io import StringIO

import pytest
from prefect.blocks.core import Block
from pydantic_settings import BaseSettings
from rich.console import Console

import prefector.blocks.list as list_module
from prefector.blocks import base
from prefector.blocks import deploy as deploy_module
from prefector.blocks.base import BlockSpec


class DummySettings(BaseSettings):
    value: int = 1


class DummyBlock(Block):
    value: int


def _spec(name: str) -> BlockSpec:
    return BlockSpec(name=name, settings_cls=DummySettings, block_cls=DummyBlock)


def test_select_targets_returns_all_when_empty():
    specs = [_spec("a"), _spec("b")]
    assert base.select_targets([], specs) == specs


def test_select_targets_filters_requested_names():
    specs = [_spec("a"), _spec("b")]
    selected = base.select_targets(["b"], specs)
    assert [spec.name for spec in selected] == ["b"]


def test_select_targets_rejects_unknown_name():
    specs = [_spec("a")]
    with pytest.raises(ValueError, match="Unknown block name\\(s\\): missing"):
        base.select_targets(["missing"], specs)


def test_deploy_block_builds_from_spec_settings(monkeypatch):
    monkeypatch.setenv("VALUE", "42")
    spec = _spec("my-block")
    # Patch block.save to avoid hitting Prefect API
    monkeypatch.setattr(DummyBlock, "save", lambda self, name, overwrite=False: None)
    deploy_module.deploy_block(spec)


def test_print_blocks_uses_block_header(monkeypatch):
    buf = StringIO()
    monkeypatch.setattr(list_module, "CONSOLE", Console(file=buf, highlight=False))
    list_module.print_blocks([_spec("a")])
    out = buf.getvalue()
    assert "a" in out
    assert "DummyBlock" in out
