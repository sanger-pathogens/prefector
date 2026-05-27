import pytest
from prefect.blocks.core import Block
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

from prefector.blocks.base import BlockBuildError, BlockSpec, _validate_specs, load_blocks, settings_model_for_block


class DummyBlock(Block):
    value: int = 1


def test_load_blocks_discovers_specs_from_directory(tmp_path):
    spec_path = tmp_path / "dummy.py"
    spec_path.write_text(
        "from pydantic_settings import BaseSettings\n"
        "from prefect.blocks.core import Block\n"
        "from prefector.blocks.base import BlockSpec\n"
        "class DummySettings(BaseSettings):\n"
        "    value: int = 1\n"
        "class DummyBlock(Block):\n"
        "    value: int\n"
        "BLOCKS = [BlockSpec(name='dummy', settings_cls=DummySettings, block_cls=DummyBlock)]\n",
        encoding="utf-8",
    )

    specs = load_blocks(tmp_path)

    spec_names = [spec.name for spec in specs]
    assert spec_names == ["dummy"]


def test_validate_specs_rejects_duplicate_names():
    class DummySettings(BaseSettings):
        value: int = 1

    specs = [
        BlockSpec(name="duplicate", settings_cls=DummySettings, block_cls=DummyBlock),
        BlockSpec(name="duplicate", settings_cls=DummySettings, block_cls=DummyBlock),
    ]
    with pytest.raises(ValueError, match="Duplicate block name\\(s\\): duplicate"):
        _validate_specs(specs)


def test_block_spec_requires_settings_and_block_classes():
    class DummySettings(BaseSettings):
        model_config = SettingsConfigDict(env_prefix="DUMMY_")
        value: int = 1

    with pytest.raises(TypeError):
        BlockSpec(name="invalid", block_cls=DummyBlock)

    with pytest.raises(TypeError):
        BlockSpec(name="invalid", settings_cls=DummySettings)


def test_settings_model_for_block_supports_nested_env(monkeypatch):
    class NestedValue(BaseModel):
        endpoint_url: str

    class NestedBlock(Block):
        endpoint: NestedValue

    settings_cls = settings_model_for_block(
        NestedBlock,
        env_prefix="TEST_",
        env_nested_delimiter="__",
    )
    monkeypatch.setenv("TEST_ENDPOINT__ENDPOINT_URL", "http://service.local")

    payload = settings_cls().model_dump()

    assert payload == {"endpoint": {"endpoint_url": "http://service.local"}}


def test_block_build_error_includes_env_var_name(monkeypatch):
    class DummyBlock(Block):
        value: int

    class DummySettings(BaseSettings):
        model_config = SettingsConfigDict(env_prefix="DUMMY_")
        value: int

    monkeypatch.delenv("DUMMY_VALUE", raising=False)
    spec = BlockSpec(name="dummy", settings_cls=DummySettings, block_cls=DummyBlock)

    with pytest.raises(BlockBuildError, match="Set DUMMY_VALUE"):
        spec.build()
