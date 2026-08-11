from typing import Any

import pytest
from prefect.blocks.core import Block
from prefect.exceptions import PrefectException
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

from prefector.blocks.base import BlockBuildError, BlockSpec, _validate_specs, load_blocks


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


def test_block_spec_rejects_settings_cls_not_a_basesettings_subclass():
    with pytest.raises(ValueError, match="settings_cls must inherit from BaseSettings"):
        BlockSpec(name="invalid", settings_cls=object, block_cls=DummyBlock)


def test_block_spec_rejects_block_cls_not_a_block_subclass():
    class DummySettings(BaseSettings):
        value: int = 1

    with pytest.raises(ValueError, match="block_cls must inherit from Block"):
        BlockSpec(name="invalid", settings_cls=DummySettings, block_cls=object)


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


def test_block_build_error_omits_hint_for_nested_field_failure(monkeypatch):
    class Nested(BaseModel):
        inner: int

    class DummySettings(BaseSettings):
        nested: Nested

    monkeypatch.setenv("NESTED", '{"inner": "not-an-int"}')
    spec = BlockSpec(name="dummy", settings_cls=DummySettings, block_cls=DummyBlock)

    with pytest.raises(BlockBuildError) as exc_info:
        spec.build()

    assert "Set" not in str(exc_info.value)


def test_build_loads_dependency_block_by_name(monkeypatch):
    class DependencySettings(BaseSettings):
        value: int = 1

    class DependencyBlock(Block):
        value: int = 1

    dependency_spec = BlockSpec(name="dep", settings_cls=DependencySettings, block_cls=DependencyBlock)

    class DummySettings(BaseSettings):
        dependency: Any = dependency_spec

    class ParentBlock(Block):
        dependency: DependencyBlock

    monkeypatch.setattr(DependencyBlock, "load", classmethod(lambda cls, name: DependencyBlock(value=42)))
    spec = BlockSpec(name="parent", settings_cls=DummySettings, block_cls=ParentBlock)

    block = spec.build()

    assert block.dependency.value == 42


def test_build_raises_when_dependency_block_load_fails(monkeypatch):
    class DependencySettings(BaseSettings):
        value: int = 1

    class DependencyBlock(Block):
        value: int = 1

    dependency_spec = BlockSpec(name="missing-dep", settings_cls=DependencySettings, block_cls=DependencyBlock)

    class DummySettings(BaseSettings):
        dependency: Any = dependency_spec

    class ParentBlock(Block):
        dependency: DependencyBlock

    def _raise_not_found(cls, name):
        raise PrefectException(f"Unable to find block document named {name}")

    monkeypatch.setattr(DependencyBlock, "load", classmethod(_raise_not_found))
    spec = BlockSpec(name="parent", settings_cls=DummySettings, block_cls=ParentBlock)

    with pytest.raises(ValueError, match="Failed to load dependency block 'missing-dep' for 'parent'"):
        spec.build()


def test_load_blocks_reuses_already_loaded_module(tmp_path):
    (tmp_path / "dummy.py").write_text(
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

    first_specs = load_blocks(tmp_path)
    second_specs = load_blocks(tmp_path)

    assert first_specs[0].settings_cls is second_specs[0].settings_cls


def test_load_blocks_discovers_multiple_spec_files_in_one_directory(tmp_path):
    for stem in ("dummy_a", "dummy_b"):
        (tmp_path / f"{stem}.py").write_text(
            "from pydantic_settings import BaseSettings\n"
            "from prefect.blocks.core import Block\n"
            "from prefector.blocks.base import BlockSpec\n"
            "class DummySettings(BaseSettings):\n"
            "    value: int = 1\n"
            "class DummyBlock(Block):\n"
            "    value: int\n"
            f"BLOCKS = [BlockSpec(name='{stem}', settings_cls=DummySettings, block_cls=DummyBlock)]\n",
            encoding="utf-8",
        )

    specs = load_blocks(tmp_path)

    assert sorted(spec.name for spec in specs) == ["dummy_a", "dummy_b"]


def test_load_blocks_skips_underscore_and_init_files(tmp_path):
    (tmp_path / "__init__.py").write_text("BLOCKS = []\n", encoding="utf-8")
    (tmp_path / "_private.py").write_text("raise RuntimeError('should never be imported')\n", encoding="utf-8")

    specs = load_blocks(tmp_path)

    assert specs == []


def test_load_blocks_rejects_non_list_blocks(tmp_path):
    (tmp_path / "dummy.py").write_text("BLOCKS = 'not-a-list'\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Spec module must expose BLOCKS as list"):
        load_blocks(tmp_path)


def test_load_blocks_rejects_non_blockspec_entries(tmp_path):
    (tmp_path / "dummy.py").write_text("BLOCKS = ['not-a-blockspec']\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Spec module has non-BlockSpec entries in BLOCKS"):
        load_blocks(tmp_path)


def test_load_blocks_reports_missing_prefect_collection_dependency(tmp_path):
    (tmp_path / "dummy.py").write_text("import prefect_nonexistent_collection\nBLOCKS = []\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Install the Prefect collection package: prefect-nonexistent-collection"):
        load_blocks(tmp_path)


def test_load_blocks_reports_missing_generic_dependency(tmp_path):
    (tmp_path / "dummy.py").write_text("import totally_nonexistent_module\nBLOCKS = []\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Install it in the current environment"):
        load_blocks(tmp_path)


def test_load_blocks_reports_import_error_without_module_name(tmp_path):
    (tmp_path / "dummy.py").write_text("raise ImportError('boom')\nBLOCKS = []\n", encoding="utf-8")

    with pytest.raises(ValueError, match="could not be imported"):
        load_blocks(tmp_path)
