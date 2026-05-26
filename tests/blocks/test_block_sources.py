from unittest.mock import MagicMock, patch

import pytest
from prefect.blocks.core import Block

from prefector.blocks.sources import (
    EnvBlockSource,
    KeeperBlockSource,
    _keeper_record_title,
    build_block_from_source,
    load_block_sources,
)


class SimpleBlock(Block):
    username: str
    password: str


class OptionalBlock(Block):
    host: str
    port: int = 5432


def test_load_block_sources_parses_env_entry(tmp_path):
    (tmp_path / "block-sources.yaml").write_text(
        "my-block:\n  source: env\n  env_var_prefix: MY_\n  fields:\n    username: USER\n",
        encoding="utf-8",
    )
    config = load_block_sources(tmp_path / "block-sources.yaml")

    assert isinstance(config.root["my-block"], EnvBlockSource)
    assert config.root["my-block"].env_var_prefix == "MY_"
    assert config.root["my-block"].fields == {"username": "USER"}


def test_load_block_sources_parses_keeper_entry(tmp_path):
    (tmp_path / "block-sources.yaml").write_text(
        "my-block:\n"
        "  source: keeper\n"
        "  record_title: trino-credentials\n"
        "  record_prefix: dlh\n"
        "  record_suffix: prod\n"
        '  separator: ":"\n',
        encoding="utf-8",
    )
    config = load_block_sources(tmp_path / "block-sources.yaml")

    entry = config.root["my-block"]
    assert isinstance(entry, KeeperBlockSource)
    assert entry.record_title == "trino-credentials"
    assert entry.record_prefix == "dlh"


def test_load_block_sources_parses_bare_list_format(tmp_path):
    (tmp_path / "block-sources.yaml").write_text(
        "- my-block:\n    source: env\n    env_var_prefix: MY_\n",
        encoding="utf-8",
    )
    config = load_block_sources(tmp_path / "block-sources.yaml")

    assert isinstance(config.root["my-block"], EnvBlockSource)
    assert config.root["my-block"].env_var_prefix == "MY_"


def test_load_block_sources_parses_blocks_key_list_format(tmp_path):
    (tmp_path / "block-sources.yaml").write_text(
        "blocks:\n"
        "  - my-block:\n"
        "      source: env\n"
        "      env_var_prefix: MY_\n"
        "  - other-block:\n"
        "      source: keeper\n"
        "      record_title: trino\n",
        encoding="utf-8",
    )
    config = load_block_sources(tmp_path / "block-sources.yaml")

    assert isinstance(config.root["my-block"], EnvBlockSource)
    assert isinstance(config.root["other-block"], KeeperBlockSource)


def test_load_block_sources_rejects_malformed_list_entry(tmp_path):
    (tmp_path / "block-sources.yaml").write_text(
        "- source: env\n  env_var_prefix: X_\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="single-key mapping"):
        load_block_sources(tmp_path / "block-sources.yaml")


def test_load_block_sources_rejects_unknown_source(tmp_path):
    (tmp_path / "block-sources.yaml").write_text(
        "my-block:\n  source: vault\n  path: secret/data/foo\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="Invalid block sources config"):
        load_block_sources(tmp_path / "block-sources.yaml")


def test_load_block_sources_accepts_empty_file(tmp_path):
    (tmp_path / "block-sources.yaml").write_text("", encoding="utf-8")
    config = load_block_sources(tmp_path / "block-sources.yaml")
    assert config.root == {}


def test_load_block_sources_raises_on_missing_file(tmp_path):
    with pytest.raises(ValueError, match="Unable to read block sources file"):
        load_block_sources(tmp_path / "nonexistent.yaml")


def test_env_interpolation_in_record_suffix_deferred_to_build_time(tmp_path, monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "staging")
    (tmp_path / "block-sources.yaml").write_text(
        "my-block:\n  source: keeper\n  record_title: trino\n  record_suffix: ${ENVIRONMENT}\n",
        encoding="utf-8",
    )
    config = load_block_sources(tmp_path / "block-sources.yaml")
    entry = config.root["my-block"]

    # load_block_sources does not interpolate; the template is preserved
    assert entry.record_suffix == "${ENVIRONMENT}"


def test_env_interpolation_resolves_prefix_at_build_time(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "PROD")
    monkeypatch.setenv("PROD_DB_USERNAME", "alice")
    monkeypatch.setenv("PROD_DB_PASSWORD", "secret")

    source = EnvBlockSource(source="env", env_var_prefix="${ENVIRONMENT}_DB_")
    block = build_block_from_source("my-block", SimpleBlock, source)

    assert block.username == "alice"
    assert block.password == "secret"


def test_env_interpolation_raises_on_unset_var(tmp_path, monkeypatch):
    monkeypatch.delenv("MISSING_VAR", raising=False)
    source = KeeperBlockSource(
        source="keeper",
        record_title="foo",
        record_suffix="${MISSING_VAR}",
    )
    with pytest.raises(ValueError, match="Environment variable 'MISSING_VAR' is not set"):
        build_block_from_source("my-block", SimpleBlock, source)


def test_build_from_env_source_reads_prefixed_vars(monkeypatch):
    monkeypatch.setenv("DB_USERNAME", "alice")
    monkeypatch.setenv("DB_PASSWORD", "secret")

    source = EnvBlockSource(source="env", env_var_prefix="DB_")
    block = build_block_from_source("my-block", SimpleBlock, source)

    assert block.username == "alice"
    assert block.password == "secret"


def test_build_from_env_source_applies_field_mapping(monkeypatch):
    monkeypatch.setenv("TRINO_USER", "bob")
    monkeypatch.setenv("TRINO_PASS", "hunter2")

    source = EnvBlockSource(
        source="env",
        env_var_prefix="TRINO_",
        fields={"username": "USER", "password": "PASS"},
    )
    block = build_block_from_source("trino", SimpleBlock, source)

    assert block.username == "bob"
    assert block.password == "hunter2"


def test_build_from_env_source_uses_defaults_for_optional_fields(monkeypatch):
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.delenv("DB_PORT", raising=False)

    source = EnvBlockSource(source="env", env_var_prefix="DB_")
    block = build_block_from_source("my-block", OptionalBlock, source)

    assert block.host == "localhost"
    assert block.port == 5432


def test_build_from_env_source_raises_block_build_error_with_var_name(monkeypatch):
    monkeypatch.delenv("DB_USERNAME", raising=False)
    monkeypatch.delenv("DB_PASSWORD", raising=False)

    from prefector.blocks.base import BlockBuildError

    source = EnvBlockSource(source="env", env_var_prefix="DB_")
    with pytest.raises(BlockBuildError, match="DB_USERNAME"):
        build_block_from_source("my-block", SimpleBlock, source)


@pytest.mark.parametrize(
    "prefix, title, suffix, sep, expected",
    [
        ("dlh", "trino-credentials", "prod", ":", "dlh:trino-credentials:prod"),
        ("dlh", "trino-credentials", "", ":", "dlh:trino-credentials"),
        ("", "trino-credentials", "prod", ":", "trino-credentials:prod"),
        ("", "trino-credentials", "", ":", "trino-credentials"),
        ("a", "b", "c", "-", "a-b-c"),
    ],
)
def test_keeper_record_title_assembly(prefix, title, suffix, sep, expected):
    source = KeeperBlockSource(
        source="keeper",
        record_title=title,
        record_prefix=prefix,
        record_suffix=suffix,
        separator=sep,
    )
    assert _keeper_record_title(source) == expected


def _make_keeper_record(
    standard: dict[str, str] | None = None,
    custom: dict[str, str] | None = None,
) -> MagicMock:
    record = MagicMock()
    record.dict = {
        "fields": [{"type": k, "value": [v]} for k, v in (standard or {}).items()],
        "custom": [{"type": "text", "label": k, "value": [v]} for k, v in (custom or {}).items()],
    }
    return record


def test_build_from_keeper_source_reads_standard_fields():
    source = KeeperBlockSource(
        source="keeper",
        record_title="trino-credentials",
        fields={"username": "login", "password": "password"},
    )
    record = _make_keeper_record(standard={"login": "alice", "password": "secret"})
    mock_sm = MagicMock()
    mock_sm.get_secret_by_title.return_value = record

    with patch.dict(
        "sys.modules",
        {
            "keeper_secrets_manager_core": MagicMock(SecretsManager=MagicMock(return_value=mock_sm)),
            "keeper_secrets_manager_core.storage": MagicMock(InMemoryKeyValueStorage=MagicMock()),
        },
    ):
        block = build_block_from_source("trino", SimpleBlock, source)

    assert block.username == "alice"
    assert block.password == "secret"


def test_build_from_keeper_source_reads_custom_fields():
    """Custom-field records (no standard template) are matched by label."""
    source = KeeperBlockSource(source="keeper", record_title="trino-credentials")
    record = _make_keeper_record(custom={"username": "alice", "password": "secret"})
    mock_sm = MagicMock()
    mock_sm.get_secret_by_title.return_value = record

    with patch.dict(
        "sys.modules",
        {
            "keeper_secrets_manager_core": MagicMock(SecretsManager=MagicMock(return_value=mock_sm)),
            "keeper_secrets_manager_core.storage": MagicMock(InMemoryKeyValueStorage=MagicMock()),
        },
    ):
        block = build_block_from_source("trino", SimpleBlock, source)

    assert block.username == "alice"
    assert block.password == "secret"


def test_build_from_keeper_source_error_does_not_expose_values():
    """Missing-field error must name the field, not print any retrieved values."""
    source = KeeperBlockSource(source="keeper", record_title="trino-credentials")
    # record has username but is missing password
    record = _make_keeper_record(custom={"username": "alice"})
    mock_sm = MagicMock()
    mock_sm.get_secret_by_title.return_value = record

    with patch.dict(
        "sys.modules",
        {
            "keeper_secrets_manager_core": MagicMock(SecretsManager=MagicMock(return_value=mock_sm)),
            "keeper_secrets_manager_core.storage": MagicMock(InMemoryKeyValueStorage=MagicMock()),
        },
    ):
        with pytest.raises(ValueError, match="missing required fields: password") as exc_info:
            build_block_from_source("trino", SimpleBlock, source)

    assert "alice" not in str(exc_info.value)


def test_build_from_keeper_source_raises_when_record_not_found():
    source = KeeperBlockSource(source="keeper", record_title="missing-record")
    mock_sm = MagicMock()
    mock_sm.get_secret_by_title.return_value = None

    with patch.dict(
        "sys.modules",
        {
            "keeper_secrets_manager_core": MagicMock(SecretsManager=MagicMock(return_value=mock_sm)),
            "keeper_secrets_manager_core.storage": MagicMock(InMemoryKeyValueStorage=MagicMock()),
        },
    ):
        with pytest.raises(ValueError, match="No Keeper record found"):
            build_block_from_source("trino", SimpleBlock, source)


def test_build_from_keeper_source_raises_on_missing_sdk():
    source = KeeperBlockSource(source="keeper", record_title="some-record")

    with patch.dict("sys.modules", {"keeper_secrets_manager_core": None}):
        with pytest.raises(ImportError, match=r"prefector\[keeper\]"):
            build_block_from_source("trino", SimpleBlock, source)


def test_resolve_sources_uses_explicit_flag(tmp_path):
    sources_file = tmp_path / "custom-sources.yaml"
    sources_file.write_text(
        "my-block:\n  source: env\n  env_var_prefix: X_\n",
        encoding="utf-8",
    )

    from prefector.blocks.cli import _resolve_sources
    from prefector.blocks.options import BlockOptions

    opts = BlockOptions(blocks_dir=tmp_path, target=(), sources=sources_file)
    config, path = _resolve_sources(opts)

    assert config is not None
    assert path == sources_file


def test_resolve_sources_falls_back_to_blocks_dir(tmp_path):
    (tmp_path / "block-sources.yaml").write_text(
        "my-block:\n  source: env\n  env_var_prefix: X_\n",
        encoding="utf-8",
    )

    from prefector.blocks.cli import _resolve_sources
    from prefector.blocks.options import BlockOptions

    opts = BlockOptions(blocks_dir=tmp_path, target=(), sources=None)
    config, path = _resolve_sources(opts)

    assert config is not None
    assert path == tmp_path / "block-sources.yaml"


def test_resolve_sources_returns_none_when_absent(tmp_path):
    from prefector.blocks.cli import _resolve_sources
    from prefector.blocks.options import BlockOptions

    opts = BlockOptions(blocks_dir=tmp_path, target=(), sources=None)
    config, path = _resolve_sources(opts)

    assert config is None
    assert path is None
