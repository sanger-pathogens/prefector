from unittest.mock import MagicMock, patch

import pytest
from prefect.blocks.core import Block
from pydantic import Field, ValidationError
from pydantic_settings import BaseSettings

from prefector.blocks.sources.keeper import KeeperSettingsSource, keeper_settings_model_for_block


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


def _mock_keeper_sdk(mock_sm: MagicMock):
    return patch.dict(
        "sys.modules",
        {
            "keeper_secrets_manager_core": MagicMock(SecretsManager=MagicMock(return_value=mock_sm)),
            "keeper_secrets_manager_core.storage": MagicMock(InMemoryKeyValueStorage=MagicMock()),
        },
    )


def _settings_cls(**source_kwargs):
    class Settings(BaseSettings):
        username: str = Field(validation_alias="login")
        password: str

        @classmethod
        def settings_customise_sources(  # noqa: PLR0913
            cls, settings_cls, init_settings, env_settings, dotenv_settings, file_secret_settings
        ):
            return (KeeperSettingsSource(settings_cls, **source_kwargs),)

    return Settings


def test_reads_standard_fields_matched_by_alias():
    record = _make_keeper_record(standard={"login": "alice", "password": "secret"})
    mock_sm = MagicMock()
    mock_sm.get_secret_by_title.return_value = record

    with _mock_keeper_sdk(mock_sm):
        settings = _settings_cls(record_title="trino-credentials", ksm_token="dummy-token")()

    assert settings.username == "alice"
    assert settings.password == "secret"


def test_reads_custom_fields_matched_by_label():
    record = _make_keeper_record(custom={"login": "alice", "password": "secret"})
    mock_sm = MagicMock()
    mock_sm.get_secret_by_title.return_value = record

    with _mock_keeper_sdk(mock_sm):
        settings = _settings_cls(record_title="trino-credentials", ksm_token="dummy-token")()

    assert settings.username == "alice"
    assert settings.password == "secret"


def test_fetches_record_only_once_for_multiple_fields():
    record = _make_keeper_record(custom={"login": "alice", "password": "secret"})
    mock_sm = MagicMock()
    mock_sm.get_secret_by_title.return_value = record

    with _mock_keeper_sdk(mock_sm):
        _settings_cls(record_title="trino-credentials", ksm_token="dummy-token")()

    mock_sm.get_secret_by_title.assert_called_once_with("trino-credentials")


def test_assembles_record_title_from_prefix_suffix_and_separator():
    record = _make_keeper_record(custom={"login": "alice", "password": "secret"})
    mock_sm = MagicMock()
    mock_sm.get_secret_by_title.return_value = record

    with _mock_keeper_sdk(mock_sm):
        _settings_cls(
            record_title="trino", record_prefix="dlh", record_suffix="prod", separator="-", ksm_token="dummy-token"
        )()

    mock_sm.get_secret_by_title.assert_called_once_with("dlh-trino-prod")


def test_passes_ksm_token_to_secrets_manager():
    record = _make_keeper_record(custom={"login": "alice", "password": "secret"})
    mock_sm = MagicMock()
    mock_sm.get_secret_by_title.return_value = record
    mock_sm_cls = MagicMock(return_value=mock_sm)
    mock_storage_cls = MagicMock()

    with patch.dict(
        "sys.modules",
        {
            "keeper_secrets_manager_core": MagicMock(SecretsManager=mock_sm_cls),
            "keeper_secrets_manager_core.storage": MagicMock(InMemoryKeyValueStorage=mock_storage_cls),
        },
    ):
        _settings_cls(record_title="trino-credentials", ksm_token="one-time-token")()

    mock_storage_cls.assert_called_once_with("one-time-token")
    mock_sm_cls.assert_called_once_with(token="one-time-token", config=mock_storage_cls.return_value)


def test_raises_when_record_not_found():
    mock_sm = MagicMock()
    mock_sm.get_secret_by_title.return_value = None

    with _mock_keeper_sdk(mock_sm), pytest.raises(ValueError, match="No Keeper record found"):
        _settings_cls(record_title="missing-record", ksm_token="dummy-token")()


def test_raises_on_missing_sdk():
    with patch.dict("sys.modules", {"keeper_secrets_manager_core": None}):
        with pytest.raises(ImportError, match=r"prefector\[keeper\]"):
            _settings_cls(record_title="trino-credentials")()


def test_missing_required_field_raises_validation_error():
    record = _make_keeper_record(custom={"login": "alice"})
    mock_sm = MagicMock()
    mock_sm.get_secret_by_title.return_value = record

    with _mock_keeper_sdk(mock_sm), pytest.raises(ValidationError, match="password"):
        _settings_cls(record_title="trino-credentials", ksm_token="dummy-token")()


def test_falls_back_to_ksm_config_env_var_when_token_omitted(monkeypatch):
    monkeypatch.setenv("KSM_CONFIG", "config-from-env")
    record = _make_keeper_record(custom={"login": "alice", "password": "secret"})
    mock_sm = MagicMock()
    mock_sm.get_secret_by_title.return_value = record
    mock_sm_cls = MagicMock(return_value=mock_sm)
    mock_storage_cls = MagicMock()

    with patch.dict(
        "sys.modules",
        {
            "keeper_secrets_manager_core": MagicMock(SecretsManager=mock_sm_cls),
            "keeper_secrets_manager_core.storage": MagicMock(InMemoryKeyValueStorage=mock_storage_cls),
        },
    ):
        _settings_cls(record_title="trino-credentials")()

    mock_storage_cls.assert_called_once_with("config-from-env")
    mock_sm_cls.assert_called_once_with(token="config-from-env", config=mock_storage_cls.return_value)


def test_raises_when_no_token_and_no_ksm_config_env_var(monkeypatch):
    monkeypatch.delenv("KSM_CONFIG", raising=False)

    with _mock_keeper_sdk(MagicMock()), pytest.raises(ValueError, match="KSM_CONFIG"):
        _settings_cls(record_title="trino-credentials")()


class _CredentialsBlock(Block):
    username: str
    password: str
    port: int = 8080


def test_keeper_settings_model_for_block_reads_matching_fields():
    record = _make_keeper_record(custom={"username": "alice", "password": "secret"})
    mock_sm = MagicMock()
    mock_sm.get_secret_by_title.return_value = record

    settings_cls = keeper_settings_model_for_block(_CredentialsBlock, record_title="creds", ksm_token="dummy-token")
    with _mock_keeper_sdk(mock_sm):
        settings = settings_cls()

    assert settings.username == "alice"
    assert settings.password == "secret"
    assert settings.port == 8080  # not in the record — falls back to the block's own default


def test_keeper_settings_model_for_block_preserves_default_factory():
    class _TaggedBlock(Block):
        username: str
        tags: list[str] = Field(default_factory=list)

    record = _make_keeper_record(custom={"username": "alice"})
    mock_sm = MagicMock()
    mock_sm.get_secret_by_title.return_value = record

    settings_cls = keeper_settings_model_for_block(_TaggedBlock, record_title="creds", ksm_token="dummy-token")
    with _mock_keeper_sdk(mock_sm):
        settings = settings_cls()

    assert settings.tags == []


def test_keeper_settings_model_for_block_assembles_record_title_from_parts():
    record = _make_keeper_record(custom={"username": "alice", "password": "secret"})
    mock_sm = MagicMock()
    mock_sm.get_secret_by_title.return_value = record

    settings_cls = keeper_settings_model_for_block(
        _CredentialsBlock,
        record_title="creds",
        record_prefix="dlh",
        record_suffix="prod",
        separator="-",
        ksm_token="dummy-token",
    )
    with _mock_keeper_sdk(mock_sm):
        settings_cls()

    mock_sm.get_secret_by_title.assert_called_once_with("dlh-creds-prod")


def test_keeper_settings_model_for_block_missing_required_field_raises():
    record = _make_keeper_record(custom={"username": "alice"})
    mock_sm = MagicMock()
    mock_sm.get_secret_by_title.return_value = record

    settings_cls = keeper_settings_model_for_block(_CredentialsBlock, record_title="creds", ksm_token="dummy-token")
    with _mock_keeper_sdk(mock_sm), pytest.raises(ValidationError, match="password"):
        settings_cls()


def test_keeper_settings_model_for_block_missing_record_raises():
    mock_sm = MagicMock()
    mock_sm.get_secret_by_title.return_value = None

    settings_cls = keeper_settings_model_for_block(_CredentialsBlock, record_title="missing", ksm_token="dummy-token")
    with _mock_keeper_sdk(mock_sm), pytest.raises(ValueError, match="No Keeper record found"):
        settings_cls()


def test_keeper_settings_model_for_block_applies_field_type_override():
    settings_cls = keeper_settings_model_for_block(_CredentialsBlock, record_title="creds", field_types={"port": str})

    assert settings_cls.model_fields["port"].annotation is str
