import os
from functools import cached_property
from typing import Any

from pydantic import BaseModel, create_model
from pydantic.fields import FieldInfo
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

from prefector.blocks.sources.common import field_definitions_for_block


def keeper_settings_model_for_block(  # noqa: PLR0913
    block_cls: type[BaseModel],
    *,
    record_title: str,
    record_prefix: str = "",
    record_suffix: str = "",
    separator: str = ":",
    ksm_token: str | None = None,
    field_types: dict[str, type[Any]] | None = None,
    field_aliases: dict[str, str] | None = None,
) -> type[BaseSettings]:
    """
    Build a BaseSettings model from an existing Pydantic/Block model, sourced from a
    Keeper Secrets Manager record instead of the environment.

    `field_types` allows overriding selected field annotations, useful for
    nested settings structures (for example, replacing a complex block field
    with another settings model).

    `field_aliases` reads a field from a specific Keeper field/custom name, without
    requiring a Block subclass just to rename a field — useful for third-party blocks
    you don't want to modify.
    """
    definitions = field_definitions_for_block(block_cls, field_types, field_aliases)

    settings_cls = create_model(
        f"{block_cls.__name__}KeeperSettings",
        __base__=BaseSettings,
        **definitions,
    )
    settings_cls.model_config = SettingsConfigDict(extra="ignore")

    def _settings_customise_sources(  # noqa: PLR0913
        cls, settings_cls, init_settings, env_settings, dotenv_settings, file_secret_settings
    ):
        return (
            KeeperSettingsSource(
                settings_cls,
                record_title=record_title,
                record_prefix=record_prefix,
                record_suffix=record_suffix,
                separator=separator,
                ksm_token=ksm_token,
            ),
        )

    settings_cls.settings_customise_sources = classmethod(_settings_customise_sources)
    return settings_cls


def _secrets_manager(ksm_token: str | None):
    try:
        from keeper_secrets_manager_core import SecretsManager  # noqa: PLC0415
        from keeper_secrets_manager_core.storage import InMemoryKeyValueStorage  # noqa: PLC0415
    except ImportError as exc:
        raise ImportError(
            "Keeper Secrets Manager support requires the keeper extra. "
            "Install it with your package manager of choice: prefector[keeper]"
        ) from exc

    token = ksm_token or os.environ.get("KSM_CONFIG")
    if not token:
        raise ValueError(
            "No Keeper token provided. Pass ksm_token explicitly or set the KSM_CONFIG environment variable."
        )
    return SecretsManager(token=token, config=InMemoryKeyValueStorage(token))


def _keeper_field_value(record: Any, name: str) -> Any:
    """Retrieve a named field from a given Keeper record.
    Checks both standard fields by label and custom fields by name
    """
    record_dict = record.dict
    for field in record_dict.get("fields", []):
        if field["type"] == name:
            value = field["value"]
            return None if value == [] else value[0]
    for field in record_dict.get("custom", []):
        if field["label"] == name:
            value = field["value"]
            return None if value == [] else value[0]
    return None


class KeeperSettingsSource(PydanticBaseSettingsSource):
    """Pydantic-settings source that reads field values from a Keeper Secrets Manager record.

    Field values are matched to the record by field name, unless a field declares a
    `validation_alias`, in which case that alias is looked up in the record instead.
    """

    def __init__(  # noqa: PLR0913
        self,
        settings_cls: type[BaseSettings],
        *,
        record_title: str,
        record_prefix: str = "",
        record_suffix: str = "",
        separator: str = ":",
        ksm_token: str | None = None,
    ) -> None:
        super().__init__(settings_cls)
        self._record_title = separator.join(part for part in (record_prefix, record_title, record_suffix) if part)
        self._ksm_token = ksm_token

    @cached_property
    def _record(self) -> Any:
        record = _secrets_manager(self._ksm_token).get_secret_by_title(self._record_title)
        if record is None:
            raise ValueError(f"No Keeper record found with title '{self._record_title}'")
        return record

    def get_field_value(self, field: FieldInfo, field_name: str) -> tuple[Any, str, bool]:
        key = field.validation_alias if isinstance(field.validation_alias, str) else field_name
        return _keeper_field_value(self._record, key), key, False

    def __call__(self) -> dict[str, Any]:
        d: dict[str, Any] = {}
        for field_name, field in self.settings_cls.model_fields.items():
            field_value, field_key, value_is_complex = self.get_field_value(field, field_name)
            field_value = self.prepare_field_value(field_name, field, field_value, value_is_complex)
            if field_value is not None:
                d[field_key] = field_value
        return d
