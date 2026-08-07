import os
from typing import Any, Optional

from pydantic import BaseModel, create_model
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

from prefector.blocks.sources.common import apply_nested_fields, field_definitions_for_block


class _NestedFieldsEnvSource(PydanticBaseSettingsSource):
    def __init__(self, settings_cls: type[BaseSettings], nested_fields: dict[str, str]) -> None:
        super().__init__(settings_cls)
        self._nested_fields = nested_fields

    def get_field_value(self, field, field_name):  # noqa: ARG002
        return None, field_name, False

    def __call__(self) -> dict[str, Any]:
        d: dict[str, Any] = {}
        apply_nested_fields(d, self._nested_fields, os.environ.get)
        return d


def env_settings_model_for_block(  # noqa: PLR0913
    block_cls: type[BaseModel],
    *,
    env_prefix: str = "",
    field_types: Optional[dict[str, type[Any]]] = None,
    field_aliases: Optional[dict[str, str]] = None,
    nested_fields: Optional[dict[str, str]] = None,
) -> type[BaseSettings]:
    """
    Build a BaseSettings model from an existing Pydantic/Block model.

    `field_types` allows overriding selected field annotations, useful for
    nested settings structures (for example, replacing a complex block field
    with another settings model).

    `field_aliases` reads a field from a specific full env var name (bypassing
    `env_prefix`), without requiring a Block subclass just to rename a field —
    useful for third-party blocks you don't want to modify.

    `nested_fields` maps a dotted `<field>.<subfield>` path to a full env var name, for
    populating one sub-field of a nested model field (for example, Prefect's
    `AwsCredentials.aws_client_parameters.endpoint_url`).
    """
    definitions = field_definitions_for_block(block_cls, field_types, field_aliases)

    settings_cls = create_model(
        f"{block_cls.__name__}Settings",
        __base__=BaseSettings,
        **definitions,
    )
    settings_cls.model_config = SettingsConfigDict(
        env_prefix=env_prefix,
        extra="ignore",
    )

    if nested_fields:

        def _settings_customise_sources(  # noqa: PLR0913
            cls, settings_cls, init_settings, env_settings, dotenv_settings, file_secret_settings
        ):
            return (
                _NestedFieldsEnvSource(settings_cls, nested_fields),
                init_settings,
                env_settings,
                dotenv_settings,
                file_secret_settings,
            )

        settings_cls.settings_customise_sources = classmethod(_settings_customise_sources)

    return settings_cls
