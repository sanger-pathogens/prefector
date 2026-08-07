from typing import Any, Optional

from pydantic import BaseModel, create_model
from pydantic_settings import BaseSettings, SettingsConfigDict

from prefector.blocks.sources.common import field_definitions_for_block


def env_settings_model_for_block(  # noqa: PLR0913
    block_cls: type[BaseModel],
    *,
    env_prefix: str = "",
    env_nested_delimiter: str = "__",
    field_types: Optional[dict[str, type[Any]]] = None,
) -> type[BaseSettings]:
    """
    Build a BaseSettings model from an existing Pydantic/Block model.

    `field_types` allows overriding selected field annotations, useful for
    nested settings structures (for example, replacing a complex block field
    with another settings model).
    """
    definitions = field_definitions_for_block(block_cls, field_types)

    settings_cls = create_model(
        f"{block_cls.__name__}Settings",
        __base__=BaseSettings,
        **definitions,
    )
    settings_cls.model_config = SettingsConfigDict(
        env_prefix=env_prefix,
        env_nested_delimiter=env_nested_delimiter,
        extra="ignore",
    )
    return settings_cls
