from dataclasses import dataclass
from typing import Annotated, Any

from prefect.blocks.core import Block
from prefect.exceptions import PrefectException
from pydantic import BaseModel, Field, StringConstraints, ValidationError, create_model
from pydantic_settings import BaseSettings, SettingsConfigDict

NonEmptyStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


def settings_model_for_block(  # noqa: PLR0913
    block_cls: type[BaseModel],
    *,
    env_prefix: str = "",
    env_nested_delimiter: str = "__",
    field_types: dict[str, type[Any]] | None = None,
) -> type[BaseSettings]:
    """
    Build a BaseSettings model from an existing Pydantic/Block model.

    `field_types` allows overriding selected field annotations, useful for
    nested settings structures (for example, replacing a complex block field
    with another settings model).
    """
    definitions: dict[str, tuple[Any, Any]] = {}
    field_types = field_types or {}

    for name, model_field in block_cls.model_fields.items():
        annotation = field_types.get(name, model_field.annotation)

        if model_field.default_factory is not None:
            default = Field(default_factory=model_field.default_factory)
        elif model_field.is_required():
            default = ...
        else:
            default = model_field.default

        definitions[name] = (annotation, default)

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


class BlockBuildError(ValueError):
    """Raised when block settings validation fails during block construction."""

    def __init__(self, name: str, settings_cls: type[BaseSettings], exception: ValidationError):
        details = []
        for error in exception.errors():
            loc = error.get("loc", ())
            field = ".".join(str(part) for part in loc) if loc else "unknown"
            message = error.get("msg", "Invalid value")
            env_var = _loc_to_env_var(settings_cls, loc)
            if env_var:
                details.append(f"{field}: {message}. Set {env_var}")
            else:
                details.append(f"{field}: {message}")

        details_text = "\n".join(details) if details else str(exception)
        super().__init__(f"Failed to build block '{name}':\n{details_text}")


def _loc_to_env_var(settings_cls: type[BaseSettings], loc: tuple[Any, ...]) -> str | None:
    """Map a Pydantic error location tuple to the corresponding environment variable name."""
    if not loc:
        return None

    env_prefix = str(settings_cls.model_config.get("env_prefix"))
    if env_prefix is None:
        return None

    env_nested_delimiter = str(settings_cls.model_config.get("env_nested_delimiter"))
    if env_nested_delimiter is None:
        return None

    head = loc[0]
    if not isinstance(head, str):
        return None

    field = settings_cls.model_fields.get(head)
    if field is None:
        head_name = head.upper()
    else:
        alias = field.validation_alias if isinstance(field.validation_alias, str) else head
        head_name = alias.upper()

    tail_parts = [part.upper() for part in loc[1:] if isinstance(part, str)]
    suffix = env_nested_delimiter.join([head_name, *tail_parts]) if tail_parts else head_name
    return f"{env_prefix}{suffix}"


@dataclass(frozen=True)
class BlockSpec:
    name: str
    settings_cls: type[BaseSettings]
    block_cls: type[Block]

    def __post_init__(self) -> None:
        if not issubclass(self.settings_cls, BaseSettings):
            raise ValueError("settings_cls must inherit from BaseSettings")
        if not issubclass(self.block_cls, Block):
            raise ValueError("block_cls must inherit from Block")

    def build(self) -> Block:
        try:
            settings = self.settings_cls()
        except ValidationError as exc:
            raise BlockBuildError(self.name, self.settings_cls, exc) from exc

        payload = self._resolve_settings(settings)
        return self.block_cls(**payload)

    def _resolve_settings(self, settings: BaseSettings) -> dict[str, Any]:
        payload = settings.model_dump()

        for field_name in settings.model_fields.keys():
            value = getattr(settings, field_name)
            if isinstance(value, BlockSpec):
                try:
                    payload[field_name] = value.block_cls.load(value.name)
                except PrefectException as exc:
                    raise ValueError(
                        f"Failed to load dependency block '{value.name}' "
                        f"for '{self.name}' ({value.block_cls.__name__})."
                    ) from exc

        return payload
