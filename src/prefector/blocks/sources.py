import os
import re
from pathlib import Path
from typing import Annotated, Any, Literal

import yaml
from prefect.blocks.core import Block
from pydantic import BaseModel, ConfigDict, Field, RootModel, ValidationError, create_model
from pydantic_settings import BaseSettings, SettingsConfigDict

from prefector.blocks.base import BlockBuildError


class EnvBlockSource(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: Literal["env"]
    env_var_prefix: str = ""
    fields: dict[str, str] = Field(default_factory=dict)  # block_field -> env_var_suffix


class KeeperBlockSource(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: Literal["keeper"]
    record_title: str
    record_prefix: str = ""
    record_suffix: str = ""
    separator: str = ":"
    ksm_token: str = ""
    fields: dict[str, str] = Field(default_factory=dict)  # block_field -> KSM field title


BlockSource = Annotated[EnvBlockSource | KeeperBlockSource, Field(discriminator="source")]


class BlockSourcesConfig(RootModel[dict[str, BlockSource]]):
    pass


def _interpolate_str(value: str, source_path: Path | None) -> str:
    def replace(match: re.Match) -> str:
        name = match.group(1)
        if name not in os.environ:
            loc = f" (referenced in {source_path})" if source_path else ""
            raise ValueError(f"Environment variable '{name}' is not set{loc}")
        return os.environ[name]

    return re.sub(r"\$\{([^}]+)}", replace, value)


def _interpolate_dict(d: dict[str, Any], source_path: Path | None) -> dict[str, Any]:
    result = {}
    for k, v in d.items():
        if isinstance(v, str):
            result[k] = _interpolate_str(v, source_path)
        elif isinstance(v, dict):
            result[k] = _interpolate_dict(v, source_path)
        else:
            result[k] = v
    return result


def _interpolate_source(source: BlockSource, source_path: Path | None) -> BlockSource:
    raw = source.model_dump()
    interpolated = _interpolate_dict(raw, source_path)
    return type(source).model_validate(interpolated)


def _normalize_sources_payload(payload: object, path: Path) -> dict:
    """Accept a flat mapping, a bare list, or a {'blocks': [...]} wrapper.

    The list form is a sequence of single-key mappings::

        - block-name:
            source: env
        - other-block:
            source: keeper
            ...
    """
    if isinstance(payload, dict) and set(payload.keys()) == {"blocks"}:
        payload = payload["blocks"]

    if isinstance(payload, list):
        result: dict = {}
        for i, item in enumerate(payload):
            if not isinstance(item, dict) or len(item) != 1:
                raise ValueError(f"Each list entry in {path} must be a single-key mapping (entry {i} is invalid)")
            result.update(item)
        return result

    if isinstance(payload, dict):
        return payload

    raise ValueError(f"Expected mapping or list in {path}, got {type(payload).__name__}")


def load_block_sources(path: Path) -> BlockSourcesConfig:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ValueError(f"Unable to read block sources file: {path}") from exc

    if payload is None:
        return BlockSourcesConfig.model_validate({})

    normalized = _normalize_sources_payload(payload, path)

    try:
        return BlockSourcesConfig.model_validate(normalized)
    except ValidationError as exc:
        raise ValueError(f"Invalid block sources config in {path}:\n{exc}") from exc


def _env_settings_for(block_cls: type[Block], source: EnvBlockSource) -> tuple[type[BaseSettings], dict[str, str]]:
    """Build a settings class keyed by source field names plus a source->block field mapping."""
    source_to_block: dict[str, str] = {}
    definitions: dict[str, tuple[Any, Any]] = {}

    for name, model_field in block_cls.model_fields.items():
        source_name = source.fields.get(name, name)
        source_to_block[source_name] = name

        if model_field.default_factory is not None:
            default = Field(default_factory=model_field.default_factory)
        elif model_field.is_required():
            default = ...
        else:
            default = model_field.default

        definitions[source_name] = (model_field.annotation, default)

    settings_cls = create_model(
        f"{block_cls.__name__}Settings",
        __base__=BaseSettings,
        **definitions,
    )
    settings_cls.model_config = SettingsConfigDict(
        env_prefix=source.env_var_prefix,
        env_nested_delimiter="__",
        extra="ignore",
    )
    return settings_cls, source_to_block


def _build_from_env(block_name: str, block_cls: type[Block], source: EnvBlockSource) -> Block:
    settings_cls, source_to_block = _env_settings_for(block_cls, source)
    try:
        settings = settings_cls()
    except ValidationError as exc:
        raise BlockBuildError(block_name, settings_cls, exc) from exc
    values = {block_field: getattr(settings, src_field) for src_field, block_field in source_to_block.items()}
    return block_cls(**values)


def _keeper_record_title(source: KeeperBlockSource) -> str:
    parts = [p for p in [source.record_prefix, source.record_title, source.record_suffix] if p]
    return source.separator.join(parts)


def _build_from_keeper(block_name: str, block_cls: type[Block], source: KeeperBlockSource) -> Block:
    try:
        from keeper_secrets_manager_core import SecretsManager  # noqa: PLC0415
        from keeper_secrets_manager_core.storage import InMemoryKeyValueStorage  # noqa: PLC0415
    except ImportError as exc:
        raise ImportError(
            "Keeper Secrets Manager support requires the keeper extra. "
            "Install it with your package manager of choice: prefector[keeper]"
        ) from exc

    token = source.ksm_token or None
    if token:
        storage = InMemoryKeyValueStorage(token)
        sm = SecretsManager(token=token, config=storage)
    else:
        sm = SecretsManager()

    record_title = _keeper_record_title(source)
    record = sm.get_secret_by_title(record_title)
    if record is None:
        raise ValueError(f"No Keeper record found with title '{record_title}' for block '{block_name}'")

    values: dict[str, Any] = {}
    for field_name in block_cls.model_fields:
        source_name = source.fields.get(field_name, field_name)
        value = _keeper_field_value(record, source_name)
        if value is not None:
            values[field_name] = value

    try:
        return block_cls(**values)
    except ValidationError as exc:
        missing = [str(e["loc"][0]) for e in exc.errors() if e["type"] == "missing"]
        raise ValueError(
            f"Failed to build block '{block_name}' from Keeper record '{record_title}': "
            f"missing required fields: {', '.join(missing)}"
        ) from None


def _keeper_field_value(record: Any, name: str) -> Any:
    """Look up a field from a Keeper record by name.

    Checks standard fields (matched by type) then custom fields (matched by label).
    Uses record.dict to access the raw field data, matching the structure returned
    by the KSM SDK: {"type": ..., "value": [...]} for standard fields and
    {"label": ..., "value": [...]} for custom fields.
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


def source_step_label(source: BlockSource, source_path: Path | None = None) -> str:
    """Return a Rich-formatted step label describing where the block's values come from."""
    resolved = _interpolate_source(source, source_path)
    if isinstance(resolved, EnvBlockSource):
        prefix = resolved.env_var_prefix or "(no prefix)"
        return f"Reading from environment [dim](prefix: {prefix})[/dim]"
    record_title = _keeper_record_title(resolved)
    return f"Fetching from Keeper [dim]({record_title})[/dim]"


def build_block_from_source(
    block_name: str,
    block_cls: type[Block],
    source: BlockSource,
    source_path: Path | None = None,
) -> Block:
    resolved = _interpolate_source(source, source_path)
    if isinstance(resolved, EnvBlockSource):
        return _build_from_env(block_name, block_cls, resolved)
    return _build_from_keeper(block_name, block_cls, resolved)
