from typing import Any, Callable, Optional

from pydantic import Field


def apply_nested_fields(d: dict[str, Any], nested_fields: dict[str, str], lookup: Callable[[str], Any]) -> None:
    for path, key in nested_fields.items():
        field_name, _, subfield_name = path.partition(".")
        value = lookup(key)
        if value is None:
            continue
        nested = d.setdefault(field_name, {})
        if isinstance(nested, dict):
            nested[subfield_name] = value


def field_definitions_for_block(
    block_cls,
    field_types: Optional[dict[str, type[Any]]] = None,
    field_aliases: Optional[dict[str, str]] = None,
) -> dict[str, tuple[Any, Any]]:
    field_types = field_types or {}
    field_aliases = field_aliases or {}
    definitions: dict[str, tuple[Any, Any]] = {}
    for name, model_field in block_cls.model_fields.items():
        annotation = field_types.get(name, model_field.annotation)
        alias = field_aliases.get(name, model_field.validation_alias)
        alias_kwargs = {"validation_alias": alias} if alias else {}

        if model_field.default_factory is not None:
            default = Field(default_factory=model_field.default_factory, **alias_kwargs)
        elif model_field.is_required():
            default = Field(..., **alias_kwargs) if alias_kwargs else ...
        else:
            default = Field(default=model_field.default, **alias_kwargs) if alias_kwargs else model_field.default

        definitions[name] = (annotation, default)
    return definitions
