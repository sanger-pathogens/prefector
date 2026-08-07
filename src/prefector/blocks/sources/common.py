from typing import Any, Optional

from pydantic import Field


def field_definitions_for_block(
    block_cls,
    field_types: Optional[dict[str, type[Any]]] = None,
) -> dict[str, tuple[Any, Any]]:
    field_types = field_types or {}
    definitions: dict[str, tuple[Any, Any]] = {}
    for name, model_field in block_cls.model_fields.items():
        annotation = field_types.get(name, model_field.annotation)

        if model_field.default_factory is not None:
            default = Field(default_factory=model_field.default_factory)
        elif model_field.is_required():
            default = ...
        else:
            default = model_field.default

        definitions[name] = (annotation, default)
    return definitions
