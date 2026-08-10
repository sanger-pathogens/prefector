from typing import Any, Callable, Optional

from pydantic import Field


def apply_nested_fields(d: dict[str, Any], nested_fields: dict[str, str], lookup: Callable[[str], Any]) -> None:
    """Populate one sub-field of a nested model field from a flat lookup, in place.

    Used by settings sources that only resolve top-level fields (env var names, Keeper
    record fields) but need to reach one sub-field of a nested model field — for example
    Prefect's built-in `AwsCredentials.aws_client_parameters.endpoint_url`.

    Args:
        d: The dict of resolved top-level field values being built by the source.
            Modified in place; existing entries (for this or other fields) are preserved.
        nested_fields: Maps a dotted `"<field>.<subfield>"` path to a key passed to
            `lookup` (an env var name, a Keeper field/custom name, etc).
        lookup: Called with each `nested_fields` value; returns the resolved value, or
            `None` if it isn't available, in which case that sub-field is left unset and
            falls back to its own default.
    """
    for path, key in nested_fields.items():
        field_name, _, subfield_name = path.partition(".")
        value = lookup(key)
        if value is None:
            continue
        nested = d.setdefault(field_name, {})
        if isinstance(nested, dict):
            nested[subfield_name] = value


def split_nested_field_aliases(field_aliases: Optional[dict[str, str]]) -> tuple[dict[str, str], dict[str, str]]:
    """Split a `field_aliases` dict into flat aliases and dotted `nested_fields` entries.

    A dotted key (`"<field>.<subfield>"`) can never be an actual field name, so it
    unambiguously marks a `nested_fields` entry rather than a flat rename.

    Args:
        field_aliases: A mapping that may contain both flat (`"field"`) and dotted
            (`"field.subfield"`) keys.

    Returns:
        A `(flat_aliases, nested_fields)` tuple, each a plain `dict[str, str]`.
    """
    field_aliases = field_aliases or {}
    flat = {k: v for k, v in field_aliases.items() if "." not in k}
    nested = {k: v for k, v in field_aliases.items() if "." in k}
    return flat, nested


def field_definitions_for_block(
    block_cls,
    field_types: Optional[dict[str, type[Any]]] = None,
    field_aliases: Optional[dict[str, str]] = None,
) -> dict[str, tuple[Any, Any]]:
    """Reflect a Block/pydantic model's fields into `pydantic.create_model` definitions.

    Reproduces each field's annotation, default (or default factory), and required-ness
    on a new model, so a settings class can be generated from a Block.

    Args:
        block_cls: The Block (or any pydantic model) class to reflect over.
        field_types: Overrides a field's annotation by name — useful for nested settings
            structures (for example, replacing a complex block field with another
            settings model).
        field_aliases: Sets/overrides a field's `validation_alias` by name, without
            requiring a Block subclass — useful when the block class itself (e.g. a
            third-party Prefect collection block) shouldn't be modified just to rename a
            field for one source. Overrides any `validation_alias` already on the field.

    Returns:
        A dict of `{field_name: (annotation, default)}`, suitable as `**kwargs` to
        `pydantic.create_model`.
    """
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
