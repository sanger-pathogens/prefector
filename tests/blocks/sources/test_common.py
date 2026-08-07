from prefect.blocks.core import Block
from pydantic import Field, create_model
from pydantic.fields import FieldInfo
from pydantic_settings import BaseSettings

from prefector.blocks.sources.common import field_definitions_for_block


class _Block(Block):
    required_field: str
    aliased_field: str = Field(validation_alias="other_name")
    defaulted_field: int = 8080
    factory_field: list[str] = Field(default_factory=list)


def test_required_field_has_no_default():
    annotation, default = field_definitions_for_block(_Block)["required_field"]
    assert annotation is str
    assert default is ...


def test_static_default_is_preserved():
    annotation, default = field_definitions_for_block(_Block)["defaulted_field"]
    assert annotation is int
    assert default == 8080


def test_default_factory_is_preserved():
    _, default = field_definitions_for_block(_Block)["factory_field"]
    assert isinstance(default, FieldInfo)
    assert default.default_factory() == []


def test_validation_alias_is_preserved():
    _, default = field_definitions_for_block(_Block)["aliased_field"]
    assert isinstance(default, FieldInfo)
    assert default.validation_alias == "other_name"


def test_field_types_overrides_annotation():
    annotation, _ = field_definitions_for_block(_Block, field_types={"required_field": int})["required_field"]
    assert annotation is int


def test_field_aliases_sets_alias_on_unaliased_field():
    _, default = field_definitions_for_block(_Block, field_aliases={"required_field": "renamed"})["required_field"]
    assert isinstance(default, FieldInfo)
    assert default.validation_alias == "renamed"
    assert default.is_required()


def test_field_aliases_overrides_existing_validation_alias():
    definitions = field_definitions_for_block(_Block, field_aliases={"aliased_field": "different_name"})
    _, default = definitions["aliased_field"]
    assert default.validation_alias == "different_name"


def test_field_aliases_lets_unmodified_third_party_block_be_renamed():
    """A field can be renamed for one source without subclassing the block class."""
    definitions = field_definitions_for_block(_Block, field_aliases={"defaulted_field": "port_override"})
    settings_cls = create_model("GeneratedSettings", __base__=BaseSettings, **definitions)

    settings = settings_cls(required_field="a", other_name="b", port_override=9090)

    assert settings.defaulted_field == 9090


def test_generated_model_honours_validation_alias():
    definitions = field_definitions_for_block(_Block)
    settings_cls = create_model("GeneratedSettings", __base__=BaseSettings, **definitions)

    settings = settings_cls(required_field="a", other_name="b")

    assert settings.aliased_field == "b"
