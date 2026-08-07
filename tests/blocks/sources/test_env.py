from prefect.blocks.core import Block
from pydantic import BaseModel

from prefector.blocks.sources.env import env_settings_model_for_block


def test_env_settings_model_for_block_supports_nested_env(monkeypatch):
    class NestedValue(BaseModel):
        endpoint_url: str

    class NestedBlock(Block):
        endpoint: NestedValue

    settings_cls = env_settings_model_for_block(
        NestedBlock,
        env_prefix="TEST_",
        env_nested_delimiter="__",
    )
    monkeypatch.setenv("TEST_ENDPOINT__ENDPOINT_URL", "http://service.local")

    payload = settings_cls().model_dump()

    assert payload == {"endpoint": {"endpoint_url": "http://service.local"}}


def test_env_settings_model_for_block_field_aliases_renames_without_block_subclass(monkeypatch):
    class ThirdPartyBlock(Block):
        aws_access_key_id: str

    settings_cls = env_settings_model_for_block(
        ThirdPartyBlock,
        field_aliases={"aws_access_key_id": "ACCESS_KEY"},
    )
    monkeypatch.setenv("ACCESS_KEY", "AKIA...")

    settings = settings_cls()

    assert settings.aws_access_key_id == "AKIA..."
