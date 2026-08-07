from prefect.blocks.core import Block
from pydantic import BaseModel

from prefector.blocks.sources.env import env_settings_model_for_block


def test_env_settings_model_for_block_supports_nested_fields(monkeypatch):
    class NestedValue(BaseModel):
        endpoint_url: str

    class NestedBlock(Block):
        endpoint: NestedValue

    settings_cls = env_settings_model_for_block(
        NestedBlock,
        nested_fields={"endpoint.endpoint_url": "TEST_ENDPOINT_URL"},
    )
    monkeypatch.setenv("TEST_ENDPOINT_URL", "http://service.local")

    payload = settings_cls().model_dump()

    assert payload == {"endpoint": {"endpoint_url": "http://service.local"}}


def test_env_settings_model_for_block_nested_fields_merges_multiple_subfields(monkeypatch):
    class ClientParameters(BaseModel):
        endpoint_url: str | None = None
        api_version: str | None = None

    class AwsCredsBlock(Block):
        aws_client_parameters: ClientParameters = ClientParameters()

    settings_cls = env_settings_model_for_block(
        AwsCredsBlock,
        nested_fields={
            "aws_client_parameters.endpoint_url": "AWS_HOSTNAME",
            "aws_client_parameters.api_version": "AWS_API_VERSION",
        },
    )
    monkeypatch.setenv("AWS_HOSTNAME", "minio.local:9000")
    monkeypatch.setenv("AWS_API_VERSION", "2016-11-15")

    settings = settings_cls()

    assert settings.aws_client_parameters.endpoint_url == "minio.local:9000"
    assert settings.aws_client_parameters.api_version == "2016-11-15"


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


def test_env_settings_model_for_block_nested_fields_missing_var_keeps_default(monkeypatch):
    class ClientParameters(BaseModel):
        endpoint_url: str | None = None

    class AwsCredsBlock(Block):
        aws_client_parameters: ClientParameters = ClientParameters()

    settings_cls = env_settings_model_for_block(
        AwsCredsBlock,
        nested_fields={"aws_client_parameters.endpoint_url": "AWS_HOSTNAME"},
    )
    monkeypatch.delenv("AWS_HOSTNAME", raising=False)

    settings = settings_cls()

    assert settings.aws_client_parameters.endpoint_url is None
