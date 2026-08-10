import click
import pytest
from click.testing import CliRunner

from prefector.prefect_connection.connection import detect_auth_mode
from prefector.prefect_connection.options import (
    PrefectConnectionArgs,
    prefect_connection_options,
)


@click.command()
@prefect_connection_options
def _cmd(connection: PrefectConnectionArgs):
    click.echo(f"api_url={connection.api_url}")
    click.echo(f"api_auth_string={connection.api_auth_string}")
    click.echo(f"direct_grant_client_id={connection.keycloak_direct_grant_client_id}")


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture(scope="module")
def connection_defaults():
    return {
        "api_url": "http://localhost:4200/api",
        "ssl_cert": None,
        "api_auth_string": None,
        "keycloak_token_url": None,
        "keycloak_scope": "openid profile email",
        "keycloak_username": None,
        "keycloak_password": None,
        "keycloak_direct_grant_client_id": "prefect-cli",
        "keycloak_client_id": None,
        "keycloak_client_secret": None,
    }


@pytest.fixture
def build_connection(connection_defaults):
    def _build(**overrides):
        return PrefectConnectionArgs(**(connection_defaults | overrides))

    return _build


def test_detect_auth_mode_returns_none_with_no_auth(build_connection):
    assert detect_auth_mode(build_connection()) is None


def test_detect_auth_mode_returns_api_when_api_auth_string_set(build_connection):
    assert detect_auth_mode(build_connection(api_auth_string="user:pass")) == "api"


@pytest.mark.parametrize(
    "overrides",
    [
        {"keycloak_username": "alice"},
        {"keycloak_password": "secret"},
        {"keycloak_username": "alice", "keycloak_password": "secret"},
    ],
)
def test_detect_auth_mode_returns_password(build_connection, overrides):
    assert detect_auth_mode(build_connection(**overrides)) == "password"


@pytest.mark.parametrize(
    "overrides",
    [
        {"keycloak_client_id": "my-app"},
        {"keycloak_client_secret": "s3cr3t"},
        {"keycloak_client_id": "my-app", "keycloak_client_secret": "s3cr3t"},
    ],
)
def test_detect_auth_mode_returns_client(build_connection, overrides):
    assert detect_auth_mode(build_connection(**overrides)) == "client"


@pytest.mark.parametrize(
    "overrides",
    [
        {"api_auth_string": "user:pass", "keycloak_username": "alice"},
        {"api_auth_string": "user:pass", "keycloak_client_id": "my-app"},
        {"keycloak_username": "alice", "keycloak_client_id": "my-app"},
        {
            "api_auth_string": "user:pass",
            "keycloak_username": "alice",
            "keycloak_client_id": "my-app",
        },
    ],
)
def test_detect_auth_mode_raises_on_multiple_modes(build_connection, overrides):
    with pytest.raises(click.UsageError, match="mutually exclusive"):
        detect_auth_mode(build_connection(**overrides))


def test_missing_api_url_exits_nonzero(runner, monkeypatch):
    monkeypatch.delenv("PREFECT_API_URL", raising=False)
    result = runner.invoke(_cmd, [])
    assert result.exit_code != 0
    assert "Missing option '--api-url'" in result.output


def test_api_url_from_cli_flag(runner, monkeypatch):
    monkeypatch.delenv("PREFECT_API_URL", raising=False)
    result = runner.invoke(_cmd, ["--api-url", "http://cli-flag/api"])
    assert result.exit_code == 0
    assert "api_url=http://cli-flag/api" in result.output


def test_api_url_from_env_var(runner, monkeypatch):
    monkeypatch.setenv("PREFECT_API_URL", "http://from-env/api")
    result = runner.invoke(_cmd, [])
    assert result.exit_code == 0
    assert "api_url=http://from-env/api" in result.output


def test_api_auth_string_from_env_var(runner, monkeypatch):
    monkeypatch.setenv("PREFECT_API_URL", "http://test/api")
    monkeypatch.setenv("PREFECT_API_AUTH_STRING", "user:secret")
    result = runner.invoke(_cmd, [])
    assert result.exit_code == 0
    assert "api_auth_string=user:secret" in result.output


def test_cli_flag_overrides_env_var_for_api_url(runner, monkeypatch):
    monkeypatch.setenv("PREFECT_API_URL", "http://from-env/api")
    result = runner.invoke(_cmd, ["--api-url", "http://override/api"])
    assert result.exit_code == 0
    assert "api_url=http://override/api" in result.output


def test_keycloak_direct_grant_client_id_defaults_to_prefect_cli(runner, monkeypatch):
    monkeypatch.delenv("PREFECT_API_URL", raising=False)
    result = runner.invoke(_cmd, ["--api-url", "http://test/api"])
    assert result.exit_code == 0
    assert "direct_grant_client_id=prefect-cli" in result.output
