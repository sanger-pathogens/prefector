import argparse
import json

import pytest
import requests
from prefect.settings import (
    PREFECT_API_AUTH_STRING,
    PREFECT_API_SSL_CERT_FILE,
    PREFECT_API_URL,
    PREFECT_CLIENT_CUSTOM_HEADERS,
)

from prefector.argparse import prefect_connection as pc


class _Response:
    def __init__(self, *, status_code=200, text="", payload=None, json_error: Exception | None = None):
        self.status_code = status_code
        self.text = text
        self._payload = {} if payload is None else payload
        self._json_error = json_error

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")

    def json(self):
        if self._json_error is not None:
            raise self._json_error
        return self._payload


@pytest.fixture(scope="module")
def prefect_args_defaults():
    return {
        "api_url": "http://prefect.localhost:24200/api",
        "ssl_cert": None,
        "api_auth_string": None,
        "keycloak_token_url": None,
        "keycloak_username": None,
        "keycloak_password": None,
        "keycloak_direct_grant_client_id": "prefect-cli",
        "keycloak_client_id": None,
        "keycloak_client_secret": None,
    }


@pytest.fixture
def build_args(prefect_args_defaults):
    def _build(**overrides):
        return argparse.Namespace(**(prefect_args_defaults | overrides))

    return _build


@pytest.fixture
def mock_post_response(monkeypatch):
    def _mock(*, status_code=200, text="", payload=None, json_error=None, exc=None):
        def _post(*_args, **_kwargs):
            if exc is not None:
                raise exc
            return _Response(status_code=status_code, text=text, payload=payload, json_error=json_error)

        monkeypatch.setattr(pc.requests, "post", _post)

    return _mock


def test_exchange_keycloak_token_returns_access_token(mock_post_response):
    mock_post_response(payload={"access_token": "token-123"})

    token = pc.exchange_keycloak_token(token_url="https://keycloak/token", form_data={"grant_type": "password"})
    assert token == "token-123"


def test_exchange_keycloak_token_raises_on_http_error(mock_post_response):
    mock_post_response(status_code=401, text='{"error":"unauthorized"}')

    with pytest.raises(requests.HTTPError, match="HTTP 401"):
        pc.exchange_keycloak_token(token_url="https://keycloak/token", form_data={})


def test_exchange_keycloak_token_raises_on_request_exception(mock_post_response):
    mock_post_response(exc=requests.RequestException("connection failed"))

    with pytest.raises(requests.RequestException, match="connection failed"):
        pc.exchange_keycloak_token(token_url="https://keycloak/token", form_data={})


def test_exchange_keycloak_token_raises_on_non_json(mock_post_response):
    mock_post_response(json_error=ValueError("not json"))

    with pytest.raises(ValueError, match="not json"):
        pc.exchange_keycloak_token(token_url="https://keycloak/token", form_data={})


@pytest.mark.parametrize(
    ("payload", "error_part"),
    [
        ({}, "missing access_token"),
        ({"error": "invalid_grant"}, "invalid_grant"),
        ({"error_description": "bad credentials"}, "bad credentials"),
    ],
)
def test_exchange_keycloak_token_raises_when_access_token_missing(mock_post_response, payload, error_part):
    mock_post_response(payload=payload)
    with pytest.raises(ValueError, match=error_part):
        pc.exchange_keycloak_token(token_url="https://keycloak/token", form_data={})


def test_set_bearer_token_returns_headers_json():
    headers_json = pc.set_bearer_token("bearer-123")
    assert json.loads(headers_json) == {"Authorization": "Bearer bearer-123"}


def test_generate_prefect_settings_basic_auth(monkeypatch, build_args):
    called = {"token": False}

    def _token(*_args, **_kwargs):
        called["token"] = True
        return "unused"

    monkeypatch.setattr(pc, "exchange_keycloak_token", _token)
    settings = pc.generate_prefect_settings(build_args(api_auth_string="user:pass"))

    assert settings[PREFECT_API_URL] == "http://prefect.localhost:24200/api"
    assert settings[PREFECT_API_AUTH_STRING] == "user:pass"
    assert PREFECT_CLIENT_CUSTOM_HEADERS not in settings
    assert called["token"] is False


def test_generate_prefect_settings_operator_keycloak(monkeypatch, build_args):
    captured = {}

    def _token(*, token_url, form_data, ssl_cert):
        captured["token_url"] = token_url
        captured["form_data"] = form_data
        captured["ssl_cert"] = ssl_cert
        return "op-token"

    monkeypatch.setattr(pc, "exchange_keycloak_token", _token)
    settings = pc.generate_prefect_settings(
        build_args(
            keycloak_token_url="https://keycloak/token",
            keycloak_username="alice",
            keycloak_password="secret",
            keycloak_direct_grant_client_id="prefect-cli",
        )
    )

    assert captured["token_url"] == "https://keycloak/token"
    assert captured["form_data"]["grant_type"] == "password"
    assert captured["form_data"]["client_id"] == "prefect-cli"
    assert captured["form_data"]["username"] == "alice"
    assert captured["form_data"]["password"] == "secret"
    assert captured["form_data"]["scope"] == "openid profile email groups"
    assert captured["ssl_cert"] is None
    assert json.loads(settings[PREFECT_CLIENT_CUSTOM_HEADERS]) == {"Authorization": "Bearer op-token"}
    assert PREFECT_API_AUTH_STRING not in settings


def test_generate_prefect_settings_client_credentials_keycloak(monkeypatch, build_args):
    captured = {}

    def _token(*, token_url, form_data, ssl_cert):
        captured["token_url"] = token_url
        captured["form_data"] = form_data
        captured["ssl_cert"] = ssl_cert
        return "ci-token"

    monkeypatch.setattr(pc, "exchange_keycloak_token", _token)
    settings = pc.generate_prefect_settings(
        build_args(
            keycloak_token_url="https://keycloak/token",
            keycloak_client_id="prefect-automation",
            keycloak_client_secret="secret-123",
        )
    )

    assert captured["token_url"] == "https://keycloak/token"
    assert captured["form_data"] == {
        "grant_type": "client_credentials",
        "client_id": "prefect-automation",
        "client_secret": "secret-123",
    }
    assert captured["ssl_cert"] is None
    assert json.loads(settings[PREFECT_CLIENT_CUSTOM_HEADERS]) == {"Authorization": "Bearer ci-token"}
    assert PREFECT_API_AUTH_STRING not in settings


def test_generate_prefect_settings_rejects_multiple_auth_modes(build_args):
    with pytest.raises(ValueError, match="mutually exclusive"):
        pc.generate_prefect_settings(
            build_args(
                api_auth_string="user:pass",
                keycloak_username="alice",
                keycloak_password="secret",
            )
        )


@pytest.mark.parametrize(
    "override",
    [
        {"keycloak_username": "alice"},
        {"keycloak_password": "secret"},
        {"keycloak_client_id": "prefect-automation"},
        {"keycloak_client_secret": "secret-123"},
    ],
)
def test_generate_prefect_settings_rejects_incomplete_credential_pairs(override, build_args):
    with pytest.raises(ValueError, match="required together"):
        pc.generate_prefect_settings(build_args(keycloak_token_url="https://keycloak/token", **override))


def test_generate_prefect_settings_requires_keycloak_token_url_for_keycloak_modes(build_args):
    with pytest.raises(ValueError, match="Keycloak token URL is required"):
        pc.generate_prefect_settings(build_args(keycloak_username="alice", keycloak_password="secret"))


def test_generate_prefect_settings_allows_no_auth_mode(build_args):
    settings = pc.generate_prefect_settings(build_args())
    assert settings[PREFECT_API_URL] == "http://prefect.localhost:24200/api"
    assert PREFECT_API_AUTH_STRING not in settings
    assert PREFECT_CLIENT_CUSTOM_HEADERS not in settings


def test_generate_prefect_settings_includes_ssl_cert_when_exists(tmp_path, build_args):
    cert = tmp_path / "ca.crt"
    cert.write_text("dummy cert", encoding="utf-8")
    settings = pc.generate_prefect_settings(build_args(ssl_cert=cert))
    assert settings[PREFECT_API_SSL_CERT_FILE] == str(cert)


def test_generate_prefect_settings_rejects_missing_ssl_cert(tmp_path, build_args):
    missing = tmp_path / "does-not-exist-ca.crt"
    with pytest.raises(ValueError, match="SSL certificate file does not exist"):
        pc.generate_prefect_settings(build_args(ssl_cert=missing))


def test_attach_prefect_connection_options_requires_api_url_without_env(monkeypatch, capsys):
    monkeypatch.delenv("PREFECT_API_URL", raising=False)
    parser = argparse.ArgumentParser()
    pc.attach_prefect_connection_options(parser)
    with pytest.raises(SystemExit) as exc:
        parser.parse_args([])
    err = capsys.readouterr().err
    assert exc.value.code == 2
    assert "the following arguments are required: --api-url" in err
