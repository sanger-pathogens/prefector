import json
from pathlib import Path
from typing import Any, Optional

import click
import requests
from prefect.settings import (
    PREFECT_API_AUTH_STRING,
    PREFECT_API_SSL_CERT_FILE,
    PREFECT_API_URL,
    PREFECT_CLIENT_CUSTOM_HEADERS,
)
from prefect.settings.legacy import Setting

from prefector.prefect_connection.options import PrefectConnectionArgs


def detect_auth_mode(args: PrefectConnectionArgs) -> Optional[str]:
    flags = {
        "api": args.api_auth_string is not None,
        "password": args.keycloak_username is not None or args.keycloak_password is not None,
        "client": args.keycloak_client_id is not None or args.keycloak_client_secret is not None,
    }
    modes = [name for name, enabled in flags.items() if enabled]
    if len(modes) > 1:
        raise click.UsageError(
            "Auth options are mutually exclusive. If required, provide one of:\n --api-auth-string, "
            "\n --keycloak-username & --keycloak-password, \n --keycloak-client-id & --keycloak-client-secret."
        )
    return modes[0] if modes else None


def build_keycloak_form_data(args: PrefectConnectionArgs, mode: str) -> dict[str, str]:
    if mode == "password":
        if not args.keycloak_username or not args.keycloak_password:
            raise ValueError("Both --keycloak-username and --keycloak-password are required together.")
        return {
            "grant_type": "password",
            "client_id": args.keycloak_direct_grant_client_id,
            "username": args.keycloak_username,
            "password": args.keycloak_password,
            "scope": "openid profile email groups",
        }

    if mode == "client":
        if not args.keycloak_client_id or not args.keycloak_client_secret:
            raise ValueError("Both --keycloak-client-id and --keycloak-client-secret are required together.")
        return {
            "grant_type": "client_credentials",
            "client_id": args.keycloak_client_id,
            "client_secret": args.keycloak_client_secret,
        }

    raise ValueError(f"Unknown auth mode: {mode}")


def exchange_keycloak_token(*, token_url: str, form_data: dict[str, str], ssl_cert: Path = None) -> str:
    response = requests.post(
        token_url,
        data=form_data,
        timeout=30,
        verify=ssl_cert,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )

    try:
        response.raise_for_status()
    except requests.HTTPError as e:
        raise RuntimeError(f"Keycloak token request failed: {response.text}") from e

    data = response.json()

    token = data.get("access_token")
    if isinstance(token, str) and token.strip():
        return token

    message = data.get("error_description") or data.get("error") or "missing access_token"
    raise ValueError(f"Keycloak token response was invalid: {message}.")


def set_bearer_token(token: str):
    return json.dumps({"Authorization": f"Bearer {token}"})


def generate_prefect_settings(args: PrefectConnectionArgs) -> dict[Setting, Any]:
    settings = {
        PREFECT_API_URL: args.api_url,
    }

    ssl_cert = getattr(args, "ssl_cert", None)
    if ssl_cert:
        if not ssl_cert.exists():
            raise click.UsageError(f"SSL certificate file does not exist: {ssl_cert}")
        settings[PREFECT_API_SSL_CERT_FILE] = str(ssl_cert)

    mode = detect_auth_mode(args)

    if mode == "api":
        settings[PREFECT_API_AUTH_STRING] = args.api_auth_string

    elif mode in {"password", "client"}:
        if not args.keycloak_token_url:
            raise click.UsageError("Keycloak token URL is required when using keycloak auth.")
        token = exchange_keycloak_token(
            token_url=args.keycloak_token_url,
            ssl_cert=ssl_cert,
            form_data=build_keycloak_form_data(args, mode),
        )
        settings[PREFECT_CLIENT_CUSTOM_HEADERS] = set_bearer_token(token)

    return settings
