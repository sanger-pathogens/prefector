import functools
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import click
from click_option_group import optgroup


@dataclass
class PrefectConnectionArgs:
    api_url: str
    ssl_cert: Optional[Path]
    api_auth_string: Optional[str]
    keycloak_token_url: Optional[str]
    keycloak_username: Optional[str]
    keycloak_password: Optional[str]
    keycloak_direct_grant_client_id: str
    keycloak_client_id: Optional[str]
    keycloak_client_secret: Optional[str]


_CONNECTION_KEYS = {f.name for f in PrefectConnectionArgs.__dataclass_fields__.values()}


def prefect_connection_options(f):
    @optgroup.group("Prefect Connection")
    @optgroup.option("--api-url", envvar="PREFECT_API_URL", required=True, help="Or set PREFECT_API_URL.")
    @optgroup.option(
        "--ssl-cert", type=click.Path(path_type=Path), envvar="SSL_CERT_FILE", help="Or set SSL_CERT_FILE."
    )
    @optgroup.option("--keycloak-token-url", help="Keycloak token URL. Provide only when using Keycloak auth.")
    @optgroup.group("Basic Prefect Auth")
    @optgroup.option("--api-auth-string", envvar="PREFECT_API_AUTH_STRING")
    @optgroup.group("Keycloak Operator Auth")
    @optgroup.option("--keycloak-username", help="Keycloak username.")
    @optgroup.option("--keycloak-password", help="Keycloak password.")
    @optgroup.option(
        "--keycloak-direct-grant-client-id",
        default="prefect-cli",
        show_default=True,
        help="Keycloak direct-grant client ID for operator login.",
    )
    @optgroup.group("Keycloak Application Auth")
    @optgroup.option(
        "--keycloak-client-id",
        help="Keycloak client ID.",
    )
    @optgroup.option(
        "--keycloak-client-secret",
        help="Keycloak client secret",
    )
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        conn = PrefectConnectionArgs(**{k: kwargs.pop(k) for k in _CONNECTION_KEYS})
        return f(*args, connection=conn, **kwargs)

    return wrapper
