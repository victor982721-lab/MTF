"""Pure, JSON-ready command helpers for cTrader activation CLI wiring.

The actual CLI owns filesystem/environment access.  These functions consume
only supplied facts and never perform network, browser or token-store I/O.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence, Set
from datetime import UTC, datetime
from typing import Any

from .ctrader_activation import (
    AccountDiscovery,
    ActivationProfile,
    BrokerAccount,
    OAuthAppConfig,
    TokenMetadata,
    build_authorization_url,
    evaluate_activation,
    record_account_discovery,
    select_discovered_demo_account,
)


def _sections(config: Mapping[str, Any]) -> tuple[ActivationProfile, OAuthAppConfig]:
    activation_raw = config.get("ctrader", config)
    if not isinstance(activation_raw, Mapping):
        raise ValueError("[ctrader] debe ser una tabla")
    oauth_raw = config.get("ctrader_oauth")
    if not isinstance(oauth_raw, Mapping):
        raise ValueError("[ctrader_oauth] debe ser una tabla")
    return ActivationProfile.from_mapping(activation_raw), OAuthAppConfig.from_mapping(oauth_raw)


def status_command(
    config: Mapping[str, Any],
    *,
    token_metadata: TokenMetadata | None = None,
    accounts: Sequence[BrokerAccount | Mapping[str, Any]] = (),
    present_env_keys: Set[str] = frozenset(),
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return a redacted status payload suitable for a CLI or local UI."""

    profile, app = _sections(config)
    status = evaluate_activation(
        profile,
        token=token_metadata,
        accounts=accounts,
        present_env_keys=present_env_keys,
        app=app,
        now=now,
    )
    return {
        "component": "ctrader_activation",
        "network_performed": False,
        "browser_opened": False,
        "profile": profile.redacted(),
        "oauth": app.redacted(present_env_keys),
        "token": token_metadata.to_dict() if token_metadata else None,
        "status": status.to_dict(),
    }


def account_discovery_command(
    token_metadata: TokenMetadata,
    accounts: Sequence[BrokerAccount | Mapping[str, Any]],
    *,
    observed_at: datetime | None = None,
    permission_scope: str | int | None = None,
) -> AccountDiscovery:
    """Normalize an account inventory observed by the caller; perform no I/O."""

    return record_account_discovery(
        token_metadata,
        accounts,
        observed_at=observed_at,
        permission_scope=permission_scope,
    )


def select_account_command(
    config: Mapping[str, Any],
    discovery: AccountDiscovery,
    *,
    account_id: str,
    environment: str,
) -> dict[str, Any]:
    """Validate selection from a prior discovery snapshot and write nothing."""

    if not isinstance(discovery, AccountDiscovery):
        raise ValueError("se requiere AccountDiscovery observado antes de seleccionar")
    profile, _ = _sections(config)
    selected = select_discovered_demo_account(
        discovery,
        account_id,
        environment=environment,
        token_ref=profile.token_ref,
    )
    return {
        "accepted": True,
        "network_performed": False,
        "discovery": discovery.redacted(),
        "selected_account": selected.redacted(),
        "config_patch": {
            "ctrader": {
                "environment": "DEMO",
                "account_id": selected.account_id,
                "account_selected": True,
                "operation_mode": profile.operation_mode.value.lower(),
            }
        },
        "next_action": "Revisa el parche y persístelo explícitamente; no se modificó ningún archivo.",
    }


def authorization_start_command(
    config: Mapping[str, Any],
    *,
    client_id: str,
    state: str,
    requested_scopes: Sequence[str],
) -> dict[str, Any]:
    """Build an authorization handoff; never opens a browser."""

    profile, app = _sections(config)
    scopes = tuple(str(scope).strip().lower() for scope in requested_scopes)
    if not scopes or not frozenset(scopes).issubset(profile.required_scopes):
        raise ValueError("requested_scopes debe ser un único scope permitido por el perfil efectivo")
    return {
        "authorization_url": build_authorization_url(
            app,
            client_id=client_id,
            scope=scopes,
            state=state,
        ),
        "browser_opened": False,
        "requested_scopes": sorted(scopes),
        "next_action": "Abre la URL manualmente o usa la API con allow_browser=True explícito.",
    }


def token_rotation_plan_command(
    config: Mapping[str, Any],
    *,
    granted_scopes: Sequence[str],
    expires_at: datetime,
) -> dict[str, Any]:
    """Describe a safe rotation without receiving or exposing token values."""

    profile, _ = _sections(config)
    scopes = sorted({str(scope).strip().lower() for scope in granted_scopes})
    return {
        "operation": "atomic_token_rotation",
        "token_ref": profile.token_ref,
        "token_store_dir": profile.token_store_dir,
        "granted_scopes": scopes,
        "expires_at": expires_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "requires_secret_input_via_runtime": True,
        "secret_values": "REDACTED",
        "network_performed": False,
        "next_action": "Entrega los tokens al runtime seguro, nunca como argumento visible ni valor TOML.",
    }


__all__ = [
    "account_discovery_command",
    "authorization_start_command",
    "select_account_command",
    "status_command",
    "token_rotation_plan_command",
]
