"""Pure, JSON-ready command helpers for cTrader activation CLI wiring.

The actual CLI owns filesystem/environment access.  These functions consume
only supplied facts and never perform network, browser or token-store I/O.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence, Set
from datetime import UTC, datetime
from typing import Any

from .ctrader_activation import (
    ActivationProfile,
    BrokerAccount,
    OAuthAppConfig,
    TokenMetadata,
    evaluate_activation,
    select_demo_account,
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


def select_account_command(
    config: Mapping[str, Any],
    accounts: Sequence[BrokerAccount | Mapping[str, Any]],
    *,
    account_id: str,
    environment: str,
) -> dict[str, Any]:
    """Validate an explicit selection and return the TOML patch; write nothing."""

    profile, _ = _sections(config)
    selected = select_demo_account(accounts, account_id, environment=environment)
    return {
        "accepted": True,
        "network_performed": False,
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


__all__ = ["select_account_command", "status_command", "token_rotation_plan_command"]
