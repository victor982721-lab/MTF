"""Explicit cTrader network composition for ``ctrader supervise``.

This adapter reuses the already verified read-only query preparation and
provider.  It is intentionally called only after the command has an explicit
``network`` source flag; it does not perform OAuth, choose an account, or
construct an execution manager.  The caller owns any safety/executor
callbacks supplied to the supervisor.
"""

from __future__ import annotations

import contextlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

from .application_services import CommandResult
from .ctrader_cli_services import CTraderCliService, QueryContext
from .market_schedule import catalog_provenance


@dataclass(slots=True)
class NetworkPreparation:
    """Authenticated DEMO read-only provider with explicit provenance."""

    provider: Any
    context: QueryContext
    provenance: Mapping[str, Any]
    observed: Mapping[str, Any]
    callbacks: Any | None = None
    binding: Any | None = None
    reconnect_callback: Any | None = None
    _closed: bool = False

    def close(self) -> None:
        self.context.client_secret = ""
        if self._closed:
            return
        self._closed = True
        try:
            if self.binding is not None:
                close_binding = getattr(self.binding, "close", None)
                if callable(close_binding):
                    close_binding()
        finally:
            close = getattr(self.provider, "close", None)
        if callable(close):
            close()

    def reconnect(self) -> Mapping[str, Any]:
        """Re-authenticate the same provider/client after a generation change."""
        callback = self.reconnect_callback
        if not callable(callback):
            raise RuntimeError("la preparación DEMO no tiene callback de reconexión")
        return cast(Mapping[str, Any], callback())


def _provider_endpoint(provider: Any) -> str:
    provider_config = getattr(provider, "config", None)
    host = str(getattr(provider_config, "host", "")).strip().lower()
    port = getattr(provider_config, "port", None)
    if not host or not isinstance(port, int) or isinstance(port, bool):
        raise RuntimeError("la sesión DEMO no conserva endpoint observado")
    return f"{host}:{port}"


def _status_field(provider: Any, name: str, default: Any = None) -> Any:
    status = getattr(provider, "status", None)
    return status.get(name, default) if isinstance(status, Mapping) else getattr(status, name, default)


def _verified_account_in_discovery(observed: Any, account_id: int) -> bool:
    if not isinstance(observed, Mapping):
        return False
    records = observed.get("records", ())
    matches = [
        record
        for record in records
        if isinstance(record, Mapping)
        and str(record.get("account_id", record.get("ctidTraderAccountId", ""))) == str(account_id)
    ]
    return len(matches) == 1 and str(matches[0].get("environment", "")).upper() == "DEMO"


def _validate_reconnect_lease(preparation: NetworkPreparation, execution_requested: bool) -> None:
    profile = preparation.context.profile
    metadata = preparation.context.metadata
    lease = preparation.context.lease
    expected_mode = "DEMO" if execution_requested else "QUERY"
    expected_scopes = {"accounts", "trading"} if execution_requested else {"accounts"}
    if str(profile.environment).upper() != "DEMO" or profile.operation_mode.value != expected_mode:
        raise RuntimeError("el perfil DEMO activo cambió durante la reconexión")
    if set(profile.required_scopes) != expected_scopes:
        raise RuntimeError("los scopes DEMO activos cambiaron durante la reconexión")
    if metadata.token_ref != profile.token_ref or metadata.is_expired(datetime.now(UTC)):
        raise RuntimeError("el token lease DEMO ya no es vigente")
    if getattr(lease, "metadata", None) != metadata or not str(getattr(lease, "access_token", "")):
        raise RuntimeError("el token lease DEMO no coincide con su metadata observada")


def _reconnect_provider(
    preparation: NetworkPreparation, service: CTraderCliService, old_generation: Any
) -> tuple[Any, int, int, str]:
    provider = preparation.provider
    status = provider.reconnect()
    new_generation = getattr(status, "generation", None)
    if new_generation is None:
        new_generation = _status_field(provider, "generation", getattr(provider, "generation", None))
    if isinstance(new_generation, bool) or not isinstance(new_generation, int) or new_generation <= 0:
        raise RuntimeError("la nueva generación DEMO no es observable")
    if isinstance(old_generation, int) and new_generation <= old_generation:
        raise RuntimeError("la reconexión no produjo una generación nueva")
    if getattr(provider, "generation", new_generation) != new_generation:
        raise RuntimeError("provider y cliente no comparten la nueva generación DEMO")
    sequence = preparation.context.sequence
    provider.authenticate(
        secret_provider=service._secret_provider(preparation.context.app, preparation.context.client_secret, sequence),
        token_provider=service._token_provider(preparation.context.profile, preparation.context.lease, sequence),
        authorize_selected=False,
    )
    observed = provider.discover_accounts(include_token=True)
    account_id = int(preparation.context.profile.account_id)
    if not _verified_account_in_discovery(observed, account_id):
        raise RuntimeError("la reconexión cambió la cuenta DEMO observada")
    readonly = service._authorize_readonly_provider(preparation.context, provider, observed)
    if isinstance(readonly, CommandResult):
        raise RuntimeError("la reconexión no superó el gate DEMO de cuenta/permisos")
    provider.subscribe(timeframes=())
    endpoint = _provider_endpoint(provider)
    if getattr(getattr(provider, "client", None), "authenticated_account_id", None) != account_id:
        raise RuntimeError("la reconexión no autenticó la misma cuenta DEMO")
    status_value = getattr(provider, "status", None)
    auth = status_value.get("auth") if isinstance(status_value, Mapping) else getattr(status_value, "auth", None)
    if str(getattr(auth, "value", auth)).upper() != "AUTHENTICATED":
        raise RuntimeError("la reconexión no dejó autenticación DEMO vigente")
    return observed, account_id, new_generation, endpoint


def _reconnect_binding(
    preparation: NetworkPreparation,
    args: Any,
    provider: Any,
    account_id: int,
    endpoint: str,
    execution_requested: bool,
) -> tuple[Any | None, Any | None]:
    binding: Any | None = None
    callbacks: Any | None = None
    if execution_requested:
        from .supervision_demo import build_demo_execution_binding

        binding = build_demo_execution_binding(
            provider,
            {
                "network_performed": True,
                "source_mode": "DEMO_OBSERVED",
                "synthetic": False,
                "environment": "DEMO",
                "account_id": account_id,
                "account_selected": True,
                "account_verified": True,
                "endpoint": endpoint,
            },
            config=preparation.context.config,
            state_dir=args.state_dir,
            resume=bool(getattr(args, "resume", True)),
            account_key=None,
        )
        callbacks = binding.callbacks
    return binding, callbacks


def _reconnect_preparation(
    preparation: NetworkPreparation,
    service: CTraderCliService,
    args: Any,
    execution_requested: bool,
) -> Mapping[str, Any]:
    if preparation._closed:
        raise RuntimeError("la preparación DEMO ya fue cerrada")
    provider = preparation.provider
    old_generation = _status_field(provider, "generation")
    old_endpoint = str(preparation.provenance.get("endpoint", "")).strip().lower()
    old_binding = preparation.binding
    preparation.binding = None
    preparation.callbacks = None
    if old_binding is not None:
        old_binding.close()
    _validate_reconnect_lease(preparation, execution_requested)
    observed, account_id, new_generation, endpoint = _reconnect_provider(preparation, service, old_generation)
    if old_endpoint and endpoint != old_endpoint:
        raise RuntimeError("la reconexión cambió el endpoint DEMO")
    binding, callbacks = _reconnect_binding(preparation, args, provider, account_id, endpoint, execution_requested)
    provenance = {
        **dict(preparation.provenance),
        "network_performed": True,
        "execution_enabled": callbacks is not None,
        "account_id": account_id,
        "account_selected": True,
        "account_verified": True,
        "endpoint": endpoint,
        "connection_generation": new_generation,
        "catalog_observation": {
            **catalog_provenance(provider, datetime.now(UTC)),
            "account_id": account_id,
            "environment": "DEMO",
            "connection_generation": new_generation,
        },
    }
    preparation.observed = {
        "account_count": len(observed.get("records", ())) if isinstance(observed, Mapping) else None,
        "catalog_verified": True,
    }
    preparation.provenance = provenance
    preparation.binding = binding
    preparation.callbacks = callbacks
    return {
        "ok": True,
        "callbacks": callbacks,
        "provenance": provenance,
        "connection_generation": new_generation,
    }


def prepare_network_supervision(args: Any) -> NetworkPreparation | CommandResult:
    """Prepare the existing cTrader provider after an explicit network gate."""
    service = CTraderCliService()
    mode = str(getattr(args, "mode", "observe")).strip().lower()
    activate = bool(getattr(args, "activate", False))
    execution_requested = mode == "demo" and activate
    defer_binding = bool(getattr(args, "defer_binding", False))
    query_context = service._prepare_query(args, execution=execution_requested)
    if isinstance(query_context, CommandResult):
        return query_context
    provider: Any | None = None
    binding: Any | None = None
    try:
        provider, observed = service._connect_and_discover(query_context)
        readonly = service._authorize_readonly_provider(query_context, provider, observed)
        if isinstance(readonly, CommandResult):
            query_context.client_secret = ""
            if provider is not None:
                provider.close()
            return readonly
        if provider is None:
            raise RuntimeError("la composición DEMO no devolvió provider")
        provider.subscribe(timeframes=())
        account_id = int(query_context.profile.account_id)
        callbacks: Any | None = None
        if execution_requested and not defer_binding:
            from .supervision_demo import build_demo_execution_binding

            binding = build_demo_execution_binding(
                provider,
                {
                    "network_performed": True,
                    "source_mode": "DEMO_OBSERVED",
                    "synthetic": False,
                    "environment": "DEMO",
                    "account_id": account_id,
                    "account_selected": True,
                    "account_verified": True,
                },
                config=query_context.config,
                state_dir=args.state_dir,
                resume=bool(getattr(args, "resume", True)),
                # Never let the free-form CLI label alter the network account
                # journal namespace; safety derives the canonical key from
                # endpoint/environment/account identity.
                account_key=None,
            )
            callbacks = binding.callbacks
        provenance = {
            "provider": "ctrader_open_api",
            "source_mode": "DEMO_OBSERVED",
            "synthetic": False,
            "environment": "DEMO",
            "network_performed": True,
            "execution_enabled": callbacks is not None,
            "account_id": account_id,
            "account_selected": True,
            "account_verified": True,
            "config_hash": query_context.config.config_hash,
            "catalog_verified": True,
            "readonly_scope": not execution_requested,
            "catalog_observation": {
                **catalog_provenance(provider, datetime.now(UTC)),
                "account_id": account_id,
                "environment": "DEMO",
                "connection_generation": getattr(provider, "generation", None),
            },
        }
        provider_config = getattr(provider, "config", None)
        host = str(getattr(provider_config, "host", "")).strip().lower()
        port = getattr(provider_config, "port", None)
        if host and isinstance(port, int) and not isinstance(port, bool):
            provenance["endpoint"] = f"{host}:{port}"
        observed_mapping = {
            "account_count": len(observed.get("records", ())) if isinstance(observed, Mapping) else None,
            "catalog_verified": True,
        }
        preparation = NetworkPreparation(provider, query_context, provenance, observed_mapping, callbacks, binding)
        preparation.reconnect_callback = lambda: _reconnect_preparation(preparation, service, args, execution_requested)
        return preparation
    except BaseException:
        query_context.client_secret = ""
        if binding is not None:
            with contextlib.suppress(Exception):
                binding.close()
        if provider is not None:
            with contextlib.suppress(Exception):
                provider.close()
        raise


__all__ = ["NetworkPreparation", "prepare_network_supervision"]
