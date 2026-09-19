"""Network DEMO execution composition for the local supervisor.

The supervisor owns lifecycle, persistence and the account writer lock.  This
module owns only the explicit bridge from an already authenticated
``CTraderProvider`` to the DEMO executor callbacks.  It deliberately does
not connect, authenticate, discover accounts, read OAuth secrets or create a
second client.  A caller must invoke it only after the network source and
the ``--mode demo --activate`` gate have been accepted.

The provider's existing :class:`~mtf_lab.data.ctrader_session.CTraderClient`
is the sole session boundary.  ``CTraderClientGateway`` validates the
server-minted session evidence again before the execution adapter is built;
local ``verified`` flags or provenance strings are never used as permission
proof.
"""

from __future__ import annotations

import contextlib
import hashlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, cast

from ..core.canonical import canonical_json
from .ctrader_account_risk import AccountRiskObserver
from .ctrader_demo_transport import (
    CTraderClientGateway,
    CTraderDemoTransport,
    ServerAccountObservation,
)
from .ctrader_executor import (
    CTraderDemoExecutor,
    DemoAccount,
    DemoAccountRequired,
    EndpointRejected,
    ExecutionPolicy,
    JsonlIntentStore,
    OrderResult,
    OrderState,
    Quote,
    RealAccountForbidden,
    RecoveryRequired,
    RiskLimitRejected,
    ScopeRejected,
    _as_mapping,
    _decimal_value,
    _parse_time,
)
from .volume_rules import VolumeGrid, VolumeRuleError


class DemoCompositionError(RuntimeError):
    """The authenticated DEMO execution boundary cannot be proven."""


@dataclass(slots=True)
class DemoExecutionBinding:
    """Resources and callbacks owned by one prepared DEMO execution seam."""

    gateway: CTraderClientGateway
    observation: ServerAccountObservation
    transport: CTraderDemoTransport
    executor: CTraderDemoExecutor
    intent_store: JsonlIntentStore
    callbacks: Any
    risk_observer: AccountRiskObserver | None = None
    canary_economics: Any | None = None

    def close(self) -> None:
        """Close only the local journal; the provider owns the shared client."""

        close = getattr(self.intent_store, "close", None)
        if callable(close):
            close()


def build_demo_execution_binding(  # noqa: C901
    provider: Any,
    provenance: Mapping[str, Any],
    *,
    config: Any,
    state_dir: str | Path,
    resume: bool = True,
    account_key: str | None = None,
    policy: ExecutionPolicy | None = None,
    canary_economics: Any | None = None,
    proto: Any | None = None,
    clock: Callable[[], datetime] | None = None,
) -> DemoExecutionBinding:
    """Build callbacks on top of one already authenticated DEMO provider.

    ``provider`` must have completed application authentication, explicit
    account selection/authentication and catalog validation before this
    function is called.  The function makes no network call itself apart from
    normal execution/reconciliation callbacks invoked later by the caller.
    """

    _validate_network_provenance(provenance)
    raw_execution = _execution_config(config)
    account_id = _observed_account_id(provenance)
    endpoint = _observed_endpoint(provenance)
    _validate_execution_config(raw_execution, account_id=account_id, endpoint=endpoint)

    client = getattr(provider, "client", None)
    if client is None:
        raise DemoCompositionError("el provider DEMO no expone su único CTraderClient autenticado")
    clock_fn = clock or (lambda: datetime.now(UTC))

    # CTraderClientGateway validates the identity-bound proof returned by the
    # client.  It is intentionally constructed over the provider's client,
    # never over a new SDK client or a caller-supplied token/session mapping.
    gateway = CTraderClientGateway(client, clock=clock_fn)
    observation = gateway.server_observation()
    if observation.account_id != account_id:
        raise DemoAccountRequired("la evidencia de sesión no coincide con la cuenta observada")
    if observation.endpoint.lower() != endpoint.lower():
        raise EndpointRejected("la evidencia de sesión no coincide con el endpoint DEMO observado")
    if "trading" not in observation.scopes:
        raise ScopeRejected("la evidencia de sesión no observa el scope exacto trading")

    spec = getattr(provider, "spec", None)
    symbol = str(getattr(spec, "symbol", "")).strip().upper()
    symbol_id = getattr(spec, "symbol_id", None)
    if not symbol or isinstance(symbol_id, bool) or not isinstance(symbol_id, int) or symbol_id <= 0:
        raise DemoCompositionError("el catálogo DEMO no contiene symbol/symbol_id observado")
    expected_symbol = str(getattr(config, "instrument", symbol)).strip().upper().replace("-", "/")
    if symbol != expected_symbol:
        raise DemoCompositionError("provider y configuración DEMO no observan el mismo instrumento")
    volume_grid = _observed_volume_grid(provider, clock_fn())
    risk_binding = _risk_binding_kwargs(
        provider,
        raw_execution,
        clock_fn(),
        volume_grid,
        connection_generation=observation.connection_generation,
        canary_economics=canary_economics,
    )

    account = DemoAccount(
        account_id,
        "DEMO",
        observation.endpoint,
        frozenset(observation.scopes),
        selected=True,
        verified=False,
    )
    execution_policy = policy or _policy_from_config(raw_execution, symbol)
    journal_path = _journal_path(state_dir, account_id, observation.endpoint, account_key=account_key)
    intent_store = JsonlIntentStore(journal_path)
    try:
        transport = CTraderDemoTransport(
            account,
            client=gateway,
            proto=proto,
            symbol_ids={symbol: symbol_id},
            symbol_names={symbol_id: symbol},
            clock=clock_fn,
            server_observation=observation,
            volume_grid=volume_grid,
        )
        executor = CTraderDemoExecutor(
            account,
            policy=execution_policy,
            transport=transport,
            intent_store=intent_store,
            clock=clock_fn,
            server_observation=observation,
            fixture_mode=False,
            **risk_binding,
        )
        if canary_economics is not None:
            _update_executor_from_canary_economics(executor, canary_economics, "BUY")
        # Recovery is registration-only: it hydrates durable identities and
        # never resubmits.  With --no-resume, an existing journal remains a
        # hard gate rather than being silently discarded.
        if intent_store.requires_recovery:
            if resume is not True:
                raise RecoveryRequired("el journal DEMO requiere recuperación explícita antes de activarse")
            from .ctrader_demo_composition import recover_execution_intents

            recover_execution_intents(journal_path, executor)
        executor.activate()
        observer = AccountRiskObserver(
            provider,
            proto=proto,
            clock=clock_fn,
            max_age_seconds=min(10.0, float(execution_policy.max_price_age_seconds)),
            state_path=journal_path.with_suffix(".risk.json"),
            journal_path=journal_path,
            allow_new_baseline=not bool(intent_store.intents),
            require_cashflows=True,
        )
        callbacks = _callbacks_for(
            executor,
            provider,
            observation,
            risk_observer=observer,
            canary_economics=canary_economics,
        )
        return DemoExecutionBinding(
            gateway, observation, transport, executor, intent_store, callbacks, observer, canary_economics
        )
    except BaseException:
        with contextlib.suppress(Exception):
            intent_store.close()
        raise


def _validate_network_provenance(provenance: Mapping[str, Any]) -> None:
    if not isinstance(provenance, Mapping):
        raise DemoCompositionError("la procedencia DEMO debe ser una tabla")
    if provenance.get("network_performed") is not True:
        raise DemoAccountRequired("la ejecución DEMO requiere una sesión observada por red")
    if provenance.get("synthetic") is True or str(provenance.get("source_mode", "")).upper() == "SYNTHETIC_FIXTURE":
        raise RealAccountForbidden("un fixture sintético no puede satisfacer la ejecución DEMO externa")
    if str(provenance.get("environment", "")).strip().upper() != "DEMO":
        raise DemoAccountRequired("la procedencia no observa environment=DEMO")
    if provenance.get("account_selected") is not True or provenance.get("account_verified") is not True:
        raise DemoAccountRequired("la cuenta DEMO debe estar seleccionada y verificada por el servidor")


def _observed_account_id(provenance: Mapping[str, Any]) -> str:
    value = provenance.get("account_id", provenance.get("selected_account_id"))
    if value is None or isinstance(value, bool) or not str(value).strip():
        raise DemoAccountRequired("falta account_id observado")
    text = str(value).strip()
    try:
        if int(text) <= 0:
            raise ValueError
    except (TypeError, ValueError) as exc:
        raise DemoAccountRequired("account_id observado debe ser positivo") from exc
    return text


def _observed_endpoint(provenance: Mapping[str, Any]) -> str:
    value = str(provenance.get("endpoint", "demo.ctraderapi.com:5035")).strip().lower()
    if value not in {
        "demo.ctraderapi.com:5035",
        "tcp://demo.ctraderapi.com:5035",
        "ssl://demo.ctraderapi.com:5035",
    }:
        raise EndpointRejected("la procedencia no observa el endpoint DEMO oficial")
    return "demo.ctraderapi.com:5035"


def _execution_config(config: Any) -> dict[str, Any]:
    raw = getattr(config, "execution", None)
    if not isinstance(raw, Mapping):
        raise DemoCompositionError("la configuración efectiva no contiene [execution]")
    return dict(raw)


def _validate_execution_config(raw: Mapping[str, Any], *, account_id: str, endpoint: str) -> None:
    if raw.get("enabled") is not True:
        raise DemoAccountRequired("[execution].enabled=true es obligatorio para --mode demo --activate")
    environment = str(raw.get("environment", "DEMO")).strip().upper()
    if environment in {"REAL", "LIVE", "PRODUCTION"}:
        raise RealAccountForbidden("la configuración de ejecución no puede ser REAL/LIVE")
    if environment != "DEMO":
        raise DemoAccountRequired("[execution].environment debe ser DEMO")
    configured_endpoint = str(raw.get("endpoint", endpoint)).strip().lower()
    if configured_endpoint not in {endpoint.lower(), f"tcp://{endpoint.lower()}", f"ssl://{endpoint.lower()}"}:
        raise EndpointRejected("[execution].endpoint no coincide con la evidencia DEMO")
    configured_account = raw.get("account_id")
    if configured_account not in {None, ""} and str(configured_account).strip() != account_id:
        raise DemoAccountRequired("[execution].account_id no coincide con la cuenta DEMO observada")
    if str(raw.get("scope", "trading")).strip().lower() != "trading":
        raise ScopeRejected("[execution].scope debe ser exactamente trading")
    required_limits = {
        "max_exposure",
        "max_positions",
        "max_inflight_intents",
        "max_daily_loss",
        "max_drawdown",
        "min_margin_level",
        "max_holding_seconds",
    }
    missing_limits = sorted(name for name in required_limits if raw.get(name) is None)
    if missing_limits:
        raise RiskLimitRejected(
            "la ejecución externa requiere límites observados/configurados explícitamente: " + ", ".join(missing_limits)
        )
    if raw.get("require_protective_stops") is not True:
        raise RiskLimitRejected("la ejecución externa requiere require_protective_stops=true")


def _policy_from_config(raw: Mapping[str, Any], symbol: str) -> ExecutionPolicy:
    fields = {
        "max_quantity",
        "fixed_quantity",
        "max_exposure",
        "max_positions",
        "max_spread",
        "max_price_age_seconds",
        "allowed_symbols",
        "no_martingale",
        "close_only_own_positions",
        "timeout_seconds",
        "close_timeout_seconds",
        "max_daily_loss",
        "max_drawdown",
        "min_margin_level",
        "max_orders_per_window",
        "order_window_seconds",
        "max_inflight_intents",
        "block_on_foreign_positions",
        "require_protective_stops",
        "relative_stop_loss",
        "relative_take_profit",
        "max_relative_stop_loss",
        "max_relative_take_profit",
        "max_slippage_points",
        "max_holding_seconds",
    }
    values = {name: raw[name] for name in fields if name in raw}
    symbols = values.get("allowed_symbols")
    if symbols is None or symbols == () or symbols == [] or symbols == "":
        values["allowed_symbols"] = frozenset({symbol})
    elif isinstance(symbols, str):
        values["allowed_symbols"] = frozenset({symbols})
    return ExecutionPolicy(required_scopes=frozenset({"trading"}), **values)


def _risk_binding_kwargs(
    provider: Any,
    raw_execution: Mapping[str, Any],
    observed_at: datetime,
    volume_grid: VolumeGrid,
    *,
    connection_generation: Any = None,
    canary_economics: Any | None = None,
) -> dict[str, Any]:
    candidate_id = raw_execution.get("market_candidate_id")
    if candidate_id is None:
        return {
            "risk_exit_policy": None,
            "risk_contract_spec": None,
            "risk_calendar": None,
            "market_candidate_id": None,
        }
    candidate_text = str(candidate_id).strip()
    if not candidate_text:
        raise RiskLimitRejected("market_candidate_id no puede estar vacío")
    risk_exit_policy = raw_execution.get("risk_exit_policy")
    risk_calendar = raw_execution.get("risk_calendar")
    if not isinstance(risk_exit_policy, Mapping):
        raise RiskLimitRejected("market_candidate_id requiere una política RiskExit efectiva")
    if not isinstance(risk_calendar, Mapping):
        raise RiskLimitRejected("market_candidate_id requiere un calendario RiskExit explícito")
    contract_spec = dict(_observed_risk_contract_spec(provider, observed_at, volume_grid))
    if canary_economics is not None:
        contract_spec = _merge_canary_economics_contract(contract_spec, canary_economics)
        risk_calendar = _canary_economics_calendar(canary_economics, connection_generation)
    if connection_generation is not None and str(connection_generation).strip():
        contract_spec["connection_generation"] = str(connection_generation).strip()
    return {
        "risk_exit_policy": risk_exit_policy,
        "risk_contract_spec": contract_spec,
        "risk_calendar": risk_calendar,
        "market_candidate_id": candidate_text,
    }


def _canary_economics_calendar(projection: Any, generation: Any) -> Mapping[str, Any]:
    calendar = getattr(projection, "calendar_state", None)
    if not isinstance(calendar, Mapping) or calendar.get("known") is not True:
        raise RiskLimitRejected("canary_economics no conserva calendario OBSERVED_BROKER")
    basis = str(calendar.get("basis", calendar.get("calendar_basis", ""))).upper()
    if basis.startswith("MODEL"):
        raise RiskLimitRejected("canary_economics calendario modelado")
    provenance = getattr(projection, "provenance", None)
    if not isinstance(provenance, Mapping):
        raise RiskLimitRejected("canary_economics sin provenance")
    observed_generation = str(provenance.get("connection_generation", provenance.get("generation", ""))).strip()
    expected = str(generation or "").strip()
    if not observed_generation or not expected or observed_generation != expected:
        raise RiskLimitRejected("canary_economics calendario no coincide con generación DEMO")
    return dict(calendar)


def _merge_canary_economics_contract(base: Mapping[str, Any], projection: Any) -> dict[str, Any]:
    """Merge typed observed economics without changing broker metadata."""

    contract = dict(base)
    to_contract = getattr(projection, "to_contract_spec", None)
    required_margin = getattr(projection, "required_margin", None)
    quantity = getattr(projection, "quantity", None)
    if not callable(to_contract) or not callable(required_margin) or quantity is None:
        raise RiskLimitRejected("canary_economics no expone projection tipada")
    try:
        buy: dict[str, Any] = dict(cast(Mapping[str, Any], to_contract("BUY")))
        margins = [Decimal(str(required_margin("BUY"))), Decimal(str(required_margin("SELL")))]
        quantity_decimal = Decimal(str(quantity))
        if quantity_decimal <= 0 or any(value <= 0 for value in margins):
            raise ValueError
        margin_per_unit = max(margins) / quantity_decimal
    except (TypeError, ValueError, ArithmeticError) as exc:
        raise RiskLimitRejected("canary_economics con margen observado inválido") from exc
    contract.update(buy)
    contract.update(
        {
            "margin_per_unit": str(margin_per_unit),
            "canary_margin_buy": str(margins[0]),
            "canary_margin_sell": str(margins[1]),
            "margin_source": "ProtoOAExpectedMarginRes",
        }
    )
    return contract


def _update_executor_from_canary_economics(executor: Any, projection: Any, side: str) -> None:
    normalized_side = str(side).upper()
    if normalized_side not in {"BUY", "SELL", "CONSERVATIVE"}:
        raise RiskLimitRejected("canary_economics side inválido")
    contract_side = "BUY" if normalized_side == "CONSERVATIVE" else normalized_side
    to_contract = getattr(projection, "to_contract_spec", None)
    calendar = getattr(projection, "calendar_state", None)
    generation = str(getattr(projection, "connection_generation", "")).strip()
    observed_at = getattr(projection, "observed_at", None)
    if not callable(to_contract) or not isinstance(calendar, Mapping) or not generation or observed_at is None:
        raise RiskLimitRejected("canary_economics contract/calendario carece de observación tipada")
    try:
        contract: dict[str, Any] = dict(cast(Mapping[str, Any], to_contract(contract_side)))
    except (TypeError, ValueError, ArithmeticError) as exc:
        raise RiskLimitRejected("canary_economics contract_spec inválido") from exc
    contract["connection_generation"] = generation
    contract["observed_at"] = observed_at
    contract["calendar_observed_at"] = observed_at
    # The executor has no public mutable contract setter: this narrow
    # composition seam replaces the effective RiskExit evidence atomically
    # immediately before planning.  It never changes policy limits or account
    # identity, and fails closed if the canonical executor does not expose the
    # expected slots.
    if hasattr(executor, "_risk_contract_spec"):
        executor._risk_contract_spec = dict(contract)
    if hasattr(executor, "_risk_calendar"):
        executor._risk_calendar = dict(calendar)
    if str(side).upper() == "CONSERVATIVE":
        required_margin = max(
            Decimal(str(projection.required_margin("BUY"))),
            Decimal(str(projection.required_margin("SELL"))),
        )
        levels = [
            Decimal(str(projection.projected_margin_level("BUY"))),
            Decimal(str(projection.projected_margin_level("SELL"))),
        ]
        updater = getattr(projection, "to_update_risk_kwargs", None)
        if not callable(updater):
            raise RiskLimitRejected("canary_economics no expone métricas tipadas")
        values: dict[str, Any] = dict(cast(Mapping[str, Any], updater("BUY")))
        values["margin_required"] = required_margin
        values["margin_level"] = min(levels)
        executor.update_risk_metrics(**values)
        return
    updater = getattr(projection, "to_update_risk_kwargs", None)
    if not callable(updater):
        raise RiskLimitRejected("canary_economics no expone métricas tipadas")
    raw_values = updater(side)
    if not isinstance(raw_values, Mapping):
        raise RiskLimitRejected("canary_economics risk kwargs inválidos")
    side_values: dict[str, Any] = dict(cast(Mapping[str, Any], raw_values))
    executor.update_risk_metrics(**side_values)


def _catalog_raw_full_symbol(provider: Any) -> Mapping[str, Any] | None:
    catalog = getattr(provider, "catalog", None)
    selected = getattr(catalog, "selected", None)
    metadata = getattr(selected, "metadata", None)
    if not isinstance(metadata, Mapping):
        return None
    raw_full = metadata.get("fullSymbol", metadata)
    return raw_full if isinstance(raw_full, Mapping) else None


def _explicit_catalog_field(
    provider: Any,
    compact_full: Mapping[str, Any],
    names: tuple[str, ...],
) -> Any:
    for name in names:
        if name in compact_full:
            return compact_full[name]
    raw_full = _catalog_raw_full_symbol(provider)
    if raw_full is not None:
        for name in names:
            if name in raw_full:
                return raw_full[name]
    return None


def _positive_decimal_text(value: Any) -> str | None:
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return str(parsed) if parsed.is_finite() and parsed > 0 else None


def _nonnegative_decimal_text(value: Any) -> str | None:
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return str(parsed) if parsed.is_finite() and parsed >= 0 else None


def _observed_cost_fields(provider: Any, full: Mapping[str, Any]) -> dict[str, Any]:
    cost_source = _explicit_catalog_field(
        provider,
        full,
        ("expected_cost_source", "cost_estimate_source", "cost_source"),
    )
    cost_currency = _explicit_catalog_field(
        provider,
        full,
        ("expected_cost_currency", "cost_currency", "commission_currency"),
    )
    cost_identity = (
        isinstance(cost_source, str)
        and bool(cost_source.strip())
        and isinstance(cost_currency, str)
        and bool(cost_currency.strip())
    )
    result: dict[str, Any] = {}
    if cost_identity:
        result["expected_cost_source"] = cost_source.strip()
        result["expected_cost_currency"] = cost_currency.strip().upper()
    for target, cost_names in (
        ("expected_cost_fixed", ("expected_cost_fixed", "expected_commission_fixed")),
        ("expected_cost_per_unit", ("expected_cost_per_unit", "expected_commission_per_unit")),
        ("expected_exit_slippage_per_unit", ("expected_exit_slippage_per_unit",)),
        ("expected_exit_slippage_pips", ("expected_exit_slippage_pips",)),
    ):
        normalized = (
            _nonnegative_decimal_text(_explicit_catalog_field(provider, full, cost_names)) if cost_identity else None
        )
        if normalized is not None:
            result[target] = normalized
    expected_costs_known = _explicit_catalog_field(
        provider,
        full,
        ("expected_costs_known", "risk_envelope_known"),
    )
    if isinstance(expected_costs_known, bool):
        result["expected_costs_known"] = expected_costs_known
    return result


def _observed_risk_contract_spec(provider: Any, observed_at: datetime, volume_grid: VolumeGrid) -> Mapping[str, Any]:
    """Project only economics explicitly present in the selected catalog.

    cTrader's digits, pip position and lot-size metadata are not silently
    converted into a monetary unit value.  A missing unit value, pip size or
    fee declaration therefore remains visible to ``RiskExit`` and blocks an
    external entry instead of becoming a broker-default guess.
    """

    try:
        from .market_schedule import catalog_provenance

        provenance = catalog_provenance(provider, observed_at)
    except (AttributeError, TypeError, ValueError):
        return {"known": False}
    if not isinstance(provenance, Mapping) or provenance.get("catalog_identity_valid") is not True:
        return {"known": False}
    full = provenance.get("full_symbol")
    if not isinstance(full, Mapping):
        return {"known": False}

    result: dict[str, Any] = {
        "known": True,
        "quantity_min": str(Decimal(volume_grid.min_volume) / Decimal(volume_grid.volume_scale)),
        "quantity_step": str(Decimal(volume_grid.step_volume) / Decimal(volume_grid.volume_scale)),
        "quantity_max": str(Decimal(volume_grid.max_volume) / Decimal(volume_grid.volume_scale)),
    }
    for target, value_names in (
        ("pip_size", ("pip_size", "pipSize")),
        ("unit_value", ("unit_value", "unitValue")),
        (
            "minimum_stop_distance",
            ("minimum_stop_distance", "min_stop_distance", "minimumStopDistance", "minStopDistance"),
        ),
        ("margin_per_unit", ("margin_per_unit", "marginPerUnit")),
    ):
        normalized = _positive_decimal_text(_explicit_catalog_field(provider, full, value_names))
        if normalized is not None:
            result[target] = normalized
    for target, flag_names in (
        (
            "fees_known",
            ("fees_known", "feesKnown", "commission_known", "commissionKnown", "costs_known", "costsKnown"),
        ),
        ("spread_known", ("spread_known", "spreadKnown")),
    ):
        value = _explicit_catalog_field(provider, full, flag_names)
        if isinstance(value, bool):
            result[target] = value
    result.update(_observed_cost_fields(provider, full))
    return result


def _observed_volume_grid(provider: Any, observed_at: datetime) -> VolumeGrid:
    try:
        from .market_schedule import catalog_provenance

        provenance = catalog_provenance(provider, observed_at)
        full_symbol = provenance.get("full_symbol") if isinstance(provenance, Mapping) else None
        return VolumeGrid.from_mapping(full_symbol, volume_scale=100)
    except VolumeRuleError as exc:
        raise RiskLimitRejected(f"observed symbol volume grid required for external opens: {exc}") from exc
    except (AttributeError, TypeError, ValueError) as exc:
        raise RiskLimitRejected("observed symbol volume grid is unavailable") from exc


def _journal_path(
    state_dir: str | Path,
    account_id: str,
    endpoint: str,
    *,
    account_key: str | None,
) -> Path:
    root = Path(state_dir).expanduser()
    if not str(root).strip():
        raise DemoCompositionError("state_dir no puede estar vacío")
    # Account identity is deliberately independent of instrument/config so a
    # second supervisor cannot become a parallel writer merely by changing a
    # strategy setting.  The supervisor's account lock should use the same
    # account-scoped key supplied by its composition root when available.
    identity = {"account_id": account_id, "environment": "DEMO", "endpoint": endpoint.lower()}
    digest = hashlib.sha256(canonical_json(identity).encode("utf-8")).hexdigest()[:24]
    key = str(account_key or f"demo-{digest}").strip()
    if not key or "/" in key or "\\" in key or key in {".", ".."}:
        raise DemoCompositionError("account_key contiene una ruta inválida")
    return root / "execution-intents" / f"{key}.jsonl"


def _refresh_observed_risk(
    observer: AccountRiskObserver | None,
    executor: CTraderDemoExecutor,
    canary_economics: Any | None = None,
    side: str = "CONSERVATIVE",
) -> None:
    if observer is None:
        return
    try:
        observer.update_executor(executor)
    except Exception as exc:
        raise RiskLimitRejected(f"la observación de riesgo DEMO no es utilizable: {type(exc).__name__}") from exc
    if canary_economics is not None:
        _update_executor_from_canary_economics(executor, canary_economics, side)


def _management_quote(
    executor: CTraderDemoExecutor,
    provider: Any,
    observation: ServerAccountObservation,
) -> Quote | None:
    if not bool(getattr(executor, "risk_exit_enabled", False)):
        return None
    try:
        # The quote book is already held by this provider.  This read does not
        # refresh a session or issue a market request.
        return _quote_from_provider(provider, observation, {})
    except (RiskLimitRejected, TypeError, ValueError):
        # Missing/stale BBO is an UNKNOWN exit input; do not invent a close
        # price or fall back to the legacy holding deadline.
        return None


def _manage_callbacks(
    executor: CTraderDemoExecutor,
    provider: Any,
    observation: ServerAccountObservation,
) -> tuple[OrderResult, ...]:
    quote = _management_quote(executor, provider, observation)
    if bool(getattr(executor, "risk_exit_enabled", False)):
        return tuple(executor.manage(quote))
    return tuple(executor.manage())


def _observe_runtime_callback(executor: Any, snapshot: Any) -> Mapping[str, Any]:
    observe = getattr(executor, "observe_runtime", None)
    if not callable(observe):
        return {"runtime_state": "UNKNOWN", "reasons": ["runtime_observer_unavailable"]}
    result = observe(snapshot)
    return result if isinstance(result, Mapping) else {"runtime_state": "UNKNOWN"}


def _callbacks_for(
    executor: CTraderDemoExecutor,
    provider: Any,
    observation: ServerAccountObservation,
    *,
    risk_observer: AccountRiskObserver | None = None,
    canary_economics: Any | None = None,
) -> Any:
    from .supervision_contracts import ExecutionCallbacks

    def on_signal(signal: Any, quote: Any | None = None) -> OrderResult:
        selected_quote = quote if isinstance(quote, Quote) else _quote_from_provider(provider, observation, signal)
        data = _as_mapping(signal)
        explicit_account = data.get("account_environment", data.get("account_mode"))
        if explicit_account is not None and str(explicit_account).upper() != "DEMO":
            raise RealAccountForbidden("signal account environment conflicts with observed DEMO account")
        # LIVE here describes the authenticated market-data stream, not a
        # REAL account. The account context comes only from this bound seam.
        data["data_mode"] = data.get("data_mode", data.get("mode", "LIVE"))
        data["account_environment"] = observation.environment
        data_side = str(data.get("side", data.get("direction", ""))).upper()
        _refresh_observed_risk(
            risk_observer, executor, canary_economics, data_side if data_side in {"BUY", "SELL"} else "CONSERVATIVE"
        )
        return executor.submit_signal(data, selected_quote)

    def manage() -> tuple[OrderResult, ...]:
        _refresh_observed_risk(risk_observer, executor, canary_economics)
        # Account metrics never substitute for the execution journal's own
        # typed, account-scoped position/ownership reconciliation.
        executor.reconcile_positions()
        return _manage_callbacks(executor, provider, observation)

    def reconcile() -> Mapping[str, Any]:
        managed = manage()
        positions = executor.reconcile_positions()
        terminal = all(
            result.state
            in {OrderState.FILLED, OrderState.REJECTED, OrderState.CANCELLED, OrderState.EXPIRED, OrderState.CLOSED}
            for result in managed
        )
        valid = positions.get("state") == "VALID" and terminal
        return {
            "ok": valid,
            "state": "VALID" if valid else "UNKNOWN",
            "positions": positions,
            "orders": [result.to_dict() for result in managed],
        }

    def reduce(_reason: Any = None) -> Mapping[str, Any]:
        del _reason
        return cast(Mapping[str, Any], executor.reduce_exposure())

    def observe_runtime(snapshot: Any) -> Mapping[str, Any]:
        return _observe_runtime_callback(executor, snapshot)

    return ExecutionCallbacks(
        executor=executor,
        on_signal=on_signal,
        manage=manage,
        reconcile=reconcile,
        reduce=reduce,
        observe_runtime=observe_runtime,
    )


def _quote_from_provider(provider: Any, observation: ServerAccountObservation, signal: Any) -> Quote:
    """Turn the latest complete provider quote-book observation into a Quote."""

    snapshotter = getattr(provider, "snapshot_quote_state", None)
    if not callable(snapshotter):
        raise RiskLimitRejected("provider no expone snapshot de cotización bid/ask")
    raw_snapshot = snapshotter()
    spec = getattr(provider, "spec", None)
    symbol = str(getattr(spec, "symbol", "")).strip().upper()
    symbol_id = getattr(spec, "symbol_id", None)
    bid, ask = _book_legs(raw_snapshot, symbol_id)
    event_time, available_at, generation, sequence = _book_timing(bid, ask, observation)
    instrument = _signal_instrument(signal) or symbol
    if instrument != symbol:
        raise RiskLimitRejected("la señal y la cotización DEMO tienen instrumentos distintos")
    source_identity = f"quote-book:{observation.session_id}:{generation}:{sequence or event_time.isoformat()}"
    return Quote(
        symbol,
        _decimal_value(bid.get("price"), "bid", positive=True),
        _decimal_value(ask.get("price"), "ask", positive=True),
        event_time,
        available_at,
        quality="VALID",
        source="ctrader-open-api",
        base_price="bid_ask",
        source_identity=source_identity,
        session_id=observation.session_id,
        connection_generation=observation.connection_generation,
        data_mode="LIVE",
        synthetic=False,
    )


def _book_legs(raw_snapshot: Any, symbol_id: Any) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    if not isinstance(raw_snapshot, Mapping):
        raise RiskLimitRejected("snapshot de cotización inválido")
    symbols = raw_snapshot.get("symbols")
    if not isinstance(symbols, Mapping) or symbol_id is None:
        raise RiskLimitRejected("provider no tiene libro bid/ask identificado")
    book = symbols.get(str(symbol_id), symbols.get(symbol_id))
    if not isinstance(book, Mapping):
        raise RiskLimitRejected("no se observó un libro bid/ask completo")
    bid = book.get("bid")
    ask = book.get("ask")
    if not isinstance(bid, Mapping) or not isinstance(ask, Mapping):
        raise RiskLimitRejected("no se observó una pareja bid/ask completa")
    _validate_book_pair(bid, ask)
    return bid, ask


def _validate_book_pair(bid: Mapping[str, Any], ask: Mapping[str, Any]) -> None:
    if bid.get("timestamp_missing") is True or ask.get("timestamp_missing") is True:
        raise RiskLimitRejected("la cotización DEMO carece de timestamp de origen")
    for side, leg in (("bid", bid), ("ask", ask)):
        state = leg.get("state")
        if state is not None and str(state).strip().upper() != "VALID":
            raise RiskLimitRejected(f"la pierna {side} no tiene calidad VALID observada")
        if leg.get("reasons", ()):
            raise RiskLimitRejected(f"la pierna {side} conserva razones de calidad bloqueantes")
    try:
        bid_price = _decimal_value(bid.get("price"), "bid", positive=True)
        ask_price = _decimal_value(ask.get("price"), "ask", positive=True)
    except (TypeError, ValueError) as exc:
        raise RiskLimitRejected("la pareja bid/ask observada es inválida") from exc
    # Equality is not an executable zero-spread quote.  Treat it as the same
    # crossed class as bid > ask; never repair or reorder provider prices.
    if bid_price >= ask_price:
        raise RiskLimitRejected("la pareja bid/ask observada está cruzada")


def _book_timing(
    bid: Mapping[str, Any],
    ask: Mapping[str, Any],
    observation: ServerAccountObservation,
) -> tuple[datetime, datetime, Any, Any]:
    bid_time = _quote_time(bid.get("event_time"), "bid event_time")
    ask_time = _quote_time(ask.get("event_time"), "ask event_time")
    bid_available = _quote_time(bid.get("available_at"), "bid available_at")
    ask_available = _quote_time(ask.get("available_at"), "ask available_at")
    event_time = max(bid_time, ask_time)
    available_at = min(bid_available, ask_available)
    if available_at < event_time:
        raise RiskLimitRejected("las piernas bid/ask no comparten disponibilidad causal")
    generation = bid.get("generation")
    if generation is None or ask.get("generation") is None or str(generation) != str(ask.get("generation")):
        raise RiskLimitRejected("las piernas bid/ask no comparten generación")
    if str(generation) != str(observation.connection_generation):
        raise RiskLimitRejected("la cotización pertenece a otra generación de conexión")
    sequence = bid.get("sequence") if bid.get("sequence") is not None else ask.get("sequence")
    return event_time, available_at, generation, sequence


def _quote_time(value: Any, name: str) -> datetime:
    if value is None:
        raise RiskLimitRejected(f"la cotización carece de {name}")
    try:
        parsed = _parse_time(value)
        if not isinstance(parsed, datetime):
            raise ValueError
        return parsed
    except (TypeError, ValueError) as exc:
        raise RiskLimitRejected(f"la cotización tiene {name} inválido") from exc


def _signal_instrument(signal: Any) -> str | None:
    value = (
        signal.get("instrument", signal.get("symbol"))
        if isinstance(signal, Mapping)
        else getattr(signal, "instrument", None)
    )
    return str(value).strip().upper().replace("-", "/") if value is not None and str(value).strip() else None


__all__ = ["DemoCompositionError", "DemoExecutionBinding", "build_demo_execution_binding"]
