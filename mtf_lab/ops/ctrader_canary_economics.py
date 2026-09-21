"""Bounded, read-only economics for a manually approved EUR/USD canary.

This module is intentionally not an execution adapter.  It consumes the
already authenticated cTrader client held by a provider, a typed and fresh
``AccountRiskSnapshot``, and the provider's causal bid/ask book.  It may issue
only the read-only trader, asset-list, and expected-margin requests needed to
prove the projection.  It never authenticates, connects, submits an order,
opens a credential store, or mutates provider metadata.

The economics are deliberately narrow.  They cover only EUR/USD with a USD
deposit asset, a maximum canary quantity of 1,000 base units, and a bounded
five-minute holding window.  Missing, partial, future, stale, ambiguous, or
cross-generation evidence raises ``CanaryEconomicsError`` rather than falling
back to a broker default.
"""

from __future__ import annotations

import dataclasses
import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import Any, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ..core.numeric import decimal_context
from ..data.ctrader_config import normalize_symbol_name
from ..data.ctrader_protocol import WireMessage, message_to_mapping, read_field, read_repeated
from .ctrader_account_risk import AccountRiskSnapshot
from .ctrader_demo_transport import load_official_proto

DEMO_ENDPOINT = "demo.ctraderapi.com:5035"
CANARY_SYMBOL = "EUR/USD"
CANARY_MAX_QUANTITY = Decimal("1000")
CANARY_MAX_HOLDING_SECONDS = Decimal("300")
PROTOCOL_VOLUME_SCALE = 100
DEFAULT_MAX_AGE_SECONDS = Decimal("10")

TRADER_REQ = 2121
TRADER_RES = 2122
ASSET_LIST_REQ = 2112
ASSET_LIST_RES = 2113
EXPECTED_MARGIN_REQ = 2139
EXPECTED_MARGIN_RES = 2140

USD_PER_LOT = "USD_PER_LOT"
DISTANCE_IN_POINTS = "SYMBOL_DISTANCE_IN_POINTS"


class CanaryEconomicsError(RuntimeError):
    """The bounded read-only economics contract cannot be proven."""


class CanaryEconomicsProtocolError(CanaryEconomicsError):
    """A correlated cTrader response violates the local contract."""


@dataclasses.dataclass(frozen=True, slots=True)
class ExitSlippageHypothesis:
    """Typed forecast input, never a broker observation or human approval."""

    pips: Decimal | int | str = Decimal("0.1")
    basis: str = "IMPLEMENTATION_HYPOTHESIS"
    source: str = "baseline-0.1pip"

    def __post_init__(self) -> None:
        value = _decimal(self.pips, "exit_slippage_hypothesis.pips", nonnegative=True)
        basis = str(self.basis).strip().upper()
        source = str(self.source).strip()
        if value != Decimal("0.1"):
            raise CanaryEconomicsError("el canary sólo admite la hipótesis de 0.1 pip")
        if basis != "IMPLEMENTATION_HYPOTHESIS":
            raise CanaryEconomicsError("exit slippage requiere basis=IMPLEMENTATION_HYPOTHESIS")
        if source != "baseline-0.1pip":
            raise CanaryEconomicsError("exit slippage requiere source=baseline-0.1pip")
        object.__setattr__(self, "pips", value)
        object.__setattr__(self, "basis", basis)
        object.__setattr__(self, "source", source)

    def to_dict(self) -> dict[str, Any]:
        return {
            "pips": _text(cast(Decimal, self.pips)),
            "basis": self.basis,
            "source": self.source,
            "observed": False,
            "forecast_only": True,
        }


@dataclasses.dataclass(frozen=True, slots=True)
class CanaryQuoteEvidence:
    """One causal bid/ask pair held by the existing provider."""

    symbol: str
    symbol_id: int
    bid: Decimal
    ask: Decimal
    spread: Decimal
    spread_pips: Decimal
    event_time: datetime
    available_at: datetime
    connection_generation: str
    sequence: str | None
    earliest_available_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "symbol_id": self.symbol_id,
            "bid": _text(self.bid),
            "ask": _text(self.ask),
            "spread": _text(self.spread),
            "spread_pips": _text(self.spread_pips),
            "event_time": _iso(self.event_time),
            "available_at": _iso(self.available_at),
            "earliest_available_at": _iso(self.earliest_available_at or self.available_at),
            "connection_generation": self.connection_generation,
            "sequence": self.sequence,
            "source": "provider.snapshot_quote_state",
            "synthetic": False,
        }


@dataclasses.dataclass(frozen=True, slots=True)
class ExpectedMarginEvidence:
    """Server-produced BUY/SELL margin for the exact canary volume."""

    account_id: str
    symbol_id: int
    volume_protocol: int
    money_digits: int
    buy_margin: Decimal
    sell_margin: Decimal
    observed_at: datetime
    received_at: datetime | None
    connection_generation: str
    client_msg_id: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "account_id": self.account_id,
            "symbol_id": self.symbol_id,
            "volume_protocol": self.volume_protocol,
            "money_digits": self.money_digits,
            "buy_margin": _text(self.buy_margin),
            "sell_margin": _text(self.sell_margin),
            "observed_at": _iso(self.observed_at),
            "received_at": _iso(self.received_at),
            "connection_generation": self.connection_generation,
            "client_msg_id": self.client_msg_id,
            "source": "ProtoOAExpectedMarginRes",
        }


@dataclasses.dataclass(frozen=True, slots=True)
class CanaryEconomicsProjection:
    """Typed, provenance-bound projection for one 1,000-unit canary window."""

    account_id: str
    symbol: str
    symbol_id: int
    quantity: Decimal
    quantity_protocol: int
    equity: Decimal
    used_margin: Decimal
    margin_available: Decimal
    buy_margin_required: Decimal
    sell_margin_required: Decimal
    buy_margin_level: Decimal
    sell_margin_level: Decimal
    observed_at: datetime
    connection_generation: str
    session_id: str
    window_start: datetime
    window_end: datetime
    max_holding_seconds: Decimal
    quote: CanaryQuoteEvidence
    expected_margin: ExpectedMarginEvidence
    contract_spec: Mapping[str, Any]
    calendar_state: Mapping[str, Any]
    risk_state: Mapping[str, Any]
    assumptions: Mapping[str, Any]
    provenance: Mapping[str, Any]

    def __post_init__(self) -> None:
        object.__setattr__(self, "contract_spec", _freeze(self.contract_spec))
        object.__setattr__(self, "calendar_state", _freeze(self.calendar_state))
        object.__setattr__(self, "risk_state", _freeze(self.risk_state))
        object.__setattr__(self, "assumptions", _freeze(self.assumptions))
        object.__setattr__(self, "provenance", _freeze(self.provenance))

    @property
    def complete(self) -> bool:
        return True

    def required_margin(self, side: str) -> Decimal:
        normalized = _side(side)
        return self.buy_margin_required if normalized == "BUY" else self.sell_margin_required

    def projected_margin_level(self, side: str) -> Decimal:
        normalized = _side(side)
        return self.buy_margin_level if normalized == "BUY" else self.sell_margin_level

    def contract_spec_for(self, side: str = "BUY") -> Mapping[str, Any]:
        """Return the RiskExit contract with side-specific observed margin/unit."""

        required = self.required_margin(side)
        with decimal_context():
            margin_per_unit = required / self.quantity
        return cast(Mapping[str, Any], _freeze({**dict(self.contract_spec), "margin_per_unit": _text(margin_per_unit)}))

    def risk_state_for(self, side: str = "BUY") -> Mapping[str, Any]:
        """Return a mapping suitable for the executor's RiskExit entry gate."""

        required = self.required_margin(side)
        level = self.projected_margin_level(side)
        return cast(
            Mapping[str, Any],
            _freeze(
                {
                    **dict(self.risk_state),
                    "margin_required": _text(required),
                    "margin_level": _text(level),
                    "margin_required_source": "ProtoOAExpectedMarginRes",
                    "margin_level_source": "DERIVED_MARGIN_AVAILABLE_OVER_EXPECTED_MARGIN",
                }
            ),
        )

    def to_update_risk_kwargs(self, side: str = "BUY") -> dict[str, Any]:
        """Return exact metrics accepted by ``executor.update_risk_metrics``."""

        required = self.required_margin(side)
        return {
            "equity": self.equity,
            "margin_available": self.margin_available,
            "margin_required": required,
            "margin_level": self.projected_margin_level(side),
            "used_margin": self.used_margin,
            "observed_at": self.observed_at,
            "connection_generation": self.connection_generation,
        }

    def to_contract_spec(self, side: str = "BUY") -> dict[str, Any]:
        return dict(self.contract_spec_for(side))

    def to_risk_entry_evidence(self, side: str = "BUY") -> dict[str, Mapping[str, Any]]:
        return {"contract_spec": self.contract_spec_for(side), "risk_state": self.risk_state_for(side)}

    def to_dict(self) -> dict[str, Any]:
        return {
            "account_id": self.account_id,
            "symbol": self.symbol,
            "symbol_id": self.symbol_id,
            "quantity": _text(self.quantity),
            "quantity_protocol": self.quantity_protocol,
            "equity": _text(self.equity),
            "used_margin": _text(self.used_margin),
            "margin_available": _text(self.margin_available),
            "buy_margin_required": _text(self.buy_margin_required),
            "sell_margin_required": _text(self.sell_margin_required),
            "buy_margin_level": _text(self.buy_margin_level),
            "sell_margin_level": _text(self.sell_margin_level),
            "observed_at": _iso(self.observed_at),
            "connection_generation": self.connection_generation,
            "session_id": self.session_id,
            "window_start": _iso(self.window_start),
            "window_end": _iso(self.window_end),
            "max_holding_seconds": _text(self.max_holding_seconds),
            "quote": self.quote.to_dict(),
            "expected_margin": self.expected_margin.to_dict(),
            "contract_spec": _thaw(self.contract_spec),
            "calendar_state": _thaw(self.calendar_state),
            "risk_state": _thaw(self.risk_state),
            "assumptions": _thaw(self.assumptions),
            "provenance": _thaw(self.provenance),
            "complete": self.complete,
        }


def observe_canary_economics(  # noqa: C901 - one bounded read-only projection gate
    provider: Any,
    account_snapshot: AccountRiskSnapshot,
    *,
    window_start: datetime,
    window_end: datetime,
    quantity: Decimal | int | str = CANARY_MAX_QUANTITY,
    max_holding_seconds: Decimal | int | str = CANARY_MAX_HOLDING_SECONDS,
    now: datetime | None = None,
    clock: Callable[[], datetime] | None = None,
    max_age_seconds: Decimal | int | str = DEFAULT_MAX_AGE_SECONDS,
    exit_slippage_hypothesis: ExitSlippageHypothesis | None = None,
    exit_slippage_basis: str | None = None,
    exit_slippage_source: str | None = None,
    # Legacy compatibility only. New callers must use the typed hypothesis
    # seam above; these fields must not be populated from CanaryApproval.
    exit_slippage_pips: Decimal | int | str | None = None,
    exit_slippage_approved: bool | None = None,
    exit_slippage_approval_source: str | None = None,
    proto: Any | None = None,
) -> CanaryEconomicsProjection:
    """Build one read-only EUR/USD canary economics projection.

    The slippage input is an implementation hypothesis, not a human approval
    and not a broker observation. It is forecast-only: it does not guarantee
    realized slippage, commissions, or other charges. The function never
    sends an order.

    An explicit ``now`` is a frozen caller as-of reference and therefore
    rejects response timestamps that arrive after it.  Omit ``now`` to use
    the injected/default clock at the terminal end of the request sequence.
    """

    if not isinstance(account_snapshot, AccountRiskSnapshot):
        raise CanaryEconomicsError("account_snapshot debe ser AccountRiskSnapshot tipado")
    proto_module = proto if proto is not None else load_official_proto()
    start_now = _utc(now if now is not None else (clock or _utc_now)(), "now")
    quantity_value = _decimal(quantity, "quantity", positive=True)
    holding = _decimal(max_holding_seconds, "max_holding_seconds", positive=True)
    max_age = _decimal(max_age_seconds, "max_age_seconds", positive=True)
    if quantity_value > CANARY_MAX_QUANTITY:
        raise CanaryEconomicsError("quantity excede el máximo canary de 1000 unidades base")
    if holding > CANARY_MAX_HOLDING_SECONDS:
        raise CanaryEconomicsError("max_holding_seconds excede 300 segundos")
    slippage_hypothesis, legacy_compatibility = _resolve_slippage_hypothesis(
        exit_slippage_hypothesis,
        exit_slippage_basis,
        exit_slippage_source,
        exit_slippage_pips,
        exit_slippage_approved,
        exit_slippage_approval_source,
    )
    start = _utc(window_start, "window_start")
    end = _utc(window_end, "window_end")
    if end <= start or start_now < start or start_now + timedelta(seconds=float(holding)) > end:
        raise CanaryEconomicsError("la ventana canary no cubre el holding observado")

    session = _validated_session(provider, account_snapshot)
    catalog = _selected_symbol(provider)
    symbol_id = _positive_int(_field(catalog, "symbolId", "symbol_id"), "symbol_id")
    symbol = _normalise_symbol(getattr(getattr(provider, "spec", None), "symbol", None))
    if symbol != CANARY_SYMBOL:
        raise CanaryEconomicsError("la proyección sólo admite EUR/USD")
    spec_id = _positive_int(getattr(getattr(provider, "spec", None), "symbol_id", None), "spec.symbol_id")
    if spec_id != symbol_id:
        raise CanaryEconomicsError("catalog symbol_id no coincide con provider.spec")

    quote = _quote_evidence(provider, symbol, symbol_id, session["generation"], catalog)
    volume_protocol = _protocol_volume(quantity_value)
    volume_grid = _validate_volume(catalog, volume_protocol, quantity_value)
    trader, trader_response = _read_trader(proto_module, session, max_age)
    assets, assets_response = _read_assets(proto_module, session, max_age)
    account_deposit_id = _positive_int(_field(trader, "depositAssetId", "deposit_asset_id"), "trader.depositAssetId")
    base_asset_id = _positive_int(_field(catalog, "baseAssetId", "base_asset_id"), "symbol.baseAssetId")
    quote_asset_id = _positive_int(_field(catalog, "quoteAssetId", "quote_asset_id"), "symbol.quoteAssetId")
    _validate_asset_mapping(assets, base_asset_id, quote_asset_id, account_deposit_id)
    expected_margin, margin_response = _read_expected_margin(proto_module, session, symbol_id, volume_protocol, max_age)
    terminal_now = start_now if now is not None else _utc((clock or _utc_now)(), "now")
    if terminal_now < start_now:
        raise CanaryEconomicsError("el reloj caller retrocedió durante la observación")
    if terminal_now.date() != start_now.date():
        raise CanaryEconomicsError("la observación canary cruzó medianoche UTC")
    _validate_snapshot_fresh(account_snapshot, terminal_now, max_age, session["generation"])
    _validate_response_freshness((trader_response, assets_response, margin_response), terminal_now, max_age)
    _validate_quote_freshness(quote, terminal_now, max_age)
    snapshot_observed_at = account_snapshot.observed_at
    assert snapshot_observed_at is not None
    evidence_observed_at = max(snapshot_observed_at, expected_margin.observed_at, quote.available_at)
    equity = _snapshot_decimal(account_snapshot.equity, "account_snapshot.equity", positive=True)
    used_margin = _snapshot_decimal(account_snapshot.used_margin, "account_snapshot.used_margin", nonnegative=True)
    if not account_snapshot.positions_complete or account_snapshot.positions:
        raise CanaryEconomicsError("el canary requiere snapshot de posiciones completo y plano")
    if used_margin != 0 or account_snapshot.margin_state != "NO_MARGIN_USED":
        raise CanaryEconomicsError("used_margin=0 y margin_state=NO_MARGIN_USED son obligatorios")
    margin_available = equity
    if expected_margin.buy_margin <= 0 or expected_margin.sell_margin <= 0:
        raise CanaryEconomicsError("expected margin positivo BUY/SELL es obligatorio")
    with decimal_context():
        buy_level = (margin_available / expected_margin.buy_margin) * Decimal("100")
        sell_level = (margin_available / expected_margin.sell_margin) * Decimal("100")

    economics = _catalog_economics(catalog, quote, quantity_value, volume_protocol, volume_grid, slippage_hypothesis)
    calendar_state = _financing_calendar(catalog, start_now, end, holding)
    contract = _contract_spec(economics, quote, quantity_value, volume_protocol, volume_grid, slippage_hypothesis)
    risk_state = {
        "equity": _text(equity),
        "equity_source": "OBSERVED_DEMO",
        "margin_available": _text(margin_available),
        "margin_available_source": "DERIVED_FLAT_ACCOUNT_NO_USED_MARGIN",
        "used_margin": _text(used_margin),
        "positions": 0,
        "intents": 0,
        "costs_known": True,
        "observed_at": _iso(account_snapshot.observed_at),
        "connection_generation": session["generation"],
    }
    assumptions = {
        "exit_slippage_pips": _text(cast(Decimal, slippage_hypothesis.pips)),
        "exit_slippage_observed": False,
        "exit_slippage_basis": slippage_hypothesis.basis,
        "exit_slippage_source": slippage_hypothesis.source,
        "legacy_compatibility_path": legacy_compatibility,
        "risk_envelope_status": "PLANNED_FORECAST_ONLY",
        "risk_envelope_guarantee": False,
        "unit_value_basis": "1 USD per price unit per EUR base unit from observed EUR/USD asset mapping",
        "minimum_stop_distance_basis": "POLICY_PRICE_QUANTUM_WHEN_BROKER_DISTANCE_ZERO",
        "financing_exclusion_basis": "MAX_HOLDING_WINDOW_OFF_OBSERVED_ROLLOVER",
    }
    provenance = {
        "source": "ctrader-open-api-read-only",
        "environment": "DEMO",
        "endpoint": DEMO_ENDPOINT,
        "account_id": session["account_id"],
        "session_id": session["session_id"],
        "connection_generation": session["generation"],
        "scopes": tuple(sorted(session["scopes"])),
        "account_snapshot": {
            "observed_at": _iso(account_snapshot.observed_at),
            "fresh": account_snapshot.fresh,
            "complete": account_snapshot.complete,
            "account_complete": getattr(account_snapshot, "account_complete", False),
            "daily_complete": account_snapshot.complete,
        },
        "freshness_checked_at": _iso(terminal_now),
        "evidence_observed_at": _iso(evidence_observed_at),
        "trader_response": _response_provenance(trader_response),
        "asset_response": _response_provenance(assets_response),
        "expected_margin_response": _response_provenance(margin_response),
        "catalog_observed": True,
        "quote": quote.to_dict(),
        "raw_catalog_fields": {
            "symbol_id": symbol_id,
            "base_asset_id": base_asset_id,
            "quote_asset_id": quote_asset_id,
            "deposit_asset_id": account_deposit_id,
        },
        "exit_slippage_hypothesis": slippage_hypothesis.to_dict(),
    }
    return CanaryEconomicsProjection(
        session["account_id"],
        symbol,
        symbol_id,
        quantity_value,
        volume_protocol,
        equity,
        used_margin,
        margin_available,
        expected_margin.buy_margin,
        expected_margin.sell_margin,
        buy_level,
        sell_level,
        evidence_observed_at,
        session["generation"],
        session["session_id"],
        start,
        end,
        holding,
        quote,
        expected_margin,
        contract,
        calendar_state,
        risk_state,
        assumptions,
        provenance,
    )


def prepare_canary_economics(*args: Any, **kwargs: Any) -> CanaryEconomicsProjection:
    """Descriptive alias for callers that name preparation explicitly."""

    return observe_canary_economics(*args, **kwargs)


def _resolve_slippage_hypothesis(
    hypothesis: ExitSlippageHypothesis | None,
    typed_basis: str | None,
    typed_source: str | None,
    legacy_pips: Decimal | int | str | None,
    legacy_approved: bool | None,
    legacy_source: str | None,
) -> tuple[ExitSlippageHypothesis, bool]:
    if hypothesis is not None:
        if (
            typed_basis is not None
            or typed_source is not None
            or legacy_pips is not None
            or legacy_approved is not None
            or legacy_source is not None
        ):
            raise CanaryEconomicsError("no mezcles seam tipado de hipótesis con campos legacy")
        if not isinstance(hypothesis, ExitSlippageHypothesis):
            raise CanaryEconomicsError("exit_slippage_hypothesis debe ser tipado")
        return hypothesis, False
    if typed_basis is not None or typed_source is not None:
        if legacy_pips is None or legacy_approved is not None or legacy_source is not None:
            raise CanaryEconomicsError("seam tipado requiere sólo pips, basis y source explícitos")
        return ExitSlippageHypothesis(legacy_pips, typed_basis or "", typed_source or ""), False
    if legacy_pips is None and legacy_approved is None and legacy_source is None:
        raise CanaryEconomicsError("exit_slippage_hypothesis explícita es obligatoria")
    if legacy_pips is None or legacy_approved is not True or not str(legacy_source or "").strip():
        raise CanaryEconomicsError("seam legacy requiere pips, approved=true y source no vacío")
    # Compatibility for existing offline callers only.  The legacy human
    # source is deliberately not copied into the projection as an approval.
    return ExitSlippageHypothesis(legacy_pips), True


def _validated_session(provider: Any, snapshot: AccountRiskSnapshot) -> dict[str, Any]:
    client = getattr(provider, "client", None)
    evidence = getattr(client, "authenticated_session_evidence", None)
    validate = getattr(client, "validate_session_evidence", None)
    request = getattr(client, "request_message", None)
    if not callable(evidence) or not callable(validate) or not callable(request):
        raise CanaryEconomicsError("provider debe enlazar el cliente autenticado existente")
    try:
        proof = evidence()
        if validate(proof) is not True:
            raise CanaryEconomicsError("la evidencia de sesión no fue validada")
    except CanaryEconomicsError:
        raise
    except Exception as exc:
        raise CanaryEconomicsError("no se pudo validar la sesión autenticada") from exc
    account_id = str(_proof(proof, "account_id", "accountId", "ctidTraderAccountId") or "").strip()
    session_id = str(_proof(proof, "session_id", "sessionId") or "").strip()
    generation = str(_proof(proof, "connection_generation", "generation") or "").strip()
    environment = str(_proof(proof, "environment", "mode") or "").strip().upper()
    endpoint = _normalise_endpoint(_proof(proof, "endpoint", "host"))
    scopes = _scopes(_proof(proof, "scopes", "permissions"))
    if not account_id.isdigit() or int(account_id) <= 0 or not session_id or not generation:
        raise CanaryEconomicsError("la evidencia de sesión carece de identidad completa")
    if environment != "DEMO" or endpoint != DEMO_ENDPOINT:
        raise CanaryEconomicsError("la sesión debe ser DEMO en el endpoint oficial")
    if "accounts" not in scopes:
        raise CanaryEconomicsError("la preparación requiere scope accounts/SCOPE_VIEW")
    if account_id != str(snapshot.account_id) or session_id != str(snapshot.session_id):
        raise CanaryEconomicsError("la sesión no coincide con account_snapshot")
    if generation != str(snapshot.connection_generation):
        raise CanaryEconomicsError("la generación no coincide con account_snapshot")
    return {
        "account_id": account_id,
        "session_id": session_id,
        "generation": generation,
        "environment": environment,
        "endpoint": endpoint,
        "scopes": scopes,
        "client": client,
    }


def _request(
    session: Mapping[str, Any],
    proto: Any,
    name: str,
    expected_payload_type: int,
    account_id: str,
    timeout_seconds: Decimal,
    **fields: Any,
) -> tuple[Any, WireMessage]:
    factory = getattr(proto, name, None)
    if factory is None and isinstance(proto, Mapping):
        factory = proto.get(name)
    if not callable(factory):
        raise CanaryEconomicsProtocolError(f"codec oficial carece de {name}")
    try:
        message = factory(ctidTraderAccountId=int(account_id), **fields)
    except Exception as exc:
        raise CanaryEconomicsProtocolError(f"no se pudo construir {name}") from exc
    _validate_request_fields(message, name, account_id, fields)
    request_id = f"canary-economics:{uuid.uuid4().hex[:12]}:{name}"
    try:
        wire = session["client"].request_message(
            message, client_msg_id=request_id, timeout_seconds=float(timeout_seconds)
        )
    except Exception as exc:
        raise CanaryEconomicsError(f"falló {name}: {type(exc).__name__}") from exc
    if not isinstance(wire, WireMessage):
        raise CanaryEconomicsProtocolError(f"{name} no devolvió WireMessage")
    if wire.client_msg_id != request_id or wire.payload_type_id != expected_payload_type:
        raise CanaryEconomicsProtocolError(f"respuesta inválida o no correlacionada en {name}")
    response_account = read_field(wire.payload, "ctidTraderAccountId", "ctid_trader_account_id", default=None)
    if response_account is None or str(response_account) != account_id:
        raise CanaryEconomicsProtocolError(f"{name} no coincide con account_id")
    generation = None if wire.connection_generation is None else str(wire.connection_generation).strip()
    if generation != str(session["generation"]):
        raise CanaryEconomicsError(f"{name} pertenece a otra generación")
    if wire.available_at is None and wire.received_at is None:
        raise CanaryEconomicsError(f"{name} carece de timestamp local")
    return wire.payload, wire


def _read_trader(
    proto: Any, session: Mapping[str, Any], timeout_seconds: Decimal
) -> tuple[Mapping[str, Any], WireMessage]:
    payload, wire = _request(session, proto, "ProtoOATraderReq", TRADER_RES, session["account_id"], timeout_seconds)
    trader = read_field(payload, "trader", default=None)
    trader_map = _as_mapping(trader)
    if trader_map is None:
        raise CanaryEconomicsProtocolError("ProtoOATraderRes carece de trader")
    observed = str(_field(trader_map, "ctidTraderAccountId", "ctid_trader_account_id") or "")
    if observed != str(session["account_id"]):
        raise CanaryEconomicsProtocolError("trader no coincide con account_id")
    return trader_map, wire


def _read_assets(
    proto: Any, session: Mapping[str, Any], timeout_seconds: Decimal
) -> tuple[dict[int, str], WireMessage]:
    payload, wire = _request(
        session, proto, "ProtoOAAssetListReq", ASSET_LIST_RES, session["account_id"], timeout_seconds
    )
    raw_assets = read_repeated(payload, "asset", "assets")
    if not raw_assets:
        raise CanaryEconomicsProtocolError("ProtoOAAssetListRes carece de assets completos")
    result: dict[int, str] = {}
    for raw in raw_assets:
        mapped = _as_mapping(raw)
        if mapped is None:
            raise CanaryEconomicsProtocolError("asset observado no es estructurado")
        asset_id = _positive_int(_field(mapped, "assetId", "asset_id"), "asset.assetId")
        name = str(_field(mapped, "name", "displayName", "display_name") or "").strip().upper()
        if not name:
            raise CanaryEconomicsError("asset observado carece de nombre")
        if asset_id in result and result[asset_id] != name:
            raise CanaryEconomicsError("asset_id tiene nombres observados ambiguos")
        result[asset_id] = name
    return result, wire


def _read_expected_margin(
    proto: Any, session: Mapping[str, Any], symbol_id: int, volume_protocol: int, timeout_seconds: Decimal
) -> tuple[ExpectedMarginEvidence, WireMessage]:
    payload, wire = _request(
        session,
        proto,
        "ProtoOAExpectedMarginReq",
        EXPECTED_MARGIN_RES,
        session["account_id"],
        timeout_seconds,
        symbolId=symbol_id,
        volume=[volume_protocol],
    )
    response_symbol = read_field(payload, "symbolId", "symbol_id", default=None)
    if response_symbol is not None and int(response_symbol) != symbol_id:
        raise CanaryEconomicsProtocolError("ExpectedMarginRes symbol_id inesperado")
    digits = _positive_int(
        read_field(payload, "moneyDigits", "money_digits", default=None), "margin.moneyDigits", allow_zero=True
    )
    entries = read_repeated(payload, "margin")
    if len(entries) != 1:
        raise CanaryEconomicsError("ExpectedMarginRes parcial o ambiguo; se requiere una pareja BUY/SELL")
    entry = _as_mapping(entries[0])
    if entry is None:
        raise CanaryEconomicsProtocolError("ExpectedMarginRes.margin no es estructurado")
    buy_raw = _integer(_field(entry, "buyMargin", "buy_margin"), "buyMargin", nonnegative=True)
    sell_raw = _integer(_field(entry, "sellMargin", "sell_margin"), "sellMargin", nonnegative=True)
    buy = _scale(buy_raw, digits)
    sell = _scale(sell_raw, digits)
    observed_at = wire.available_at or wire.received_at
    if observed_at is None:
        raise CanaryEconomicsError("ExpectedMarginRes carece de timestamp")
    return (
        ExpectedMarginEvidence(
            str(session["account_id"]),
            symbol_id,
            volume_protocol,
            digits,
            buy,
            sell,
            observed_at,
            wire.received_at,
            str(session["generation"]),
            str(wire.client_msg_id),
        ),
        wire,
    )


def _quote_evidence(
    provider: Any,
    symbol: str,
    symbol_id: int,
    generation: str,
    catalog: Mapping[str, Any],
) -> CanaryQuoteEvidence:
    snapshotter = getattr(provider, "snapshot_quote_state", None)
    if not callable(snapshotter):
        raise CanaryEconomicsError("provider no expone snapshot_quote_state()")
    raw = snapshotter()
    if not isinstance(raw, Mapping) or not isinstance(raw.get("symbols"), Mapping):
        raise CanaryEconomicsError("snapshot BBO incompleto")
    book = raw["symbols"].get(str(symbol_id), raw["symbols"].get(symbol_id))
    if not isinstance(book, Mapping):
        raise CanaryEconomicsError("BBO del símbolo no observado")
    bid = _quote_leg(book.get("bid"), "bid")
    ask = _quote_leg(book.get("ask"), "ask")
    if bid["generation"] != ask["generation"] or bid["generation"] != generation:
        raise CanaryEconomicsError("BBO no comparte la generación DEMO observada")
    event_time = max(bid["event_time"], ask["event_time"])
    # A single BBO is usable only once both asynchronous legs are available;
    # retain the older receipt separately so freshness cannot be masked by
    # taking only the newer leg.
    available_at = max(bid["available_at"], ask["available_at"])
    earliest_available_at = min(bid["available_at"], ask["available_at"])
    bid_price = cast(Decimal, bid["price"])
    ask_price = cast(Decimal, ask["price"])
    if bid_price >= ask_price:
        raise CanaryEconomicsError("BBO cruzado o de spread cero")
    digits = _positive_int(_field(catalog, "digits"), "symbol.digits")
    pip_position = _positive_int(_field(catalog, "pipPosition", "pip_position"), "symbol.pipPosition")
    if pip_position > digits:
        raise CanaryEconomicsError("pipPosition no puede exceder digits")
    pip_size = Decimal(1).scaleb(-pip_position)
    price_quantum = Decimal(1).scaleb(-digits)
    for side, price in (("bid", bid_price), ("ask", ask_price)):
        if price % price_quantum != 0:
            raise CanaryEconomicsError(f"BBO {side} excede price quantum observado")
    spread = ask_price - bid_price
    with decimal_context():
        spread_pips = spread / pip_size
    return CanaryQuoteEvidence(
        symbol,
        symbol_id,
        bid_price,
        ask_price,
        spread,
        spread_pips,
        event_time,
        available_at,
        generation,
        bid["sequence"] if bid["sequence"] is not None else ask["sequence"],
        earliest_available_at,
    )


def _quote_leg(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise CanaryEconomicsError(f"BBO {name} ausente")
    if value.get("timestamp_missing") is True or value.get("synthetic") is True:
        raise CanaryEconomicsError(f"BBO {name} carece de evidencia causal")
    state = value.get("state")
    if state is not None and str(state).strip().upper() != "VALID":
        raise CanaryEconomicsError(f"BBO {name} no es VALID")
    reasons = value.get("reasons", ())
    if reasons:
        raise CanaryEconomicsError(f"BBO {name} conserva razones de calidad")
    price = _decimal(value.get("price"), f"{name}.price", positive=True)
    event_time = _utc(value.get("event_time"), f"{name}.event_time")
    available_at = _utc(value.get("available_at"), f"{name}.available_at")
    if available_at < event_time:
        raise CanaryEconomicsError(f"BBO {name}.available_at precede event_time")
    generation = str(value.get("generation", "")).strip()
    if not generation:
        raise CanaryEconomicsError(f"BBO {name}.generation ausente")
    sequence = value.get("sequence")
    sequence_text = str(sequence) if sequence is not None else None
    return {
        "price": price,
        "event_time": event_time,
        "available_at": available_at,
        "generation": generation,
        "sequence": sequence_text,
    }


def _catalog_economics(
    catalog: Mapping[str, Any],
    quote: CanaryQuoteEvidence,
    quantity: Decimal,
    volume_protocol: int,
    volume_grid: tuple[int, int, int],
    slippage_hypothesis: ExitSlippageHypothesis,
) -> dict[str, Any]:
    digits = _positive_int(_field(catalog, "digits"), "symbol.digits")
    pip_position = _positive_int(_field(catalog, "pipPosition", "pip_position"), "symbol.pipPosition")
    if pip_position > digits:
        raise CanaryEconomicsError("pipPosition inválido para EUR/USD")
    price_quantum = Decimal(1).scaleb(-digits)
    pip_size = Decimal(1).scaleb(-pip_position)
    distance_set = _enum_catalog(
        _field(catalog, "distanceSetIn", "distance_set_in"),
        {1: DISTANCE_IN_POINTS},
    )
    sl_distance = _integer(_field(catalog, "slDistance", "sl_distance"), "symbol.slDistance", nonnegative=True)
    tp_distance = _integer(_field(catalog, "tpDistance", "tp_distance"), "symbol.tpDistance", nonnegative=True)
    if distance_set != DISTANCE_IN_POINTS or sl_distance != 0 or tp_distance != 0:
        raise CanaryEconomicsError("EUR/USD canary requiere sl/tp distance observado en cero puntos")
    commission_type = _enum_catalog(_field(catalog, "commissionType", "commission_type"), {2: USD_PER_LOT})
    if commission_type != USD_PER_LOT:
        raise CanaryEconomicsError("commissionType distinto de USD_PER_LOT no está soportado")
    rate_raw = _integer(
        _field(catalog, "preciseTradingCommissionRate", "precise_trading_commission_rate"),
        "preciseTradingCommissionRate",
        nonnegative=True,
    )
    if rate_raw <= 0:
        raise CanaryEconomicsError("preciseTradingCommissionRate debe ser positivo y observado")
    min_raw = _integer(
        _field(catalog, "preciseMinCommission", "precise_min_commission"),
        "preciseMinCommission",
        nonnegative=True,
    )
    if min_raw != 0:
        raise CanaryEconomicsError("preciseMinCommission distinto de cero no está soportado")
    lot_size_protocol = _positive_int(_field(catalog, "lotSize", "lot_size"), "symbol.lotSize")
    if lot_size_protocol % PROTOCOL_VOLUME_SCALE:
        raise CanaryEconomicsError("lotSize no representa unidades base exactas")
    lot_units = Decimal(lot_size_protocol) / Decimal(PROTOCOL_VOLUME_SCALE)
    with decimal_context():
        commission_per_lot = Decimal(rate_raw) / Decimal(10**8)
        commission_side_per_unit = commission_per_lot / lot_units
        commission_roundtrip_per_unit = commission_side_per_unit * Decimal("2")
        commission_roundtrip_total = commission_roundtrip_per_unit * quantity
        slippage_per_unit = cast(Decimal, slippage_hypothesis.pips) * pip_size
        slippage_total = slippage_per_unit * quantity
    return {
        "digits": digits,
        "pip_position": pip_position,
        "distance_set_in": distance_set,
        "sl_distance_points": sl_distance,
        "tp_distance_points": tp_distance,
        "price_quantum": price_quantum,
        "pip_size": pip_size,
        "unit_value": Decimal("1"),
        "commission_type": commission_type,
        "precise_trading_commission_rate_raw": rate_raw,
        "commission_per_lot": commission_per_lot,
        "lot_size_protocol": lot_size_protocol,
        "lot_units_base": lot_units,
        "commission_side_per_unit": commission_side_per_unit,
        "commission_roundtrip_per_unit": commission_roundtrip_per_unit,
        "commission_roundtrip_total": commission_roundtrip_total,
        "precise_min_commission": Decimal("0"),
        "slippage_per_unit": slippage_per_unit,
        "slippage_total": slippage_total,
        "quantity": quantity,
        "quantity_protocol": volume_protocol,
        "volume_grid": volume_grid,
        "spread": quote.spread,
        "spread_pips": quote.spread_pips,
        "exit_slippage_basis": slippage_hypothesis.basis,
        "exit_slippage_source": slippage_hypothesis.source,
    }


def _contract_spec(
    economics: Mapping[str, Any],
    quote: CanaryQuoteEvidence,
    quantity: Decimal,
    volume_protocol: int,
    volume_grid: tuple[int, int, int],
    slippage_hypothesis: ExitSlippageHypothesis,
) -> dict[str, Any]:
    price_quantum = cast(Decimal, economics["price_quantum"])
    minimum_protocol, maximum_protocol, step_protocol = volume_grid
    with decimal_context():
        broker_min_quantity = Decimal(minimum_protocol) / Decimal(PROTOCOL_VOLUME_SCALE)
        broker_max_quantity = Decimal(maximum_protocol) / Decimal(PROTOCOL_VOLUME_SCALE)
        broker_step_quantity = Decimal(step_protocol) / Decimal(PROTOCOL_VOLUME_SCALE)
    return {
        "known": True,
        "symbol": CANARY_SYMBOL,
        "quantity_min": _text(quantity),
        "quantity_step": _text(broker_step_quantity),
        "quantity_max": _text(min(CANARY_MAX_QUANTITY, broker_max_quantity)),
        "quantity_policy_max": _text(CANARY_MAX_QUANTITY),
        "quantity_protocol": volume_protocol,
        "volume_scale": PROTOCOL_VOLUME_SCALE,
        "broker_min_volume_protocol": minimum_protocol,
        "broker_max_volume_protocol": maximum_protocol,
        "broker_step_volume_protocol": step_protocol,
        "broker_min_quantity": _text(broker_min_quantity),
        "broker_max_quantity": _text(broker_max_quantity),
        "quantity_min_source": "CANARY_POLICY_BOUND",
        "pip_size": _text(cast(Decimal, economics["pip_size"])),
        "price_quantum": _text(price_quantum),
        "unit_value": _text(cast(Decimal, economics["unit_value"])),
        "unit_value_currency": "USD",
        "unit_value_basis": "observed EUR base / USD quote / USD deposit asset mapping",
        "minimum_stop_distance": _text(price_quantum),
        "minimum_stop_distance_source": "POLICY_PRICE_QUANTUM",
        "broker_minimum_stop_distance": "0",
        "broker_sl_distance_points": 0,
        "broker_tp_distance_points": 0,
        "broker_distance_set_in": DISTANCE_IN_POINTS,
        "fees_known": True,
        "spread_known": True,
        "expected_costs_known": True,
        "risk_envelope_known": True,
        "expected_cost_currency": "USD",
        "expected_cost_source": "OBSERVED_CTRADER_SYMBOL_PLUS_IMPLEMENTATION_HYPOTHESIS",
        "expected_cost_fixed": "0",
        "expected_commission_fixed": "0",
        "expected_cost_per_unit": _text(cast(Decimal, economics["commission_roundtrip_per_unit"])),
        "expected_commission_per_unit": _text(cast(Decimal, economics["commission_roundtrip_per_unit"])),
        "expected_exit_slippage_per_unit": _text(cast(Decimal, economics["slippage_per_unit"])),
        "expected_exit_slippage_pips": _text(cast(Decimal, slippage_hypothesis.pips)),
        "exit_slippage_observed": False,
        "exit_slippage_basis": slippage_hypothesis.basis,
        "exit_slippage_source": slippage_hypothesis.source,
        "risk_envelope_basis": "PLANNED_FORECAST_WITH_IMPLEMENTATION_HYPOTHESIS",
        "risk_envelope_guarantee": False,
        "observed_spread": _text(quote.spread),
        "observed_spread_pips": _text(quote.spread_pips),
        "commission_type": economics["commission_type"],
        "precise_trading_commission_rate_raw": economics["precise_trading_commission_rate_raw"],
        "precise_min_commission": "0",
        "lot_size_protocol": economics["lot_size_protocol"],
        "lot_units_base": _text(cast(Decimal, economics["lot_units_base"])),
        "margin_source": "ProtoOAExpectedMarginRes",
        "financing_source": "observed_symbol_swap_and_schedule_fields",
    }


def _financing_calendar(
    catalog: Mapping[str, Any], now: datetime, window_end: datetime, holding: Decimal
) -> dict[str, Any]:
    swap_time = _integer(_field(catalog, "swapTime", "swap_time"), "symbol.swapTime", nonnegative=True)
    swap_period = _integer(_field(catalog, "swapPeriod", "swap_period"), "symbol.swapPeriod", nonnegative=True)
    timezone_name = str(_field(catalog, "scheduleTimeZone", "schedule_time_zone") or "").strip()
    if swap_period != 24 or not 0 <= swap_time < 24 * 60 or not timezone_name:
        raise CanaryEconomicsError("swapTime/swapPeriod/scheduleTimeZone observados incompletos")
    try:
        timezone = ZoneInfo(timezone_name)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise CanaryEconomicsError("scheduleTimeZone observado no es resoluble") from exc
    local_now = now.astimezone(timezone)
    local_end = window_end.astimezone(timezone)
    local_holding_end = (now + timedelta(seconds=float(holding))).astimezone(timezone)
    rollover_time = time(swap_time // 60, swap_time % 60)
    candidate = datetime.combine(local_now.date(), rollover_time, tzinfo=timezone)
    if candidate <= local_now:
        candidate = candidate + timedelta(days=1)
    if candidate <= local_holding_end:
        raise CanaryEconomicsError("ventana de holding cruza rollover; financiación no excluible")
    return {
        "known": True,
        "basis": "OBSERVED_BROKER",
        "financing_known": True,
        "financing_excluded": True,
        "swap_time_minutes": swap_time,
        "swap_period_hours": swap_period,
        "schedule_timezone": timezone_name,
        "next_rollover_at": _iso(candidate.astimezone(UTC)),
        "observation_window_end": _iso(local_end.astimezone(UTC)),
        "max_holding_seconds": _text(holding),
    }


def _validate_snapshot_fresh(snapshot: AccountRiskSnapshot, now: datetime, max_age: Decimal, generation: str) -> None:
    if snapshot.fresh is not True or getattr(snapshot, "account_complete", False) is not True:
        raise CanaryEconomicsError("account risk snapshot debe ser fresh y account_complete")
    if snapshot.connection_generation != generation:
        raise CanaryEconomicsError("account risk snapshot pertenece a otra generación")
    if snapshot.observed_at is None:
        raise CanaryEconomicsError("account risk snapshot carece de observed_at")
    age = (now - snapshot.observed_at).total_seconds()
    if age < 0 or Decimal(str(age)) > max_age:
        raise CanaryEconomicsError(f"account risk snapshot fuera de frescura: age={age:.3f}")


def _validate_response_freshness(responses: Sequence[WireMessage], now: datetime, max_age: Decimal) -> None:
    for response in responses:
        observed_at = response.available_at or response.received_at
        if observed_at is None:
            raise CanaryEconomicsError("respuesta cTrader carece de timestamp local")
        age = (now - observed_at).total_seconds()
        if age < 0 or Decimal(str(age)) > max_age:
            raise CanaryEconomicsError(f"respuesta cTrader fuera de frescura: age={age:.3f}")


def _validate_quote_freshness(quote: CanaryQuoteEvidence, now: datetime, max_age: Decimal) -> None:
    latest_age = (now - quote.available_at).total_seconds()
    earliest = quote.earliest_available_at or quote.available_at
    earliest_age = (now - earliest).total_seconds()
    if latest_age < 0:
        raise CanaryEconomicsError(f"BBO disponible en el futuro: age={latest_age:.3f}")
    if earliest_age < 0:
        raise CanaryEconomicsError(f"BBO timestamp antiguo en el futuro: age={earliest_age:.3f}")
    if Decimal(str(earliest_age)) > max_age:
        raise CanaryEconomicsError(f"BBO fuera de frescura: oldest_age={earliest_age:.3f}")


def _validate_asset_mapping(assets: Mapping[int, str], base_id: int, quote_id: int, deposit_id: int) -> None:
    if base_id not in assets or quote_id not in assets or deposit_id not in assets:
        raise CanaryEconomicsError("base/quote/deposit asset mapping incompleto")
    if base_id == quote_id or quote_id != deposit_id:
        raise CanaryEconomicsError("asset mapping EUR/USD no coincide con deposit asset")
    if assets[base_id] != "EUR" or assets[quote_id] != "USD" or assets[deposit_id] != "USD":
        raise CanaryEconomicsError("la proyección sólo admite EUR base y USD quote/deposit")


def _validate_volume(catalog: Mapping[str, Any], volume_protocol: int, quantity: Decimal) -> tuple[int, int, int]:
    minimum = _integer(_field(catalog, "minVolume", "min_volume"), "symbol.minVolume", nonnegative=True)
    maximum = _positive_int(_field(catalog, "maxVolume", "max_volume"), "symbol.maxVolume")
    step = _positive_int(_field(catalog, "stepVolume", "step_volume"), "symbol.stepVolume")
    if volume_protocol > maximum or volume_protocol < minimum:
        raise CanaryEconomicsError("quantity queda fuera del rango de volumen observado")
    if (volume_protocol - minimum) % step:
        raise CanaryEconomicsError("quantity no coincide con stepVolume observado")
    if quantity > CANARY_MAX_QUANTITY:
        raise CanaryEconomicsError("quantity excede el máximo operativo")
    return minimum, maximum, step


def _selected_symbol(provider: Any) -> Mapping[str, Any]:
    catalog = getattr(provider, "catalog", None)
    selected = getattr(catalog, "selected", None)
    symbols = getattr(catalog, "symbols", None)
    if selected is None or not isinstance(symbols, Sequence) or not any(item is selected for item in symbols):
        raise CanaryEconomicsError("catalog selected no está ligado a su lista observada")
    metadata = getattr(selected, "metadata", None)
    metadata_map = _as_mapping(metadata)
    if metadata_map is None:
        raise CanaryEconomicsError("catalog metadata no es estructurado")
    full_raw = metadata_map.get("fullSymbol", metadata_map.get("full_symbol", metadata_map))
    full = _as_mapping(full_raw)
    if full is None:
        raise CanaryEconomicsError("catalog fullSymbol no observado")
    merged = dict(metadata_map)
    merged.update(full)
    selected_id = _positive_int(getattr(selected, "symbol_id", None), "catalog.selected.symbol_id")
    full_id = _positive_int(_field(merged, "symbolId", "symbol_id"), "fullSymbol.symbolId")
    if selected_id != full_id:
        raise CanaryEconomicsError("catalog selected/fullSymbol identity mismatch")
    requested = _normalise_symbol(getattr(catalog, "requested_symbol", None))
    selected_name = _normalise_symbol(getattr(selected, "name", None))
    if requested != CANARY_SYMBOL or selected_name != requested:
        raise CanaryEconomicsError("catalog no identifica exactamente EUR/USD")
    return merged


def _validate_request_fields(message: Any, name: str, account_id: str, fields: Mapping[str, Any]) -> None:
    observed_account = read_field(message, "ctidTraderAccountId", "ctid_trader_account_id", default=None)
    if observed_account is None or str(observed_account) != account_id:
        raise CanaryEconomicsProtocolError(f"{name} account_id no coincide")
    for key, expected in fields.items():
        actual = read_field(message, key, _snake(key), default=None)
        if key == "volume":
            if tuple(int(item) for item in read_repeated(message, key)) != tuple(int(item) for item in expected):
                raise CanaryEconomicsProtocolError("ExpectedMarginReq volume no coincide con cantidad exacta")
        elif actual is None or str(actual) != str(expected):
            raise CanaryEconomicsProtocolError(f"{name} {key} no coincide")


def _response_provenance(wire: WireMessage) -> dict[str, Any]:
    return {
        "payload_type": wire.payload_type_id,
        "client_msg_id": wire.client_msg_id,
        "received_at": _iso(wire.received_at),
        "available_at": _iso(wire.available_at),
        "connection_generation": str(wire.connection_generation) if wire.connection_generation is not None else None,
    }


def _field(value: Any, *names: str) -> Any:
    if isinstance(value, Mapping):
        for name in names:
            if name in value and value[name] is not None:
                return value[name]
        return None
    return read_field(value, *names, default=None)


def _as_mapping(value: Any) -> Mapping[str, Any] | None:
    if isinstance(value, Mapping):
        return {str(key): item for key, item in value.items()}
    if value is None:
        return None
    mapped = message_to_mapping(value)
    return mapped if isinstance(mapped, Mapping) else None


def _proof(value: Any, *names: str) -> Any:
    if isinstance(value, Mapping):
        for name in names:
            if name in value:
                return value[name]
        return None
    for name in names:
        marker = object()
        raw = getattr(value, name, marker)
        if raw is not marker:
            return raw
    return None


def _scopes(value: Any) -> frozenset[str]:
    if value is None:
        return frozenset()
    if isinstance(value, str):
        raw = (value,)
    else:
        try:
            raw = tuple(value)
        except TypeError:
            raw = (value,)
    normalized: set[str] = set()
    for item in raw:
        text = str(getattr(item, "name", item)).strip().upper()
        if text in {"SCOPE_VIEW", "VIEW", "ACCOUNTS"}:
            normalized.add("accounts")
        elif text in {"SCOPE_TRADE", "TRADE", "TRADING"}:
            normalized.update({"accounts", "trading"})
    return frozenset(normalized)


def _normalise_endpoint(value: Any) -> str | None:
    if value is None:
        return None
    raw = str(value).strip().lower().rstrip("/")
    for prefix in ("tcp://", "ssl://"):
        if raw.startswith(prefix):
            raw = raw[len(prefix) :]
    return DEMO_ENDPOINT if raw == DEMO_ENDPOINT else None


def _normalise_symbol(value: Any) -> str:
    return normalize_symbol_name(str(value or ""))


def _enum_catalog(value: Any, numbers: Mapping[int, str]) -> str:
    if isinstance(value, str):
        normalized = value.strip().upper()
        if normalized in numbers.values():
            return normalized
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise CanaryEconomicsError("enum de catálogo ausente o inválido") from None
    if number not in numbers:
        raise CanaryEconomicsError("enum de catálogo no soportado")
    return numbers[number]


def _protocol_volume(quantity: Decimal) -> int:
    with decimal_context():
        scaled = quantity * Decimal(PROTOCOL_VOLUME_SCALE)
    if scaled != scaled.to_integral_value() or scaled <= 0:
        raise CanaryEconomicsError("quantity no representa volumen protocolario exacto")
    return int(scaled)


def _side(value: Any) -> str:
    normalized = str(value).strip().upper()
    if normalized in {"BUY", "LONG"}:
        return "BUY"
    if normalized in {"SELL", "SHORT"}:
        return "SELL"
    raise CanaryEconomicsError("side debe ser BUY o SELL")


def _decimal(value: Any, name: str, *, positive: bool = False, nonnegative: bool = False) -> Decimal:
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise CanaryEconomicsError(f"{name} inválido") from exc
    if not result.is_finite() or (positive and result <= 0) or (nonnegative and result < 0):
        raise CanaryEconomicsError(f"{name} inválido")
    return result


def _snapshot_decimal(value: Any, name: str, *, positive: bool = False, nonnegative: bool = False) -> Decimal:
    return _decimal(value, name, positive=positive, nonnegative=nonnegative)


def _integer(value: Any, name: str, *, nonnegative: bool = False) -> int:
    if isinstance(value, bool) or value is None:
        raise CanaryEconomicsError(f"{name} ausente")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise CanaryEconomicsError(f"{name} inválido") from exc
    if nonnegative and result < 0:
        raise CanaryEconomicsError(f"{name} inválido")
    return result


def _positive_int(value: Any, name: str, *, allow_zero: bool = False) -> int:
    result = _integer(value, name, nonnegative=allow_zero)
    if result == 0 and allow_zero:
        return result
    if result <= 0:
        raise CanaryEconomicsError(f"{name} inválido")
    return result


def _scale(value: int, digits: int) -> Decimal:
    return Decimal(value).scaleb(-digits)


def _utc(value: Any, name: str) -> datetime:
    if isinstance(value, datetime):
        result = value
    else:
        text = str(value or "").strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            result = datetime.fromisoformat(text)
        except ValueError as exc:
            raise CanaryEconomicsError(f"{name} inválido") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise CanaryEconomicsError(f"{name} requiere zona horaria")
    return result.astimezone(UTC)


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _text(value: Decimal | None) -> str | None:
    return format(value, "f") if value is not None else None


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z") if value else None


def _snake(name: str) -> str:
    return "".join("_" + char.lower() if char.isupper() else char for char in name).lstrip("_")


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple, set, frozenset)):
        return tuple(_freeze(item) for item in value)
    return value


def _thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    return value


__all__ = [
    "ASSET_LIST_REQ",
    "ASSET_LIST_RES",
    "CANARY_MAX_HOLDING_SECONDS",
    "CANARY_MAX_QUANTITY",
    "CANARY_SYMBOL",
    "CanaryEconomicsError",
    "CanaryEconomicsProtocolError",
    "CanaryEconomicsProjection",
    "CanaryQuoteEvidence",
    "ExitSlippageHypothesis",
    "ExpectedMarginEvidence",
    "EXPECTED_MARGIN_REQ",
    "EXPECTED_MARGIN_RES",
    "observe_canary_economics",
    "prepare_canary_economics",
]
