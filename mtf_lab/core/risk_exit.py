"""Pure, shared risk sizing and exit decisions for the CFD/PAPER domain.

The module has no provider, clock, persistence or broker dependency.  It is
the single policy seam shared by the research simulator and a future DEMO
composition.  A policy is opt-in: callers that do not provide one retain the
legacy CFD behaviour.

All prices, quantities and money values remain :class:`~decimal.Decimal`.
Stops and targets are *planned* prices only.  An exit decision is actionable
only when the caller supplies an observed executable price; the evaluator
never fabricates a stop price after a gap or a missing quote.
"""

from __future__ import annotations

import dataclasses
import hashlib
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import Any, cast

from .canonical import canonical_json
from .numeric import decimal_context

D0 = Decimal("0")
D1 = Decimal("1")
POLICY_VERSION = "risk-exit-v1"
INTRADAY_PROFILE = "INTRADAY"
MULTIDAY_PROFILE = "MULTIDAY"
EXIT_NONE = "NONE"
EXIT_STOP_LOSS = "STOP_LOSS"
EXIT_TAKE_PROFIT = "TAKE_PROFIT"
EXIT_TIME = "TIME_EXIT"
EXIT_UNKNOWN = "UNKNOWN"
_PROFILES = frozenset({INTRADAY_PROFILE, MULTIDAY_PROFILE})
_EQUITY_SOURCE_ALIASES = {
    "VIRTUAL_PAPER_ONLY": "VIRTUAL_PAPER_ONLY",
    "OBSERVED_DEMO": "OBSERVED_DEMO",
    "SERVER_OBSERVED": "OBSERVED_DEMO",
}


class RiskExitError(ValueError):
    """Invalid policy input or malformed observed evidence."""


def _decimal(value: Any, *, name: str, minimum: Decimal | None = None, positive: bool = False) -> Decimal:
    if isinstance(value, bool):
        raise RiskExitError(f"{name} no puede ser booleano")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise RiskExitError(f"{name} no es Decimal válido") from exc
    if not result.is_finite():
        raise RiskExitError(f"{name} debe ser finito")
    if positive and result <= D0:
        raise RiskExitError(f"{name} debe ser positivo")
    if minimum is not None and result < minimum:
        raise RiskExitError(f"{name} debe ser >= {minimum}")
    return result


def _integer(value: Any, *, name: str, minimum: int = 0) -> int:
    if isinstance(value, bool):
        raise RiskExitError(f"{name} debe ser entero")
    if isinstance(value, float) and not value.is_integer():
        raise RiskExitError(f"{name} debe ser entero")
    if isinstance(value, Decimal) and value != value.to_integral_value():
        raise RiskExitError(f"{name} debe ser entero")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise RiskExitError(f"{name} debe ser entero") from exc
    if result < minimum:
        raise RiskExitError(f"{name} debe ser >= {minimum}")
    return result


def _utc(value: Any, *, name: str) -> datetime:
    if isinstance(value, datetime):
        result = value
    else:
        text = str(value).strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            result = datetime.fromisoformat(text)
        except (TypeError, ValueError) as exc:
            raise RiskExitError(f"{name} debe ser ISO-8601") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise RiskExitError(f"{name} debe incluir zona horaria")
    return result.astimezone(UTC)


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z") if value else None


def _text(value: Decimal | None) -> str | None:
    return format(value, "f") if value is not None else None


def _optional_decimal(value: Any, name: str) -> Decimal | None:
    return _decimal(value, name=name) if value is not None else None


def _strict_bool(value: Any, *, name: str) -> bool:
    if not isinstance(value, bool):
        raise RiskExitError(f"{name} debe ser booleano")
    return value


def _equity_source(value: Any) -> str:
    return _EQUITY_SOURCE_ALIASES.get(str(value or "").strip().upper(), "")


def _profile(value: Any) -> str:
    raw = str(value or INTRADAY_PROFILE).strip().upper().replace("-", "_")
    aliases = {
        "INTRADAY": INTRADAY_PROFILE,
        "DAY": INTRADAY_PROFILE,
        "MULTIDAY": MULTIDAY_PROFILE,
        "SWING": MULTIDAY_PROFILE,
    }
    try:
        return aliases[raw]
    except KeyError as exc:
        raise RiskExitError(f"holding_profile no soportado: {value!r}") from exc


@dataclasses.dataclass(frozen=True, slots=True)
class RiskExitPolicy:
    """Immutable policy shared by sizing, exits and later DEMO composition."""

    version: str = POLICY_VERSION
    stop_atr_multiple: Decimal = Decimal("1.5")
    take_profit_atr_multiple: Decimal = Decimal("3")
    planned_risk_fraction: Decimal = Decimal("0.0025")
    max_daily_loss_fraction: Decimal = Decimal("0.01")
    max_drawdown_fraction: Decimal = Decimal("0.05")
    max_positions: int = 1
    max_intents: int = 1
    martingale_allowed: bool = False
    holding_profile: str = INTRADAY_PROFILE
    intraday_max_bars: int = 5
    intraday_max_minutes: Decimal = Decimal("30")
    multiday_max_hours: Decimal = Decimal("72")
    multiday_preclose_minutes: Decimal = Decimal("60")
    exit_latency_seconds: Decimal = D0
    initial_equity: Decimal = Decimal("10000")
    equity_basis: str = "VIRTUAL_PAPER_ONLY"
    account_currency: str = "USD"
    quote_currency: str = "USD"

    def __post_init__(self) -> None:
        _validate_policy_identity(self)
        _normalise_policy_decimals(self)
        _normalise_policy_integers(self)
        _validate_policy_ranges(self)
        _normalise_policy_metadata(self)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> RiskExitPolicy:
        if not isinstance(value, Mapping):
            raise RiskExitError("RiskExitPolicy debe ser mapping")
        allowed = {field.name for field in dataclasses.fields(cls)}
        supplied_hash = value.get("policy_hash")
        conventions = {
            "initial_risk_convention": "filled_quantity * stop_distance * unit_value",
            "economic_r_convention": "settled_net_pnl / initial_risk",
            "risk_budget_convention": "account_equity * planned_risk_fraction; not initial_risk",
            "risk_envelope_convention": (
                "initial_risk + expected_cost_fixed + filled_quantity * "
                "(expected_cost_per_unit + expected_exit_slippage_per_unit)"
            ),
        }
        unknown = set(value) - allowed - {"policy_hash"} - set(conventions)
        if unknown:
            raise RiskExitError(f"claves RiskExit desconocidas: {sorted(unknown)}")
        for key, expected in conventions.items():
            if key in value and value[key] != expected:
                raise RiskExitError(f"{key} no coincide con la convención RiskExit")
        policy = cls(**{key: item for key, item in value.items() if key != "policy_hash" and key not in conventions})
        if supplied_hash is not None and str(supplied_hash) != policy.policy_hash:
            raise RiskExitError("policy_hash RiskExit inválido")
        return policy

    @property
    def policy_hash(self) -> str:
        return hashlib.sha256(canonical_json(self.to_dict()).encode("utf-8")).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "stop_atr_multiple": _text(self.stop_atr_multiple),
            "take_profit_atr_multiple": _text(self.take_profit_atr_multiple),
            "planned_risk_fraction": _text(self.planned_risk_fraction),
            "max_daily_loss_fraction": _text(self.max_daily_loss_fraction),
            "max_drawdown_fraction": _text(self.max_drawdown_fraction),
            "max_positions": self.max_positions,
            "max_intents": self.max_intents,
            "martingale_allowed": self.martingale_allowed,
            "holding_profile": self.holding_profile,
            "intraday_max_bars": self.intraday_max_bars,
            "intraday_max_minutes": _text(self.intraday_max_minutes),
            "multiday_max_hours": _text(self.multiday_max_hours),
            "multiday_preclose_minutes": _text(self.multiday_preclose_minutes),
            "exit_latency_seconds": _text(self.exit_latency_seconds),
            "initial_equity": _text(self.initial_equity),
            "equity_basis": self.equity_basis,
            "account_currency": self.account_currency,
            "quote_currency": self.quote_currency,
            "initial_risk_convention": "filled_quantity * stop_distance * unit_value",
            "economic_r_convention": "settled_net_pnl / initial_risk",
            "risk_budget_convention": "account_equity * planned_risk_fraction; not initial_risk",
            "risk_envelope_convention": (
                "initial_risk + expected_cost_fixed + filled_quantity * "
                "(expected_cost_per_unit + expected_exit_slippage_per_unit)"
            ),
        }

    def serialize(self) -> dict[str, Any]:
        data = self.to_dict()
        data["policy_hash"] = self.policy_hash
        return data


def _validate_policy_identity(policy: RiskExitPolicy) -> None:
    if str(policy.version).strip() != POLICY_VERSION:
        raise RiskExitError(f"versión RiskExit no soportada: {policy.version!r}")
    if not isinstance(policy.martingale_allowed, bool) or policy.martingale_allowed:
        raise RiskExitError("la política RiskExit no admite martingala")
    if str(policy.equity_basis).strip().upper() not in {"VIRTUAL_PAPER_ONLY", "OBSERVED_DEMO"}:
        raise RiskExitError("equity_basis debe ser VIRTUAL_PAPER_ONLY u OBSERVED_DEMO")


def _normalise_policy_decimals(policy: RiskExitPolicy) -> None:
    names = (
        "stop_atr_multiple",
        "take_profit_atr_multiple",
        "planned_risk_fraction",
        "max_daily_loss_fraction",
        "max_drawdown_fraction",
        "intraday_max_minutes",
        "multiday_max_hours",
        "multiday_preclose_minutes",
        "exit_latency_seconds",
        "initial_equity",
    )
    for name in names:
        minimum = D1 if name in {"stop_atr_multiple", "take_profit_atr_multiple", "initial_equity"} else D0
        object.__setattr__(
            policy,
            name,
            _decimal(getattr(policy, name), name=name, minimum=minimum, positive=name == "initial_equity"),
        )


def _normalise_policy_integers(policy: RiskExitPolicy) -> None:
    object.__setattr__(policy, "holding_profile", _profile(policy.holding_profile))
    for name in ("max_positions", "max_intents", "intraday_max_bars"):
        object.__setattr__(policy, name, _integer(getattr(policy, name), name=name, minimum=1))


def _validate_policy_ranges(policy: RiskExitPolicy) -> None:
    if not D0 < policy.planned_risk_fraction < D1:
        raise RiskExitError("planned_risk_fraction debe estar entre 0 y 1")
    for name in ("max_daily_loss_fraction", "max_drawdown_fraction"):
        value = getattr(policy, name)
        if not D0 < value < D1:
            raise RiskExitError(f"{name} debe estar entre 0 y 1")


def _normalise_policy_metadata(policy: RiskExitPolicy) -> None:
    account = str(policy.account_currency).strip().upper()
    quote = str(policy.quote_currency).strip().upper()
    if not account or not quote:
        raise RiskExitError("account_currency/quote_currency son obligatorias")
    object.__setattr__(policy, "account_currency", account)
    object.__setattr__(policy, "quote_currency", quote)
    object.__setattr__(policy, "equity_basis", str(policy.equity_basis).strip().upper())


@dataclasses.dataclass(frozen=True, slots=True)
class EntryPlan:
    """A planned, not yet executed, entry with immutable initial risk."""

    allowed: bool
    direction: str
    entry_price: Decimal | None
    quantity: Decimal | None
    initial_stop: Decimal | None
    take_profit: Decimal | None
    risk_amount: Decimal | None
    risk_per_unit: Decimal | None
    atr: Decimal | None
    planned_at: datetime | None
    holding_profile: str
    policy_hash: str
    reasons: tuple[str, ...] = ()
    assumptions: Mapping[str, Any] = dataclasses.field(default_factory=dict)
    eligible_for_demo: bool = False
    mode: str = "DEMO_GATED"
    risk_budget: Decimal | None = None
    expected_total_loss_at_stop: Decimal | None = None
    expected_total_loss_at_stop_basis: str | None = None
    expected_cost_fixed: Decimal | None = None
    expected_cost_per_unit: Decimal | None = None
    expected_exit_slippage_per_unit: Decimal | None = None
    expected_cost_currency: str | None = None
    expected_cost_source: str | None = None
    risk_envelope_known: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.allowed, bool) or not isinstance(self.eligible_for_demo, bool):
            raise RiskExitError("allowed/eligible_for_demo deben ser booleanos")
        if not isinstance(self.risk_envelope_known, bool):
            raise RiskExitError("risk_envelope_known debe ser booleano")
        object.__setattr__(self, "direction", _direction(self.direction))
        object.__setattr__(self, "holding_profile", _profile(self.holding_profile))
        mode = str(self.mode).strip().upper().replace("-", "_")
        if mode not in {"DEMO_GATED", "VIRTUAL_DIAGNOSTIC"}:
            raise RiskExitError(f"mode EntryPlan no soportado: {mode!r}")
        if mode == "VIRTUAL_DIAGNOSTIC" and self.eligible_for_demo:
            raise RiskExitError("VIRTUAL_DIAGNOSTIC nunca es elegible para DEMO")
        object.__setattr__(self, "mode", mode)
        object.__setattr__(self, "reasons", tuple(str(item) for item in self.reasons))
        object.__setattr__(self, "assumptions", MappingProxyType(dict(self.assumptions or {})))
        for name in (
            "entry_price",
            "quantity",
            "initial_stop",
            "take_profit",
            "risk_amount",
            "risk_per_unit",
            "atr",
            "risk_budget",
        ):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _decimal(value, name=name))
        if self.planned_at is not None:
            object.__setattr__(self, "planned_at", _utc(self.planned_at, name="planned_at"))
        for name, value in _normalise_entry_plan_costs(self).items():
            object.__setattr__(self, name, value)

    @property
    def initial_risk(self) -> Decimal | None:
        return self.risk_amount

    @property
    def equity_source(self) -> str | None:
        value = self.assumptions.get("equity_source")
        return str(value) if value is not None else None

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> EntryPlan:
        if not isinstance(value, Mapping):
            raise RiskExitError("EntryPlan debe ser mapping")
        when = value.get("planned_at")
        mode = str(value.get("mode", "DEMO_GATED")).strip().upper().replace("-", "_")
        if mode not in {"DEMO_GATED", "VIRTUAL_DIAGNOSTIC"}:
            raise RiskExitError(f"mode EntryPlan no soportado: {mode!r}")
        return cls(
            _strict_bool(value.get("allowed", False), name="allowed"),
            _direction(value.get("direction", "")),
            _optional_decimal(value.get("entry_price"), "entry_price"),
            _optional_decimal(value.get("quantity"), "quantity"),
            _optional_decimal(value.get("initial_stop"), "initial_stop"),
            _optional_decimal(value.get("take_profit"), "take_profit"),
            _optional_decimal(value.get("risk_amount"), "risk_amount"),
            _optional_decimal(value.get("risk_per_unit"), "risk_per_unit"),
            _optional_decimal(value.get("atr"), "atr"),
            _utc(when, name="planned_at") if when is not None else None,
            _profile(value.get("holding_profile")),
            str(value.get("policy_hash", "")),
            tuple(str(item) for item in value.get("reasons", ())),
            dict(value.get("assumptions", {})) if isinstance(value.get("assumptions", {}), Mapping) else {},
            _strict_bool(value.get("eligible_for_demo", False), name="eligible_for_demo"),
            mode,
            _optional_decimal(value.get("risk_budget"), "risk_budget"),
            _optional_decimal(value.get("expected_total_loss_at_stop"), "expected_total_loss_at_stop"),
            str(value["expected_total_loss_at_stop_basis"])
            if value.get("expected_total_loss_at_stop_basis") is not None
            else None,
            _optional_decimal(value.get("expected_cost_fixed"), "expected_cost_fixed"),
            _optional_decimal(value.get("expected_cost_per_unit"), "expected_cost_per_unit"),
            _optional_decimal(value.get("expected_exit_slippage_per_unit"), "expected_exit_slippage_per_unit"),
            str(value["expected_cost_currency"]) if value.get("expected_cost_currency") is not None else None,
            str(value["expected_cost_source"]) if value.get("expected_cost_source") is not None else None,
            _strict_bool(value.get("risk_envelope_known", False), name="risk_envelope_known"),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "allowed": self.allowed,
            "direction": self.direction,
            "entry_price": _text(self.entry_price),
            "quantity": _text(self.quantity),
            "initial_stop": _text(self.initial_stop),
            "take_profit": _text(self.take_profit),
            "risk_amount": _text(self.risk_amount),
            "risk_per_unit": _text(self.risk_per_unit),
            "atr": _text(self.atr),
            "planned_at": _iso(self.planned_at),
            "holding_profile": self.holding_profile,
            "policy_hash": self.policy_hash,
            "reasons": list(self.reasons),
            "assumptions": _jsonable(self.assumptions),
            "eligible_for_demo": self.eligible_for_demo,
            "mode": self.mode,
            "risk_budget": _text(self.risk_budget),
            "expected_total_loss_at_stop": _text(self.expected_total_loss_at_stop),
            "expected_total_loss_at_stop_basis": self.expected_total_loss_at_stop_basis,
            "expected_cost_fixed": _text(self.expected_cost_fixed),
            "expected_cost_per_unit": _text(self.expected_cost_per_unit),
            "expected_exit_slippage_per_unit": _text(self.expected_exit_slippage_per_unit),
            "expected_cost_currency": self.expected_cost_currency,
            "expected_cost_source": self.expected_cost_source,
            "risk_envelope_known": self.risk_envelope_known,
        }


def _normalise_entry_plan_costs(plan: EntryPlan) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for name in (
        "expected_total_loss_at_stop",
        "expected_cost_fixed",
        "expected_cost_per_unit",
        "expected_exit_slippage_per_unit",
    ):
        value = getattr(plan, name)
        values[name] = _decimal(value, name=name, minimum=D0) if value is not None else None
    for name in ("expected_cost_currency", "expected_cost_source", "expected_total_loss_at_stop_basis"):
        value = getattr(plan, name)
        values[name] = str(value).strip() or None if value is not None else None
    return values


@dataclasses.dataclass(frozen=True, slots=True)
class ExitDecision:
    """One tick-driven decision with price excursions, not economic PnL.

    ``r_multiple`` is retained as a compatibility alias for the maximum
    favorable excursion in R (``mfe_r``).  It is deliberately not a return
    metric: only a consumer with a settled ledger can calculate net R.
    """

    action: str
    reason: str
    executable_price: Decimal | None
    planned_trigger_price: Decimal | None
    observed_at: datetime | None
    latency_seconds: Decimal
    server_side: bool
    gap: bool
    bars_held: int
    holding_seconds: Decimal | None
    mfe_price: Decimal | None
    mae_price: Decimal | None
    # Compatibility field name.  Its meaning is MFE R, never net/economic R.
    r_multiple: Decimal | None
    mae_r: Decimal | None
    policy_hash: str
    triggered_at: datetime | None = None
    requested_latency_seconds: Decimal | None = None
    filled_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.observed_at is not None:
            object.__setattr__(self, "observed_at", _utc(self.observed_at, name="observed_at"))
        for name in ("triggered_at", "filled_at"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _utc(value, name=name))
        object.__setattr__(self, "latency_seconds", _decimal(self.latency_seconds, name="latency_seconds", minimum=D0))
        for name in ("mfe_price", "mae_price", "r_multiple", "mae_r"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _decimal(value, name=name))
        if self.requested_latency_seconds is not None:
            object.__setattr__(
                self,
                "requested_latency_seconds",
                _decimal(self.requested_latency_seconds, name="requested_latency_seconds", minimum=D0),
            )
        object.__setattr__(self, "bars_held", _integer(self.bars_held, name="bars_held", minimum=0))

    @property
    def exit_price(self) -> Decimal | None:
        return self.executable_price

    @property
    def mfe_r(self) -> Decimal | None:
        """Maximum favorable excursion in initial-stop R."""

        return self.r_multiple

    def to_dict(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "reason": self.reason,
            "executable_price": _text(self.executable_price),
            "exit_price": _text(self.executable_price),
            "planned_trigger_price": _text(self.planned_trigger_price),
            "observed_at": _iso(self.observed_at),
            "latency_seconds": _text(self.latency_seconds),
            "server_side": self.server_side,
            "gap": self.gap,
            "bars_held": self.bars_held,
            "holding_seconds": _text(self.holding_seconds),
            "mfe_price": _text(self.mfe_price),
            "mae_price": _text(self.mae_price),
            "mfe_r": _text(self.mfe_r),
            "mae_r": _text(self.mae_r),
            # Legacy report readers may still ask for r_multiple.  At this
            # layer it is explicitly the MFE-R compatibility alias only.
            "r_multiple": _text(self.mfe_r),
            "policy_hash": self.policy_hash,
            "triggered_at": _iso(self.triggered_at),
            "decision_at": _iso(self.triggered_at),
            "requested_latency_seconds": _text(self.requested_latency_seconds),
            "filled_at": _iso(self.filled_at),
        }


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(item) for item in value]
    if isinstance(value, Decimal):
        return _text(value)
    if isinstance(value, datetime):
        return _iso(value)
    return value


def _direction(value: Any) -> str:
    raw = str(getattr(value, "value", value)).strip().upper()
    if raw in {"LONG", "BUY", "UP"}:
        return "LONG"
    if raw in {"SHORT", "SELL", "DOWN"}:
        return "SHORT"
    raise RiskExitError(f"dirección no soportada: {value!r}")


def _known(mapping: Mapping[str, Any] | None, key: str = "known") -> bool:
    return isinstance(mapping, Mapping) and mapping.get(key) is True


def _spec_values(spec: Mapping[str, Any] | None, *, diagnostic: bool = False) -> tuple[dict[str, Decimal], list[str]]:
    if not isinstance(spec, Mapping):
        return {}, ["CONTRACT_SPEC_UNKNOWN"]
    reasons: list[str] = []
    if spec.get("known") is not True:
        reasons.append("CONTRACT_SPEC_UNKNOWN")
        if not diagnostic:
            return {}, reasons
    values, field_reasons = _spec_required_values(spec)
    reasons.extend(field_reasons)
    optional, optional_reasons = _spec_optional_values(spec)
    values.update(optional)
    reasons.extend(optional_reasons)
    if spec.get("fees_known") is not True:
        reasons.append("FEES_UNKNOWN")
    if spec.get("spread_known") is not True:
        reasons.append("SPREAD_UNKNOWN")
    return values, reasons


def _spec_required_values(spec: Mapping[str, Any]) -> tuple[dict[str, Decimal], list[str]]:
    values: dict[str, Decimal] = {}
    reasons: list[str] = []
    required = {
        "pip_size": "pip_size",
        "quantity_min": "quantity_min",
        "quantity_step": "quantity_step",
        "unit_value": "unit_value",
        "quantity_max": "quantity_max",
    }
    for key, output in required.items():
        try:
            values[output] = _decimal(spec.get(key), name=key, positive=True)
        except RiskExitError:
            reasons.append(f"CONTRACT_{key.upper()}_UNKNOWN")
    return values, reasons


def _spec_optional_values(spec: Mapping[str, Any]) -> tuple[dict[str, Decimal], list[str]]:
    values: dict[str, Decimal] = {}
    reasons: list[str] = []
    minimum_stop = spec.get("minimum_stop_distance", spec.get("min_stop_distance"))
    try:
        values["minimum_stop_distance"] = _decimal(minimum_stop, name="minimum_stop_distance", positive=True)
    except RiskExitError:
        reasons.append("CONTRACT_MINIMUM_STOP_DISTANCE_UNKNOWN")
    margin_per_unit = spec.get("margin_per_unit")
    if margin_per_unit is not None:
        try:
            values["margin_per_unit"] = _decimal(margin_per_unit, name="margin_per_unit", positive=True)
        except RiskExitError:
            reasons.append("CONTRACT_MARGIN_PER_UNIT_UNKNOWN")
    return values, reasons


def _read_cost_decimal(
    spec: Mapping[str, Any], keys: tuple[str, ...], name: str
) -> tuple[Decimal | None, str | None, str | None]:
    key = next((item for item in keys if item in spec), None)
    if key is None:
        return None, None, f"{name}_UNKNOWN"
    try:
        return _decimal(spec[key], name=key, minimum=D0), key, None
    except RiskExitError:
        return None, key, f"{name}_INVALID"


def _read_exit_slippage(spec: Mapping[str, Any]) -> tuple[Decimal | None, str | None, list[str]]:
    direct, direct_key, direct_reason = _read_cost_decimal(
        spec,
        (
            "expected_exit_slippage_per_unit",
            "expected_slippage_per_unit",
            "expected_exit_slippage",
            "expected_exit_slippage_price",
            "expected_slippage",
            "exit_slippage_per_unit",
            "exit_slippage",
            "slippage_per_unit",
        ),
        "EXIT_SLIPPAGE_ESTIMATE",
    )
    pips, pips_key, pips_reason = _read_cost_decimal(
        spec,
        ("expected_exit_slippage_pips", "expected_slippage_pips", "exit_slippage_pips", "slippage_pips"),
        "EXIT_SLIPPAGE_ESTIMATE",
    )
    reasons: list[str] = []
    if direct_reason is not None and direct_key is not None:
        reasons.append(direct_reason)
    if pips_reason is not None and pips_key is not None:
        reasons.append(pips_reason)
    if direct_key is None and pips_key is None:
        reasons.append("EXIT_SLIPPAGE_ESTIMATE_UNKNOWN")
    if pips is not None:
        try:
            pip_size = _decimal(spec.get("pip_size"), name="pip_size", positive=True)
            unit_value = _decimal(spec.get("unit_value"), name="unit_value", positive=True)
        except RiskExitError:
            reasons.append("EXIT_SLIPPAGE_ESTIMATE_UNKNOWN")
        else:
            with decimal_context():
                derived = pips * pip_size * unit_value
            if direct is not None and direct != derived:
                reasons.append("COST_ESTIMATE_INCONSISTENT")
            if direct is None:
                direct = derived
    if direct is None:
        reasons.append("EXIT_SLIPPAGE_ESTIMATE_UNKNOWN")
    return direct, direct_key or pips_key, reasons


def _spec_cost_components(spec: Mapping[str, Any]) -> tuple[dict[str, Any], list[str], tuple[str, ...]]:
    fixed, fixed_key, fixed_reason = _read_cost_decimal(
        spec,
        (
            "expected_cost_fixed",
            "expected_fixed_cost",
            "expected_commission_fixed",
            "expected_fee_fixed",
            "commission_fixed",
            "cost_fixed",
            "fixed_cost",
            "fee_fixed",
        ),
        "COST_ESTIMATE_FIXED",
    )
    variable, variable_key, variable_reason = _read_cost_decimal(
        spec,
        (
            "expected_cost_per_unit",
            "expected_variable_cost_per_unit",
            "expected_commission_per_unit",
            "expected_fee_per_unit",
            "commission_per_unit",
            "cost_per_unit",
            "variable_cost_per_unit",
            "fee_per_unit",
        ),
        "COST_ESTIMATE_PER_UNIT",
    )
    exit_slip, exit_slip_key, slip_reasons = _read_exit_slippage(spec)
    reasons = [item for item in (fixed_reason, variable_reason) if item is not None]
    reasons.extend(slip_reasons)
    source_keys = tuple(item for item in (fixed_key, variable_key, exit_slip_key) if item is not None)
    return (
        {
            "expected_cost_fixed": fixed,
            "expected_cost_per_unit": variable,
            "expected_exit_slippage_per_unit": exit_slip,
        },
        reasons,
        source_keys,
    )


def _spec_cost_values(spec: Mapping[str, Any] | None, policy: RiskExitPolicy) -> tuple[dict[str, Any], list[str]]:
    """Read explicit stop-loss cost components; absence is never zero."""

    if not isinstance(spec, Mapping):
        return {}, ["COST_ESTIMATE_UNKNOWN", "RISK_ENVELOPE_UNKNOWN"]
    values, reasons, source_keys = _spec_cost_components(spec)
    cost_currency = (
        str(spec.get("expected_cost_currency", spec.get("cost_currency", policy.account_currency))).strip().upper()
    )
    if not cost_currency or cost_currency != policy.account_currency:
        reasons.append("COST_ESTIMATE_CURRENCY_UNKNOWN")
    if spec.get("expected_costs_known", spec.get("risk_envelope_known", True)) is not True:
        reasons.append("COST_ESTIMATE_UNKNOWN")
    if spec.get("fees_known") is not True:
        reasons.append("FEES_UNKNOWN")
    if reasons:
        reasons.append("RISK_ENVELOPE_UNKNOWN")
    reasons = list(dict.fromkeys(reasons))
    source = str(spec.get("expected_cost_source", spec.get("cost_estimate_source", ""))).strip()
    return {
        **values,
        "expected_cost_currency": cost_currency or None,
        "expected_cost_source": source or ("contract_spec:" + ",".join(source_keys or ("missing",))),
        "risk_envelope_known": not reasons,
    }, reasons


def _calendar_reason(calendar_state: Mapping[str, Any] | None, *, allow_modelled: bool = False) -> str | None:
    if not isinstance(calendar_state, Mapping) or calendar_state.get("known") is not True:
        return "CALENDAR_UNKNOWN"
    basis = str(calendar_state.get("basis", calendar_state.get("calendar_basis", ""))).strip().upper()
    if (basis.startswith("MODELED") or basis.startswith("MODEL")) and not allow_modelled:
        # An account flag cannot upgrade a modelled historical/public-hours
        # calendar into an observed venue/account calendar.
        return "CALENDAR_UNVERIFIED"
    if calendar_state.get("financing_known") is False:
        return "FINANCING_UNKNOWN"
    return None


def _risk_reasons(
    policy: RiskExitPolicy,
    *,
    equity: Decimal,
    risk_state: Mapping[str, Any] | None,
    calendar_state: Mapping[str, Any] | None,
    equity_source: str,
) -> list[str]:
    result = [calendar_reason] if (calendar_reason := _calendar_reason(calendar_state)) else []
    if not isinstance(risk_state, Mapping):
        return [*result, "RISK_STATE_UNKNOWN"]
    state_source = _equity_source(risk_state.get("equity_source"))
    requested_source = _equity_source(equity_source)
    if not state_source:
        result.append("EQUITY_STATE_PROVENANCE_UNKNOWN")
    elif not requested_source or state_source != requested_source:
        result.append("EQUITY_STATE_SOURCE_MISMATCH")
    state_equity = risk_state.get("equity")
    if state_equity is not None:
        try:
            if _decimal(state_equity, name="risk_state.equity") != equity:
                result.append("EQUITY_STATE_MISMATCH")
        except RiskExitError:
            result.append("EQUITY_STATE_INVALID")
    if risk_state.get("bar_clock_known") is not True:
        result.append("RISK_BAR_CLOCK_UNKNOWN")
    result.extend(_risk_budget_reasons(policy, equity, risk_state))
    result.extend(_risk_capacity_reasons(policy, risk_state))
    if risk_state.get("costs_known") is not True:
        result.append("COSTS_UNKNOWN")
    return result


def _risk_budget_reasons(policy: RiskExitPolicy, equity: Decimal, risk_state: Mapping[str, Any]) -> list[str]:
    result: list[str] = []
    daily = risk_state.get("daily_pnl")
    drawdown = risk_state.get("drawdown")
    daily_anchor = risk_state.get("daily_anchor_equity")
    high_water = risk_state.get("high_water_equity")
    try:
        daily_value = _decimal(daily, name="daily_pnl") if daily is not None else None
        drawdown_value = _decimal(drawdown, name="drawdown", minimum=D0) if drawdown is not None else None
        daily_anchor_value = (
            _decimal(daily_anchor, name="daily_anchor_equity", positive=True) if daily_anchor is not None else None
        )
        high_water_value = (
            _decimal(high_water, name="high_water_equity", positive=True) if high_water is not None else None
        )
    except RiskExitError:
        return ["RISK_STATE_INVALID"]
    if daily_value is None:
        result.append("DAILY_PNL_UNKNOWN")
    elif daily_anchor_value is None:
        result.append("DAILY_ANCHOR_UNKNOWN")
    else:
        with decimal_context():
            daily_breached = daily_value <= -(daily_anchor_value * policy.max_daily_loss_fraction)
        if daily_breached:
            result.append("MAX_DAILY_LOSS")
    if high_water_value is None:
        result.append("HIGH_WATER_UNKNOWN")
    elif high_water_value < equity:
        result.append("HIGH_WATER_INVALID")
    else:
        with decimal_context():
            derived_drawdown = max(D0, high_water_value - equity)
        if drawdown_value is not None and drawdown_value != derived_drawdown:
            result.append("DRAWDOWN_INCONSISTENT")
        effective_drawdown = max(derived_drawdown, drawdown_value or D0)
        with decimal_context():
            drawdown_breached = effective_drawdown >= high_water_value * policy.max_drawdown_fraction
        if drawdown_breached:
            result.append("MAX_DRAWDOWN")
    return result


def _risk_capacity_reasons(policy: RiskExitPolicy, risk_state: Mapping[str, Any]) -> list[str]:
    result: list[str] = []
    for key, limit, reason in (
        ("positions", policy.max_positions, "MAX_POSITIONS"),
        ("intents", policy.max_intents, "MAX_INTENTS"),
    ):
        raw = risk_state.get(key)
        if raw is None:
            result.append(f"{key.upper()}_UNKNOWN")
        else:
            try:
                if _integer(raw, name=key) >= limit:
                    result.append(reason)
            except RiskExitError:
                result.append(f"{key.upper()}_INVALID")
    return result


def _floor_grid(value: Decimal, minimum: Decimal, step: Decimal) -> Decimal:
    with decimal_context():
        steps = (value / step).to_integral_value(rounding="ROUND_FLOOR")
        return steps * step if steps > 0 else D0


def _blocked_plan(
    policy: RiskExitPolicy,
    direction: str,
    when: datetime | None,
    reasons: list[str],
    *,
    entry_price: Decimal | None = None,
    atr: Decimal | None = None,
    risk_amount: Decimal | None = None,
    risk_per_unit: Decimal | None = None,
    assumptions: Mapping[str, Any] | None = None,
    quantity: Decimal | None = None,
    initial_stop: Decimal | None = None,
    take_profit: Decimal | None = None,
    eligible_for_demo: bool = False,
    mode: str = "DEMO_GATED",
    risk_budget: Decimal | None = None,
    expected_total_loss_at_stop: Decimal | None = None,
    expected_total_loss_at_stop_basis: str | None = None,
    expected_cost_fixed: Decimal | None = None,
    expected_cost_per_unit: Decimal | None = None,
    expected_exit_slippage_per_unit: Decimal | None = None,
    expected_cost_currency: str | None = None,
    expected_cost_source: str | None = None,
    risk_envelope_known: bool = False,
) -> EntryPlan:
    return EntryPlan(
        False,
        direction,
        entry_price,
        quantity,
        initial_stop,
        take_profit,
        risk_amount,
        risk_per_unit,
        atr,
        when,
        policy.holding_profile,
        policy.policy_hash,
        tuple(dict.fromkeys(reasons)),
        dict(assumptions or {}),
        eligible_for_demo,
        mode,
        risk_budget,
        expected_total_loss_at_stop,
        expected_total_loss_at_stop_basis,
        expected_cost_fixed,
        expected_cost_per_unit,
        expected_exit_slippage_per_unit,
        expected_cost_currency,
        expected_cost_source,
        risk_envelope_known,
    )


def _plan_mode(mode: Any) -> tuple[str, bool]:
    selected = str(mode).strip().upper().replace("-", "_")
    if selected in {"DIAGNOSTIC", "VIRTUAL_DIAGNOSTICS"}:
        selected = "VIRTUAL_DIAGNOSTIC"
    if selected not in {"DEMO_GATED", "VIRTUAL_DIAGNOSTIC"}:
        raise RiskExitError(f"mode RiskExit no soportado: {mode!r}")
    return selected, selected == "VIRTUAL_DIAGNOSTIC"


def _plan_provenance(policy: RiskExitPolicy, source: str) -> list[str]:
    reasons: list[str] = []
    canonical = _equity_source(source)
    if policy.equity_basis == "OBSERVED_DEMO":
        if canonical != "OBSERVED_DEMO":
            reasons.append("EQUITY_PROVENANCE_UNKNOWN")
    elif canonical != "VIRTUAL_PAPER_ONLY":
        if canonical == "OBSERVED_DEMO":
            reasons.append("EQUITY_BASIS_MISMATCH")
        else:
            reasons.append("EQUITY_PROVENANCE_UNKNOWN")
    return reasons


def _calculate_entry_values(
    policy: RiskExitPolicy,
    side: str,
    price: Decimal,
    atr: Decimal,
    equity: Decimal,
    spec: Mapping[str, Decimal],
    requested_quantity: Any | None,
    cost_values: Mapping[str, Any],
) -> tuple[
    Decimal,
    Decimal,
    Decimal | None,
    Decimal | None,
    Decimal | None,
    Decimal | None,
    Decimal,
    Decimal,
    list[str],
    dict[str, Any],
]:
    with decimal_context():
        stop_distance = atr * policy.stop_atr_multiple
        risk_amount = equity * policy.planned_risk_fraction
        risk_per_unit = stop_distance * spec["unit_value"] if "unit_value" in spec else None
        cost_fixed = cost_values.get("expected_cost_fixed")
        cost_per_unit = cost_values.get("expected_cost_per_unit")
        exit_slippage = cost_values.get("expected_exit_slippage_per_unit")
        costs_known = bool(cost_values.get("risk_envelope_known") is True) and all(
            isinstance(value, Decimal) for value in (cost_fixed, cost_per_unit, exit_slippage)
        )
        variable_cost = (
            cost_per_unit + exit_slippage
            if isinstance(cost_per_unit, Decimal) and isinstance(exit_slippage, Decimal)
            else None
        )
        effective_per_unit = (
            risk_per_unit + variable_cost if risk_per_unit is not None and variable_cost is not None else None
        )
        available_for_quantity = risk_amount - cost_fixed if costs_known and cost_fixed is not None else risk_amount
        raw_quantity = (
            max(D0, available_for_quantity) / effective_per_unit
            if effective_per_unit is not None and available_for_quantity > D0
            else risk_amount / risk_per_unit
            if risk_per_unit is not None and not costs_known
            else D0
            if costs_known
            else None
        )
        if requested_quantity is not None and raw_quantity is not None:
            requested = _decimal(requested_quantity, name="requested_quantity", positive=True)
            raw_quantity = min(raw_quantity, requested)
        quantity = (
            _floor_grid(raw_quantity, spec["quantity_min"], spec["quantity_step"])
            if raw_quantity is not None and "quantity_min" in spec and "quantity_step" in spec
            else None
        )
        reasons: list[str] = []
        if quantity is not None and "quantity_max" in spec:
            maximum = _floor_grid(spec["quantity_max"], spec["quantity_min"], spec["quantity_step"])
            if maximum < spec["quantity_min"]:
                reasons.append("CONTRACT_QUANTITY_MAX_INVALID")
            else:
                quantity = min(quantity, maximum)
        if quantity is not None and quantity < spec["quantity_min"]:
            reasons.extend(["QUANTITY_BELOW_MIN", "RISK_BUDGET_WOULD_BE_EXCEEDED"])
        if risk_per_unit is None or quantity is None:
            reasons.append("RISK_SIZING_UNKNOWN")
        actual_risk = quantity * risk_per_unit if quantity is not None and risk_per_unit is not None else None
        stop = price - stop_distance if side == "LONG" else price + stop_distance
        target = (
            price + atr * policy.take_profit_atr_multiple
            if side == "LONG"
            else price - atr * policy.take_profit_atr_multiple
        )
        if "minimum_stop_distance" in spec and stop_distance < spec["minimum_stop_distance"]:
            reasons.append("STOP_DISTANCE_BELOW_MINIMUM")
        expected_cost = (
            cost_fixed + quantity * variable_cost
            if costs_known and cost_fixed is not None and variable_cost is not None and quantity is not None
            else None
        )
        expected_total = actual_risk + expected_cost if actual_risk is not None and expected_cost is not None else None
    components = {
        "expected_total_loss_at_stop": expected_total,
        "expected_cost_fixed": cost_fixed if costs_known else None,
        "expected_cost_per_unit": cost_per_unit if costs_known else None,
        "expected_exit_slippage_per_unit": exit_slippage if costs_known else None,
        "expected_cost_variable_per_unit": variable_cost if costs_known else None,
        "expected_cost_currency": cost_values.get("expected_cost_currency"),
        "expected_cost_source": cost_values.get("expected_cost_source"),
        "risk_envelope_known": bool(cost_values.get("risk_envelope_known") is True and risk_per_unit is not None),
        "expected_total_loss_at_stop_basis": (
            "initial_risk + fixed_cost + filled_quantity * (commission_per_unit + exit_slippage_per_unit)"
        ),
    }
    return (
        stop_distance,
        risk_amount,
        risk_per_unit,
        raw_quantity,
        quantity,
        actual_risk,
        stop,
        target,
        reasons,
        components,
    )


def _stop_side_value(contract: Mapping[str, Any], explicit: Any | None, keys: tuple[str, ...]) -> Any | None:
    if explicit is not None:
        return explicit
    return next((contract[key] for key in keys if key in contract), None)


def _stop_minimum(contract: Mapping[str, Any]) -> Decimal | None:
    raw = contract.get("minimum_stop_distance", contract.get("min_stop_distance"))
    if raw is None:
        return None
    try:
        return _decimal(raw, name="minimum_stop_distance", positive=True)
    except RiskExitError:
        return None


def _stop_executable_check(
    side: str, stop: Decimal, raw: Any | None, minimum: Decimal | None
) -> tuple[str | None, Decimal | None, str]:
    if side == "LONG":
        basis = "executable_bid - initial_stop (LONG)"
        name = "executable_bid"
    else:
        basis = "initial_stop - executable_ask (SHORT)"
        name = "executable_ask"
    if raw is None:
        return "STOP_EXECUTABLE_SIDE_UNKNOWN", None, basis
    try:
        executable = _decimal(raw, name=name, positive=True)
    except RiskExitError:
        return "STOP_EXECUTABLE_SIDE_UNKNOWN", None, basis
    with decimal_context():
        distance = executable - stop if side == "LONG" else stop - executable
    if distance <= D0:
        return "STOP_EXECUTABLE_SIDE_INVALID", distance, basis
    if minimum is not None and distance < minimum:
        return "STOP_DISTANCE_BELOW_MINIMUM_EXECUTABLE", distance, basis
    return None, distance, basis


def _stop_executable_side_metrics(
    side: str,
    stop: Decimal,
    contract_spec: Mapping[str, Any] | None,
    executable_bid: Any | None,
    executable_ask: Any | None,
) -> tuple[str | None, Decimal | None, str]:
    contract = contract_spec if isinstance(contract_spec, Mapping) else {}
    minimum = _stop_minimum(contract)
    if side == "LONG":
        raw = _stop_side_value(contract, executable_bid, ("executable_bid", "entry_bid", "bid"))
        return _stop_executable_check(side, stop, raw, minimum)
    if side == "SHORT":
        raw = _stop_side_value(contract, executable_ask, ("executable_ask", "entry_ask", "ask"))
        return _stop_executable_check(side, stop, raw, minimum)
    raise RiskExitError(f"dirección no soportada: {side!r}")


def _margin_reasons(
    spec: Mapping[str, Decimal], risk_state: Mapping[str, Any] | None, quantity: Decimal | None
) -> list[str]:
    if not isinstance(risk_state, Mapping):
        return ["MARGIN_STATE_UNKNOWN"]
    available_raw = risk_state.get("margin_available")
    required_raw = risk_state.get("margin_required")
    try:
        available = _decimal(available_raw, name="margin_available", minimum=D0) if available_raw is not None else None
        required = _decimal(required_raw, name="margin_required", minimum=D0) if required_raw is not None else None
        if required is None and quantity is not None and spec.get("margin_per_unit") is not None:
            with decimal_context():
                required = quantity * spec["margin_per_unit"]
    except RiskExitError:
        return ["MARGIN_STATE_INVALID"]
    reasons: list[str] = []
    if available is None:
        reasons.append("MARGIN_AVAILABLE_UNKNOWN")
    if required is None:
        reasons.append("MARGIN_REQUIRED_UNKNOWN")
    if available is not None and required is not None:
        with decimal_context():
            margin_short = available < required
        if margin_short:
            reasons.append("MARGIN_INSUFFICIENT")
    return reasons


def plan_entry(
    policy: RiskExitPolicy,
    *,
    direction: Any,
    entry_price: Any,
    atr: Any,
    equity: Any,
    available_at: Any,
    contract_spec: Mapping[str, Any] | None,
    calendar_state: Mapping[str, Any] | None,
    risk_state: Mapping[str, Any] | None = None,
    requested_quantity: Any | None = None,
    equity_source: str | None = None,
    mode: str = "DEMO_GATED",
    executable_bid: Any | None = None,
    executable_ask: Any | None = None,
) -> EntryPlan:
    """Plan a sized entry; diagnostics never become an executable approval."""

    side = _direction(direction)
    when = _utc(available_at, name="available_at")
    selected_mode, diagnostic = _plan_mode(mode)
    try:
        price = _decimal(entry_price, name="entry_price", positive=True)
        atr_value = _decimal(atr, name="atr", positive=True)
        equity_value = _decimal(equity, name="equity", positive=True)
    except RiskExitError as exc:
        return _blocked_plan(policy, side, when, [str(exc)], mode=selected_mode)
    source = str(equity_source or "").strip().upper()
    provenance_reasons = _plan_provenance(policy, source)
    spec, reasons = _spec_values(contract_spec, diagnostic=diagnostic)
    cost_values, cost_reasons = _spec_cost_values(contract_spec, policy)
    reasons = [*provenance_reasons, *reasons]
    reasons.extend(cost_reasons)
    reasons.extend(
        _risk_reasons(
            policy,
            equity=equity_value,
            risk_state=risk_state,
            calendar_state=calendar_state,
            equity_source=source,
        )
    )
    calendar_entry_reason = _entry_calendar_reason(policy, when, calendar_state)
    if calendar_entry_reason is not None:
        reasons.append(calendar_entry_reason)
    if reasons and not diagnostic:
        return _blocked_plan(policy, side, when, reasons, entry_price=price, atr=atr_value)
    (
        _,
        risk_budget,
        risk_per_unit,
        raw_quantity,
        quantity,
        actual_risk,
        stop,
        target,
        value_reasons,
        cost_components,
    ) = _calculate_entry_values(policy, side, price, atr_value, equity_value, spec, requested_quantity, cost_values)
    reasons.extend(value_reasons)
    reasons.extend(_margin_reasons(spec, risk_state, quantity))
    stop_reason, observed_stop_distance, observed_stop_basis = _stop_executable_side_metrics(
        side, stop, contract_spec, executable_bid, executable_ask
    )
    if stop_reason is not None:
        reasons.append(stop_reason)
    assumptions = {
        "risk_fraction": _text(policy.planned_risk_fraction),
        "stop_atr_multiple": _text(policy.stop_atr_multiple),
        "take_profit_atr_multiple": _text(policy.take_profit_atr_multiple),
        "quantity_raw": _text(raw_quantity),
        "quantity_grid_origin": "0",
        "equity_basis": policy.equity_basis,
        "equity_source": source or policy.equity_basis,
        "initial_r_is_immutable": True,
        "mode": selected_mode,
        "eligible_for_demo": not diagnostic and not reasons,
        "risk_budget": _text(risk_budget),
        "actual_risk_amount": _text(actual_risk),
        "initial_risk_basis": "filled_quantity * stop_distance * unit_value",
        "initial_risk_currency": policy.account_currency,
        "risk_budget_currency": policy.account_currency,
        "minimum_stop_distance_basis": observed_stop_basis,
        "minimum_stop_distance_observed": _text(observed_stop_distance),
        "risk_envelope_known": cost_components["risk_envelope_known"],
        "expected_total_loss_at_stop": _text(cost_components["expected_total_loss_at_stop"]),
        "expected_total_loss_at_stop_basis": cost_components["expected_total_loss_at_stop_basis"],
        "expected_cost_fixed": _text(cost_components["expected_cost_fixed"]),
        "expected_cost_per_unit": _text(cost_components["expected_cost_per_unit"]),
        "expected_exit_slippage_per_unit": _text(cost_components["expected_exit_slippage_per_unit"]),
        "expected_cost_variable_per_unit": _text(cost_components["expected_cost_variable_per_unit"]),
        "expected_cost_currency": cost_components["expected_cost_currency"],
        "expected_cost_source": cost_components["expected_cost_source"],
        "expected_cost_components": {
            "fixed": _text(cost_components["expected_cost_fixed"]),
            "commission_per_unit": _text(cost_components["expected_cost_per_unit"]),
            "exit_slippage_per_unit": _text(cost_components["expected_exit_slippage_per_unit"]),
            "currency": cost_components["expected_cost_currency"],
            "source": cost_components["expected_cost_source"],
        },
    }
    if diagnostic:
        assumptions["diagnostic_only"] = True
    if reasons:
        return _blocked_plan(
            policy,
            side,
            when,
            reasons,
            entry_price=price,
            atr=atr_value,
            risk_amount=actual_risk,
            risk_per_unit=risk_per_unit,
            risk_budget=risk_budget,
            quantity=quantity,
            initial_stop=stop,
            take_profit=target,
            assumptions=assumptions,
            eligible_for_demo=False,
            mode=selected_mode,
            expected_total_loss_at_stop=cost_components["expected_total_loss_at_stop"],
            expected_total_loss_at_stop_basis=cost_components["expected_total_loss_at_stop_basis"],
            expected_cost_fixed=cost_components["expected_cost_fixed"],
            expected_cost_per_unit=cost_components["expected_cost_per_unit"],
            expected_exit_slippage_per_unit=cost_components["expected_exit_slippage_per_unit"],
            expected_cost_currency=cost_components["expected_cost_currency"],
            expected_cost_source=cost_components["expected_cost_source"],
            risk_envelope_known=cost_components["risk_envelope_known"],
        )
    return EntryPlan(
        True,
        side,
        price,
        quantity,
        stop,
        target,
        actual_risk,
        risk_per_unit,
        atr_value,
        when,
        policy.holding_profile,
        policy.policy_hash,
        (),
        assumptions,
        not diagnostic,
        selected_mode,
        risk_budget,
        cost_components["expected_total_loss_at_stop"],
        cost_components["expected_total_loss_at_stop_basis"],
        cost_components["expected_cost_fixed"],
        cost_components["expected_cost_per_unit"],
        cost_components["expected_exit_slippage_per_unit"],
        cost_components["expected_cost_currency"],
        cost_components["expected_cost_source"],
        cost_components["risk_envelope_known"],
    )


def _time_exit_reason(
    policy: RiskExitPolicy,
    *,
    now: datetime,
    entry_at: datetime,
    bars_held: int,
    calendar_state: Mapping[str, Any] | None,
    allow_modelled_calendar: bool = False,
) -> str | None:
    intraday = policy.holding_profile == INTRADAY_PROFILE
    if intraday:
        if bars_held >= policy.intraday_max_bars:
            return "MAX_INTRADAY_BARS"
        # The 30-minute value is a pre-cut margin, not a second holding cap.
        buffer_minutes = policy.intraday_max_minutes
    else:
        if _duration_seconds(entry_at, now) >= policy.multiday_max_hours * Decimal("3600"):
            return "MAX_MULTIDAY_HOURS"
        buffer_minutes = policy.multiday_preclose_minutes
    calendar_reason = _calendar_reason(calendar_state, allow_modelled=allow_modelled_calendar)
    if calendar_reason:
        return calendar_reason
    assert isinstance(calendar_state, Mapping)
    allowed = (
        {"financing_at", "daily_close_at", "daily_cut_at", "weekly_close_at", "holiday_close_at", "market_cut_at"}
        if intraday
        else {"weekly_close_at", "holiday_close_at", "market_cut_at"}
    )
    for key, raw in _calendar_cut_items(calendar_state):
        if key not in allowed:
            continue
        cut = _utc(raw, name=key)
        remaining = _duration_seconds(now, cut)
        if D0 <= remaining <= buffer_minutes * Decimal("60"):
            return f"PRE_{key.upper()}"
    return None


def _calendar_cut_items(calendar_state: Mapping[str, Any]) -> tuple[tuple[str, Any], ...]:
    items: list[tuple[str, Any]] = []
    for key in (
        "financing_at",
        "daily_close_at",
        "daily_cut_at",
        "weekly_close_at",
        "holiday_close_at",
        "market_cut_at",
    ):
        raw = calendar_state.get(key)
        if raw is not None:
            items.append((key, raw))
    generic = calendar_state.get("next_cut_at", calendar_state.get("next_event_at"))
    if generic is not None:
        kind = str(calendar_state.get("next_cut_kind", calendar_state.get("event_kind", ""))).strip().upper()
        aliases = {
            "FINANCING": "financing_at",
            "FINANCING_CUT": "financing_at",
            "DAILY_CLOSE": "daily_close_at",
            "DAILY_CUT": "daily_cut_at",
            "WEEKLY_CLOSE": "weekly_close_at",
            "HOLIDAY_CLOSE": "holiday_close_at",
            "MARKET_CUT": "market_cut_at",
        }
        selected = aliases.get(kind)
        if selected is not None and not any(key == selected for key, _ in items):
            items.append((selected, generic))
    return tuple(items)


def _entry_calendar_reason(
    policy: RiskExitPolicy, when: datetime, calendar_state: Mapping[str, Any] | None
) -> str | None:
    if not isinstance(calendar_state, Mapping) or calendar_state.get("known") is not True:
        return None
    if calendar_state.get("market_open") is False or calendar_state.get("session_open") is False:
        return "MARKET_CLOSED"
    intraday = policy.holding_profile == INTRADAY_PROFILE
    allowed = (
        {"financing_at", "daily_close_at", "daily_cut_at", "weekly_close_at", "holiday_close_at", "market_cut_at"}
        if intraday
        else {"weekly_close_at", "holiday_close_at", "market_cut_at"}
    )
    buffer = policy.intraday_max_minutes if intraday else policy.multiday_preclose_minutes
    for key, raw in _calendar_cut_items(calendar_state):
        if key not in allowed:
            continue
        cut = _utc(raw, name=key)
        remaining = _duration_seconds(when, cut)
        if D0 <= remaining <= buffer * Decimal("60"):
            return f"ENTRY_PRE_{key.upper()}"
    return None


def _mfe_mae(
    side: str,
    entry: Decimal,
    favorable_price: Any | None,
    adverse_price: Any | None,
    stop_distance: Decimal | None,
) -> tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None]:
    if favorable_price is None or adverse_price is None:
        return None, None, None, None
    favorable = _decimal(favorable_price, name="favorable_price", positive=True)
    adverse = _decimal(adverse_price, name="adverse_price", positive=True)
    with decimal_context():
        mfe = favorable - entry if side == "LONG" else entry - favorable
        mae = entry - adverse if side == "LONG" else adverse - entry
        mfe = max(D0, mfe)
        mae = max(D0, mae)
        mfe_r = mfe / stop_distance if stop_distance and stop_distance > D0 else None
        mae_r = mae / stop_distance if stop_distance and stop_distance > D0 else None
    return mfe, mae, mfe_r, mae_r


def _duration_seconds(start: datetime, end: datetime) -> Decimal:
    delta = end - start
    micros = (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
    with decimal_context():
        return Decimal(micros).scaleb(-6)


def _decision(
    policy: RiskExitPolicy,
    action: str,
    reason: str,
    *,
    executable_price: Decimal | None,
    planned_trigger_price: Decimal | None,
    observed_at: datetime,
    server_side: bool,
    gap: bool,
    bars_held: int,
    holding_seconds: Decimal | None,
    mfe_price: Decimal | None,
    mae_price: Decimal | None,
    mfe_r: Decimal | None,
    mae_r: Decimal | None,
) -> ExitDecision:
    latency = D0 if server_side and action in {EXIT_STOP_LOSS, EXIT_TAKE_PROFIT} else policy.exit_latency_seconds
    return ExitDecision(
        action,
        reason,
        executable_price,
        planned_trigger_price,
        observed_at,
        latency,
        server_side,
        gap,
        bars_held,
        holding_seconds,
        mfe_price,
        mae_price,
        mfe_r,
        mae_r,
        policy.policy_hash,
        observed_at if action in {EXIT_STOP_LOSS, EXIT_TAKE_PROFIT, EXIT_TIME} else None,
        latency if action in {EXIT_STOP_LOSS, EXIT_TAKE_PROFIT, EXIT_TIME} else None,
        None,
    )


def evaluate_exit(
    policy: RiskExitPolicy,
    entry_plan: EntryPlan | Mapping[str, Any] | None = None,
    *,
    current_price: Any | None,
    observed_at: Any,
    entry_at: Any | None = None,
    bars_held: int = 0,
    calendar_state: Mapping[str, Any] | None = None,
    gap: bool = False,
    executable_price: Any | None = None,
    current_price_is_executable: bool = False,
    favorable_price: Any | None = None,
    adverse_price: Any | None = None,
    server_side_stop: bool = False,
) -> ExitDecision:
    """Evaluate one tick against immutable levels and an explicit fill side.

    ``current_price`` is context only unless the caller sets
    ``current_price_is_executable=True``.  Production composition should pass
    the observed bid/ask in ``executable_price`` so a midpoint can never be
    mistaken for a liquidation price.
    """

    if not isinstance(current_price_is_executable, bool):
        raise RiskExitError("current_price_is_executable debe ser booleano")
    now = _utc(observed_at, name="observed_at")
    bars = _integer(bars_held, name="bars_held", minimum=0)
    plan = _coerce_entry_plan(entry_plan)
    if plan is None or (not plan.allowed and plan.mode != "VIRTUAL_DIAGNOSTIC"):
        return _decision(
            policy,
            EXIT_UNKNOWN,
            "ENTRY_PLAN_REQUIRED" if plan is None else "ENTRY_PLAN_BLOCKED",
            executable_price=None,
            planned_trigger_price=None,
            observed_at=now,
            server_side=server_side_stop,
            gap=gap,
            bars_held=bars,
            holding_seconds=None,
            mfe_price=None,
            mae_price=None,
            mfe_r=None,
            mae_r=None,
        )
    if plan.policy_hash != policy.policy_hash:
        return _decision(
            policy,
            EXIT_UNKNOWN,
            "ENTRY_PLAN_POLICY_MISMATCH",
            executable_price=None,
            planned_trigger_price=None,
            observed_at=now,
            server_side=server_side_stop,
            gap=gap,
            bars_held=bars,
            holding_seconds=None,
            mfe_price=None,
            mae_price=None,
            mfe_r=None,
            mae_r=None,
        )
    if plan.entry_price is None or plan.initial_stop is None or plan.take_profit is None:
        return _decision(
            policy,
            EXIT_UNKNOWN,
            "ENTRY_PLAN_LEVELS_UNKNOWN",
            executable_price=None,
            planned_trigger_price=None,
            observed_at=now,
            server_side=server_side_stop,
            gap=gap,
            bars_held=bars,
            holding_seconds=None,
            mfe_price=None,
            mae_price=None,
            mfe_r=None,
            mae_r=None,
        )
    side = _direction(plan.direction)
    started = _utc(entry_at if entry_at is not None else plan.planned_at, name="entry_at")
    distance = abs(plan.entry_price - plan.initial_stop)
    mfe, mae, mfe_r, mae_r = _mfe_mae(side, plan.entry_price, favorable_price, adverse_price, distance)
    holding = _duration_seconds(started, now)
    if holding < D0:
        return _decision(
            policy,
            EXIT_UNKNOWN,
            "ENTRY_TIME_AFTER_OBSERVATION",
            executable_price=None,
            planned_trigger_price=None,
            observed_at=now,
            server_side=server_side_stop,
            gap=gap,
            bars_held=bars,
            holding_seconds=None,
            mfe_price=mfe,
            mae_price=mae,
            mfe_r=mfe_r,
            mae_r=mae_r,
        )
    if executable_price is None and current_price is not None and not current_price_is_executable:
        return _decision(
            policy,
            EXIT_UNKNOWN,
            "EXECUTABLE_PRICE_REQUIRED",
            executable_price=None,
            planned_trigger_price=None,
            observed_at=now,
            server_side=server_side_stop,
            gap=gap,
            bars_held=bars,
            holding_seconds=holding,
            mfe_price=mfe,
            mae_price=mae,
            mfe_r=mfe_r,
            mae_r=mae_r,
        )
    if current_price is None and executable_price is None:
        return _decision(
            policy,
            EXIT_UNKNOWN,
            "NO_EXECUTABLE_PRICE",
            executable_price=None,
            planned_trigger_price=None,
            observed_at=now,
            server_side=server_side_stop,
            gap=gap,
            bars_held=bars,
            holding_seconds=holding,
            mfe_price=mfe,
            mae_price=mae,
            mfe_r=mfe_r,
            mae_r=mae_r,
        )
    current = _decimal(
        executable_price if executable_price is not None else current_price,
        name="executable_price",
        positive=True,
    )
    if (side == "LONG" and current <= plan.initial_stop) or (side == "SHORT" and current >= plan.initial_stop):
        return _decision(
            policy,
            EXIT_STOP_LOSS,
            "STOP_LOSS_GAP" if gap else "STOP_LOSS_TICK",
            executable_price=current,
            planned_trigger_price=plan.initial_stop,
            observed_at=now,
            server_side=server_side_stop,
            gap=gap,
            bars_held=bars,
            holding_seconds=holding,
            mfe_price=mfe,
            mae_price=mae,
            mfe_r=mfe_r,
            mae_r=mae_r,
        )
    if (side == "LONG" and current >= plan.take_profit) or (side == "SHORT" and current <= plan.take_profit):
        return _decision(
            policy,
            EXIT_TAKE_PROFIT,
            "TAKE_PROFIT_GAP" if gap else "TAKE_PROFIT_TICK",
            executable_price=current,
            planned_trigger_price=plan.take_profit,
            observed_at=now,
            server_side=server_side_stop,
            gap=gap,
            bars_held=bars,
            holding_seconds=holding,
            mfe_price=mfe,
            mae_price=mae,
            mfe_r=mfe_r,
            mae_r=mae_r,
        )
    time_reason = _time_exit_reason(
        policy,
        now=now,
        entry_at=started,
        bars_held=bars,
        calendar_state=calendar_state,
        allow_modelled_calendar=plan.mode == "VIRTUAL_DIAGNOSTIC",
    )
    action = (
        EXIT_UNKNOWN
        if time_reason in {"CALENDAR_UNKNOWN", "CALENDAR_UNVERIFIED", "FINANCING_UNKNOWN"}
        else EXIT_TIME
        if time_reason
        else EXIT_NONE
    )
    return _decision(
        policy,
        action,
        time_reason or "NO_EXIT",
        executable_price=current if action == EXIT_TIME else None,
        planned_trigger_price=None,
        observed_at=now,
        server_side=False,
        gap=False,
        bars_held=bars,
        holding_seconds=holding,
        mfe_price=mfe,
        mae_price=mae,
        mfe_r=mfe_r,
        mae_r=mae_r,
    )


def _coerce_entry_plan(value: EntryPlan | Mapping[str, Any] | None) -> EntryPlan | None:
    if value is None:
        return None
    if isinstance(value, EntryPlan):
        return value
    if isinstance(value, Mapping):
        return EntryPlan.from_mapping(value)
    raise RiskExitError("entry_plan debe ser EntryPlan o mapping")


def serialize(value: RiskExitPolicy | EntryPlan | ExitDecision | Mapping[str, Any]) -> dict[str, Any]:
    """Serialize a policy/plan/decision without leaking Decimal objects."""

    if isinstance(value, RiskExitPolicy):
        return value.serialize()
    if isinstance(value, (EntryPlan, ExitDecision)):
        return value.to_dict()
    return cast(dict[str, Any], _jsonable(value))


SizedEntry = EntryPlan
RiskDecision = ExitDecision


__all__ = [
    "EntryPlan",
    "ExitDecision",
    "INTRADAY_PROFILE",
    "MULTIDAY_PROFILE",
    "POLICY_VERSION",
    "RiskExitError",
    "RiskExitPolicy",
    "RiskDecision",
    "SizedEntry",
    "EXIT_NONE",
    "EXIT_STOP_LOSS",
    "EXIT_TAKE_PROFIT",
    "EXIT_TIME",
    "EXIT_UNKNOWN",
    "evaluate_exit",
    "plan_entry",
    "serialize",
]
