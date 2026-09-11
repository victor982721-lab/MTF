"""Evaluación causal y simulación virtual, sin conectores de ejecución.

La evaluación separa tiempo de mercado, disponibilidad y vencimiento. Las
observaciones tardías no se usan antes de estar disponibles; una liquidación
que todavía no puede ocurrir queda ``PENDING`` en observación continua y sólo
queda ``INDETERMINATE`` cuando la captura se declaró completa sin precio
admisible. El mismo módulo sirve para replay, backtest y watch.
"""
from __future__ import annotations

import dataclasses
import enum
import math
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any


class Outcome(str, enum.Enum):
    WIN = "WIN"
    LOSS = "LOSS"
    TIE = "TIE"
    PENDING = "PENDING"
    INDETERMINATE = "INDETERMINATE"


# One canonical vocabulary is shared with runtime/state.  ``trade`` and
# ``close`` are explicit compatibility aliases for ``traded``; no other value
# is silently converted.
PRICE_BASE_ALIASES = {
    "trade": "traded",
    "traded": "traded",
    "close": "traded",
    "bid": "bid",
    "ask": "ask",
    "mid": "mid",
}
VALID_PRICE_BASES = frozenset({"traded", "bid", "ask", "mid"})
QUALITY_ALLOWED_LABELS = frozenset({
    "VALID", "VALIDATED", "OK", "GOOD", "SYNTHETIC", "SYNTHETIC_VALID",
    "SYNTHETIC_VALIDATED", "VALID_DATA", "DATA_QUALITY_VALIDATED",
    "VALIDATED_LOCAL", "PUBLIC_PROVIDER_CLOSED", "CLOSED_VALID",
})
QUALITY_BLOCKED_TOKENS = (
    "INVALID", "UNKNOWN", "DISCONNECTED", "STALE", "GAP", "PARTIAL",
    "OPEN", "UNRECONCILED", "OUT_OF_ORDER", "DUPLICATE", "LATE",
    "INSUFFICIENT", "PENDING", "ANOM",
)


def normalize_price_base(
    value: Any,
    *,
    default: str | None = None,
    allow_none: bool = True,
) -> str | None:
    """Normalize an explicit price base for evaluator and runtime book.

    ``None`` means wildcard only when ``allow_none`` is true.  Unknown values
    raise instead of becoming ``traded``.
    """

    if value is None:
        if default is not None:
            value = default
        elif allow_none:
            return None
        else:
            raise ValueError("price base is required")
    raw = value.value if hasattr(value, "value") else value
    if not isinstance(raw, str):
        raise ValueError(f"price base must be text/enum, got {value!r}")
    text = raw.strip().lower()
    try:
        return PRICE_BASE_ALIASES[text]
    except KeyError as exc:
        raise ValueError(f"unknown price base: {value!r}; expected traded/bid/ask/mid") from exc


def price_bases_match(actual: Any, requested: Any | None) -> bool:
    """Compare bases after explicit alias normalization."""

    actual_base = normalize_price_base(actual, allow_none=False)
    requested_base = normalize_price_base(requested, allow_none=True)
    return requested_base is None or actual_base == requested_base


def normalize_completion(
    data_complete: bool | None = True,
    *,
    capture_complete: bool | None = None,
) -> bool:
    """Resolve the legacy ``data_complete`` name and new capture state.

    Completeness is deliberately separate from ``as_of``: an open capture may
    have a current watermark, while a complete replay may mark an unresolved
    horizon ``INDETERMINATE``.
    """

    if data_complete is not None and not isinstance(data_complete, bool):
        raise ValueError("data_complete must be bool or None")
    if capture_complete is not None and not isinstance(capture_complete, bool):
        raise ValueError("capture_complete must be bool or None")
    if capture_complete is not None:
        if data_complete not in (None, True) and data_complete != capture_complete:
            raise ValueError("data_complete and capture_complete disagree")
        return capture_complete
    return True if data_complete is None else data_complete


def outcome_for_missing_price(
    *,
    capture_complete: bool,
    reason: str,
) -> tuple[Outcome, str]:
    """Use one missing-data transition for batch evaluator and runtime state."""

    return (Outcome.INDETERMINATE if capture_complete else Outcome.PENDING, reason)


def quality_label_is_usable(value: Any) -> bool:
    """Strict shared quality gate for price observations."""

    if isinstance(value, Mapping):
        value = value.get("status", value.get("quality", "UNKNOWN"))
    elif hasattr(value, "status") and not isinstance(value, (str, bytes)):
        value = getattr(value, "status")
    label = str(value or "").strip().upper().replace("-", "_").replace(" ", "_")
    if not label or any(token in label for token in QUALITY_BLOCKED_TOKENS):
        return False
    return label in QUALITY_ALLOWED_LABELS or label.startswith("VALID_") or label.startswith("SYNTHETIC_VALID")


def parse_bool(value: Any, *, default: bool | None = None, name: str = "boolean") -> bool:
    """Parse persisted booleans without Python truthiness surprises."""

    if value is None and default is not None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "1", "yes", "y", "on"}:
            return True
        if text in {"false", "0", "no", "n", "off"}:
            return False
    raise ValueError(f"{name} must be boolean, got {value!r}")


def normalize_observation_times(timestamp: Any, available_at: Any | None = None) -> tuple[datetime, datetime]:
    """Return market and availability times, rejecting availability in the past."""

    market = parse_ts(timestamp)
    available = parse_ts(available_at) if available_at is not None else market
    if available < market:
        raise ValueError("available_at cannot precede market timestamp")
    return market, available


def _record(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {field.name: getattr(value, field.name) for field in dataclasses.fields(value)}
    if hasattr(value, "model_dump"):
        return dict(value.model_dump())
    if hasattr(value, "to_dict"):
        result = value.to_dict()
        if isinstance(result, Mapping):
            return dict(result)
    if hasattr(value, "__dict__"):
        return {k: v for k, v in vars(value).items() if not k.startswith("_")}
    raise TypeError(f"expected a mapping/dataclass/object, got {type(value)!r}")


def parse_ts(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        dt = datetime.fromtimestamp(float(value), UTC)
    else:
        text = str(value).strip()
        if text.endswith("Z") or text.endswith("z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError("timestamp must include a timezone")
    return dt.astimezone(UTC)


def iso_ts(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z") if value is not None else None


@dataclasses.dataclass(frozen=True, slots=True)
class PricePoint:
    """A price observation with distinct market and availability times."""

    timestamp: datetime
    price: float
    available_at: datetime | None = None
    source: str = "unknown"
    base_price: str = "unknown"
    quality: str = "UNKNOWN"
    resolution: str = "UNKNOWN"
    closed: bool = True
    source_ordinal: int = 0
    point_id: str | None = None
    instrument: str = "UNKNOWN"

    def __post_init__(self) -> None:
        market = parse_ts(self.timestamp)
        available = parse_ts(self.available_at) if self.available_at is not None else market
        quality = str(self.quality or "UNKNOWN")
        if available < market:
            quality = f"INVALID:availability_before_market:{quality}"
        try:
            base = normalize_price_base(self.base_price, allow_none=False)
        except ValueError:
            base = str(self.base_price or "unknown").strip().lower() or "unknown"
            quality = f"INVALID:unknown_price_base:{base}:{quality}"
        if not math.isfinite(float(self.price)):
            raise ValueError("price must be finite")
        closed = parse_bool(self.closed, name="closed")
        instrument = str(self.instrument or "UNKNOWN").strip() or "UNKNOWN"
        source = str(self.source or "unknown").strip() or "unknown"
        resolution = str(self.resolution or "UNKNOWN")
        point_id = None if self.point_id is None else str(self.point_id).strip() or None
        object.__setattr__(self, "timestamp", market)
        object.__setattr__(self, "available_at", available)
        object.__setattr__(self, "base_price", base or "unknown")
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "quality", quality)
        object.__setattr__(self, "resolution", resolution)
        object.__setattr__(self, "closed", closed)
        object.__setattr__(self, "instrument", instrument)
        object.__setattr__(self, "point_id", point_id)
        if isinstance(self.source_ordinal, bool) or int(self.source_ordinal) != self.source_ordinal:
            raise ValueError("source_ordinal must be integer")
        object.__setattr__(self, "source_ordinal", int(self.source_ordinal))

    @property
    def market_time(self) -> datetime:
        return self.timestamp

    @property
    def available_ts(self) -> datetime:
        return self.available_at or self.timestamp

    @property
    def identity(self) -> str:
        return str(self.point_id or f"{self.source}:{self.instrument}:{self.timestamp.isoformat()}:{self.source_ordinal}")

    @property
    def observation_id(self) -> str:
        return self.identity

    def usable_as_of(self, as_of: datetime | None) -> bool:
        return as_of is None or self.available_ts <= parse_ts(as_of)

    def to_dict(self) -> dict[str, Any]:
        return {
            "point_id": self.point_id,
            "observation_id": self.identity,
            "timestamp": iso_ts(self.timestamp),
            "market_time": iso_ts(self.timestamp),
            "price": float(self.price),
            "available_at": iso_ts(self.available_ts),
            "source": self.source,
            "base_price": self.base_price,
            "quality": self.quality,
            "resolution": self.resolution,
            "closed": self.closed,
            "source_ordinal": self.source_ordinal,
            "instrument": self.instrument,
        }


def _enum_value(value: Any) -> Any:
    return value.value if hasattr(value, "value") else value


def _extract_point(value: Any, ordinal: int) -> PricePoint:
    row = _record(value)
    end = row.get("end_ts", row.get("interval_end", row.get("end", row.get("close_ts", row.get("close_time")))))
    timestamp_value = end if end is not None else row.get("event_ts", row.get("event_time", row.get("interval_start", row.get("timestamp", row.get("ts", row.get("time"))))))
    if timestamp_value is None:
        raise ValueError("price point has no timestamp")
    available_value = row.get("available_ts", row.get("available_at", row.get("received_ts", row.get("received_at"))))
    raw_base = _enum_value(row.get("price_base", row.get("price_basis", row.get("base_price", row.get("price_type", "")))))
    base = str(raw_base or "").lower()
    if "price" in row and row["price"] is not None:
        price = row["price"]; base = base or "traded"
    elif "close" in row and row["close"] is not None:
        price = row["close"]; base = base or "close"
    elif "mid" in row and row["mid"] is not None:
        price = row["mid"]; base = base or "mid"
    else:
        raise ValueError("price point has no explicit price/close/mid value")
    # Unspecified quality is the generic evaluator's neutral default; an
    # explicit UNKNOWN label remains blocked below. Provider/runtime paths
    # always carry an explicit label.
    quality = row.get("quality", row.get("data_quality", "VALID"))
    if isinstance(quality, Mapping):
        quality = quality.get("status", "UNKNOWN")
    elif hasattr(quality, "status"):
        quality = getattr(quality, "status")
    resolution = _enum_value(row.get("resolution", row.get("timeframe", row.get("interval", "UNKNOWN"))))
    point_id = row.get("observation_id", row.get("point_id", row.get("candle_id", row.get("data_id", row.get("event_id", row.get("source_event_id"))))))
    return PricePoint(
        timestamp=parse_ts(timestamp_value), price=float(price),
        available_at=parse_ts(available_value) if available_value is not None else None,
        source=str(row.get("source", row.get("provider", "unknown"))), base_price=base or "unknown",
        quality=str(quality), resolution=str(resolution), closed=parse_bool(row.get("closed", row.get("is_closed", True)), name="closed"),
        source_ordinal=int(row.get("source_ordinal", row.get("ordinal", ordinal))), point_id=str(point_id) if point_id is not None else None,
        instrument=str(row.get("instrument", row.get("symbol", "UNKNOWN")) or "UNKNOWN"),
    )


def normalize_points(points: Iterable[Any]) -> list[PricePoint]:
    result = [_extract_point(point, ordinal) for ordinal, point in enumerate(points)]
    # El orden de mercado es primario; disponibilidad y ordinal son desempates
    # estables para mensajes agrupados o timestamps iguales.
    result.sort(key=lambda p: (p.timestamp, p.available_ts, p.source_ordinal, p.identity))
    return result


@dataclasses.dataclass(frozen=True, slots=True)
class EvaluationSpec:
    horizons_seconds: tuple[float, ...] = (60.0, 180.0, 300.0)
    entry_latency_seconds: float = 1.0
    entry_rule: str = "first_observation_at_or_after"
    exit_rule: str = "last_observation_at_or_before"
    horizon_from: str = "entry"
    stake: float = 1.0
    max_price_age_seconds: float = 120.0
    tie_tolerance: float = 0.0
    payout_net: float = 0.80
    loss_amount: float = 1.0
    tie_net: float = 0.0
    costs: float = 0.0
    requested_base_price: str | None = None
    require_closed: bool = True

    def __post_init__(self) -> None:
        horizons = tuple(float(x) for x in self.horizons_seconds)
        if not horizons or any(not math.isfinite(x) or x <= 0 for x in horizons):
            raise ValueError("horizons_seconds must contain positive finite values")
        object.__setattr__(self, "horizons_seconds", horizons)
        for name in ("entry_latency_seconds", "stake", "max_price_age_seconds", "tie_tolerance", "payout_net", "loss_amount", "costs"):
            value = float(getattr(self, name))
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be a finite non-negative number")
            object.__setattr__(self, name, value)
        if self.stake <= 0:
            raise ValueError("stake must be greater than zero")
        if self.entry_rule != "first_observation_at_or_after":
            raise ValueError(f"unsupported entry_rule: {self.entry_rule!r}")
        if self.exit_rule not in {"last_observation_at_or_before", "first_observation_at_or_after"}:
            raise ValueError(f"unsupported exit_rule: {self.exit_rule!r}")
        if self.horizon_from not in {"entry", "detection"}:
            raise ValueError("horizon_from must be entry or detection")
        if not isinstance(self.require_closed, bool):
            raise ValueError("require_closed must be boolean")
        object.__setattr__(
            self,
            "requested_base_price",
            normalize_price_base(self.requested_base_price, allow_none=True),
        )

    def to_dict(self) -> dict[str, Any]:
        return {field.name: getattr(self, field.name) for field in dataclasses.fields(self)}


@dataclasses.dataclass(frozen=True, slots=True)
class SimulationResult:
    simulation_id: str
    signal_id: str | None
    simulation_type: str
    horizon_seconds: float
    direction: str
    detected_ts: str
    entry_ts: str | None
    expiry_ts: str
    entry_price: float | None
    final_price: float | None
    outcome: Outcome
    stake: float
    net_result: float | None
    price_base: str
    quality: str
    resolution: str
    assumptions: dict[str, Any]
    reason: str | None = None
    entry_market_ts: str | None = None
    final_market_ts: str | None = None
    entry_available_ts: str | None = None
    final_available_ts: str | None = None
    entry_point_id: str | None = None
    final_point_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        result = {field.name: getattr(self, field.name) for field in dataclasses.fields(self)}
        result["outcome"] = self.outcome.value
        return result


def break_even_probability(*, payout_net: float = 0.80, loss_amount: float = 1.0, costs: float = 0.0, stake: float = 1.0) -> float:
    """Equilibrio sin empates: ``(S*L+C)/(S*(R+L))``.

    ``payout_net`` y ``loss_amount`` son multiplicadores del stake; ``costs``
    es un coste absoluto por resultado. Si no existe retorno positivo, el
    equilibrio es infinito y se devuelve ``math.inf``.
    """
    payout_net = float(payout_net); loss_amount = float(loss_amount); costs = float(costs); stake = float(stake)
    if any(not math.isfinite(x) for x in (payout_net, loss_amount, costs, stake)) or payout_net < 0 or loss_amount < 0 or costs < 0 or stake <= 0:
        raise ValueError("invalid break-even parameters")
    denominator = stake * (payout_net + loss_amount)
    return (stake * loss_amount + costs) / denominator if denominator > 0 else math.inf


_BLOCKED_QUALITY = frozenset(token.lower() for token in QUALITY_BLOCKED_TOKENS)


def _quality_admissible(value: PricePoint | Any) -> bool:
    if isinstance(value, PricePoint) or hasattr(value, "quality"):
        return quality_label_is_usable(getattr(value, "quality"))
    return quality_label_is_usable(value)


def _base_matches(point: PricePoint, wanted: str | None) -> bool:
    try:
        return price_bases_match(point.base_price, wanted)
    except ValueError:
        return False


@dataclasses.dataclass(frozen=True, slots=True)
class Selection:
    point: PricePoint
    use_time: datetime


_Selection = Selection


def observation_age_seconds(point: PricePoint, target: datetime, *, rule: str) -> float:
    """Conservative age: market distance plus any availability delay."""

    target_dt = parse_ts(target)
    if rule == "first_observation_at_or_after":
        market_distance = (point.timestamp - target_dt).total_seconds()
    elif rule == "last_observation_at_or_before":
        market_distance = (target_dt - point.timestamp).total_seconds()
    else:
        raise ValueError(f"unsupported selection rule: {rule!r}")
    availability_delay = max(0.0, (point.available_ts - target_dt).total_seconds())
    return max(0.0, market_distance, availability_delay)


def select_price_point(
    points: Sequence[PricePoint],
    target: datetime,
    *,
    rule: str,
    requested_base_price: str | None = None,
    require_closed: bool = True,
    max_price_age_seconds: float = math.inf,
    as_of: datetime | None = None,
    exclude_identity: str | None = None,
    exclude_market_time: datetime | None = None,
    instrument: str | None = None,
) -> tuple[Selection | None, str | None]:
    """Shared deterministic selection policy for batch and runtime callers.

    A price is usable only after its ``available_ts``. For a final ``before``
    rule without an explicit as-of watermark, the implicit cutoff is the
    target plus the configured grace period; this prevents a complete replay
    from selecting a future correction without making the grace explicit.
    """

    target_dt = parse_ts(target)
    if as_of is not None:
        cutoff = parse_ts(as_of)
    elif rule == "last_observation_at_or_before" and math.isfinite(float(max_price_age_seconds)):
        cutoff = target_dt + timedelta(seconds=float(max_price_age_seconds))
    else:
        cutoff = None
    eligible: list[PricePoint] = []
    for point in points:
        try:
            if require_closed and not point.closed:
                continue
            if not _quality_admissible(point):
                continue
            # Unknown base labels remain unavailable; they never fall back.
            normalize_price_base(point.base_price, allow_none=False)
            if not _base_matches(point, requested_base_price):
                continue
            if point.identity == exclude_identity:
                continue
            if exclude_market_time is not None and point.timestamp <= parse_ts(exclude_market_time):
                continue
            if cutoff is not None and point.available_ts > cutoff:
                continue
            if instrument and point.instrument.upper() not in {"", "UNKNOWN"} and point.instrument.upper() != str(instrument).upper():
                continue
            eligible.append(point)
        except (TypeError, ValueError):
            continue
    if rule == "first_observation_at_or_after":
        candidates = [point for point in eligible if point.timestamp >= target_dt]
        candidates.sort(key=lambda point: (point.timestamp, point.available_ts, point.source_ordinal, point.identity))
    elif rule == "last_observation_at_or_before":
        candidates = [point for point in eligible if point.timestamp <= target_dt]
        candidates.sort(key=lambda point: (point.timestamp, point.available_ts, point.source_ordinal, point.identity), reverse=True)
    else:
        raise ValueError(f"unsupported selection rule: {rule!r}")
    for point in candidates:
        age = observation_age_seconds(point, target_dt, rule=rule)
        if age > float(max_price_age_seconds):
            return None, "MAX_PRICE_AGE_EXCEEDED"
        use_time = max(target_dt, point.available_ts) if rule == "first_observation_at_or_after" else point.available_ts
        return Selection(point, use_time), None
    return None, "PRICE_NOT_AVAILABLE" if rule == "first_observation_at_or_after" else "PRICE_NOT_AVAILABLE_BY_CUTOFF"


class DirectionalEvaluator:
    """Evalúa resultados sin seleccionar precios favorables retrospectivamente."""
    def __init__(self, spec: EvaluationSpec | None = None):
        self.spec = spec or EvaluationSpec()

    def _select(
        self,
        points: Sequence[PricePoint],
        target: datetime,
        *,
        rule: str,
        as_of: datetime | None = None,
        exclude_point_id: str | None = None,
        exclude_market_time: datetime | None = None,
        instrument: str | None = None,
    ) -> tuple[Selection | None, str | None]:
        return select_price_point(
            points,
            target,
            rule=rule,
            requested_base_price=self.spec.requested_base_price,
            require_closed=self.spec.require_closed,
            max_price_age_seconds=self.spec.max_price_age_seconds,
            as_of=as_of,
            exclude_identity=exclude_point_id,
            exclude_market_time=exclude_market_time,
            instrument=instrument,
        )

    @staticmethod
    def _direction(signal_row: Mapping[str, Any]) -> str | None:
        raw = str(signal_row.get("direction", signal_row.get("side", ""))).upper()
        aliases = {"UP": "UP", "LONG": "UP", "BUY": "UP", "BULL": "UP", "ALCISTA": "UP", "DOWN": "DOWN", "SHORT": "DOWN", "SELL": "DOWN", "BEAR": "DOWN", "BAJISTA": "DOWN"}
        return aliases.get(raw)

    def _result(
        self, *, sim_id: str, signal_id: str | None, simulation_type: str, horizon: float, direction: str,
        detected: datetime, expiry: datetime, entry: _Selection | None, final: _Selection | None,
        outcome: Outcome, reason: str | None, assumptions: dict[str, Any], net: float | None,
    ) -> SimulationResult:
        return SimulationResult(
            simulation_id=sim_id, signal_id=signal_id, simulation_type=simulation_type,
            horizon_seconds=horizon, direction=direction, detected_ts=iso_ts(detected) or "",
            entry_ts=iso_ts(entry.use_time) if entry else None, expiry_ts=iso_ts(expiry) or "",
            entry_price=entry.point.price if entry else None, final_price=final.point.price if final else None,
            outcome=outcome, stake=self.spec.stake, net_result=net,
            price_base=entry.point.base_price if entry else self.spec.requested_base_price or "unknown",
            quality=(entry.point.quality if entry else final.point.quality if final else "UNKNOWN"),
            resolution=(entry.point.resolution if entry else final.point.resolution if final else "UNKNOWN"),
            assumptions=assumptions, reason=reason,
            entry_market_ts=iso_ts(entry.point.timestamp) if entry else None,
            final_market_ts=iso_ts(final.point.timestamp) if final else None,
            entry_available_ts=iso_ts(entry.point.available_ts) if entry else None,
            final_available_ts=iso_ts(final.point.available_ts) if final else None,
            entry_point_id=entry.point.identity if entry else None,
            final_point_id=final.point.identity if final else None,
        )

    def evaluate_prepared(
        self, signal: Any, points: Sequence[PricePoint], *, horizon_seconds: float | None = None,
        simulation_id: str | None = None, simulation_type: str = "DIRECTIONAL",
        data_complete: bool | None = True, as_of: datetime | None = None,
        capture_complete: bool | None = None,
    ) -> SimulationResult:
        signal_row = _record(signal)
        complete = normalize_completion(data_complete, capture_complete=capture_complete)
        horizon = float(horizon_seconds if horizon_seconds is not None else self.spec.horizons_seconds[0])
        if horizon <= 0 or not math.isfinite(horizon):
            raise ValueError("horizon_seconds must be positive and finite")
        detected_value = signal_row.get("detected_ts", signal_row.get("detected_at", signal_row.get("timestamp", signal_row.get("ts"))))
        if detected_value is None:
            raise ValueError("signal has no detected timestamp")
        detected = parse_ts(detected_value)
        signal_id = signal_row.get("signal_id", signal_row.get("id"))
        direction = self._direction(signal_row)
        sim_id = str(simulation_id or f"{signal_id or 'signal'}:{horizon:g}:{simulation_type}")
        entry_target = detected + timedelta(seconds=self.spec.entry_latency_seconds)
        assumptions: dict[str, Any] = {
            **self.spec.to_dict(),
            "selection_rule_entry": self.spec.entry_rule,
            "selection_rule_exit": self.spec.exit_rule,
            "target_entry_ts": iso_ts(entry_target),
            "data_complete": complete,
            "capture_complete": complete,
            "as_of": iso_ts(parse_ts(as_of)) if as_of is not None else None,
            "no_favorable_lookahead": True,
        }
        if direction is None:
            assumptions["invalid_direction"] = signal_row.get("direction", signal_row.get("side"))
            expiry = detected + timedelta(seconds=horizon)
            return self._result(sim_id=sim_id, signal_id=str(signal_id) if signal_id is not None else None, simulation_type=simulation_type, horizon=horizon, direction="UNKNOWN", detected=detected, expiry=expiry, entry=None, final=None, outcome=Outcome.INDETERMINATE, reason="INVALID_DIRECTION", assumptions=assumptions, net=None)
        if not points:
            expiry = entry_target + timedelta(seconds=horizon)
            outcome = Outcome.INDETERMINATE if complete else Outcome.PENDING
            return self._result(sim_id=sim_id, signal_id=str(signal_id) if signal_id is not None else None, simulation_type=simulation_type, horizon=horizon, direction=direction, detected=detected, expiry=expiry, entry=None, final=None, outcome=outcome, reason="NO_PRICE_POINTS", assumptions=assumptions, net=None)
        point_instruments = {point.instrument.upper() for point in points if point.instrument.upper() not in {"", "UNKNOWN"}}
        signal_instrument = str(signal_row.get("instrument", "")).upper()
        if len(point_instruments) > 1 or (signal_instrument and point_instruments and signal_instrument not in point_instruments):
            expiry = entry_target + timedelta(seconds=horizon)
            return self._result(sim_id=sim_id, signal_id=str(signal_id) if signal_id is not None else None, simulation_type=simulation_type, horizon=horizon, direction=direction, detected=detected, expiry=expiry, entry=None, final=None, outcome=Outcome.INDETERMINATE, reason="INSTRUMENT_MISMATCH", assumptions=assumptions, net=None)
        entry, entry_reason = self._select(points, entry_target, rule=self.spec.entry_rule, as_of=as_of, instrument=signal_instrument or None)
        if entry is None:
            expiry = entry_target + timedelta(seconds=horizon)
            outcome = Outcome.PENDING if not complete else Outcome.INDETERMINATE
            assumptions["target_expiry_ts"] = iso_ts(expiry)
            return self._result(sim_id=sim_id, signal_id=str(signal_id) if signal_id is not None else None, simulation_type=simulation_type, horizon=horizon, direction=direction, detected=detected, expiry=expiry, entry=None, final=None, outcome=outcome, reason=entry_reason, assumptions=assumptions, net=None)
        entry_time = entry.use_time
        expiry = (entry_time if self.spec.horizon_from == "entry" else detected) + timedelta(seconds=horizon)
        assumptions.update({"target_expiry_ts": iso_ts(expiry), "effective_entry_ts": iso_ts(entry_time), "entry_market_ts": iso_ts(entry.point.timestamp), "entry_point_id": entry.point.identity})
        # An open capture cannot terminalize a last-before result until the
        # expiry plus grace period. A first-after result becomes terminal as
        # soon as an eligible post-expiry observation is actually available.
        if not complete and self.spec.exit_rule == "last_observation_at_or_before":
            final_deadline = expiry + timedelta(seconds=self.spec.max_price_age_seconds)
            if as_of is None or parse_ts(as_of) < final_deadline:
                return self._result(
                    sim_id=sim_id, signal_id=str(signal_id) if signal_id is not None else None,
                    simulation_type=simulation_type, horizon=horizon, direction=direction,
                    detected=detected, expiry=expiry, entry=entry, final=None,
                    outcome=Outcome.PENDING, reason="FINAL_PRICE_NOT_YET_DUE",
                    assumptions=assumptions, net=None,
                )
        # No se permite liquidar contra la misma observación ni contra una
        # marca de mercado anterior a la entrada efectiva.
        final_as_of = as_of
        if self.spec.exit_rule == "last_observation_at_or_before":
            availability_cutoff = expiry + timedelta(seconds=self.spec.max_price_age_seconds)
            final_as_of = min(final_as_of, availability_cutoff) if final_as_of is not None else availability_cutoff
        final, final_reason = self._select(points, expiry, rule=self.spec.exit_rule, as_of=final_as_of, exclude_point_id=entry.point.identity, exclude_market_time=entry.point.timestamp, instrument=str(signal_row.get("instrument")) if signal_row.get("instrument") else None)
        if final is None:
            # En watch, un vencimiento futuro o un precio aún no disponible es
            # trabajo pendiente; en replay completo queda indeterminado.
            latest_available = max((point.available_ts for point in points), default=None)
            pending_reason = final_reason or "FINAL_PRICE_NOT_AVAILABLE"
            if not complete and (latest_available is None or latest_available < expiry or pending_reason in {"PRICE_NOT_AVAILABLE", "PRICE_NOT_AVAILABLE_BY_CUTOFF", "PRICE_AVAILABILITY_INVALID"}):
                outcome = Outcome.PENDING
            else:
                outcome = Outcome.INDETERMINATE
            assumptions["entry_point_id"] = entry.point.identity
            return self._result(sim_id=sim_id, signal_id=str(signal_id) if signal_id is not None else None, simulation_type=simulation_type, horizon=horizon, direction=direction, detected=detected, expiry=expiry, entry=entry, final=None, outcome=outcome, reason=pending_reason, assumptions=assumptions, net=None)
        assumptions.update({"final_market_ts": iso_ts(final.point.timestamp), "final_available_ts": iso_ts(final.point.available_ts), "final_point_id": final.point.identity})
        delta = final.point.price - entry.point.price
        if abs(delta) <= self.spec.tie_tolerance:
            outcome = Outcome.TIE
        elif (direction == "UP" and delta > 0) or (direction == "DOWN" and delta < 0):
            outcome = Outcome.WIN
        else:
            outcome = Outcome.LOSS
        return self._result(sim_id=sim_id, signal_id=str(signal_id) if signal_id is not None else None, simulation_type=simulation_type, horizon=horizon, direction=direction, detected=detected, expiry=expiry, entry=entry, final=final, outcome=outcome, reason=None, assumptions=assumptions, net=self.net_result(outcome))

    def evaluate(self, signal: Any, points: Iterable[Any], *, horizon_seconds: float | None = None, simulation_id: str | None = None, simulation_type: str = "DIRECTIONAL", data_complete: bool | None = True, as_of: datetime | None = None, capture_complete: bool | None = None) -> SimulationResult:
        return self.evaluate_prepared(signal, normalize_points(points), horizon_seconds=horizon_seconds, simulation_id=simulation_id, simulation_type=simulation_type, data_complete=data_complete, as_of=as_of, capture_complete=capture_complete)

    def net_result(self, outcome: Outcome) -> float | None:
        if outcome in {Outcome.INDETERMINATE, Outcome.PENDING}:
            return None
        if outcome is Outcome.WIN:
            return self.spec.stake * self.spec.payout_net - self.spec.costs
        if outcome is Outcome.LOSS:
            return -self.spec.stake * self.spec.loss_amount - self.spec.costs
        return self.spec.tie_net - self.spec.costs

    def evaluate_all(self, signal: Any, points: Iterable[Any], *, signal_id_prefix: str | None = None, simulation_type: str = "DIRECTIONAL", data_complete: bool | None = True, as_of: datetime | None = None, capture_complete: bool | None = None) -> list[SimulationResult]:
        prepared = normalize_points(points)
        row = _record(signal)
        prefix = signal_id_prefix or str(row.get("signal_id", row.get("id", "signal")))
        return [self.evaluate_prepared(row, prepared, horizon_seconds=horizon, simulation_id=f"{prefix}:{horizon:g}:{simulation_type}", simulation_type=simulation_type, data_complete=data_complete, as_of=as_of, capture_complete=capture_complete) for horizon in self.spec.horizons_seconds]


class VirtualContract:
    """Contrato UP/DOWN virtual; nunca envía órdenes."""
    def __init__(self, *, stake: float = 1.0, payout_net: float = 0.80, loss_amount: float = 1.0, tie_net: float = 0.0, costs: float = 0.0, tie_tolerance: float = 0.0):
        self.stake = float(stake); self.payout_net = float(payout_net); self.loss_amount = float(loss_amount); self.tie_net = float(tie_net); self.costs = float(costs); self.tie_tolerance = float(tie_tolerance)
        if self.stake <= 0 or self.payout_net < 0 or self.loss_amount < 0 or self.tie_tolerance < 0 or self.costs < 0:
            raise ValueError("invalid virtual contract amounts")

    @property
    def break_even(self) -> float:
        return break_even_probability(payout_net=self.payout_net, loss_amount=self.loss_amount, costs=self.costs, stake=self.stake)

    def evaluator(self, **kwargs: Any) -> DirectionalEvaluator:
        return DirectionalEvaluator(EvaluationSpec(stake=self.stake, payout_net=self.payout_net, loss_amount=self.loss_amount, tie_net=self.tie_net, costs=self.costs, tie_tolerance=self.tie_tolerance, **kwargs))

    def settle(self, result: SimulationResult) -> SimulationResult:
        if result.outcome in {Outcome.INDETERMINATE, Outcome.PENDING}:
            net = None
        elif result.outcome is Outcome.WIN:
            net = self.stake * self.payout_net - self.costs
        elif result.outcome is Outcome.LOSS:
            net = -self.stake * self.loss_amount - self.costs
        else:
            net = self.tie_net - self.costs
        return dataclasses.replace(result, stake=self.stake, net_result=net, assumptions={**result.assumptions, "virtual_contract": {"stake": self.stake, "payout_net": self.payout_net, "loss_amount": self.loss_amount, "tie_net": self.tie_net, "costs": self.costs}})


class VirtualContractSimulator:
    def __init__(self, contract: VirtualContract | EvaluationSpec | None = None, spec: EvaluationSpec | None = None):
        if isinstance(contract, EvaluationSpec) and spec is None:
            spec = contract; contract = None
        if contract is None and spec is not None:
            contract = VirtualContract(stake=spec.stake, payout_net=spec.payout_net, loss_amount=spec.loss_amount, tie_net=spec.tie_net, costs=spec.costs, tie_tolerance=spec.tie_tolerance)
        self.contract = contract or VirtualContract()
        self.spec = spec or EvaluationSpec(stake=self.contract.stake, payout_net=self.contract.payout_net, loss_amount=self.contract.loss_amount, tie_net=self.contract.tie_net, costs=self.contract.costs, tie_tolerance=self.contract.tie_tolerance)
        # Dos descripciones distintas del mismo contrato serían ambiguas.
        if contract is not None and spec is not None:
            values = (self.contract.stake, self.contract.payout_net, self.contract.loss_amount, self.contract.tie_net, self.contract.costs, self.contract.tie_tolerance)
            expected = (self.spec.stake, self.spec.payout_net, self.spec.loss_amount, self.spec.tie_net, self.spec.costs, self.spec.tie_tolerance)
            if values != expected:
                raise ValueError("VirtualContract y EvaluationSpec no son equivalentes")
        self.evaluator = DirectionalEvaluator(self.spec)

    def evaluate(self, signal: Any, points: Iterable[Any], *, horizon_seconds: float | None = None, simulation_id: str | None = None, data_complete: bool | None = True, as_of: datetime | None = None, capture_complete: bool | None = None) -> SimulationResult:
        result = self.evaluator.evaluate(signal, points, horizon_seconds=horizon_seconds, simulation_id=simulation_id, simulation_type="VIRTUAL_CONTRACT", data_complete=data_complete, as_of=as_of, capture_complete=capture_complete)
        return self.contract.settle(result)

    def evaluate_prepared(self, signal: Any, points: Sequence[PricePoint], *, horizon_seconds: float | None = None, simulation_id: str | None = None, data_complete: bool | None = True, as_of: datetime | None = None, capture_complete: bool | None = None) -> SimulationResult:
        result = self.evaluator.evaluate_prepared(signal, points, horizon_seconds=horizon_seconds, simulation_id=simulation_id, simulation_type="VIRTUAL_CONTRACT", data_complete=data_complete, as_of=as_of, capture_complete=capture_complete)
        return self.contract.settle(result)

    def evaluate_all(self, signal: Any, points: Iterable[Any], *, data_complete: bool | None = True, as_of: datetime | None = None, capture_complete: bool | None = None) -> list[SimulationResult]:
        prepared = normalize_points(points)
        return [self.contract.settle(result) for result in self.evaluator.evaluate_all(signal, prepared, simulation_type="VIRTUAL_CONTRACT", data_complete=data_complete, as_of=as_of, capture_complete=capture_complete)]


__all__ = [
    "DirectionalEvaluator", "EvaluationSpec", "Outcome", "PricePoint", "Selection",
    "SimulationResult", "VirtualContract", "VirtualContractSimulator",
    "PRICE_BASE_ALIASES", "VALID_PRICE_BASES", "normalize_price_base",
    "price_bases_match", "normalize_completion", "outcome_for_missing_price",
    "normalize_observation_times", "observation_age_seconds",
    "quality_label_is_usable", "select_price_point",
    "break_even_probability", "iso_ts", "normalize_points", "parse_ts",
]
