"""Causal, virtual-only evaluation of directional signals.

There is intentionally no order/execution connector in this module.  A
simulation chooses the first usable observation at or after an explicit target
time, applies a configured latency and age bound, and records when the target
could not be resolved.  It never searches for a favorable price in a future
window.
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
    INDETERMINATE = "INDETERMINATE"


def _record(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        # asdict() deep-copies mappingproxy metadata used by core Candle and
        # puede fallar; una copia superficial conserva los campos auditables.
        return {field.name: getattr(value, field.name) for field in dataclasses.fields(value)}
    if hasattr(value, "model_dump"):
        return dict(value.model_dump())
    if hasattr(value, "__dict__"):
        return {k: v for k, v in vars(value).items() if not k.startswith("_")}
    raise TypeError(f"expected a mapping/dataclass/object, got {type(value)!r}")


def parse_ts(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(float(value), UTC)
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def iso_ts(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


@dataclasses.dataclass(frozen=True, slots=True)
class PricePoint:
    """One observed price with timing/provenance needed for causal replay."""

    timestamp: datetime
    price: float
    available_at: datetime | None = None
    source: str = "unknown"
    base_price: str = "unknown"
    quality: str = "UNKNOWN"
    resolution: str = "UNKNOWN"
    closed: bool = True
    source_ordinal: int = 0

    def __post_init__(self) -> None:
        if not math.isfinite(float(self.price)):
            raise ValueError("price must be finite")

    @property
    def available_ts(self) -> datetime:
        return self.available_at or self.timestamp

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": iso_ts(self.timestamp),
            "price": float(self.price),
            "available_at": iso_ts(self.available_ts),
            "source": self.source,
            "base_price": self.base_price,
            "quality": self.quality,
            "resolution": self.resolution,
            "closed": self.closed,
            "source_ordinal": self.source_ordinal,
        }


def _extract_point(value: Any, ordinal: int) -> PricePoint:
    row = _record(value)
    # ``timestamp`` means an event time; candle rows use end_ts/close_ts so
    # that a closed candle cannot be used before its interval is over.
    end = row.get("end_ts", row.get("interval_end", row.get("end", row.get("close_ts", row.get("close_time")))))
    timestamp_value = end if end is not None else row.get("event_ts", row.get("event_time", row.get("interval_start", row.get("timestamp", row.get("ts", row.get("time"))))))
    if timestamp_value is None:
        raise ValueError("price point has no timestamp")
    timestamp = parse_ts(timestamp_value)
    available_value = row.get("available_ts", row.get("available_at", row.get("received_ts", row.get("received_at"))))
    available = parse_ts(available_value) if available_value is not None else timestamp
    # Explicitly select one configured/declared price base.  The caller may
    # request a different base, but this adapter never silently swaps bid/ask.
    raw_base = row.get("price_base", row.get("price_basis", row.get("base_price", row.get("price_type", ""))))
    if hasattr(raw_base, "value"):
        raw_base = raw_base.value
    base = str(raw_base or "").lower()
    if "price" in row and row["price"] is not None:
        price = row["price"]
        base = base or "trade"
    elif "mid" in row and row["mid"] is not None:
        price = row["mid"]
        base = base or "mid"
    elif "close" in row and row["close"] is not None:
        price = row["close"]
        base = base or "close"
    else:
        raise ValueError("price point has no explicit price/close/mid value")
    return PricePoint(
        timestamp=timestamp,
        price=float(price),
        available_at=available,
        source=str(row.get("source", row.get("provider", "unknown"))),
        base_price=base or "unknown",
        quality=str(row.get("quality", row.get("data_quality", "UNKNOWN"))),
        resolution=str(row.get("resolution", row.get("timeframe", row.get("interval", "UNKNOWN")))),
        closed=bool(row.get("closed", row.get("is_closed", True))),
        source_ordinal=int(row.get("source_ordinal", row.get("ordinal", ordinal))),
    )


def normalize_points(points: Iterable[Any]) -> list[PricePoint]:
    result: list[PricePoint] = []
    for ordinal, point in enumerate(points):
        result.append(_extract_point(point, ordinal))
    # Stable sorting ensures same-timestamp duplicates are resolved by source
    # order, not by the price (which would be a retrospective selection).
    result.sort(key=lambda p: (p.timestamp, p.source_ordinal))
    return result


@dataclasses.dataclass(frozen=True, slots=True)
class EvaluationSpec:
    """Explicit assumptions for directional and contract simulations."""

    horizons_seconds: tuple[float, ...] = (60.0, 180.0, 300.0)
    entry_latency_seconds: float = 1.0
    stake: float = 1.0
    max_price_age_seconds: float = 120.0
    tie_tolerance: float = 0.0
    payout_net: float = 0.80
    loss_amount: float = 1.0
    tie_net: float = 0.0
    costs: float = 0.0
    horizon_from: str = "entry"  # ``entry`` avoids using detection close as entry.
    requested_base_price: str | None = None
    require_closed: bool = True

    def __post_init__(self) -> None:
        horizons = tuple(float(x) for x in self.horizons_seconds)
        if not horizons or any(x <= 0 or not math.isfinite(x) for x in horizons):
            raise ValueError("horizons_seconds must contain positive finite values")
        object.__setattr__(self, "horizons_seconds", horizons)
        for name in ("entry_latency_seconds", "stake", "max_price_age_seconds", "tie_tolerance", "payout_net", "loss_amount", "tie_net", "costs"):
            value = float(getattr(self, name))
            if not math.isfinite(value) or (name in {"entry_latency_seconds", "stake", "max_price_age_seconds", "tie_tolerance", "payout_net", "loss_amount", "costs"} and value < 0):
                raise ValueError(f"{name} must be a finite non-negative number")
            object.__setattr__(self, name, value)
        if self.horizon_from not in {"entry", "detection"}:
            raise ValueError("horizon_from must be 'entry' or 'detection'")
        if self.stake <= 0:
            raise ValueError("stake must be greater than zero")
        if self.requested_base_price is not None:
            object.__setattr__(self, "requested_base_price", str(self.requested_base_price).lower())

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


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

    def to_dict(self) -> dict[str, Any]:
        result = dataclasses.asdict(self)
        result["outcome"] = self.outcome.value
        return result


def break_even_probability(*, payout_net: float = 0.80, costs: float = 0.0, stake: float = 1.0) -> float:
    """Return p for zero expected net result when there are no ties.

    With one unit risked, net win ``payout_net`` and explicit costs ``c``,
    ``p = (stake + c) / (stake + payout_net)``.  For zero costs this is the
    requested ``1 / (1 + payout_net)``.
    """

    payout_net = float(payout_net); costs = float(costs); stake = float(stake)
    if payout_net < 0 or costs < 0 or stake <= 0:
        raise ValueError("payout_net/costs must be non-negative and stake positive")
    denominator = stake + payout_net
    return (stake + costs) / denominator if denominator else math.inf


class DirectionalEvaluator:
    """Evaluate signal direction using first-observation causal selection."""

    def __init__(self, spec: EvaluationSpec | None = None):
        self.spec = spec or EvaluationSpec()

    def _select(
        self,
        points: Sequence[PricePoint],
        target: datetime,
        *,
        requested_base_price: str | None = None,
    ) -> tuple[PricePoint | None, str | None]:
        for point in points:
            if point.timestamp < target:
                continue
            if self.spec.require_closed and not point.closed:
                continue
            if requested_base_price is not None:
                wanted = requested_base_price.lower()
                actual = point.base_price.lower()
                # A normalized OHLC row has a traded-price basis, while the
                # simulation may explicitly request its close. This is not a
                # bid/ask/mid substitution: it is the declared close field of
                # the same traded OHLC bar.
                close_alias = wanted == "close" and actual in {"close", "traded", "trade"}
                trade_alias = wanted in {"trade", "traded"} and actual in {"close", "traded", "trade"}
                if actual != wanted and not close_alias and not trade_alias:
                    continue
            age = (point.timestamp - target).total_seconds()
            if age > self.spec.max_price_age_seconds:
                return None, "MAX_PRICE_AGE_EXCEEDED"
            if point.available_ts > point.timestamp + timedelta(seconds=self.spec.max_price_age_seconds):
                return None, "PRICE_AVAILABILITY_INVALID"
            return point, None
        return None, "PRICE_NOT_AVAILABLE"

    def evaluate(
        self,
        signal: Any,
        points: Iterable[Any],
        *,
        horizon_seconds: float | None = None,
        simulation_id: str | None = None,
        simulation_type: str = "DIRECTIONAL",
    ) -> SimulationResult:
        signal_row = _record(signal)
        normalized = normalize_points(points)
        horizon = float(horizon_seconds if horizon_seconds is not None else self.spec.horizons_seconds[0])
        if horizon <= 0:
            raise ValueError("horizon_seconds must be positive")
        direction = str(signal_row.get("direction", signal_row.get("side", ""))).upper()
        if direction not in {"UP", "DOWN", "LONG", "SHORT", "BUY", "SELL"}:
            direction = "UP" if direction in {"BULL", "ALCISTA"} else "DOWN" if direction in {"BEAR", "BAJISTA"} else direction
        if direction == "LONG": direction = "UP"
        if direction == "SHORT": direction = "DOWN"
        detected_value = signal_row.get("detected_ts", signal_row.get("detected_at", signal_row.get("timestamp", signal_row.get("ts"))))
        if detected_value is None:
            raise ValueError("signal has no detected timestamp")
        detected = parse_ts(detected_value)
        entry_target = detected + timedelta(seconds=self.spec.entry_latency_seconds)
        expiry = (entry_target if self.spec.horizon_from == "entry" else detected) + timedelta(seconds=horizon)
        sim_id = simulation_id or f"{signal_row.get('signal_id', signal_row.get('id', 'signal'))}:{horizon:g}:{simulation_type}"
        assumptions = {
            **self.spec.to_dict(),
            "selection_rule": "first_observation_at_or_after_target",
            "target_entry_ts": iso_ts(entry_target),
            "target_expiry_ts": iso_ts(expiry),
            "no_favorable_lookahead": True,
        }
        base = self.spec.requested_base_price or "unknown"
        if not normalized:
            return SimulationResult(str(sim_id), signal_row.get("signal_id"), simulation_type, horizon, direction, iso_ts(detected), None, iso_ts(expiry), None, None, Outcome.INDETERMINATE, self.spec.stake, None, base, "UNKNOWN", "UNKNOWN", assumptions, "NO_PRICE_POINTS")
        entry, reason = self._select(normalized, entry_target, requested_base_price=self.spec.requested_base_price)
        if entry is None:
            return SimulationResult(str(sim_id), signal_row.get("signal_id"), simulation_type, horizon, direction, iso_ts(detected), None, iso_ts(expiry), None, None, Outcome.INDETERMINATE, self.spec.stake, None, base, "UNKNOWN", "UNKNOWN", assumptions, reason)
        final, reason = self._select(normalized, expiry, requested_base_price=self.spec.requested_base_price)
        if final is None:
            return SimulationResult(str(sim_id), signal_row.get("signal_id"), simulation_type, horizon, direction, iso_ts(detected), iso_ts(entry.timestamp), iso_ts(expiry), entry.price, None, Outcome.INDETERMINATE, self.spec.stake, None, entry.base_price, entry.quality, entry.resolution, assumptions, reason)
        delta = final.price - entry.price
        tolerance = self.spec.tie_tolerance
        if abs(delta) <= tolerance:
            outcome = Outcome.TIE
        elif (direction == "UP" and delta > 0) or (direction == "DOWN" and delta < 0):
            outcome = Outcome.WIN
        else:
            outcome = Outcome.LOSS
        net = self.net_result(outcome)
        assumptions["selected_entry_rule"] = "first_observation_at_or_after_target"
        assumptions["selected_final_rule"] = "first_observation_at_or_after_expiry"
        return SimulationResult(str(sim_id), signal_row.get("signal_id"), simulation_type, horizon, direction, iso_ts(detected), iso_ts(entry.timestamp), iso_ts(expiry), entry.price, final.price, outcome, self.spec.stake, net, entry.base_price, entry.quality if entry.quality != "UNKNOWN" else final.quality, entry.resolution if entry.resolution != "UNKNOWN" else final.resolution, assumptions)

    def net_result(self, outcome: Outcome) -> float | None:
        if outcome is Outcome.INDETERMINATE:
            return None
        if outcome is Outcome.WIN:
            return self.spec.stake * self.spec.payout_net - self.spec.costs
        if outcome is Outcome.LOSS:
            return -self.spec.stake * self.spec.loss_amount - self.spec.costs
        return self.spec.tie_net - self.spec.costs

    def evaluate_all(self, signal: Any, points: Iterable[Any], *, signal_id_prefix: str | None = None) -> list[SimulationResult]:
        points_list = list(points)
        signal_row = _record(signal)
        prefix = signal_id_prefix or str(signal_row.get("signal_id", signal_row.get("id", "signal")))
        return [self.evaluate(signal_row, points_list, horizon_seconds=h, simulation_id=f"{prefix}:{h:g}:DIRECTIONAL") for h in self.spec.horizons_seconds]


class VirtualContract:
    """UP/DOWN virtual contract assumptions, never connected to a broker."""

    def __init__(
        self,
        *,
        stake: float = 1.0,
        payout_net: float = 0.80,
        loss_amount: float = 1.0,
        tie_net: float = 0.0,
        costs: float = 0.0,
        tie_tolerance: float = 0.0,
    ):
        self.stake = float(stake); self.payout_net = float(payout_net); self.loss_amount = float(loss_amount); self.tie_net = float(tie_net); self.costs = float(costs); self.tie_tolerance = float(tie_tolerance)
        if self.stake <= 0 or self.payout_net < 0 or self.loss_amount < 0 or self.tie_tolerance < 0 or self.costs < 0:
            raise ValueError("invalid virtual contract amounts")

    @property
    def break_even(self) -> float:
        return break_even_probability(payout_net=self.payout_net, costs=self.costs, stake=self.stake)

    def evaluator(self, **kwargs: Any) -> DirectionalEvaluator:
        return DirectionalEvaluator(EvaluationSpec(stake=self.stake, payout_net=self.payout_net, loss_amount=self.loss_amount, tie_net=self.tie_net, costs=self.costs, tie_tolerance=self.tie_tolerance, **kwargs))

    def settle(self, result: SimulationResult) -> SimulationResult:
        # Recompute net from the contract so a result cannot claim a different
        # payout than the explicitly configured assumptions.
        net = None if result.outcome is Outcome.INDETERMINATE else (
            self.stake * self.payout_net - self.costs if result.outcome is Outcome.WIN else
            -self.stake * self.loss_amount - self.costs if result.outcome is Outcome.LOSS else
            self.tie_net - self.costs
        )
        return dataclasses.replace(result, stake=self.stake, net_result=net, assumptions={**result.assumptions, "virtual_contract": {"stake": self.stake, "payout_net": self.payout_net, "loss_amount": self.loss_amount, "tie_net": self.tie_net, "costs": self.costs}})


class VirtualContractSimulator:
    def __init__(self, contract: VirtualContract | EvaluationSpec | None = None, spec: EvaluationSpec | None = None):
        # Accept both VirtualContractSimulator(spec=...) and the convenient
        # positional VirtualContractSimulator(EvaluationSpec(...)) form.
        if isinstance(contract, EvaluationSpec) and spec is None:
            spec = contract
            contract = None
        if contract is None and spec is not None:
            # Un contrato no especificado hereda exactamente los supuestos de
            # EvaluationSpec; evita liquidar con el payout por defecto cuando
            # el TOML define otro pago, pérdida o coste.
            contract = VirtualContract(stake=spec.stake, payout_net=spec.payout_net, loss_amount=spec.loss_amount, tie_net=spec.tie_net, costs=spec.costs, tie_tolerance=spec.tie_tolerance)
        self.contract = contract or VirtualContract()
        self.spec = spec or EvaluationSpec(stake=self.contract.stake, payout_net=self.contract.payout_net, loss_amount=self.contract.loss_amount, tie_net=self.contract.tie_net, costs=self.contract.costs, tie_tolerance=self.contract.tie_tolerance)
        self.evaluator = DirectionalEvaluator(self.spec)

    def evaluate(self, signal: Any, points: Iterable[Any], *, horizon_seconds: float | None = None, simulation_id: str | None = None) -> SimulationResult:
        return self.contract.settle(self.evaluator.evaluate(signal, points, horizon_seconds=horizon_seconds, simulation_id=simulation_id, simulation_type="VIRTUAL_CONTRACT"))

    def evaluate_all(self, signal: Any, points: Iterable[Any]) -> list[SimulationResult]:
        return [self.contract.settle(x) for x in self.evaluator.evaluate_all(signal, points)]
