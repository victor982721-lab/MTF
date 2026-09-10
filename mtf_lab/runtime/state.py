"""Estado serializable usado por el procesador incremental.

Este módulo no abre conexiones ni conoce un proveedor.  Acepta los modelos
canónicos de ``mtf_lab.core`` y también los registros equivalentes del módulo
``mtf_lab.data`` mediante adaptación por atributos, conservando la base de
precio declarada.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from typing import Any, Iterable, Mapping, Sequence

from ..core import (
    Candle,
    ConditionResult,
    ConditionState,
    DataQuality,
    DecisionKind,
    Evaluation,
    EventKind,
    IndicatorConfig,
    IndicatorPoint,
    IndicatorSeries,
    MarketEvent,
    OperationMode,
    PriceBase,
    PreparationEpisode,
    Signal,
    StrategyConfig,
    Timeframe,
    parse_timeframe,
)
from ..core.aggregation import _Bucket  # type: ignore[attr-defined]


UTC = timezone.utc


def utc(value: datetime | str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        result = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        result = datetime.fromisoformat(text)
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("Los timestamps del runtime deben incluir zona horaria")
    return result.astimezone(UTC)


def iso(value: datetime | None) -> str | None:
    return utc(value).isoformat().replace("+00:00", "Z") if value is not None else None


def _attr(record: Any, *names: str, default: Any = None) -> Any:
    if isinstance(record, Mapping):
        for name in names:
            if name in record and record[name] is not None:
                return record[name]
        return default
    for name in names:
        value = getattr(record, name, None)
        if value is not None:
            return value
    return default


def mode_from(value: Any, *, synthetic: bool = False) -> OperationMode:
    if isinstance(value, OperationMode):
        return value
    if value is None or str(value).strip() == "":
        return OperationMode.SYNTHETIC if synthetic else OperationMode.REPLAY
    text = str(value).strip().upper()
    if text in {"LIVE", "OBSERVATION_EN_DIRECTO", "OBSERVACIÓN EN DIRECTO", "OBSERVACION_EN_DIRECTO"}:
        return OperationMode.LIVE
    if text in {"SYNTHETIC", "SINTETICO", "SINTÉTICO"}:
        return OperationMode.SYNTHETIC
    if text == "REPLAY":
        return OperationMode.REPLAY
    raise ValueError(f"modo desconocido: {value!r}")


def quality_from(value: Any, *, synthetic: bool = False) -> DataQuality:
    if isinstance(value, DataQuality):
        return value
    # The provider/data layer has a compact DataQuality record with a boolean
    # ``valid`` field rather than the core flag set. Preserve that public
    # label instead of turning the object into the string ``DataQuality(...)``.
    if hasattr(value, "valid") and not isinstance(value, (str, bytes, Mapping)):
        result = DataQuality.good(synthetic=synthetic)
        if not bool(getattr(value, "valid")):
            result = result.with_flags("invalid", reason="source_quality_invalid")
        return result
    result = DataQuality.good(synthetic=synthetic)
    if value is None:
        return result
    if isinstance(value, Mapping):
        raw_flags = value.get("flags", ())
        if isinstance(raw_flags, str):
            raw_flags = (raw_flags,)
        reasons = tuple(str(item) for item in value.get("reasons", ()))
        for raw in raw_flags:
            try:
                result = result.with_flags(str(raw).lower(), reason="")
            except ValueError:
                result = result.with_flags("invalid", reason=f"unknown_quality_flag:{raw}")
        status = str(value.get("status", "")).strip().lower()
        if status and status not in {"valid", "validated", "ok", "good", "synthetic", "synthetic_valid", "synthetic_validated", "data_quality_validated", "closed_valid"}:
            try:
                result = result.with_flags(status, reason=status)
            except ValueError:
                result = result.with_flags("invalid", reason=status)
        return DataQuality(result.flags, reasons or result.reasons, result.source)
    raw = str(value).strip().lower()
    if raw in {"", "valid", "validated", "ok", "good", "synthetic", "synthetic_validated", "synthetic_valid", "valid_data", "data_quality_validated", "public_provider_closed", "closed_valid", "validated_local"}:
        return result
    try:
        return result.with_flags(raw, reason=raw)
    except ValueError:
        return result.with_flags("invalid", reason=raw)


def quality_label_is_usable(value: Any) -> bool:
    """Return whether a public quality label is safe for virtual evaluation.

    Adapters commonly expose labels such as ``VALID``,
    ``SYNTHETIC_VALIDATED`` or ``DATA_QUALITY_VALIDATED``. Unknown labels,
    gaps, stale/disconnected states and anomaly labels remain blocked rather
    than being accepted because they happen to contain the word ``valid``.
    """

    if isinstance(value, DataQuality):
        return bool(value.valid)
    if hasattr(value, "valid") and not isinstance(value, (str, bytes, Mapping)):
        return bool(getattr(value, "valid"))
    label = str(value or "").strip().upper().replace("-", "_").replace(" ", "_")
    if not label:
        return False
    blocked_tokens = ("INVALID", "UNKNOWN", "DISCONNECTED", "STALE", "GAP", "PARTIAL", "OPEN", "UNRECONCILED", "OUT_OF_ORDER", "DUPLICATE", "LATE", "ANOM")
    if any(token in label for token in blocked_tokens):
        return False
    return label in {
        "VALID", "VALIDATED", "OK", "GOOD", "SYNTHETIC", "SYNTHETIC_VALID", "SYNTHETIC_VALIDATED", "VALID_DATA", "DATA_QUALITY_VALIDATED", "CLOSED_VALID",
    } or label.startswith("VALID_") or label.startswith("SYNTHETIC_VALID")


def _price_base(value: Any, default: PriceBase | None = None) -> PriceBase:
    if isinstance(value, PriceBase):
        return value
    if value is None or str(value).strip() == "":
        if default is None:
            raise ValueError("base de precio ausente")
        return default
    text = str(value).strip().lower()
    if text in {"trade", "traded", "close"}:
        return PriceBase.TRADED
    return PriceBase(text)


def _event_kind(value: Any) -> EventKind:
    if isinstance(value, EventKind):
        return value
    text = str(value).strip().lower()
    try:
        return EventKind(text)
    except ValueError as exc:
        raise ValueError(f"event_kind desconocido: {value!r}") from exc


def _strict_bool(value: Any, *, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "si", "sí"}:
        return True
    if text in {"0", "false", "no"}:
        return False
    raise ValueError(f"booleano inválido: {value!r}")


def to_core_event(record: Any, *, mode: OperationMode | str = OperationMode.REPLAY) -> MarketEvent:
    """Normaliza un Event de cualquiera de los dos schemas locales."""

    if isinstance(record, MarketEvent):
        return record
    # Data-layer records cross through the explicit translation boundary so
    # flags, reasons, provenance and stable identities are not reduced to a
    # generic ``good`` value at runtime.
    if record.__class__.__module__.startswith("mtf_lab.data") and record.__class__.__name__ == "Event":
        from ..data.translation import data_event_to_core
        translated = data_event_to_core(record)
        metadata = getattr(record, "metadata", {}) or {}
        provenance = metadata.get("provenance") if isinstance(metadata, Mapping) else None
        explicit_mode = provenance.get("mode") if isinstance(provenance, Mapping) else (metadata.get("mode") if isinstance(metadata, Mapping) else None)
        if explicit_mode is None and not bool(getattr(record, "synthetic", False)):
            target_mode = mode_from(mode)
            metadata = dict(translated.metadata)
            provenance = metadata.get("provenance")
            if isinstance(provenance, Mapping):
                provenance = dict(provenance); provenance["mode"] = "LIVE" if target_mode is OperationMode.LIVE else target_mode.value; metadata["provenance"] = provenance
            translated = replace(translated, mode=target_mode, metadata=metadata)
        return translated
    instrument = str(_attr(record, "instrument", "symbol", default="unknown"))
    event_time = utc(_attr(record, "event_time", "event_ts", "timestamp", "ts"))
    if event_time is None:
        raise ValueError("evento sin event_time")
    basis = _price_base(_attr(record, "price_base", "price_basis", "price_type", default=None))
    raw_price = _attr(record, "price", default=None)
    bid = _attr(record, "bid", default=None)
    ask = _attr(record, "ask", default=None)
    mid = _attr(record, "mid", default=None)
    if basis is PriceBase.BID and bid is None:
        raise ValueError("evento BID sin bid explícito")
    if basis is PriceBase.ASK and ask is None:
        raise ValueError("evento ASK sin ask explícito")
    if basis is PriceBase.MID and (mid is None or bid is None or ask is None):
        raise ValueError("evento MID requiere mid, bid y ask explícitos")
    metadata = _attr(record, "metadata", default={}) or {}
    synthetic = _strict_bool(_attr(record, "synthetic", "is_synthetic", default=False), default=False)
    event_id = _attr(record, "event_id", "data_id", default=None)
    if callable(event_id):
        event_id = event_id()
    return MarketEvent(
        instrument=instrument,
        event_time=event_time,
        price=raw_price if basis is PriceBase.TRADED else None,
        quantity=_attr(record, "quantity", default=None),
        bid=bid,
        ask=ask,
        received_at=utc(_attr(record, "received_at", "received_ts", default=None)),
        available_at=utc(_attr(record, "available_at", "available_ts", default=None)),
        source=str(_attr(record, "source", "provider", default="unknown")),
        mode=mode_from(_attr(record, "mode", default=mode), synthetic=synthetic),
        price_base=basis,
        event_kind=_event_kind(_attr(record, "event_kind", "kind", default="trade")),
        sequence=_attr(record, "sequence", "source_sequence", default=None),
        source_event_id=_attr(record, "source_event_id", default=None),
        event_id=event_id,
        quality=quality_from(_attr(record, "quality", default=None), synthetic=synthetic),
        metadata=metadata,
    )


def to_core_candle(record: Any, *, mode: OperationMode | str = OperationMode.REPLAY) -> Candle:
    """Normaliza una vela nativa o derivada al modelo del núcleo."""

    if isinstance(record, Candle):
        return record
    if record.__class__.__module__.startswith("mtf_lab.data") and record.__class__.__name__ == "Bar":
        from ..data.translation import data_bar_to_core
        translated = data_bar_to_core(record)
        metadata = getattr(record, "metadata", {}) or {}
        provenance = metadata.get("provenance") if isinstance(metadata, Mapping) else None
        explicit_mode = provenance.get("mode") if isinstance(provenance, Mapping) else (metadata.get("mode") if isinstance(metadata, Mapping) else None)
        if explicit_mode is None and not bool(getattr(record, "synthetic", False)):
            target_mode = mode_from(mode)
            metadata = dict(translated.metadata)
            provenance = metadata.get("provenance")
            if isinstance(provenance, Mapping):
                provenance = dict(provenance); provenance["mode"] = "LIVE" if target_mode is OperationMode.LIVE else target_mode.value; metadata["provenance"] = provenance
            translated = replace(translated, mode=target_mode, metadata=metadata)
        return translated
    start = utc(_attr(record, "start", "interval_start", "start_ts"))
    end = utc(_attr(record, "end", "interval_end", "end_ts"))
    if start is None or end is None:
        raise ValueError("vela sin inicio/fin")
    raw_tf = _attr(record, "timeframe", "resolution", "resolution_seconds", default="M1")
    tf = parse_timeframe(raw_tf)
    synthetic = _strict_bool(_attr(record, "synthetic", "is_synthetic", default=False), default=False)
    raw_base = _attr(record, "price_base", "price_basis", default=None)
    if raw_base is None:
        raise ValueError("vela sin base de precio explícita")
    return Candle(
        instrument=str(_attr(record, "instrument", "symbol", default="unknown")),
        timeframe=tf,
        start=start,
        end=end,
        open=float(_attr(record, "open")),
        high=float(_attr(record, "high")),
        low=float(_attr(record, "low")),
        close=float(_attr(record, "close")),
        volume=float(_attr(record, "volume", default=0.0) or 0.0),
        event_count=int(_attr(record, "event_count", "trade_count", default=0) or 0),
        source=str(_attr(record, "source", "provider", default="unknown")),
        mode=mode_from(_attr(record, "mode", default=mode), synthetic=synthetic),
        price_base=_price_base(raw_base),
        closed=_strict_bool(_attr(record, "closed", "is_closed", default=True)),
        available_at=utc(_attr(record, "available_at", "available_ts", default=None)),
        received_at=utc(_attr(record, "received_at", "received_ts", default=None)),
        quality=quality_from(_attr(record, "quality", default=None), synthetic=synthetic),
        candle_id=str(_attr(record, "candle_id", "data_id", "source_record_id", default="") or "") or None,
        origin=str(_attr(record, "origin", default="native")),
        metadata={**(_attr(record, "metadata", default={}) or {}), **({"revision": int(_attr(record, "revision", default=0) or 0)} if _attr(record, "revision", default=None) is not None else {})},
    )


def quality_dict(value: DataQuality) -> dict[str, Any]:
    return {"status": value.status, "flags": sorted(flag.value for flag in value.flags), "reasons": list(value.reasons), "source": value.source}


def quality_from_dict(value: Mapping[str, Any] | None) -> DataQuality:
    return quality_from(value or {})


def event_dict(event: MarketEvent) -> dict[str, Any]:
    return {
        "instrument": event.instrument,
        "event_time": iso(event.event_time),
        "price": event.price,
        "quantity": event.quantity,
        "bid": event.bid,
        "ask": event.ask,
        "received_at": iso(event.received_at),
        "available_at": iso(event.available_at),
        "source": event.source,
        "mode": event.mode.value,
        "price_base": event.price_base.value,
        "event_kind": event.event_kind.value,
        "sequence": event.sequence,
        "source_event_id": event.source_event_id,
        "event_id": event.event_id,
        "quality": quality_dict(event.quality),
        "metadata": dict(event.metadata),
    }


def event_from_dict(value: Mapping[str, Any]) -> MarketEvent:
    return to_core_event(value, mode=value.get("mode", "REPLAY"))


def candle_dict(candle: Candle) -> dict[str, Any]:
    return {
        "instrument": candle.instrument,
        "timeframe": candle.timeframe.name,
        "start": iso(candle.start),
        "end": iso(candle.end),
        "open": candle.open,
        "high": candle.high,
        "low": candle.low,
        "close": candle.close,
        "volume": candle.volume,
        "event_count": candle.event_count,
        "source": candle.source,
        "mode": candle.mode.value,
        "price_base": candle.price_base.value,
        "closed": candle.closed,
        "available_at": iso(candle.available_at),
        "received_at": iso(candle.received_at),
        "quality": quality_dict(candle.quality),
        "candle_id": candle.candle_id,
        "origin": candle.origin,
        "metadata": dict(candle.metadata),
    }


def candle_from_dict(value: Mapping[str, Any]) -> Candle:
    return to_core_candle(value, mode=value.get("mode", "REPLAY"))


def condition_dict(condition: ConditionResult) -> dict[str, Any]:
    return condition.as_dict()


def evaluation_dict(evaluation: Evaluation) -> dict[str, Any]:
    return evaluation.as_dict()


def evaluation_from_dict(value: Mapping[str, Any]) -> Evaluation:
    conditions = tuple(
        ConditionResult(
            str(item["name"]),
            ConditionState(str(item["state"])),
            item.get("observed"),
            item.get("expected"),
            str(item.get("reason", "")),
            bool(item.get("mandatory", True)),
        )
        for item in value.get("conditions", ())
    )
    return Evaluation(
        timestamp=utc(value["timestamp"]) or datetime.fromtimestamp(0, UTC),
        instrument=str(value.get("instrument", "unknown")),
        stage=str(value.get("stage", "unknown")),
        decision=DecisionKind(str(value.get("decision", "blocked"))),
        direction=value.get("direction"),
        conditions=conditions,
        values=value.get("values", {}),
        reasons=tuple(str(item) for item in value.get("reasons", ())),
        episode_id=value.get("episode_id"),
        mode=mode_from(value.get("mode")),
        quality=quality_from_dict(value.get("quality")),
        available_at=utc(value.get("available_at")),
    )


def signal_dict(signal: Signal) -> dict[str, Any]:
    return signal.as_dict()


def signal_from_dict(value: Mapping[str, Any]) -> Signal:
    return Signal(
        signal_id=str(value["signal_id"]),
        instrument=str(value["instrument"]),
        direction=str(value["direction"]),
        detected_at=utc(value["detected_at"]) or datetime.fromtimestamp(0, UTC),
        context_start=utc(value["context_start"]) or datetime.fromtimestamp(0, UTC),
        preparation_start=utc(value["preparation_start"]) or datetime.fromtimestamp(0, UTC),
        trigger_start=utc(value["trigger_start"]) or datetime.fromtimestamp(0, UTC),
        trigger_end=utc(value["trigger_end"]) or datetime.fromtimestamp(0, UTC),
        episode_id=str(value["episode_id"]),
        values=value.get("values", {}),
        mode=mode_from(value.get("mode")),
        quality=quality_from_dict(value.get("quality")),
    )


@dataclass(frozen=True, slots=True)
class SimulationConfig:
    """Supuestos virtuales explícitos, sin conexión de ejecución."""

    horizons_seconds: tuple[float, ...] = (60.0, 180.0, 300.0)
    entry_latency_seconds: float = 1.0
    entry_rule: str = "first_observation_at_or_after"
    exit_rule: str = "last_observation_at_or_before"
    max_price_age_seconds: float = 120.0
    stake: float = 1.0
    payout_net: float = 0.80
    loss_amount: float = 1.0
    tie_net: float = 0.0
    tie_tolerance: float = 0.0
    costs: float = 0.0
    horizon_from: str = "entry"
    requested_base_price: str = "traded"
    resolution: str = "event"

    def __post_init__(self) -> None:
        horizons = tuple(float(value) for value in self.horizons_seconds)
        if not horizons or any(not math.isfinite(value) or value <= 0 for value in horizons):
            raise ValueError("horizons_seconds debe contener duraciones positivas")
        object.__setattr__(self, "horizons_seconds", horizons)
        for name in ("entry_latency_seconds", "max_price_age_seconds", "stake", "payout_net", "loss_amount", "tie_tolerance", "costs"):
            value = float(getattr(self, name))
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} debe ser finito y no negativo")
            object.__setattr__(self, name, value)
        if self.stake <= 0:
            raise ValueError("stake debe ser positivo")
        if self.horizon_from not in {"entry", "detection"}:
            raise ValueError("horizon_from debe ser entry o detection")
        if self.entry_rule != "first_observation_at_or_after":
            raise ValueError("entry_rule no soportada por runtime")
        if self.exit_rule not in {"last_observation_at_or_before", "first_observation_at_or_after"}:
            raise ValueError("exit_rule no soportada por runtime")
        object.__setattr__(self, "requested_base_price", _price_base(self.requested_base_price).value)

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any] | None) -> "SimulationConfig":
        if mapping is None:
            return cls()
        allowed = {"horizons_seconds", "horizons_minutes", "entry_latency_seconds", "entry_rule", "exit_rule", "max_price_age_seconds", "stake", "payout_net", "net_payout", "loss_amount", "tie_net", "tie_return", "tie_tolerance", "costs", "horizon_from", "requested_base_price", "resolution"}
        unknown = set(mapping) - allowed
        if unknown:
            raise ValueError(f"Claves desconocidas en simulation: {sorted(unknown)}")
        raw = dict(mapping)
        if "horizons_minutes" in raw:
            if "horizons_seconds" in raw:
                raise ValueError("horizons_minutes y horizons_seconds son incompatibles")
            raw["horizons_seconds"] = tuple(float(value) * 60.0 for value in raw.pop("horizons_minutes"))
        for alias, target in (("net_payout", "payout_net"), ("tie_return", "tie_net")):
            if alias in raw:
                if target in raw:
                    raise ValueError(f"{alias} y {target} son incompatibles")
                raw[target] = raw.pop(alias)
        return cls(**raw)

    def to_dict(self) -> dict[str, Any]:
        return {
            "horizons_seconds": list(self.horizons_seconds),
            "entry_latency_seconds": self.entry_latency_seconds,
            "entry_rule": self.entry_rule,
            "exit_rule": self.exit_rule,
            "max_price_age_seconds": self.max_price_age_seconds,
            "stake": self.stake,
            "payout_net": self.payout_net,
            "loss_amount": self.loss_amount,
            "tie_net": self.tie_net,
            "tie_tolerance": self.tie_tolerance,
            "costs": self.costs,
            "horizon_from": self.horizon_from,
            "requested_base_price": self.requested_base_price,
            "resolution": self.resolution,
        }


@dataclass(frozen=True, slots=True)
class PriceObservation:
    timestamp: datetime
    available_at: datetime
    price: float
    base_price: str
    source: str
    resolution: str
    closed: bool = True
    quality: str = "valid"

    def __post_init__(self) -> None:
        if self.timestamp.tzinfo is None or self.available_at.tzinfo is None:
            raise ValueError("PriceObservation requiere timestamps conscientes")
        if not math.isfinite(float(self.price)):
            raise ValueError("PriceObservation.price debe ser finito")

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": iso(self.timestamp),
            "available_at": iso(self.available_at),
            "price": self.price,
            "base_price": self.base_price,
            "source": self.source,
            "resolution": self.resolution,
            "closed": self.closed,
            "quality": self.quality,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "PriceObservation":
        return cls(
            timestamp=utc(value["timestamp"]) or datetime.fromtimestamp(0, UTC),
            available_at=utc(value["available_at"]) or datetime.fromtimestamp(0, UTC),
            price=float(value["price"]),
            base_price=str(value.get("base_price", "traded")),
            source=str(value.get("source", "unknown")),
            resolution=str(value.get("resolution", "unknown")),
            closed=bool(value.get("closed", True)),
            quality=str(value.get("quality", "valid")),
        )


@dataclass(frozen=True, slots=True)
class PendingSimulation:
    simulation_id: str
    signal_id: str
    instrument: str
    direction: str
    horizon_seconds: float
    detected_at: datetime
    entry_due_at: datetime
    expiry_at: datetime
    entry_rule: str = "first_observation_at_or_after"
    exit_rule: str = "last_observation_at_or_before"
    status: str = "PENDING"
    entry_at: datetime | None = None
    entry_price: float | None = None
    final_at: datetime | None = None
    final_price: float | None = None
    outcome: str | None = None
    net_result: float | None = None
    price_base: str = "traded"
    resolution: str = "event"
    quality: str = "valid"
    reason: str | None = None
    final_observation: PriceObservation | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "simulation_id": self.simulation_id,
            "signal_id": self.signal_id,
            "instrument": self.instrument,
            "direction": self.direction,
            "horizon_seconds": self.horizon_seconds,
            "detected_at": iso(self.detected_at),
            "entry_due_at": iso(self.entry_due_at),
            "expiry_at": iso(self.expiry_at),
            "entry_rule": self.entry_rule,
            "exit_rule": self.exit_rule,
            "status": self.status,
            "entry_at": iso(self.entry_at),
            "entry_price": self.entry_price,
            "final_at": iso(self.final_at),
            "final_price": self.final_price,
            "outcome": self.outcome,
            "net_result": self.net_result,
            "price_base": self.price_base,
            "resolution": self.resolution,
            "quality": self.quality,
            "reason": self.reason,
            "final_observation": self.final_observation.to_dict() if self.final_observation else None,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "PendingSimulation":
        return cls(
            simulation_id=str(value["simulation_id"]),
            signal_id=str(value["signal_id"]),
            instrument=str(value["instrument"]),
            direction=str(value["direction"]),
            horizon_seconds=float(value["horizon_seconds"]),
            detected_at=utc(value["detected_at"]) or datetime.fromtimestamp(0, UTC),
            entry_due_at=utc(value["entry_due_at"]) or datetime.fromtimestamp(0, UTC),
            expiry_at=utc(value["expiry_at"]) or datetime.fromtimestamp(0, UTC),
            entry_rule=str(value.get("entry_rule", "first_observation_at_or_after")),
            exit_rule=str(value.get("exit_rule", "last_observation_at_or_before")),
            status=str(value.get("status", "PENDING")),
            entry_at=utc(value.get("entry_at")),
            entry_price=float(value["entry_price"]) if value.get("entry_price") is not None else None,
            final_at=utc(value.get("final_at")),
            final_price=float(value["final_price"]) if value.get("final_price") is not None else None,
            outcome=value.get("outcome"),
            net_result=float(value["net_result"]) if value.get("net_result") is not None else None,
            price_base=str(value.get("price_base", "traded")),
            resolution=str(value.get("resolution", "event")),
            quality=str(value.get("quality", "valid")),
            reason=value.get("reason"),
            final_observation=PriceObservation.from_dict(value["final_observation"]) if value.get("final_observation") else None,
        )


def config_hash(strategy: StrategyConfig, simulation: SimulationConfig, timeframes: Sequence[Timeframe], mode: OperationMode, price_base: PriceBase | str = PriceBase.TRADED) -> str:
    payload = {
        "strategy": {
            "name": strategy.name,
            "context_timeframe": strategy.context_timeframe.name,
            "preparation_timeframe": strategy.preparation_timeframe.name,
            "trigger_timeframe": strategy.trigger_timeframe.name,
            "context_lookback": strategy.context_lookback,
            "preparation_lookback": strategy.preparation_lookback,
            "max_distance_atr": strategy.max_distance_atr,
            "rsi_threshold": strategy.rsi_threshold,
            "preparation_ttl_bars": strategy.preparation_ttl_bars,
            "require_closed": strategy.require_closed,
            "one_signal_per_episode": strategy.one_signal_per_episode,
            "optional_filters": dict(strategy.optional_filters),
            "indicators": {
                "ema_fast": strategy.indicators.ema_fast,
                "ema_slow": strategy.indicators.ema_slow,
                "rsi_period": strategy.indicators.rsi_period,
                "atr_period": strategy.indicators.atr_period,
                "wilder": strategy.indicators.wilder,
            },
        },
        "simulation": simulation.to_dict(),
        "timeframes": [tf.name for tf in timeframes],
        "mode": mode.value,
        "price_base": price_base.value if isinstance(price_base, PriceBase) else str(price_base),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


__all__ = [
    "PendingSimulation",
    "PriceObservation",
    "SimulationConfig",
    "candle_dict",
    "candle_from_dict",
    "config_hash",
    "evaluation_dict",
    "evaluation_from_dict",
    "event_dict",
    "event_from_dict",
    "iso",
    "mode_from",
    "quality_from",
    "quality_from_dict",
    "quality_label_is_usable",
    "signal_dict",
    "signal_from_dict",
    "to_core_candle",
    "to_core_event",
    "utc",
]
