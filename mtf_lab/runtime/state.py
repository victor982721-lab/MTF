"""Estado serializable usado por el procesador incremental.

Este módulo no abre conexiones ni conoce un proveedor.  Acepta los modelos
canónicos de ``mtf_lab.core`` y también los registros equivalentes del módulo
``mtf_lab.data`` mediante adaptación por atributos, conservando la base de
precio declarada.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any

from ..core import (
    Candle,
    ConditionResult,
    ConditionState,
    DataQuality,
    DecisionKind,
    Evaluation,
    EventKind,
    MarketEvent,
    OperationMode,
    PriceBase,
    Signal,
    StrategyConfig,
    Timeframe,
    parse_timeframe,
)
from ..ops.simulation import (
    iso_ts as simulation_iso_ts,
)
from ..ops.simulation import (
    normalize_price_base,
    parse_bool,
)
from ..ops.simulation import (
    parse_ts as simulation_parse_ts,
)
from ..ops.simulation import (
    quality_label_is_usable as shared_quality_label_is_usable,
)


def _timeframe(value: Timeframe | str | int) -> Timeframe:
    """Resolve the union accepted by core models at runtime boundaries."""

    return parse_timeframe(value)


def _timeframe_name(value: Timeframe | str | int) -> str:
    return _timeframe(value).name


def utc(value: datetime | str | None) -> datetime | None:
    """Parse one timestamp through the shared simulation policy."""

    return simulation_parse_ts(value) if value is not None else None


def iso(value: datetime | None) -> str | None:
    return simulation_iso_ts(value)


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
    """Normalize execution mode without defaulting unknown text to replay."""

    if synthetic:
        return OperationMode.SYNTHETIC
    if value is None:
        return OperationMode.REPLAY
    if isinstance(value, OperationMode):
        return value
    text = str(value).strip().upper()
    if text in {"LIVE", "OBSERVATION_EN_DIRECTO", "OBSERVACIÓN EN DIRECTO", "OBSERVACION_EN_DIRECTO"}:
        return OperationMode.LIVE
    if text in {"SYNTHETIC", "SINTETICO", "SINTÉTICO", "OFFLINE"}:
        return OperationMode.SYNTHETIC
    if text == "REPLAY":
        return OperationMode.REPLAY
    raise ValueError(f"unknown operation mode: {value!r}")


def quality_from(value: Any, *, synthetic: bool = False) -> DataQuality:
    if isinstance(value, DataQuality):
        return value
    # The provider/data layer has a compact DataQuality record with a boolean
    # ``valid`` field rather than the core flag set. Preserve that public
    # label instead of turning the object into the string ``DataQuality(...)``.
    if hasattr(value, "valid") and not isinstance(value, (str, bytes, Mapping)):
        return _quality_from_legacy(value, synthetic=synthetic)
    result = DataQuality.good(synthetic=synthetic)
    if value is None:
        return result
    if isinstance(value, Mapping):
        return _quality_from_mapping(value, result)
    return _quality_from_label(value, result)


def _quality_from_legacy(value: Any, *, synthetic: bool) -> DataQuality:
    result = DataQuality.good(synthetic=synthetic)
    if not bool(value.valid):
        result = result.with_flags("invalid", reason="source_quality_invalid")
    return result


def _quality_from_mapping(value: Mapping[str, Any], result: DataQuality) -> DataQuality:
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
    if status and status not in {
        "valid",
        "validated",
        "ok",
        "good",
        "synthetic",
        "synthetic_valid",
        "synthetic_validated",
        "data_quality_validated",
        "closed_valid",
    }:
        try:
            result = result.with_flags(status, reason=status)
        except ValueError:
            result = result.with_flags("invalid", reason=status)
    raw_source = value.get("source", result.source)
    source = raw_source if raw_source is None or isinstance(raw_source, str) else str(raw_source)
    return DataQuality(result.flags, reasons or result.reasons, source)


def _quality_from_label(value: Any, result: DataQuality) -> DataQuality:
    raw = str(value).strip().lower()
    if raw in {
        "",
        "valid",
        "validated",
        "ok",
        "good",
        "synthetic",
        "synthetic_validated",
        "synthetic_valid",
        "valid_data",
        "data_quality_validated",
        "public_provider_closed",
        "closed_valid",
        "validated_local",
    }:
        return result
    try:
        return result.with_flags(raw, reason=raw)
    except ValueError:
        return result.with_flags("invalid", reason=raw)


def quality_label_is_usable(value: Any) -> bool:
    """Compatibility wrapper over the shared quality gate."""

    return shared_quality_label_is_usable(value)


def _price_base(value: Any, default: PriceBase | None = None) -> PriceBase:
    fallback = default.value if isinstance(default, PriceBase) else None
    raw = value.value if isinstance(value, PriceBase) else value
    if isinstance(raw, str) and raw.strip().lower() in {"native", "provider_native", "provider-native"}:
        return PriceBase.NATIVE
    canonical = normalize_price_base(value, default=fallback, allow_none=default is not None)
    if canonical is None:
        raise ValueError("base de precio ausente")
    return PriceBase(canonical)


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


def _is_data_schema(record: Any, name: str) -> bool:
    return bool(record.__class__.__module__.startswith("mtf_lab.data") and record.__class__.__name__ == name)


def _translate_data_event(record: Any, mode: OperationMode | str) -> MarketEvent:
    from ..data.translation import data_event_to_core

    translated = data_event_to_core(record)
    metadata = getattr(record, "metadata", {}) or {}
    provenance = metadata.get("provenance") if isinstance(metadata, Mapping) else None
    explicit_mode = (
        provenance.get("mode")
        if isinstance(provenance, Mapping)
        else (metadata.get("mode") if isinstance(metadata, Mapping) else None)
    )
    if explicit_mode is None and not bool(getattr(record, "synthetic", False)):
        target_mode = mode_from(mode)
        metadata = dict(translated.metadata)
        provenance = metadata.get("provenance")
        if isinstance(provenance, Mapping):
            provenance = dict(provenance)
            provenance["mode"] = "LIVE" if target_mode is OperationMode.LIVE else target_mode.value
            metadata["provenance"] = provenance
        translated = replace(translated, mode=target_mode, metadata=metadata)
    return translated


def _event_price_fields(record: Any) -> tuple[PriceBase, Any, Any, Any, Any, Any]:
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
    if basis is PriceBase.MID and not math.isclose(
        float(mid), (float(bid) + float(ask)) / 2.0, rel_tol=1e-12, abs_tol=1e-12
    ):
        raise ValueError("mid no coincide con el promedio explícito de bid y ask")
    selected = {
        PriceBase.TRADED: raw_price,
        PriceBase.BID: bid,
        PriceBase.ASK: ask,
        PriceBase.MID: mid,
        PriceBase.NATIVE: raw_price,
    }[basis]
    if selected is None:
        raise ValueError(f"falta precio explícito para la base {basis.value}")
    if (
        raw_price is not None
        and basis is not PriceBase.TRADED
        and not math.isclose(float(raw_price), float(selected), rel_tol=1e-12, abs_tol=1e-12)
    ):
        raise ValueError("price no coincide con la base explícita")
    return basis, raw_price, bid, ask, mid, selected


def _event_metadata(record: Any, basis: PriceBase, mid: Any) -> dict[str, Any]:
    metadata = _attr(record, "metadata", default={}) or {}
    if not isinstance(metadata, Mapping):
        raise ValueError("metadata debe ser mapping")
    metadata = dict(metadata)
    if basis is PriceBase.MID:
        metadata.setdefault("mid", float(mid))
    return metadata


def _mapping_event_to_core(record: Any, mode: OperationMode | str) -> MarketEvent:
    instrument = str(_attr(record, "instrument", "symbol", default="unknown"))
    event_time = utc(_attr(record, "event_time", "event_ts", "timestamp", "ts"))
    if event_time is None:
        raise ValueError("evento sin event_time")
    basis, raw_price, bid, ask, mid, _selected = _event_price_fields(record)
    metadata = _event_metadata(record, basis, mid)
    synthetic = _strict_bool(_attr(record, "synthetic", "is_synthetic", default=False), default=False)
    received_at = utc(_attr(record, "received_at", "received_ts", default=None))
    available_at = utc(_attr(record, "available_at", "available_ts", default=None))
    if available_at is not None and available_at < event_time:
        raise ValueError("available_at no puede preceder a event_time")
    event_id_value = _attr(record, "event_id", "data_id", default=None)
    if callable(event_id_value):
        event_id_value = event_id_value()
    event_id = None if event_id_value is None else str(event_id_value)
    return MarketEvent(
        instrument=instrument,
        event_time=event_time,
        price=raw_price if basis is PriceBase.TRADED else None,
        quantity=_attr(record, "quantity", default=None),
        bid=bid,
        ask=ask,
        received_at=received_at,
        available_at=available_at,
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


def to_core_event(record: Any, *, mode: OperationMode | str = OperationMode.REPLAY) -> MarketEvent:
    """Normaliza un Event de cualquiera de los dos schemas locales."""

    if isinstance(record, MarketEvent):
        return record
    # Data-layer records cross through the explicit translation boundary so
    # flags, reasons, provenance and stable identities are not reduced to a
    # generic ``good`` value at runtime.
    if _is_data_schema(record, "Event"):
        return _translate_data_event(record, mode)
    return _mapping_event_to_core(record, mode)


def to_core_candle(record: Any, *, mode: OperationMode | str = OperationMode.REPLAY) -> Candle:
    """Normaliza una vela nativa o derivada al modelo del núcleo."""

    if isinstance(record, Candle):
        return record
    if record.__class__.__module__.startswith("mtf_lab.data") and record.__class__.__name__ == "Bar":
        from ..data.translation import data_bar_to_core

        translated = data_bar_to_core(record)
        metadata = getattr(record, "metadata", {}) or {}
        provenance = metadata.get("provenance") if isinstance(metadata, Mapping) else None
        explicit_mode = (
            provenance.get("mode")
            if isinstance(provenance, Mapping)
            else (metadata.get("mode") if isinstance(metadata, Mapping) else None)
        )
        if explicit_mode is None and not bool(getattr(record, "synthetic", False)):
            target_mode = mode_from(mode)
            metadata = dict(translated.metadata)
            provenance = metadata.get("provenance")
            if isinstance(provenance, Mapping):
                provenance = dict(provenance)
                provenance["mode"] = "LIVE" if target_mode is OperationMode.LIVE else target_mode.value
                metadata["provenance"] = provenance
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
        metadata={
            **(_attr(record, "metadata", default={}) or {}),
            **(
                {"revision": int(_attr(record, "revision", default=0) or 0)}
                if _attr(record, "revision", default=None) is not None
                else {}
            ),
        },
    )


def quality_dict(value: DataQuality) -> dict[str, Any]:
    return {
        "status": value.status,
        "flags": sorted(flag.value for flag in value.flags),
        "reasons": list(value.reasons),
        "source": value.source,
    }


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
        "mid": event.selected_price if event.price_base is PriceBase.MID else None,
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
    event = to_core_event(value, mode=value.get("mode", "REPLAY"))
    # ``to_core_event`` adds an explicit MID metadata hint for plain mappings.
    # Preserve the original metadata shape when a checkpoint came from a core
    # event whose provider translation did not carry that hint.
    metadata = value.get("metadata")
    if (
        event.price_base is PriceBase.MID
        and isinstance(metadata, Mapping)
        and "mid" not in metadata
        and "mid" in event.metadata
    ):
        event = replace(event, metadata={key: item for key, item in event.metadata.items() if key != "mid"})
    return event


def candle_dict(candle: Candle) -> dict[str, Any]:
    return {
        "instrument": candle.instrument,
        "timeframe": candle.timeframe_name,
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
        for name in (
            "entry_latency_seconds",
            "max_price_age_seconds",
            "stake",
            "payout_net",
            "loss_amount",
            "tie_tolerance",
            "costs",
        ):
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
    def from_mapping(cls, mapping: Mapping[str, Any] | None) -> SimulationConfig:
        if mapping is None:
            return cls()
        allowed = {
            "horizons_seconds",
            "horizons_minutes",
            "entry_latency_seconds",
            "entry_rule",
            "exit_rule",
            "max_price_age_seconds",
            "stake",
            "payout_net",
            "net_payout",
            "loss_amount",
            "tie_net",
            "tie_return",
            "tie_tolerance",
            "costs",
            "horizon_from",
            "requested_base_price",
            "resolution",
        }
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

    @property
    def canonical_price_base(self) -> str:
        return self.requested_base_price

    def missing_outcome(self, reason: str, *, capture_complete: bool) -> tuple[str, str]:
        """Shared completeness transition for runtime callers."""
        if not isinstance(capture_complete, bool):
            raise ValueError("capture_complete debe ser booleano")
        return ("INDETERMINATE" if capture_complete else "PENDING", reason)

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
    """One market observation with explicit availability and identity.

    ``timestamp`` is market time; ``available_at`` is the first time the
    observation may be consumed by the detector.  An availability timestamp
    earlier than market time is retained as an ``INVALID`` quality label so
    the runtime book blocks it instead of correcting it silently.
    """

    timestamp: datetime
    available_at: datetime
    price: float
    base_price: str
    source: str
    resolution: str
    closed: bool = True
    quality: str = "valid"
    instrument: str = "UNKNOWN"
    observation_id: str | None = None
    source_ordinal: int = 0
    source_sequence: int | str | None = None

    def __post_init__(self) -> None:
        market = simulation_parse_ts(self.timestamp)
        available = simulation_parse_ts(self.available_at)
        quality = str(self.quality or "UNKNOWN")
        if available < market:
            quality = f"INVALID:availability_before_market:{quality}"
        try:
            base = _price_base(self.base_price).value
        except (TypeError, ValueError):
            base = str(self.base_price or "unknown").strip().lower() or "unknown"
            quality = f"INVALID:unknown_price_base:{base}:{quality}"
        if not math.isfinite(float(self.price)):
            raise ValueError("PriceObservation.price debe ser finito")
        closed = parse_bool(self.closed, name="closed")
        source = str(self.source or "unknown").strip() or "unknown"
        resolution = str(self.resolution or "UNKNOWN").strip() or "UNKNOWN"
        instrument = str(self.instrument or "UNKNOWN").strip() or "UNKNOWN"
        observation_id = None if self.observation_id is None else str(self.observation_id).strip() or None
        if isinstance(self.source_ordinal, bool) or int(self.source_ordinal) != self.source_ordinal:
            raise ValueError("source_ordinal debe ser entero")
        object.__setattr__(self, "timestamp", market)
        object.__setattr__(self, "available_at", available)
        object.__setattr__(self, "base_price", base or "unknown")
        object.__setattr__(self, "quality", quality)
        object.__setattr__(self, "closed", closed)
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "resolution", resolution)
        object.__setattr__(self, "instrument", instrument)
        object.__setattr__(self, "observation_id", observation_id)
        object.__setattr__(self, "source_ordinal", int(self.source_ordinal))

    @property
    def market_time(self) -> datetime:
        return self.timestamp

    @property
    def available_ts(self) -> datetime:
        return self.available_at

    @property
    def effective_available_at(self) -> datetime:
        return self.available_at

    @property
    def identity(self) -> str:
        return str(
            self.observation_id
            or f"{self.source}:{self.instrument}:{self.timestamp.isoformat()}:{self.source_ordinal}:{self.base_price}"
        )

    @property
    def point_id(self) -> str:
        return self.identity

    def usable_as_of(self, as_of: datetime | None) -> bool:
        return quality_label_is_usable(self.quality) and (
            as_of is None or self.available_at <= simulation_parse_ts(as_of)
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": iso(self.timestamp),
            "market_time": iso(self.timestamp),
            "available_at": iso(self.available_at),
            "price": self.price,
            "base_price": self.base_price,
            "source": self.source,
            "resolution": self.resolution,
            "closed": self.closed,
            "quality": self.quality,
            "instrument": self.instrument,
            "observation_id": self.observation_id,
            "identity": self.identity,
            "point_id": self.observation_id,
            "source_ordinal": self.source_ordinal,
            "source_sequence": self.source_sequence,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> PriceObservation:
        timestamp = value.get("timestamp", value.get("market_time"))
        available = value.get("available_at", value.get("available_ts", timestamp))
        return cls(
            timestamp=simulation_parse_ts(timestamp),
            available_at=simulation_parse_ts(available),
            price=float(value["price"]),
            base_price=str(value.get("base_price", value.get("price_base", "traded"))),
            source=str(value.get("source", "unknown")),
            resolution=str(value.get("resolution", "unknown")),
            closed=parse_bool(value.get("closed", True), name="closed"),
            quality=str(value.get("quality", "valid")),
            instrument=str(value.get("instrument", value.get("symbol", "UNKNOWN"))),
            observation_id=(
                str(value["observation_id"])
                if value.get("observation_id") is not None
                else (str(value["point_id"]) if value.get("point_id") is not None else None)
            ),
            source_ordinal=int(value.get("source_ordinal", value.get("ordinal", 0))),
            source_sequence=value.get("source_sequence", value.get("sequence")),
        )


def _normalize_pending_identity(value: PendingSimulation) -> None:
    for name in ("simulation_id", "signal_id", "instrument", "direction"):
        item = getattr(value, name)
        if not isinstance(item, str) or not item.strip():
            raise ValueError(f"{name} debe ser texto no vacío")
        object.__setattr__(value, name, item.strip())


def _normalize_pending_timing(value: PendingSimulation) -> datetime:
    horizon = float(value.horizon_seconds)
    if not math.isfinite(horizon) or horizon <= 0:
        raise ValueError("horizon_seconds debe ser positivo y finito")
    object.__setattr__(value, "horizon_seconds", horizon)
    detected = simulation_parse_ts(value.detected_at)
    entry_due = simulation_parse_ts(value.entry_due_at)
    expiry = simulation_parse_ts(value.expiry_at)
    if entry_due < detected:
        raise ValueError("entry_due_at no puede preceder detected_at")
    if expiry < entry_due:
        raise ValueError("expiry_at no puede preceder entry_due_at")
    object.__setattr__(value, "detected_at", detected)
    object.__setattr__(value, "entry_due_at", entry_due)
    object.__setattr__(value, "expiry_at", expiry)
    return detected


def _normalize_pending_optional_times(value: PendingSimulation, detected: datetime) -> None:
    for name in ("entry_at", "final_at", "detection_available_at"):
        item = getattr(value, name)
        if item is None:
            continue
        parsed = simulation_parse_ts(item)
        if name == "detection_available_at" and parsed < detected:
            raise ValueError("detection_available_at no puede preceder detected_at")
        object.__setattr__(value, name, parsed)


def _normalize_pending_prices(value: PendingSimulation) -> None:
    for name in ("entry_price", "final_price", "net_result"):
        item = getattr(value, name)
        if item is not None and not math.isfinite(float(item)):
            raise ValueError(f"{name} debe ser finito")
        object.__setattr__(value, name, float(item) if item is not None else None)


def _normalize_pending_status(value: PendingSimulation) -> None:
    status = str(value.status).strip().upper()
    if status not in {"PENDING", "RESOLVED", "INDETERMINATE"}:
        raise ValueError(f"status de simulación desconocido: {value.status!r}")
    object.__setattr__(value, "status", status)
    if value.outcome is not None:
        outcome = str(value.outcome).strip().upper()
        if outcome not in {"WIN", "LOSS", "TIE", "PENDING", "INDETERMINATE"}:
            raise ValueError(f"outcome de simulación desconocido: {value.outcome!r}")
        object.__setattr__(value, "outcome", outcome)
    if not isinstance(value.capture_complete, bool):
        raise ValueError("capture_complete debe ser booleano")


def _normalize_pending_contract(value: PendingSimulation) -> None:
    object.__setattr__(value, "price_base", _price_base(value.price_base).value)
    resolution = str(value.resolution).strip()
    if not resolution:
        raise ValueError("resolution no puede estar vacía")
    object.__setattr__(value, "resolution", resolution)
    object.__setattr__(value, "quality", str(value.quality or "UNKNOWN"))
    if value.final_observation is not None and not isinstance(value.final_observation, PriceObservation):
        raise ValueError("final_observation debe ser PriceObservation")


@dataclass(frozen=True, slots=True)
class PendingSimulation:
    """Lifecycle record for one signal/horizon virtual evaluation.

    Market times and availability are kept separate, and ``capture_complete``
    is an explicit transition input: an open capture remains ``PENDING`` when
    data is absent; a complete replay may become ``INDETERMINATE``.
    """

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
    capture_complete: bool = False
    detection_available_at: datetime | None = None

    def __post_init__(self) -> None:
        _normalize_pending_identity(self)
        detected = _normalize_pending_timing(self)
        _normalize_pending_optional_times(self, detected)
        _normalize_pending_prices(self)
        _normalize_pending_status(self)
        _normalize_pending_contract(self)

    @property
    def identity(self) -> str:
        return self.simulation_id

    @property
    def signal_identity(self) -> str:
        return self.signal_id

    @property
    def detected_market_at(self) -> datetime:
        return self.detected_at

    @property
    def entry_market_due_at(self) -> datetime:
        return self.entry_due_at

    @property
    def expiry_market_at(self) -> datetime:
        return self.expiry_at

    @property
    def is_pending(self) -> bool:
        return self.status == "PENDING"

    @property
    def is_complete(self) -> bool:
        return self.status in {"RESOLVED", "INDETERMINATE"}

    def missing_outcome(self, reason: str, *, capture_complete: bool | None = None) -> tuple[str, str]:
        complete = self.capture_complete if capture_complete is None else capture_complete
        if not isinstance(complete, bool):
            raise ValueError("capture_complete debe ser booleano")
        return ("INDETERMINATE" if complete else "PENDING", reason)

    def to_dict(self) -> dict[str, Any]:
        return {
            "simulation_id": self.simulation_id,
            "signal_id": self.signal_id,
            "instrument": self.instrument,
            "direction": self.direction,
            "horizon_seconds": self.horizon_seconds,
            "detected_at": iso(self.detected_at),
            "detected_market_at": iso(self.detected_at),
            "detection_available_at": iso(self.detection_available_at),
            "entry_due_at": iso(self.entry_due_at),
            "entry_market_due_at": iso(self.entry_due_at),
            "expiry_at": iso(self.expiry_at),
            "expiry_market_at": iso(self.expiry_at),
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
            "capture_complete": self.capture_complete,
            "identity": self.identity,
            "final_observation": self.final_observation.to_dict() if self.final_observation else None,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> PendingSimulation:
        return cls(
            simulation_id=str(value["simulation_id"]),
            signal_id=str(value["signal_id"]),
            instrument=str(value["instrument"]),
            direction=str(value["direction"]),
            horizon_seconds=float(value["horizon_seconds"]),
            detected_at=simulation_parse_ts(value.get("detected_at", value.get("detected_market_at"))),
            entry_due_at=simulation_parse_ts(value["entry_due_at"]),
            expiry_at=simulation_parse_ts(value["expiry_at"]),
            entry_rule=str(value.get("entry_rule", "first_observation_at_or_after")),
            exit_rule=str(value.get("exit_rule", "last_observation_at_or_before")),
            status=str(value.get("status", "PENDING")),
            entry_at=simulation_parse_ts(value["entry_at"]) if value.get("entry_at") is not None else None,
            entry_price=float(value["entry_price"]) if value.get("entry_price") is not None else None,
            final_at=simulation_parse_ts(value["final_at"]) if value.get("final_at") is not None else None,
            final_price=float(value["final_price"]) if value.get("final_price") is not None else None,
            outcome=value.get("outcome"),
            net_result=float(value["net_result"]) if value.get("net_result") is not None else None,
            price_base=str(value.get("price_base", "traded")),
            resolution=str(value.get("resolution", "event")),
            quality=str(value.get("quality", "valid")),
            reason=value.get("reason"),
            final_observation=PriceObservation.from_dict(value["final_observation"])
            if value.get("final_observation")
            else None,
            capture_complete=parse_bool(value.get("capture_complete", False), name="capture_complete"),
            detection_available_at=simulation_parse_ts(value["detection_available_at"])
            if value.get("detection_available_at") is not None
            else None,
        )


def config_hash(
    strategy: StrategyConfig,
    simulation: SimulationConfig,
    timeframes: Sequence[Timeframe],
    mode: OperationMode,
    price_base: PriceBase | str = PriceBase.TRADED,
) -> str:
    payload = {
        "strategy": {
            "name": strategy.name,
            "context_timeframe": _timeframe_name(strategy.context_timeframe),
            "preparation_timeframe": _timeframe_name(strategy.preparation_timeframe),
            "trigger_timeframe": _timeframe_name(strategy.trigger_timeframe),
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
        "timeframes": [_timeframe_name(tf) for tf in timeframes],
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
