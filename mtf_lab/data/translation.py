"""Explicit translation between provider records and the MTF core models.

The data adapters intentionally use a small provider-neutral ``Event``/``Bar``
model while the causal engine uses ``MarketEvent``/``Candle``.  This module is
the only bridge between those two contracts.  It never guesses a mode, price
base, quality state or provenance value: unknown values raise
:class:`TranslationError` and the caller must keep the affected evaluation
blocked.

The reconciliation helper compares native provider candles with candles derived
from *actual* normalized events.  It never manufactures ticks from native
OHLC, fills a gap, or rewrites the native observation.  A missing side or a
mismatch is returned as an explicit blocking finding.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import math
from typing import Any, Iterable, Literal, Mapping, Sequence

from mtf_lab.core.models import (
    Candle as CoreCandle,
    EventKind,
    MarketEvent as CoreEvent,
    OperationMode,
    PriceBase,
    Timeframe,
    normalize_utc,
    parse_timeframe,
)
from mtf_lab.core.quality import (
    DataQuality as CoreQuality,
    QualityFlag,
    merge_quality,
    validate_candle,
    validate_event,
)
from mtf_lab.data.models import Bar as DataBar
from mtf_lab.data.models import Event as DataEvent
from mtf_lab.data.models import Provenance as DataProvenance
from mtf_lab.data.models import isoformat_utc


UTC = timezone.utc


class TranslationError(ValueError):
    """An invalid/unknown translation value that must remain blocking."""

    blocking = True

    def __init__(self, message: str, *, code: str = "TRANSLATION_INVALID", reasons: Sequence[str] = ()) -> None:
        self.code = code
        self.reasons = tuple(reasons) or (message,)
        super().__init__(message)


_DATA_MODES = {
    "SYNTHETIC": "SYNTHETIC",
    "SINTETICO": "SYNTHETIC",
    "SINTÉTICO": "SYNTHETIC",
    "REPLAY": "REPLAY",
    "IMPORT": "IMPORT",
    "LIVE": "OBSERVACIÓN EN DIRECTO",
    "OBSERVACIÓN EN DIRECTO": "OBSERVACIÓN EN DIRECTO",
    "OBSERVATION_EN_DIRECTO": "OBSERVATION_EN_DIRECTO",
}
_CORE_MODES = {"LIVE": OperationMode.LIVE, "REPLAY": OperationMode.REPLAY, "SYNTHETIC": OperationMode.SYNTHETIC}
_BASES = {"traded": PriceBase.TRADED, "bid": PriceBase.BID, "ask": PriceBase.ASK, "mid": PriceBase.MID}
_QUALITY_FLAGS = {flag.value: flag for flag in QualityFlag}
_EVENT_KINDS = {kind.value: kind for kind in EventKind}


def _text(value: Any, *, name: str) -> str:
    if isinstance(value, str):
        result = value.strip()
    elif isinstance(value, (OperationMode, PriceBase, EventKind, QualityFlag)):
        result = value.value
    else:
        raise TranslationError(f"{name} debe ser una cadena/enum conocida; llegó {value!r}", code=f"{name.upper()}_INVALID")
    if not result:
        raise TranslationError(f"{name} no puede estar vacío", code=f"{name.upper()}_INVALID")
    return result


def _data_mode(value: Any, *, name: str = "mode") -> str:
    raw = _text(value, name=name).upper()
    if raw not in _DATA_MODES:
        raise TranslationError(f"modo desconocido: {value!r}; no se sustituye por REPLAY", code="MODE_UNKNOWN")
    return _DATA_MODES[raw]


def _core_mode(value: Any, *, name: str = "mode") -> OperationMode:
    if isinstance(value, OperationMode):
        return value
    raw = _text(value, name=name).upper()
    try:
        return _CORE_MODES[raw]
    except KeyError as exc:
        raise TranslationError(f"modo core desconocido: {value!r}", code="MODE_UNKNOWN") from exc


def _data_base(value: Any, *, name: str = "price_basis") -> str:
    raw = _text(value, name=name).lower()
    if raw not in _BASES:
        raise TranslationError(f"base de precio desconocida: {value!r}; no se intercambia silenciosamente", code="PRICE_BASE_UNKNOWN")
    return raw


def _core_base(value: Any, *, name: str = "price_base") -> PriceBase:
    if isinstance(value, PriceBase):
        return value
    raw = _text(value, name=name).lower()
    try:
        return _BASES[raw]
    except KeyError as exc:
        raise TranslationError(f"base de precio core desconocida: {value!r}", code="PRICE_BASE_UNKNOWN") from exc


def _strict_bool(value: Any, *, name: str) -> bool:
    if not isinstance(value, bool):
        raise TranslationError(f"{name} debe ser booleano, no se interpreta por truthiness: {value!r}", code="STATE_INVALID")
    return value


def _strict_nonnegative_int(value: Any, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise TranslationError(f"{name} debe ser entero no negativo: {value!r}", code="STATE_INVALID")
    return value


def _strict_id(value: Any, *, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TranslationError(f"{name} debe ser una identidad no vacía", code="IDENTITY_INVALID")
    return value.strip()


def _quality_flags(raw: Any, *, name: str = "quality_flags") -> frozenset[QualityFlag]:
    if raw is None:
        return frozenset()
    if isinstance(raw, (str, QualityFlag)):
        values: Sequence[Any] = (raw,)
    elif isinstance(raw, (list, tuple, set, frozenset)):
        values = tuple(raw)
    else:
        raise TranslationError(f"{name} debe ser una secuencia de estados conocidos", code="QUALITY_INVALID")
    flags: set[QualityFlag] = set()
    for item in values:
        raw_flag = item.value if isinstance(item, QualityFlag) else str(item).strip().lower()
        try:
            flags.add(_QUALITY_FLAGS[raw_flag])
        except KeyError as exc:
            raise TranslationError(f"estado de calidad desconocido: {item!r}; permanece bloqueante", code="QUALITY_UNKNOWN") from exc
    return frozenset(flags)


def _quality_reasons(raw: Any, *, name: str = "quality_reasons") -> tuple[str, ...]:
    if raw is None:
        return ()
    if isinstance(raw, str):
        return (raw,)
    if not isinstance(raw, (list, tuple)):
        raise TranslationError(f"{name} debe ser una secuencia de razones", code="QUALITY_INVALID")
    return tuple(str(item) for item in raw)


def _quality_from_data_metadata(metadata: Mapping[str, Any], *, source: str, synthetic: bool, closed: bool | None = None) -> CoreQuality:
    """Decode data-side quality fields without hiding unknown states."""

    raw_quality = metadata.get("quality")
    if isinstance(raw_quality, CoreQuality):
        quality = raw_quality
    elif isinstance(raw_quality, Mapping):
        flags_raw = raw_quality.get("flags", raw_quality.get("quality_flags"))
        reasons_raw = raw_quality.get("reasons", raw_quality.get("quality_reasons"))
        quality = CoreQuality(
            flags=_quality_flags(flags_raw),
            reasons=_quality_reasons(reasons_raw),
            source=raw_quality.get("source", source),
        )
    elif raw_quality is not None:
        raise TranslationError("metadata.quality debe ser DataQuality o tabla", code="QUALITY_INVALID")
    else:
        quality = CoreQuality(
            flags=_quality_flags(metadata.get("quality_flags")),
            reasons=_quality_reasons(metadata.get("quality_reasons")),
            source=metadata.get("quality_source", source),
        )
    flags = set(quality.flags)
    reasons = list(quality.reasons)
    if synthetic:
        flags.add(QualityFlag.SYNTHETIC)
    if closed is False:
        flags.add(QualityFlag.OPEN)
    return CoreQuality(frozenset(flags), tuple(reasons), quality.source or source)


def _validate_metadata_quality(metadata: Mapping[str, Any]) -> None:
    """Validate serialized quality fields even when core.quality is present."""

    raw_quality = metadata.get("quality")
    if raw_quality is not None:
        if isinstance(raw_quality, CoreQuality):
            _validate_core_quality(raw_quality)
        elif isinstance(raw_quality, Mapping):
            _quality_flags(raw_quality.get("flags", raw_quality.get("quality_flags")))
            _quality_reasons(raw_quality.get("reasons", raw_quality.get("quality_reasons")))
        else:
            raise TranslationError("metadata.quality inválida", code="QUALITY_INVALID")
    if "quality_flags" in metadata:
        _quality_flags(metadata.get("quality_flags"))
    if "quality_reasons" in metadata:
        _quality_reasons(metadata.get("quality_reasons"))


def _validate_core_quality(value: Any) -> CoreQuality:
    if not isinstance(value, CoreQuality):
        raise TranslationError("quality core inválida o ausente", code="QUALITY_INVALID")
    # Rebuild through the strict decoders so malformed objects created outside
    # the dataclass constructor cannot silently lose an unknown state.
    return CoreQuality(
        flags=_quality_flags(value.flags),
        reasons=_quality_reasons(value.reasons),
        source=value.source,
    )


def _quality_to_data_metadata(metadata: Mapping[str, Any], quality: CoreQuality) -> dict[str, Any]:
    result = dict(metadata)
    result["quality_flags"] = sorted(flag.value for flag in quality.flags)
    result["quality_reasons"] = list(quality.reasons)
    if quality.source is not None:
        result["quality_source"] = quality.source
    # Keep a structured copy too, so a JSON persistence adapter can round-trip
    # both flags and reasons without depending on enum reprs.
    result["quality"] = {
        "flags": sorted(flag.value for flag in quality.flags),
        "reasons": list(quality.reasons),
        "source": quality.source,
    }
    return result


def _provenance_mapping(raw: Any, *, fallback: DataProvenance) -> dict[str, Any]:
    if raw is None:
        return fallback.to_dict()
    if isinstance(raw, DataProvenance):
        # Validate all state-bearing fields even when the object was assembled
        # dynamically without static type checking.
        _data_mode(raw.mode, name="provenance.mode")
        _data_base(raw.price_basis, name="provenance.price_basis")
        return raw.to_dict()
    if not isinstance(raw, Mapping):
        raise TranslationError("metadata.provenance debe ser DataProvenance o tabla", code="PROVENANCE_INVALID")
    if "mode" in raw:
        _data_mode(raw["mode"], name="provenance.mode")
    if "price_basis" in raw:
        _data_base(raw["price_basis"], name="provenance.price_basis")
    result = dict(raw)
    # A provenance table may contain future provider fields; preserve them
    # rather than dropping them, while validating state fields above.
    result.setdefault("provider", fallback.provider)
    result.setdefault("mode", fallback.mode)
    result.setdefault("instrument", fallback.instrument)
    result.setdefault("price_basis", fallback.price_basis)
    result.setdefault("synthetic", fallback.synthetic)
    if not isinstance(result["synthetic"], bool):
        raise TranslationError("provenance.synthetic debe ser booleano", code="PROVENANCE_INVALID")
    return result


def _fallback_provenance(*, source: str, mode: str, instrument: str, price_basis: str, resolution: str | None = None, synthetic: bool = False, notes: Sequence[str] = ()) -> DataProvenance:
    return DataProvenance(
        provider=source,
        mode=_data_mode(mode),
        instrument=instrument,
        resolutions=(resolution,) if resolution else (),
        price_basis=_data_base(price_basis),
        synthetic=synthetic,
        notes=tuple(notes),
    )


def _data_mode_from_metadata(metadata: Mapping[str, Any], *, synthetic: bool, default: str = "REPLAY") -> str:
    raw_provenance = metadata.get("provenance")
    if isinstance(raw_provenance, DataProvenance):
        raw_mode = raw_provenance.mode
    elif isinstance(raw_provenance, Mapping) and "mode" in raw_provenance:
        raw_mode = raw_provenance["mode"]
    elif "mode" in metadata:
        # Accepting a direct mode is useful for lightweight serialized records,
        # but it is still validated; an unknown value cannot be ignored.
        raw_mode = metadata["mode"]
    else:
        raw_mode = "SYNTHETIC" if synthetic else default
    canonical = _data_mode(raw_mode)
    if synthetic and canonical not in {"SYNTHETIC"}:
        raise TranslationError("registro sintético con provenance.mode no sintético", code="PROVENANCE_CONFLICT")
    return canonical


def _metadata_with_translation(metadata: Mapping[str, Any], *, data_id: str, provenance: Mapping[str, Any], **identity: Any) -> dict[str, Any]:
    result = dict(metadata)
    result["provenance"] = dict(provenance)
    result["_mtf_data_identity"] = {"data_id": data_id, **identity}
    return result


def _identity_mapping(metadata: Mapping[str, Any]) -> Mapping[str, Any] | None:
    raw = metadata.get("_mtf_data_identity")
    if raw is None:
        return None
    if not isinstance(raw, Mapping):
        raise TranslationError("_mtf_data_identity inválida", code="IDENTITY_INVALID")
    return raw


def data_event_to_core(event: DataEvent) -> CoreEvent:
    """Translate one data-layer event to the causal core event."""

    if not isinstance(event, DataEvent):
        raise TranslationError(f"se esperaba data.Event, llegó {type(event).__name__}", code="TYPE_INVALID")
    basis = _data_base(event.price_basis)
    synthetic = _strict_bool(event.synthetic, name="synthetic")
    snapshot = _strict_bool(event.is_snapshot, name="is_snapshot")
    if event.source_event_id is not None:
        _strict_id(event.source_event_id, name="source_event_id")
    if event.side is not None and event.side not in {"buy", "sell", "unknown"}:
        raise TranslationError(f"side desconocido: {event.side!r}", code="SIDE_UNKNOWN")
    metadata = dict(event.metadata)
    identity = _identity_mapping(metadata)
    mode = _data_mode_from_metadata(metadata, synthetic=synthetic, default="REPLAY")
    core_mode = _core_mode("SYNTHETIC" if mode == "SYNTHETIC" else ("LIVE" if mode in {"OBSERVACIÓN EN DIRECTO", "OBSERVATION_EN_DIRECTO"} else "REPLAY"))
    # A non-traded basis must have its raw field.  Copying ``price`` into a
    # missing bid/ask/mid would make an unknown quote look valid.
    if basis == "bid" and event.bid is None:
        raise TranslationError("evento BID sin campo bid explícito", code="PRICE_BASE_MISSING")
    if basis == "ask" and event.ask is None:
        raise TranslationError("evento ASK sin campo ask explícito", code="PRICE_BASE_MISSING")
    if basis == "mid" and event.mid is None:
        raise TranslationError("evento MID sin campo mid explícito", code="PRICE_BASE_MISSING")
    if basis == "mid" and (event.bid is None or event.ask is None):
        raise TranslationError("evento MID requiere bid y ask explícitos para el contrato core", code="PRICE_BASE_MISSING")
    selected_raw = {"traded": event.price, "bid": event.bid, "ask": event.ask, "mid": event.mid}[basis]
    if selected_raw is None or not math.isclose(float(event.price), float(selected_raw), rel_tol=1e-12, abs_tol=1e-12):
        raise TranslationError("price no coincide con la base explícita del evento", code="PRICE_BASE_CONFLICT")
    if basis == "mid" and not math.isclose(float(event.mid), (float(event.bid) + float(event.ask)) / 2.0, rel_tol=1e-12, abs_tol=1e-12):
        raise TranslationError("el core sólo representa MID como promedio explícito de bid y ask", code="PRICE_BASE_CONFLICT")
    quality = _quality_from_data_metadata(metadata, source=event.source, synthetic=synthetic)
    fallback_provenance = _fallback_provenance(
        source=event.source,
        mode=mode,
        instrument=event.instrument,
        price_basis=basis,
        synthetic=synthetic,
        notes=("traducción data.Event -> core.MarketEvent",),
    )
    provenance = _provenance_mapping(metadata.get("provenance"), fallback=fallback_provenance)
    metadata = _metadata_with_translation(
        metadata,
        data_id=event.data_id,
        provenance=provenance,
        source_event_id=event.source_event_id,
        source_sequence=event.source_sequence,
        price_basis=basis,
        mid=event.mid,
        side=event.side,
        is_snapshot=snapshot,
    )
    event_kind_raw = metadata.get("event_kind", metadata.get("kind", "trade"))
    if isinstance(event_kind_raw, EventKind):
        event_kind = event_kind_raw
    else:
        try:
            event_kind = _EVENT_KINDS[str(event_kind_raw).strip().lower()]
        except KeyError as exc:
            raise TranslationError(f"event_kind desconocido: {event_kind_raw!r}", code="EVENT_KIND_UNKNOWN") from exc
    core_event_id = _strict_id(identity["core_event_id"], name="core_event_id") if identity and identity.get("core_event_id") is not None else event.data_id
    return CoreEvent(
        instrument=event.instrument,
        event_time=normalize_utc(event.event_time, "event_time"),
        price=event.price if basis == "traded" else None,
        quantity=event.quantity,
        bid=event.bid,
        ask=event.ask,
        received_at=normalize_utc(event.received_at, "received_at") if event.received_at else None,
        available_at=normalize_utc(event.available_at, "available_at") if event.available_at else None,
        source=event.source,
        mode=core_mode,
        price_base=_core_base(basis),
        event_kind=event_kind,
        sequence=event.source_sequence,
        source_event_id=event.source_event_id,
        event_id=core_event_id,
        quality=quality,
        metadata=metadata,
    )


def core_event_to_data(event: CoreEvent) -> DataEvent:
    """Translate one core event back, retaining explicit quote components."""

    if not isinstance(event, CoreEvent):
        raise TranslationError(f"se esperaba core.MarketEvent, llegó {type(event).__name__}", code="TYPE_INVALID")
    core_event_id = _strict_id(event.event_id, name="event_id")
    basis_enum = _core_base(event.price_base)
    basis = basis_enum.value
    mode_enum = _core_mode(event.mode)
    event_kind_raw = event.event_kind
    if isinstance(event_kind_raw, EventKind):
        event_kind = event_kind_raw
    else:
        try:
            event_kind = _EVENT_KINDS[str(event_kind_raw).strip().lower()]
        except KeyError as exc:
            raise TranslationError(f"event_kind core desconocido: {event_kind_raw!r}", code="EVENT_KIND_UNKNOWN") from exc
    metadata = dict(event.metadata)
    _validate_metadata_quality(metadata)
    quality = _validate_core_quality(event.quality)
    synthetic = mode_enum is OperationMode.SYNTHETIC or QualityFlag.SYNTHETIC in quality.flags
    provenance_raw = metadata.get("provenance")
    if provenance_raw is not None:
        if isinstance(provenance_raw, Mapping) and "mode" in provenance_raw:
            data_mode = _data_mode(provenance_raw["mode"], name="provenance.mode")
        elif isinstance(provenance_raw, DataProvenance):
            data_mode = _data_mode(provenance_raw.mode, name="provenance.mode")
        else:
            raise TranslationError("provenance en core no tiene modo interpretable", code="PROVENANCE_INVALID")
    else:
        data_mode = "SYNTHETIC" if synthetic else ("OBSERVACIÓN EN DIRECTO" if mode_enum is OperationMode.LIVE else "REPLAY")
    if synthetic and data_mode != "SYNTHETIC":
        raise TranslationError("quality sintética con provenance.mode no sintético", code="PROVENANCE_CONFLICT")
    if basis == "bid" and event.bid is None:
        raise TranslationError("evento core BID sin bid explícito", code="PRICE_BASE_MISSING")
    if basis == "ask" and event.ask is None:
        raise TranslationError("evento core ASK sin ask explícito", code="PRICE_BASE_MISSING")
    selected = event.selected_price
    if selected is None or not math.isfinite(float(selected)):
        raise TranslationError(f"evento core sin precio para base {basis}", code="PRICE_BASE_MISSING")
    # For core MID, selected_price may be explicitly derived by core from bid /
    # ask.  Keep that derivation visible instead of silently presenting it as a
    # traded price.
    if basis == "mid" and metadata.get("mid") is not None:
        try:
            explicit_mid = float(metadata["mid"])
        except (TypeError, ValueError) as exc:
            raise TranslationError("metadata.mid inválido", code="PRICE_BASE_INVALID") from exc
        if not math.isclose(explicit_mid, float(selected), rel_tol=1e-12, abs_tol=1e-12):
            raise TranslationError("metadata.mid no coincide con bid/ask del core", code="PRICE_BASE_CONFLICT")
        mid = explicit_mid
    else:
        mid = float(selected) if basis == "mid" else (metadata.get("mid") if metadata.get("mid") is not None else None)
        if basis == "mid" and event.bid is not None and event.ask is not None:
            metadata["mid_derived_from_bid_ask"] = True
    fallback = _fallback_provenance(
        source=event.source,
        mode=data_mode,
        instrument=event.instrument,
        price_basis=basis,
        synthetic=synthetic,
        notes=("traducción core.MarketEvent -> data.Event",),
    )
    provenance = _provenance_mapping(provenance_raw, fallback=fallback)
    metadata = _quality_to_data_metadata(metadata, quality)
    side = metadata.get("side")
    if side is not None:
        if not isinstance(side, str) or side not in {"buy", "sell", "unknown"}:
            raise TranslationError(f"side desconocido: {side!r}", code="SIDE_UNKNOWN")
    metadata = _metadata_with_translation(
        metadata,
        data_id=core_event_id,
        provenance=provenance,
        core_event_id=core_event_id,
        event_kind=event_kind.value,
    )
    return DataEvent(
        instrument=event.instrument,
        event_time=normalize_utc(event.event_time, "event_time"),
        price=float(selected),
        bid=event.bid,
        ask=event.ask,
        mid=mid,
        price_basis=basis,
        quantity=event.quantity,
        received_at=normalize_utc(event.received_at, "received_at") if event.received_at else None,
        available_at=normalize_utc(event.available_at, "available_at") if event.available_at else None,
        source=event.source,
        source_event_id=event.source_event_id,
        source_sequence=event.sequence,
        side=side,
        is_snapshot=_strict_bool(metadata.get("is_snapshot", False), name="is_snapshot"),
        synthetic=synthetic,
        metadata=metadata,
    )


def data_bar_to_core(bar: DataBar) -> CoreCandle:
    """Translate one data-layer OHLC bar to a core candle."""

    if not isinstance(bar, DataBar):
        raise TranslationError(f"se esperaba data.Bar, llegó {type(bar).__name__}", code="TYPE_INVALID")
    basis = _data_base(bar.price_basis)
    closed = _strict_bool(bar.closed, name="closed")
    synthetic = _strict_bool(bar.synthetic, name="synthetic")
    if closed and bar.available_at is not None:
        available_at = normalize_utc(bar.available_at, "available_at")
        interval_end = normalize_utc(bar.interval_end, "interval_end")
        if available_at < interval_end:
            raise TranslationError("vela cerrada disponible antes de su final", code="STATE_INVALID")
    revision = _strict_nonnegative_int(bar.revision, name="revision")
    if bar.source_record_id is not None:
        _strict_id(bar.source_record_id, name="source_record_id")
    metadata = dict(bar.metadata)
    identity = _identity_mapping(metadata)
    mode = _data_mode_from_metadata(metadata, synthetic=synthetic, default="REPLAY")
    core_mode = _core_mode("SYNTHETIC" if mode == "SYNTHETIC" else ("LIVE" if mode in {"OBSERVACIÓN EN DIRECTO", "OBSERVATION_EN_DIRECTO"} else "REPLAY"))
    quality = _quality_from_data_metadata(metadata, source=bar.source, synthetic=synthetic, closed=closed)
    fallback = _fallback_provenance(
        source=bar.source,
        mode=mode,
        instrument=bar.instrument,
        price_basis=basis,
        resolution=bar.resolution,
        synthetic=synthetic,
        notes=("traducción data.Bar -> core.Candle",),
    )
    provenance = _provenance_mapping(metadata.get("provenance"), fallback=fallback)
    metadata["revision"] = revision
    metadata["volume_was_none"] = bar.volume is None
    metadata["trade_count_was_none"] = bar.trade_count is None
    metadata = _metadata_with_translation(
        metadata,
        data_id=bar.data_id,
        provenance=provenance,
        source_record_id=bar.source_record_id,
        revision=revision,
        closed=closed,
        synthetic=synthetic,
    )
    # Core Candle stores revision in metadata because its stable public model
    # predates revisioned provider candles.  candle_id remains the data ID.
    core_candle_id = _strict_id(identity["core_candle_id"], name="core_candle_id") if identity and identity.get("core_candle_id") is not None else bar.data_id
    return CoreCandle(
        instrument=bar.instrument,
        timeframe=bar.resolution,
        start=normalize_utc(bar.interval_start, "interval_start"),
        end=normalize_utc(bar.interval_end, "interval_end"),
        open=bar.open,
        high=bar.high,
        low=bar.low,
        close=bar.close,
        volume=bar.volume or 0.0,
        event_count=bar.trade_count or 0,
        source=bar.source,
        mode=core_mode,
        price_base=_core_base(basis),
        closed=closed,
        available_at=normalize_utc(bar.available_at, "available_at") if bar.available_at else None,
        received_at=normalize_utc(bar.received_at, "received_at") if bar.received_at else None,
        quality=quality,
        candle_id=core_candle_id,
        origin=str(metadata.get("origin", "provider")),
        metadata=metadata,
    )


def core_bar_to_data(bar: CoreCandle) -> DataBar:
    """Translate a core candle back, restoring revision and source identity."""

    if not isinstance(bar, CoreCandle):
        raise TranslationError(f"se esperaba core.Candle, llegó {type(bar).__name__}", code="TYPE_INVALID")
    core_candle_id = _strict_id(bar.candle_id, name="candle_id")
    basis = _core_base(bar.price_base).value
    mode_enum = _core_mode(bar.mode)
    closed = _strict_bool(bar.closed, name="closed")
    metadata = dict(bar.metadata)
    _validate_metadata_quality(metadata)
    quality = _validate_core_quality(bar.quality)
    identity = _identity_mapping(metadata)
    source_record_id = metadata.get("source_record_id")
    revision_raw = metadata.get("revision", 0)
    if identity:
        source_record_id = identity.get("source_record_id", source_record_id)
        revision_raw = identity.get("revision", revision_raw)
    if source_record_id is not None and (not isinstance(source_record_id, str) or not source_record_id.strip()):
        raise TranslationError("source_record_id inválido", code="IDENTITY_INVALID")
    revision = _strict_nonnegative_int(revision_raw, name="revision")
    synthetic = mode_enum is OperationMode.SYNTHETIC or QualityFlag.SYNTHETIC in quality.flags
    provenance_raw = metadata.get("provenance")
    if provenance_raw is not None:
        if isinstance(provenance_raw, Mapping) and "mode" in provenance_raw:
            data_mode = _data_mode(provenance_raw["mode"], name="provenance.mode")
        elif isinstance(provenance_raw, DataProvenance):
            data_mode = _data_mode(provenance_raw.mode, name="provenance.mode")
        else:
            raise TranslationError("provenance en core no tiene modo interpretable", code="PROVENANCE_INVALID")
    else:
        data_mode = "SYNTHETIC" if synthetic else ("OBSERVACIÓN EN DIRECTO" if mode_enum is OperationMode.LIVE else "REPLAY")
    if synthetic and data_mode != "SYNTHETIC":
        raise TranslationError("quality sintética con provenance.mode no sintético", code="PROVENANCE_CONFLICT")
    fallback = _fallback_provenance(
        source=bar.source,
        mode=data_mode,
        instrument=bar.instrument,
        price_basis=basis,
        resolution=bar.timeframe_name,
        synthetic=synthetic,
        notes=("traducción core.Candle -> data.Bar",),
    )
    provenance = _provenance_mapping(provenance_raw, fallback=fallback)
    metadata = _quality_to_data_metadata(metadata, quality)
    metadata = _metadata_with_translation(
        metadata,
        data_id=core_candle_id,
        provenance=provenance,
        core_candle_id=core_candle_id,
        revision=revision,
    )
    restored_volume = None if metadata.get("volume_was_none") is True else bar.volume
    restored_trade_count = None if metadata.get("trade_count_was_none") is True else bar.event_count
    return DataBar(
        instrument=bar.instrument,
        interval_start=normalize_utc(bar.start, "start"),
        interval_end=normalize_utc(bar.end, "end"),
        open=bar.open,
        high=bar.high,
        low=bar.low,
        close=bar.close,
        resolution_seconds=bar.timeframe.seconds,
        volume=restored_volume,
        trade_count=restored_trade_count,
        price_basis=basis,
        source=bar.source,
        source_record_id=source_record_id or str(bar.candle_id),
        received_at=normalize_utc(bar.received_at, "received_at") if bar.received_at else None,
        available_at=normalize_utc(bar.available_at, "available_at") if bar.available_at else None,
        closed=closed,
        synthetic=synthetic,
        revision=revision,
        metadata=metadata,
    )


# Short aliases make the conversion direction explicit in integrations.
event_to_core = data_event_to_core
core_to_event = core_event_to_data
bar_to_core = data_bar_to_core
core_to_bar = core_bar_to_data
translate_event_to_core = data_event_to_core
translate_core_event = core_event_to_data
translate_bar_to_core = data_bar_to_core
translate_core_bar = core_bar_to_data
data_to_core_event = data_event_to_core
core_to_data_event = core_event_to_data
data_to_core_bar = data_bar_to_core
core_to_data_bar = core_bar_to_data


@dataclass(frozen=True, slots=True)
class ReconciliationItem:
    key: tuple[str, str, datetime]
    status: Literal["MATCH", "MISMATCH", "MISSING_NATIVE", "MISSING_EVENTS", "BLOCKED"]
    native: CoreCandle | None = None
    event_bar: CoreCandle | None = None
    reasons: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": {"instrument": self.key[0], "timeframe": self.key[1], "start": isoformat_utc(self.key[2])},
            "status": self.status,
            "reasons": list(self.reasons),
            "native_candle_id": self.native.candle_id if self.native else None,
            "event_bar_id": self.event_bar.candle_id if self.event_bar else None,
        }


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    timeframe: Timeframe
    instrument: str
    native_candles: tuple[CoreCandle, ...]
    event_bars: tuple[CoreCandle, ...]
    items: tuple[ReconciliationItem, ...]
    blocking_reasons: tuple[str, ...] = ()
    invented_event_count: int = 0

    @property
    def matches(self) -> tuple[ReconciliationItem, ...]:
        return tuple(item for item in self.items if item.status == "MATCH")

    @property
    def mismatches(self) -> tuple[ReconciliationItem, ...]:
        return tuple(item for item in self.items if item.status == "MISMATCH")

    @property
    def missing_native(self) -> tuple[ReconciliationItem, ...]:
        return tuple(item for item in self.items if item.status == "MISSING_NATIVE")

    @property
    def missing_events(self) -> tuple[ReconciliationItem, ...]:
        return tuple(item for item in self.items if item.status == "MISSING_EVENTS")

    @property
    def matched_count(self) -> int:
        return sum(item.status == "MATCH" for item in self.items)

    @property
    def mismatch_count(self) -> int:
        return sum(item.status == "MISMATCH" for item in self.items)

    @property
    def missing_native_count(self) -> int:
        return sum(item.status == "MISSING_NATIVE" for item in self.items)

    @property
    def missing_events_count(self) -> int:
        return sum(item.status == "MISSING_EVENTS" for item in self.items)

    @property
    def blocked(self) -> bool:
        return bool(self.blocking_reasons) or any(item.status in {"MISMATCH", "MISSING_NATIVE", "MISSING_EVENTS", "BLOCKED"} for item in self.items)

    def to_dict(self) -> dict[str, Any]:
        return {
            "instrument": self.instrument,
            "timeframe": self.timeframe.name,
            "native_count": len(self.native_candles),
            "event_bar_count": len(self.event_bars),
            "matched_count": self.matched_count,
            "mismatch_count": self.mismatch_count,
            "missing_native_count": self.missing_native_count,
            "missing_events_count": self.missing_events_count,
            "blocked": self.blocked,
            "blocking_reasons": list(self.blocking_reasons),
            "invented_event_count": self.invented_event_count,
            "items": [item.to_dict() for item in self.items],
        }


def _event_key(event: CoreEvent, timeframe: Timeframe) -> tuple[str, str, datetime]:
    seconds = math.floor(event.event_time.timestamp() / timeframe.seconds) * timeframe.seconds
    start = datetime.fromtimestamp(seconds, tz=UTC)
    return event.instrument, timeframe.name, start


def _core_event_bar(events: Sequence[CoreEvent], timeframe: Timeframe, *, instrument: str) -> CoreCandle:
    ordered = sorted(
        events,
        key=lambda event: (
            event.event_time,
            event.effective_available_at,
            event.received_at or event.event_time,
            (0, event.sequence) if isinstance(event.sequence, (int, float)) and not isinstance(event.sequence, bool) else (1, str(event.sequence or "")),
            event.event_id or "",
        ),
    )
    prices = [event.selected_price for event in ordered]
    if not prices or any(price is None or not math.isfinite(float(price)) for price in prices):
        raise TranslationError("eventos sin precio seleccionado para reconciliar", code="PRICE_BASE_MISSING")
    start = _event_key(ordered[0], timeframe)[2]
    end = start + timeframe.delta
    quality = merge_quality(*(event.quality for event in ordered), source="reconciled-events")
    available = max(event.effective_available_at for event in ordered)
    closed = all(event.effective_available_at >= end for event in ordered)
    if not closed:
        quality = quality.with_flags(QualityFlag.OPEN, reason="eventos disponibles antes del cierre del intervalo")
    mode = OperationMode.SYNTHETIC if all(event.mode is OperationMode.SYNTHETIC for event in ordered) else (OperationMode.LIVE if any(event.mode is OperationMode.LIVE for event in ordered) else OperationMode.REPLAY)
    base = ordered[0].price_base
    if any(event.price_base is not base for event in ordered):
        raise TranslationError("eventos de bases de precio mezcladas en una misma vela", code="PRICE_BASE_CONFLICT")
    event_ids = [event.source_event_id or event.event_id for event in ordered]
    return CoreCandle(
        instrument=instrument,
        timeframe=timeframe,
        start=start,
        end=end,
        open=float(prices[0]),
        high=max(float(price) for price in prices),
        low=min(float(price) for price in prices),
        close=float(prices[-1]),
        volume=sum(float(event.quantity or 0.0) for event in ordered),
        event_count=len(ordered),
        source="reconciled-events",
        mode=mode,
        price_base=base,
        closed=closed,
        available_at=available,
        received_at=max((event.received_at or event.event_time for event in ordered), default=None),
        quality=quality,
        origin="reconciled",
        metadata={"reconciled_from_event_ids": event_ids, "no_ticks_invented": True},
    )


def reconcile_native_candles(
    native: Iterable[DataBar | CoreCandle],
    events: Iterable[DataEvent | CoreEvent],
    timeframe: Timeframe | str,
    *,
    instrument: str | None = None,
    tolerance: float = 1e-9,
    compare_volume: bool = False,
) -> ReconciliationResult:
    """Compare provider-native candles with candles from real events only.

    ``native`` and ``events`` are copied into tuples; neither input is mutated.
    A missing side, invalid quality, discontinuity, or numeric mismatch is a
    blocking reason.  The helper is appropriate for bootstrap/recovery checks,
    not for silently choosing whichever source is more favorable.
    """

    if tolerance < 0 or not math.isfinite(float(tolerance)):
        raise TranslationError("tolerance debe ser finita y no negativa", code="CONFIG_INVALID")
    tf = parse_timeframe(timeframe)
    native_core: list[CoreCandle] = []
    event_core: list[CoreEvent] = []
    blocking: list[str] = []
    for item in native:
        try:
            candidate = data_bar_to_core(item) if isinstance(item, DataBar) else item
            if not isinstance(candidate, CoreCandle):
                raise TranslationError("native contiene un tipo distinto de Bar/Candle", code="TYPE_INVALID")
            validation = validate_candle(candidate, require_closed=False)
            if not validation.accepted:
                blocking.extend(f"native {candidate.candle_id}: {issue.code}" for issue in validation.issues)
            native_core.append(candidate)
        except TranslationError as exc:
            blocking.append(str(exc))
    for item in events:
        try:
            candidate = data_event_to_core(item) if isinstance(item, DataEvent) else item
            if not isinstance(candidate, CoreEvent):
                raise TranslationError("events contiene un tipo distinto de Event/MarketEvent", code="TYPE_INVALID")
            validation = validate_event(candidate)
            if not validation.accepted:
                blocking.extend(f"event {candidate.event_id}: {issue.code}" for issue in validation.issues)
            elif not candidate.quality.valid:
                blocking.extend(f"event {candidate.event_id}: quality_blocked")
            event_core.append(candidate)
        except TranslationError as exc:
            blocking.append(str(exc))
    if instrument is None:
        candidates = {c.instrument for c in native_core} | {e.instrument for e in event_core}
        if len(candidates) == 1:
            instrument = next(iter(candidates))
        elif not candidates:
            instrument = "unknown"
        else:
            blocking.append(f"instrumentos mezclados: {sorted(candidates)}")
            instrument = sorted(candidates)[0]
    if not instrument:
        blocking.append("instrumento vacío")
        instrument = "unknown"
    if not native_core and not event_core:
        blocking.append("sin velas nativas ni eventos para reconciliar")
    native_by_key: dict[tuple[str, str, datetime], CoreCandle] = {}
    for candle in native_core:
        key = (candle.instrument, candle.timeframe_name, candle.start)
        if key in native_by_key:
            blocking.append(f"vela nativa duplicada: {key}")
        else:
            native_by_key[key] = candle
    events_by_key: dict[tuple[str, str, datetime], list[CoreEvent]] = defaultdict(list)
    seen_event_ids: set[str] = set()
    for event in event_core:
        if event.instrument != instrument:
            blocking.append(f"evento de instrumento distinto: {event.instrument}")
        if event.event_id in seen_event_ids:
            blocking.append(f"evento duplicado: {event.event_id}")
        seen_event_ids.add(event.event_id)
        events_by_key[_event_key(event, tf)].append(event)
    event_bars_by_key: dict[tuple[str, str, datetime], CoreCandle] = {}
    for key, grouped in events_by_key.items():
        try:
            event_bar = _core_event_bar(grouped, tf, instrument=key[0])
            event_bars_by_key[key] = event_bar
            if not event_bar.closed:
                blocking.append(f"{key}: event-derived candle remains open")
            if not event_bar.quality.valid:
                blocking.append(f"{key}: event-derived candle quality blocked")
        except TranslationError as exc:
            blocking.append(f"{key}: {exc}")
    items: list[ReconciliationItem] = []
    for key in sorted(set(native_by_key) | set(event_bars_by_key), key=lambda value: (value[0], value[1], value[2])):
        native_bar = native_by_key.get(key)
        event_bar = event_bars_by_key.get(key)
        if native_bar is None:
            items.append(ReconciliationItem(key, "MISSING_NATIVE", event_bar=event_bar, reasons=("no existe vela nativa para los eventos",)))
            continue
        if event_bar is None:
            items.append(ReconciliationItem(key, "MISSING_EVENTS", native=native_bar, reasons=("no existen eventos observados para la vela nativa",)))
            continue
        reasons: list[str] = []
        if native_bar.price_base is not event_bar.price_base:
            reasons.append(f"price_base: native={native_bar.price_base.value} events={event_bar.price_base.value}")
        if not native_bar.closed:
            reasons.append("native candle is open")
        for field_name in ("open", "high", "low", "close"):
            left = float(getattr(native_bar, field_name))
            right = float(getattr(event_bar, field_name))
            if not math.isclose(left, right, rel_tol=tolerance, abs_tol=tolerance):
                reasons.append(f"{field_name}: native={left!r} events={right!r}")
        if compare_volume and not math.isclose(float(native_bar.volume), float(event_bar.volume), rel_tol=tolerance, abs_tol=tolerance):
            reasons.append(f"volume: native={native_bar.volume!r} events={event_bar.volume!r}")
        status: Literal["MATCH", "MISMATCH"] = "MISMATCH" if reasons else "MATCH"
        items.append(ReconciliationItem(key, status, native=native_bar, event_bar=event_bar, reasons=tuple(reasons)))
    return ReconciliationResult(
        timeframe=tf,
        instrument=instrument,
        native_candles=tuple(native_core),
        event_bars=tuple(event_bars_by_key[key] for key in sorted(event_bars_by_key, key=lambda value: (value[0], value[1], value[2]))),
        items=tuple(items),
        blocking_reasons=tuple(dict.fromkeys(blocking)),
        invented_event_count=0,
    )


# Name emphasizing the bootstrap use case for callers.
bootstrap_reconcile = reconcile_native_candles


__all__ = [
    "ReconciliationItem",
    "ReconciliationResult",
    "TranslationError",
    "bar_to_core",
    "bootstrap_reconcile",
    "translate_bar_to_core",
    "translate_core_bar",
    "data_to_core_event",
    "core_to_data_event",
    "data_to_core_bar",
    "core_to_data_bar",
    "translate_core_event",
    "translate_event_to_core",
    "core_bar_to_data",
    "core_event_to_data",
    "core_to_bar",
    "core_to_event",
    "data_bar_to_core",
    "data_event_to_core",
    "event_to_core",
    "reconcile_native_candles",
]
