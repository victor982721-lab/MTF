"""Explicit translations from detector and cTrader quotes to the PAPER product."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from typing import Any

from ..core.canonical import instant_text
from ..core.strategy import Signal
from ..data.capture import CaptureContractError as CTraderPipelineError
from ..data.capture import parse_instant
from ..data.models import Event
from ..runtime.state import signal_from_dict
from .cfd_simulation import CFDQuote, CFDSignal, Direction


def _iso(value: datetime | None) -> str | None:
    return instant_text(value) if value else None


def signal_to_cfd_signal(
    signal: Signal | Mapping[str, Any], *, capture_hash: str | None = None, strategy: str = "trend_pullback_v1"
) -> CFDSignal:
    """Adaptador puro: sólo acepta señales emitidas por el detector."""

    if isinstance(signal, Mapping):
        signal = signal_from_dict(signal)
    if not isinstance(signal, Signal):
        raise CTraderPipelineError("se esperaba Signal del detector RuntimeCoordinator")
    metadata = {
        "source": "RuntimeCoordinator",
        "capture_hash": capture_hash,
        "episode_id": signal.episode_id,
        "context_start": _iso(signal.context_start),
        "preparation_start": _iso(signal.preparation_start),
        "trigger_start": _iso(signal.trigger_start),
        "trigger_end": _iso(signal.trigger_end),
        "values": dict(signal.values),
        "mode": signal.mode.value,
        "quality_flags": sorted(flag.value for flag in signal.quality.flags),
    }
    direction = {"UP": Direction.LONG, "DOWN": Direction.SHORT}.get(signal.direction)
    if direction is None:
        raise CTraderPipelineError("detector signal direction must be UP or DOWN")
    return CFDSignal(
        signal_id=signal.signal_id,
        instrument=signal.instrument,
        direction=direction,
        detected_at=signal.detected_at,
        available_at=signal.detected_at,
        strategy=strategy,
        quality=signal.quality.status,
        metadata=metadata,
    )


_PROVIDER_QUALITY = {
    "VALID": "VALID",
    "INCOMPLETE": "UNKNOWN",
    "UNKNOWN": "UNKNOWN",
    "STALE": "STALE",
    "INVALID": "INVALID",
    "DISCONNECTED": "DISCONNECTED",
}
_PROVIDER_REASONS = {
    "MISSING_BID": "MISSING_BID",
    "MISSING_ASK": "MISSING_ASK",
    "MISSING_SOURCE_TIMESTAMP": "MISSING_SOURCE_TIMESTAMP",
    "STALE_BID": "STALE",
    "STALE_ASK": "STALE",
    "CROSSED": "CROSSED",
    "OUT_OF_ORDER": "OUT_OF_ORDER",
    "REJECTED_UPDATE": "OUT_OF_ORDER",
    "SNAPSHOT": "SNAPSHOT",
    "DISCONNECTED": "DISCONNECTED",
    "SESSION_CHANGED": "SESSION_MISMATCH",
    "AVAILABILITY_UNKNOWN": "MISSING_SOURCE_TIMESTAMP",
}
_CORE_BLOCKING_FLAGS = frozenset(
    {
        "invalid",
        "out_of_order",
        "gap",
        "disconnected",
        "stale",
        "unreconciled",
        "insufficient",
        "partial",
        "open",
        "late",
        "duplicate",
    }
)


def _quote_evidence(metadata: Mapping[str, Any]) -> tuple[str, tuple[str, ...]]:
    raw = metadata.get("quote_quality", {})
    if not isinstance(raw, Mapping):
        raise CTraderPipelineError("provider quote_quality must be a typed mapping")
    quality = _PROVIDER_QUALITY.get(str(raw.get("state", "UNKNOWN")), "UNKNOWN")
    reasons = tuple(
        dict.fromkeys(_PROVIDER_REASONS.get(reason, "INVALID_QUALITY") for reason in raw.get("reasons", ()))
    )
    flags = set(metadata.get("quality_flags", ()))
    core_quality = metadata.get("quality")
    if isinstance(core_quality, Mapping):
        flags.update(core_quality.get("flags", ()))
    if flags & _CORE_BLOCKING_FLAGS and quality == "VALID":
        quality = "INVALID"
    if metadata.get("availability_policy") not in {None, "observed"}:
        quality = "UNKNOWN"
        reasons = (*reasons, "MISSING_SOURCE_TIMESTAMP")
    return quality, reasons


def _side_evidence(metadata: Mapping[str, Any], side: str) -> Mapping[str, Any]:
    raw = metadata.get("quote_quality", {})
    if not isinstance(raw, Mapping):
        raise CTraderPipelineError("provider quote quality is malformed")
    evidence = raw.get(side, {})
    if not isinstance(evidence, Mapping):
        raise CTraderPipelineError(f"provider {side} evidence is malformed")
    return evidence


def spot_event_to_cfd_quote(event: Event, *, capture_hash: str | None = None) -> CFDQuote:
    """Preserve side evidence; never promote a stale/unknown quote to VALID.

    One-sided updates are valid observations, not automatic executable quotes.
    Price float->Decimal text is deliberate at the provider boundary (cTrader
    relative prices are bounded by its declared scale); accounting stays Decimal.
    """
    if not isinstance(event, Event):
        raise CTraderPipelineError("expected a normalized cTrader Event")
    metadata = dict(event.metadata)
    quality, reasons = _quote_evidence(metadata)
    bid = _side_evidence(metadata, "bid")
    ask = _side_evidence(metadata, "ask")
    if event.is_snapshot:
        quality = "SNAPSHOT"
    elif quality == "VALID" and metadata.get("synthetic_fixture", False):
        quality = "SYNTHETIC"
    metadata.update(
        {
            "capture_hash": capture_hash,
            "event_data_id": event.data_id,
            "source_event_id": event.source_event_id,
            "is_snapshot": event.is_snapshot,
        }
    )
    return CFDQuote(
        instrument=event.instrument,
        market_time=event.event_time,
        bid=Decimal(str(event.bid)) if event.bid is not None else None,
        ask=Decimal(str(event.ask)) if event.ask is not None else None,
        quote_id=event.source_event_id or event.data_id,
        available_at=event.effective_available_at,
        source=event.source,
        sequence=event.source_sequence,
        quality=quality,
        quote_quality=quality,
        quote_reasons=reasons,
        metadata=metadata,
        bid_source_timestamp=parse_instant(bid.get("source_timestamp")),
        ask_source_timestamp=parse_instant(ask.get("source_timestamp")),
        bid_received_at=parse_instant(bid.get("received_at")),
        ask_received_at=parse_instant(ask.get("received_at")),
        bid_available_at=parse_instant(bid.get("available_at")),
        ask_available_at=parse_instant(ask.get("available_at")),
        bid_source_timestamp_missing=bool(bid.get("timestamp_missing", True)),
        ask_source_timestamp_missing=bool(ask.get("timestamp_missing", True)),
        bid_quality=_PROVIDER_QUALITY.get(bid.get("state", "UNKNOWN"), "UNKNOWN"),
        ask_quality=_PROVIDER_QUALITY.get(ask.get("state", "UNKNOWN"), "UNKNOWN"),
        connection_generation=metadata.get("connection_generation", metadata.get("quote_generation", 0)),
        is_snapshot=event.is_snapshot,
    )
