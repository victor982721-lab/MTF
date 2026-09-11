"""Causal normalization use case; no authorization or transport lifecycle.

``normalize_ctrader_capture`` is the explicit materialized inspection API.
Long replays use ``CausalNormalizer`` with the application session instead.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import Any

from ..core.canonical import fingerprint, instant_text
from ..data.capture import (
    CAPTURE_VERSION,
    CaptureContractError,
    CaptureEnvelope,
    CaptureOrder,
    MessageClass,
    capture_fingerprint,
    envelope_from_raw,
    iter_capture_order,
    parse_instant,
)
from ..data.ctrader import (
    CTraderConfig,
    CTraderInstrumentSpec,
    CTraderNormalizationResult,
    CTraderProvider,
    DeterministicTransport,
    WireMessage,
)
from ..data.models import Bar, Event

NORMALIZATION_VERSION = 2
CTraderPipelineError = CaptureContractError


def payload_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, WireMessage):
        value = value.payload
    if isinstance(value, Mapping):
        return value
    # Optional SDK decoding stays at this boundary, never at module import.
    from ..data.ctrader_protocol import message_to_mapping

    mapped = message_to_mapping(value)
    if not isinstance(mapped, Mapping):
        raise CaptureContractError("SpotEvent must be a mapping or Protobuf message")
    return mapped


def capture_envelopes(
    rows: Iterable[Any],
    *,
    received_at: datetime | Callable[[int, Mapping[str, Any]], datetime] | None = None,
) -> Iterable[CaptureEnvelope]:
    for index, value in enumerate(rows):
        if isinstance(value, CaptureEnvelope):
            yield value
            continue
        if isinstance(value, WireMessage) and value.ingest_sequence is not None:
            yield CaptureEnvelope.from_mapping(value.capture_envelope())
            continue
        raw = payload_mapping(value)
        receipt = received_at(index, raw) if callable(received_at) else received_at
        yield envelope_from_raw(raw, index, received_at=receipt)


@dataclass(frozen=True, slots=True)
class CaptureCoverage:
    """Coverage, dataset end, continuity and quality are independent facts."""

    requested_start: datetime | None = None
    requested_end: datetime | None = None
    observed_start: datetime | None = None
    observed_end: datetime | None = None
    dataset_end_declared: bool = False
    continuity: str = "UNKNOWN"
    availability_known: bool = False

    def __post_init__(self) -> None:
        for name in ("requested_start", "requested_end", "observed_start", "observed_end"):
            object.__setattr__(self, name, parse_instant(getattr(self, name)))
        if not isinstance(self.dataset_end_declared, bool) or not isinstance(self.availability_known, bool):
            raise CaptureContractError("coverage declarations require boolean evidence")
        if self.continuity not in {"UNKNOWN", "CONTINUOUS", "DISCONTINUOUS"}:
            raise CaptureContractError("unknown continuity evidence")
        for start, end in ((self.requested_start, self.requested_end), (self.observed_start, self.observed_end)):
            if start and end and start > end:
                raise CaptureContractError("coverage interval is reversed")

    @property
    def complete(self) -> bool:
        if not self.dataset_end_declared or self.continuity != "CONTINUOUS":
            return False
        if self.observed_start is None or self.observed_end is None:
            return False
        if self.requested_start and self.observed_start > self.requested_start:
            return False
        return self.requested_end is None or self.observed_end >= self.requested_end

    def to_dict(self) -> dict[str, Any]:
        return {
            "requested_start": instant_text(self.requested_start) if self.requested_start else None,
            "requested_end": instant_text(self.requested_end) if self.requested_end else None,
            "observed_start": instant_text(self.observed_start) if self.observed_start else None,
            "observed_end": instant_text(self.observed_end) if self.observed_end else None,
            "dataset_end_declared": self.dataset_end_declared,
            "continuity": self.continuity,
            "availability_known": self.availability_known,
            "coverage_satisfied": self.complete,
        }


@dataclass(frozen=True, slots=True)
class CTraderCapture:
    payloads: tuple[Mapping[str, Any], ...]
    records: tuple[Event | Bar, ...]
    quote_events: tuple[Event, ...]
    bars: tuple[Bar, ...]
    issues: tuple[str, ...]
    capture_id: str
    capture_hash: str
    provenance: Mapping[str, Any]
    snapshot_count: int = 0
    envelopes: tuple[CaptureEnvelope, ...] = ()
    coverage: CaptureCoverage = field(default_factory=CaptureCoverage)
    record_count: int | None = None
    quote_count: int | None = None
    bar_count: int | None = None

    @property
    def events(self) -> tuple[Event, ...]:
        return self.quote_events

    @property
    def is_complete(self) -> bool:
        return self.coverage.complete

    def to_dict(self, *, include_payloads: bool = False) -> dict[str, Any]:
        result = {
            "schema_version": CAPTURE_VERSION,
            "normalization_version": NORMALIZATION_VERSION,
            "capture_id": self.capture_id,
            "capture_hash": self.capture_hash,
            "record_count": len(self.records) if self.record_count is None else self.record_count,
            "quote_event_count": len(self.quote_events) if self.quote_count is None else self.quote_count,
            "bar_count": len(self.bars) if self.bar_count is None else self.bar_count,
            "snapshot_count": self.snapshot_count,
            "synthetic": bool(self.provenance.get("synthetic", False)),
            "issues": list(self.issues),
            "provenance": dict(self.provenance),
            "coverage": self.coverage.to_dict(),
        }
        if include_payloads:
            result["payloads"] = [dict(item) for item in self.payloads]
            result["envelopes"] = [item.to_dict() for item in self.envelopes]
        return result


def _metadata(record: Event | Bar, envelope: CaptureEnvelope, mode: str) -> dict[str, Any]:
    result = dict(record.metadata)
    source_quality = result.get("quality")
    if isinstance(source_quality, str):
        result["source_quality"] = result.pop("quality")
    synthetic = bool(envelope.payload.get("synthetic_fixture", envelope.payload.get("synthetic", False)))
    result.update(
        {
            "mode": mode,
            "source_mode": "SYNTHETIC_FIXTURE" if synthetic else "LIVE",
            "synthetic_fixture": synthetic,
            "capture_schema": CAPTURE_VERSION,
            "normalization_version": NORMALIZATION_VERSION,
            "ingest_sequence": envelope.ingest_sequence,
            "connection_generation": envelope.connection_generation,
            "availability_policy": envelope.availability_policy,
            "original_received_at": instant_text(envelope.received_at) if envelope.received_at else None,
            "source_identity": envelope.source_identity,
            "provenance": {
                "provider": "ctrader-open-api",
                "mode": mode,
                "instrument": record.instrument,
                "price_basis": record.price_basis,
                "synthetic": synthetic,
                "source_mode": "SYNTHETIC_FIXTURE" if synthetic else "LIVE",
            },
        }
    )
    if envelope.availability_policy != "observed":
        flags = set(result.get("quality_flags", ()))
        flags.add("insufficient")
        result["quality_flags"] = sorted(flags)
        result["quality_reasons"] = [*result.get("quality_reasons", ()), "original_receipt_unknown"]
    return result


class CausalNormalizer:
    """One normalization state per application session and connection generation."""

    def __init__(
        self,
        spec: CTraderInstrumentSpec,
        *,
        quote_basis: str = "mid",
        mode: str = "REPLAY",
        max_quote_age_seconds: float = 90.0,
    ) -> None:
        self.spec = spec
        self.mode = mode
        self.quote_basis = quote_basis
        self.max_quote_age_seconds = max_quote_age_seconds
        self.generation: int | None = None
        self.provider = CTraderProvider(
            CTraderConfig(
                symbol=spec.symbol,
                symbol_id=spec.symbol_id,
                digits=spec.digits,
                pip_position=spec.pip_position,
                price_scale=spec.price_scale,
                quote_basis=quote_basis,
            ),
            transport=DeterministicTransport(),
            max_quote_age_seconds=max_quote_age_seconds,
        )

    def normalize(self, envelope: CaptureEnvelope) -> CTraderNormalizationResult:
        if envelope.available_at is None:
            raise CaptureContractError("normalization requires declared availability")
        if self.generation != envelope.connection_generation:
            self.provider.reset_generation(envelope.connection_generation)
            self.generation = envelope.connection_generation
        if envelope.message_class is MessageClass.CONNECTION:
            self.provider.reset_discontinuity()
        if envelope.message_class not in {MessageClass.SPOT, MessageClass.REVISION}:
            return CTraderNormalizationResult((), (), ())
        payload_time = parse_instant(envelope.payload.get("timestamp"), unit="ms")
        if payload_time is not None and envelope.event_time is not None and payload_time != envelope.event_time:
            raise CaptureContractError("SpotEvent timestamp and envelope event_time disagree")
        result = self.provider.normalize_spot(
            envelope.payload,
            received_at=envelope.received_at,
            available_at=envelope.available_at,
            snapshot=bool(envelope.payload.get("snapshot", envelope.payload.get("isSnapshot", False))),
            sequence=envelope.ingest_sequence,
        )
        quotes = tuple(self._event(item, envelope) for item in result.quote_events)
        bars = tuple(self._bar(item, envelope) for item in result.bars)
        return replace(result, records=(*quotes, *bars), quote_events=quotes, bars=bars)

    def _event(self, event: Event, envelope: CaptureEnvelope) -> Event:
        return replace(
            event,
            received_at=envelope.received_at,
            available_at=envelope.available_at,
            source_event_id=envelope.observation_id,
            source_sequence=envelope.ingest_sequence,
            metadata=_metadata(event, envelope, self.mode),
        )

    def _bar(self, bar: Bar, envelope: CaptureEnvelope) -> Bar:
        return replace(
            bar,
            received_at=envelope.received_at,
            available_at=envelope.available_at,
            metadata=_metadata(bar, envelope, self.mode),
        )

    def snapshot(self) -> dict[str, Any]:
        return {
            "version": NORMALIZATION_VERSION,
            "generation": self.generation,
            "quote_state": self.provider.snapshot_quote_state(),
        }

    def restore(self, state: Mapping[str, Any]) -> None:
        if state["version"] != NORMALIZATION_VERSION:
            raise CaptureContractError("normalization snapshot version mismatch")
        self.generation = state["generation"]
        self.provider.restore_quote_state(state["quote_state"])


def capture_identity(envelope_hash: str, spec: CTraderInstrumentSpec, quote_basis: str) -> str:
    return fingerprint(
        {
            "envelopes": envelope_hash,
            "normalizer": NORMALIZATION_VERSION,
            "basis": quote_basis,
            "instrument": spec.to_dict(),
        }
    )


def observed_coverage(envelopes: tuple[CaptureEnvelope, ...], coverage: CaptureCoverage | None) -> CaptureCoverage:
    instants = [item.available_at for item in envelopes if item.available_at is not None]
    requested = coverage or CaptureCoverage()
    return replace(
        requested,
        observed_start=min(instants) if instants else None,
        observed_end=max(instants) if instants else None,
        availability_known=all(item.received_at is not None for item in envelopes),
    )


def normalize_ctrader_capture(
    payloads: Iterable[Any],
    *,
    spec: CTraderInstrumentSpec,
    quote_basis: str = "mid",
    mode: str = "REPLAY",
    received_at: datetime | Callable[[int, Mapping[str, Any]], datetime] | None = None,
    order: CaptureOrder = "as_observed",
    coverage: CaptureCoverage | None = None,
) -> CTraderCapture:
    """Materialized inspection preserving recorded availability and sequence.

    Raw legacy files preserve physical order as their ingestion sequence; their
    event timestamp never masquerades as original reception. For large replays
    use ``CTraderPipeline.run`` or feed envelopes to ``CTraderPaperSession``.
    """
    envelopes = tuple(iter_capture_order(capture_envelopes(payloads, received_at=received_at), mode=order))
    normalizer = CausalNormalizer(spec, quote_basis=quote_basis, mode=mode)
    records: list[Event | Bar] = []
    quotes: list[Event] = []
    bars: list[Bar] = []
    issues: list[str] = []
    snapshots = 0
    for envelope in envelopes:
        result = normalizer.normalize(envelope)
        quotes.extend(result.quote_events)
        bars.extend(result.bars)
        records.extend(result.bars)
        records.extend(result.quote_events)
        issues.extend(result.issues)
        snapshots += int(result.snapshot)
    digest = capture_identity(capture_fingerprint(envelopes, mode=order), spec, quote_basis)
    observed = observed_coverage(envelopes, coverage)
    synthetic = any(
        bool(item.payload.get("synthetic_fixture", item.payload.get("synthetic", False))) for item in envelopes
    )
    provenance = {
        "provider": "ctrader-open-api",
        "instrument": spec.symbol,
        "symbol_id": spec.symbol_id,
        "price_basis": quote_basis,
        "mode": mode,
        "order": order,
        "source_mode": "SYNTHETIC_FIXTURE" if synthetic else "LIVE",
        "synthetic": synthetic,
        "capture_hash": digest,
        "capture_version": CAPTURE_VERSION,
        "normalization_version": NORMALIZATION_VERSION,
        "resolutions": sorted({item.resolution for item in bars}),
        "coverage_start": instant_text(observed.observed_start) if observed.observed_start else None,
        "coverage_end": instant_text(observed.observed_end) if observed.observed_end else None,
    }
    return CTraderCapture(
        tuple(item.to_dict()["payload"] for item in envelopes if item.message_class is MessageClass.SPOT),
        tuple(records),
        tuple(quotes),
        tuple(bars),
        tuple(dict.fromkeys(issues)),
        "cap_" + digest[:32],
        digest,
        provenance,
        snapshots,
        envelopes,
        observed,
    )


def synthetic_ctrader_capture(
    *, start: datetime = datetime(2026, 1, 1, tzinfo=UTC), symbol_id: int = 99, count: int = 190, mode: str = "REPLAY"
) -> CTraderCapture:
    """Explicitly complete synthetic dataset with durable observed envelopes."""
    from ..data.paper_fixture import synthetic_ctrader_payloads

    rows = synthetic_ctrader_payloads(start=start, symbol_id=symbol_id, count=count)

    def receipt(index: int, raw: Mapping[str, Any]) -> datetime:
        instant = parse_instant(raw["timestamp"], unit="ms")
        assert instant is not None
        return instant + timedelta(seconds=1)

    coverage = CaptureCoverage(
        requested_start=start + timedelta(minutes=1, seconds=1),
        requested_end=start + timedelta(minutes=count, seconds=1),
        dataset_end_declared=True,
        continuity="CONTINUOUS",
    )
    envelopes = list(capture_envelopes(rows, received_at=receipt))
    end = start + timedelta(minutes=count, seconds=1)
    assert coverage.requested_start is not None
    envelopes.append(
        CaptureEnvelope(
            end,
            end,
            end,
            count,
            0,
            MessageClass.END,
            {
                "continuity": "CONTINUOUS",
                "requested_start": instant_text(coverage.requested_start),
                "requested_end": instant_text(end),
            },
        )
    )
    return normalize_ctrader_capture(
        envelopes, spec=CTraderInstrumentSpec(symbol_id=symbol_id), mode=mode, coverage=coverage
    )
