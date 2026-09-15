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
    normalize_trendbar,
)
from ..data.ctrader_errors import CTraderDataError
from ..data.ctrader_protocol import TREND_PERIODS, read_field, read_repeated
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
    # Historical replay keeps the original response receipt separately from
    # the market-time watermark used for corrected ordering.  A non-observed
    # policy therefore does not, by itself, mean that the receipt is unknown:
    # an exported historical page has ``historical_event_time`` plus a real
    # ``received_at``.  Only an explicitly unknown policy or a missing receipt
    # is insufficient evidence for quality purposes.
    if envelope.availability_policy == "unknown" or envelope.received_at is None:
        flags = set(result.get("quality_flags", ()))
        flags.add("insufficient")
        result["quality_flags"] = sorted(flags)
        result["quality_reasons"] = [*result.get("quality_reasons", ()), "original_receipt_unknown"]
    return result


def _bounded_selection_marker(envelope: CaptureEnvelope) -> bool:
    """Return the exporter marker for a gap-checked bounded history page."""

    payload = envelope.payload
    if payload.get("bounded_selection_verified") is True:
        return True
    marker = payload.get("bounded_selection")
    if isinstance(marker, Mapping):
        return True
    provenance = payload.get("capture_provenance")
    return isinstance(provenance, Mapping) and provenance.get("bounded_selection_verified") is True


def _bounded_selection_marked(envelopes: tuple[CaptureEnvelope, ...]) -> bool:
    pages = tuple(item for item in envelopes if item.message_class is MessageClass.TRENDBAR)
    ends = tuple(item for item in envelopes if item.message_class is MessageClass.END)
    # Older bounded exports marked every trendbar page but predated the END
    # marker.  Accept that additive shape while rejecting an explicit false
    # marker on an END envelope.
    return (
        bool(pages)
        and all(_bounded_selection_marker(item) for item in pages)
        and not any(
            not _bounded_selection_marker(item) for item in ends if "bounded_selection_verified" in item.payload
        )
    )


def _period_key(value: Any) -> tuple[str, Any] | None:
    """Return a comparable protocol period identity without selecting one."""

    if value is None:
        return None
    if isinstance(value, bool):
        return ("invalid", value)
    if isinstance(value, int):
        return ("code", value)
    text = str(value).strip().upper()
    return ("code", TREND_PERIODS[text]) if text in TREND_PERIODS else ("raw", text)


def _historical_provenance(
    payload: Mapping[str, Any],
) -> tuple[Mapping[str, Any], Mapping[str, Any], list[str]]:
    issues: list[str] = []
    raw_provenance = read_field(payload, "capture_provenance", default=None)
    provenance: Mapping[str, Any] = raw_provenance if isinstance(raw_provenance, Mapping) else {}
    if raw_provenance is not None and not isinstance(raw_provenance, Mapping):
        issues.append("historical capture_provenance must be a mapping")
    raw_period_context = provenance.get("period_context")
    period_context: Mapping[str, Any] = raw_period_context if isinstance(raw_period_context, Mapping) else {}
    if raw_period_context is not None and not isinstance(raw_period_context, Mapping):
        issues.append("historical capture_provenance.period_context must be a mapping")
    return provenance, period_context, issues


def _historical_requested_period(provenance: Mapping[str, Any], period_context: Mapping[str, Any]) -> Any | None:
    for source in (provenance, period_context):
        value = source.get("requested_period")
        if value is not None:
            return value
    request = provenance.get("request")
    if not isinstance(request, Mapping):
        return None
    request_payload = request.get("payload")
    return read_field(request_payload, "period", default=None) if isinstance(request_payload, Mapping) else None


def _historical_response_period(
    payload: Mapping[str, Any], provenance: Mapping[str, Any], period_context: Mapping[str, Any]
) -> tuple[Any | None, Any | None]:
    provenance_response = provenance.get("response_period")
    if provenance_response is None:
        provenance_response = period_context.get("response_period")
    page_response = read_field(payload, "period", default=None)
    return (provenance_response if provenance_response is not None else page_response), provenance_response


def _period_discrepancy(left_name: str, left: Any, right_name: str, right: Any) -> str | None:
    if left is None or right is None or _period_key(left) == _period_key(right):
        return None
    return f"trendbar period discrepancy: {left_name}={left!r}, {right_name}={right!r}"


def _historical_period_context(payload: Mapping[str, Any]) -> tuple[Any | None, Any | None, tuple[str, ...]]:
    """Read explicit request/response period evidence from a history page.

    ``ProtoOATrendbar.period`` is optional.  The page response and the capture
    provenance are therefore separate evidence sources; if both are present,
    they must agree before any child is normalized.  No timeframe is selected
    here: ``normalize_trendbar`` remains the single protocol resolver.
    """

    provenance, period_context, issues = _historical_provenance(payload)
    requested_period = _historical_requested_period(provenance, period_context)
    response_period, provenance_response = _historical_response_period(payload, provenance, period_context)
    page_response = read_field(payload, "period", default=None)
    for item in (
        _period_discrepancy("requested", requested_period, "response", response_period),
        _period_discrepancy("provenance_response", provenance_response, "page_response", page_response),
    ):
        if item is not None:
            issues.append(item)
    return requested_period, response_period, tuple(issues)


def _historical_trendbars(payload: Mapping[str, Any]) -> tuple[Any, ...]:
    return read_repeated(payload, "trendbar", "trendbars")


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
        if envelope.message_class is MessageClass.TRENDBAR:
            # A historical GetTrendbars response is deliberately kept out of
            # the SpotEvent/quote path.  ProtoOATrendbar.period is optional,
            # so preserve the response/request context from the historical
            # page instead of asking the provider's live SpotEvent decoder to
            # guess it (or silently dropping periodless children).
            requested_period, response_period, context_issues = _historical_period_context(envelope.payload)
            historical_bars: list[Bar] = []
            issues = list(context_issues)
            if not context_issues:
                for index, raw_bar in enumerate(_historical_trendbars(envelope.payload)):
                    try:
                        bar = normalize_trendbar(
                            raw_bar,
                            spec=self.spec,
                            received_at=envelope.received_at,
                            available_at=envelope.available_at,
                            requested_period=requested_period,
                            response_period=response_period,
                            request_id=f"history-{envelope.observation_id}-bar-{index}",
                            mode=self.mode,
                            availability_policy=envelope.availability_policy,
                        )
                    except CTraderDataError as exc:
                        issues.append(f"trendbar[{index}]:{exc}")
                        continue
                    historical_bars.append(self._bar(bar, envelope))
            normalized_bars = tuple(historical_bars)
            return CTraderNormalizationResult(
                records=normalized_bars,
                quote_events=(),
                bars=normalized_bars,
                issues=tuple(dict.fromkeys(issues)),
                snapshot=False,
                symbol_id=self.spec.symbol_id,
                quote_quality=None,
            )
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
        spot_bars = tuple(self._bar(item, envelope) for item in result.bars)
        return replace(result, records=(*quotes, *spot_bars), quote_events=quotes, bars=spot_bars)

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


def observed_coverage(
    envelopes: tuple[CaptureEnvelope, ...],
    coverage: CaptureCoverage | None,
    *,
    order: CaptureOrder = "as_observed",
) -> CaptureCoverage:
    if order not in {"as_observed", "market_time_corrected"}:
        raise CaptureContractError(f"unsupported coverage order: {order}")
    # Coverage intervals are expressed in the same time domain as the replay
    # contract.  Corrected historical replay is ordered by market ``event_time``
    # while ``available_at`` remains a reconstructed availability watermark.
    corrected_bounded = order == "market_time_corrected" and _bounded_selection_marked(envelopes)
    instants: list[datetime] = []
    for item in envelopes:
        instant = item.event_time if corrected_bounded else item.available_at
        if instant is not None:
            instants.append(instant)
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
    observed = observed_coverage(envelopes, coverage, order=order)
    synthetic = any(
        bool(item.payload.get("synthetic_fixture", item.payload.get("synthetic", False))) for item in envelopes
    )
    bounded_selection_verified = _bounded_selection_marked(envelopes)
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
        "bounded_selection_verified": bounded_selection_verified,
    }
    return CTraderCapture(
        tuple(
            item.to_dict()["payload"]
            for item in envelopes
            if item.message_class in {MessageClass.SPOT, MessageClass.TRENDBAR}
        ),
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
