"""Canonical, provider-neutral records used by the data adapters.

The data layer deliberately keeps this module small.  Providers may have very
different wire formats, but the rest of MTF Lab should only have to understand
``Event`` and ``Bar``.  All datetimes are timezone-aware UTC values; a source
timestamp and a local receipt/availability timestamp are kept separate so a
caller cannot accidentally use receipt time as market time.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Iterable, Iterator, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any, Literal, overload

from ..core.canonical import canonical_json

PriceBasis = Literal["traded", "bid", "ask", "mid", "native"]
DataMode = Literal["SYNTHETIC", "REPLAY", "OBSERVACIÓN EN DIRECTO", "OBSERVATION_EN_DIRECTO", "IMPORT"]
PRICE_BASES = frozenset({"traded", "bid", "ask", "mid", "native"})


def resolution_to_seconds(value: int | str) -> int:
    """Parse a compact resolution such as ``M1`` or a minute integer."""

    if isinstance(value, bool):
        raise ValueError("resolution must be a string or integer, not bool")
    if isinstance(value, int):
        minutes = value
    else:
        text = str(value).strip().upper()
        if text.startswith("M"):
            text = text[1:]
            minutes = int(text)
        elif text.startswith("S"):
            seconds = int(text[1:])
            if seconds <= 0:
                raise ValueError("resolution must be positive")
            return seconds
        else:
            minutes = int(text)
    if minutes <= 0:
        raise ValueError("resolution must be positive")
    return minutes * 60


def resolution_name(value: int | str) -> str:
    seconds = resolution_to_seconds(value)
    return f"M{seconds // 60}" if seconds % 60 == 0 else f"S{seconds}"


def ensure_utc(value: datetime, *, field_name: str = "timestamp") -> datetime:
    """Return *value* in UTC, rejecting a naive datetime.

    Naive timestamps are deliberately rejected at this boundary.  Importers
    may accept one only when the user explicitly supplied an ``assume_timezone``
    setting and have already attached that timezone.
    """

    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be datetime, got {type(value).__name__}")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must include a timezone")
    return value.astimezone(UTC)


def isoformat_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    return ensure_utc(value).isoformat().replace("+00:00", "Z")


def _finite(value: float | int | None, name: str, *, allow_zero: bool = True) -> float:
    if value is None:
        raise ValueError(f"{name} is required")
    if isinstance(value, bool):
        raise TypeError(f"{name} must be numeric, not bool")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    if not allow_zero and result <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return result


def normalize_price_basis(value: PriceBasis | str) -> PriceBasis:
    """Valida una base explícita sin convertir native en traded."""

    if not isinstance(value, str):
        raise TypeError("price_basis must be text")
    normalized = value.strip().lower()
    aliases = {"provider-native": "native", "provider_native": "native"}
    normalized = aliases.get(normalized, normalized)
    if normalized not in PRICE_BASES:
        raise ValueError(f"unsupported price_basis: {value!r}")
    return normalized  # type: ignore[return-value]


def _detach(value: Any) -> Any:
    """Copy nested mappings without trying to pickle an existing proxy."""

    if isinstance(value, Mapping):
        return {key: _detach(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_detach(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_detach(item) for item in value)
    if isinstance(value, set):
        return {_detach(item) for item in value}
    return deepcopy(value)


def _copy_metadata(value: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if value is None:
        return MappingProxyType({})
    if not isinstance(value, Mapping):
        raise TypeError("metadata must be a mapping")
    # Deep-copy nested dict/list payloads so a frozen record cannot be changed
    # indirectly through an input object retained by the caller. Lists remain
    # lists for the historical JSON/persistence contract.
    return MappingProxyType(_detach(value))


def _canonical_json(value: Any) -> str:
    """Canonical JSON for identities; arbitrary ``default=str`` is forbidden."""

    return canonical_json(value)


def _event_times(
    event_time: datetime,
    received_at: datetime | None,
    available_at: datetime | None,
) -> tuple[datetime, datetime | None, datetime | None]:
    normalized_event = ensure_utc(event_time, field_name="event_time")
    normalized_available = ensure_utc(available_at, field_name="available_at") if available_at is not None else None
    if normalized_available is not None and normalized_available < normalized_event:
        raise ValueError("available_at must not precede event_time")
    normalized_received = ensure_utc(received_at, field_name="received_at") if received_at is not None else None
    if normalized_received is not None and normalized_received < normalized_event:
        raise ValueError("received_at must not precede event_time")
    return normalized_event, normalized_received, normalized_available


def _normalize_event_values(event: Event) -> None:
    object.__setattr__(event, "price", _finite(event.price, "price", allow_zero=False))
    for name in ("bid", "ask", "mid"):
        value = getattr(event, name)
        if value is not None:
            object.__setattr__(event, name, _finite(value, name, allow_zero=False))
    if event.quantity is not None:
        object.__setattr__(event, "quantity", _finite(event.quantity, "quantity"))
        if event.quantity < 0:
            raise ValueError("quantity must not be negative")


def _normalize_event_state(event: Event) -> None:
    if event.side is not None:
        if not isinstance(event.side, str) or event.side.strip().lower() not in {"buy", "sell", "unknown"}:
            raise ValueError(f"unsupported side: {event.side!r}")
        object.__setattr__(event, "side", event.side.strip().lower())
    for name in ("is_snapshot", "synthetic"):
        if not isinstance(getattr(event, name), bool):
            raise TypeError(f"{name} must be boolean")


def _normalize_event_identity(event: Event) -> None:
    if event.source_event_id is not None:
        if not isinstance(event.source_event_id, str) or not event.source_event_id.strip():
            raise ValueError("source_event_id must be a non-empty string")
        object.__setattr__(event, "source_event_id", event.source_event_id.strip())
    sequence = event.source_sequence
    if sequence is not None and (
        isinstance(sequence, bool)
        or not isinstance(sequence, (int, str))
        or (isinstance(sequence, str) and not sequence.strip())
    ):
        raise TypeError("source_sequence must be a non-empty string or integer")


def _bar_window(bar: Bar) -> tuple[str, datetime, datetime]:
    if not isinstance(bar.instrument, str) or not bar.instrument.strip():
        raise ValueError("instrument must not be empty")
    instrument = bar.instrument.strip()
    start = ensure_utc(bar.interval_start, field_name="interval_start")
    end = ensure_utc(bar.interval_end, field_name="interval_end")
    if end <= start:
        raise ValueError("interval_end must be after interval_start")
    if isinstance(bar.resolution_seconds, bool) or not isinstance(bar.resolution_seconds, int):
        raise TypeError("resolution_seconds must be an integer")
    if bar.resolution_seconds <= 0:
        raise ValueError("resolution_seconds must be positive")
    if int((end - start).total_seconds()) != bar.resolution_seconds:
        raise ValueError("interval length must equal resolution_seconds")
    return instrument, start, end


def _bar_values(bar: Bar) -> dict[str, float]:
    values = {name: _finite(getattr(bar, name), name, allow_zero=False) for name in ("open", "high", "low", "close")}
    if values["high"] < max(values["open"], values["close"], values["low"]):
        raise ValueError("high must be >= open, close and low")
    if values["low"] > min(values["open"], values["close"], values["high"]):
        raise ValueError("low must be <= open, close and high")
    return values


def _bar_availability(bar: Bar, start: datetime, end: datetime) -> tuple[datetime | None, datetime | None]:
    received_at = None
    if bar.received_at is not None:
        received_at = ensure_utc(bar.received_at, field_name="received_at")
        if received_at < start:
            raise ValueError("received_at must not precede interval_start")
    available_at = None
    if bar.available_at is not None:
        available_at = ensure_utc(bar.available_at, field_name="available_at")
        if available_at < start or (bar.closed and available_at < end):
            raise ValueError("available_at must not precede interval_start/end for a closed bar")
    return received_at, available_at


def _bar_volume(bar: Bar) -> float | None:
    if bar.volume is None:
        return None
    volume = _finite(bar.volume, "volume")
    if volume < 0:
        raise ValueError("volume must not be negative")
    return volume


def _bar_trade_count(bar: Bar) -> int | None:
    if bar.trade_count is None:
        return None
    if isinstance(bar.trade_count, bool) or int(bar.trade_count) != bar.trade_count:
        raise ValueError("trade_count must be an integer")
    if bar.trade_count < 0:
        raise ValueError("trade_count must not be negative")
    return int(bar.trade_count)


def _normalize_provenance_text(value: Any, *, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must not be empty")
    return value.strip()


def _normalize_provenance_resolutions(value: Any) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or any(not isinstance(item, str) or not item.strip() for item in value):
        raise TypeError("resolutions must contain non-empty strings")
    return tuple(item.strip() for item in value)


def _normalize_provenance_notes(value: Any) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or any(not isinstance(note, str) for note in value):
        raise TypeError("notes must contain strings")
    return tuple(value)


@dataclass(frozen=True, slots=True)
class Event:
    """A normalized market event, normally a trade or a quote observation."""

    instrument: str
    event_time: datetime
    price: float
    bid: float | None = None
    ask: float | None = None
    mid: float | None = None
    price_basis: PriceBasis = "traded"
    quantity: float | None = None
    received_at: datetime | None = None
    available_at: datetime | None = None
    source: str = "unknown"
    source_event_id: str | None = None
    source_sequence: int | str | None = None
    side: str | None = None
    is_snapshot: bool = False
    synthetic: bool = False
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.instrument, str) or not self.instrument.strip():
            raise ValueError("instrument must not be empty")
        instrument = self.instrument.strip()
        object.__setattr__(self, "instrument", instrument)
        event_time, received_at, available_at = _event_times(self.event_time, self.received_at, self.available_at)
        object.__setattr__(self, "event_time", event_time)
        object.__setattr__(self, "received_at", received_at)
        object.__setattr__(self, "available_at", available_at)
        object.__setattr__(self, "price_basis", normalize_price_basis(self.price_basis))
        _normalize_event_values(self)
        _normalize_event_state(self)
        _normalize_event_identity(self)
        object.__setattr__(self, "metadata", _copy_metadata(self.metadata))

    @property
    def selected_price(self) -> float:
        """The already-selected price; no bid/ask/mid conversion is done."""

        return self.price

    @property
    def availability_known(self) -> bool:
        """True cuando existe recepción o disponibilidad observada."""

        return self.available_at is not None or self.received_at is not None

    @property
    def effective_available_at(self) -> datetime:
        """Fallback lógico histórico; no sustituye evidencia de disponibilidad."""

        return self.available_at or self.received_at or self.event_time

    @property
    def event_id(self) -> str:
        """Compatibility alias for a normalized source identity."""

        return self.data_id

    @property
    def sequence(self) -> int | str | None:
        return self.source_sequence

    @property
    def price_base(self) -> PriceBasis:
        return self.price_basis

    @property
    def data_id(self) -> str:
        """Stable identity that does not depend only on price/timestamp."""

        if self.source_event_id:
            identity: dict[str, Any] = {
                "source": self.source,
                "instrument": self.instrument,
                "source_event_id": self.source_event_id,
            }
        else:
            identity = {
                "source": self.source,
                "instrument": self.instrument,
                "event_time": isoformat_utc(self.event_time),
                "source_sequence": self.source_sequence,
                "price_basis": self.price_basis,
            }
        return hashlib.sha256(_canonical_json(identity).encode("utf-8")).hexdigest()

    @property
    def identity_key(self) -> tuple[Any, ...]:
        return (
            self.source,
            self.instrument,
            self.source_event_id or isoformat_utc(self.event_time),
            self.source_sequence,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "record_type": "event",
            "data_id": self.data_id,
            "instrument": self.instrument,
            "event_time": isoformat_utc(self.event_time),
            "price": self.price,
            "bid": self.bid,
            "ask": self.ask,
            "mid": self.mid,
            "price_basis": self.price_basis,
            "quantity": self.quantity,
            "received_at": isoformat_utc(self.received_at),
            "available_at": isoformat_utc(self.available_at),
            "source": self.source,
            "source_event_id": self.source_event_id,
            "source_sequence": self.source_sequence,
            "side": self.side,
            "is_snapshot": self.is_snapshot,
            "synthetic": self.synthetic,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True, slots=True)
class Bar:
    """A normalized OHLC bar with a half-open interval ``[start, end)``."""

    instrument: str
    interval_start: datetime
    interval_end: datetime
    open: float
    high: float
    low: float
    close: float
    resolution_seconds: int
    volume: float | None = None
    trade_count: int | None = None
    price_basis: PriceBasis = "traded"
    source: str = "unknown"
    source_record_id: str | None = None
    received_at: datetime | None = None
    available_at: datetime | None = None
    closed: bool = True
    synthetic: bool = False
    revision: int = 0
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        instrument, start, end = _bar_window(self)
        object.__setattr__(self, "instrument", instrument)
        object.__setattr__(self, "interval_start", start)
        object.__setattr__(self, "interval_end", end)
        received_at, available_at = _bar_availability(self, start, end)
        object.__setattr__(self, "received_at", received_at)
        object.__setattr__(self, "available_at", available_at)
        object.__setattr__(self, "price_basis", normalize_price_basis(self.price_basis))
        if not isinstance(self.closed, bool) or not isinstance(self.synthetic, bool):
            raise TypeError("closed and synthetic must be boolean")
        values = _bar_values(self)
        for name, value in values.items():
            object.__setattr__(self, name, value)
        volume = _bar_volume(self)
        object.__setattr__(self, "volume", volume)
        trade_count = _bar_trade_count(self)
        object.__setattr__(self, "trade_count", trade_count)
        if isinstance(self.revision, bool) or not isinstance(self.revision, int) or self.revision < 0:
            raise ValueError("revision must be a non-negative integer")
        if self.source_record_id is not None:
            if not isinstance(self.source_record_id, str) or not self.source_record_id.strip():
                raise ValueError("source_record_id must be a non-empty string")
            object.__setattr__(self, "source_record_id", self.source_record_id.strip())
        if not isinstance(self.source, str):
            raise TypeError("source must be a string")
        object.__setattr__(self, "source", self.source.strip() or "unknown")
        object.__setattr__(self, "metadata", _copy_metadata(self.metadata))

    @property
    def resolution(self) -> str:
        minutes = self.resolution_seconds // 60
        return f"M{minutes}" if self.resolution_seconds % 60 == 0 else f"S{self.resolution_seconds}"

    @property
    def start(self) -> datetime:
        return self.interval_start

    @property
    def end(self) -> datetime:
        return self.interval_end

    @property
    def timeframe(self) -> str:
        return self.resolution

    @property
    def timeframe_name(self) -> str:
        return self.resolution

    @property
    def candle_id(self) -> str:
        return self.data_id

    @property
    def event_count(self) -> int:
        return self.trade_count or 0

    @property
    def availability_known(self) -> bool:
        """True cuando existe recepción o disponibilidad observada."""

        return self.available_at is not None or self.received_at is not None

    @property
    def is_native(self) -> bool:
        """True sólo para una serie provider-native explícita."""

        return self.price_basis == "native"

    @property
    def is_synthetic(self) -> bool:
        return self.synthetic

    @property
    def data_id(self) -> str:
        """Stable identity for a source interval and revision."""

        identity = {
            "source": self.source,
            "instrument": self.instrument,
            "resolution_seconds": self.resolution_seconds,
            "interval_start": isoformat_utc(self.interval_start),
            "source_record_id": self.source_record_id,
            "revision": self.revision,
        }
        return hashlib.sha256(_canonical_json(identity).encode("utf-8")).hexdigest()

    @property
    def identity_key(self) -> tuple[Any, ...]:
        return (
            self.source,
            self.instrument,
            self.resolution_seconds,
            isoformat_utc(self.interval_start),
            self.source_record_id,
            self.revision,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "record_type": "bar",
            "data_id": self.data_id,
            "instrument": self.instrument,
            "interval_start": isoformat_utc(self.interval_start),
            "interval_end": isoformat_utc(self.interval_end),
            "resolution": self.resolution,
            "resolution_seconds": self.resolution_seconds,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "trade_count": self.trade_count,
            "price_basis": self.price_basis,
            "source": self.source,
            "source_record_id": self.source_record_id,
            "received_at": isoformat_utc(self.received_at),
            "available_at": isoformat_utc(self.available_at),
            "closed": self.closed,
            "synthetic": self.synthetic,
            "revision": self.revision,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True, slots=True)
class ValidationIssue:
    code: str
    message: str
    severity: Literal["ERROR", "WARNING"] = "ERROR"
    row: int | None = None
    field: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "severity": self.severity,
            "row": self.row,
            "field": self.field,
        }


class DataValidationError(ValueError):
    """Raised when strict import/normalization finds one or more errors."""

    def __init__(self, message: str, issues: Sequence[ValidationIssue] = ()) -> None:
        self.issues = tuple(issues)
        if self.issues:
            detail = "; ".join(
                f"{i.code}{f' row={i.row}' if i.row is not None else ''}: {i.message}" for i in self.issues[:8]
            )
            if len(self.issues) > 8:
                detail += f"; ... {len(self.issues) - 8} more"
            message = f"{message}: {detail}"
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class DataQuality:
    """Validation summary; no missing interval is silently filled."""

    valid: bool
    record_count: int
    duplicate_count: int = 0
    out_of_order_count: int = 0
    invalid_count: int = 0
    gap_count: int = 0
    coverage_start: datetime | None = None
    coverage_end: datetime | None = None
    notes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.valid, bool):
            raise TypeError("valid must be boolean")
        for name in ("record_count", "duplicate_count", "out_of_order_count", "invalid_count", "gap_count"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if not isinstance(self.notes, (list, tuple)) or any(not isinstance(note, str) for note in self.notes):
            raise TypeError("notes must contain strings")
        object.__setattr__(self, "notes", tuple(self.notes))
        if self.coverage_start is not None:
            object.__setattr__(self, "coverage_start", ensure_utc(self.coverage_start, field_name="coverage_start"))
        if self.coverage_end is not None:
            object.__setattr__(self, "coverage_end", ensure_utc(self.coverage_end, field_name="coverage_end"))

    def to_dict(self) -> dict[str, Any]:
        return {
            "valid": self.valid,
            "record_count": self.record_count,
            "duplicate_count": self.duplicate_count,
            "out_of_order_count": self.out_of_order_count,
            "invalid_count": self.invalid_count,
            "gap_count": self.gap_count,
            "coverage_start": isoformat_utc(self.coverage_start),
            "coverage_end": isoformat_utc(self.coverage_end),
            "notes": list(self.notes),
        }


@dataclass(frozen=True, slots=True)
class Provenance:
    """Source and coverage metadata attached to an import or generated set."""

    provider: str
    mode: DataMode
    instrument: str
    resolutions: tuple[str, ...] = ()
    price_basis: PriceBasis = "traded"
    source_uri: str | None = None
    source_hash: str | None = None
    coverage_start: datetime | None = None
    coverage_end: datetime | None = None
    generated_seed: int | None = None
    synthetic: bool = False
    retrieved_at: datetime | None = None
    notes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        provider = _normalize_provenance_text(self.provider, name="provider")
        instrument = _normalize_provenance_text(self.instrument, name="instrument")
        mode = _normalize_provenance_text(self.mode, name="mode")
        object.__setattr__(self, "provider", provider)
        object.__setattr__(self, "instrument", instrument)
        object.__setattr__(self, "mode", mode)
        object.__setattr__(self, "price_basis", normalize_price_basis(self.price_basis))
        resolutions = _normalize_provenance_resolutions(self.resolutions)
        object.__setattr__(self, "resolutions", resolutions)
        if self.coverage_start is not None:
            object.__setattr__(self, "coverage_start", ensure_utc(self.coverage_start, field_name="coverage_start"))
        if self.coverage_end is not None:
            object.__setattr__(self, "coverage_end", ensure_utc(self.coverage_end, field_name="coverage_end"))
        if (
            self.coverage_start is not None
            and self.coverage_end is not None
            and self.coverage_end < self.coverage_start
        ):
            raise ValueError("coverage_end must not precede coverage_start")
        if self.generated_seed is not None and (
            isinstance(self.generated_seed, bool) or not isinstance(self.generated_seed, int)
        ):
            raise TypeError("generated_seed must be an integer")
        if not isinstance(self.synthetic, bool):
            raise TypeError("synthetic must be boolean")
        if self.retrieved_at is not None:
            object.__setattr__(self, "retrieved_at", ensure_utc(self.retrieved_at, field_name="retrieved_at"))
        object.__setattr__(self, "notes", _normalize_provenance_notes(self.notes))

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "mode": self.mode,
            "instrument": self.instrument,
            "resolutions": list(self.resolutions),
            "price_basis": self.price_basis,
            "source_uri": self.source_uri,
            "source_hash": self.source_hash,
            "coverage_start": isoformat_utc(self.coverage_start),
            "coverage_end": isoformat_utc(self.coverage_end),
            "generated_seed": self.generated_seed,
            "synthetic": self.synthetic,
            "retrieved_at": isoformat_utc(self.retrieved_at),
            "notes": list(self.notes),
        }


@dataclass(frozen=True, slots=True)
class DataSet:
    """A small sequence-like result shared by synthetic and import adapters."""

    records: tuple[Event | Bar, ...]
    provenance: Provenance
    quality: DataQuality
    issues: tuple[ValidationIssue, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.records, (list, tuple)):
            raise TypeError("records must be a sequence")
        if any(not isinstance(record, (Event, Bar)) for record in self.records):
            raise TypeError("records must contain Event or Bar values")
        object.__setattr__(self, "records", tuple(self.records))
        if not isinstance(self.provenance, Provenance):
            raise TypeError("provenance must be Provenance")
        if not isinstance(self.quality, DataQuality):
            raise TypeError("quality must be DataQuality")
        if not isinstance(self.issues, (list, tuple)) or any(
            not isinstance(issue, ValidationIssue) for issue in self.issues
        ):
            raise TypeError("issues must contain ValidationIssue values")
        object.__setattr__(self, "issues", tuple(self.issues))

    def __iter__(self) -> Iterator[Event | Bar]:
        return iter(self.records)

    def __len__(self) -> int:
        return len(self.records)

    @overload
    def __getitem__(self, item: int) -> Event | Bar: ...

    @overload
    def __getitem__(self, item: slice) -> tuple[Event | Bar, ...]: ...

    def __getitem__(self, item: int | slice) -> Event | Bar | tuple[Event | Bar, ...]:
        return self.records[item]

    @property
    def bars(self) -> tuple[Bar, ...]:
        return tuple(record for record in self.records if isinstance(record, Bar))

    @property
    def candles(self) -> tuple[Bar, ...]:
        return self.bars

    @property
    def events(self) -> tuple[Event, ...]:
        return tuple(record for record in self.records if isinstance(record, Event))

    def to_dict(self) -> dict[str, Any]:
        return {
            "provenance": self.provenance.to_dict(),
            "quality": self.quality.to_dict(),
            "issues": [issue.to_dict() for issue in self.issues],
            "records": [record.to_dict() for record in self.records],
        }


def infer_quality(records: Iterable[Event | Bar], issues: Sequence[ValidationIssue] = ()) -> DataQuality:
    """Compute a conservative quality summary for already-normalized records."""

    values = tuple(records)
    errors = [i for i in issues if i.severity == "ERROR"]
    duplicate_count = sum(1 for i in issues if i.code == "DUPLICATE")
    out_of_order_count = sum(1 for i in issues if i.code == "OUT_OF_ORDER")
    invalid_count = sum(1 for i in issues if i.code.startswith("INVALID") or i.code in {"PARSE_ERROR", "MISSING_FIELD"})
    times = [record.event_time if isinstance(record, Event) else record.interval_start for record in values]
    ordered = sorted(times)
    coverage_end = (
        max((record.interval_end for record in values if isinstance(record, Bar)), default=None)
        if values and all(isinstance(record, Bar) for record in values)
        else (
            max((record.event_time for record in values if isinstance(record, Event)), default=None) if values else None
        )
    )
    gap_count = 0
    if values and all(isinstance(record, Bar) for record in values):
        bars = sorted((record for record in values if isinstance(record, Bar)), key=lambda item: item.interval_start)
        for previous, current in zip(bars, bars[1:], strict=False):
            if current.interval_start > previous.interval_start + (current.interval_end - current.interval_start):
                gap_count += 1
    quality_notes = [issue.message for issue in issues if issue.severity == "WARNING"]
    if gap_count:
        quality_notes.append(
            f"{gap_count} hueco(s) de cobertura: no se rellenaron; continuidad/indicadores afectados requieren reconciliación"
        )
    return DataQuality(
        valid=not errors,
        record_count=len(values),
        duplicate_count=duplicate_count,
        out_of_order_count=out_of_order_count,
        invalid_count=invalid_count,
        gap_count=gap_count,
        coverage_start=min(ordered) if ordered else None,
        coverage_end=coverage_end,
        notes=tuple(quality_notes),
    )


# ``Candle`` is kept as a readable synonym for callers that prefer the term.
Candle = Bar
