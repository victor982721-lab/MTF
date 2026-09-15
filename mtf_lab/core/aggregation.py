"""Agregación causal de eventos a velas de intervalos fijos."""

from __future__ import annotations

import math
from collections import deque
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, cast

from .historical_calendar import HistoricalQuoteCalendar
from .models import Candle, EventKind, MarketEvent, OperationMode, PriceBase, Timeframe, normalize_utc, parse_timeframe
from .quality import DataQuality, QualityFlag, QualityIssue, QualityReason, QualityReport, merge_quality, validate_event

_OrderKey = tuple[datetime, datetime, datetime, tuple[int, int | float | str], str]
BAR_CLOCK_BASIS = "UTC_TIMEFRAME_BOUNDARIES_NO_OHLC_IMPUTATION"
_QUOTE_BUCKET_STATE_VERSION = 1
_QUALITY_REASON_SAMPLE_LIMIT = 64
_QUALITY_SOURCE_SAMPLE_LIMIT = 16
_KNOWN_QUALITY_FLAG_VALUES = frozenset(flag.value for flag in QualityFlag)
_KNOWN_QUALITY_REASON_VALUES = frozenset(reason.value for reason in QualityReason)


def bar_clock_ordinal(timestamp: datetime, timeframe: Timeframe | str) -> int:
    """Nominal UTC boundaries, not a count of observed candles or quotes."""
    timestamp = normalize_utc(timestamp, "bar_clock_timestamp")
    delta = timestamp - datetime(1970, 1, 1, tzinfo=UTC)
    microseconds = (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds
    seconds = int(cast(Any, parse_timeframe(timeframe).seconds))
    return int(microseconds // (seconds * 1_000_000))


def interval_start(timestamp: datetime, timeframe: Timeframe | str) -> datetime:
    """Devuelve el inicio de ``[inicio, fin)`` anclado a Unix epoch UTC."""

    timestamp = normalize_utc(timestamp, "timestamp")
    tf = parse_timeframe(timeframe)
    seconds = timestamp.timestamp()
    start_epoch = math.floor(seconds / tf.seconds) * tf.seconds
    return datetime.fromtimestamp(start_epoch, tz=UTC)


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z") if value else None


def _finite_float(value: object, name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{name} debe ser numérico")
    try:
        result = float(cast(Any, value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} debe ser numérico") from exc
    if not math.isfinite(result):
        raise ValueError(f"{name} debe ser finito")
    return result


def _positive_count(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} debe ser entero no negativo")
    return value


def _bounded_count_mapping(value: object, name: str, allowed: frozenset[str], *, max_entries: int) -> dict[str, int]:
    if not isinstance(value, Mapping) or len(value) > max_entries:
        raise ValueError(f"{name} inválido o excede la cardinalidad acotada")
    result: dict[str, int] = {}
    for key, raw_count in value.items():
        if not isinstance(key, str) or key not in allowed:
            raise ValueError(f"{name} contiene una clave desconocida")
        result[key] = _positive_count(raw_count, f"{name}[{key}]")
    return result


def _state_time(value: object, name: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{name} debe ser timestamp ISO")
    text = value[:-1] + "+00:00" if value.endswith(("Z", "z")) else value
    try:
        return normalize_utc(datetime.fromisoformat(text), name)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} debe ser timestamp ISO con zona horaria") from exc


def _accumulator_collections(
    value: Mapping[str, object],
) -> tuple[Iterable[object], Iterable[object], Mapping[str, object], Mapping[str, object], Mapping[str, object]]:
    required = {
        "open",
        "high",
        "low",
        "close",
        "volume",
        "event_count",
        "first_event_time",
        "last_event_time",
        "first_available_at",
        "latest_available_at",
        "first_received_at",
        "latest_received_at",
        "received_count",
        "quality_flags",
        "quality_flag_counts",
        "quality_reasons",
        "quality_reason_counts",
        "quality_reason_overflow",
        "source_counts",
        "source_overflow",
        "previous_open_seconds",
        "max_open_seconds",
        "internal_gap_count",
        "gap",
    }
    if set(value) != required:
        raise ValueError("estado del acumulador continuo incompleto o con claves desconocidas")
    raw_flags = value["quality_flags"]
    raw_reasons = value["quality_reasons"]
    raw_flag_counts = value["quality_flag_counts"]
    raw_reason_counts = value["quality_reason_counts"]
    raw_sources = value["source_counts"]
    if not isinstance(raw_flags, (list, tuple, set, frozenset)):
        raise ValueError("quality_flags debe ser secuencia")
    if len(raw_flags) > len(_KNOWN_QUALITY_FLAG_VALUES):
        raise ValueError("quality_flags excede la cardinalidad acotada")
    if any(
        not isinstance(item, (str, QualityFlag))
        or (item.value if isinstance(item, QualityFlag) else item) not in _KNOWN_QUALITY_FLAG_VALUES
        for item in raw_flags
    ):
        raise ValueError("quality_flags contiene una bandera desconocida")
    if not isinstance(raw_reasons, (list, tuple)) or not all(isinstance(item, str) for item in raw_reasons):
        raise ValueError("quality_reasons inválidas")
    _bounded_count_mapping(
        raw_flag_counts,
        "quality_flag_counts",
        _KNOWN_QUALITY_FLAG_VALUES,
        max_entries=len(_KNOWN_QUALITY_FLAG_VALUES),
    )
    _bounded_count_mapping(
        raw_reason_counts,
        "quality_reason_counts",
        _KNOWN_QUALITY_REASON_VALUES,
        max_entries=len(_KNOWN_QUALITY_REASON_VALUES),
    )
    if not isinstance(raw_sources, Mapping) or len(raw_sources) > _QUALITY_SOURCE_SAMPLE_LIMIT:
        raise ValueError("source_counts inválido o excede la muestra acotada")
    if any(not isinstance(key, str) or not key for key in raw_sources):
        raise ValueError("source_counts contiene una clave inválida")
    if not isinstance(value["gap"], bool):
        raise ValueError("gap debe ser booleano")
    return (
        cast(Iterable[object], raw_flags),
        cast(Iterable[object], raw_reasons),
        cast(Mapping[str, object], raw_flag_counts),
        cast(Mapping[str, object], raw_reason_counts),
        cast(Mapping[str, object], raw_sources),
    )


@dataclass(slots=True)
class _QuoteBucketAccumulator:
    """Constant-memory OHLC/coverage state for ``continuous_quotes``.

    The source is admitted in causal order by :class:`CandleAggregator`, so
    the first/last values and open/close can be updated online.  Quality
    reasons retain a bounded first-seen sample plus explicit overflow counts;
    flags and known reason counters use finite vocabularies.
    """

    open: float
    high: float
    low: float
    close: float
    volume: float
    event_count: int
    first_event_time: datetime
    last_event_time: datetime
    first_available_at: datetime
    latest_available_at: datetime
    first_received_at: datetime | None
    latest_received_at: datetime | None
    received_count: int
    quality_flags: set[QualityFlag] = field(default_factory=set)
    quality_flag_counts: dict[str, int] = field(default_factory=dict)
    quality_reasons: list[str] = field(default_factory=list)
    quality_reason_seen: set[str] = field(default_factory=set)
    quality_reason_counts: dict[str, int] = field(default_factory=dict)
    quality_reason_overflow: int = 0
    source_counts: dict[str, int] = field(default_factory=dict)
    source_overflow: int = 0
    previous_open_seconds: float | None = None
    max_open_seconds: float = 0.0
    internal_gap_count: int = 0
    gap: bool = False

    @classmethod
    def from_event(cls, event: MarketEvent) -> _QuoteBucketAccumulator:
        price = event.selected_price
        if price is None:
            raise ValueError("No hay precio compatible con la base solicitada")
        value = _finite_float(price, "selected_price")
        available = event.effective_available_at
        received = event.received_at
        result = cls(
            value,
            value,
            value,
            value,
            event.quantity or 0.0,
            1,
            event.event_time,
            event.event_time,
            available,
            available,
            received,
            received,
            1 if received is not None else 0,
        )
        result._record_quality(event)
        result._record_source(event.source)
        return result

    def append(self, event: MarketEvent, open_seconds: float, max_gap_seconds: float) -> None:
        price = event.selected_price
        if price is None:
            raise ValueError("No hay precio compatible con la base solicitada")
        value = _finite_float(price, "selected_price")
        self.high = max(self.high, value)
        self.low = min(self.low, value)
        self.close = value
        self.volume += event.quantity or 0.0
        self.event_count += 1
        self.last_event_time = event.event_time
        self.latest_available_at = max(self.latest_available_at, event.effective_available_at)
        received = event.received_at
        if received is not None:
            self.received_count += 1
            self.first_received_at = self.first_received_at or received
            self.latest_received_at = (
                received if self.latest_received_at is None else max(self.latest_received_at, received)
            )
        self.previous_open_seconds = open_seconds
        self.max_open_seconds = max(self.max_open_seconds, open_seconds)
        if open_seconds > max_gap_seconds:
            self.internal_gap_count += 1
            self.gap = True
        self._record_quality(event)
        self._record_source(event.source)

    def _record_source(self, source: str) -> None:
        if source in self.source_counts:
            self.source_counts[source] += 1
        elif len(self.source_counts) < _QUALITY_SOURCE_SAMPLE_LIMIT:
            self.source_counts[source] = 1
        else:
            self.source_overflow += 1

    def _record_quality(self, event: MarketEvent) -> None:
        for flag in event.quality.flags:
            self.quality_flags.add(flag)
            key = flag.value
            self.quality_flag_counts[key] = self.quality_flag_counts.get(key, 0) + 1
        for raw_reason in event.quality.reasons:
            reason = str(raw_reason)
            if reason in _KNOWN_QUALITY_REASON_VALUES:
                self.quality_reason_counts[reason] = self.quality_reason_counts.get(reason, 0) + 1
            if reason in self.quality_reason_seen:
                continue
            if len(self.quality_reasons) < _QUALITY_REASON_SAMPLE_LIMIT:
                self.quality_reasons.append(reason)
                self.quality_reason_seen.add(reason)
            else:
                self.quality_reason_overflow += 1

    def quality(self, *, source: str) -> DataQuality:
        reasons = list(self.quality_reasons)
        if self.quality_reason_overflow:
            reasons.append(f"quality_reasons_truncated:{self.quality_reason_overflow}")
        if self.source_overflow:
            reasons.append(f"source_counts_truncated:{self.source_overflow}")
        return DataQuality(frozenset(self.quality_flags), tuple(reasons), source)

    def to_dict(self) -> dict[str, object]:
        return {
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "event_count": self.event_count,
            "first_event_time": _iso(self.first_event_time),
            "last_event_time": _iso(self.last_event_time),
            "first_available_at": _iso(self.first_available_at),
            "latest_available_at": _iso(self.latest_available_at),
            "first_received_at": _iso(self.first_received_at),
            "latest_received_at": _iso(self.latest_received_at),
            "received_count": self.received_count,
            "quality_flags": sorted(flag.value for flag in self.quality_flags),
            "quality_flag_counts": dict(self.quality_flag_counts),
            "quality_reasons": list(self.quality_reasons),
            "quality_reason_counts": dict(self.quality_reason_counts),
            "quality_reason_overflow": self.quality_reason_overflow,
            "source_counts": dict(self.source_counts),
            "source_overflow": self.source_overflow,
            "previous_open_seconds": self.previous_open_seconds,
            "max_open_seconds": self.max_open_seconds,
            "internal_gap_count": self.internal_gap_count,
            "gap": self.gap,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> _QuoteBucketAccumulator:
        raw_flags, raw_reasons, raw_flag_counts, raw_reason_counts, raw_sources = _accumulator_collections(value)
        try:
            flags = {QualityFlag(item.value if isinstance(item, QualityFlag) else str(item)) for item in raw_flags}
        except ValueError as exc:
            raise ValueError("quality_flags contiene una bandera desconocida") from exc
        reasons = [str(item) for item in raw_reasons]
        if len(reasons) > _QUALITY_REASON_SAMPLE_LIMIT or len(set(reasons)) != len(reasons):
            raise ValueError("quality_reasons debe ser muestra única y acotada")
        flag_counts = _bounded_count_mapping(
            raw_flag_counts,
            "quality_flag_counts",
            _KNOWN_QUALITY_FLAG_VALUES,
            max_entries=len(_KNOWN_QUALITY_FLAG_VALUES),
        )
        reason_counts = _bounded_count_mapping(
            raw_reason_counts,
            "quality_reason_counts",
            _KNOWN_QUALITY_REASON_VALUES,
            max_entries=len(_KNOWN_QUALITY_REASON_VALUES),
        )
        source_counts = {str(key): _positive_count(item, f"source_counts[{key}]") for key, item in raw_sources.items()}
        first_received = value["first_received_at"]
        latest_received = value["latest_received_at"]
        gap = cast(bool, value["gap"])
        result = cls(
            _finite_float(value["open"], "open"),
            _finite_float(value["high"], "high"),
            _finite_float(value["low"], "low"),
            _finite_float(value["close"], "close"),
            _finite_float(value["volume"], "volume"),
            _positive_count(value["event_count"], "event_count"),
            _state_time(value["first_event_time"], "first_event_time"),
            _state_time(value["last_event_time"], "last_event_time"),
            _state_time(value["first_available_at"], "first_available_at"),
            _state_time(value["latest_available_at"], "latest_available_at"),
            _state_time(first_received, "first_received_at") if first_received is not None else None,
            _state_time(latest_received, "latest_received_at") if latest_received is not None else None,
            _positive_count(value["received_count"], "received_count"),
            flags,
            flag_counts,
            reasons,
            set(reasons),
            reason_counts,
            _positive_count(value["quality_reason_overflow"], "quality_reason_overflow"),
            source_counts,
            _positive_count(value["source_overflow"], "source_overflow"),
            _finite_float(value["previous_open_seconds"], "previous_open_seconds")
            if value["previous_open_seconds"] is not None
            else None,
            _finite_float(value["max_open_seconds"], "max_open_seconds"),
            _positive_count(value["internal_gap_count"], "internal_gap_count"),
            gap,
        )
        if (
            result.event_count < 1
            or result.event_count < result.received_count
            or result.volume < 0
            or result.high < max(result.open, result.close)
            or result.low > min(result.open, result.close)
            or result.first_event_time > result.last_event_time
            or result.first_available_at > result.latest_available_at
            or result.max_open_seconds < 0
            or (result.previous_open_seconds is not None and result.previous_open_seconds < 0)
            or result.internal_gap_count > result.event_count - 1
        ):
            raise ValueError("contadores del acumulador inconsistentes")
        if result.received_count == 0 and (
            result.first_received_at is not None or result.latest_received_at is not None
        ):
            raise ValueError("timestamps de recepción inconsistentes")
        if result.received_count > 0 and (result.first_received_at is None or result.latest_received_at is None):
            raise ValueError("timestamps de recepción faltantes")
        if (
            result.first_received_at is not None
            and result.latest_received_at is not None
            and result.first_received_at > result.latest_received_at
        ):
            raise ValueError("orden de recepción inconsistente")
        return result


@dataclass(frozen=True, slots=True)
class AggregationResult:
    """Salida de una inserción incremental."""

    emitted: tuple[Candle, ...] = ()
    accepted: bool = True
    issues: tuple[QualityIssue, ...] = ()

    @property
    def candles(self) -> tuple[Candle, ...]:
        return self.emitted

    @property
    def partial(self) -> bool:
        return any(
            candle.quality.has(QualityFlag.PARTIAL) or bool(candle.metadata.get("partial")) for candle in self.emitted
        )


@dataclass(slots=True)
class _Bucket:
    start: datetime
    end: datetime
    events: list[MarketEvent] = field(default_factory=list)
    # True when the source began after the interval start. It is intentionally
    # distinct from temporal ``closed``: an elapsed interval can still have
    # incomplete source coverage and must not be presented as a complete bar.
    partial: bool | None = None
    # Only continuous quote coverage uses this compact state. Strict mode
    # leaves this field None and preserves the historical event list exactly.
    accumulator: _QuoteBucketAccumulator | None = None

    def __post_init__(self) -> None:
        if self.partial is None:
            self.partial = bool(
                (self.events and min(event.event_time for event in self.events) > self.start)
                or (self.accumulator is not None and self.accumulator.first_event_time > self.start)
            )


class CandleAggregator:
    """Agregador de una temporalidad para un instrumento/base de precio.

    En modo incremental los eventos deben llegar en orden causal. Un evento
    que cae en un intervalo ya cerrado se rechaza y queda registrado como
    ``out_of_order``/``unreconciled``: no se reescribe retrospectivamente una
    observación ya emitida. El backfill puede procesarse en otra sesión o
    generar revisiones separadas.
    """

    def __init__(
        self,
        timeframe: Timeframe | str,
        *,
        instrument: str | None = None,
        price_base: PriceBase | str = PriceBase.TRADED,
        source: str = "aggregated",
        mode: OperationMode | str = OperationMode.REPLAY,
        reject_out_of_order: bool = True,
        max_seen_event_ids: int | None = None,
        max_issues: int | None = None,
        coverage_mode: str = "strict",
        max_quote_gap_seconds: float | None = None,
        historical_calendar: HistoricalQuoteCalendar | None = None,
    ) -> None:
        self.timeframe = parse_timeframe(timeframe)
        self.instrument = instrument.strip() if instrument else None
        if self.instrument == "":
            self.instrument = None
        self.price_base = price_base if isinstance(price_base, PriceBase) else PriceBase(str(price_base).lower())
        self.source = str(source).strip() or "aggregated"
        self.mode = mode if isinstance(mode, OperationMode) else OperationMode(str(mode).upper())
        self.reject_out_of_order = reject_out_of_order
        if coverage_mode not in {"strict", "continuous_quotes"}:
            raise ValueError("coverage_mode debe ser strict o continuous_quotes")
        if coverage_mode == "continuous_quotes" and (
            isinstance(max_quote_gap_seconds, bool)
            or not isinstance(max_quote_gap_seconds, (int, float))
            or not math.isfinite(max_quote_gap_seconds)
            or max_quote_gap_seconds <= 0
        ):
            raise ValueError("continuous_quotes requiere max_quote_gap_seconds finito y positivo")
        self.coverage_mode = coverage_mode
        self.max_quote_gap_seconds = max_quote_gap_seconds
        if historical_calendar is not None and (
            not isinstance(historical_calendar, HistoricalQuoteCalendar)
            or coverage_mode != "continuous_quotes"
            or self.mode not in {OperationMode.REPLAY, OperationMode.SYNTHETIC}
        ):
            raise ValueError("historical_calendar sólo admite cobertura histórica explícita; no operación LIVE")
        self.historical_calendar = historical_calendar
        if coverage_mode == "continuous_quotes" and not reject_out_of_order:
            raise ValueError("continuous_quotes exige orden causal estricto")
        if max_seen_event_ids is not None and (isinstance(max_seen_event_ids, bool) or int(max_seen_event_ids) <= 0):
            raise ValueError("max_seen_event_ids debe ser entero positivo")
        if max_issues is not None and (isinstance(max_issues, bool) or int(max_issues) <= 0):
            raise ValueError("max_issues debe ser entero positivo")
        self.max_seen_event_ids = int(max_seen_event_ids) if max_seen_event_ids is not None else None
        self.max_issues = (
            int(max_issues)
            if max_issues is not None
            else (self.max_seen_event_ids if self.max_seen_event_ids is not None else None)
        )
        self._bucket: _Bucket | None = None
        self._closed_through: datetime | None = None
        self._seen_event_ids: set[str] = set()
        self._seen_event_order: deque[str] = deque()
        self._last_order_key: _OrderKey | None = None
        self._issues: deque[QualityIssue] | list[QualityIssue] = (
            deque(maxlen=self.max_issues) if self.max_issues is not None else []
        )
        self._last_event_time: datetime | None = None

    @property
    def issues(self) -> tuple[QualityIssue, ...]:
        return tuple(self._issues)

    @property
    def last_event_time(self) -> datetime | None:
        return self._last_event_time

    def _event_order_key(self, event: MarketEvent) -> _OrderKey:
        received = event.received_at or event.event_time
        available = event.available_at or received
        sequence = event.sequence
        # Mezcla tipos no comparables convirtiendo sólo la clave de orden, no
        # la identidad persistida. event_id es el desempate final estable.
        seq_key = (
            (0, sequence)
            if isinstance(sequence, (int, float)) and not isinstance(sequence, bool)
            else (1, str(sequence or ""))
        )
        return event.event_time, available, received, seq_key, event.event_id or ""

    def _new_issue(self, code: str, message: str, event: MarketEvent | None = None) -> QualityIssue:
        issue = QualityIssue(
            code, message, record_id=event.event_id if event else None, timestamp=event.event_time if event else None
        )
        self._issues.append(issue)
        return issue

    def _admit_event(self, event: MarketEvent) -> tuple[bool, QualityIssue | None]:
        validation = validate_event(event)
        if not validation.accepted:
            issue = self._new_issue("invalid_event", "; ".join(item.message for item in validation.issues), event)
            return False, issue
        if self.coverage_mode == "continuous_quotes" and event.event_kind is not EventKind.QUOTE:
            issue = self._new_issue("quote_coverage_requires_quote", "La cobertura exige eventos Bid/Ask", event)
            return False, issue
        if self.instrument is None:
            self.instrument = event.instrument
        if event.instrument != self.instrument:
            issue = self._new_issue(
                "instrument_mismatch", f"Se esperaba {self.instrument}, llegó {event.instrument}", event
            )
            return False, issue
        if event.price_base is not self.price_base:
            issue = self._new_issue(
                "price_base_mismatch",
                f"Se esperaba {self.price_base.value}, llegó {event.price_base.value}",
                event,
            )
            return False, issue
        if event.event_id in self._seen_event_ids:
            issue = self._new_issue("duplicate_event", f"Evento duplicado: {event.event_id}", event)
            return False, issue
        order_key = self._event_order_key(event)
        if self._last_order_key is not None and order_key < self._last_order_key:
            issue = self._new_issue("out_of_order", f"Evento fuera de orden: {event.event_id}", event)
            if self.reject_out_of_order:
                return False, issue
        return True, None

    def _record_event(self, event: MarketEvent) -> None:
        order_key = self._event_order_key(event)
        event_id = event.event_id
        assert event_id is not None
        self._seen_event_ids.add(event_id)
        self._seen_event_order.append(event_id)
        if self.max_seen_event_ids is not None:
            while len(self._seen_event_order) > self.max_seen_event_ids:
                evicted_id = self._seen_event_order.popleft()
                self._seen_event_ids.discard(evicted_id)
        self._last_order_key = max(self._last_order_key, order_key) if self._last_order_key is not None else order_key
        self._last_event_time = (
            max(self._last_event_time, event.event_time) if self._last_event_time else event.event_time
        )

    def add(self, event: MarketEvent) -> AggregationResult:
        """Añade un evento y emite cualquier vela anterior ya cerrada."""

        if not isinstance(event, MarketEvent):
            raise TypeError("CandleAggregator.add requiere MarketEvent")
        accepted, issue = self._admit_event(event)
        if not accepted:
            assert issue is not None
            return AggregationResult((), False, (issue,))
        self._record_event(event)
        return self._insert_event(event)

    def _insert_event(self, event: MarketEvent) -> AggregationResult:
        start = interval_start(event.event_time, self.timeframe)
        end = start + self.timeframe.delta
        if self._closed_through is not None and start < self._closed_through:
            issue = self._new_issue("late_closed_interval", "El evento pertenece a un intervalo ya cerrado", event)
            # No se integra en la observación original, aunque conserva el
            # event_id como visto para que un retry no duplique más evidencia.
            return AggregationResult((), False, (issue,))
        if self._bucket is None:
            return self._start_bucket(event, start, end)
        if start < self._bucket.start:
            issue = self._new_issue("out_of_order_interval", "El intervalo retrocede respecto al bucket activo", event)
            return AggregationResult((), False, (issue,))
        if start > self._bucket.start:
            return self._advance_bucket(event, start, end)
        if self.coverage_mode == "continuous_quotes":
            self._append_continuous_event(event)
        else:
            self._bucket.events.append(event)
        return AggregationResult()

    def _start_bucket(self, event: MarketEvent, start: datetime, end: datetime) -> AggregationResult:
        partial = event.event_time > start
        if self.coverage_mode == "continuous_quotes":
            self._bucket = _Bucket(start, end, partial=partial, accumulator=_QuoteBucketAccumulator.from_event(event))
            if not partial:
                return AggregationResult()
            issue = self._new_issue(
                "partial_bucket", "El arranque ocurrió a mitad de intervalo; la cobertura no es completa", event
            )
            return AggregationResult((), True, (issue,))
        self._bucket = _Bucket(start, end, partial=partial)
        self._bucket.events.append(event)
        if not partial:
            return AggregationResult()
        issue = self._new_issue(
            "partial_bucket", "El arranque ocurrió a mitad de intervalo; la cobertura no es completa", event
        )
        return AggregationResult((), True, (issue,))

    def _advance_bucket(self, event: MarketEvent, start: datetime, end: datetime) -> AggregationResult:
        assert self._bucket is not None
        contiguous_quotes = self._quote_continuity(event, start)
        emitted = [self._build_candle(self._bucket, closed=True, emitted_at=event.effective_available_at)]
        self._closed_through = self._bucket.end
        gap_issue: QualityIssue | None = None
        if start > self._bucket.end:
            scheduled = self.historical_calendar is not None and self.historical_calendar.covers_closed(
                self._bucket.end, start
            )
            gap_issue = self._new_issue(
                "modeled_scheduled_closure" if scheduled else "gap",
                f"Sin eventos entre {self._bucket.end.isoformat()} y {start.isoformat()}",
                event,
            )
        # In an explicitly selected bounded-gap quote stream, the first *change* in a
        # new interval need not occur at an exact second. Do not invent a
        # boundary quote or change OHLC; only the coverage test differs.
        # The first bucket and missing whole intervals remain partial.
        partial = event.event_time > start and not contiguous_quotes
        if self.coverage_mode == "continuous_quotes":
            self._bucket = _Bucket(start, end, partial=partial, accumulator=_QuoteBucketAccumulator.from_event(event))
        else:
            self._bucket = _Bucket(start, end, partial=partial)
            self._bucket.events.append(event)
        if not partial:
            return AggregationResult(tuple(emitted), True, (gap_issue,) if gap_issue is not None else ())
        partial_issue = self._new_issue("partial_bucket", "El nuevo intervalo comenzó a mitad de intervalo", event)
        issues = tuple(item for item in (gap_issue, partial_issue) if item is not None)
        return AggregationResult(tuple(emitted), True, issues)

    def _quote_continuity(self, event: MarketEvent, start: datetime) -> bool:
        bucket = self._bucket
        if (
            self.coverage_mode != "continuous_quotes"
            or self.max_quote_gap_seconds is None
            or bucket is None
            or bucket.end > start
            or (not bucket.events and bucket.accumulator is None)
        ):
            return False
        if bucket.end != start and (
            self.historical_calendar is None or not self.historical_calendar.covers_closed(bucket.end, start)
        ):
            return False
        previous_time = (
            bucket.accumulator.last_event_time
            if self.coverage_mode == "continuous_quotes" and bucket.accumulator is not None
            else bucket.events[-1].event_time
        )
        gap = self._open_seconds(previous_time, event.event_time)
        return 0 <= gap <= self.max_quote_gap_seconds

    def _append_continuous_event(self, event: MarketEvent) -> None:
        bucket = self._bucket
        if bucket is None or bucket.accumulator is None:
            raise RuntimeError("continuous bucket sin acumulador")
        previous_time = bucket.accumulator.last_event_time
        gap = self._open_seconds(previous_time, event.event_time)
        assert self.max_quote_gap_seconds is not None
        bucket.accumulator.append(event, gap, self.max_quote_gap_seconds)

    def _open_seconds(self, start: datetime, end: datetime) -> float:
        if self.historical_calendar is not None:
            return float(self.historical_calendar.open_seconds_between(start, end))
        return (end - start).total_seconds()

    def close_until(self, watermark: datetime, *, include_current: bool = True) -> AggregationResult:
        """Cierra buckets cuyo ``end`` no supera el watermark.

        No crea velas vacías: los intervalos sin eventos permanecen como
        cobertura ausente, no como precios inventados. ``include_current`` se
        mantiene por compatibilidad de API y sólo afecta al bucket activo.
        """

        watermark = normalize_utc(watermark, "watermark")
        if self._bucket is None or not include_current or watermark < self._bucket.end:
            return AggregationResult()
        candle = self._build_candle(self._bucket, closed=True, emitted_at=watermark)
        self._closed_through = self._bucket.end
        self._bucket = None
        return AggregationResult((candle,), True, ())

    def flush(self, *, close_final: bool = True) -> AggregationResult:
        """Finaliza el bucket activo al terminar una sesión de replay."""

        if self._bucket is None:
            return AggregationResult()
        # A final bucket that began mid-interval is emitted as an explicit
        # partial/open record, never as a complete closed candle.
        closed = bool(close_final and not self._bucket.partial)
        candle = self._build_candle(self._bucket, closed=closed, emitted_at=(self._bucket.end if closed else None))
        if close_final:
            self._closed_through = self._bucket.end
            self._bucket = None
        return AggregationResult((candle,), True, ())

    def _build_candle(self, bucket: _Bucket, *, closed: bool, emitted_at: datetime | None = None) -> Candle:
        if self.coverage_mode == "continuous_quotes":
            return self._build_continuous_candle(bucket, closed=closed, emitted_at=emitted_at)
        return self._build_strict_candle(bucket, closed=closed, emitted_at=emitted_at)

    def _build_strict_candle(self, bucket: _Bucket, *, closed: bool, emitted_at: datetime | None = None) -> Candle:
        events = sorted(bucket.events, key=self._event_order_key)
        prices = [event.selected_price for event in events]
        if not prices or any(price is None for price in prices):
            raise ValueError("No hay precios compatibles con la base solicitada")
        values = [float(price) for price in prices if price is not None]
        quality = merge_quality(*(event.quality for event in events), source=self.source)
        if self._quote_bucket_has_gap(bucket, events):
            quality = quality.with_flags(QualityFlag.GAP, reason="quote_gap_exceeds_declared_bound")
        if self.mode is OperationMode.SYNTHETIC:
            quality = quality.with_flags(QualityFlag.SYNTHETIC)
        first_event = events[0]
        last_event = events[-1]
        partial = bool(bucket.partial)
        if partial:
            quality = quality.with_flags(QualityFlag.PARTIAL, reason="coverage_started_mid_interval")
        latest_availability = max(event.effective_available_at for event in events)
        knowledge_at = emitted_at or latest_availability
        if knowledge_at.tzinfo is None:
            knowledge_at = knowledge_at.replace(tzinfo=UTC)
        # ``available_at`` is the first known/emitted moment, never the nominal
        # end of an interval when a later boundary event revealed the close.
        # Candle validation still enforces >= end for temporally closed bars.
        available_at = max(knowledge_at, latest_availability)
        if closed:
            available_at = max(bucket.end, available_at)
        latest_received = max((event.received_at or event.event_time for event in events), default=None)
        if self.coverage_mode == "continuous_quotes":
            observed_receipts = [event.received_at for event in events if event.received_at is not None]
            latest_received = max(observed_receipts, default=None)
        metadata = {
            "interval": "[start,end)",
            "partial": partial,
            "coverage_start": first_event.event_time.isoformat(),
            "coverage_end": last_event.event_time.isoformat(),
            "knowledge_at": knowledge_at.isoformat(),
            "emitted_at": emitted_at.isoformat() if emitted_at is not None else None,
        }
        if self.coverage_mode == "continuous_quotes":
            metadata["coverage_mode"] = self.coverage_mode
            metadata["max_quote_gap_seconds"] = self.max_quote_gap_seconds
            metadata["received_at_observed"] = all(event.received_at is not None for event in events)
            metadata["volume_basis"] = "UNKNOWN_NOT_TRADED_VOLUME"
            if self.historical_calendar is not None:
                metadata["historical_calendar"] = self.historical_calendar.to_dict()
        return Candle(
            instrument=self.instrument or events[0].instrument,
            timeframe=self.timeframe,
            start=bucket.start,
            end=bucket.end,
            open=values[0],
            high=max(values),
            low=min(values),
            close=values[-1],
            volume=sum(event.quantity or 0.0 for event in events),
            event_count=len(events),
            source=self.source,
            mode=self.mode,
            price_base=self.price_base,
            closed=closed,
            available_at=available_at,
            received_at=latest_received,
            quality=quality,
            origin="aggregated",
            metadata=metadata,
        )

    def _build_continuous_candle(self, bucket: _Bucket, *, closed: bool, emitted_at: datetime | None = None) -> Candle:
        accumulator = bucket.accumulator
        if accumulator is None or accumulator.event_count < 1:
            raise ValueError("bucket continuo sin acumulador")
        assert self.max_quote_gap_seconds is not None
        trailing_open_seconds = self._open_seconds(accumulator.last_event_time, bucket.end)
        has_gap = accumulator.gap or trailing_open_seconds > self.max_quote_gap_seconds
        quality = accumulator.quality(source=self.source)
        if has_gap:
            quality = quality.with_flags(QualityFlag.GAP, reason="quote_gap_exceeds_declared_bound")
        if self.mode is OperationMode.SYNTHETIC:
            quality = quality.with_flags(QualityFlag.SYNTHETIC)
        partial = bool(bucket.partial)
        if partial:
            quality = quality.with_flags(QualityFlag.PARTIAL, reason="coverage_started_mid_interval")
        knowledge_at = emitted_at or accumulator.latest_available_at
        if knowledge_at.tzinfo is None:
            knowledge_at = knowledge_at.replace(tzinfo=UTC)
        available_at = max(knowledge_at, accumulator.latest_available_at)
        if closed:
            available_at = max(bucket.end, available_at)
        metadata = {
            "interval": "[start,end)",
            "partial": partial,
            "coverage_start": accumulator.first_event_time.isoformat(),
            "coverage_end": accumulator.last_event_time.isoformat(),
            "knowledge_at": knowledge_at.isoformat(),
            "emitted_at": emitted_at.isoformat() if emitted_at is not None else None,
            "coverage_mode": self.coverage_mode,
            "max_quote_gap_seconds": self.max_quote_gap_seconds,
            "received_at_observed": accumulator.received_count == accumulator.event_count,
            "volume_basis": "UNKNOWN_NOT_TRADED_VOLUME",
        }
        if self.historical_calendar is not None:
            metadata["historical_calendar"] = self.historical_calendar.to_dict()
        return Candle(
            instrument=self.instrument or "unknown",
            timeframe=self.timeframe,
            start=bucket.start,
            end=bucket.end,
            open=accumulator.open,
            high=accumulator.high,
            low=accumulator.low,
            close=accumulator.close,
            volume=accumulator.volume,
            event_count=accumulator.event_count,
            source=self.source,
            mode=self.mode,
            price_base=self.price_base,
            closed=closed,
            available_at=available_at,
            received_at=accumulator.latest_received_at,
            quality=quality,
            origin="aggregated",
            metadata=metadata,
        )

    def export_bucket_state(self) -> dict[str, object] | None:
        """Export bounded continuous-quote state without retaining raw events."""

        if self.coverage_mode != "continuous_quotes":
            raise ValueError("export_bucket_state sólo aplica a continuous_quotes")
        bucket = self._bucket
        if bucket is None:
            return None
        if bucket.accumulator is None:
            raise ValueError("bucket continuo sin acumulador")
        return {
            "version": _QUOTE_BUCKET_STATE_VERSION,
            "coverage_mode": self.coverage_mode,
            "timeframe": self.timeframe.name,
            "instrument": self.instrument,
            "start": _iso(bucket.start),
            "end": _iso(bucket.end),
            "partial": bool(bucket.partial),
            "max_quote_gap_seconds": self.max_quote_gap_seconds,
            "stats": bucket.accumulator.to_dict(),
            "last_order_key": _order_key_to_dict(self._last_order_key),
        }

    def restore_bucket_state(self, value: Mapping[str, object] | None) -> None:
        """Restore a validated bounded continuous-quote bucket atomically."""

        if self.coverage_mode != "continuous_quotes":
            raise ValueError("restore_bucket_state sólo aplica a continuous_quotes")
        if value is None:
            self._bucket = None
            return
        instrument = value.get("instrument")
        if not isinstance(instrument, str) or not instrument.strip():
            raise ValueError("instrument del bucket continuo inválido")
        if self.instrument is not None and instrument != self.instrument:
            raise ValueError("instrument del bucket continuo incompatible")
        bucket, order_key = _decode_bucket_state(value, self.timeframe, self.max_quote_gap_seconds)
        if self.instrument is None:
            self.instrument = instrument
        self._bucket = bucket
        self._last_order_key = order_key
        assert bucket.accumulator is not None
        self._last_event_time = bucket.accumulator.last_event_time

    def _quote_bucket_has_gap(self, bucket: _Bucket, events: list[MarketEvent]) -> bool:
        if self.coverage_mode != "continuous_quotes" or self.max_quote_gap_seconds is None or not events:
            return False
        gaps = (
            self._open_seconds(left.event_time, right.event_time)
            for left, right in zip(events, events[1:], strict=False)
        )
        # Absence after the last quote is only assessed through this bar's
        # boundary, not through a weekend until the next opening quote.
        last_gap = self._open_seconds(events[-1].event_time, bucket.end)
        return last_gap > self.max_quote_gap_seconds or any(gap > self.max_quote_gap_seconds for gap in gaps)


def _event_sort_key(event: MarketEvent) -> _OrderKey:
    received = event.received_at or event.event_time
    available = event.available_at or received
    sequence = event.sequence
    seq_key = (
        (0, sequence)
        if isinstance(sequence, (int, float)) and not isinstance(sequence, bool)
        else (1, str(sequence or ""))
    )
    return event.event_time, available, received, seq_key, event.event_id or ""


def _order_key_to_dict(value: _OrderKey | None) -> dict[str, object] | None:
    if value is None:
        return None
    sequence_kind = "numeric" if value[3][0] == 0 else "text"
    return {
        "event_time": _iso(value[0]),
        "available": _iso(value[1]),
        "received": _iso(value[2]),
        "sequence_kind": sequence_kind,
        "sequence": value[3][1],
        "event_id": value[4],
    }


def _order_key_from_mapping(value: object) -> _OrderKey | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise ValueError("last_order_key debe ser mapping o None")
    required = {"event_time", "available", "received", "sequence_kind", "sequence", "event_id"}
    if set(value) != required:
        raise ValueError("last_order_key incompleto o con claves desconocidas")
    kind = value["sequence_kind"]
    sequence = value["sequence"]
    if kind == "numeric":
        if isinstance(sequence, bool) or not isinstance(sequence, (int, float)):
            raise ValueError("sequence numérico inválido")
        if isinstance(sequence, float) and not math.isfinite(sequence):
            raise ValueError("sequence debe ser finito")
        seq_key: tuple[int, int | float | str] = (0, sequence)
    elif kind == "text":
        if not isinstance(sequence, str):
            raise ValueError("sequence textual inválido")
        seq_key = (1, sequence)
    else:
        raise ValueError("sequence_kind inválido")
    event_id = value["event_id"]
    if not isinstance(event_id, str) or not event_id.strip():
        raise ValueError("event_id inválido")
    return (
        _state_time(value["event_time"], "last_order_key.event_time"),
        _state_time(value["available"], "last_order_key.available"),
        _state_time(value["received"], "last_order_key.received"),
        seq_key,
        event_id,
    )


def _decode_bucket_identity(
    value: Mapping[str, object], timeframe: Timeframe, max_quote_gap_seconds: float | None
) -> tuple[datetime, datetime, bool]:
    required = {
        "version",
        "coverage_mode",
        "timeframe",
        "instrument",
        "start",
        "end",
        "partial",
        "max_quote_gap_seconds",
        "stats",
        "last_order_key",
    }
    if set(value) != required:
        raise ValueError("estado de bucket continuo incompleto o con claves desconocidas")
    if value["version"] != _QUOTE_BUCKET_STATE_VERSION:
        raise ValueError("versión de bucket continuo no soportada")
    if value["coverage_mode"] != "continuous_quotes" or value["timeframe"] != timeframe.name:
        raise ValueError("identidad de bucket continuo incompatible")
    instrument = value["instrument"]
    if not isinstance(instrument, str) or not instrument.strip():
        raise ValueError("instrument del bucket continuo inválido")
    if not isinstance(value["partial"], bool):
        raise ValueError("bucket.partial debe ser booleano")
    start = _state_time(value["start"], "bucket.start")
    end = _state_time(value["end"], "bucket.end")
    if end - start != timeframe.delta:
        raise ValueError("intervalo de bucket no coincide con timeframe")
    max_gap = _finite_float(value["max_quote_gap_seconds"], "max_quote_gap_seconds")
    if max_quote_gap_seconds is None or max_gap != max_quote_gap_seconds:
        raise ValueError("max_quote_gap_seconds incompatible")
    return start, end, value["partial"]


def _decode_bucket_state(
    value: Mapping[str, object], timeframe: Timeframe, max_quote_gap_seconds: float | None
) -> tuple[_Bucket, _OrderKey | None]:
    start, end, partial = _decode_bucket_identity(value, timeframe, max_quote_gap_seconds)
    stats = value["stats"]
    if not isinstance(stats, Mapping):
        raise ValueError("stats de bucket continuo debe ser mapping")
    accumulator = _QuoteBucketAccumulator.from_mapping(stats)
    if not start <= accumulator.first_event_time < end or not start <= accumulator.last_event_time < end:
        raise ValueError("timestamps de bucket fuera del intervalo")
    if accumulator.first_event_time > accumulator.last_event_time:
        raise ValueError("orden temporal del bucket inválido")
    order_key = _order_key_from_mapping(value["last_order_key"])
    if order_key is None or order_key[0] != accumulator.last_event_time:
        raise ValueError("last_order_key no corresponde al último evento")
    return _Bucket(start, end, partial=partial, accumulator=accumulator), order_key


def aggregate_events(
    events: Iterable[MarketEvent],
    timeframe: Timeframe | str,
    *,
    instrument: str | None = None,
    price_base: PriceBase | str = PriceBase.TRADED,
    source: str = "aggregated",
    mode: OperationMode | str = OperationMode.REPLAY,
    close_final: bool = True,
    reject_invalid: bool = True,
    return_report: bool = False,
) -> list[Candle] | tuple[list[Candle], QualityReport]:
    """Agrega un lote en orden determinista y devuelve velas sin huecos ficticios.

    Por defecto el lote histórico cierra su último intervalo para facilitar
    importación/backtest, pero la disponibilidad queda en ``end`` (o después
    si hubo recepción tardía), por lo que una evaluación en mitad del
    intervalo no puede usar ese cierre. Para observar una sesión parcial use
    ``close_final=False``.
    """

    raw_events = list(events)
    issues: list[QualityIssue] = []
    valid_events: list[MarketEvent] = []
    for event in raw_events:
        if not isinstance(event, MarketEvent):
            issues.append(QualityIssue("event_type", "Se esperaba MarketEvent"))
            continue
        validation = validate_event(event)
        if not validation.accepted:
            issues.extend(validation.issues)
            continue
        valid_events.append(event)
    # Conserva evidencia de que la fuente llegó fuera de orden antes de
    # aplicar el orden determinista de reproducción. No se descarta por
    # sorpresa en lote: ``reject_invalid`` controla sólo la inserción.
    input_keys = [_event_sort_key(event) for event in valid_events]
    if any(right < left for left, right in zip(input_keys, input_keys[1:], strict=False)):
        issues.append(QualityIssue("out_of_order", "La entrada contenía eventos fuera de orden"))
    valid_events.sort(key=_event_sort_key)
    unique: list[MarketEvent] = []
    seen: set[str] = set()
    duplicates = 0
    for event in valid_events:
        event_id = event.event_id
        assert event_id is not None
        if event_id in seen:
            duplicates += 1
            issues.append(
                QualityIssue(
                    "duplicate_event",
                    f"Evento duplicado: {event_id}",
                    record_id=event_id,
                    timestamp=event.event_time,
                )
            )
            continue
        seen.add(event_id)
        unique.append(event)
    aggregator = CandleAggregator(
        timeframe,
        instrument=instrument,
        price_base=price_base,
        source=source,
        mode=mode,
        reject_out_of_order=reject_invalid,
    )
    candles: list[Candle] = []
    for event in unique:
        result = aggregator.add(event)
        candles.extend(result.emitted)
        issues.extend(result.issues)
    final = aggregator.flush(close_final=close_final)
    candles.extend(final.emitted)
    issues.extend(final.issues)
    if return_report:
        report = QualityReport(
            accepted=len(unique),
            rejected=len(raw_events) - len(unique),
            duplicates=duplicates,
            out_of_order=sum(1 for issue in issues if issue.code.startswith("out_of_order")),
            invalid=sum(1 for issue in issues if issue.code in {"invalid_event", "non_finite", "timestamp_invalid"}),
            gaps=sum(1 for issue in issues if issue.code == "gap"),
            coverage_start=min((event.event_time for event in unique), default=None),
            coverage_end=max((event.event_time for event in unique), default=None),
            issues=tuple(issues),
        )
        return candles, report
    return candles


__all__ = [
    "AggregationResult",
    "CandleAggregator",
    "aggregate_events",
    "interval_start",
]
