"""Agregación causal de eventos a velas de intervalos fijos."""

from __future__ import annotations

import math
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime

from .models import Candle, MarketEvent, OperationMode, PriceBase, Timeframe, normalize_utc, parse_timeframe
from .quality import QualityFlag, QualityIssue, QualityReport, merge_quality, validate_event

_OrderKey = tuple[datetime, datetime, datetime, tuple[int, int | float | str], str]


def interval_start(timestamp: datetime, timeframe: Timeframe | str) -> datetime:
    """Devuelve el inicio de ``[inicio, fin)`` anclado a Unix epoch UTC."""

    timestamp = normalize_utc(timestamp, "timestamp")
    tf = parse_timeframe(timeframe)
    seconds = timestamp.timestamp()
    start_epoch = math.floor(seconds / tf.seconds) * tf.seconds
    return datetime.fromtimestamp(start_epoch, tz=UTC)


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

    def __post_init__(self) -> None:
        if self.partial is None:
            self.partial = bool(self.events and min(event.event_time for event in self.events) > self.start)


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
    ) -> None:
        self.timeframe = parse_timeframe(timeframe)
        self.instrument = instrument.strip() if instrument else None
        if self.instrument == "":
            self.instrument = None
        self.price_base = price_base if isinstance(price_base, PriceBase) else PriceBase(str(price_base).lower())
        self.source = str(source).strip() or "aggregated"
        self.mode = mode if isinstance(mode, OperationMode) else OperationMode(str(mode).upper())
        self.reject_out_of_order = reject_out_of_order
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
        self._bucket.events.append(event)
        return AggregationResult()

    def _start_bucket(self, event: MarketEvent, start: datetime, end: datetime) -> AggregationResult:
        partial = event.event_time > start
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
        emitted = [self._build_candle(self._bucket, closed=True, emitted_at=event.effective_available_at)]
        self._closed_through = self._bucket.end
        gap_issue: QualityIssue | None = None
        if start > self._bucket.end:
            gap_issue = self._new_issue(
                "gap", f"Sin eventos entre {self._bucket.end.isoformat()} y {start.isoformat()}", event
            )
        partial = event.event_time > start
        self._bucket = _Bucket(start, end, partial=partial)
        self._bucket.events.append(event)
        if not partial:
            return AggregationResult(tuple(emitted), True, (gap_issue,) if gap_issue is not None else ())
        partial_issue = self._new_issue("partial_bucket", "El nuevo intervalo comenzó a mitad de intervalo", event)
        issues = tuple(item for item in (gap_issue, partial_issue) if item is not None)
        return AggregationResult(tuple(emitted), True, issues)

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
        events = sorted(bucket.events, key=self._event_order_key)
        prices = [event.selected_price for event in events]
        if not prices or any(price is None for price in prices):
            raise ValueError("No hay precios compatibles con la base solicitada")
        values = [float(price) for price in prices if price is not None]
        quality = merge_quality(*(event.quality for event in events), source=self.source)
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
        metadata = {
            "interval": "[start,end)",
            "partial": partial,
            "coverage_start": first_event.event_time.isoformat(),
            "coverage_end": last_event.event_time.isoformat(),
            "knowledge_at": knowledge_at.isoformat(),
            "emitted_at": emitted_at.isoformat() if emitted_at is not None else None,
        }
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
