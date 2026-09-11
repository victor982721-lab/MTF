"""Calidad y validación de datos para el núcleo.

La calidad no se reduce a un booleano: una barra puede ser sintética pero
válida, o válida para visualización y no válida para una evaluación cerrada
por estar abierta, atrasada o sin reconciliar.  Las banderas se conservan
para que persistencia e interfaz puedan explicar cada bloqueo.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import math
from typing import Any, Iterable, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - sólo soporte de type checkers
    from .models import Candle, MarketEvent


class QualityFlag(str, Enum):
    VALID = "valid"
    SYNTHETIC = "synthetic"
    INVALID = "invalid"
    DUPLICATE = "duplicate"
    OUT_OF_ORDER = "out_of_order"
    GAP = "gap"
    DISCONNECTED = "disconnected"
    STALE = "stale"
    UNRECONCILED = "unreconciled"
    INSUFFICIENT = "insufficient"
    PARTIAL = "partial"
    OPEN = "open"
    LATE = "late"


class QualityState(str, Enum):
    """Estados de calidad que pueden viajar por una frontera de dominio.

    ``QualityFlag`` conserva la representación histórica de múltiples
    banderas. Este enum ofrece un estado escalar para consumidores que no
    deben interpretar texto libre. Cuando hay varias banderas, ``state`` usa
    exactamente la precedencia documentada por :attr:`DataQuality.status`.
    """

    VALID = "valid"
    SYNTHETIC = "synthetic"
    INVALID = "invalid"
    DUPLICATE = "duplicate"
    OUT_OF_ORDER = "out_of_order"
    GAP = "gap"
    DISCONNECTED = "disconnected"
    STALE = "stale"
    UNRECONCILED = "unreconciled"
    INSUFFICIENT = "insufficient"
    PARTIAL = "partial"
    OPEN = "open"
    LATE = "late"
    UNKNOWN = "unknown"
    BLOCKED = "blocked"


class QualityReason(str, Enum):
    """Códigos de razón conocidos, separados de su texto de presentación.

    Las razones heredadas pueden seguir siendo texto descriptivo en
    ``DataQuality.reasons``. ``reason_codes`` sólo reconoce coincidencias
    exactas con este vocabulario; nunca clasifica por una búsqueda de
    subcadena.
    """

    INSTRUMENT_MISSING = "instrument_missing"
    TIMESTAMP_INVALID = "timestamp_invalid"
    INTERVAL_INVALID = "interval_invalid"
    TIMEFRAME_MISMATCH = "timeframe_mismatch"
    NON_FINITE = "non_finite"
    PRICE_BASE_MISSING = "price_base_missing"
    PRICE_BASE_CONFLICT = "price_base_conflict"
    PRICE_BASE_AMBIGUOUS = "price_base_ambiguous"
    QUALITY_BLOCKED = "quality_blocked"
    QUALITY_UNKNOWN = "quality_unknown"
    QUALITY_INVALID = "quality_invalid"
    QUANTITY_NEGATIVE = "quantity_negative"
    CANDLE_OPEN = "candle_open"
    CANDLE_SHAPE_INVALID = "candle_shape_invalid"
    OHLC_INCOHERENT = "ohlc_incoherent"
    NEGATIVE_MEASURE = "negative_measure"
    SOURCE_TIMESTAMP_MISSING = "source_timestamp_missing"
    MISSING_SOURCE_TIMESTAMP = "source_timestamp_missing"
    AVAILABILITY_UNKNOWN = "availability_unknown"
    SNAPSHOT = "snapshot"
    CROSSED = "crossed"
    STALE = "stale"
    OUT_OF_ORDER = "out_of_order"
    OUT_OF_ORDER_REASON = "out_of_order"
    DISCONNECTED = "disconnected"
    DISCONNECTED_REASON = "disconnected"
    SESSION_MISMATCH = "session_mismatch"
    FUTURE_SOURCE_TIMESTAMP = "future_source_timestamp"
    GAP = "gap"
    GAP_REASON = "gap"
    PARTIAL = "partial"
    PARTIAL_REASON = "partial"
    OPEN = "open"
    OPEN_REASON = "open"
    LATE = "late"
    LATE_REASON = "late"


# Names used by a few adapters/readers; aliases keep one vocabulary rather
# than creating parallel enums with subtly different values.
QualityStatus = QualityState
ReasonCode = QualityReason


_QUALITY_REASON_BY_VALUE = {reason.value: reason for reason in QualityReason}


_BLOCKING_FLAGS = frozenset(
    {
        QualityFlag.INVALID,
        QualityFlag.DUPLICATE,
        QualityFlag.OUT_OF_ORDER,
        QualityFlag.GAP,
        QualityFlag.DISCONNECTED,
        QualityFlag.STALE,
        QualityFlag.UNRECONCILED,
        QualityFlag.INSUFFICIENT,
        QualityFlag.PARTIAL,
        QualityFlag.OPEN,
        QualityFlag.LATE,
    }
)


@dataclass(frozen=True, slots=True)
class DataQuality:
    """Banderas y razones de calidad de un evento o una vela."""

    flags: frozenset[QualityFlag] = field(default_factory=frozenset)
    reasons: tuple[str, ...] = ()
    source: str | None = None

    def __post_init__(self) -> None:
        normalized: set[QualityFlag] = set()
        for flag in self.flags:
            if isinstance(flag, QualityFlag):
                normalized.add(flag)
                continue
            if not isinstance(flag, str):
                raise TypeError("quality flags must be QualityFlag values or strings")
            normalized.add(QualityFlag(flag.strip().lower()))
        object.__setattr__(self, "flags", frozenset(normalized))
        normalized_reasons: list[str] = []
        for reason in self.reasons:
            if isinstance(reason, QualityReason):
                normalized_reasons.append(reason.value)
            elif isinstance(reason, str):
                normalized_reasons.append(reason)
            else:
                raise TypeError("quality reasons must be strings or QualityReason values")
        object.__setattr__(self, "reasons", tuple(normalized_reasons))
        if self.source is not None:
            if not isinstance(self.source, str):
                raise TypeError("quality source must be a string")
            object.__setattr__(self, "source", self.source)

    @classmethod
    def valid(cls, *, synthetic: bool = False, source: str | None = None) -> "DataQuality":
        return cls(
            frozenset({QualityFlag.SYNTHETIC} if synthetic else set()),
            source=source,
        )

    @classmethod
    def good(cls, *, synthetic: bool = False, source: str | None = None) -> "DataQuality":
        """Alias legible para callers del núcleo y compatibilidad de API."""
        return cls(
            frozenset({QualityFlag.SYNTHETIC} if synthetic else set()),
            source=source,
        )

    @classmethod
    def from_state(
        cls,
        state: QualityState | str,
        reasons: Iterable[str | QualityReason] = (),
        *,
        source: str | None = None,
    ) -> "DataQuality":
        """Construct quality from an exact typed state.

        Unknown states are conservatively represented as ``invalid`` rather
        than silently becoming valid.
        """

        if isinstance(state, QualityState):
            normalized = state
        elif isinstance(state, str):
            try:
                normalized = QualityState(state.strip().lower())
            except ValueError:
                normalized = QualityState.UNKNOWN
        else:
            raise TypeError("quality state must be QualityState or string")
        if normalized is QualityState.VALID:
            flags: frozenset[QualityFlag] = frozenset()
        elif normalized is QualityState.UNKNOWN or normalized is QualityState.BLOCKED:
            flags = frozenset({QualityFlag.INVALID})
        else:
            flags = frozenset({QualityFlag(normalized.value)})
        return cls(flags, tuple(reasons), source)

    @classmethod
    def from_flag(
        cls,
        flag: QualityFlag | str,
        reason: str | QualityReason = "",
        *,
        source: str | None = None,
    ) -> "DataQuality":
        normalized_flag = flag if isinstance(flag, QualityFlag) else QualityFlag(flag)
        return cls(frozenset({normalized_flag}), (reason,) if reason else (), source)

    @property
    def usable(self) -> bool:
        """Alias de ``valid`` para llamadores del detector."""

        return self.valid

    @property
    def state(self) -> QualityState:
        """Estado tipado con la misma precedencia que ``status``."""

        try:
            return QualityState(self.status)
        except ValueError:  # pragma: no cover - defensive for future flags
            return QualityState.UNKNOWN

    @property
    def quality_state(self) -> QualityState:
        """Alias explícito para fronteras que evitan el nombre genérico state."""

        return self.state

    @property
    def reason_codes(self) -> tuple[QualityReason, ...]:
        """Razones conocidas por coincidencia exacta, sin heurísticas de texto."""

        return tuple(
            code
            for reason in self.reasons
            if (code := _QUALITY_REASON_BY_VALUE.get(reason)) is not None
        )

    @property
    def typed_reasons(self) -> tuple[QualityReason, ...]:
        """Alias legible para consumidores que requieren razones tipadas."""

        return self.reason_codes

    @property
    def status(self) -> str:
        if not self.flags:
            return QualityFlag.VALID.value
        # Orden estable y útil para registros humanos.
        for candidate in (
            QualityFlag.INVALID,
            QualityFlag.DISCONNECTED,
            QualityFlag.STALE,
            QualityFlag.UNRECONCILED,
            QualityFlag.GAP,
            QualityFlag.OUT_OF_ORDER,
            QualityFlag.DUPLICATE,
            QualityFlag.INSUFFICIENT,
            QualityFlag.PARTIAL,
            QualityFlag.OPEN,
            QualityFlag.LATE,
            QualityFlag.SYNTHETIC,
        ):
            if candidate in self.flags:
                return candidate.value
        return sorted(flag.value for flag in self.flags)[0]

    def has(self, flag: QualityFlag | str) -> bool:
        return QualityFlag(flag) in self.flags

    def with_flags(
        self,
        *flags: QualityFlag | str,
        reason: str | QualityReason = "",
    ) -> "DataQuality":
        extra: set[QualityFlag] = set()
        for flag in flags:
            if isinstance(flag, QualityFlag):
                extra.add(flag)
            elif isinstance(flag, str):
                extra.add(QualityFlag(flag.strip().lower()))
            else:
                raise TypeError("quality flags must be QualityFlag values or strings")
        reasons = self.reasons + ((reason,) if reason else ())
        return DataQuality(self.flags | frozenset(extra), reasons, self.source)

    def to_dict(self) -> dict[str, Any]:
        """Serializa calidad sin convertir enums u objetos arbitrarios a texto."""

        return {
            "state": self.state.value,
            "flags": sorted(flag.value for flag in self.flags),
            "reasons": list(self.reasons),
            "reason_codes": [reason.value for reason in self.reason_codes],
            "source": self.source,
        }


class _ValidityDescriptor:
    """Compatibilidad: ``DataQuality.valid`` es bool en instancias y
    ``DataQuality.valid()`` fabrica una calidad válida al acceder a la clase.
    """

    def __get__(self, instance: DataQuality | None, owner: type[DataQuality]):
        if instance is None:
            return owner.good
        return not bool(instance.flags & _BLOCKING_FLAGS)


# Se instala después de que la clase exista para conservar ambos usos sin que
# un classmethod o un property oculten al otro.
DataQuality.valid = _ValidityDescriptor()  # type: ignore[attr-defined]


@dataclass(frozen=True, slots=True)
class QualityIssue:
    code: str
    message: str
    severity: str = "error"
    record_id: str | None = None
    timestamp: Any | None = None


@dataclass(frozen=True, slots=True)
class ValidationResult:
    """Resultado explícito de validación, sin descartar evidencia en silencio."""

    accepted: bool
    issues: tuple[QualityIssue, ...] = ()
    quality: DataQuality = field(default_factory=DataQuality.good)

    @property
    def valid(self) -> bool:
        return self.accepted and self.quality.valid


@dataclass(frozen=True, slots=True)
class QualityReport:
    """Resumen de calidad de una cobertura de eventos o barras."""

    accepted: int = 0
    rejected: int = 0
    duplicates: int = 0
    out_of_order: int = 0
    invalid: int = 0
    gaps: int = 0
    coverage_start: Any | None = None
    coverage_end: Any | None = None
    issues: tuple[QualityIssue, ...] = ()

    @property
    def coverage(self) -> tuple[Any | None, Any | None]:
        return self.coverage_start, self.coverage_end


def _is_finite(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def validate_event(event: "MarketEvent") -> ValidationResult:
    """Valida una instancia de :class:`MarketEvent` sin cambiarla."""

    issues: list[QualityIssue] = []
    event_id = getattr(event, "event_id", None)
    if not getattr(event, "instrument", ""):
        issues.append(QualityIssue("instrument_missing", "Falta instrumento", record_id=event_id))
    if not getattr(event, "event_time", None) or getattr(event.event_time, "tzinfo", None) is None:
        issues.append(QualityIssue("timestamp_invalid", "event_time debe ser consciente de zona horaria", record_id=event_id))
    for field_name in ("price", "quantity", "bid", "ask"):
        value = getattr(event, field_name, None)
        if value is not None and not _is_finite(value):
            issues.append(QualityIssue("non_finite", f"{field_name} no es finito", record_id=event_id))
    selected = getattr(event, "selected_price", None)
    price_base = getattr(event, "price_base", None)
    if selected is None or not _is_finite(selected):
        issues.append(
            QualityIssue(
                "price_base_missing",
                f"No existe precio para la base explícita {price_base!s}",
                record_id=event_id,
            )
        )
    if getattr(event, "quantity", None) is not None and event.quantity < 0:
        issues.append(QualityIssue("quantity_negative", "quantity no puede ser negativa", record_id=event_id))
    quality = getattr(event, "quality", None)
    if quality is not None and getattr(quality, "flags", frozenset()) & _BLOCKING_FLAGS:
        issues.append(QualityIssue("quality_blocked", ",".join(sorted(flag.value for flag in quality.flags)), record_id=event_id))
    if issues:
        return ValidationResult(False, tuple(issues), DataQuality(frozenset({QualityFlag.INVALID}), tuple(issue.code for issue in issues)))
    return ValidationResult(True, (), event.quality)


def validate_candle(candle: "Candle", *, require_closed: bool = False) -> ValidationResult:
    """Valida OHLC, intervalo, temporalidad y calidad de una vela."""

    issues: list[QualityIssue] = []
    candle_id = getattr(candle, "candle_id", None)
    try:
        start = candle.start
        end = candle.end
        timeframe = candle.timeframe
        if start.tzinfo is None or end.tzinfo is None:
            issues.append(QualityIssue("timestamp_invalid", "start/end requieren zona horaria", record_id=candle_id))
        if end <= start:
            issues.append(QualityIssue("interval_invalid", "end debe ser posterior a start", record_id=candle_id))
        if end - start != timeframe.delta:
            issues.append(QualityIssue("timeframe_mismatch", "intervalo no coincide con timeframe", record_id=candle_id))
    except Exception as exc:  # pragma: no cover - defensa para entradas externas
        issues.append(QualityIssue("candle_shape_invalid", str(exc), record_id=candle_id))
    for field_name in ("open", "high", "low", "close", "volume"):
        if not _is_finite(getattr(candle, field_name, None)):
            issues.append(QualityIssue("non_finite", f"{field_name} no es finito", record_id=candle_id))
    if not issues:
        if candle.high < max(candle.open, candle.close, candle.low):
            issues.append(QualityIssue("ohlc_incoherent", "high incompatible con OHLC", record_id=candle_id))
        if candle.low > min(candle.open, candle.close, candle.high):
            issues.append(QualityIssue("ohlc_incoherent", "low incompatible con OHLC", record_id=candle_id))
        if candle.volume < 0 or candle.event_count < 0:
            issues.append(QualityIssue("negative_measure", "volume/event_count no puede ser negativo", record_id=candle_id))
    if require_closed and not candle.closed:
        issues.append(QualityIssue("candle_open", "se requiere una vela cerrada", record_id=candle_id))
    if candle.quality.flags & _BLOCKING_FLAGS:
        issues.append(QualityIssue("quality_blocked", ",".join(sorted(f.value for f in candle.quality.flags)), record_id=candle_id))
    if issues:
        return ValidationResult(False, tuple(issues), DataQuality(frozenset({QualityFlag.INVALID}), tuple(issue.code for issue in issues)))
    return ValidationResult(True, (), candle.quality)


def merge_quality(*qualities: DataQuality | None, source: str | None = None) -> DataQuality:
    flags: set[QualityFlag] = set()
    reasons: list[str] = []
    for quality in qualities:
        if quality is None:
            continue
        flags.update(quality.flags)
        reasons.extend(quality.reasons)
    return DataQuality(frozenset(flags), tuple(dict.fromkeys(reasons)), source)


__all__ = [
    "DataQuality",
    "QualityFlag",
    "QualityReason",
    "QualityState",
    "QualityStatus",
    "ReasonCode",
    "QualityIssue",
    "QualityReport",
    "ValidationResult",
    "merge_quality",
    "validate_candle",
    "validate_event",
]


@dataclass(frozen=True, slots=True)
class FreshnessAssessment:
    """Edades separadas del feed y de la última vela cerrada."""
    feed_age_seconds: float | None
    closed_candle_age_seconds: float | None
    quality: DataQuality


def assess_freshness(
    *,
    now: Any,
    last_received_at: Any | None,
    last_closed_end: Any | None,
    max_feed_age_seconds: float,
    max_closed_candle_age_seconds: float,
) -> FreshnessAssessment:
    """Marca ``STALE`` sin confundir atraso de feed con edad normal de M15."""
    from datetime import datetime, timezone
    def parse(value: Any | None) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            dt = value
        else:
            text = str(value).replace("Z", "+00:00")
            dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            raise ValueError("freshness timestamps require timezone")
        return dt.astimezone(timezone.utc)
    current = parse(now)
    if current is None:
        raise ValueError("now is required")
    feed_dt = parse(last_received_at); candle_dt = parse(last_closed_end)
    feed_age = (current - feed_dt).total_seconds() if feed_dt else None
    candle_age = (current - candle_dt).total_seconds() if candle_dt else None
    if feed_age is not None and feed_age < 0 or candle_age is not None and candle_age < 0:
        raise ValueError("freshness timestamps cannot be in the future")
    flags: set[QualityFlag] = set(); reasons: list[str] = []
    if feed_age is None or feed_age > float(max_feed_age_seconds):
        flags.add(QualityFlag.STALE); reasons.append(f"feed_age_seconds={feed_age!r} > {max_feed_age_seconds}")
    if candle_age is None or candle_age > float(max_closed_candle_age_seconds):
        flags.add(QualityFlag.LATE); reasons.append(f"closed_candle_age_seconds={candle_age!r} > {max_closed_candle_age_seconds}")
    return FreshnessAssessment(feed_age, candle_age, DataQuality(frozenset(flags), tuple(reasons)))

__all__.extend(["FreshnessAssessment", "assess_freshness"])
