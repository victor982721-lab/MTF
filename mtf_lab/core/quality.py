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
            normalized.add(flag if isinstance(flag, QualityFlag) else QualityFlag(str(flag).lower()))
        object.__setattr__(self, "flags", frozenset(normalized))
        object.__setattr__(self, "reasons", tuple(str(reason) for reason in self.reasons))
        if self.source is not None:
            object.__setattr__(self, "source", str(self.source))

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
    def from_flag(cls, flag: QualityFlag | str, reason: str = "", *, source: str | None = None) -> "DataQuality":
        return cls(frozenset({QualityFlag(flag)}), (reason,) if reason else (), source)

    @property
    def usable(self) -> bool:
        """Alias de ``valid`` para llamadores del detector."""

        return self.valid

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

    def with_flags(self, *flags: QualityFlag | str, reason: str = "") -> "DataQuality":
        extra = {flag if isinstance(flag, QualityFlag) else QualityFlag(str(flag).lower()) for flag in flags}
        reasons = self.reasons + ((reason,) if reason else ())
        return DataQuality(self.flags | frozenset(extra), reasons, self.source)


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
