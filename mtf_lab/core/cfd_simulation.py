"""Deterministic local CFD/PAPER domain.

This module is the single implementation behind the historical
``mtf_lab.ops.cfd_simulation`` import facade.  It deliberately has no provider,
SQLite, socket or broker dependency.  Quotes carry independent bid/ask
evidence and the simulator exposes one state machine for incremental ingest,
replay, checkpoints and completion.
"""

from __future__ import annotations

import hashlib
from collections import deque
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, fields, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import Enum, StrEnum
from typing import Any

from .canonical import fingerprint as _strict_fingerprint
from .cfd_quality import (
    QuoteAssessment,
    QuoteLeg,
    QuoteQuality,
    QuoteReason,
    QuoteSide,
    assess_pair,
    quality_from,
    quality_reasons_from,
)
from .numeric import (
    DECIMAL_POLICY_VERSION,
    DEFAULT_DECIMAL_POLICY,
    decimal_context,
    quantize_decimal,
    seconds_decimal,
)

D0 = Decimal("0")
D1 = Decimal("1")
CFD_SNAPSHOT_VERSION = 3
CFD_PRODUCT = "FOREX_CFD_LOCAL_PAPER"
CFD_ECONOMICS_LEGACY_VERSION = "cfd-economics-v1"
CFD_ECONOMICS_VERSION = "cfd-economics-v2"
ECONOMICS_VERSION = CFD_ECONOMICS_VERSION
LEGACY_ECONOMICS_VERSION = CFD_ECONOMICS_LEGACY_VERSION
_SUPPORTED_ECONOMICS_VERSIONS = frozenset({CFD_ECONOMICS_LEGACY_VERSION, CFD_ECONOMICS_VERSION})
_ALLOWED_FILL_POLICIES = {"first_quote_at_or_after"}
_SIGNAL_USABLE_QUALITY = frozenset(
    {
        QuoteQuality.VALID,
        QuoteQuality.VALIDATED,
        QuoteQuality.OK,
        QuoteQuality.GOOD,
        QuoteQuality.CLOSED_VALID,
        QuoteQuality.SYNTHETIC,
        QuoteQuality.SYNTHETIC_VALID,
        QuoteQuality.SYNTHETIC_VALIDATED,
        QuoteQuality.DATA_QUALITY_VALIDATED,
        QuoteQuality.PUBLIC_PROVIDER_CLOSED,
    }
)


class CFDSimulationError(ValueError):
    """Configuration or input error; no trade is silently fabricated."""

    def __init__(self, message: str, *, code: str = "CFD_INPUT_INVALID") -> None:
        super().__init__(message)
        self.code = code


class Direction(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"


class TradeState(StrEnum):
    PENDING = "PENDING"
    FILLED = "FILLED"
    CLOSED = "CLOSED"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"


class EconomicState(StrEnum):
    NOT_SETTLED = "NOT_SETTLED"
    DETERMINED = "DETERMINED"
    INDETERMINATE = "INDETERMINATE"


def decimal(value: Any, *, name: str, minimum: Decimal | None = None, positive: bool = False) -> Decimal:
    """Convert exact textual/numeric input to a finite Decimal."""

    if isinstance(value, bool):
        raise CFDSimulationError(f"{name} debe ser Decimal/número, no booleano")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise CFDSimulationError(f"{name} no es decimal válido: {value!r}") from exc
    if not result.is_finite():
        raise CFDSimulationError(f"{name} debe ser finito")
    if positive and result <= D0:
        raise CFDSimulationError(f"{name} debe ser positivo")
    if minimum is not None and result < minimum:
        raise CFDSimulationError(f"{name} debe ser >= {minimum}")
    return result


def _normalise_economics_version(value: Any, *, default: str = CFD_ECONOMICS_VERSION) -> str:
    """Normalize the additive economics contract without upgrading legacy data."""

    if value is None:
        value = default
    raw = str(value).strip().lower().replace("_", "-")
    aliases = {
        "1": CFD_ECONOMICS_LEGACY_VERSION,
        "v1": CFD_ECONOMICS_LEGACY_VERSION,
        "cfd-economics-v1": CFD_ECONOMICS_LEGACY_VERSION,
        "2": CFD_ECONOMICS_VERSION,
        "v2": CFD_ECONOMICS_VERSION,
        "cfd-economics-v2": CFD_ECONOMICS_VERSION,
    }
    try:
        return aliases[raw]
    except KeyError as exc:
        raise CFDSimulationError(f"economics_version no soportada: {value!r}") from exc


def _utc(value: Any, *, name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except (TypeError, ValueError) as exc:
            raise CFDSimulationError(f"{name} debe ser ISO-8601 con zona: {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CFDSimulationError(f"{name} debe incluir zona horaria")
    return parsed.astimezone(UTC)


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z") if value else None


def _id(value: Any, *, name: str) -> str:
    text = str(value).strip() if value is not None else ""
    if not text:
        raise CFDSimulationError(f"{name} no puede estar vacío")
    return text


def _direction(value: Any) -> Direction:
    if isinstance(value, Direction):
        return value
    aliases = {
        "LONG": Direction.LONG,
        "BUY": Direction.LONG,
        "UP": Direction.LONG,
        "SHORT": Direction.SHORT,
        "SELL": Direction.SHORT,
        "DOWN": Direction.SHORT,
    }
    try:
        return aliases[str(value).strip().upper()]
    except KeyError as exc:
        raise CFDSimulationError(f"dirección desconocida: {value!r}") from exc


def _bool(value: Any, *, name: str, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        raw = value.strip().lower()
        if raw in {"true", "1", "yes", "si", "sí", "on"}:
            return True
        if raw in {"false", "0", "no", "off"}:
            return False
    raise CFDSimulationError(f"{name} debe ser booleano: {value!r}")


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list, set, frozenset)):
        return [_jsonable(item) for item in value]
    if isinstance(value, datetime):
        return _iso(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return _jsonable(value.to_dict())
    return value


def _digest(value: Any) -> str:
    return _strict_fingerprint(value)


def _timedelta_seconds(value: Decimal) -> timedelta:
    with decimal_context():
        micros = int((value * Decimal(1_000_000)).to_integral_value())
    return timedelta(microseconds=micros)


def _instrument_quote_currency(instrument: str, configured: str | None) -> str | None:
    if configured:
        return configured.strip().upper() or None
    parts = instrument.upper().replace("-", "/").split("/")
    return parts[1] if len(parts) == 2 and parts[1] else None


@dataclass(frozen=True, slots=True)
class CFDQuote:
    """Quote with independent bid/ask provenance and availability evidence.

    ``bid`` or ``ask`` may be ``None`` for a one-sided update.  The simulator
    retains the other side in its quote book, but a fill is never possible
    until the required side is present and usable.  Legacy full quotes with no
    side metadata use ``market_time`` as their explicit source-time evidence;
    provider adapters opt into strict side evidence by supplying the metadata
    keys documented below.
    """

    instrument: str
    market_time: datetime
    bid: Decimal | None
    ask: Decimal | None
    quote_id: str
    available_at: datetime | None = None
    source: str = "fixture"
    sequence: int | str | None = None
    quality: Any = QuoteQuality.VALID
    metadata: Mapping[str, Any] | None = None
    # Typed adapter-facing spellings; quality remains for compatibility.
    quote_quality: Any = None
    quote_reasons: Sequence[QuoteReason | str] | None = None
    source_timestamp_missing: bool | None = None
    bid_source_timestamp_missing: bool | None = None
    ask_source_timestamp_missing: bool | None = None
    bid_age_seconds: Decimal | int | str | None = None
    ask_age_seconds: Decimal | int | str | None = None
    bid_source_timestamp: datetime | None = None
    ask_source_timestamp: datetime | None = None
    bid_received_at: datetime | None = None
    ask_received_at: datetime | None = None
    bid_available_at: datetime | None = None
    ask_available_at: datetime | None = None
    bid_quality: Any = None
    ask_quality: Any = None
    bid_timestamp_known: bool | None = None
    ask_timestamp_known: bool | None = None
    updated_sides: tuple[str, ...] | Sequence[str] | str | None = None
    session_generation: int | str | None = None
    connection_generation: int | str | None = None
    is_snapshot: bool | None = None
    disconnected: bool = False
    bid_quality_reasons: Sequence[QuoteReason | str] | None = None
    ask_quality_reasons: Sequence[QuoteReason | str] | None = None

    def __post_init__(self) -> None:
        metadata = dict(self.metadata or {})
        if isinstance(self.quote_quality, Mapping):
            metadata.setdefault("quote_quality", self.quote_quality)
        elif isinstance(self.quality, Mapping):
            metadata.setdefault("quote_quality", self.quality)
        metadata = _expand_quote_metadata(metadata)
        basic = _quote_basic(self, metadata)
        session, connection = _quote_session(self, metadata)
        strict = _quote_is_strict(self, metadata)
        legs = _quote_legs(self, metadata, basic["market"], basic["available"], strict)
        updated = _updated_sides(self.updated_sides, metadata, basic["bid"] is not None, basic["ask"] is not None)
        values = _quote_values(
            self,
            metadata=metadata,
            basic=basic,
            session=session,
            connection=connection,
            legs=legs,
            updated=updated,
        )
        for name, value in values.items():
            object.__setattr__(self, name, value)

    @property
    def identity(self) -> str:
        return self.quote_id

    @property
    def available_ts(self) -> datetime:
        return self.available_at or self.market_time

    @property
    def spread(self) -> Decimal | None:
        if self.bid is None or self.ask is None:
            return None
        with decimal_context():
            return self.ask - self.bid

    @property
    def mid(self) -> Decimal | None:
        """Arithmetic midpoint only; it is not evidence of an operable quote."""

        if self.bid is None or self.ask is None:
            return None
        with decimal_context():
            return (self.bid + self.ask) / Decimal(2)

    def mid_at(
        self,
        *,
        at: datetime | None = None,
        max_age_seconds: Decimal | int | str | None = None,
        expected_session: int | str | None = None,
        connected: bool = True,
        out_of_order: bool = False,
    ) -> Decimal | None:
        """Return a midpoint only when both legs pass the operational gate."""

        if not self.operable_for(
            QuoteSide.MID,
            at=at,
            max_age_seconds=max_age_seconds,
            expected_session=expected_session,
            connected=connected,
            out_of_order=out_of_order,
        ):
            return None
        return self.mid

    @property
    def has_both(self) -> bool:
        return self.bid is not None and self.ask is not None

    def leg(self, side: QuoteSide | str) -> QuoteLeg:
        selected = side if isinstance(side, QuoteSide) else QuoteSide(str(side).lower())
        if selected is QuoteSide.MID:
            raise CFDSimulationError("mid no es una pierna independiente")
        if selected is QuoteSide.BID:
            return QuoteLeg(
                selected,
                self.bid,
                self.bid_source_timestamp,
                self.bid_received_at,
                self.bid_available_at,
                quality_from(self.bid_quality),
                bool(self.bid_timestamp_known),
                self.source,
                self.sequence,
                self.session_generation,
                quality_reasons=tuple(self.bid_quality_reasons or ()),
            )
        return QuoteLeg(
            selected,
            self.ask,
            self.ask_source_timestamp,
            self.ask_received_at,
            self.ask_available_at,
            quality_from(self.ask_quality),
            bool(self.ask_timestamp_known),
            self.source,
            self.sequence,
            self.session_generation,
            quality_reasons=tuple(self.ask_quality_reasons or ()),
        )

    def assessment_for(
        self,
        side: QuoteSide | str,
        *,
        at: datetime | None = None,
        max_age_seconds: Decimal | int | str | None = None,
        expected_session: int | str | None = None,
        connected: bool = True,
        out_of_order: bool = False,
    ) -> QuoteAssessment:
        selected = side if isinstance(side, QuoteSide) else QuoteSide(str(side).lower())
        checked = at or self.available_ts
        pair = _quote_pair_assessment(
            self,
            checked=checked,
            max_age_seconds=max_age_seconds,
            expected_session=expected_session,
            connected=connected,
            out_of_order=out_of_order,
        )
        forced = _forced_reasons(
            self.quote_reasons or (),
            selected,
            bid_quality=quality_from(self.bid_quality),
            ask_quality=quality_from(self.ask_quality),
            bid_timestamp_known=bool(self.bid_timestamp_known),
            ask_timestamp_known=bool(self.ask_timestamp_known),
        )
        pair = _apply_forced_reasons(pair, forced)
        return pair if selected is QuoteSide.MID else _select_side_assessment(pair, selected, forced)

    def operable_for(self, side: QuoteSide | str, **kwargs: Any) -> bool:
        return self.assessment_for(side, **kwargs).usable

    def to_dict(self) -> dict[str, Any]:
        return {
            "quote_contract_version": 2,
            "instrument": self.instrument,
            "market_time": _iso(self.market_time),
            "available_at": _iso(self.available_ts),
            "bid": str(self.bid) if self.bid is not None else None,
            "ask": str(self.ask) if self.ask is not None else None,
            "mid": str(self.mid) if self.mid is not None else None,
            "spread": str(self.spread) if self.spread is not None else None,
            "quote_id": self.quote_id,
            "identity": self.identity,
            "source": self.source,
            "sequence": self.sequence,
            "quality": self.quality,
            "quote_quality": self.quote_quality,
            "quote_reasons": list(self.quote_reasons or ()),
            "source_timestamp_missing": self.source_timestamp_missing,
            "bid_source_timestamp_missing": self.bid_source_timestamp_missing,
            "ask_source_timestamp_missing": self.ask_source_timestamp_missing,
            "bid_age_seconds": str(self.bid_age_seconds) if self.bid_age_seconds is not None else None,
            "ask_age_seconds": str(self.ask_age_seconds) if self.ask_age_seconds is not None else None,
            "metadata": _jsonable(self.metadata),
            "bid_source_timestamp": _iso(self.bid_source_timestamp),
            "ask_source_timestamp": _iso(self.ask_source_timestamp),
            "bid_received_at": _iso(self.bid_received_at),
            "ask_received_at": _iso(self.ask_received_at),
            "bid_available_at": _iso(self.bid_available_at),
            "ask_available_at": _iso(self.ask_available_at),
            "bid_quality": self.bid_quality,
            "ask_quality": self.ask_quality,
            "bid_quality_reasons": [
                reason.value if isinstance(reason, QuoteReason) else reason
                for reason in (self.bid_quality_reasons or ())
            ],
            "ask_quality_reasons": [
                reason.value if isinstance(reason, QuoteReason) else reason
                for reason in (self.ask_quality_reasons or ())
            ],
            "bid_timestamp_known": self.bid_timestamp_known,
            "ask_timestamp_known": self.ask_timestamp_known,
            "updated_sides": list(self.updated_sides or ()),
            "session_generation": self.session_generation,
            "connection_generation": self.connection_generation,
            "is_snapshot": self.is_snapshot,
            "disconnected": self.disconnected,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> CFDQuote:
        if not isinstance(value, Mapping):
            raise CFDSimulationError(f"quote debe ser mapping, llegó {type(value).__name__}")
        return cls(
            instrument=value.get("instrument", value.get("symbol", "")),
            market_time=value.get("market_time", value.get("timestamp", value.get("event_time"))),
            bid=value.get("bid", value.get("bid_price")),
            ask=value.get("ask", value.get("ask_price")),
            quote_id=value.get("quote_id", value.get("id", value.get("identity", ""))),
            available_at=value.get("available_at", value.get("available_ts")),
            source=value.get("source", value.get("provider", "fixture")),
            sequence=value.get("sequence", value.get("source_sequence")),
            quality=value.get("quality", value.get("quality_status", "VALID")),
            metadata=value.get("metadata", {}),
            quote_quality=value.get("quote_quality"),
            quote_reasons=value.get("quote_reasons"),
            source_timestamp_missing=value.get("source_timestamp_missing"),
            bid_source_timestamp_missing=value.get("bid_source_timestamp_missing"),
            ask_source_timestamp_missing=value.get("ask_source_timestamp_missing"),
            bid_age_seconds=value.get("bid_age_seconds"),
            ask_age_seconds=value.get("ask_age_seconds"),
            bid_source_timestamp=value.get("bid_source_timestamp"),
            ask_source_timestamp=value.get("ask_source_timestamp"),
            bid_received_at=value.get("bid_received_at"),
            ask_received_at=value.get("ask_received_at"),
            bid_available_at=value.get("bid_available_at"),
            ask_available_at=value.get("ask_available_at"),
            bid_quality=value.get("bid_quality", value.get("bid_quality_status")),
            ask_quality=value.get("ask_quality", value.get("ask_quality_status")),
            bid_quality_reasons=value.get("bid_quality_reasons"),
            ask_quality_reasons=value.get("ask_quality_reasons"),
            bid_timestamp_known=value.get("bid_timestamp_known"),
            ask_timestamp_known=value.get("ask_timestamp_known"),
            updated_sides=value.get("updated_sides", value.get("changed_sides", value.get("updated_side"))),
            session_generation=value.get("session_generation"),
            connection_generation=value.get("connection_generation"),
            is_snapshot=value.get("is_snapshot", value.get("snapshot")),
            disconnected=value.get("disconnected", False),
        )


def _quote_basic(quote: CFDQuote, metadata: Mapping[str, Any]) -> dict[str, Any]:
    instrument = _id(quote.instrument, name="instrument").upper()
    market, available = _quote_times(quote, metadata)
    bid, ask = _quote_prices(quote)
    raw_quality, quality, reasons = _quote_quality(quote, metadata)
    _validate_cross(bid, ask, quality, reasons)
    snapshot = _bool(
        quote.is_snapshot if quote.is_snapshot is not None else metadata.get("is_snapshot", metadata.get("snapshot")),
        name="is_snapshot",
    )
    disconnected = _bool(quote.disconnected or metadata.get("disconnected", False), name="disconnected")
    return {
        "instrument": instrument,
        "market": market,
        "available": available,
        "bid": bid,
        "ask": ask,
        "raw_quality": raw_quality,
        "quality": quality,
        "reasons": reasons,
        "snapshot": snapshot,
        "disconnected": disconnected,
    }


def _quote_times(quote: CFDQuote, metadata: Mapping[str, Any] | None = None) -> tuple[datetime, datetime]:
    market = _utc(quote.market_time, name="market_time")
    available = _utc(quote.available_at, name="available_at") if quote.available_at is not None else market
    metadata = metadata or {}
    # A side may arrive later than the envelope timestamp.  Promote the
    # effective quote availability before the quote is exposed to the book so
    # a fill cannot observe that side early.  Source timestamps are market
    # evidence, not availability evidence, and are checked separately.
    for side in ("bid", "ask"):
        for kind in ("received_at", "available_at"):
            explicit = getattr(quote, f"{side}_{kind}")
            value = explicit if explicit is not None else metadata.get(f"{side}_{kind}")
            if value is None:
                continue
            side_time = _utc(value, name=f"{side}_{kind}")
            if side_time < market:
                raise CFDSimulationError(f"{side}_{kind} no puede preceder market_time")
            available = max(available, side_time)
    if available < market:
        raise CFDSimulationError("available_at no puede preceder market_time")
    return market, available


def _quote_prices(quote: CFDQuote) -> tuple[Decimal | None, Decimal | None]:
    bid = decimal(quote.bid, name="bid", positive=True) if quote.bid is not None else None
    ask = decimal(quote.ask, name="ask", positive=True) if quote.ask is not None else None
    return bid, ask


def _expand_quote_metadata(metadata: Mapping[str, Any]) -> dict[str, Any]:
    nested = metadata.get("quote_quality")
    if not isinstance(nested, Mapping):
        return dict(metadata)
    result = dict(metadata)
    for side in ("bid", "ask"):
        evidence = nested.get(side)
        if not isinstance(evidence, Mapping):
            continue
        mapping = {
            f"{side}_source_timestamp": evidence.get("source_timestamp"),
            f"{side}_received_at": evidence.get("received_at"),
            f"{side}_available_at": evidence.get("available_at"),
            f"{side}_source_timestamp_missing": evidence.get("timestamp_missing"),
            f"{side}_quality": evidence.get("state"),
            f"{side}_age_seconds": evidence.get("age_seconds"),
        }
        for key, value in mapping.items():
            if value is not None and key not in result:
                result[key] = value
    if "reasons" in nested and "quote_reasons" not in result:
        result["quote_reasons"] = nested["reasons"]
    return result


def _quote_quality(
    quote: CFDQuote,
    metadata: Mapping[str, Any],
) -> tuple[Any, QuoteQuality, tuple[str, ...]]:
    raw_quality = (
        quote.quote_quality if quote.quote_quality is not None else metadata.get("quote_quality", quote.quality)
    )
    quality = quality_from(raw_quality)
    raw_reasons = quote.quote_reasons
    if raw_reasons is None and isinstance(raw_quality, Mapping):
        raw_reasons = tuple(reason.value for reason in quality_reasons_from(raw_quality))
    return raw_quality, quality, _quote_reasons(raw_reasons, metadata)


def _validate_cross(
    bid: Decimal | None,
    ask: Decimal | None,
    quality: QuoteQuality,
    reasons: Sequence[str],
) -> None:
    del quality, reasons
    crossed = bid is not None and ask is not None and ask < bid
    if crossed:
        raise CFDSimulationError("ask debe ser mayor o igual que bid", code="CROSSED_QUOTE")


def _quote_session(quote: CFDQuote, metadata: Mapping[str, Any]) -> tuple[int | str | None, int | str | None]:
    session = quote.session_generation
    if session is None:
        session = metadata.get("session_generation", quote.connection_generation)
    if session is None:
        session = metadata.get("connection_generation")
    connection = quote.connection_generation
    if connection is None:
        connection = metadata.get("connection_generation", session)
    return session, connection


def _quote_is_strict(quote: CFDQuote, metadata: Mapping[str, Any]) -> bool:
    if _has_side_contract(metadata):
        return True
    return any(
        value is not None
        for value in (
            quote.quote_quality,
            quote.quote_reasons,
            quote.source_timestamp_missing,
            quote.bid_source_timestamp_missing,
            quote.ask_source_timestamp_missing,
            quote.bid_timestamp_known,
            quote.ask_timestamp_known,
            quote.bid_source_timestamp,
            quote.ask_source_timestamp,
            quote.bid_received_at,
            quote.ask_received_at,
            quote.bid_available_at,
            quote.ask_available_at,
            quote.bid_quality,
            quote.ask_quality,
            quote.bid_quality_reasons,
            quote.ask_quality_reasons,
        )
    )


def _quote_legs(
    quote: CFDQuote,
    metadata: Mapping[str, Any],
    market: datetime,
    available: datetime,
    strict: bool,
) -> dict[str, Any]:
    bid_ts, bid_known = _side_timestamp(
        "bid",
        quote.bid_source_timestamp,
        quote.bid_timestamp_known,
        metadata,
        market,
        strict,
        explicit_missing=quote.bid_source_timestamp_missing,
        global_missing=quote.source_timestamp_missing,
    )
    ask_ts, ask_known = _side_timestamp(
        "ask",
        quote.ask_source_timestamp,
        quote.ask_timestamp_known,
        metadata,
        market,
        strict,
        explicit_missing=quote.ask_source_timestamp_missing,
        global_missing=quote.source_timestamp_missing,
    )
    bid_fallback = _side_availability_fallback("bid", quote, metadata, available)
    ask_fallback = _side_availability_fallback("ask", quote, metadata, available)
    bid_received = _side_datetime("bid_received_at", quote.bid_received_at, metadata, bid_fallback)
    ask_received = _side_datetime("ask_received_at", quote.ask_received_at, metadata, ask_fallback)
    bid_available = _side_datetime("bid_available_at", quote.bid_available_at, metadata, available)
    ask_available = _side_datetime("ask_available_at", quote.ask_available_at, metadata, available)
    quality = quality_from(
        quote.quote_quality if quote.quote_quality is not None else metadata.get("quote_quality", quote.quality)
    )
    bid_quality, bid_quality_reasons = _side_quality_evidence("bid", quote.bid_quality, metadata, quality)
    ask_quality, ask_quality_reasons = _side_quality_evidence("ask", quote.ask_quality, metadata, quality)
    bid_quality_reasons = tuple(dict.fromkeys((*bid_quality_reasons, *quality_reasons_from(quote.bid_quality_reasons))))
    ask_quality_reasons = tuple(dict.fromkeys((*ask_quality_reasons, *quality_reasons_from(quote.ask_quality_reasons))))
    return {
        "bid_source_timestamp": bid_ts,
        "ask_source_timestamp": ask_ts,
        "bid_timestamp_known": bid_known,
        "ask_timestamp_known": ask_known,
        "bid_received_at": bid_received,
        "ask_received_at": ask_received,
        "bid_available_at": bid_available,
        "ask_available_at": ask_available,
        "bid_quality": bid_quality,
        "ask_quality": ask_quality,
        "bid_quality_reasons": bid_quality_reasons,
        "ask_quality_reasons": ask_quality_reasons,
    }


def _quote_values(
    quote: CFDQuote,
    *,
    metadata: Mapping[str, Any],
    basic: Mapping[str, Any],
    session: int | str | None,
    connection: int | str | None,
    legs: Mapping[str, Any],
    updated: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "instrument": basic["instrument"],
        "market_time": basic["market"],
        "available_at": basic["available"],
        "bid": basic["bid"],
        "ask": basic["ask"],
        "quote_id": _id(quote.quote_id, name="quote_id"),
        "source": _id(quote.source, name="source"),
        "quality": basic["quality"].value,
        "quote_quality": basic["quality"].value,
        "quote_reasons": basic["reasons"],
        "source_timestamp_missing": quote.source_timestamp_missing,
        "bid_source_timestamp_missing": not legs["bid_timestamp_known"],
        "ask_source_timestamp_missing": not legs["ask_timestamp_known"],
        "bid_age_seconds": _age_value(quote.bid_age_seconds, legs["bid_source_timestamp"], basic["available"]),
        "ask_age_seconds": _age_value(quote.ask_age_seconds, legs["ask_source_timestamp"], basic["available"]),
        "metadata": metadata,
        "updated_sides": updated,
        "session_generation": session,
        "connection_generation": connection,
        "is_snapshot": basic["snapshot"],
        "disconnected": basic["disconnected"],
        **legs,
    }


def _quote_pair_assessment(
    quote: CFDQuote,
    *,
    checked: datetime,
    max_age_seconds: Decimal | int | str | None,
    expected_session: int | str | None,
    connected: bool,
    out_of_order: bool,
) -> QuoteAssessment:
    pair = assess_pair(
        quote.leg(QuoteSide.BID),
        quote.leg(QuoteSide.ASK),
        at=checked,
        max_age_seconds=max_age_seconds,
        common_quality=quote.quality,
        expected_session=expected_session,
        connected=connected and not quote.disconnected,
        snapshot=bool(quote.is_snapshot),
        out_of_order=out_of_order
        or _flag(quote.metadata or {}, "out_of_order")
        or QuoteReason.OUT_OF_ORDER.value in (quote.quote_reasons or ()),
        crossed=_flag(quote.metadata or {}, "crossed") or QuoteReason.CROSSED.value in (quote.quote_reasons or ()),
    )
    future_sources = tuple(
        QuoteReason.FUTURE_SOURCE_TIMESTAMP
        for side in (QuoteSide.BID, QuoteSide.ASK)
        if (source := getattr(quote, f"{side.value}_source_timestamp")) is not None and source > quote.market_time
    )
    if future_sources:
        pair = replace(
            pair,
            usable=False,
            reasons=tuple(dict.fromkeys((*pair.reasons, *future_sources))),
        )
    return pair


def _apply_forced_reasons(pair: QuoteAssessment, forced: Sequence[QuoteReason]) -> QuoteAssessment:
    if not forced:
        return pair
    return replace(pair, usable=False, reasons=tuple(dict.fromkeys((*pair.reasons, *forced))))


def _select_side_assessment(
    pair: QuoteAssessment,
    selected: QuoteSide,
    forced: Sequence[QuoteReason],
) -> QuoteAssessment:
    leg = pair.bid if selected is QuoteSide.BID else pair.ask
    assert leg is not None
    reasons = tuple(dict.fromkeys((*leg.reasons, *forced)))
    usable = leg.usable and pair.common_usable and not forced and QuoteReason.CROSSED not in pair.reasons
    return QuoteAssessment(
        selected,
        usable,
        reasons,
        leg if selected is QuoteSide.BID else None,
        leg if selected is QuoteSide.ASK else None,
        pair.checked_at,
        pair.common_usable,
    )


def _flag(metadata: Mapping[str, Any], key: str) -> bool:
    try:
        return _bool(metadata.get(key), name=key)
    except CFDSimulationError:
        return False


def _has_side_contract(metadata: Mapping[str, Any]) -> bool:
    keys = {
        "quote_contract_version",
        "source_timestamp_missing",
        "bid_age_seconds",
        "ask_age_seconds",
        "bid_source_timestamp",
        "ask_source_timestamp",
        "bid_source_timestamp_missing",
        "ask_source_timestamp_missing",
        "bid_timestamp_known",
        "ask_timestamp_known",
        "bid_quality",
        "ask_quality",
        "bid_quality_reasons",
        "ask_quality_reasons",
        "bid_received_at",
        "ask_received_at",
        "bid_available_at",
        "ask_available_at",
    }
    return bool(keys.intersection(metadata))


def _side_timestamp(
    side: str,
    explicit: datetime | None,
    known: bool | None,
    metadata: Mapping[str, Any],
    market: datetime,
    strict: bool,
    *,
    explicit_missing: bool | None = None,
    global_missing: bool | None = None,
) -> tuple[datetime | None, bool]:
    timestamp = _timestamp_value(side, explicit, metadata)
    timestamp_known = _timestamp_known(
        side,
        known,
        metadata,
        strict,
        explicit_missing=explicit_missing,
        global_missing=global_missing,
    )
    if timestamp is None and timestamp_known and not strict:
        timestamp = market
    if timestamp is None:
        timestamp_known = False
    return timestamp, timestamp_known


def _timestamp_value(side: str, explicit: datetime | None, metadata: Mapping[str, Any]) -> datetime | None:
    value = explicit if explicit is not None else metadata.get(f"{side}_source_timestamp")
    return _utc(value, name=f"{side}_source_timestamp") if value is not None else None


def _timestamp_known(
    side: str,
    known: bool | None,
    metadata: Mapping[str, Any],
    strict: bool,
    *,
    explicit_missing: bool | None,
    global_missing: bool | None,
) -> bool:
    missing_key = f"{side}_source_timestamp_missing"
    known_key = f"{side}_timestamp_known"
    if explicit_missing is not None:
        return not _bool(explicit_missing, name=missing_key)
    if global_missing is not None:
        return not _bool(global_missing, name="source_timestamp_missing")
    if known is not None:
        return _bool(known, name=known_key)
    if known_key in metadata:
        return _bool(metadata.get(known_key), name=known_key)
    if missing_key in metadata:
        return not _bool(metadata.get(missing_key), name=missing_key)
    if "source_timestamp_missing" in metadata:
        return not _bool(metadata.get("source_timestamp_missing"), name="source_timestamp_missing")
    return not strict


def _quote_reasons(raw: Any, metadata: Mapping[str, Any]) -> tuple[str, ...]:
    if raw is None:
        raw = metadata.get("quote_reasons", ())
    values = (raw,) if isinstance(raw, str) else tuple(raw or ())
    allowed = {reason.value for reason in QuoteReason}
    allowed.update(
        {
            "BID_SOURCE_TIMESTAMP_MISSING",
            "ASK_SOURCE_TIMESTAMP_MISSING",
            "BID_STALE",
            "ASK_STALE",
            "BID_OUT_OF_ORDER",
            "ASK_OUT_OF_ORDER",
            "BID_INVALID",
            "ASK_INVALID",
            "QUOTE_CROSSED",
            "QUOTE_DISCONNECTED",
            "QUOTE_SNAPSHOT",
            "QUOTE_SESSION_MISMATCH",
        }
    )
    result = []
    for item in values:
        value = item.value if isinstance(item, QuoteReason) else item
        normalized = str(value).strip().upper().replace("-", "_").replace(" ", "_")
        if normalized in allowed:
            result.append(normalized)
        else:
            # Unknown diagnostic text cannot be treated as harmless metadata at
            # the fill boundary.
            result.append(QuoteReason.INVALID_QUALITY.value)
    return tuple(dict.fromkeys(result))


_SIDE_REASON_MAP: dict[str, dict[str, QuoteReason]] = {
    "bid": {
        "BID_SOURCE_TIMESTAMP_MISSING": QuoteReason.MISSING_SOURCE_TIMESTAMP,
        "BID_STALE": QuoteReason.STALE,
        "BID_OUT_OF_ORDER": QuoteReason.OUT_OF_ORDER,
        "BID_INVALID": QuoteReason.INVALID_QUALITY,
    },
    "ask": {
        "ASK_SOURCE_TIMESTAMP_MISSING": QuoteReason.MISSING_SOURCE_TIMESTAMP,
        "ASK_STALE": QuoteReason.STALE,
        "ASK_OUT_OF_ORDER": QuoteReason.OUT_OF_ORDER,
        "ASK_INVALID": QuoteReason.INVALID_QUALITY,
    },
}
_QUOTE_REASON_MAP = {
    "QUOTE_CROSSED": QuoteReason.CROSSED,
    "QUOTE_DISCONNECTED": QuoteReason.DISCONNECTED,
    "QUOTE_SNAPSHOT": QuoteReason.SNAPSHOT,
    "QUOTE_SESSION_MISMATCH": QuoteReason.SESSION_MISMATCH,
}


_SIDE_GENERIC_REASONS = frozenset({"STALE", "MISSING_SOURCE_TIMESTAMP"})
_DIRECT_REASON_MAP = {reason.value: reason for reason in QuoteReason}


def _forced_reasons(
    values: Sequence[str],
    side: QuoteSide,
    *,
    bid_quality: QuoteQuality,
    ask_quality: QuoteQuality,
    bid_timestamp_known: bool,
    ask_timestamp_known: bool,
) -> tuple[QuoteReason, ...]:
    result = [
        reason
        for raw in values
        if (
            reason := _forced_reason(
                raw,
                side,
                bid_quality=bid_quality,
                ask_quality=ask_quality,
                bid_timestamp_known=bid_timestamp_known,
                ask_timestamp_known=ask_timestamp_known,
            )
        )
        is not None
    ]
    return tuple(dict.fromkeys(result))


def _forced_reason(
    raw: str,
    side: QuoteSide,
    *,
    bid_quality: QuoteQuality,
    ask_quality: QuoteQuality,
    bid_timestamp_known: bool,
    ask_timestamp_known: bool,
) -> QuoteReason | None:
    if raw in _SIDE_GENERIC_REASONS:
        return _side_generic_reason(
            raw,
            side,
            bid_quality=bid_quality,
            ask_quality=ask_quality,
            bid_timestamp_known=bid_timestamp_known,
            ask_timestamp_known=ask_timestamp_known,
        )
    direct = _DIRECT_REASON_MAP.get(raw)
    if direct is not None:
        return direct
    if side is QuoteSide.MID:
        return _mid_reason(raw)
    return _SIDE_REASON_MAP.get(side.value, {}).get(raw) or _QUOTE_REASON_MAP.get(raw)


def _side_generic_reason(
    raw: str,
    side: QuoteSide,
    *,
    bid_quality: QuoteQuality,
    ask_quality: QuoteQuality,
    bid_timestamp_known: bool,
    ask_timestamp_known: bool,
) -> QuoteReason | None:
    if raw == "STALE":
        return _generic_stale_reason(side, bid_quality, ask_quality)
    if raw == "MISSING_SOURCE_TIMESTAMP":
        return _generic_timestamp_reason(side, bid_timestamp_known, ask_timestamp_known)
    return None


def _generic_stale_reason(
    side: QuoteSide,
    bid_quality: QuoteQuality,
    ask_quality: QuoteQuality,
) -> QuoteReason | None:
    if side is QuoteSide.MID:
        return QuoteReason.STALE if bid_quality is QuoteQuality.VALID and ask_quality is QuoteQuality.VALID else None
    quality = bid_quality if side is QuoteSide.BID else ask_quality
    if quality is QuoteQuality.STALE:
        return QuoteReason.STALE
    if bid_quality is not QuoteQuality.STALE and ask_quality is not QuoteQuality.STALE:
        return QuoteReason.STALE
    return None


def _generic_timestamp_reason(
    side: QuoteSide,
    bid_timestamp_known: bool,
    ask_timestamp_known: bool,
) -> QuoteReason | None:
    if side is QuoteSide.MID:
        return QuoteReason.MISSING_SOURCE_TIMESTAMP if bid_timestamp_known and ask_timestamp_known else None
    timestamp_known = bid_timestamp_known if side is QuoteSide.BID else ask_timestamp_known
    if not timestamp_known:
        return QuoteReason.MISSING_SOURCE_TIMESTAMP
    if bid_timestamp_known and ask_timestamp_known:
        return QuoteReason.MISSING_SOURCE_TIMESTAMP
    return None


def _mid_reason(raw: str) -> QuoteReason | None:
    values = {
        "BID_SOURCE_TIMESTAMP_MISSING": QuoteReason.MISSING_SOURCE_TIMESTAMP,
        "ASK_SOURCE_TIMESTAMP_MISSING": QuoteReason.MISSING_SOURCE_TIMESTAMP,
        "BID_STALE": QuoteReason.STALE,
        "ASK_STALE": QuoteReason.STALE,
        "BID_OUT_OF_ORDER": QuoteReason.OUT_OF_ORDER,
        "ASK_OUT_OF_ORDER": QuoteReason.OUT_OF_ORDER,
        "BID_INVALID": QuoteReason.INVALID_QUALITY,
        "ASK_INVALID": QuoteReason.INVALID_QUALITY,
    }
    return values.get(raw) or _QUOTE_REASON_MAP.get(raw)


def _age_value(explicit: Any, timestamp: datetime | None, available: datetime) -> Decimal | None:
    if explicit is not None:
        return decimal(explicit, name="side_age_seconds", minimum=D0)
    return seconds_decimal(available - timestamp) if timestamp is not None else None


def _side_datetime(name: str, explicit: datetime | None, metadata: Mapping[str, Any], fallback: datetime) -> datetime:
    value = explicit if explicit is not None else metadata.get(name)
    return _utc(value, name=name) if value is not None else fallback


def _side_availability_fallback(
    side: str, quote: CFDQuote, metadata: Mapping[str, Any], fallback: datetime
) -> datetime:
    explicit = getattr(quote, f"{side}_available_at")
    value = explicit if explicit is not None else metadata.get(f"{side}_available_at")
    return _utc(value, name=f"{side}_available_at") if value is not None else fallback


def _side_quality(side: str, explicit: Any, metadata: Mapping[str, Any], fallback: QuoteQuality) -> QuoteQuality:
    value = (
        explicit
        if explicit is not None
        else metadata.get(f"{side}_quality", metadata.get(f"{side}_quality_status", fallback))
    )
    return quality_from(value)


def _side_quality_evidence(
    side: str, explicit: Any, metadata: Mapping[str, Any], fallback: QuoteQuality
) -> tuple[QuoteQuality, tuple[QuoteReason, ...]]:
    value = (
        explicit
        if explicit is not None
        else metadata.get(f"{side}_quality", metadata.get(f"{side}_quality_status", fallback))
    )
    return quality_from(value), quality_reasons_from(value)


def _updated_sides(raw: Any, metadata: Mapping[str, Any], has_bid: bool, has_ask: bool) -> tuple[str, ...]:
    raw = raw if raw is not None else _metadata_updated_sides(metadata)
    values = _normalise_side_names(raw)
    if values:
        return values
    if _flag(metadata, "partial_update"):
        inferred = _infer_partial_sides(metadata)
        if inferred:
            return inferred
    return tuple(side for side, present in (("bid", has_bid), ("ask", has_ask)) if present)


def _metadata_updated_sides(metadata: Mapping[str, Any]) -> Any:
    return metadata.get("updated_sides", metadata.get("changed_sides", metadata.get("updated_side")))


def _normalise_side_names(raw: Any) -> tuple[str, ...]:
    values: Iterable[str]
    if isinstance(raw, str):
        values = (item.strip().lower() for item in raw.replace(";", ",").split(",") if item.strip())
    elif raw is None:
        values = ()
    else:
        values = (str(item).strip().lower() for item in raw)
    selected = tuple(item for item in values if item in {"bid", "ask"})
    return tuple(dict.fromkeys(selected))


def _infer_partial_sides(metadata: Mapping[str, Any]) -> tuple[str, ...]:
    event_time = metadata.get("event_time", metadata.get("market_time"))
    if event_time is None:
        return ()
    changed: list[str] = []
    for side in ("bid", "ask"):
        source = metadata.get(f"{side}_source_timestamp")
        if source is None:
            continue
        try:
            if _utc(source, name=f"{side}_source_timestamp") == _utc(event_time, name="event_time"):
                changed.append(side)
        except (CFDSimulationError, ValueError):
            continue
    return tuple(changed)


@dataclass(frozen=True, slots=True)
class CFDSignal:
    """A direction decision; it contains no executable broker instruction."""

    signal_id: str
    instrument: str
    direction: Direction
    detected_at: datetime
    available_at: datetime | None = None
    strategy: str = "unknown"
    quality: QuoteQuality | str = QuoteQuality.VALID
    metadata: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        detected = _utc(self.detected_at, name="detected_at")
        available = _utc(self.available_at, name="available_at") if self.available_at is not None else detected
        if available < detected:
            raise CFDSimulationError("signal available_at no puede preceder detected_at")
        object.__setattr__(self, "signal_id", _id(self.signal_id, name="signal_id"))
        object.__setattr__(self, "instrument", _id(self.instrument, name="instrument").upper())
        object.__setattr__(self, "direction", _direction(self.direction))
        object.__setattr__(self, "detected_at", detected)
        object.__setattr__(self, "available_at", available)
        object.__setattr__(self, "strategy", str(self.strategy or "unknown"))
        object.__setattr__(self, "quality", quality_from(self.quality).value)
        object.__setattr__(self, "metadata", dict(self.metadata or {}))

    @property
    def identity(self) -> str:
        return self.signal_id

    def to_dict(self) -> dict[str, Any]:
        return {
            "signal_id": self.signal_id,
            "identity": self.identity,
            "instrument": self.instrument,
            "direction": self.direction.value,
            "detected_at": _iso(self.detected_at),
            "available_at": _iso(self.available_at),
            "strategy": self.strategy,
            "quality": self.quality,
            "metadata": _jsonable(self.metadata),
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> CFDSignal:
        return cls(
            signal_id=value.get("signal_id", value.get("id", "")),
            instrument=value.get("instrument", value.get("symbol", "")),
            direction=value.get("direction", value.get("side", "")),
            detected_at=value.get("detected_at", value.get("detected_ts", value.get("timestamp"))),
            available_at=value.get("available_at", value.get("available_ts")),
            strategy=value.get("strategy", "unknown"),
            quality=value.get("quality", "VALID"),
            metadata=value.get("metadata", {}),
        )


@dataclass(frozen=True, slots=True)
class CFDConfig:
    """Explicit linear-unit Forex/CFD simulation assumptions."""

    instrument: str = "EUR/USD"
    units: Decimal = Decimal("1000")
    pip_size: Decimal = Decimal("0.0001")
    price_precision: int = 5
    horizons_seconds: tuple[Decimal, ...] = (Decimal("60"), Decimal("180"), Decimal("300"))
    decision_latency_seconds: Decimal = Decimal("0")
    entry_latency_seconds: Decimal = Decimal("0")
    close_latency_seconds: Decimal = Decimal("0")
    max_quote_age_seconds: Decimal = Decimal("5")
    max_spread: Decimal | None = None
    fill_policy: str = "first_quote_at_or_after"
    close_policy: str = "first_quote_at_or_after"
    commission_fixed: Decimal = Decimal("0")
    commission_per_unit: Decimal = Decimal("0")
    slippage_pips: Decimal = Decimal("0")
    account_currency: str = "USD"
    quote_currency: str | None = None
    conversion_rate: Decimal | None = None
    financing_required: bool = False
    financing_rate_per_second: Decimal | None = None
    decimal_policy_version: str = DECIMAL_POLICY_VERSION
    terminal_retention: int = 512
    event_retention: int = 4096
    quote_id_retention: int = 4096
    commission_known: bool = True
    max_active_trades: int = 4096
    economics_version: str = CFD_ECONOMICS_VERSION

    def __post_init__(self) -> None:
        values = _normalise_config_values(self)
        for name, value in values.items():
            object.__setattr__(self, name, value)
        _validate_config(self)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> CFDConfig:
        allowed = {item.name for item in fields(cls)}
        unknown = set(value) - allowed
        if unknown:
            raise CFDSimulationError(f"claves CFD desconocidas: {sorted(unknown)}")
        data = dict(value)
        for name in (
            "units",
            "pip_size",
            "decision_latency_seconds",
            "entry_latency_seconds",
            "close_latency_seconds",
            "max_quote_age_seconds",
            "max_spread",
            "commission_fixed",
            "commission_per_unit",
            "slippage_pips",
            "conversion_rate",
            "financing_rate_per_second",
        ):
            if name in data and data[name] is not None:
                data[name] = Decimal(str(data[name]))
        if "horizons_seconds" in data:
            data["horizons_seconds"] = tuple(Decimal(str(item)) for item in data["horizons_seconds"])
        return cls(**data)

    @property
    def slippage_price(self) -> Decimal:
        with decimal_context():
            return self.slippage_pips * self.pip_size

    @property
    def config_hash(self) -> str:
        return _digest(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "instrument": self.instrument,
            "units": str(self.units),
            "pip_size": str(self.pip_size),
            "price_precision": self.price_precision,
            "horizons_seconds": [str(x) for x in self.horizons_seconds],
            "decision_latency_seconds": str(self.decision_latency_seconds),
            "entry_latency_seconds": str(self.entry_latency_seconds),
            "close_latency_seconds": str(self.close_latency_seconds),
            "max_quote_age_seconds": str(self.max_quote_age_seconds),
            "max_spread": str(self.max_spread) if self.max_spread is not None else None,
            "fill_policy": self.fill_policy,
            "close_policy": self.close_policy,
            "commission_fixed": str(self.commission_fixed),
            "commission_per_unit": str(self.commission_per_unit),
            "commission_known": self.commission_known,
            "slippage_pips": str(self.slippage_pips),
            "account_currency": self.account_currency,
            "quote_currency": self.quote_currency,
            "conversion_rate": str(self.conversion_rate) if self.conversion_rate is not None else None,
            "financing_required": self.financing_required,
            "financing_rate_per_second": str(self.financing_rate_per_second)
            if self.financing_rate_per_second is not None
            else None,
            "decimal_policy_version": self.decimal_policy_version,
            "terminal_retention": self.terminal_retention,
            "event_retention": self.event_retention,
            "quote_id_retention": self.quote_id_retention,
            "max_active_trades": self.max_active_trades,
            "economics_version": self.economics_version,
        }


def _normalise_config_values(config: CFDConfig) -> dict[str, Any]:
    values: dict[str, Any] = {
        "instrument": _id(config.instrument, name="instrument").upper(),
        "units": decimal(config.units, name="units", positive=True),
        "pip_size": decimal(config.pip_size, name="pip_size", positive=True),
        "horizons_seconds": _normalise_horizons(config.horizons_seconds),
        "account_currency": _id(config.account_currency, name="account_currency").upper(),
        "quote_currency": _instrument_quote_currency(config.instrument, config.quote_currency),
        "economics_version": _normalise_economics_version(config.economics_version),
    }
    values.update(_normalise_config_latencies(config))
    values.update(_normalise_config_costs(config))
    values.update(_normalise_config_retention(config))
    return values


def _normalise_horizons(raw: Iterable[Any]) -> tuple[Decimal, ...]:
    horizons = tuple(decimal(item, name="horizon", positive=True) for item in raw)
    if not horizons:
        raise CFDSimulationError("horizons_seconds no puede estar vacío")
    return horizons


def _normalise_config_latencies(config: CFDConfig) -> dict[str, Any]:
    values: dict[str, Any] = {
        name: decimal(getattr(config, name), name=name, minimum=D0)
        for name in (
            "decision_latency_seconds",
            "entry_latency_seconds",
            "close_latency_seconds",
            "max_quote_age_seconds",
        )
    }
    values["max_spread"] = (
        decimal(config.max_spread, name="max_spread", minimum=D0) if config.max_spread is not None else None
    )
    values["fill_policy"] = str(config.fill_policy).strip()
    values["close_policy"] = str(config.close_policy).strip()
    return values


def _normalise_config_costs(config: CFDConfig) -> dict[str, Any]:
    values: dict[str, Any] = {
        name: decimal(getattr(config, name), name=name, minimum=D0)
        for name in ("commission_fixed", "commission_per_unit", "slippage_pips")
    }
    values["commission_known"] = config.commission_known
    values["conversion_rate"] = (
        decimal(config.conversion_rate, name="conversion_rate", positive=True)
        if config.conversion_rate is not None
        else None
    )
    values["financing_rate_per_second"] = (
        decimal(config.financing_rate_per_second, name="financing_rate_per_second", minimum=D0)
        if config.financing_rate_per_second is not None
        else None
    )
    return values


def _normalise_config_retention(config: CFDConfig) -> dict[str, Any]:
    return {
        "decimal_policy_version": config.decimal_policy_version,
        "terminal_retention": config.terminal_retention,
        "event_retention": config.event_retention,
        "quote_id_retention": config.quote_id_retention,
        "max_active_trades": config.max_active_trades,
    }


def _validate_config(config: CFDConfig) -> None:
    _normalise_economics_version(config.economics_version)
    if (
        isinstance(config.price_precision, bool)
        or not isinstance(config.price_precision, int)
        or config.price_precision < 0
    ):
        raise CFDSimulationError("price_precision debe ser entero no negativo")
    if config.fill_policy not in _ALLOWED_FILL_POLICIES or config.close_policy not in _ALLOWED_FILL_POLICIES:
        raise CFDSimulationError("fill_policy/close_policy desconocida; sólo first_quote_at_or_after")
    if not isinstance(config.financing_required, bool):
        raise CFDSimulationError("financing_required debe ser booleano")
    if not isinstance(config.commission_known, bool):
        raise CFDSimulationError("commission_known debe ser booleano")
    _validate_retention(config)
    if config.decimal_policy_version != DECIMAL_POLICY_VERSION:
        raise CFDSimulationError(f"política Decimal no soportada: {config.decimal_policy_version!r}")


def _validate_retention(config: CFDConfig) -> None:
    names = ("terminal_retention", "event_retention", "quote_id_retention", "max_active_trades")
    for name in names:
        value = getattr(config, name)
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise CFDSimulationError(f"{name} debe ser entero positivo")


@dataclass(frozen=True, slots=True)
class CFDEconomicResult:
    """Economic knowledge kept separate from the lifecycle state."""

    state: EconomicState
    gross_pnl_quote: Decimal | None = None
    costs_quote: Decimal | None = None
    gross_pnl_account: Decimal | None = None
    costs_account: Decimal | None = None
    net_pnl: Decimal | None = None
    reason: str | None = None

    def __post_init__(self) -> None:
        state = self.state if isinstance(self.state, EconomicState) else EconomicState(str(self.state).upper())
        object.__setattr__(self, "state", state)
        for name in ("gross_pnl_quote", "costs_quote", "gross_pnl_account", "costs_account", "net_pnl"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, decimal(value, name=name))

    @property
    def determined(self) -> bool:
        return self.state is EconomicState.DETERMINED

    def to_dict(self) -> dict[str, Any]:
        return {
            "state": self.state.value,
            "economic_state": self.state.value,
            "gross_pnl_quote": str(self.gross_pnl_quote) if self.gross_pnl_quote is not None else None,
            "costs_quote": str(self.costs_quote) if self.costs_quote is not None else None,
            "gross_pnl_account": str(self.gross_pnl_account) if self.gross_pnl_account is not None else None,
            "costs_account": str(self.costs_account) if self.costs_account is not None else None,
            "net_pnl": str(self.net_pnl) if self.net_pnl is not None else None,
            "reason": self.reason,
            "economic_reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class CFDTrade:
    """Auditable lifecycle state and economic facts for one signal/horizon."""

    trade_id: str
    signal_id: str
    instrument: str
    direction: Direction
    units: Decimal
    horizon_seconds: Decimal
    state: TradeState
    detected_at: datetime
    signal_available_at: datetime
    decision_at: datetime
    entry_target_at: datetime
    fill_policy: str
    close_policy: str
    pip_size: Decimal
    price_precision: int
    account_currency: str
    quote_currency: str | None
    entry_market_at: datetime | None = None
    entry_available_at: datetime | None = None
    entry_quote_id: str | None = None
    entry_price: Decimal | None = None
    entry_side: str | None = None
    close_target_at: datetime | None = None
    close_market_at: datetime | None = None
    close_available_at: datetime | None = None
    close_quote_id: str | None = None
    close_price: Decimal | None = None
    pips: Decimal | None = None
    gross_pnl_quote: Decimal | None = None
    commission_quote: Decimal | None = None
    slippage_quote: Decimal | None = None
    financing_quote: Decimal | None = None
    gross_pnl_account: Decimal | None = None
    costs_account: Decimal | None = None
    net_pnl: Decimal | None = None
    conversion_rate: Decimal | None = None
    quality: QuoteQuality | str = QuoteQuality.VALID
    reason: str | None = None
    lineage: Mapping[str, Any] | None = None
    product: str = CFD_PRODUCT
    economics_version: str = CFD_ECONOMICS_VERSION
    # v2 keeps the quote-reference prices alongside execution prices.  This
    # makes the explicit slippage cost auditable without changing the legacy
    # fields or silently reinterpreting a v1 snapshot.
    entry_reference_price: Decimal | None = None
    close_reference_price: Decimal | None = None
    reference_gross_pnl_quote: Decimal | None = None

    def __post_init__(self) -> None:
        values = _normalise_trade_values(self)
        for name, value in values.items():
            object.__setattr__(self, name, value)
        _validate_trade(self)

    @property
    def identity(self) -> str:
        return self.trade_id

    @property
    def is_terminal(self) -> bool:
        return self.state in {TradeState.CLOSED, TradeState.REJECTED, TradeState.UNKNOWN}

    @property
    def close_observed(self) -> bool:
        """Whether a close fill was observed, even if its net is unknown."""
        return self.close_market_at is not None and self.close_price is not None

    @property
    def effective_fill_at(self) -> datetime | None:
        return self.entry_available_at

    @property
    def economic_result(self) -> CFDEconomicResult:
        if self.state in {TradeState.PENDING, TradeState.FILLED}:
            return CFDEconomicResult(EconomicState.NOT_SETTLED, reason="TRADE_NOT_CLOSED")
        if self.net_pnl is None:
            return CFDEconomicResult(
                EconomicState.INDETERMINATE,
                self.gross_pnl_quote,
                _quote_costs(self),
                self.gross_pnl_account,
                self.costs_account,
                None,
                self.reason or "NET_RESULT_UNKNOWN",
            )
        return CFDEconomicResult(
            EconomicState.DETERMINED,
            self.gross_pnl_quote,
            _quote_costs(self),
            self.gross_pnl_account,
            self.costs_account,
            self.net_pnl,
            self.reason,
        )

    def to_dict(self) -> dict[str, Any]:
        def dec(value: Decimal | None) -> str | None:
            return str(value) if value is not None else None

        return {
            "product": self.product,
            "trade_id": self.trade_id,
            "identity": self.identity,
            "signal_id": self.signal_id,
            "instrument": self.instrument,
            "direction": self.direction.value,
            "units": dec(self.units),
            "horizon_seconds": dec(self.horizon_seconds),
            "state": self.state.value,
            "close_observed": self.close_observed,
            "detected_at": _iso(self.detected_at),
            "signal_available_at": _iso(self.signal_available_at),
            "decision_at": _iso(self.decision_at),
            "entry_target_at": _iso(self.entry_target_at),
            "fill_policy": self.fill_policy,
            "close_policy": self.close_policy,
            "pip_size": dec(self.pip_size),
            "price_precision": self.price_precision,
            "account_currency": self.account_currency,
            "quote_currency": self.quote_currency,
            "entry_market_at": _iso(self.entry_market_at),
            "entry_available_at": _iso(self.entry_available_at),
            "entry_quote_id": self.entry_quote_id,
            "entry_price": dec(self.entry_price),
            "entry_side": self.entry_side,
            "close_target_at": _iso(self.close_target_at),
            "close_market_at": _iso(self.close_market_at),
            "close_available_at": _iso(self.close_available_at),
            "close_quote_id": self.close_quote_id,
            "close_price": dec(self.close_price),
            "pips": dec(self.pips),
            "gross_pnl_quote": dec(self.gross_pnl_quote),
            "commission_quote": dec(self.commission_quote),
            "slippage_quote": dec(self.slippage_quote),
            "financing_quote": dec(self.financing_quote),
            "gross_pnl_account": dec(self.gross_pnl_account),
            "costs_account": dec(self.costs_account),
            "net_pnl": dec(self.net_pnl),
            "conversion_rate": dec(self.conversion_rate),
            "quality": self.quality,
            "reason": self.reason,
            "lineage": _jsonable(self.lineage),
            "economics_version": self.economics_version,
            "entry_reference_price": dec(self.entry_reference_price),
            "close_reference_price": dec(self.close_reference_price),
            "reference_gross_pnl_quote": dec(self.reference_gross_pnl_quote),
            "economic_result": self.economic_result.to_dict(),
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> CFDTrade:
        allowed = {item.name for item in fields(cls)}
        data = {key: item for key, item in value.items() if key in allowed}
        data.pop("product", None) if data.get("product") is None else None
        data.setdefault("product", CFD_PRODUCT)
        return cls(**data)


def _normalise_trade_values(trade: CFDTrade) -> dict[str, Any]:
    values: dict[str, Any] = {
        "trade_id": _id(trade.trade_id, name="trade_id"),
        "signal_id": _id(trade.signal_id, name="signal_id"),
        "instrument": _id(trade.instrument, name="instrument").upper(),
        "direction": _direction(trade.direction),
        "units": decimal(trade.units, name="units", positive=True),
        "horizon_seconds": decimal(trade.horizon_seconds, name="horizon_seconds", positive=True),
        "state": trade.state if isinstance(trade.state, TradeState) else TradeState(str(trade.state).upper()),
        "quality": quality_from(trade.quality).value,
        "account_currency": _id(trade.account_currency, name="account_currency").upper(),
        "quote_currency": trade.quote_currency.upper() if trade.quote_currency else None,
        "lineage": dict(trade.lineage or {}),
        "product": trade.product,
        "economics_version": _normalise_economics_version(trade.economics_version),
    }
    values.update(
        {
            name: _utc(getattr(trade, name), name=name)
            for name in ("detected_at", "signal_available_at", "decision_at", "entry_target_at")
        }
    )
    values.update(
        {
            name: _optional_time(getattr(trade, name), name)
            for name in (
                "entry_market_at",
                "entry_available_at",
                "close_target_at",
                "close_market_at",
                "close_available_at",
            )
        }
    )
    values.update(
        {
            name: _optional_decimal(getattr(trade, name), name)
            for name in (
                "pip_size",
                "entry_price",
                "close_price",
                "entry_reference_price",
                "close_reference_price",
                "reference_gross_pnl_quote",
                "pips",
                "gross_pnl_quote",
                "commission_quote",
                "slippage_quote",
                "financing_quote",
                "gross_pnl_account",
                "costs_account",
                "net_pnl",
                "conversion_rate",
            )
        }
    )
    return values


def _optional_time(value: Any, name: str) -> datetime | None:
    return _utc(value, name=name) if value is not None else None


def _optional_decimal(value: Any, name: str) -> Decimal | None:
    return decimal(value, name=name) if value is not None else None


def _validate_trade(trade: CFDTrade) -> None:
    _normalise_economics_version(trade.economics_version)
    if (
        isinstance(trade.price_precision, bool)
        or not isinstance(trade.price_precision, int)
        or trade.price_precision < 0
    ):
        raise CFDSimulationError("price_precision inválido en trade")
    if trade.product != CFD_PRODUCT:
        raise CFDSimulationError("product de CFD no soportado")


def _quote_costs(trade: CFDTrade) -> Decimal | None:
    commission, slippage, financing = (trade.commission_quote, trade.slippage_quote, trade.financing_quote)
    if commission is None or financing is None:
        return None
    with decimal_context():
        if trade.economics_version == CFD_ECONOMICS_VERSION:
            # v2 embeds slippage in the executed prices.  The field remains
            # available as an informational decomposition, but it is not a
            # second monetary deduction.
            return commission + financing
        if slippage is None:
            return None
        return commission + slippage + financing


@dataclass(frozen=True, slots=True)
class CFDReplayResult:
    """Result view of the same state machine used by incremental ingest."""

    trades: tuple[CFDTrade, ...]
    events: tuple[Mapping[str, Any], ...] = ()
    capture_complete: bool = True
    counters: Mapping[str, int] | None = None
    finished: bool = False

    def __iter__(self) -> Iterator[CFDTrade]:
        return iter(self.trades)

    def __len__(self) -> int:
        return len(self.trades)

    def __getitem__(self, item: int | slice) -> CFDTrade | tuple[CFDTrade, ...]:
        return self.trades[item]

    @property
    def economics_version(self) -> str | None:
        versions = {trade.economics_version for trade in self.trades}
        return next(iter(versions)) if len(versions) == 1 else None

    @property
    def closed(self) -> tuple[CFDTrade, ...]:
        # A close fill with unknown commission/conversion is still a closed
        # position. EconomicState carries the independent uncertainty.
        return tuple(item for item in self.trades if item.state is TradeState.CLOSED or item.close_observed)

    def to_dict(self) -> dict[str, Any]:
        return {
            "product": CFD_PRODUCT,
            "economics_version": self.economics_version,
            "capture_complete": self.capture_complete,
            "finished": self.finished,
            "trades": [item.to_dict() for item in self.trades],
            "events": [_jsonable(item) for item in self.events],
            "counters": dict(self.counters or {}),
        }


class _QuoteBook:
    """Retain side state without making the simulator a provider adapter."""

    def __init__(self, current: CFDQuote | None = None) -> None:
        self.current = current
        self.last_reasons: tuple[QuoteReason, ...] = ()

    def apply(self, quote: CFDQuote) -> CFDQuote | None:
        self.last_reasons = ()
        previous = self.current
        if previous is None:
            self.current = quote
            return quote
        updated = _book_updated_sides(quote)
        if not updated:
            return previous
        values, reasons = _book_side_values(previous, quote, updated)
        self.last_reasons = reasons
        merged = _merge_book_quote(previous, quote, values, updated, reasons)
        if merged is None:
            self.last_reasons = (*self.last_reasons, QuoteReason.CROSSED)
            return None
        self.current = merged
        return merged

    def to_dict(self) -> dict[str, Any] | None:
        return self.current.to_dict() if self.current is not None else None


def _book_updated_sides(quote: CFDQuote) -> tuple[str, ...]:
    if quote.updated_sides:
        return tuple(quote.updated_sides)
    return tuple(side for side, present in (("bid", quote.bid is not None), ("ask", quote.ask is not None)) if present)


def _book_side_values(
    previous: CFDQuote,
    quote: CFDQuote,
    updated: Sequence[str],
) -> tuple[dict[str, Any], tuple[QuoteReason, ...]]:
    values: dict[str, Any] = {}
    reasons: list[QuoteReason] = []
    for side in ("bid", "ask"):
        use_new, reason = _book_side_choice(previous, quote, side, side in updated)
        if reason is not None:
            reasons.append(reason)
        source = quote if use_new else previous
        values.update(_book_side_fields(source, side))
    return values, tuple(dict.fromkeys(reasons))


def _book_side_choice(
    previous: CFDQuote,
    quote: CFDQuote,
    side: str,
    requested: bool,
) -> tuple[bool, QuoteReason | None]:
    if not requested:
        return False, None
    old_timestamp = getattr(previous, f"{side}_source_timestamp")
    new_timestamp = getattr(quote, f"{side}_source_timestamp")
    if old_timestamp is not None and new_timestamp is not None and new_timestamp < old_timestamp:
        return False, QuoteReason.OUT_OF_ORDER
    return True, None


def _book_side_fields(quote: CFDQuote, side: str) -> dict[str, Any]:
    names = (
        "",
        "_source_timestamp",
        "_received_at",
        "_available_at",
        "_quality",
        "_quality_reasons",
        "_timestamp_known",
    )
    return {
        f"{side}{suffix}" if suffix else side: getattr(quote, f"{side}{suffix}" if suffix else side) for suffix in names
    }


def _merge_book_quote(
    previous: CFDQuote,
    quote: CFDQuote,
    values: Mapping[str, Any],
    updated: Sequence[str],
    reasons: Sequence[QuoteReason],
) -> CFDQuote | None:
    metadata = {
        **dict(previous.metadata or {}),
        **dict(quote.metadata or {}),
        "book_updated_sides": list(updated),
        "quote_reasons": list(quote.quote_reasons or ()),
    }
    if reasons:
        metadata["out_of_order"] = True
    try:
        return CFDQuote(
            instrument=quote.instrument,
            market_time=quote.market_time,
            bid=values["bid"],
            ask=values["ask"],
            quote_id=quote.quote_id,
            available_at=quote.available_at,
            source=quote.source,
            sequence=quote.sequence,
            quality=quote.quality,
            metadata=metadata,
            bid_source_timestamp=values["bid_source_timestamp"],
            ask_source_timestamp=values["ask_source_timestamp"],
            bid_received_at=values["bid_received_at"],
            ask_received_at=values["ask_received_at"],
            bid_available_at=values["bid_available_at"],
            ask_available_at=values["ask_available_at"],
            bid_quality=values["bid_quality"],
            ask_quality=values["ask_quality"],
            bid_quality_reasons=values["bid_quality_reasons"],
            ask_quality_reasons=values["ask_quality_reasons"],
            bid_timestamp_known=values["bid_timestamp_known"],
            ask_timestamp_known=values["ask_timestamp_known"],
            updated_sides=tuple(updated),
            session_generation=quote.session_generation,
            connection_generation=quote.connection_generation,
            is_snapshot=quote.is_snapshot,
            disconnected=quote.disconnected,
        )
    except CFDSimulationError:
        return None


def _build_trade(config: CFDConfig, trade_id: str, signal: CFDSignal, horizon: Decimal) -> CFDTrade:
    decision_at = max(
        signal.detected_at + _timedelta_seconds(config.decision_latency_seconds),
        signal.available_at or signal.detected_at,
    )
    entry_target = decision_at + _timedelta_seconds(config.entry_latency_seconds)
    return CFDTrade(
        trade_id=trade_id,
        signal_id=signal.signal_id,
        instrument=signal.instrument,
        direction=signal.direction,
        units=config.units,
        horizon_seconds=horizon,
        state=TradeState.PENDING,
        detected_at=signal.detected_at,
        signal_available_at=signal.available_at or signal.detected_at,
        decision_at=decision_at,
        entry_target_at=entry_target,
        fill_policy=config.fill_policy,
        close_policy=config.close_policy,
        pip_size=config.pip_size,
        price_precision=config.price_precision,
        account_currency=config.account_currency,
        quote_currency=config.quote_currency,
        quality=signal.quality,
        lineage={
            "parent_signal_id": signal.signal_id,
            "strategy": signal.strategy,
            "config_hash": config.config_hash,
            "economics_version": config.economics_version,
        },
        economics_version=config.economics_version,
    )


def _reject_invalid_signal(trade: CFDTrade, signal: CFDSignal, instrument: str) -> CFDTrade:
    if signal.instrument != instrument:
        return replace(
            trade, state=TradeState.REJECTED, reason="INSTRUMENT_MISMATCH", quality=QuoteQuality.UNKNOWN.value
        )
    if quality_from(signal.quality) not in _SIGNAL_USABLE_QUALITY:
        return replace(
            trade, state=TradeState.REJECTED, reason="SIGNAL_QUALITY_BLOCKED", quality=QuoteQuality.UNKNOWN.value
        )
    return trade


def _close_target_is_eligible(trade: CFDTrade, quote: CFDQuote, max_age: Decimal) -> bool:
    return trade.close_target_at is not None and _target_is_eligible(trade.close_target_at, quote, max_age)


def _trade_has_entry(trade: CFDTrade) -> bool:
    return trade.entry_price is not None and trade.entry_available_at is not None


def _target_is_eligible(target: datetime, quote: CFDQuote, max_age: Decimal) -> bool:
    if quote.market_time < target or quote.available_ts < target:
        return False
    return seconds_decimal(quote.available_ts - target) <= max_age


def _quote_fillable(
    quote: CFDQuote,
    side: QuoteSide,
    watermark: datetime,
    config: CFDConfig,
    session_generation: int | str | None,
    connected: bool,
) -> bool:
    if not quote.operable_for(
        side,
        at=watermark,
        max_age_seconds=config.max_quote_age_seconds,
        expected_session=session_generation,
        connected=connected,
    ):
        return False
    return config.max_spread is None or (quote.spread is not None and quote.spread <= config.max_spread)


def _entry_price(
    raw: Decimal,
    direction: Direction,
    slippage_price: Decimal,
    quantize: Callable[[Decimal], Decimal],
) -> Decimal:
    with decimal_context():
        slipped = raw + slippage_price if direction is Direction.LONG else raw - slippage_price
    return quantize(slipped)


def _validate_advance_clock(
    previous: datetime | None,
    current: datetime,
    capture_complete: bool,
) -> None:
    if previous is not None and current < previous:
        raise CFDSimulationError("watermark no puede retroceder")
    if not isinstance(capture_complete, bool):
        raise CFDSimulationError("capture_complete debe ser booleano")


def _trade_deadline(trade: CFDTrade, max_age: Decimal) -> datetime | None:
    target = trade.entry_target_at if trade.state is TradeState.PENDING else trade.close_target_at
    return target + _timedelta_seconds(max_age) if target is not None else None


def _advance_trade(
    trade: CFDTrade,
    current: datetime,
    max_age: Decimal,
    capture_complete: bool,
) -> CFDTrade | None:
    if trade.state not in {TradeState.PENDING, TradeState.FILLED}:
        return None
    deadline = _trade_deadline(trade, max_age)
    expired = deadline is not None and current > deadline
    if not expired and not capture_complete:
        return None
    if expired:
        reason = "ENTRY_QUOTE_WINDOW_EXPIRED" if trade.state is TradeState.PENDING else "CLOSE_QUOTE_WINDOW_EXPIRED"
    else:
        reason = "ENTRY_QUOTE_NOT_AVAILABLE" if trade.state is TradeState.PENDING else "CLOSE_QUOTE_NOT_AVAILABLE"
    return replace(trade, state=TradeState.UNKNOWN, reason=reason, quality=QuoteQuality.UNKNOWN.value)


class CFDSimulator:
    """Incremental/replay state machine for linear bid/ask positions."""

    def __init__(
        self,
        config: CFDConfig | Mapping[str, Any] | None = None,
        *,
        terminal_lookup: Callable[[str], CFDTrade | None] | None = None,
        terminal_retention: int | None = None,
        event_retention: int | None = None,
        quote_id_retention: int | None = None,
        max_active_trades: int | None = None,
    ) -> None:
        base_config = config if isinstance(config, CFDConfig) else CFDConfig.from_mapping(config or {})
        overrides: dict[str, Any] = {
            name: value
            for name, value in (
                ("terminal_retention", terminal_retention),
                ("event_retention", event_retention),
                ("quote_id_retention", quote_id_retention),
                ("max_active_trades", max_active_trades),
            )
            if value is not None
        }
        self.config = replace(base_config, **overrides) if overrides else base_config
        self._terminal_lookup = terminal_lookup
        self._trades: dict[str, CFDTrade] = {}
        self._terminal_order: deque[str] = deque()
        self._archive_required = False
        self._seen_quotes: set[str] = set()
        self._quote_order: deque[str] = deque()
        self._events: deque[dict[str, Any]] = deque(maxlen=self.config.event_retention)
        self._last_watermark: datetime | None = None
        self._last_sequence: int | None = None
        self._book = _QuoteBook()
        self._finished = False
        self._connected = True
        self._session_generation: int | str | None = None
        self._session_baseline_required = False
        self._counters: dict[str, int] = {
            "signals_submitted": 0,
            "trades_submitted": 0,
            "fills": 0,
            "closures": 0,
            "unknown": 0,
            "rejected": 0,
            "quotes_ingested": 0,
            "quotes_duplicate": 0,
            "quotes_out_of_order": 0,
            "quotes_instrument_mismatch": 0,
            "quotes_blocked": 0,
            "terminal_evicted": 0,
            "archive_required": 0,
            "capacity_rejections": 0,
            "expiry_unknown": 0,
            "events_evicted": 0,
        }

    @property
    def trades(self) -> tuple[CFDTrade, ...]:
        return tuple(self._trades.values())

    @property
    def positions(self) -> tuple[CFDTrade, ...]:
        return self.trades

    @property
    def events(self) -> tuple[Mapping[str, Any], ...]:
        return tuple(self._events)

    @property
    def counters(self) -> Mapping[str, int]:
        result = dict(self._counters)
        result.update(
            {
                "trades_retained": len(self._trades),
                "terminal_retained": len(self._terminal_order),
                "active_retained": sum(not item.is_terminal for item in self._trades.values()),
            }
        )
        return result

    @property
    def finished(self) -> bool:
        return self._finished

    @property
    def archive_required(self) -> bool:
        return self._archive_required

    @property
    def active_capacity(self) -> tuple[int, int]:
        return self._active_count(), self.config.max_active_trades

    @property
    def current_quote(self) -> CFDQuote | None:
        return self._book.current

    @property
    def session_generation(self) -> int | str | None:
        return self._session_generation

    def reset(self) -> None:
        self._trades.clear()
        self._terminal_order.clear()
        self._archive_required = False
        self._seen_quotes.clear()
        self._quote_order.clear()
        self._events.clear()
        self._last_watermark = None
        self._last_sequence = None
        self._book = _QuoteBook()
        self._finished = False
        self._connected = True
        self._session_generation = None
        self._session_baseline_required = False
        for key in self._counters:
            self._counters[key] = 0

    def _event(self, event: Mapping[str, Any]) -> None:
        if len(self._events) == self._events.maxlen:
            self._counters["events_evicted"] += 1
        self._events.append(dict(_jsonable(event)))

    def _transition(self, trade: CFDTrade, *, previous: CFDTrade | None = None) -> None:
        self._trades[trade.trade_id] = trade
        self._count_transition(trade, previous)
        if trade.is_terminal and trade.trade_id not in self._terminal_order:
            self._terminal_order.append(trade.trade_id)
        self._evict_terminals()

    def _count_transition(self, trade: CFDTrade, previous: CFDTrade | None) -> None:
        if previous is None:
            self._counters["trades_submitted"] += 1
            if trade.state is TradeState.REJECTED:
                self._counters["rejected"] += 1
            return
        if previous.state is trade.state:
            return
        counter: str | None = {
            TradeState.FILLED: "fills",
            TradeState.CLOSED: "closures",
            TradeState.UNKNOWN: "unknown",
            TradeState.REJECTED: "rejected",
        }.get(trade.state)
        if counter is not None:
            self._counters[counter] += 1

    def _evict_terminals(self) -> None:
        while len(self._terminal_order) > self.config.terminal_retention:
            trade_id = self._terminal_order.popleft()
            if trade_id in self._trades:
                if self._terminal_lookup is None:
                    self._archive_required = True
                    self._counters["archive_required"] += 1
                del self._trades[trade_id]
                self._counters["terminal_evicted"] += 1

    def _active_count(self) -> int:
        return sum(not item.is_terminal for item in self._trades.values())

    def submit(
        self, signal: CFDSignal | Mapping[str, Any], *, horizon_seconds: Decimal | int | str | None = None
    ) -> CFDTrade:
        if self._finished:
            raise CFDSimulationError("la sesión CFD ya terminó; no se aceptan nuevas señales")
        signal_obj = signal if isinstance(signal, CFDSignal) else CFDSignal.from_mapping(signal)
        horizon = self._signal_horizon(horizon_seconds)
        existing = self._existing_trade(signal_obj, horizon)
        if existing is not None:
            return existing
        trade_id = self._trade_id(signal_obj, horizon)
        terminal = self._lookup_terminal(trade_id, signal_obj)
        if terminal is not None:
            return terminal
        if self._archive_required:
            raise CFDSimulationError(
                "se requiere resolver el archivo durable antes de aceptar nuevas señales",
                code="ARCHIVE_REQUIRED",
            )
        if self._active_count() >= self.config.max_active_trades:
            self._counters["capacity_rejections"] += 1
            raise CFDSimulationError(
                "capacidad activa CFD excedida; avance o drene la sesión",
                code="ACTIVE_CAPACITY_EXCEEDED",
            )
        trade = _build_trade(self.config, trade_id, signal_obj, horizon)
        trade = _reject_invalid_signal(trade, signal_obj, self.config.instrument)
        self._counters["signals_submitted"] += 1
        self._transition(trade)
        self._event(
            {
                "event": "submitted",
                "trade_id": trade.trade_id,
                "signal_id": trade.signal_id,
                "state": trade.state.value,
                "at": _iso(trade.decision_at),
            }
        )
        return trade

    def _signal_horizon(self, value: Decimal | int | str | None) -> Decimal:
        return (
            decimal(value, name="horizon_seconds", positive=True)
            if value is not None
            else self.config.horizons_seconds[0]
        )

    def _existing_trade(self, signal: CFDSignal, horizon: Decimal) -> CFDTrade | None:
        return next(
            (
                item
                for item in self._trades.values()
                if item.signal_id == signal.signal_id and item.horizon_seconds == horizon
            ),
            None,
        )

    def _lookup_terminal(self, trade_id: str, signal: CFDSignal) -> CFDTrade | None:
        if self._terminal_lookup is None:
            return None
        retained = self._terminal_lookup(trade_id)
        if retained is None:
            return None
        if not isinstance(retained, CFDTrade) or not retained.is_terminal:
            raise CFDSimulationError("terminal_lookup debe devolver sólo CFDTrade terminales")
        self._event({"event": "duplicate_terminal", "trade_id": trade_id, "signal_id": signal.signal_id})
        return retained

    def submit_all(self, signal: CFDSignal | Mapping[str, Any]) -> tuple[CFDTrade, ...]:
        signal_obj = signal if isinstance(signal, CFDSignal) else CFDSignal.from_mapping(signal)
        self._preflight_signal_batch(signal_obj)
        return tuple(self.submit(signal_obj, horizon_seconds=horizon) for horizon in self.config.horizons_seconds)

    def _preflight_signal_batch(self, signal: CFDSignal) -> None:
        if self._finished:
            raise CFDSimulationError("la sesión CFD ya terminó; no se aceptan nuevas señales")
        if self._archive_required and self._terminal_lookup is None:
            raise CFDSimulationError(
                "se requiere resolver el archivo durable antes de aceptar nuevas señales",
                code="ARCHIVE_REQUIRED",
            )
        needed = sum(self._existing_trade(signal, horizon) is None for horizon in self.config.horizons_seconds)
        if self._active_count() + needed > self.config.max_active_trades:
            self._counters["capacity_rejections"] += 1
            raise CFDSimulationError(
                "capacidad activa CFD excedida; avance o drene la sesión",
                code="ACTIVE_CAPACITY_EXCEEDED",
            )

    def ingest(
        self, item: CFDSignal | CFDQuote | Mapping[str, Any], *, kind: str | None = None, capture_complete: bool = False
    ) -> tuple[CFDTrade, ...]:
        """Ingest a signal or quote through the same state machine used by replay."""

        if isinstance(item, CFDSignal):
            return self.submit_all(item)
        if isinstance(item, CFDQuote):
            return self.on_quote(item, capture_complete=capture_complete)
        if not isinstance(item, Mapping):
            raise CFDSimulationError(f"ingest no admite {type(item).__name__}")
        selected = _ingest_kind(item, kind)
        if selected == "signal":
            return self.submit_all(CFDSignal.from_mapping(item))
        if selected == "quote":
            return self.on_quote(item, capture_complete=capture_complete)
        raise CFDSimulationError(f"tipo de ingest no soportado: {kind!r}")

    def ingest_signal(self, signal: CFDSignal | Mapping[str, Any]) -> tuple[CFDTrade, ...]:
        return self.submit_all(signal)

    def ingest_quote(
        self, quote: CFDQuote | Mapping[str, Any], *, capture_complete: bool = False
    ) -> tuple[CFDTrade, ...]:
        return self.on_quote(quote, capture_complete=capture_complete)

    def _session_gate(self, quote: CFDQuote) -> bool:
        if not self._session_connected(quote):
            return False
        if self._generation_changed(quote):
            return False
        return self._baseline_gate(quote)

    def _session_connected(self, quote: CFDQuote) -> bool:
        if quote.disconnected:
            self._connected = False
            self._block_session_quote(quote, QuoteReason.DISCONNECTED)
            return False
        if not self._connected:
            self._block_session_quote(quote, QuoteReason.DISCONNECTED)
            return False
        return True

    def _generation_changed(self, quote: CFDQuote) -> bool:
        generation = quote.session_generation
        if generation is None:
            if self._session_generation is None:
                return False
            self._session_baseline_required = True
            self._book = _QuoteBook()
            self._event(
                {"event": "session_changed", "quote_id": quote.identity, "reason": QuoteReason.SESSION_MISMATCH.value}
            )
            return True
        if self._session_generation is None:
            self._session_generation = generation
            return False
        if generation == self._session_generation:
            return False
        self._session_generation = generation
        self._session_baseline_required = True
        self._book = _QuoteBook()
        self._event({"event": "session_changed", "quote_id": quote.identity, "session_generation": generation})
        return True

    def _baseline_gate(self, quote: CFDQuote) -> bool:
        if not self._session_baseline_required:
            return True
        self._book.apply(quote)
        self._counters["quotes_blocked"] += 1
        reason = QuoteReason.SNAPSHOT if quote.is_snapshot else QuoteReason.SESSION_MISMATCH
        self._event({"event": "quote_blocked", "quote_id": quote.identity, "reason": reason.value})
        if not quote.is_snapshot and quote.has_both:
            self._session_baseline_required = False
        return False

    def _block_session_quote(self, quote: CFDQuote, reason: QuoteReason) -> None:
        self._counters["quotes_blocked"] += 1
        self._event({"event": "quote_blocked", "quote_id": quote.identity, "reason": reason.value})

    def disconnect(self, *, reason: str = "DISCONNECTED") -> None:
        self._connected = False
        self._event({"event": "disconnected", "reason": reason})

    def reconnect(self, session_generation: int | str | None = None) -> None:
        self._connected = True
        self._session_generation = session_generation
        self._session_baseline_required = True
        self._book = _QuoteBook()
        self._event({"event": "reconnected", "session_generation": session_generation})

    def on_quote(self, quote: CFDQuote | Mapping[str, Any], *, capture_complete: bool = False) -> tuple[CFDTrade, ...]:
        if not isinstance(capture_complete, bool):
            raise CFDSimulationError("capture_complete debe ser booleano")
        quote_obj = self._coerce_quote(quote)
        if quote_obj is None:
            return ()
        if self._finished:
            self._event({"event": "quote_ignored", "quote_id": quote_obj.identity, "reason": "SESSION_FINISHED"})
            return ()
        # Reject before clock advancement, deduplication or book mutation. A
        # wrong-symbol quote must not be able to contaminate a later partial
        # update for the configured instrument.
        if quote_obj.instrument != self.config.instrument:
            self._counters["quotes_instrument_mismatch"] += 1
            self._counters["quotes_blocked"] += 1
            self._event(
                {
                    "event": "quote_blocked",
                    "quote_id": quote_obj.identity,
                    "reason": "INSTRUMENT_MISMATCH",
                    "instrument": quote_obj.instrument,
                    "expected_instrument": self.config.instrument,
                }
            )
            return ()
        watermark = quote_obj.available_ts
        if not self._accept_quote_clock(quote_obj, watermark):
            return ()
        if self._remember_quote(quote_obj):
            return ()
        self._counters["quotes_ingested"] += 1
        if not self._session_gate(quote_obj):
            return ()
        book_quote = self._book.apply(quote_obj)
        if self._book_is_blocked(book_quote, quote_obj):
            return ()
        assert book_quote is not None
        changed = self._apply_quote_to_trades(book_quote, watermark)
        if capture_complete:
            changed.extend(self.advance(watermark, capture_complete=True))
        return tuple(dict((item.trade_id, item) for item in changed).values())

    def _coerce_quote(self, quote: CFDQuote | Mapping[str, Any]) -> CFDQuote | None:
        try:
            return quote if isinstance(quote, CFDQuote) else CFDQuote.from_mapping(quote)
        except (CFDSimulationError, TypeError, ValueError) as exc:
            self._counters["quotes_blocked"] += 1
            self._event({"event": "quote_blocked", "reason": "INVALID_QUOTE", "detail": str(exc)})
            return None

    def _accept_quote_clock(self, quote: CFDQuote, watermark: datetime) -> bool:
        if self._last_watermark is not None and watermark < self._last_watermark:
            self._reject_out_of_order(quote, watermark)
            return False
        sequence = _sequence_int(quote.sequence)
        if sequence is not None and self._last_sequence is not None and sequence < self._last_sequence:
            self._reject_out_of_order(quote, watermark, sequence=sequence)
            return False
        self._last_watermark = watermark
        if sequence is not None:
            self._last_sequence = sequence
        return True

    def _reject_out_of_order(self, quote: CFDQuote, watermark: datetime, *, sequence: int | None = None) -> None:
        self._counters["quotes_out_of_order"] += 1
        self._counters["quotes_blocked"] += 1
        event: dict[str, Any] = {
            "event": "quote_blocked",
            "quote_id": quote.identity,
            "reason": QuoteReason.OUT_OF_ORDER.value,
            "at": _iso(watermark),
        }
        if sequence is not None:
            event["sequence"] = sequence
        self._event(event)

    def _remember_quote(self, quote: CFDQuote) -> bool:
        if quote.identity in self._seen_quotes:
            self._counters["quotes_duplicate"] += 1
            self._event({"event": "duplicate_quote", "quote_id": quote.identity, "at": _iso(quote.available_ts)})
            return True
        self._seen_quotes.add(quote.identity)
        self._quote_order.append(quote.identity)
        while len(self._quote_order) > self.config.quote_id_retention:
            self._seen_quotes.discard(self._quote_order.popleft())
        return False

    def _book_is_blocked(self, quote: CFDQuote | None, observed: CFDQuote) -> bool:
        if quote is not None and not self._book.last_reasons:
            return False
        self._counters["quotes_blocked"] += 1
        reasons = [item.value for item in self._book.last_reasons]
        if not reasons:
            reasons = [QuoteReason.MISSING_BID.value, QuoteReason.MISSING_ASK.value]
        self._event({"event": "quote_blocked", "quote_id": observed.identity, "reason": reasons})
        return True

    def _apply_quote_to_trades(self, quote: CFDQuote, watermark: datetime) -> list[CFDTrade]:
        changed: list[CFDTrade] = []
        for trade in tuple(self._trades.values()):
            changed.extend(self._apply_quote_to_trade(trade, quote, watermark))
        return changed

    def _apply_quote_to_trade(self, trade: CFDTrade, quote: CFDQuote, watermark: datetime) -> list[CFDTrade]:
        if trade.state not in {TradeState.PENDING, TradeState.FILLED} or trade.instrument != quote.instrument:
            return []
        changed: list[CFDTrade] = []
        if trade.state is TradeState.PENDING:
            selected = self._entry_fill(trade, quote, watermark)
            if selected is not None:
                self._transition(selected, previous=trade)
                changed.append(selected)
                self._event(
                    {
                        "event": "filled",
                        "trade_id": selected.trade_id,
                        "quote_id": quote.identity,
                        "at": _iso(selected.entry_available_at),
                        "price": str(selected.entry_price),
                    }
                )
                trade = selected
        if trade.state is TradeState.FILLED:
            selected = self._close_fill(trade, quote, watermark)
            if selected is not None:
                self._transition(selected, previous=trade)
                changed.append(selected)
                self._event(
                    {
                        "event": "closed" if selected.state is TradeState.CLOSED else "unknown",
                        "trade_id": selected.trade_id,
                        "quote_id": quote.identity,
                        "at": _iso(selected.close_available_at),
                        "reason": selected.reason,
                    }
                )
        return changed

    def advance(self, watermark: datetime, *, capture_complete: bool = False) -> tuple[CFDTrade, ...]:
        """Advance time without inventing a quote; expired windows become UNKNOWN."""

        current = _utc(watermark, name="watermark")
        _validate_advance_clock(self._last_watermark, current, capture_complete)
        self._last_watermark = current
        if self._finished:
            return ()
        changed: list[CFDTrade] = []
        for trade in tuple(self._trades.values()):
            updated = _advance_trade(trade, current, self.config.max_quote_age_seconds, capture_complete)
            if updated is None:
                continue
            self._transition(updated, previous=trade)
            changed.append(updated)
            if updated.reason == "ENTRY_QUOTE_WINDOW_EXPIRED" or updated.reason == "CLOSE_QUOTE_WINDOW_EXPIRED":
                self._counters["expiry_unknown"] += 1
            self._event({"event": "unknown", "trade_id": trade.trade_id, "reason": updated.reason, "at": _iso(current)})
        return tuple(changed)

    def finish(self, watermark: datetime | None = None, *, capture_complete: bool = True) -> CFDReplayResult:
        """Close the session idempotently; unresolved work becomes UNKNOWN."""

        if not isinstance(capture_complete, bool):
            raise CFDSimulationError("capture_complete debe ser booleano")
        if self._finished:
            return self._result(capture_complete=True)
        current = watermark or self._last_watermark
        if current is not None:
            self.advance(current, capture_complete=capture_complete)
        if capture_complete:
            if current is None:
                # No wall clock is consulted.  Use only a recorded logical
                # target if one exists; otherwise there is nothing to advance.
                targets = [item.entry_target_at for item in self._trades.values() if not item.is_terminal]
                if targets:
                    self.advance(max(targets), capture_complete=True)
            self._finished = True
            self._event({"event": "finished", "at": _iso(self._last_watermark)})
        return self._result(capture_complete=capture_complete)

    def _entry_fill(self, trade: CFDTrade, quote: CFDQuote, watermark: datetime) -> CFDTrade | None:
        target = trade.entry_target_at
        if not _target_is_eligible(target, quote, self.config.max_quote_age_seconds):
            return None
        required = QuoteSide.ASK if trade.direction is Direction.LONG else QuoteSide.BID
        if not _quote_fillable(quote, required, watermark, self.config, self._session_generation, self._connected):
            return None
        raw = quote.ask if trade.direction is Direction.LONG else quote.bid
        if raw is None:
            return None
        price = _entry_price(raw, trade.direction, self.config.slippage_price, self._quantize)
        reference_price = self._quantize(raw) if trade.economics_version == CFD_ECONOMICS_VERSION else None
        close_target = max(quote.available_ts, target) + _timedelta_seconds(
            trade.horizon_seconds + self.config.close_latency_seconds
        )
        return replace(
            trade,
            state=TradeState.FILLED,
            entry_market_at=quote.market_time,
            entry_available_at=quote.available_ts,
            entry_quote_id=quote.identity,
            entry_price=price,
            entry_reference_price=reference_price,
            entry_side="ask" if trade.direction is Direction.LONG else "bid",
            close_target_at=close_target,
            quality=quote.quality,
            lineage={**dict(trade.lineage or {}), "entry_quote_id": quote.identity, "entry_source": quote.source},
        )

    def _close_fill(self, trade: CFDTrade, quote: CFDQuote, watermark: datetime) -> CFDTrade | None:
        if not _close_target_is_eligible(trade, quote, self.config.max_quote_age_seconds):
            return None
        required = QuoteSide.BID if trade.direction is Direction.LONG else QuoteSide.ASK
        if not _quote_fillable(quote, required, watermark, self.config, self._session_generation, self._connected):
            return None
        raw = quote.bid if trade.direction is Direction.LONG else quote.ask
        if raw is None or not _trade_has_entry(trade):
            return None
        values = self._close_values(trade, quote, raw)
        state = TradeState.CLOSED
        return replace(
            trade,
            state=state,
            close_market_at=quote.market_time,
            close_available_at=quote.available_ts,
            close_quote_id=quote.identity,
            close_price=values["price"],
            close_reference_price=values["reference_close"],
            reference_gross_pnl_quote=values["reference_gross"],
            pips=values["pips"],
            gross_pnl_quote=values["gross"],
            commission_quote=values["commission"],
            slippage_quote=values["slippage"],
            financing_quote=values["financing"],
            gross_pnl_account=values["gross_account"],
            costs_account=values["costs_account"],
            net_pnl=values["net"],
            conversion_rate=self.config.conversion_rate,
            quality=QuoteQuality.UNKNOWN.value if values["unknown_reason"] else quote.quality,
            reason=values["unknown_reason"],
            lineage={**dict(trade.lineage or {}), "close_quote_id": quote.identity, "close_source": quote.source},
        )

    def _close_values(self, trade: CFDTrade, quote: CFDQuote, raw: Decimal) -> dict[str, Any]:
        assert trade.entry_price is not None and trade.entry_available_at is not None
        reference_close: Decimal | None = None
        reference_gross: Decimal | None = None
        if trade.economics_version == CFD_ECONOMICS_VERSION:
            price, reference_close, gross, pips, slippage, reference_gross = self._close_price_values_v2(trade, raw)
        else:
            price, _sign, gross, pips = self._close_price_values(trade, raw)
            slippage = self._slippage_cost(trade)
        commission = self._commission_value(trade)
        financing, financing_missing = self._financing_value(trade, quote)
        costs_quote = self._costs_value(
            commission,
            D0 if trade.economics_version == CFD_ECONOMICS_VERSION else slippage,
            financing,
        )
        gross_account, costs_account, net, unknown_reason = self._economic_values(
            gross, costs_quote, financing_missing, commission is None
        )
        return {
            "price": self._quantize(price),
            "reference_close": reference_close,
            "reference_gross": reference_gross,
            "pips": pips,
            "gross": gross,
            "commission": commission,
            "slippage": slippage,
            "financing": financing,
            "gross_account": gross_account,
            "costs_account": costs_account,
            "net": net,
            "unknown_reason": unknown_reason,
        }

    def _close_price_values(self, trade: CFDTrade, raw: Decimal) -> tuple[Decimal, Decimal, Decimal, Decimal]:
        assert trade.entry_price is not None
        with decimal_context():
            price = (
                raw - self.config.slippage_price
                if trade.direction is Direction.LONG
                else raw + self.config.slippage_price
            )
            sign = D1 if trade.direction is Direction.LONG else Decimal("-1")
            gross = (price - trade.entry_price) * trade.units * sign
            pips = ((price - trade.entry_price) / trade.pip_size) * sign
        return price, sign, gross, pips

    def _close_price_values_v2(
        self, trade: CFDTrade, raw: Decimal
    ) -> tuple[Decimal, Decimal, Decimal, Decimal, Decimal, Decimal]:
        """Calculate v2 economics from quantized reference and execution prices.

        ``entry_price``/``close_price`` remain the adverse execution prices,
        while gross/pips are calculated from those execution prices.  The
        reference gross and slippage impact are retained separately for
        diagnostics; slippage is not deducted a second time.
        """

        assert trade.entry_price is not None
        reference_entry = trade.entry_reference_price
        if reference_entry is None:
            # Defensive compatibility for a v2 mapping created before the
            # reference fields were added.  New v2 trades always persist them.
            with decimal_context():
                reference_entry = (
                    trade.entry_price - self.config.slippage_price
                    if trade.direction is Direction.LONG
                    else trade.entry_price + self.config.slippage_price
                )
            reference_entry = self._quantize(reference_entry)
        reference_close = self._quantize(raw)
        with decimal_context():
            execution_close = (
                raw - self.config.slippage_price
                if trade.direction is Direction.LONG
                else raw + self.config.slippage_price
            )
            execution_close = self._quantize(execution_close)
            sign = D1 if trade.direction is Direction.LONG else Decimal("-1")
            reference_gross = (reference_close - reference_entry) * trade.units * sign
            execution_gross = (execution_close - trade.entry_price) * trade.units * sign
            slippage = reference_gross - execution_gross
            if slippage < D0:
                # Tick rounding can erase or very rarely reverse a sub-tick
                # adverse move; never report a negative cost.
                slippage = D0
            execution_pips = ((execution_close - trade.entry_price) / trade.pip_size) * sign
        return execution_close, reference_close, execution_gross, execution_pips, slippage, reference_gross

    def _commission_value(self, trade: CFDTrade) -> Decimal | None:
        if not self.config.commission_known:
            return None
        with decimal_context():
            return self.config.commission_fixed + self.config.commission_per_unit * trade.units

    def _financing_value(self, trade: CFDTrade, quote: CFDQuote) -> tuple[Decimal | None, bool]:
        assert trade.entry_price is not None and trade.entry_available_at is not None
        missing = self.config.financing_required and self.config.financing_rate_per_second is None
        if missing:
            return None, True
        hold_seconds = seconds_decimal(quote.available_ts - trade.entry_available_at)
        with decimal_context():
            value = abs(trade.entry_price * trade.units) * (self.config.financing_rate_per_second or D0) * hold_seconds
        return value, False

    def _costs_value(
        self,
        commission: Decimal | None,
        slippage: Decimal,
        financing: Decimal | None,
    ) -> Decimal | None:
        if commission is None:
            return None
        with decimal_context():
            return commission + slippage + (financing or D0)

    def _economic_values(
        self,
        gross_quote: Decimal,
        costs_quote: Decimal | None,
        financing_missing: bool,
        commission_missing: bool,
    ) -> tuple[Decimal | None, Decimal | None, Decimal | None, str | None]:
        if costs_quote is None:
            gross_account, conversion_reason = self._gross_account(gross_quote)
            reason = "COMMISSION_UNKNOWN" if commission_missing else conversion_reason
            return gross_account, None, None, reason
        gross_account, costs_account, net, reason = self._accounting(gross_quote, costs_quote, financing_missing)
        if commission_missing:
            return gross_account, None, None, "COMMISSION_UNKNOWN"
        return gross_account, costs_account, net, reason

    def _gross_account(self, gross_quote: Decimal) -> tuple[Decimal | None, str | None]:
        quote = self.config.quote_currency
        account = self.config.account_currency
        if quote is None or account == quote:
            return gross_quote, None
        if self.config.conversion_rate is None:
            return None, "CONVERSION_RATE_MISSING"
        with decimal_context():
            return gross_quote * self.config.conversion_rate, None

    def _accounting(
        self, gross_quote: Decimal, costs_quote: Decimal, financing_missing: bool
    ) -> tuple[Decimal | None, Decimal | None, Decimal | None, str | None]:
        if financing_missing:
            return None, None, None, "FINANCING_RATE_MISSING"
        quote = self.config.quote_currency
        account = self.config.account_currency
        if quote is None or account == quote:
            with decimal_context():
                return gross_quote, costs_quote, gross_quote - costs_quote, None
        if self.config.conversion_rate is None:
            return None, None, None, "CONVERSION_RATE_MISSING"
        with decimal_context():
            gross_account = gross_quote * self.config.conversion_rate
            costs_account = costs_quote * self.config.conversion_rate
            return gross_account, costs_account, gross_account - costs_account, None

    def _slippage_cost(self, trade: CFDTrade) -> Decimal:
        if self.config.slippage_price == D0:
            return D0
        with decimal_context():
            return self.config.slippage_price * trade.units * Decimal("2")

    def _quantize(self, value: Decimal) -> Decimal:
        quantum = Decimal(1).scaleb(-self.config.price_precision)
        return quantize_decimal(value, quantum, policy=DEFAULT_DECIMAL_POLICY)

    def _trade_id(self, signal: CFDSignal, horizon: Decimal) -> str:
        payload = f"{signal.signal_id}|{signal.instrument}|{signal.direction.value}|{horizon}|{self.config.config_hash}"
        return "cfd_" + hashlib.sha256(payload.encode()).hexdigest()[:32]

    def drain_events(self) -> tuple[Mapping[str, Any], ...]:
        drained = tuple(self._events)
        self._events.clear()
        return drained

    def _result(self, *, capture_complete: bool) -> CFDReplayResult:
        return CFDReplayResult(self.trades, self.events, capture_complete, self.counters, self._finished)

    def replay(
        self,
        signals: Iterable[CFDSignal | CFDQuote | Mapping[str, Any]] = (),
        quotes: Iterable[CFDQuote | Mapping[str, Any]] | None = None,
        *,
        capture_complete: bool = True,
        records: Iterable[CFDSignal | CFDQuote | Mapping[str, Any]] | None = None,
    ) -> CFDReplayResult:
        """Feed caller-provided records through ``submit``/``on_quote``.

        The two-iterable form is retained for compatibility and submits all
        signals before consuming quotes.  New causal captures should use the
        single ``records`` iterable (or :meth:`replay_interleaved`) so signals
        and quotes remain interleaved in their observed order.  No form sorts
        by market timestamps.
        """

        if not isinstance(capture_complete, bool):
            raise CFDSimulationError("capture_complete debe ser booleano")
        signal_items = list(signals)
        if records is not None:
            if quotes is not None or signal_items:
                raise CFDSimulationError("records no puede combinarse con signals/quotes")
            return self._replay_interleaved(records, capture_complete=capture_complete)
        if quotes is None:
            return self._replay_interleaved(signal_items, capture_complete=capture_complete)
        quote_items = list(quotes)
        if any(_is_quote_record(item) for item in signal_items):
            if quote_items:
                raise CFDSimulationError("una captura intercalada no puede combinarse con quotes separados")
            return self._replay_interleaved(signal_items, capture_complete=capture_complete)
        return self._replay_separate(signal_items, quote_items, capture_complete=capture_complete)

    def _replay_separate(
        self,
        signals: Sequence[CFDSignal | CFDQuote | Mapping[str, Any]],
        quotes: Sequence[CFDQuote | Mapping[str, Any]],
        *,
        capture_complete: bool,
    ) -> CFDReplayResult:
        for signal in signals:
            if isinstance(signal, CFDQuote):
                raise CFDSimulationError("quotes deben ir en el segundo iterable de replay")
            self.submit_all(signal if isinstance(signal, CFDSignal) else CFDSignal.from_mapping(signal))
        for quote in quotes:
            self._replay_quote(quote)
        return self._finish_replay(capture_complete=capture_complete)

    def _replay_quote(self, quote: CFDQuote | Mapping[str, Any]) -> None:
        if isinstance(quote, CFDQuote):
            self.on_quote(quote, capture_complete=False)
            return
        try:
            quote_obj = CFDQuote.from_mapping(quote)
        except (CFDSimulationError, TypeError, ValueError):
            self.on_quote(quote, capture_complete=False)
            return
        self.on_quote(quote_obj, capture_complete=False)

    def _finish_replay(self, *, capture_complete: bool) -> CFDReplayResult:
        last_watermark = self._last_watermark
        if capture_complete:
            return self.finish(last_watermark, capture_complete=True)
        if last_watermark is not None:
            self.advance(last_watermark, capture_complete=False)
        return self._result(capture_complete=False)

    def _replay_interleaved(
        self,
        records: Iterable[CFDSignal | CFDQuote | Mapping[str, Any]],
        *,
        capture_complete: bool,
    ) -> CFDReplayResult:
        """Replay a single causal stream without a retrospective sort."""

        for record in records:
            self._replay_record(record)
        return self._finish_replay(capture_complete=capture_complete)

    def _replay_record(self, record: CFDSignal | CFDQuote | Mapping[str, Any]) -> None:
        if isinstance(record, CFDSignal):
            self.submit_all(record)
        elif isinstance(record, CFDQuote):
            self.on_quote(record, capture_complete=False)
        elif isinstance(record, Mapping):
            selected = _ingest_kind(record, None)
            if selected == "signal":
                self.submit_all(CFDSignal.from_mapping(record))
            elif selected == "quote":
                self.on_quote(record, capture_complete=False)
            else:
                raise CFDSimulationError(f"tipo de replay no soportado: {selected!r}")
        else:
            raise CFDSimulationError(f"registro de replay no soportado: {type(record).__name__}")

    def replay_interleaved(
        self,
        records: Iterable[CFDSignal | CFDQuote | Mapping[str, Any]],
        *,
        capture_complete: bool = True,
    ) -> CFDReplayResult:
        """Public explicit entry point for an interleaved causal capture."""

        return self._replay_interleaved(records, capture_complete=capture_complete)

    replay_records = replay_interleaved

    def stream(
        self,
        signals: Iterable[CFDSignal | CFDQuote | Mapping[str, Any]] = (),
        quotes: Iterable[CFDQuote | Mapping[str, Any]] | None = None,
        *,
        capture_complete: bool = False,
        records: Iterable[CFDSignal | CFDQuote | Mapping[str, Any]] | None = None,
    ) -> CFDReplayResult:
        return self.replay(signals, quotes, capture_complete=capture_complete, records=records)

    def snapshot(self) -> dict[str, Any]:
        """Return a versioned, exact checkpoint of active and retained state."""

        state: dict[str, Any] = {
            "snapshot_version": CFD_SNAPSHOT_VERSION,
            "product": CFD_PRODUCT,
            "economics_version": self.config.economics_version,
            "config": self.config.to_dict(),
            "config_hash": self.config.config_hash,
            "decimal_policy_version": DECIMAL_POLICY_VERSION,
            "trades": [trade.to_dict() for trade in self.trades],
            "terminal_order": list(self._terminal_order),
            "archive_required": self._archive_required,
            "events": [_jsonable(event) for event in self.events],
            "seen_quote_ids": list(self._quote_order),
            "quote_book": self._book.to_dict(),
            "last_watermark": _iso(self._last_watermark),
            "last_sequence": self._last_sequence,
            "finished": self._finished,
            "connected": self._connected,
            "session_generation": self._session_generation,
            "session_baseline_required": self._session_baseline_required,
            "counters": self.counters,
            "retention": {
                "terminal": self.config.terminal_retention,
                "events": self.config.event_retention,
                "quote_ids": self.config.quote_id_retention,
                "max_active": self.config.max_active_trades,
            },
        }
        state["snapshot_hash"] = _digest(state)
        return state

    checkpoint = snapshot

    def _restore_snapshot(
        self, snapshot: Mapping[str, Any], *, config: CFDConfig | Mapping[str, Any] | None = None
    ) -> None:
        restored_config = _validate_snapshot(snapshot, config)
        self.config = restored_config
        self._clear_restore_state()
        self._restore_trades(snapshot)
        self._restore_history(snapshot)
        self._restore_book_and_clock(snapshot)
        self._restore_flags(snapshot)
        self._restore_counters(snapshot)
        _validate_finished_state(self._finished, self._trades.values())

    def _clear_restore_state(self) -> None:
        self._trades.clear()
        self._terminal_order.clear()
        self._events = deque(maxlen=self.config.event_retention)
        self._seen_quotes.clear()
        self._quote_order.clear()

    def _restore_trades(self, snapshot: Mapping[str, Any]) -> None:
        economics_version = _snapshot_economics_version(snapshot)
        for raw_trade in snapshot.get("trades", ()):
            if isinstance(raw_trade, CFDTrade):
                trade = raw_trade
            else:
                if not isinstance(raw_trade, Mapping):
                    raise CFDSimulationError("trade inválido en snapshot CFD")
                trade_data = dict(raw_trade)
                trade_data.setdefault("economics_version", economics_version)
                trade = CFDTrade.from_mapping(trade_data)
            if trade.trade_id in self._trades:
                raise CFDSimulationError(f"trade duplicado en snapshot: {trade.trade_id}")
            if trade.economics_version != self.config.economics_version:
                raise CFDSimulationError("trade y snapshot usan economics_version distintos")
            self._trades[trade.trade_id] = trade
        terminal_ids = [str(trade_id) for trade_id in snapshot.get("terminal_order", ())]
        if len(set(terminal_ids)) != len(terminal_ids):
            raise CFDSimulationError("terminal_order duplicado en snapshot CFD")
        for trade_id in terminal_ids:
            self._restore_terminal_id(trade_id)
        if self._active_count() > self.config.max_active_trades:
            raise CFDSimulationError("snapshot excede la capacidad activa CFD")

    def _restore_terminal_id(self, trade_id: str) -> None:
        trade = self._trades.get(trade_id)
        if trade is None or not trade.is_terminal:
            raise CFDSimulationError(f"terminal_order referencia trade no terminal: {trade_id}")
        self._terminal_order.append(trade_id)

    def _restore_history(self, snapshot: Mapping[str, Any]) -> None:
        for event in snapshot.get("events", ()):
            if isinstance(event, Mapping):
                self._events.append(dict(event))
        quote_ids = [str(quote_id) for quote_id in snapshot.get("seen_quote_ids", ())]
        if len(set(quote_ids)) != len(quote_ids):
            raise CFDSimulationError("seen_quote_ids duplicados en snapshot CFD")
        for quote_id in quote_ids:
            self._quote_order.append(str(quote_id))
            self._seen_quotes.add(str(quote_id))
        while len(self._quote_order) > self.config.quote_id_retention:
            self._seen_quotes.discard(self._quote_order.popleft())

    def _restore_book_and_clock(self, snapshot: Mapping[str, Any]) -> None:
        raw_book = snapshot.get("quote_book")
        self._book = _QuoteBook(CFDQuote.from_mapping(raw_book) if isinstance(raw_book, Mapping) else None)
        if self._book.current is not None and self._book.current.instrument != self.config.instrument:
            raise CFDSimulationError("quote_book usa un instrumento distinto al snapshot")
        raw_watermark = snapshot.get("last_watermark")
        self._last_watermark = _utc(raw_watermark, name="last_watermark") if raw_watermark is not None else None
        raw_sequence = snapshot.get("last_sequence")
        self._last_sequence = int(raw_sequence) if raw_sequence is not None else None

    def _restore_flags(self, snapshot: Mapping[str, Any]) -> None:
        self._finished = _bool(snapshot.get("finished", False), name="finished")
        self._archive_required = _bool(snapshot.get("archive_required", False), name="archive_required")
        self._connected = _bool(snapshot.get("connected", True), name="connected")
        self._session_generation = snapshot.get("session_generation")
        self._session_baseline_required = _bool(
            snapshot.get("session_baseline_required", False), name="session_baseline_required"
        )

    def _restore_counters(self, snapshot: Mapping[str, Any]) -> None:
        counters = snapshot.get("counters", {})
        for key in self._counters:
            value = counters.get(key, 0) if isinstance(counters, Mapping) else 0
            self._counters[key] = int(value)

    def restore(
        self, snapshot: Mapping[str, Any], *, config: CFDConfig | Mapping[str, Any] | None = None
    ) -> CFDSimulator:
        """Restore this session in place without reopening terminal trades."""
        self._restore_snapshot(snapshot, config=config)
        return self

    @classmethod
    def from_snapshot(
        cls, snapshot: Mapping[str, Any], *, config: CFDConfig | Mapping[str, Any] | None = None
    ) -> CFDSimulator:
        selected = config if config is not None else snapshot.get("config", {})
        result = cls(selected)
        return result.restore(snapshot, config=selected)


def _validate_snapshot(
    snapshot: Mapping[str, Any],
    config: CFDConfig | Mapping[str, Any] | None,
) -> CFDConfig:
    if not isinstance(snapshot, Mapping):
        raise CFDSimulationError("snapshot debe ser mapping")
    if snapshot.get("snapshot_version") != CFD_SNAPSHOT_VERSION:
        raise CFDSimulationError(f"snapshot CFD incompatible: {snapshot.get('snapshot_version')!r}")
    if snapshot.get("product") != CFD_PRODUCT:
        raise CFDSimulationError("snapshot no corresponde al producto CFD PAPER")
    if snapshot.get("decimal_policy_version") != DECIMAL_POLICY_VERSION:
        raise CFDSimulationError("snapshot usa una política Decimal distinta")
    _validate_snapshot_hash(snapshot)
    economics_version = _snapshot_economics_version(snapshot)
    raw_config = snapshot.get("config")
    selected = raw_config if config is None else config
    if isinstance(selected, CFDConfig):
        restored = selected
    else:
        if not isinstance(selected, Mapping):
            raise CFDSimulationError("snapshot CFD no contiene una configuración válida")
        config_data = dict(selected)
        config_data.setdefault("economics_version", economics_version)
        restored = CFDConfig.from_mapping(config_data)
    if restored.economics_version != economics_version:
        raise CFDSimulationError("configuración y snapshot usan economics_version distintos")
    expected_config_hash = snapshot.get("config_hash")
    legacy_config_hash = _digest(raw_config) if economics_version == CFD_ECONOMICS_LEGACY_VERSION else None
    if restored.config_hash != expected_config_hash and legacy_config_hash != expected_config_hash:
        raise CFDSimulationError("configuración CFD distinta a la del snapshot")
    return restored


def _snapshot_economics_version(snapshot: Mapping[str, Any]) -> str:
    """Resolve the version while treating pre-H1 snapshots as legacy v1."""

    raw_top = snapshot.get("economics_version")
    raw_config = snapshot.get("config")
    raw_config_version = raw_config.get("economics_version") if isinstance(raw_config, Mapping) else None
    if raw_top is not None and raw_config_version is not None:
        top = _normalise_economics_version(raw_top, default=CFD_ECONOMICS_LEGACY_VERSION)
        config_version = _normalise_economics_version(raw_config_version, default=CFD_ECONOMICS_LEGACY_VERSION)
        if top != config_version:
            raise CFDSimulationError("snapshot y configuración usan economics_version distintos")
        return top
    if raw_top is not None:
        return _normalise_economics_version(raw_top, default=CFD_ECONOMICS_LEGACY_VERSION)
    if raw_config_version is not None:
        return _normalise_economics_version(raw_config_version, default=CFD_ECONOMICS_LEGACY_VERSION)
    return CFD_ECONOMICS_LEGACY_VERSION


def _validate_snapshot_hash(snapshot: Mapping[str, Any]) -> None:
    expected = snapshot.get("snapshot_hash")
    if not expected:
        return
    material = dict(snapshot)
    material.pop("snapshot_hash", None)
    if _digest(material) != expected:
        raise CFDSimulationError("hash de snapshot CFD inválido")


def _validate_finished_state(finished: bool, trades: Iterable[CFDTrade]) -> None:
    if finished and any(not trade.is_terminal for trade in trades):
        raise CFDSimulationError("snapshot terminado contiene trades no terminales")


def _ingest_kind(item: Mapping[str, Any], kind: str | None) -> str:
    selected = str(kind or item.get("kind", item.get("record_type", ""))).strip().lower()
    if selected in {"signal", "cfd_signal"}:
        return "signal"
    if selected in {"quote", "cfd_quote", "event"}:
        return "quote"
    if not selected and "signal_id" in item and "direction" in item:
        return "signal"
    if not selected:
        return "quote"
    return selected


def _is_quote_record(item: Any) -> bool:
    if isinstance(item, CFDQuote):
        return True
    if isinstance(item, Mapping):
        return _ingest_kind(item, None) == "quote"
    return False


def _sequence_int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# Clear aliases for callers who prefer a longer name.
ForexCFDSimulator = CFDSimulator
ForexCFDConfig = CFDConfig
ForexCFDSignal = CFDSignal
ForexCFDQuote = CFDQuote
ForexCFDTrade = CFDTrade
EconomicResult = CFDEconomicResult


def known_fixture_eurusd_long() -> tuple[CFDSignal, tuple[CFDQuote, ...]]:
    """Offline fixture: 1000 EUR, ask 1.1002 to bid 1.1005 = 0.30 USD."""

    start = datetime(2026, 1, 1, tzinfo=UTC)
    signal = CFDSignal("fixture-long-1000", "EUR/USD", Direction.LONG, start, strategy="offline_fixture")
    quotes = (
        CFDQuote("EUR/USD", start, Decimal("1.1000"), Decimal("1.1002"), "fixture-entry"),
        CFDQuote("EUR/USD", start + timedelta(seconds=60), Decimal("1.1005"), Decimal("1.1007"), "fixture-close"),
    )
    return signal, quotes


__all__ = [
    "CFDConfig",
    "CFDEconomicResult",
    "CFDQuote",
    "CFDReplayResult",
    "CFDSignal",
    "CFDSimulationError",
    "CFDSimulator",
    "CFDTrade",
    "CFD_SNAPSHOT_VERSION",
    "CFD_ECONOMICS_LEGACY_VERSION",
    "CFD_ECONOMICS_VERSION",
    "ECONOMICS_VERSION",
    "LEGACY_ECONOMICS_VERSION",
    "CFD_PRODUCT",
    "Direction",
    "EconomicResult",
    "EconomicState",
    "ForexCFDConfig",
    "ForexCFDQuote",
    "ForexCFDSignal",
    "ForexCFDSimulator",
    "ForexCFDTrade",
    "TradeState",
    "known_fixture_eurusd_long",
    "decimal",
]
