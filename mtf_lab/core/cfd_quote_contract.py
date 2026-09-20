"""Pure CFD quote contract and side-evidence normalization.

The quote boundary is kept separate from the CFD simulator lifecycle.  The
legacy ``mtf_lab.core.cfd_simulation`` module re-exports the public names and
shared scalar coercions from here, so callers retain the historical import
path, exception identity, and serialized quote contract.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any

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
from .numeric import decimal_context, seconds_decimal

D0 = Decimal("0")


class CFDSimulationError(ValueError):
    """Configuration or input error; no trade is silently fabricated."""

    def __init__(self, message: str, *, code: str = "CFD_INPUT_INVALID") -> None:
        super().__init__(message)
        self.code = code


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
            # A merged BBO may retain the other side from an older envelope.
            # Its receipt/availability timestamp is valid evidence for that
            # side even when the new envelope's market time is later.  Never
            # move the envelope backwards; only a future side observation can
            # promote the quote's effective availability.
            if side_time >= market:
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


__all__ = [
    "CFDQuote",
    "CFDSimulationError",
    "QuoteAssessment",
    "QuoteLeg",
    "QuoteQuality",
    "QuoteReason",
    "QuoteSide",
    "assess_pair",
    "decimal",
    "quality_from",
    "quality_reasons_from",
]
