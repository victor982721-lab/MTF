"""cTrader market models, normalization, and provider adapter.

Only this module converts cTrader market payloads into the provider-neutral
``Event``/``Bar`` records.  Connection lifecycle belongs to
``ctrader_session``; protocol presence belongs to ``ctrader_protocol``.
The quote book retains bid and ask independently, including the source and
availability clock for each leg.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, Literal, TypeAlias

from .ctrader_accounts import normalize_account_payload
from .ctrader_config import CTraderConfig, normalize_symbol_name
from .ctrader_errors import (
    AuthState,
    CTraderAuthError,
    CTraderConfigurationError,
    CTraderDataError,
)
from .ctrader_protocol import (
    PAYLOAD,
    TREND_PERIOD_NAMES,
    TREND_PERIODS,
    DependencyReport,
    WireMessage,
    jsonable,
    message_to_mapping,
    read_field,
    read_repeated,
    stable_hash,
)
from .ctrader_session import CTraderClient, CTraderStatus
from .ctrader_transport import CTraderTransport
from .models import Bar, Event, ensure_utc, resolution_to_seconds

_DEFAULT_AVAILABILITY = object()
QuoteBasis: TypeAlias = Literal["mid", "bid", "ask"]


class QuoteQualityState(StrEnum):
    """Operational quality of an assembled bid/ask quote."""

    VALID = "VALID"
    INCOMPLETE = "INCOMPLETE"
    UNKNOWN = "UNKNOWN"
    STALE = "STALE"
    INVALID = "INVALID"
    DISCONNECTED = "DISCONNECTED"


class QuoteQualityReason(StrEnum):
    MISSING_BID = "MISSING_BID"
    MISSING_ASK = "MISSING_ASK"
    MISSING_SOURCE_TIMESTAMP = "MISSING_SOURCE_TIMESTAMP"
    STALE_BID = "STALE_BID"
    STALE_ASK = "STALE_ASK"
    CROSSED = "CROSSED"
    OUT_OF_ORDER = "OUT_OF_ORDER"
    REJECTED_UPDATE = "REJECTED_UPDATE"
    SNAPSHOT = "SNAPSHOT"
    DISCONNECTED = "DISCONNECTED"
    SESSION_CHANGED = "SESSION_CHANGED"
    AVAILABILITY_UNKNOWN = "AVAILABILITY_UNKNOWN"

    # Readable aliases for callers that use the longer names.
    MISSING_TIMESTAMP = "MISSING_SOURCE_TIMESTAMP"
    BID_STALE = "STALE_BID"
    ASK_STALE = "STALE_ASK"


@dataclass(frozen=True, slots=True)
class QuoteLegQuality:
    """Evidence retained for one side of a quote."""

    price: float | None
    source_timestamp: datetime | None
    received_at: datetime | None
    available_at: datetime | None
    timestamp_missing: bool
    age_seconds: float | None
    state: QuoteQualityState
    reasons: tuple[QuoteQualityReason, ...] = ()
    generation: int = 0

    @property
    def usable(self) -> bool:
        return self.price is not None and self.state is QuoteQualityState.VALID

    def to_dict(self) -> dict[str, Any]:
        return {
            "price": self.price,
            "source_timestamp": _iso(self.source_timestamp),
            "received_at": _iso(self.received_at),
            "available_at": _iso(self.available_at),
            "timestamp_missing": self.timestamp_missing,
            "age_seconds": self.age_seconds,
            "state": self.state.value,
            "reasons": [reason.value for reason in self.reasons],
            "generation": self.generation,
        }


@dataclass(frozen=True, slots=True)
class QuoteQuality:
    """Combined quality decision with typed reasons and per-side evidence."""

    state: QuoteQualityState
    reasons: tuple[QuoteQualityReason, ...]
    bid: QuoteLegQuality
    ask: QuoteLegQuality
    max_age_seconds: float | None = None

    @property
    def usable(self) -> bool:
        return self.state is QuoteQualityState.VALID and self.bid.usable and self.ask.usable

    def to_dict(self) -> dict[str, Any]:
        return {
            "state": self.state.value,
            "reasons": [reason.value for reason in self.reasons],
            "max_age_seconds": self.max_age_seconds,
            "usable": self.usable,
            "bid": self.bid.to_dict(),
            "ask": self.ask.to_dict(),
        }

    def metadata(self) -> dict[str, Any]:
        """Flat fields ease report/query consumers without string parsing."""

        return {
            "quality_state": self.state.value,
            "quality_reasons": [reason.value for reason in self.reasons],
            "quote_quality": self.to_dict(),
            "bid_quality_state": self.bid.state.value,
            "bid_quality_reasons": [reason.value for reason in self.bid.reasons],
            "ask_quality_state": self.ask.state.value,
            "ask_quality_reasons": [reason.value for reason in self.ask.reasons],
            "bid_age_seconds": self.bid.age_seconds,
            "ask_age_seconds": self.ask.age_seconds,
            "max_quote_age_seconds": self.max_age_seconds,
            "quote_usable": self.usable,
        }


@dataclass(frozen=True, slots=True)
class CTraderInstrumentSpec:
    symbol: str = "EUR/USD"
    symbol_id: int | None = None
    digits: int = 5
    pip_position: int = 4
    price_scale: int = 100_000

    def __post_init__(self) -> None:
        if self.symbol_id is not None and (isinstance(self.symbol_id, bool) or int(self.symbol_id) <= 0):
            raise CTraderDataError("symbol_id debe ser positivo")
        if self.digits <= 0 or self.pip_position <= 0 or self.pip_position > self.digits or self.price_scale <= 0:
            raise CTraderDataError("especificación de escala inválida")
        object.__setattr__(self, "symbol", normalize_symbol_name(self.symbol))

    def price_from_relative(self, value: Any) -> float:
        try:
            relative = int(value)
        except (TypeError, ValueError) as exc:
            raise CTraderDataError(f"precio relativo inválido: {value!r}") from exc
        if relative < 0:
            raise CTraderDataError("precio relativo no puede ser negativo")
        result = round(relative / float(self.price_scale), self.digits)
        if not math.isfinite(result) or result <= 0:
            raise CTraderDataError("precio escalado inválido")
        return result

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "symbol_id": self.symbol_id,
            "digits": self.digits,
            "pip_position": self.pip_position,
            "price_scale": self.price_scale,
            "periods": dict(TREND_PERIODS),
        }


@dataclass(frozen=True, slots=True)
class CTraderSymbol:
    symbol_id: int
    name: str
    digits: int | None = None
    pip_position: int | None = None
    enabled: bool | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.symbol_id <= 0:
            raise CTraderDataError("symbol_id debe ser positivo")
        object.__setattr__(self, "name", str(self.name).strip())
        object.__setattr__(self, "metadata", dict(self.metadata))

    @property
    def normalized_name(self) -> str:
        return normalize_symbol_name(self.name)

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol_id": self.symbol_id,
            "name": self.name,
            "digits": self.digits,
            "pip_position": self.pip_position,
            "enabled": self.enabled,
            "metadata": jsonable(self.metadata),
        }


@dataclass(frozen=True, slots=True)
class SymbolCatalog:
    symbols: tuple[CTraderSymbol, ...]
    requested_symbol: str
    selected: CTraderSymbol | None = None

    def find(self, symbol: str | None = None) -> CTraderSymbol | None:
        wanted_text = symbol or self.requested_symbol
        wanted = normalize_symbol_name(wanted_text)
        return next(
            (
                item
                for item in self.symbols
                if item.normalized_name == wanted or item.name.upper() == str(wanted_text).upper()
            ),
            None,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "requested_symbol": self.requested_symbol,
            "selected": self.selected.symbol_id if self.selected else None,
            "symbols": [item.to_dict() for item in self.symbols],
        }


@dataclass(frozen=True, slots=True)
class CTraderNormalizationResult:
    records: tuple[Event | Bar, ...]
    quote_events: tuple[Event, ...] = ()
    bars: tuple[Bar, ...] = ()
    issues: tuple[str, ...] = ()
    snapshot: bool = False
    symbol_id: int | None = None
    quote_quality: QuoteQuality | None = None

    def __iter__(self) -> Iterator[Event | Bar]:
        return iter(self.records)

    def __len__(self) -> int:
        return len(self.records)

    def __getitem__(self, index: int) -> Event | Bar:
        return self.records[index]


@dataclass(frozen=True, slots=True)
class CTraderFetchResult:
    bars: tuple[Bar, ...]
    timeframe: str
    symbol: CTraderInstrumentSpec
    request: WireMessage
    response: WireMessage
    has_more: bool = False
    issues: tuple[str, ...] = ()

    def __iter__(self) -> Iterator[Bar]:
        return iter(self.bars)

    def __len__(self) -> int:
        return len(self.bars)

    def to_dict(self) -> dict[str, Any]:
        return {
            "timeframe": self.timeframe,
            "symbol": self.symbol.to_dict(),
            "count": len(self.bars),
            "has_more": self.has_more,
            "issues": list(self.issues),
            "request": self.request.to_dict(),
            "response_type": self.response.payload_type_name,
        }


@dataclass(frozen=True, slots=True)
class CTraderSessionWindow:
    weekday: int
    open_minute: int
    close_minute: int

    def __post_init__(self) -> None:
        if (
            not 0 <= int(self.weekday) <= 6
            or not 0 <= int(self.open_minute) < 1_440
            or not 0 <= int(self.close_minute) <= 1_440
        ):
            raise CTraderConfigurationError("ventana de sesión inválida")


class CTraderMarketCalendar:
    """Explicit schedule; no schedule remains UNKNOWN, never OPEN."""

    def __init__(self, windows: Iterable[CTraderSessionWindow] = (), holidays: Iterable[str] = ()) -> None:
        self.windows = tuple(windows)
        self.holidays = frozenset(str(item) for item in holidays)

    def state(
        self,
        when: datetime,
        *,
        last_quote_at: datetime | None = None,
        max_quote_age_seconds: float = 90.0,
    ) -> str:
        instant = ensure_utc(when, field_name="when")
        if instant.date().isoformat() in self.holidays:
            return "CLOSED_SCHEDULED"
        minute = instant.hour * 60 + instant.minute
        active = tuple(
            item
            for item in self.windows
            if item.weekday == instant.weekday() and item.open_minute <= minute < item.close_minute
        )
        if not active:
            return "CLOSED_SCHEDULED" if self.windows else "UNKNOWN"
        if last_quote_at is None:
            return "OPEN_NO_QUOTE"
        age = (instant - ensure_utc(last_quote_at, field_name="last_quote_at")).total_seconds()
        return "OPEN" if age <= float(max_quote_age_seconds) else "OPEN_NO_QUOTE"


@dataclass(frozen=True, slots=True)
class CTraderHistoryResult:
    bars: tuple[Bar, ...]
    timeframe: str
    pages: int
    complete: bool
    has_more: bool
    issues: tuple[str, ...] = ()
    # The compact public summary intentionally omits payload bytes.  Query
    # callers that explicitly request a capture export use these detached raw
    # response pages plus their receive metadata to preserve native trendbars
    # without reconstructing bid/ask quotes.
    raw_pages: tuple[Mapping[str, Any], ...] = ()
    page_metadata: tuple[Mapping[str, Any], ...] = ()

    def __iter__(self) -> Iterator[Bar]:
        return iter(self.bars)

    def __len__(self) -> int:
        return len(self.bars)

    def to_dict(self) -> dict[str, Any]:
        return {
            "timeframe": self.timeframe,
            "count": len(self.bars),
            "pages": self.pages,
            "complete": self.complete,
            "has_more": self.has_more,
            "issues": list(self.issues),
            "raw_page_count": len(self.raw_pages),
        }


class CTraderProvider:
    """Read-only cTrader adapter with a per-symbol, per-side quote book."""

    name = "ctrader_open_api"

    def __init__(
        self,
        config: CTraderConfig | Mapping[str, Any] | None = None,
        *,
        client: CTraderClient | None = None,
        transport: CTraderTransport | None = None,
        max_quote_age_seconds: float = 90.0,
        clock: Callable[[], datetime] | None = None,
        monotonic: Callable[[], float] | None = None,
    ) -> None:
        self.config = config if isinstance(config, CTraderConfig) else CTraderConfig.from_mapping(config)
        self.client = client or CTraderClient(self.config, transport=transport)
        self.instrument = self.config.symbol
        self.spec = CTraderInstrumentSpec(
            self.config.symbol,
            self.config.symbol_id,
            self.config.digits,
            self.config.pip_position,
            self.config.price_scale,
        )
        self.catalog: SymbolCatalog | None = None
        self._subscribed: set[str] = set()
        self._quote_state: dict[int, dict[str, dict[str, Any]]] = {}
        self._generation = 0
        self._discontinuity_reason: str | None = None
        if (
            isinstance(max_quote_age_seconds, bool)
            or not math.isfinite(float(max_quote_age_seconds))
            or float(max_quote_age_seconds) < 0
        ):
            raise CTraderConfigurationError("max_quote_age_seconds debe ser finito y no negativo")
        self.max_quote_age_seconds = float(max_quote_age_seconds)
        self._clock = clock or (lambda: datetime.now(UTC))
        self._monotonic = monotonic or time_monotonic

    @property
    def status(self) -> CTraderStatus:
        return self.client.status

    @property
    def dependency(self) -> DependencyReport:
        report = self.client.dependency
        if not isinstance(report, DependencyReport):
            raise CTraderDataError("dependency report inválido")
        return report

    @property
    def generation(self) -> int:
        return self._generation

    def connect(self) -> CTraderStatus:
        status = self.client.connect()
        self.reset_generation(status.generation)
        return status

    def close(self) -> None:
        self.reset_discontinuity("DISCONNECTED")
        self.client.close()

    def authenticate(self, **kwargs: Any) -> AuthState:
        return self.client.authenticate(**kwargs)

    def discover_accounts(
        self, *, token_provider: Callable[[str], str] | None = None, include_token: bool = False
    ) -> dict[str, Any]:
        return self.client.discover_accounts(token_provider=token_provider, include_token=include_token)

    def authorize_account(self, account_id: int, *, token_provider: Callable[[str], str] | None = None) -> AuthState:
        return self.client.authorize_account(account_id, token_provider=token_provider)

    def _require_authenticated(self) -> int:
        if self.client.status.auth is not AuthState.AUTHENTICATED:
            self.client._set_status(
                auth=AuthState.ACCOUNT_REQUIRED,
                action="Autentique una cuenta cTrader antes de consultar mercado",
            )
            raise CTraderAuthError("cuenta no autenticada", action=self.client.status.action)
        account_id = self.client.authenticated_account_id or self.config.account_id
        if not account_id:
            raise CTraderAuthError("falta account_id", action="Configure ctidTraderAccountId")
        return int(account_id)

    def resolve_symbol(self, *, include_archived: bool = False) -> SymbolCatalog:
        account_id = self._require_authenticated()
        response = self.client.request(
            "PROTO_OA_SYMBOLS_LIST_REQ",
            {"ctidTraderAccountId": account_id, "includeArchivedSymbols": bool(include_archived)},
        )
        symbols: list[CTraderSymbol] = []
        for raw in read_repeated(response.payload, "symbol", "symbols"):
            symbol_id = _int_or_none(read_field(raw, "symbolId", "symbol_id", default=None))
            name = read_field(raw, "symbolName", "name", default=None)
            if symbol_id is None or name is None:
                continue
            symbols.append(
                CTraderSymbol(
                    symbol_id,
                    str(name),
                    _int_or_none(read_field(raw, "digits", default=None)),
                    _int_or_none(read_field(raw, "pipPosition", "pip_position", default=None)),
                    _bool_or_none(read_field(raw, "enabled", default=None)),
                    _mapping(raw),
                )
            )
        wanted = normalize_symbol_name(self.config.symbol)
        matches = [item for item in symbols if item.normalized_name == wanted]
        if not matches:
            self.catalog = SymbolCatalog(tuple(symbols), self.config.symbol, None)
            raise CTraderDataError(f"símbolo no encontrado en catálogo: {self.config.symbol}")
        if len(matches) != 1:
            self.catalog = SymbolCatalog(tuple(symbols), self.config.symbol, None)
            raise CTraderDataError(f"símbolo ambiguo en catálogo: {self.config.symbol}; seleccione symbol_id explícito")
        selected = matches[0]
        if self.config.symbol_id is not None and selected.symbol_id != self.config.symbol_id:
            self.catalog = SymbolCatalog(tuple(symbols), self.config.symbol, None)
            raise CTraderDataError(f"symbol_id {self.config.symbol_id} no corresponde al nombre {self.config.symbol}")
        if (
            selected.digits is None
            or selected.digits <= 0
            or selected.pip_position is None
            or selected.pip_position <= 0
        ):
            detail_response = self.client.request(
                "PROTO_OA_SYMBOL_BY_ID_REQ",
                # The installed OpenApiPy schema declares symbolId as a
                # repeated field on SymbolByIdReq, even when one symbol is
                # requested.  Passing a scalar only fails when the real codec
                # constructs the generated message.
                {"ctidTraderAccountId": account_id, "symbolId": [selected.symbol_id]},
            )
            detail_items = read_repeated(detail_response.payload, "symbol")
            detail = detail_items[0] if detail_items else None
            if detail is None:
                self.catalog = SymbolCatalog(tuple(symbols), self.config.symbol, None)
                raise CTraderDataError("respuesta SymbolById sin entidad completa")
            digits = _int_or_none(read_field(detail, "digits", default=None))
            pip_position = _int_or_none(read_field(detail, "pipPosition", "pip_position", default=None))
            if digits is None or digits <= 0 or pip_position is None or pip_position <= 0 or pip_position > digits:
                self.catalog = SymbolCatalog(tuple(symbols), self.config.symbol, None)
                raise CTraderDataError("entidad SymbolById sin digits/pipPosition observados")
            selected = CTraderSymbol(
                selected.symbol_id,
                selected.name,
                digits,
                pip_position,
                selected.enabled,
                {**dict(selected.metadata), **_mapping(detail)},
            )
            symbols = [selected if item.symbol_id == selected.symbol_id else item for item in symbols]
        selected_digits = selected.digits
        selected_pip_position = selected.pip_position
        if selected_digits is None or selected_pip_position is None:
            raise CTraderDataError("símbolo seleccionado sin digits/pipPosition observados")
        self.catalog = SymbolCatalog(tuple(symbols), self.config.symbol, selected)
        self.instrument = selected.name
        self.spec = CTraderInstrumentSpec(
            selected.name,
            selected.symbol_id,
            selected_digits,
            selected_pip_position,
            self.config.price_scale,
        )
        return self.catalog

    def _ensure_symbol(self) -> CTraderInstrumentSpec:
        if self.spec.symbol_id is None:
            self.resolve_symbol()
        if self.spec.symbol_id is None:
            raise CTraderDataError("symbol_id no resuelto")
        return self.spec

    def fetch(
        self,
        timeframe: str = "M1",
        *,
        count: int | None = None,
        from_timestamp: datetime | None = None,
        to_timestamp: datetime | None = None,
    ) -> CTraderFetchResult:
        account_id = self._require_authenticated()
        spec = self._ensure_symbol()
        period = _period_code(timeframe)
        count_value = int(count if count is not None else self.config.historical_count)
        if count_value <= 0:
            raise CTraderConfigurationError("count debe ser positivo")
        params: dict[str, Any] = {
            "ctidTraderAccountId": account_id,
            "period": period,
            "symbolId": spec.symbol_id,
            "count": count_value,
        }
        if from_timestamp is not None:
            params["fromTimestamp"] = _unix_ms(from_timestamp)
        if to_timestamp is not None:
            params["toTimestamp"] = _unix_ms(to_timestamp)
        request = WireMessage("PROTO_OA_GET_TRENDBARS_REQ", params, None, False)
        response = self.client.request(request.payload_type, request.payload)
        received = self._clock()
        raw_bars = read_repeated(response.payload, "trendbar", "trendbars")
        bars: list[Bar] = []
        issues: list[str] = []
        for raw in raw_bars:
            try:
                bars.append(
                    normalize_trendbar(
                        raw,
                        spec=spec,
                        received_at=received,
                        request_id=response.client_msg_id or "history",
                        mode="REPLAY",
                    )
                )
            except CTraderDataError as exc:
                issues.append(str(exc))
        bars.sort(key=lambda item: item.interval_start)
        actual_request = WireMessage(request.payload_type, request.payload, response.client_msg_id, False)
        has_more = bool(read_field(response.payload, "hasMore", "has_more", default=False))
        return CTraderFetchResult(
            tuple(bars), _period_name(period), spec, actual_request, response, has_more, tuple(issues)
        )

    fetch_bars = fetch
    fetch_trendbars = fetch

    def fetch_history(
        self,
        timeframe: str = "M1",
        *,
        count: int | None = None,
        from_timestamp: datetime | None = None,
        to_timestamp: datetime | None = None,
        max_pages: int = 20,
    ) -> CTraderHistoryResult:
        if isinstance(max_pages, bool) or max_pages <= 0:
            raise CTraderConfigurationError("max_pages debe ser positivo")
        start_bound = ensure_utc(from_timestamp, field_name="from_timestamp") if from_timestamp is not None else None
        cursor_to = to_timestamp
        collected: dict[tuple[datetime, int], Bar] = {}
        issues: list[str] = []
        raw_pages: list[Mapping[str, Any]] = []
        page_metadata: list[Mapping[str, Any]] = []
        pages = 0
        has_more = False
        previous_earliest: datetime | None = None
        while pages < max_pages:
            page = self.fetch(timeframe, count=count, from_timestamp=start_bound, to_timestamp=cursor_to)
            pages += 1
            raw_pages.append(message_to_mapping(page.response.payload))
            page_metadata.append(
                {
                    "received_at": page.response.received_at,
                    "available_at": page.response.available_at,
                    "ingest_sequence": page.response.ingest_sequence,
                    "connection_generation": page.response.connection_generation,
                    "source_identity": page.response.source_identity,
                    "request": page.request.to_dict(),
                    "response_type": page.response.payload_type_name,
                }
            )
            issues.extend(page.issues)
            has_more = page.has_more
            for bar in page.bars:
                collected[(bar.interval_start, int(bar.revision))] = bar
            if any(not bar.closed for bar in page.bars):
                issues.append("histórico incluye un intervalo abierto; cobertura cerrada incompleta")
            if not page.has_more:
                break
            earliest = min((bar.interval_start for bar in page.bars), default=None)
            if earliest is None or (previous_earliest is not None and earliest >= previous_earliest):
                issues.append("histórico hasMore sin progreso; cobertura incompleta")
                break
            previous_earliest = earliest
            if start_bound is not None and earliest <= start_bound:
                break
            cursor_to = earliest - timedelta(seconds=resolution_to_seconds(_period_name(_period_code(timeframe))))
        else:
            issues.append("histórico excedió max_pages; cobertura incompleta")
        bars = tuple(sorted(collected.values(), key=lambda item: (item.interval_start, item.revision)))
        # Any normalization issue, including an invalid trendbar or an empty
        # page, makes the historical result partial.  A false hasMore flag is
        # not sufficient evidence of a complete native series.
        if not bars:
            issues.append("histórico sin barras válidas; cobertura incompleta")
        complete = bool(bars) and not has_more and not issues
        return CTraderHistoryResult(
            bars,
            _period_name(_period_code(timeframe)),
            pages,
            complete,
            has_more,
            tuple(issues),
            tuple(raw_pages),
            tuple(page_metadata),
        )

    def subscribe_spots(self, *, subscribe_to_spot_timestamp: bool = True) -> WireMessage:
        account_id = self._require_authenticated()
        spec = self._ensure_symbol()
        return self.client.request(
            "PROTO_OA_SUBSCRIBE_SPOTS_REQ",
            {
                "ctidTraderAccountId": account_id,
                "symbolId": [spec.symbol_id],
                "subscribeToSpotTimestamp": bool(subscribe_to_spot_timestamp),
            },
        )

    def subscribe_live_trendbars(self, *, timeframes: Iterable[str] | None = None) -> tuple[WireMessage, ...]:
        account_id = self._require_authenticated()
        spec = self._ensure_symbol()
        responses: list[WireMessage] = []
        for timeframe in tuple(timeframes or self.config.timeframes):
            period = _period_code(timeframe)
            responses.append(
                self.client.request(
                    "PROTO_OA_SUBSCRIBE_LIVE_TRENDBAR_REQ",
                    {"ctidTraderAccountId": account_id, "period": period, "symbolId": spec.symbol_id},
                )
            )
            self._subscribed.add(_period_name(period))
        return tuple(responses)

    def subscribe(
        self,
        *,
        timeframes: Iterable[str] | None = None,
        subscribe_to_spot_timestamp: bool = True,
    ) -> tuple[WireMessage, ...]:
        account_id = self._require_authenticated()
        spec = self._ensure_symbol()
        responses = [
            self.client.request(
                "PROTO_OA_SUBSCRIBE_SPOTS_REQ",
                {
                    "ctidTraderAccountId": account_id,
                    "symbolId": [spec.symbol_id],
                    "subscribeToSpotTimestamp": bool(subscribe_to_spot_timestamp),
                },
            )
        ]
        for timeframe in tuple(timeframes or self.config.timeframes):
            period = _period_code(timeframe)
            responses.append(
                self.client.request(
                    "PROTO_OA_SUBSCRIBE_LIVE_TRENDBAR_REQ",
                    {"ctidTraderAccountId": account_id, "period": period, "symbolId": spec.symbol_id},
                )
            )
            self._subscribed.add(_period_name(period))
        return tuple(responses)

    def reconnect(self) -> CTraderStatus:
        self._subscribed.clear()
        self.reset_discontinuity("SESSION_CHANGED")
        status = self.client.connect_with_retry()
        self.reset_generation(status.generation)
        return status

    def reauthenticate_and_resubscribe(
        self,
        *,
        secret_provider: Callable[[str], str],
        token_provider: Callable[[str], str],
        timeframes: Iterable[str] | None = None,
    ) -> CTraderStatus:
        self.client.authenticate(secret_provider=secret_provider, token_provider=token_provider)
        self.subscribe(timeframes=timeframes)
        return self.status

    def reset_generation(self, generation: int) -> None:
        if isinstance(generation, bool) or int(generation) < 0:
            raise CTraderDataError("generation debe ser entero no negativo")
        self._generation = int(generation)
        self._quote_state.clear()
        self._discontinuity_reason = None

    def reset_discontinuity(self, reason: str = "DISCONTINUITY", *, generation: int | None = None) -> None:
        if generation is not None:
            self.reset_generation(generation)
        else:
            self._quote_state.clear()
        reason_text = str(reason).strip() or "DISCONTINUITY"
        self._discontinuity_reason = reason_text

    def snapshot_quote_state(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "generation": self._generation,
            "max_quote_age_seconds": self.max_quote_age_seconds,
            "discontinuity_reason": self._discontinuity_reason,
            "symbols": {
                str(symbol_id): {
                    leg: {key: _iso(value) if isinstance(value, datetime) else value for key, value in leg_data.items()}
                    for leg, leg_data in state.items()
                }
                for symbol_id, state in self._quote_state.items()
            },
        }

    def restore_quote_state(self, snapshot: Mapping[str, Any], *, generation: int | None = None) -> None:
        if not isinstance(snapshot, Mapping):
            raise CTraderDataError("snapshot de quote_state desconocido")
        schema = snapshot.get("schema_version")
        if isinstance(schema, bool) or not isinstance(schema, int) or schema != 1:
            raise CTraderDataError("snapshot de quote_state desconocido")
        raw_generation = snapshot.get("generation", self._generation)
        if isinstance(raw_generation, bool) or not isinstance(raw_generation, int) or raw_generation < 0:
            raise CTraderDataError("snapshot generation inválida")
        target_generation = raw_generation if generation is None else generation
        if isinstance(target_generation, bool) or not isinstance(target_generation, int) or target_generation < 0:
            raise CTraderDataError("generation inválida")
        if target_generation != raw_generation:
            raise CTraderDataError("snapshot de quote_state pertenece a otra generación")
        symbols_raw = snapshot.get("symbols", {})
        if not isinstance(symbols_raw, Mapping):
            raise CTraderDataError("snapshot symbols inválido")
        restored: dict[int, dict[str, dict[str, Any]]] = {}
        for raw_symbol, raw_state in symbols_raw.items():
            symbol_id = _int_or_none(raw_symbol)
            if symbol_id is None:
                raise CTraderDataError("snapshot contiene símbolo inválido")
            restored[symbol_id] = _restore_quote_state_symbol(raw_state)
        reason = snapshot.get("discontinuity_reason")
        if reason is not None and not isinstance(reason, str):
            raise CTraderDataError("snapshot discontinuity_reason inválida")
        self._generation = target_generation
        self._quote_state = restored
        self._discontinuity_reason = reason

    def normalize_spot(
        self,
        payload: Any,
        *,
        received_at: datetime | None = None,
        available_at: datetime | None | object = _DEFAULT_AVAILABILITY,
        snapshot: bool = False,
        sequence: int | str | None = None,
        generation: int | None = None,
        max_quote_age_seconds: float | None = None,
    ) -> CTraderNormalizationResult:
        """Normalize one SpotEvent while preserving both book legs."""
        if generation is not None and int(generation) != self._generation:
            self.reset_generation(int(generation))
        max_age = _validated_quote_age(self.max_quote_age_seconds, max_quote_age_seconds)
        result = normalize_spot_event(
            payload,
            spec=self.spec,
            quote_basis=self.config.quote_basis,
            received_at=received_at,
            available_at=available_at,
            snapshot=snapshot,
            sequence=sequence,
            generation=self._generation,
            max_quote_age_seconds=max_age,
        )
        symbol_id = _int_or_none(read_field(payload, "symbolId", "symbol_id", default=self.spec.symbol_id))
        if symbol_id is None:
            return result
        availability = _availability_time(received_at, available_at)
        event_time = _provider_event_time(payload, received_at, availability)
        if event_time is None:
            return _without_quote(
                result,
                "spot sin timestamp ni received_at; disponibilidad desconocida",
            )
        state = self._quote_state.setdefault(int(symbol_id), {})
        accepted, rejected = _accept_quote_legs(
            state,
            payload,
            event_time=event_time,
            received_at=received_at,
            available_at=availability,
            timestamp_missing=read_field(payload, "timestamp", default=None) is None,
            sequence=sequence,
            generation=self._generation,
            spec=self.spec,
        )
        if not _book_has_both(state):
            return _incomplete_book_result(result, rejected)
        combined = _compose_book_quote(
            state,
            symbol_id=symbol_id,
            event_time=event_time,
            received_at=received_at,
            available_at=availability,
            snapshot=snapshot,
            sequence=sequence,
            generation=self._generation,
            spec=self.spec,
            quote_basis=self.config.quote_basis,
            max_age_seconds=max_age,
        )
        if not combined.quote_events:
            return result
        quality = _book_quality(
            state,
            received_at=received_at,
            available_at=availability,
            max_age_seconds=max_age,
            rejected=rejected,
            discontinuity=self._discontinuity_reason,
        )
        events = _decorate_book_events(
            combined.quote_events,
            state=state,
            quality=quality,
            accepted=accepted,
            rejected=rejected,
            generation=self._generation,
        )
        issues = _book_issues(result, combined, quality, rejected)
        return CTraderNormalizationResult(
            records=tuple(events) + tuple(result.bars),
            quote_events=tuple(events),
            bars=result.bars,
            issues=issues,
            snapshot=bool(snapshot or combined.snapshot),
            symbol_id=combined.symbol_id if combined.symbol_id is not None else symbol_id,
            quote_quality=quality,
        )

    def stream(
        self,
        *,
        duration_seconds: float | None = None,
        max_events: int | None = None,
        timeout_seconds: float = 0.25,
    ) -> Iterator[Event | Bar]:
        start = self._monotonic()
        count = 0
        while max_events is None or count < max_events:
            if duration_seconds is not None and self._monotonic() - start >= duration_seconds:
                break
            message = self.client.poll_event(timeout_seconds)
            if message is None:
                continue
            if message.payload_type_id == PAYLOAD["PROTO_HEARTBEAT_EVENT"]:
                self.client.heartbeat()
                continue
            if message.payload_type_id != PAYLOAD["PROTO_OA_SPOT_EVENT"]:
                continue
            normalized = self.normalize_spot(
                message.payload,
                received_at=message.received_at or self._clock(),
                available_at=message.available_at if message.available_at is not None else _DEFAULT_AVAILABILITY,
                snapshot=False,
                sequence=message.ingest_sequence if message.ingest_sequence is not None else count,
                generation=message.connection_generation
                if message.connection_generation is not None
                else self._generation,
            )
            for record in normalized.records:
                count += 1
                yield record

    iter_events = stream


def _restore_quote_state_leg(raw_leg: Any) -> dict[str, Any]:
    if not isinstance(raw_leg, Mapping):
        raise CTraderDataError("snapshot contiene pierna inválida")
    leg_data = dict(raw_leg)
    for key in ("event_time", "source_time", "received_at", "available_at"):
        value = leg_data.get(key)
        if value is not None:
            leg_data[key] = _parse_iso(value)
    return leg_data


def _restore_quote_state_symbol(raw_state: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(raw_state, Mapping):
        raise CTraderDataError("snapshot contiene símbolo inválido")
    state: dict[str, dict[str, Any]] = {}
    for leg in ("bid", "ask"):
        raw_leg = raw_state.get(leg)
        if raw_leg is not None:
            state[leg] = _restore_quote_state_leg(raw_leg)
    return state


def _validated_quote_age(default: float, override: float | None) -> float:
    value = default if override is None else float(override)
    if not math.isfinite(value) or value < 0:
        raise CTraderDataError("max_quote_age_seconds debe ser finito y no negativo")
    return value


def _provider_event_time(
    payload: Any,
    received_at: datetime | None,
    available_at: datetime | None,
) -> datetime | None:
    raw_timestamp = read_field(payload, "timestamp", default=None)
    if raw_timestamp is None:
        return _received_or_none(received_at) or _received_or_none(available_at)
    return _timestamp_ms(raw_timestamp, unit="ms")


def _accept_quote_legs(
    state: dict[str, dict[str, Any]],
    payload: Any,
    *,
    event_time: datetime,
    received_at: datetime | None,
    available_at: datetime | None,
    timestamp_missing: bool,
    sequence: int | str | None,
    generation: int,
    spec: CTraderInstrumentSpec,
) -> tuple[set[str], list[QuoteQualityReason]]:
    accepted: set[str] = set()
    rejected: list[QuoteQualityReason] = []
    for side in ("bid", "ask"):
        raw_value = read_field(payload, side, default=None)
        if raw_value is None:
            continue
        try:
            price = spec.price_from_relative(raw_value)
        except CTraderDataError:
            rejected.append(QuoteQualityReason.REJECTED_UPDATE)
            continue
        previous = state.get(side)
        previous_time = previous.get("event_time") if previous is not None else None
        if previous_time is not None:
            previous_instant = ensure_utc(previous_time, field_name="previous event_time")
            if event_time < previous_instant:
                rejected.extend((QuoteQualityReason.OUT_OF_ORDER, QuoteQualityReason.REJECTED_UPDATE))
                continue
        state[side] = _book_leg(
            price,
            raw_value,
            event_time=event_time,
            received_at=received_at,
            available_at=available_at,
            timestamp_missing=timestamp_missing,
            sequence=sequence,
            generation=generation,
        )
        accepted.add(side)
    return accepted, rejected


def _book_leg(
    price: float,
    raw_value: Any,
    *,
    event_time: datetime,
    received_at: datetime | None,
    available_at: datetime | None,
    timestamp_missing: bool,
    sequence: int | str | None,
    generation: int,
) -> dict[str, Any]:
    receipt = _received_or_none(received_at)
    return {
        "price": price,
        "raw": int(raw_value),
        "event_time": event_time,
        "source_time": None if timestamp_missing else event_time,
        "received_at": receipt,
        "available_at": _received_or_none(available_at),
        "timestamp_missing": timestamp_missing,
        "sequence": sequence,
        "generation": generation,
    }


def _book_has_both(state: Mapping[str, Mapping[str, Any]]) -> bool:
    return "bid" in state and "ask" in state


def _without_quote(
    result: CTraderNormalizationResult,
    *issues: str,
) -> CTraderNormalizationResult:
    return replace(
        result,
        quote_events=(),
        records=tuple(result.bars),
        issues=tuple(dict.fromkeys((*result.issues, *issues))),
    )


def _incomplete_book_result(
    result: CTraderNormalizationResult,
    rejected: Sequence[QuoteQualityReason],
) -> CTraderNormalizationResult:
    if not rejected:
        return result
    return _without_quote(result, *(_quality_text(item) for item in rejected))


def _compose_book_quote(
    state: Mapping[str, Mapping[str, Any]],
    *,
    symbol_id: int,
    event_time: datetime,
    received_at: datetime | None,
    available_at: datetime | None,
    snapshot: bool,
    sequence: int | str | None,
    generation: int,
    spec: CTraderInstrumentSpec,
    quote_basis: str,
    max_age_seconds: float,
) -> CTraderNormalizationResult:
    payload = {
        "symbolId": symbol_id,
        "timestamp": int(event_time.timestamp() * 1000),
        "bid": state["bid"]["raw"],
        "ask": state["ask"]["raw"],
        "snapshot": bool(snapshot),
    }
    return normalize_spot_event(
        payload,
        spec=spec,
        quote_basis=quote_basis,
        received_at=received_at,
        available_at=available_at,
        snapshot=snapshot,
        sequence=sequence,
        generation=generation,
        max_quote_age_seconds=max_age_seconds,
    )


def _decorate_book_events(
    events: Sequence[Event],
    *,
    state: Mapping[str, Mapping[str, Any]],
    quality: QuoteQuality,
    accepted: set[str],
    rejected: Sequence[QuoteQualityReason],
    generation: int,
) -> list[Event]:
    result: list[Event] = []
    for event in events:
        metadata = {
            **dict(event.metadata or {}),
            **quality.metadata(),
            "bid_source_timestamp": _iso(state["bid"].get("source_time")),
            "ask_source_timestamp": _iso(state["ask"].get("source_time")),
            "bid_received_at": _iso(state["bid"].get("received_at")),
            "ask_received_at": _iso(state["ask"].get("received_at")),
            "bid_available_at": _iso(state["bid"].get("available_at")),
            "ask_available_at": _iso(state["ask"].get("available_at")),
            "bid_source_timestamp_missing": bool(state["bid"].get("timestamp_missing")),
            "ask_source_timestamp_missing": bool(state["ask"].get("timestamp_missing")),
            "partial_update": accepted != {"bid", "ask"},
            "quote_state_retained": True,
            "connection_generation": generation,
        }
        if rejected:
            metadata["rejected_update_reasons"] = [item.value for item in _unique_reasons(rejected)]
        result.append(replace(event, metadata=metadata))
    return result


def _book_issues(
    original: CTraderNormalizationResult,
    combined: CTraderNormalizationResult,
    quality: QuoteQuality,
    rejected: Sequence[QuoteQualityReason],
) -> tuple[str, ...]:
    return tuple(
        dict.fromkeys(
            (
                *(issue for issue in (*original.issues, *combined.issues) if not issue.startswith("quote_basis=")),
                *(_quality_text(item) for item in rejected),
                *(_quality_text(item) for item in quality.reasons),
            )
        )
    )


def normalize_trendbar(
    raw: Any,
    *,
    spec: CTraderInstrumentSpec,
    received_at: datetime | None = None,
    available_at: datetime | None | object = _DEFAULT_AVAILABILITY,
    request_id: str = "",
    mode: str = "REPLAY",
    revision: int = 0,
    availability_policy: str | None = None,
) -> Bar:
    """Transform relative OHLC into a native cTrader trendbar.

    A missing receipt is preserved as unknown availability; it never becomes
    ``datetime.now()`` or zero latency.  Trendbars are native provider
    series and are never relabeled as traded ticks.
    """

    low_raw = read_field(raw, "low", default=None)
    open_delta = read_field(raw, "deltaOpen", "delta_open", default=None)
    close_delta = read_field(raw, "deltaClose", "delta_close", default=None)
    high_delta = read_field(raw, "deltaHigh", "delta_high", default=None)
    timestamp_minutes = read_field(raw, "utcTimestampInMinutes", "utc_timestamp_in_minutes", default=None)
    period_raw = read_field(raw, "period", default=None)
    volume_raw = read_field(raw, "volume", default=None)
    if None in {low_raw, open_delta, close_delta, high_delta, timestamp_minutes, volume_raw}:
        raise CTraderDataError("trendbar incompleto: low/deltas/timestamp/volume son necesarios")
    try:
        low = spec.price_from_relative(low_raw)
        open_price = spec.price_from_relative(int(low_raw) + int(open_delta))
        close_price = spec.price_from_relative(int(low_raw) + int(close_delta))
        high_price = spec.price_from_relative(int(low_raw) + int(high_delta))
        start = datetime.fromtimestamp(int(timestamp_minutes) * 60, UTC)
        period_code = _period_code(period_raw)
        timeframe = _period_name(period_code)
        seconds = resolution_to_seconds(timeframe)
        protocol_volume = int(volume_raw)
    except (TypeError, ValueError, OverflowError) as exc:
        raise CTraderDataError(f"trendbar inválido: {exc}") from exc
    if (
        protocol_volume < 0
        or high_price < max(open_price, close_price, low)
        or low > min(open_price, close_price, high_price)
    ):
        raise CTraderDataError("trendbar con OHLC/volumen incoherente")
    end = start + timedelta(seconds=seconds)
    received = ensure_utc(received_at, field_name="received_at") if received_at is not None else None
    available = _availability_time(received, available_at)
    closed = bool(available is not None and available >= end)
    token = f"ctrader|{spec.symbol_id}|{timeframe}|{int(timestamp_minutes)}|r{int(revision)}"
    metadata = {
        "provider": "ctrader",
        "mode": mode,
        "relative_scale": spec.price_scale,
        "period_code": period_code,
        "request_id": request_id,
        "price_basis_note": "trendbar nativa del proveedor; no se infiere bid/ask/mid",
        "protocol_volume": protocol_volume,
        "no_tick_interpolation": True,
        "closed_evidence": "received_at>=interval_end" if closed else "not_observed",
        "availability_policy": availability_policy
        or ("OBSERVED_RECEIPT" if received is not None else "UNKNOWN_NO_RECEIPT"),
        "availability_unknown": received is None,
        "native_price_basis": True,
    }
    return Bar(
        instrument=spec.symbol,
        interval_start=start,
        interval_end=end,
        open=open_price,
        high=high_price,
        low=low,
        close=close_price,
        resolution_seconds=seconds,
        volume=None,
        trade_count=None,
        price_basis="native",
        source="ctrader-open-api",
        source_record_id="ctrader-bar-" + hashlib.sha256(token.encode()).hexdigest()[:32],
        received_at=received,
        available_at=available,
        closed=closed,
        synthetic=False,
        revision=int(revision),
        metadata=metadata,
    )


def normalize_spot_event(
    payload: Any,
    *,
    spec: CTraderInstrumentSpec,
    quote_basis: str = "mid",
    received_at: datetime | None = None,
    available_at: datetime | None | object = _DEFAULT_AVAILABILITY,
    snapshot: bool = False,
    sequence: int | str | None = None,
    generation: int = 0,
    max_quote_age_seconds: float | None = 90.0,
    timestamp_unit: str = "ms",
) -> CTraderNormalizationResult:
    """Normalize one SpotEvent without keeping mutable book state."""
    received = _received_or_none(received_at)
    available = _availability_time(received, available_at)
    snapshot = bool(snapshot or read_field(payload, "snapshot", "isSnapshot", "is_snapshot", default=False))
    symbol_id = _int_or_none(read_field(payload, "symbolId", "symbol_id", default=spec.symbol_id))
    _validate_symbol_id(spec, symbol_id)
    event_time, raw_timestamp, timestamp_missing = _spot_time(
        payload,
        received=received,
        available=available,
        timestamp_unit=timestamp_unit,
    )
    bid_raw, ask_raw, bid, ask = _spot_prices(payload, spec)
    basis = _quote_basis(quote_basis)
    issues, reasons = _spot_quality_inputs(
        bid,
        ask,
        timestamp_missing=timestamp_missing,
        basis=basis,
    )
    selected = _select_quote_price(bid, ask, basis, digits=spec.digits)
    quality = _stateless_quality(
        bid,
        ask,
        event_time=event_time,
        received_at=received,
        available_at=available,
        timestamp_missing=timestamp_missing,
        max_age_seconds=max_quote_age_seconds,
        reasons=reasons,
        generation=generation,
    )
    issues.extend(_quality_issues(quality))
    event = _build_spot_event(
        payload,
        spec=spec,
        symbol_id=symbol_id,
        event_time=event_time,
        available=available,
        received=received,
        raw_timestamp=raw_timestamp,
        timestamp_missing=timestamp_missing,
        bid_raw=bid_raw,
        ask_raw=ask_raw,
        bid=bid,
        ask=ask,
        selected=selected,
        basis=basis,
        snapshot=snapshot,
        sequence=sequence,
        generation=generation,
        quality=quality,
    )
    bars, bar_issues = _normalize_spot_bars(
        payload,
        spec=spec,
        received=received,
        available=available,
        sequence=sequence,
    )
    issues.extend(bar_issues)
    quotes = (event,) if event is not None else ()
    return CTraderNormalizationResult(
        quotes + tuple(bars),
        quotes,
        tuple(bars),
        tuple(dict.fromkeys(issues)),
        bool(snapshot),
        symbol_id,
        quality,
    )


def _validate_symbol_id(spec: CTraderInstrumentSpec, symbol_id: int | None) -> None:
    if spec.symbol_id is not None and symbol_id is not None and symbol_id != spec.symbol_id:
        raise CTraderDataError(f"spot event symbolId {symbol_id} no coincide con {spec.symbol_id}")


def _spot_time(
    payload: Any,
    *,
    received: datetime | None,
    available: datetime | None,
    timestamp_unit: str,
) -> tuple[datetime, Any, bool]:
    raw_timestamp = read_field(payload, "timestamp", default=None)
    declared_unit = read_field(payload, "timestamp_unit", "timestampUnit", default=timestamp_unit)
    if str(declared_unit).strip().lower() not in {"ms", "millisecond", "milliseconds"}:
        raise CTraderDataError("SpotEvent timestamp debe declararse en milisegundos (ms)")
    if raw_timestamp is None:
        event_time = received or available
        if event_time is None:
            raise CTraderDataError("SpotEvent sin timestamp ms y sin received_at; disponibilidad desconocida")
        return event_time, raw_timestamp, True
    return _timestamp_ms(raw_timestamp, unit=declared_unit), raw_timestamp, False


def _spot_prices(
    payload: Any,
    spec: CTraderInstrumentSpec,
) -> tuple[Any, Any, float | None, float | None]:
    bid_raw = read_field(payload, "bid", default=None)
    ask_raw = read_field(payload, "ask", default=None)
    bid = spec.price_from_relative(bid_raw) if bid_raw is not None else None
    ask = spec.price_from_relative(ask_raw) if ask_raw is not None else None
    return bid_raw, ask_raw, bid, ask


def _quote_basis(value: str) -> QuoteBasis:
    basis = str(value).strip().lower()
    if basis == "mid":
        return "mid"
    if basis == "bid":
        return "bid"
    if basis == "ask":
        return "ask"
    raise CTraderDataError(f"quote_basis no soportado: {value!r}")


def _spot_quality_inputs(
    bid: float | None,
    ask: float | None,
    *,
    timestamp_missing: bool,
    basis: str,
) -> tuple[list[str], list[QuoteQualityReason]]:
    issues: list[str] = []
    reasons: list[QuoteQualityReason] = []
    if bid is None:
        reasons.append(QuoteQualityReason.MISSING_BID)
    if ask is None:
        reasons.append(QuoteQualityReason.MISSING_ASK)
    if timestamp_missing and (bid is not None or ask is not None):
        issues.append("source timestamp ausente; received_at no demuestra frescura de mercado")
        reasons.append(QuoteQualityReason.MISSING_SOURCE_TIMESTAMP)
    if bid is not None and ask is not None and bid >= ask:
        issues.append("quote cruzado; bid debe ser menor que ask")
        reasons.append(QuoteQualityReason.CROSSED)
    if (
        (basis == "mid" and (bid is None or ask is None))
        or (basis == "bid" and bid is None)
        or (basis == "ask" and ask is None)
    ):
        issues.append(f"quote_basis={basis} no evaluable con bid/ask opcionales presentes")
    return issues, reasons


def _select_quote_price(
    bid: float | None,
    ask: float | None,
    basis: QuoteBasis,
    *,
    digits: int,
) -> float | None:
    if basis == "mid" and bid is not None and ask is not None:
        return round((bid + ask) / 2.0, digits)
    if basis == "bid":
        return bid
    if basis == "ask":
        return ask
    return None


def _quality_issues(quality: QuoteQuality) -> list[str]:
    return [
        _quality_text(reason)
        for reason in quality.reasons
        if reason in {QuoteQualityReason.STALE_BID, QuoteQualityReason.STALE_ASK}
    ]


def _build_spot_event(
    payload: Any,
    *,
    spec: CTraderInstrumentSpec,
    symbol_id: int | None,
    event_time: datetime,
    available: datetime | None,
    received: datetime | None,
    raw_timestamp: Any,
    timestamp_missing: bool,
    bid_raw: Any,
    ask_raw: Any,
    bid: float | None,
    ask: float | None,
    selected: float | None,
    basis: QuoteBasis,
    snapshot: bool,
    sequence: int | str | None,
    generation: int,
    quality: QuoteQuality,
) -> Event | None:
    if selected is None:
        return None
    event_id = (
        "ctrader-spot-"
        + stable_hash(
            {
                "schema": "ctrader-spot-v2",
                "symbol_id": symbol_id,
                "timestamp_ms": raw_timestamp,
                "bid": bid_raw,
                "ask": ask_raw,
                "sequence": sequence,
                "snapshot": snapshot,
                "generation": generation,
            }
        )[:32]
    )
    metadata = {
        "provider": "ctrader",
        "symbol_id": symbol_id,
        "raw_timestamp_ms": raw_timestamp,
        "timestamp_unit": "ms",
        "source_timestamp_missing": timestamp_missing,
        "quote_basis": basis,
        "bid_relative": bid_raw,
        "ask_relative": ask_raw,
        "session_close_relative": read_field(payload, "sessionClose", "session_close", default=None),
        "quality": "PUBLIC_PROVIDER",
        "connection_generation": generation,
        "availability_unknown": available is None,
        "available_at_policy": "OBSERVED_RECEIPT" if available is not None else "UNKNOWN_NO_RECEIPT",
        **quality.metadata(),
    }
    return Event(
        instrument=spec.symbol,
        event_time=event_time,
        price=selected,
        bid=bid,
        ask=ask,
        mid=selected if basis == "mid" else None,
        price_basis=basis,
        received_at=received,
        available_at=available,
        source="ctrader-open-api",
        source_event_id=event_id,
        source_sequence=sequence,
        is_snapshot=bool(snapshot),
        synthetic=False,
        metadata=metadata,
    )


def _normalize_spot_bars(
    payload: Any,
    *,
    spec: CTraderInstrumentSpec,
    received: datetime | None,
    available: datetime | None,
    sequence: int | str | None,
) -> tuple[list[Bar], list[str]]:
    bars: list[Bar] = []
    issues: list[str] = []
    for index, raw_bar in enumerate(read_repeated(payload, "trendbar", "trendbars")):
        try:
            bars.append(
                normalize_trendbar(
                    raw_bar,
                    spec=spec,
                    received_at=received,
                    available_at=available,
                    request_id=f"spot-{sequence if sequence is not None else index}",
                    mode="LIVE",
                )
            )
        except CTraderDataError as exc:
            issues.append(str(exc))
    return bars, issues


def _book_quality(
    state: Mapping[str, Mapping[str, Any]],
    *,
    received_at: datetime | None,
    available_at: datetime | None = None,
    max_age_seconds: float,
    rejected: Sequence[QuoteQualityReason] = (),
    discontinuity: str | None = None,
) -> QuoteQuality:
    bid = _leg_quality(
        state.get("bid"),
        received_at=received_at,
        available_at=available_at,
        max_age_seconds=max_age_seconds,
        side="bid",
    )
    ask = _leg_quality(
        state.get("ask"),
        received_at=received_at,
        available_at=available_at,
        max_age_seconds=max_age_seconds,
        side="ask",
    )
    reasons = list(_unique_reasons((*bid.reasons, *ask.reasons, *rejected)))
    if discontinuity:
        reasons.append(QuoteQualityReason.DISCONNECTED)
    if bid.price is None or ask.price is None:
        overall = QuoteQualityState.INCOMPLETE
    elif QuoteQualityReason.CROSSED in reasons or bid.price >= ask.price:
        if QuoteQualityReason.CROSSED not in reasons:
            reasons.append(QuoteQualityReason.CROSSED)
        overall = QuoteQualityState.INVALID
    elif any(
        reason in reasons
        for reason in (QuoteQualityReason.MISSING_SOURCE_TIMESTAMP, QuoteQualityReason.AVAILABILITY_UNKNOWN)
    ):
        overall = QuoteQualityState.UNKNOWN
    elif any(reason in reasons for reason in (QuoteQualityReason.STALE_BID, QuoteQualityReason.STALE_ASK)):
        overall = QuoteQualityState.STALE
    elif rejected:
        overall = QuoteQualityState.UNKNOWN
    else:
        overall = QuoteQualityState.VALID
    return QuoteQuality(overall, tuple(_unique_reasons(reasons)), bid, ask, max_age_seconds)


def _stateless_quality(
    bid: float | None,
    ask: float | None,
    *,
    event_time: datetime,
    received_at: datetime | None,
    available_at: datetime | None,
    timestamp_missing: bool,
    max_age_seconds: float | None,
    reasons: Sequence[QuoteQualityReason],
    generation: int,
) -> QuoteQuality:
    state: dict[str, dict[str, Any]] = {}
    if bid is not None:
        state["bid"] = {
            "price": bid,
            "event_time": event_time,
            "source_time": None if timestamp_missing else event_time,
            "received_at": received_at,
            "available_at": available_at,
            "timestamp_missing": timestamp_missing,
            "generation": generation,
        }
    if ask is not None:
        state["ask"] = {
            "price": ask,
            "event_time": event_time,
            "received_at": received_at,
            "available_at": available_at,
            "timestamp_missing": timestamp_missing,
            "generation": generation,
        }
    return _book_quality(
        state,
        received_at=received_at,
        available_at=available_at,
        max_age_seconds=float(max_age_seconds) if max_age_seconds is not None else float("inf"),
        rejected=reasons,
    )


def _leg_quality(
    leg_data: Mapping[str, Any] | None,
    *,
    received_at: datetime | None,
    available_at: datetime | None,
    max_age_seconds: float,
    side: str,
) -> QuoteLegQuality:
    if leg_data is None:
        reason = QuoteQualityReason.MISSING_BID if side == "bid" else QuoteQualityReason.MISSING_ASK
        return QuoteLegQuality(None, None, None, None, False, None, QuoteQualityState.INCOMPLETE, (reason,))
    source_time = leg_data.get("source_time", leg_data.get("event_time"))
    receipt = leg_data.get("received_at")
    available = leg_data.get("available_at")
    timestamp_missing = bool(leg_data.get("timestamp_missing", False))
    reasons: list[QuoteQualityReason] = []
    age: float | None = None
    if timestamp_missing:
        reasons.append(QuoteQualityReason.MISSING_SOURCE_TIMESTAMP)
    if available_at is None:
        reasons.append(QuoteQualityReason.AVAILABILITY_UNKNOWN)
    elif source_time is not None and not timestamp_missing:
        age = max(
            0.0,
            (
                ensure_utc(available_at, field_name="available_at") - ensure_utc(source_time, field_name="event_time")
            ).total_seconds(),
        )
        stale_reason = QuoteQualityReason.STALE_BID if side == "bid" else QuoteQualityReason.STALE_ASK
        if age > max_age_seconds:
            reasons.append(stale_reason)
    if receipt is None:
        reasons.append(QuoteQualityReason.AVAILABILITY_UNKNOWN)
    state = (
        QuoteQualityState.UNKNOWN
        if QuoteQualityReason.AVAILABILITY_UNKNOWN in reasons or QuoteQualityReason.MISSING_SOURCE_TIMESTAMP in reasons
        else QuoteQualityState.STALE
        if any(reason in reasons for reason in (QuoteQualityReason.STALE_BID, QuoteQualityReason.STALE_ASK))
        else QuoteQualityState.VALID
    )
    raw_price = leg_data.get("price")
    if raw_price is None:
        raise CTraderDataError(f"pierna {side} sin precio observado")
    try:
        price = float(raw_price)
    except (TypeError, ValueError) as exc:
        raise CTraderDataError(f"precio de pierna {side} inválido") from exc
    if not math.isfinite(price) or price <= 0:
        raise CTraderDataError(f"precio de pierna {side} inválido")
    return QuoteLegQuality(
        price,
        source_time,
        receipt,
        available,
        timestamp_missing,
        age,
        state,
        tuple(_unique_reasons(reasons)),
        int(leg_data.get("generation", 0)),
    )


def _quality_text(reason: QuoteQualityReason) -> str:
    return f"quote_quality:{reason.value}"


def _unique_reasons(reasons: Iterable[QuoteQualityReason]) -> tuple[QuoteQualityReason, ...]:
    result: list[QuoteQualityReason] = []
    for reason in reasons:
        if reason not in result:
            result.append(reason)
    return tuple(result)


def _received_or_none(value: datetime | None) -> datetime | None:
    return ensure_utc(value, field_name="received_at") if value is not None else None


def _availability_time(
    received_at: datetime | None,
    available_at: datetime | None | object,
) -> datetime | None:
    received = _received_or_none(received_at)
    if available_at is _DEFAULT_AVAILABILITY:
        return received
    if available_at is None:
        return None
    if not isinstance(available_at, datetime):
        raise CTraderDataError("available_at debe ser datetime o None")
    return _received_or_none(available_at)


def _timestamp_ms(value: Any, *, unit: str = "ms") -> datetime:
    if isinstance(value, datetime):
        return ensure_utc(value, field_name="timestamp")
    if str(unit).strip().lower() not in {"ms", "millisecond", "milliseconds"}:
        raise CTraderDataError("SpotEvent timestamp debe declararse en milisegundos (ms)")
    if isinstance(value, bool):
        raise CTraderDataError(f"timestamp ms inválido: {value!r}")
    try:
        number = int(value)
        return datetime.fromtimestamp(number / 1_000.0, UTC)
    except (TypeError, ValueError, OverflowError) as exc:
        raise CTraderDataError(f"timestamp ms inválido: {value!r}") from exc


def _unix_ms(value: datetime) -> int:
    return int(ensure_utc(value, field_name="timestamp").timestamp() * 1_000)


def _period_code(value: Any) -> int:
    if isinstance(value, bool):
        raise CTraderConfigurationError("period inválido")
    if isinstance(value, int):
        if value in TREND_PERIOD_NAMES:
            return value
        raise CTraderConfigurationError(f"period code no soportado: {value}")
    text = str(value).strip().upper()
    if text in TREND_PERIODS:
        return TREND_PERIODS[text]
    raise CTraderConfigurationError(f"period no soportado: {value!r}")


def _period_name(value: Any) -> str:
    return TREND_PERIOD_NAMES[_period_code(value)]


def _as_sequence(value: Any) -> tuple[Any, ...]:
    if value is None:
        return ()
    if isinstance(value, (str, bytes, Mapping)):
        return (value,)
    try:
        return tuple(value)
    except TypeError:
        return (value,)


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return {str(key): jsonable(item) for key, item in value.items()}
    return message_to_mapping(value)


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _bool_or_none(value: Any) -> bool | None:
    return None if value is None else bool(value)


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z") if value else None


def _parse_iso(value: Any) -> datetime:
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    return ensure_utc(datetime.fromisoformat(text), field_name="snapshot timestamp")


def time_monotonic() -> float:
    import time

    return time.monotonic()


__all__ = [
    "CTraderFetchResult",
    "CTraderHistoryResult",
    "CTraderInstrumentSpec",
    "CTraderMarketCalendar",
    "CTraderNormalizationResult",
    "CTraderProvider",
    "CTraderSessionWindow",
    "CTraderSymbol",
    "QuoteLegQuality",
    "QuoteQuality",
    "QuoteQualityReason",
    "QuoteQualityState",
    "SymbolCatalog",
    "normalize_account_payload",
    "normalize_spot_event",
    "normalize_symbol_name",
    "normalize_trendbar",
]
