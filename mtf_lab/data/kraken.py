"""Public Kraken Spot adapters (REST OHLC and WebSocket v2 trades/OHLC).

Only public market-data endpoints are used.  This module has no credential or
order code by design.  REST is used for an explicit finite historical snapshot;
WebSocket v2 is used for ongoing public observations.  A reconnect changes the
stream status to ``needs_reconciliation`` because a reconnect alone does not
prove that events during the gap were recovered.

The wire mapping follows Kraken's current public documentation:

* REST ``/0/public/OHLC`` returns up to 720 rows and always includes a final
  not-yet-committed row, which is excluded by ``fetch_ohlc`` unless
  ``include_open=True``.
* WebSocket v2 uses slash-separated symbols (``BTC/USD``), RFC3339 timestamps,
  and a ``data`` array for both trade snapshots and updates.
* A trade message can contain multiple trades; each trade is normalized
  independently and keyed by ``trade_id`` when present.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import threading
import time
from collections import OrderedDict
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, cast, overload
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .models import (
    Bar,
    DataQuality,
    DataSet,
    Event,
    Provenance,
    ValidationIssue,
    ensure_utc,
    infer_quality,
    resolution_name,
)

_SUPPORTED_INTERVALS = {1, 5, 15, 30, 60, 240, 1440, 10080, 21600}


class _WebSocketConnector(Protocol):
    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...


async def _close_async_iterator(iterator: AsyncIterator[Any]) -> None:
    close = getattr(iterator, "aclose", None)
    if callable(close):
        await cast(Callable[[], Awaitable[None]], close)()


class KrakenError(RuntimeError):
    """Base exception for explicit public API failures."""


class KrakenConfigurationError(KrakenError, ValueError):
    pass


class KrakenTransportError(KrakenError):
    """DNS/TLS/timeout/HTTP or WebSocket transport failure."""


class KrakenAPIError(KrakenError):
    """Kraken returned an error in the response body or subscription ack."""


@dataclass(frozen=True, slots=True)
class KrakenFetchResult:
    """REST OHLC result; sequence-like over closed bars.

    ``open_bar`` exposes the provider's final in-progress row separately.  It
    is never mixed into ``bars`` unless ``include_open=True`` was requested.
    """

    bars: tuple[Bar, ...]
    open_bar: Bar | None
    provenance: Provenance
    quality: DataQuality
    last_timestamp: datetime | None
    request_uri: str
    raw_response_hash: str
    issues: tuple[ValidationIssue, ...] = ()

    def __iter__(self) -> Iterator[Bar]:
        return iter(self.bars)

    def __len__(self) -> int:
        return len(self.bars)

    @overload
    def __getitem__(self, item: int) -> Bar: ...

    @overload
    def __getitem__(self, item: slice) -> tuple[Bar, ...]: ...

    def __getitem__(self, item: int | slice) -> Bar | tuple[Bar, ...]:
        return self.bars[item]

    @property
    def dataset(self) -> DataSet:
        records: tuple[Bar, ...] = self.bars + ((self.open_bar,) if self.open_bar is not None else ())
        return DataSet(records=records, provenance=self.provenance, quality=self.quality, issues=self.issues)


@dataclass(slots=True)
class KrakenStreamStatus:
    """Observable connection/continuity state for a public stream."""

    state: str = "DISCONNECTED"
    connected_at: datetime | None = None
    last_message_at: datetime | None = None
    last_event_at: datetime | None = None
    last_error: str | None = None
    reconnect_count: int = 0
    events_received: int = 0
    duplicate_events: int = 0
    snapshot_seen: bool = False
    discontinuity: bool = False
    needs_reconciliation: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "connected_at": _iso(self.connected_at),
            "last_message_at": _iso(self.last_message_at),
            "last_event_at": _iso(self.last_event_at),
            "last_error": self.last_error,
            "reconnect_count": self.reconnect_count,
            "events_received": self.events_received,
            "duplicate_events": self.duplicate_events,
            "snapshot_seen": self.snapshot_seen,
            "discontinuity": self.discontinuity,
            "needs_reconciliation": self.needs_reconciliation,
        }


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z") if value else None


def _parse_rfc3339(value: Any, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise KrakenAPIError(f"{field_name} must be an RFC3339 string")
    text = value.strip()
    if text.endswith("Z") or text.endswith("z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
        return ensure_utc(parsed, field_name=field_name)
    except (TypeError, ValueError) as exc:
        raise KrakenAPIError(f"invalid {field_name}: {value!r}") from exc


def _trade_count(value: Any, field_name: str) -> int:
    numeric = _number(value, field_name)
    if int(numeric) != numeric or numeric < 0:
        raise KrakenAPIError(f"{field_name} must be a non-negative integer")
    return int(numeric)


def _number(value: Any, field_name: str, *, positive: bool = False) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise KrakenAPIError(f"{field_name} must be numeric") from exc
    if not (result == result) or result in {float("inf"), float("-inf")}:
        raise KrakenAPIError(f"{field_name} must be finite")
    if positive and result <= 0:
        raise KrakenAPIError(f"{field_name} must be positive")
    return result


def _rest_pair(pair: str) -> str:
    canonical = normalize_pair(pair)
    return canonical


def normalize_pair(pair: str) -> str:
    """Normalize display pair for Kraken v2 while preserving explicit aliasing.

    Kraken's v2 display symbol is slash-separated and uses ``BTC/USD``.  The
    legacy ``XBT/USD`` spelling is accepted as an input convenience and is
    recorded as an alias in adapter provenance; no other pair is rewritten.
    """

    text = str(pair).strip().upper().replace("-", "/")
    # REST sin assetVersion=1 puede devolver claves internas como XXBTZUSD;
    # aceptarlas aquí hace explícita la conciliación sin confundirlas con un
    # símbolo de WebSocket. Sólo se normalizan alias conocidos.
    internal_aliases = {
        "XXBTZUSD": "BTC/USD",
        "XBTZUSD": "BTC/USD",
        "XXBT/USD": "BTC/USD",
        "XXBTZUSDT": "BTC/USDT",
        "XBTZUSDT": "BTC/USDT",
    }
    if "/" not in text and text in internal_aliases:
        text = internal_aliases[text]
    if "/" not in text:
        raise KrakenConfigurationError("pair must use slash format or a known Kraken internal alias, e.g. BTC/USD")
    base, quote = (part.strip() for part in text.split("/", 1))
    if not base or not quote or "/" in quote:
        raise KrakenConfigurationError(f"invalid pair: {pair!r}")
    if base == "XBT":
        base = "BTC"
    return f"{base}/{quote}"


class KrakenPublicAdapter:
    """Public, unauthenticated Kraken Spot market-data adapter."""

    rest_endpoint = "https://api.kraken.com/0/public/OHLC"
    websocket_endpoint = "wss://ws.kraken.com/v2"

    def __init__(
        self,
        pair: str = "BTC/USD",
        *,
        rest_endpoint: str | None = None,
        websocket_endpoint: str | None = None,
        timeout_seconds: float = 15.0,
        max_reconnects: int = 3,
        reconnect_backoff_seconds: float = 5.0,
        rest_min_interval_seconds: float = 1.0,
        heartbeat_seconds: float = 20.0,
        user_agent: str = "mtf-lab-public-data/0.1",
    ) -> None:
        self.requested_pair = str(pair).strip().upper()
        self.pair = normalize_pair(pair)
        self.rest_endpoint = rest_endpoint or self.rest_endpoint
        self.websocket_endpoint = websocket_endpoint or self.websocket_endpoint
        if timeout_seconds <= 0:
            raise KrakenConfigurationError("timeout_seconds must be positive")
        if max_reconnects < 0:
            raise KrakenConfigurationError("max_reconnects must not be negative")
        if reconnect_backoff_seconds < 0:
            raise KrakenConfigurationError("reconnect_backoff_seconds must not be negative")
        if rest_min_interval_seconds < 0:
            raise KrakenConfigurationError("rest_min_interval_seconds must not be negative")
        if heartbeat_seconds <= 0:
            raise KrakenConfigurationError("heartbeat_seconds must be positive")
        self.timeout_seconds = timeout_seconds
        self.max_reconnects = max_reconnects
        self.reconnect_backoff_seconds = reconnect_backoff_seconds
        self.rest_min_interval_seconds = rest_min_interval_seconds
        self.heartbeat_seconds = heartbeat_seconds
        self.user_agent = user_agent
        self.status = KrakenStreamStatus()
        self._last_rest_request = 0.0
        self._request_lock = threading.Lock()
        # Ventana acotada para deduplicación; no acumula todo el histórico en
        # memoria durante un ``watch`` continuo.
        self._seen_trade_ids: OrderedDict[str, None] = OrderedDict()
        self._seen_trade_id_limit = 100_000

    @property
    def name(self) -> str:
        return "kraken_public"

    @property
    def instrument(self) -> str:
        return self.pair

    def fetch(self, interval: int = 1, **kwargs: Any) -> KrakenFetchResult:
        """Alias del contrato intercambiable para la instantánea OHLC."""
        return self.fetch_ohlc(interval=interval, **kwargs)

    def stream(self, **kwargs: Any) -> Iterator[Event]:
        """Alias del contrato intercambiable para el canal público trade."""
        return self.iter_trades(**kwargs)

    @property
    def ws_symbol(self) -> str:
        return self.pair

    @property
    def rest_symbol(self) -> str:
        # assetVersion=1 requests display names and avoids internal XXBTZUSD
        # keys; the parser still accepts an internal key in a response.
        return self.pair

    @staticmethod
    def _validate_fetch_options(interval: int, mode: str) -> None:
        if interval not in _SUPPORTED_INTERVALS:
            raise KrakenConfigurationError(
                f"unsupported Kraken OHLC interval: {interval}; allowed={sorted(_SUPPORTED_INTERVALS)}"
            )
        if mode not in {"OBSERVACIÓN EN DIRECTO", "REPLAY", "IMPORT"}:
            raise KrakenConfigurationError("mode must be OBSERVACIÓN EN DIRECTO, REPLAY or IMPORT")

    def _rest_request_uri(self, interval: int, since: int | datetime | None) -> str:
        params: dict[str, str | int] = {"pair": self.rest_symbol, "assetVersion": 1, "interval": interval}
        if since is not None:
            if isinstance(since, datetime):
                since_dt = ensure_utc(since, field_name="since")
                params["since"] = int(since_dt.timestamp())
            else:
                if isinstance(since, bool):
                    raise KrakenConfigurationError("since must be Unix seconds or datetime")
                params["since"] = int(since)
        return f"{self.rest_endpoint}?{urlencode(params)}"

    @staticmethod
    def _limit_rest_rows(rows: list[Any]) -> tuple[list[Any], bool]:
        truncated = len(rows) > 720
        return (rows[-720:] if truncated else rows), truncated

    def _parse_rest_bars(
        self,
        rows: list[Any],
        *,
        interval: int,
        pair_key: str,
        observed_at: datetime,
    ) -> list[Bar]:
        bars: list[Bar] = []
        for row_index, row in enumerate(rows):
            if not isinstance(row, (list, tuple)) or len(row) < 8:
                size = len(row) if isinstance(row, (list, tuple)) else "non-array"
                raise KrakenAPIError(f"Kraken OHLC row {row_index} has {size} fields; expected >=8")
            try:
                timestamp = datetime.fromtimestamp(int(row[0]), tz=UTC)
            except (TypeError, ValueError, OverflowError) as exc:
                raise KrakenAPIError(f"invalid OHLC timestamp at row {row_index}: {row[0]!r}") from exc
            bars.append(
                Bar(
                    instrument=self.pair,
                    interval_start=timestamp,
                    interval_end=timestamp + timedelta(minutes=interval),
                    open=_number(row[1], "open", positive=True),
                    high=_number(row[2], "high", positive=True),
                    low=_number(row[3], "low", positive=True),
                    close=_number(row[4], "close", positive=True),
                    resolution_seconds=interval * 60,
                    volume=_number(row[6], "volume") if row[6] is not None else None,
                    trade_count=_trade_count(row[7], f"trade_count row {row_index}"),
                    price_basis="traded",
                    source="kraken-rest",
                    source_record_id=f"kraken-rest:{pair_key}:{int(row[0])}",
                    received_at=observed_at,
                    available_at=observed_at,
                    closed=row_index < len(rows) - 1,
                    synthetic=False,
                    metadata={
                        "provider_pair_key": pair_key,
                        # Kraken puede devolver VWAP=0 en una vela sin
                        # operaciones; eso es falta de actividad, no un precio.
                        "vwap": _number(row[5], "vwap"),
                        "provider_row_index": row_index,
                        "provider_last_is_open": row_index == len(rows) - 1,
                    },
                )
            )
        return bars

    @staticmethod
    def _rest_last_timestamp(result: Mapping[str, Any], bars: list[Bar]) -> datetime | None:
        if "last" not in result:
            return bars[-1].interval_start if bars else None
        try:
            return datetime.fromtimestamp(int(result["last"]), tz=UTC)
        except (TypeError, ValueError, OverflowError) as exc:
            raise KrakenAPIError(f"invalid Kraken result.last: {result['last']!r}") from exc

    def _fetch_result(
        self,
        *,
        bars: list[Bar],
        include_open: bool,
        interval: int,
        mode: str,
        request_uri: str,
        raw_hash: str,
        observed_at: datetime,
        truncated: bool,
        last_timestamp: datetime | None,
    ) -> KrakenFetchResult:
        open_bar = bars[-1] if bars else None
        closed_bars = tuple(bars[:-1]) if bars else ()
        returned_bars = tuple(bars) if include_open else closed_bars
        notes = [
            "Kraken REST público sin autenticación",
            "la última fila del endpoint es la vela no comprometida; se separó de las cerradas",
            "Kraken limita la respuesta a 720 entradas recientes",
            "la base de precio es traded/último precio; no se derivaron bid/ask/mid",
        ]
        if truncated:
            notes.append("respuesta recortada a las 720 entradas más recientes según el límite REST")
        if self.requested_pair != self.pair:
            notes.append(f"alias de par normalizado: {self.requested_pair} -> {self.pair}")
        provenance = Provenance(
            provider="kraken",
            mode=mode,  # type: ignore[arg-type]
            instrument=self.pair,
            resolutions=(resolution_name(interval),),
            price_basis="traded",
            source_uri=request_uri,
            source_hash=raw_hash,
            coverage_start=returned_bars[0].interval_start if returned_bars else None,
            coverage_end=returned_bars[-1].interval_end if returned_bars else None,
            synthetic=False,
            retrieved_at=observed_at,
            notes=tuple(notes),
        )
        return KrakenFetchResult(
            bars=returned_bars,
            open_bar=open_bar,
            provenance=provenance,
            quality=infer_quality(returned_bars),
            last_timestamp=last_timestamp,
            request_uri=request_uri,
            raw_response_hash=raw_hash,
        )

    def fetch_ohlc(
        self,
        interval: int = 1,
        *,
        since: int | datetime | None = None,
        include_open: bool = False,
        mode: str = "OBSERVACIÓN EN DIRECTO",
        now: datetime | None = None,
    ) -> KrakenFetchResult:
        """Fetch finite OHLC data and return closed bars plus open row.

        ``since`` is passed to Kraken as seconds since Unix epoch.  Kraken may
        still return only its most recent 720 entries, and the final entry is
        always treated as in-progress according to the API contract.
        """

        self._validate_fetch_options(interval, mode)
        request_uri = self._rest_request_uri(interval, since)
        response_bytes = self._get(request_uri)
        raw_hash = hashlib.sha256(response_bytes).hexdigest()
        try:
            payload = json.loads(response_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise KrakenTransportError(f"Kraken REST returned non-JSON body (sha256={raw_hash})") from exc
        if not isinstance(payload, dict):
            raise KrakenAPIError("Kraken REST response must be an object")
        self._raise_body_error(payload, request_uri=request_uri, raw_hash=raw_hash)
        result = payload.get("result")
        if not isinstance(result, dict):
            raise KrakenAPIError("Kraken REST response has no result object")
        pair_key, rows = self._find_pair_rows(result)
        if not isinstance(rows, list):
            raise KrakenAPIError(f"Kraken REST result for {pair_key!r} is not an array")
        # El contrato público limita la ventana a 720 entradas. Conservamos
        # las más recientes y dejamos la última (si la hay) separada como
        # ``open_bar``; nunca se rellena lo que quedó fuera de la ventana.
        rows, truncated = self._limit_rest_rows(rows)
        observed_at = ensure_utc(now, field_name="now") if now is not None else datetime.now(UTC)
        bars = self._parse_rest_bars(rows, interval=interval, pair_key=pair_key, observed_at=observed_at)
        return self._fetch_result(
            bars=bars,
            include_open=include_open,
            interval=interval,
            mode=mode,
            request_uri=request_uri,
            raw_hash=raw_hash,
            observed_at=observed_at,
            truncated=truncated,
            last_timestamp=self._rest_last_timestamp(result, bars),
        )

    def fetch_ohlc_with_retries(
        self,
        interval: int = 1,
        *,
        since: int | datetime | None = None,
        include_open: bool = False,
        mode: str = "OBSERVACIÓN EN DIRECTO",
        attempts: int = 3,
        retry_backoff_seconds: float = 1.0,
    ) -> KrakenFetchResult:
        """Retry transport failures only; API/data errors remain explicit.

        The caller should pass a ``since`` cursor (usually with one interval
        overlap) when recovering a discontinuity.  The method does not claim
        that a reconnect or retry recovered events not present in REST OHLC.
        """

        if attempts < 1:
            raise KrakenConfigurationError("attempts must be positive")
        if retry_backoff_seconds < 0:
            raise KrakenConfigurationError("retry_backoff_seconds must not be negative")
        last_error: KrakenTransportError | None = None
        for attempt in range(attempts):
            try:
                return self.fetch_ohlc(interval, since=since, include_open=include_open, mode=mode)
            except KrakenTransportError as exc:
                last_error = exc
                if attempt + 1 >= attempts:
                    raise
                if retry_backoff_seconds:
                    time.sleep(retry_backoff_seconds * min(2**attempt, 8))
        assert last_error is not None
        raise last_error

    def recover_ohlc(
        self,
        interval: int = 1,
        *,
        since: int | datetime,
        overlap_intervals: int = 1,
        include_open: bool = False,
        attempts: int = 3,
    ) -> KrakenFetchResult:
        """Fetch a bounded REST overlap for explicit discontinuity recovery."""

        if overlap_intervals < 0:
            raise KrakenConfigurationError("overlap_intervals must not be negative")
        cursor: int | datetime
        if isinstance(since, datetime):
            cursor = ensure_utc(since, field_name="since") - timedelta(minutes=interval * overlap_intervals)
        else:
            cursor = int(since) - interval * 60 * overlap_intervals
        return self.fetch_ohlc_with_retries(
            interval,
            since=cursor,
            include_open=include_open,
            attempts=attempts,
        )

    def _get(self, uri: str) -> bytes:
        with self._request_lock:
            delay = self.rest_min_interval_seconds - (time.monotonic() - self._last_rest_request)
            if delay > 0:
                time.sleep(delay)
            self._last_rest_request = time.monotonic()
        request = Request(uri, headers={"Accept": "application/json", "User-Agent": self.user_agent})
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                body = response.read()
                if not isinstance(body, bytes):
                    raise KrakenTransportError("Kraken REST response body is not bytes")
                return body
        except HTTPError as exc:
            body = exc.read()
            raw_hash = hashlib.sha256(body).hexdigest()
            try:
                payload = json.loads(body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                payload = None
            if isinstance(payload, dict):
                self._raise_body_error(payload, request_uri=uri, raw_hash=raw_hash, http_status=exc.code)
            raise KrakenTransportError(f"Kraken REST HTTP {exc.code} for {uri} (body_sha256={raw_hash})") from exc
        except (URLError, TimeoutError, OSError) as exc:
            raise KrakenTransportError(f"Kraken REST request failed for {uri}: {exc}") from exc

    @staticmethod
    def _raise_body_error(
        payload: Mapping[str, Any], *, request_uri: str, raw_hash: str, http_status: int | None = None
    ) -> None:
        errors = payload.get("error")
        if errors:
            detail = "; ".join(str(item) for item in errors) if isinstance(errors, list) else str(errors)
            suffix = f" HTTP {http_status}" if http_status is not None else ""
            raise KrakenAPIError(f"Kraken API error{suffix}: {detail} (uri={request_uri}, body_sha256={raw_hash})")

    def _find_pair_rows(self, result: Mapping[str, Any]) -> tuple[str, Any]:
        candidates = [key for key in result if key != "last"]
        if not candidates:
            raise KrakenAPIError("Kraken REST result contains no pair data")
        preferred = [self.pair, self.pair.replace("BTC", "XBT"), "XXBTZUSD" if self.pair == "BTC/USD" else ""]
        for key in preferred:
            if key and key in result:
                return key, result[key]
        if len(candidates) == 1:
            return candidates[0], result[candidates[0]]
        raise KrakenAPIError(f"Kraken REST response has ambiguous pair keys: {candidates}")

    def iter_trades(
        self,
        *,
        duration_seconds: float | None = None,
        max_events: int | None = None,
        include_snapshot: bool = True,
        stop_event: threading.Event | None = None,
        status_callback: Callable[[KrakenStreamStatus], None] | None = None,
        heartbeat_callback: Callable[[datetime], None] | None = None,
    ) -> Iterator[Event]:
        """Synchronous iterator over normalized public trade events.

        For code already running an asyncio loop, use :meth:`aiter_trades`.
        ``max_events`` or ``duration_seconds`` is recommended for smoke tests;
        omitting both starts an unbounded observation stream.
        """

        yield from self._sync_async_iter(
            self.aiter_trades(
                duration_seconds=duration_seconds,
                max_events=max_events,
                include_snapshot=include_snapshot,
                stop_event=stop_event,
                status_callback=status_callback,
                heartbeat_callback=heartbeat_callback,
            )
        )

    def _remember_trade(self, event: Event) -> bool:
        if event.data_id in self._seen_trade_ids:
            self.status.duplicate_events += 1
            return False
        self._seen_trade_ids[event.data_id] = None
        self._seen_trade_ids.move_to_end(event.data_id)
        while len(self._seen_trade_ids) > self._seen_trade_id_limit:
            self._seen_trade_ids.popitem(last=False)
        return True

    def _message_trade_events(self, message: Mapping[str, Any]) -> Iterator[Event]:
        message_type = message.get("type")
        if message_type == "snapshot":
            self.status.snapshot_seen = True
        data = message.get("data")
        if not isinstance(data, list):
            raise KrakenAPIError("Kraken trade message data must be an array")
        for trade in data:
            if not isinstance(trade, dict):
                raise KrakenAPIError("Kraken trade item must be an object")
            event = self._normalize_trade(trade, is_snapshot=message_type == "snapshot")
            if self._remember_trade(event):
                yield event

    async def aiter_trades(
        self,
        *,
        duration_seconds: float | None = None,
        max_events: int | None = None,
        include_snapshot: bool = True,
        stop_event: threading.Event | None = None,
        status_callback: Callable[[KrakenStreamStatus], None] | None = None,
        heartbeat_callback: Callable[[datetime], None] | None = None,
    ) -> AsyncIterator[Event]:
        if duration_seconds is not None and duration_seconds <= 0:
            raise KrakenConfigurationError("duration_seconds must be positive")
        if max_events is not None and max_events < 1:
            raise KrakenConfigurationError("max_events must be positive")
        params = {"channel": "trade", "symbol": [self.ws_symbol], "snapshot": bool(include_snapshot)}
        emitted = 0
        messages = self._aiter_messages(
            "trade",
            params,
            duration_seconds=duration_seconds,
            stop_event=stop_event,
            status_callback=status_callback,
            heartbeat_callback=heartbeat_callback,
        )
        try:
            async for message in messages:
                for event in self._message_trade_events(message):
                    self.status.events_received += 1
                    self.status.last_event_at = event.event_time
                    emitted += 1
                    yield event
                    if max_events is not None and emitted >= max_events:
                        self.status.state = "STOPPED"
                        return
        finally:
            await _close_async_iterator(messages)

    def iter_ohlc(
        self,
        interval: int = 1,
        *,
        duration_seconds: float | None = None,
        max_updates: int | None = None,
        include_snapshot: bool = True,
        stop_event: threading.Event | None = None,
    ) -> Iterator[Bar]:
        yield from self._sync_async_iter(
            self.aiter_ohlc(
                interval,
                duration_seconds=duration_seconds,
                max_updates=max_updates,
                include_snapshot=include_snapshot,
                stop_event=stop_event,
            )
        )

    async def aiter_ohlc(
        self,
        interval: int = 1,
        *,
        duration_seconds: float | None = None,
        max_updates: int | None = None,
        include_snapshot: bool = True,
        stop_event: threading.Event | None = None,
    ) -> AsyncIterator[Bar]:
        if interval not in _SUPPORTED_INTERVALS:
            raise KrakenConfigurationError(f"unsupported Kraken OHLC interval: {interval}")
        if max_updates is not None and max_updates < 1:
            raise KrakenConfigurationError("max_updates must be positive")
        params = {
            "channel": "ohlc",
            "symbol": [self.ws_symbol],
            "interval": interval,
            "snapshot": bool(include_snapshot),
        }
        revisions: dict[datetime, int] = {}
        emitted = 0
        messages = self._aiter_messages("ohlc", params, duration_seconds=duration_seconds, stop_event=stop_event)
        try:
            async for message in messages:
                message_type = message.get("type")
                data = message.get("data")
                if not isinstance(data, list):
                    raise KrakenAPIError("Kraken OHLC message data must be an array")
                observed_at = self.status.last_message_at or datetime.now(UTC)
                for candle in data:
                    if not isinstance(candle, dict):
                        raise KrakenAPIError("Kraken OHLC item must be an object")
                    bar = self._normalize_ohlc_update(
                        candle, interval=interval, observed_at=observed_at, revision=revisions
                    )
                    if message_type == "snapshot":
                        self.status.snapshot_seen = True
                    emitted += 1
                    yield bar
                    if max_updates is not None and emitted >= max_updates:
                        self.status.state = "STOPPED"
                        return
        finally:
            await _close_async_iterator(messages)

    @staticmethod
    def _websocket_connect() -> _WebSocketConnector:
        try:
            import websockets  # type: ignore[import-not-found]
        except ImportError as exc:
            raise KrakenTransportError("WebSocket observation requires optional dependency 'websockets'") from exc
        connector: _WebSocketConnector = websockets.connect
        return connector

    async def _heartbeat(
        self,
        socket: Any,
        *,
        heartbeat_callback: Callable[[datetime], None] | None,
        status_callback: Callable[[KrakenStreamStatus], None] | None,
    ) -> None:
        ping = {"method": "ping", "req_id": int(time.time() * 1000) % 2_000_000_000}
        await socket.send(json.dumps(ping, separators=(",", ":")))
        heartbeat_at = datetime.now(UTC)
        if heartbeat_callback is not None:
            with suppress(Exception):
                heartbeat_callback(heartbeat_at)
        self._notify_status(status_callback, self.status)

    async def _receive_stream_message(
        self,
        socket: Any,
        *,
        channel: str,
        heartbeat_callback: Callable[[datetime], None] | None,
        status_callback: Callable[[KrakenStreamStatus], None] | None,
    ) -> dict[str, Any] | None:
        try:
            raw = await asyncio.wait_for(socket.recv(), timeout=self.heartbeat_seconds)
        except TimeoutError:
            await self._heartbeat(
                socket,
                heartbeat_callback=heartbeat_callback,
                status_callback=status_callback,
            )
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            message = json.loads(raw)
        except (TypeError, json.JSONDecodeError) as exc:
            raise KrakenAPIError("Kraken WebSocket returned non-JSON message") from exc
        if not isinstance(message, dict):
            raise KrakenAPIError("Kraken WebSocket message must be an object")
        self.status.last_message_at = datetime.now(UTC)
        self._notify_status(status_callback, self.status)
        if message.get("success") is False or message.get("error"):
            detail = message.get("error") or message.get("warnings") or "subscription failed"
            raise KrakenAPIError(f"Kraken WebSocket {channel} error: {detail}")
        if message.get("channel") != channel or message.get("type") not in {"snapshot", "update"}:
            # pong/heartbeat/subscription acknowledgements are still observed
            # for status but are not data rows.
            return None
        return message

    async def _stream_connection(
        self,
        connect: Callable[..., Any],
        channel: str,
        params: Mapping[str, Any],
        *,
        started: float,
        duration_seconds: float | None,
        stop_event: threading.Event | None,
        reconnects: int,
        status_callback: Callable[[KrakenStreamStatus], None] | None,
        heartbeat_callback: Callable[[datetime], None] | None,
    ) -> AsyncIterator[dict[str, Any]]:
        self.status.state = "CONNECTING"
        self._notify_status(status_callback, self.status)
        # ``ping_interval`` is a protocol-level heartbeat; the explicit JSON
        # ping below also keeps the application-level connection active when
        # no trades arrive.
        async with connect(
            self.websocket_endpoint,
            open_timeout=self.timeout_seconds,
            close_timeout=self.timeout_seconds,
            ping_interval=max(5.0, min(self.heartbeat_seconds, 20.0)),
            ping_timeout=self.timeout_seconds,
        ) as socket:
            now = datetime.now(UTC)
            self.status.state = "CONNECTED"
            self.status.connected_at = now
            self._notify_status(status_callback, self.status)
            self.status.last_error = None
            if reconnects:
                self.status.reconnect_count += 1
                self.status.discontinuity = True
                self.status.needs_reconciliation = True
            self._notify_status(status_callback, self.status)
            request = {"method": "subscribe", "params": dict(params), "req_id": int(time.time() * 1000) % 2_000_000_000}
            await socket.send(json.dumps(request, separators=(",", ":")))
            while not self._stream_done(started, duration_seconds, stop_event):
                message = await self._receive_stream_message(
                    socket,
                    channel=channel,
                    heartbeat_callback=heartbeat_callback,
                    status_callback=status_callback,
                )
                if message is not None:
                    yield message

    async def _aiter_messages(
        self,
        channel: str,
        params: Mapping[str, Any],
        *,
        duration_seconds: float | None,
        stop_event: threading.Event | None,
        status_callback: Callable[[KrakenStreamStatus], None] | None = None,
        heartbeat_callback: Callable[[datetime], None] | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        try:
            connect = self._websocket_connect()
        except KrakenTransportError:
            self.status.state = "ERROR"
            self.status.last_error = "websockets package is not installed"
            raise
        started = time.monotonic()
        reconnects = 0
        while True:
            if self._stream_done(started, duration_seconds, stop_event):
                self.status.state = "STOPPED"
                return
            try:
                async for message in self._stream_connection(
                    connect,
                    channel,
                    params,
                    started=started,
                    duration_seconds=duration_seconds,
                    stop_event=stop_event,
                    reconnects=reconnects,
                    status_callback=status_callback,
                    heartbeat_callback=heartbeat_callback,
                ):
                    yield message
            except KrakenAPIError:
                self.status.state = "ERROR"
                self._notify_status(status_callback, self.status)
                raise
            except asyncio.CancelledError:
                self.status.state = "STOPPED"
                raise
            except Exception as exc:
                self.status.state = "DISCONNECTED"
                self.status.last_error = f"{type(exc).__name__}: {exc}"
                self._notify_status(status_callback, self.status)
                self.status.discontinuity = True
                self.status.needs_reconciliation = True
                if reconnects >= self.max_reconnects:
                    raise KrakenTransportError(
                        f"Kraken WebSocket disconnected after {reconnects} reconnects: {exc}"
                    ) from exc
                reconnects += 1
                delay = self.reconnect_backoff_seconds * min(2 ** (reconnects - 1), 8)
                if delay:
                    await asyncio.sleep(delay)

    @staticmethod
    def _stream_done(started: float, duration_seconds: float | None, stop_event: threading.Event | None) -> bool:
        return (duration_seconds is not None and time.monotonic() - started >= duration_seconds) or bool(
            stop_event and stop_event.is_set()
        )

    @staticmethod
    def _notify_status(callback: Callable[[KrakenStreamStatus], None] | None, status: KrakenStreamStatus) -> None:
        if callback is None:
            return
        try:
            callback(status)
        except Exception:
            # Observability callbacks must not alter transport correctness.
            return

    def _normalize_trade(self, trade: Mapping[str, Any], *, is_snapshot: bool) -> Event:
        symbol = normalize_pair(str(trade.get("symbol", self.pair)))
        if symbol != self.pair:
            raise KrakenAPIError(f"trade symbol {symbol!r} does not match subscribed pair {self.pair!r}")
        timestamp = _parse_rfc3339(trade.get("timestamp"), "trade.timestamp")
        trade_id = trade.get("trade_id")
        source_id = (
            str(trade_id)
            if trade_id is not None
            else hashlib.sha256(json.dumps(dict(trade), sort_keys=True, default=str).encode()).hexdigest()
        )
        side = str(trade.get("side", "unknown")).lower()
        if side not in {"buy", "sell"}:
            side = "unknown"
        received_at = datetime.now(UTC)
        return Event(
            instrument=self.pair,
            event_time=timestamp,
            price=_number(trade.get("price"), "trade.price", positive=True),
            price_basis="traded",
            quantity=_number(trade.get("qty"), "trade.qty") if trade.get("qty") is not None else None,
            received_at=received_at,
            available_at=received_at,
            source="kraken-websocket-v2",
            source_event_id=source_id,
            source_sequence=trade_id,
            side=side,
            is_snapshot=is_snapshot,
            synthetic=False,
            metadata={
                "ord_type": trade.get("ord_type"),
                "provider_symbol": trade.get("symbol"),
            },
        )

    def _normalize_ohlc_update(
        self,
        candle: Mapping[str, Any],
        *,
        interval: int,
        observed_at: datetime,
        revision: dict[datetime, int],
    ) -> Bar:
        begin_value = candle.get("interval_begin", candle.get("timestamp"))
        start = _parse_rfc3339(begin_value, "ohlc.interval_begin")
        current_revision = revision.get(start, -1) + 1
        revision[start] = current_revision
        close = _number(candle.get("close"), "ohlc.close", positive=True)
        return Bar(
            instrument=self.pair,
            interval_start=start,
            interval_end=start + timedelta(minutes=interval),
            open=_number(candle.get("open"), "ohlc.open", positive=True),
            high=_number(candle.get("high"), "ohlc.high", positive=True),
            low=_number(candle.get("low"), "ohlc.low", positive=True),
            close=close,
            resolution_seconds=interval * 60,
            volume=_number(candle.get("volume"), "ohlc.volume") if candle.get("volume") is not None else None,
            trade_count=_trade_count(candle.get("trades"), "ohlc.trades") if candle.get("trades") is not None else None,
            price_basis="traded",
            source="kraken-websocket-v2",
            source_record_id=f"kraken-ws-ohlc:{self.pair}:{int(start.timestamp())}",
            received_at=observed_at,
            available_at=observed_at,
            closed=observed_at >= start + timedelta(minutes=interval),
            synthetic=False,
            revision=current_revision,
            metadata={
                "provider_interval": candle.get("interval", interval),
                "vwap": _number(candle.get("vwap"), "ohlc.vwap") if candle.get("vwap") is not None else None,
            },
        )

    @staticmethod
    def _sync_async_iter(async_iterator: AsyncIterator[Any]) -> Iterator[Any]:
        """Bridge an async generator for ordinary synchronous CLI callers."""

        loop = asyncio.new_event_loop()
        try:
            while True:
                try:
                    yield loop.run_until_complete(async_iterator.__anext__())
                except StopAsyncIteration:
                    return
        finally:
            with suppress(Exception):
                loop.run_until_complete(_close_async_iterator(async_iterator))
            loop.close()


class KrakenAdapter(KrakenPublicAdapter):
    """Compatibility name for discovery by a CLI/provider registry.

    ``instrument`` is accepted as an alias for the public pair argument; the
    underlying adapter remains public-data-only and has no order operations.
    """

    def __init__(self, instrument: str = "BTC/USD", **kwargs: Any) -> None:
        super().__init__(pair=instrument, **kwargs)
