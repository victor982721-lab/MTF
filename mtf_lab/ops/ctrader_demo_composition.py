"""Offline composition of the cTrader DEMO vertical slice.

This module is deliberately an offline composition root, not another broker
client.  It joins the already-tested boundaries in one deterministic fixture:

``credential refs -> application auth -> account discovery -> explicit DEMO
selection/account auth -> symbol catalog/history -> RuntimeCoordinator ->
durable intent -> official Protobuf DEMO transport -> execution -> reconcile
-> close -> durable reporting``.

The loopback server below implements the transport protocol in process and
round-trips every message through :class:`SdkProtobufCodec`.  It never opens a
socket, reads a token store, starts OAuth, or contacts cTrader.  The fixture
data is synthetic and is kept separate from the production capture helpers so
that the existing strategy configuration is not changed merely to manufacture
an execution result.
"""

from __future__ import annotations

import dataclasses
import importlib
import json
import os
import queue
from collections import deque
from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, NoReturn

from ..configuration import EffectiveConfig
from ..core import Signal
from ..data.capture import CaptureEnvelope, MessageClass
from ..data.ctrader import (
    CTraderClient,
    CTraderConfig,
    CTraderInstrumentSpec,
    CTraderProvider,
    SdkProtobufCodec,
    WireMessage,
    message_to_mapping,
    read_field,
    synthetic_spot_event,
    synthetic_trendbar,
)
from ..runtime.consumers import (
    SignalConsumerCheckpoint,
    SignalConsumerEvent,
    SignalConsumerResult,
)
from ..runtime.integration import RuntimeCoordinator
from ..runtime.integration import capture_hash as runtime_capture_hash
from ..runtime.state import PriceObservation, signal_dict, signal_from_dict
from .ctrader_capture import CaptureCoverage, CTraderCapture, capture_envelopes, normalize_ctrader_capture
from .ctrader_demo_transport import (
    DEMO_PROTOBUF_ENDPOINT,
    CTraderClientGateway,
    CTraderDemoTransport,
    ServerAccountObservation,
)
from .ctrader_executor import (
    CTraderDemoExecutor,
    DecimalValue,
    DemoAccount,
    ExecutionError,
    ExecutionIntent,
    ExecutionPolicy,
    Fill,
    IntentStore,
    JsonlIntentStore,
    OrderResult,
    OrderState,
    Quote,
    RealAccountForbidden,
    Side,
)
from .ctrader_paper_adapters import spot_event_to_cfd_quote
from .persistence import SQLiteStore

OFFLINE_COMPOSITION_VERSION = 1
DEFAULT_ACCOUNT_ID = 123
DEFAULT_SYMBOL_ID = 99
DEFAULT_COUNT = 1_020
DEFAULT_START = datetime(2026, 1, 1, tzinfo=UTC)

_OFFICIAL_REQUEST_NAMES = {
    2100: "ProtoOAApplicationAuthReq",
    2149: "ProtoOAGetAccountListByAccessTokenReq",
    2102: "ProtoOAAccountAuthReq",
    2114: "ProtoOASymbolsListReq",
    2116: "ProtoOASymbolByIdReq",
    2127: "ProtoOASubscribeSpotsReq",
    2135: "ProtoOASubscribeLiveTrendbarReq",
    2137: "ProtoOAGetTrendbarsReq",
    2106: "ProtoOANewOrderReq",
    2124: "ProtoOAReconcileReq",
    2111: "ProtoOAClosePositionReq",
}


class OfflineCompositionError(RuntimeError):
    """The deterministic composition fixture could not prove a stage."""


@dataclasses.dataclass(frozen=True, slots=True)
class DemoMarketFixture:
    """Synthetic SpotEvent rows and matching native history evidence."""

    start: datetime
    end: datetime
    symbol_id: int
    rows: tuple[Mapping[str, Any], ...]
    trendbars: tuple[Mapping[str, Any], ...]
    capture: CTraderCapture


@dataclasses.dataclass(frozen=True, slots=True)
class DemoCompositionResult:
    """Objects and a compact, secret-free receipt from one fixture run."""

    report: Mapping[str, Any]
    fixture: DemoMarketFixture
    client: CTraderClient
    wire_transport: LoopbackProtobufTransport
    server: OfflineDemoServer
    provider: CTraderProvider
    application: CTraderDemoApplicationResult
    gateway: CTraderClientGateway
    observation: ServerAccountObservation
    demo_transport: CTraderDemoTransport
    executor: CTraderDemoExecutor
    intent_store: IntentStore
    open_result: OrderResult
    reconciled_result: OrderResult
    close_result: OrderResult


class LoopbackProtobufTransport:
    """A synchronous in-process cTrader transport with a real codec boundary."""

    def __init__(self, codec: Any, handler: Callable[[WireMessage], WireMessage | None]) -> None:
        self.codec = codec
        self.handler = handler
        self.connected = False
        self.sent: list[WireMessage] = []
        self.received: list[WireMessage] = []
        self.inbound: queue.Queue[WireMessage] = queue.Queue()
        self.connect_calls = 0
        self.close_calls = 0

    def connect(self, timeout: float | None = None) -> None:
        del timeout
        self.connect_calls += 1
        self.connected = True

    def close(self) -> None:
        self.close_calls += 1
        self.connected = False
        while True:
            try:
                self.inbound.get_nowait()
            except queue.Empty:
                return

    def send(self, message: WireMessage) -> None:
        if not self.connected:
            raise RuntimeError("offline loopback transport is disconnected")
        # Round-trip the outbound request through the installed official
        # codec.  The fake server therefore sees generated Protobuf messages,
        # not the caller's dictionaries.
        request = self.codec.decode(self.codec.encode(message))
        self.sent.append(request)
        response = self.handler(request)
        if response is None:
            return
        decoded_response = self.codec.decode(self.codec.encode(response))
        self.received.append(decoded_response)
        self.inbound.put(decoded_response)

    def receive(self, timeout: float | None = None) -> WireMessage | None:
        if not self.connected:
            raise RuntimeError("offline loopback transport is disconnected")
        try:
            return self.inbound.get(timeout=timeout if timeout is not None else 0)
        except queue.Empty:
            return None


class OfflineDemoServer:
    """Minimal server-observed DEMO fixture for the full local vertical slice."""

    def __init__(
        self,
        proto: Any,
        model: Any,
        *,
        fixture: DemoMarketFixture,
        account_id: int = DEFAULT_ACCOUNT_ID,
    ) -> None:
        self.proto = proto
        self.model = model
        self.fixture = fixture
        self.account_id = int(account_id)
        self.calls: list[dict[str, Any]] = []
        self.open_intent_id: str | None = None
        self.open_order_id = 7001
        self.close_order_id = 7002
        self.position_id = 9001
        self._open_submission_count = 0
        self._reconcile_count = 0
        self._close_count = 0
        self.order_exists = False
        self.position_open = False

    @property
    def payload_types(self) -> tuple[int, ...]:
        return tuple(int(item["payload_type"]) for item in self.calls)

    @property
    def message_names(self) -> tuple[str, ...]:
        return tuple(str(item["payload_name"]) for item in self.calls)

    def handle(self, request: WireMessage) -> WireMessage:
        payload_type = request.payload_type_id
        if payload_type is None:
            raise OfflineCompositionError(f"loopback request lacks payload type: {request.payload!r}")
        self.calls.append(
            {
                "payload_type": int(payload_type),
                "payload_name": _OFFICIAL_REQUEST_NAMES.get(int(payload_type), request.payload_type_name),
                "client_msg_id": request.client_msg_id,
            }
        )
        handlers: dict[int, Callable[[WireMessage], WireMessage]] = {
            2100: self._application_auth,
            2149: self._account_discovery,
            2102: self._account_auth,
            2114: self._symbols_list,
            2116: self._symbol_by_id,
            2127: self._subscribe_spots,
            2135: self._subscribe_trendbars,
            2137: self._trendbars,
            2106: self._new_order,
            2124: self._reconcile,
            2175: self._order_list,
            2181: self._order_details,
            2133: self._deal_list,
            2111: self._close_position,
        }
        handler = handlers.get(int(payload_type))
        if handler is None:
            raise AssertionError(f"unexpected offline cTrader payload type: {payload_type}")
        return handler(request)

    def _response(self, request: WireMessage, payload_type: int, body: Any) -> WireMessage:
        body.payloadType = int(payload_type)
        return WireMessage(int(payload_type), body, request.client_msg_id)

    def _application_auth(self, request: WireMessage) -> WireMessage:
        return self._response(request, 2101, self.proto.ProtoOAApplicationAuthRes())

    def _account_discovery(self, request: WireMessage) -> WireMessage:
        # The generated schema marks accessToken as required even though this
        # local response is only an in-process fixture.  It is never recorded
        # in ``calls`` or exported by the composition report.
        response = self.proto.ProtoOAGetAccountListByAccessTokenRes(
            permissionScope=1,
            accessToken="offline-fixture-value",
        )
        response.ctidTraderAccount.add(ctidTraderAccountId=self.account_id, isLive=False)
        return self._response(request, 2150, response)

    def _account_auth(self, request: WireMessage) -> WireMessage:
        return self._response(
            request,
            2103,
            self.proto.ProtoOAAccountAuthRes(ctidTraderAccountId=self.account_id),
        )

    def _symbols_list(self, request: WireMessage) -> WireMessage:
        response = self.proto.ProtoOASymbolsListRes(ctidTraderAccountId=self.account_id)
        response.symbol.add(symbolId=self.fixture.symbol_id, symbolName="EUR/USD", enabled=True)
        return self._response(request, 2115, response)

    def _symbol_by_id(self, request: WireMessage) -> WireMessage:
        response = self.proto.ProtoOASymbolByIdRes(ctidTraderAccountId=self.account_id)
        response.symbol.add(symbolId=self.fixture.symbol_id, digits=5, pipPosition=4)
        return self._response(request, 2117, response)

    def _subscribe_spots(self, request: WireMessage) -> WireMessage:
        return self._response(request, 2128, self.proto.ProtoOASubscribeSpotsRes(ctidTraderAccountId=self.account_id))

    def _subscribe_trendbars(self, request: WireMessage) -> WireMessage:
        return self._response(
            request,
            2165,
            self.proto.ProtoOASubscribeLiveTrendbarRes(ctidTraderAccountId=self.account_id),
        )

    def _trendbars(self, request: WireMessage) -> WireMessage:
        requested_period = int(getattr(request.payload, "period", 1))
        response = self.proto.ProtoOAGetTrendbarsRes(
            ctidTraderAccountId=self.account_id,
            period=requested_period,
            symbolId=self.fixture.symbol_id,
            timestamp=int(self.fixture.end.timestamp() * 1000),
        )
        # The fixture's history is M1.  The provider call in this composition
        # is intentionally M1; aggregation into M5/M15 belongs to runtime.
        if requested_period == 1:
            for raw in self.fixture.trendbars:
                bar = response.trendbar.add()
                for name in (
                    "volume",
                    "period",
                    "low",
                    "deltaOpen",
                    "deltaClose",
                    "deltaHigh",
                    "utcTimestampInMinutes",
                ):
                    if name in raw:
                        setattr(bar, name, int(raw[name]))
        return self._response(request, 2138, response)

    def _new_order(self, request: WireMessage) -> WireMessage:
        self._open_submission_count += 1
        self.open_intent_id = str(request.client_msg_id or "")
        self.order_exists = True
        response = self._execution_event(
            execution_type=11,
            order_id=self.open_order_id,
            order_status=1,
            requested_volume=100,
            executed_volume=50,
            execution_price=1.12105,
            deal_id=8101,
            deal_volume=50,
            client_order_id=self.open_intent_id,
            position_status=1,
            position_volume=50,
            closing=False,
        )
        return self._response(request, 2126, response)

    def _reconcile(self, request: WireMessage) -> WireMessage:
        self._reconcile_count += 1
        response = self.proto.ProtoOAReconcileRes(ctidTraderAccountId=self.account_id)
        if self.order_exists and self.open_intent_id:
            order = response.order.add(
                orderId=self.open_order_id,
                orderType=1,
                orderStatus=2,
                executedVolume=100,
                executionPrice=1.12105,
                clientOrderId=self.open_intent_id,
                positionId=self.position_id,
            )
            order.tradeData.symbolId = self.fixture.symbol_id
            order.tradeData.volume = 100
            order.tradeData.tradeSide = 1
            # The first reconcile resolves the partial order and establishes
            # the position observed by subsequent position-list calls.
            if not self.position_open and str(request.client_msg_id or "") != "reconcile:positions":
                self.position_open = True
            if self.position_open:
                position = response.position.add(positionId=self.position_id, positionStatus=1, swap=0, price=1.12105)
                position.tradeData.symbolId = self.fixture.symbol_id
                position.tradeData.volume = 100
                position.tradeData.tradeSide = 1
                position.tradeData.label = self.open_intent_id
        return self._response(request, 2125, response)

    def _order_list(self, request: WireMessage) -> WireMessage:
        response = self.proto.ProtoOAOrderListRes(ctidTraderAccountId=self.account_id, hasMore=False)
        return self._response(request, 2176, response)

    def _order_details(self, request: WireMessage) -> WireMessage:
        response = self.proto.ProtoOAOrderDetailsRes(ctidTraderAccountId=self.account_id)
        return self._response(request, 2182, response)

    def _deal_list(self, request: WireMessage) -> WireMessage:
        response = self.proto.ProtoOADealListRes(ctidTraderAccountId=self.account_id, hasMore=False)
        return self._response(request, 2134, response)

    def _close_position(self, request: WireMessage) -> WireMessage:
        requested_position = int(getattr(request.payload, "positionId", 0))
        if requested_position != self.position_id or not self.position_open:
            raise AssertionError("offline close did not target the open fixture position")
        self._close_count += 1
        self.position_open = False
        response = self._execution_event(
            execution_type=3,
            order_id=self.close_order_id,
            order_status=2,
            requested_volume=100,
            executed_volume=100,
            execution_price=1.12090,
            deal_id=8102,
            deal_volume=100,
            client_order_id=None,
            position_status=2,
            position_volume=100,
            closing=True,
        )
        self.order_exists = False
        self.open_intent_id = None
        return self._response(request, 2126, response)

    def _execution_event(
        self,
        *,
        execution_type: int,
        order_id: int,
        order_status: int,
        requested_volume: int,
        executed_volume: int,
        execution_price: float,
        deal_id: int,
        deal_volume: int,
        client_order_id: str | None,
        position_status: int,
        position_volume: int,
        closing: bool,
    ) -> Any:
        event = self.proto.ProtoOAExecutionEvent(
            ctidTraderAccountId=self.account_id,
            executionType=execution_type,
        )
        order = event.order
        order.orderId = order_id
        order.orderType = 1
        order.orderStatus = order_status
        order.executedVolume = executed_volume
        order.executionPrice = execution_price
        order.positionId = self.position_id
        order.closingOrder = closing
        if client_order_id is not None:
            order.clientOrderId = client_order_id
        order.tradeData.symbolId = self.fixture.symbol_id
        order.tradeData.volume = requested_volume
        order.tradeData.tradeSide = 1
        deal = event.deal
        deal.dealId = deal_id
        deal.orderId = order_id
        deal.positionId = self.position_id
        deal.volume = deal_volume
        deal.filledVolume = deal_volume
        deal.symbolId = self.fixture.symbol_id
        deal.createTimestamp = int(self.fixture.end.timestamp() * 1000)
        deal.executionTimestamp = int(self.fixture.end.timestamp() * 1000)
        deal.executionPrice = execution_price
        deal.tradeSide = 1
        deal.dealStatus = 3 if execution_type == 11 else 2
        position = event.position
        position.positionId = self.position_id
        position.positionStatus = position_status
        position.swap = 0
        position.price = execution_price
        position.tradeData.symbolId = self.fixture.symbol_id
        position.tradeData.volume = position_volume
        position.tradeData.tradeSide = 1
        if client_order_id is not None:
            position.tradeData.label = client_order_id
        return event


class DemoExecutionSignalConsumer:
    """Runtime consumer that submits detector signals exactly once.

    The consumer is constructed before the first observation is ingested.  It
    receives only signals emitted by ``IncrementalProcessor``; it never scans
    a paper result or creates a synthetic ``Signal`` after the fact.
    """

    consumer_type = "ctrader_demo_execution"
    product = "CTRADER_DEMO_EXECUTION"

    def __init__(
        self,
        executor: CTraderDemoExecutor,
        quote_resolver: Callable[[Signal], Quote],
        *,
        max_events: int = 4096,
    ) -> None:
        if isinstance(max_events, bool) or max_events <= 0:
            raise ValueError("max_events must be a positive integer")
        self.executor = executor
        self.quote_resolver = quote_resolver
        self.max_events = int(max_events)
        self._events: deque[SignalConsumerEvent] = deque(maxlen=self.max_events)
        self._signals: deque[Signal] = deque(maxlen=self.max_events)
        self._signal_ids: set[str] = set()
        self._signal_order: deque[str] = deque()
        self._results: list[OrderResult] = []
        self._sequence = 0
        self.signal_count = 0
        self.observation_count = 0
        self.advance_count = 0
        self._last_watermark: datetime | None = None

    @property
    def events(self) -> tuple[SignalConsumerEvent, ...]:
        return tuple(self._events)

    @property
    def signals(self) -> tuple[Signal, ...]:
        return tuple(self._signals)

    @property
    def execution_results(self) -> tuple[OrderResult, ...]:
        return tuple(self._results)

    @property
    def pending_simulations(self) -> tuple[Any, ...]:
        return ()

    @property
    def completed_simulations(self) -> tuple[Any, ...]:
        return ()

    def _emit(
        self,
        kind: str,
        identity: str,
        *,
        timestamp: datetime | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> SignalConsumerEvent:
        self._sequence += 1
        event = SignalConsumerEvent(
            kind=kind,
            identity=identity,
            sequence=self._sequence,
            timestamp=timestamp,
            payload={"product": self.product, **dict(payload or {})},
        )
        self._events.append(event)
        return event

    def on_signal(self, signal: Signal) -> SignalConsumerResult:
        if not isinstance(signal, Signal):
            raise TypeError("on_signal requires core.Signal")
        if signal.signal_id in self._signal_ids:
            return SignalConsumerResult()
        self._signals.append(signal)
        self._signal_ids.add(signal.signal_id)
        self._signal_order.append(signal.signal_id)
        while len(self._signal_order) > self.max_events:
            self._signal_ids.discard(self._signal_order.popleft())
        self.signal_count += 1
        try:
            quote = self.quote_resolver(signal)
            result = self.executor.submit_signal(signal, quote)
        except ExecutionError as exc:
            event = self._emit(
                "execution_blocked",
                signal.signal_id,
                timestamp=signal.detected_at,
                payload={"error": type(exc).__name__, "reason": str(exc)},
            )
            return SignalConsumerResult(events=(event,))
        self._results.append(result)
        event = self._emit(
            "execution_result",
            result.intent.intent_id,
            timestamp=signal.detected_at,
            payload={
                "signal_id": signal.signal_id,
                "intent_id": result.intent.intent_id,
                "state": result.state.value,
                "filled_quantity": str(result.filled_quantity),
                "position_ids": list(result.position_ids),
            },
        )
        return SignalConsumerResult(events=(event,))

    def on_observation(
        self,
        observation: PriceObservation,
        *,
        watermark: datetime,
    ) -> SignalConsumerResult:
        if not isinstance(observation, PriceObservation):
            raise TypeError("on_observation requires PriceObservation")
        self.observation_count += 1
        self._last_watermark = _utc(watermark, "watermark")
        event = self._emit(
            "observation",
            observation.identity,
            timestamp=observation.available_at,
            payload={
                "instrument": observation.instrument,
                "quality": observation.quality,
                "available_at": _iso(observation.available_at),
            },
        )
        return SignalConsumerResult(events=(event,))

    def advance(
        self,
        watermark: datetime,
        *,
        capture_complete: bool = False,
    ) -> SignalConsumerResult:
        if not isinstance(capture_complete, bool):
            raise ValueError("capture_complete must be boolean")
        self.advance_count += 1
        self._last_watermark = _utc(watermark, "watermark")
        event = self._emit(
            "advance",
            f"advance:{self.advance_count}",
            timestamp=self._last_watermark,
            payload={"capture_complete": capture_complete},
        )
        return SignalConsumerResult(events=(event,))

    def checkpoint(self) -> SignalConsumerCheckpoint:
        return SignalConsumerCheckpoint(
            self.consumer_type,
            {
                "max_events": self.max_events,
                "events": [event.to_dict() for event in self._events],
                "signals": [signal_dict(signal) for signal in self._signals],
                "signal_ids": list(self._signal_order),
                "sequence": self._sequence,
                "signal_count": self.signal_count,
                "observation_count": self.observation_count,
                "advance_count": self.advance_count,
                "last_watermark": _iso(self._last_watermark),
            },
        )

    def restore(self, checkpoint: SignalConsumerCheckpoint | Mapping[str, Any]) -> None:
        snapshot = (
            checkpoint
            if isinstance(checkpoint, SignalConsumerCheckpoint)
            else SignalConsumerCheckpoint.from_mapping(checkpoint)
        )
        if snapshot.consumer_type != self.consumer_type:
            raise ValueError(f"checkpoint type {snapshot.consumer_type!r} is incompatible")
        self._events.clear()
        self._signals.clear()
        self._signal_ids.clear()
        self._signal_order.clear()
        for raw in snapshot.state.get("events", ()):
            if isinstance(raw, Mapping):
                self._events.append(SignalConsumerEvent.from_mapping(raw))
        for raw in snapshot.state.get("signals", ()):
            if isinstance(raw, Mapping):
                signal = signal_from_dict(raw)
                self._signals.append(signal)
                self._signal_ids.add(signal.signal_id)
                self._signal_order.append(signal.signal_id)
        self._sequence = int(snapshot.state.get("sequence", 0))
        self.signal_count = int(snapshot.state.get("signal_count", len(self._signals)))
        self.observation_count = int(snapshot.state.get("observation_count", 0))
        self.advance_count = int(snapshot.state.get("advance_count", 0))
        self._last_watermark = _parse_optional_time(snapshot.state.get("last_watermark"))


@dataclasses.dataclass(frozen=True, slots=True)
class CTraderDemoApplicationResult:
    """Receipt from the generic cTrader DEMO application service."""

    session_id: str
    analysis_id: str
    provider: CTraderProvider
    history: Any
    subscriptions: tuple[Any, ...]
    discovery: tuple[Mapping[str, Any], ...]
    gateway: CTraderClientGateway
    observation: ServerAccountObservation
    demo_transport: CTraderDemoTransport
    executor: CTraderDemoExecutor
    consumer: DemoExecutionSignalConsumer
    coordinator: RuntimeCoordinator
    normalized_events: tuple[Any, ...]
    runtime_result: Any
    execution_results: tuple[OrderResult, ...]
    reconciled_results: tuple[OrderResult, ...]
    close_results: tuple[OrderResult, ...]
    report: Mapping[str, Any]

    @property
    def signals(self) -> tuple[Signal, ...]:
        return self.coordinator.signals


class CTraderDemoApplication:
    """Generic composition service for an authenticated cTrader DEMO run.

    The caller supplies the client, secret/token reference resolvers, explicit
    selected account, execution policy, and durable intent store.  No OAuth
    browser, token store, or network connection is created here; the supplied
    ``CTraderClient`` is the only session boundary and the supplied provider is
    required to use that same client.
    """

    def __init__(
        self,
        store: SQLiteStore,
        config: EffectiveConfig,
        *,
        client: CTraderClient,
        secret_provider: Callable[[str], str],
        token_provider: Callable[[str], str],
        selected_account_id: int,
        intent_store: IntentStore,
        policy: ExecutionPolicy,
        provider: CTraderProvider | None = None,
        activate: bool = False,
        proto: Any | None = None,
        clock: Callable[[], datetime] | None = None,
        market_clock: Callable[[], datetime] | None = None,
        source_mode: str = "UNKNOWN",
    ) -> None:
        if isinstance(selected_account_id, bool) or int(selected_account_id) <= 0:
            raise ValueError("selected_account_id must be positive")
        if not callable(secret_provider) or not callable(token_provider):
            raise TypeError("secret_provider and token_provider must be callable")
        if not isinstance(activate, bool):
            raise TypeError("activate must be boolean")
        if activate and not _is_durable_intent_store(intent_store):
            raise OfflineCompositionError(
                "activate=True requires a verified JsonlIntentStore; MemoryIntentStore is observation-only"
            )
        self.store = store
        self.config = config
        self.client = client
        self.secret_provider = secret_provider
        self.token_provider = token_provider
        self.selected_account_id = int(selected_account_id)
        self.intent_store = intent_store
        self.policy = policy
        self.provider = provider
        self.activate = activate
        self.proto = proto
        self.clock = clock or (lambda: datetime.now(UTC))
        self.market_clock = market_clock or self.clock
        self.source_mode = str(source_mode).strip().upper() or "UNKNOWN"
        self._current_time = [_utc(self.clock(), "clock")]
        self._started = False
        self._closed = False
        self._recovered_intents: tuple[ExecutionIntent, ...] = ()
        self._startup_reconciled_results: tuple[OrderResult, ...] = ()

    def run(
        self,
        payloads: Iterable[Any],
        *,
        session_id: str = "ctrader-demo-application",
        history_count: int | None = None,
        history_from: datetime | None = None,
        history_to: datetime | None = None,
        capture_complete: bool = False,
        reconcile: bool = True,
        close_positions: bool = False,
        subscribe: bool = False,
        resume: bool = False,
        resume_source: str = "full_prefix",
        stream_identity: str | None = None,
        completion_evidence: Mapping[str, Any] | None = None,
    ) -> CTraderDemoApplicationResult:
        self._validate_run_options(
            capture_complete,
            reconcile,
            close_positions,
            subscribe,
            resume,
            resume_source,
            stream_identity,
            completion_evidence,
        )
        self._started = True
        try:
            discovery, proof = self._authenticate_session()
            provider, catalog, subscriptions, history = self._prepare_market(
                history_count,
                history_from,
                history_to,
                subscribe,
            )
            events, source_rows, normalization_issues = self._normalize_payloads(provider, payloads)
            if not events:
                raise OfflineCompositionError("provider produced no complete spot events")
            gateway, observation, demo_transport, executor = self._build_execution(provider)
            consumer, coordinator, runtime_result = self._run_runtime(
                provider,
                executor,
                events,
                session_id=session_id,
                capture_complete=capture_complete,
                resume=resume,
                resume_source=resume_source,
                stream_identity=stream_identity,
                reconcile_at_start=reconcile,
            )
            execution_results = consumer.execution_results
            post_results = self._reconcile(executor, execution_results) if reconcile else execution_results
            by_intent = {result.intent.intent_id: result for result in self._startup_reconciled_results}
            by_intent.update({result.intent.intent_id: result for result in post_results})
            reconciled_results = tuple(by_intent.values())
            close_results = self._close_positions(executor) if close_positions else ()
            report = _application_report(
                self,
                provider=provider,
                catalog=catalog,
                history=history,
                subscriptions=subscriptions,
                discovery=discovery,
                proof=proof,
                observation=observation,
                server_rows=source_rows,
                normalization_issues=normalization_issues,
                coordinator=coordinator,
                consumer=consumer,
                execution_results=execution_results,
                reconciled_results=reconciled_results,
                close_results=close_results,
                capture_complete=capture_complete,
                completion_evidence=completion_evidence,
            )
            return CTraderDemoApplicationResult(
                coordinator.session_id,
                coordinator.analysis_id,
                provider,
                history,
                tuple(subscriptions),
                discovery,
                gateway,
                observation,
                demo_transport,
                executor,
                consumer,
                coordinator,
                tuple(events),
                runtime_result,
                execution_results,
                tuple(reconciled_results),
                tuple(close_results),
                report,
            )
        except BaseException:
            self.close()
            raise

    @staticmethod
    def _validate_run_options(
        capture_complete: bool,
        reconcile: bool,
        close_positions: bool,
        subscribe: bool,
        resume: bool,
        resume_source: str,
        stream_identity: str | None,
        completion_evidence: Mapping[str, Any] | None,
    ) -> None:
        options = (capture_complete, reconcile, close_positions, subscribe, resume)
        if not all(isinstance(value, bool) for value in options):
            raise TypeError("capture_complete, reconcile, close_positions, subscribe and resume must be boolean")
        if str(resume_source).strip().lower() != "full_prefix":
            raise OfflineCompositionError("only resume_source=full_prefix is supported; suffix input is rejected")
        if resume and (stream_identity is None or not str(stream_identity).strip()):
            raise OfflineCompositionError("resume requires an explicit stream_identity")
        if capture_complete and not _completion_is_verified(completion_evidence):
            raise OfflineCompositionError("capture_complete=True requires explicit END/CONTINUOUS completion evidence")

    def _authenticate_session(self) -> tuple[tuple[Mapping[str, Any], ...], Any]:
        if self.client.config.environment != "demo":
            raise RealAccountForbidden("DEMO composition rejects REAL/LIVE client configuration")
        self.client.connect()
        self.client.authenticate(
            secret_provider=self.secret_provider,
            token_provider=self.token_provider,
            authorize_selected=False,
        )
        discovery = tuple(dict(item) for item in self.client.discovered_accounts)
        _selected_demo_record(discovery, self.selected_account_id)
        self.client.authorize_account(self.selected_account_id, token_provider=self.token_provider)
        proof = self.client.authenticated_session_evidence()
        return discovery, proof

    def _prepare_market(
        self,
        history_count: int | None,
        history_from: datetime | None,
        history_to: datetime | None,
        subscribe: bool,
    ) -> tuple[CTraderProvider, Any, tuple[Any, ...], Any]:
        provider = self.provider or CTraderProvider(
            self.client.config,
            client=self.client,
            clock=self.market_clock,
        )
        if provider.client is not self.client:
            raise OfflineCompositionError("provider and CTraderClient must be the same session")
        expected_symbol = self.config.instrument.upper().replace("-", "/")
        if (
            provider.config.environment != "demo"
            or provider.config.symbol != self.client.config.symbol
            or provider.config.symbol != expected_symbol
        ):
            raise RealAccountForbidden("provider/client environment or symbol mismatch at DEMO boundary")
        catalog = provider.resolve_symbol()
        subscriptions = provider.subscribe(timeframes=self.client.config.timeframes) if subscribe else ()
        history = provider.fetch(
            "M1",
            count=history_count or self.client.config.historical_count,
            from_timestamp=history_from,
            to_timestamp=history_to,
        )
        return provider, catalog, tuple(subscriptions), history

    def _build_execution(
        self,
        provider: CTraderProvider,
    ) -> tuple[CTraderClientGateway, ServerAccountObservation, CTraderDemoTransport, CTraderDemoExecutor]:
        account = DemoAccount(
            str(self.selected_account_id),
            "DEMO",
            DEMO_PROTOBUF_ENDPOINT,
            frozenset({"trading"}),
            selected=True,
            verified=False,
        )
        gateway = CTraderClientGateway(self.client, clock=lambda: self._current_time[0])
        observation = gateway.server_observation()
        symbol_id = int(provider.spec.symbol_id or 0)
        demo_transport = CTraderDemoTransport(
            account,
            client=gateway,
            proto=self.proto,
            symbol_ids={provider.spec.symbol: symbol_id},
            symbol_names={symbol_id: provider.spec.symbol},
            clock=lambda: self._current_time[0],
            server_observation=observation,
            # The offline server fixture has no full-symbol catalog payload;
            # keep its protocol volume grid explicit rather than inventing a
            # broker default in the external transport.
            volume_grid={"min_volume": 100, "max_volume": 100_000, "step_volume": 100},
        )
        executor = CTraderDemoExecutor(
            account,
            policy=self.policy,
            transport=demo_transport,
            intent_store=self.intent_store,
            clock=lambda: self._current_time[0],
            server_observation=observation,
            fixture_mode=self.source_mode == "SYNTHETIC_FIXTURE",
        )
        if self.activate:
            executor.activate()
        return gateway, observation, demo_transport, executor

    def _run_runtime(
        self,
        provider: CTraderProvider,
        executor: CTraderDemoExecutor,
        events: list[Any],
        *,
        session_id: str,
        capture_complete: bool,
        resume: bool,
        resume_source: str,
        stream_identity: str | None,
        reconcile_at_start: bool,
    ) -> tuple[DemoExecutionSignalConsumer, RuntimeCoordinator, Any]:
        observed_events: list[Any] = []

        def quote_resolver(signal: Signal) -> Quote:
            observation = getattr(executor, "server_observation", None)
            return _quote_for_observed_signal(
                observed_events,
                signal,
                session_id=getattr(observation, "session_id", None),
                connection_generation=getattr(observation, "connection_generation", None),
                synthetic=self.source_mode == "SYNTHETIC_FIXTURE",
            )

        self._recovered_intents = ()
        self._startup_reconciled_results = ()
        consumer = DemoExecutionSignalConsumer(executor, quote_resolver)
        dataset_hash = _stream_identity(stream_identity, events)
        local_loopback = isinstance(self.client.transport, LoopbackProtobufTransport)
        stored_session_id = self.store.create_session(
            session_id=session_id,
            mode="REPLAY",
            provider=provider.name,
            instrument=provider.spec.symbol,
            config=self.config.to_dict(),
            code_version=self.config.version,
            dataset_ref=dataset_hash,
            metadata={
                "product": "CTRADER_DEMO_EXECUTION",
                "source_mode": self.source_mode,
                "network_performed": False if local_loopback else None,
            },
        )
        coordinator = RuntimeCoordinator(
            self.store,
            stored_session_id,
            self.config,
            mode="REPLAY",
            dataset_hash=dataset_hash,
            variant="trend_pullback_v1",
            partition="demo",
            source="ctrader-open-api",
            max_candles=512,
            signal_consumer=consumer,
            resume=resume,
            checkpoint_every=100,
            clock=lambda: self._current_time[0],
            identity_extra={"product": "CTRADER_DEMO_EXECUTION", "source_mode": self.source_mode},
        )
        resume_checkpoint = self._require_resume_checkpoint(coordinator) if resume else None
        if resume:
            if self.activate and not reconcile_at_start:
                raise OfflineCompositionError("activated resume requires reconciliation before new entries")
            raw_path = getattr(self.intent_store, "path", None)
            if not isinstance(raw_path, (str, Path)):
                raise OfflineCompositionError("resume requires a durable JSONL intent journal")
            self._recovered_intents = recover_execution_intents(raw_path, executor)
            recovered_results = tuple(
                result
                for intent in self._recovered_intents
                if (result := executor.result(intent.intent_id)) is not None
            )
            self._startup_reconciled_results = (
                self._reconcile(executor, recovered_results) if reconcile_at_start else recovered_results
            )
        del resume_source  # validated at the public boundary; only full prefix is supported
        start_index = 0
        if resume and coordinator.processor.events_processed:
            checkpoint_identity = resume_checkpoint.get("last_event_id") if resume_checkpoint else None
            if not checkpoint_identity:
                raise OfflineCompositionError("resume checkpoint lacks source event identity")
            coordinator.processor.last_event_id = str(checkpoint_identity)
            matches = [
                index
                for index, event in enumerate(events)
                if str(getattr(event, "event_id", "")) == str(checkpoint_identity)
            ]
            if len(matches) != 1:
                raise OfflineCompositionError(
                    "resume requires the complete source prefix containing the checkpoint event identity"
                )
            start_index = matches[0] + 1
            observed_events.extend(events[:start_index])
        for event in events[start_index:]:
            observed_events.append(event)
            self._current_time[0] = max(self._current_time[0], event.effective_available_at)
            coordinator.process(event)
        runtime_result = coordinator.advance(self._current_time[0], complete=capture_complete)
        if capture_complete:
            coordinator.finish(status="COMPLETED")
        else:
            coordinator.checkpoint()
        return consumer, coordinator, runtime_result

    def _require_resume_checkpoint(self, coordinator: RuntimeCoordinator) -> Mapping[str, Any]:
        checkpoint = self.store.get_checkpoint(
            coordinator.session_id,
            coordinator.checkpoint_name,
            analysis_id=coordinator.analysis_id,
            allow_alternate=False,
        )
        if checkpoint is None:
            raise OfflineCompositionError(
                "resume requires a compatible persisted checkpoint; it cannot degrade to a new analysis"
            )
        if str(checkpoint.get("session_id", "")) != coordinator.session_id:
            raise OfflineCompositionError("resume checkpoint session identity mismatch")
        if str(checkpoint.get("analysis_id", "")) != coordinator.analysis_id:
            raise OfflineCompositionError("resume checkpoint analysis identity mismatch")
        state = checkpoint.get("state")
        if not isinstance(state, Mapping) or str(state.get("config_hash", "")) != self.config.config_hash:
            raise OfflineCompositionError("resume checkpoint configuration identity mismatch")
        return checkpoint

    def _normalize_payloads(
        self,
        provider: CTraderProvider,
        payloads: Iterable[Any],
    ) -> tuple[list[Any], int, tuple[str, ...]]:
        issues: list[str] = []
        normalized_events: list[Any] = []
        rows = 0
        previous_generation: int | None = None
        for index, source in enumerate(payloads):
            if isinstance(source, CaptureEnvelope) and source.message_class is MessageClass.CONNECTION:
                raise OfflineCompositionError("connection control envelope requires reconciliation before ingest")
            if isinstance(source, CaptureEnvelope) and source.message_class not in {
                MessageClass.SPOT,
                MessageClass.REVISION,
            }:
                # END/connection/control envelopes are completion and session
                # evidence; they are not silently reinterpreted as SpotEvent.
                continue
            (
                payload,
                received_at,
                available_at,
                snapshot,
                sequence,
                generation,
            ) = _source_payload(
                source,
                index,
                self.clock,
                allow_synthetic_receipt=self.source_mode == "SYNTHETIC_FIXTURE",
            )
            if previous_generation is not None and generation != previous_generation:
                raise OfflineCompositionError(
                    "connection generation changed during DEMO ingest; explicit reconciliation is required"
                )
            previous_generation = generation
            result = provider.normalize_spot(
                payload,
                received_at=received_at,
                available_at=available_at,
                snapshot=snapshot,
                sequence=sequence,
                generation=generation,
            )
            rows += 1
            issues.extend(str(item) for item in result.issues)
            for event in result.quote_events:
                # CTraderProvider keeps its human-readable public quality
                # label in ``metadata[quality]``.  The core translation
                # contract expects a structured DataQuality mapping there;
                # preserve the label under source_quality instead of letting
                # the provider->runtime boundary reject every event.
                metadata = dict(event.metadata)
                raw_quality = metadata.get("quality")
                if raw_quality is not None and not isinstance(raw_quality, Mapping):
                    metadata["source_quality"] = raw_quality
                    metadata["quality"] = {
                        "flags": metadata.get("quality_flags", ()),
                        "reasons": metadata.get("quality_reasons", ()),
                        "source": "ctrader-open-api",
                    }
                metadata["source_mode"] = self.source_mode
                metadata["synthetic_fixture"] = self.source_mode == "SYNTHETIC_FIXTURE"
                normalized_events.append(dataclasses.replace(event, metadata=metadata))
        return normalized_events, rows, tuple(dict.fromkeys(issues))

    @staticmethod
    def _reconcile(executor: CTraderDemoExecutor, results: Iterable[OrderResult]) -> tuple[OrderResult, ...]:
        reconciled: list[OrderResult] = []
        for result in results:
            if result.state in {OrderState.UNKNOWN, OrderState.PARTIAL, OrderState.SUBMITTED, OrderState.CLOSE_PARTIAL}:
                reconciled.append(executor.reconcile(result.intent.intent_id))
            else:
                reconciled.append(result)
        return tuple(reconciled)

    @staticmethod
    def _close_positions(executor: CTraderDemoExecutor) -> tuple[OrderResult, ...]:
        results: list[OrderResult] = []
        for position in tuple(executor.positions()):
            results.append(executor.close_position(position))
        return tuple(results)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._started:
            self.client.close()

    def __enter__(self) -> CTraderDemoApplication:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


class CTraderDemoOfflineComposition:
    """Fixture wrapper that instantiates the generic application service."""

    def __init__(
        self,
        store: SQLiteStore,
        config: EffectiveConfig,
        *,
        start: datetime = DEFAULT_START,
        count: int = DEFAULT_COUNT,
        symbol_id: int = DEFAULT_SYMBOL_ID,
        account_id: int = DEFAULT_ACCOUNT_ID,
        intent_store: IntentStore | None = None,
        intent_journal_path: str | Path | None = None,
    ) -> None:
        if intent_store is not None and intent_journal_path is not None:
            raise ValueError("provide intent_store or intent_journal_path, not both")
        if count < 1_000:
            raise ValueError("the default strategy fixture requires at least 1000 M1 observations")
        self.store = store
        self.config = config
        self.fixture = synthetic_demo_market_fixture(start=start, count=count, symbol_id=symbol_id)
        self.account_id = int(account_id)
        self._owned_intent_store = intent_store is None
        if intent_store is not None:
            self.intent_store = intent_store
        else:
            target = (
                Path(intent_journal_path)
                if intent_journal_path is not None
                else Path(os.environ.get("MTF_LAB_STATE_DIR", "~/.local/state/mtf-lab")).expanduser()
                / "ctrader-demo-intents.jsonl"
            )
            self.intent_store = JsonlIntentStore(target)
        self.application: CTraderDemoApplication | None = None
        self.server: OfflineDemoServer | None = None
        self.wire_transport: LoopbackProtobufTransport | None = None

    def run(self) -> DemoCompositionResult:
        try:
            return self._run_fixture()
        except BaseException:
            self.close()
            raise

    def _run_fixture(self) -> DemoCompositionResult:
        proto = _load_official_proto()
        model = importlib.import_module("mtf_lab.data.protobuf_generated.OpenApiModelMessages_pb2")
        server = OfflineDemoServer(proto, model, fixture=self.fixture, account_id=self.account_id)
        codec = SdkProtobufCodec()
        wire_transport = LoopbackProtobufTransport(codec, server.handle)
        client_config = CTraderConfig(
            environment="demo",
            symbol="EUR/USD",
            symbol_id=None,
            account_id=None,
            client_id="offline-fixture-client",
            client_secret_ref="fixture-client-secret-ref",
            access_token_ref="fixture-access-token-ref",
            request_timeout_seconds=1.0,
            heartbeat_seconds=60.0,
            request_rate_limit=1_000_000.0,
            historical_rate_limit=1_000_000.0,
        )
        client = CTraderClient(
            client_config,
            transport=wire_transport,
            codec=codec,
            wall_clock=lambda: self.fixture.start,
        )
        secret_refs: list[str] = []
        token_refs: list[str] = []

        def secret_provider(reference: str) -> str:
            secret_refs.append(str(reference))
            return "offline-fixture-value"

        def token_provider(reference: str) -> str:
            token_refs.append(str(reference))
            return "offline-fixture-value"

        application = CTraderDemoApplication(
            self.store,
            self.config,
            client=client,
            secret_provider=secret_provider,
            token_provider=token_provider,
            selected_account_id=self.account_id,
            intent_store=self.intent_store,
            policy=ExecutionPolicy(
                max_quantity=DecimalValue("1"),
                fixed_quantity=DecimalValue("1"),
                max_positions=1,
                max_exposure=DecimalValue("10000"),
                max_spread=DecimalValue("0.01"),
                max_price_age_seconds=DecimalValue("5"),
                allowed_symbols=frozenset({"EUR/USD"}),
                timeout_seconds=DecimalValue("1"),
            ),
            activate=True,
            proto=proto,
            clock=lambda: self.fixture.start,
            market_clock=lambda: self.fixture.end + timedelta(minutes=1),
            source_mode="SYNTHETIC_FIXTURE",
        )
        self.application = application
        self.server = server
        self.wire_transport = wire_transport
        app_result = application.run(
            self.fixture.rows,
            session_id="ctrader-demo-composition",
            history_count=len(self.fixture.trendbars),
            history_from=self.fixture.start,
            history_to=self.fixture.end,
            capture_complete=True,
            reconcile=True,
            close_positions=True,
            subscribe=True,
            completion_evidence=self.fixture.capture.coverage.to_dict(),
        )
        open_result = app_result.execution_results[0] if app_result.execution_results else _missing_result("open")
        reconciled = app_result.reconciled_results[0] if app_result.reconciled_results else open_result
        close_result = app_result.close_results[0] if app_result.close_results else _missing_result("close")
        report = dict(app_result.report)
        report["credential_provider_calls"] = len(secret_refs) + len(token_refs)
        return DemoCompositionResult(
            report,
            self.fixture,
            client,
            wire_transport,
            server,
            app_result.provider,
            app_result,
            app_result.gateway,
            app_result.observation,
            app_result.demo_transport,
            app_result.executor,
            self.intent_store,
            open_result,
            reconciled,
            close_result,
        )

    def close(self) -> None:
        if self.application is not None:
            self.application.close()
            self.application = None
        if self._owned_intent_store:
            close = getattr(self.intent_store, "close", None)
            if callable(close):
                close()

    def __enter__(self) -> CTraderDemoOfflineComposition:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def run_offline_demo_composition(
    store: SQLiteStore,
    config: EffectiveConfig,
    *,
    intent_journal_path: str | Path,
    start: datetime = DEFAULT_START,
    count: int = DEFAULT_COUNT,
    symbol_id: int = DEFAULT_SYMBOL_ID,
    account_id: int = DEFAULT_ACCOUNT_ID,
) -> DemoCompositionResult:
    """Run the fixture and close client/journal resources before returning."""

    with CTraderDemoOfflineComposition(
        store,
        config,
        start=start,
        count=count,
        symbol_id=symbol_id,
        account_id=account_id,
        intent_journal_path=intent_journal_path,
    ) as composition:
        return composition.run()


def recover_execution_intents(
    path: str | Path,
    executor: CTraderDemoExecutor,
) -> tuple[ExecutionIntent, ...]:
    """Hydrate journaled intents and conservative last-known outcomes.

    Recovery never calls transport.submit. Every intent without a trustworthy
    update is restored as UNKNOWN so the executor's existing risk gate blocks
    subsequent opens until an explicit reconciliation resolves it. Repeated
    identical intent records are idempotent; conflicting payloads are a hard
    error.
    """
    intents, updates = _read_intent_journal(path)
    recovered: list[ExecutionIntent] = []
    for intent_id, intent in intents.items():
        result = _result_from_journal_update(intent, updates.get(intent_id))
        executor.restore_intent(intent, result=result)
        recovered.append(intent)
    executor.complete_recovery()
    return tuple(recovered)


def _read_intent_journal(
    path: str | Path,
) -> tuple[dict[str, ExecutionIntent], dict[str, Mapping[str, Any]]]:
    target = Path(path)
    try:
        lines = target.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise OfflineCompositionError(f"cannot read intent journal: {target.name}") from exc
    intents: dict[str, ExecutionIntent] = {}
    intent_payloads: dict[str, dict[str, Any]] = {}
    updates: dict[str, Mapping[str, Any]] = {}
    for line_number, line in enumerate(lines, 1):
        record = _decode_journal_line(line, line_number)
        if record is None:
            continue
        journal_type = str(record.get("journal_type", ""))
        if journal_type == "intent":
            intent = _intent_from_journal_record(record, line_number)
            canonical = intent.to_dict()
            previous = intent_payloads.get(intent.intent_id)
            if previous is not None and previous != canonical:
                raise OfflineCompositionError(f"conflicting duplicate intent payload at journal line {line_number}")
            if previous is None:
                intents[intent.intent_id] = intent
                intent_payloads[intent.intent_id] = canonical
        elif journal_type == "update":
            raw_id = str(record.get("intent_id", "")).strip()
            if not raw_id:
                raise OfflineCompositionError(f"update without intent_id at journal line {line_number}")
            updates[raw_id] = dict(record)
    unknown_updates = set(updates) - set(intents)
    if unknown_updates:
        raise OfflineCompositionError(f"journal update references unknown intent: {sorted(unknown_updates)}")
    return intents, updates


def _decode_journal_line(
    line: str,
    line_number: int,
) -> Mapping[str, Any] | None:
    if not line.strip():
        return None
    try:
        record = json.loads(line)
    except json.JSONDecodeError as exc:
        raise OfflineCompositionError(f"invalid intent journal line {line_number}") from exc
    if not isinstance(record, Mapping):
        raise OfflineCompositionError(f"invalid intent journal line {line_number}")
    return record


def _intent_from_journal_record(
    record: Mapping[str, Any],
    line_number: int,
) -> ExecutionIntent:
    try:
        return ExecutionIntent(
            str(record["intent_id"]),
            str(record["signal_id"]),
            str(record["symbol"]),
            Side.parse(record["side"]),
            record["quantity"],
            record["requested_price"],
            record["created_at"],
            str(record["account_id"]),
            str(record.get("kind", "OPEN")),
            record.get("position_id"),
            record.get("metadata", {}),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise OfflineCompositionError(f"invalid intent journal line {line_number}") from exc


def _result_from_journal_update(
    intent: ExecutionIntent,
    update: Mapping[str, Any] | None,
) -> OrderResult:
    if update is None:
        return OrderResult(
            intent,
            OrderState.UNKNOWN,
            unknown_reason="RECOVERY_NO_OBSERVED_OUTCOME",
            uncertainty_reason="RECOVERY",
        )
    try:
        raw_state = str(update.get("state", "UNKNOWN")).upper()
        state = OrderState(raw_state)
    except ValueError as exc:
        raise OfflineCompositionError(f"invalid state in update for intent {intent.intent_id}") from exc
    if state is OrderState.INTENT_RECORDED:
        state = OrderState.UNKNOWN
    raw_fills = update.get("fills", ())
    if not isinstance(raw_fills, (list, tuple)):
        raise OfflineCompositionError(f"invalid fills in update for intent {intent.intent_id}")
    fills: list[Fill] = []
    try:
        for raw_fill in raw_fills:
            if not isinstance(raw_fill, Mapping):
                raise TypeError("fill must be a mapping")
            fills.append(
                Fill(
                    str(raw_fill["fill_id"]),
                    raw_fill["quantity"],
                    raw_fill["price"],
                    raw_fill["timestamp"],
                )
            )
        position_ids = tuple(str(item) for item in update.get("position_ids", ()))
        result = OrderResult(
            intent,
            state,
            str(update["order_id"]) if update.get("order_id") is not None else None,
            update.get("filled_quantity", "0"),
            tuple(fills),
            str(update["reject_reason"]) if update.get("reject_reason") is not None else None,
            position_ids,
            str(update["unknown_reason"]) if update.get("unknown_reason") is not None else None,
            bool(update.get("reconciled", False)),
            str(update["uncertainty_reason"]) if update.get("uncertainty_reason") is not None else None,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise OfflineCompositionError(f"invalid outcome update for intent {intent.intent_id}") from exc
    if state in {OrderState.FILLED, OrderState.CLOSED} and result.order_id is None and not result.fills:
        result.state = OrderState.UNKNOWN
        result.unknown_reason = "RECOVERY_TERMINAL_EVIDENCE_INCOMPLETE"
        result.uncertainty_reason = "RECOVERY"
    return result


def _quote_for_observed_signal(
    observed_events: Iterable[Any],
    signal: Signal,
    *,
    session_id: str | None = None,
    connection_generation: str | int | None = None,
    synthetic: bool = False,
) -> Quote:
    candidates: list[Any] = []
    for event in observed_events:
        if event.event_time > signal.trigger_end:
            continue
        cfd_quote = spot_event_to_cfd_quote(event)
        if cfd_quote.bid is not None and cfd_quote.ask is not None:
            candidates.append(cfd_quote)
    if not candidates:
        raise OfflineCompositionError("signal has no preceding complete bid/ask quote")
    selected = max(candidates, key=lambda item: (item.market_time, str(item.quote_id)))
    quality = getattr(selected.quality, "value", selected.quality)
    return Quote(
        selected.instrument,
        selected.bid,
        selected.ask,
        selected.market_time,
        selected.available_at,
        quality=str(quality),
        source=selected.source,
        base_price="bid_ask",
        source_identity=selected.quote_id or selected.metadata.get("source_event_id"),
        session_id=session_id,
        connection_generation=connection_generation or selected.connection_generation,
        data_mode="SYNTHETIC" if synthetic else "LIVE",
        synthetic=synthetic,
    )


def _source_payload(
    source: Any,
    index: int,
    clock: Callable[[], datetime],
    *,
    allow_synthetic_receipt: bool = True,
) -> tuple[Any, datetime | None, datetime | None, bool, int | str, int]:
    if isinstance(source, CaptureEnvelope):
        return _capture_source_payload(source)
    if isinstance(source, WireMessage):
        return _wire_source_payload(source, index, clock, allow_synthetic_receipt=allow_synthetic_receipt)
    return _mapping_source_payload(source, index, clock, allow_synthetic_receipt=allow_synthetic_receipt)


def _capture_source_payload(
    source: CaptureEnvelope,
) -> tuple[Any, datetime | None, datetime | None, bool, int | str, int]:
    return (
        source.payload,
        source.received_at,
        source.available_at,
        bool(source.payload.get("snapshot", source.payload.get("isSnapshot", False))),
        source.ingest_sequence,
        source.connection_generation,
    )


def _wire_source_payload(
    source: WireMessage,
    index: int,
    clock: Callable[[], datetime],
    *,
    allow_synthetic_receipt: bool,
) -> tuple[Any, datetime | None, datetime | None, bool, int | str, int]:
    payload = source.payload
    if not allow_synthetic_receipt and (source.received_at is None or source.available_at is None):
        raise OfflineCompositionError("external source requires observed WireMessage receipt metadata")
    wire_received = source.received_at or _utc(clock(), "clock")
    wire_available = source.available_at or wire_received
    sequence = source.ingest_sequence if source.ingest_sequence is not None else index
    generation = source.connection_generation if source.connection_generation is not None else 0
    return (
        payload,
        wire_received,
        wire_available,
        bool(read_field(payload, "snapshot", "isSnapshot", default=False)),
        sequence,
        generation,
    )


def _mapping_source_payload(
    source: Any,
    index: int,
    clock: Callable[[], datetime],
    *,
    allow_synthetic_receipt: bool,
) -> tuple[Any, datetime | None, datetime | None, bool, int | str, int]:
    payload = source if isinstance(source, Mapping) else message_to_mapping(source)
    if not isinstance(payload, Mapping):
        raise OfflineCompositionError(f"source row {index} is not a mapping/protobuf message")
    raw_timestamp = read_field(payload, "timestamp", "timestamp_ms", default=None)
    event_time = _parse_provider_time(raw_timestamp)
    received_raw = read_field(payload, "received_at", "receivedAt", default=None)
    received: datetime | None = _parse_provider_time(received_raw) if received_raw is not None else None
    if received is None:
        if not allow_synthetic_receipt:
            raise OfflineCompositionError("external source requires observed received_at metadata")
        received = event_time + timedelta(seconds=1) if event_time is not None else _utc(clock(), "clock")
    available_raw = read_field(payload, "available_at", "availableAt", default=None)
    available: datetime | None = _parse_provider_time(available_raw) if available_raw is not None else received
    raw_sequence = read_field(payload, "sequence", "source_sequence", "ingest_sequence", default=index)
    if isinstance(raw_sequence, bool) or not isinstance(raw_sequence, (int, str)):
        raise OfflineCompositionError("source sequence must be an integer or non-empty string")
    if isinstance(raw_sequence, str) and not raw_sequence.strip():
        raise OfflineCompositionError("source sequence must not be empty")
    raw_generation = read_field(payload, "connection_generation", "generation", default=0)
    if isinstance(raw_generation, bool):
        raise OfflineCompositionError("source connection generation must be a nonnegative integer")
    try:
        generation = int(raw_generation)
    except (TypeError, ValueError) as exc:
        raise OfflineCompositionError("source connection generation must be an integer") from exc
    if generation < 0:
        raise OfflineCompositionError("source connection generation must be nonnegative")
    if not allow_synthetic_receipt and generation <= 0:
        raise OfflineCompositionError("external source requires a positive connection generation")
    return (
        payload,
        received,
        available,
        bool(read_field(payload, "snapshot", "isSnapshot", default=False)),
        raw_sequence,
        generation,
    )


def _parse_provider_time(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _utc(value, "provider timestamp")
    try:
        return datetime.fromtimestamp(float(value) / 1000.0, UTC)
    except (TypeError, ValueError, OverflowError) as exc:
        raise OfflineCompositionError("provider timestamp is not valid milliseconds") from exc


def _selected_demo_record(discovery: Iterable[Mapping[str, Any]], account_id: int) -> Mapping[str, Any]:
    records = tuple(discovery)
    for record in records:
        if int(record.get("account_id", 0)) == int(account_id):
            if str(record.get("environment", "")).upper() != "DEMO":
                raise OfflineCompositionError("selected account is not server-observed DEMO")
            return record
    raise OfflineCompositionError("selected account is absent from application-auth discovery")


def _is_durable_intent_store(store: IntentStore) -> bool:
    if not isinstance(store, JsonlIntentStore):
        return False
    raw_path = getattr(store, "path", None)
    if not isinstance(raw_path, Path):
        return False
    try:
        return raw_path.exists() and raw_path.is_file()
    except OSError:
        return False


def _stream_identity(stream_identity: str | None, events: Iterable[Any]) -> str:
    if stream_identity is None:
        return runtime_capture_hash(events)
    value = str(stream_identity).strip()
    if not value:
        raise OfflineCompositionError("stream_identity must not be empty")
    return value


def _completion_is_verified(evidence: Mapping[str, Any] | None) -> bool:
    if not isinstance(evidence, Mapping):
        return False
    continuity = str(evidence.get("continuity", "")).upper()
    end_declared = evidence.get("dataset_end_declared") is True
    end_seen = evidence.get("end_seen") is True or evidence.get("coverage_satisfied") is True
    return continuity in {"CONTINUOUS", "VERIFIED", "RECOVERED", "RECOVERED_BOUNDED"} and end_declared and end_seen


def _application_report(
    application: CTraderDemoApplication,
    *,
    provider: CTraderProvider,
    catalog: Any,
    history: Any,
    subscriptions: Iterable[Any],
    discovery: Iterable[Mapping[str, Any]],
    proof: Any,
    observation: ServerAccountObservation,
    server_rows: int,
    normalization_issues: Iterable[str],
    coordinator: RuntimeCoordinator,
    consumer: DemoExecutionSignalConsumer,
    execution_results: Iterable[OrderResult],
    reconciled_results: Iterable[OrderResult],
    close_results: Iterable[OrderResult],
    capture_complete: bool,
    completion_evidence: Mapping[str, Any] | None,
) -> dict[str, Any]:
    execution_tuple = tuple(execution_results)
    reconciled_tuple = tuple(reconciled_results)
    close_tuple = tuple(close_results)
    recovered_ids = {intent.intent_id for intent in application._recovered_intents}
    intent_ids = {result.intent.intent_id for result in (*execution_tuple, *close_tuple)} | recovered_ids
    receipt = _intent_receipt(application.intent_store, intent_ids)
    wire_client = getattr(application.client, "transport", None)
    sent = getattr(wire_client, "sent", ())
    received = getattr(wire_client, "received", ())
    names = tuple(_message_class_name(item) for item in sent)
    response_names = tuple(_message_class_name(item) for item in received)
    local_loopback = isinstance(wire_client, LoopbackProtobufTransport)
    network_performed: bool | None = False if local_loopback else None
    external_account_consulted: bool | None = False if local_loopback else None
    return {
        "composition_version": OFFLINE_COMPOSITION_VERSION,
        "network_performed": network_performed,
        "oauth_real_executed": False if local_loopback else None,
        "external_account_consulted": external_account_consulted,
        "boundary": "LOCAL_LOOPBACK" if local_loopback else "UNVERIFIED_EXTERNAL",
        "capture_complete": capture_complete,
        "completion_evidence_verified": _completion_is_verified(completion_evidence),
        "resume": {
            "input_mode": "full_prefix",
            "recovered_intent_ids": sorted(recovered_ids),
            "startup_reconciled_ids": sorted(
                result.intent.intent_id for result in application._startup_reconciled_results
            ),
        },
        "source_mode": application.source_mode,
        "fixture_is_synthetic": application.source_mode == "SYNTHETIC_FIXTURE",
        "fixture_rows": server_rows,
        "stream_identity": coordinator.dataset_hash,
        "normalization_issues": sorted(set(str(item) for item in normalization_issues)),
        "stages": {
            "credential_refs_injected": {"state": "A", "resolved": True},
            "application_auth": {
                "state": "A",
                "message": "ProtoOAApplicationAuthReq" if "ProtoOAApplicationAuthReq" in names else None,
            },
            "account_discovery": {"state": "A", "accounts": len(tuple(discovery))},
            "demo_selection": {"state": "A", "account_id": int(application.selected_account_id)},
            "account_auth": {
                "state": "A",
                "message": "ProtoOAAccountAuthReq" if "ProtoOAAccountAuthReq" in names else None,
            },
            "symbol_catalog": {"state": "A", "selected": catalog.selected.to_dict() if catalog.selected else None},
            "market_data": {
                "state": "A",
                "history_bars": len(history.bars),
                "subscriptions": len(tuple(subscriptions)),
                "provider": provider.name,
            },
            "runtime_coordinator": {
                "state": "A",
                "analysis_id": coordinator.analysis_id,
                "signals": len(coordinator.signals),
                "consumer_type": consumer.consumer_type,
                "signals_consumed": consumer.signal_count,
            },
            "risk_safety_gates": {
                "state": "A" if application.activate and consumer.signal_count else "BLOCKED",
                "activated": application.activate,
                "signals_seen": consumer.signal_count,
            },
            "durable_intent": {
                "state": "A" if receipt["durable"] and receipt["intent_records"] else "UNVERIFIED",
                "durable": receipt["durable"],
                "intent_records": receipt["intent_records"],
                "event_records": receipt["event_records"],
                "intent_ids": sorted(intent_ids),
            },
            "new_order": {
                "state": "A" if execution_tuple else "BLOCKED",
                "message": "ProtoOANewOrderReq" if "ProtoOANewOrderReq" in names else None,
                "results": [result.state.value for result in execution_tuple],
            },
            "execution_events": {
                "state": "A" if "ProtoOAExecutionEvent" in response_names else "BLOCKED",
                "message": "ProtoOAExecutionEvent",
            },
            "reconcile": {
                "state": "A" if reconciled_tuple and "ProtoOAReconcileReq" in names else "BLOCKED",
                "message": "ProtoOAReconcileReq" if "ProtoOAReconcileReq" in names else None,
                "results": [result.state.value for result in reconciled_tuple],
                "resolved_terminal": bool(reconciled_tuple)
                and all(
                    result.state
                    not in {OrderState.UNKNOWN, OrderState.PARTIAL, OrderState.SUBMITTED, OrderState.CLOSE_PARTIAL}
                    for result in reconciled_tuple
                ),
            },
            "close_position": {
                "state": "A"
                if close_tuple and all(result.state is OrderState.CLOSED for result in close_tuple)
                else "UNRESOLVED",
                "message": "ProtoOAClosePositionReq" if "ProtoOAClosePositionReq" in names else None,
                "results": [result.state.value for result in close_tuple],
                "resolved_terminal": bool(close_tuple)
                and all(result.state is OrderState.CLOSED for result in close_tuple),
            },
            "persistence_reporting": {
                "state": "A" if receipt["durable"] else "UNVERIFIED",
                "sqlite_session_id": coordinator.session_id,
                "analysis_id": coordinator.analysis_id,
                "signals": len(coordinator.signals),
                "intent_records": receipt["intent_records"],
            },
        },
        "wire": {
            "codec": "SdkProtobufCodec",
            "loopback_only": True,
            "payload_names": list(names),
            "response_payload_names": list(response_names),
            "new_order_count": names.count("ProtoOANewOrderReq"),
            "close_count": names.count("ProtoOAClosePositionReq"),
            "reconcile_count": names.count("ProtoOAReconcileReq"),
        },
        "session_proof": {
            "account_id": int(proof.account_id),
            "environment": proof.environment,
            "endpoint": proof.endpoint,
            "scopes": sorted(proof.scopes),
            "connection_generation": str(proof.connection_generation),
            "origin": "local_loopback_fixture" if local_loopback else "authenticated_client",
            "server_observed_external": False if local_loopback else None,
            "observation_source": observation.source,
        },
        "external_validation": {
            "state": "B",
            "pending": [
                "real OAuth/application approval",
                "server account discovery and explicit DEMO selection",
                "broker catalog/conditions and an authorized DEMO route",
            ],
        },
        "user_intervention": {
            "state": "C",
            "pending": [
                "register/approve cTrader application",
                "provide CTRADER_CLIENT_ID and CTRADER_CLIENT_SECRET through a secret provider",
                "complete OAuth and discover/select the actual DEMO account",
                "verify Pepperstone contractual, storage, and cost conditions for Mexico",
            ],
        },
    }


def _intent_receipt(store: IntentStore, intent_ids: set[str]) -> dict[str, Any]:
    raw_path = getattr(store, "path", None)
    if not isinstance(raw_path, (str, Path)):
        return {"durable": False, "intent_records": 0, "event_records": 0}
    path = Path(raw_path)
    durable = path.exists()
    intent_records = 0
    event_records = 0
    if durable:
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            lines = []
        for line in lines:
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if str(record.get("intent_id", "")) not in intent_ids:
                continue
            if record.get("journal_type") == "intent":
                intent_records += 1
            elif record.get("journal_type") in {"event", "update"}:
                event_records += 1
    return {"durable": bool(durable), "intent_records": intent_records, "event_records": event_records}


def _message_class_name(message: Any) -> str:
    return type(getattr(message, "payload", None)).__name__


def _parse_optional_time(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).replace("Z", "+00:00")
    return _utc(datetime.fromisoformat(text), "checkpoint time")


def _missing_result(kind: str) -> NoReturn:
    raise OfflineCompositionError(f"fixture did not produce a {kind} execution result")


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z") if value is not None else None


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} requires a timezone")
    return value.astimezone(UTC)


def synthetic_demo_market_fixture(
    *,
    start: datetime = DEFAULT_START,
    count: int = DEFAULT_COUNT,
    symbol_id: int = DEFAULT_SYMBOL_ID,
) -> DemoMarketFixture:
    """Build a causal, complete synthetic stream for the unchanged strategy.

    The first 900 points establish a stable bullish context.  The following
    60 points trend, 15 points pull back near EMA20, five points pause, and the
    next five points cross back above EMA20.  These are fixture mechanics, not
    strategy parameters or a performance claim.
    """

    if count < 1_000:
        raise ValueError("synthetic DEMO fixture requires at least 1000 M1 observations")
    start = _utc(start, "start")
    price = 1.10000
    closes: list[float] = []
    for index in range(count):
        if index < 900:
            step = 0.00002
        elif index < 960:
            step = 0.00008
        elif index < 975:
            step = -0.00017
        elif index < 980:
            step = 0.0
        elif index < 985:
            step = 0.00080
        else:
            step = 0.00002
        price += step
        closes.append(price)

    rows: list[Mapping[str, Any]] = []
    trendbars: list[Mapping[str, Any]] = []
    scale = 100_000
    for index, close in enumerate(closes):
        event_time = start + timedelta(minutes=index)
        previous = closes[index - 1] if index else close
        low = min(previous, close) - 0.00010
        high = max(previous, close) + 0.00010
        trendbars.append(
            synthetic_trendbar(
                timestamp_minutes=int(event_time.timestamp() // 60),
                period="M1",
                low_relative=round(low * scale),
                delta_open=round(previous * scale) - round(low * scale),
                delta_close=round(close * scale) - round(low * scale),
                delta_high=round(high * scale) - round(low * scale),
                volume=42,
            )
        )
        rows.append(
            synthetic_spot_event(
                timestamp_ms=int(event_time.timestamp() * 1000),
                symbol_id=symbol_id,
                bid_relative=round((close - 0.00010) * scale),
                ask_relative=round((close + 0.00010) * scale),
                snapshot=index == 0,
            )
        )
    end = start + timedelta(minutes=count)

    def receipt(index: int, raw: Mapping[str, Any]) -> datetime:
        del index
        return _utc(datetime.fromtimestamp(float(raw["timestamp"]) / 1000.0, UTC), "receipt") + timedelta(seconds=1)

    envelopes = list(capture_envelopes(rows, received_at=receipt))
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
                "requested_start": (start + timedelta(seconds=1)).isoformat().replace("+00:00", "Z"),
                "requested_end": end.isoformat().replace("+00:00", "Z"),
            },
        )
    )
    coverage = CaptureCoverage(
        requested_start=start + timedelta(seconds=1),
        requested_end=end,
        dataset_end_declared=True,
        continuity="CONTINUOUS",
    )
    capture = normalize_ctrader_capture(
        envelopes,
        spec=CTraderInstrumentSpec(symbol="EUR/USD", symbol_id=symbol_id, digits=5, pip_position=4),
        quote_basis="mid",
        mode="REPLAY",
        coverage=coverage,
    )
    return DemoMarketFixture(start, end, symbol_id, tuple(rows), tuple(trendbars), capture)


def _load_official_proto() -> Any:
    try:
        return importlib.import_module("mtf_lab.data.protobuf_generated.OpenApiMessages_pb2")
    except Exception as exc:  # pragma: no cover - optional dependency
        raise OfflineCompositionError("official cTrader generated protobuf module is unavailable") from exc


__all__ = [
    "CTraderDemoApplication",
    "CTraderDemoApplicationResult",
    "CTraderDemoOfflineComposition",
    "DEFAULT_ACCOUNT_ID",
    "DEFAULT_COUNT",
    "DEFAULT_START",
    "DEFAULT_SYMBOL_ID",
    "DemoCompositionResult",
    "DemoExecutionSignalConsumer",
    "DemoMarketFixture",
    "LoopbackProtobufTransport",
    "OFFLINE_COMPOSITION_VERSION",
    "OfflineCompositionError",
    "OfflineDemoServer",
    "recover_execution_intents",
    "run_offline_demo_composition",
    "synthetic_demo_market_fixture",
]
