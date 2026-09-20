"""Single-reader cTrader session and authentication boundary.

The session owns the lifecycle of one transport connection.  Every inbound
byte is consumed by the receive pump; request callers only wait on their
private correlation queue.  All sends, including heartbeat replies, pass
through one lock.  A new connection receives a new generation and a new stop
event, so an old reader cannot publish into the new session.
"""

from __future__ import annotations

import contextlib
import queue
import secrets
import threading
import time
import uuid
from collections import deque
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any, Protocol

from .ctrader_accounts import normalize_account_payload
from .ctrader_config import CTraderConfig
from .ctrader_errors import (
    AuthState,
    ConnectionState,
    CTraderAuthError,
    CTraderConfigurationError,
    CTraderDependencyError,
    CTraderProtocolError,
    CTraderRequestCancelled,
    CTraderRequestTimeout,
    CTraderTransportError,
    DependencyState,
    RequestPhase,
)
from .ctrader_protocol import (
    PAYLOAD,
    SESSION_CONTROL_PAYLOAD_TYPES,
    SESSION_CONTROL_TYPE_NAMES,
    CTraderCodec,
    DependencyReport,
    WireMessage,
    dependency_report,
    message_payload_type,
    read_field,
    read_repeated,
)
from .ctrader_transport import (
    CTraderRateLimiter,
    CTraderTransport,
    TcpTlsTransport,
)

# Authentication responses are an explicit protocol allowlist.  Do not infer
# these pairs arithmetically: cTrader has unrelated payloads with nearby ids,
# and the response type is part of the authentication proof boundary.
_AUTH_RESPONSE_TYPES = {
    PAYLOAD["PROTO_OA_APPLICATION_AUTH_REQ"]: PAYLOAD["PROTO_OA_APPLICATION_AUTH_RES"],
    PAYLOAD["PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ"]: PAYLOAD["PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_RES"],
    PAYLOAD["PROTO_OA_ACCOUNT_AUTH_REQ"]: PAYLOAD["PROTO_OA_ACCOUNT_AUTH_RES"],
}


class Clock(Protocol):
    """Monotonic clock used for I/O deadlines and heartbeat scheduling."""

    def __call__(self) -> float: ...


class WallClock(Protocol):
    """UTC wall clock used only for observable receipt/status metadata."""

    def __call__(self) -> datetime: ...


class Scheduler(Protocol):
    """Injectable wait primitive used by the reader without ``sleep`` calls."""

    def wait(self, event: threading.Event, timeout: float) -> bool: ...


class EventScheduler:
    """Production scheduler backed by an interruptible Event wait."""

    def wait(self, event: threading.Event, timeout: float) -> bool:
        return event.wait(timeout)


class CancellationToken:
    def __init__(self) -> None:
        self._event = threading.Event()

    def cancel(self) -> None:
        self._event.set()

    @property
    def cancelled(self) -> bool:
        return self._event.is_set()


@dataclass(slots=True)
class RequestRecord:
    request_id: str
    payload_type: str
    timeout_seconds: float
    sent_at: datetime | None = None
    generation: int = 0
    phase: RequestPhase = RequestPhase.CREATED


@dataclass(frozen=True, slots=True)
class AuthenticatedSessionEvidence:
    """Identity-bound proof minted only after server account authentication."""

    account_id: int
    environment: str
    endpoint: str
    scopes: frozenset[str]
    session_id: str
    connection_generation: str
    authenticated_at: datetime
    expires_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "account_id": self.account_id,
            "environment": self.environment,
            "endpoint": self.endpoint,
            "scopes": sorted(self.scopes),
            "session_id": self.session_id,
            "connection_generation": self.connection_generation,
            "authenticated_at": _iso(self.authenticated_at),
            "expires_at": _iso(self.expires_at),
            "server_observed": True,
        }


@dataclass(frozen=True, slots=True)
class CTraderStatus:
    connection: ConnectionState = ConnectionState.DISCONNECTED
    dependency: DependencyState = DependencyState.OPTIONAL
    auth: AuthState = AuthState.NOT_CONFIGURED
    action: str = ""
    last_error: str | None = None
    reconnect_attempt: int = 0
    last_message_at: datetime | None = None
    last_event_at: datetime | None = None
    dropped_messages: int = 0
    queue_size: int = 0
    heartbeat_count: int = 0
    pending_requests: int = 0
    needs_reconciliation: bool = False
    generation: int = 0
    discontinuity_reason: str | None = None
    last_request_phase: RequestPhase | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "connection": self.connection.value,
            "dependency": self.dependency.value,
            "auth": self.auth.value,
            "action": self.action,
            "last_error": self.last_error,
            "reconnect_attempt": self.reconnect_attempt,
            "last_message_at": _iso(self.last_message_at),
            "last_event_at": _iso(self.last_event_at),
            "dropped_messages": self.dropped_messages,
            "queue_size": self.queue_size,
            "heartbeat_count": self.heartbeat_count,
            "pending_requests": self.pending_requests,
            "needs_reconciliation": self.needs_reconciliation,
            "generation": self.generation,
            "discontinuity_reason": self.discontinuity_reason,
            "last_request_phase": self.last_request_phase.value if self.last_request_phase else None,
        }


@dataclass(frozen=True, slots=True)
class _PendingFailure:
    error: BaseException


@dataclass(slots=True)
class _PendingRequest:
    record: RequestRecord
    response_queue: queue.Queue[WireMessage | _PendingFailure]


def replace_status(status: CTraderStatus, **changes: Any) -> CTraderStatus:
    return replace(status, **changes)


class CTraderClient:
    """Correlating client with one receive reader per connection generation."""

    def __init__(
        self,
        config: CTraderConfig | Mapping[str, Any] | None = None,
        *,
        transport: CTraderTransport | None = None,
        codec: CTraderCodec | None = None,
        clock: Clock = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        wall_clock: WallClock | None = None,
        scheduler: Scheduler | None = None,
        rate_limiter: CTraderRateLimiter | None = None,
        next_ingest_sequence: int = 0,
        initial_generation: int = 0,
    ) -> None:
        _validate_resume_counter(next_ingest_sequence, "next_ingest_sequence")
        _validate_resume_counter(initial_generation, "initial_generation")
        self.config = config if isinstance(config, CTraderConfig) else CTraderConfig.from_mapping(config)
        self._clock = clock
        self._sleep = sleep
        self._wall_clock = wall_clock or (lambda: datetime.now(UTC))
        self._scheduler = scheduler or EventScheduler()
        self._rate_limiter = rate_limiter or CTraderRateLimiter(
            request_rate=self.config.request_rate_limit,
            historical_rate=self.config.historical_rate_limit,
            clock=clock,
            sleep=sleep,
        )
        if transport is None:
            if codec is None:
                try:
                    from .ctrader_protocol import SdkProtobufCodec

                    codec = SdkProtobufCodec()
                except CTraderDependencyError:
                    codec = None
            transport = TcpTlsTransport(
                self.config.host or "",
                self.config.port,
                codec=codec,
                timeout_seconds=self.config.request_timeout_seconds,
                monotonic=clock,
            )
        self.transport = transport
        self._event_queue: queue.Queue[WireMessage] = queue.Queue(maxsize=self.config.queue_maxsize)
        # Control messages are never evicted to preserve authorization,
        # execution, error, and discontinuity evidence.  Market data remains
        # bounded and a dropped item makes reconciliation explicit.
        self._control_queue: queue.Queue[WireMessage] = queue.Queue(maxsize=self.config.queue_maxsize)
        self._requests: dict[str, RequestRecord] = {}
        self._pending: dict[str, _PendingRequest] = {}
        self._request_counter = 0
        self._state_lock = threading.RLock()
        self._send_lock = threading.RLock()
        self._lifecycle_lock = threading.RLock()
        self._reader_thread: threading.Thread | None = None
        self._reader_stop: threading.Event | None = None
        self._reader_generation: int | None = None
        self._generation = initial_generation
        self._last_heartbeat_monotonic = self._clock()
        self._recent_request_phases: deque[RequestPhase] = deque(maxlen=64)
        self._retired_request_ids: deque[str] = deque(maxlen=512)
        self._retired_request_id_set: set[str] = set()
        self._ingest_sequence = next_ingest_sequence
        self._account_discovery: dict[str, Any] | None = None
        self._session_evidence: AuthenticatedSessionEvidence | None = None
        self._application_authenticated = False
        self._authenticated_account_id: int | None = None
        dependency = (
            DependencyState.AVAILABLE
            if not isinstance(transport, TcpTlsTransport)
            else (DependencyState.AVAILABLE if codec is not None else DependencyState.MISSING)
        )
        self._status = CTraderStatus(
            dependency=dependency,
            auth=self._initial_auth_state(),
            generation=self._generation,
        )

    @property
    def generation(self) -> int:
        return self._generation

    @property
    def authenticated_account_id(self) -> int | None:
        return self._authenticated_account_id

    @property
    def discovered_accounts(self) -> tuple[dict[str, Any], ...]:
        if self._account_discovery is None:
            return ()
        return tuple(dict(record) for record in self._account_discovery.get("records", ()))

    def _initial_auth_state(self) -> AuthState:
        if self.config.auth_configured:
            return AuthState.READY
        if (
            self.config.client_id
            or self.config.client_secret_ref
            or self.config.access_token_ref
            or self.config.account_id
        ):
            return AuthState.REQUIRED
        return AuthState.NOT_CONFIGURED

    @property
    def status(self) -> CTraderStatus:
        with self._state_lock:
            return replace_status(
                self._status,
                queue_size=self._event_queue.qsize() + self._control_queue.qsize(),
                pending_requests=len(self._pending),
            )

    @property
    def dependency(self) -> DependencyReport:
        return dependency_report()

    def _set_status(self, **changes: Any) -> None:
        with self._state_lock:
            phase = changes.get("last_request_phase")
            if isinstance(phase, RequestPhase):
                self._recent_request_phases.append(phase)
            self._status = replace_status(self._status, **changes)

    def _wait(self, event: threading.Event, timeout: float) -> bool:
        wait = getattr(self._scheduler, "wait", None)
        if callable(wait):
            return bool(wait(event, max(0.0, timeout)))
        if callable(self._scheduler):
            return bool(self._scheduler(event, max(0.0, timeout)))
        raise TypeError("scheduler debe exponer wait(event, timeout)")

    def _next_request_id(self) -> str:
        with self._state_lock:
            self._request_counter += 1
            return f"ctrader-{self._request_counter:08d}"

    def _stop_reader(self, *, close_transport: bool) -> None:
        stop, thread = self._reader_stop, self._reader_thread
        if stop is not None:
            stop.set()
        if close_transport:
            with contextlib.suppress(Exception):
                self.transport.close()
        if thread is not None and thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=1.0)
            if thread.is_alive():
                # Never reuse a transport while an old reader could still
                # consume bytes from it.  The caller must repair the transport
                # boundary instead of creating a second concurrent reader.
                raise CTraderTransportError("el lector de la generación anterior no terminó; conexión no reutilizada")
        self._reader_stop = None
        self._reader_thread = None
        self._reader_generation = None

    def _start_reader(self, generation: int) -> None:
        stop = threading.Event()
        thread = threading.Thread(
            target=self._receive_pump,
            args=(generation, stop),
            name=f"mtf-ctrader-receive-{generation}",
            daemon=True,
        )
        self._reader_stop = stop
        self._reader_thread = thread
        self._reader_generation = generation
        thread.start()

    def _fail_pending(self, error: BaseException, *, generation: int | None = None) -> None:
        with self._state_lock:
            pending = tuple(
                item for item in self._pending.values() if generation is None or item.record.generation == generation
            )
        for item in pending:
            phase = (
                RequestPhase.FAILED_AFTER_SEND
                if item.record.phase is RequestPhase.SENT
                else RequestPhase.FAILED_BEFORE_SEND
            )
            item.record.phase = phase
            failure = _with_request_phase(error, phase)
            with contextlib.suppress(queue.Full):
                item.response_queue.put_nowait(_PendingFailure(failure))
                # The waiting request wakes from its deadline if the queue was
                # already occupied; the status still records the failure.

    def _on_reader_failure(self, generation: int, error: BaseException) -> None:
        if generation != self._generation:
            return
        auth_state = (
            AuthState.REQUIRED
            if self._status.auth is AuthState.AUTHENTICATED or self._application_authenticated
            else self._status.auth
        )
        self._session_evidence = None
        self._set_status(
            connection=ConnectionState.DISCONNECTED,
            auth=auth_state,
            needs_reconciliation=True,
            discontinuity_reason=type(error).__name__,
            last_error=str(error),
            action="Reconecte, reautentique y concilie el intervalo perdido",
        )
        self._fail_pending(error, generation=generation)

    def _stamp_message(self, message: WireMessage, generation: int) -> WireMessage:
        with self._state_lock:
            sequence = self._ingest_sequence
            self._ingest_sequence += 1
        received = message.received_at or self._wall_clock()
        available = message.available_at or received
        source_identity = message.source_identity or type(self.transport).__name__
        return message.with_capture_metadata(
            received_at=received,
            available_at=available,
            ingest_sequence=sequence,
            connection_generation=generation,
            source_identity=source_identity,
        )

    def _receive_pump(self, generation: int, stop: threading.Event) -> None:
        while not stop.is_set():
            try:
                self._maybe_heartbeat(generation=generation)
                message = self.transport.receive(0.10)
                if message is None:
                    self._wait(stop, 0.005)
                    continue
                if generation != self._generation or stop.is_set():
                    continue
                if message.connection_generation is not None and message.connection_generation != generation:
                    continue
                message = self._stamp_message(message, generation)
                now = message.received_at or self._wall_clock()
                self._set_status(last_message_at=now)
                if self._dispatch_inbound(message, generation, stop, now):
                    continue
                self._set_status(last_event_at=now)
                self._publish_event(message, stop=stop)
            except Exception as exc:
                if not stop.is_set():
                    self._on_reader_failure(generation, exc)
                return

    def _dispatch_inbound(
        self,
        message: WireMessage,
        generation: int,
        stop: threading.Event,
        now: datetime,
    ) -> bool:
        if self._invalidate_from_control(message, generation):
            return False
        if message.payload_type_id == PAYLOAD["PROTO_HEARTBEAT_EVENT"]:
            return self._handle_heartbeat_message(generation, stop)
        if message.client_msg_id and self._route_response(message, generation):
            return True
        if message.client_msg_id and self._is_retired_request(str(message.client_msg_id)):
            self._set_status(
                needs_reconciliation=True,
                discontinuity_reason="late_response",
                action="respuesta tardía de request vencida; concilie antes de continuar",
            )
            return True
        return False

    def _is_retired_request(self, request_id: str) -> bool:
        with self._state_lock:
            return str(request_id) in self._retired_request_id_set

    def _invalidate_from_control(self, message: WireMessage, generation: int) -> bool:
        kind = _session_control_kind(message)
        if kind is None or not self._control_targets_current(message.payload):
            return False
        self._session_evidence = None
        self._authenticated_account_id = None
        self._account_discovery = None
        if kind in {"CLIENT_DISCONNECT", "ACCOUNTS_TOKEN_INVALIDATED"}:
            self._application_authenticated = False
        self._set_status(
            auth=AuthState.INVALID if kind == "ACCOUNTS_TOKEN_INVALIDATED" else AuthState.REQUIRED,
            needs_reconciliation=True,
            discontinuity_reason=f"session_control:{kind}",
            action="Sesión invalidada por control del servidor; reautentique antes de continuar",
        )
        self._fail_pending(
            CTraderTransportError(
                f"sesión invalidada por {kind}",
                phase=RequestPhase.FAILED_AFTER_SEND,
            ),
            generation=generation,
        )
        return True

    def _control_targets_current(self, payload: Any) -> bool:
        account_id = self._authenticated_account_id
        if account_id is None:
            return False
        targets = _control_account_targets(payload)
        return targets is None or account_id in targets

    def _handle_heartbeat_message(self, generation: int, stop: threading.Event) -> bool:
        try:
            self.heartbeat(generation=generation)
        except Exception as exc:
            if not stop.is_set():
                self._on_reader_failure(generation, exc)
        return True

    def _route_response(self, message: WireMessage, generation: int) -> bool:
        with self._state_lock:
            pending = self._pending.get(str(message.client_msg_id))
        if pending is None or pending.record.generation != generation:
            return False
        try:
            pending.response_queue.put_nowait(message)
        except queue.Full:
            self._set_status(
                needs_reconciliation=True,
                discontinuity_reason="correlated_response_queue_full",
                action="respuesta correlacionada descartada; concilie",
            )
        return True

    def _connect_once(self, *, state: ConnectionState) -> CTraderStatus:
        if isinstance(self.transport, TcpTlsTransport):
            report = self.dependency
            if not report.available:
                dependency = (
                    report.codec_state if report.codec_state is not DependencyState.NOT_VERIFIED else report.sdk_state
                )
                self._set_status(
                    dependency=dependency,
                    connection=ConnectionState.FAILED,
                    action="Verifique el runtime de protobuf generado y las dependencias mantenidas; no se instaló automáticamente",
                )
                raise CTraderDependencyError(report.message or "SDK/codec cTrader no operativo")
        with self._lifecycle_lock:
            had_session = self._application_authenticated or self._status.auth is AuthState.AUTHENTICATED
            self._stop_reader(close_transport=True)
            self._fail_pending(CTraderTransportError("la conexión anterior fue reemplazada por una nueva generación"))
            self._set_status(connection=state, action="")
            try:
                self.transport.connect(self.config.request_timeout_seconds)
            except Exception as exc:
                self._set_status(
                    connection=ConnectionState.FAILED,
                    last_error=str(exc),
                    action="Verifique host/puerto/TLS o use DeterministicTransport",
                )
                raise
            self._generation += 1
            auth_state = AuthState.REQUIRED if had_session else self._status.auth
            if had_session:
                self._authenticated_account_id = None
                self._application_authenticated = False
                self._account_discovery = None
            self._session_evidence = None
            self._last_heartbeat_monotonic = self._clock()
            self._set_status(
                connection=ConnectionState.CONNECTED,
                auth=auth_state,
                last_error=None,
                reconnect_attempt=0,
                generation=self._generation,
                discontinuity_reason="new_connection_generation" if had_session else None,
                needs_reconciliation=bool(had_session),
                action="Reautentique y resuscriba si había sesión previa" if auth_state is AuthState.REQUIRED else "",
            )
            self._start_reader(self._generation)
            return self.status

    def connect(self) -> CTraderStatus:
        return self._connect_once(state=ConnectionState.CONNECTING)

    def connect_with_retry(self) -> CTraderStatus:
        if isinstance(self.transport, TcpTlsTransport):
            report = self.dependency
            if not report.available:
                dependency = (
                    report.codec_state if report.codec_state is not DependencyState.NOT_VERIFIED else report.sdk_state
                )
                self._set_status(
                    dependency=dependency,
                    connection=ConnectionState.FAILED,
                    action="Instale/verifique el extra opcional cTrader antes de reconectar",
                )
                raise CTraderDependencyError(report.message or "SDK/codec cTrader no operativo")
        last_error: Exception | None = None
        for attempt in range(1, self.config.max_reconnects + 1):
            try:
                self._set_status(
                    reconnect_attempt=attempt,
                    connection=ConnectionState.RECONNECTING if attempt > 1 else ConnectionState.CONNECTING,
                )
                return self._connect_once(
                    state=ConnectionState.RECONNECTING if attempt > 1 else ConnectionState.CONNECTING
                )
            except Exception as exc:
                last_error = exc
                self._set_status(
                    connection=ConnectionState.RECONNECTING,
                    last_error=str(exc),
                    action="Reintento programado; una reconexión no reconcilia eventos perdidos",
                )
                if attempt < self.config.max_reconnects:
                    delay = min(
                        self.config.reconnect_backoff_seconds * (2 ** (attempt - 1)),
                        self.config.reconnect_backoff_max_seconds,
                    )
                    self._sleep(delay)
        self._set_status(
            connection=ConnectionState.FAILED,
            action="Agotados reintentos; ejecute conciliación/backfill antes de habilitar análisis",
        )
        raise CTraderTransportError(
            f"no se pudo reconectar tras {self.config.max_reconnects} intentos: {last_error}"
        ) from last_error

    def close(self) -> None:
        with self._lifecycle_lock:
            self._stop_reader(close_transport=True)
            error = CTraderTransportError("sesión cTrader cerrada")
            self._fail_pending(error)
            with self._state_lock:
                self._requests.clear()
                self._pending.clear()
            self._authenticated_account_id = None
            self._application_authenticated = False
            self._account_discovery = None
            self._session_evidence = None
            self._set_status(connection=ConnectionState.CLOSED, pending_requests=0)

    def _send_serialized(self, message: WireMessage, *, generation: int | None = None) -> None:
        with self._send_lock:
            target_generation = self._generation if generation is None else generation
            if target_generation != self._generation or self._status.connection is not ConnectionState.CONNECTED:
                raise CTraderTransportError("conexión no disponible para esta generación")
            self.transport.send(message)

    def heartbeat(self, *, generation: int | None = None) -> None:
        with self._send_lock:
            target_generation = self._generation if generation is None else generation
            if target_generation != self._generation or self._status.connection is not ConnectionState.CONNECTED:
                raise CTraderTransportError("conexión no disponible para heartbeat")
            self.transport.send(WireMessage(PAYLOAD["PROTO_HEARTBEAT_EVENT"], None, None, True))
            self._last_heartbeat_monotonic = self._clock()
            self._set_status(heartbeat_count=self._status.heartbeat_count + 1)

    def _maybe_heartbeat(self, *, generation: int | None = None) -> None:
        target_generation = self._generation if generation is None else generation
        if target_generation != self._generation or self._status.connection is not ConnectionState.CONNECTED:
            return
        if self._clock() - self._last_heartbeat_monotonic < self.config.heartbeat_seconds:
            return
        self.heartbeat(generation=target_generation)

    def _publish_event(
        self,
        message: WireMessage,
        *,
        stop: threading.Event | None = None,
    ) -> None:
        if self._is_market_message(message):
            self._publish_market(message)
            return
        self._publish_control(message, stop=stop)

    @staticmethod
    def _is_market_message(message: WireMessage) -> bool:
        return message.payload_type_id in {
            PAYLOAD["PROTO_OA_SPOT_EVENT"],
            PAYLOAD["PROTO_OA_SUBSCRIBE_LIVE_TRENDBAR_RES"],
        }

    def _publish_control(
        self,
        message: WireMessage,
        *,
        stop: threading.Event | None,
    ) -> None:
        while True:
            try:
                self._control_queue.put(message, timeout=0.05)
                return
            except queue.Full:
                if stop is not None and stop.is_set():
                    return

    def _publish_market(self, message: WireMessage) -> None:
        try:
            self._event_queue.put_nowait(message)
            return
        except queue.Full:
            self._evict_market_message()
        with contextlib.suppress(queue.Full):
            self._event_queue.put_nowait(message)
        self._set_status(
            dropped_messages=self._status.dropped_messages + 1,
            needs_reconciliation=True,
            discontinuity_reason="market_backpressure",
            action="Cola de mercado llena; se descartó tráfico y requiere conciliación",
        )

    def _evict_market_message(self) -> None:
        with contextlib.suppress(queue.Empty):
            self._event_queue.get_nowait()

    def request(
        self,
        payload_type: int | str,
        payload: Mapping[str, Any] | Any = None,
        *,
        timeout_seconds: float | None = None,
        cancel: CancellationToken | Callable[[], bool] | None = None,
        client_msg_id: str | None = None,
    ) -> WireMessage:
        self._require_request_connection()
        timeout = float(timeout_seconds if timeout_seconds is not None else self.config.request_timeout_seconds)
        if timeout <= 0:
            raise CTraderRequestTimeout("timeout debe ser positivo")
        request_id = str(client_msg_id or self._next_request_id())
        record = RequestRecord(
            request_id,
            WireMessage(payload_type).payload_type_name,
            timeout,
            generation=self._generation,
            phase=RequestPhase.WAITING_TO_SEND,
        )
        pending = _PendingRequest(record, queue.Queue(maxsize=1))
        with self._state_lock:
            if request_id in self._pending:
                raise CTraderTransportError(
                    f"client_msg_id ya está en vuelo: {request_id}",
                    phase=RequestPhase.FAILED_BEFORE_SEND,
                )
            self._requests[request_id] = record
            self._pending[request_id] = pending
        try:
            self._send_request(record, payload_type, payload, cancel)
            return self._await_request(record, pending, cancel)
        finally:
            self._unregister_request(request_id)

    def _require_request_connection(self) -> None:
        if self._status.connection is not ConnectionState.CONNECTED:
            raise CTraderTransportError(f"conexión no disponible: {self._status.connection.value}")

    def _mark_request_phase(self, record: RequestRecord, phase: RequestPhase, **changes: Any) -> None:
        record.phase = phase
        self._set_status(last_request_phase=phase, **changes)

    def _send_request(
        self,
        record: RequestRecord,
        payload_type: int | str,
        payload: Any,
        cancel: CancellationToken | Callable[[], bool] | None,
    ) -> None:
        with self._send_lock:
            if _cancelled(cancel):
                self._mark_request_phase(record, RequestPhase.CANCELLED_BEFORE_SEND)
                raise CTraderRequestCancelled(
                    "request cancelado antes de enviar",
                    phase=RequestPhase.CANCELLED_BEFORE_SEND,
                )
            historical = record.payload_type in {
                "PROTO_OA_GET_TRENDBARS_REQ",
                "PROTO_OA_GET_TICKDATA_REQ",
            }
            self._rate_limiter.acquire(historical=historical)
            if _cancelled(cancel):
                self._mark_request_phase(record, RequestPhase.CANCELLED_BEFORE_SEND)
                raise CTraderRequestCancelled(
                    "request cancelado antes de enviar",
                    phase=RequestPhase.CANCELLED_BEFORE_SEND,
                )
            if self._generation != record.generation or self._status.connection is not ConnectionState.CONNECTED:
                self._mark_request_phase(record, RequestPhase.FAILED_BEFORE_SEND)
                raise CTraderTransportError(
                    "conexión reemplazada antes de enviar",
                    phase=RequestPhase.FAILED_BEFORE_SEND,
                )
            try:
                self.transport.send(WireMessage(payload_type, payload, record.request_id, False))
            except CTraderTransportError as exc:
                failure = _with_request_phase(exc, RequestPhase.FAILED_AFTER_SEND)
                self._mark_request_phase(
                    record,
                    RequestPhase.FAILED_AFTER_SEND,
                    connection=ConnectionState.DISCONNECTED,
                    needs_reconciliation=True,
                    discontinuity_reason="send_transport_error",
                    action="falló el envío; concilie antes de reintentar",
                )
                raise failure from exc
            except Exception:
                self._mark_request_phase(record, RequestPhase.FAILED_BEFORE_SEND)
                raise
            record.sent_at = self._wall_clock()
            self._mark_request_phase(record, RequestPhase.SENT)

    def _await_request(
        self,
        record: RequestRecord,
        pending: _PendingRequest,
        cancel: CancellationToken | Callable[[], bool] | None,
    ) -> WireMessage:
        deadline = self._clock() + record.timeout_seconds
        while True:
            if _cancelled(cancel):
                self._mark_request_phase(
                    record,
                    RequestPhase.CANCELLED_AFTER_SEND,
                    needs_reconciliation=True,
                    action="request cancelada; concilie si el servidor pudo recibirla",
                )
                raise CTraderRequestCancelled(
                    f"request cancelado: {record.request_id}",
                    phase=RequestPhase.CANCELLED_AFTER_SEND,
                )
            remaining = deadline - self._clock()
            if remaining <= 0:
                self._mark_request_phase(
                    record,
                    RequestPhase.TIMED_OUT,
                    needs_reconciliation=True,
                    action="request vencida; concilie respuestas tardías antes de continuar",
                )
                raise CTraderRequestTimeout(
                    f"timeout esperando {record.payload_type} ({record.request_id})",
                    phase=RequestPhase.TIMED_OUT,
                )
            try:
                result = pending.response_queue.get(timeout=min(remaining, 0.10))
            except queue.Empty:
                continue
            if isinstance(result, _PendingFailure):
                self._mark_request_phase(record, RequestPhase.FAILED_AFTER_SEND)
                error = result.error
                if isinstance(error, CTraderTransportError):
                    raise error
                raise _with_request_phase(
                    CTraderTransportError(str(error)),
                    RequestPhase.FAILED_AFTER_SEND,
                ) from error
            return self._finish_response(record, result)

    def _finish_response(self, record: RequestRecord, message: WireMessage) -> WireMessage:
        self._mark_request_phase(record, RequestPhase.COMPLETED)
        if not _is_error_message(message):
            return message
        code, description, retry_after = _error_fields(message.payload)
        if str(code).upper() in {
            "OA_AUTH_TOKEN_EXPIRED",
            "ACCOUNT_NOT_AUTHORIZED",
            "RET_ACCOUNT_DISABLED",
        }:
            auth = AuthState.EXPIRED if "EXPIRED" in str(code).upper() else AuthState.INVALID
            self._set_status(
                auth=auth,
                action="Renueve autorización OAuth2 y vuelva a autenticar la cuenta",
            )
        raise CTraderProtocolError(str(code), description, retry_after=retry_after)

    def _unregister_request(self, request_id: str) -> None:
        with self._state_lock:
            record = self._requests.get(request_id)
            self._pending.pop(request_id, None)
            self._requests.pop(request_id, None)
            if (
                record is not None
                and record.phase is not RequestPhase.COMPLETED
                and request_id not in self._retired_request_id_set
            ):
                if len(self._retired_request_ids) == self._retired_request_ids.maxlen:
                    old = self._retired_request_ids.popleft()
                    self._retired_request_id_set.discard(old)
                self._retired_request_ids.append(request_id)
                self._retired_request_id_set.add(request_id)
            pending_count = len(self._pending)
        self._set_status(pending_requests=pending_count)

    def request_message(
        self,
        message: Any,
        *,
        client_msg_id: str,
        timeout_seconds: float | None = None,
    ) -> WireMessage:
        """Send one generated Protobuf message through this same session.

        This is the typed gateway used by the DEMO adapter.  It does not
        inspect or retry an SDK ``Deferred``; the session's synchronous
        transport/codec path remains the sole request implementation.
        """

        if not client_msg_id or not str(client_msg_id).strip():
            raise ValueError("client_msg_id debe ser texto no vacío")
        payload_type = message_payload_type(message)
        if payload_type is None:
            raise CTraderProtocolError(
                "LOCAL_MESSAGE",
                f"mensaje sin payloadType reconocido: {type(message).__name__}",
            )
        return self.request(
            payload_type,
            message,
            timeout_seconds=timeout_seconds,
            client_msg_id=str(client_msg_id),
        )

    def poll_event(self, timeout_seconds: float | None = None) -> WireMessage | None:
        self._maybe_heartbeat()
        try:
            message = self._control_queue.get_nowait()
        except queue.Empty:
            try:
                message = self._event_queue.get(timeout=timeout_seconds if timeout_seconds is not None else 0)
            except queue.Empty:
                return None
        self._set_status(
            queue_size=self._event_queue.qsize() + self._control_queue.qsize(),
            last_event_at=message.available_at or message.received_at or self._wall_clock(),
        )
        return message

    def iter_events(
        self,
        *,
        timeout_seconds: float = 0.25,
        max_events: int | None = None,
        duration_seconds: float | None = None,
    ) -> Iterator[WireMessage]:
        started = self._clock()
        count = 0
        while max_events is None or count < max_events:
            if duration_seconds is not None and self._clock() - started >= duration_seconds:
                break
            message = self.poll_event(timeout_seconds)
            if message is None:
                continue
            count += 1
            yield message

    def mark_authenticated(self, account_id: int | None = None) -> None:
        selected = account_id if account_id is not None else self.config.account_id
        if selected is None:
            self._authenticated_account_id = None
            self._set_status(auth=AuthState.ACCOUNT_REQUIRED, action="Configure ctidTraderAccountId")
            return
        if isinstance(selected, bool) or int(selected) <= 0:
            raise CTraderAuthError("account_id debe ser positivo")
        self._authenticated_account_id = int(selected)
        self._session_evidence = None
        self._set_status(auth=AuthState.AUTHENTICATED, action="")

    def _copy_account_discovery(self, *, include_token: bool = False) -> dict[str, Any]:
        if self._account_discovery is None:
            raise CTraderAuthError(
                "no hay cuentas observadas; autentique la aplicación primero",
                action="Ejecute application auth y descubra las cuentas autorizadas",
            )
        snapshot = {
            "records": [dict(record) for record in self._account_discovery["records"]],
            "permissionScope": self._account_discovery.get("permissionScope"),
        }
        if include_token and "accessToken" in self._account_discovery:
            snapshot["accessToken"] = self._account_discovery["accessToken"]
        return snapshot

    def _discover_accounts_for_token(self, token: str) -> dict[str, Any]:
        response = self.request(
            "PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ",
            {"accessToken": token},
        )
        self._require_auth_response(
            response,
            request_type=PAYLOAD["PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ"],
            operation="account discovery",
        )
        self._account_discovery = normalize_account_payload(response.payload)
        # Keep the server echo in memory for explicit token-binding checks.
        # Ordinary discovery snapshots remain redacted by default.
        token_echo = read_field(response.payload, "accessToken", "access_token", default=None)
        if isinstance(token_echo, str):
            if not token_echo or not secrets.compare_digest(token_echo, token):
                self._account_discovery = None
                self._set_status(
                    auth=AuthState.INVALID,
                    action="La respuesta de discovery no corresponde al access token presentado",
                )
                raise CTraderAuthError("account discovery token echo mismatch", action=self._status.action)
            self._account_discovery["accessToken"] = token_echo
        return self._copy_account_discovery()

    def discover_accounts(
        self,
        *,
        token_provider: Callable[[str], str] | None = None,
        include_token: bool = False,
    ) -> dict[str, Any]:
        """Return account observations; opt-in token echoes must never be logged."""
        if self._account_discovery is not None:
            return self._copy_account_discovery(include_token=include_token)
        if not self._application_authenticated:
            self._set_status(
                auth=AuthState.REQUIRED,
                action="Autentique la aplicación OAuth antes de descubrir cuentas",
            )
            raise CTraderAuthError("la aplicación no está autenticada", action=self._status.action)
        if not self.config.access_token_ref or token_provider is None:
            self._set_status(
                auth=AuthState.ACCOUNT_REQUIRED,
                action="Proporcione access_token_ref/token_provider para descubrir cuentas",
            )
            raise CTraderAuthError("se requiere access token para descubrir cuentas", action=self._status.action)
        token = token_provider(self.config.access_token_ref)
        if not token:
            self._set_status(auth=AuthState.INVALID, action="La referencia de token no devolvió un valor")
            raise CTraderAuthError("token_provider vacío", action=self._status.action)
        try:
            self._discover_accounts_for_token(token)
            return self._copy_account_discovery(include_token=include_token)
        finally:
            token = ""

    def authenticate(
        self,
        *,
        secret_provider: Callable[[str], str] | None = None,
        token_provider: Callable[[str], str] | None = None,
        authorize_selected: bool = True,
    ) -> AuthState:
        self._validate_auth_arguments(secret_provider, token_provider, authorize_selected)
        assert secret_provider is not None
        assert token_provider is not None
        assert self.config.client_secret_ref is not None
        assert self.config.access_token_ref is not None
        self._authenticate_application(secret_provider)
        return self._discover_and_authorize(token_provider, authorize_selected)

    def _validate_auth_arguments(
        self,
        secret_provider: Callable[[str], str] | None,
        token_provider: Callable[[str], str] | None,
        authorize_selected: bool,
    ) -> None:
        if not isinstance(authorize_selected, bool):
            raise CTraderAuthError("authorize_selected debe ser booleano")
        if not self.config.client_id or not self.config.client_secret_ref:
            self._set_status(
                auth=AuthState.REQUIRED,
                action="Configure client_id y una referencia de secreto efímero",
            )
            raise CTraderAuthError("faltan client_id/client_secret_ref", action=self._status.action)
        if secret_provider is None:
            self._set_status(
                auth=AuthState.REQUIRED,
                action="Proporcione secret_provider(ref); no se capturan secretos en config",
            )
            raise CTraderAuthError("se requiere secret_provider", action=self._status.action)
        if not self.config.access_token_ref or token_provider is None:
            self._set_status(
                auth=AuthState.ACCOUNT_REQUIRED,
                action="Configure access_token_ref/token_provider para descubrir cuentas",
            )
            raise CTraderAuthError("se requiere access token para descubrir cuentas", action=self._status.action)

    def _authenticate_application(self, secret_provider: Callable[[str], str]) -> None:
        secret_ref = self.config.client_secret_ref
        if secret_ref is None:
            raise CTraderAuthError("faltan client_id/client_secret_ref")
        secret = secret_provider(secret_ref)
        if not secret:
            self._set_status(auth=AuthState.INVALID, action="La referencia de secreto no devolvió un valor")
            raise CTraderAuthError("secret_provider vacío", action=self._status.action)
        try:
            response = self.request(
                "PROTO_OA_APPLICATION_AUTH_REQ",
                {"clientId": self.config.client_id, "clientSecret": secret},
            )
            self._require_auth_response(
                response,
                request_type=PAYLOAD["PROTO_OA_APPLICATION_AUTH_REQ"],
                operation="application auth",
            )
            self._application_authenticated = True
        finally:
            secret = ""

    def _discover_and_authorize(
        self,
        token_provider: Callable[[str], str],
        authorize_selected: bool,
    ) -> AuthState:
        token_ref = self.config.access_token_ref
        if token_ref is None:
            raise CTraderAuthError("falta access_token_ref")
        token = token_provider(token_ref)
        if not token:
            self._set_status(auth=AuthState.INVALID, action="La referencia de token no devolvió un valor")
            raise CTraderAuthError("token_provider vacío", action=self._status.action)
        try:
            discovery = self._discover_accounts_for_token(token)
            records = discovery["records"]
            if self.config.account_id is None:
                return self._account_selection_required(records)
            if not authorize_selected:
                self._set_status(
                    auth=AuthState.ACCOUNT_REQUIRED,
                    action="Valide el entorno DEMO y autorice explícitamente la cuenta seleccionada",
                )
                return self._status.auth
            selected_record = self._select_account_record(records, self.config.account_id)
            response = self.request(
                "PROTO_OA_ACCOUNT_AUTH_REQ",
                {"ctidTraderAccountId": self.config.account_id, "accessToken": token},
            )
            self._require_auth_response(
                response,
                request_type=PAYLOAD["PROTO_OA_ACCOUNT_AUTH_REQ"],
                operation="account auth",
            )
            self._mint_session_evidence(selected_record, response)
            self._authenticated_account_id = self.config.account_id
        finally:
            token = ""
        self._set_status(auth=AuthState.AUTHENTICATED, action="")
        return self._status.auth

    def _account_selection_required(self, records: Iterable[Mapping[str, Any]]) -> AuthState:
        account_ids = [int(record["account_id"]) for record in records]
        self._set_status(
            auth=AuthState.ACCOUNT_REQUIRED,
            action=f"Seleccione explícitamente una cuenta autorizada entre {account_ids}",
        )
        return self._status.auth

    def _select_account_record(
        self,
        records: Iterable[Mapping[str, Any]],
        account_id: int,
    ) -> Mapping[str, Any]:
        records_tuple = tuple(records)
        account_ids = [int(record["account_id"]) for record in records_tuple]
        if int(account_id) not in account_ids:
            self._set_status(
                auth=AuthState.ACCOUNT_REQUIRED,
                action=f"Seleccione una cuenta autorizada entre {account_ids}",
            )
            raise CTraderAuthError("account_id no está en la respuesta OAuth2", action=self._status.action)
        selected: Mapping[str, Any] = next(
            record for record in records_tuple if int(record["account_id"]) == int(account_id)
        )
        if str(selected.get("environment", "UNKNOWN")).upper() != "DEMO":
            self._set_status(
                auth=AuthState.ACCOUNT_REQUIRED,
                action="La cuenta observada no está clasificada como DEMO; no se autoriza.",
            )
            raise CTraderAuthError("la cuenta seleccionada no está observada como DEMO", action=self._status.action)
        return selected

    def authorize_account(
        self,
        account_id: int,
        *,
        token_provider: Callable[[str], str] | None = None,
    ) -> AuthState:
        if not self._application_authenticated or self._account_discovery is None:
            raise CTraderAuthError(
                "descubra cuentas después de autenticar la aplicación",
                action="Ejecute authenticate() antes de authorize_account()",
            )
        if isinstance(account_id, bool) or int(account_id) <= 0:
            raise CTraderAuthError("account_id debe ser positivo")
        selected = int(account_id)
        records = tuple(self._account_discovery.get("records", ()))
        account_ids = {int(record["account_id"]) for record in records}
        if selected not in account_ids:
            self._set_status(
                auth=AuthState.ACCOUNT_REQUIRED,
                action=f"Seleccione una cuenta autorizada entre {sorted(account_ids)}",
            )
            raise CTraderAuthError("account_id no está en la respuesta OAuth2", action=self._status.action)
        selected_record = next(record for record in records if int(record["account_id"]) == selected)
        if str(selected_record.get("environment", "UNKNOWN")).upper() != "DEMO":
            self._set_status(
                auth=AuthState.ACCOUNT_REQUIRED,
                action="La cuenta observada no está clasificada como DEMO; no se autoriza.",
            )
            raise CTraderAuthError("la cuenta seleccionada no está observada como DEMO", action=self._status.action)
        if not self.config.access_token_ref or token_provider is None:
            self._set_status(
                auth=AuthState.ACCOUNT_REQUIRED,
                action="Proporcione access_token_ref/token_provider para autorizar la cuenta",
            )
            raise CTraderAuthError("se requiere access token para autorizar la cuenta", action=self._status.action)
        token = token_provider(self.config.access_token_ref)
        if not token:
            self._set_status(auth=AuthState.INVALID, action="La referencia de token no devolvió un valor")
            raise CTraderAuthError("token_provider vacío", action=self._status.action)
        try:
            response = self.request(
                "PROTO_OA_ACCOUNT_AUTH_REQ",
                {"ctidTraderAccountId": selected, "accessToken": token},
            )
            self._require_auth_response(
                response,
                request_type=PAYLOAD["PROTO_OA_ACCOUNT_AUTH_REQ"],
                operation="account auth",
            )
        finally:
            token = ""
        self._mint_session_evidence(selected_record, response)
        self._authenticated_account_id = selected
        self._set_status(auth=AuthState.AUTHENTICATED, action="")
        return self._status.auth

    def _require_auth_response(self, response: WireMessage, *, request_type: int, operation: str) -> None:
        expected = _AUTH_RESPONSE_TYPES[request_type]
        observed = response.payload_type_id
        payload_type = message_payload_type(response.payload)
        if observed != expected or (payload_type is not None and payload_type != expected):
            if request_type == PAYLOAD["PROTO_OA_APPLICATION_AUTH_REQ"]:
                self._application_authenticated = False
            self._account_discovery = None
            self._authenticated_account_id = None
            self._session_evidence = None
            self._set_status(
                auth=AuthState.INVALID,
                action=f"respuesta {operation} con tipo Protobuf inesperado; no se acreditó la sesión",
            )
            raise CTraderAuthError("respuesta de autenticación con tipo inesperado", action=self._status.action)

    def _mint_session_evidence(self, account_record: Mapping[str, Any], response: WireMessage) -> None:
        if str(account_record.get("environment", "")).upper() != "DEMO":
            raise CTraderAuthError("evidence account is not server-observed DEMO")
        response_account = read_field(
            response.payload,
            "ctidTraderAccountId",
            "ctid_trader_account_id",
            default=None,
        )
        if response_account is None:
            self._session_evidence = None
            self._set_status(
                auth=AuthState.INVALID,
                action="AccountAuthRes sin ctidTraderAccountId; no se acreditó la sesión",
            )
            raise CTraderAuthError("respuesta account-auth sin account_id observado")
        if int(response_account) != int(account_record["account_id"]):
            self._session_evidence = None
            self._set_status(
                auth=AuthState.INVALID,
                action="AccountAuthRes no coincide con la cuenta observada",
            )
            raise CTraderAuthError("account auth response does not match discovered account")
        permission = self._account_discovery.get("permissionScope") if self._account_discovery else None
        scopes = _permission_scopes(permission)
        if not scopes:
            # Read-only authentication remains usable, but execution proof is
            # not minted without explicit server scope.
            self._session_evidence = None
            return
        self._session_evidence = AuthenticatedSessionEvidence(
            account_id=int(account_record["account_id"]),
            environment="DEMO",
            endpoint=f"{self.config.host}:{self.config.port}",
            scopes=frozenset(scopes),
            session_id=f"ctrader-session-{uuid.uuid4().hex}",
            connection_generation=str(self._generation),
            authenticated_at=self._wall_clock(),
        )

    def authenticated_session_evidence(self) -> AuthenticatedSessionEvidence:
        proof = self._session_evidence
        if proof is None:
            raise CTraderAuthError("no hay evidencia de sesión autenticada")
        self.validate_session_evidence(proof)
        return proof

    def validate_session_evidence(self, proof: AuthenticatedSessionEvidence | Any) -> bool:
        self._require_current_proof(proof)
        self._require_authenticated_connection()
        self._require_proof_identity(proof)
        self._check_proof_expiry(proof)
        return True

    def _require_current_proof(self, proof: Any) -> None:
        if not isinstance(proof, AuthenticatedSessionEvidence) or proof is not self._session_evidence:
            raise CTraderAuthError("la evidencia de sesión no es la instancia observada por esta sesión")

    def _require_authenticated_connection(self) -> None:
        if self._status.connection is not ConnectionState.CONNECTED or self._status.auth is not AuthState.AUTHENTICATED:
            raise CTraderAuthError("la sesión cTrader ya no está conectada/autenticada")

    def _require_proof_identity(self, proof: AuthenticatedSessionEvidence) -> None:
        expected_endpoint = f"{self.config.host}:{self.config.port}".lower()
        if proof.connection_generation != str(self._generation):
            raise CTraderAuthError("la evidencia pertenece a otra generación de conexión")
        if (
            proof.account_id != self._authenticated_account_id
            or proof.environment != self.config.environment.upper()
            or proof.endpoint.lower() != expected_endpoint
        ):
            raise CTraderAuthError("la evidencia no coincide con la cuenta/sesión actual")

    def _check_proof_expiry(self, proof: AuthenticatedSessionEvidence) -> None:
        if proof.expires_at is not None and self._wall_clock() >= proof.expires_at:
            self._session_evidence = None
            self._set_status(auth=AuthState.EXPIRED, action="renueve la evidencia de sesión")
            raise CTraderAuthError("la evidencia de sesión expiró")


def _permission_scopes(value: Any) -> set[str]:
    """Map only the exact cTrader permission enum to effective scopes."""

    named = getattr(value, "name", None)
    if named is not None:
        value = named
    if isinstance(value, bool) or value is None:
        return set()
    if isinstance(value, int):
        return {"accounts"} if value == 0 else ({"accounts", "trading"} if value == 1 else set())
    text = str(value).strip().upper()
    if text in {"0", "SCOPE_VIEW"}:
        return {"accounts"}
    if text in {"1", "SCOPE_TRADE"}:
        return {"accounts", "trading"}
    return set()


def _session_control_kind(message: WireMessage) -> str | None:
    payload_type_id = message.payload_type_id
    by_id = {
        SESSION_CONTROL_PAYLOAD_TYPES["PROTO_OA_ACCOUNT_DISCONNECT_EVENT"]: "ACCOUNT_DISCONNECT",
        SESSION_CONTROL_PAYLOAD_TYPES["PROTO_OA_CLIENT_DISCONNECT_EVENT"]: "CLIENT_DISCONNECT",
        SESSION_CONTROL_PAYLOAD_TYPES["PROTO_OA_ACCOUNTS_TOKEN_INVALIDATED_EVENT"]: "ACCOUNTS_TOKEN_INVALIDATED",
    }
    if payload_type_id in by_id:
        return by_id[payload_type_id]
    aliases = {
        "ACCOUNT_DISCONNECT": "ACCOUNT_DISCONNECT",
        "CLIENT_DISCONNECT": "CLIENT_DISCONNECT",
        "ACCOUNTS_TOKEN_INVALIDATED": "ACCOUNTS_TOKEN_INVALIDATED",
        "TOKEN_INVALIDATED": "ACCOUNTS_TOKEN_INVALIDATED",
    }
    name = str(message.payload_type).strip().upper()
    if name not in SESSION_CONTROL_TYPE_NAMES:
        return None
    return aliases.get(name)


def _control_account_targets(payload: Any) -> frozenset[int] | None:
    singular = read_field(
        payload,
        "ctidTraderAccountId",
        "ctid_trader_account_id",
        "accountId",
        "account_id",
        default=None,
    )
    if singular is not None:
        parsed = _positive_account_id(singular)
        return frozenset({parsed}) if parsed is not None else None
    plural = read_repeated(
        payload,
        "ctidTraderAccountIds",
        "ctid_trader_account_ids",
        "accountIds",
        "account_ids",
    )
    if not plural:
        return None
    parsed_ids = tuple(_positive_account_id(item) for item in plural)
    return (
        frozenset(item for item in parsed_ids if item is not None)
        if all(item is not None for item in parsed_ids)
        else None
    )


def _positive_account_id(value: Any) -> int | None:
    try:
        account_id = int(value)
    except (TypeError, ValueError):
        return None
    return account_id if account_id > 0 else None


def _validate_resume_counter(value: int, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise CTraderConfigurationError(f"{name} debe ser entero no negativo")


def _with_request_phase(error: BaseException, phase: RequestPhase) -> CTraderTransportError:
    if isinstance(error, CTraderTransportError) and error.phase is not None:
        return error
    return CTraderTransportError(str(error), phase=phase)


def _cancelled(cancel: CancellationToken | Callable[[], bool] | None) -> bool:
    if cancel is None:
        return False
    if isinstance(cancel, CancellationToken):
        return cancel.cancelled
    return bool(cancel())


def _is_error_message(message: WireMessage) -> bool:
    return (
        message.payload_type_id
        in {
            PAYLOAD["PROTO_ERROR_RES"],
            PAYLOAD["PROTO_OA_ERROR_RES"],
        }
        or read_field(message.payload, "errorCode", "error_code", default=None) is not None
    )


def _error_fields(payload: Any) -> tuple[str, str, int | None]:
    code = read_field(payload, "errorCode", "error_code", default="UNKNOWN_ERROR")
    description = read_field(payload, "description", default="")
    retry = read_field(payload, "retryAfter", "retry_after", default=None)
    try:
        retry_value = int(retry) if retry is not None else None
    except (TypeError, ValueError):
        retry_value = None
    return str(code), str(description or ""), retry_value


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z") if value else None


CTraderSession = CTraderClient
SessionEvidence = AuthenticatedSessionEvidence

__all__ = [
    "CancellationToken",
    "Clock",
    "CTraderClient",
    "CTraderSession",
    "CTraderStatus",
    "AuthenticatedSessionEvidence",
    "SessionEvidence",
    "EventScheduler",
    "RequestRecord",
    "RequestPhase",
    "Scheduler",
    "WallClock",
    "replace_status",
]
