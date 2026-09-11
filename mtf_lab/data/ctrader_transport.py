"""Transport boundaries for cTrader.

Only this module knows about TCP framing or the deterministic in-memory
fixture.  A session owns receive calls and serializes sends; transports remain
small, synchronous, and free of OAuth/application state.
"""

from __future__ import annotations

import contextlib
import queue
import socket
import ssl
import struct
import threading
import time
from collections.abc import Callable, Iterable
from typing import Protocol, runtime_checkable

from .ctrader_errors import (
    CTraderConfigurationError,
    CTraderDependencyError,
    CTraderTransportError,
)
from .ctrader_protocol import MAX_FRAME_LENGTH, CTraderCodec, WireMessage


@runtime_checkable
class CTraderTransport(Protocol):
    def connect(self, timeout: float | None = None) -> None: ...

    def close(self) -> None: ...

    def send(self, message: WireMessage) -> None: ...

    def receive(self, timeout: float | None = None) -> WireMessage | None: ...


class TcpTlsTransport:
    """Incremental ``int32`` big-endian + Protobuf envelope over TLS."""

    def __init__(
        self,
        host: str,
        port: int = 5035,
        *,
        codec: CTraderCodec | None = None,
        timeout_seconds: float = 10.0,
        socket_factory: Callable[..., socket.socket] = socket.create_connection,
        ssl_context: ssl.SSLContext | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.host = str(host)
        self.port = int(port)
        self.codec = codec
        self.timeout_seconds = float(timeout_seconds)
        self.socket_factory = socket_factory
        self.ssl_context = ssl_context or ssl.create_default_context()
        self.monotonic = monotonic
        self._socket: ssl.SSLSocket | socket.socket | None = None
        self._recv_buffer = bytearray()
        self._io_lock = threading.RLock()

    @property
    def connected(self) -> bool:
        return self._socket is not None

    def connect(self, timeout: float | None = None) -> None:
        if self.codec is None:
            raise CTraderDependencyError(
                "TcpTlsTransport requiere SdkProtobufCodec; instale el extra opcional fuera de este flujo"
            )
        effective_timeout = timeout if timeout is not None else self.timeout_seconds
        try:
            raw = self.socket_factory((self.host, self.port), effective_timeout)
            raw.settimeout(effective_timeout)
            self._socket = self.ssl_context.wrap_socket(raw, server_hostname=self.host)
            self._recv_buffer.clear()
        except (OSError, ssl.SSLError) as exc:
            self._socket = None
            raise CTraderTransportError(f"no se pudo conectar TLS a {self.host}:{self.port}: {exc}") from exc

    def close(self) -> None:
        with self._io_lock:
            sock, self._socket = self._socket, None
            self._recv_buffer.clear()
            if sock is not None:
                with contextlib.suppress(OSError, AttributeError):
                    # shutdown makes a blocked recv wake on common socket
                    # implementations; close remains the fallback.
                    sock.shutdown(socket.SHUT_RDWR)
                with contextlib.suppress(OSError, AttributeError):
                    sock.close()

    def _send_all(self, data: bytes) -> None:
        if self._socket is None:
            raise CTraderTransportError("transporte TLS desconectado")
        if len(data) > MAX_FRAME_LENGTH:
            raise CTraderTransportError("frame Protobuf excede MAX_FRAME_LENGTH")
        frame = struct.pack("!i", len(data)) + data
        try:
            with self._io_lock:
                if self._socket is None:
                    raise CTraderTransportError("transporte TLS desconectado")
                self._socket.sendall(frame)
        except CTraderTransportError:
            raise
        except OSError as exc:
            raise CTraderTransportError(f"error enviando frame TLS: {exc}") from exc

    def send(self, message: WireMessage) -> None:
        if self.codec is None:
            raise CTraderDependencyError("codec Protobuf no disponible")
        self._send_all(self.codec.encode(message))

    def _read_into_buffer(self, timeout: float | None) -> bool:
        sock = self._socket
        if sock is None:
            raise CTraderTransportError("transporte TLS desconectado")
        sock.settimeout(timeout if timeout is not None else self.timeout_seconds)
        try:
            block = sock.recv(65_536)
        except TimeoutError:
            return False
        except OSError as exc:
            raise CTraderTransportError(f"error recibiendo frame TLS: {exc}") from exc
        if not block:
            raise CTraderTransportError("el proxy cerró la conexión TLS")
        self._recv_buffer.extend(block)
        return True

    def receive(self, timeout: float | None = None) -> WireMessage | None:
        if self._socket is None:
            raise CTraderTransportError("transporte TLS desconectado")
        deadline = self.monotonic() + timeout if timeout is not None else None
        while True:
            if len(self._recv_buffer) >= 4:
                (length,) = struct.unpack("!i", self._recv_buffer[:4])
                if length < 0 or length > MAX_FRAME_LENGTH:
                    raise CTraderTransportError(f"longitud de frame inválida: {length}")
                total = 4 + length
                if len(self._recv_buffer) >= total:
                    body = bytes(self._recv_buffer[4:total])
                    del self._recv_buffer[:total]
                    if self.codec is None:
                        raise CTraderDependencyError("codec Protobuf no disponible")
                    return self.codec.decode(body)
            if deadline is not None:
                remaining = deadline - self.monotonic()
                if remaining <= 0:
                    return None
            else:
                remaining = self.timeout_seconds
            # A short upper bound lets the session scheduler check heartbeat
            # and stop state even if no bytes arrive.
            if not self._read_into_buffer(min(remaining, 0.25)):
                return None


class DeterministicTransport:
    """No-network transport with a scripted response handler."""

    def __init__(
        self,
        handler: Callable[[WireMessage], Iterable[WireMessage] | WireMessage | None] | None = None,
        *,
        inbound_maxsize: int = 1_024,
        fail_connect_times: int = 0,
    ) -> None:
        if isinstance(inbound_maxsize, bool) or inbound_maxsize <= 0:
            raise ValueError("inbound_maxsize debe ser positivo")
        self.handler = handler
        self.inbound: queue.Queue[WireMessage] = queue.Queue(maxsize=inbound_maxsize)
        self.sent: list[WireMessage] = []
        self.connect_calls = 0
        self.close_calls = 0
        self.fail_connect_times = int(fail_connect_times)
        self.connected = False
        self._send_lock = threading.RLock()

    def connect(self, timeout: float | None = None) -> None:
        del timeout
        self.connect_calls += 1
        if self.connect_calls <= self.fail_connect_times:
            raise CTraderTransportError(f"fixture connect failure {self.connect_calls}")
        self.connected = True

    def close(self) -> None:
        self.close_calls += 1
        self.connected = False
        while True:
            try:
                self.inbound.get_nowait()
            except queue.Empty:
                break

    def push(self, message: WireMessage) -> None:
        try:
            self.inbound.put_nowait(message)
        except queue.Full as exc:
            raise CTraderTransportError("fixture inbound queue llena") from exc

    def send(self, message: WireMessage) -> None:
        with self._send_lock:
            if not self.connected:
                raise CTraderTransportError("fixture desconectado")
            self.sent.append(message)
            handler = self.handler
            if handler is None:
                return
            response = handler(message)
            if response is None:
                return
            if isinstance(response, WireMessage):
                response = (response,)
            for item in response:
                self.push(item)

    def receive(self, timeout: float | None = None) -> WireMessage | None:
        if not self.connected:
            raise CTraderTransportError("fixture desconectado")
        try:
            return self.inbound.get(timeout=timeout if timeout is not None else 0)
        except queue.Empty:
            return None


class CTraderRateLimiter:
    """Per-connection request limiter for normal and historical calls."""

    def __init__(
        self,
        *,
        request_rate: float = 50.0,
        historical_rate: float = 5.0,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if request_rate <= 0 or historical_rate <= 0:
            raise CTraderConfigurationError("los límites de solicitudes deben ser positivos")
        self.request_rate = float(request_rate)
        self.historical_rate = float(historical_rate)
        self.clock = clock
        self.sleep = sleep
        self._next_allowed = {False: 0.0, True: 0.0}
        self._lock = threading.RLock()

    def acquire(self, *, historical: bool = False) -> None:
        with self._lock:
            now = float(self.clock())
            interval = 1.0 / (self.historical_rate if historical else self.request_rate)
            wait = max(0.0, self._next_allowed[historical] - now)
            if wait > 0:
                self.sleep(wait)
                now = float(self.clock())
            self._next_allowed[historical] = max(now, self._next_allowed[historical]) + interval


__all__ = [
    "CTraderRateLimiter",
    "CTraderTransport",
    "DeterministicTransport",
    "TcpTlsTransport",
]
