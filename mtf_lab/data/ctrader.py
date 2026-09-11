"""Proveedor de consulta para cTrader Open API (Protobuf sobre TCP/TLS).

Esta integración es deliberadamente *read-only*: no expone mensajes de
órdenes, posiciones, depósitos ni retiros. El transporte, el codec y el
proveedor están desacoplados para que las pruebas usen un
``DeterministicTransport`` sin instalar el SDK oficial ni abrir red.

Referencias oficiales consultadas (vigentes al implementar esta versión):

* https://help.ctrader.com/open-api/proxies-endpoints/ --
  ``live.ctraderapi.com:5035``/``demo.ctraderapi.com:5035`` para Protobuf.
* https://help.ctrader.com/open-api/connection/ -- TCP con SSL, heartbeat
  ``ProtoHeartbeatEvent`` y colas de mensajes.
* https://help.ctrader.com/open-api/account-authentication/ -- OAuth2 y
  secuencia ``ProtoOAApplicationAuthReq`` /
  ``ProtoOAGetAccountListByAccessTokenReq`` /
  ``ProtoOAAccountAuthReq``.
* https://help.ctrader.com/open-api/symbol-data/ -- símbolos, cotizaciones,
  trendbars y escala relativa de precio ``/100000``.
* https://github.com/spotware/openapi-proto-messages -- IDs y campos Protobuf
  mantenidos por Spotware.

No se importa ``twisted.internet.reactor`` ni se arranca ningún reactor al
importar este módulo. El SDK ``ctrader_open_api`` y ``google-protobuf`` son
opcionales; su ausencia se informa como dependencia accionable y no bloquea
el modo fixture/offline.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field, fields, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
import hashlib
import importlib
import importlib.util
import importlib.metadata
import subprocess
import sys
import json
import math
import queue
import socket
import ssl
import struct
import time
import threading
from typing import Any, Callable, Iterable, Iterator, Mapping, Protocol, Sequence, runtime_checkable

from .models import Bar, Event, ensure_utc, resolution_name, resolution_to_seconds


UTC = timezone.utc
SDK_MODULE = "ctrader_open_api"
PROTOBUF_MODULE = "google.protobuf"
MAX_FRAME_LENGTH = 15_000_000  # límite usado por el SDK oficial TcpProtocol.


# IDs oficiales ProtoOAPayloadType/Common payload types. No se usan mensajes
# de ejecución: sólo autenticación/metadata/cotizaciones/trendbars/heartbeat.
PAYLOAD = {
    "PROTO_ERROR_RES": 50,
    "PROTO_HEARTBEAT_EVENT": 51,
    "PROTO_OA_APPLICATION_AUTH_REQ": 2100,
    "PROTO_OA_APPLICATION_AUTH_RES": 2101,
    "PROTO_OA_ACCOUNT_AUTH_REQ": 2102,
    "PROTO_OA_ACCOUNT_AUTH_RES": 2103,
    "PROTO_OA_SYMBOLS_LIST_REQ": 2114,
    "PROTO_OA_SYMBOLS_LIST_RES": 2115,
    "PROTO_OA_SYMBOL_BY_ID_REQ": 2116,
    "PROTO_OA_SYMBOL_BY_ID_RES": 2117,
    "PROTO_OA_SUBSCRIBE_SPOTS_REQ": 2127,
    "PROTO_OA_SUBSCRIBE_SPOTS_RES": 2128,
    "PROTO_OA_SPOT_EVENT": 2131,
    "PROTO_OA_SUBSCRIBE_LIVE_TRENDBAR_REQ": 2135,
    "PROTO_OA_SUBSCRIBE_LIVE_TRENDBAR_RES": 2165,
    "PROTO_OA_GET_TRENDBARS_REQ": 2137,
    "PROTO_OA_GET_TRENDBARS_RES": 2138,
    "PROTO_OA_ERROR_RES": 2142,
    "PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ": 2149,
    "PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_RES": 2150,
}
PAYLOAD_NAMES = {value: key for key, value in PAYLOAD.items()}
WIRE_CLASS_NAMES = {
    "PROTO_OA_APPLICATION_AUTH_REQ": "ProtoOAApplicationAuthReq",
    "PROTO_OA_ACCOUNT_AUTH_REQ": "ProtoOAAccountAuthReq",
    "PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ": "ProtoOAGetAccountListByAccessTokenReq",
    "PROTO_OA_SYMBOLS_LIST_REQ": "ProtoOASymbolsListReq",
    "PROTO_OA_SYMBOL_BY_ID_REQ": "ProtoOASymbolByIdReq",
    "PROTO_OA_SUBSCRIBE_SPOTS_REQ": "ProtoOASubscribeSpotsReq",
    "PROTO_OA_SUBSCRIBE_LIVE_TRENDBAR_REQ": "ProtoOASubscribeLiveTrendbarReq",
    "PROTO_OA_GET_TRENDBARS_REQ": "ProtoOAGetTrendbarsReq",
    "PROTO_HEARTBEAT_EVENT": "ProtoHeartbeatEvent",
}

# Official ProtoOATrendbarPeriod enum values.
TREND_PERIODS = {"M1": 1, "M5": 5, "M15": 7}
TREND_PERIOD_NAMES = {value: key for key, value in TREND_PERIODS.items()}


class CTraderError(RuntimeError):
    """Base de errores del proveedor cTrader."""


class CTraderConfigurationError(CTraderError, ValueError):
    pass


class CTraderDependencyError(CTraderError):
    pass


class CTraderTransportError(CTraderError):
    pass


class CTraderProtocolError(CTraderError):
    def __init__(self, error_code: str, description: str = "", *, retry_after: int | None = None) -> None:
        self.error_code = str(error_code)
        self.description = str(description or "")
        self.retry_after = retry_after
        suffix = f": {self.description}" if self.description else ""
        super().__init__(f"cTrader error {self.error_code}{suffix}")


class CTraderAuthError(CTraderError):
    def __init__(self, message: str, *, action: str = "") -> None:
        self.action = action
        super().__init__(message)


class CTraderRequestTimeout(CTraderError):
    pass


class CTraderRequestCancelled(CTraderError):
    pass


class CTraderDataError(CTraderError, ValueError):
    pass


class DependencyState(str, Enum):
    AVAILABLE = "AVAILABLE"
    MISSING = "MISSING"
    UNIMPORTABLE = "UNIMPORTABLE"
    NOT_VERIFIED = "NOT_VERIFIED"
    OPTIONAL = "OPTIONAL"


class ConnectionState(str, Enum):
    DISCONNECTED = "DISCONNECTED"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    RECONNECTING = "RECONNECTING"
    FAILED = "FAILED"
    CLOSED = "CLOSED"


class AuthState(str, Enum):
    NOT_CONFIGURED = "NOT_CONFIGURED"
    REQUIRED = "REQUIRED"
    READY = "READY"
    AUTHENTICATED = "AUTHENTICATED"
    ACCOUNT_REQUIRED = "ACCOUNT_REQUIRED"
    EXPIRED = "EXPIRED"
    INVALID = "INVALID"


@dataclass(frozen=True, slots=True)
class DependencyReport:
    sdk_module: str
    protobuf_module: str
    sdk_state: DependencyState
    protobuf_state: DependencyState
    sdk_version: str | None = None
    message: str = ""
    codec_state: DependencyState = DependencyState.NOT_VERIFIED

    @property
    def codec_operational(self) -> bool:
        return self.codec_state is DependencyState.AVAILABLE

    @property
    def available(self) -> bool:
        return self.sdk_state is DependencyState.AVAILABLE and self.protobuf_state is DependencyState.AVAILABLE and self.codec_operational

    def to_dict(self) -> dict[str, Any]:
        return {
            "sdk_module": self.sdk_module,
            "protobuf_module": self.protobuf_module,
            "sdk_state": self.sdk_state.value,
            "protobuf_state": self.protobuf_state.value,
            "codec_state": self.codec_state.value,
            "codec_operational": self.codec_operational,
            "sdk_version": self.sdk_version,
            "available": self.available,
            "message": self.message,
        }


def dependency_report() -> DependencyReport:
    """Inspecciona dependencias sin importar ``ctrader_open_api`` en proceso.

    El paquete oficial importa ``twisted.internet.reactor`` desde su
    ``__init__``. La sonda usa metadata/specs y un subprocess aislado para
    diferenciar instalado, no importable y codec operativo sin arrancar reactor
    en el proceso del proveedor.
    """

    try:
        sdk_spec = importlib.util.find_spec(SDK_MODULE)
    except ModuleNotFoundError:
        sdk_spec = None
    try:
        protobuf_spec = importlib.util.find_spec(PROTOBUF_MODULE)
    except ModuleNotFoundError:
        protobuf_spec = None
    sdk_state = DependencyState.AVAILABLE if sdk_spec is not None else DependencyState.MISSING
    protobuf_state = DependencyState.AVAILABLE if protobuf_spec is not None else DependencyState.MISSING
    try:
        sdk_version = importlib.metadata.version("ctrader-open-api")
    except importlib.metadata.PackageNotFoundError:
        sdk_version = None
    messages: list[str] = []
    codec_state = DependencyState.NOT_VERIFIED
    if sdk_state is DependencyState.MISSING or protobuf_state is DependencyState.MISSING:
        codec_state = DependencyState.MISSING
        # An installed SDK cannot be imported without its required protobuf
        # runtime; report that distinction instead of calling it AVAILABLE.
        if sdk_state is DependencyState.AVAILABLE and protobuf_state is DependencyState.MISSING:
            sdk_state = DependencyState.UNIMPORTABLE
    else:
        probe = (
            "from ctrader_open_api.messages import OpenApiCommonMessages_pb2 as c; "
            "from ctrader_open_api.protobuf import Protobuf; "
            "assert hasattr(c, 'ProtoMessage') and hasattr(c, 'ProtoHeartbeatEvent') and hasattr(Protobuf, 'get'); "
            "print('codec-ok')"
        )
        try:
            result = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, timeout=5, check=False)
            if result.returncode == 0 and "codec-ok" in result.stdout:
                codec_state = DependencyState.AVAILABLE
            else:
                codec_state = DependencyState.UNIMPORTABLE
                detail = (result.stderr or result.stdout).strip().splitlines()[-1:]
                detail_text = detail[0] if detail else "error desconocido"
                messages.append("codec Protobuf no importable: " + detail_text)
                # `find_spec` only proves that a module path exists.  A
                # subprocess import is the evidence needed to distinguish an
                # installed-but-unimportable SDK/protobuf pair without loading
                # Twisted's reactor in this process.
                lowered = (result.stderr or result.stdout).lower()
                if "ctrader_open_api" in lowered:
                    sdk_state = DependencyState.UNIMPORTABLE
                if "google.protobuf" in lowered or "protobuf" in lowered:
                    protobuf_state = DependencyState.UNIMPORTABLE
        except (OSError, subprocess.TimeoutExpired) as exc:
            codec_state = DependencyState.UNIMPORTABLE
            messages.append(f"sonda codec falló: {type(exc).__name__}")
            if sdk_state is DependencyState.AVAILABLE:
                sdk_state = DependencyState.UNIMPORTABLE
    if sdk_state is DependencyState.MISSING:
        messages.append(f"falta {SDK_MODULE}")
    if protobuf_state is DependencyState.MISSING:
        messages.append(f"falta {PROTOBUF_MODULE}")
    return DependencyReport(
        SDK_MODULE,
        PROTOBUF_MODULE,
        sdk_state,
        protobuf_state,
        sdk_version=sdk_version,
        message="; ".join(messages),
        codec_state=codec_state,
    )


def _positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise CTraderConfigurationError(f"{name} debe ser entero positivo")
    return value


def _finite(value: Any, name: str, *, minimum: float | None = None) -> float:
    if isinstance(value, bool):
        raise CTraderConfigurationError(f"{name} debe ser numérico")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise CTraderConfigurationError(f"{name} debe ser numérico") from exc
    if not math.isfinite(result) or (minimum is not None and result < minimum):
        raise CTraderConfigurationError(f"{name} debe ser finito y >= {minimum}")
    return result


def normalize_symbol_name(value: str) -> str:
    text = str(value).strip().upper().replace("-", "/")
    if text in {"EURUSD", "EUR/USD"}:
        return "EUR/USD"
    if "/" not in text and len(text) == 6:
        return f"{text[:3]}/{text[3:]}"
    return text


@dataclass(frozen=True, slots=True)
class CTraderConfig:
    """Configuración sin valores secretos.

    ``client_secret_ref``, ``access_token_ref`` y ``refresh_token_ref`` son
    nombres para un proveedor de secretos externo al objeto; nunca se aceptan
    campos de secreto en texto plano ni se incluyen en ``to_dict``.
    """

    environment: str = "demo"
    host: str | None = None
    port: int = 5035
    symbol: str = "EUR/USD"
    symbol_id: int | None = None
    account_id: int | None = None
    client_id: str | None = None
    client_secret_ref: str | None = None
    access_token_ref: str | None = None
    refresh_token_ref: str | None = None
    quote_basis: str = "mid"
    timeframes: tuple[str, ...] = ("M1", "M5", "M15")
    digits: int = 5
    price_scale: int = 100_000
    pip_position: int = 4
    request_timeout_seconds: float = 5.0
    max_reconnects: int = 3
    reconnect_backoff_seconds: float = 1.0
    reconnect_backoff_max_seconds: float = 30.0
    heartbeat_seconds: float = 10.0
    queue_maxsize: int = 256
    historical_count: int = 500
    request_rate_limit: float = 50.0
    historical_rate_limit: float = 5.0

    def __post_init__(self) -> None:
        env = str(self.environment).strip().lower()
        if env not in {"demo", "live"}:
            raise CTraderConfigurationError("environment debe ser demo o live")
        host = self.host or ("demo.ctraderapi.com" if env == "demo" else "live.ctraderapi.com")
        if not str(host).strip():
            raise CTraderConfigurationError("host no puede estar vacío")
        port = _positive_int(self.port, "port")
        if port > 65535:
            raise CTraderConfigurationError("port fuera de rango")
        symbol = normalize_symbol_name(self.symbol)
        if not symbol:
            raise CTraderConfigurationError("symbol no puede estar vacío")
        if self.symbol_id is not None:
            _positive_int(self.symbol_id, "symbol_id")
        if self.account_id is not None:
            _positive_int(self.account_id, "account_id")
        quote_basis = str(self.quote_basis).lower()
        if quote_basis not in {"mid", "bid", "ask"}:
            raise CTraderConfigurationError("quote_basis debe ser mid, bid o ask")
        periods = tuple(str(value).upper() for value in self.timeframes)
        if not periods or any(value not in TREND_PERIODS for value in periods):
            raise CTraderConfigurationError(f"timeframes sólo admite {sorted(TREND_PERIODS)}")
        if len(set(periods)) != len(periods):
            raise CTraderConfigurationError("timeframes no puede repetir periodos")
        digits = _positive_int(self.digits, "digits")
        scale = _positive_int(self.price_scale, "price_scale")
        pip_position = _positive_int(self.pip_position, "pip_position")
        if pip_position > digits:
            raise CTraderConfigurationError("pip_position no puede exceder digits")
        object.__setattr__(self, "environment", env)
        object.__setattr__(self, "host", str(host).strip())
        object.__setattr__(self, "port", port)
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "quote_basis", quote_basis)
        object.__setattr__(self, "timeframes", periods)
        object.__setattr__(self, "digits", digits)
        object.__setattr__(self, "price_scale", scale)
        object.__setattr__(self, "pip_position", pip_position)
        for name in ("request_timeout_seconds", "reconnect_backoff_seconds", "reconnect_backoff_max_seconds", "heartbeat_seconds"):
            object.__setattr__(self, name, _finite(getattr(self, name), name, minimum=0.001))
        object.__setattr__(self, "max_reconnects", _positive_int(self.max_reconnects, "max_reconnects"))
        object.__setattr__(self, "queue_maxsize", _positive_int(self.queue_maxsize, "queue_maxsize"))
        object.__setattr__(self, "historical_count", _positive_int(self.historical_count, "historical_count"))
        for name in ("request_rate_limit", "historical_rate_limit"):
            object.__setattr__(self, name, _finite(getattr(self, name), name, minimum=0.001))

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any] | None) -> "CTraderConfig":
        if mapping is None:
            return cls()
        if not isinstance(mapping, Mapping):
            raise TypeError("ctrader config debe ser un mapping")
        allowed = {item.name for item in fields(cls)} | {"credentials"}
        unknown = set(mapping) - allowed
        if unknown:
            raise CTraderConfigurationError(f"claves desconocidas en ctrader: {sorted(unknown)}")
        raw = dict(mapping)
        credentials = raw.pop("credentials", None)
        if credentials is not None:
            if not isinstance(credentials, Mapping):
                raise CTraderConfigurationError("credentials debe ser tabla")
            allowed_credentials = {"client_id", "client_secret_ref", "access_token_ref", "refresh_token_ref"}
            unknown_credentials = set(credentials) - allowed_credentials
            if unknown_credentials:
                raise CTraderConfigurationError(f"claves desconocidas en ctrader.credentials: {sorted(unknown_credentials)}")
            for forbidden in ("client_secret", "access_token", "refresh_token"):
                if forbidden in credentials:
                    raise CTraderConfigurationError(f"{forbidden} no se acepta; use sólo una referencia/proveedor efímero")
            for key, value in credentials.items():
                if key in raw:
                    raise CTraderConfigurationError(f"credencial duplicada: {key}")
                raw[key] = value
        for forbidden in ("client_secret", "access_token", "refresh_token"):
            if forbidden in raw:
                raise CTraderConfigurationError(f"{forbidden} no se acepta; no se capturan secretos")
        return cls(**raw)

    @property
    def auth_configured(self) -> bool:
        """Whether application OAuth and an access-token reference are ready.

        ``account_id`` is intentionally not part of this gate.  The official
        OAuth flow first authenticates the application and asks for the list
        of accounts granted to the access token; selecting one is a separate,
        explicit step and must never be inferred from the first record.
        """

        return bool(self.client_id and self.client_secret_ref and self.access_token_ref)

    def to_dict(self) -> dict[str, Any]:
        return {
            "environment": self.environment,
            "host": self.host,
            "port": self.port,
            "symbol": self.symbol,
            "symbol_id": self.symbol_id,
            "account_id": self.account_id,
            "client_id_configured": bool(self.client_id),
            "client_secret_ref_configured": bool(self.client_secret_ref),
            "access_token_ref_configured": bool(self.access_token_ref),
            "refresh_token_ref_configured": bool(self.refresh_token_ref),
            "quote_basis": self.quote_basis,
            "timeframes": list(self.timeframes),
            "digits": self.digits,
            "price_scale": self.price_scale,
            "pip_position": self.pip_position,
            "request_timeout_seconds": self.request_timeout_seconds,
            "max_reconnects": self.max_reconnects,
            "reconnect_backoff_seconds": self.reconnect_backoff_seconds,
            "reconnect_backoff_max_seconds": self.reconnect_backoff_max_seconds,
            "heartbeat_seconds": self.heartbeat_seconds,
            "queue_maxsize": self.queue_maxsize,
            "historical_count": self.historical_count,
            "request_rate_limit": self.request_rate_limit,
            "historical_rate_limit": self.historical_rate_limit,
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
        return {"symbol": self.symbol, "symbol_id": self.symbol_id, "digits": self.digits, "pip_position": self.pip_position, "price_scale": self.price_scale, "periods": dict(TREND_PERIODS)}


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

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol_id": self.symbol_id,
            "name": self.name,
            "digits": self.digits,
            "pip_position": self.pip_position,
            "enabled": self.enabled,
            "metadata": _jsonable(self.metadata),
        }

    @property
    def normalized_name(self) -> str:
        return normalize_symbol_name(self.name)


@dataclass(frozen=True, slots=True)
class SymbolCatalog:
    symbols: tuple[CTraderSymbol, ...]
    requested_symbol: str
    selected: CTraderSymbol | None = None

    def find(self, symbol: str | None = None) -> CTraderSymbol | None:
        wanted = normalize_symbol_name(symbol or self.requested_symbol)
        return next((item for item in self.symbols if item.normalized_name == wanted or item.name.upper() == str(symbol or self.requested_symbol).upper()), None)

    def to_dict(self) -> dict[str, Any]:
        return {
            "requested_symbol": self.requested_symbol,
            "selected": self.selected.symbol_id if self.selected else None,
            "symbols": [item.to_dict() for item in self.symbols],
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
        }


@dataclass(frozen=True, slots=True)
class WireMessage:
    """Envelope lógico equivalente a ``ProtoMessage``."""

    payload_type: int | str
    payload: Any = None
    client_msg_id: str | None = None
    is_event: bool = False

    @property
    def payload_type_id(self) -> int | None:
        if isinstance(self.payload_type, int):
            return self.payload_type
        value = PAYLOAD.get(str(self.payload_type).upper())
        if value is not None:
            return value
        if str(self.payload_type).isdigit():
            return int(str(self.payload_type))
        return None

    @property
    def payload_type_name(self) -> str:
        return PAYLOAD_NAMES.get(self.payload_type_id, str(self.payload_type))

    def to_dict(self, *, redact: bool = True) -> dict[str, Any]:
        return {"payload_type": self.payload_type_name, "payload_type_id": self.payload_type_id, "client_msg_id": self.client_msg_id, "is_event": self.is_event, "payload": _redact(self.payload) if redact else _jsonable(self.payload)}


@dataclass(frozen=True, slots=True)
class RequestRecord:
    request_id: str
    payload_type: str
    timeout_seconds: float
    sent_at: datetime


class CancellationToken:
    def __init__(self) -> None:
        import threading

        self._event = threading.Event()

    def cancel(self) -> None:
        self._event.set()

    @property
    def cancelled(self) -> bool:
        return self._event.is_set()


@runtime_checkable
class CTraderTransport(Protocol):
    def connect(self, timeout: float | None = None) -> None: ...
    def close(self) -> None: ...
    def send(self, message: WireMessage) -> None: ...
    def receive(self, timeout: float | None = None) -> WireMessage | None: ...


class CTraderCodec(Protocol):
    def encode(self, message: WireMessage) -> bytes: ...
    def decode(self, payload: bytes) -> WireMessage: ...


class SdkProtobufCodec:
    """Codec basado en el SDK oficial, cargado sólo bajo demanda."""

    def __init__(self) -> None:
        report = dependency_report()
        if not report.available:
            raise CTraderDependencyError(report.message or "SDK Protobuf cTrader no disponible")
        try:
            common = importlib.import_module("ctrader_open_api.messages.OpenApiCommonMessages_pb2")
            protobuf_module = importlib.import_module("ctrader_open_api.protobuf")
            self._proto_message = common.ProtoMessage
            self._heartbeat = common.ProtoHeartbeatEvent
            self._protobuf = protobuf_module.Protobuf
        except Exception as exc:  # pragma: no cover - depende del extra instalado
            raise CTraderDependencyError(f"SDK cTrader presente pero no cargable: {exc}") from exc

    def _payload_message(self, message: WireMessage) -> Any:
        payload = message.payload
        if payload is None and message.payload_type_id == PAYLOAD["PROTO_HEARTBEAT_EVENT"]:
            return self._heartbeat()
        if hasattr(payload, "SerializeToString"):
            return payload
        name = WIRE_CLASS_NAMES.get(message.payload_type_name, message.payload_type_name)
        params = dict(payload or {}) if isinstance(payload, Mapping) else {}
        try:
            return self._protobuf.get(name, **params)
        except Exception as exc:  # pragma: no cover - sólo con SDK real
            raise CTraderProtocolError("LOCAL_CODEC", f"no se pudo construir {name}: {exc}") from exc

    def encode(self, message: WireMessage) -> bytes:
        payload_message = self._payload_message(message)
        # The official TcpProtocol wraps every concrete payload, including
        # ProtoHeartbeatEvent, in ProtoMessage (the heartbeat payload itself
        # serializes to empty bytes because its enum has a default).
        envelope = self._proto_message(payload=payload_message.SerializeToString(), clientMsgId=message.client_msg_id, payloadType=payload_message.payloadType)
        return envelope.SerializeToString()

    def decode(self, payload: bytes) -> WireMessage:
        envelope = self._proto_message()
        envelope.ParseFromString(payload)
        if envelope.payloadType == PAYLOAD["PROTO_HEARTBEAT_EVENT"]:
            return WireMessage(envelope.payloadType, None, getattr(envelope, "clientMsgId", None) or None, True)
        extracted = self._protobuf.extract(envelope)
        return WireMessage(envelope.payloadType, extracted, getattr(envelope, "clientMsgId", None) or None, False)


class TcpTlsTransport:
    """Framing TCP ``int32`` big-endian + Protobuf envelope + TLS."""

    def __init__(self, host: str, port: int = 5035, *, codec: CTraderCodec | None = None, timeout_seconds: float = 10.0, socket_factory: Callable[..., socket.socket] = socket.create_connection, ssl_context: ssl.SSLContext | None = None) -> None:
        self.host = str(host)
        self.port = int(port)
        self.codec = codec
        self.timeout_seconds = float(timeout_seconds)
        self.socket_factory = socket_factory
        self.ssl_context = ssl_context or ssl.create_default_context()
        self._socket: ssl.SSLSocket | None = None
        self._recv_buffer = bytearray()

    @property
    def connected(self) -> bool:
        return self._socket is not None

    def connect(self, timeout: float | None = None) -> None:
        if self.codec is None:
            raise CTraderDependencyError("TcpTlsTransport requiere SdkProtobufCodec; instale el extra opcional fuera de este flujo")
        try:
            raw = self.socket_factory((self.host, self.port), timeout if timeout is not None else self.timeout_seconds)
            raw.settimeout(timeout if timeout is not None else self.timeout_seconds)
            self._socket = self.ssl_context.wrap_socket(raw, server_hostname=self.host)
            self._recv_buffer.clear()
        except (OSError, ssl.SSLError) as exc:
            raise CTraderTransportError(f"no se pudo conectar TLS a {self.host}:{self.port}: {exc}") from exc

    def close(self) -> None:
        sock, self._socket = self._socket, None
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass

    def _send_all(self, data: bytes) -> None:
        if self._socket is None:
            raise CTraderTransportError("transporte TLS desconectado")
        if len(data) > MAX_FRAME_LENGTH:
            raise CTraderTransportError("frame Protobuf excede MAX_FRAME_LENGTH")
        frame = struct.pack("!i", len(data)) + data
        try:
            self._socket.sendall(frame)
        except OSError as exc:
            raise CTraderTransportError(f"error enviando frame TLS: {exc}") from exc

    def send(self, message: WireMessage) -> None:
        if self.codec is None:
            raise CTraderDependencyError("codec Protobuf no disponible")
        self._send_all(self.codec.encode(message))

    def _read_into_buffer(self, timeout: float | None) -> bool:
        if self._socket is None:
            raise CTraderTransportError("transporte TLS desconectado")
        self._socket.settimeout(timeout if timeout is not None else self.timeout_seconds)
        try:
            block = self._socket.recv(65536)
        except socket.timeout:
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
        deadline = time.monotonic() + timeout if timeout is not None else None
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
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    # Cualquier header/body parcial permanece en _recv_buffer y
                    # se completará en la siguiente llamada.
                    return None
            else:
                remaining = self.timeout_seconds
            if not self._read_into_buffer(min(remaining, 0.25)):
                return None


class DeterministicTransport:
    """Transporte fixture, sin red y con script de respuestas determinista."""

    def __init__(self, handler: Callable[[WireMessage], Iterable[WireMessage] | WireMessage | None] | None = None, *, inbound_maxsize: int = 1024, fail_connect_times: int = 0) -> None:
        if inbound_maxsize <= 0:
            raise ValueError("inbound_maxsize debe ser positivo")
        self.handler = handler
        self.inbound: queue.Queue[WireMessage] = queue.Queue(maxsize=inbound_maxsize)
        self.sent: list[WireMessage] = []
        self.connect_calls = 0
        self.close_calls = 0
        self.fail_connect_times = int(fail_connect_times)
        self.connected = False

    def connect(self, timeout: float | None = None) -> None:
        self.connect_calls += 1
        if self.connect_calls <= self.fail_connect_times:
            raise CTraderTransportError(f"fixture connect failure {self.connect_calls}")
        self.connected = True

    def close(self) -> None:
        self.close_calls += 1
        self.connected = False

    def push(self, message: WireMessage) -> None:
        try:
            self.inbound.put_nowait(message)
        except queue.Full as exc:
            raise CTraderTransportError("fixture inbound queue llena") from exc

    def send(self, message: WireMessage) -> None:
        if not self.connected:
            raise CTraderTransportError("fixture desconectado")
        self.sent.append(message)
        if self.handler is None:
            return
        response = self.handler(message)
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
            return self.inbound.get(timeout=timeout or 0)
        except queue.Empty:
            return None


class CTraderRateLimiter:
    """Per-connection limiter matching official 50/5 requests-per-second caps."""

    def __init__(self, *, request_rate: float = 50.0, historical_rate: float = 5.0, clock: Callable[[], float] = time.monotonic, sleep: Callable[[float], None] = time.sleep) -> None:
        if request_rate <= 0 or historical_rate <= 0:
            raise CTraderConfigurationError("los límites de solicitudes deben ser positivos")
        self.request_rate = float(request_rate)
        self.historical_rate = float(historical_rate)
        self.clock = clock
        self.sleep = sleep
        self._next_allowed = {False: 0.0, True: 0.0}

    def acquire(self, *, historical: bool = False) -> None:
        now = float(self.clock())
        interval = 1.0 / (self.historical_rate if historical else self.request_rate)
        wait = max(0.0, self._next_allowed[historical] - now)
        if wait > 0:
            self.sleep(wait)
            now += wait
        self._next_allowed[historical] = max(now, self._next_allowed[historical]) + interval


class CTraderClient:
    """Cliente correlacionador de requests/responses y eventos."""

    def __init__(self, config: CTraderConfig | Mapping[str, Any] | None = None, *, transport: CTraderTransport | None = None, codec: CTraderCodec | None = None, clock: Callable[[], float] = time.monotonic, sleep: Callable[[float], None] = time.sleep, rate_limiter: CTraderRateLimiter | None = None) -> None:
        self.config = config if isinstance(config, CTraderConfig) else CTraderConfig.from_mapping(config)
        self._clock = clock
        self._sleep = sleep
        self._rate_limiter = rate_limiter or CTraderRateLimiter(request_rate=self.config.request_rate_limit, historical_rate=self.config.historical_rate_limit, clock=clock, sleep=sleep)
        if transport is None:
            if codec is None:
                # Construcción perezosa: no falla al importar ni al crear el
                # objeto; connect() deja el estado de dependencia accionable.
                try:
                    codec = SdkProtobufCodec()
                except CTraderDependencyError:
                    codec = None
            transport = TcpTlsTransport(self.config.host or "", self.config.port, codec=codec, timeout_seconds=self.config.request_timeout_seconds)
        self.transport = transport
        self._event_queue: queue.Queue[WireMessage] = queue.Queue(maxsize=self.config.queue_maxsize)
        self._requests: dict[str, RequestRecord] = {}
        self._request_counter = 0
        self._request_lock = threading.RLock()
        self._response_queues: dict[str, queue.Queue[WireMessage]] = {}
        self._pump_stop = threading.Event()
        self._pump_thread: threading.Thread | None = None
        self._last_heartbeat_monotonic = self._clock()
        # Safe, normalized account metadata only.  Access tokens are never
        # retained here; the response is kept so callers can inspect the
        # observed inventory without issuing a second OAuth request.
        self._account_discovery: dict[str, Any] | None = None
        self._application_authenticated = False
        self._authenticated_account_id: int | None = None
        self._status = CTraderStatus(dependency=DependencyState.AVAILABLE if not isinstance(transport, TcpTlsTransport) else (DependencyState.AVAILABLE if codec is not None else DependencyState.MISSING), auth=self._initial_auth_state())

    @property
    def authenticated_account_id(self) -> int | None:
        """Server-authorized account for the current connection, if any."""

        return self._authenticated_account_id

    @property
    def discovered_accounts(self) -> tuple[dict[str, Any], ...]:
        """Detached, non-secret account records observed through OAuth."""

        if self._account_discovery is None:
            return ()
        return tuple(dict(record) for record in self._account_discovery.get("records", ()))

    def _initial_auth_state(self) -> AuthState:
        if self.config.auth_configured:
            return AuthState.READY
        if self.config.client_id or self.config.client_secret_ref or self.config.access_token_ref or self.config.account_id:
            return AuthState.REQUIRED
        return AuthState.NOT_CONFIGURED

    @property
    def status(self) -> CTraderStatus:
        return replace_status(self._status, queue_size=self._event_queue.qsize(), pending_requests=len(self._requests))

    @property
    def dependency(self) -> DependencyReport:
        return dependency_report()

    def _set_status(self, **changes: Any) -> None:
        self._status = replace_status(self._status, **changes)

    def _start_receive_pump(self) -> None:
        if self._pump_thread and self._pump_thread.is_alive():
            return
        self._pump_stop.clear()
        self._pump_thread = threading.Thread(target=self._receive_pump, name="mtf-ctrader-receive", daemon=True)
        self._pump_thread.start()

    def _stop_receive_pump(self) -> None:
        self._pump_stop.set()
        thread, self._pump_thread = self._pump_thread, None
        if thread and thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=1.0)
        self._response_queues.clear()

    def _receive_pump(self) -> None:
        while not self._pump_stop.is_set():
            try:
                message = self.transport.receive(0.25)
            except Exception as exc:
                auth_state = AuthState.REQUIRED if (self._status.auth is AuthState.AUTHENTICATED or self._application_authenticated) else self._status.auth
                self._set_status(connection=ConnectionState.DISCONNECTED, auth=auth_state, needs_reconciliation=True, last_error=str(exc), action="Reconecte, reautentique y concilie el intervalo perdido")
                return
            if message is None:
                try:
                    self._maybe_heartbeat()
                except Exception as exc:
                    self._set_status(last_error=str(exc), needs_reconciliation=True)
                # DeterministicTransport is non-blocking; prevent a hot loop
                # while retaining one receive pump for post-request fixtures.
                self._pump_stop.wait(0.005)
                continue
            now = datetime.now(UTC)
            self._set_status(last_message_at=now)
            if message.payload_type_id == PAYLOAD["PROTO_HEARTBEAT_EVENT"]:
                try:
                    self.heartbeat()
                except Exception as exc:
                    self._set_status(last_error=str(exc), needs_reconciliation=True)
                continue
            if message.client_msg_id:
                response_queue = self._response_queues.get(str(message.client_msg_id))
                if response_queue is not None:
                    try:
                        response_queue.put_nowait(message)
                    except queue.Full:
                        self._set_status(needs_reconciliation=True, action="respuesta correlacionada descartada; concilie")
                    continue
                # A response arriving after timeout/cancellation is not a
                # market event. Do not leak it into the stream where a caller
                # could mistake it for a quote; surface the reconciliation
                # requirement instead.
                if str(message.client_msg_id).startswith("ctrader-"):
                    self._set_status(
                        needs_reconciliation=True,
                        action="respuesta tardía de request vencida; concilie antes de continuar",
                    )
                    continue
            self._set_status(last_event_at=now)
            self._publish_event(message)

    def connect(self) -> CTraderStatus:
        if isinstance(self.transport, TcpTlsTransport):
            report = self.dependency
            if not report.available:
                state = report.codec_state if report.codec_state is not DependencyState.NOT_VERIFIED else report.sdk_state
                self._set_status(
                    dependency=state,
                    connection=ConnectionState.FAILED,
                    action="Instale/verifique el extra opcional ctrader-open-api/google-protobuf; no se instaló automáticamente",
                )
                raise CTraderDependencyError(report.message or "SDK/codec cTrader no operativo")
        self._set_status(connection=ConnectionState.CONNECTING, action="")
        try:
            self.transport.connect(self.config.request_timeout_seconds)
        except Exception as exc:
            self._set_status(connection=ConnectionState.FAILED, last_error=str(exc), action="Verifique host/puerto/TLS o use DeterministicTransport")
            raise
        had_session = self._application_authenticated or self._status.auth is AuthState.AUTHENTICATED
        auth_state = AuthState.REQUIRED if had_session else self._status.auth
        if had_session:
            self._authenticated_account_id = None
            self._application_authenticated = False
            self._account_discovery = None
        self._set_status(connection=ConnectionState.CONNECTED, auth=auth_state, last_error=None, reconnect_attempt=0, action="Reautentique y resuscriba si había sesión previa" if auth_state is AuthState.REQUIRED else "")
        self._start_receive_pump()
        return self.status

    def connect_with_retry(self) -> CTraderStatus:
        if isinstance(self.transport, TcpTlsTransport):
            report = self.dependency
            if not report.available:
                state = report.codec_state if report.codec_state is not DependencyState.NOT_VERIFIED else report.sdk_state
                self._set_status(
                    dependency=state,
                    connection=ConnectionState.FAILED,
                    action="Instale/verifique el extra opcional cTrader antes de reconectar",
                )
                raise CTraderDependencyError(report.message or "SDK/codec cTrader no operativo")
        last_error: Exception | None = None
        for attempt in range(1, self.config.max_reconnects + 1):
            try:
                self._set_status(reconnect_attempt=attempt, connection=ConnectionState.RECONNECTING if attempt > 1 else ConnectionState.CONNECTING)
                self.transport.connect(self.config.request_timeout_seconds)
                had_session = self._application_authenticated or self._status.auth is AuthState.AUTHENTICATED
                auth_state = AuthState.REQUIRED if had_session else self._status.auth
                if had_session:
                    self._authenticated_account_id = None
                    self._application_authenticated = False
                    self._account_discovery = None
                self._set_status(connection=ConnectionState.CONNECTED, auth=auth_state, last_error=None, action="Reautentique y resuscriba si había sesión previa" if auth_state is AuthState.REQUIRED else self._status.action)
                self._start_receive_pump()
                return self.status
            except Exception as exc:
                last_error = exc
                self._set_status(connection=ConnectionState.RECONNECTING, last_error=str(exc), action="Reintento programado; una reconexión no reconcilia eventos perdidos")
                if attempt < self.config.max_reconnects:
                    delay = min(self.config.reconnect_backoff_seconds * (2 ** (attempt - 1)), self.config.reconnect_backoff_max_seconds)
                    self._sleep(delay)
        self._set_status(connection=ConnectionState.FAILED, action="Agotados reintentos; ejecute conciliación/backfill antes de habilitar análisis")
        raise CTraderTransportError(f"no se pudo reconectar tras {self.config.max_reconnects} intentos: {last_error}") from last_error

    def close(self) -> None:
        self._stop_receive_pump()
        self.transport.close()
        self._requests.clear()
        self._authenticated_account_id = None
        self._application_authenticated = False
        self._account_discovery = None
        self._set_status(connection=ConnectionState.CLOSED, pending_requests=0)

    def _next_request_id(self) -> str:
        self._request_counter += 1
        return f"ctrader-{self._request_counter:08d}"

    def _publish_event(self, message: WireMessage) -> None:
        try:
            self._event_queue.put_nowait(message)
        except queue.Full:
            try:
                self._event_queue.get_nowait()
            except queue.Empty:
                pass
            try:
                self._event_queue.put_nowait(message)
            except queue.Full:
                pass
            self._set_status(dropped_messages=self._status.dropped_messages + 1, needs_reconciliation=True, action="Cola de eventos llena; se descartó tráfico y requiere conciliación")

    def heartbeat(self) -> None:
        self.transport.send(WireMessage(PAYLOAD["PROTO_HEARTBEAT_EVENT"], None, None, True))
        self._last_heartbeat_monotonic = self._clock()
        self._set_status(heartbeat_count=self._status.heartbeat_count + 1)

    def _maybe_heartbeat(self) -> None:
        if self._status.connection is ConnectionState.CONNECTED and self._clock() - self._last_heartbeat_monotonic >= self.config.heartbeat_seconds:
            self.heartbeat()

    def request(self, payload_type: int | str, payload: Mapping[str, Any] | Any = None, *, timeout_seconds: float | None = None, cancel: CancellationToken | Callable[[], bool] | None = None) -> WireMessage:
        # One receive pump owns the transport; callers cannot steal each
        # other's correlated response while events are being queued.
        with self._request_lock:
            return self._request_locked(payload_type, payload, timeout_seconds=timeout_seconds, cancel=cancel)

    def _request_locked(self, payload_type: int | str, payload: Mapping[str, Any] | Any = None, *, timeout_seconds: float | None = None, cancel: CancellationToken | Callable[[], bool] | None = None) -> WireMessage:
        if self._status.connection is not ConnectionState.CONNECTED:
            raise CTraderTransportError(f"conexión no disponible: {self._status.connection.value}")
        timeout = float(timeout_seconds if timeout_seconds is not None else self.config.request_timeout_seconds)
        if timeout <= 0:
            raise CTraderRequestTimeout("timeout debe ser positivo")
        if _cancelled(cancel):
            raise CTraderRequestCancelled("request cancelado antes de enviar")
        request_id = self._next_request_id()
        record = RequestRecord(request_id, WireMessage(payload_type).payload_type_name, timeout, datetime.now(UTC))
        if self._pump_thread and self._pump_thread.is_alive():
            return self._request_via_pump(request_id, record, payload_type, payload, cancel=cancel)
        self._requests[request_id] = record
        self._set_status(pending_requests=len(self._requests))
        try:
            self._maybe_heartbeat()
            historical = WireMessage(payload_type).payload_type_name in {"PROTO_OA_GET_TRENDBARS_REQ", "PROTO_OA_GET_TICKDATA_REQ"}
            self._rate_limiter.acquire(historical=historical)
            self.transport.send(WireMessage(payload_type, payload, request_id, False))
            deadline = self._clock() + timeout
            while True:
                if _cancelled(cancel):
                    raise CTraderRequestCancelled(f"request cancelado: {request_id}")
                remaining = deadline - self._clock()
                if remaining <= 0:
                    raise CTraderRequestTimeout(f"timeout esperando {record.payload_type} ({request_id})")
                message = self.transport.receive(min(remaining, 0.25))
                if message is None:
                    continue
                now = datetime.now(UTC)
                self._set_status(last_message_at=now)
                if message.payload_type_id == PAYLOAD["PROTO_HEARTBEAT_EVENT"]:
                    self.heartbeat()
                    continue
                if message.client_msg_id == request_id:
                    if _is_error_message(message):
                        code, description, retry_after = _error_fields(message.payload)
                        if str(code).upper() in {"OA_AUTH_TOKEN_EXPIRED", "ACCOUNT_NOT_AUTHORIZED", "RET_ACCOUNT_DISABLED"}:
                            self._set_status(auth=AuthState.EXPIRED if "EXPIRED" in str(code).upper() else AuthState.INVALID, action="Renueve autorización OAuth2 y vuelva a autenticar la cuenta")
                        raise CTraderProtocolError(str(code), description, retry_after=retry_after)
                    return message
                self._set_status(last_event_at=now)
                self._publish_event(message)
        except CTraderRequestCancelled:
            self._set_status(needs_reconciliation=True, action="request cancelada; concilie si el servidor pudo recibirla")
            raise
        except CTraderRequestTimeout:
            self._set_status(needs_reconciliation=True, action="request vencida; concilie respuestas tardías antes de continuar")
            raise
        except CTraderTransportError:
            auth_state = AuthState.REQUIRED if (self._status.auth is AuthState.AUTHENTICATED or self._application_authenticated) else self._status.auth
            self._set_status(connection=ConnectionState.DISCONNECTED, auth=auth_state, needs_reconciliation=True, action="Reconecte, reautentique y concilie el intervalo perdido")
            raise
        finally:
            self._requests.pop(request_id, None)
            self._set_status(pending_requests=len(self._requests))

    def _request_via_pump(self, request_id: str, record: RequestRecord, payload_type: int | str, payload: Any, *, cancel: CancellationToken | Callable[[], bool] | None = None) -> WireMessage:
        response_queue: queue.Queue[WireMessage] = queue.Queue(maxsize=1)
        self._response_queues[request_id] = response_queue
        self._requests[request_id] = record
        self._set_status(pending_requests=len(self._requests))
        try:
            historical = record.payload_type in {"PROTO_OA_GET_TRENDBARS_REQ", "PROTO_OA_GET_TICKDATA_REQ"}
            self._rate_limiter.acquire(historical=historical)
            self.transport.send(WireMessage(payload_type, payload, request_id, False))
            deadline = self._clock() + record.timeout_seconds
            while True:
                if _cancelled(cancel):
                    raise CTraderRequestCancelled(f"request cancelado: {request_id}")
                remaining = deadline - self._clock()
                if remaining <= 0:
                    raise CTraderRequestTimeout(f"timeout esperando {record.payload_type} ({request_id})")
                try:
                    message = response_queue.get(timeout=min(remaining, 0.25))
                except queue.Empty:
                    continue
                if _is_error_message(message):
                    code, description, retry_after = _error_fields(message.payload)
                    if str(code).upper() in {"OA_AUTH_TOKEN_EXPIRED", "ACCOUNT_NOT_AUTHORIZED", "RET_ACCOUNT_DISABLED"}:
                        self._set_status(
                            auth=AuthState.EXPIRED if "EXPIRED" in str(code).upper() else AuthState.INVALID,
                            action="Renueve autorización OAuth2 y vuelva a autenticar la cuenta",
                        )
                    raise CTraderProtocolError(str(code), description, retry_after=retry_after)
                return message
        except CTraderRequestTimeout:
            self._set_status(needs_reconciliation=True, action="request vencida; concilie respuestas tardías antes de continuar")
            raise
        except CTraderRequestCancelled:
            self._set_status(needs_reconciliation=True, action="request cancelada; concilie si el servidor pudo recibirla")
            raise
        except CTraderTransportError:
            self._set_status(connection=ConnectionState.DISCONNECTED, needs_reconciliation=True, action="Reconecte y concilie el intervalo perdido")
            raise
        finally:
            self._response_queues.pop(request_id, None)
            self._requests.pop(request_id, None)
            self._set_status(pending_requests=len(self._requests))

    def poll_event(self, timeout_seconds: float | None = None) -> WireMessage | None:
        self._maybe_heartbeat()
        try:
            message = self._event_queue.get(timeout=timeout_seconds or 0)
        except queue.Empty:
            return None
        self._set_status(queue_size=self._event_queue.qsize(), last_event_at=datetime.now(UTC))
        return message

    def iter_events(self, *, timeout_seconds: float = 0.25, max_events: int | None = None, duration_seconds: float | None = None) -> Iterator[WireMessage]:
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
        self._set_status(auth=AuthState.AUTHENTICATED, action="")

    def _copy_account_discovery(self) -> dict[str, Any]:
        """Return a detached copy of the last safe account inventory."""

        if self._account_discovery is None:
            raise CTraderAuthError(
                "no hay cuentas observadas; autentique la aplicación primero",
                action="Ejecute application auth y descubra las cuentas autorizadas",
            )
        return {
            "records": [dict(record) for record in self._account_discovery["records"]],
            "permissionScope": self._account_discovery.get("permissionScope"),
        }

    def _discover_accounts_for_token(self, token: str) -> dict[str, Any]:
        """Query and retain only normalized, non-secret account metadata."""

        response = self.request(
            "PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ",
            {"accessToken": token},
        )
        self._account_discovery = _normalize_account_payload(response.payload)
        return self._copy_account_discovery()

    def discover_accounts(
        self,
        *,
        token_provider: Callable[[str], str] | None = None,
    ) -> dict[str, Any]:
        """Expose the observed account inventory without selecting an account.

        ``authenticate`` populates this snapshot after application OAuth.  A
        token provider is accepted as a narrow fallback for callers that have
        already authenticated the application but have not yet queried the
        account list.  The token itself is never retained in the snapshot.
        """

        if self._account_discovery is not None:
            return self._copy_account_discovery()
        if not self._application_authenticated:
            self._set_status(
                auth=AuthState.REQUIRED,
                action="Autentique la aplicación OAuth antes de descubrir cuentas",
            )
            raise CTraderAuthError(
                "la aplicación no está autenticada",
                action=self._status.action,
            )
        if not self.config.access_token_ref or token_provider is None:
            self._set_status(
                auth=AuthState.ACCOUNT_REQUIRED,
                action="Proporcione access_token_ref/token_provider para descubrir cuentas",
            )
            raise CTraderAuthError(
                "se requiere access token para descubrir cuentas",
                action=self._status.action,
            )
        token = token_provider(self.config.access_token_ref)
        if not token:
            self._set_status(
                auth=AuthState.INVALID,
                action="La referencia de token no devolvió un valor",
            )
            raise CTraderAuthError("token_provider vacío", action=self._status.action)
        try:
            return self._discover_accounts_for_token(token)
        finally:
            # No se persiste; la variable local queda fuera de alcance después.
            token = ""

    def authenticate(
        self,
        *,
        secret_provider: Callable[[str], str] | None = None,
        token_provider: Callable[[str], str] | None = None,
        authorize_selected: bool = True,
    ) -> AuthState:
        """Authenticate app and discover accounts; account authorization is explicit.

        ``authorize_selected`` preserves the convenience for callers that
        already supplied an account id, while the CLI passes ``False`` to
        enforce discovery and DEMO/environment validation first.
        """
        if not isinstance(authorize_selected, bool):
            raise CTraderAuthError("authorize_selected debe ser booleano")

        if not self.config.client_id or not self.config.client_secret_ref:
            self._set_status(auth=AuthState.REQUIRED, action="Configure client_id y una referencia de secreto efímero")
            raise CTraderAuthError("faltan client_id/client_secret_ref", action=self._status.action)
        if secret_provider is None:
            self._set_status(auth=AuthState.REQUIRED, action="Proporcione secret_provider(ref); no se capturan secretos en config")
            raise CTraderAuthError("se requiere secret_provider", action=self._status.action)
        secret = secret_provider(self.config.client_secret_ref)
        if not secret:
            self._set_status(auth=AuthState.INVALID, action="La referencia de secreto no devolvió un valor")
            raise CTraderAuthError("secret_provider vacío", action=self._status.action)
        try:
            self.request("PROTO_OA_APPLICATION_AUTH_REQ", {"clientId": self.config.client_id, "clientSecret": secret})
            self._application_authenticated = True
        finally:
            # No se persiste; la variable local queda fuera de alcance después.
            secret = ""
        if not self.config.access_token_ref or token_provider is None:
            self._set_status(auth=AuthState.ACCOUNT_REQUIRED, action="Configure access_token_ref/token_provider para descubrir cuentas")
            raise CTraderAuthError("se requiere access token para descubrir cuentas", action=self._status.action)
        token = token_provider(self.config.access_token_ref)
        if not token:
            self._set_status(auth=AuthState.INVALID, action="La referencia de token no devolvió un valor")
            raise CTraderAuthError("token_provider vacío", action=self._status.action)
        try:
            discovery = self._discover_accounts_for_token(token)
            records = discovery["records"]
            account_ids = [int(record["account_id"]) for record in records]
            if self.config.account_id is None:
                # The inventory is useful even when a human still has to
                # select an account.  Never pick the first account implicitly.
                self._set_status(
                    auth=AuthState.ACCOUNT_REQUIRED,
                    action=f"Seleccione explícitamente una cuenta autorizada entre {account_ids}",
                )
                return self._status.auth
            if not authorize_selected:
                # Discovery is intentionally returned even when a stale or
                # real account_id is present in configuration; the caller
                # validates the observed environment before authorizing.
                self._set_status(auth=AuthState.ACCOUNT_REQUIRED, action="Valide el entorno DEMO y autorice explícitamente la cuenta seleccionada")
                return self._status.auth
            if self.config.account_id not in account_ids:
                self._set_status(auth=AuthState.ACCOUNT_REQUIRED, action=f"Seleccione una cuenta autorizada entre {account_ids}")
                raise CTraderAuthError("account_id no está en la respuesta OAuth2", action=self._status.action)
            selected_record = next(
                record for record in records if int(record["account_id"]) == int(self.config.account_id)
            )
            if str(selected_record.get("environment", "UNKNOWN")).upper() != "DEMO":
                self._set_status(
                    auth=AuthState.ACCOUNT_REQUIRED,
                    action="La cuenta observada no está clasificada como DEMO; no se autoriza.",
                )
                raise CTraderAuthError("la cuenta seleccionada no está observada como DEMO", action=self._status.action)
            self.request("PROTO_OA_ACCOUNT_AUTH_REQ", {"ctidTraderAccountId": self.config.account_id, "accessToken": token})
            self._authenticated_account_id = self.config.account_id
        finally:
            token = ""
        self._set_status(auth=AuthState.AUTHENTICATED, action="")
        return self._status.auth

    def authorize_account(
        self,
        account_id: int,
        *,
        token_provider: Callable[[str], str] | None = None,
    ) -> AuthState:
        """Authorize one account after the observed inventory is checked."""

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
            self._set_status(auth=AuthState.ACCOUNT_REQUIRED, action=f"Seleccione una cuenta autorizada entre {sorted(account_ids)}")
            raise CTraderAuthError("account_id no está en la respuesta OAuth2", action=self._status.action)
        selected_record = next(record for record in records if int(record["account_id"]) == selected)
        if str(selected_record.get("environment", "UNKNOWN")).upper() != "DEMO":
            self._set_status(auth=AuthState.ACCOUNT_REQUIRED, action="La cuenta observada no está clasificada como DEMO; no se autoriza.")
            raise CTraderAuthError("la cuenta seleccionada no está observada como DEMO", action=self._status.action)
        if not self.config.access_token_ref or token_provider is None:
            self._set_status(auth=AuthState.ACCOUNT_REQUIRED, action="Proporcione access_token_ref/token_provider para autorizar la cuenta")
            raise CTraderAuthError("se requiere access token para autorizar la cuenta", action=self._status.action)
        token = token_provider(self.config.access_token_ref)
        if not token:
            self._set_status(auth=AuthState.INVALID, action="La referencia de token no devolvió un valor")
            raise CTraderAuthError("token_provider vacío", action=self._status.action)
        try:
            self.request("PROTO_OA_ACCOUNT_AUTH_REQ", {"ctidTraderAccountId": selected, "accessToken": token})
        finally:
            token = ""
        self._authenticated_account_id = selected
        self._set_status(auth=AuthState.AUTHENTICATED, action="")
        return self._status.auth


@dataclass(frozen=True, slots=True)
class CTraderNormalizationResult:
    records: tuple[Event | Bar, ...]
    quote_events: tuple[Event, ...] = ()
    bars: tuple[Bar, ...] = ()
    issues: tuple[str, ...] = ()
    snapshot: bool = False
    symbol_id: int | None = None

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
        return {"timeframe": self.timeframe, "symbol": self.symbol.to_dict(), "count": len(self.bars), "has_more": self.has_more, "issues": list(self.issues), "request": self.request.to_dict(), "response_type": self.response.payload_type_name}


@dataclass(frozen=True, slots=True)
class CTraderSessionWindow:
    weekday: int
    open_minute: int
    close_minute: int

    def __post_init__(self) -> None:
        if not 0 <= int(self.weekday) <= 6 or not 0 <= int(self.open_minute) < 1440 or not 0 <= int(self.close_minute) <= 1440:
            raise CTraderConfigurationError("ventana de sesión inválida")


class CTraderMarketCalendar:
    """Explicit session calendar; empty schedules remain UNKNOWN, never open."""

    def __init__(self, windows: Iterable[CTraderSessionWindow] = (), holidays: Iterable[str] = ()) -> None:
        self.windows = tuple(windows)
        self.holidays = frozenset(str(item) for item in holidays)

    def state(self, when: datetime, *, last_quote_at: datetime | None = None, max_quote_age_seconds: float = 90.0) -> str:
        instant = ensure_utc(when, field_name="when")
        if instant.date().isoformat() in self.holidays:
            return "CLOSED_SCHEDULED"
        minute = instant.hour * 60 + instant.minute
        windows = [item for item in self.windows if item.weekday == instant.weekday() and item.open_minute <= minute < item.close_minute]
        if windows:
            if last_quote_at is None:
                return "OPEN_NO_QUOTE"
            age = (instant - ensure_utc(last_quote_at, field_name="last_quote_at")).total_seconds()
            return "OPEN" if age <= float(max_quote_age_seconds) else "OPEN_NO_QUOTE"
        return "CLOSED_SCHEDULED" if self.windows else "UNKNOWN"


@dataclass(frozen=True, slots=True)
class CTraderHistoryResult:
    """Paginated historical result with an explicit completeness flag."""
    bars: tuple[Bar, ...]
    timeframe: str
    pages: int
    complete: bool
    has_more: bool
    issues: tuple[str, ...] = ()

    def __iter__(self) -> Iterator[Bar]:
        return iter(self.bars)

    def __len__(self) -> int:
        return len(self.bars)

    def to_dict(self) -> dict[str, Any]:
        return {"timeframe": self.timeframe, "count": len(self.bars), "pages": self.pages, "complete": self.complete, "has_more": self.has_more, "issues": list(self.issues)}


class CTraderProvider:
    """Adaptador neutral que sólo devuelve Event/Bar canónicos."""

    name = "ctrader_open_api"

    def __init__(self, config: CTraderConfig | Mapping[str, Any] | None = None, *, client: CTraderClient | None = None, transport: CTraderTransport | None = None) -> None:
        self.config = config if isinstance(config, CTraderConfig) else CTraderConfig.from_mapping(config)
        self.client = client or CTraderClient(self.config, transport=transport)
        self.instrument = self.config.symbol
        self.spec = CTraderInstrumentSpec(self.config.symbol, self.config.symbol_id, self.config.digits, self.config.pip_position, self.config.price_scale)
        self.catalog: SymbolCatalog | None = None
        self._subscribed: set[str] = set()
        # cTrader spot messages may update only one leg. Keep each leg's
        # source/receipt clocks so a partial bid update cannot erase or
        # rejuvenate the previous ask.
        self._quote_state: dict[int, dict[str, Any]] = {}

    @property
    def status(self) -> CTraderStatus:
        return self.client.status

    @property
    def dependency(self) -> DependencyReport:
        return self.client.dependency

    def connect(self) -> CTraderStatus:
        return self.client.connect()

    def close(self) -> None:
        self._quote_state.clear()
        self.client.close()

    def authenticate(self, **kwargs: Any) -> AuthState:
        return self.client.authenticate(**kwargs)

    def discover_accounts(
        self,
        *,
        token_provider: Callable[[str], str] | None = None,
    ) -> dict[str, Any]:
        """Return the OAuth-observed account inventory without selecting one."""

        return self.client.discover_accounts(token_provider=token_provider)

    def authorize_account(
        self,
        account_id: int,
        *,
        token_provider: Callable[[str], str] | None = None,
    ) -> AuthState:
        return self.client.authorize_account(account_id, token_provider=token_provider)

    def _require_authenticated(self) -> int:
        if self.client.status.auth is not AuthState.AUTHENTICATED:
            self.client._set_status(auth=AuthState.ACCOUNT_REQUIRED, action="Autentique una cuenta cTrader antes de consultar mercado")
            raise CTraderAuthError("cuenta no autenticada", action=self.client.status.action)
        account_id = self.client.authenticated_account_id or self.config.account_id
        if not account_id:
            raise CTraderAuthError("falta account_id", action="Configure ctidTraderAccountId")
        return int(account_id)

    def resolve_symbol(self, *, include_archived: bool = False) -> SymbolCatalog:
        """Resolve a symbol from the account catalogue and fetch full metadata.

        ``ProtoOASymbolsListRes`` contains ``ProtoOALightSymbol`` records.  A
        light record is sufficient to find an id but not to establish digits,
        pip position, volume limits, schedule or commission.  When those
        fields are absent, the official ``ProtoOASymbolByIdReq`` is required;
        defaults are never presented as observed broker data.
        """

        account_id = self._require_authenticated()
        response = self.client.request(
            "PROTO_OA_SYMBOLS_LIST_REQ",
            {"ctidTraderAccountId": account_id, "includeArchivedSymbols": bool(include_archived)},
        )
        symbols_raw = _field(response.payload, "symbol", "symbols", default=())
        symbols: list[CTraderSymbol] = []
        for raw in _as_sequence(symbols_raw):
            symbol_id = _field(raw, "symbolId", "symbol_id", default=None)
            name = _field(raw, "symbolName", "name", default=None)
            if symbol_id is None or name is None:
                continue
            symbols.append(
                CTraderSymbol(
                    int(symbol_id),
                    str(name),
                    _int_or_none(_field(raw, "digits")),
                    _int_or_none(_field(raw, "pipPosition", "pip_position")),
                    _bool_or_none(_field(raw, "enabled")),
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

        # A light symbol may omit all effective trading specifications.  Ask
        # for the full entity before building the instrument spec.
        if selected.digits is None or selected.digits <= 0 or selected.pip_position is None or selected.pip_position <= 0:
            detail_response = self.client.request(
                "PROTO_OA_SYMBOL_BY_ID_REQ",
                {"ctidTraderAccountId": account_id, "symbolId": selected.symbol_id},
            )
            detail_raw = _field(detail_response.payload, "symbol", default=None)
            detail_items = _as_sequence(detail_raw)
            detail = detail_items[0] if detail_items else None
            if detail is None:
                self.catalog = SymbolCatalog(tuple(symbols), self.config.symbol, None)
                raise CTraderDataError("respuesta SymbolById sin entidad completa")
            digits = _int_or_none(_field(detail, "digits"))
            pip_position = _int_or_none(_field(detail, "pipPosition", "pip_position"))
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
        self.catalog = SymbolCatalog(tuple(symbols), self.config.symbol, selected)
        self.instrument = selected.name
        self.spec = CTraderInstrumentSpec(
            selected.name,
            selected.symbol_id,
            int(selected.digits),
            int(selected.pip_position),
            self.config.price_scale,
        )
        return self.catalog

    def _ensure_symbol(self) -> CTraderInstrumentSpec:
        if self.spec.symbol_id is None:
            self.resolve_symbol()
        if self.spec.symbol_id is None:
            raise CTraderDataError("symbol_id no resuelto")
        return self.spec

    def fetch(self, timeframe: str = "M1", *, count: int | None = None, from_timestamp: datetime | None = None, to_timestamp: datetime | None = None) -> CTraderFetchResult:
        account_id = self._require_authenticated()
        spec = self._ensure_symbol()
        period = _period_code(timeframe)
        count_value = int(count if count is not None else self.config.historical_count)
        if count_value <= 0:
            raise CTraderConfigurationError("count debe ser positivo")
        params: dict[str, Any] = {"ctidTraderAccountId": account_id, "period": period, "symbolId": spec.symbol_id, "count": count_value}
        if from_timestamp is not None:
            params["fromTimestamp"] = _unix_ms(from_timestamp)
        if to_timestamp is not None:
            params["toTimestamp"] = _unix_ms(to_timestamp)
        request = WireMessage("PROTO_OA_GET_TRENDBARS_REQ", params, None, False)
        response = self.client.request(request.payload_type, request.payload)
        raw_bars = _field(response.payload, "trendbar", "trendbars", default=())
        received = datetime.now(UTC)
        bars: list[Bar] = []
        issues: list[str] = []
        for raw in _as_sequence(raw_bars):
            try:
                bars.append(normalize_trendbar(raw, spec=spec, received_at=received, request_id=response.client_msg_id or "history", mode="REPLAY"))
            except CTraderDataError as exc:
                issues.append(str(exc))
        bars.sort(key=lambda item: item.interval_start)
        actual_request = WireMessage(request.payload_type, request.payload, response.client_msg_id, False)
        return CTraderFetchResult(tuple(bars), _period_name(period), spec, actual_request, response, bool(_field(response.payload, "hasMore", "has_more", default=False)), tuple(issues))

    fetch_bars = fetch
    fetch_trendbars = fetch
    def fetch_history(self, timeframe: str = "M1", *, count: int | None = None, from_timestamp: datetime | None = None, to_timestamp: datetime | None = None, max_pages: int = 20) -> CTraderHistoryResult:
        """Fetch bounded pages and expose truncation/no-progress explicitly."""
        if isinstance(max_pages, bool) or max_pages <= 0:
            raise CTraderConfigurationError("max_pages debe ser positivo")
        start_bound = ensure_utc(from_timestamp, field_name="from_timestamp") if from_timestamp is not None else None
        cursor_to = to_timestamp
        collected: dict[tuple[datetime, int], Bar] = {}
        issues: list[str] = []
        pages = 0
        has_more = False
        previous_earliest: datetime | None = None
        while pages < max_pages:
            page = self.fetch(timeframe, count=count, from_timestamp=start_bound, to_timestamp=cursor_to)
            pages += 1; issues.extend(page.issues); has_more = page.has_more
            for bar in page.bars:
                collected[(bar.interval_start, int(bar.revision))] = bar
            if any(not bar.closed for bar in page.bars):
                issues.append("histórico incluye un intervalo abierto; cobertura cerrada incompleta")
            if not page.has_more:
                break
            earliest = min((bar.interval_start for bar in page.bars), default=None)
            if earliest is None or (previous_earliest is not None and earliest >= previous_earliest):
                issues.append("histórico hasMore sin progreso; cobertura incompleta"); break
            previous_earliest = earliest
            if start_bound is not None and earliest <= start_bound:
                break
            cursor_to = earliest - timedelta(seconds=resolution_to_seconds(_period_name(_period_code(timeframe))))
        else:
            issues.append("histórico excedió max_pages; cobertura incompleta")
        bars = tuple(sorted(collected.values(), key=lambda item: (item.interval_start, item.revision)))
        complete = not has_more and not any("incompleta" in issue or "abierto" in issue for issue in issues)
        return CTraderHistoryResult(bars, _period_name(_period_code(timeframe)), pages, complete, has_more, tuple(issues))


    def subscribe_spots(self, *, subscribe_to_spot_timestamp: bool = True) -> WireMessage:
        account_id = self._require_authenticated()
        spec = self._ensure_symbol()
        response = self.client.request("PROTO_OA_SUBSCRIBE_SPOTS_REQ", {"ctidTraderAccountId": account_id, "symbolId": [spec.symbol_id], "subscribeToSpotTimestamp": bool(subscribe_to_spot_timestamp)})
        return response

    def subscribe_live_trendbars(self, *, timeframes: Iterable[str] | None = None) -> tuple[WireMessage, ...]:
        account_id = self._require_authenticated()
        spec = self._ensure_symbol()
        responses: list[WireMessage] = []
        for timeframe in tuple(timeframes or self.config.timeframes):
            period = _period_code(timeframe)
            responses.append(self.client.request("PROTO_OA_SUBSCRIBE_LIVE_TRENDBAR_REQ", {"ctidTraderAccountId": account_id, "period": period, "symbolId": spec.symbol_id}))
            self._subscribed.add(_period_name(period))
        return tuple(responses)

    def subscribe(self, *, timeframes: Iterable[str] | None = None, subscribe_to_spot_timestamp: bool = True) -> tuple[WireMessage, ...]:
        account_id = self._require_authenticated()
        spec = self._ensure_symbol()
        responses: list[WireMessage] = []
        spots = self.client.request("PROTO_OA_SUBSCRIBE_SPOTS_REQ", {"ctidTraderAccountId": account_id, "symbolId": [spec.symbol_id], "subscribeToSpotTimestamp": bool(subscribe_to_spot_timestamp)})
        responses.append(spots)
        periods = tuple(timeframes or self.config.timeframes)
        for timeframe in periods:
            period = _period_code(timeframe)
            response = self.client.request("PROTO_OA_SUBSCRIBE_LIVE_TRENDBAR_REQ", {"ctidTraderAccountId": account_id, "period": period, "symbolId": spec.symbol_id})
            responses.append(response)
            self._subscribed.add(_period_name(period))
        return tuple(responses)

    def reconnect(self) -> CTraderStatus:
        self._subscribed.clear()
        self._quote_state.clear()
        return self.client.connect_with_retry()

    def reauthenticate_and_resubscribe(self, *, secret_provider: Callable[[str], str], token_provider: Callable[[str], str], timeframes: Iterable[str] | None = None) -> CTraderStatus:
        self.client.authenticate(secret_provider=secret_provider, token_provider=token_provider)
        self.subscribe(timeframes=timeframes)
        return self.status

    def normalize_spot(self, payload: Any, *, received_at: datetime | None = None, snapshot: bool = False, sequence: int | str | None = None) -> CTraderNormalizationResult:
        """Normalize a spot update while retaining bid/ask leg state."""

        result = normalize_spot_event(
            payload,
            spec=self.spec,
            quote_basis=self.config.quote_basis,
            received_at=received_at,
            snapshot=snapshot,
            sequence=sequence,
        )
        symbol_id = _int_or_none(_field(payload, "symbolId", "symbol_id", default=self.spec.symbol_id))
        if symbol_id is None:
            return result
        received = ensure_utc(received_at, field_name="received_at") if received_at is not None else datetime.now(UTC)
        raw_timestamp = _field(payload, "timestamp", default=None)
        try:
            event_time = _timestamp_ms(raw_timestamp) if raw_timestamp is not None else received
        except CTraderDataError:
            return result
        state = self._quote_state.setdefault(int(symbol_id), {})
        changed = False
        state_issues: list[str] = []
        for leg in ("bid", "ask"):
            raw_value = _field(payload, leg, default=None)
            if raw_value is None:
                continue
            try:
                price = self.spec.price_from_relative(raw_value)
            except CTraderDataError:
                continue
            previous_time = state.get(f"{leg}_event_time")
            if previous_time is not None and event_time < previous_time:
                # Do not let an out-of-order update rejuvenate a leg.
                state_issues.append(f"{leg} update fuera de orden")
                continue
            state[leg] = price
            state[f"{leg}_raw"] = int(raw_value)
            state[f"{leg}_event_time"] = event_time
            state[f"{leg}_received_at"] = received
            state[f"{leg}_timestamp_missing"] = raw_timestamp is None
            changed = True
        if not changed or "bid" not in state or "ask" not in state:
            if state_issues:
                return replace(result, issues=tuple(dict.fromkeys((*result.issues, *state_issues))))
            return result
        # If the source update was partial, synthesize only the missing wire
        # leg from our retained state; this is a stateful quote composition,
        # not tick interpolation. Native trendbars remain those from the
        # original message.
        has_both = _field(payload, "bid", default=None) is not None and _field(payload, "ask", default=None) is not None
        if has_both:
            combined = result
        else:
            combined_payload = dict(payload) if isinstance(payload, Mapping) else _mapping(payload)
            combined_payload["symbolId"] = symbol_id
            combined_payload["bid"] = state["bid_raw"]
            combined_payload["ask"] = state["ask_raw"]
            combined = normalize_spot_event(
                combined_payload,
                spec=self.spec,
                quote_basis=self.config.quote_basis,
                received_at=received,
                snapshot=snapshot,
                sequence=sequence,
            )
        if not combined.quote_events:
            return result
        bid_event = state.get("bid_event_time", event_time)
        ask_event = state.get("ask_event_time", event_time)
        bid_received = state.get("bid_received_at", received)
        ask_received = state.get("ask_received_at", received)
        leg_metadata = {
            "bid_source_timestamp": _iso(bid_event),
            "ask_source_timestamp": _iso(ask_event),
            "bid_received_at": _iso(bid_received),
            "ask_received_at": _iso(ask_received),
            "bid_age_seconds": max(0.0, (received - bid_event).total_seconds()),
            "ask_age_seconds": max(0.0, (received - ask_event).total_seconds()),
            "partial_update": not has_both,
            "quote_state_retained": True,
            "bid_source_timestamp_missing": bool(state.get("bid_timestamp_missing", False)),
            "ask_source_timestamp_missing": bool(state.get("ask_timestamp_missing", False)),
        }
        events: list[Event] = []
        for event in combined.quote_events:
            metadata = {**dict(event.metadata or {}), **leg_metadata}
            events.append(replace(event, metadata=metadata))
        return CTraderNormalizationResult(
            records=tuple(events) + tuple(result.bars),
            quote_events=tuple(events),
            bars=result.bars,
            issues=tuple(dict.fromkeys((*(issue for issue in result.issues if not issue.startswith("quote_basis=")), *state_issues, *combined.issues))),
            snapshot=bool(snapshot or result.snapshot),
            symbol_id=result.symbol_id if result.symbol_id is not None else symbol_id,
        )

    def stream(self, *, duration_seconds: float | None = None, max_events: int | None = None, timeout_seconds: float = 0.25) -> Iterator[Event | Bar]:
        start = time.monotonic()
        count = 0
        while max_events is None or count < max_events:
            if duration_seconds is not None and time.monotonic() - start >= duration_seconds:
                break
            message = self.client.poll_event(timeout_seconds)
            if message is None:
                continue
            if message.payload_type_id == PAYLOAD["PROTO_HEARTBEAT_EVENT"]:
                self.client.heartbeat()
                continue
            if message.payload_type_id != PAYLOAD["PROTO_OA_SPOT_EVENT"]:
                continue
            normalized = self.normalize_spot(message.payload, received_at=datetime.now(UTC), snapshot=False, sequence=count)
            for record in normalized.records:
                count += 1
                yield record

    iter_events = stream


# Aliases explícitos para integradores que nombran el adaptador por protocolo.
CTraderAdapter = CTraderProvider
CTraderOpenAPIProvider = CTraderProvider


class KrakenLikeProvider(CTraderProvider):  # pragma: no cover - compatibility name only
    """Deprecated alias intentionally not exported as a production route."""


def normalize_trendbar(raw: Any, *, spec: CTraderInstrumentSpec, received_at: datetime | None = None, request_id: str = "", mode: str = "REPLAY", revision: int = 0) -> Bar:
    """Transforma ProtoOATrendbar relativo a OHLC absoluto escalado."""

    low_raw = _field(raw, "low", default=None)
    open_delta = _field(raw, "deltaOpen", "delta_open", default=None)
    close_delta = _field(raw, "deltaClose", "delta_close", default=None)
    high_delta = _field(raw, "deltaHigh", "delta_high", default=None)
    timestamp_minutes = _field(raw, "utcTimestampInMinutes", "utc_timestamp_in_minutes", default=None)
    period_raw = _field(raw, "period", default=None)
    volume_raw = _field(raw, "volume", default=None)
    if None in {low_raw, open_delta, close_delta, high_delta, timestamp_minutes, volume_raw}:
        raise CTraderDataError("trendbar incompleto: low/deltas/timestamp/volume son necesarios")
    try:
        low = spec.price_from_relative(low_raw)
        open_price = spec.price_from_relative(int(low_raw) + int(open_delta))
        close_price = spec.price_from_relative(int(low_raw) + int(close_delta))
        high_price = spec.price_from_relative(int(low_raw) + int(high_delta))
        start = datetime.fromtimestamp(int(timestamp_minutes) * 60, UTC)
        period_code = _period_code(period_raw) if not isinstance(period_raw, int) or period_raw not in TREND_PERIOD_NAMES else int(period_raw)
        timeframe = _period_name(period_code)
        seconds = resolution_to_seconds(timeframe)
        protocol_volume = int(volume_raw)
    except (TypeError, ValueError, OverflowError) as exc:
        raise CTraderDataError(f"trendbar inválido: {exc}") from exc
    if protocol_volume < 0 or high_price < max(open_price, close_price, low) or low > min(open_price, close_price, high_price):
        raise CTraderDataError("trendbar con OHLC/volumen incoherente")
    end = start + timedelta(seconds=seconds)
    received = ensure_utc(received_at, field_name="received_at") if received_at is not None else None
    # A trendbar is closed only with observed receipt at/after interval end.
    closed = bool(received is not None and received >= end)
    available = received
    # Request id sólo documenta procedencia; la identidad base de una vela es
    # símbolo/periodo/inicio y la revisión explícita, no la llamada que la
    # recuperó. Así dos lecturas del mismo histórico son idempotentes.
    token = f"ctrader|{spec.symbol_id}|{timeframe}|{int(timestamp_minutes)}|r{int(revision)}"
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
        price_basis="traded",
        source="ctrader-open-api",
        source_record_id="ctrader-bar-" + hashlib.sha256(token.encode()).hexdigest()[:32],
        received_at=received,
        available_at=available,
        closed=closed,
        synthetic=False,
        revision=int(revision),
        metadata={"provider": "ctrader", "mode": mode, "relative_scale": spec.price_scale, "period_code": period_code, "request_id": request_id, "price_basis_note": "trendbar del proveedor; no se infiere bid/ask/mid", "protocol_volume": protocol_volume, "no_tick_interpolation": True, "closed_evidence": "received_at>=interval_end" if closed else "not_observed"},
    )


def normalize_spot_event(payload: Any, *, spec: CTraderInstrumentSpec, quote_basis: str = "mid", received_at: datetime | None = None, snapshot: bool = False, sequence: int | str | None = None) -> CTraderNormalizationResult:
    """Normaliza bid/ask y trendbars repetidos de ProtoOASpotEvent."""

    received = ensure_utc(received_at, field_name="received_at") if received_at is not None else datetime.now(UTC)
    snapshot = bool(snapshot or _field(payload, "snapshot", "isSnapshot", "is_snapshot", default=False))
    symbol_id = _int_or_none(_field(payload, "symbolId", "symbol_id", default=spec.symbol_id))
    if spec.symbol_id is not None and symbol_id is not None and symbol_id != spec.symbol_id:
        raise CTraderDataError(f"spot event symbolId {symbol_id} no coincide con {spec.symbol_id}")
    raw_timestamp = _field(payload, "timestamp", default=None)
    timestamp_missing = raw_timestamp is None
    event_time = _timestamp_ms(raw_timestamp) if raw_timestamp is not None else received
    # A timestamp de origen adelantado (por reloj del proveedor) no se le
    # asigna una disponibilidad anterior: el registro queda disponible como
    # mínimo en max(event_time, received_at).
    available = max(received, event_time)
    bid_raw = _field(payload, "bid", default=None)
    ask_raw = _field(payload, "ask", default=None)
    bid = spec.price_from_relative(bid_raw) if bid_raw is not None else None
    ask = spec.price_from_relative(ask_raw) if ask_raw is not None else None
    basis = str(quote_basis).lower()
    issues: list[str] = []
    if timestamp_missing:
        issues.append("source timestamp ausente; received_at no demuestra frescura de mercado")
    quote_events: list[Event] = []
    if basis == "mid" and bid is not None and ask is not None:
        selected = round((bid + ask) / 2.0, spec.digits)
    elif basis == "bid" and bid is not None:
        selected = bid
    elif basis == "ask" and ask is not None:
        selected = ask
    else:
        selected = None
        issues.append(f"quote_basis={basis} no evaluable con bid/ask opcionales presentes")
    if selected is not None:
        token_payload = {"symbol_id": symbol_id, "timestamp": raw_timestamp, "bid": bid_raw, "ask": ask_raw, "sequence": sequence, "snapshot": snapshot}
        event_id = "ctrader-spot-" + hashlib.sha256(json.dumps(token_payload, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()[:32]
        quote_events.append(Event(
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
            metadata={"provider": "ctrader", "symbol_id": symbol_id, "raw_timestamp_ms": raw_timestamp, "source_timestamp_missing": timestamp_missing, "quote_basis": basis, "bid_relative": bid_raw, "ask_relative": ask_raw, "session_close_relative": _field(payload, "sessionClose", "session_close", default=None), "quality": "PUBLIC_PROVIDER"},
        ))
    bars: list[Bar] = []
    for index, raw_bar in enumerate(_as_sequence(_field(payload, "trendbar", "trendbars", default=()))):
        try:
            bars.append(normalize_trendbar(raw_bar, spec=spec, received_at=received, request_id=f"spot-{sequence if sequence is not None else index}", mode="LIVE"))
        except CTraderDataError as exc:
            issues.append(str(exc))
    return CTraderNormalizationResult(tuple(quote_events) + tuple(bars), tuple(quote_events), tuple(bars), tuple(issues), bool(snapshot), symbol_id)


def synthetic_trendbar(*, timestamp_minutes: int, period: str = "M1", low_relative: int = 110_000, delta_open: int = 10, delta_close: int = 20, delta_high: int = 30, volume: int = 42) -> dict[str, Any]:
    """Fixture sintético de protocolo, no es cotización real de EUR/USD."""

    return {"period": TREND_PERIODS[period.upper()], "low": low_relative, "deltaOpen": delta_open, "deltaClose": delta_close, "deltaHigh": delta_high, "utcTimestampInMinutes": timestamp_minutes, "volume": volume, "synthetic_fixture": True}


def synthetic_spot_event(*, timestamp_ms: int, symbol_id: int, bid_relative: int = 110_000, ask_relative: int = 110_020, trendbars: Sequence[Mapping[str, Any]] = (), snapshot: bool = False) -> dict[str, Any]:
    """Fixture sintético completo de spot/trendbars para pruebas offline."""

    return {"symbolId": symbol_id, "timestamp": timestamp_ms, "bid": bid_relative, "ask": ask_relative, "trendbar": list(trendbars), "synthetic_fixture": True, "snapshot": snapshot}


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
    code = _period_code(value)
    return TREND_PERIOD_NAMES[code]


def _timestamp_ms(value: Any) -> datetime:
    if isinstance(value, datetime):
        return ensure_utc(value, field_name="timestamp")
    try:
        number = int(value)
        return datetime.fromtimestamp(number / 1000.0, UTC)
    except (TypeError, ValueError, OverflowError) as exc:
        raise CTraderDataError(f"timestamp ms inválido: {value!r}") from exc


def _unix_ms(value: datetime) -> int:
    return int(ensure_utc(value, field_name="timestamp").timestamp() * 1000)


def _field(value: Any, *names: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        for name in names:
            if name in value:
                return value[name]
        return default
    # Generated protobuf messages expose scalar defaults (0/empty string) for
    # absent optional fields. HasField is therefore consulted first so a
    # missing bid/ask/timestamp remains unknown instead of becoming zero.
    has_field = getattr(value, "HasField", None)
    for name in names:
        if callable(has_field):
            try:
                if not has_field(name):
                    continue
            except (ValueError, TypeError):
                # Repeated fields do not support HasField; inspect normally.
                pass
        if hasattr(value, name):
            return getattr(value, name)
    return default


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
    """Return safe, JSON-ready fields from a mapping or generated message."""

    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    descriptor = getattr(value, "DESCRIPTOR", None)
    fields_by_name = getattr(descriptor, "fields", ()) if descriptor is not None else ()
    result: dict[str, Any] = {}
    for field_descriptor in fields_by_name:
        name = str(field_descriptor.name)
        raw = _field(value, name, default=None)
        if raw is None:
            continue
        try:
            if getattr(field_descriptor, "label", None) == getattr(field_descriptor, "LABEL_REPEATED", 3) or (
                getattr(field_descriptor, "is_repeated", False)
            ):
                result[name] = [_jsonable(item) for item in raw]
            elif getattr(field_descriptor, "message_type", None) is not None:
                result[name] = _mapping(raw)
            else:
                result[name] = _jsonable(raw)
        except (TypeError, ValueError):
            continue
    if not result:
        for name in ("symbolId", "symbolName", "digits", "pipPosition", "enabled"):
            if hasattr(value, name):
                result[name] = _jsonable(getattr(value, name))
    return result


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _bool_or_none(value: Any) -> bool | None:
    return None if value is None else bool(value)


def _enum_label(owner: Any, field_name: str, value: Any) -> Any:
    if value is None or isinstance(value, str):
        return value
    descriptor = getattr(owner, "DESCRIPTOR", None)
    fields_by_name = getattr(descriptor, "fields_by_name", None)
    field = fields_by_name.get(field_name) if fields_by_name is not None else None
    enum_descriptor = getattr(field, "enum_type", None)
    if enum_descriptor is not None:
        try:
            number = int(value)
        except (TypeError, ValueError):
            return value
        for item in getattr(enum_descriptor, "values", ()):
            if int(item.number) == number:
                return str(item.name)
    return value


def _normalize_account_record(item: Any) -> dict[str, Any]:
    """Normalize one official ``ProtoOACtidTraderAccount`` record."""

    value = _field(
        item,
        "ctidTraderAccountId",
        "ctid_trader_account_id",
        "account_id",
        "id",
        default=item if isinstance(item, (int, str)) else None,
    )
    try:
        account_id = int(value)
    except (TypeError, ValueError) as exc:
        raise CTraderDataError("respuesta OAuth contiene una cuenta sin account_id válido") from exc
    if account_id <= 0:
        raise CTraderDataError("respuesta OAuth contiene un account_id no positivo")

    is_live = _field(item, "isLive", "is_live", default=None)
    if is_live is None:
        environment = "UNKNOWN"
    elif isinstance(is_live, str):
        environment = "LIVE" if is_live.strip().lower() in {"1", "true", "yes", "live"} else "DEMO"
    else:
        environment = "LIVE" if bool(is_live) else "DEMO"

    record: dict[str, Any] = {"account_id": account_id, "environment": environment}
    # Keep non-sensitive account metadata when the SDK/schema provides it.
    optional_fields = (
        ("traderLogin", "trader_login", int),
        ("lastClosingDealTimestamp", "last_closing_deal_timestamp", int),
        ("lastBalanceUpdateTimestamp", "last_balance_update_timestamp", int),
        ("brokerTitleShort", "broker_title_short", str),
    )
    for source_name, output_name, converter in optional_fields:
        raw_value = _field(item, source_name, output_name, default=None)
        if raw_value is None:
            continue
        try:
            record[output_name] = converter(raw_value)
        except (TypeError, ValueError):
            # Optional metadata must not make a valid account undiscoverable.
            continue
    return record


def _normalize_account_payload(payload: Any) -> dict[str, Any]:
    """Keep the account-list response shape while removing the access token."""

    raw_records = _field(
        payload,
        "ctidTraderAccount",
        "ctidTraderAccounts",
        "accounts",
        "records",
        default=(),
    )
    if isinstance(payload, (list, tuple)):
        raw_records = payload
    records = [_normalize_account_record(item) for item in _as_sequence(raw_records)]
    ids = [record["account_id"] for record in records]
    if len(ids) != len(set(ids)):
        raise CTraderDataError("respuesta OAuth contiene account_id duplicados")
    permission_scope = _enum_label(
        payload,
        "permissionScope",
        _field(payload, "permissionScope", "permission_scope", default=None),
    )
    if permission_scope is not None:
        for record in records:
            record["permission_scope"] = permission_scope
    return {
        "records": records,
        # This is response metadata, not an account selection.  Preserve it
        # verbatim (including None when the provider omitted the optional
        # field) and never include the response accessToken.
        "permissionScope": permission_scope,
    }


def _account_ids(payload: Any) -> list[int]:
    return [int(record["account_id"]) for record in _normalize_account_payload(payload)["records"]]


def _is_error_message(message: WireMessage) -> bool:
    return message.payload_type_id in {PAYLOAD["PROTO_ERROR_RES"], PAYLOAD["PROTO_OA_ERROR_RES"]} or _field(message.payload, "errorCode", "error_code", default=None) is not None


def _error_fields(payload: Any) -> tuple[str, str, int | None]:
    code = _field(payload, "errorCode", "error_code", default="UNKNOWN_ERROR")
    description = _field(payload, "description", default="")
    retry = _field(payload, "retryAfter", "retry_after", default=None)
    return str(code), str(description or ""), _int_or_none(retry)


def _cancelled(cancel: CancellationToken | Callable[[], bool] | None) -> bool:
    if cancel is None:
        return False
    if isinstance(cancel, CancellationToken):
        return cancel.cancelled
    return bool(cancel())


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z") if value else None


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, datetime):
        return _iso(value)
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return _jsonable(value.to_dict())
    if hasattr(value, "DESCRIPTOR") and hasattr(value, "SerializeToString"):
        try:
            from google.protobuf.json_format import MessageToDict
            return _jsonable(MessageToDict(value, preserving_proto_field_name=True))
        except Exception:
            return {"protobuf_type": type(value).__name__}
    if hasattr(value, "value") and not isinstance(value, (str, bytes)):
        return value.value
    if hasattr(value, "__dict__") and not isinstance(value, type):
        return {str(key): _jsonable(item) for key, item in vars(value).items() if not str(key).startswith("_")}
    return value


def _redact(value: Any) -> Any:
    if hasattr(value, "DESCRIPTOR") and hasattr(value, "SerializeToString"):
        return _redact(_jsonable(value))
    if isinstance(value, Mapping):
        return {str(key): ("<redacted>" if str(key).lower() in {"clientsecret", "client_secret", "accesstoken", "access_token", "refreshtoken", "refresh_token"} else _redact(item)) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_redact(item) for item in value]
    return _jsonable(value)


def replace_status(status: CTraderStatus, **changes: Any) -> CTraderStatus:
    """``dataclasses.replace`` local para mantener anotación explícita."""

    from dataclasses import replace

    return replace(status, **changes)


__all__ = [
    "AuthState",
    "Bar",
    "CancellationToken",
    "CTraderAuthError",
    "CTraderClient",
    "CTraderCodec",
    "CTraderConfig",
    "CTraderConfigurationError",
    "CTraderDataError",
    "CTraderDependencyError",
    "CTraderError",
    "CTraderFetchResult",
    "CTraderInstrumentSpec",
    "CTraderNormalizationResult",
    "CTraderProtocolError",
    "CTraderProvider",
    "CTraderRateLimiter",
    "CTraderAdapter",
    "CTraderOpenAPIProvider",
    "CTraderRequestCancelled",
    "CTraderRequestTimeout",
    "CTraderStatus",
    "CTraderSymbol",
    "CTraderTransport",
    "CTraderTransportError",
    "ConnectionState",
    "DependencyReport",
    "DependencyState",
    "DeterministicTransport",
    "Event",
    "PAYLOAD",
    "PAYLOAD_NAMES",
    "SdkProtobufCodec",
    "SymbolCatalog",
    "TREND_PERIODS",
    "TcpTlsTransport",
    "WireMessage",
    "WIRE_CLASS_NAMES",
    "dependency_report",
    "normalize_spot_event",
    "normalize_symbol_name",
    "normalize_trendbar",
    "synthetic_spot_event",
    "synthetic_trendbar",
]
