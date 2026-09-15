"""cTrader wire constants, Protobuf presence and codec contracts.

This module deliberately has no session, socket, OAuth, or domain-state
dependencies.  It is the single place where generated Protobuf messages are
read.  The DEMO execution adapter imports :func:`field_present` and
:func:`read_field` from here as well, so optional and repeated fields have one
presence policy across both adapters.
"""

from __future__ import annotations

import hashlib
import importlib
import importlib.metadata
import importlib.util
import json
import re
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Protocol, runtime_checkable

from .ctrader_errors import (
    CTraderDependencyError,
    CTraderProtocolError,
    DependencyState,
)

SDK_MODULE = "ctrader_open_api"
PROTOBUF_MODULE = "google.protobuf"
GENERATED_PROTOBUF_PACKAGE = "mtf_lab.data.protobuf_generated"
SCHEMA_REVISION = "openapi-proto-messages@91:017413087c1c23c1866bbf07ff24d56574047253"
MAX_FRAME_LENGTH = 15_000_000
PROTOBUF_SAFETY_STATE_PATCH_REQUIREMENT_SATISFIED = "PATCH_REQUIREMENT_SATISFIED"
# Compatibility symbol only; the serialized state is intentionally
# advisory-specific and never means that an external server is approved.
PROTOBUF_SAFETY_STATE_SAFE = PROTOBUF_SAFETY_STATE_PATCH_REQUIREMENT_SATISFIED
PROTOBUF_SAFETY_STATE_VULNERABLE = "VULNERABLE"
PROTOBUF_SAFETY_STATE_NOT_VERIFIED = "NOT_VERIFIED"

# Official ProtoOAPayloadType/Common payload types used by the read-only data
# adapter.  Execution payloads intentionally are not added here.
PAYLOAD: dict[str, int] = {
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
    "PROTO_OA_GET_TICKDATA_REQ": 2145,
    "PROTO_OA_GET_TICKDATA_RES": 2146,
    "PROTO_OA_ERROR_RES": 2142,
    "PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_REQ": 2149,
    "PROTO_OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_RES": 2150,
}
SESSION_CONTROL_PAYLOAD_TYPES = {
    "PROTO_OA_ACCOUNTS_TOKEN_INVALIDATED_EVENT": 2147,
    "PROTO_OA_CLIENT_DISCONNECT_EVENT": 2148,
    "PROTO_OA_ACCOUNT_DISCONNECT_EVENT": 2164,
}
SESSION_CONTROL_TYPE_NAMES = frozenset(
    {
        *SESSION_CONTROL_PAYLOAD_TYPES,
        "ACCOUNT_DISCONNECT",
        "CLIENT_DISCONNECT",
        "ACCOUNTS_TOKEN_INVALIDATED",
        "TOKEN_INVALIDATED",
    }
)
PAYLOAD.update(SESSION_CONTROL_PAYLOAD_TYPES)
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
    "PROTO_OA_GET_TICKDATA_REQ": "ProtoOAGetTickDataReq",
    "PROTO_OA_GET_TICKDATA_RES": "ProtoOAGetTickDataRes",
    "PROTO_HEARTBEAT_EVENT": "ProtoHeartbeatEvent",
}

# Concrete message payload defaults used by the shared typed gateway.  These
# are protocol IDs, not inferred from a string at runtime; generated SDK
# bundled generated descriptors remain authoritative.
MESSAGE_PAYLOAD_TYPES = {
    "ProtoOAApplicationAuthReq": 2100,
    "ProtoOAAccountAuthReq": 2102,
    "ProtoOAGetAccountListByAccessTokenReq": 2149,
    "ProtoOASymbolsListReq": 2114,
    "ProtoOASymbolByIdReq": 2116,
    "ProtoOASubscribeSpotsReq": 2127,
    "ProtoOASubscribeLiveTrendbarReq": 2135,
    "ProtoOAGetTrendbarsReq": 2137,
    "ProtoOAGetTickDataReq": 2145,
    "ProtoOAGetTickDataRes": 2146,
    "ProtoOANewOrderReq": 2106,
    "ProtoOAClosePositionReq": 2111,
    "ProtoOAReconcileReq": 2124,
    "ProtoOAOrderListReq": 2175,
    "ProtoOADealListReq": 2133,
    "ProtoOADealListByPositionIdReq": 2179,
    "ProtoOAOrderDetailsReq": 2181,
    "ProtoHeartbeatEvent": 51,
}

TREND_PERIODS = {"M1": 1, "M5": 5, "M15": 7}
TREND_PERIOD_NAMES = {value: key for key, value in TREND_PERIODS.items()}

_CAPTURE_MESSAGE_CLASSES = frozenset({"spot", "clock", "connection", "revision", "end"})
_CAPTURE_STATES = frozenset({"CONNECTED", "DISCONNECTED", "RECONNECTED", "RECONCILED", "END"})
_CAPTURE_REASON_CODES = frozenset(
    {
        "EOF",
        "TRANSPORT_ERROR",
        "DECODE_ERROR",
        "CLIENT_CLOSED",
        "RECONNECTED",
        "DISCONNECTED",
        "SESSION_CHANGED",
        "TIMEOUT",
        "RECONCILED",
        "UNKNOWN",
    }
)
_CAPTURE_SPOT_FIELDS = frozenset(
    {
        "symbolId",
        "symbol_id",
        "timestamp",
        "timestamp_ms",
        "timestamp_unit",
        "timestampUnit",
        "bid",
        "ask",
        "sessionClose",
        "session_close",
        "trendbar",
        "trendbars",
        "snapshot",
        "isSnapshot",
        "is_snapshot",
        "sequence",
        "sourceSequence",
        "source_sequence",
        "synthetic_fixture",
        "synthetic",
    }
)
_CAPTURE_TRENDBAR_FIELDS = frozenset(
    {
        "period",
        "low",
        "deltaOpen",
        "delta_open",
        "deltaClose",
        "delta_close",
        "deltaHigh",
        "delta_high",
        "utcTimestampInMinutes",
        "utc_timestamp_in_minutes",
        "volume",
        "synthetic_fixture",
    }
)
_CAPTURE_CONNECTION_FIELDS = frozenset({"state", "reason_code", "reasonCode", "reason", "error_code", "errorCode"})


@dataclass(frozen=True, slots=True)
class DependencyReport:
    sdk_module: str
    protobuf_module: str
    sdk_state: DependencyState
    protobuf_state: DependencyState
    sdk_version: str | None = None
    message: str = ""
    codec_state: DependencyState = DependencyState.NOT_VERIFIED
    codec_backend: str | None = None
    protobuf_version: str | None = None
    protobuf_implementation: str | None = None
    schema_revision: str | None = None
    security_state: str = PROTOBUF_SAFETY_STATE_NOT_VERIFIED

    @property
    def codec_operational(self) -> bool:
        return (
            self.codec_state is DependencyState.AVAILABLE
            and self.security_state == PROTOBUF_SAFETY_STATE_PATCH_REQUIREMENT_SATISFIED
        )

    @property
    def available(self) -> bool:
        return self.protobuf_state is DependencyState.AVAILABLE and self.codec_operational

    def to_dict(self) -> dict[str, Any]:
        return {
            "sdk_module": self.sdk_module,
            "protobuf_module": self.protobuf_module,
            "sdk_state": self.sdk_state.value,
            "protobuf_state": self.protobuf_state.value,
            "codec_state": self.codec_state.value,
            "codec_operational": self.codec_operational,
            "sdk_version": self.sdk_version,
            "codec_backend": self.codec_backend,
            "protobuf_version": self.protobuf_version,
            "protobuf_implementation": self.protobuf_implementation,
            "schema_revision": self.schema_revision,
            "security_state": self.security_state,
            "available": self.available,
            "message": self.message,
        }


def dependency_report() -> DependencyReport:
    """Inspect the bundled codec without loading the legacy SDK reactor."""
    sdk_state, protobuf_state = _dependency_states()
    sdk_version = _sdk_version()
    messages: list[str] = []
    if protobuf_state is DependencyState.MISSING:
        probe = _CodecProbe(DependencyState.MISSING, "falta google.protobuf")
    else:
        probe = _probe_codec()
    if probe.detail:
        messages.append(probe.detail)
    security_state = _protobuf_security_state(probe.protobuf_version)
    if security_state == PROTOBUF_SAFETY_STATE_VULNERABLE and probe.protobuf_version:
        messages.append(f"protobuf {probe.protobuf_version} no cumple el mínimo de seguridad del codec")
    if probe.state is not DependencyState.AVAILABLE and not probe.detail:
        messages.append("codec Protobuf generado no disponible")
    return DependencyReport(
        SDK_MODULE,
        PROTOBUF_MODULE,
        sdk_state,
        protobuf_state,
        sdk_version=sdk_version,
        message="; ".join(messages),
        codec_state=probe.state,
        codec_backend=probe.backend,
        protobuf_version=probe.protobuf_version,
        protobuf_implementation=probe.implementation,
        schema_revision=probe.schema_revision,
        security_state=security_state,
    )


def _dependency_states() -> tuple[DependencyState, DependencyState]:
    return _module_state(SDK_MODULE), _module_state(PROTOBUF_MODULE)


def _module_state(name: str) -> DependencyState:
    try:
        return DependencyState.AVAILABLE if importlib.util.find_spec(name) is not None else DependencyState.MISSING
    except ModuleNotFoundError:
        return DependencyState.MISSING


def _sdk_version() -> str | None:
    try:
        return importlib.metadata.version("ctrader-open-api")
    except importlib.metadata.PackageNotFoundError:
        return None


@dataclass(frozen=True, slots=True)
class _CodecProbe:
    state: DependencyState
    detail: str | None = None
    backend: str | None = None
    protobuf_version: str | None = None
    implementation: str | None = None
    schema_revision: str | None = None


def _probe_codec() -> _CodecProbe:
    probe = """
import json
import google.protobuf
from google.protobuf.internal import api_implementation

result = {
    "protobuf_version": getattr(google.protobuf, "__version__", None),
    "implementation": api_implementation.Type(),
}
try:
    import mtf_lab.data.protobuf_generated as g
    from mtf_lab.data.protobuf_generated import OpenApiCommonMessages_pb2 as c
    from mtf_lab.data.protobuf_generated import OpenApiMessages_pb2 as m

    assert hasattr(c, "ProtoMessage")
    assert hasattr(c, "ProtoHeartbeatEvent")
    assert hasattr(m, "ProtoOANewOrderReq")
    result.update({"codec": "bundled_official_generated", "schema_revision": g.SCHEMA_REVISION})
    print(json.dumps(result, sort_keys=True))
except Exception as exc:
    result["error_type"] = type(exc).__name__
    print(json.dumps(result, sort_keys=True))
    raise
""".strip()
    try:
        result = subprocess.run(
            [sys.executable, "-c", probe],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return _CodecProbe(DependencyState.UNIMPORTABLE, f"sonda codec falló: {type(exc).__name__}")
    lines = (result.stdout or "").strip().splitlines()
    metadata: dict[str, Any] = {}
    if lines:
        try:
            decoded = json.loads(lines[-1])
        except json.JSONDecodeError:
            decoded = {}
        if isinstance(decoded, dict):
            metadata = decoded
    version = metadata.get("protobuf_version")
    implementation = metadata.get("implementation")
    backend = metadata.get("codec")
    schema_revision = metadata.get("schema_revision")
    if result.returncode == 0 and backend == "bundled_official_generated":
        return _CodecProbe(
            DependencyState.AVAILABLE,
            backend=backend,
            protobuf_version=str(version) if version is not None else None,
            implementation=str(implementation) if implementation is not None else None,
            schema_revision=str(schema_revision) if schema_revision is not None else None,
        )
    error_type = metadata.get("error_type")
    detail = f"codec Protobuf no importable: {error_type}" if error_type else "codec Protobuf no importable"
    return _CodecProbe(
        DependencyState.UNIMPORTABLE,
        detail,
        backend=backend,
        protobuf_version=str(version) if version is not None else None,
        implementation=str(implementation) if implementation is not None else None,
        schema_revision=str(schema_revision) if schema_revision is not None else None,
    )


def _protobuf_security_state(version: str | None) -> str:
    """Classify the known pure-Python protobuf recursion-DoS floor."""

    if version is None:
        return PROTOBUF_SAFETY_STATE_NOT_VERIFIED
    match = re.match(r"^(\d+)(?:\.(\d+))?(?:\.(\d+))?", str(version).strip())
    if match is None:
        return PROTOBUF_SAFETY_STATE_NOT_VERIFIED
    major, minor, patch = (int(part or 0) for part in match.groups())
    if major < 4:
        return PROTOBUF_SAFETY_STATE_VULNERABLE
    if major == 4 and (minor, patch) < (25, 8):
        return PROTOBUF_SAFETY_STATE_VULNERABLE
    if major == 5 and (minor, patch) < (29, 5):
        return PROTOBUF_SAFETY_STATE_VULNERABLE
    if major == 6 and (minor, patch) < (31, 1):
        return PROTOBUF_SAFETY_STATE_VULNERABLE
    return PROTOBUF_SAFETY_STATE_PATCH_REQUIREMENT_SATISFIED


@dataclass(frozen=True, slots=True)
class WireMessage:
    """Logical envelope plus reader-owned local capture metadata."""

    payload_type: int | str
    payload: Any = None
    client_msg_id: str | None = None
    is_event: bool = False
    received_at: datetime | None = None
    available_at: datetime | None = None
    ingest_sequence: int | None = None
    connection_generation: int | None = None
    message_class: str | None = None
    source_identity: str | None = None

    def __post_init__(self) -> None:
        received, available = _wire_times(self.received_at, self.available_at)
        _validate_wire_counters(self.ingest_sequence, self.connection_generation)
        object.__setattr__(self, "received_at", received)
        object.__setattr__(self, "available_at", available)
        if self.message_class is not None:
            object.__setattr__(self, "message_class", str(self.message_class).strip().lower() or None)

    def with_capture_metadata(
        self,
        *,
        received_at: datetime | None,
        available_at: datetime | None = None,
        ingest_sequence: int | None = None,
        connection_generation: int | None = None,
        message_class: str | None = None,
        source_identity: str | None = None,
    ) -> WireMessage:
        """Return a detached message stamped by the sole receive reader."""
        return replace(
            self,
            received_at=received_at,
            available_at=received_at if available_at is None else available_at,
            ingest_sequence=ingest_sequence,
            connection_generation=connection_generation,
            message_class=message_class or self.message_class or _wire_message_class(self),
            source_identity=source_identity if source_identity is not None else self.source_identity,
        )

    def capture_envelope(self) -> dict[str, Any]:
        return _capture_wire_envelope(self)

    @property
    def payload_type_id(self) -> int | None:
        if isinstance(self.payload_type, int) and not isinstance(self.payload_type, bool):
            return self.payload_type
        value = PAYLOAD.get(str(self.payload_type).upper())
        if value is not None:
            return value
        text = str(self.payload_type)
        return int(text) if text.isdigit() else None

    @property
    def payload_type_name(self) -> str:
        payload_type_id = self.payload_type_id
        if payload_type_id is None:
            return str(self.payload_type)
        return PAYLOAD_NAMES.get(payload_type_id, str(self.payload_type))

    def to_dict(self, *, redact: bool = True) -> dict[str, Any]:
        payload = redact_value(self.payload) if redact else jsonable(self.payload)
        return {
            "payload_type": self.payload_type_name,
            "payload_type_id": self.payload_type_id,
            "client_msg_id": self.client_msg_id,
            "is_event": self.is_event,
            "received_at": _wire_iso(self.received_at),
            "available_at": _wire_iso(self.available_at),
            "ingest_sequence": self.ingest_sequence,
            "connection_generation": self.connection_generation,
            "message_class": self.message_class,
            "source_identity": self.source_identity,
            "payload": payload,
        }


def _wire_times(
    received_value: datetime | None,
    available_value: datetime | None,
) -> tuple[datetime | None, datetime | None]:
    received = _wire_utc(received_value)
    available = _wire_utc(available_value)
    if received is not None and available is not None and available < received:
        raise ValueError("WireMessage.available_at cannot precede received_at")
    return received, available


def _validate_wire_counters(
    ingest_sequence: int | None,
    connection_generation: int | None,
) -> None:
    for name, value in (
        ("ingest_sequence", ingest_sequence),
        ("connection_generation", connection_generation),
    ):
        if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 0):
            raise ValueError(f"WireMessage.{name} must be a non-negative integer")


def _wire_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("WireMessage timestamps require timezone")
    return value.astimezone(UTC)


def _wire_iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z") if value else None


def _wire_event_time(payload: Any) -> datetime | None:
    if not isinstance(payload, Mapping):
        return None
    raw = payload.get("timestamp", payload.get("timestamp_ms"))
    if raw is None:
        return None
    try:
        return datetime.fromtimestamp(int(raw) / 1000.0, UTC)
    except (TypeError, ValueError, OverflowError):
        return None


def _wire_message_class(message: WireMessage) -> str:
    if message.payload_type_id == PAYLOAD.get("PROTO_HEARTBEAT_EVENT"):
        return "clock"
    if message.payload_type_id == PAYLOAD.get("PROTO_OA_SPOT_EVENT"):
        return "spot"
    if str(message.payload_type).upper() in SESSION_CONTROL_TYPE_NAMES:
        return "connection"
    if "DISCONNECT" in str(message.payload_type).upper():
        return "connection"
    return "revision"


def _capture_message_class(message: WireMessage) -> str:
    value = message.message_class or _wire_message_class(message)
    return value if value in {"spot", "clock", "connection", "revision", "end"} else "revision"


def capture_envelope(message: WireMessage) -> dict[str, Any]:
    if not isinstance(message, WireMessage):
        raise TypeError("message debe ser WireMessage")
    return _capture_wire_envelope(message)


def _capture_wire_envelope(message: WireMessage) -> dict[str, Any]:
    message_class = _capture_message_class(message)
    payload = _capture_payload(message, message_class)
    event_time = _wire_event_time(payload) if message_class in {"spot", "revision"} else None
    return {
        "capture_schema": 1,
        "event_time": event_time,
        "received_at": message.received_at,
        "available_at": message.available_at,
        "ingest_sequence": message.ingest_sequence,
        "connection_generation": message.connection_generation,
        "source_identity": message.source_identity,
        "message_class": message_class,
        "availability_policy": "observed" if message.received_at is not None else "unknown",
        "payload": payload,
    }


def _capture_payload(message: WireMessage, message_class: str) -> dict[str, Any]:
    if message_class == "clock":
        if message.payload_type_id != PAYLOAD.get("PROTO_HEARTBEAT_EVENT"):
            raise CTraderProtocolError("CAPTURE_UNSUPPORTED", "clock capture requires heartbeat payload")
        return {"clock_kind": "heartbeat"}
    if message_class in {"spot", "revision"}:
        if message.payload_type_id != PAYLOAD.get("PROTO_OA_SPOT_EVENT"):
            raise CTraderProtocolError(
                "CAPTURE_UNSUPPORTED",
                "only ProtoOASpotEvent may enter the market capture ledger",
            )
        return _capture_spot_payload(message.payload)
    if message_class == "connection":
        return _capture_connection_payload(message.payload)
    if message_class == "end":
        return _capture_end_payload(message.payload)
    raise CTraderProtocolError("CAPTURE_UNSUPPORTED", "message class is not allowlisted")


def _capture_spot_payload(value: Any) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name in sorted(_CAPTURE_SPOT_FIELDS):
        if not field_present(value, name):
            continue
        raw = read_field(value, name, default=None)
        if name in {"trendbar", "trendbars"}:
            if "trendbar" in result:
                continue
            result["trendbar"] = [_capture_trendbar(item) for item in read_repeated(value, name)]
        else:
            result[name] = _capture_scalar(raw)
    return result


def _capture_trendbar(value: Any) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name in sorted(_CAPTURE_TRENDBAR_FIELDS):
        if field_present(value, name):
            result[name] = _capture_scalar(read_field(value, name, default=None))
    return result


def _capture_connection_payload(value: Any) -> dict[str, Any]:
    raw_state = read_field(value, "state", default="DISCONNECTED")
    state = str(raw_state).strip().upper()
    if state not in _CAPTURE_STATES - {"END"}:
        raise CTraderProtocolError("CAPTURE_UNSUPPORTED", "connection state is not allowlisted")
    raw_reason = read_field(
        value,
        "reason_code",
        "reasonCode",
        "reason",
        "error_code",
        "errorCode",
        default="UNKNOWN",
    )
    reason = str(raw_reason).strip().upper()
    if reason not in _CAPTURE_REASON_CODES:
        reason = "UNKNOWN"
    return {"state": state, "reason_code": reason}


def _capture_end_payload(value: Any) -> dict[str, Any]:
    if value not in (None, {}, {"state": "END"}):
        raise CTraderProtocolError("CAPTURE_UNSUPPORTED", "end capture payload is not empty")
    return {"state": "END"}


def _capture_scalar(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        if value != value or value in {float("inf"), float("-inf")}:
            raise CTraderProtocolError("CAPTURE_UNSUPPORTED", "non-finite market scalar")
        return value
    raise CTraderProtocolError("CAPTURE_UNSUPPORTED", "unstructured market payload field")


@runtime_checkable
class CTraderCodec(Protocol):
    def encode(self, message: WireMessage) -> bytes: ...

    def decode(self, payload: bytes) -> WireMessage: ...


def _field_descriptor(value: Any, name: str) -> Any | None:
    descriptor = getattr(value, "DESCRIPTOR", None)
    fields_by_name = getattr(descriptor, "fields_by_name", None)
    return fields_by_name.get(name) if fields_by_name is not None else None


def message_payload_type(value: Any) -> int | None:
    """Return the concrete payload type from explicit field/default metadata."""
    if isinstance(value, WireMessage):
        return value.payload_type_id
    raw = read_field(value, "payloadType", "payload_type", default=None)
    if raw is not None:
        try:
            return int(raw)
        except (TypeError, ValueError):
            return None
    descriptor_field = _field_descriptor(value, "payloadType")
    default = getattr(descriptor_field, "default_value", None)
    if default is not None:
        try:
            return int(default)
        except (TypeError, ValueError):
            pass
    return MESSAGE_PAYLOAD_TYPES.get(type(value).__name__)


def field_present(value: Any, name: str) -> bool:
    """Return presence for mappings and generated Protobuf messages."""
    if value is None:
        return False
    if isinstance(value, Mapping):
        return _mapping_field_present(value, name)
    return _protobuf_field_present(value, name)


def _mapping_field_present(value: Mapping[str, Any], name: str) -> bool:
    return name in value and value[name] is not None


def _protobuf_field_present(value: Any, name: str) -> bool:
    field = _field_descriptor(value, name)
    has_field = getattr(value, "HasField", None)
    if callable(has_field):
        try:
            return bool(has_field(name))
        except (ValueError, TypeError):
            if field is not None and _is_repeated_field(field):
                return True
    if field is not None:
        if _is_repeated_field(field):
            return True
        if getattr(field, "has_presence", None):
            return False
    return hasattr(value, name) and getattr(value, name) is not None


def read_field(value: Any, *names: str, default: Any = None) -> Any:
    """Read the first present field without materializing Protobuf defaults."""

    if value is None:
        return default
    for name in names:
        if isinstance(value, Mapping):
            if name in value and value[name] is not None:
                return value[name]
            continue
        if field_present(value, name):
            return getattr(value, name)
    return default


def read_repeated(value: Any, *names: str) -> tuple[Any, ...]:
    """Read a repeated field with an empty tuple for absent or empty values."""

    raw = read_field(value, *names, default=None)
    if raw is None:
        return ()
    if isinstance(raw, (str, bytes, Mapping)):
        return (raw,)
    try:
        return tuple(raw)
    except TypeError:
        return (raw,)


def _is_repeated_field(field: Any) -> bool:
    return bool(
        getattr(field, "is_repeated", False) or getattr(field, "label", None) == getattr(field, "LABEL_REPEATED", 3)
    )


def message_to_mapping(value: Any) -> dict[str, Any]:
    """Convert a mapping/generated message to detached JSON-ready fields."""
    if isinstance(value, Mapping):
        return {str(key): jsonable(item) for key, item in value.items()}
    if value is None:
        return {}
    result = _descriptor_mapping(value)
    return result or _fixture_mapping(value)


def _descriptor_mapping(value: Any) -> dict[str, Any]:
    descriptor = getattr(value, "DESCRIPTOR", None)
    fields = getattr(descriptor, "fields", ()) if descriptor is not None else ()
    result: dict[str, Any] = {}
    for field in fields:
        name = str(field.name)
        raw = read_field(value, name, default=None)
        if raw is None:
            continue
        result[name] = _mapped_field(field, raw)
    return result


def _mapped_field(field: Any, raw: Any) -> Any:
    if _is_repeated_field(field):
        return [jsonable(item) for item in raw]
    if getattr(field, "message_type", None) is not None:
        return message_to_mapping(raw)
    return jsonable(raw)


def _fixture_mapping(value: Any) -> dict[str, Any]:
    known = (
        "symbolId",
        "symbolName",
        "digits",
        "pipPosition",
        "enabled",
        "bid",
        "ask",
        "timestamp",
    )
    return {name: jsonable(getattr(value, name)) for name in known if hasattr(value, name)}


def enum_name(owner: Any, field_name: str, value: Any = None) -> str | None:
    """Resolve an enum number through the field descriptor, never substring matching."""

    if value is None:
        value = read_field(owner, field_name, default=None)
    if value is None:
        return None
    if isinstance(value, str):
        return value.upper()
    field = _field_descriptor(owner, field_name)
    enum_descriptor = getattr(field, "enum_type", None)
    try:
        number = int(value)
    except (TypeError, ValueError):
        return str(value).upper()
    for item in getattr(enum_descriptor, "values", ()):
        if int(item.number) == number:
            return str(item.name).upper()
    return None


# Public aliases with names commonly used by adapter code.  Keeping these in
# this module gives the DEMO and market adapters the same implementation.
has_field = field_present
read_optional = read_field
read_enum = enum_name


class SdkProtobufCodec:
    """Codec backed by pinned Spotware-generated messages.

    The historical public name is retained for callers, but the legacy
    ``ctrader-open-api`` package is deliberately not a runtime fallback.  The
    generated package is loaded lazily so normal offline MTF imports remain
    independent of protobuf and no Twisted reactor is imported.
    """

    def __init__(self) -> None:
        report = dependency_report()
        if not report.available:
            raise CTraderDependencyError(report.message or "codec Protobuf cTrader no disponible")
        try:
            common = importlib.import_module(f"{GENERATED_PROTOBUF_PACKAGE}.OpenApiCommonMessages_pb2")
            messages = importlib.import_module(f"{GENERATED_PROTOBUF_PACKAGE}.OpenApiMessages_pb2")
            self._proto_message = common.ProtoMessage
            self._heartbeat = common.ProtoHeartbeatEvent
            self._payload_by_id, self._payload_by_name = _generated_payload_registry(common, messages)
        except Exception as exc:  # pragma: no cover - optional runtime
            raise CTraderDependencyError(f"mensajes Protobuf generados no cargables: {exc}") from exc

    def _payload_message(self, message: WireMessage) -> Any:
        payload = message.payload
        if payload is None and message.payload_type_id == PAYLOAD["PROTO_HEARTBEAT_EVENT"]:
            return self._heartbeat()
        if hasattr(payload, "SerializeToString"):
            concrete_type = message_payload_type(payload)
            expected_type = message.payload_type_id
            if expected_type is not None and concrete_type is not None and expected_type != concrete_type:
                raise CTraderProtocolError("LOCAL_CODEC", "payloadType no coincide con el mensaje generado")
            return payload
        name = WIRE_CLASS_NAMES.get(message.payload_type_name, message.payload_type_name)
        message_class = self._payload_by_name.get(name)
        if message_class is None and message.payload_type_id is not None:
            message_class = self._payload_by_id.get(message.payload_type_id)
        if message_class is None:
            raise CTraderProtocolError("LOCAL_CODEC", f"payload Protobuf no allowlisted: {name}")
        if payload is None:
            params: dict[str, Any] = {}
        elif isinstance(payload, Mapping):
            params = dict(payload)
        else:
            raise CTraderProtocolError("LOCAL_CODEC", f"payload {name} debe ser mensaje o mapping")
        try:
            return message_class(**params)
        except Exception as exc:  # pragma: no cover - optional runtime
            raise CTraderProtocolError("LOCAL_CODEC", f"no se pudo construir {name}") from exc

    def encode(self, message: WireMessage) -> bytes:
        payload_message = self._payload_message(message)
        payload_type = message_payload_type(payload_message)
        if payload_type is None:
            raise CTraderProtocolError("LOCAL_CODEC", "mensaje generado sin payloadType")
        expected_type = message.payload_type_id
        if expected_type is not None and expected_type != payload_type:
            raise CTraderProtocolError("LOCAL_CODEC", "payloadType no coincide con el mensaje generado")
        envelope = self._proto_message(
            payload=payload_message.SerializeToString(),
            payloadType=payload_type,
        )
        if message.client_msg_id is not None:
            envelope.clientMsgId = str(message.client_msg_id)
        encoded = envelope.SerializeToString()
        if not isinstance(encoded, bytes):
            raise CTraderProtocolError("LOCAL_CODEC", "SerializeToString no devolvió bytes")
        if len(encoded) > MAX_FRAME_LENGTH:
            raise CTraderProtocolError("FRAME_TOO_LONG", "frame Protobuf excede MAX_FRAME_LENGTH")
        return encoded

    def decode(self, payload: bytes) -> WireMessage:
        if not isinstance(payload, bytes):
            raise CTraderProtocolError("LOCAL_CODEC", "frame Protobuf debe ser bytes")
        if len(payload) > MAX_FRAME_LENGTH:
            raise CTraderProtocolError("FRAME_TOO_LONG", "frame Protobuf excede MAX_FRAME_LENGTH")
        envelope = self._proto_message()
        try:
            envelope.ParseFromString(payload)
        except Exception as exc:  # pragma: no cover - optional runtime
            raise CTraderProtocolError("LOCAL_CODEC", "frame Protobuf inválido") from exc
        client_id = read_field(envelope, "clientMsgId", "client_msg_id", default=None) or None
        payload_type = read_field(envelope, "payloadType", "payload_type", default=None)
        if payload_type is None:
            raise CTraderProtocolError("LOCAL_CODEC", "frame Protobuf sin payloadType")
        if payload_type == PAYLOAD["PROTO_HEARTBEAT_EVENT"]:
            return WireMessage(PAYLOAD["PROTO_HEARTBEAT_EVENT"], None, client_id, True)
        message_class = self._payload_by_id.get(int(payload_type))
        if message_class is None:
            raise CTraderProtocolError("LOCAL_CODEC", f"payload Protobuf desconocido: {payload_type}")
        raw_payload = read_field(envelope, "payload", default=None)
        if raw_payload is None:
            raw_payload = b""
        if not isinstance(raw_payload, bytes):
            raise CTraderProtocolError("LOCAL_CODEC", "payload Protobuf no es bytes")
        extracted = message_class()
        try:
            extracted.ParseFromString(raw_payload)
        except Exception as exc:  # pragma: no cover - optional runtime
            raise CTraderProtocolError("LOCAL_CODEC", "payload Protobuf inválido") from exc
        return WireMessage(int(payload_type), extracted, client_id, False)


def _generated_payload_registry(common: Any, messages: Any) -> tuple[dict[int, Any], dict[str, Any]]:
    by_id: dict[int, Any] = {}
    by_name: dict[str, Any] = {}
    for module in (common, messages):
        _register_generated_module(module, by_id, by_name)
    for name, payload_type in MESSAGE_PAYLOAD_TYPES.items():
        message_class = by_id.get(payload_type)
        if message_class is not None:
            by_name.setdefault(name, message_class)
    for name, payload_type in PAYLOAD.items():
        message_class = by_id.get(payload_type)
        if message_class is not None:
            by_name.setdefault(name, message_class)
    return by_id, by_name


def _register_generated_module(module: Any, by_id: dict[int, Any], by_name: dict[str, Any]) -> None:
    descriptor = getattr(module, "DESCRIPTOR", None)
    message_descriptors = getattr(descriptor, "message_types_by_name", {})
    for name, message_descriptor in message_descriptors.items():
        payload_type = _descriptor_payload_type(message_descriptor)
        if payload_type is None:
            continue
        message_class = getattr(module, name, None)
        if message_class is None:
            raise CTraderDependencyError(f"descriptor Protobuf sin clase {name}")
        previous = by_id.get(payload_type)
        if previous is not None and previous is not message_class:
            raise CTraderDependencyError(f"payloadType duplicado en codec: {payload_type}")
        by_id[payload_type] = message_class
        by_name[name] = message_class
        alias = name.removeprefix("ProtoOA").removeprefix("Proto")
        if alias:
            by_name.setdefault(alias, message_class)


def _descriptor_payload_type(message_descriptor: Any) -> int | None:
    field = getattr(message_descriptor, "fields_by_name", {}).get("payloadType")
    if field is None:
        return None
    try:
        return int(field.default_value)
    except (TypeError, ValueError):
        return None


def jsonable(value: Any) -> Any:
    """Serialize only known structured values for stable diagnostics/hashes."""
    if isinstance(value, Mapping):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(item) for item in value]
    if isinstance(value, Enum):
        return value.value
    return _jsonable_leaf(value)


def _jsonable_leaf(value: Any) -> Any:
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return jsonable(value.to_dict())
    if hasattr(value, "DESCRIPTOR") and hasattr(value, "SerializeToString"):
        return message_to_mapping(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return {"type": f"{type(value).__module__}.{type(value).__qualname__}"}


def canonical_json(value: Any) -> str:
    return json.dumps(jsonable(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def redact_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        secret_names = {
            "clientsecret",
            "client_secret",
            "accesstoken",
            "access_token",
            "refreshtoken",
            "refresh_token",
        }
        return {
            str(key): "<redacted>" if str(key).lower() in secret_names else redact_value(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [redact_value(item) for item in value]
    return jsonable(value)


__all__ = [
    "CTraderCodec",
    "DependencyReport",
    "MAX_FRAME_LENGTH",
    "MESSAGE_PAYLOAD_TYPES",
    "PAYLOAD",
    "PAYLOAD_NAMES",
    "PROTOBUF_MODULE",
    "SDK_MODULE",
    "SESSION_CONTROL_PAYLOAD_TYPES",
    "SESSION_CONTROL_TYPE_NAMES",
    "SdkProtobufCodec",
    "TREND_PERIODS",
    "TREND_PERIOD_NAMES",
    "WIRE_CLASS_NAMES",
    "WireMessage",
    "canonical_json",
    "capture_envelope",
    "dependency_report",
    "enum_name",
    "field_present",
    "has_field",
    "jsonable",
    "message_payload_type",
    "message_to_mapping",
    "read_enum",
    "read_field",
    "read_optional",
    "read_repeated",
    "redact_value",
    "stable_hash",
]
