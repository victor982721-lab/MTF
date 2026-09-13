"""Typed, DEMO-only cTrader execution adapter.

The execution seam deliberately sits on top of :class:`mtf_lab.data.ctrader.CTraderClient`.
That client owns the TCP/TLS session, framing, one receive pump and request
correlation.  This module only builds official protobuf messages and translates
the correlated response body into the executor's small domain records.

No socket, OAuth flow, token store or SDK ``Client`` is created here.  A
``CTraderClientGateway`` is the one explicit composition adapter for the
project's existing client; tests may inject an in-process ``OfficialGateway``
with the same synchronous contract.
"""

from __future__ import annotations

import contextlib
import dataclasses
import importlib
import math
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, Protocol, runtime_checkable

from ..data.ctrader import (
    CTraderClient,
    CTraderProtocolError,
    CTraderRequestCancelled,
    CTraderRequestTimeout,
    CTraderTransportError,
    RequestPhase,
    WireMessage,
)
from ..data.ctrader import (
    enum_name as _central_enum_name,
)
from ..data.ctrader import (
    field_present as _central_field_present,
)
from ..data.ctrader import (
    read_repeated as _central_read_repeated,
)
from ..data.ctrader_protocol import MESSAGE_PAYLOAD_TYPES as _MESSAGE_PAYLOAD_TYPES
from ..data.ctrader_session import AuthenticatedSessionEvidence
from .ctrader_executor import (
    DecimalValue,
    DemoAccount,
    DemoAccountRequired,
    EndpointRejected,
    ExecutionIntent,
    ExecutionLocalFailure,
    ExecutionTransportFailure,
    Fill,
    OrderSnapshot,
    OrderState,
    Position,
    RealAccountForbidden,
    ScopeRejected,
    SendPhase,
    ServerObservationProof,
    Side,
    _decimal_text,
    _decimal_value,
    verify_demo_account,
)
from .volume_rules import VolumeGrid, VolumeRuleError

DEMO_PROTOBUF_ENDPOINT = "demo.ctraderapi.com:5035"
LIVE_PROTOBUF_ENDPOINT = "live.ctraderapi.com:5035"
VOLUME_SCALE = 100  # official protocol: 100 means 1.00 base units

# Payload values are part of the official protocol.  They are kept here rather
# than inferred from class names at runtime so a typo cannot silently send a
# different request type.
PAYLOAD_TYPES: dict[str, int] = dict(_MESSAGE_PAYLOAD_TYPES)
PAYLOAD_TYPES.update(
    {
        "ProtoOAExecutionEvent": 2126,
        "ProtoOAOrderErrorEvent": 2132,
        "ProtoOACancelOrderReq": 2108,
        "ProtoOAOrderListRes": 2176,
        "ProtoOADealListRes": 2134,
        "ProtoOADealListByPositionIdRes": 2180,
        "ProtoOAOrderDetailsRes": 2182,
        "ProtoOAReconcileRes": 2125,
        "ProtoOAErrorRes": 2142,
    }
)

_RESPONSE_TYPES: dict[int, frozenset[int]] = {
    PAYLOAD_TYPES["ProtoOANewOrderReq"]: frozenset(
        {
            PAYLOAD_TYPES["ProtoOAExecutionEvent"],
            PAYLOAD_TYPES["ProtoOAOrderErrorEvent"],
            PAYLOAD_TYPES["ProtoOAErrorRes"],
        }
    ),
    PAYLOAD_TYPES["ProtoOAClosePositionReq"]: frozenset(
        {
            PAYLOAD_TYPES["ProtoOAExecutionEvent"],
            PAYLOAD_TYPES["ProtoOAOrderErrorEvent"],
            PAYLOAD_TYPES["ProtoOAErrorRes"],
        }
    ),
    PAYLOAD_TYPES["ProtoOACancelOrderReq"]: frozenset(
        {
            PAYLOAD_TYPES["ProtoOAExecutionEvent"],
            PAYLOAD_TYPES["ProtoOAOrderErrorEvent"],
            PAYLOAD_TYPES["ProtoOAErrorRes"],
        }
    ),
    PAYLOAD_TYPES["ProtoOAReconcileReq"]: frozenset(
        {PAYLOAD_TYPES["ProtoOAReconcileRes"], PAYLOAD_TYPES["ProtoOAErrorRes"]}
    ),
    PAYLOAD_TYPES["ProtoOAOrderListReq"]: frozenset(
        {PAYLOAD_TYPES["ProtoOAOrderListRes"], PAYLOAD_TYPES["ProtoOAErrorRes"]}
    ),
    PAYLOAD_TYPES["ProtoOAOrderDetailsReq"]: frozenset(
        {PAYLOAD_TYPES["ProtoOAOrderDetailsRes"], PAYLOAD_TYPES["ProtoOAErrorRes"]}
    ),
    PAYLOAD_TYPES["ProtoOADealListReq"]: frozenset(
        {PAYLOAD_TYPES["ProtoOADealListRes"], PAYLOAD_TYPES["ProtoOAErrorRes"]}
    ),
    PAYLOAD_TYPES["ProtoOADealListByPositionIdReq"]: frozenset(
        {PAYLOAD_TYPES["ProtoOADealListByPositionIdRes"], PAYLOAD_TYPES["ProtoOAErrorRes"]}
    ),
}


class OfficialAdapterError(RuntimeError):
    """Base class for a local adapter, protocol or transport failure."""


class OfficialValidationError(ExecutionLocalFailure, OfficialAdapterError, ValueError):
    """Message/configuration validation failed before a gateway call."""


class OfficialProtocolError(OfficialAdapterError):
    """A response was received but violates the local protocol contract."""


class OfficialTransportError(ExecutionTransportFailure, OfficialAdapterError):
    """The gateway failed independently of order outcome."""


class OfficialTimeoutError(OfficialTransportError, TimeoutError):
    """A sent request did not produce a response before its deadline."""

    def __init__(self, message: str, *, phase: SendPhase = SendPhase.SENT_NO_RESPONSE) -> None:
        super().__init__(message, phase=phase)
        self.phase = phase

    @property
    def uncertain(self) -> bool:
        return self.phase in {SendPhase.SENT, SendPhase.SENT_NO_RESPONSE}


class OfficialUncertainSendError(OfficialTimeoutError):
    """The call reached the send phase but its result is not known."""

    @property
    def uncertain(self) -> bool:
        return super().uncertain


# Backward-compatible spelling used by an early draft of the adapter.
OfficialSendUncertainError = OfficialUncertainSendError


class OfficialCancelledError(OfficialTransportError):
    """The caller cancelled before or during a gateway request."""

    def __init__(self, message: str, *, phase: SendPhase) -> None:
        super().__init__(message, phase=phase)
        self.phase = phase


class OfficialSDKUnavailable(OfficialValidationError):
    """The requested generated protobuf class/module is unavailable."""


class OfficialMessageError(OfficialValidationError):
    """An official message cannot be built or has insufficient evidence."""


class OfficialResponseError(ExecutionTransportFailure, OfficialProtocolError):
    """A response was received after send but violates the local contract."""


class OfficialCorrelationError(OfficialResponseError):
    """Account, client correlation or session generation does not match."""


class OfficialGatewayConfigurationError(OfficialValidationError):
    """A gateway is not the project's explicit synchronous gateway contract."""


@runtime_checkable
class OfficialGateway(Protocol):
    """Synchronous gateway used by the adapter.

    ``send`` must return the *body* of the correlated ``WireMessage``.  It must
    not return a Deferred, coroutine or future: resolving asynchronous SDK
    objects belongs in the composition layer, before this protocol is used.
    """

    def send(self, message: Any, *, client_msg_id: str, timeout_seconds: float) -> Any: ...


@runtime_checkable
class CTraderRequestMessageClient(Protocol):
    """Typed request and authenticated-proof methods of the existing client."""

    def request_message(self, message: Any, *, client_msg_id: str, timeout_seconds: float) -> WireMessage: ...

    def authenticated_session_evidence(self) -> Any: ...

    def validate_session_evidence(self, proof: Any) -> Any: ...


def _client_request_phase(error: BaseException | None = None) -> RequestPhase | None:
    """Read only this request's phase, never session-wide mutable status."""

    phase = getattr(error, "phase", None) if error is not None else None
    if isinstance(phase, RequestPhase):
        return phase
    if phase is not None:
        try:
            return RequestPhase(str(getattr(phase, "value", phase)))
        except ValueError:
            return None
    return None


class CTraderClientGateway:
    """Adapt the repository's single :class:`CTraderClient` to ``OfficialGateway``.

    The adapter never calls ``request``/``send`` by discovery and never creates
    a second network client.  An SDK ``ctrader_open_api.Client`` is rejected at
    construction because its ``send`` returns Twisted ``Deferred`` objects and
    does not satisfy the synchronous contract.
    """

    def __init__(
        self,
        client: CTraderClient,
        *,
        clock: Callable[[], datetime] | None = None,
        session: AuthenticatedSession | None = None,
    ) -> None:
        if _is_sdk_client(client) or not isinstance(client, CTraderRequestMessageClient):
            raise OfficialGatewayConfigurationError(
                "CTraderClientGateway requiere el CTraderClient local con request_message() y "
                "authenticated_session_evidence(); no acepta ctrader_open_api.Client ni otro lector"
            )
        if session is not None:
            raise OfficialGatewayConfigurationError(
                "la evidencia de sesión debe provenir de authenticated_session_evidence(), no del caller"
            )
        self.client = client
        self.clock = clock or (lambda: datetime.now(UTC))
        try:
            proof = client.authenticated_session_evidence()
            validated = client.validate_session_evidence(proof)
            if validated is not True:
                raise OfficialGatewayConfigurationError("validate_session_evidence no confirmó el proof")
            self.session = _normalize_session_evidence(proof)
        except OfficialAdapterError:
            raise
        except Exception as exc:
            raise OfficialGatewayConfigurationError(
                "CTraderClient no expone evidencia de sesión autenticada válida"
            ) from exc

    def _validated_session(self) -> AuthenticatedSession:
        try:
            proof = self.client.authenticated_session_evidence()
            validated = self.client.validate_session_evidence(proof)
            if validated is not True:
                raise OfficialGatewayConfigurationError("validate_session_evidence no confirmó el proof")
            return _normalize_session_evidence(proof)
        except OfficialAdapterError:
            raise
        except Exception as exc:
            raise OfficialGatewayConfigurationError("no se pudo leer evidencia de sesión autenticada") from exc

    def _refresh_session(self) -> AuthenticatedSession:
        current = self._validated_session()
        if (
            current.session_id != self.session.session_id
            or current.connection_generation != self.session.connection_generation
        ):
            raise OfficialCorrelationError("la sesión o generación cambió; se requiere nueva evidencia")
        if current.account_id != self.session.account_id or current.endpoint.lower() != self.session.endpoint.lower():
            raise OfficialCorrelationError("la cuenta o endpoint de sesión cambió")
        if not current.valid_at(self.clock()):
            raise OfficialGatewayConfigurationError("la evidencia de sesión autenticada está vencida")
        self.session = current
        return current

    def server_observation(self) -> ServerAccountObservation:
        return ServerAccountObservation.from_session(self._refresh_session())

    def send(self, message: Any, *, client_msg_id: str, timeout_seconds: float) -> Any:
        self._refresh_session()
        payload_type = _payload_type_for_message(message)
        # Make the protocol type explicit before crossing the session boundary.
        _set(message, "payloadType", payload_type)
        wire = self._request(message, client_msg_id=client_msg_id, timeout_seconds=timeout_seconds)
        return self._validate_wire(wire, client_msg_id=client_msg_id, payload_type=payload_type)

    def _request(self, message: Any, *, client_msg_id: str, timeout_seconds: float) -> WireMessage:
        try:
            return self.client.request_message(
                message,
                client_msg_id=str(client_msg_id),
                timeout_seconds=float(timeout_seconds),
            )
        except OfficialAdapterError:
            raise
        except (CTraderRequestTimeout, CTraderRequestCancelled, CTraderTransportError, CTraderProtocolError) as exc:
            raise self._map_phase_error(exc) from exc
        except Exception as exc:
            raise OfficialTransportError("falló CTraderClient.request_message") from exc

    def _map_phase_error(self, exc: BaseException) -> OfficialAdapterError:
        phase = _client_request_phase(exc)
        if isinstance(exc, CTraderRequestTimeout):
            if phase in {
                RequestPhase.FAILED_BEFORE_SEND,
                RequestPhase.CANCELLED_BEFORE_SEND,
                RequestPhase.WAITING_TO_SEND,
            }:
                return OfficialTransportError(str(exc), phase=SendPhase.LOCAL_ERROR)
            return OfficialUncertainSendError(str(exc), phase=SendPhase.SENT_NO_RESPONSE)
        if isinstance(exc, CTraderRequestCancelled):
            target = (
                SendPhase.CANCELLED_BEFORE_SEND
                if phase in {RequestPhase.CANCELLED_BEFORE_SEND, RequestPhase.WAITING_TO_SEND, RequestPhase.CREATED}
                else SendPhase.SENT
            )
            return OfficialCancelledError(str(exc), phase=target)
        if isinstance(exc, CTraderTransportError):
            if phase in {
                RequestPhase.FAILED_BEFORE_SEND,
                RequestPhase.CANCELLED_BEFORE_SEND,
                RequestPhase.WAITING_TO_SEND,
                RequestPhase.CREATED,
            }:
                return OfficialTransportError(str(exc), phase=SendPhase.LOCAL_ERROR)
            return OfficialUncertainSendError(str(exc), phase=SendPhase.SENT)
        if isinstance(exc, CTraderProtocolError):
            return OfficialResponseError(str(exc), phase=SendPhase.SENT)
        return OfficialTransportError(str(exc))

    def _validate_wire(self, wire: Any, *, client_msg_id: str, payload_type: int) -> Any:
        if not isinstance(wire, WireMessage):
            raise OfficialResponseError("request_message debe devolver WireMessage", phase=SendPhase.RESPONSE_RECEIVED)
        if wire.client_msg_id != str(client_msg_id):
            raise OfficialCorrelationError(
                f"respuesta correlacionada con id inesperado: {wire.client_msg_id!r}",
                phase=SendPhase.RESPONSE_RECEIVED,
            )
        expected = _RESPONSE_TYPES.get(payload_type)
        if expected is not None and wire.payload_type_id not in expected:
            raise OfficialResponseError(
                f"payload de respuesta inesperado: {wire.payload_type_id!r} para {payload_type}",
                phase=SendPhase.RESPONSE_RECEIVED,
            )
        return wire.payload


@dataclasses.dataclass(frozen=True, slots=True)
class AuthenticatedSession:
    """Server-authenticated session evidence, including its connection generation."""

    session_id: str
    account_id: str
    environment: str
    endpoint: str
    scopes: frozenset[str]
    authenticated_at: datetime
    connection_generation: str = ""
    expires_at: datetime | None = None
    evidence_source: str = "client"

    def __post_init__(self) -> None:
        values = _authenticated_session_values(self)
        for name, value in zip(
            (
                "session_id",
                "account_id",
                "environment",
                "endpoint",
                "scopes",
                "authenticated_at",
                "expires_at",
                "connection_generation",
                "evidence_source",
            ),
            values,
            strict=True,
        ):
            object.__setattr__(self, name, value)

    def valid_at(self, when: datetime) -> bool:
        at = _parse_time(when)
        return at >= self.authenticated_at and (self.expires_at is None or at < self.expires_at)


def _proof_value(proof: Any, *names: str, default: Any = None) -> Any:
    for name in names:
        if isinstance(proof, Mapping):
            if name in proof:
                return proof[name]
        else:
            marker = object()
            value = getattr(proof, name, marker)
            if value is not marker:
                return value
    return default


def _normalize_session_evidence(proof: Any) -> AuthenticatedSession:
    """Normalize only structured proof returned by the authenticated client."""

    if isinstance(proof, AuthenticatedSession):
        return proof
    if isinstance(proof, AuthenticatedSessionEvidence):
        # This concrete type is minted and retained by CTraderClient; it is
        # accepted only after validate_session_evidence() has checked identity
        # and current connection generation.
        return AuthenticatedSession(
            session_id=str(proof.session_id),
            account_id=str(proof.account_id),
            environment=str(proof.environment),
            endpoint=str(proof.endpoint),
            scopes=proof.scopes,
            authenticated_at=proof.authenticated_at,
            connection_generation=str(proof.connection_generation),
            expires_at=proof.expires_at,
            evidence_source="ctrader-client",
        )
    if proof is None:
        raise OfficialGatewayConfigurationError("authenticated_session_evidence() devolvió None")
    # The client may use its own immutable evidence class. A mapping from a
    # caller is intentionally not an authorization primitive; require the
    # method boundary and a non-caller source marker.
    source = _proof_value(proof, "evidence_source", "source", default=None)
    if source is None:
        raise OfficialGatewayConfigurationError("la evidencia de sesión carece de source")
    try:
        return AuthenticatedSession(
            session_id=str(_proof_value(proof, "session_id", "sessionId")),
            account_id=str(_proof_value(proof, "account_id", "accountId")),
            environment=str(_proof_value(proof, "environment", "mode")),
            endpoint=str(_proof_value(proof, "endpoint", "host")),
            scopes=_proof_value(proof, "scopes", "permissions", default=()),
            authenticated_at=_proof_value(proof, "authenticated_at", "authenticatedAt"),
            connection_generation=str(_proof_value(proof, "connection_generation", "generation")),
            expires_at=_proof_value(proof, "expires_at", "expiresAt"),
            evidence_source=str(source),
        )
    except OfficialAdapterError:
        raise
    except Exception as exc:
        raise OfficialGatewayConfigurationError("evidencia de sesión con forma inválida") from exc


@dataclasses.dataclass(frozen=True, slots=True)
class ServerAccountObservation(ServerObservationProof):
    """Server-produced DEMO evidence tied to an authenticated session when available.

    ``source='fixture-server'`` is reserved for offline tests.  Production
    evidence should be created with :meth:`from_session`, which preserves the
    session id and connection generation instead of accepting a caller boolean.
    """

    account_id: str
    environment: str
    endpoint: str
    scopes: frozenset[str]
    observed_at: datetime
    source: str = "ctrader-open-api"
    session_id: str | None = None
    connection_generation: str | None = None
    expires_at: datetime | None = None

    def __post_init__(self) -> None:
        account_id = str(self.account_id).strip()
        environment = str(self.environment).strip().upper()
        endpoint = str(self.endpoint).strip()
        source = str(self.source).strip()
        if not account_id or not account_id.isdigit() or int(account_id) <= 0 or environment != "DEMO":
            raise DemoAccountRequired("la evidencia del servidor debe identificar una cuenta DEMO numérica")
        if not _is_official_demo_endpoint(endpoint):
            raise EndpointRejected("la evidencia del servidor debe usar el endpoint DEMO oficial")
        scopes = _normalize_scopes(self.scopes)
        if not _scope_has_trading(scopes):
            raise ScopeRejected("la evidencia del servidor no observa scope trading")
        observed_at = _parse_time(self.observed_at)
        expires_at = _parse_time(self.expires_at) if self.expires_at is not None else None
        if expires_at is not None and expires_at <= observed_at:
            raise ValueError("expires_at debe ser posterior a observed_at")
        session_id = str(self.session_id).strip() if self.session_id is not None else None
        generation = str(self.connection_generation).strip() if self.connection_generation is not None else None
        if source not in {"fixture-server", "test"} and not session_id:
            raise DemoAccountRequired("la evidencia externa debe estar ligada a una sesión autenticada")
        object.__setattr__(self, "account_id", account_id)
        object.__setattr__(self, "environment", environment)
        object.__setattr__(self, "endpoint", endpoint)
        object.__setattr__(self, "scopes", scopes)
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "session_id", session_id)
        object.__setattr__(self, "connection_generation", generation)
        object.__setattr__(self, "expires_at", expires_at)

    @classmethod
    def from_session(
        cls,
        session: AuthenticatedSession,
        *,
        observed_at: datetime | None = None,
        source: str = "ctrader-open-api",
    ) -> ServerAccountObservation:
        return cls(
            account_id=session.account_id,
            environment=session.environment,
            endpoint=session.endpoint,
            scopes=session.scopes,
            observed_at=observed_at or session.authenticated_at,
            source=source,
            session_id=session.session_id,
            connection_generation=session.connection_generation,
            expires_at=session.expires_at,
        )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> ServerAccountObservation:
        if not isinstance(value, Mapping) or value.get("server_observed") is not True:
            raise DemoAccountRequired("verified=true local no sustituye evidencia server_observed=True")
        session = value.get("session")
        if isinstance(session, Mapping):
            return cls.from_session(
                AuthenticatedSession(
                    session_id=str(session.get("session_id", "")),
                    account_id=str(session.get("account_id", value.get("account_id", value.get("id", "")))),
                    environment=str(session.get("environment", value.get("environment", ""))),
                    endpoint=str(session.get("endpoint", value.get("endpoint", DEMO_PROTOBUF_ENDPOINT))),
                    scopes=session.get("scopes", value.get("scopes", value.get("permissions", ()))),
                    authenticated_at=session.get("authenticated_at", value.get("observed_at", datetime.now(UTC))),
                    connection_generation=str(session.get("connection_generation", "")),
                    expires_at=session.get("expires_at"),
                ),
                observed_at=value.get("observed_at"),
                source=str(value.get("source", "ctrader-open-api")),
            )
        return cls(
            account_id=str(value.get("account_id", value.get("id", ""))),
            environment=str(value.get("environment", "")),
            endpoint=str(value.get("endpoint", DEMO_PROTOBUF_ENDPOINT)),
            scopes=value.get("scopes", value.get("permissions", ())),
            observed_at=value.get("observed_at", datetime.now(UTC)),
            source=str(value.get("source", "ctrader-open-api")),
            session_id=value.get("session_id"),
            connection_generation=value.get("connection_generation"),
            expires_at=value.get("expires_at"),
        )

    def matches_account(self, account: DemoAccount) -> bool:
        return (
            self.account_id == str(account.account_id)
            and self.environment == str(account.environment).upper()
            and self.endpoint.lower() == str(account.endpoint).strip().lower()
        )

    def valid_at(self, when: datetime) -> bool:
        at = _parse_time(when)
        return at >= self.observed_at and (self.expires_at is None or at < self.expires_at)

    def matches_session(self, session: AuthenticatedSession, *, at: datetime | None = None) -> bool:
        check_at = _parse_time(at or self.observed_at)
        return (
            self.session_id == session.session_id
            and self.connection_generation == session.connection_generation
            and self.account_id == session.account_id
            and self.endpoint == session.endpoint
            and self.scopes.issuperset(session.scopes)
            and self.valid_at(check_at)
            and session.valid_at(check_at)
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "account_id": self.account_id,
            "environment": self.environment,
            "endpoint": self.endpoint,
            "scopes": sorted(self.scopes),
            "observed_at": self.observed_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "source": self.source,
            "session_id": self.session_id,
            "connection_generation": self.connection_generation,
            "expires_at": self.expires_at.astimezone(UTC).isoformat().replace("+00:00", "Z")
            if self.expires_at
            else None,
            "server_observed": True,
        }


@dataclasses.dataclass(frozen=True, slots=True)
class CTraderDemoTransportConfig:
    endpoint: str = DEMO_PROTOBUF_ENDPOINT
    required_scopes: frozenset[str] = frozenset({"trading"})
    timeout_seconds: float = 5.0
    volume_scale: int = VOLUME_SCALE
    history_page_limit: int = 4
    history_window_seconds: int = 7 * 24 * 60 * 60

    def __post_init__(self) -> None:
        if not _is_official_demo_endpoint(self.endpoint):
            raise EndpointRejected("official cTrader transport only accepts demo endpoint")
        timeout = float(self.timeout_seconds)
        if timeout <= 0 or not math.isfinite(timeout):
            raise ValueError("timeout_seconds must be positive and finite")
        scale = int(self.volume_scale)
        if scale <= 0:
            raise ValueError("volume_scale must be positive")
        required = _normalize_scopes(self.required_scopes)
        if not _scope_has_trading(required):
            raise ScopeRejected("official DEMO transport requires scope trading")
        if isinstance(self.history_page_limit, bool) or int(self.history_page_limit) <= 0:
            raise ValueError("history_page_limit must be positive")
        if isinstance(self.history_window_seconds, bool) or int(self.history_window_seconds) <= 0:
            raise ValueError("history_window_seconds must be positive")
        object.__setattr__(self, "endpoint", str(self.endpoint).strip())
        object.__setattr__(self, "required_scopes", required)
        object.__setattr__(self, "timeout_seconds", timeout)
        object.__setattr__(self, "volume_scale", scale)
        object.__setattr__(self, "history_page_limit", int(self.history_page_limit))
        object.__setattr__(self, "history_window_seconds", int(self.history_window_seconds))


class CTraderDemoTransport:
    """Official-message adapter for a selected DEMO account.

    ``client`` is an already composed synchronous :class:`OfficialGateway`.
    The adapter itself never connects, starts OAuth, reads credentials or
    retries a possibly sent order.
    """

    def __init__(
        self,
        account: DemoAccount | Mapping[str, Any],
        *,
        client: OfficialGateway,
        proto: Any | None = None,
        symbol_ids: Mapping[str, int] | None = None,
        symbol_names: Mapping[int, str] | None = None,
        config: CTraderDemoTransportConfig | None = None,
        clock: Callable[[], datetime] | None = None,
        server_observation: ServerAccountObservation | Mapping[str, Any] | None = None,
        volume_grid: VolumeGrid | Mapping[str, Any] | None = None,
    ) -> None:
        _validate_transport_gateway(account, client)
        account_obj = account if isinstance(account, DemoAccount) else DemoAccount.from_mapping(account)
        config_obj = config or CTraderDemoTransportConfig(endpoint=DEMO_PROTOBUF_ENDPOINT)
        clock_fn = clock or (lambda: datetime.now(UTC))
        _validate_transport_endpoints(account_obj, config_obj)
        observation = _resolve_observation(client, server_observation)
        _validate_observation(account_obj, client, observation, clock_fn)
        account_obj = _account_with_observation(account_obj, observation)
        self.account = verify_demo_account(
            account_obj,
            required_scopes=config_obj.required_scopes,
            endpoint=config_obj.endpoint,
        )
        self._account_endpoint = self.account.endpoint
        self.config = config_obj
        self.clock = clock_fn
        self.endpoint = config_obj.endpoint
        self.account_id = self.account.account_id
        self.scopes = self.account.scopes
        self.client = client
        self.proto = proto if proto is not None else load_official_proto()
        self.symbol_ids = {str(key).upper(): int(value) for key, value in (symbol_ids or {}).items()}
        self.symbol_names = {int(key): str(value).upper() for key, value in (symbol_names or {}).items()}
        self.server_observation = observation
        try:
            self.volume_grid = (
                volume_grid
                if isinstance(volume_grid, VolumeGrid)
                else VolumeGrid.from_mapping(volume_grid, volume_scale=config_obj.volume_scale)
                if volume_grid is not None
                else None
            )
        except VolumeRuleError as exc:
            raise OfficialMessageError(f"invalid observed symbol volume grid: {exc}") from exc
        if self.volume_grid is not None and self.volume_grid.volume_scale != config_obj.volume_scale:
            raise OfficialMessageError("observed volume grid scale differs from transport volume_scale")
        self._snapshots: dict[str, OrderSnapshot] = {}
        self._requested_quantities: dict[str, DecimalValue] = {}
        self._intent_created_at: dict[str, datetime] = {}
        self._position_clients: dict[str, str] = {}
        self._seen_deals: set[tuple[str, str, str]] = set()
        self._intent_kinds: dict[str, str] = {}
        self._close_positions: dict[str, str] = {}
        self._registered_intents: dict[str, ExecutionIntent] = {}

    def register_intent(self, intent: ExecutionIntent) -> None:
        if intent.account_id != self.account_id:
            raise OfficialCorrelationError("intent account id does not match the DEMO transport")
        if intent.kind not in {"OPEN", "CLOSE"}:
            raise OfficialMessageError("intent kind must be OPEN or CLOSE")
        previous = self._registered_intents.get(intent.intent_id)
        if previous is not None and previous != intent:
            raise OfficialCorrelationError(f"intent id already registered with a different payload: {intent.intent_id}")
        if previous is not None:
            return
        self._registered_intents[intent.intent_id] = intent
        self._intent_kinds[intent.intent_id] = intent.kind
        self._requested_quantities[intent.intent_id] = _decimal_value(intent.quantity, "intent quantity", positive=True)
        self._intent_created_at[intent.intent_id] = _parse_time(intent.created_at)
        if intent.kind == "CLOSE" and intent.position_id is not None:
            self._close_positions[intent.intent_id] = intent.position_id

    def validate_open_quantity(self, quantity: Any) -> int:
        if self.volume_grid is None:
            raise OfficialMessageError("opening requires observed minVolume/maxVolume/stepVolume metadata")
        try:
            return self.volume_grid.validate_open_quantity(quantity)
        except VolumeRuleError as exc:
            raise OfficialMessageError(str(exc)) from exc

    def verify_endpoint(self, endpoint: str) -> bool:
        text = str(endpoint).strip().lower()
        return _is_official_demo_endpoint(text) and text == self.endpoint.lower()

    def available_scopes(self) -> frozenset[str]:
        return self.scopes

    def build_application_auth(self, client_id: str, client_secret: str) -> Any:
        request = self._message("ProtoOAApplicationAuthReq")
        _set(request, "clientId", str(client_id))
        _set(request, "clientSecret", str(client_secret))
        return request

    def build_account_auth(self, access_token: str) -> Any:
        request = self._message("ProtoOAAccountAuthReq")
        _set(request, "ctidTraderAccountId", _int_id(self.account_id, "account_id"))
        _set(request, "accessToken", str(access_token))
        return request

    def build_new_order(self, intent: ExecutionIntent) -> Any:
        if intent.account_id != self.account_id or intent.kind != "OPEN":
            raise OfficialCorrelationError("new order requires an OPEN intent for the selected DEMO account")
        symbol_id = self.symbol_ids.get(intent.symbol.upper())
        if symbol_id is None:
            raise OfficialMessageError(f"symbol id not configured for DEMO transport: {intent.symbol}")
        protocol_volume = self.validate_open_quantity(intent.quantity)
        request = self._message("ProtoOANewOrderReq")
        _set(request, "ctidTraderAccountId", _int_id(self.account_id, "account_id"))
        _set(request, "symbolId", int(symbol_id))
        _set(request, "orderType", self._enum("ProtoOAOrderType", "MARKET", message=request, field_name="orderType"))
        _set(
            request,
            "tradeSide",
            self._enum("ProtoOATradeSide", intent.side.value, message=request, field_name="tradeSide"),
        )
        _set(request, "volume", protocol_volume)
        _set(request, "clientOrderId", intent.intent_id)
        _set(request, "label", intent.intent_id)
        _set_order_options(request, intent.metadata)
        return request

    def build_close_position(self, position: Position, *, client_order_id: str) -> Any:
        if position.account_id != self.account_id or position.owner != "mtf-lab":
            raise DemoAccountRequired("official transport refuses a foreign position")
        request = self._message("ProtoOAClosePositionReq")
        _set(request, "ctidTraderAccountId", _int_id(self.account_id, "account_id"))
        _set(request, "positionId", _int_id(position.position_id, "position_id"))
        if self.volume_grid is not None:
            try:
                protocol_volume = self.volume_grid.validate_close_quantity(position.quantity)
            except VolumeRuleError as exc:
                raise OfficialMessageError(str(exc)) from exc
        else:
            protocol_volume = _volume_to_protocol(position.quantity, self.config.volume_scale)
        _set(request, "volume", protocol_volume)
        # ProtoOAClosePositionReq has no clientOrderId field.  Correlation is
        # carried by the WireMessage request id in CTraderClientGateway.
        return request

    def build_reconcile(self) -> Any:
        request = self._message("ProtoOAReconcileReq")
        _set(request, "ctidTraderAccountId", _int_id(self.account_id, "account_id"))
        # This field is not in the installed 0.9.2 schema.  Plain fixture
        # messages retain the old visible attribute for compatibility only.
        if not _has_descriptor(request):
            _set(request, "returnProtectionOrders", False)
        return request

    def build_order_list(self, *, from_timestamp: datetime | None = None, to_timestamp: datetime | None = None) -> Any:
        request = self._message("ProtoOAOrderListReq")
        _set(request, "ctidTraderAccountId", _int_id(self.account_id, "account_id"))
        _set_optional_timestamp(request, "fromTimestamp", from_timestamp)
        _set_optional_timestamp(request, "toTimestamp", to_timestamp)
        return request

    def build_order_details(self, order_id: str | int) -> Any:
        request = self._message("ProtoOAOrderDetailsReq")
        _set(request, "ctidTraderAccountId", _int_id(self.account_id, "account_id"))
        _set(request, "orderId", _int_id(order_id, "order_id"))
        return request

    def build_deal_list(
        self,
        *,
        from_timestamp: datetime | None = None,
        to_timestamp: datetime | None = None,
        max_rows: int | None = None,
    ) -> Any:
        request = self._message("ProtoOADealListReq")
        _set(request, "ctidTraderAccountId", _int_id(self.account_id, "account_id"))
        _set_optional_timestamp(request, "fromTimestamp", from_timestamp)
        _set_optional_timestamp(request, "toTimestamp", to_timestamp)
        if max_rows is not None:
            if isinstance(max_rows, bool) or int(max_rows) <= 0:
                raise OfficialMessageError("max_rows debe ser entero positivo")
            _set(request, "maxRows", int(max_rows))
        return request

    def build_deal_list_by_position_id(self, position_id: str | int) -> Any:
        request = self._message("ProtoOADealListByPositionIdReq")
        _set(request, "ctidTraderAccountId", _int_id(self.account_id, "account_id"))
        _set(request, "positionId", _int_id(position_id, "position_id"))
        return request

    def submit(self, intent: ExecutionIntent, *, timeout_seconds: float) -> OrderSnapshot:
        message = self.build_new_order(intent)
        # Validate the observed volume grid before retaining a correlation
        # record or crossing the gateway boundary.
        self.register_intent(intent)
        self._requested_quantities[intent.intent_id] = _decimal_value(intent.quantity, "intent quantity", positive=True)
        self._intent_created_at[intent.intent_id] = _parse_time(intent.created_at)
        response = self._send(message, client_msg_id=intent.intent_id, timeout_seconds=timeout_seconds)
        if response is None:
            raise OfficialTimeoutError("official cTrader transport returned no response")
        return self._snapshot_from_response(
            response,
            client_order_id=intent.intent_id,
            requested_quantity=intent.quantity,
            closing=False,
        )

    def get_order(self, client_order_id: str) -> OrderSnapshot | None:
        key = str(client_order_id)
        response = self._send(
            self.build_reconcile(),
            client_msg_id=f"reconcile:{key}",
            timeout_seconds=self.config.timeout_seconds,
        )
        self._check_account(response, required=True)
        closing = self._intent_kinds.get(key) == "CLOSE"
        expected_position_id = self._close_positions.get(key) if closing else None
        matching = self._matching_order_response(response, key, expected_position_id=expected_position_id)
        if matching is not None:
            return self._snapshot_from_response(
                matching,
                client_order_id=key,
                requested_quantity=self._requested_quantity(matching, key),
                closing=closing,
                require_account=False,
                require_client_correlation=not closing,
                expected_position_id=expected_position_id,
            )
        closing = self._intent_kinds.get(key) == "CLOSE"
        return self._recover_from_history(
            key,
            closing=closing,
            expected_position_id=self._close_positions.get(key) if closing else None,
        )

    def _recover_from_history(
        self, key: str, *, closing: bool = False, expected_position_id: str | None = None
    ) -> OrderSnapshot | None:
        if not self._history_available():
            # A fixture without historical descriptors cannot prove a current
            # state. Never return a local snapshot as a server observation.
            return None
        order = self._find_historical_order(key, expected_position_id=expected_position_id)
        if order is None:
            return None
        requested = self._requested_quantity(order, key)
        order_id = _field(order, "orderId", "order_id", default=None)
        detail = self._history_detail(key, order_id)
        combined = detail or order
        deal_response = self._history_deals(key, order_id=order_id, expected_position_id=expected_position_id)
        if deal_response is not None and _many(deal_response, "deal", "deals"):
            combined = _merge_response_details(combined, deal_response)
        return self._snapshot_from_response(
            combined,
            client_order_id=key,
            requested_quantity=requested,
            closing=closing,
            require_client_correlation=not closing,
            expected_position_id=expected_position_id,
        )

    def _history_detail(self, key: str, order_id: Any) -> Any | None:
        if order_id is None or not self._message_available("ProtoOAOrderDetailsReq"):
            return None
        response = self._send(
            self.build_order_details(order_id),
            client_msg_id=f"history-detail:{key}:{order_id}",
            timeout_seconds=self.config.timeout_seconds,
        )
        self._check_account(response, required=True)
        return response

    def _history_deals(
        self, key: str, *, order_id: Any | None = None, expected_position_id: str | None = None
    ) -> Any | None:
        if not self._message_available("ProtoOADealListReq"):
            return None
        now = _parse_time(self.clock())
        start = self._intent_created_at.get(key, now - timedelta(seconds=self.config.history_window_seconds))
        response = self._send(
            self.build_deal_list(from_timestamp=start, to_timestamp=now),
            client_msg_id=f"history-deals:{key}",
            timeout_seconds=self.config.timeout_seconds,
        )
        self._check_account(response, required=True)
        return _filter_history_deals(response, order_id=order_id, expected_position_id=expected_position_id)

    def list_positions(self, account_id: str) -> Sequence[Position]:
        if str(account_id) != self.account_id:
            return ()
        response = self._send(
            self.build_reconcile(),
            client_msg_id="reconcile:positions",
            timeout_seconds=self.config.timeout_seconds,
        )
        self._check_account(response, required=True)
        positions: list[Position] = []
        for raw in _many(response, "position", "positions"):
            position = self._position_from_proto(raw)
            if position is not None:
                positions.append(position)
        return tuple(positions)

    def close_position(self, position: Position, *, client_order_id: str, timeout_seconds: float) -> OrderSnapshot:
        if position.account_id != self.account_id or position.owner != "mtf-lab":
            raise DemoAccountRequired("official transport refuses a foreign position")
        self._intent_kinds[str(client_order_id)] = "CLOSE"
        self._close_positions[str(client_order_id)] = position.position_id
        self._requested_quantities[client_order_id] = _decimal_value(
            position.quantity, "position quantity", positive=True
        )
        response = self._send(
            self.build_close_position(position, client_order_id=client_order_id),
            client_msg_id=client_order_id,
            timeout_seconds=timeout_seconds,
        )
        if response is None:
            raise OfficialTimeoutError("official cTrader close returned no response")
        return self._snapshot_from_response(
            response,
            client_order_id=client_order_id,
            requested_quantity=position.quantity,
            closing=True,
            expected_position_id=position.position_id,
            require_client_correlation=False,
        )

    def cancel_order(
        self, order_id: str, *, client_order_id: str | None = None, timeout_seconds: float
    ) -> OrderSnapshot:
        key = str(client_order_id or "")
        known = self._snapshots.get(key) if key else None
        if known is None and not key:
            raise OfficialMessageError("cancel requires a known client_order_id or cached order")
        target_order = str(order_id or (known.order_id if known else ""))
        if not target_order:
            raise OfficialMessageError("cancel requires a known order id")
        if known is not None and known.order_id is not None and str(known.order_id) != target_order:
            raise OfficialCorrelationError("cancel order id does not match the known client correlation")
        request = self._message("ProtoOACancelOrderReq")
        _set(request, "ctidTraderAccountId", _int_id(self.account_id, "account_id"))
        _set(request, "orderId", _int_id(target_order, "order_id"))
        response = self._send(
            request,
            client_msg_id=f"cancel:{key or target_order}",
            timeout_seconds=timeout_seconds,
        )
        if response is None:
            raise OfficialTimeoutError("official cTrader cancel returned no response")
        requested = known.requested_quantity if known is not None else self._requested_quantity(response, key)
        return self._snapshot_from_response(
            response,
            client_order_id=key or target_order,
            requested_quantity=requested,
            closing=False,
            require_client_correlation=False,
        )

    def _message(self, name: str) -> Any:
        factory = getattr(self.proto, name, None)
        if factory is None and isinstance(self.proto, Mapping):
            factory = self.proto.get(name)
        if factory is None:
            raise OfficialSDKUnavailable(f"injected official protobuf module lacks {name}")
        try:
            return factory() if callable(factory) else factory
        except Exception as exc:
            raise OfficialMessageError(f"cannot instantiate official protobuf message {name}") from exc

    def _enum(self, enum_name: str, member: str, *, message: Any, field_name: str) -> int | str:
        value = _module_enum_value(self.proto, enum_name, member)
        if value is not None:
            return value
        return _descriptor_enum_value(message, field_name, member)

    def _send(self, message: Any, *, client_msg_id: str, timeout_seconds: float) -> Any:
        if not str(client_msg_id).strip():
            raise OfficialMessageError("client_msg_id must be non-empty")
        timeout = float(timeout_seconds)
        if timeout <= 0 or not math.isfinite(timeout):
            raise OfficialMessageError("timeout_seconds must be positive and finite")
        try:
            # This is the one and only gateway call.  There is deliberately no
            # signature probing, Deferred inspection or second send attempt.
            return self.client.send(
                message,
                client_msg_id=str(client_msg_id),
                timeout_seconds=timeout,
            )
        except OfficialAdapterError:
            raise
        except TimeoutError as exc:
            raise OfficialUncertainSendError(str(exc), phase=SendPhase.SENT_NO_RESPONSE) from exc
        except Exception as exc:
            raise OfficialTransportError("falló el gateway DEMO; no se reintentó la orden") from exc

    def _requested_quantity(self, response: Any, key: str) -> DecimalValue:
        known = self._requested_quantities.get(str(key))
        if known is not None:
            return known
        cached = self._snapshots.get(str(key))
        if cached is not None:
            return cached.requested_quantity
        order = _field(response, "order", "order_data", default=None) or response
        raw = _field(order, "volume", "requestedVolume", "requested_volume", default=None)
        if raw is None:
            trade_data = _field(order, "tradeData", "trade_data", default=None)
            raw = _field(trade_data, "volume", "requestedVolume", "requested_volume", default=None)
        if raw is None:
            raise OfficialMessageError(f"requested quantity unavailable for {key}; no se asumió cantidad=1")
        quantity = _volume_from_protocol(raw, self.config.volume_scale)
        if quantity <= 0:
            raise OfficialMessageError(f"requested quantity is non-positive for {key}")
        return quantity

    def _snapshot_from_response(
        self,
        response: Any,
        *,
        client_order_id: str,
        requested_quantity: Decimal | DecimalValue | float,
        closing: bool,
        require_client_correlation: bool = True,
        require_account: bool = True,
        expected_position_id: str | None = None,
    ) -> OrderSnapshot:
        if response is None:
            raise OfficialTimeoutError("official cTrader transport returned no response")
        self._check_account(response, required=require_account)
        requested = _decimal_value(requested_quantity, "requested quantity", positive=True)
        order = _response_order(response)
        _validate_response_correlation(
            response,
            order,
            client_order_id,
            required=require_client_correlation,
        )
        if closing:
            _validate_close_response_kind(
                response,
                order,
                requested_quantity=requested,
                expected_position_id=expected_position_id,
                volume_scale=self.config.volume_scale,
            )
        error_code = _field(response, "errorCode", "error_code", default=None)
        description = _field(response, "description", default=None)
        execution = _enum_field_name(response, "executionType", "execution_type")
        order_status = _enum_field_name(order, "orderStatus", "order_status")
        deal_items = _many(response, "deal", "deals")
        deal = deal_items[0] if deal_items else _field(response, "deal", default=None)
        deal_status = _enum_field_name(deal, "dealStatus", "deal_status") if deal is not None else ""
        order_id = _response_order_id(order, deal)
        position_ids = _position_ids(response, order, deal_items or deal)
        previous = self._snapshots.get(str(client_order_id))
        if previous is not None:
            if previous.order_id is not None and order_id is not None and str(previous.order_id) != str(order_id):
                raise OfficialCorrelationError(
                    "same client correlation returned a different server order id",
                    phase=SendPhase.RESPONSE_RECEIVED,
                )
            if previous.position_ids and position_ids and set(previous.position_ids) != set(position_ids):
                raise OfficialCorrelationError(
                    "same client correlation returned a different position identity",
                    phase=SendPhase.RESPONSE_RECEIVED,
                )
        if (
            expected_position_id is not None
            and error_code is None
            and execution not in {"ORDER_REJECTED", "ORDER_CANCELLED", "ORDER_EXPIRED", "ORDER_CANCEL_REJECTED"}
            and order_status not in {"ORDER_STATUS_REJECTED", "ORDER_STATUS_CANCELLED", "ORDER_STATUS_EXPIRED"}
            and str(expected_position_id) not in position_ids
        ):
            raise OfficialResponseError(
                "close response lacks the requested position identity",
                phase=SendPhase.RESPONSE_RECEIVED,
            )
        fills = self._merge_fills(client_order_id, response, deal, requested)
        filled = _response_filled_quantity(self, response, order, fills)
        if filled > requested:
            raise OfficialResponseError("filled quantity exceeds requested quantity", phase=SendPhase.RESPONSE_RECEIVED)
        state, reason = _state_from_exact_evidence(
            execution=execution,
            order_status=order_status,
            deal_status=deal_status,
            filled=filled,
            requested=requested,
            closing=closing,
            error_code=str(error_code) if error_code is not None else None,
        )
        reject_reason = _response_reason(error_code, description, state, execution, order_status, deal_status)
        stop_loss, take_profit = _response_protection(response, order)
        snapshot = OrderSnapshot(
            str(order_id) if order_id is not None else None,
            str(client_order_id),
            state,
            requested,
            filled,
            tuple(fills),
            reject_reason,
            tuple(position_ids),
            _parse_time(self.clock()),
            reason,
            stop_loss=stop_loss,
            take_profit=take_profit,
        )
        self._snapshots[str(client_order_id)] = snapshot
        for pid in position_ids:
            self._position_clients[pid] = str(client_order_id)
        return snapshot

    def _merge_fills(
        self,
        client_order_id: str,
        response: Any,
        deal: Any,
        requested: DecimalValue,
    ) -> tuple[Fill, ...]:
        existing = self._snapshots.get(str(client_order_id))
        by_id: dict[str, Fill] = {fill.fill_id: fill for fill in (existing.fills if existing else ())}
        for fill in self._fills_from(response, deal, requested, client_order_id=client_order_id):
            by_id.setdefault(fill.fill_id, fill)
        return tuple(by_id.values())

    def _fills_from(
        self,
        response: Any,
        deal: Any,
        requested: DecimalValue,
        *,
        client_order_id: str | None = None,
    ) -> tuple[Fill, ...]:
        candidates = _many(response, "deal", "deals")
        if deal is not None and all(raw is not deal for raw in candidates):
            candidates.append(deal)
        cached = self._snapshots.get(str(client_order_id)) if client_order_id is not None else None
        used = sum((fill.quantity for fill in cached.fills), DecimalValue("0")) if cached else DecimalValue("0")
        fills: list[Fill] = []
        for raw in candidates:
            fill = self._fill_from_candidate(raw, response, requested, used)
            if fill is None:
                continue
            fills.append(fill)
            used += fill.quantity
        return tuple(fills)

    def _fill_from_candidate(
        self,
        raw: Any,
        response: Any,
        requested: DecimalValue,
        used: DecimalValue,
    ) -> Fill | None:
        deal_id = _field(raw, "dealId", "deal_id", default=None)
        if deal_id is None:
            return None
        account_id = str(_field(response, "ctidTraderAccountId", "ctid_trader_account_id", default=self.account_id))
        scope_key = (account_id, self.account.environment, str(deal_id))
        if scope_key in self._seen_deals:
            return None
        raw_volume = _field(raw, "filledVolume", "filled_volume", default=None)
        raw_volume = raw_volume if raw_volume is not None else _field(raw, "volume", default=None)
        if raw_volume is None:
            return None
        quantity = _volume_from_protocol(raw_volume, self.config.volume_scale)
        remaining = requested - used
        if quantity <= 0 or remaining <= 0:
            return None
        quantity = min(quantity, remaining)
        price_raw = _field(raw, "executionPrice", "execution_price", default=None)
        if price_raw is None:
            return None
        try:
            price = _decimal_value(price_raw, "execution price", positive=True)
        except ValueError:
            return None
        stamp_raw = _field(raw, "executionTimestamp", "execution_timestamp", default=None)
        stamp = _ms_time(stamp_raw) if stamp_raw is not None else _parse_time(self.clock())
        self._seen_deals.add(scope_key)
        return Fill(str(deal_id), quantity, price, stamp)

    def _position_from_proto(self, raw: Any) -> Position | None:
        components = self._position_components(raw)
        if components is None:
            return None
        pid, symbol, volume, price, side_raw, client_id = components
        trade = _field(raw, "tradeData", "trade_data", default=None) or raw
        opened_raw = _field(trade, "openTimestamp", "open_timestamp", default=None)
        opened_at = _ms_time(opened_raw) if opened_raw is not None else None
        stop_loss = _optional_server_price(raw, "stopLoss", "stop_loss")
        take_profit = _optional_server_price(raw, "takeProfit", "take_profit")
        side = _trade_side(side_raw, owner=trade, field_name="tradeSide")
        owner = "mtf-lab" if client_id and str(client_id) in self._requested_quantities else "external"
        return Position(
            str(pid),
            self.account_id,
            symbol,
            side,
            volume,
            price,
            str(client_id) if client_id else None,
            owner=owner,
            opened_at=opened_at,
            stop_loss=stop_loss,
            take_profit=take_profit,
        )

    def _position_components(self, raw: Any) -> tuple[Any, str, DecimalValue, DecimalValue, Any, Any] | None:
        pid = _field(raw, "positionId", "position_id", default=None)
        if pid is None:
            return None
        status = _enum_field_name(raw, "positionStatus", "position_status")
        if _has_descriptor(raw) and not status:
            return None
        if status.startswith("__UNKNOWN_") or status in {"POSITION_STATUS_CLOSED", "POSITION_STATUS_ERROR"}:
            return None
        trade = _field(raw, "tradeData", "trade_data", default=None) or raw
        symbol_id = _field(trade, "symbolId", "symbol_id", default=None)
        symbol_id = symbol_id if symbol_id is not None else _field(raw, "symbolId", "symbol_id", default=None)
        raw_volume = _field(trade, "volume", default=None)
        raw_volume = raw_volume if raw_volume is not None else _field(raw, "volume", default=None)
        raw_price = _field(raw, "price", "entryPrice", "entry_price", default=None)
        side_raw = _field(trade, "tradeSide", "trade_side", default=None)
        side_raw = side_raw if side_raw is not None else _field(raw, "tradeSide", "trade_side", default=None)
        if symbol_id is None or raw_volume is None or raw_price is None or side_raw is None:
            return None
        volume = _volume_from_protocol(raw_volume, self.config.volume_scale)
        price = _decimal_value(raw_price, "position price", positive=True)
        if volume <= 0:
            return None
        symbol = self.symbol_names.get(int(symbol_id), str(symbol_id))
        client_id = self._position_clients.get(str(pid)) or _field(
            trade, "label", "clientOrderId", "client_order_id", default=None
        )
        return pid, symbol, volume, price, side_raw, client_id

    def _check_account(self, response: Any, *, required: bool = False) -> None:
        if response is None:
            if required:
                raise OfficialResponseError(
                    "official response lacks account identity", phase=SendPhase.RESPONSE_RECEIVED
                )
            return
        account_id = _field(response, "ctidTraderAccountId", "ctid_trader_account_id", default=None)
        if account_id is None and required:
            raise OfficialResponseError("official response lacks account identity", phase=SendPhase.RESPONSE_RECEIVED)
        if account_id is not None and str(account_id) != self.account_id:
            raise OfficialCorrelationError("official response account id mismatch")

    def _matching_order_response(
        self, response: Any, key: str, *, expected_position_id: str | None = None
    ) -> Any | None:
        if response is None:
            return None
        self._check_account(response)
        # An execution event returned directly by a local gateway is a valid
        # current result when its embedded correlation is absent or exact.
        if _field(response, "executionType", "execution_type", default=None) is not None:
            candidate = _field(response, "order", "order_data", default=None) or response
            candidate_id = _field(candidate, "clientOrderId", "client_order_id", "label", default=None)
            if (
                candidate_id is not None
                and str(candidate_id) == key
                and (
                    expected_position_id is None
                    or _is_closing_order(candidate, expected_position_id, response=response)
                )
            ):
                return response
            if (
                candidate_id is None
                and expected_position_id is not None
                and _is_closing_order(candidate, expected_position_id, response=response)
            ):
                return response
        for order in _many(response, "order", "orders"):
            candidate_id = _field(order, "clientOrderId", "client_order_id", "label", default=None)
            if (
                candidate_id is not None
                and str(candidate_id) == key
                and (expected_position_id is None or _is_closing_order(order, expected_position_id, response=response))
            ):
                return order
            if (
                candidate_id is None
                and expected_position_id is not None
                and _is_closing_order(order, expected_position_id, response=response)
            ):
                return order
        return None

    def _history_available(self) -> bool:
        return self._message_available("ProtoOAOrderListReq") and self._message_available("ProtoOAOrderListRes")

    def _message_available(self, name: str) -> bool:
        factory = getattr(self.proto, name, None)
        return factory is not None or (isinstance(self.proto, Mapping) and name in self.proto)

    def _find_historical_order(self, key: str, *, expected_position_id: str | None = None) -> Any | None:
        now = _parse_time(self.clock())
        start = self._intent_created_at.get(key, now - timedelta(seconds=self.config.history_window_seconds))
        seen: set[tuple[str, str]] = set()
        current_to = now
        for page in range(self.config.history_page_limit):
            response = self._send(
                self.build_order_list(from_timestamp=start, to_timestamp=current_to),
                client_msg_id=f"history-orders:{key}:{page}",
                timeout_seconds=self.config.timeout_seconds,
            )
            self._check_account(response, required=True)
            order, identities, has_more, next_to = _historical_page(
                response, key, expected_position_id=expected_position_id
            )
            if order is not None:
                return order
            if not identities or identities.issubset(seen) or not has_more:
                return None
            seen.update(identities)
            if next_to is None or next_to >= current_to or next_to < start:
                return None
            current_to = next_to
        return None


class CTraderOfficialDemoTransport(CTraderDemoTransport):
    """Descriptive alias emphasizing official Protobuf message semantics."""


def load_official_proto() -> Any:
    try:
        return importlib.import_module("mtf_lab.data.protobuf_generated.OpenApiMessages_pb2")
    except Exception as exc:
        raise OfficialSDKUnavailable("install/inject Spotware OpenApiPy generated protobuf module") from exc


def _parse_time(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
        dt = datetime.fromtimestamp(float(value), UTC)
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError as exc:
            raise OfficialMessageError("timestamp must be ISO-8601 or a datetime") from exc
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise OfficialMessageError("timestamp must include timezone")
    return dt.astimezone(UTC)


def _authenticated_session_identity(session: AuthenticatedSession) -> tuple[str, str, str, str]:
    account_id = str(session.account_id).strip()
    environment = str(session.environment).strip().upper()
    endpoint = str(session.endpoint).strip()
    session_id = str(session.session_id).strip()
    if not session_id or not account_id.isdigit() or int(account_id) <= 0:
        raise DemoAccountRequired("la sesión autenticada debe identificar una cuenta numérica")
    if environment != "DEMO":
        raise RealAccountForbidden("la evidencia de sesión no es DEMO")
    if not _is_official_demo_endpoint(endpoint):
        raise EndpointRejected("la sesión autenticada debe usar el endpoint DEMO oficial")
    return session_id, account_id, environment, endpoint


def _authenticated_session_policy(session: AuthenticatedSession) -> frozenset[str]:
    scopes = _normalize_scopes(session.scopes)
    if not _scope_has_trading(scopes):
        raise ScopeRejected("la sesión autenticada no observa permiso exacto de trading")
    return scopes


def _authenticated_session_times(session: AuthenticatedSession) -> tuple[datetime, datetime | None, str, str]:
    authenticated_at = _parse_time(session.authenticated_at)
    generation = str(session.connection_generation).strip()
    if not generation:
        raise OfficialGatewayConfigurationError("la evidencia de sesión debe incluir generation")
    expires_at = _parse_time(session.expires_at) if session.expires_at is not None else None
    if expires_at is not None and expires_at <= authenticated_at:
        raise ValueError("expires_at debe ser posterior a authenticated_at")
    evidence_source = str(session.evidence_source).strip().lower()
    if not evidence_source or evidence_source in {"caller", "local", "unknown"}:
        raise OfficialGatewayConfigurationError("la evidencia debe indicar un origen de cliente autenticado")
    return authenticated_at, expires_at, generation, evidence_source


def _authenticated_session_values(session: AuthenticatedSession) -> tuple[Any, ...]:
    session_id, account_id, environment, endpoint = _authenticated_session_identity(session)
    scopes = _authenticated_session_policy(session)
    authenticated_at, expires_at, generation, evidence_source = _authenticated_session_times(session)
    return (
        session_id,
        account_id,
        environment,
        endpoint,
        scopes,
        authenticated_at,
        expires_at,
        generation,
        evidence_source,
    )


def _validate_transport_gateway(account: DemoAccount | Mapping[str, Any], client: Any) -> None:
    raw_environment = _raw_account_environment(account)
    if "REAL" in raw_environment or "LIVE" in raw_environment or raw_environment in {"PRODUCTION", "PAPER_LIVE"}:
        raise RealAccountForbidden("official transport is DEMO-only; REAL/LIVE is rejected")
    if _is_sdk_client(client):
        raise OfficialGatewayConfigurationError(
            "ctrader_open_api.Client devuelve Deferred; componga CTraderClientGateway"
        )
    if not isinstance(client, OfficialGateway):
        raise OfficialGatewayConfigurationError("client no implementa OfficialGateway.send explícito")


def _raw_account_environment(account: DemoAccount | Mapping[str, Any]) -> str:
    value = (
        account.get("environment", account.get("mode", ""))
        if isinstance(account, Mapping)
        else getattr(account, "environment", "")
    )
    return str(value).strip().upper()


def _validate_transport_endpoints(account: DemoAccount, config: CTraderDemoTransportConfig) -> None:
    if not _is_official_demo_endpoint(account.endpoint):
        raise EndpointRejected("selected account endpoint is not the official DEMO endpoint")
    if config.endpoint.lower() != DEMO_PROTOBUF_ENDPOINT:
        raise EndpointRejected("official cTrader transport requires demo.ctraderapi.com:5035")


def _resolve_observation(
    client: Any, observation: ServerAccountObservation | Mapping[str, Any] | None
) -> ServerAccountObservation:
    if observation is None and isinstance(client, CTraderClientGateway):
        return client.server_observation()
    if isinstance(observation, Mapping):
        return ServerAccountObservation.from_mapping(observation)
    if not isinstance(observation, ServerAccountObservation):
        raise DemoAccountRequired(
            "el transporte oficial requiere ServerAccountObservation; verified=true no es suficiente"
        )
    return observation


def _validate_observation(
    account: DemoAccount, client: Any, observation: ServerAccountObservation, clock: Callable[[], datetime]
) -> None:
    if observation.account_id != account.account_id:
        raise OfficialCorrelationError("server observation account id mismatch", phase=SendPhase.LOCAL_ERROR)
    if observation.environment != "DEMO":
        raise RealAccountForbidden("server observation is not DEMO")
    if observation.endpoint.lower() != DEMO_PROTOBUF_ENDPOINT:
        raise EndpointRejected("server observation endpoint does not match official DEMO endpoint")
    if not observation.valid_at(clock()):
        raise DemoAccountRequired("la evidencia del servidor está vencida")
    if isinstance(client, CTraderClientGateway) and (
        not observation.session_id or not observation.matches_session(client.session, at=clock())
    ):
        raise DemoAccountRequired("la evidencia externa no coincide con la sesión autenticada del gateway")


def _account_with_observation(account: DemoAccount, observation: ServerAccountObservation) -> DemoAccount:
    scopes = set(account.scopes) | set(observation.scopes)
    if _scope_has_trading(frozenset(scopes)):
        scopes.add("trading")
    account = dataclasses.replace(account, scopes=frozenset(scopes)) if scopes != set(account.scopes) else account
    return dataclasses.replace(account, verified=True) if not account.verified else account


def _is_official_demo_endpoint(endpoint: str) -> bool:
    text = str(endpoint).strip().lower()
    return (
        text == DEMO_PROTOBUF_ENDPOINT
        or text == f"tcp://{DEMO_PROTOBUF_ENDPOINT}"
        or text == f"ssl://{DEMO_PROTOBUF_ENDPOINT}"
    )


def _is_demo_endpoint(endpoint: str) -> bool:
    """Compatibility spelling for the strict official DEMO endpoint check."""

    return _is_official_demo_endpoint(endpoint)


def _is_sdk_client(client: Any) -> bool:
    cls = type(client)
    return cls.__name__ == "Client" and str(cls.__module__).startswith("ctrader_open_api")


def _normalize_scopes(value: Iterable[str] | str) -> frozenset[str]:
    values = value.replace(",", " ").split() if isinstance(value, str) else value
    return frozenset(str(item).strip().lower() for item in values if str(item).strip())


def _scope_has_trading(scopes: frozenset[str]) -> bool:
    return "trading" in scopes


def _payload_type_for_message(message: Any) -> int:
    name = type(message).__name__
    try:
        return PAYLOAD_TYPES[name]
    except KeyError as exc:
        raise OfficialMessageError(f"unsupported official request message: {name}") from exc


def _module_enum_value(proto: Any, enum_name: str, member: str) -> int | str | None:
    enum_type = getattr(proto, enum_name, None)
    if enum_type is None and isinstance(proto, Mapping):
        enum_type = proto.get(enum_name)
    if enum_type is None:
        return None
    value = getattr(enum_type, "Value", None)
    if callable(value):
        try:
            resolved = value(member)
            return resolved if isinstance(resolved, (int, str)) else None
        except (KeyError, ValueError, TypeError):
            return None
    candidate = getattr(enum_type, member, None)
    return int(candidate) if candidate is not None and not callable(candidate) else None


def _descriptor_enum_value(message: Any, field_name: str, member: str) -> int | str:
    descriptor = _field_descriptor(message, field_name)
    enum_descriptor = getattr(descriptor, "enum_type", None)
    if enum_descriptor is not None:
        for item in getattr(enum_descriptor, "values", ()):
            if str(item.name).upper() == str(member).upper():
                return int(item.number)
        raise OfficialMessageError(f"unknown enum {member} for {field_name}")
    # Plain fixture messages have no descriptor; preserve inspectability.
    return member


def _set(message: Any, name: str, value: Any) -> None:
    declared = _field_declared(message, name)
    if declared is False:
        raise OfficialMessageError(f"official message has no field {name}")
    try:
        setattr(message, name, value)
    except Exception as exc:
        raise OfficialMessageError(f"cannot set official field {name}") from exc


def _set_optional_timestamp(message: Any, name: str, value: datetime | None) -> None:
    if value is None:
        return
    _set(message, name, _timestamp_ms(value))


def _set_optional_field(message: Any, name: str, value: Any) -> None:
    """Set an optional generated field when the installed schema exposes it."""

    if value is None or _field_declared(message, name) is False:
        return
    _set(message, name, value)


def _validated_order_options(metadata: Mapping[str, Any]) -> Mapping[str, Any]:
    raw = metadata.get("order_options", {}) if isinstance(metadata, Mapping) else {}
    if not isinstance(raw, Mapping):
        raise OfficialMessageError("order_options must be a mapping")
    return raw


def _set_numeric_order_option(message: Any, raw: Mapping[str, Any], source: str, target: str) -> None:
    if source not in raw:
        return
    value = raw[source]
    if target in {"trailingStopLoss", "guaranteedStopLoss"}:
        if not isinstance(value, bool):
            raise OfficialMessageError(f"{source} must be boolean")
    else:
        if isinstance(value, bool):
            raise OfficialMessageError(f"{source} must be integer")
        try:
            value = int(value)
        except (TypeError, ValueError) as exc:
            raise OfficialMessageError(f"{source} must be integer") from exc
        if value <= 0 and target != "slippageInPoints":
            raise OfficialMessageError(f"{source} must be positive")
        if value < 0:
            raise OfficialMessageError(f"{source} must be non-negative")
    _set_optional_field(message, target, value)


def _set_price_order_option(message: Any, raw: Mapping[str, Any], source: str, target: str) -> None:
    if source not in raw or raw[source] is None:
        return
    try:
        value = float(raw[source])
    except (TypeError, ValueError) as exc:
        raise OfficialMessageError(f"{source} must be finite") from exc
    if not math.isfinite(value) or value <= 0:
        raise OfficialMessageError(f"{source} must be finite and positive")
    _set_optional_field(message, target, value)


def _set_expiration_order_option(message: Any, raw: Mapping[str, Any]) -> None:
    if raw.get("expiration_timestamp") is None:
        return
    expiration = raw["expiration_timestamp"]
    if isinstance(expiration, datetime):
        expiration = _timestamp_ms(expiration)
    else:
        try:
            expiration = int(expiration)
        except (TypeError, ValueError) as exc:
            raise OfficialMessageError("expiration_timestamp must be milliseconds") from exc
    if expiration <= 0:
        raise OfficialMessageError("expiration_timestamp must be positive")
    _set_optional_field(message, "expirationTimestamp", expiration)


def _set_order_options(message: Any, metadata: Mapping[str, Any]) -> None:
    raw = _validated_order_options(metadata)
    for source, target in (
        ("relative_stop_loss", "relativeStopLoss"),
        ("relative_take_profit", "relativeTakeProfit"),
        ("slippage_in_points", "slippageInPoints"),
        ("trailing_stop_loss", "trailingStopLoss"),
        ("guaranteed_stop_loss", "guaranteedStopLoss"),
    ):
        _set_numeric_order_option(message, raw, source, target)
    for source, target in (("stop_loss", "stopLoss"), ("take_profit", "takeProfit")):
        _set_price_order_option(message, raw, source, target)
    _set_expiration_order_option(message, raw)
    if raw.get("comment") is not None:
        _set_optional_field(message, "comment", str(raw["comment"]))


def _timestamp_ms(value: datetime) -> int:
    stamp = _parse_time(value)
    return int(stamp.timestamp() * 1000)


_MISSING = object()


def _has_descriptor(value: Any) -> bool:
    return getattr(value, "DESCRIPTOR", None) is not None


def _field_descriptor(owner: Any, name: str) -> Any | None:
    descriptor = getattr(owner, "DESCRIPTOR", None)
    fields_by_name = getattr(descriptor, "fields_by_name", None)
    return fields_by_name.get(name) if fields_by_name is not None else None


def _field_declared(owner: Any, name: str) -> bool | None:
    descriptor = _field_descriptor(owner, name)
    if descriptor is not None:
        return True
    if _has_descriptor(owner):
        return False
    if isinstance(owner, Mapping):
        return name in owner
    # A plain local fixture has no schema to consult; allow its message
    # builder to assign declared-by-contract fields.  Presence for reads still
    # comes from its instance dictionary.
    return None


def _field_present(owner: Any, name: str) -> bool:
    if owner is None:
        return False
    if isinstance(owner, Mapping):
        return name in owner
    if _has_descriptor(owner):
        return _generated_field_present(owner, name)
    values = vars(owner) if hasattr(owner, "__dict__") else {}
    return name in values


def _generated_field_present(owner: Any, name: str) -> bool:
    """Delegate generated-message presence to the shared protocol helper."""

    return bool(_central_field_present(owner, name))


def _field(value: Any, *names: str, default: Any = None) -> Any:
    if value is None:
        return default
    for name in names:
        if not _field_present(value, name):
            continue
        if isinstance(value, Mapping):
            return value[name]
        result = getattr(value, name)
        descriptor = _field_descriptor(value, name)
        if descriptor is not None and getattr(descriptor, "is_repeated", False) and not result:
            continue
        return result
    return default


def _many(value: Any, *names: str) -> list[Any]:
    if value is None:
        return []
    if _has_descriptor(value):
        return list(_central_read_repeated(value, *names))
    for name in names:
        if not _field_present(value, name):
            continue
        raw = value[name] if isinstance(value, Mapping) else getattr(value, name)
        if isinstance(raw, (str, bytes, Mapping)):
            return [raw]
        try:
            return list(raw)
        except TypeError:
            return [raw]
    return []


def _enum_name(owner: Any, field_name: str) -> str:
    value = _field(owner, field_name, default=None)
    if value is None:
        return ""
    if _has_descriptor(owner):
        resolved = _central_enum_name(owner, field_name, value)
        if resolved is not None:
            return str(resolved).upper()
        return _unknown_enum_name(value)
    return _fixture_enum_name(value)


def _unknown_enum_name(value: Any) -> str:
    try:
        return f"__UNKNOWN_{int(value)}"
    except (TypeError, ValueError):
        return f"__UNKNOWN_{value}"


def _protocol_true(value: Any) -> bool:
    return value is True or (isinstance(value, int) and not isinstance(value, bool) and value == 1)


def _fixture_enum_name(value: Any) -> str:
    if isinstance(value, str):
        return value.strip().upper()
    enum_name = getattr(value, "name", None)
    return str(enum_name if enum_name is not None else value).strip().upper()


def _enum_field_name(owner: Any, *field_names: str) -> str:
    for name in field_names:
        if _field_present(owner, name):
            return _enum_name(owner, name)
    return ""


def _trade_side(value: Any, *, owner: Any | None = None, field_name: str = "tradeSide") -> Side:
    name = _enum_name(owner, field_name) if owner is not None else str(value).strip().upper()
    if name == "BUY":
        return Side.BUY
    if name == "SELL":
        return Side.SELL
    raise OfficialResponseError(f"unknown trade side enum: {name or value!r}")


def _volume_to_protocol(quantity: Any, scale: int) -> int:
    value = _decimal_value(quantity, "volume", positive=True)
    try:
        scaled = value * DecimalValue(int(scale))
        integral = scaled.to_integral_value(rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError) as exc:
        raise OfficialMessageError("volume cannot be represented in protocol units") from exc
    if scaled != integral:
        raise OfficialMessageError(f"volume {_decimal_text(value)} no es múltiplo exacto de 1/{int(scale)}")
    result = int(integral)
    if result <= 0:
        raise OfficialMessageError("volume rounds to zero in official protocol units")
    return result


def _volume_from_protocol(value: Any, scale: int) -> DecimalValue:
    if isinstance(value, bool):
        raise OfficialResponseError("protocol volume cannot be boolean")
    try:
        parsed = DecimalValue(value)
        raw = int(parsed)
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise OfficialResponseError("protocol volume must be an integer") from exc
    if parsed != DecimalValue(raw):
        raise OfficialResponseError("protocol volume must be an integer")
    if raw <= 0:
        return DecimalValue("0")
    return DecimalValue(raw) / DecimalValue(int(scale))


def _ms_time(value: Any) -> datetime:
    if isinstance(value, bool):
        raise OfficialResponseError("execution timestamp must be milliseconds")
    try:
        raw = int(value)
    except (TypeError, ValueError) as exc:
        raise OfficialResponseError("execution timestamp must be integer milliseconds") from exc
    return datetime.fromtimestamp(raw / 1000, UTC)


def _int_id(value: Any, name: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise OfficialMessageError(f"{name} must be an integer cTrader id") from exc
    if parsed <= 0:
        raise OfficialMessageError(f"{name} must be positive")
    return parsed


def _response_order(response: Any) -> Any:
    return _field(response, "order", "order_data", default=None) or response


def _validate_response_correlation(response: Any, order: Any, client_order_id: str, *, required: bool = True) -> None:
    response_client = _field(response, "clientOrderId", "client_order_id", "clientMsgId", "client_msg_id", default=None)
    order_client = _field(order, "clientOrderId", "client_order_id", "label", default=None)
    if required and response_client is None and order_client is None:
        raise OfficialCorrelationError(
            "official response lacks client correlation",
            phase=SendPhase.RESPONSE_RECEIVED,
        )
    if response_client is not None and str(response_client) != str(client_order_id):
        raise OfficialCorrelationError(
            "official response client correlation mismatch", phase=SendPhase.RESPONSE_RECEIVED
        )
    if order_client is not None and str(order_client) != str(client_order_id):
        raise OfficialCorrelationError("official order client correlation mismatch", phase=SendPhase.RESPONSE_RECEIVED)


def _response_filled_quantity(adapter: Any, response: Any, order: Any, fills: Sequence[Fill]) -> DecimalValue:
    filled = sum((fill.quantity for fill in fills), DecimalValue("0"))
    for owner in (order, response):
        raw_executed = _field(owner, "executedVolume", "executed_volume", default=None)
        if raw_executed is not None:
            filled = max(filled, _volume_from_protocol(raw_executed, adapter.config.volume_scale))
    return filled


def _response_reason(
    error_code: Any, description: Any, state: OrderState, execution: str, order_status: str, deal_status: str
) -> str | None:
    if error_code is not None:
        text = str(error_code)
        return text + f": {description}" if description else text
    if state in {OrderState.CANCELLED, OrderState.EXPIRED}:
        return execution or order_status or deal_status
    return None


def _response_order_id(order: Any, deal: Any) -> Any:
    order_id = _field(order, "orderId", "order_id", default=None)
    return order_id if order_id is not None else _field(deal, "orderId", "order_id", default=None)


def _optional_server_price(owner: Any, *names: str) -> DecimalValue | None:
    raw = _field(owner, *names, default=None)
    if raw is None:
        return None
    try:
        return _decimal_value(raw, names[0], positive=True)
    except ValueError as exc:
        raise OfficialResponseError(
            f"official response has invalid {names[0]}", phase=SendPhase.RESPONSE_RECEIVED
        ) from exc


def _response_protection(response: Any, order: Any) -> tuple[DecimalValue | None, DecimalValue | None]:
    stop_loss = _optional_server_price(order, "stopLoss", "stop_loss")
    take_profit = _optional_server_price(order, "takeProfit", "take_profit")
    position = _field(response, "position", default=None)
    positions = _many(response, "position", "positions")
    if positions:
        position = positions[0]
    if stop_loss is None:
        stop_loss = _optional_server_price(position, "stopLoss", "stop_loss")
    if take_profit is None:
        take_profit = _optional_server_price(position, "takeProfit", "take_profit")
    return stop_loss, take_profit


def _position_ids(response: Any, order: Any, deal: Any) -> list[str]:
    result: list[str] = []
    owners: list[Any] = [response, order]
    if isinstance(deal, (list, tuple)):
        owners.extend(deal)
    else:
        owners.append(deal)
    for owner in owners:
        value = _field(owner, "positionId", "position_id", default=None)
        if value is not None and str(value) not in result:
            result.append(str(value))
    return result


_EXECUTION_SIMPLE_STATES: dict[str, OrderState] = {
    "ORDER_REJECTED": OrderState.REJECTED,
    "ORDER_CANCEL_REJECTED": OrderState.REJECTED,
    "ORDER_CANCELLED": OrderState.CANCELLED,
    "ORDER_EXPIRED": OrderState.EXPIRED,
}
_ORDER_SIMPLE_STATES: dict[str, OrderState] = {
    "ORDER_STATUS_REJECTED": OrderState.REJECTED,
    "ORDER_STATUS_CANCELLED": OrderState.CANCELLED,
    "ORDER_STATUS_EXPIRED": OrderState.EXPIRED,
}
_DEAL_REJECT_STATES = frozenset({"REJECTED", "INTERNALLY_REJECTED", "ERROR", "MISSED"})
_ORDER_REJECT_ERROR_CODES = frozenset(
    {
        "REJECTED",
        "ORDER_REJECTED",
        "NOT_ENOUGH_MONEY",
        "MAX_EXPOSURE_REACHED",
        "POSITION_NOT_FOUND",
        "ORDER_NOT_FOUND",
        "POSITION_NOT_OPEN",
        "POSITION_LOCKED",
        "TOO_MANY_POSITIONS",
        "TRADING_BAD_VOLUME",
        "TRADING_BAD_STOPS",
        "TRADING_BAD_PRICES",
        "TRADING_BAD_STAKE",
        "TRADING_BAD_EXPIRATION_DATE",
        "TRADING_DISABLED",
        "TRADING_NOT_ALLOWED",
        "UNABLE_TO_CANCEL_ORDER",
        "UNABLE_TO_AMEND_ORDER",
        "SHORT_SELLING_NOT_ALLOWED",
        "SYMBOL_HAS_HOLIDAY",
        "NO_QUOTES",
    }
)


def _filled_state(
    *, filled: DecimalValue, requested: DecimalValue, closing: bool, partial: bool, missing_reason: str
) -> tuple[OrderState, str | None]:
    if filled <= 0:
        return OrderState.UNKNOWN, missing_reason
    if partial and filled < requested:
        return (OrderState.CLOSE_PARTIAL if closing else OrderState.PARTIAL), None
    return (OrderState.CLOSED if closing else OrderState.FILLED), None


def _state_from_execution(
    execution: str, *, filled: DecimalValue, requested: DecimalValue, closing: bool
) -> tuple[OrderState, str | None] | None:
    simple = _EXECUTION_SIMPLE_STATES.get(execution)
    if simple is not None:
        return simple, None
    if execution == "ORDER_PARTIAL_FILL":
        return _filled_state(
            filled=filled,
            requested=requested,
            closing=closing,
            partial=True,
            missing_reason="ORDER_PARTIAL_FILL_WITHOUT_QUANTITY",
        )
    if execution == "ORDER_FILLED":
        return _filled_state(
            filled=filled,
            requested=requested,
            closing=closing,
            partial=False,
            missing_reason="ORDER_FILLED_WITHOUT_QUANTITY",
        )
    if execution == "ORDER_ACCEPTED":
        return None if filled > 0 else (OrderState.SUBMITTED, None)
    if execution.startswith("__UNKNOWN_"):
        return OrderState.UNKNOWN, f"UNKNOWN_EXECUTION_TYPE:{execution}"
    return None


def _state_from_deal(
    deal_status: str, *, filled: DecimalValue, requested: DecimalValue, closing: bool
) -> tuple[OrderState, str | None] | None:
    if deal_status in _DEAL_REJECT_STATES:
        return OrderState.REJECTED, deal_status
    if deal_status == "PARTIALLY_FILLED":
        return _filled_state(
            filled=filled,
            requested=requested,
            closing=closing,
            partial=True,
            missing_reason="PARTIALLY_FILLED_WITHOUT_QUANTITY",
        )
    if deal_status == "FILLED":
        return _filled_state(
            filled=filled,
            requested=requested,
            closing=closing,
            partial=False,
            missing_reason="FILLED_DEAL_WITHOUT_QUANTITY",
        )
    return None


def _state_from_order(
    order_status: str, *, filled: DecimalValue, requested: DecimalValue, closing: bool
) -> tuple[OrderState, str | None] | None:
    simple = _ORDER_SIMPLE_STATES.get(order_status)
    if simple is not None:
        return simple, None
    if order_status == "ORDER_STATUS_FILLED":
        return _filled_state(
            filled=filled,
            requested=requested,
            closing=closing,
            partial=False,
            missing_reason="ORDER_STATUS_FILLED_WITHOUT_QUANTITY",
        )
    if order_status == "ORDER_STATUS_ACCEPTED":
        return (
            (
                OrderState.SUBMITTED,
                None,
            )
            if filled <= 0
            else (OrderState.CLOSE_PARTIAL if closing else OrderState.PARTIAL, None)
        )
    return None


def _state_from_exact_evidence(
    *,
    execution: str,
    order_status: str,
    deal_status: str,
    filled: DecimalValue,
    requested: DecimalValue,
    closing: bool,
    error_code: str | None,
) -> tuple[OrderState, str | None]:
    if error_code:
        if error_code in _ORDER_REJECT_ERROR_CODES:
            return OrderState.REJECTED, error_code
        return OrderState.UNKNOWN, f"REMOTE_PROTOCOL_ERROR:{error_code}"
    for resolver, value in (
        (_state_from_execution, execution),
        (_state_from_deal, deal_status),
        (_state_from_order, order_status),
    ):
        resolved = resolver(value, filled=filled, requested=requested, closing=closing)
        if resolved is not None:
            return resolved
    if filled > 0:
        return (OrderState.CLOSE_PARTIAL if closing else OrderState.PARTIAL), None
    return OrderState.SUBMITTED, None


def _is_closing_order(order: Any, expected_position_id: str, *, response: Any | None = None) -> bool:
    """Accept a position-id match only when the protocol marks a close."""

    closing = _field(order, "closingOrder", "closing_order", default=None)
    if not _protocol_true(closing):
        return False
    owner = response if response is not None else order
    deal = _field(owner, "deal", default=None)
    return str(expected_position_id) in _position_ids(owner, order, deal)


def _validate_close_response_kind(
    response: Any,
    order: Any,
    *,
    requested_quantity: DecimalValue,
    expected_position_id: str | None,
    volume_scale: int,
) -> None:
    """Validate close marker, identity and optional protocol quantity."""

    # Error responses have no order-kind envelope; their explicit error code
    # is enough to classify a rejected close without inventing one.
    if _field(response, "errorCode", "error_code", default=None) is not None:
        return
    execution = _enum_field_name(response, "executionType", "execution_type")
    order_status = _enum_field_name(order, "orderStatus", "order_status")
    if execution in {"ORDER_REJECTED", "ORDER_CANCELLED", "ORDER_EXPIRED", "ORDER_CANCEL_REJECTED"} or order_status in {
        "ORDER_STATUS_REJECTED",
        "ORDER_STATUS_CANCELLED",
        "ORDER_STATUS_EXPIRED",
    }:
        return
    closing = _field(order, "closingOrder", "closing_order", default=None)
    if not _protocol_true(closing):
        raise OfficialResponseError(
            "close response lacks closingOrder=true",
            phase=SendPhase.RESPONSE_RECEIVED,
        )
    if expected_position_id is not None and not _is_closing_order(order, expected_position_id, response=response):
        raise OfficialResponseError(
            "close response lacks the requested position identity",
            phase=SendPhase.RESPONSE_RECEIVED,
        )
    trade_data = _field(order, "tradeData", "trade_data", default=None)
    raw_volume = _field(order, "volume", "requestedVolume", "requested_volume", default=None)
    if raw_volume is None:
        raw_volume = _field(trade_data, "volume", "requestedVolume", "requested_volume", default=None)
    if raw_volume is not None:
        observed_quantity = _volume_from_protocol(raw_volume, volume_scale)
        if observed_quantity != requested_quantity:
            raise OfficialCorrelationError(
                "close response quantity does not match requested position quantity",
                phase=SendPhase.RESPONSE_RECEIVED,
            )


def _historical_page(
    response: Any, key: str, *, expected_position_id: str | None = None
) -> tuple[Any | None, set[tuple[str, str]], bool, datetime | None]:
    if response is None:
        return None, set(), False, None
    identities: set[tuple[str, str]] = set()
    timestamps: list[datetime] = []
    for order in _many(response, "order", "orders"):
        candidate = _field(order, "clientOrderId", "client_order_id", "label", default=None)
        if (
            candidate is not None
            and str(candidate) == key
            and (expected_position_id is None or _is_closing_order(order, expected_position_id))
        ):
            return order, identities, False, None
        if expected_position_id is not None and _is_closing_order(order, expected_position_id):
            return order, identities, False, None
        order_id = str(_field(order, "orderId", "order_id", default=""))
        identities.add((order_id, str(candidate or "")))
        raw_time = _field(order, "utcLastUpdateTimestamp", "utc_last_update_timestamp", default=None)
        if raw_time is not None:
            with contextlib.suppress(OfficialAdapterError):
                timestamps.append(_ms_time(raw_time))
    has_more = bool(_field(response, "hasMore", "has_more", default=False))
    next_to = min(timestamps) - timedelta(milliseconds=1) if timestamps else None
    return None, identities, has_more, next_to


def _filter_history_deals(
    response: Any, *, order_id: Any | None, expected_position_id: str | None
) -> Mapping[str, Any] | None:
    """Keep only deals proven to belong to the recovered order/position."""

    candidates = _many(response, "deal", "deals")
    if not candidates:
        return None
    if order_id is None and expected_position_id is None:
        return {"deals": []}
    relevant: list[Any] = []
    for deal in candidates:
        if order_id is not None:
            observed_order = _field(deal, "orderId", "order_id", default=None)
            if observed_order is None or str(observed_order) != str(order_id):
                continue
        if expected_position_id is not None:
            observed_position = _field(deal, "positionId", "position_id", default=None)
            if observed_position is None or str(observed_position) != str(expected_position_id):
                continue
        relevant.append(deal)
    filtered: dict[str, Any] = {"deals": relevant}
    account_id = _field(response, "ctidTraderAccountId", "ctid_trader_account_id", default=None)
    if account_id is not None:
        filtered["ctidTraderAccountId"] = account_id
    return filtered


def _merge_response_details(primary: Any, secondary: Any) -> Any:
    """Combine deal containers without mutating generated protobuf messages."""

    if primary is None:
        return secondary
    if secondary is None:
        return primary
    if isinstance(primary, Mapping):
        merged = dict(primary)
        deals = _many(secondary, "deal", "deals")
        if deals:
            merged["deals"] = [*_many(primary, "deal", "deals"), *deals]
        return merged
    # A generated message is immutable from this adapter's perspective. Build
    # a read-only mapping facade so a deal-list page can be combined with an
    # order/detail message without mutating SDK-owned protobuf objects.
    deals = _many(secondary, "deal", "deals")
    if deals:
        order = _field(primary, "order", "order_data", default=None) or primary
        combined: dict[str, Any] = {"order": order, "deals": deals}
        account_id = _field(primary, "ctidTraderAccountId", "ctid_trader_account_id", default=None)
        if account_id is not None:
            combined["ctidTraderAccountId"] = account_id
        return combined
    return primary


__all__ = [
    "AuthenticatedSession",
    "CTraderClientGateway",
    "CTraderDemoTransport",
    "CTraderDemoTransportConfig",
    "CTraderOfficialDemoTransport",
    "CTraderRequestMessageClient",
    "DEMO_PROTOBUF_ENDPOINT",
    "LIVE_PROTOBUF_ENDPOINT",
    "OfficialAdapterError",
    "OfficialCancelledError",
    "OfficialCorrelationError",
    "OfficialGateway",
    "OfficialGatewayConfigurationError",
    "OfficialMessageError",
    "OfficialProtocolError",
    "OfficialResponseError",
    "OfficialSDKUnavailable",
    "OfficialSendUncertainError",
    "OfficialTimeoutError",
    "OfficialTransportError",
    "OfficialUncertainSendError",
    "OfficialValidationError",
    "PAYLOAD_TYPES",
    "SendPhase",
    "ServerAccountObservation",
    "VOLUME_SCALE",
    "load_official_proto",
    "_field",
    "_many",
    "_volume_from_protocol",
    "_volume_to_protocol",
]
