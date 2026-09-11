"""Official cTrader Open API Protobuf transport, DEMO-only and injected.

This adapter is deliberately separate from ``ctrader_executor.py``.  It does
not open sockets, perform OAuth, or read credentials.  A caller injects an
already-created official SDK client/gateway and the official generated
Protobuf module.  Tests inject a local fake with the same official message
names.

Official protocol facts used here:

* Protobuf demo endpoint: ``demo.ctraderapi.com:5035``.
* ``ProtoOANewOrderReq`` fields include ``ctidTraderAccountId``, ``symbolId``,
  ``orderType``, ``tradeSide``, ``volume`` (0.01 units), and ``clientOrderId``.
* ``ProtoOAClosePositionReq`` uses account id, position id, and volume.
* ``ProtoOAExecutionEvent`` carries an order, deal, position and an
  ``executionType`` such as ``ORDER_ACCEPTED``, ``ORDER_FILLED``,
  ``ORDER_PARTIAL_FILL`` or ``ORDER_REJECTED``.
* ``ProtoOAReconcileReq/Res`` is the read-only correlation/reconciliation path;
  it is never a reason to submit the same order again.

The adapter returns the normalized records expected by the existing demo
executor's ``ExecutionTransport`` protocol.  In real deployments the current
executor remains the safety gate and an operator must still select and verify a
DEMO account and activate it explicitly.
"""

from __future__ import annotations

import dataclasses
import importlib
import inspect
import math
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, Callable, Protocol

from .ctrader_executor import (
    DemoAccount,
    DemoAccountRequired,
    ExecutionIntent,
    EndpointRejected,
    Fill,
    OrderSnapshot,
    OrderState,
    Position,
    RealAccountForbidden,
    ScopeRejected,
    Side,
    verify_demo_account,
)


DEMO_PROTOBUF_ENDPOINT = "demo.ctraderapi.com:5035"
LIVE_PROTOBUF_ENDPOINT = "live.ctraderapi.com:5035"
VOLUME_SCALE = 100  # official protocol: 1000 means 10.00 units


def _parse_time(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        dt = datetime.fromtimestamp(float(value), UTC)
    else:
        text = str(value).strip()
        if text.endswith("Z"): text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError("timestamp must include timezone")
    return dt.astimezone(UTC)


class OfficialTransportError(TimeoutError):
    pass


class OfficialSDKUnavailable(OfficialTransportError):
    pass


class OfficialCorrelationError(OfficialTransportError):
    pass


class OfficialMessageError(OfficialTransportError):
    pass


class OfficialGateway(Protocol):
    """Injected sync gateway used by the adapter and local fake tests."""

    def send(self, message: Any, **kwargs: Any) -> Any: ...


@dataclasses.dataclass(frozen=True, slots=True)
class ServerAccountObservation:
    """Explicit server-produced DEMO evidence used by the external route."""

    account_id: str
    environment: str
    endpoint: str
    scopes: frozenset[str]
    observed_at: datetime
    source: str = "ctrader-open-api"

    def __post_init__(self) -> None:
        object.__setattr__(self, "account_id", str(self.account_id).strip())
        object.__setattr__(self, "environment", str(self.environment).strip().upper())
        object.__setattr__(self, "endpoint", str(self.endpoint).strip())
        raw_scopes = self.scopes
        if isinstance(raw_scopes, str):
            scopes = frozenset(item.strip().lower() for item in raw_scopes.replace(",", " ").split() if item.strip())
        else:
            scopes = frozenset(str(item).strip().lower() for item in raw_scopes)
        object.__setattr__(self, "scopes", scopes)
        if not self.account_id or not self.account_id.isdigit() or int(self.account_id) <= 0 or self.environment != "DEMO":
            raise DemoAccountRequired("la evidencia del servidor debe identificar una cuenta DEMO numérica")
        if not _is_demo_endpoint(self.endpoint):
            raise EndpointRejected("la evidencia del servidor debe usar el endpoint DEMO")
        if "trading" not in self.scopes and "scope_trade" not in self.scopes and "trade" not in self.scopes:
            raise ScopeRejected("la evidencia del servidor no observa scope trading")
        object.__setattr__(self, "observed_at", _parse_time(self.observed_at))

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ServerAccountObservation":
        if not isinstance(value, Mapping) or value.get("server_observed") is not True:
            raise DemoAccountRequired("verified=true local no sustituye evidencia server_observed=True")
        return cls(
            account_id=str(value.get("account_id", value.get("id", ""))),
            environment=str(value.get("environment", "")),
            endpoint=str(value.get("endpoint", DEMO_PROTOBUF_ENDPOINT)),
            scopes=value.get("scopes", value.get("permissions", ())),
            observed_at=value.get("observed_at", datetime.now(UTC)),
            source=str(value.get("source", "ctrader-open-api")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "account_id": self.account_id,
            "environment": self.environment,
            "endpoint": self.endpoint,
            "scopes": sorted(self.scopes),
            "observed_at": self.observed_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "source": self.source,
            "server_observed": True,
        }


@dataclasses.dataclass(frozen=True, slots=True)
class CTraderDemoTransportConfig:
    endpoint: str = DEMO_PROTOBUF_ENDPOINT
    required_scopes: frozenset[str] = frozenset({"trading"})
    timeout_seconds: float = 5.0
    volume_scale: int = VOLUME_SCALE

    def __post_init__(self) -> None:
        if not _is_demo_endpoint(self.endpoint):
            raise EndpointRejected("official cTrader transport only accepts demo endpoint")
        if self.timeout_seconds <= 0 or not math.isfinite(float(self.timeout_seconds)):
            raise ValueError("timeout_seconds must be positive and finite")
        if int(self.volume_scale) <= 0:
            raise ValueError("volume_scale must be positive")
        required = frozenset(str(x).lower() for x in self.required_scopes)
        if not ({"trading", "trade", "scope_trade"} & required):
            raise ScopeRejected("official DEMO transport requires scope trading")
        object.__setattr__(self, "required_scopes", required)
        object.__setattr__(self, "volume_scale", int(self.volume_scale))


class CTraderDemoTransport:
    """Injected official-message transport for a selected DEMO account.

    ``client`` may be the Spotware Python SDK client or a small local fake.  It
    must expose ``send(message, ...)``; the adapter never calls ``connect`` or
    any OAuth endpoint.  ``proto`` is the generated module containing the
    official ``ProtoOA...`` classes.  If omitted, ``load_official_proto``
    imports the installed SDK lazily and fails with ``OfficialSDKUnavailable``
    when it is not installed.
    """

    def __init__(self, account: DemoAccount | Mapping[str, Any], *, client: OfficialGateway, proto: Any | None = None, symbol_ids: Mapping[str, int] | None = None, symbol_names: Mapping[int, str] | None = None, config: CTraderDemoTransportConfig | None = None, clock: Callable[[], datetime] | None = None, server_observation: ServerAccountObservation | Mapping[str, Any] | None = None):
        raw_environment = str(account.get("environment", account.get("mode", "")) if isinstance(account, Mapping) else getattr(account, "environment", "")).strip().upper()
        if "REAL" in raw_environment or "LIVE" in raw_environment or raw_environment in {"PRODUCTION", "PAPER_LIVE"}:
            raise RealAccountForbidden("official transport is DEMO-only; REAL/LIVE is rejected")
        self.account = account if isinstance(account, DemoAccount) else DemoAccount.from_mapping(account)
        self._account_endpoint = self.account.endpoint
        self.config = config or CTraderDemoTransportConfig(endpoint=DEMO_PROTOBUF_ENDPOINT)
        if str(self.config.endpoint).strip().lower() != DEMO_PROTOBUF_ENDPOINT:
            raise EndpointRejected("official cTrader transport requires demo.ctraderapi.com:5035")
        if self._account_endpoint.strip().lower() != DEMO_PROTOBUF_ENDPOINT:
            raise EndpointRejected("selected account endpoint is not the official DEMO endpoint")
        if isinstance(server_observation, Mapping):
            server_observation = ServerAccountObservation.from_mapping(server_observation)
        if not isinstance(server_observation, ServerAccountObservation):
            raise DemoAccountRequired(
                "el transporte oficial requiere ServerAccountObservation; "
                "verified=true caller-provided no es suficiente"
            )
        if server_observation.account_id != self.account.account_id:
            raise OfficialCorrelationError("server observation account id mismatch")
        if server_observation.environment != "DEMO":
            raise RealAccountForbidden("server observation is not DEMO")
        if server_observation.endpoint.strip().lower() != DEMO_PROTOBUF_ENDPOINT:
            raise EndpointRejected("server observation endpoint does not match official DEMO endpoint")
        self.server_observation = server_observation
        # Official OAuth responses may expose SCOPE_TRADE; normalize that
        # permission to the executor's trading gate without changing the raw
        # account descriptor passed by the caller.
        normalized_scopes = set(self.account.scopes) | set(self.server_observation.scopes)
        if "scope_trade" in normalized_scopes or "trade" in normalized_scopes:
            normalized_scopes.add("trading")
        if normalized_scopes != set(self.account.scopes):
            self.account = dataclasses.replace(self.account, scopes=frozenset(normalized_scopes))
        if not self.account.verified:
            self.account = dataclasses.replace(self.account, verified=True)
        # This check intentionally happens before proto/client method use.
        self.account = verify_demo_account(self.account, required_scopes=self.config.required_scopes, endpoint=self.config.endpoint)
        self.endpoint = self.config.endpoint
        self.account_id = self.account.account_id
        self.scopes = self.account.scopes
        self.client = client
        self.proto = proto if proto is not None else load_official_proto()
        self.symbol_ids = {str(key).upper(): int(value) for key, value in (symbol_ids or {}).items()}
        self.symbol_names = {int(key): str(value).upper() for key, value in (symbol_names or {}).items()}
        self.clock = clock or (lambda: datetime.now(UTC))
        self._snapshots: dict[str, OrderSnapshot] = {}
        self._requested_quantities: dict[str, float] = {}
        self._position_clients: dict[str, str] = {}

    def verify_endpoint(self, endpoint: str) -> bool:
        text = str(endpoint)
        return _is_demo_endpoint(text) and text in {self.endpoint, self._account_endpoint}

    def available_scopes(self) -> frozenset[str]:
        return self.scopes

    def build_application_auth(self, client_id: str, client_secret: str) -> Any:
        """Build, but never send, the official application auth message."""
        request = self._message("ProtoOAApplicationAuthReq")
        _set(request, "clientId", str(client_id)); _set(request, "clientSecret", str(client_secret))
        return request

    def build_account_auth(self, access_token: str) -> Any:
        """Build, but never send, the official account auth message."""
        request = self._message("ProtoOAAccountAuthReq")
        _set(request, "ctidTraderAccountId", int(self.account_id)); _set(request, "accessToken", str(access_token))
        return request

    def build_new_order(self, intent: ExecutionIntent) -> Any:
        symbol_id = self.symbol_ids.get(intent.symbol.upper())
        if symbol_id is None:
            raise OfficialMessageError(f"symbol id not configured for DEMO transport: {intent.symbol}")
        request = self._message("ProtoOANewOrderReq")
        _set(request, "ctidTraderAccountId", int(self.account_id))
        _set(request, "symbolId", int(symbol_id))
        # OpenApiPy exposes enum descriptors through the field descriptor,
        # rather than as module-level classes. The injected fixture exposes
        # the latter, so _enum supports both shapes and returns the numeric
        # value expected by generated protobuf messages.
        _set(request, "orderType", self._enum("ProtoOAOrderType", "MARKET", message=request, field_name="orderType"))
        _set(request, "tradeSide", self._enum("ProtoOATradeSide", intent.side.value, message=request, field_name="tradeSide"))
        _set(request, "volume", _volume_to_protocol(intent.quantity, self.config.volume_scale))
        _set(request, "clientOrderId", intent.intent_id)
        _set(request, "label", intent.intent_id)
        return request

    def build_close_position(self, position: Position, *, client_order_id: str) -> Any:
        request = self._message("ProtoOAClosePositionReq")
        _set(request, "ctidTraderAccountId", int(self.account_id))
        _set(request, "positionId", _int_id(position.position_id, "position_id"))
        _set(request, "volume", _volume_to_protocol(position.quantity, self.config.volume_scale))
        # ClosePositionReq has no clientOrderId field in the official schema;
        # the gateway metadata still carries the correlation id.
        return request

    def build_reconcile(self) -> Any:
        request = self._message("ProtoOAReconcileReq")
        _set(request, "ctidTraderAccountId", int(self.account_id))
        # Older generated schemas (including ctrader-open-api 0.9.2) do not
        # define returnProtectionOrders. Preserve it for simple injected
        # fixtures, but never assign an unknown protobuf field.
        descriptor = getattr(request, "DESCRIPTOR", None)
        fields_by_name = getattr(descriptor, "fields_by_name", None)
        if descriptor is None or (fields_by_name is not None and "returnProtectionOrders" in fields_by_name):
            _set(request, "returnProtectionOrders", False)
        return request

    def submit(self, intent: ExecutionIntent, *, timeout_seconds: float) -> OrderSnapshot:
        self._requested_quantities[intent.intent_id] = intent.quantity
        message = self.build_new_order(intent)
        response = self._send(message, client_msg_id=intent.intent_id, timeout_seconds=timeout_seconds)
        return self._snapshot_from_response(response, client_order_id=intent.intent_id, requested_quantity=intent.quantity, closing=False)

    def get_order(self, client_order_id: str) -> OrderSnapshot | None:
        response = self._send(self.build_reconcile(), client_msg_id=f"reconcile:{client_order_id}", timeout_seconds=self.config.timeout_seconds)
        if response is None: return None
        orders = _many(response, "order", "orders")
        for order in orders:
            candidate = _field(order, "clientOrderId", "client_order_id", "label")
            if candidate is not None and str(candidate) == str(client_order_id):
                return self._snapshot_from_response(order, client_order_id=client_order_id, requested_quantity=self._requested_quantity(client_order_id, order), closing=False)
        # Some injected gateways return the matching execution event directly.
        if _field(response, "executionType", "execution_type") is not None:
            return self._snapshot_from_response(response, client_order_id=client_order_id, requested_quantity=self._requested_quantity(client_order_id, response), closing=False)
        return self._snapshots.get(str(client_order_id))

    def list_positions(self, account_id: str) -> Sequence[Position]:
        if str(account_id) != self.account_id: return ()
        response = self._send(self.build_reconcile(), client_msg_id="reconcile:positions", timeout_seconds=self.config.timeout_seconds)
        positions = []
        for raw in _many(response, "position", "positions"):
            position = self._position_from_proto(raw)
            if position is not None: positions.append(position)
        return tuple(positions)

    def close_position(self, position: Position, *, client_order_id: str, timeout_seconds: float) -> OrderSnapshot:
        self._requested_quantities[client_order_id] = position.quantity
        response = self._send(self.build_close_position(position, client_order_id=client_order_id), client_msg_id=client_order_id, timeout_seconds=timeout_seconds)
        return self._snapshot_from_response(response, client_order_id=client_order_id, requested_quantity=position.quantity, closing=True)

    def _message(self, name: str) -> Any:
        factory = getattr(self.proto, name, None)
        if factory is None and isinstance(self.proto, Mapping): factory = self.proto.get(name)
        if factory is None: raise OfficialSDKUnavailable(f"injected official protobuf module lacks {name}")
        return factory() if callable(factory) else factory

    def _enum(self, enum_name: str, member: str, *, message: Any | None = None, field_name: str | None = None) -> Any:
        enum_type = getattr(self.proto, enum_name, None)
        if enum_type is None and isinstance(self.proto, Mapping): enum_type = self.proto.get(enum_name)
        if enum_type is not None:
            value = getattr(enum_type, "Value", None)
            if callable(value):
                try: return value(member)
                except Exception: pass
            candidate = getattr(enum_type, member, None)
            if candidate is not None:
                return candidate
        # Generated protobuf Python classes keep enum descriptors on the
        # field, e.g. ProtoOANewOrderReq.DESCRIPTOR.fields_by_name[...].
        if message is not None and field_name:
            descriptor = getattr(message, "DESCRIPTOR", None)
            fields_by_name = getattr(descriptor, "fields_by_name", None)
            field = fields_by_name.get(field_name) if fields_by_name is not None else None
            enum_descriptor = getattr(field, "enum_type", None)
            if enum_descriptor is not None:
                for item in getattr(enum_descriptor, "values", ()):
                    if str(getattr(item, "name", "")).upper() == str(member).upper():
                        return int(item.number)
        # Keep simple injected messages easy to inspect in tests; generated
        # protobuf paths above always return a numeric enum value.
        return member

    def _send(self, message: Any, *, client_msg_id: str, timeout_seconds: float) -> Any:
        sender = getattr(self.client, "request", None) or getattr(self.client, "send", None)
        if not callable(sender):
            raise OfficialTransportError("injected SDK client must expose request or send")
        kwargs: dict[str, Any] = {}
        try:
            signature = inspect.signature(sender)
            parameters = signature.parameters
            accepts_kwargs = any(
                parameter.kind is inspect.Parameter.VAR_KEYWORD
                for parameter in parameters.values()
            )
            if accepts_kwargs or "client_msg_id" in parameters:
                kwargs["client_msg_id"] = client_msg_id
            elif "clientMsgId" in parameters:
                kwargs["clientMsgId"] = client_msg_id
            if accepts_kwargs or "timeout_seconds" in parameters:
                kwargs["timeout_seconds"] = timeout_seconds
            elif "timeout" in parameters:
                kwargs["timeout"] = timeout_seconds
        except (TypeError, ValueError):
            # C-extension gateways may not expose a signature. One call with
            # no optional kwargs is safer than retrying a possibly sent order.
            kwargs = {}
        try:
            result = sender(message, **kwargs)
            if hasattr(result, "result") and callable(result.result):
                result = result.result(timeout=timeout_seconds)
            if hasattr(result, "addCallback") or hasattr(result, "__await__"):
                # The executor is synchronous; an unresolved Twisted
                # Deferred/coroutine is an unknown acknowledgement, not a
                # reason to issue another order.
                raise TimeoutError("injected official SDK returned asynchronous response without sync gateway")
            if hasattr(result, "value") and not _is_message(result):
                result = result.value
            return result
        except TimeoutError:
            raise
        except Exception as exc:
            raise OfficialTransportError(
                "falló el envío al gateway SDK; no se reintentó la orden"
            ) from exc

    def _requested_quantity(self, client_order_id: str, response: Any) -> float:
        known = self._requested_quantities.get(str(client_order_id))
        if known is not None: return known
        cached = self._snapshots.get(str(client_order_id))
        if cached is not None: return cached.requested_quantity
        order = _field(response, "order", "order_data") or response
        raw = _field(order, "volume", "requestedVolume", "requested_volume", default=0)
        return _volume_from_protocol(raw, self.config.volume_scale) if raw else 1.0

    def _snapshot_from_response(self, response: Any, *, client_order_id: str, requested_quantity: float, closing: bool) -> OrderSnapshot:
        if response is None: raise TimeoutError("official cTrader transport returned no response")
        account_id = _field(response, "ctidTraderAccountId", "ctid_trader_account_id")
        if account_id is not None and str(account_id) != self.account_id: raise OfficialCorrelationError("official response account id mismatch")
        response_client = _field(response, "clientOrderId", "client_order_id", "clientMsgId", "client_msg_id")
        order = _field(response, "order", "order_data") or response
        order_client = _field(order, "clientOrderId", "client_order_id", "label")
        if response_client is not None and str(response_client) != client_order_id: raise OfficialCorrelationError("official response client correlation mismatch")
        if order_client is not None and str(order_client) != client_order_id: raise OfficialCorrelationError("official order client correlation mismatch")
        error = _field(response, "errorCode", "error_code", "description", default=None)
        execution = _enum_field_name(response, "executionType", "execution_type")
        order_status = _enum_field_name(order, "orderStatus", "order_status")
        deal = _field(response, "deal")
        deal_status = _enum_field_name(deal, "dealStatus", "deal_status") if deal is not None else ""
        requested = float(requested_quantity)
        fills = self._fills_from(response, deal, requested)
        filled = sum(fill.quantity for fill in fills)
        raw_executed = _field(order, "executedVolume", "executed_volume", default=None)
        if raw_executed is not None: filled = max(filled, min(requested, _volume_from_protocol(raw_executed, self.config.volume_scale)))
        if error or "REJECT" in execution or "REJECT" in order_status or "REJECT" in deal_status or any(token in deal_status for token in ("ERROR", "MISSED", "INTERNALLY_REJECTED")):
            state = OrderState.REJECTED
        elif closing:
            if filled >= requested - 1e-12 or "FILLED" in execution or "FILLED" in order_status or deal_status == "FILLED": state = OrderState.CLOSED
            elif filled > 0 or "PARTIAL" in execution or "PARTIAL" in deal_status: state = OrderState.CLOSE_PARTIAL
            else: state = OrderState.SUBMITTED
        elif filled >= requested - 1e-12 or "FILLED" in execution or "FILLED" in order_status or deal_status == "FILLED":
            if filled <= 0: filled = requested
            state = OrderState.FILLED
        elif filled > 0 or "PARTIAL" in execution or "PARTIAL" in deal_status:
            state = OrderState.PARTIAL
        else:
            state = OrderState.SUBMITTED
        order_id = _field(order, "orderId", "order_id") or _field(deal, "orderId", "order_id") if deal is not None else _field(order, "orderId", "order_id")
        position_ids: list[str] = []
        position_raw = _field(response, "position")
        if position_raw is not None:
            pid = _field(position_raw, "positionId", "position_id")
            if pid is not None: position_ids.append(str(pid))
        pid_deal = _field(deal, "positionId", "position_id") if deal is not None else None
        if pid_deal is not None and str(pid_deal) not in position_ids: position_ids.append(str(pid_deal))
        snapshot = OrderSnapshot(str(order_id) if order_id is not None else None, client_order_id, state, requested, min(requested, filled), tuple(fills), str(error) if error else None, tuple(position_ids), _parse_time(self.clock()))
        self._snapshots[client_order_id] = snapshot
        for pid in position_ids: self._position_clients[pid] = client_order_id
        return snapshot

    def _fills_from(self, response: Any, deal: Any, requested: float) -> tuple[Fill, ...]:
        candidates = _many(response, "deal", "deals")
        if deal is not None and deal not in candidates: candidates.append(deal)
        fills: list[Fill] = []
        remaining = float(requested)
        for index, raw in enumerate(candidates):
            volume = _field(raw, "filledVolume", "filled_volume", "volume", default=0)
            quantity = min(remaining, _volume_from_protocol(volume, self.config.volume_scale))
            price = float(_field(raw, "executionPrice", "execution_price", default=0) or 0)
            if quantity <= 0 or price <= 0: continue
            stamp_raw = _field(raw, "executionTimestamp", "execution_timestamp", "utcLastUpdateTimestamp", default=None)
            timestamp = _ms_time(stamp_raw) if stamp_raw is not None else _parse_time(self.clock())
            fill_id = str(_field(raw, "dealId", "deal_id", default=f"deal-{index}"))
            fills.append(Fill(fill_id, quantity, price, timestamp))
            remaining = max(0.0, remaining - quantity)
            if remaining <= 1e-12:
                break
        return tuple(fills)

    def _position_from_proto(self, raw: Any) -> Position | None:
        pid = _field(raw, "positionId", "position_id")
        if pid is None: return None
        trade = _field(raw, "tradeData", "trade_data") or raw
        symbol_id = _field(trade, "symbolId", "symbol_id", default=_field(raw, "symbolId", "symbol_id", default=0))
        symbol = self.symbol_names.get(int(symbol_id), str(symbol_id))
        volume = _volume_from_protocol(_field(trade, "volume", default=_field(raw, "volume", default=0)), self.config.volume_scale)
        price = float(_field(raw, "price", "entryPrice", "entry_price", default=0) or 0)
        side_raw = _field(
            trade,
            "tradeSide",
            "trade_side",
            default=_field(raw, "tradeSide", "trade_side", default=None),
        )
        if volume <= 0 or price <= 0 or side_raw is None:
            return None
        side = _trade_side(side_raw, owner=trade, field_name="tradeSide")
        client_id = self._position_clients.get(str(pid)) or _field(trade, "label", "clientOrderId", "client_order_id", default=None)
        return Position(str(pid), self.account_id, symbol, side, volume, price, str(client_id) if client_id else None, owner="mtf-lab")


class CTraderOfficialDemoTransport(CTraderDemoTransport):
    """Descriptive alias emphasizing official Protobuf message semantics."""


def load_official_proto() -> Any:
    try:
        return importlib.import_module("ctrader_open_api.messages.OpenApiMessages_pb2")
    except Exception as exc:
        raise OfficialSDKUnavailable("install/inject Spotware OpenApiPy generated protobuf module") from exc


def _is_demo_endpoint(endpoint: str) -> bool:
    text = str(endpoint).strip().lower()
    if any(token in text for token in ("live", "real", "production")): return False
    return "demo.ctraderapi.com" in text or text.startswith("demo:") or "sandbox" in text


def _set(message: Any, name: str, value: Any) -> None:
    try: setattr(message, name, value)
    except Exception as exc: raise OfficialMessageError(f"cannot set official field {name}") from exc


def _field(value: Any, *names: str, default: Any = None) -> Any:
    if value is None: return default
    if isinstance(value, Mapping):
        for name in names:
            if name in value: return value[name]
    for name in names:
        if hasattr(value, name): return getattr(value, name)
    return default


def _many(value: Any, *names: str) -> list[Any]:
    if value is None: return []
    for name in names:
        raw = _field(value, name, default=None)
        if raw is None: continue
        if isinstance(raw, (list, tuple)): return list(raw)
        # protobuf repeated containers are iterable but not scalar messages.
        if hasattr(raw, "__iter__") and not isinstance(raw, (str, bytes, Mapping)):
            try: return list(raw)
            except TypeError: pass
        return [raw]
    return []


def _name(value: Any) -> str:
    if value is None: return ""
    if isinstance(value, str): return value.upper()
    return str(getattr(value, "name", getattr(value, "value", value))).upper()


def _enum_field_name(owner: Any, *field_names: str) -> str:
    """Resolve generated-protobuf enum integers to their symbolic names."""

    value = _field(owner, *field_names, default=None)
    if value is None:
        return ""
    if isinstance(value, str):
        return value.upper()
    descriptor = getattr(owner, "DESCRIPTOR", None)
    fields_by_name = getattr(descriptor, "fields_by_name", None)
    for field_name in field_names:
        field = fields_by_name.get(field_name) if fields_by_name is not None else None
        enum_descriptor = getattr(field, "enum_type", None)
        if enum_descriptor is not None:
            try:
                number = int(value)
            except (TypeError, ValueError):
                break
            for item in getattr(enum_descriptor, "values", ()):
                if int(item.number) == number:
                    return str(item.name).upper()
            break
    return _name(value)


def _trade_side(value: Any, *, owner: Any | None = None, field_name: str = "tradeSide") -> Side:
    name = _enum_field_name(owner, field_name) if owner is not None else _name(value)
    if owner is None and not isinstance(value, str):
        name = _name(value)
    if name in {"BUY", "1", "PROTOOA_TRADESIDE_BUY"}: return Side.BUY
    if name in {"SELL", "2", "PROTOOA_TRADESIDE_SELL"}: return Side.SELL
    return Side.parse(name)


def _volume_to_protocol(quantity: float, scale: int) -> int:
    result = int(round(float(quantity) * scale))
    if result <= 0: raise OfficialMessageError("volume rounds to zero in official protocol units")
    return result


def _volume_from_protocol(value: Any, scale: int) -> float:
    try: return float(value) / float(scale)
    except (TypeError, ValueError): return 0.0


def _ms_time(value: Any) -> datetime:
    return datetime.fromtimestamp(float(value) / 1000.0, UTC)


def _int_id(value: Any, name: str) -> int:
    try: return int(value)
    except (TypeError, ValueError) as exc: raise OfficialMessageError(f"{name} must be an integer cTrader id") from exc


def _is_message(value: Any) -> bool:
    return value is not None and (hasattr(value, "DESCRIPTOR") or value.__class__.__name__.startswith("ProtoOA"))


__all__ = [
    "CTraderDemoTransport", "CTraderOfficialDemoTransport", "CTraderDemoTransportConfig", "ServerAccountObservation", "DEMO_PROTOBUF_ENDPOINT", "LIVE_PROTOBUF_ENDPOINT", "OfficialCorrelationError", "OfficialGateway", "OfficialMessageError", "OfficialSDKUnavailable", "OfficialTransportError", "VOLUME_SCALE", "load_official_proto",
]
