"""Read-only account risk observations for an authenticated cTrader client.

This module is deliberately narrower than the DEMO execution adapter.  It
does not create a client, connect, authenticate, read credentials, submit an
order, or use the local execution fixture.  A caller supplies the existing
``CTraderProvider`` and the observer uses exactly ``provider.client`` for the
typed Open API requests.

The account and position messages use the official Protobuf 91 descriptors
bundled with MTF Lab.  Monetary integers are converted only when the server
also supplies ``moneyDigits``.  Missing commissions/conversion fees, missing
position margin, incomplete deal pages, a changed connection generation, or
stale response evidence leave the affected metric unknown rather than
assuming zero or manufacturing an infinite margin ratio.
"""

from __future__ import annotations

import contextlib
import dataclasses
import json
import math
import os
import stat
import tempfile
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

from ..core.numeric import decimal_context
from ..data.ctrader_protocol import (
    WireMessage,
    enum_name,
    read_field,
    read_repeated,
)
from .ctrader_demo_transport import load_official_proto

# These values are fixed by the official Open API schema 91.  Keeping them
# local avoids expanding the execution adapter's request allowlist merely to
# read account state.
TRADER_REQ = 2121
TRADER_RES = 2122
RECONCILE_REQ = 2124
RECONCILE_RES = 2125
DEAL_LIST_REQ = 2133
DEAL_LIST_RES = 2134
UNREALIZED_PNL_REQ = 2187
UNREALIZED_PNL_RES = 2188

DEFAULT_MAX_AGE_SECONDS = 10.0
DEFAULT_MAX_PAGES = 16
DEFAULT_MAX_ROWS = 1000
MAX_MONEY_DIGITS = 38


class AccountRiskObservationError(RuntimeError):
    """The authenticated read-only account boundary cannot be proven."""


class AccountRiskProtocolError(AccountRiskObservationError):
    """A generated response is missing identity or required evidence."""


_RISK_STATE_VERSION = 1
_RISK_STATE_SCOPE = "OBSERVED_SINCE_ARM"
_RISK_STATE_MAX_BYTES = 8192
_RISK_JOURNAL_MAX_BYTES = _RISK_STATE_MAX_BYTES * 128
_RISK_LIFECYCLE_EVENTS = frozenset({"ACTIVATED", "DEACTIVATED", "PAUSED", "RESUMED"})


@dataclasses.dataclass(frozen=True, slots=True)
class _SessionIdentity:
    account_id: str
    session_id: str
    generation: str
    environment: str
    endpoint: str


@dataclasses.dataclass(frozen=True, slots=True)
class _HighWaterState:
    account_id: str
    environment: str
    endpoint: str
    peak_equity: Decimal
    armed_at: datetime
    updated_at: datetime


class _HighWaterStore:
    """Small private state file for the observed-since-arm equity high-water."""

    def __init__(
        self,
        state_path: str | Path | None,
        journal_path: str | Path | None,
        *,
        allow_new_baseline: bool,
    ) -> None:
        self.path = _private_state_path(state_path) if state_path is not None else None
        self.journal_path = Path(journal_path).expanduser() if journal_path is not None else None
        if self.journal_path is not None:
            _validate_private_path(self.journal_path, allow_missing=True)
        self.allow_new_baseline = bool(allow_new_baseline)
        self._identity_key: tuple[str, str, str] | None = None
        self._state: _HighWaterState | None = None
        self._status = "UNAVAILABLE" if self.path is None else "UNARMED"
        self._reason: str | None = None
        self._rearm_requested = False

    @property
    def available(self) -> bool:
        return self.path is not None

    def observe(
        self,
        identity: _SessionIdentity,
        equity: Decimal | None,
        observed_at: datetime | None,
        *,
        data_ready: bool,
    ) -> dict[str, Any]:
        self._ensure(identity)
        peak = self._state.peak_equity if self._state is not None else None
        if not data_ready or equity is None or observed_at is None:
            return self._result(None, peak)
        if self._status not in {"UNARMED", "READY"}:
            return self._result(None, None)
        try:
            with decimal_context():
                if self._state is None:
                    state = _HighWaterState(
                        identity.account_id,
                        identity.environment,
                        identity.endpoint,
                        equity,
                        observed_at,
                        observed_at,
                    )
                elif equity > self._state.peak_equity:
                    state = dataclasses.replace(
                        self._state,
                        peak_equity=equity,
                        updated_at=observed_at,
                    )
                else:
                    state = self._state
                if self.path is not None and state != self._state:
                    self._write(state)
                self._state = state
                self._status = "READY"
                self._reason = None
                drawdown = max(Decimal("0"), state.peak_equity - equity)
        except Exception as exc:
            self._status = "INVALID"
            self._reason = f"risk_state_persist_failed:{type(exc).__name__}"
            return self._result(None, None)
        return self._result(drawdown, state.peak_equity)

    def rearm_after_human(self, *, confirm: bool = False) -> dict[str, Any]:
        """Arm a new baseline only after an explicit human confirmation."""

        if confirm is not True:
            raise AccountRiskObservationError("rearm_after_human requiere confirm=True")
        if self.path is None:
            raise AccountRiskObservationError("rearm_after_human requiere state_path persistente")
        empty, reason = self._journal_empty()
        if not empty:
            raise AccountRiskObservationError(reason or "no se puede rearmar con intents existentes")
        self._rearm_requested = True
        self._identity_key = None
        self._state = None
        self._status = "UNARMED"
        self._reason = "risk_state_rearm_after_human"
        return {"rearm_requested": True, "state": self._status, "reason": self._reason}

    def _ensure(self, identity: _SessionIdentity) -> None:
        key = (identity.account_id, identity.environment, identity.endpoint)
        if self._identity_key == key and not self._rearm_requested:
            return
        self._identity_key = key
        if self.path is None:
            self._status = "UNARMED"
            self._reason = None
            self._state = None
            return
        if self._rearm_requested:
            self._status = "UNARMED"
            self._reason = "risk_state_rearm_after_human"
            self._state = None
            self._rearm_requested = False
            return
        try:
            state = self._read()
        except FileNotFoundError:
            empty, reason = self._journal_empty()
            if not empty:
                self._status, self._reason, self._state = "INVALID", reason, None
            elif self.allow_new_baseline:
                self._status, self._reason, self._state = "UNARMED", None, None
            else:
                self._status, self._reason, self._state = "UNARMED", "risk_state_baseline_required", None
            return
        except AccountRiskObservationError as exc:
            # A present but unreadable/corrupt state is not a first run.
            # Even a lifecycle-only journal cannot authorize an implicit reset;
            # only the explicit human rearm path may do so.
            self._status, self._reason, self._state = "INVALID", str(exc), None
            return
        expected = (identity.account_id, identity.environment, identity.endpoint)
        actual = (state.account_id, state.environment, state.endpoint)
        if actual != expected:
            self._status = "INVALID"
            self._reason = "risk_state_identity_mismatch"
            self._state = None
            return
        self._state = state
        self._status = "READY"
        self._reason = None

    def _result(self, drawdown: Decimal | None, peak: Decimal | None) -> dict[str, Any]:
        return {
            "drawdown": drawdown,
            "high_water_equity": peak,
            "state": self._status,
            "complete": self._status == "READY",
            "reason": self._reason,
        }

    def _journal_empty(self) -> tuple[bool, str | None]:
        if self.journal_path is None:
            return False, "risk_journal_unobserved"
        try:
            st = _private_lstat(self.journal_path, allow_missing=True)
        except AccountRiskObservationError as exc:
            return False, str(exc)
        if st is None:
            return False, "risk_journal_missing"
        try:
            _validate_private_file_stat(st)
        except AccountRiskObservationError as exc:
            return False, str(exc)
        if st.st_size > _RISK_JOURNAL_MAX_BYTES:
            return False, "risk_journal_too_large"
        if st.st_size == 0:
            return True, None
        try:
            raw = _read_private_bytes(self.journal_path, limit=_RISK_JOURNAL_MAX_BYTES)
        except AccountRiskObservationError:
            return False, "risk_journal_corrupt"
        return _journal_content_empty(raw)

    def _read(self) -> _HighWaterState:
        if self.path is None:
            raise FileNotFoundError
        raw = _read_private_bytes(self.path)
        value = _decode_risk_state(raw)
        account_id, environment, endpoint = _state_identity(value)
        try:
            peak = Decimal(str(value["peak_equity"]))
        except Exception as exc:
            raise AccountRiskObservationError("risk_state_peak_invalid") from exc
        if not peak.is_finite() or peak < 0:
            raise AccountRiskObservationError("risk_state_peak_invalid")
        armed_at = _utc(value["armed_at"], "risk_state.armed_at")
        updated_at = _utc(value["updated_at"], "risk_state.updated_at")
        if updated_at < armed_at:
            raise AccountRiskObservationError("risk_state_time_order_invalid")
        return _HighWaterState(account_id, environment, endpoint, peak, armed_at, updated_at)

    def _write(self, state: _HighWaterState) -> None:
        if self.path is None:
            return
        payload = {
            "version": _RISK_STATE_VERSION,
            "scope": _RISK_STATE_SCOPE,
            "identity": {
                "account_id": state.account_id,
                "environment": state.environment,
                "endpoint": state.endpoint,
            },
            "peak_equity": _text(state.peak_equity),
            "armed_at": _iso(state.armed_at),
            "updated_at": _iso(state.updated_at),
        }
        encoded = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
        if len(encoded) > _RISK_STATE_MAX_BYTES:
            raise AccountRiskObservationError("risk_state_too_large")
        parent = self.path.parent
        fd, temporary = tempfile.mkstemp(prefix=".account-risk-", dir=parent)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "wb", closefd=True) as stream:
                stream.write(encoded)
                stream.flush()
                os.fdatasync(stream.fileno())
            os.replace(temporary, self.path)
            _sync_directory(parent)
            _validate_private_file(self.path)
            temporary = ""
        finally:
            if temporary:
                with contextlib.suppress(FileNotFoundError):
                    os.unlink(temporary)


def _journal_content_empty(raw: bytes) -> tuple[bool, str | None]:
    try:
        records = [json.loads(line) for line in raw.decode("utf-8").splitlines() if line.strip()]
    except (UnicodeDecodeError, json.JSONDecodeError):
        return False, "risk_journal_corrupt"
    if not records:
        return True, None
    for record in records:
        if not isinstance(record, Mapping):
            return False, "risk_journal_corrupt"
        journal_type = str(record.get("journal_type", "")).strip().lower()
        event_type = str(record.get("event_type", "")).strip().upper()
        if journal_type in {"intent", "update"}:
            return False, "risk_journal_nonempty"
        if journal_type != "event" or event_type not in _RISK_LIFECYCLE_EVENTS:
            return False, "risk_journal_nonempty"
    return True, None


def _decode_risk_state(raw: bytes) -> Mapping[str, Any]:
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AccountRiskObservationError("risk_state_corrupt_json") from exc
    if not isinstance(value, Mapping):
        raise AccountRiskObservationError("risk_state_corrupt_shape")
    if set(value) != {"version", "scope", "identity", "peak_equity", "armed_at", "updated_at"}:
        raise AccountRiskObservationError("risk_state_corrupt_fields")
    if value.get("version") != _RISK_STATE_VERSION or value.get("scope") != _RISK_STATE_SCOPE:
        raise AccountRiskObservationError("risk_state_version_or_scope_invalid")
    identity = value.get("identity")
    if not isinstance(identity, Mapping) or set(identity) != {"account_id", "environment", "endpoint"}:
        raise AccountRiskObservationError("risk_state_identity_invalid")
    return value


def _state_identity(value: Mapping[str, Any]) -> tuple[str, str, str]:
    identity = value["identity"]
    assert isinstance(identity, Mapping)
    account_id = str(identity.get("account_id", "")).strip()
    environment = str(identity.get("environment", "")).strip().upper()
    endpoint = _normalise_endpoint(identity.get("endpoint"))
    if not account_id.isdigit() or int(account_id) <= 0 or environment != "DEMO" or endpoint is None:
        raise AccountRiskObservationError("risk_state_identity_invalid")
    return account_id, environment, endpoint


@dataclasses.dataclass(frozen=True, slots=True)
class _Response:
    payload: Any
    payload_type: int
    generation: str | None
    observed_at: datetime | None
    client_msg_id: str | None


@dataclasses.dataclass(frozen=True, slots=True)
class AccountRiskSnapshot(Mapping[str, Any]):
    """JSON-safe account risk projection with explicit evidence state.

    The numeric fields are strings when known so a caller can persist the
    projection without losing decimal spelling.  ``to_executor_kwargs``
    returns ``Decimal`` instances for the executor's exact parser.
    """

    account_id: str | None
    session_id: str | None
    connection_generation: str | None
    observed_at: datetime | None
    day_start: datetime | None
    day_end: datetime | None
    balance: str | None
    equity: str | None
    used_margin: str | None
    margin_level: str | None
    realized_daily_pnl: str | None
    realized_daily_gross_pnl: str | None
    realized_daily_costs: str | None
    unrealized_daily_pnl: str | None
    unrealized_gross_pnl: str | None
    daily_pnl: str | None
    positions: tuple[Mapping[str, Any], ...]
    freshness_state: str
    fresh: bool
    complete: bool
    margin_state: str
    reasons: tuple[str, ...] = ()
    pages: int = 0
    deals_complete: bool = False
    positions_complete: bool = False
    unrealized_complete: bool = False
    fees_complete: bool = False
    cache_invalidated: bool = False
    cache_invalidation_reason: str | None = None
    margin_calculation_type: str | None = None
    drawdown: str | None = None
    high_water_equity: str | None = None
    risk_state: str = "UNAVAILABLE"
    risk_state_complete: bool = False
    risk_state_reason: str | None = None

    @property
    def generation(self) -> str | None:
        return self.connection_generation

    @property
    def freshness(self) -> str:
        return self.freshness_state

    @property
    def completeness(self) -> bool:
        return self.complete

    def to_executor_kwargs(self) -> dict[str, Any]:
        """Return only exact current values; incomplete values become None."""

        def decimal_or_none(value: str | None) -> Decimal | None:
            if value is None:
                return None
            return Decimal(value)

        usable = self.fresh and self.complete
        return {
            "equity": decimal_or_none(self.equity) if usable else None,
            "realized_daily_pnl": decimal_or_none(self.realized_daily_pnl) if usable else None,
            "unrealized_daily_pnl": decimal_or_none(self.unrealized_daily_pnl) if usable else None,
            "daily_pnl": decimal_or_none(self.daily_pnl) if usable else None,
            "drawdown": decimal_or_none(self.drawdown) if usable else None,
            # A zero used margin is an observed state, but the mathematical
            # ratio is intentionally left unknown.  The executor may consume
            # used_margin plus margin_state in a later compatibility extension.
            "used_margin": decimal_or_none(self.used_margin) if usable else None,
            "margin_level": decimal_or_none(self.margin_level) if usable else None,
            "observed_at": self.observed_at,
            "connection_generation": self.connection_generation,
        }

    def to_dict(self) -> dict[str, Any]:
        """Return a detached, JSON-serializable projection."""

        return {
            "account_id": self.account_id,
            "session_id": self.session_id,
            "connection_generation": self.connection_generation,
            "generation": self.connection_generation,
            "observed_at": _iso(self.observed_at),
            "day_start": _iso(self.day_start),
            "day_end": _iso(self.day_end),
            "balance": self.balance,
            "equity": self.equity,
            "used_margin": self.used_margin,
            "margin_level": self.margin_level,
            "margin_state": self.margin_state,
            "margin_calculation_type": self.margin_calculation_type,
            "realized_daily_pnl": self.realized_daily_pnl,
            "realized_daily_gross_pnl": self.realized_daily_gross_pnl,
            "realized_daily_costs": self.realized_daily_costs,
            "unrealized_daily_pnl": self.unrealized_daily_pnl,
            "unrealized_pnl": self.unrealized_daily_pnl,
            "unrealized_gross_pnl": self.unrealized_gross_pnl,
            "daily_pnl": self.daily_pnl,
            "positions": [dict(item) for item in self.positions],
            "freshness": self.freshness_state,
            "freshness_state": self.freshness_state,
            "fresh": self.fresh,
            "complete": self.complete,
            "completeness": {
                "complete": self.complete,
                "positions": self.positions_complete,
                "unrealized_pnl": self.unrealized_complete,
                "deals": self.deals_complete,
                "fees": self.fees_complete,
                "risk_state": self.risk_state_complete,
                "reasons": list(self.reasons),
            },
            "pages": self.pages,
            "deals_complete": self.deals_complete,
            "positions_complete": self.positions_complete,
            "unrealized_complete": self.unrealized_complete,
            "fees_complete": self.fees_complete,
            "cache_invalidated": self.cache_invalidated,
            "cache_invalidation_reason": self.cache_invalidation_reason,
            "drawdown": self.drawdown,
            "high_water_equity": self.high_water_equity,
            "risk_state": self.risk_state,
            "risk_state_complete": self.risk_state_complete,
            "risk_state_reason": self.risk_state_reason,
            "reasons": list(self.reasons),
        }

    # A small mapping-like compatibility surface keeps composition code from
    # having to know whether it received a dataclass or a detached projection.
    def __getitem__(self, key: str) -> Any:
        return self.to_dict()[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.to_dict())

    def __len__(self) -> int:
        return len(self.to_dict())

    def get(self, key: str, default: Any = None) -> Any:
        return self.to_dict().get(key, default)


class AccountRiskObserver:
    """Read current account metrics through one existing authenticated client."""

    def __init__(
        self,
        provider: Any,
        *,
        proto: Any | None = None,
        clock: Callable[[], datetime] | None = None,
        max_age_seconds: float = DEFAULT_MAX_AGE_SECONDS,
        max_pages: int = DEFAULT_MAX_PAGES,
        max_rows: int = DEFAULT_MAX_ROWS,
        state_path: str | Path | None = None,
        journal_path: str | Path | None = None,
        allow_new_baseline: bool = False,
    ) -> None:
        client = getattr(provider, "client", None)
        if client is None or not callable(getattr(client, "request_message", None)):
            raise AccountRiskObservationError(
                "AccountRiskObserver requiere provider.client.request_message() del cliente existente"
            )
        self.provider = provider
        self.client = client
        self.proto = proto if proto is not None else load_official_proto()
        self.clock = clock or (lambda: datetime.now(UTC))
        self.max_age_seconds = _positive_float(max_age_seconds, "max_age_seconds")
        self.max_pages = _positive_int(max_pages, "max_pages")
        self.max_rows = _positive_int(max_rows, "max_rows")
        self._cache_key: tuple[str, str] | None = None
        self._cache_invalidated_reason: str | None = None
        self.last_observation: AccountRiskSnapshot | None = None
        self._request_counter = 0
        self._request_nonce = uuid.uuid4().hex[:12]
        self._high_water = _HighWaterStore(
            state_path,
            journal_path,
            allow_new_baseline=allow_new_baseline,
        )

    def observe(self, *, now: datetime | None = None) -> AccountRiskSnapshot:
        """Query trader, positions, unrealized PnL, and bounded daily deals."""

        current = _utc(now or self.clock(), "now")
        day_start = datetime(current.year, current.month, current.day, tzinfo=UTC)
        try:
            identity = self._session_identity()
        except Exception as exc:
            snapshot = self._invalid_snapshot(
                current,
                day_start,
                (f"session_unobserved:{type(exc).__name__}",),
            )
            self.last_observation = snapshot
            return snapshot

        cache_invalidated = False
        cache_reason = None
        cache_key = (identity.generation, day_start.date().isoformat())
        if self._cache_key is not None and self._cache_key != cache_key:
            old_generation, old_day = self._cache_key
            cache_invalidated = True
            if old_generation != identity.generation:
                cache_reason = "connection_generation_changed"
            elif old_day != cache_key[1]:
                cache_reason = "utc_day_changed"
            self._cache_invalidated_reason = cache_reason
        self._cache_key = cache_key

        try:
            components = self._collect(identity, day_start, current)
            snapshot = self._compose_snapshot(
                identity,
                day_start,
                current,
                components,
                cache_invalidated=cache_invalidated,
                cache_reason=cache_reason,
            )
        except Exception as exc:
            # A failed read invalidates any previously usable metrics.  The
            # error remains a reason; the observer never turns a transport or
            # protocol failure into a zero-valued risk metric.
            snapshot = self._invalid_snapshot(
                current,
                day_start,
                (f"observation_failed:{type(exc).__name__}",),
                identity=identity,
                cache_invalidated=cache_invalidated,
                cache_reason=cache_reason,
            )
        self.last_observation = snapshot
        return snapshot

    def _collect(self, identity: _SessionIdentity, day_start: datetime, current: datetime) -> dict[str, Any]:
        responses: list[_Response] = []
        trader_response = self._request("ProtoOATraderReq", TRADER_RES, identity.account_id)
        responses.append(trader_response)
        trader = self._account_body(trader_response.payload, identity.account_id)
        trader_digits = _optional_money_digits(trader, "moneyDigits")
        margin_calculation_type = enum_name(
            trader,
            "totalMarginCalculationType",
            read_field(trader, "totalMarginCalculationType", "total_margin_calculation_type", default=None),
        )
        balance = _scaled_optional(trader, "balance", trader_digits)
        component_reasons: list[str] = []
        if trader_digits is None:
            component_reasons.append("trader_money_digits_unobserved")
        if balance is None:
            component_reasons.append("trader_balance_unobserved")

        reconcile_response = self._request("ProtoOAReconcileReq", RECONCILE_RES, identity.account_id)
        responses.append(reconcile_response)
        positions, positions_complete, position_reasons = self._positions(
            reconcile_response.payload, identity.account_id, trader_digits
        )
        component_reasons.extend(position_reasons)

        pnl_response = self._request("ProtoOAGetPositionUnrealizedPnLReq", UNREALIZED_PNL_RES, identity.account_id)
        responses.append(pnl_response)
        unrealized = self._unrealized(
            pnl_response.payload,
            identity.account_id,
            tuple(item["position_id"] for item in positions),
        )
        component_reasons.extend(unrealized["reasons"])

        deals = self._deals(identity.account_id, day_start, current, trader_digits)
        responses.extend(deals["responses"])
        component_reasons.extend(deals["reasons"])
        final_identity = self._session_identity()
        if final_identity != identity:
            component_reasons.append("connection_generation_changed")
        component_reasons.extend(self._freshness_reasons(responses, current, identity.generation))
        return {
            "responses": responses,
            "trader_digits": trader_digits,
            "margin_calculation_type": margin_calculation_type,
            "balance": balance,
            "positions": positions,
            "positions_complete": positions_complete,
            "unrealized": unrealized,
            "deals": deals,
            "reasons": component_reasons,
        }

    def _compose_snapshot(
        self,
        identity: _SessionIdentity,
        day_start: datetime,
        current: datetime,
        components: Mapping[str, Any],
        *,
        cache_invalidated: bool,
        cache_reason: str | None,
    ) -> AccountRiskSnapshot:
        reasons = list(cast(Sequence[str], components["reasons"]))
        positions = cast(list[dict[str, Any]], components["positions"])
        unrealized = cast(Mapping[str, Any], components["unrealized"])
        deals = cast(Mapping[str, Any], components["deals"])
        balance_value = cast(Decimal | None, components["balance"])
        unrealized_value = cast(Decimal | None, unrealized["net"] if unrealized["complete"] else None)
        gross_unrealized_value = cast(Decimal | None, unrealized["gross"] if unrealized["complete"] else None)
        with decimal_context():
            equity_value = (
                balance_value + unrealized_value if balance_value is not None and unrealized_value is not None else None
            )
            if equity_value is not None and equity_value < 0:
                reasons.append("equity_negative")
                equity_value = None
            used_margin, margin_state, margin_reasons = self._used_margin(
                positions,
                cast(str | None, components.get("margin_calculation_type")),
            )
            reasons.extend(margin_reasons)
            margin_level = self._margin_level(used_margin, equity_value, reasons)
        fresh = self._is_fresh(reasons)
        complete = self._is_complete(components, reasons)
        responses = cast(Sequence[_Response], components["responses"])
        observed_at = max((item.observed_at for item in responses if item.observed_at is not None), default=None)
        high_water = self._high_water.observe(
            identity,
            equity_value,
            observed_at,
            data_ready=fresh and complete and equity_value is not None,
        )
        if high_water["reason"] is not None:
            reasons.append(str(high_water["reason"]))
        drawdown = cast(Decimal | None, high_water["drawdown"])
        peak_equity = cast(Decimal | None, high_water["high_water_equity"])
        if not fresh or not complete:
            usable_realized = None
            usable_daily = None
        else:
            usable_realized = cast(Decimal | None, deals["net"])
            usable_daily = (
                usable_realized + unrealized_value
                if usable_realized is not None and unrealized_value is not None
                else None
            )
        freshness_state = "FRESH" if fresh else ("STALE" if any("stale" in item for item in reasons) else "UNKNOWN")
        reported_balance = balance_value if fresh else None
        reported_equity = equity_value if fresh else None
        reported_used_margin = used_margin if fresh else None
        reported_margin_level = margin_level if fresh else None
        reported_realized_gross = deals["gross"] if fresh else None
        reported_realized_costs = deals["costs"] if fresh else None
        reported_unrealized = unrealized_value if fresh else None
        reported_unrealized_gross = gross_unrealized_value if fresh else None
        reported_drawdown = drawdown if fresh and complete else None
        reported_peak_equity = peak_equity if fresh else None
        return AccountRiskSnapshot(
            identity.account_id,
            identity.session_id,
            identity.generation,
            observed_at,
            day_start,
            current,
            _text(reported_balance),
            _text(reported_equity),
            _text(reported_used_margin),
            _text(reported_margin_level),
            _text(usable_realized),
            _text(reported_realized_gross),
            _text(reported_realized_costs),
            _text(reported_unrealized),
            _text(reported_unrealized_gross),
            _text(usable_daily),
            tuple(positions),
            freshness_state,
            fresh,
            complete,
            margin_state,
            tuple(_dedupe(reasons)),
            int(deals["pages"]),
            bool(deals["complete"]),
            bool(components["positions_complete"]),
            bool(unrealized["complete"]),
            bool(deals["fees_complete"]),
            cache_invalidated,
            cache_reason,
            cast(str | None, components.get("margin_calculation_type")),
            _text(reported_drawdown),
            _text(reported_peak_equity),
            str(high_water["state"]),
            bool(high_water["complete"]),
            cast(str | None, high_water["reason"]),
        )

    def _margin_level(self, used_margin: Decimal | None, equity: Decimal | None, reasons: list[str]) -> Decimal | None:
        if used_margin == 0:
            reasons.append("NO_MARGIN_USED")
            return None
        if used_margin is None:
            return None
        if equity is None:
            reasons.append("margin_equity_unobserved")
            return None
        return (equity / used_margin) * Decimal("100")

    @staticmethod
    def _is_fresh(reasons: Sequence[str]) -> bool:
        return not any(
            reason.startswith("response_")
            or reason in {"connection_generation_changed", "positions_pnl_snapshot_mismatch"}
            for reason in reasons
        )

    @staticmethod
    def _is_complete(components: Mapping[str, Any], reasons: Sequence[str]) -> bool:
        blocked = {
            "connection_generation_changed",
            "positions_pnl_snapshot_mismatch",
            "position_margin_unobserved",
            "margin_aggregation_unobserved",
            "margin_net_aggregation_unobserved",
            "trader_balance_unobserved",
            "trader_money_digits_unobserved",
            "equity_negative",
            "response_generation_unobserved",
        }
        return bool(
            components["positions_complete"]
            and components["unrealized"]["complete"]
            and components["deals"]["complete"]
            and components["deals"]["fees_complete"]
            and not any(reason in blocked for reason in reasons)
            and "response_timestamp_unobserved" not in reasons
        )

    def update_executor(self, executor: Any, *, now: datetime | None = None) -> dict[str, Any]:
        """Observe and update one executor without bypassing its risk gates."""

        snapshot = self.observe(now=now)
        kwargs = snapshot.to_executor_kwargs()
        # Current executor versions do not expose used_margin.  Future
        # compatibility versions may accept it to represent NO_MARGIN_USED
        # without encoding an arbitrary large margin ratio.
        update = getattr(executor, "update_risk_metrics", None)
        if not callable(update):
            raise AccountRiskObservationError("executor no expone update_risk_metrics()")
        parameters: Mapping[str, Any]
        try:
            import inspect

            parameters = cast(Mapping[str, Any], inspect.signature(update).parameters)
        except (TypeError, ValueError):
            parameters = {}
        if "used_margin" not in parameters:
            kwargs.pop("used_margin", None)
        elif "margin_state" in parameters:
            kwargs["margin_state"] = snapshot.margin_state
        executor_status = update(**kwargs)
        projection = snapshot.to_dict()
        projection["observation"] = dict(projection)
        projection["executor"] = executor_status
        return projection

    def rearm_after_human(self, *, confirm: bool = False) -> dict[str, Any]:
        """Explicitly request a new high-water baseline; never called implicitly."""

        return self._high_water.rearm_after_human(confirm=confirm)

    # Alias used by composition roots that phrase the operation as observe.
    observe_and_update = update_executor

    def _session_identity(self) -> _SessionIdentity:
        proof = self.client.authenticated_session_evidence()
        if self.client.validate_session_evidence(proof) is not True:
            raise AccountRiskObservationError("la evidencia de sesión no fue validada por el cliente")
        account = _proof_value(proof, "account_id", "accountId", "ctidTraderAccountId")
        session = _proof_value(proof, "session_id", "sessionId")
        generation = _proof_value(proof, "connection_generation", "generation")
        environment = str(_proof_value(proof, "environment", "mode") or "").strip().upper()
        endpoint = _normalise_endpoint(_proof_value(proof, "endpoint", "host"))
        account_text = str(account).strip()
        session_text = str(session).strip()
        generation_text = str(generation).strip()
        if not account_text.isdigit() or int(account_text) <= 0:
            raise AccountRiskObservationError("la evidencia no identifica una cuenta numérica")
        if not session_text or not generation_text:
            raise AccountRiskObservationError("la evidencia carece de session_id/generation")
        if environment != "DEMO" or endpoint is None:
            raise AccountRiskObservationError("el observador sólo acepta evidencia DEMO")
        return _SessionIdentity(account_text, session_text, generation_text, environment, endpoint)

    def _request(self, message_name: str, expected_response: int, account_id: str) -> _Response:
        factory = getattr(self.proto, message_name, None)
        if factory is None and isinstance(self.proto, Mapping):
            factory = self.proto.get(message_name)
        if not callable(factory):
            raise AccountRiskProtocolError(f"codec oficial carece de {message_name}")
        try:
            message = factory(ctidTraderAccountId=int(account_id))
        except Exception as exc:
            raise AccountRiskProtocolError(f"no se pudo construir {message_name}") from exc
        self._request_counter += 1
        request_id = f"account-risk:{self._request_nonce}:{self._request_counter:06d}"
        try:
            wire = self.client.request_message(message, client_msg_id=request_id, timeout_seconds=self.max_age_seconds)
        except Exception as exc:
            raise AccountRiskObservationError(f"falló {message_name}: {type(exc).__name__}") from exc
        if not isinstance(wire, WireMessage):
            raise AccountRiskProtocolError("request_message no devolvió WireMessage")
        if wire.client_msg_id != request_id:
            raise AccountRiskProtocolError(f"correlación inesperada en {message_name}")
        if wire.payload_type_id != expected_response:
            raise AccountRiskProtocolError(
                f"respuesta inesperada en {message_name}: {wire.payload_type_id!r}, esperado {expected_response}"
            )
        response_account = read_field(
            wire.payload,
            "ctidTraderAccountId",
            "ctid_trader_account_id",
            default=None,
        )
        if response_account is None or str(response_account) != account_id:
            raise AccountRiskProtocolError(f"respuesta {message_name} no coincide con account_id")
        generation = None if wire.connection_generation is None else str(wire.connection_generation).strip()
        observed_at = wire.available_at or wire.received_at
        return _Response(wire.payload, expected_response, generation or None, observed_at, wire.client_msg_id)

    def _account_body(self, payload: Any, account_id: str) -> Any:
        trader = read_field(payload, "trader", default=None)
        if trader is None:
            raise AccountRiskProtocolError("ProtoOATraderRes carece de trader")
        observed = read_field(trader, "ctidTraderAccountId", "ctid_trader_account_id", default=None)
        if observed is None or str(observed) != account_id:
            raise AccountRiskProtocolError("ProtoOATrader no coincide con account_id")
        return trader

    def _positions(
        self, payload: Any, account_id: str, trader_digits: int | None
    ) -> tuple[list[dict[str, Any]], bool, list[str]]:
        reasons: list[str] = []
        if isinstance(payload, Mapping) and "position" not in payload and "positions" not in payload:
            return [], False, ["positions_list_incomplete"]
        positions: list[dict[str, Any]] = []
        seen: set[str] = set()
        for raw in read_repeated(payload, "position", "positions"):
            position_id = _required_text(raw, "positionId", "position_id")
            if position_id is None:
                reasons.append("position_id_unobserved")
                continue
            if position_id in seen:
                reasons.append("duplicate_position_id")
                continue
            seen.add(position_id)
            status = enum_name(
                raw, "positionStatus", read_field(raw, "positionStatus", "position_status", default=None)
            )
            if status not in {"POSITION_STATUS_OPEN", "OPEN", "1"}:
                if status in {"POSITION_STATUS_CLOSED", "CLOSED", "POSITION_STATUS_ERROR", "ERROR"}:
                    continue
                reasons.append("position_status_unobserved")
                continue
            trade_data = read_field(raw, "tradeData", "trade_data", default=None)
            symbol_id = read_field(trade_data, "symbolId", "symbol_id", default=None)
            volume = read_field(trade_data, "volume", default=None)
            if symbol_id is None or volume is None:
                reasons.append("position_trade_data_incomplete")
            position_digits = _optional_money_digits(raw, "moneyDigits")
            if position_digits is None:
                position_digits = trader_digits
            used_margin = _scaled_optional(raw, "usedMargin", trader_digits=position_digits)
            positions.append(
                {
                    "position_id": position_id,
                    "symbol_id": str(symbol_id) if symbol_id is not None else None,
                    "volume_protocol": _int_text(volume),
                    "used_margin": _text(used_margin),
                    "money_digits": position_digits,
                    "updated_at": _ms_iso(read_field(raw, "utcLastUpdateTimestamp", default=None)),
                }
            )
        return positions, not reasons, reasons

    def _unrealized(self, payload: Any, account_id: str, position_ids: tuple[str, ...]) -> dict[str, Any]:
        reasons: list[str] = []
        if (
            isinstance(payload, Mapping)
            and "positionUnrealizedPnL" not in payload
            and "position_unrealized_pnl" not in payload
        ):
            return {"net": None, "gross": None, "complete": False, "reasons": ["unrealized_pnl_list_incomplete"]}
        digits = _required_money_digits(payload, "moneyDigits", reasons)
        entries = read_repeated(payload, "positionUnrealizedPnL", "position_unrealized_pnl")
        expected = set(position_ids)
        seen: set[str] = set()
        net = Decimal("0")
        gross = Decimal("0")
        for raw in entries:
            position_id = _required_text(raw, "positionId", "position_id")
            if position_id is None:
                reasons.append("position_id_unobserved")
                continue
            if position_id in seen:
                reasons.append("duplicate_unrealized_position_id")
                continue
            seen.add(position_id)
            raw_gross = _required_integer(raw, "grossUnrealizedPnL", reasons)
            raw_net = _required_integer(raw, "netUnrealizedPnL", reasons)
            if raw_gross is None or raw_net is None or digits is None:
                continue
            with decimal_context():
                gross += _scale_integer(raw_gross, digits)
                net += _scale_integer(raw_net, digits)
        if seen != expected:
            reasons.append("positions_pnl_snapshot_mismatch")
        complete = not reasons and seen == expected and (not expected or digits is not None)
        return {
            "net": net if complete else None,
            "gross": gross if complete else None,
            "complete": complete,
            "reasons": reasons,
        }

    def _deals(self, account_id: str, day_start: datetime, now: datetime, trader_digits: int | None) -> dict[str, Any]:
        current_to = _timestamp_ms(now)
        start_ms = _timestamp_ms(day_start)
        responses: list[_Response] = []
        records: dict[str, Any] = {}
        reasons: list[str] = []
        complete = False
        structure_complete = True
        fees_complete = True
        gross_total = Decimal("0")
        cost_total = Decimal("0")
        net_total = Decimal("0")
        for page_number in range(self.max_pages):
            response = self._request_deal_page(account_id, start_ms, current_to, page_number)
            responses.append(response)
            page = self._deal_page(response, start_ms, _timestamp_ms(now), records, trader_digits)
            reasons.extend(page["reasons"])
            if page["structure_complete"] is False:
                structure_complete = False
            with decimal_context():
                gross_total += cast(Decimal, page["gross"])
            if page["fees_complete"] is False:
                fees_complete = False
            if page["net"] is not None:
                with decimal_context():
                    net_total += cast(Decimal, page["net"])
                    cost_total += cast(Decimal, page["costs"])
            if not page["has_more"]:
                complete = True
                break
            timestamps = cast(list[int], page["timestamps"])
            if not timestamps:
                reasons.append("deal_pagination_no_progress")
                break
            oldest = min(timestamps)
            if oldest <= start_ms or oldest > current_to:
                reasons.append("deal_pagination_no_progress")
                break
            current_to = oldest - 1
        else:
            reasons.append("deal_pagination_limit")
        if not complete and "deal_pagination_limit" not in reasons:
            reasons.append("deals_incomplete")
        # No closePositionDetail in a complete page is a valid observed zero;
        # it is not a missing fee.  A close detail with a missing fee remains
        # unknown and is never coerced to zero.
        if not fees_complete:
            net_value: Decimal | None = None
            cost_value: Decimal | None = None
        else:
            net_value = net_total
            cost_value = cost_total
        return {
            "responses": responses,
            "records": records,
            "pages": len(responses),
            "complete": complete and structure_complete,
            "fees_complete": fees_complete,
            "gross": gross_total if complete and structure_complete else None,
            "costs": cost_value,
            "net": net_value if complete else None,
            "reasons": reasons,
        }

    def _deal_page(
        self,
        response: _Response,
        start_ms: int,
        end_ms: int,
        records: dict[str, Any],
        trader_digits: int | None,
    ) -> dict[str, Any]:
        reasons: list[str] = []
        if isinstance(response.payload, Mapping) and "deal" not in response.payload and "deals" not in response.payload:
            return {
                "has_more": False,
                "timestamps": [],
                "gross": Decimal("0"),
                "costs": Decimal("0"),
                "net": None,
                "fees_complete": False,
                "structure_complete": False,
                "reasons": ["deals_list_incomplete"],
            }
        has_more = _required_bool(response.payload, "hasMore", reasons)
        timestamps: list[int] = []
        gross_total = Decimal("0")
        cost_total = Decimal("0")
        net_total = Decimal("0")
        fees_complete = True
        for raw in read_repeated(response.payload, "deal", "deals"):
            deal_id = _required_text(raw, "dealId", "deal_id")
            execution_ms = _required_integer(raw, "executionTimestamp", reasons)
            if deal_id is None or execution_ms is None:
                continue
            if execution_ms < start_ms or execution_ms > end_ms:
                reasons.append("deal_timestamp_out_of_bounds")
                continue
            timestamps.append(execution_ms)
            if deal_id in records:
                reasons.append("duplicate_deal_id")
                continue
            records[deal_id] = raw
            detail = read_field(raw, "closePositionDetail", "close_position_detail", default=None)
            if detail is None:
                continue
            result = _realized_detail(detail, raw, trader_digits)
            if result["gross"] is None:
                reasons.append("realized_gross_unobserved")
                fees_complete = False
                continue
            with decimal_context():
                gross_total += cast(Decimal, result["gross"])
            if result["net"] is None:
                fees_complete = False
                reasons.extend(result["reasons"])
            else:
                with decimal_context():
                    cost_total += cast(Decimal, result["costs"])
                    net_total += cast(Decimal, result["net"])
        return {
            "has_more": has_more,
            "timestamps": timestamps,
            "gross": gross_total,
            "costs": cost_total,
            "net": net_total if fees_complete else None,
            "fees_complete": fees_complete,
            "structure_complete": not any(not item.startswith("realized_") for item in reasons),
            "reasons": reasons,
        }

    def _request_deal_page(self, account_id: str, from_ms: int, to_ms: int, page_number: int) -> _Response:
        factory = getattr(self.proto, "ProtoOADealListReq", None)
        if factory is None and isinstance(self.proto, Mapping):
            factory = self.proto.get("ProtoOADealListReq")
        if not callable(factory):
            raise AccountRiskProtocolError("codec oficial carece de ProtoOADealListReq")
        message = factory(
            ctidTraderAccountId=int(account_id),
            fromTimestamp=int(from_ms),
            toTimestamp=int(to_ms),
            maxRows=int(self.max_rows),
        )
        self._request_counter += 1
        request_id = f"account-risk:{self._request_nonce}:{self._request_counter:06d}"
        try:
            wire = self.client.request_message(message, client_msg_id=request_id, timeout_seconds=self.max_age_seconds)
        except Exception as exc:
            raise AccountRiskObservationError("falló ProtoOADealListReq") from exc
        if (
            not isinstance(wire, WireMessage)
            or wire.client_msg_id != request_id
            or wire.payload_type_id != DEAL_LIST_RES
        ):
            raise AccountRiskProtocolError("respuesta inválida o no correlacionada en ProtoOADealListReq")
        response_account = read_field(wire.payload, "ctidTraderAccountId", "ctid_trader_account_id", default=None)
        if response_account is None or str(response_account) != account_id:
            raise AccountRiskProtocolError("ProtoOADealListRes no coincide con account_id")
        observed_at = wire.available_at or wire.received_at
        generation = None if wire.connection_generation is None else str(wire.connection_generation).strip() or None
        return _Response(wire.payload, DEAL_LIST_RES, generation, observed_at, wire.client_msg_id)

    def _used_margin(
        self,
        positions: Sequence[Mapping[str, Any]],
        calculation_type: str | None,
    ) -> tuple[Decimal | None, str, list[str]]:
        if not positions:
            return Decimal("0"), "NO_MARGIN_USED", []
        values: list[Decimal] = []
        for position in positions:
            raw = position.get("used_margin")
            if raw is None or position.get("money_digits") is None:
                return None, "UNKNOWN", ["position_margin_unobserved"]
            values.append(Decimal(raw))
        if len(values) == 1 or calculation_type == "SUM":
            return sum(values, Decimal("0")), "OBSERVED", []
        if calculation_type == "MAX":
            return max(values), "OBSERVED", []
        if calculation_type == "NET":
            return None, "UNKNOWN", ["margin_net_aggregation_unobserved"]
        return None, "UNKNOWN", ["margin_aggregation_unobserved"]

    def _freshness_reasons(self, responses: Sequence[_Response], now: datetime, generation: str) -> list[str]:
        reasons: list[str] = []
        for response in responses:
            if response.generation is None:
                reasons.append("response_generation_unobserved")
            elif response.generation != generation:
                reasons.append("connection_generation_changed")
            if response.observed_at is None:
                reasons.append("response_timestamp_unobserved")
                continue
            age = (now - response.observed_at).total_seconds()
            if age < 0 or age > self.max_age_seconds:
                reasons.append(f"response_stale:{age:.3f}")
        return reasons

    def _invalid_snapshot(
        self,
        current: datetime,
        day_start: datetime,
        reasons: Sequence[str],
        *,
        identity: _SessionIdentity | None = None,
        cache_invalidated: bool = False,
        cache_reason: str | None = None,
    ) -> AccountRiskSnapshot:
        return AccountRiskSnapshot(
            identity.account_id if identity else None,
            identity.session_id if identity else None,
            identity.generation if identity else None,
            None,
            day_start,
            current,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            (),
            "UNKNOWN",
            False,
            False,
            "UNKNOWN",
            tuple(_dedupe(reasons)),
            0,
            False,
            False,
            False,
            False,
            cache_invalidated,
            cache_reason,
        )


def observe_account_risk(provider: Any, **kwargs: Any) -> AccountRiskSnapshot:
    """Convenience entry point for composition roots."""

    now = kwargs.pop("now", None)
    return AccountRiskObserver(provider, **kwargs).observe(now=now)


def update_executor_risk(provider: Any, executor: Any, **kwargs: Any) -> dict[str, Any]:
    """Observe account state and feed the same observation into an executor."""

    return AccountRiskObserver(
        provider,
        **{
            key: value
            for key, value in kwargs.items()
            if key
            in {
                "proto",
                "clock",
                "max_age_seconds",
                "max_pages",
                "max_rows",
                "state_path",
                "journal_path",
                "allow_new_baseline",
            }
        },
    ).update_executor(executor, now=kwargs.get("now"))


def rearm_after_human(observer: AccountRiskObserver, *, confirm: bool = False) -> dict[str, Any]:
    """Explicit helper for an operator-approved high-water rearm."""

    return observer.rearm_after_human(confirm=confirm)


def _private_state_path(value: str | Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise AccountRiskObservationError("risk state_path debe ser una ruta absoluta fuera del checkout")
    _validate_private_path(path, allow_missing=True)
    checkout = Path(__file__).resolve().parents[2]
    resolved = path.resolve(strict=False)
    if resolved == checkout or checkout in resolved.parents:
        raise AccountRiskObservationError("risk state_path no puede vivir dentro del checkout")
    return path


def _validate_private_path(path: Path, *, allow_missing: bool) -> None:
    if not path.is_absolute():
        raise AccountRiskObservationError("ruta de estado debe ser absoluta")
    _validate_private_parent(path.parent)
    stat_result = _private_lstat(path, allow_missing=allow_missing)
    if stat_result is None:
        return
    _validate_private_file_stat(stat_result)


def _validate_private_parent(parent: Path) -> None:
    if not parent.is_absolute():
        raise AccountRiskObservationError("directorio de estado debe ser absoluto")
    try:
        direct = os.lstat(parent)
    except FileNotFoundError as exc:
        raise AccountRiskObservationError("directorio de estado inexistente") from exc
    except OSError as exc:
        raise AccountRiskObservationError("no se pudo inspeccionar el directorio de estado") from exc
    if stat.S_ISLNK(direct.st_mode) or not stat.S_ISDIR(direct.st_mode):
        raise AccountRiskObservationError("directorio de estado no es un directorio regular")
    if direct.st_uid != os.getuid() or stat.S_IMODE(direct.st_mode) & 0o077:
        raise AccountRiskObservationError("directorio de estado no es privado")
    current = parent.parent
    while current != Path(current.anchor):
        try:
            stat_result = os.lstat(current)
        except FileNotFoundError as exc:
            raise AccountRiskObservationError("directorio de estado inexistente") from exc
        except OSError as exc:
            raise AccountRiskObservationError("no se pudo inspeccionar el directorio de estado") from exc
        if stat.S_ISLNK(stat_result.st_mode) or not stat.S_ISDIR(stat_result.st_mode):
            raise AccountRiskObservationError("ruta de estado contiene un componente no regular")
        current = current.parent


def _private_lstat(path: Path, *, allow_missing: bool) -> os.stat_result | None:
    try:
        return os.lstat(path)
    except FileNotFoundError:
        if allow_missing:
            return None
        raise
    except OSError as exc:
        raise AccountRiskObservationError("no se pudo inspeccionar el archivo de estado") from exc


def _validate_private_file_stat(stat_result: os.stat_result) -> None:
    if stat.S_ISLNK(stat_result.st_mode) or not stat.S_ISREG(stat_result.st_mode):
        raise AccountRiskObservationError("archivo de estado no es regular")
    if stat_result.st_nlink != 1 or stat_result.st_uid != os.getuid():
        raise AccountRiskObservationError("archivo de estado tiene identidad insegura")
    if stat.S_IMODE(stat_result.st_mode) != 0o600:
        raise AccountRiskObservationError("archivo de estado debe tener modo 0600")


def _validate_private_file(path: Path) -> None:
    stat_result = _private_lstat(path, allow_missing=False)
    assert stat_result is not None
    _validate_private_file_stat(stat_result)


def _read_private_bytes(path: Path, *, limit: int = _RISK_STATE_MAX_BYTES) -> bytes:
    _validate_private_file(path)
    flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise AccountRiskObservationError("no se pudo abrir el archivo de estado") from exc
    try:
        stat_result = os.fstat(fd)
        _validate_private_file_stat(stat_result)
        if stat_result.st_size > limit:
            raise AccountRiskObservationError("risk_state_too_large")
        return os.read(fd, limit + 1)
    finally:
        os.close(fd)


def _sync_directory(parent: Path) -> None:
    flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(parent, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _proof_value(proof: Any, *names: str) -> Any:
    for name in names:
        value = read_field(proof, name, default=None)
        if value is not None:
            return value
    return None


def _normalise_endpoint(value: Any) -> str | None:
    if value is None:
        return None
    raw = str(value).strip().lower().rstrip("/")
    for prefix in ("tcp://", "ssl://"):
        if raw.startswith(prefix):
            raw = raw[len(prefix) :]
    return "demo.ctraderapi.com:5035" if raw == "demo.ctraderapi.com:5035" else None


def _required_text(value: Any, *names: str) -> str | None:
    raw = read_field(value, *names, default=None)
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return None
    text = str(raw).strip()
    return text if text else None


def _required_integer(value: Any, name: str, reasons: list[str]) -> int | None:
    raw = read_field(value, name, _snake(name), default=None)
    if raw is None or isinstance(raw, bool):
        reasons.append(f"{_snake(name)}_unobserved")
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        reasons.append(f"{_snake(name)}_invalid")
        return None


def _required_bool(value: Any, name: str, reasons: list[str]) -> bool:
    raw = read_field(value, name, _snake(name), default=None)
    if raw is None or isinstance(raw, bool) is False and not isinstance(raw, (int, str)):
        reasons.append(f"{_snake(name)}_unobserved")
        return False
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, int):
        return bool(raw)
    text = str(raw).strip().lower()
    if text in {"true", "1"}:
        return True
    if text in {"false", "0"}:
        return False
    reasons.append(f"{_snake(name)}_invalid")
    return False


def _optional_money_digits(value: Any, name: str) -> int | None:
    raw = read_field(value, name, _snake(name), default=None)
    if raw is None:
        return None
    if isinstance(raw, bool):
        return None
    try:
        digits = int(raw)
    except (TypeError, ValueError):
        return None
    return digits if 0 <= digits <= MAX_MONEY_DIGITS else None


def _required_money_digits(value: Any, name: str, reasons: list[str]) -> int | None:
    digits = _optional_money_digits(value, name)
    if digits is None:
        reasons.append(f"{_snake(name)}_unobserved")
    return digits


def _scaled_optional(
    value: Any, name: str, digits: int | None = None, *, required: bool = False, trader_digits: int | None = None
) -> Decimal | None:
    raw = read_field(value, name, _snake(name), default=None)
    if raw is None or isinstance(raw, bool):
        if required:
            return None
        return None
    resolved_digits = digits if digits is not None else trader_digits
    if resolved_digits is None:
        return None
    try:
        integer = int(raw)
    except (TypeError, ValueError):
        return None
    return _scale_integer(integer, resolved_digits)


def _scale_integer(value: int, digits: int) -> Decimal:
    return Decimal(value).scaleb(-digits)


def _realized_detail(detail: Any, deal: Any, trader_digits: int | None) -> dict[str, Any]:
    reasons: list[str] = []
    digits = _detail_money_digits(detail, deal, trader_digits)
    if digits is None:
        return {"gross": None, "costs": None, "net": None, "reasons": ["realized_money_digits_unobserved"]}
    values = {
        field: _scaled_detail_value(detail, field, digits, reasons) for field in ("grossProfit", "swap", "commission")
    }
    # This optional fee is intentionally not defaulted to zero.  A caller can
    # still inspect realized gross PnL, but the net daily figure stays None.
    conversion = _scaled_detail_value(detail, "pnlConversionFee", digits, reasons)
    gross = values["grossProfit"]
    swap = values["swap"]
    commission = values["commission"]
    with decimal_context():
        costs = (
            swap + commission + conversion
            if swap is not None and commission is not None and conversion is not None
            else None
        )
        net = gross + costs if gross is not None and costs is not None else None
    return {"gross": gross, "costs": costs, "net": net, "reasons": reasons}


def _detail_money_digits(detail: Any, deal: Any, trader_digits: int | None) -> int | None:
    digits = _optional_money_digits(detail, "moneyDigits")
    if digits is None:
        digits = _optional_money_digits(deal, "moneyDigits")
    return trader_digits if digits is None else digits


def _scaled_detail_value(detail: Any, field: str, digits: int, reasons: list[str]) -> Decimal | None:
    raw = read_field(detail, field, _snake(field), default=None)
    reason_prefix = f"realized_{_snake(field)}"
    if raw is None or isinstance(raw, bool):
        reasons.append(f"{reason_prefix}_unobserved")
        return None
    try:
        return _scale_integer(int(raw), digits)
    except (TypeError, ValueError):
        reasons.append(f"{reason_prefix}_invalid")
        return None


def _positive_float(value: Any, name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{name} debe ser positivo y finito")
    result = float(value)
    if not math.isfinite(result) or result <= 0:
        raise ValueError(f"{name} debe ser positivo y finito")
    return result


def _positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or int(value) <= 0:
        raise ValueError(f"{name} debe ser entero positivo")
    return int(value)


def _utc(value: Any, name: str) -> datetime:
    if isinstance(value, datetime):
        result = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            result = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"{name} debe ser ISO-8601") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError(f"{name} debe incluir zona horaria")
    return result.astimezone(UTC)


def _timestamp_ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z") if value else None


def _ms_iso(value: Any) -> str | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return _iso(datetime.fromtimestamp(int(value) / 1000, UTC))
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def _text(value: Decimal | None) -> str | None:
    return format(value, "f") if value is not None else None


def _int_text(value: Any) -> str | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return str(int(value))
    except (TypeError, ValueError):
        return None


def _snake(name: str) -> str:
    result = []
    for char in name:
        result.append("_" + char.lower() if char.isupper() else char)
    return "".join(result).lstrip("_")


def _dedupe(values: Sequence[str]) -> list[str]:
    return list(dict.fromkeys(str(item) for item in values if str(item).strip()))


# Stable aliases for composition code and callers that name the component
# after the cTrader provider rather than the generic account observer.
CTraderAccountRiskObserver = AccountRiskObserver
CTraderAccountRiskSnapshot = AccountRiskSnapshot
AccountRiskObservation = AccountRiskSnapshot

__all__ = [
    "AccountRiskObservationError",
    "AccountRiskProtocolError",
    "AccountRiskObserver",
    "AccountRiskObservation",
    "AccountRiskSnapshot",
    "CTraderAccountRiskObserver",
    "CTraderAccountRiskSnapshot",
    "observe_account_risk",
    "update_executor_risk",
]
