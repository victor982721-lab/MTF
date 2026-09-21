"""Pure authorization boundary for zero-spread quotes in one DEMO canary.

This module intentionally has no provider, transport, persistence, or account
observer dependency.  It only binds the already-approved canary identity and
time window so an adapter can make an explicit, auditable decision about a
genuinely observed ``bid == ask`` quote.
"""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime, timedelta
from typing import Any

CANARY_ZERO_SPREAD_SYMBOL = "EUR/USD"
CANARY_ZERO_SPREAD_ENDPOINT = "demo.ctraderapi.com:5035"
_MAX_PREPARATION_LEAD = timedelta(minutes=5)
_MAX_ORDER_WINDOW = timedelta(minutes=20)
_HEX_DIGITS = frozenset("0123456789abcdefABCDEF")


class CanaryQuoteAuthorizationError(ValueError):
    """Malformed or out-of-scope zero-spread canary authorization."""


def _required_text(value: Any, *, name: str) -> str:
    if not isinstance(value, str):
        raise CanaryQuoteAuthorizationError(f"{name} debe ser texto")
    text = value.strip()
    if not text or any(ord(char) < 32 or ord(char) == 127 for char in text):
        raise CanaryQuoteAuthorizationError(f"{name} debe ser texto no vacío y sin controles")
    return text


def _utc(value: Any, *, name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise CanaryQuoteAuthorizationError(f"{name} debe ser ISO-8601") from exc
    else:
        raise CanaryQuoteAuthorizationError(f"{name} debe ser datetime aware UTC")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CanaryQuoteAuthorizationError(f"{name} debe ser datetime aware UTC")
    return parsed.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _candidate_text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


@dataclasses.dataclass(frozen=True, slots=True)
class CanaryZeroSpreadAuthorization:
    """Immutable, approval-bound scope for one DEMO zero-spread canary.

    The authorization does not accept a generic mapping or an enabled/disabled
    boolean.  A caller must construct this typed value with the approved
    digest, identity, fixed symbol/endpoint, and bounded UTC window.
    """

    approval_digest: str
    authorization_source: str
    account_id: str
    session_id: str
    connection_generation: str
    preparation_start: datetime
    window_start: datetime
    window_end: datetime
    symbol: str = CANARY_ZERO_SPREAD_SYMBOL
    endpoint: str = CANARY_ZERO_SPREAD_ENDPOINT

    def __post_init__(self) -> None:
        digest = _required_text(self.approval_digest, name="approval_digest")
        if len(digest) != 64 or any(char not in _HEX_DIGITS for char in digest):
            raise CanaryQuoteAuthorizationError("approval_digest debe ser hexadecimal de 64 caracteres")
        source = _required_text(self.authorization_source, name="authorization_source")
        account = _required_text(self.account_id, name="account_id")
        session = _required_text(self.session_id, name="session_id")
        generation = _required_text(self.connection_generation, name="connection_generation")
        symbol = _required_text(self.symbol, name="symbol")
        endpoint = _required_text(self.endpoint, name="endpoint")
        if symbol != CANARY_ZERO_SPREAD_SYMBOL:
            raise CanaryQuoteAuthorizationError("zero-spread sólo está autorizado para EUR/USD")
        if endpoint != CANARY_ZERO_SPREAD_ENDPOINT:
            raise CanaryQuoteAuthorizationError("endpoint de zero-spread fuera del alcance DEMO aprobado")
        preparation_start = _utc(self.preparation_start, name="preparation_start")
        window_start = _utc(self.window_start, name="window_start")
        window_end = _utc(self.window_end, name="window_end")
        if not preparation_start <= window_start <= window_end:
            raise CanaryQuoteAuthorizationError("la ventana requiere preparation_start <= window_start <= window_end")
        if window_start - preparation_start > _MAX_PREPARATION_LEAD:
            raise CanaryQuoteAuthorizationError("preparation_start no puede anteceder más de cinco minutos")
        if window_end - window_start > _MAX_ORDER_WINDOW:
            raise CanaryQuoteAuthorizationError("la ventana de órdenes no puede exceder veinte minutos")
        if {preparation_start.date(), window_start.date(), window_end.date()} != {preparation_start.date()}:
            raise CanaryQuoteAuthorizationError("la autorización no puede cruzar la fecha UTC")
        object.__setattr__(self, "approval_digest", digest.lower())
        object.__setattr__(self, "authorization_source", source)
        object.__setattr__(self, "account_id", account)
        object.__setattr__(self, "session_id", session)
        object.__setattr__(self, "connection_generation", generation)
        object.__setattr__(self, "preparation_start", preparation_start)
        object.__setattr__(self, "window_start", window_start)
        object.__setattr__(self, "window_end", window_end)
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "endpoint", endpoint)

    def matches(
        self,
        *,
        account_id: Any,
        session_id: Any,
        connection_generation: Any,
        endpoint: Any,
        symbol: Any,
        now: Any,
        require_order_window: bool = False,
    ) -> bool:
        """Return whether current identity and time are inside this scope.

        Preparation checks are accepted from ``preparation_start``.  A caller
        that is about to admit an order must pass ``require_order_window=True``
        so the lower bound becomes ``window_start``.  The upper bound is
        always exclusive; after ``window_end`` the authorization is expired.
        Malformed or missing inputs fail closed as ``False``.
        """

        if not isinstance(require_order_window, bool):
            return False
        if (
            _candidate_text(account_id) != self.account_id
            or _candidate_text(session_id) != self.session_id
            or _candidate_text(connection_generation) != self.connection_generation
            or _candidate_text(endpoint) != self.endpoint
            or _candidate_text(symbol) != self.symbol
        ):
            return False
        try:
            current = _utc(now, name="now")
        except CanaryQuoteAuthorizationError:
            return False
        lower_bound = self.window_start if require_order_window else self.preparation_start
        return lower_bound <= current < self.window_end

    def to_dict(self) -> dict[str, str]:
        """Return the non-secret, JSON-serializable audit projection."""

        return {
            "approval_digest": self.approval_digest,
            "authorization_source": self.authorization_source,
            "account_id": self.account_id,
            "symbol": self.symbol,
            "session_id": self.session_id,
            "connection_generation": self.connection_generation,
            "endpoint": self.endpoint,
            "preparation_start": _iso(self.preparation_start),
            "window_start": _iso(self.window_start),
            "window_end": _iso(self.window_end),
        }


__all__ = ["CanaryQuoteAuthorizationError", "CanaryZeroSpreadAuthorization"]
