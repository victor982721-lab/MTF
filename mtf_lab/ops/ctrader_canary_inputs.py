"""Producer of observed inputs for the bounded DEMO canary.

This module is deliberately below the canary executor.  It owns no account
writer, does not activate an executor and never submits an order.  Its only
job is to consume an already authenticated :class:`CTraderProvider` through
the existing single-reader watch path and return a typed, in-process context
containing:

* a fresh, provider-normalized bid/ask observation;
* an ATR/runtime snapshot published by ``RuntimeCoordinator``; and
* two explicitly manual BUY/SELL instructions for a human-approved canary.

The returned object is not reconstructible from JSON.  Session evidence is
validated against the live client and connection generation in the same
process.  JSON/dict values are used only for the existing runtime seam and
for diagnostic output; they are never treated as authentication or market
evidence.
"""

from __future__ import annotations

import contextlib
import hashlib
import tempfile
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Any, Protocol

from ..configuration import EffectiveConfig
from ..core import parse_timeframe
from ..core.canary_quote import CanaryZeroSpreadAuthorization
from ..core.canonical import fingerprint
from ..data.models import Bar, Event
from ..ops.ctrader_executor import DecimalValue, Quote
from ..ops.ctrader_warmup import CTraderWarmupError, CTraderWarmupResult, fetch_causal_warmup
from ..ops.ctrader_watch import (
    CTraderWatchContext,
    CTraderWatchError,
    CTraderWatchOptions,
    CTraderWatchResult,
    CTraderWatchRunner,
    WatchStopReason,
)
from ..ops.persistence import SQLiteStore

SCHEMA = "mtf-lab.ctrader-canary-inputs.v1"
_CANONICAL_RUNTIME = Path.home() / ".local" / "share" / "mtf-lab" / "runtime"
_REQUIRED_RUNTIME_FIELDS = (
    "market_candidate_id",
    "trigger_timeframe",
    "trigger_bar_count",
    "risk_bar_clock_basis",
    "latest_trigger_start",
    "latest_trigger_end",
    "latest_trigger_available_at",
    "atr",
    "last_market",
    "last_available",
    "data_mode",
)
_RISK_BAR_CLOCK_BASIS = "UTC_TIMEFRAME_BOUNDARIES_NO_OHLC_IMPUTATION"


class CanaryInputState(StrEnum):
    READY = "READY"
    BLOCKED = "BLOCKED"


class CanaryInputError(RuntimeError):
    """A canary input cannot be accredited without weakening a gate."""


class RuntimeObserver(Protocol):
    def __call__(self, snapshot: Mapping[str, Any]) -> Mapping[str, Any]: ...


class MarketWindowObserver(Protocol):
    def __call__(self, provider: Any, start: datetime, end: datetime) -> Any: ...


class RiskPlanner(Protocol):
    def __call__(self, signal: Mapping[str, Any], quote: Quote, *, requested_quantity: Decimal) -> Any: ...


def _aware(value: Any, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} debe ser datetime con zona horaria")
    return value.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_time(value: Any, name: str) -> datetime:
    if isinstance(value, datetime):
        return _aware(value, name)
    if not isinstance(value, str) or not value.strip():
        raise CanaryInputError(f"{name} ausente")
    try:
        return _aware(datetime.fromisoformat(value.strip().replace("Z", "+00:00")), name)
    except ValueError as exc:
        raise CanaryInputError(f"{name} inválido") from exc


def _decimal(value: Any, name: str, *, positive: bool = False) -> Decimal:
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise CanaryInputError(f"{name} inválido") from exc
    if not parsed.is_finite() or (positive and parsed <= 0):
        raise CanaryInputError(f"{name} inválido")
    return parsed


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise CanaryInputError(f"{name} no es Mapping")
    return value


def _provider_name(provider: Any) -> str:
    value = str(getattr(provider, "name", type(provider).__name__)).strip()
    if not value:
        raise CanaryInputError("provider sin identidad")
    return value


def _connection_value(status: Any) -> str:
    value = getattr(status, "connection", status)
    return str(getattr(value, "value", value)).strip().upper()


def _auth_value(status: Any) -> str | None:
    value = getattr(status, "auth", None)
    if value is None:
        return None
    return str(getattr(value, "value", value)).strip().upper()


def _proof_field(proof: Any, *names: str) -> Any:
    # A mapping is intentionally not a proof.  This prevents a JSON payload
    # or a caller-owned dict from being promoted into session attestation.
    if isinstance(proof, Mapping):
        raise CanaryInputError("la evidencia de sesión no puede ser un payload JSON")
    marker = object()
    for name in names:
        value = getattr(proof, name, marker)
        if value is not marker:
            return value
    return None


def _scopes(value: Any) -> frozenset[str]:
    if isinstance(value, str) or not isinstance(value, Collection):
        raise CanaryInputError("la evidencia de sesión no declara scopes tipados")
    result = frozenset(str(item).strip().lower() for item in value if str(item).strip())
    if not result:
        raise CanaryInputError("la evidencia de sesión no declara scopes")
    return result


@dataclass(frozen=True, slots=True)
class CanarySessionEvidence:
    """Identity proof extracted from the currently authenticated client.

    The normal constructor is useful for tests that provide a typed fake
    gateway.  Production callers should use :meth:`from_provider`, which
    invokes the client's own ``authenticated_session_evidence`` and
    ``validate_session_evidence`` methods before copying any field.
    """

    provider: str
    session_id: str
    account_id: str
    environment: str
    endpoint: str
    scopes: frozenset[str]
    connection_generation: str
    authenticated_at: datetime
    evidence_source: str
    expires_at: datetime | None = None

    def __post_init__(self) -> None:  # noqa: C901 - one normalized BBO/authentication gate
        provider = str(self.provider).strip()
        session_id = str(self.session_id).strip()
        account_id = str(self.account_id).strip()
        environment = str(self.environment).strip().upper()
        endpoint = str(self.endpoint).strip()
        generation = str(self.connection_generation).strip()
        source = str(self.evidence_source).strip()
        if not provider or not session_id or not endpoint or not generation or not source:
            raise ValueError("evidencia de sesión incompleta")
        if not account_id.isdigit() or int(account_id) <= 0:
            raise ValueError("account_id de sesión inválido")
        if environment != "DEMO":
            raise ValueError("la canaria sólo admite evidencia DEMO")
        scopes = frozenset(str(item).strip().lower() for item in self.scopes if str(item).strip())
        if not {"accounts", "trading"}.issubset(scopes):
            raise ValueError("la evidencia de sesión requiere accounts+trading")
        authenticated_at = _aware(self.authenticated_at, "authenticated_at")
        expires_at = _aware(self.expires_at, "expires_at") if self.expires_at is not None else None
        if expires_at is not None and expires_at <= authenticated_at:
            raise ValueError("expires_at debe ser posterior a authenticated_at")
        object.__setattr__(self, "provider", provider)
        object.__setattr__(self, "session_id", session_id)
        object.__setattr__(self, "account_id", account_id)
        object.__setattr__(self, "environment", environment)
        object.__setattr__(self, "endpoint", endpoint)
        object.__setattr__(self, "scopes", scopes)
        object.__setattr__(self, "connection_generation", generation)
        object.__setattr__(self, "authenticated_at", authenticated_at)
        object.__setattr__(self, "expires_at", expires_at)
        object.__setattr__(self, "evidence_source", source)

    @classmethod
    def from_provider(cls, provider: Any) -> CanarySessionEvidence:  # noqa: C901 - one typed session-proof gate
        """Read and validate proof from the existing provider/client only."""

        client = getattr(provider, "client", None)
        owner = client if callable(getattr(client, "authenticated_session_evidence", None)) else provider
        getter = getattr(owner, "authenticated_session_evidence", None)
        validator = getattr(owner, "validate_session_evidence", None)
        if not callable(getter) or not callable(validator):
            raise CanaryInputError("provider no expone evidencia de sesión autenticada tipada")
        try:
            proof = getter()
            if isinstance(proof, Mapping):
                raise CanaryInputError("la evidencia de sesión no puede ser un payload JSON")
            if validator(proof) is not True:
                raise CanaryInputError("validate_session_evidence no confirmó la sesión")
        except CanaryInputError:
            raise
        except Exception as exc:
            raise CanaryInputError(f"sesión DEMO no verificable: {type(exc).__name__}") from exc

        status = getattr(provider, "status", None)
        if status is not None:
            connection = _connection_value(status)
            if connection not in {"CONNECTED", "HEALTHY"}:
                raise CanaryInputError(f"sesión no está conectada: {connection or 'UNKNOWN'}")
            auth = _auth_value(status)
            if auth is not None and auth != "AUTHENTICATED":
                raise CanaryInputError(f"sesión no está autenticada: {auth or 'UNKNOWN'}")
        generation = _proof_field(proof, "connection_generation", "generation")
        provider_generation = getattr(provider, "generation", None)
        if generation is None or provider_generation is None or str(generation) != str(provider_generation):
            raise CanaryInputError("la evidencia y la generación actual no coinciden")
        authenticated_at = _proof_field(proof, "authenticated_at", "authenticatedAt")
        if authenticated_at is None:
            raise CanaryInputError("la evidencia carece de authenticated_at")
        return cls(
            provider=_provider_name(provider),
            session_id=str(_proof_field(proof, "session_id", "sessionId") or ""),
            account_id=str(_proof_field(proof, "account_id", "accountId") or ""),
            environment=str(_proof_field(proof, "environment", "mode") or ""),
            endpoint=str(_proof_field(proof, "endpoint", "host") or ""),
            scopes=_scopes(_proof_field(proof, "scopes", "permissions")),
            connection_generation=str(generation),
            authenticated_at=authenticated_at,
            expires_at=_proof_field(proof, "expires_at", "expiresAt"),
            evidence_source=str(_proof_field(proof, "evidence_source", "source") or type(proof).__name__),
        )

    def validate_current(self, provider: Any, now: datetime) -> None:
        """Validate this typed proof against the same provider process."""

        if _provider_name(provider) != self.provider:
            raise CanaryInputError("provider de la evidencia no coincide")
        generation = getattr(provider, "generation", None)
        if generation is None or str(generation) != self.connection_generation:
            raise CanaryInputError("la generación del provider cambió")
        status = getattr(provider, "status", None)
        if status is not None:
            if _connection_value(status) not in {"CONNECTED", "HEALTHY"}:
                raise CanaryInputError("el provider ya no está conectado")
            auth = _auth_value(status)
            if auth is not None and auth != "AUTHENTICATED":
                raise CanaryInputError("el provider ya no está autenticado")
        current = _aware(now, "clock")
        if current < self.authenticated_at:
            raise CanaryInputError("la evidencia de sesión es futura")
        if self.expires_at is not None and current >= self.expires_at:
            raise CanaryInputError("la evidencia de sesión expiró")

    def runner_provenance(self) -> dict[str, Any]:
        """Return non-authoritative metadata for the existing watch runner."""

        return {
            "provider": self.provider,
            "source_mode": "LIVE",
            "synthetic": False,
            "environment": self.environment,
            "network_performed": True,
            "execution_enabled": False,
            "session_id": self.session_id,
            "connection_generation": self.connection_generation,
            "account_id": self.account_id,
            "endpoint": self.endpoint,
            "data_identity": {
                "provider": self.provider,
                "session_id": self.session_id,
                "connection_generation": self.connection_generation,
                "account_id": self.account_id,
                "endpoint": self.endpoint,
            },
        }


@dataclass(frozen=True, slots=True)
class CanaryCaptureBounds:
    """Hard limits for one input collection invocation."""

    deadline: datetime
    max_events: int
    window_start: datetime
    window_end: datetime
    order_window_start: datetime | None = None
    order_window_end: datetime | None = None

    def __post_init__(self) -> None:
        deadline = _aware(self.deadline, "deadline")
        start = _aware(self.window_start, "window_start")
        end = _aware(self.window_end, "window_end")
        order_start = _aware(self.order_window_start or start, "order_window_start")
        order_end = _aware(self.order_window_end or end, "order_window_end")
        if isinstance(self.max_events, bool) or not isinstance(self.max_events, int) or self.max_events <= 0:
            raise ValueError("max_events debe ser entero positivo")
        if end <= start:
            raise ValueError("window_end debe ser posterior a window_start")
        if deadline > end:
            raise ValueError("deadline no puede exceder window_end")
        if order_start < start or order_end > end or order_end <= order_start:
            raise ValueError("la ventana de orden debe estar contenida en la captura")
        object.__setattr__(self, "deadline", deadline)
        object.__setattr__(self, "window_start", start)
        object.__setattr__(self, "window_end", end)
        object.__setattr__(self, "order_window_start", order_start)
        object.__setattr__(self, "order_window_end", order_end)

    def to_dict(self) -> dict[str, Any]:
        return {
            "deadline": _iso(self.deadline),
            "max_events": self.max_events,
            "window_start": _iso(self.window_start),
            "window_end": _iso(self.window_end),
            "order_window_start": _iso(self.order_window_start or self.window_start),
            "order_window_end": _iso(self.order_window_end or self.window_end),
        }


@dataclass(frozen=True, slots=True)
class ObservedBBO:
    """One provider-normalized, causally available bid/ask pair."""

    symbol: str
    symbol_id: int
    bid: Decimal
    ask: Decimal
    event_time: datetime
    available_at: datetime
    received_at: datetime | None
    sequence: int | str | None
    connection_generation: str
    source_event_id: str
    source: str
    quality_state: str = "VALID"
    quote_usable: bool = True
    synthetic: bool = False
    zero_spread_authorization: CanaryZeroSpreadAuthorization | None = None
    zero_spread_evidence: CanarySessionEvidence | None = None
    zero_spread_observed_at: datetime | None = None
    earliest_available_at: datetime | None = None

    def __post_init__(self) -> None:  # noqa: C901 - one normalized BBO/authentication gate
        symbol = str(self.symbol).strip().upper().replace("-", "/")
        if not symbol or isinstance(self.symbol_id, bool) or int(self.symbol_id) <= 0:
            raise ValueError("BBO sin identidad de instrumento")
        bid = _decimal(self.bid, "bid", positive=True)
        ask = _decimal(self.ask, "ask", positive=True)
        if bid > ask:
            raise ValueError("BBO cruzado o spread cero")
        event_time = _aware(self.event_time, "event_time")
        available_at = _aware(self.available_at, "available_at")
        received_at = _aware(self.received_at, "received_at") if self.received_at is not None else None
        earliest_available_at = _aware(self.earliest_available_at or available_at, "earliest_available_at")
        if earliest_available_at > available_at:
            raise ValueError("earliest_available_at no puede ser posterior a available_at")
        if available_at < event_time or (received_at is not None and received_at < event_time):
            raise ValueError("BBO con disponibilidad no causal")
        generation = str(self.connection_generation).strip()
        source_event_id = str(self.source_event_id).strip()
        source = str(self.source).strip()
        if not generation or not source_event_id or not source:
            raise ValueError("BBO sin procedencia")
        if str(self.quality_state).upper() != "VALID" or self.quote_usable is not True or self.synthetic:
            raise ValueError("BBO no es observado VALID")
        authorization = self.zero_spread_authorization
        evidence = self.zero_spread_evidence
        observed_at = self.zero_spread_observed_at
        if bid == ask:
            if not isinstance(authorization, CanaryZeroSpreadAuthorization):
                raise ValueError("BBO cruzado o spread cero")
            if not isinstance(evidence, CanarySessionEvidence) or observed_at is None:
                raise ValueError("BBO zero-spread sin contexto DEMO tipado")
            observed_at = _aware(observed_at, "zero_spread_observed_at")
            if not authorization.matches(
                account_id=evidence.account_id,
                session_id=evidence.session_id,
                connection_generation=generation,
                endpoint=evidence.endpoint,
                symbol=symbol,
                now=observed_at,
                require_order_window=False,
            ):
                raise ValueError("BBO zero-spread fuera del contexto autorizado")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "symbol_id", int(self.symbol_id))
        object.__setattr__(self, "bid", bid)
        object.__setattr__(self, "ask", ask)
        object.__setattr__(self, "event_time", event_time)
        object.__setattr__(self, "available_at", available_at)
        object.__setattr__(self, "received_at", received_at)
        object.__setattr__(self, "connection_generation", generation)
        object.__setattr__(self, "source_event_id", source_event_id)
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "quality_state", "VALID")
        object.__setattr__(self, "zero_spread_observed_at", observed_at)
        object.__setattr__(self, "earliest_available_at", earliest_available_at)

    @staticmethod
    def _zero_spread_metadata_allowed(
        metadata: Mapping[str, Any],
        authorization: CanaryZeroSpreadAuthorization | None,
        *,
        evidence: CanarySessionEvidence,
        symbol: str,
        now: datetime,
    ) -> bool:
        """Require provider evidence plus the typed authorization scope."""

        if not isinstance(authorization, CanaryZeroSpreadAuthorization):
            return False
        if metadata.get("canary_zero_spread_authorized") is not True:
            return False
        if metadata.get("canary_zero_spread_approval_digest") != authorization.approval_digest:
            return False
        if str(metadata.get("canary_zero_spread_raw_relation", "")).upper() != "BID_EQUALS_ASK":
            return False
        return authorization.matches(
            account_id=evidence.account_id,
            session_id=evidence.session_id,
            connection_generation=evidence.connection_generation,
            endpoint=evidence.endpoint,
            symbol=symbol,
            now=now,
            require_order_window=False,
        )

    @staticmethod
    def _provider_earliest_available_at(metadata: Mapping[str, Any], event: Event, available_at: datetime) -> datetime:
        """Keep the older observed leg; never infer freshness from one leg."""

        bid_raw = metadata.get("bid_available_at")
        ask_raw = metadata.get("ask_available_at")
        shared_generation = metadata.get("connection_generation")
        bid_generation = metadata.get("bid_connection_generation", metadata.get("bid_generation"))
        ask_generation = metadata.get("ask_connection_generation", metadata.get("ask_generation"))
        if bid_generation is not None or ask_generation is not None:
            if bid_generation is None or ask_generation is None or shared_generation is None:
                raise CanaryInputError("BBO sin generación por pierna completa")
            if str(bid_generation) != str(ask_generation) or str(bid_generation) != str(shared_generation):
                raise CanaryInputError("BBO con generaciones por pierna incompatibles")
        bid_sequence = metadata.get("bid_source_sequence", metadata.get("bid_sequence"))
        ask_sequence = metadata.get("ask_source_sequence", metadata.get("ask_sequence"))
        if (bid_sequence is not None or ask_sequence is not None) and (
            bid_sequence is None or ask_sequence is None or str(bid_sequence) != str(ask_sequence)
        ):
            raise CanaryInputError("BBO con secuencias por pierna incompatibles")
        if bid_raw is not None or ask_raw is not None:
            if bid_raw is None or ask_raw is None:
                raise CanaryInputError("BBO sin disponibilidad observada para ambas piernas")
            bid_available = _parse_time(bid_raw, "bid_available_at")
            ask_available = _parse_time(ask_raw, "ask_available_at")
            if bid_available > available_at or ask_available > available_at:
                raise CanaryInputError("BBO tiene disponibilidad por pierna inconsistente")
            return min(bid_available, ask_available)
        if metadata.get("partial_update") is False and event.available_at is not None:
            # Both legs were updated in the same provider frame.  The shared
            # availability is then an explicit source fact, not an async-book
            # freshness fallback.
            return available_at
        raise CanaryInputError("BBO sin disponibilidad por pierna verificable")

    @classmethod
    def from_event(  # noqa: C901 - one normalized-BBO quality gate
        cls,
        event: Event,
        *,
        evidence: CanarySessionEvidence,
        now: datetime,
        window_start: datetime,
        window_end: datetime,
        max_age_seconds: Decimal,
        zero_spread_authorization: CanaryZeroSpreadAuthorization | None = None,
    ) -> ObservedBBO:
        if not isinstance(event, Event):
            raise CanaryInputError("BBO no proviene de Event normalizado")
        metadata = dict(event.metadata) if isinstance(event.metadata, Mapping) else {}
        if event.synthetic or event.is_snapshot:
            raise CanaryInputError("BBO sintético/snapshot no es elegible")
        if event.bid is None or event.ask is None or event.bid > event.ask:
            raise CanaryInputError("BBO incompleto o cruzado")
        if str(metadata.get("quality_state", "")).upper() != "VALID":
            raise CanaryInputError("BBO sin quality_state=VALID")
        if metadata.get("quote_usable") is not True or bool(metadata.get("partial_update", False)):
            raise CanaryInputError("BBO parcial/no utilizable")
        event_time = _aware(event.event_time, "event_time")
        available_at = event.available_at
        if available_at is None:
            raise CanaryInputError("BBO sin available_at observado")
        available_at = _aware(available_at, "available_at")
        current = _aware(now, "clock")
        start = _aware(window_start, "window_start")
        end = _aware(window_end, "window_end")
        if available_at < start or available_at >= end:
            raise CanaryInputError("BBO fuera de la ventana aprobada")
        age = (current - available_at).total_seconds()
        if age < 0:
            raise CanaryInputError("BBO tiene disponibilidad futura")
        if age > float(max_age_seconds):
            raise CanaryInputError("BBO DEMO stale")
        generation = metadata.get("connection_generation")
        if generation is None or str(generation) != evidence.connection_generation:
            raise CanaryInputError("BBO pertenece a otra generación")
        symbol_id = metadata.get("symbol_id")
        symbol = str(metadata.get("symbol", event.instrument)).strip().upper().replace("-", "/")
        if isinstance(symbol_id, bool) or not isinstance(symbol_id, int) or symbol_id <= 0:
            raise CanaryInputError("BBO requiere la identidad tipada del provider")
        if event.bid == event.ask and not cls._zero_spread_metadata_allowed(
            metadata,
            zero_spread_authorization,
            evidence=evidence,
            symbol=symbol,
            now=current,
        ):
            raise CanaryInputError("BBO spread cero requiere contexto DEMO tipado y metadata efectiva")
        return cls(
            symbol=symbol,
            symbol_id=symbol_id,
            bid=Decimal(str(event.bid)),
            ask=Decimal(str(event.ask)),
            event_time=event_time,
            available_at=available_at,
            received_at=event.received_at,
            sequence=event.source_sequence,
            connection_generation=str(generation),
            source_event_id=event.source_event_id or event.data_id,
            source=event.source,
            zero_spread_authorization=zero_spread_authorization,
            zero_spread_evidence=evidence,
            zero_spread_observed_at=current,
            earliest_available_at=available_at,
        )

    @classmethod
    def from_provider_event(  # noqa: C901 - one provider identity/BBO gate
        cls,
        event: Event,
        *,
        provider: Any,
        evidence: CanarySessionEvidence,
        now: datetime,
        window_start: datetime,
        window_end: datetime,
        max_age_seconds: Decimal,
        zero_spread_authorization: CanaryZeroSpreadAuthorization | None = None,
    ) -> ObservedBBO:
        if not isinstance(event, Event):
            raise CanaryInputError("BBO no proviene de Event normalizado")
        metadata = dict(event.metadata) if isinstance(event.metadata, Mapping) else {}
        spec = getattr(provider, "spec", None)
        symbol = str(getattr(spec, "symbol", "")).strip().upper().replace("-", "/")
        symbol_id = getattr(spec, "symbol_id", None)
        if not symbol or isinstance(symbol_id, bool) or not isinstance(symbol_id, int) or symbol_id <= 0:
            raise CanaryInputError("provider sin spec de instrumento observada")
        if event.instrument.upper().replace("-", "/") != symbol:
            raise CanaryInputError("BBO de otro instrumento")
        if event.synthetic or event.is_snapshot:
            raise CanaryInputError("BBO sintético/snapshot no es elegible")
        if event.bid is None or event.ask is None or event.bid > event.ask:
            raise CanaryInputError("BBO incompleto o cruzado")
        if str(metadata.get("quality_state", "")).upper() != "VALID":
            raise CanaryInputError("BBO sin quality_state=VALID")
        if metadata.get("quote_usable") is not True or bool(metadata.get("partial_update", False)):
            raise CanaryInputError("BBO parcial/no utilizable")
        available_at = event.available_at
        if available_at is None:
            raise CanaryInputError("BBO sin available_at observado")
        current = _aware(now, "clock")
        start = _aware(window_start, "window_start")
        end = _aware(window_end, "window_end")
        available_at = _aware(available_at, "available_at")
        earliest_available_at = cls._provider_earliest_available_at(metadata, event, available_at)
        if available_at < start or available_at >= end:
            raise CanaryInputError("BBO fuera de la ventana aprobada")
        age = (current - available_at).total_seconds()
        if age < 0:
            raise CanaryInputError("BBO tiene disponibilidad futura")
        if age > float(max_age_seconds):
            raise CanaryInputError("BBO DEMO stale")
        earliest_age = (current - earliest_available_at).total_seconds()
        if earliest_age < 0:
            raise CanaryInputError("BBO tiene disponibilidad futura en una pierna")
        if earliest_age > float(max_age_seconds):
            raise CanaryInputError("BBO DEMO stale en la pierna más antigua")
        if earliest_available_at < start or earliest_available_at >= end:
            raise CanaryInputError("BBO fuera de ventana por la pierna más antigua")
        generation = metadata.get("connection_generation")
        if generation is None or str(generation) != evidence.connection_generation:
            raise CanaryInputError("BBO pertenece a otra generación")
        source_event_id = event.source_event_id or event.data_id
        if event.bid == event.ask and not cls._zero_spread_metadata_allowed(
            metadata,
            zero_spread_authorization,
            evidence=evidence,
            symbol=symbol,
            now=current,
        ):
            raise CanaryInputError("BBO spread cero requiere contexto DEMO tipado y metadata efectiva")
        observed = cls(
            symbol=symbol,
            symbol_id=symbol_id,
            bid=Decimal(str(event.bid)),
            ask=Decimal(str(event.ask)),
            event_time=event.event_time,
            available_at=available_at,
            received_at=event.received_at,
            sequence=event.source_sequence,
            connection_generation=str(generation),
            source_event_id=source_event_id,
            source=event.source,
            zero_spread_authorization=zero_spread_authorization,
            zero_spread_evidence=evidence,
            zero_spread_observed_at=current,
            earliest_available_at=earliest_available_at,
        )
        return observed

    def to_quote(self, *, evidence: CanarySessionEvidence) -> Quote:
        return Quote(
            self.symbol,
            DecimalValue(self.bid),
            DecimalValue(self.ask),
            self.event_time,
            self.available_at,
            earliest_available_at=self.earliest_available_at,
            quality="VALID",
            source=self.source,
            base_price="bid_ask",
            source_identity=f"quote:{evidence.session_id}:{self.connection_generation}:{self.source_event_id}",
            session_id=evidence.session_id,
            connection_generation=self.connection_generation,
            data_mode="LIVE",
            synthetic=False,
            zero_spread_authorization=self.zero_spread_authorization,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "symbol_id": self.symbol_id,
            "bid": str(self.bid),
            "ask": str(self.ask),
            "event_time": _iso(self.event_time),
            "available_at": _iso(self.available_at),
            "earliest_available_at": _iso(self.earliest_available_at or self.available_at),
            "received_at": _iso(self.received_at) if self.received_at is not None else None,
            "sequence": self.sequence,
            "connection_generation": self.connection_generation,
            "source_event_id": self.source_event_id,
            "source": self.source,
            "quality_state": self.quality_state,
            "quote_usable": self.quote_usable,
            "synthetic": self.synthetic,
            "zero_spread_effective": self.bid == self.ask and self.zero_spread_authorization is not None,
            "zero_spread_authorization": (
                self.zero_spread_authorization.to_dict() if self.zero_spread_authorization is not None else None
            ),
        }


@dataclass(frozen=True, slots=True)
class CanaryManualInstruction:
    """A manual instruction, never a detector signal."""

    direction: str
    signal_id: str
    instrument: str
    runtime: Mapping[str, Any]

    def __post_init__(self) -> None:
        direction = str(self.direction).strip().upper()
        if direction not in {"BUY", "SELL"}:
            raise ValueError("dirección manual inválida")
        signal_id = str(self.signal_id).strip()
        instrument = str(self.instrument).strip().upper().replace("-", "/")
        if not signal_id or not instrument or not isinstance(self.runtime, Mapping):
            raise ValueError("instrucción manual incompleta")
        object.__setattr__(self, "direction", direction)
        object.__setattr__(self, "signal_id", signal_id)
        object.__setattr__(self, "instrument", instrument)
        object.__setattr__(self, "runtime", MappingProxyType(dict(self.runtime)))

    def to_signal(self) -> dict[str, Any]:
        return {
            "signal_id": self.signal_id,
            "instrument": self.instrument,
            "direction": self.direction,
            "mode": "DEMO",
            "account_environment": "DEMO",
            "data_mode": self.runtime.get("data_mode"),
            "manual_canary": True,
            "detected": False,
            "detected_signal": False,
            "signal_origin": "MANUAL_CANARY",
            "source": "MANUAL_CANARY",
            "market_candidate_id": self.runtime.get("market_candidate_id"),
            "atr": self.runtime.get("atr"),
        }


@dataclass(frozen=True, slots=True)
class CanaryInputContext:
    """Opaque READY context consumed by the canary coordinator."""

    provenance: CanarySessionEvidence
    bounds: CanaryCaptureBounds
    runtime_snapshot: Mapping[str, Any]
    quote: Quote
    bbo: ObservedBBO
    buy: CanaryManualInstruction
    sell: CanaryManualInstruction
    watch_result: CTraderWatchResult
    warmup: CTraderWarmupResult | None = None
    technical_only: bool = False
    strategy_ready: bool = True
    missing_warmups: tuple[str, ...] = ()
    runtime_provenance: Mapping[str, Any] = field(default_factory=dict)
    zero_spread_authorization: CanaryZeroSpreadAuthorization | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.provenance, CanarySessionEvidence):
            raise ValueError("contexto sin evidencia tipada")
        if not isinstance(self.bbo, ObservedBBO) or not isinstance(self.quote, Quote):
            raise ValueError("contexto sin BBO/Quote tipados")
        if self.buy.direction != "BUY" or self.sell.direction != "SELL":
            raise ValueError("contexto manual requiere BUY y SELL")
        if self.bbo.bid == self.bbo.ask and self.bbo.zero_spread_authorization != self.zero_spread_authorization:
            raise ValueError("contexto zero-spread no coincide con el BBO observado")
        object.__setattr__(self, "runtime_snapshot", MappingProxyType(dict(self.runtime_snapshot)))
        object.__setattr__(self, "missing_warmups", tuple(str(item) for item in self.missing_warmups))
        object.__setattr__(self, "runtime_provenance", MappingProxyType(dict(self.runtime_provenance)))

    @property
    def buy_signal(self) -> Mapping[str, Any]:
        signal = self.buy.to_signal()
        signal.update({"manual_technical": self.technical_only, "strategy_ready": self.strategy_ready})
        return MappingProxyType(signal)

    @property
    def sell_signal(self) -> Mapping[str, Any]:
        signal = self.sell.to_signal()
        signal.update({"manual_technical": self.technical_only, "strategy_ready": self.strategy_ready})
        return MappingProxyType(signal)

    def to_canary_inputs(self) -> Any:
        """Adapt to ``tools.demo_canary.CanaryInputs`` without JSON roundtrip."""

        from tools.demo_canary import CanaryInputs

        return CanaryInputs(
            self.buy_signal,
            self.runtime_snapshot,
            self.sell_signal,
            self.runtime_snapshot,
        )

    def validate_for_executor(self, executor: Any) -> Mapping[str, Any]:
        """Run the existing in-memory runtime seam; never activates execution."""

        observe = getattr(executor, "observe_runtime", None)
        if not callable(observe):
            raise CanaryInputError("executor sin observe_runtime")
        observed = observe(dict(self.runtime_snapshot))
        validated = _validate_runtime_snapshot(observed, instrument=self.buy.instrument, quote=self.bbo)
        return validated

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": SCHEMA,
            "state": CanaryInputState.READY.value,
            "provenance": {
                "provider": self.provenance.provider,
                "session_id": self.provenance.session_id,
                "connection_generation": self.provenance.connection_generation,
                "account_id": self.provenance.account_id,
                "environment": self.provenance.environment,
                "evidence_source": self.provenance.evidence_source,
            },
            "bounds": self.bounds.to_dict(),
            "runtime": dict(self.runtime_snapshot),
            "bbo": self.bbo.to_dict(),
            "manual_instructions": [self.buy.to_signal(), self.sell.to_signal()],
            "technical_only": self.technical_only,
            "strategy_ready": self.strategy_ready,
            "missing_warmups": list(self.missing_warmups),
            "runtime_provenance": dict(self.runtime_provenance),
            "zero_spread_authorization": (
                self.zero_spread_authorization.to_dict() if self.zero_spread_authorization is not None else None
            ),
            "watch": self.watch_result.to_dict(),
            "warmup": self.warmup.to_dict() if self.warmup is not None else None,
        }


@dataclass(frozen=True, slots=True)
class CanaryInputResult:
    """Bounded result; quality failures are represented as BLOCKED."""

    state: CanaryInputState
    reason: str | None
    gates: Mapping[str, Any] = field(default_factory=dict)
    context: CanaryInputContext | None = None
    messages: int = 0
    events: int = 0

    @property
    def ok(self) -> bool:
        return self.state is CanaryInputState.READY and self.context is not None

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": SCHEMA,
            "state": self.state.value,
            "ok": self.ok,
            "reason": self.reason,
            "gates": dict(self.gates),
            "messages": self.messages,
            "events": self.events,
            "context": self.context.to_dict() if self.context is not None else None,
        }


def _blocked(reason: str, *, gates: Mapping[str, Any] | None = None) -> CanaryInputResult:
    return CanaryInputResult(CanaryInputState.BLOCKED, str(reason), dict(gates or {}))


def _canonical_runtime_path(path: Path) -> bool:
    try:
        resolved = path.expanduser().resolve()
        canonical = _CANONICAL_RUNTIME.expanduser().resolve()
    except OSError:
        return True
    return resolved == canonical or canonical in resolved.parents or resolved in canonical.parents


def _max_quote_age(config: Any, provider: Any) -> Decimal:
    execution = getattr(config, "execution", {})
    quality = getattr(config, "quality", {})
    for source in (execution, quality):
        if isinstance(source, Mapping):
            for name in ("max_price_age_seconds", "max_feed_age_seconds"):
                if source.get(name) is not None:
                    source_age = _decimal(source[name], name, positive=True)
                    return source_age
    fallback_age: Any = getattr(provider, "max_quote_age_seconds", 30)
    return _decimal(fallback_age, "max_quote_age_seconds", positive=True)


def _validate_config_instrument(config: Any, provider: Any) -> str:
    configured = str(getattr(config, "instrument", "")).strip().upper().replace("-", "/")
    spec = getattr(provider, "spec", None)
    observed = str(getattr(spec, "symbol", "")).strip().upper().replace("-", "/")
    if not configured or not observed or configured != observed:
        raise CanaryInputError("instrumento configurado y provider no coinciden")
    return configured


def _validate_runtime_snapshot(  # noqa: C901 - one runtime input quality gate
    snapshot: Any,
    *,
    instrument: str,
    quote: ObservedBBO | None = None,
    expected_candidate: str | None = None,
    technical_only: bool = False,
    producer_metadata: Mapping[str, Any] | None = None,
) -> Mapping[str, Any]:
    data = _mapping(snapshot, "runtime snapshot")
    if data.get("runtime_state") != "VALID":
        raise CanaryInputError("runtime snapshot no está VALID")
    missing = [name for name in _REQUIRED_RUNTIME_FIELDS if data.get(name) is None]
    if missing:
        raise CanaryInputError(f"runtime snapshot carece de {missing[0]}")
    if str(data.get("data_mode", "")).upper() not in {"LIVE", "DEMO_OBSERVED"}:
        raise CanaryInputError("runtime snapshot no proviene de datos DEMO observados")
    if str(data.get("risk_bar_clock_basis", "")).upper() != _RISK_BAR_CLOCK_BASIS:
        raise CanaryInputError("runtime snapshot usa un reloj de riesgo no admitido")
    if technical_only:
        if not isinstance(producer_metadata, Mapping):
            raise CanaryInputError("runtime técnico sin metadata del producer")
        if producer_metadata.get("atr_period") != 14 or producer_metadata.get("trigger_timeframe") != "M1":
            raise CanaryInputError("runtime técnico requiere metadata efectiva ATR14/M1")
        if str(data.get("trigger_timeframe", "")).upper() != "M1":
            raise CanaryInputError("runtime técnico requiere trigger_timeframe=M1")
    if expected_candidate is not None and str(data.get("market_candidate_id")) != expected_candidate:
        raise CanaryInputError("runtime snapshot usa otro market_candidate_id")
    count = data.get("trigger_bar_count")
    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        raise CanaryInputError("runtime trigger_bar_count inválido")
    atr = _decimal(data.get("atr"), "runtime atr", positive=True)
    start = _parse_time(data.get("latest_trigger_start"), "latest_trigger_start")
    end = _parse_time(data.get("latest_trigger_end"), "latest_trigger_end")
    available = _parse_time(data.get("latest_trigger_available_at"), "latest_trigger_available_at")
    market = _parse_time(data.get("last_market"), "last_market")
    last_available = _parse_time(data.get("last_available"), "last_available")
    if end <= start or available < end or last_available < market:
        raise CanaryInputError("runtime snapshot conserva tiempos no causales")
    if quote is not None and last_available < quote.available_at:
        raise CanaryInputError("ATR/runtime es anterior al BBO observado")
    normalized = dict(data)
    normalized["instrument"] = instrument
    normalized["atr"] = str(atr)
    normalized["runtime_state"] = "VALID"
    return MappingProxyType(normalized)


def _strategy_warmup_reasons(reasons: Sequence[str]) -> tuple[str, ...]:
    """Return only the processor's explicit indicator warmup blockers."""

    allowed = {"M1", "M5", "M15"}
    result = tuple(
        reason
        for reason in reasons
        if isinstance(reason, str) and reason.startswith("warmup:") and reason.split(":", 1)[1] in allowed
    )
    return result if len(result) == len(tuple(reasons)) else ()


def _missing_warmups(coordinator: Any) -> tuple[str, ...]:
    processor = getattr(coordinator, "processor", None)
    status = getattr(processor, "status", None)
    raw = status.get("warmup_pending", {}) if isinstance(status, Mapping) else {}
    if not isinstance(raw, Mapping):
        return ()
    return tuple(
        f"{str(timeframe).upper()}:{int(pending)}"
        for timeframe, pending in raw.items()
        if isinstance(pending, int) and not isinstance(pending, bool) and pending > 0
    )


def _validate_runtime_producer(  # noqa: C901 - one producer/config identity gate
    coordinator: Any,
    config: Any,
    snapshot: Mapping[str, Any],
    *,
    technical_only: bool,
) -> dict[str, Any]:
    """Bind the public snapshot to the effective coordinator/config producer.

    The snapshot intentionally does not carry an ``atr_period`` field.  This
    seam therefore verifies the actual processor/config objects that produced
    it and records their effective hashes locally, rather than trusting a
    caller-supplied raw mapping to attest to ATR14/M1.
    """

    processor = getattr(coordinator, "processor", None)
    if processor is None:
        raise CanaryInputError("runtime producer sin processor verificable")
    config_indicators = getattr(config, "indicators", None)
    strategy = getattr(config, "strategy", None)
    strategy_indicators = getattr(strategy, "indicators", None)
    processor_indicators = getattr(processor, "indicator_config", None)
    if processor_indicators is None:
        processor_indicators = getattr(getattr(processor, "strategy_config", None), "indicators", None)
    if config_indicators is None or strategy_indicators is None or processor_indicators is None:
        raise CanaryInputError("config/processor sin indicator_config efectivo")
    indicator_names = ("ema_fast", "ema_slow", "rsi_period", "atr_period", "wilder")
    config_values = {name: getattr(config_indicators, name, None) for name in indicator_names}
    strategy_values = {name: getattr(strategy_indicators, name, None) for name in indicator_names}
    processor_values = {name: getattr(processor_indicators, name, None) for name in indicator_names}
    if config_values != strategy_values or config_values != processor_values:
        raise CanaryInputError("snapshot no coincide con el indicator_config efectivo")
    expected_trigger = parse_timeframe(getattr(strategy, "trigger_timeframe", "")).name
    if str(snapshot.get("trigger_timeframe", "")).upper() != expected_trigger:
        raise CanaryInputError("snapshot trigger_timeframe no coincide con la configuración efectiva")
    if technical_only and (expected_trigger != "M1" or config_values.get("atr_period") != 14):
        raise CanaryInputError("canary técnico requiere ATR14/M1 efectivo")
    expected_candidate = _config_candidate(config)
    processor_candidate = str(getattr(processor, "market_candidate_id", "") or "").strip()
    if expected_candidate is None or processor_candidate != expected_candidate:
        raise CanaryInputError("snapshot candidate no coincide con el producer efectivo")
    latest = processor.latest_indicator_point(expected_trigger, at=processor.last_available_at)
    if latest is None or latest.atr is None or not latest.quality.valid:
        raise CanaryInputError("producer aún no tiene ATR válido para el timeframe trigger")
    observed_atr = _decimal(snapshot.get("atr"), "snapshot atr", positive=True)
    if observed_atr != _decimal(latest.atr, "producer atr", positive=True):
        raise CanaryInputError("snapshot ATR no coincide con el punto del producer")
    point_end = _aware(latest.end, "producer trigger end")
    point_available = _aware(latest.available_at, "producer trigger availability")
    if _parse_time(snapshot.get("latest_trigger_end"), "latest_trigger_end") != point_end:
        raise CanaryInputError("snapshot trigger end no coincide con el producer")
    if _parse_time(snapshot.get("latest_trigger_available_at"), "latest_trigger_available_at") != point_available:
        raise CanaryInputError("snapshot trigger availability no coincide con el producer")
    return {
        "producer": "RuntimeCoordinator.market_profile_snapshot",
        "config_hash": getattr(config, "config_hash", None),
        "indicator_config_hash": fingerprint(config_values),
        "atr_period": config_values["atr_period"],
        "trigger_timeframe": expected_trigger,
        "market_candidate_id": expected_candidate,
        "atr_observed": True,
    }


class _InputWatchRunner(CTraderWatchRunner):
    """Watch runner seam that retains only the latest valid normalized BBO."""

    def __init__(
        self,
        *args: Any,
        input_evidence: CanarySessionEvidence,
        bounds: CanaryCaptureBounds,
        max_age: Decimal,
        technical_only: bool = False,
        runtime_observer: RuntimeObserver | None = None,
        expected_candidate: str | None = None,
        zero_spread_authorization: CanaryZeroSpreadAuthorization | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.input_evidence = input_evidence
        self.input_bounds = bounds
        self.input_max_age = max_age
        self.technical_only = technical_only
        self.runtime_observer = runtime_observer
        self.expected_candidate = expected_candidate
        self.zero_spread_authorization = zero_spread_authorization
        self.latest_bbo: ObservedBBO | None = None
        self.bbo_rejections: list[str] = []
        self.technical_runtime_snapshot: Mapping[str, Any] | None = None
        self.technical_runtime_provenance: Mapping[str, Any] = {}
        self.technical_ready = False

    def _process_record(self, record: Event | Bar, raw_synthetic: bool) -> bool:
        accepted = super()._process_record(record, raw_synthetic)
        if isinstance(self._last_decorated_record, Event):
            try:
                candidate = ObservedBBO.from_provider_event(
                    self._last_decorated_record,
                    provider=self.context.provider,
                    evidence=self.input_evidence,
                    now=self._clock(),
                    window_start=self.input_bounds.order_window_start or self.input_bounds.window_start,
                    window_end=self.input_bounds.order_window_end or self.input_bounds.window_end,
                    max_age_seconds=self.input_max_age,
                    zero_spread_authorization=self.zero_spread_authorization,
                )
            except (CanaryInputError, TypeError, ValueError) as exc:
                reason = str(exc) or type(exc).__name__
                if reason not in self.bbo_rejections and len(self.bbo_rejections) < 8:
                    self.bbo_rejections.append(reason[:160])
            else:
                self.latest_bbo = candidate
        if self.technical_only and self.latest_bbo is not None and callable(self.runtime_observer):
            try:
                raw_runtime = self.coordinator.market_profile_snapshot()
                if raw_runtime is not None:
                    producer_metadata = _validate_runtime_producer(
                        self.coordinator,
                        self.context.config,
                        raw_runtime,
                        technical_only=True,
                    )
                    observed_runtime = self.runtime_observer(raw_runtime)
                    self.technical_runtime_snapshot = _validate_runtime_snapshot(
                        observed_runtime,
                        instrument=str(self.context.config.instrument),
                        quote=self.latest_bbo,
                        expected_candidate=self.expected_candidate,
                        technical_only=True,
                        producer_metadata=producer_metadata,
                    )
                    self.technical_runtime_provenance = producer_metadata
                    self.technical_ready = True
            except (CanaryInputError, TypeError, ValueError):
                # A later event may complete ATR/clock/quote causality.  Do
                # not turn a transient warmup state into a fatal watch error.
                self.technical_runtime_snapshot = None
                self.technical_runtime_provenance = {}
        return accepted

    def _poll_stop_reason(self, started: float) -> WatchStopReason | None:
        if self.technical_only and self.technical_ready:
            return WatchStopReason.STOP_REQUESTED
        return super()._poll_stop_reason(started)


def _validate_warmup(  # noqa: C901 - one native warmup causality gate
    result: CTraderWarmupResult, config: Any, instrument: str
) -> None:
    if not isinstance(result, CTraderWarmupResult):
        raise CanaryInputError("warmup debe ser CTraderWarmupResult tipado")
    expected = {str(item.name).upper() for item in getattr(config, "timeframes", ())}
    if not expected:
        raise CanaryInputError("configuración sin temporalidades")
    for timeframe, raw_bars in result.bars.items():
        normalized = str(timeframe).upper()
        if normalized not in expected:
            raise CanaryInputError(f"warmup timeframe no configurado: {normalized}")
        bars = tuple(raw_bars)
        previous: Bar | None = None
        for bar in bars:
            if not isinstance(bar, Bar):
                raise CanaryInputError(f"warmup {normalized} contiene registro no-Bar")
            if bar.instrument != instrument or bar.timeframe != normalized:
                raise CanaryInputError(f"warmup {normalized} usa instrumento/temporalidad distintos")
            if bar.synthetic or bar.price_basis != "native" or not bar.closed or bar.available_at is None:
                raise CanaryInputError(f"warmup {normalized} no es native cerrado observado")
            if bar.available_at < bar.interval_end:
                raise CanaryInputError(f"warmup {normalized} no acredita disponibilidad causal")
            if previous is not None and bar.interval_start != previous.interval_end:
                raise CanaryInputError(f"warmup {normalized} tiene hueco/reordenamiento")
            previous = bar
    if not result.bars:
        raise CanaryInputError("warmup nativo vacío")


def _config_candidate(config: Any) -> str | None:
    execution = getattr(config, "execution", {})
    if isinstance(execution, Mapping) and execution.get("market_candidate_id") is not None:
        value = str(execution.get("market_candidate_id")).strip()
        return value or None
    return None


def _manual_id(direction: str, evidence: CanarySessionEvidence, bbo: ObservedBBO) -> str:
    token = f"{evidence.session_id}|{evidence.connection_generation}|{bbo.source_event_id}|{direction}"
    return "manual-canary-" + hashlib.sha256(token.encode("utf-8")).hexdigest()[:24]


def collect_canary_inputs(  # noqa: C901 - one bounded observed-input orchestration gate
    provider: Any,
    config: EffectiveConfig,
    *,
    network: bool,
    deadline: datetime,
    max_events: int,
    window_start: datetime,
    window_end: datetime,
    preparation_start: datetime | None = None,
    session_evidence: CanarySessionEvidence | None = None,
    runtime_observer: RuntimeObserver | None = None,
    executor: Any | None = None,
    risk_planner: RiskPlanner | None = None,
    requested_quantity: Decimal | None = None,
    warmup: CTraderWarmupResult | None = None,
    fetch_warmup: bool = True,
    technical_only: bool = False,
    zero_spread_authorization: CanaryZeroSpreadAuthorization | None = None,
    minimum_execution_margin_seconds: Decimal | int | float = Decimal("300"),
    warmup_cutoff: datetime | None = None,
    market_window_state: MarketWindowObserver | None = None,
    isolated_state_dir: str | Path | None = None,
    clock: Callable[[], datetime] | None = None,
    monotonic: Callable[[], float] | None = None,
    close_provider: bool = False,
) -> CanaryInputResult:
    """Collect one bounded, observed canary context.

    ``network=True`` is an explicit call-site gate.  The function still never
    authenticates, creates a client, subscribes a second reader, activates an
    executor or sends an order.  The supplied provider is the already prepared
    session; history and live events use that same provider/client.
    """

    gates: dict[str, Any] = {
        "network_requested": bool(network),
        "execution_enabled": False,
        "orders_attempted": False,
        "second_provider_created": False,
        "isolated_state": True,
        "manual_technical": bool(technical_only),
        "strategy_ready": None,
        "zero_spread_authorization_bound": False,
    }
    if zero_spread_authorization is not None and not isinstance(
        zero_spread_authorization, CanaryZeroSpreadAuthorization
    ):
        return _blocked("TYPED_ZERO_SPREAD_AUTHORIZATION_REQUIRED", gates=gates)
    if zero_spread_authorization is not None and technical_only is not True:
        return _blocked("ZERO_SPREAD_REQUIRES_TECHNICAL_CANARY", gates=gates)
    if network is not True:
        return _blocked("EXPLICIT_NETWORK_GATE_REQUIRED", gates=gates)
    if isinstance(provider, Mapping) or provider is None:
        return _blocked("TYPED_PROVIDER_REQUIRED", gates=gates)
    try:
        order_start = _aware(window_start, "window_start")
        order_end = _aware(window_end, "window_end")
        capture_start = _aware(preparation_start, "preparation_start") if preparation_start is not None else order_start
        preparation_seconds = (order_start - capture_start).total_seconds()
        if preparation_seconds < 0 or preparation_seconds > 300:
            return _blocked("PREPARATION_WINDOW_INVALID", gates=gates)
        bounds = CanaryCaptureBounds(
            deadline,
            max_events,
            capture_start,
            order_end,
            order_window_start=order_start,
            order_window_end=order_end,
        )
        gates["preparation_start"] = _iso(capture_start)
        gates["order_window_start"] = _iso(order_start)
        gates["order_window_end"] = _iso(order_end)
        clock_fn = clock or (lambda: datetime.now(UTC))
        start_now = _aware(clock_fn(), "clock")
        if start_now < bounds.window_start:
            return _blocked("WINDOW_NOT_STARTED", gates={**gates, "window": "NOT_STARTED"})
        if start_now >= bounds.window_end:
            return _blocked("MARKET_WINDOW_CLOSED", gates={**gates, "window": "CLOSED"})
        if start_now >= bounds.deadline:
            return _blocked("DEADLINE_EXPIRED", gates=gates)
        instrument = _validate_config_instrument(config, provider)
        minimum_margin = _decimal(
            minimum_execution_margin_seconds,
            "minimum_execution_margin_seconds",
            positive=False,
        )
        if minimum_margin < 0:
            return _blocked("EXECUTION_MARGIN_INVALID", gates=gates)
        if technical_only and str(getattr(config, "price_base", "")).lower() in {"mid", "bid", "ask"}:
            if fetch_warmup or warmup is not None:
                return _blocked("TECHNICAL_MID_REQUIRES_FETCH_WARMUP_FALSE", gates=gates)
            indicators = getattr(config, "indicators", None)
            strategy = getattr(config, "strategy", None)
            if getattr(indicators, "atr_period", None) != 14 or str(
                getattr(strategy, "trigger_timeframe", "")
            ).upper() not in {"M1", "1M"}:
                return _blocked("TECHNICAL_CONFIG_REQUIRES_ATR14_M1", gates=gates)
        evidence = session_evidence or CanarySessionEvidence.from_provider(provider)
        if not isinstance(evidence, CanarySessionEvidence):
            return _blocked("TYPED_SESSION_EVIDENCE_REQUIRED", gates=gates)
        evidence.validate_current(provider, start_now)
        if zero_spread_authorization is not None:
            if (
                zero_spread_authorization.preparation_start != bounds.window_start
                or zero_spread_authorization.window_start != bounds.order_window_start
                or zero_spread_authorization.window_end != bounds.order_window_end
                or not zero_spread_authorization.matches(
                    account_id=evidence.account_id,
                    session_id=evidence.session_id,
                    connection_generation=evidence.connection_generation,
                    endpoint=evidence.endpoint,
                    symbol=instrument,
                    now=start_now,
                    require_order_window=False,
                )
            ):
                return _blocked("ZERO_SPREAD_AUTHORIZATION_CONTEXT_MISMATCH", gates=gates)
            setter = getattr(provider, "set_canary_zero_spread_authorization", None)
            if not callable(setter):
                return _blocked("ZERO_SPREAD_PROVIDER_SCOPE_UNAVAILABLE", gates=gates)
            try:
                setter(zero_spread_authorization)
            except Exception as exc:
                return _blocked(f"ZERO_SPREAD_PROVIDER_SCOPE_BLOCKED:{type(exc).__name__}", gates=gates)
            gates["zero_spread_authorization_bound"] = True
            gates["zero_spread_authorization"] = zero_spread_authorization.to_dict()
        gates.update(
            {
                "session_observed": True,
                "provider": evidence.provider,
                "session_id_observed": True,
                "generation_observed": evidence.connection_generation,
                "accounts_scope": "accounts" in evidence.scopes,
                "trading_scope": "trading" in evidence.scopes,
            }
        )
        if market_window_state is None:
            from .market_schedule import observed_market_window_state

            market_window_state = observed_market_window_state
        try:
            state = market_window_state(provider, bounds.window_start, bounds.window_end)
        except Exception as exc:
            return _blocked(f"MARKET_WINDOW_UNVERIFIED:{type(exc).__name__}", gates=gates)
        if str(getattr(state, "value", state)).strip().upper() != "OPEN":
            return _blocked(f"MARKET_WINDOW_{str(getattr(state, 'value', state) or 'UNKNOWN').upper()}", gates=gates)
        gates["market_window"] = "OPEN"
        max_age = _max_quote_age(config, provider)
        if warmup is None and fetch_warmup:
            try:
                warmup = fetch_causal_warmup(
                    provider,
                    config,
                    cutoff=_aware(warmup_cutoff or start_now, "warmup_cutoff"),
                )
                if _aware(clock_fn(), "clock") >= bounds.deadline:
                    return _blocked("DEADLINE_EXPIRED_DURING_WARMUP", gates=gates)
            except (CTraderWarmupError, AttributeError, TypeError, ValueError) as exc:
                return _blocked(f"WARMUP_BLOCKED:{type(exc).__name__}", gates=gates)
        if warmup is not None:
            _validate_warmup(warmup, config, instrument)
            gates["native_warmup_observed"] = True
        elif fetch_warmup:
            return _blocked("WARMUP_REQUIRED", gates=gates)
        observer: RuntimeObserver | None = runtime_observer
        if observer is None and executor is not None:
            observer = getattr(executor, "observe_runtime", None)
        if not callable(observer):
            return _blocked("RUNTIME_OBSERVER_REQUIRED", gates=gates)
        preparation_now = _aware(clock_fn(), "clock")
        if preparation_now >= bounds.deadline:
            return _blocked(
                "DEADLINE_EXPIRED_AFTER_PREPARATION",
                gates={**gates, "capture_now": _iso(preparation_now)},
            )
        remaining = (bounds.deadline - preparation_now).total_seconds()
        if remaining <= 0:
            return _blocked("DEADLINE_EXPIRED_AFTER_PREPARATION", gates=gates)
        provenance = evidence.runner_provenance()
        provenance["manual_technical"] = technical_only
        technical_gap = min(Decimal("30"), max_age) if technical_only else None
        if technical_gap is not None:
            gates["technical_canary_quote_gap_seconds"] = str(technical_gap)
        temp_root: tempfile.TemporaryDirectory[str] | None = None
        if isolated_state_dir is not None:
            state_root = Path(isolated_state_dir).expanduser()
            if _canonical_runtime_path(state_root):
                return _blocked("CANONICAL_RUNTIME_FORBIDDEN", gates=gates)
            if not state_root.is_dir():
                return _blocked("ISOLATED_STATE_DIR_REQUIRED", gates=gates)
            temp_root = tempfile.TemporaryDirectory(prefix="mtf-canary-inputs-", dir=str(state_root))
        else:
            temp_root = tempfile.TemporaryDirectory(prefix="mtf-canary-inputs-")
        assert temp_root is not None
        with contextlib.ExitStack() as stack:
            stack.callback(temp_root.cleanup)
            db_path = Path(temp_root.name) / "canary.sqlite3"
            with SQLiteStore(db_path) as store:
                watch_context = CTraderWatchContext(
                    provider=provider,
                    config=config,
                    store=store,
                    provenance=provenance,
                    close_provider=close_provider,
                    paper_enabled=False,
                    bootstrap_bars=(warmup.bars if warmup is not None else {}),
                    bootstrap_metadata=(
                        {"cutoff": _iso(warmup.cutoff), "source": warmup.to_dict()} if warmup is not None else {}
                    ),
                    clock=clock_fn,
                    monotonic=monotonic,
                    zero_spread_authorization=zero_spread_authorization,
                )
                capture_now = _aware(clock_fn(), "clock")
                if capture_now >= bounds.deadline:
                    return _blocked(
                        "DEADLINE_EXPIRED_BEFORE_CAPTURE",
                        gates={**gates, "capture_now": _iso(capture_now)},
                    )
                remaining = (bounds.deadline - capture_now).total_seconds()
                if remaining <= 0:
                    return _blocked(
                        "DEADLINE_EXPIRED_BEFORE_CAPTURE",
                        gates={**gates, "capture_now": _iso(capture_now)},
                    )
                runner = _InputWatchRunner(
                    watch_context,
                    # A technical stream may legitimately be quiet up to its
                    # configured quote-gap bound (for example, 10-second
                    # ticks with a 30-second gate); do not idle out at 5s.
                    CTraderWatchOptions(
                        duration_seconds=remaining,
                        max_events=max_events,
                        idle_timeout_seconds=min(
                            remaining,
                            float(technical_gap) if technical_gap is not None else 5.0,
                        ),
                        poll_timeout_seconds=min(0.25, max(0.01, remaining)),
                        checkpoint_every=max_events,
                        checkpoint_name="ctrader-canary-inputs",
                        resume=False,
                        mode="LIVE",
                        technical_canary_quote_gap_seconds=(
                            float(technical_gap) if technical_gap is not None else None
                        ),
                    ),
                    input_evidence=evidence,
                    bounds=bounds,
                    max_age=max_age,
                    technical_only=technical_only,
                    runtime_observer=observer,
                    expected_candidate=_config_candidate(config),
                    zero_spread_authorization=zero_spread_authorization,
                )
                try:
                    watch_result = runner.run()
                    gates.update(
                        {
                            "watch_stop_reason": watch_result.stop_reason,
                            "watch_reconciliation_state": watch_result.status.get("reconciliation_state"),
                        }
                    )
                except (CTraderWatchError, RuntimeError) as exc:
                    gates["watch_stop_reason"] = "ERROR"
                    if warmup is not None and str(getattr(config, "price_base", "")).lower() in {
                        "mid",
                        "bid",
                        "ask",
                    }:
                        return _blocked(
                            "NATIVE_WARMUP_NOT_SEMANTICALLY_VALID_FOR_QUOTE_PROFILE",
                            gates={**gates, "native_to_quote_conversion": "BLOCKED"},
                        )
                    return _blocked(f"WATCH_BLOCKED:{type(exc).__name__}", gates=gates)
                try:
                    final_now = _aware(clock_fn(), "clock")
                    if final_now >= bounds.deadline:
                        return _blocked(
                            "DEADLINE_EXPIRED_DURING_CAPTURE",
                            gates={**gates, "capture_now": _iso(final_now)},
                        )
                    evidence.validate_current(provider, final_now)
                    if zero_spread_authorization is not None and not zero_spread_authorization.matches(
                        account_id=evidence.account_id,
                        session_id=evidence.session_id,
                        connection_generation=evidence.connection_generation,
                        endpoint=evidence.endpoint,
                        symbol=instrument,
                        now=final_now,
                        require_order_window=False,
                    ):
                        return _blocked("ZERO_SPREAD_AUTHORIZATION_EXPIRED", gates=gates)
                    final_state = market_window_state(provider, bounds.window_start, bounds.window_end)
                    if str(getattr(final_state, "value", final_state)).strip().upper() != "OPEN":
                        return _blocked("MARKET_WINDOW_CLOSED_DURING_CAPTURE", gates=gates)
                    status = runner.coordinator.status()
                    if status.freshness_state != "VALID":
                        return _blocked(
                            f"BBO_QUALITY_{status.freshness_state}",
                            gates={**gates, "watch_status": status.freshness_state},
                        )
                    if status.connection not in {"CONNECTED", "HEALTHY"}:
                        return _blocked("WATCH_DISCONNECTED", gates=gates)
                    if status.reconciliation_state not in {"NOT_APPLICABLE", "VERIFIED", "RECONCILED"}:
                        return _blocked("WATCH_RECONCILIATION_REQUIRED", gates=gates)
                    if status.continuity_state == "BROKEN":
                        return _blocked("WATCH_CONTINUITY_BROKEN", gates=gates)
                    analysis_reasons = tuple(str(item) for item in status.analysis_blocked_reasons)
                    technical_marker = "technical_canary_signals_disabled"
                    if technical_only and technical_marker in analysis_reasons:
                        gates["technical_signals_disabled"] = True
                    effective_analysis_reasons = tuple(
                        reason for reason in analysis_reasons if not (technical_only and reason == technical_marker)
                    )
                    warmup_reasons = _strategy_warmup_reasons(effective_analysis_reasons)
                    missing_warmups = _missing_warmups(runner.coordinator)
                    strategy_ready = not technical_only and not effective_analysis_reasons and not missing_warmups
                    gates.update(
                        {
                            "strategy_ready": strategy_ready,
                            "missing_warmups": list(missing_warmups),
                            "manual_technical": technical_only,
                        }
                    )
                    if effective_analysis_reasons and not (technical_only and warmup_reasons):
                        return _blocked(
                            f"RUNTIME_BLOCKED:{effective_analysis_reasons[0]}",
                            gates={**gates, "analysis_blocked_reasons": list(analysis_reasons)},
                        )
                    if missing_warmups and not warmup_reasons and technical_only:
                        return _blocked("STRATEGY_WARMUP_UNREPORTED", gates=gates)
                    bbo = runner.latest_bbo
                    if bbo is None:
                        reason = runner.bbo_rejections[0] if runner.bbo_rejections else "no_valid_bbo_observed"
                        return _blocked(f"BBO_BLOCKED:{reason}", gates=gates)
                    raw_runtime = runner.coordinator.market_profile_snapshot()
                    if raw_runtime is None:
                        return _blocked("RUNTIME_PROFILE_UNAVAILABLE", gates=gates)
                    candidate = _config_candidate(config)
                    producer_metadata = _validate_runtime_producer(
                        runner.coordinator,
                        config,
                        raw_runtime,
                        technical_only=technical_only,
                    )
                    if technical_only and runner.technical_runtime_snapshot is not None:
                        runtime_snapshot = runner.technical_runtime_snapshot
                    else:
                        observed_runtime = observer(raw_runtime)
                        runtime_snapshot = _validate_runtime_snapshot(
                            observed_runtime,
                            instrument=instrument,
                            quote=bbo,
                            expected_candidate=candidate,
                            technical_only=technical_only,
                            producer_metadata=producer_metadata,
                        )
                    remaining_window = (bounds.window_end - final_now).total_seconds()
                    gates.update(
                        {
                            "execution_window_remaining_seconds": remaining_window,
                            "minimum_execution_margin_seconds": str(minimum_margin),
                        }
                    )
                    if remaining_window < float(minimum_margin):
                        return _blocked("EXECUTION_MARGIN_INSUFFICIENT", gates=gates)
                    if risk_planner is None and executor is not None:
                        risk_planner = getattr(executor, "risk_entry_plan", None)
                    if requested_quantity is not None and callable(risk_planner):
                        quantity = _decimal(requested_quantity, "requested_quantity", positive=True)
                        for instruction in ("BUY", "SELL"):
                            signal = {
                                "signal_id": f"preflight-{instruction.lower()}",
                                "instrument": instrument,
                                "direction": instruction,
                                "manual_canary": True,
                            }
                            try:
                                plan = risk_planner(
                                    signal,
                                    bbo.to_quote(evidence=evidence),
                                    requested_quantity=quantity,
                                )
                            except Exception as exc:
                                return _blocked(f"RISK_PLAN_BLOCKED_{instruction}:{type(exc).__name__}", gates=gates)
                            if plan is None or getattr(plan, "allowed", False) is not True:
                                return _blocked(f"RISK_PLAN_BLOCKED_{instruction}", gates=gates)
                        gates["risk_entry_plan_checked"] = True
                    else:
                        gates["risk_entry_plan_checked"] = False
                    buy = CanaryManualInstruction(
                        "BUY",
                        _manual_id("BUY", evidence, bbo),
                        instrument,
                        runtime_snapshot,
                    )
                    sell = CanaryManualInstruction(
                        "SELL",
                        _manual_id("SELL", evidence, bbo),
                        instrument,
                        runtime_snapshot,
                    )
                    context = CanaryInputContext(
                        evidence,
                        bounds,
                        runtime_snapshot,
                        bbo.to_quote(evidence=evidence),
                        bbo,
                        buy,
                        sell,
                        watch_result,
                        warmup,
                        technical_only,
                        strategy_ready,
                        missing_warmups,
                        producer_metadata,
                        zero_spread_authorization,
                    )
                    gates.update(
                        {
                            "bbo_valid_fresh": True,
                            "runtime_snapshot_valid": True,
                            "manual_instructions": True,
                            "execution_enabled": False,
                        }
                    )
                    return CanaryInputResult(
                        CanaryInputState.READY,
                        None,
                        gates,
                        context,
                        watch_result.messages,
                        watch_result.events,
                    )
                except CanaryInputError as exc:
                    return _blocked(str(exc), gates=gates)
                except (TypeError, ValueError, KeyError) as exc:
                    return _blocked(f"INPUTS_BLOCKED:{type(exc).__name__}", gates=gates)
                except RuntimeError as exc:
                    return _blocked(f"INPUTS_BLOCKED:{type(exc).__name__}", gates=gates)
    except CanaryInputError as exc:
        return _blocked(str(exc), gates=gates)
    except (TypeError, ValueError, OSError, CTraderWarmupError) as exc:
        return _blocked(f"INPUTS_BLOCKED:{type(exc).__name__}", gates=gates)


__all__ = [
    "CanaryCaptureBounds",
    "CanaryInputContext",
    "CanaryInputError",
    "CanaryInputResult",
    "CanaryInputState",
    "CanaryManualInstruction",
    "CanarySessionEvidence",
    "ObservedBBO",
    "collect_canary_inputs",
]
