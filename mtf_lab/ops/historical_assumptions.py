"""Explicit modeled assumptions for the local EUR/USD historical study.

The objects in this module are not broker metadata.  They make the minimum
assumptions needed for a virtual diagnostic reproducible while keeping unknown
contract, fee, margin, exception and receipt facts explicitly unknown.  No
factory has a default model: callers must provide the literal ``model_id``
before a result is produced.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ..core.canonical import canonical_json, instant_text

ASSUMPTIONS_SCHEMA = "mtf-lab.historical-assumptions.v1"
VIRTUAL_EURUSD_10K_MODEL_ID = "eurusd_virtual_10k_unit_value_v1"
EURUSD_VIRTUAL_10K_MODEL_ID = VIRTUAL_EURUSD_10K_MODEL_ID
VIRTUAL_ACCOUNT_ASSUMPTIONS_MODEL_ID = VIRTUAL_EURUSD_10K_MODEL_ID
HISTORICAL_ASSUMPTIONS_MODEL_ID = VIRTUAL_EURUSD_10K_MODEL_ID

PEPPERSTONE_PUBLIC_CALENDAR_MODEL_ID = "pepperstone_public_calendar_template_v1"
PEPPERSTONE_CALENDAR_TEMPLATE_ID = PEPPERSTONE_PUBLIC_CALENDAR_MODEL_ID
CALENDAR_TIMEZONE = "America/New_York"
CALENDAR_BASIS = "MODELED_CURRENT_PUBLIC_PEPPERSTONE_NOT_HISTORICAL_ACCOUNT"
CALENDAR_PROVENANCE_URI = "https://pepperstone.com/en/about-us/trading-hours"
CALENDAR_DOCUMENTED_AT = datetime(2026, 9, 13, tzinfo=UTC)
RISK_BAR_CLOCK_BASIS = "UTC_TIMEFRAME_BOUNDARIES_NO_OHLC_IMPUTATION"


class HistoricalAssumptionError(ValueError):
    """A requested modeled assumption is missing or inconsistent."""


def _decimal(value: Any, *, name: str, positive: bool = False) -> Decimal:
    if isinstance(value, bool):
        raise HistoricalAssumptionError(f"{name} no admite booleanos")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise HistoricalAssumptionError(f"{name} no es Decimal válido") from exc
    if not result.is_finite() or (positive and result <= 0):
        raise HistoricalAssumptionError(f"{name} debe ser finito y positivo")
    return result


def _utc(value: datetime, *, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise HistoricalAssumptionError(f"{name} debe ser datetime aware")
    return value.astimezone(UTC)


def _seconds_between(start: datetime, end: datetime) -> Decimal:
    delta = end - start
    micros = (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
    return Decimal(micros) / Decimal(1_000_000)


def _hash_mapping(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(_jsonable(value)).encode("utf-8")).hexdigest()


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(item) for item in value]
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return instant_text(value)
    if isinstance(value, time):
        return value.isoformat(timespec="seconds")
    return value


@dataclass(frozen=True, slots=True)
class HistoricalEntryGate:
    """Modeled entry gate for a quote; it is never an account observation."""

    model_id: str
    at: datetime
    allowed: bool
    reason: str | None
    preclose_minutes: Decimal
    basis: str = CALENDAR_BASIS

    def to_dict(self) -> dict[str, Any]:
        return {
            "model_id": self.model_id,
            "at": instant_text(self.at),
            "allowed": self.allowed,
            "reason": self.reason,
            "preclose_minutes": format(self.preclose_minutes, "f"),
            "basis": self.basis,
            "observed": False,
        }


@dataclass(frozen=True, slots=True)
class HistoricalCalendarObservation:
    """Dynamic calendar marks generated from one quote timestamp."""

    model_id: str
    at: datetime
    next_financing_at: datetime
    next_weekly_close_at: datetime
    next_weekly_open_at: datetime
    next_daily_break_close_at: datetime
    next_daily_break_open_at: datetime
    known: bool = True
    exceptions_known: bool = False
    financing_amount_known: bool = False
    observed: bool = False
    basis: str = CALENDAR_BASIS
    calendar_hash: str | None = None

    def __post_init__(self) -> None:
        if self.model_id != PEPPERSTONE_PUBLIC_CALENDAR_MODEL_ID or self.known is not True:
            raise HistoricalAssumptionError("calendar model_id no registrado")
        for name in (
            "at",
            "next_financing_at",
            "next_weekly_close_at",
            "next_weekly_open_at",
            "next_daily_break_close_at",
            "next_daily_break_open_at",
        ):
            object.__setattr__(self, name, _utc(getattr(self, name), name=name))
        if self.basis != CALENDAR_BASIS or self.observed:
            raise HistoricalAssumptionError("el calendario histórico debe permanecer modelado/no observado")
        if not isinstance(self.known, bool) or not isinstance(self.exceptions_known, bool):
            raise HistoricalAssumptionError("flags de calendario inválidos")
        if not isinstance(self.financing_amount_known, bool) or not isinstance(self.observed, bool):
            raise HistoricalAssumptionError("flags de financiación inválidos")
        if self.calendar_hash is not None and len(self.calendar_hash) != 64:
            raise HistoricalAssumptionError("calendar_hash inválido")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": ASSUMPTIONS_SCHEMA,
            "model_id": self.model_id,
            "at": instant_text(self.at),
            "next_financing_at": instant_text(self.next_financing_at),
            "next_weekly_close_at": instant_text(self.next_weekly_close_at),
            "next_weekly_open_at": instant_text(self.next_weekly_open_at),
            "next_daily_break_close_at": instant_text(self.next_daily_break_close_at),
            "next_daily_break_open_at": instant_text(self.next_daily_break_open_at),
            "known": self.known,
            "exceptions_known": self.exceptions_known,
            "financing_amount_known": self.financing_amount_known,
            "observed": self.observed,
            "basis": self.basis,
            "calendar_hash": self.calendar_hash,
        }

    def to_calendar_state(self) -> dict[str, Any]:
        """Map to the RiskExit calendar contract without claiming observation."""

        return {
            "known": self.known,
            "financing_known": self.financing_amount_known,
            "exceptions_known": self.exceptions_known,
            "financing_at": instant_text(self.next_financing_at),
            "weekly_close_at": instant_text(self.next_weekly_close_at),
            "weekly_open_at": instant_text(self.next_weekly_open_at),
            "daily_break_close_at": instant_text(self.next_daily_break_close_at),
            "daily_break_open_at": instant_text(self.next_daily_break_open_at),
            # Daily breaks must use their daily-specific aliases.  The
            # generic ``market_cut_at`` is venue/session-wide in RiskExit and
            # is intentionally not populated with this recurring break: that
            # would make MULTIDAY positions exit before every daily break.
            "daily_close_at": instant_text(self.next_daily_break_close_at),
            "daily_cut_at": instant_text(self.next_daily_break_close_at),
            "market_open_at": instant_text(self.next_daily_break_open_at),
            "calendar_model_id": self.model_id,
            "calendar_basis": self.basis,
            "calendar_observed": self.observed,
            "calendar_hash": self.calendar_hash,
            "provenance_uri": CALENDAR_PROVENANCE_URI,
        }


@dataclass(frozen=True, slots=True)
class HistoricalCalendarTemplate:
    """Current-public-hours template projected onto a historical quote clock."""

    model_id: str
    timezone: str = CALENDAR_TIMEZONE
    daily_financing_time: time = time(17, 0)
    friday_close_time: time = time(16, 55)
    sunday_open_time: time = time(17, 1)
    # Pepperstone publishes the daily 23:59-00:01 break in server time.
    # Projecting that server schedule onto the requested America/New_York
    # clock yields a 16:59-17:01 local break (with the corresponding DST
    # shift in UTC), not a midnight New York break.
    daily_break_close_time: time = time(16, 59)
    daily_break_open_time: time = time(17, 1)
    provenance_uri: str = CALENDAR_PROVENANCE_URI
    documented_at: datetime = CALENDAR_DOCUMENTED_AT
    basis: str = CALENDAR_BASIS
    exceptions_known: bool = False
    financing_amount_known: bool = False
    observed: bool = False

    def __post_init__(self) -> None:
        if self.model_id != PEPPERSTONE_PUBLIC_CALENDAR_MODEL_ID:
            raise HistoricalAssumptionError("calendar model_id no registrado")
        if self.timezone != CALENDAR_TIMEZONE:
            raise HistoricalAssumptionError("timezone de calendario no soportada")
        try:
            ZoneInfo(self.timezone)
        except ZoneInfoNotFoundError as exc:
            raise HistoricalAssumptionError("timezone America/New_York no disponible") from exc
        expected = {
            "daily_financing_time": time(17, 0),
            "friday_close_time": time(16, 55),
            "sunday_open_time": time(17, 1),
            "daily_break_close_time": time(16, 59),
            "daily_break_open_time": time(17, 1),
        }
        if any(getattr(self, name) != value for name, value in expected.items()):
            raise HistoricalAssumptionError("horario del template público no coincide")
        object.__setattr__(self, "documented_at", _utc(self.documented_at, name="documented_at"))
        if not self.provenance_uri.strip() or self.basis != CALENDAR_BASIS or self.observed:
            raise HistoricalAssumptionError("provenance/basis del calendario inválidos")
        if not isinstance(self.exceptions_known, bool) or not isinstance(self.financing_amount_known, bool):
            raise HistoricalAssumptionError("flags del calendario inválidos")

    def _local(self, value: datetime) -> datetime:
        return _utc(value, name="quote_time").astimezone(ZoneInfo(self.timezone))

    @staticmethod
    def _next_weekday(day: date, weekday: int) -> date:
        return day + timedelta(days=(weekday - day.weekday()) % 7)

    def _next_financing(self, local: datetime) -> datetime:
        day = local.date()
        for offset in range(8):
            candidate_day = day + timedelta(days=offset)
            if candidate_day.weekday() >= 5:
                continue
            candidate = datetime.combine(candidate_day, self.daily_financing_time, tzinfo=local.tzinfo)
            if candidate > local:
                return candidate.astimezone(UTC)
        raise HistoricalAssumptionError("no se pudo proyectar financing_at")

    def _next_weekly_close(self, local: datetime) -> datetime:
        day = self._next_weekday(local.date(), 4)
        candidate = datetime.combine(day, self.friday_close_time, tzinfo=local.tzinfo)
        if candidate <= local:
            day += timedelta(days=7)
            candidate = datetime.combine(day, self.friday_close_time, tzinfo=local.tzinfo)
        return candidate.astimezone(UTC)

    def _next_weekly_open(self, local: datetime) -> datetime:
        day = self._next_weekday(local.date(), 6)
        candidate = datetime.combine(day, self.sunday_open_time, tzinfo=local.tzinfo)
        if candidate <= local:
            day += timedelta(days=7)
            candidate = datetime.combine(day, self.sunday_open_time, tzinfo=local.tzinfo)
        return candidate.astimezone(UTC)

    def _next_daily_break(self, local: datetime) -> tuple[datetime, datetime]:
        close_day = local.date()
        close = datetime.combine(close_day, self.daily_break_close_time, tzinfo=local.tzinfo)
        if close <= local:
            close_day += timedelta(days=1)
            close = datetime.combine(close_day, self.daily_break_close_time, tzinfo=local.tzinfo)
        open_at = datetime.combine(close_day, self.daily_break_open_time, tzinfo=local.tzinfo)
        return close.astimezone(UTC), open_at.astimezone(UTC)

    def observation_for(self, at: datetime) -> HistoricalCalendarObservation:
        when = _utc(at, name="quote_time")
        local = self._local(when)
        break_close, break_open = self._next_daily_break(local)
        return HistoricalCalendarObservation(
            self.model_id,
            when,
            self._next_financing(local),
            self._next_weekly_close(local),
            self._next_weekly_open(local),
            break_close,
            break_open,
            known=True,
            exceptions_known=self.exceptions_known,
            financing_amount_known=self.financing_amount_known,
            observed=self.observed,
            basis=self.basis,
            calendar_hash=self.calendar_hash,
        )

    def state_for_quote(self, at: datetime) -> dict[str, Any]:
        return self.observation_for(at).to_calendar_state()

    def entry_gate(
        self,
        at: datetime,
        *,
        preclose_minutes: Decimal | int | str = 30,
        include_daily_preclose: bool = True,
    ) -> HistoricalEntryGate:
        """Block modeled closures without changing quote data.

        Intraday profiles use the daily financing/break pre-close window.
        MULTIDAY profiles still cannot enter during the actual daily break,
        but do not receive a synthetic daily pre-close filter; their only
        modeled pre-close window is the weekly closure supplied by the same
        template.
        """

        when = _utc(at, name="quote_time")
        minutes = _decimal(preclose_minutes, name="preclose_minutes")
        if minutes < 0:
            raise HistoricalAssumptionError("preclose_minutes debe ser no negativo")
        if not isinstance(include_daily_preclose, bool):
            raise HistoricalAssumptionError("include_daily_preclose debe ser booleano")
        local = self._local(when)
        reason: str | None
        if (
            local.weekday() == 5
            or (local.weekday() == 6 and local.time() < self.sunday_open_time)
            or local.weekday() == 4
            and local.time() >= self.friday_close_time
        ):
            reason = "WEEKEND_CLOSED"
        elif (
            self.daily_break_close_time <= self.daily_break_open_time
            and self.daily_break_close_time <= local.time() < self.daily_break_open_time
        ) or (
            self.daily_break_close_time > self.daily_break_open_time
            and (local.time() >= self.daily_break_close_time or local.time() < self.daily_break_open_time)
        ):
            reason = "DAILY_BREAK"
        else:
            observation = self.observation_for(when)
            window = minutes * Decimal("60")
            events = [("WEEKLY_CLOSE", observation.next_weekly_close_at)]
            if include_daily_preclose:
                events.extend(
                    (
                        ("FINANCING", observation.next_financing_at),
                        ("DAILY_BREAK", observation.next_daily_break_close_at),
                    )
                )
            reason = next(
                (f"PRE_{name}" for name, event in events if Decimal("0") <= _seconds_between(when, event) <= window),
                None,
            )
        return HistoricalEntryGate(self.model_id, when, reason is None, reason, minutes, self.basis)

    @property
    def calendar_hash(self) -> str:
        return _hash_mapping(self.to_dict(include_hash=False))

    def to_dict(self, *, include_hash: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = {
            "schema": ASSUMPTIONS_SCHEMA,
            "model_id": self.model_id,
            "timezone": self.timezone,
            "daily_financing_time": self.daily_financing_time.isoformat(timespec="seconds"),
            "friday_close_time": self.friday_close_time.isoformat(timespec="seconds"),
            "sunday_open_time": self.sunday_open_time.isoformat(timespec="seconds"),
            "daily_break_close_time": self.daily_break_close_time.isoformat(timespec="seconds"),
            "daily_break_open_time": self.daily_break_open_time.isoformat(timespec="seconds"),
            "provenance_uri": self.provenance_uri,
            "documented_at": instant_text(self.documented_at),
            "basis": self.basis,
            "exceptions_known": self.exceptions_known,
            "financing_amount_known": self.financing_amount_known,
            "observed": self.observed,
        }
        if include_hash:
            result["calendar_hash"] = self.calendar_hash
        return result


@dataclass(frozen=True, slots=True)
class VirtualAccountAssumptions:
    """Modeled EUR/USD virtual account context for gross-only diagnostics."""

    model_id: str
    instrument: str = "EUR/USD"
    account_currency: str = "USD"
    initial_equity: Decimal = Decimal("10000")
    unit_value: Decimal = Decimal("1")
    quantity_min: Decimal = Decimal("1")
    quantity_step: Decimal = Decimal("1")
    fees_known: bool = False
    quantity_max: Decimal | None = None
    minimum_stop_distance: Decimal | None = None
    margin_per_unit: Decimal | None = None
    contract_observed: bool = False
    broker_observed: bool = False
    grid_observed: bool = False
    eligible_for_demo: bool = False
    basis: str = "MODELED_VIRTUAL_ACCOUNT_NOT_BROKER_CONTRACT"
    disclaimer: str = (
        "Cuenta virtual modelada de USD 10,000; unit_value=USD por variación de precio "
        "por 1 EUR nominal. No es grid/contrato observado, no acredita broker ni es elegible para DEMO."
    )

    def __post_init__(self) -> None:
        if self.model_id != VIRTUAL_EURUSD_10K_MODEL_ID:
            raise HistoricalAssumptionError("account model_id no registrado")
        if self.instrument != "EUR/USD" or self.account_currency != "USD":
            raise HistoricalAssumptionError("el modelo sólo admite EUR/USD en USD")
        for name in ("initial_equity", "unit_value", "quantity_min", "quantity_step"):
            object.__setattr__(self, name, _decimal(getattr(self, name), name=name, positive=True))
        for name in ("quantity_max", "minimum_stop_distance", "margin_per_unit"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _decimal(value, name=name, positive=True))
        if not isinstance(self.fees_known, bool) or self.fees_known:
            raise HistoricalAssumptionError("fees_known debe permanecer False")
        if any(
            not isinstance(getattr(self, name), bool) or getattr(self, name)
            for name in ("contract_observed", "broker_observed", "grid_observed", "eligible_for_demo")
        ):
            raise HistoricalAssumptionError("el contexto modelado no puede ser observado/elegible")
        if self.basis != "MODELED_VIRTUAL_ACCOUNT_NOT_BROKER_CONTRACT":
            raise HistoricalAssumptionError("basis de cuenta virtual inválida")

    @property
    def contract_spec(self) -> Mapping[str, Any]:
        """RiskExit input with modeled minimum/grid and explicit unknown caps."""

        return MappingProxyType(
            {
                "known": False,
                "basis": self.basis,
                "pip_size": "0.0001",
                "unit_value": self.unit_value,
                "quantity_min": self.quantity_min,
                "quantity_step": self.quantity_step,
                "quantity_max": self.quantity_max,
                "minimum_stop_distance": self.minimum_stop_distance,
                "margin_per_unit": self.margin_per_unit,
                "fees_known": self.fees_known,
                "spread_known": True,
                "server_side_stops": True,
                "server_protection_basis": "MODELED_SERVER_PROTECTIONS_NOT_OBSERVED",
                "contract_observed": self.contract_observed,
                "broker_observed": self.broker_observed,
                "eligible_for_demo": self.eligible_for_demo,
            }
        )

    @property
    def assumption_hash(self) -> str:
        return _hash_mapping(self.to_dict(include_hash=False))

    def to_dict(self, *, include_hash: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = {
            "schema": ASSUMPTIONS_SCHEMA,
            "model_id": self.model_id,
            "instrument": self.instrument,
            "account_currency": self.account_currency,
            "initial_equity": self.initial_equity,
            "unit_value": self.unit_value,
            "unit_value_basis": "USD_PER_PRICE_CHANGE_PER_EUR_NOMINAL_UNIT",
            "quantity_min": self.quantity_min,
            "quantity_step": self.quantity_step,
            "quantity_max": self.quantity_max,
            "minimum_stop_distance": self.minimum_stop_distance,
            "margin_per_unit": self.margin_per_unit,
            "fees_known": self.fees_known,
            "contract_observed": self.contract_observed,
            "broker_observed": self.broker_observed,
            "grid_observed": self.grid_observed,
            "eligible_for_demo": self.eligible_for_demo,
            "basis": self.basis,
            "disclaimer": self.disclaimer,
            "contract_spec": dict(self.contract_spec),
        }
        if include_hash:
            result["assumption_hash"] = self.assumption_hash
        return result


@dataclass(frozen=True, slots=True)
class HistoricalAssumptionsModel:
    """Combined explicit account and public-calendar model."""

    account: VirtualAccountAssumptions
    calendar: HistoricalCalendarTemplate

    @property
    def model_id(self) -> str:
        return self.account.model_id

    @property
    def calendar_model_id(self) -> str:
        return self.calendar.model_id

    @property
    def assumption_hash(self) -> str:
        return _hash_mapping(self.to_dict(include_hash=False))

    def calendar_state(self, at: datetime) -> dict[str, Any]:
        return self.calendar.state_for_quote(at)

    def to_dict(self, *, include_hash: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = {
            "schema": ASSUMPTIONS_SCHEMA,
            "model_id": self.model_id,
            "account": self.account.to_dict(),
            "calendar": self.calendar.to_dict(),
            "mode": "VIRTUAL_DIAGNOSTIC",
            "eligible_for_demo": False,
            "assumption_basis": "MODELED_NOT_OBSERVED",
        }
        if include_hash:
            result["assumption_hash"] = self.assumption_hash
        return result


def calendar_template_for(model_id: str) -> HistoricalCalendarTemplate:
    """Return a calendar only for the explicitly supplied literal ID."""

    if not isinstance(model_id, str) or model_id != PEPPERSTONE_PUBLIC_CALENDAR_MODEL_ID:
        raise HistoricalAssumptionError(f"calendar model_id requerido: {PEPPERSTONE_PUBLIC_CALENDAR_MODEL_ID}")
    return HistoricalCalendarTemplate(model_id=model_id)


def historical_assumptions_for(model_id: str) -> HistoricalAssumptionsModel:
    """Build the explicit account/calendar model; there is no implicit default."""

    if not isinstance(model_id, str) or model_id != VIRTUAL_EURUSD_10K_MODEL_ID:
        raise HistoricalAssumptionError(f"model_id requerido: {VIRTUAL_EURUSD_10K_MODEL_ID}")
    return HistoricalAssumptionsModel(
        account=VirtualAccountAssumptions(model_id=model_id),
        calendar=calendar_template_for(PEPPERSTONE_PUBLIC_CALENDAR_MODEL_ID),
    )


__all__ = [
    "ASSUMPTIONS_SCHEMA",
    "CALENDAR_BASIS",
    "CALENDAR_DOCUMENTED_AT",
    "CALENDAR_PROVENANCE_URI",
    "CALENDAR_TIMEZONE",
    "EURUSD_VIRTUAL_10K_MODEL_ID",
    "HISTORICAL_ASSUMPTIONS_MODEL_ID",
    "HistoricalAssumptionError",
    "HistoricalAssumptionsModel",
    "HistoricalCalendarObservation",
    "HistoricalCalendarTemplate",
    "HistoricalEntryGate",
    "PEPPERSTONE_PUBLIC_CALENDAR_MODEL_ID",
    "PEPPERSTONE_CALENDAR_TEMPLATE_ID",
    "RISK_BAR_CLOCK_BASIS",
    "VIRTUAL_ACCOUNT_ASSUMPTIONS_MODEL_ID",
    "VIRTUAL_EURUSD_10K_MODEL_ID",
    "VirtualAccountAssumptions",
    "calendar_template_for",
    "historical_assumptions_for",
]
