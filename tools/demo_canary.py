#!/usr/bin/env python3
"""Small, fail-closed DEMO canary runner.

The runner is deliberately not a broker client and not a second executor.  A
caller supplies the already authenticated provider/session owned by the DEMO
supervisor.  The default operation only prepares the existing execution
composition and evaluates gates; it never calls ``submit_signal`` or
``close_position``.  The order path is reachable only when both the explicit
approval object and ``execute=True`` are supplied.

The module also exposes a static CLI preflight and a canonical network path.
The latter may execute only after the existing supervisor has supplied the
authenticated session proof, account writer lock, runtime snapshots, fresh
risk observations and an explicit private approval; this tool never recreates
or serializes those resources.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import sys
import tempfile
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, cast

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from mtf_lab.configuration import ConfigError, EffectiveConfig, load_config
from mtf_lab.core.canary_quote import CanaryZeroSpreadAuthorization
from mtf_lab.core.canonical import canonical_json
from mtf_lab.core.numeric import decimal_context
from mtf_lab.core.risk_exit import CanaryTrialRiskBoundary, RiskExitPolicy
from mtf_lab.ops.ctrader_executor import CTraderDemoExecutor, OrderResult, OrderState, Quote, _as_mapping
from mtf_lab.ops.supervision_demo import (
    DemoExecutionBinding,
    _quote_from_provider,
    build_demo_execution_binding,
)
from mtf_lab.ops.volume_rules import VolumeGrid, VolumeRuleError

if TYPE_CHECKING:
    from mtf_lab.ops.ctrader_account_risk import AccountRiskSnapshot
    from mtf_lab.ops.ctrader_canary_economics import CanaryEconomicsProjection
    from mtf_lab.ops.supervision import SupervisorStateStore

SCHEMA = "mtf-lab.demo-canary.v1"
MAX_APPROVED_BASE_QUANTITY = Decimal("1000")
MAX_RISK_FRACTION = Decimal("0.0005")
MAX_TRIAL_LOSS_FRACTION = Decimal("0.001")
MAX_MUTATION_MESSAGES = 6
_TERMINAL_OPEN_STATES = frozenset({OrderState.FILLED})
_UNRESOLVED_STATES = frozenset({OrderState.UNKNOWN, OrderState.SUBMITTED, OrderState.PARTIAL})


class CanaryGateError(RuntimeError):
    """A canary gate is missing or an execution result is not proven."""

    def __init__(
        self,
        reason: str,
        *,
        gates: Mapping[str, Any] | None = None,
        mutation_messages: int = 0,
    ) -> None:
        super().__init__(str(reason))
        self.reason = str(reason)
        self.gates = dict(gates or {})
        self.mutation_messages = int(mutation_messages)


@dataclass(frozen=True, slots=True)
class CanaryApproval:
    """Human-approved scope for one bounded two-cycle experiment."""

    approved: bool
    approval_id: str
    account_id: str
    symbol: str
    window_start: datetime
    window_end: datetime
    max_holding_seconds: Decimal
    max_quantity: Decimal
    max_risk_fraction: Decimal
    max_mutation_messages: int
    max_trial_loss_fraction: Decimal = Decimal("0.001")
    stop_loss_atr_multiple: Decimal = Decimal("1.5")
    take_profit_atr_multiple: Decimal = Decimal("3")
    exit_slippage_pips: Decimal | None = None
    exit_slippage_approved: bool = False
    exit_slippage_approval_source: str | None = None
    canary_trial_anchor_authorized: bool = False
    canary_trial_anchor_authorization_source: str | None = None
    canary_zero_spread_authorized: bool = False
    canary_zero_spread_authorization_source: str | None = None

    def __post_init__(self) -> None:  # noqa: C901
        approval_id = str(self.approval_id).strip()
        account = str(self.account_id).strip()
        symbol = str(self.symbol).strip().upper().replace("-", "/")
        if not approval_id or any(char in approval_id for char in "\x00\r\n"):
            raise ValueError("approval_id debe ser texto no vacío")
        if not account or not account.isdigit() or int(account) <= 0:
            raise ValueError("CanaryApproval.account_id debe ser positivo")
        if not symbol:
            raise ValueError("CanaryApproval.symbol no puede estar vacío")
        start = _aware(self.window_start, "window_start")
        end = _aware(self.window_end, "window_end")
        if end <= start:
            raise ValueError("window_end debe ser posterior a window_start")
        holding = _positive_decimal(self.max_holding_seconds, "max_holding_seconds")
        if (end - start).total_seconds() < float(holding):
            raise ValueError("la ventana aprobada debe cubrir max_holding_seconds")
        quantity = _positive_decimal(self.max_quantity, "max_quantity")
        risk = _positive_decimal(self.max_risk_fraction, "max_risk_fraction")
        trial_loss = _positive_decimal(self.max_trial_loss_fraction, "max_trial_loss_fraction")
        if quantity > MAX_APPROVED_BASE_QUANTITY:
            raise ValueError("max_quantity excede el máximo aprobado de 1000 unidades base")
        if risk > MAX_RISK_FRACTION:
            raise ValueError("max_risk_fraction excede 0.0005")
        if trial_loss > MAX_TRIAL_LOSS_FRACTION:
            raise ValueError("max_trial_loss_fraction excede 0.001")
        if isinstance(self.max_mutation_messages, bool) or self.max_mutation_messages < 4:
            raise ValueError("max_mutation_messages debe cubrir cuatro mutaciones BUY/close/SELL/close")
        if self.max_mutation_messages > MAX_MUTATION_MESSAGES:
            raise ValueError("max_mutation_messages excede 6")
        stop_multiple = _positive_decimal(self.stop_loss_atr_multiple, "stop_loss_atr_multiple")
        target_multiple = _positive_decimal(self.take_profit_atr_multiple, "take_profit_atr_multiple")
        if stop_multiple != Decimal("1.5") or target_multiple != Decimal("3"):
            raise ValueError("SL/TP de aprobación deben ser 1.5 ATR y 3 ATR")
        slippage = (
            None
            if self.exit_slippage_pips is None
            else _positive_decimal(self.exit_slippage_pips, "exit_slippage_pips")
        )
        if self.exit_slippage_approved and (
            slippage != Decimal("0.1") or not str(self.exit_slippage_approval_source or "").strip()
        ):
            raise ValueError("exit_slippage aprobado requiere 0.1 pip y procedencia")
        if not isinstance(self.canary_trial_anchor_authorized, bool):
            raise ValueError("canary_trial_anchor_authorized debe ser booleano")
        trial_source = str(self.canary_trial_anchor_authorization_source or "").strip() or None
        if self.canary_trial_anchor_authorized and not trial_source:
            raise ValueError("canary_trial_anchor_authorized requiere fuente humana")
        if not self.canary_trial_anchor_authorized and trial_source is not None:
            raise ValueError("fuente de canary_trial_anchor requiere autorización explícita")
        if not isinstance(self.canary_zero_spread_authorized, bool):
            raise ValueError("canary_zero_spread_authorized debe ser booleano")
        zero_source = str(self.canary_zero_spread_authorization_source or "").strip() or None
        if self.canary_zero_spread_authorized and not zero_source:
            raise ValueError("canary_zero_spread_authorized requiere fuente humana")
        if not self.canary_zero_spread_authorized and zero_source is not None:
            raise ValueError("fuente de canary_zero_spread requiere autorización explícita")
        object.__setattr__(self, "approval_id", approval_id)
        object.__setattr__(self, "account_id", account)
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "window_start", start)
        object.__setattr__(self, "window_end", end)
        object.__setattr__(self, "max_holding_seconds", holding)
        object.__setattr__(self, "max_quantity", quantity)
        object.__setattr__(self, "max_risk_fraction", risk)
        object.__setattr__(self, "max_trial_loss_fraction", trial_loss)
        object.__setattr__(self, "stop_loss_atr_multiple", stop_multiple)
        object.__setattr__(self, "take_profit_atr_multiple", target_multiple)
        object.__setattr__(self, "exit_slippage_pips", slippage)
        object.__setattr__(
            self, "exit_slippage_approval_source", str(self.exit_slippage_approval_source or "").strip() or None
        )
        object.__setattr__(self, "canary_trial_anchor_authorization_source", trial_source)
        object.__setattr__(self, "canary_zero_spread_authorization_source", zero_source)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> CanaryApproval:
        if not isinstance(value, Mapping) or value.get("approved") is not True:
            raise CanaryGateError("approval privada ausente o no aprobada")
        limits_raw = value.get("limits")
        limits: Mapping[str, Any] = limits_raw if isinstance(limits_raw, Mapping) else {}

        def pick(name: str, *aliases: str) -> Any:
            for key in (name, *aliases):
                if key in value:
                    return value[key]
                if key in limits:
                    return limits[key]
            return None

        try:
            return cls(
                approved=True,
                approval_id=str(value.get("approval_id", value.get("id", ""))),
                account_id=str(pick("account_id", "selected_account_id")),
                symbol=str(pick("symbol", "instrument")),
                window_start=_timestamp(pick("window_start", "start"), "window_start"),
                window_end=_timestamp(pick("window_end", "end"), "window_end"),
                max_holding_seconds=Decimal(str(pick("max_holding_seconds", "holding_seconds"))),
                max_quantity=Decimal(str(pick("max_quantity", "max_base_quantity", "max_quantity_base_units"))),
                max_risk_fraction=Decimal(
                    str(pick("max_risk_fraction", "risk_fraction", "max_planned_risk_fraction_per_cycle"))
                ),
                max_trial_loss_fraction=(
                    Decimal("0.001")
                    if pick(
                        "max_trial_loss_fraction",
                        "stop_trial_loss_fraction",
                        "trial_loss_fraction",
                        "loss_budget_fraction",
                    )
                    is None
                    else Decimal(
                        str(
                            pick(
                                "max_trial_loss_fraction",
                                "stop_trial_loss_fraction",
                                "trial_loss_fraction",
                                "loss_budget_fraction",
                            )
                        )
                    )
                ),
                max_mutation_messages=int(pick("max_mutation_messages", "mutation_budget")),
                stop_loss_atr_multiple=Decimal(str(pick("stop_loss_atr_multiple", "stop_loss_atr", "sl_atr_multiple"))),
                take_profit_atr_multiple=Decimal(
                    str(pick("take_profit_atr_multiple", "take_profit_atr", "tp_atr_multiple"))
                ),
                exit_slippage_pips=(
                    Decimal(str(pick("exit_slippage_pips", "slippage_pips")))
                    if pick("exit_slippage_pips", "slippage_pips") is not None
                    else None
                ),
                exit_slippage_approved=pick("exit_slippage_approved", "slippage_approved") is True,
                exit_slippage_approval_source=pick("exit_slippage_approval_source", "slippage_approval_source"),
                canary_trial_anchor_authorized=pick("canary_trial_anchor_authorized") is True,
                canary_trial_anchor_authorization_source=pick(
                    "canary_trial_anchor_authorization_source",
                    "canary_trial_anchor_authorized_by",
                    "trial_anchor_authorization_source",
                    "human_authorization_source",
                ),
                canary_zero_spread_authorized=pick("canary_zero_spread_authorized") is True,
                canary_zero_spread_authorization_source=pick("canary_zero_spread_authorization_source"),
            )
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise CanaryGateError("approval privada con esquema inválido") from exc


def load_approval_file(path: str | Path) -> CanaryApproval:
    """Read one private approval file without echoing its identifier."""

    from tools.ctrader_query_launcher import _json_file

    value = _json_file(Path(path).expanduser(), label="aprobación DEMO")
    return CanaryApproval.from_mapping(value)


def _approval_document_digest(approval: CanaryApproval) -> str:
    """Fingerprint the normalized approval document, not only its ID."""

    return hashlib.sha256(canonical_json(approval).encode("utf-8")).hexdigest()


def _zero_spread_authorization(
    approval: CanaryApproval,
    observation: Any,
    *,
    preparation_start: datetime | None = None,
) -> CanaryZeroSpreadAuthorization | None:
    """Bind the explicit quote exception to an already verified DEMO session."""

    if approval.canary_zero_spread_authorized is not True:
        return None
    if approval.approved is not True:
        raise CanaryGateError("spread cero requiere aprobación humana")
    account = str(getattr(observation, "account_id", "") or "").strip()
    if account != approval.account_id or str(getattr(observation, "environment", "")).upper() != "DEMO":
        raise CanaryGateError("spread cero requiere la misma cuenta DEMO observada")
    try:
        return CanaryZeroSpreadAuthorization(
            approval_digest=_approval_document_digest(approval),
            authorization_source=str(approval.canary_zero_spread_authorization_source or ""),
            account_id=account,
            symbol=approval.symbol,
            session_id=str(getattr(observation, "session_id", "") or ""),
            connection_generation=str(getattr(observation, "connection_generation", "") or ""),
            endpoint=str(getattr(observation, "endpoint", "") or ""),
            preparation_start=preparation_start or approval.window_start - timedelta(minutes=5),
            window_start=approval.window_start,
            window_end=approval.window_end,
        )
    except (TypeError, ValueError) as exc:
        raise CanaryGateError("contexto de spread cero inválido") from exc


def _set_provider_zero_spread(provider: Any, context: CanaryZeroSpreadAuthorization) -> None:
    setter = getattr(provider, "set_canary_zero_spread_authorization", None)
    if not callable(setter):
        raise CanaryGateError("provider no admite el contexto tipado de spread cero")
    setter(context)


def _trial_ledger_context(
    approval: CanaryApproval,
    binding: Any,
    *,
    trial_equity: Decimal,
    trial_loss_budget: Decimal,
    trial_cashflow_total: Decimal | None,
) -> dict[str, Any]:
    observation = getattr(binding, "observation", None)
    if observation is None:
        raise CanaryGateError("trial ledger requiere observación server")
    account_id = str(getattr(observation, "account_id", "")).strip()
    session_id = str(getattr(observation, "session_id", "") or "").strip()
    generation = str(getattr(observation, "connection_generation", "") or "").strip()
    endpoint = str(getattr(observation, "endpoint", "") or "").strip().lower()
    if not account_id or not session_id or not generation or not endpoint:
        raise CanaryGateError("trial ledger requiere account/session/generation/endpoint")
    if account_id != approval.account_id:
        raise CanaryGateError("trial ledger account_id no coincide con approval")
    if approval.canary_trial_anchor_authorized is True and trial_cashflow_total is None:
        raise CanaryGateError("trial ledger requiere cashflow_total observado")
    return {
        "approval_digest": _approval_document_digest(approval),
        "canary_trial_anchor_authorized": approval.canary_trial_anchor_authorized,
        "canary_trial_anchor_authorization_source": approval.canary_trial_anchor_authorization_source,
        "canary_zero_spread_authorized": approval.canary_zero_spread_authorized,
        "canary_zero_spread_authorization_source": approval.canary_zero_spread_authorization_source,
        "account_id": account_id,
        "session_id": session_id,
        "connection_generation": generation,
        "endpoint": endpoint,
        "window_start": approval.window_start.isoformat(),
        "window_end": approval.window_end.isoformat(),
        "trial_equity": str(trial_equity),
        "trial_cashflow_total": None if trial_cashflow_total is None else str(trial_cashflow_total),
        "trial_loss_fraction": str(approval.max_trial_loss_fraction),
        "trial_loss_budget": str(trial_loss_budget),
    }


class _ApprovalLedger:
    """Single-use approval reservation with crash-conservative state."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).expanduser()
        self.path = self.root / "canary-approval-ledger.json"

    @property
    def _approval_dir(self) -> Path:
        return self.root

    def _read(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"schema": SCHEMA, "approvals": {}}
        if self.path.is_symlink():
            raise CanaryGateError("ledger de aprobación es symlink")
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise CanaryGateError("ledger de aprobación ilegible") from exc
        if not isinstance(raw, dict) or raw.get("schema") != SCHEMA or not isinstance(raw.get("approvals"), dict):
            raise CanaryGateError("ledger de aprobación con esquema desconocido")
        return raw

    def _write(self, value: Mapping[str, Any]) -> None:
        self._approval_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        if self._approval_dir.is_symlink() or not self._approval_dir.is_dir():
            raise CanaryGateError("directorio de ledger no es regular")
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=self._approval_dir, prefix=".canary-ledger.", delete=False
            ) as stream:
                temporary = Path(stream.name)
                stream.write(json.dumps(dict(value), ensure_ascii=False, sort_keys=True, separators=(",", ":")))
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.chmod(temporary, 0o600)
            os.replace(temporary, self.path)
            # The rename makes the ledger visible atomically; fsync the
            # containing directory as well so a crash cannot acknowledge a
            # one-shot approval only in the page cache.
            directory_fd = os.open(self._approval_dir, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            temporary = None
        finally:
            if temporary is not None:
                with contextlib.suppress(OSError):
                    temporary.unlink()

    @staticmethod
    def _digest(approval_id: str) -> str:
        return hashlib.sha256(approval_id.encode("utf-8")).hexdigest()

    @staticmethod
    def _validate_context_record(record: Mapping[str, Any], context: Mapping[str, Any]) -> None:
        keys = (
            "approval_digest",
            "canary_trial_anchor_authorized",
            "canary_trial_anchor_authorization_source",
            "canary_zero_spread_authorized",
            "canary_zero_spread_authorization_source",
            "account_id",
            "session_id",
            "connection_generation",
            "endpoint",
            "window_start",
            "window_end",
            "trial_equity",
            "trial_cashflow_total",
            "trial_loss_fraction",
            "trial_loss_budget",
        )
        for key in keys:
            expected = context.get(key)
            if key not in record or record.get(key) != expected:
                raise CanaryGateError(f"trial ledger context mismatch: {key}")

    def reserve(
        self,
        approval: CanaryApproval,
        *,
        trial_equity: Decimal,
        trial_loss_budget: Decimal,
        context: Mapping[str, Any],
    ) -> str:
        raw = self._read()
        digest = self._digest(approval.approval_id)
        if digest in raw["approvals"]:
            raise CanaryGateError("approval DEMO ya consumida o quedó en estado incierto")
        if context.get("approval_digest") != _approval_document_digest(approval):
            raise CanaryGateError("trial ledger approval_digest no coincide con approval")
        if context.get("trial_equity") != str(trial_equity) or context.get("trial_loss_budget") != str(
            trial_loss_budget
        ):
            raise CanaryGateError("trial ledger equity/budget no coincide con la reserva")
        run_id = uuid.uuid4().hex
        raw["approvals"][digest] = {
            "state": "STARTED",
            "run_id": run_id,
            "mutation_budget": approval.max_mutation_messages,
            "canary_trial_anchor_authorized": approval.canary_trial_anchor_authorized,
            "canary_trial_anchor_authorization_source": approval.canary_trial_anchor_authorization_source,
            **dict(context),
            "trial_loss_committed": "0",
            "trial_loss_observed": None,
            "mutation_history": [],
        }
        self._write(raw)
        return digest

    def update_trial(  # noqa: C901
        self,
        digest: str,
        *,
        mutation_messages: int,
        trial_loss_committed: Decimal,
        trial_loss_observed: Decimal | None = None,
        context: Mapping[str, Any],
        operation: str | None = None,
        status: str = "PENDING",
        error: str | None = None,
    ) -> None:
        raw = self._read()
        record = raw["approvals"].get(digest)
        if not isinstance(record, dict) or record.get("state") != "STARTED":
            raise CanaryGateError("reserva de approval no está STARTED")
        self._validate_context_record(record, context)
        prior_mutations = int(record.get("mutation_messages", 0) or 0)
        record["mutation_messages"] = max(prior_mutations, int(mutation_messages))
        try:
            prior_loss = Decimal(str(record.get("trial_loss_committed", "0")))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise CanaryGateError("trial_loss_committed corrupto") from exc
        try:
            incoming_loss = Decimal(str(trial_loss_committed))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise CanaryGateError("trial_loss_committed inválido") from exc
        if not incoming_loss.is_finite() or incoming_loss < 0:
            raise CanaryGateError("trial_loss_committed inválido")
        observed_prior_raw = record.get("trial_loss_observed")
        try:
            observed_prior = Decimal(str(observed_prior_raw)) if observed_prior_raw is not None else Decimal("0")
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise CanaryGateError("trial_loss_observed corrupto") from exc
        observed_current = observed_prior if trial_loss_observed is None else Decimal(str(trial_loss_observed))
        if not observed_current.is_finite() or observed_current < 0:
            raise CanaryGateError("trial_loss_observed inválido")
        committed = max(prior_loss, incoming_loss, observed_current)
        record["trial_loss_committed"] = str(committed)
        if trial_loss_observed is not None:
            record["trial_loss_observed"] = str(max(observed_prior, Decimal(str(trial_loss_observed))))
        history = record.setdefault("mutation_history", [])
        if not isinstance(history, list):
            raise CanaryGateError("mutation_history corrupto")
        if operation is not None:
            if history and history[-1].get("operation") == operation and history[-1].get("status") == "PENDING":
                history[-1]["status"] = str(status)
                if error:
                    history[-1]["error"] = str(error)
            else:
                history.append(
                    {
                        "sequence": len(history) + 1,
                        "operation": str(operation),
                        "status": str(status),
                        **({"error": str(error)} if error else {}),
                    }
                )
        self._write(raw)

    def finish(
        self,
        digest: str,
        *,
        state: str,
        mutation_messages: int,
        context: Mapping[str, Any],
        error: str | None = None,
    ) -> None:
        raw = self._read()
        record = raw["approvals"].get(digest)
        if not isinstance(record, dict):
            raise CanaryGateError("reserva de approval desapareció")
        self._validate_context_record(record, context)
        record["state"] = str(state)
        prior_mutations = int(record.get("mutation_messages", 0) or 0)
        record["mutation_messages"] = max(prior_mutations, int(mutation_messages))
        if error:
            record["last_error"] = str(error)
        history = record.get("mutation_history")
        if isinstance(history, list) and history and history[-1].get("status") == "PENDING":
            history[-1]["status"] = "UNKNOWN" if state == "UNKNOWN" else str(state)
            if error:
                history[-1]["error"] = str(error)
        self._write(raw)


@dataclass(frozen=True, slots=True)
class CanaryInputs:
    """Two caller-provided strategy/runtime pairs; no synthetic signals."""

    buy_signal: Any
    buy_runtime: Mapping[str, Any]
    sell_signal: Any
    sell_runtime: Mapping[str, Any]

    def cycles(self) -> tuple[tuple[str, Any, Mapping[str, Any]], ...]:
        return (
            ("BUY", self.buy_signal, self.buy_runtime),
            ("SELL", self.sell_signal, self.sell_runtime),
        )

    @classmethod
    def from_observed_runtime(
        cls,
        buy_runtime: Mapping[str, Any],
        sell_runtime: Mapping[str, Any],
        *,
        instrument: str,
        buy_signal_id: str = "manual-canary-buy",
        sell_signal_id: str = "manual-canary-sell",
    ) -> CanaryInputs:
        """Build explicit manual intents from normalized executor snapshots.

        The snapshots must already be the output of
        ``CTraderDemoExecutor.observe_runtime`` after the real watch has
        supplied causal warmup/ATR data.  This helper does not calculate ATR,
        mark a feed READY, or turn a detector signal into an order.
        """

        expected = str(instrument).strip().upper().replace("-", "/")
        if not expected:
            raise CanaryGateError("manual canary requiere instrumento")
        for label, snapshot in (("BUY", buy_runtime), ("SELL", sell_runtime)):
            if not isinstance(snapshot, Mapping):
                raise CanaryGateError(f"runtime {label} no es Mapping")
            if snapshot.get("runtime_state") != "VALID":
                raise CanaryGateError(f"runtime {label} no está VALID")
            if snapshot.get("data_mode") not in {"LIVE", "DEMO_OBSERVED"}:
                raise CanaryGateError(f"runtime {label} no es datos DEMO observados")
            if str(snapshot.get("trigger_timeframe", "")).upper() != "M1":
                raise CanaryGateError(f"runtime {label} no acredita ATR/trigger M1")
            for name in (
                "market_candidate_id",
                "risk_bar_clock_basis",
                "trigger_bar_count",
                "latest_trigger_end",
                "latest_trigger_available_at",
                "last_market",
                "last_available",
                "atr",
            ):
                if snapshot.get(name) is None:
                    raise CanaryGateError(f"runtime {label} carece de {name}")

        def manual_signal(signal_id: str, direction: str, runtime: Mapping[str, Any]) -> dict[str, Any]:
            return {
                "signal_id": signal_id,
                "instrument": expected,
                "direction": direction,
                "mode": "DEMO",
                "account_environment": "DEMO",
                "data_mode": runtime["data_mode"],
                "manual_canary": True,
                "market_candidate_id": runtime["market_candidate_id"],
                "atr": runtime["atr"],
            }

        return cls(
            manual_signal(buy_signal_id, "BUY", buy_runtime),
            buy_runtime,
            manual_signal(sell_signal_id, "SELL", sell_runtime),
            sell_runtime,
        )


@dataclass(frozen=True, slots=True)
class CanaryResult:
    """Secret-free result containing only actionable gates and cycle states."""

    ok: bool
    state: str
    mode: str
    gates: Mapping[str, Any]
    cycles: tuple[Mapping[str, Any], ...] = ()
    mutation_messages: int = 0
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": SCHEMA,
            "ok": self.ok,
            "state": self.state,
            "mode": self.mode,
            "gates": dict(self.gates),
            "cycles": [dict(item) for item in self.cycles],
            "mutation_messages": self.mutation_messages,
            "error": self.error,
        }


@dataclass(slots=True)
class PreparedCanarySession:
    """One live provider/binding prepared by the canonical CLI composition."""

    provider: Any
    config: EffectiveConfig
    provenance: Mapping[str, Any]
    binding: DemoExecutionBinding | None
    preparation: Any
    writer_store: SupervisorStateStore
    defer_activation: bool = False
    zero_spread_authorization: CanaryZeroSpreadAuthorization | None = None

    def close(self) -> None:
        if self.binding is not None:
            with contextlib.suppress(Exception):
                if bool(getattr(self.binding.executor, "active", False)):
                    self.binding.executor.deactivate("prepared_session_closed")
        close = getattr(self.preparation, "close", None)
        if callable(close):
            close()

    def _arm_unlocked(self, canary_economics: CanaryEconomicsProjection) -> None:
        """Build/activate the binding only after projection and account lock."""

        if self.binding is not None:
            raise CanaryGateError("prepared session ya tiene binding armado")
        self.binding = build_demo_execution_binding(
            self.provider,
            self.provenance,
            config=self.config,
            state_dir=self.writer_store.root,
            resume=True,
            canary_economics=canary_economics,
            account_key=None,
            defer_activation=self.defer_activation,
            zero_spread_authorization=self.zero_spread_authorization,
        )

    def arm(self, canary_economics: CanaryEconomicsProjection) -> None:
        with self.writer_store.lock():
            self._arm_unlocked(canary_economics)

    def observe_and_arm(
        self,
        projection_factory: Callable[[], CanaryEconomicsProjection],
        *,
        config_factory: Callable[[CanaryEconomicsProjection], EffectiveConfig] | None = None,
    ) -> CanaryEconomicsProjection:
        """Observe, derive effective config, and arm under one account lock."""

        with self.writer_store.lock():
            projection = projection_factory()
            if config_factory is not None:
                self.config = config_factory(projection)
            self._arm_unlocked(projection)
            return projection


def _aware(value: Any, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{name} debe ser datetime aware")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} debe incluir zona horaria")
    return value.astimezone(UTC)


def _positive_decimal(value: Any, name: str) -> Decimal:
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{name} debe ser Decimal positivo") from exc
    if not parsed.is_finite() or parsed <= 0:
        raise ValueError(f"{name} debe ser Decimal positivo")
    return parsed


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _timestamp(value: Any, name: str) -> datetime:
    if isinstance(value, datetime):
        return _aware(value, name)
    if not isinstance(value, str) or not value.strip():
        raise CanaryGateError(f"{name} desconocido")
    try:
        return _aware(datetime.fromisoformat(value.replace("Z", "+00:00")), name)
    except ValueError as exc:
        raise CanaryGateError(f"{name} inválido") from exc


def _raw_execution(config: Any) -> Mapping[str, Any]:
    raw = getattr(config, "execution", None)
    if not isinstance(raw, Mapping):
        raise CanaryGateError("configuración efectiva sin [execution]")
    return raw


def _preactivation_config(config: EffectiveConfig) -> EffectiveConfig:
    """Return an in-memory static validation view without enabling execution."""

    return replace(
        config,
        execution=MappingProxyType({**dict(_raw_execution(config)), "enabled": True}),
    )


def _effective_canary_config(config: EffectiveConfig, projection: CanaryEconomicsProjection) -> EffectiveConfig:
    """Promote only observed canary execution fields in memory.

    The persisted profile remains execution-disabled.  The effective calendar
    is copied from the typed, same-generation economics projection; no
    ``known=true`` placeholder is promoted before that observation exists.
    """

    calendar = getattr(projection, "calendar_state", None)
    if not isinstance(calendar, Mapping):
        raise CanaryGateError("projection canary sin risk_calendar tipado")
    raw = dict(_raw_execution(_preactivation_config(config)))
    raw["risk_calendar"] = dict(calendar)
    return replace(config, execution=MappingProxyType(raw))


def _signal_mapping(value: Any) -> Mapping[str, Any]:
    try:
        mapped = _as_mapping(value)
    except (TypeError, ValueError) as exc:
        raise CanaryGateError("señal no es Mapping/dataclass convertible") from exc
    if not isinstance(mapped, Mapping):
        raise CanaryGateError("señal no es Mapping/dataclass convertible")
    return mapped


def _coerce_inputs(value: Any) -> CanaryInputs:
    if isinstance(value, CanaryInputs):
        return value
    adapter = getattr(value, "to_canary_inputs", None)
    if callable(adapter):
        converted = adapter()
        if isinstance(converted, CanaryInputs):
            return converted
    raise CanaryGateError("inputs no son CanaryInputs ni contexto observado adaptable")


def _validate_binding_identity(
    binding: Any,
    provider: Any,
    provenance: Mapping[str, Any],
    config: Any,
    approval: CanaryApproval,
) -> None:
    observation = getattr(binding, "observation", None)
    if observation is None:
        raise CanaryGateError("binding sin observación server")
    if str(getattr(observation, "account_id", "")).strip() != approval.account_id:
        raise CanaryGateError("binding account_id no coincide con approval")
    if str(provenance.get("account_id", "")).strip() != approval.account_id:
        raise CanaryGateError("provenance account_id no coincide con approval")
    configured_account = str(getattr(config, "ctrader", {}).get("account_id", "")).strip()
    if configured_account != approval.account_id:
        raise CanaryGateError("config account_id no coincide con approval")
    symbol = str(getattr(getattr(provider, "spec", None), "symbol", "")).strip().upper().replace("-", "/")
    configured_symbol = str(getattr(config, "instrument", "")).strip().upper().replace("-", "/")
    observed_symbol = str(getattr(observation, "symbol", symbol)).strip().upper().replace("-", "/")
    if symbol != approval.symbol or configured_symbol != approval.symbol or observed_symbol != approval.symbol:
        raise CanaryGateError("provider/config/binding symbol no coincide con approval")
    provenance_symbol = str(provenance.get("symbol", "")).strip().upper().replace("-", "/")
    if provenance_symbol and provenance_symbol != approval.symbol:
        raise CanaryGateError("provenance symbol no coincide con approval")
    observed_endpoint = str(getattr(observation, "endpoint", "")).strip().lower()
    provenance_endpoint = str(provenance.get("endpoint", "")).strip().lower()
    provider_config = getattr(provider, "config", None)
    provider_host = str(getattr(provider_config, "host", "")).strip().lower()
    provider_port = getattr(provider_config, "port", None)
    provider_endpoint = f"{provider_host}:{provider_port}" if provider_host and provider_port else observed_endpoint
    if not provenance_endpoint or observed_endpoint != provenance_endpoint or observed_endpoint != provider_endpoint:
        raise CanaryGateError("binding/provenance/provider endpoint no coincide")
    provider_generation = str(getattr(provider, "generation", "")).strip()
    observed_generation = str(getattr(observation, "connection_generation", "") or "").strip()
    provenance_generation = str(provenance.get("connection_generation", "")).strip()
    if not provider_generation or not observed_generation or provider_generation != observed_generation:
        raise CanaryGateError("binding/provider connection_generation no coincide")
    if provenance_generation and provenance_generation != observed_generation:
        raise CanaryGateError("provenance connection_generation no coincide")


def _validate_prepared_binding(  # noqa: C901
    binding: Any,
    provider: Any,
    provenance: Mapping[str, Any],
    config: Any,
    approval: CanaryApproval,
) -> None:
    from mtf_lab.ops.ctrader_demo_transport import CTraderClientGateway, CTraderDemoTransport, ServerAccountObservation
    from mtf_lab.ops.ctrader_executor import CTraderDemoExecutor, JsonlIntentStore
    from mtf_lab.ops.supervision_demo import DemoExecutionBinding

    if not isinstance(binding, DemoExecutionBinding):
        raise CanaryGateError("prepared binding no es DemoExecutionBinding canónico")
    if not isinstance(binding.gateway, CTraderClientGateway):
        raise CanaryGateError("prepared binding no usa CTraderClientGateway")
    if not isinstance(binding.observation, ServerAccountObservation):
        raise CanaryGateError("prepared binding no conserva ServerAccountObservation")
    if not isinstance(binding.transport, CTraderDemoTransport) or not isinstance(binding.executor, CTraderDemoExecutor):
        raise CanaryGateError("prepared binding no usa transporte/executor DEMO canónicos")
    if not isinstance(binding.intent_store, JsonlIntentStore):
        raise CanaryGateError("prepared binding no usa journal JSONL durable")
    if binding.transport.client is not binding.gateway or binding.executor.transport is not binding.transport:
        raise CanaryGateError("prepared binding no comparte gateway/transport único")
    if binding.observation.environment != "DEMO" or "trading" not in binding.observation.scopes:
        raise CanaryGateError("prepared binding no conserva evidencia server DEMO+trading")
    _validate_binding_identity(binding, provider, provenance, config, approval)


def _risk_policy(raw: Mapping[str, Any]) -> RiskExitPolicy:
    value = raw.get("risk_exit_policy")
    if not isinstance(value, Mapping):
        raise CanaryGateError("risk_exit_policy es obligatorio para el límite de 0.05%")
    try:
        policy = RiskExitPolicy.from_mapping(value)
    except (TypeError, ValueError) as exc:
        raise CanaryGateError(f"risk_exit_policy inválida: {type(exc).__name__}") from exc
    if policy.equity_basis != "OBSERVED_DEMO":
        raise CanaryGateError("risk_exit_policy.equity_basis debe ser OBSERVED_DEMO")
    if policy.planned_risk_fraction > MAX_RISK_FRACTION:
        raise CanaryGateError("planned_risk_fraction excede 0.0005")
    return policy


def validate_static_config(  # noqa: C901
    config: Any,
    approval: CanaryApproval | None = None,
    *,
    observed_economics: bool = False,
) -> dict[str, Any]:
    """Validate only canary-specific configuration; perform no I/O."""

    raw = _raw_execution(config)
    required = (
        "enabled",
        "environment",
        "endpoint",
        "scope",
        "max_quantity",
        "max_exposure",
        "max_positions",
        "max_inflight_intents",
        "max_daily_loss",
        "max_drawdown",
        "min_margin_level",
        "max_holding_seconds",
        "require_protective_stops",
        "market_candidate_id",
        "risk_exit_policy",
        "risk_calendar",
    )
    missing = [name for name in required if raw.get(name) is None]
    gates: dict[str, Any] = {
        "execution_config_present": not missing,
        "missing_execution_fields": missing,
        "demo_only": str(raw.get("environment", "")).upper() == "DEMO",
        "execution_enabled": raw.get("enabled") is True,
        "trading_scope_configured": str(raw.get("scope", "")).lower() == "trading",
        "one_position": raw.get("max_positions") == 1,
        "one_inflight_intent": raw.get("max_inflight_intents") == 1,
        "protective_stops": raw.get("require_protective_stops") is True,
    }
    if missing:
        raise CanaryGateError("faltan campos explícitos de configuración DEMO", gates=gates)
    required_gate_names = (
        "execution_config_present",
        "demo_only",
        "execution_enabled",
        "trading_scope_configured",
        "one_position",
        "one_inflight_intent",
        "protective_stops",
    )
    if not all(bool(gates[name]) for name in required_gate_names):
        raise CanaryGateError("configuración DEMO no satisface gates mínimos", gates=gates)
    indicators = getattr(config, "indicators", None)
    strategy = getattr(config, "strategy", None)
    if indicators is not None and getattr(indicators, "atr_period", None) != 14:
        raise CanaryGateError("canary requiere ATR14 observado")
    if strategy is not None and str(getattr(strategy, "trigger_timeframe", "")).upper() != "M1":
        raise CanaryGateError("canary requiere trigger M1 observado")
    policy = _risk_policy(raw)
    if policy.stop_atr_multiple != Decimal("1.5") or policy.take_profit_atr_multiple != Decimal("3"):
        raise CanaryGateError("RiskExit SL/TP de config deben ser 1.5 ATR y 3 ATR", gates=gates)
    calendar = raw.get("risk_calendar")
    if not isinstance(calendar, Mapping):
        raise CanaryGateError("risk_calendar debe ser tabla", gates=gates)
    if not observed_economics and calendar.get("known") is not True:
        raise CanaryGateError("risk_calendar observado/known=true es obligatorio", gates=gates)
    basis = str(calendar.get("basis", calendar.get("calendar_basis", ""))).upper()
    if not observed_economics and basis.startswith("MODEL"):
        raise CanaryGateError("risk_calendar modelado no es elegible para canary", gates=gates)
    candidate = str(raw.get("market_candidate_id", "")).strip()
    if not candidate:
        raise CanaryGateError("market_candidate_id es obligatorio para RiskExit DEMO", gates=gates)
    if approval is not None:
        if (
            approval.stop_loss_atr_multiple != policy.stop_atr_multiple
            or approval.take_profit_atr_multiple != policy.take_profit_atr_multiple
        ):
            raise CanaryGateError("aprobación y RiskExit discrepan en SL/TP", gates=gates)
        ctrader = getattr(config, "ctrader", None)
        if not isinstance(ctrader, Mapping):
            raise CanaryGateError("configuración efectiva sin [ctrader] explícito", gates=gates)
        if str(ctrader.get("environment", "")).upper() != "DEMO":
            raise CanaryGateError("[ctrader].environment debe ser DEMO", gates=gates)
        if ctrader.get("enabled") is not True or ctrader.get("account_selected") is not True:
            raise CanaryGateError("[ctrader] debe declarar cuenta DEMO seleccionada y habilitada", gates=gates)
        if str(ctrader.get("account_id", "")).strip() != approval.account_id:
            raise CanaryGateError("[ctrader].account_id no coincide con aprobación", gates=gates)
        scopes = {str(item).strip().lower() for item in ctrader.get("required_scopes", ())}
        if scopes != {"accounts", "trading"}:
            raise CanaryGateError("[ctrader].required_scopes debe observar accounts+trading", gates=gates)
        if str(raw.get("account_id", "")).strip() != approval.account_id:
            raise CanaryGateError("execution.account_id no coincide con aprobación", gates=gates)
        allowed = raw.get("allowed_symbols")
        symbols = (
            {str(item).upper().replace("-", "/") for item in allowed}
            if isinstance(allowed, (list, tuple, set, frozenset))
            else set()
        )
        if symbols and approval.symbol not in symbols:
            raise CanaryGateError("símbolo de aprobación no está allowlisted", gates=gates)
        configured_holding = _decimal_or_none(raw.get("max_holding_seconds"))
        if configured_holding != approval.max_holding_seconds:
            raise CanaryGateError("max_holding_seconds no coincide con la ventana aprobada", gates=gates)
    gates.update({"risk_exit_policy": True, "risk_calendar": True, "candidate": candidate})
    if approval is not None:
        gates.update(
            {
                "canary_trial_anchor_authorized": approval.canary_trial_anchor_authorized is True,
                "canary_trial_anchor_authorization_source": bool(
                    str(approval.canary_trial_anchor_authorization_source or "").strip()
                ),
            }
        )
    return gates


def derive_canary_quantity(grid: VolumeGrid, approved_max: Decimal) -> Decimal:
    """Choose the observed minimum grid quantity, never a literal lot fraction."""

    minimum = Decimal(grid.min_volume) / Decimal(grid.volume_scale)
    if minimum > approved_max or minimum > MAX_APPROVED_BASE_QUANTITY:
        raise CanaryGateError("minVolume observado excede el máximo aprobado de 1000 unidades base")
    try:
        grid.validate_open_quantity(minimum)
    except VolumeRuleError as exc:
        raise CanaryGateError("minVolume/stepVolume observados no forman cantidad válida") from exc
    return minimum


def _signal_direction(signal: Mapping[str, Any], expected: str) -> str:
    direction = str(signal.get("direction", signal.get("side", ""))).upper()
    aliases = {"UP": "BUY", "LONG": "BUY", "DOWN": "SELL", "SHORT": "SELL"}
    direction = aliases.get(direction, direction)
    if direction != expected:
        raise CanaryGateError(f"la señal {expected} no coincide con la dirección esperada")
    if signal.get("manual_canary") is not True:
        raise CanaryGateError("canary requiere manual_canary=true; no usa señal algorítmica implícita")
    if not str(signal.get("signal_id", "")).strip():
        raise CanaryGateError("cada señal requiere signal_id explícito")
    return direction


def _risk_metrics(executor: Any) -> tuple[Mapping[str, Any], Decimal]:
    status = executor.status(refresh=False)
    risk = status.get("risk") if isinstance(status, Mapping) else None
    if not isinstance(risk, Mapping):
        raise CanaryGateError("risk_status ausente")
    metrics = risk.get("metrics")
    if not isinstance(metrics, Mapping):
        raise CanaryGateError("métricas de cuenta ausentes")
    equity = _decimal_or_none(metrics.get("equity"))
    if equity is None or equity <= 0:
        raise CanaryGateError("equity DEMO observada desconocida")
    return risk, equity


def _risk_exit_metrics(risk: Mapping[str, Any]) -> Mapping[str, Any]:
    value = risk.get("risk_exit")
    metrics = value.get("metrics") if isinstance(value, Mapping) else None
    return metrics if isinstance(metrics, Mapping) else {}


def _daily_anchor_ready(risk: Mapping[str, Any]) -> bool:
    metrics = _risk_exit_metrics(risk)
    return bool(
        metrics.get("daily_anchor_verified") is True
        and str(metrics.get("daily_loss_state", "")).upper() == "READY"
        and metrics.get("daily_anchor_equity") is not None
        and metrics.get("cashflows_complete") is True
    )


def _trial_cashflow_total(risk: Mapping[str, Any], *, required: bool) -> Decimal | None:
    """Read the current observed net-cashflow total without resetting daily anchors."""

    metrics = _risk_exit_metrics(risk)
    raw = metrics.get("daily_cashflow_total")
    if raw is None:
        if required:
            raise CanaryGateError("trial anchor requiere cashflow_total observado")
        return None
    try:
        value = Decimal(str(raw))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise CanaryGateError("trial cashflow_total observado inválido") from exc
    if not value.is_finite():
        raise CanaryGateError("trial cashflow_total observado no es finito")
    if required and metrics.get("cashflows_complete") is not True:
        raise CanaryGateError("trial anchor requiere cashflow_history completo")
    return value


def _observed_trial_loss(
    risk: Mapping[str, Any],
    *,
    anchor_equity: Decimal,
    anchor_cashflow_total: Decimal | None,
    required: bool,
) -> Decimal | None:
    """Compute conservative loss since the canary anchor, including cashflow."""

    _metrics, equity = _risk_metrics_from_risk(risk)
    current_cashflow = _trial_cashflow_total(risk, required=required)
    if anchor_cashflow_total is None or current_cashflow is None:
        return None
    # Daily anchor/high-water state remains owned by AccountRiskObserver.  A
    # canary trial gets this separate local anchor so a prior daily loss cannot
    # be reset while the observed cashflow delta is still accounted for.
    with decimal_context():
        return max(Decimal("0"), anchor_equity - equity + (current_cashflow - anchor_cashflow_total))


def _canary_trial_boundary(
    approval: CanaryApproval,
    binding: Any,
    *,
    approval_digest: str,
    trial_equity: Decimal,
    trial_cashflow_total: Decimal,
    trial_loss_budget: Decimal,
) -> CanaryTrialRiskBoundary:
    """Bind the trial budget to approval, ledger reservation and session proof."""

    if approval.canary_trial_anchor_authorized is not True:
        raise CanaryGateError("canary trial anchor requiere autorización humana explícita")
    observation = getattr(binding, "observation", None)
    if observation is None:
        raise CanaryGateError("canary trial boundary carece de observación server")
    session_id = str(getattr(observation, "session_id", "") or "").strip()
    generation = str(getattr(observation, "connection_generation", "") or "").strip()
    if not session_id or not generation:
        raise CanaryGateError("canary trial boundary carece de session_id/generation")
    try:
        return CanaryTrialRiskBoundary(
            approval_digest=approval_digest,
            authorization_source=str(approval.canary_trial_anchor_authorization_source or ""),
            account_id=str(getattr(observation, "account_id", approval.account_id)),
            session_id=session_id,
            connection_generation=generation,
            window_start=approval.window_start,
            window_end=approval.window_end,
            trial_equity=trial_equity,
            trial_cashflow_total=trial_cashflow_total,
            trial_loss_fraction=approval.max_trial_loss_fraction,
            trial_loss_budget=trial_loss_budget,
        )
    except (TypeError, ValueError) as exc:
        raise CanaryGateError("canary trial boundary inválido") from exc


def _risk_metrics_from_risk(risk: Mapping[str, Any]) -> tuple[Mapping[str, Any], Decimal]:
    metrics = risk.get("metrics")
    if not isinstance(metrics, Mapping):
        raise CanaryGateError("métricas de cuenta ausentes")
    equity = _decimal_or_none(metrics.get("equity"))
    if equity is None or equity <= 0:
        raise CanaryGateError("equity DEMO observada desconocida")
    return metrics, equity


def _require_fresh_risk(binding: Any, risk: Mapping[str, Any], now: datetime) -> None:
    observed_at = _timestamp(risk.get("metrics_observed_at"), "metrics_observed_at")
    current = _aware(now, "clock")
    age = (current - observed_at).total_seconds()
    if age < 0 or age > float(binding.executor.policy.max_price_age_seconds):
        raise CanaryGateError("métricas de riesgo DEMO no son frescas")
    generation = str(risk.get("metrics_connection_generation", "")).strip()
    expected = str(getattr(binding.observation, "connection_generation", "")).strip()
    if not generation or not expected or generation != expected:
        raise CanaryGateError("métricas de riesgo no están ligadas a la generación server")
    if risk.get("position_state") != "VALID" or risk.get("positions_fresh") is not True:
        raise CanaryGateError("posición/exposición DEMO no tiene snapshot fresco y VALID")


def _quote(
    provider: Any,
    observation: Any,
    signal: Mapping[str, Any],
    *,
    zero_spread_authorization: CanaryZeroSpreadAuthorization | None = None,
    now: datetime | None = None,
) -> Quote:
    return _quote_from_provider(
        provider,
        observation,
        signal,
        zero_spread_authorization=zero_spread_authorization,
        now=now,
    )


def _check_window(approval: CanaryApproval, now: datetime) -> None:
    current = _aware(now, "clock")
    if current < approval.window_start or current >= approval.window_end:
        raise CanaryGateError("ventana DEMO fuera de la aprobación explícita")


def _check_market_window(
    provider: Any,
    approval: CanaryApproval,
    now: datetime,
    helper: Callable[[Any, datetime, datetime], Any] | None,
) -> None:
    """Require the observed tri-state window helper to return exactly OPEN."""

    window_helper = helper
    if window_helper is None:
        try:
            from mtf_lab.ops.market_schedule import observed_market_window_state
        except ImportError as exc:
            raise CanaryGateError("observed_market_window_state no está disponible") from exc
        window_helper = observed_market_window_state
    start = _aware(now, "clock")
    end = min(approval.window_end, start + timedelta(seconds=float(approval.max_holding_seconds)))
    if end <= start:
        raise CanaryGateError("ventana de mercado DEMO vacía")
    try:
        state = window_helper(provider, start, end)
    except Exception as exc:
        raise CanaryGateError(f"ventana de mercado no verificable: {type(exc).__name__}") from exc
    normalized = str(getattr(state, "value", state)).strip().upper()
    if normalized != "OPEN":
        raise CanaryGateError(f"ventana de mercado DEMO no está OPEN: {normalized or 'UNKNOWN'}")


def _check_positions(executor: Any, *, empty: bool) -> Mapping[str, Any]:
    try:
        snapshot = executor.reconcile_positions()
    except Exception as exc:
        raise CanaryGateError(f"reconciliación de posiciones no disponible: {type(exc).__name__}") from exc
    if not isinstance(snapshot, Mapping) or snapshot.get("state") != "VALID":
        raise CanaryGateError("snapshot de posiciones DEMO no es VALID")
    foreign = snapshot.get("foreign_positions")
    owned = snapshot.get("owned_positions")
    if not isinstance(foreign, list) or not isinstance(owned, list):
        raise CanaryGateError("snapshot de posiciones no conserva ownership explícito")
    if foreign:
        raise CanaryGateError("foreign exposure bloquea canary")
    if empty and owned:
        raise CanaryGateError("la cuenta ya tiene exposición propia antes del ciclo")
    if not empty and len(owned) != 1:
        raise CanaryGateError("la canary exige exactamente una posición propia")
    return snapshot


def _refresh_risk(
    binding: Any,
    callback: Callable[[Any], Mapping[str, Any]] | None,
    *,
    now: datetime | Callable[[], datetime],
    side: str = "CONSERVATIVE",
    economics_refresh: Callable[[Any, Mapping[str, Any]], Any] | None = None,
    apply_economics: bool = True,
) -> Mapping[str, Any]:
    refresh = callback
    if refresh is None:
        observer = getattr(binding, "risk_observer", None)
        refresh = getattr(observer, "update_executor", None)
    if not callable(refresh):
        raise CanaryGateError("risk observer callback no fue provisto")
    try:
        value = refresh(binding.executor)
    except Exception as exc:
        raise CanaryGateError(f"observación de riesgo no está READY: {type(exc).__name__}") from exc
    if not isinstance(value, Mapping):
        raise CanaryGateError("risk observer no devolvió snapshot")
    # ``now`` is intentionally sampled after the observer callback.  A clock
    # value captured before a network read can be earlier than response
    # timestamps and falsely classify a fresh snapshot as future/stale.
    current = now() if callable(now) else now
    risk, _equity = _risk_metrics(binding.executor)
    _require_fresh_risk(binding, risk, current)
    if risk.get("position_state") not in {None, "VALID"}:
        raise CanaryGateError("position snapshot DEMO no es utilizable")
    projection = None
    if apply_economics:
        projection = economics_refresh(binding.executor, value) if economics_refresh is not None else None
        if projection is not None:
            binding.canary_economics = projection
        else:
            projection = getattr(binding, "canary_economics", None)
    if projection is not None:
        from mtf_lab.ops.supervision_demo import _update_executor_from_canary_economics

        _update_executor_from_canary_economics(binding.executor, projection, side)
        # Re-read the risk projection after applying the same-generation,
        # side-specific economics.  This prevents a stale projection from
        # becoming the effective contract merely because account metrics were
        # refreshed successfully.
        risk, _equity = _risk_metrics(binding.executor)
        current = now() if callable(now) else now
        _require_fresh_risk(binding, risk, current)
    return value


def _plan_for(  # noqa: C901
    binding: Any,
    provider: Any,
    signal: Mapping[str, Any],
    runtime: Mapping[str, Any],
    quantity: Decimal,
    approval: CanaryApproval,
    now: datetime,
) -> tuple[Quote, Mapping[str, Any]]:
    try:
        normalized_runtime = binding.executor.observe_runtime(runtime)
    except Exception as exc:
        raise CanaryGateError(f"runtime snapshot no es utilizable: {type(exc).__name__}") from exc
    if not isinstance(normalized_runtime, Mapping) or normalized_runtime.get("runtime_state") != "VALID":
        raise CanaryGateError("runtime snapshot no quedó VALID tras validación del executor")
    for name in ("last_market", "last_available", "latest_trigger_end", "latest_trigger_available_at"):
        _timestamp(normalized_runtime.get(name), name)
    last_market = _timestamp(normalized_runtime.get("last_market"), "last_market")
    last_available = _timestamp(normalized_runtime.get("last_available"), "last_available")
    trigger_end = _timestamp(normalized_runtime.get("latest_trigger_end"), "latest_trigger_end")
    trigger_available = _timestamp(normalized_runtime.get("latest_trigger_available_at"), "latest_trigger_available_at")
    current = _aware(now, "clock")
    if last_available < last_market or trigger_available < trigger_end:
        raise CanaryGateError("runtime snapshot no conserva causalidad de disponibilidad")
    if current < last_available or current < trigger_available:
        raise CanaryGateError("runtime snapshot pertenece al futuro")
    if (current - last_available).total_seconds() > float(binding.executor.policy.max_price_age_seconds):
        raise CanaryGateError("runtime snapshot está stale")
    try:
        quote = _quote(
            provider,
            binding.observation,
            signal,
            zero_spread_authorization=getattr(binding.executor, "canary_zero_spread_authorization", None),
            now=current,
        )
    except CanaryGateError:
        raise
    except Exception as exc:
        raise CanaryGateError(f"BBO DEMO no es utilizable: {type(exc).__name__}") from exc
    age = (now - (quote.available_at or quote.timestamp)).total_seconds()
    if age < 0 or age > float(binding.executor.policy.max_price_age_seconds):
        raise CanaryGateError("BBO DEMO no es fresco/causal para canary")
    try:
        plan = binding.executor.risk_entry_plan(signal, quote, requested_quantity=quantity)
    except CanaryGateError:
        raise
    except Exception as exc:
        raise CanaryGateError(f"RiskExit plan bloqueado: {type(exc).__name__}") from exc
    if plan is None:
        raise CanaryGateError("RiskExit plan ausente; no se puede probar límite de 0.05%")
    if not plan.allowed or not plan.eligible_for_demo or plan.mode != "DEMO_GATED":
        raise CanaryGateError("RiskExit plan no es elegible para DEMO")
    if plan.quantity is None or plan.quantity <= 0 or plan.quantity > approval.max_quantity:
        raise CanaryGateError("cantidad RiskExit fuera del volumen aprobado")
    try:
        binding.transport.volume_grid.validate_open_quantity(plan.quantity)
    except (AttributeError, TypeError, ValueError, VolumeRuleError) as exc:
        raise CanaryGateError("cantidad RiskExit fuera del grid DEMO observado") from exc
    if plan.initial_stop is None or plan.take_profit is None:
        raise CanaryGateError("RiskExit plan sin SL/TP protectivos")
    atr = _decimal_or_none(signal.get("atr"))
    if atr is None or atr <= 0:
        raise CanaryGateError("señal manual sin ATR14 positivo")
    direction = str(signal.get("direction", signal.get("side", ""))).upper()
    entry = quote.ask if direction in {"BUY", "UP", "LONG"} else quote.bid
    expected_stop = entry - atr * Decimal("1.5") if direction in {"BUY", "UP", "LONG"} else entry + atr * Decimal("1.5")
    expected_target = entry + atr * Decimal("3") if direction in {"BUY", "UP", "LONG"} else entry - atr * Decimal("3")
    if plan.initial_stop != expected_stop or plan.take_profit != expected_target:
        raise CanaryGateError("RiskExit SL/TP no coincide con aprobación 1.5 ATR/3 ATR")
    if not plan.risk_envelope_known or plan.expected_total_loss_at_stop is None:
        raise CanaryGateError("costos/riesgo total desconocidos; no se opera")
    _risk, equity = _risk_metrics(binding.executor)
    ceiling = equity * approval.max_risk_fraction
    if plan.expected_total_loss_at_stop > ceiling:
        raise CanaryGateError("pérdida total esperada excede 0.05% de equity")
    return quote, {
        "quantity": str(plan.quantity),
        "equity": str(equity),
        "risk_budget": str(plan.risk_budget) if plan.risk_budget is not None else None,
        "expected_total_loss_at_stop": str(plan.expected_total_loss_at_stop),
        "stop_loss": str(plan.initial_stop),
        "take_profit": str(plan.take_profit),
        "risk_envelope_known": plan.risk_envelope_known,
    }


def _result_state(result: OrderResult) -> str:
    return result.state.value if isinstance(result.state, OrderState) else str(result.state)


def _reconcile_open(executor: Any, result: OrderResult) -> OrderResult:
    current = result
    if current.state in _UNRESOLVED_STATES:
        current = executor.reconcile(current.intent.intent_id)
    if current.state not in _TERMINAL_OPEN_STATES:
        raise CanaryGateError(f"entrada no quedó FILLED; no se reintentó: {_result_state(current)}")
    if current.filled_quantity != current.intent.quantity:
        raise CanaryGateError("entrada FILLED con cantidad distinta; no se reintentó")
    return current


def _close_verified(executor: Any, position_id: str) -> OrderResult:
    result = cast(OrderResult, executor.close_position(position_id))
    if result.state in _UNRESOLVED_STATES or result.state is OrderState.CLOSE_PARTIAL:
        result = cast(OrderResult, executor.reconcile(result.intent.intent_id))
    if result.state is not OrderState.CLOSED:
        raise CanaryGateError(f"cierre no quedó CLOSED; no se reintentó: {_result_state(result)}")
    return result


def prepare_cli_session(
    config_path: str | Path,
    state_dir: str | Path,
    *,
    execution: bool = False,
) -> PreparedCanarySession:
    """Use the existing authenticated CLI composition; never accepts secrets.

    This performs the explicit DEMO execution preflight through
    ``CTraderCliService._prepare_query(execution=True)`` via
    ``prepare_network_supervision`` for both read-only and execute callers.
    It does not build a second client, create a token, or arm an executor;
    ``defer_binding`` remains true.  The caller must supply observed runtime
    inputs and a human approval object before asking for execution.
    """

    from types import SimpleNamespace

    from mtf_lab.ops.supervision import SupervisorStateStore, _network_account_key
    from mtf_lab.ops.supervision_composition import prepare_network_supervision

    # Network read-only and execute paths both need the DEMO/trading query
    # profile.  ``defer_binding`` is the separate gate that keeps the
    # read-only path from constructing or activating the executor/journal.
    # The ``execution`` flag only controls what the caller does with the
    # prepared session after authentication.
    args = SimpleNamespace(
        mode="demo",
        activate=True,
        config=Path(config_path).expanduser(),
        state_dir=Path(state_dir).expanduser(),
        resume=True,
        account_key="fixture",
        defer_binding=True,
    )
    prepared = prepare_network_supervision(args)
    if not hasattr(prepared, "provider") or not hasattr(prepared, "binding"):
        payload = getattr(prepared, "payload", None)
        state = payload.get("state") if isinstance(payload, Mapping) else type(prepared).__name__
        raise CanaryGateError(f"preparación DEMO rechazada: {state}")
    binding = getattr(prepared, "binding", None)
    provider = getattr(prepared, "provider", None)
    context = getattr(prepared, "context", None)
    provenance = getattr(prepared, "provenance", None)
    config = getattr(context, "config", None)
    if provider is None or not isinstance(provenance, Mapping) or config is None:
        raise CanaryGateError("preparación DEMO no devolvió provider/config/provenance completos")
    account_key = _network_account_key(provenance, config, "fixture", provider)
    store = SupervisorStateStore(Path(state_dir).expanduser(), account_key)
    return PreparedCanarySession(
        provider,
        config,
        provenance,
        binding,
        prepared,
        store,
        defer_activation=bool(execution),
    )


def _cycle(  # noqa: C901
    binding: Any,
    provider: Any,
    approval: CanaryApproval,
    expected: str,
    signal: Mapping[str, Any],
    runtime: Mapping[str, Any],
    quantity: Decimal,
    now: datetime,
    clock: Callable[[], datetime],
    market_window_state: Callable[[Any, datetime, datetime], Any] | None,
    planned: tuple[Quote, Mapping[str, Any]] | None = None,
    mutation_announce: Callable[[str, int, str, str | None], None] | None = None,
) -> tuple[Mapping[str, Any], int]:
    _signal_direction(signal, expected)
    if approval.window_end - _aware(now, "clock") < timedelta(seconds=float(approval.max_holding_seconds)):
        raise CanaryGateError("la ventana restante no cubre el holding aprobado")
    quote, plan = planned or _plan_for(binding, provider, signal, runtime, quantity, approval, now)
    mutations = 1
    if mutation_announce is not None:
        mutation_announce("OPEN", mutations, "PENDING", None)
    try:
        result = binding.executor.submit_signal(signal, quote)
    except Exception as exc:
        raise CanaryGateError(
            f"entrada no pudo confirmarse; no se reintentó: {type(exc).__name__}",
            mutation_messages=mutations,
        ) from exc
    if mutation_announce is not None:
        mutation_announce("OPEN", mutations, "CONFIRMED", None)
    try:
        opened = _reconcile_open(binding.executor, result)
    except CanaryGateError as exc:
        if exc.mutation_messages >= mutations:
            raise
        raise CanaryGateError(str(exc), gates=exc.gates, mutation_messages=mutations) from exc
    except Exception as exc:
        raise CanaryGateError(
            f"reconciliación de entrada falló; no se reintentó: {type(exc).__name__}",
            mutation_messages=mutations,
        ) from exc
    if opened.intent.quantity > approval.max_quantity:
        raise CanaryGateError("intent ejecutado excede el volumen máximo aprobado")
    try:
        binding.transport.volume_grid.validate_open_quantity(opened.intent.quantity)
    except (AttributeError, TypeError, ValueError, VolumeRuleError) as exc:
        raise CanaryGateError("intent ejecutado quedó fuera del grid observado") from exc
    try:
        positions = _check_positions(binding.executor, empty=False)
    except CanaryGateError as exc:
        raise CanaryGateError(str(exc), gates=exc.gates, mutation_messages=mutations) from exc
    owned = positions.get("owned_positions", [])
    position_id = str(owned[0].get("position_id", "")) if owned and isinstance(owned[0], Mapping) else ""
    if not position_id or position_id not in set(opened.position_ids):
        raise CanaryGateError("posición abierta no coincide con intent/ownership")
    close_now = _aware(clock(), "clock")
    if close_now - _aware(now, "entry clock") > timedelta(seconds=float(approval.max_holding_seconds)):
        raise CanaryGateError("holding aprobado vencido antes del cierre")
    try:
        _check_window(approval, close_now)
        _check_market_window(provider, approval, close_now, market_window_state)
    except CanaryGateError as exc:
        raise CanaryGateError(str(exc), gates=exc.gates, mutation_messages=1) from exc
    mutations += 1
    if mutation_announce is not None:
        mutation_announce("CLOSE", mutations, "PENDING", None)
    try:
        close = _close_verified(binding.executor, position_id)
    except CanaryGateError as exc:
        if exc.mutation_messages >= mutations:
            raise
        raise CanaryGateError(str(exc), gates=exc.gates, mutation_messages=mutations) from exc
    except Exception as exc:
        raise CanaryGateError(
            f"close/cierre no pudo confirmarse; no se reintentó: {type(exc).__name__}",
            mutation_messages=mutations,
        ) from exc
    if mutation_announce is not None:
        mutation_announce("CLOSE", mutations, "CONFIRMED", None)
    try:
        _check_positions(binding.executor, empty=True)
    except CanaryGateError as exc:
        raise CanaryGateError(str(exc), gates=exc.gates, mutation_messages=mutations) from exc
    return {
        "direction": expected,
        "signal_id": str(signal.get("signal_id")),
        "open_state": _result_state(opened),
        "close_state": _result_state(close),
        "position_id_observed": True,
        "plan": plan,
    }, mutations


def run_demo_canary(  # noqa: C901
    provider: Any,
    provenance: Mapping[str, Any],
    config: Any,
    *,
    state_dir: str | Path,
    approval: CanaryApproval,
    inputs: Any,
    execute: bool = False,
    writer_store: Any | None = None,
    risk_refresh: Callable[[Any], Mapping[str, Any]] | None = None,
    economics_refresh: Callable[[Any, Mapping[str, Any]], Any] | None = None,
    post_close_risk_refresh: Callable[[Any], Mapping[str, Any]] | None = None,
    market_window_state: Callable[[Any, datetime, datetime], Any] | None = None,
    clock: Callable[[], datetime] | None = None,
    proto: Any | None = None,
    account_key: str | None = None,
    prepared_session: PreparedCanarySession | None = None,
) -> CanaryResult:
    """Prepare gates, and optionally execute exactly BUY/close then SELL/close.

    ``provider`` must already be the authenticated, account-selected provider
    owned by the supervisor.  This function never performs OAuth/discovery and
    never creates a client.  ``execute=False`` is the safe default.
    """

    observed_economics = (
        prepared_session is not None and getattr(prepared_session.binding, "canary_economics", None) is not None
    )
    validate_static_config(config, approval, observed_economics=observed_economics)
    inputs = _coerce_inputs(inputs)
    if execute and not approval.approved:
        raise CanaryGateError("execute=True requiere aprobación humana explícita")
    if not execute and prepared_session is None:
        gates = validate_static_config(config, approval)
        return CanaryResult(
            False,
            "PREFLIGHT_RUNTIME_REQUIRED",
            "preflight",
            {**gates, "orders_attempted": False, "binding_armed": False},
            error="use network_cli_preflight o una PreparedCanarySession; no se arma execution en preflight",
        )
    buy_signal = _signal_mapping(inputs.buy_signal)
    sell_signal = _signal_mapping(inputs.sell_signal)
    if len({str(buy_signal.get("signal_id", "")), str(sell_signal.get("signal_id", ""))}) != 2:
        raise CanaryGateError("BUY y SELL requieren signal_id distintos")
    clock_fn = clock or (lambda: datetime.now(UTC))
    if prepared_session is not None:
        if not isinstance(prepared_session, PreparedCanarySession) or prepared_session.binding is None:
            raise CanaryGateError("prepared_session no es una sesión canónica DEMO")
        _validate_prepared_binding(prepared_session.binding, provider, provenance, config, approval)
        if (
            provider is not prepared_session.provider
            or config is not prepared_session.config
            or provenance is not prepared_session.provenance
        ):
            raise CanaryGateError("prepared_session no coincide con provider/config de la canary")
        binding = prepared_session.binding
        owns_binding = False
        if writer_store is not None and writer_store is not prepared_session.writer_store:
            raise CanaryGateError("writer store no coincide con prepared_session")
        writer_store = prepared_session.writer_store
    else:
        binding = None
        owns_binding = True
    if writer_store is None:
        from mtf_lab.ops.supervision import SupervisorStateStore, _network_account_key

        account_key_for_lock = _network_account_key(provenance, config, "fixture", provider)
        writer_store = SupervisorStateStore(state_dir, account_key_for_lock)
    else:
        from mtf_lab.ops.supervision import SupervisorStateStore

        if not isinstance(writer_store, SupervisorStateStore):
            raise CanaryGateError("writer_store debe ser SupervisorStateStore canónico")
    lock_context = writer_store.lock()
    with lock_context:
        try:
            zero_spread: CanaryZeroSpreadAuthorization | None = None
            if approval.canary_zero_spread_authorized is True:
                from mtf_lab.ops.ctrader_canary_inputs import CanarySessionEvidence

                zero_evidence = CanarySessionEvidence.from_provider(provider)
                zero_evidence.validate_current(provider, clock_fn())
                zero_spread = _zero_spread_authorization(approval, zero_evidence)
                assert zero_spread is not None
                if not zero_spread.matches(
                    account_id=zero_evidence.account_id,
                    symbol=approval.symbol,
                    session_id=zero_evidence.session_id,
                    connection_generation=zero_evidence.connection_generation,
                    endpoint=zero_evidence.endpoint,
                    now=clock_fn(),
                    require_order_window=True,
                ):
                    raise CanaryGateError("spread cero fuera de la identidad/ventana de órdenes")
                if prepared_session is not None:
                    if prepared_session.zero_spread_authorization != zero_spread:
                        raise CanaryGateError("spread cero no coincide con la aprobación de preparación")
                    # Preserve the very capability carried through capture
                    # and the inactive executor, not merely an equal rebuild.
                    zero_spread = prepared_session.zero_spread_authorization
                    assert zero_spread is not None
                _set_provider_zero_spread(provider, zero_spread)
            elif getattr(provider, "canary_zero_spread_authorization", None) is not None:
                raise CanaryGateError("provider conserva un opt-in de spread cero no aprobado para este trial")
            if binding is None:
                binding = build_demo_execution_binding(
                    provider,
                    provenance,
                    config=config,
                    state_dir=state_dir,
                    resume=True,
                    account_key=account_key,
                    proto=proto,
                    clock=clock_fn,
                    defer_activation=bool(execute),
                    zero_spread_authorization=zero_spread,
                )
            _validate_binding_identity(binding, provider, provenance, config, approval)
            if getattr(binding.executor, "canary_zero_spread_authorization", None) != zero_spread:
                raise CanaryGateError("executor no conserva el contexto de spread cero de esta aprobación")
            if execute and approval.canary_trial_anchor_authorized is True:
                from mtf_lab.ops.supervision_demo import update_executor_from_canary_observation

                canary_observer = getattr(binding, "risk_observer", None)
                if not callable(getattr(canary_observer, "observe", None)):
                    raise CanaryGateError("canary trial autorizado requiere observer.observe() y account_complete")
                if risk_refresh is None:

                    def canary_risk_refresh(executor: Any) -> Mapping[str, Any]:
                        return update_executor_from_canary_observation(
                            canary_observer,
                            executor,
                            expected_observation=getattr(binding, "observation", None),
                        )

                    risk_refresh = canary_risk_refresh
            _check_window(approval, clock_fn())
            _check_market_window(provider, approval, clock_fn(), market_window_state)
            _check_positions(binding.executor, empty=True)
            _refresh_risk(
                binding,
                risk_refresh,
                now=clock_fn,
                side="CONSERVATIVE",
                economics_refresh=economics_refresh,
            )
            grid = binding.transport.volume_grid
            if not isinstance(grid, VolumeGrid):
                raise CanaryGateError("catálogo DEMO sin VolumeGrid observado")
            quantity = derive_canary_quantity(grid, approval.max_quantity)
            if binding.executor.policy.max_quantity < quantity:
                raise CanaryGateError("max_quantity configurado es menor que minVolume observado")
            initial_risk, trial_equity = _risk_metrics(binding.executor)
            if not _daily_anchor_ready(initial_risk) and approval.canary_trial_anchor_authorized is not True:
                raise CanaryGateError(
                    "daily anchor UNKNOWN requiere autorización explícita del trial anchor",
                    gates={"daily_complete": False, "trial_anchor_authorized": False},
                )
            require_trial_observation = bool(execute and getattr(binding, "risk_observer", None) is not None)
            trial_cashflow_anchor = _trial_cashflow_total(initial_risk, required=require_trial_observation)
            trial_loss_budget = trial_equity * approval.max_trial_loss_fraction
            cycles: list[Mapping[str, Any]] = []
            mutation_count = 0
            trial_loss_committed = Decimal("0")
            ledger = _ApprovalLedger(writer_store.root) if execute else None
            reservation: str | None = None
            ledger_context = (
                _trial_ledger_context(
                    approval,
                    binding,
                    trial_equity=trial_equity,
                    trial_loss_budget=trial_loss_budget,
                    trial_cashflow_total=trial_cashflow_anchor,
                )
                if execute
                else None
            )

            def publish_trial_loss(
                *,
                mutation_messages: int,
                committed: Decimal,
                observed: Decimal | None = None,
                operation: str | None = None,
                status: str = "PENDING",
                error: str | None = None,
            ) -> None:
                if ledger is None or reservation is None or ledger_context is None:
                    return
                ledger.update_trial(
                    reservation,
                    mutation_messages=mutation_messages,
                    trial_loss_committed=committed,
                    trial_loss_observed=observed,
                    context=ledger_context,
                    operation=operation,
                    status=status,
                    error=error,
                )
                if approval.canary_trial_anchor_authorized is True:
                    updater = getattr(binding.executor, "update_canary_trial_loss", None)
                    if not callable(updater):
                        raise CanaryGateError("executor no expone actualización monotónica del trial")
                    updater(
                        committed=committed,
                        observed=observed,
                        expected_digest=str(ledger_context["approval_digest"]),
                    )

            try:
                if ledger is not None:
                    assert ledger_context is not None
                    reservation = ledger.reserve(
                        approval,
                        trial_equity=trial_equity,
                        trial_loss_budget=trial_loss_budget,
                        context=ledger_context,
                    )
                if execute:
                    if reservation is None or ledger is None or ledger_context is None:
                        raise CanaryGateError("canary execute requiere reserva durable de aprobación")
                    if approval.canary_trial_anchor_authorized is True:
                        if trial_cashflow_anchor is None:
                            raise CanaryGateError("canary trial requiere cashflow_total observado")
                        setter = getattr(binding.executor, "set_canary_trial_risk", None)
                        if not callable(setter):
                            raise CanaryGateError("executor no expone canary trial risk boundary")
                        setter(
                            _canary_trial_boundary(
                                approval,
                                binding,
                                approval_digest=str(ledger_context["approval_digest"]),
                                trial_equity=trial_equity,
                                trial_cashflow_total=trial_cashflow_anchor,
                                trial_loss_budget=trial_loss_budget,
                            )
                        )
                    if not bool(getattr(binding.executor, "active", False)):
                        activate = getattr(binding.executor, "activate", None)
                        if not callable(activate):
                            raise CanaryGateError("canary execute requiere executor.activate()")
                        activate()
                cycles_input = (
                    ("BUY", buy_signal, inputs.buy_runtime),
                    ("SELL", sell_signal, inputs.sell_runtime),
                )
                for cycle_index, (expected, signal, runtime) in enumerate(cycles_input):
                    _refresh_risk(
                        binding,
                        risk_refresh,
                        now=clock_fn,
                        side=expected,
                        economics_refresh=economics_refresh,
                    )
                    current_risk, _current_equity = _risk_metrics(binding.executor)
                    observed_trial_loss = _observed_trial_loss(
                        current_risk,
                        anchor_equity=trial_equity,
                        anchor_cashflow_total=trial_cashflow_anchor,
                        required=require_trial_observation,
                    )
                    if observed_trial_loss is not None:
                        trial_loss_committed = max(trial_loss_committed, observed_trial_loss)
                        if trial_loss_committed >= trial_loss_budget:
                            if reservation is not None and ledger is not None:
                                publish_trial_loss(
                                    mutation_messages=mutation_count,
                                    committed=trial_loss_committed,
                                    observed=observed_trial_loss,
                                    operation="TRIAL_BUDGET",
                                    status="BREACHED",
                                )
                            raise CanaryGateError("trial loss observado alcanzó 0.001 de equity")
                    _check_window(approval, clock_fn())
                    _check_market_window(provider, approval, clock_fn(), market_window_state)
                    if mutation_count + 2 > approval.max_mutation_messages:
                        raise CanaryGateError("presupuesto de mensajes mutación agotado")
                    if not execute:
                        _signal_direction(signal, expected)
                        _quote, plan = _plan_for(binding, provider, signal, runtime, quantity, approval, clock_fn())
                        trial_loss_committed = max(
                            trial_loss_committed,
                            Decimal(str(plan["expected_total_loss_at_stop"])),
                        )
                        if trial_loss_committed > trial_loss_budget:
                            raise CanaryGateError("trial loss budget de 0.001 de equity excedido")
                        cycles.append({"direction": expected, "state": "PRECHECKED"})
                        continue
                    planned = _plan_for(binding, provider, signal, runtime, quantity, approval, clock_fn())
                    planned_loss = Decimal(str(planned[1]["expected_total_loss_at_stop"]))
                    if cycle_index == 0:
                        trial_loss_committed = max(trial_loss_committed, planned_loss)
                    else:
                        # The first cycle's realized loss is already included
                        # in the observed trial anchor above; reserve the
                        # second cycle on top of that observed amount.
                        trial_loss_committed = (
                            max(trial_loss_committed, observed_trial_loss or Decimal("0")) + planned_loss
                        )
                    if trial_loss_committed > trial_loss_budget:
                        raise CanaryGateError("trial loss budget de 0.001 de equity excedido")
                    if reservation is not None and ledger is not None:
                        publish_trial_loss(
                            mutation_messages=mutation_count,
                            committed=trial_loss_committed,
                        )

                    def announce(
                        operation: str,
                        sequence: int,
                        status: str,
                        error: str | None,
                        base_mutations: int = mutation_count,
                        base_loss: Decimal = trial_loss_committed,
                    ) -> None:
                        if reservation is not None and ledger is not None:
                            publish_trial_loss(
                                mutation_messages=base_mutations + sequence,
                                committed=base_loss,
                                operation=operation,
                                status=status,
                                error=error,
                            )

                    try:
                        cycle, used = _cycle(
                            binding,
                            provider,
                            approval,
                            expected,
                            signal,
                            runtime,
                            quantity,
                            clock_fn(),
                            clock_fn,
                            market_window_state,
                            planned=planned,
                            mutation_announce=announce,
                        )
                    except CanaryGateError as exc:
                        # _cycle reports only its local OPEN/CLOSE count;
                        # preserve completed prior cycles when an exception
                        # crosses the runner boundary and reaches CLI.
                        total_mutations = mutation_count + exc.mutation_messages
                        if total_mutations > exc.mutation_messages:
                            raise CanaryGateError(
                                exc.reason,
                                gates=exc.gates,
                                mutation_messages=total_mutations,
                            ) from exc
                        raise
                    mutation_count += used
                    final_trial_loss: Decimal | None = None
                    if execute:
                        final_refresh = post_close_risk_refresh or risk_refresh
                        _refresh_risk(
                            binding,
                            final_refresh,
                            now=clock_fn,
                            side="CONSERVATIVE",
                            apply_economics=False,
                        )
                        _check_positions(binding.executor, empty=True)
                        final_risk, _final_equity = _risk_metrics(binding.executor)
                        final_trial_loss = _observed_trial_loss(
                            final_risk,
                            anchor_equity=trial_equity,
                            anchor_cashflow_total=trial_cashflow_anchor,
                            required=require_trial_observation,
                        )
                        if final_trial_loss is not None:
                            trial_loss_committed = max(trial_loss_committed, final_trial_loss)
                        if trial_loss_committed >= trial_loss_budget:
                            breach_gates = {
                                "state": "RISK_BUDGET_BREACHED",
                                "trial_loss_committed": str(trial_loss_committed),
                                "trial_loss_budget": str(trial_loss_budget),
                                "trial_loss_observed": str(final_trial_loss) if final_trial_loss is not None else None,
                                "positions_closed": True,
                            }
                            if reservation is not None and ledger is not None:
                                publish_trial_loss(
                                    mutation_messages=mutation_count,
                                    committed=trial_loss_committed,
                                    observed=final_trial_loss,
                                    operation="TRIAL_BUDGET",
                                    status="BREACHED",
                                )
                            raise CanaryGateError(
                                "trial loss observado alcanzó 0.001 de equity después del cierre",
                                gates=breach_gates,
                                mutation_messages=mutation_count,
                            )
                    if reservation is not None and ledger is not None:
                        publish_trial_loss(
                            mutation_messages=mutation_count,
                            committed=trial_loss_committed,
                            observed=final_trial_loss,
                        )
                    cycles.append(cycle)
                if reservation is not None and ledger is not None:
                    if ledger_context is None:
                        raise CanaryGateError("reserva de canaria sin contexto durable")
                    ledger.finish(
                        reservation,
                        state="COMPLETED",
                        mutation_messages=mutation_count,
                        context=ledger_context,
                    )
                if not execute:
                    binding.executor.deactivate("preflight_only")
                    return CanaryResult(True, "PREFLIGHT_OK", "preflight", {"quantity": str(quantity)}, tuple(cycles))
                binding.executor.deactivate("canary_complete")
                return CanaryResult(
                    True,
                    "CANARY_COMPLETED",
                    "canary",
                    {"quantity": str(quantity), "approval": True},
                    tuple(cycles),
                    mutation_count,
                )
            except BaseException as exc:
                if isinstance(exc, CanaryGateError):
                    mutation_count = max(mutation_count, exc.mutation_messages)
                if reservation is not None and ledger is not None:
                    with contextlib.suppress(Exception):
                        if ledger_context is None:
                            raise CanaryGateError("reserva de canaria sin contexto durable") from exc
                        ledger_state = (
                            "RISK_BUDGET_BREACHED"
                            if isinstance(exc, CanaryGateError) and exc.gates.get("state") == "RISK_BUDGET_BREACHED"
                            else ("UNKNOWN" if mutation_count else "ABORTED")
                        )
                        ledger.finish(
                            reservation,
                            state=ledger_state,
                            mutation_messages=mutation_count,
                            context=ledger_context,
                            error=str(exc),
                        )
                raise
        finally:
            if binding is not None:
                with contextlib.suppress(Exception):
                    if bool(getattr(binding.executor, "active", False)):
                        binding.executor.deactivate("canary_runner_exit")
                if owns_binding:
                    binding.close()


def run_prepared_canary(
    session: PreparedCanarySession,
    *,
    approval: CanaryApproval,
    inputs: Any,
    execute: bool = False,
    risk_refresh: Callable[[Any], Mapping[str, Any]] | None = None,
    economics_refresh: Callable[[Any, Mapping[str, Any]], Any] | None = None,
    post_close_risk_refresh: Callable[[Any], Mapping[str, Any]] | None = None,
    market_window_state: Callable[[Any, datetime, datetime], Any] | None = None,
    clock: Callable[[], datetime] | None = None,
) -> CanaryResult:
    """Run the bounded runner on a session prepared by ``prepare_cli_session``."""

    if session.binding is None:
        raise CanaryGateError("la sesión preparada es read-only; execution=True requiere aprobación posterior")
    return run_demo_canary(
        session.provider,
        session.provenance,
        session.config,
        state_dir=session.writer_store.root,
        approval=approval,
        inputs=inputs,
        execute=execute,
        writer_store=session.writer_store,
        risk_refresh=risk_refresh,
        economics_refresh=economics_refresh,
        post_close_risk_refresh=post_close_risk_refresh,
        market_window_state=market_window_state,
        clock=clock,
        prepared_session=session,
    )


def static_cli_preflight(config_path: str | Path, approval_file: str | Path | None = None) -> dict[str, Any]:
    """Read only the effective TOML and return canary-specific blockers."""

    try:
        config = load_config(config_path)
        approval = load_approval_file(approval_file) if approval_file is not None else None
        gates = validate_static_config(_preactivation_config(config), approval, observed_economics=True)
    except (ConfigError, CanaryGateError, OSError, ValueError) as exc:
        return {
            "schema": SCHEMA,
            "ok": False,
            "state": "PREFLIGHT_BLOCKED",
            "mode": "preflight",
            "gates": getattr(exc, "gates", {}),
            "error": str(exc),
            "network_performed": False,
            "orders_attempted": False,
            "approval_loaded": approval_file is not None,
        }
    pending_gates = {
        **gates,
        "execution_enabled": False,
        "ready_for_orders": False,
        "pending_observed_activation": True,
        "observed_economics": False,
        "calendar_config_present": isinstance(_raw_execution(config).get("risk_calendar"), Mapping),
        "calendar_observed": False,
    }
    return {
        "schema": SCHEMA,
        "ok": True,
        "state": "PREFLIGHT_CONFIG_OK_PENDING_OBSERVED_GATES",
        "mode": "preflight",
        "gates": pending_gates,
        "execution_enabled": False,
        "ready_for_orders": False,
        "network_performed": False,
        "orders_attempted": False,
        "approval_loaded": approval_file is not None,
        "next_action": "órdenes bloqueadas hasta superar los gates observados y estar dentro de la ventana aprobada; se requiere sesión DEMO/trading, BBO/ATR/riesgo/calendario frescos y activación explícita",
    }


def network_cli_preflight(  # noqa: C901
    config_path: str | Path,
    state_dir: str | Path,
    *,
    execute: bool = False,
    approval: CanaryApproval | None = None,
    max_events: int | None = None,
) -> dict[str, Any]:
    """Prepare the real CLI session, optionally running the bounded canary.

    With ``execute=False`` the existing query composition authenticates and
    verifies the server account and catalog in read-only mode.  With
    ``execute=True`` the explicit private approval and the observed producer
    inputs are passed through the same canonical binding to the two-cycle
    runner; no order is attempted unless all of those gates pass.
    """

    preparation_start: datetime | None = None
    if execute:
        if approval is None:
            return {
                "schema": SCHEMA,
                "ok": False,
                "state": "APPROVAL_REQUIRED",
                "mode": "canary",
                "network_performed": False,
                "orders_attempted": False,
            }
        if max_events is None or isinstance(max_events, bool) or max_events <= 0:
            return {
                "schema": SCHEMA,
                "ok": False,
                "state": "MAX_EVENTS_REQUIRED",
                "mode": "canary",
                "network_performed": False,
                "orders_attempted": False,
            }
        assert approval is not None
        assert max_events is not None
        approved: CanaryApproval | None = approval
        event_limit: int | None = max_events
        preparation_start = approval.window_start - timedelta(minutes=5)
        current = datetime.now(UTC)
        if current < preparation_start or current >= approval.window_end:
            return {
                "schema": SCHEMA,
                "ok": False,
                "state": "PREPARATION_WINDOW_NOT_STARTED"
                if current < preparation_start
                else "PREPARATION_WINDOW_CLOSED",
                "mode": "canary",
                "network_performed": False,
                "orders_attempted": False,
                "preparation_start": preparation_start.isoformat(),
                "window_end": approval.window_end.isoformat(),
            }
    else:
        approved = None
        event_limit = None
    session = prepare_cli_session(config_path, state_dir, execution=execute)
    try:
        if execute:
            assert approved is not None and event_limit is not None and preparation_start is not None
            from mtf_lab.ops.ctrader_account_risk import AccountRiskObserver
            from mtf_lab.ops.ctrader_canary_economics import ExitSlippageHypothesis, observe_canary_economics
            from mtf_lab.ops.ctrader_canary_inputs import CanarySessionEvidence, collect_canary_inputs
            from mtf_lab.ops.market_schedule import observed_market_window_state
            from mtf_lab.ops.supervision_demo import (
                ensure_demo_journal_parent,
                update_executor_from_canary_observation,
            )

            def economics_window_start() -> datetime:
                return approved.window_start if datetime.now(UTC) >= approved.window_start else preparation_start

            try:
                # The persisted profile is intentionally execution-disabled.
                # Validate the remaining canary contract against an in-memory
                # activation view only; the actual enabled/calendar view is
                # created below from the observed projection.
                pre_activation = _preactivation_config(session.config)
                validate_static_config(pre_activation, approved, observed_economics=True)
            except CanaryGateError as exc:
                return {
                    "schema": SCHEMA,
                    "ok": False,
                    "state": "CONFIG_BLOCKED",
                    "mode": "canary",
                    "gates": dict(exc.gates),
                    "error": exc.reason,
                    "network_performed": True,
                    "orders_attempted": False,
                }

            endpoint = str(session.provenance.get("endpoint", "demo.ctraderapi.com:5035"))
            with session.writer_store.lock():
                journal_path = ensure_demo_journal_parent(
                    state_dir,
                    approved.account_id,
                    endpoint,
                    account_key=None,
                )
                risk_state_path = journal_path.with_suffix(".risk.json")
                journal_empty = not journal_path.exists() or journal_path.stat().st_size == 0
                allow_new_baseline = journal_empty and not risk_state_path.exists()
                observer = AccountRiskObserver(
                    session.provider,
                    state_path=risk_state_path,
                    journal_path=journal_path,
                    allow_new_baseline=allow_new_baseline,
                    require_cashflows=True,
                )
            evidence = CanarySessionEvidence.from_provider(session.provider)
            zero_spread = _zero_spread_authorization(
                approved,
                evidence,
                preparation_start=preparation_start,
            )
            if zero_spread is not None:
                _set_provider_zero_spread(session.provider, zero_spread)
                session.zero_spread_authorization = zero_spread

            def refresh_quotes_from_same_reader() -> None:
                """Bound quote refresh through the provider's existing reader."""

                stream = getattr(session.provider, "stream", None)
                if callable(stream):
                    # A live provider may enter this path without a preloaded
                    # BBO.  Poll only the existing authenticated reader; do
                    # not create a client, reconnect, or synthesize prices.
                    for _record in cast(
                        Iterable[Any],
                        stream(
                            max_events=max(1, min(int(event_limit), 4)),
                            duration_seconds=5.0,
                            timeout_seconds=0.25,
                        ),
                    ):
                        del _record

            def _projection_from_snapshot(snapshot: AccountRiskSnapshot | None) -> CanaryEconomicsProjection:
                if snapshot is None:
                    raise CanaryGateError("account risk snapshot no está disponible")
                if not snapshot.fresh or getattr(snapshot, "account_complete", False) is not True:
                    raise CanaryGateError(
                        "account risk snapshot no está fresh+account_complete",
                        gates={
                            "fresh": snapshot.fresh,
                            "complete": snapshot.complete,
                            "account_complete": getattr(snapshot, "account_complete", False),
                            "reasons": list(snapshot.reasons),
                        },
                    )
                if snapshot.complete is not True and approved.canary_trial_anchor_authorized is not True:
                    raise CanaryGateError(
                        "daily anchor UNKNOWN requiere autorización explícita del trial anchor",
                        gates={
                            "fresh": snapshot.fresh,
                            "account_complete": getattr(snapshot, "account_complete", False),
                            "daily_complete": snapshot.complete,
                            "trial_anchor_authorized": False,
                            "reasons": list(snapshot.reasons),
                        },
                    )
                return observe_canary_economics(
                    session.provider,
                    snapshot,
                    # Before the approved execution window this is only a
                    # forecast/readiness projection.  The runner's own
                    # pre-order gates still use approved.window_start.
                    window_start=economics_window_start(),
                    window_end=approved.window_end,
                    quantity=approved.max_quantity,
                    max_holding_seconds=approved.max_holding_seconds,
                    # This is a forecast-only implementation hypothesis.  It
                    # is deliberately independent from CanaryApproval and is
                    # never attributed to the human approval bytes.
                    exit_slippage_hypothesis=ExitSlippageHypothesis(
                        pips=Decimal("0.1"),
                        basis="IMPLEMENTATION_HYPOTHESIS",
                        source="baseline-0.1pip",
                    ),
                    zero_spread_authorization=zero_spread,
                )

            def make_projection() -> CanaryEconomicsProjection:
                # Omit explicit ``now``: AccountRiskObserver takes its
                # terminal clock sample after the bounded request sequence.
                refresh_quotes_from_same_reader()
                return _projection_from_snapshot(observer.observe())

            def activate_effective_config(projection: CanaryEconomicsProjection) -> EffectiveConfig:
                effective = _effective_canary_config(session.config, projection)
                validate_static_config(effective, approved, observed_economics=True)
                return effective

            try:
                session.observe_and_arm(make_projection, config_factory=activate_effective_config)
            except CanaryGateError as exc:
                return {
                    "schema": SCHEMA,
                    "ok": False,
                    "state": "ACCOUNT_RISK_BLOCKED",
                    "mode": "canary",
                    "gates": dict(exc.gates),
                    "error": exc.reason,
                    "network_performed": True,
                    "orders_attempted": False,
                }
            except Exception as exc:
                return {
                    "schema": SCHEMA,
                    "ok": False,
                    "state": "ECONOMICS_BLOCKED",
                    "mode": "canary",
                    "error": type(exc).__name__,
                    "network_performed": True,
                    "orders_attempted": False,
                }
            if session.binding is None:
                return {
                    "schema": SCHEMA,
                    "ok": False,
                    "state": "EXECUTION_BINDING_REQUIRED",
                    "mode": "canary",
                    "network_performed": True,
                    "orders_attempted": False,
                }
            # build_demo_execution_binding owns the live observer used by the
            # binding.  Switch the closure to that exact instance; using the
            # pre-arm observer here would re-project the initial snapshot
            # after every fresh risk callback.
            bound_observer = session.binding.risk_observer
            observer_callback: Callable[[Any], Mapping[str, Any]] | None
            if approved.canary_trial_anchor_authorized is True:
                if bound_observer is None or not callable(getattr(bound_observer, "observe", None)):
                    return {
                        "schema": SCHEMA,
                        "ok": False,
                        "state": "RISK_OBSERVER_REQUIRED",
                        "mode": "canary",
                        "error": "canary trial autorizado requiere observer.observe() y account_complete",
                        "network_performed": True,
                        "orders_attempted": False,
                    }

                def trial_observer_callback(executor: Any) -> Mapping[str, Any]:
                    return update_executor_from_canary_observation(
                        bound_observer,
                        executor,
                        expected_observation=getattr(session.binding, "observation", None),
                    )

                observer_callback = trial_observer_callback
            else:
                observer_callback = getattr(bound_observer, "update_executor", None)
            if bound_observer is None or not callable(observer_callback):
                return {
                    "schema": SCHEMA,
                    "ok": False,
                    "state": "RISK_OBSERVER_REQUIRED",
                    "mode": "canary",
                    "network_performed": True,
                    "orders_attempted": False,
                }
            observer = bound_observer

            def risk_refresh(executor: CTraderDemoExecutor) -> Mapping[str, Any]:
                refresh_quotes_from_same_reader()
                observed = observer_callback(executor)
                return observed

            def post_close_risk_refresh(executor: CTraderDemoExecutor) -> Mapping[str, Any]:
                # After a confirmed close, account/cashflow observation is
                # sufficient for the trial budget.  Do not spend another
                # five-second quote poll or rebuild forecast economics here.
                return observer_callback(executor)

            def economics_refresh(
                executor: CTraderDemoExecutor, observed: Mapping[str, Any]
            ) -> CanaryEconomicsProjection:
                del executor, observed
                snapshot = observer.last_observation
                refreshed = _projection_from_snapshot(snapshot)
                return refreshed

            try:
                with session.writer_store.lock():
                    observed = risk_refresh(session.binding.executor)
                    refreshed = economics_refresh(session.binding.executor, observed)
                    session.binding.canary_economics = refreshed
                    from mtf_lab.ops.supervision_demo import _update_executor_from_canary_economics

                    _update_executor_from_canary_economics(session.binding.executor, refreshed, "CONSERVATIVE")
            except Exception as exc:
                return {
                    "schema": SCHEMA,
                    "ok": False,
                    "state": "RISK_REFRESH_BLOCKED",
                    "mode": "canary",
                    "error": type(exc).__name__,
                    "network_performed": True,
                    "orders_attempted": False,
                }

            input_result = collect_canary_inputs(
                session.provider,
                session.config,
                network=True,
                deadline=approved.window_end,
                max_events=event_limit,
                window_start=approved.window_start,
                window_end=approved.window_end,
                preparation_start=preparation_start,
                session_evidence=evidence,
                runtime_observer=session.binding.executor.observe_runtime,
                # Do not pass executor: the collector's compatibility
                # fallback would otherwise re-enable risk_entry_plan before
                # the post-collection financial refresh.
                executor=None,
                risk_planner=None,
                requested_quantity=approved.max_quantity,
                fetch_warmup=False,
                technical_only=True,
                zero_spread_authorization=zero_spread,
                minimum_execution_margin_seconds=Decimal("300"),
                market_window_state=observed_market_window_state,
                isolated_state_dir=state_dir,
                close_provider=False,
            )
            if not input_result.ok or input_result.context is None:
                return {
                    "schema": SCHEMA,
                    "ok": False,
                    "state": "OBSERVED_INPUTS_BLOCKED",
                    "mode": "canary",
                    "gates": dict(input_result.gates),
                    "reason": input_result.reason,
                    "network_performed": True,
                    "orders_attempted": False,
                }
            result = run_prepared_canary(
                session,
                approval=approved,
                inputs=input_result.context,
                execute=True,
                risk_refresh=risk_refresh,
                economics_refresh=economics_refresh,
                post_close_risk_refresh=post_close_risk_refresh,
                market_window_state=observed_market_window_state,
            )
            return result.to_dict()
        status = None
        risk = None
        grid = None
        try:
            from mtf_lab.ops.supervision_demo import _observed_volume_grid

            grid = _observed_volume_grid(session.provider, datetime.now(UTC))
        except Exception:
            grid = None
        if session.binding is not None:
            status = session.binding.executor.status(refresh=False)
            risk = status.get("risk") if isinstance(status, Mapping) else None
            grid = session.binding.transport.volume_grid
        trading_scope_observed = False
        server_session_proof_observed = False
        try:
            client = getattr(session.provider, "client", None)
            proof_reader = getattr(client, "authenticated_session_evidence", None)
            proof_validator = getattr(client, "validate_session_evidence", None)
            proof = proof_reader() if callable(proof_reader) else None
            valid_proof = proof_validator(proof) is True if callable(proof_validator) else False
            scopes = getattr(proof, "scopes", ())
            server_account = str(getattr(proof, "account_id", "")).strip()
            server_endpoint = str(getattr(proof, "endpoint", "")).strip().lower()
            server_session_proof_observed = bool(
                valid_proof
                and server_account == str(session.provenance.get("account_id", "")).strip()
                and server_endpoint == str(session.provenance.get("endpoint", "")).strip().lower()
            )
            trading_scope_observed = server_session_proof_observed and "trading" in {
                str(scope).strip().lower() for scope in scopes
            }
        except Exception:
            server_session_proof_observed = False
            trading_scope_observed = False
        gates = {
            "server_session_observed": server_session_proof_observed,
            "account_selected": session.provenance.get("account_selected") is True,
            "account_verified": session.provenance.get("account_verified") is True,
            "trading_scope_observed": trading_scope_observed,
            "catalog_volume_grid_observed": isinstance(grid, VolumeGrid),
            "risk_snapshot_ready": (
                isinstance(risk, Mapping)
                and isinstance(risk.get("metrics"), Mapping)
                and risk["metrics"].get("equity") is not None
            ),
            "runtime_inputs_observed": False,
            "orders_attempted": False,
        }
        return {
            "schema": SCHEMA,
            "ok": False,
            "state": "OBSERVED_RUNTIME_INPUTS_REQUIRED" if execute else "NETWORK_PREFLIGHT_INPUTS_REQUIRED",
            "mode": "canary" if execute else "preflight",
            "gates": gates,
            "next_action": "La canaria sigue pendiente de inputs/gates vivos dentro de la preparación y ventana aprobadas; el productor técnico es automático y no se fabrican ATR, warmup ni READY.",
            "network_performed": True,
        }
    finally:
        session.close()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--approval-file", type=Path, help="aprobación privada 0600; nunca se imprime su ID")
    parser.add_argument("--preflight", action="store_true", help="valida sólo gates de configuración; no usa red")
    parser.add_argument(
        "--network",
        action="store_true",
        help="prepara la sesión DEMO mediante el CLI canónico; sin --execute es sólo lectura",
    )
    parser.add_argument("--state-dir", type=Path, help="directorio privado explícito para sesión, ledger y canary")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="habilita la canary DEMO acotada con aprobación privada e inputs observados",
    )
    parser.add_argument("--max-events", type=int, help="límite explícito del productor causal en --execute")
    parser.add_argument(
        "--canary",
        action="store_true",
        help="rechazado desde CLI: requiere provider autenticado y writer lock inyectados por el supervisor",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.network:
        if args.state_dir is None:
            print("--network requiere --state-dir privado explícito", file=sys.stderr)
            return 2
        try:
            approval = load_approval_file(args.approval_file) if args.approval_file is not None else None
            payload = network_cli_preflight(
                args.config,
                args.state_dir,
                execute=args.execute,
                approval=approval,
                max_events=args.max_events,
            )
            if args.approval_file is not None:
                payload["approval_loaded"] = True
        except Exception as exc:
            mutation_messages = int(getattr(exc, "mutation_messages", 0) or 0)
            execution_unknown = bool(args.execute and mutation_messages > 0)
            exception_gates = getattr(exc, "gates", {})
            budget_breached = bool(
                args.execute
                and isinstance(exception_gates, Mapping)
                and exception_gates.get("state") == "RISK_BUDGET_BREACHED"
            )
            payload = {
                "schema": SCHEMA,
                "ok": False,
                "state": (
                    "RISK_BUDGET_BREACHED"
                    if budget_breached
                    else ("CANARY_EXECUTION_UNKNOWN" if execution_unknown else "NETWORK_PREFLIGHT_BLOCKED")
                ),
                "mode": "canary" if args.execute else "preflight",
                "error": f"{type(exc).__name__}",
                "network_performed": True if args.execute else None,
                "orders_attempted": execution_unknown,
                "mutation_messages": mutation_messages,
                "execution_state": (
                    "RISK_BUDGET_BREACHED" if budget_breached else ("UNKNOWN" if execution_unknown else "NOT_STARTED")
                ),
                "account_reconciliation_required": execution_unknown and not budget_breached,
                "human_review_required": execution_unknown,
            }
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return 0 if payload.get("ok") else 2
    if args.canary:
        print(
            json.dumps(
                {
                    "schema": SCHEMA,
                    "ok": False,
                    "state": "PREPARED_PROVIDER_REQUIRED",
                    "mode": "canary",
                    "network_performed": False,
                    "orders_attempted": False,
                    "error": "use run_demo_canary() dentro del supervisor con sesión y writer lock existentes",
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 2
    if not args.preflight:
        print("elige --preflight; la canary CLI no crea sesiones ni credenciales", file=sys.stderr)
        return 2
    payload = static_cli_preflight(args.config, args.approval_file)
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0 if payload.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
