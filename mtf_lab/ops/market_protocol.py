"""Frozen local protocol for the historical market-research campaign.

This module contains policy and identity only.  It does not acquire market
data, open a provider connection, or evaluate a strategy.  A protocol is
deliberately explicit enough that a holdout cannot be opened by accident or
silently changed between attempts.
"""

from __future__ import annotations

import json
import os
import stat
import uuid
from collections.abc import Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Literal, TypeAlias, cast

from ..core.canonical import canonical_json, fingerprint, instant_text
from ..core.models import parse_timeframe
from ..core.risk_exit import RiskExitPolicy as CoreRiskExitPolicy
from ..data.historical import DatasetManifest

PROTOCOL_SCHEMA = "mtf-lab.market-research-protocol.v1"
PROTOCOL_VERSION = 1
CandidateId = Literal[
    "tp_fast_v1",
    "tp_intraday_slow_v1",
    "tp_multiday_v1",
    "dc_m5_v1",
    "dc_m15_v1",
    "dc_h1_v1",
]
CANDIDATE_IDS: tuple[CandidateId, ...] = (
    "tp_fast_v1",
    "tp_intraday_slow_v1",
    "tp_multiday_v1",
    "dc_m5_v1",
    "dc_m15_v1",
    "dc_h1_v1",
)
_REPOSITORY = Path(__file__).resolve().parents[2]


class MarketProtocolError(ValueError):
    """Input, identity, or protocol-gate error."""


RiskExitPolicy: TypeAlias = CoreRiskExitPolicy


def _decimal(value: Any, *, name: str, positive: bool = False, maximum: Decimal | None = None) -> Decimal:
    if isinstance(value, bool):
        raise MarketProtocolError(f"{name} no admite booleanos")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise MarketProtocolError(f"{name} no es Decimal válido") from exc
    if not result.is_finite() or (positive and result <= 0):
        raise MarketProtocolError(f"{name} debe ser finito y positivo")
    if maximum is not None and result > maximum:
        raise MarketProtocolError(f"{name} excede {maximum}")
    return result


def _aware(value: Any, *, name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise MarketProtocolError(f"{name} debe ser ISO-8601 aware") from exc
    else:
        raise MarketProtocolError(f"{name} debe ser datetime o ISO-8601")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise MarketProtocolError(f"{name} debe incluir zona horaria")
    return parsed.astimezone(UTC)


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return instant_text(value)
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return _jsonable(value.to_dict())
    return value


def _safe_path(path: str | Path) -> Path:
    target = Path(path).expanduser()
    if not target.is_absolute():
        raise MarketProtocolError("la ruta del protocolo debe ser absoluta")
    target = Path(os.path.abspath(os.fspath(target)))
    try:
        target.relative_to(_REPOSITORY)
    except ValueError:
        return target
    raise MarketProtocolError("el artefacto del protocolo debe estar fuera del checkout")


def _check_parent(target: Path) -> None:
    current = Path(target.anchor)
    for part in target.parent.parts:
        if part == target.anchor:
            continue
        current /= part
        try:
            info = os.lstat(current)
        except FileNotFoundError:
            break
        if stat.S_ISLNK(info.st_mode):
            raise MarketProtocolError(f"el padre no puede ser symlink: {current}")
        if not stat.S_ISDIR(info.st_mode):
            raise MarketProtocolError(f"el padre no es directorio: {current}")


def _open_parent(target: Path, *, create: bool) -> int:
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    fd = os.open(target.anchor, flags)
    try:
        for component in target.parent.parts:
            if component == target.anchor:
                continue
            try:
                child = os.open(component, flags, dir_fd=fd)
            except FileNotFoundError as exc:
                if not create:
                    raise MarketProtocolError(f"directorio padre inexistente: {target.parent}") from exc
                with suppress(FileExistsError):
                    os.mkdir(component, 0o700, dir_fd=fd)
                child = os.open(component, flags, dir_fd=fd)
            os.close(fd)
            fd = child
    except BaseException:
        with suppress(OSError):
            os.close(fd)
        raise
    return fd


def _exclusive_json(path: str | Path, payload: Mapping[str, Any]) -> Path:
    target = _safe_path(path)
    parent_fd = _open_parent(target, create=True)
    token = uuid.uuid4().hex
    temporary_name = f".{target.name}.{os.getpid()}.{token}.tmp"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    try:
        try:
            info = os.stat(target.name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            info = None
        if info is not None:
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise MarketProtocolError(f"destino del protocolo no es un archivo regular exclusivo: {target}")
            raise MarketProtocolError(f"el protocolo ya existe; no se sobrescribe: {target}")
        fd = os.open(temporary_name, flags, 0o600, dir_fd=parent_fd)
    except BaseException:
        os.close(parent_fd)
        raise
    try:
        data = (canonical_json(_jsonable(payload)) + "\n").encode("utf-8")
        offset = 0
        while offset < len(data):
            offset += os.write(fd, data[offset:])
        os.fchmod(fd, 0o600)
        os.fsync(fd)
    except BaseException:
        with suppress(OSError):
            os.close(fd)
        with suppress(OSError):
            os.unlink(temporary_name, dir_fd=parent_fd)
        os.close(parent_fd)
        raise
    os.close(fd)
    try:
        os.link(temporary_name, target.name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
        fd_target = os.open(target.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent_fd)
        try:
            info = os.fstat(fd_target)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 2:
                raise MarketProtocolError("destino del protocolo perdió identidad exclusiva")
            os.fsync(fd_target)
        finally:
            os.close(fd_target)
        os.unlink(temporary_name, dir_fd=parent_fd)
        os.fsync(parent_fd)
    except BaseException:
        with suppress(OSError):
            os.unlink(temporary_name, dir_fd=parent_fd)
        os.close(parent_fd)
        raise
    os.close(parent_fd)
    return target


@dataclass(frozen=True, slots=True)
class CandidateSpec:
    """One of the six pre-registered strategy identities."""

    candidate_id: CandidateId
    strategy_impl: str
    timeframes: tuple[str, ...]
    role: str
    hypothesis: str
    parameter_scope: str = "FROZEN"

    def __post_init__(self) -> None:
        candidate = str(self.candidate_id)
        if candidate not in CANDIDATE_IDS:
            raise MarketProtocolError(f"candidate_id no soportado: {candidate}")
        normalized: list[str] = []
        for value in self.timeframes:
            try:
                normalized.append(parse_timeframe(value).name)
            except (TypeError, ValueError) as exc:
                raise MarketProtocolError(f"timeframe no soportado: {value!r}") from exc
        if not normalized or len(set(normalized)) != len(normalized):
            raise MarketProtocolError("cada candidate requiere temporalidades únicas")
        if not str(self.strategy_impl).strip() or not str(self.role).strip() or not str(self.hypothesis).strip():
            raise MarketProtocolError("candidate requiere strategy_impl, role e hypothesis")
        if str(self.parameter_scope).upper() != "FROZEN":
            raise MarketProtocolError("los parámetros de candidatos deben quedar FROZEN")
        object.__setattr__(self, "candidate_id", candidate)
        object.__setattr__(self, "strategy_impl", str(self.strategy_impl).strip())
        object.__setattr__(self, "timeframes", tuple(normalized))
        object.__setattr__(self, "role", str(self.role).strip())
        object.__setattr__(self, "hypothesis", str(self.hypothesis).strip())

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "strategy_impl": self.strategy_impl,
            "timeframes": list(self.timeframes),
            "role": self.role,
            "hypothesis": self.hypothesis,
            "parameter_scope": self.parameter_scope,
        }


DEFAULT_CANDIDATES: tuple[CandidateSpec, ...] = (
    CandidateSpec(
        "tp_fast_v1",
        "TrendPullbackStrategy",
        ("M1", "M5", "M15"),
        "BASELINE_FROZEN",
        "La baseline causal rápida se evalúa sin afirmar rentabilidad.",
    ),
    CandidateSpec(
        "tp_intraday_slow_v1",
        "TrendPullbackStrategy",
        ("M5", "M15", "H1"),
        "BASELINE_FROZEN_INTRADAY_SLOW",
        "La baseline lenta intradía se evalúa como identidad separada, sin afirmar rentabilidad.",
    ),
    CandidateSpec(
        "tp_multiday_v1",
        "TrendPullbackStrategy",
        ("M15", "H1", "D1"),
        "BASELINE_FROZEN_MULTIDAY",
        "La baseline multiday requiere datos y disponibilidad que cubran su temporalidad, sin afirmar rentabilidad.",
    ),
    CandidateSpec(
        "dc_m5_v1",
        "Donchian20M5Strategy",
        ("M5",),
        "DONCHIAN20_M5_CHALLENGER",
        "Donchian20 M5 es challenger implementado; no afirma rentabilidad.",
    ),
    CandidateSpec(
        "dc_m15_v1",
        "Donchian20M15Strategy",
        ("M15",),
        "DONCHIAN20_M15_CHALLENGER",
        "Donchian20 M15 requiere una implementación explícita; no afirma rentabilidad.",
    ),
    CandidateSpec(
        "dc_h1_v1",
        "Donchian20H1Strategy",
        ("H1",),
        "DONCHIAN20_H1_CHALLENGER",
        "Donchian20 H1 requiere una implementación explícita; no afirma rentabilidad.",
    ),
)


def _scenario_contract() -> dict[str, Any]:
    """Project the single owner scenario catalog into the preregistered hash."""

    try:
        from .historical_backtest import SCENARIO_PARAMETERS, SCENARIOS
    except ImportError as exc:
        raise MarketProtocolError("catálogo de escenarios owner no disponible") from exc
    return {
        "mode": "VIRTUAL_DIAGNOSTIC",
        "models": {
            name: {
                **dict(SCENARIO_PARAMETERS[name]),
                "role": (
                    "BASELINE_MODELED"
                    if name == "base"
                    else "EXTREME_DIAGNOSTIC"
                    if name == "extreme"
                    else "ADVERSE_COST_STRESS"
                ),
            }
            for name in SCENARIOS
        },
        "auto_promote": False,
        "trading_enabled": False,
    }


def quant_audit(policy: RiskExitPolicy, *, equity: Decimal | int | str | None = None) -> dict[str, Any]:
    """Use the shared core audit without inventing account state."""

    method = getattr(policy, "quant_audit", None)
    if callable(method):
        raw = cast(Mapping[str, Any], method(equity=equity))
        return dict(raw)
    result = dict(policy.serialize())
    result.update(
        {
            "status": "ASSESSED",
            "capital_basis": "EXPLICIT" if equity is not None else "NOT_ASSESSED",
            "capital": str(equity) if equity is not None else None,
            "broker_observed": False,
        }
    )
    return result


@dataclass(frozen=True, slots=True)
class ResearchProtocol:
    """Immutable approved default protocol for the local campaign."""

    candidates: tuple[CandidateSpec, ...] = DEFAULT_CANDIDATES
    warmup_start_year: int = 2015
    development_start_year: int = 2016
    development_end_year: int = 2019
    walkforward_years: tuple[int, ...] = (2020, 2021, 2022, 2023)
    holdout_start_year: int = 2024
    holdout_end_year: int = 2025
    holdout_locked: bool = True
    holdout_open: bool = False
    confirmation_access: str = "SINGLE"
    min_holdout_episodes: int = 120
    min_holdout_sessions: int = 100
    min_holdout_blocks: int = 20
    block_days: int = 14
    min_walkforward_episodes: int = 20
    bootstrap_iterations: int = 100_000
    bootstrap_seed: int = 20_260_913
    bootstrap_block_days: int = 14
    bootstrap_sensitivity: tuple[int, ...] = (7, 28)
    alpha: Decimal = Decimal("0.05")
    alpha_looks: int = 2
    alpha_candidates: int = 6
    stress_mean_r_min: Decimal = Decimal("0.05")
    max_drawdown_fraction: Decimal = Decimal("0.05")
    drop_best_count: int = 5
    risk_policy: RiskExitPolicy = field(default_factory=RiskExitPolicy)
    schema: str = PROTOCOL_SCHEMA
    version: int = PROTOCOL_VERSION

    def __post_init__(self) -> None:
        if self.schema != PROTOCOL_SCHEMA or self.version != PROTOCOL_VERSION:
            raise MarketProtocolError("schema/version del protocolo incompatible")
        if tuple(item.candidate_id for item in self.candidates) != CANDIDATE_IDS:
            raise MarketProtocolError("el protocolo requiere exactamente los seis candidatos registrados")
        if self.walkforward_years != (2020, 2021, 2022, 2023):
            raise MarketProtocolError("walk-forward requiere 2020/2021/2022/2023")
        if (
            self.warmup_start_year != 2015
            or self.development_start_year != 2016
            or self.development_end_year != 2019
            or self.holdout_start_year != 2024
            or self.holdout_end_year != 2025
        ):
            raise MarketProtocolError("periodos históricos no coinciden con el protocolo aprobado")
        if not self.holdout_locked or self.holdout_open or str(self.confirmation_access).upper() != "SINGLE":
            raise MarketProtocolError("el holdout debe permanecer cerrado y con acceso confirmatorio único")
        for name in (
            "min_holdout_episodes",
            "min_holdout_sessions",
            "min_holdout_blocks",
            "block_days",
            "min_walkforward_episodes",
            "bootstrap_iterations",
            "alpha_looks",
            "alpha_candidates",
            "drop_best_count",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise MarketProtocolError(f"{name} debe ser entero positivo")
        if (
            isinstance(self.bootstrap_block_days, bool)
            or not isinstance(self.bootstrap_block_days, int)
            or self.bootstrap_block_days != 14
        ):
            raise MarketProtocolError("bootstrap primario requiere bloques de 14 días")
        if self.bootstrap_sensitivity != (7, 28):
            raise MarketProtocolError("sensibilidad bootstrap requiere 7/28 días explícitos")
        object.__setattr__(
            self,
            "stress_mean_r_min",
            _decimal(self.stress_mean_r_min, name="stress_mean_r_min", maximum=Decimal("1")),
        )
        object.__setattr__(
            self,
            "max_drawdown_fraction",
            _decimal(self.max_drawdown_fraction, name="max_drawdown_fraction", positive=True, maximum=Decimal("1")),
        )
        object.__setattr__(self, "alpha", _decimal(self.alpha, name="alpha", positive=True, maximum=Decimal("1")))
        object.__setattr__(self, "confirmation_access", str(self.confirmation_access).upper())

    @classmethod
    def default(cls) -> ResearchProtocol:
        return cls()

    @property
    def candidate_ids(self) -> tuple[CandidateId, ...]:
        return CANDIDATE_IDS

    @property
    def bootstrap_sensitivity_days(self) -> tuple[int, ...]:
        return self.bootstrap_sensitivity

    def candidate(self, candidate_id: str) -> CandidateSpec:
        for candidate in self.candidates:
            if candidate.candidate_id == candidate_id:
                return candidate
        raise MarketProtocolError(f"candidate no registrado: {candidate_id}")

    def to_dict(self, *, include_hash: bool = True) -> dict[str, Any]:
        result: dict[str, Any] = {
            "schema": self.schema,
            "version": self.version,
            "candidates": [item.to_dict() for item in self.candidates],
            "periods": {
                "warmup_start_year": self.warmup_start_year,
                "development_start_year": self.development_start_year,
                "development_end_year": self.development_end_year,
                "walkforward_years": list(self.walkforward_years),
                "holdout_start_year": self.holdout_start_year,
                "holdout_end_year": self.holdout_end_year,
            },
            "holdout": {
                "locked": self.holdout_locked,
                "open": self.holdout_open,
                "confirmation_access": self.confirmation_access,
            },
            "sample": {
                "min_holdout_episodes": self.min_holdout_episodes,
                "min_holdout_sessions": self.min_holdout_sessions,
                "min_holdout_blocks": self.min_holdout_blocks,
                "min_walkforward_episodes": self.min_walkforward_episodes,
                "block_days": self.block_days,
            },
            "bootstrap": {
                "iterations": self.bootstrap_iterations,
                "seed": self.bootstrap_seed,
                "block_days": self.bootstrap_block_days,
                "sensitivity": list(self.bootstrap_sensitivity),
                "sensitivity_days": list(self.bootstrap_sensitivity),
                "alpha": str(self.alpha),
                "looks": self.alpha_looks,
                "candidates": self.alpha_candidates,
            },
            "scenarios": _scenario_contract(),
            "evidence_gates": {
                "comparison": "CANDIDATE_VS_NO_OPERATION",
                "stress_mean_r_min": str(self.stress_mean_r_min),
                "lcb_base_positive": True,
                "lcb_stress_positive": True,
                "max_drawdown_fraction": str(self.max_drawdown_fraction),
                "drop_best_count": self.drop_best_count,
                "drop_best_net_nonnegative": True,
                "costs_marks_r_complete_required": True,
                "min_holdout_episodes": self.min_holdout_episodes,
                "min_holdout_sessions": self.min_holdout_sessions,
                "min_holdout_blocks": self.min_holdout_blocks,
                "min_walkforward_episodes_each": self.min_walkforward_episodes,
            },
            "ranking": [
                "stress_drawdown",
                "concentration",
                "sensitivity",
                "margin",
                "candidate_id",
            ],
            "risk_policy": quant_audit(self.risk_policy),
        }
        if include_hash:
            result["protocol_hash"] = fingerprint(result)
        return result

    @property
    def protocol_hash(self) -> str:
        return str(self.to_dict()["protocol_hash"])


def write_default_protocol(path: str | Path) -> Path:
    """Create the canonical default protocol artifact exactly once."""

    protocol = ResearchProtocol.default()
    payload = protocol.to_dict()
    return _exclusive_json(path, payload)


def read_protocol(path: str | Path) -> ResearchProtocol:
    """Read one prior regular protocol artifact without following symlinks."""

    target = _safe_path(path)
    _check_parent(target)
    flags = os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(target, flags)
    except OSError as exc:
        raise MarketProtocolError(f"no se pudo leer protocolo: {target}") from exc
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise MarketProtocolError("el protocolo debe ser un archivo regular exclusivo")
        with os.fdopen(fd, "r", encoding="utf-8") as stream:
            raw = json.load(stream)
        fd = -1
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MarketProtocolError(f"protocolo ilegible: {target}") from exc
    finally:
        if fd >= 0:
            with suppress(OSError):
                os.close(fd)
    if not isinstance(raw, Mapping):
        raise MarketProtocolError("protocolo debe ser objeto JSON")
    expected_hash = raw.get("protocol_hash")
    material = dict(raw)
    material.pop("protocol_hash", None)
    if not isinstance(expected_hash, str) or expected_hash != fingerprint(material):
        raise MarketProtocolError("protocol_hash inválido")
    return (
        ResearchProtocol.default()
        if raw.get("protocol_hash") == ResearchProtocol.default().protocol_hash
        else _protocol_from_mapping(raw)
    )


def _protocol_from_mapping(raw: Mapping[str, Any]) -> ResearchProtocol:
    expected_hash = raw.get("protocol_hash")
    material = dict(raw)
    material.pop("protocol_hash", None)
    if not isinstance(expected_hash, str) or expected_hash != fingerprint(material):
        raise MarketProtocolError("protocol_hash inválido")
    periods = _section(raw, "periods")
    holdout = _section(raw, "holdout")
    sample = _section(raw, "sample")
    bootstrap = _section(raw, "bootstrap")
    risk_raw = _section(raw, "risk_policy")
    risk_fields = {
        "version",
        "stop_atr_multiple",
        "take_profit_atr_multiple",
        "planned_risk_fraction",
        "max_daily_loss_fraction",
        "max_drawdown_fraction",
        "max_positions",
        "max_intents",
        "martingale_allowed",
        "holding_profile",
        "intraday_max_bars",
        "intraday_max_minutes",
        "multiday_max_hours",
        "multiday_preclose_minutes",
        "exit_latency_seconds",
        "initial_equity",
        "equity_basis",
        "account_currency",
        "quote_currency",
    }
    risk_policy = CoreRiskExitPolicy.from_mapping({key: value for key, value in risk_raw.items() if key in risk_fields})
    candidates_raw = raw.get("candidates")
    if not isinstance(candidates_raw, Sequence):
        raise MarketProtocolError("candidates faltantes")
    candidates = tuple(
        CandidateSpec(
            cast(CandidateId, item.get("candidate_id")),
            str(item.get("strategy_impl", "")),
            tuple(str(value) for value in item.get("timeframes", ())),
            str(item.get("role", "")),
            str(item.get("hypothesis", "")),
            str(item.get("parameter_scope", "FROZEN")),
        )
        for item in candidates_raw
        if isinstance(item, Mapping)
    )
    protocol = ResearchProtocol(
        candidates=candidates,
        warmup_start_year=int(periods.get("warmup_start_year", 2015)),
        development_start_year=int(periods.get("development_start_year", 2016)),
        development_end_year=int(periods.get("development_end_year", 2019)),
        walkforward_years=tuple(int(value) for value in periods.get("walkforward_years", ())),
        holdout_start_year=int(periods.get("holdout_start_year", 2024)),
        holdout_end_year=int(periods.get("holdout_end_year", 2025)),
        holdout_locked=bool(holdout.get("locked", True)),
        holdout_open=bool(holdout.get("open", False)),
        confirmation_access=str(holdout.get("confirmation_access", "SINGLE")),
        min_holdout_episodes=int(sample.get("min_holdout_episodes", 120)),
        min_holdout_sessions=int(sample.get("min_holdout_sessions", 100)),
        min_holdout_blocks=int(sample.get("min_holdout_blocks", 20)),
        block_days=int(sample.get("block_days", 14)),
        min_walkforward_episodes=int(sample.get("min_walkforward_episodes", 20)),
        bootstrap_iterations=int(bootstrap.get("iterations", 100_000)),
        bootstrap_seed=int(bootstrap.get("seed", 20_260_913)),
        bootstrap_block_days=int(bootstrap.get("block_days", 14)),
        bootstrap_sensitivity=tuple(
            int(value) for value in bootstrap.get("sensitivity_days", bootstrap.get("sensitivity", (7, 28)))
        ),
        alpha=Decimal(str(bootstrap.get("alpha", "0.05"))),
        alpha_looks=int(bootstrap.get("looks", 2)),
        alpha_candidates=int(bootstrap.get("candidates", 6)),
        stress_mean_r_min=Decimal(str(_section(raw, "evidence_gates").get("stress_mean_r_min", "0.05"))),
        max_drawdown_fraction=Decimal(str(_section(raw, "evidence_gates").get("max_drawdown_fraction", "0.05"))),
        drop_best_count=int(_section(raw, "evidence_gates").get("drop_best_count", 5)),
        risk_policy=risk_policy,
    )
    if protocol.protocol_hash != expected_hash:
        raise MarketProtocolError("protocol_hash no coincide con los campos normalizados")
    return protocol


def _section(raw: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    value = raw.get(name)
    return value if isinstance(value, Mapping) else {}


__all__ = [
    "CANDIDATE_IDS",
    "CandidateId",
    "CandidateSpec",
    "DEFAULT_CANDIDATES",
    "DatasetManifest",
    "MarketProtocolError",
    "PROTOCOL_SCHEMA",
    "PROTOCOL_VERSION",
    "ResearchProtocol",
    "RiskExitPolicy",
    "quant_audit",
    "read_protocol",
    "write_default_protocol",
]
