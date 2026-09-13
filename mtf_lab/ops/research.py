"""Offline research services for the causal CFD backtest vertical.

The public functions in this module are intentionally usable by a future CLI
without importing the CLI itself.  ``run_research`` creates one private,
versioned manifest at an explicitly supplied path outside the checkout.  It
registers every variant/horizon trial before any result is calculated, then
delegates the actual capture replay to :mod:`mtf_lab.ops.cfd_backtest`.

The statistical helpers are small standard-library implementations.  They are
reporting and screening tools, not a promise of market performance.  When the
available sample or its assumptions are insufficient, they return an explicit
``NOT_ASSESSED`` state rather than a fabricated number.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import stat
import uuid
from collections import defaultdict
from collections.abc import Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from statistics import mean, median, stdev
from typing import Any, cast

from ..configuration import EffectiveConfig, load_config, packaged_config_path
from ..core.canonical import canonical_json, fingerprint, instant_text
from ..core.numeric import decimal_context
from ..data.capture import CaptureContractError, parse_instant
from .cfd_backtest import (
    SUPPORTED_VARIANTS,
    CFDBacktestError,
    ExecutionModel,
    ExecutionPlan,
    ResearchOrder,
    ResearchWindow,
    assign_trade_window,
    build_execution_plan,
    build_research_windows,
    load_cfd_capture,
    run_cfd_backtest,
    synthetic_cfd_capture,
)

RESEARCH_SCHEMA = "mtf-lab.research-manifest.v1"
RESEARCH_VERSION = 1
PRODUCT = "FOREX_CFD_LOCAL_PAPER"
DETECTOR = "TrendPullbackStrategy"
DEFAULT_CONFIG_NAME = "ctrader_pipeline_fixture.toml"
_CHECKOUT = Path(__file__).resolve().parents[2]
_VARIANT_DETECTORS = {
    "trend_pullback_v1": "TrendPullbackStrategy",
    "donchian20_m5_v1": "Donchian20M5Strategy",
    "m1_trigger_reference": "M1ReferenceProjection",
}
_VARIANT_HYPOTHESES: dict[str, dict[str, str]] = {
    "trend_pullback_v1": {
        "role": "BASELINE_FROZEN",
        "hypothesis": "La implementación baseline causal sirve como referencia congelada; no afirma rentabilidad.",
        "profitability_claim": "NONE",
    },
    "m1_trigger_reference": {
        "role": "DIAGNOSTIC_CONTROL",
        "hypothesis": "La proyección M1 se conserva como control diagnóstico de señales; no afirma rentabilidad.",
        "profitability_claim": "NONE",
    },
    "donchian20_m5_v1": {
        "role": "DONCHIAN20_M5_IMPLEMENTED_CHALLENGER",
        "hypothesis": "Donchian20 M5 se compara como challenger implementado; no afirma rentabilidad.",
        "profitability_claim": "NONE",
    },
}
_DECISION_POLICY = {
    "fixture": "EVIDENCE_INSUFFICIENT",
    "insufficient_inputs_or_costs_or_holdout": "NOT_ASSESSED",
    "evaluable_sample": "REQUIRES_HUMAN_REVIEW",
    "auto_accept": "PROHIBITED",
    "auto_promote": "PROHIBITED",
    "basis": "evidence_gates_only_not_pnl_or_sharpe",
}


class ResearchError(ValueError):
    """A research service input or manifest does not satisfy its contract."""


@dataclass(frozen=True, slots=True)
class ResearchVariant:
    """A named trial over an implemented StrategyProtocol strategy.

    Each supported variant routes to its real owner implementation.  The
    research layer does not silently relabel the baseline as a challenger.
    """

    name: str
    description: str = ""

    def __post_init__(self) -> None:
        name = str(self.name).strip()
        if not name:
            raise ResearchError("variant name no puede estar vacío")
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "description", str(self.description).strip())

    def to_dict(self) -> dict[str, str]:
        return {"name": self.name, "description": self.description}


def _variant_hypothesis(variant: ResearchVariant) -> dict[str, str]:
    try:
        hypothesis = dict(_VARIANT_HYPOTHESES[variant.name])
    except KeyError as exc:
        raise ResearchError(f"variant sin hipótesis preregistrada: {variant.name}") from exc
    hypothesis["description"] = variant.description
    return hypothesis


def _pending_decision() -> dict[str, Any]:
    return {
        "status": "PENDING_RESULTS",
        "decision": "NOT_DECIDED",
        "promote": False,
        "basis": _DECISION_POLICY["basis"],
    }


def _holdout_statistics_insufficient(value: Any) -> bool:
    if not isinstance(value, Mapping):
        return False
    return any(
        isinstance(value.get(key), Mapping) and value[key].get("status") == "NOT_ASSESSED"
        for key in ("psr", "dsr", "bootstrap_daily")
    )


def _result_decision(result: Mapping[str, Any], *, fixture: bool) -> dict[str, Any]:
    if fixture:
        return {
            "status": _DECISION_POLICY["fixture"],
            "decision": "NO_AUTO_ACCEPT_OR_PROMOTION",
            "promote": False,
            "reason": "synthetic_fixture_not_external_evidence",
            "basis": _DECISION_POLICY["basis"],
        }
    reasons: list[str] = []
    if result.get("capture_complete") is not True:
        reasons.append("capture_incomplete")
    costs = result.get("costs")
    if not isinstance(costs, Mapping) or costs.get("state") != "KNOWN" or int(costs.get("unknown_count", 0) or 0) > 0:
        reasons.append("costs_insufficient_or_unknown")
    validation = result.get("validation")
    holdout = (
        next(
            (
                item
                for item in validation.get("windows", ())
                if isinstance(item, Mapping) and item.get("kind") == "holdout"
            ),
            None,
        )
        if isinstance(validation, Mapping)
        else None
    )
    if (
        not isinstance(holdout, Mapping)
        or holdout.get("oos_status") != "ASSESSED"
        or int(holdout.get("oos_trade_count", 0) or 0) <= 0
    ):
        reasons.append("holdout_insufficient")
    quality = result.get("quality")
    if isinstance(quality, Mapping):
        if quality.get("availability_known") is False:
            reasons.append("input_availability_unknown")
        if quality.get("continuity") not in {None, "CONTINUOUS"}:
            reasons.append("input_continuity_not_continuous")
    if _holdout_statistics_insufficient(result.get("statistics")):
        reasons.append("holdout_statistics_insufficient")
    if reasons:
        status = _DECISION_POLICY["insufficient_inputs_or_costs_or_holdout"]
        reason = ";".join(sorted(set(reasons)))
    else:
        status = _DECISION_POLICY["evaluable_sample"]
        reason = "evaluable_sample_requires_human_review"
    return {
        "status": status,
        "decision": "NO_AUTO_ACCEPT_OR_PROMOTION" if status != "REQUIRES_HUMAN_REVIEW" else "HUMAN_REVIEW_REQUIRED",
        "promote": False,
        "reason": reason,
        "basis": _DECISION_POLICY["basis"],
    }


def _commit_research_decisions(
    manifest: dict[str, Any],
    trials: Sequence[Mapping[str, Any]],
    results: Sequence[dict[str, Any]],
    *,
    fixture: bool,
) -> tuple[dict[str, dict[str, Any]], dict[str, int]]:
    decisions: dict[str, dict[str, Any]] = {}
    counts: dict[str, int] = {}
    for trial, result in zip(trials, results, strict=True):
        result["hypothesis"] = trial.get("hypothesis")
        decision = _result_decision(result, fixture=fixture)
        result["decision"] = decision
        trial_id = str(trial["trial_id"])
        decisions[trial_id] = decision
        counts[decision["status"]] = counts.get(decision["status"], 0) + 1
    manifest["decisions"] = decisions
    for trial in manifest.get("trials", ()):
        if isinstance(trial, dict) and str(trial.get("trial_id")) in decisions:
            trial["decision"] = decisions[str(trial["trial_id"])]
    return decisions, counts


@dataclass(frozen=True, slots=True)
class _ResearchContext:
    """Validated inputs and registered work for one research manifest."""

    target: Path
    config: EffectiveConfig
    capture: Any
    fixture: bool
    capture_path: str | Path | None
    order: ResearchOrder
    execution_plan: ExecutionPlan
    trials: list[dict[str, Any]]
    windows: tuple[ResearchWindow, ...]
    manifest: dict[str, Any]


@dataclass(slots=True)
class _ManifestOwner:
    """Process-local identity for the manifest write lock of one run."""

    run_id: str
    token: str
    lock_name: str
    target_stat: tuple[int, int] | None


_MANIFEST_OWNERS: dict[Path, _ManifestOwner] = {}


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return instant_text(value)
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return _jsonable(value.to_dict())
    return value


def _digest(value: Any) -> str:
    return fingerprint(_jsonable(value))


def _decimal(value: Any, *, name: str, positive: bool = False) -> Decimal:
    if isinstance(value, bool):
        raise ResearchError(f"{name} no admite booleanos")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ResearchError(f"{name} no es decimal válido: {value!r}") from exc
    if not result.is_finite() or (positive and result <= 0):
        raise ResearchError(f"{name} no es finito o no es positivo")
    return result


def _aware_timestamp(value: Any, *, name: str) -> datetime:
    try:
        parsed = parse_instant(value)
    except (CaptureContractError, TypeError, ValueError) as exc:
        raise ResearchError(f"{name} debe ser ISO-8601 con zona horaria") from exc
    if parsed is None:
        raise ResearchError(f"{name} es obligatorio")
    return parsed.astimezone(UTC)


def _safe_manifest_path(path: str | Path | None) -> Path:
    if path is None:
        raise ResearchError("manifest_path es obligatorio y debe ser explícito")
    target = Path(path).expanduser()
    if not target.is_absolute():
        raise ResearchError("manifest_path debe ser una ruta absoluta")
    target = Path(os.path.abspath(os.fspath(target)))
    if not target.name:
        raise ResearchError("manifest_path debe apuntar a un archivo")
    try:
        target.relative_to(_CHECKOUT)
    except ValueError:
        pass
    else:
        raise ResearchError("manifest privado debe estar fuera del checkout")
    _check_manifest_parent_components(target)
    try:
        info = os.lstat(target)
    except FileNotFoundError:
        return target
    except OSError as exc:
        raise ResearchError(f"manifest_path no se pudo inspeccionar: {target}: {exc}") from exc
    _validate_manifest_regular(info, target)
    return target


def _check_manifest_parent_components(target: Path) -> None:
    current = Path(target.anchor)
    for component in target.parent.parts:
        if component == target.anchor:
            continue
        current /= component
        try:
            info = os.lstat(current)
        except FileNotFoundError:
            break
        except OSError as exc:
            raise ResearchError(f"directorio padre de manifest ilegible: {current}: {exc}") from exc
        if stat.S_ISLNK(info.st_mode):
            raise ResearchError(f"directorio padre de manifest no puede ser symlink: {current}")
        if not stat.S_ISDIR(info.st_mode):
            raise ResearchError(f"directorio padre de manifest no es directorio: {current}")


def _validate_manifest_regular(info: os.stat_result, target: Path, *, allow_linked: bool = False) -> None:
    if stat.S_ISLNK(info.st_mode):
        raise ResearchError(f"manifest_path no puede ser un symlink: {target}")
    if not stat.S_ISREG(info.st_mode):
        raise ResearchError(f"manifest_path debe ser un archivo regular: {target}")
    if not allow_linked and info.st_nlink != 1:
        raise ResearchError(f"manifest_path no puede ser un hardlink compartido: {target}")


def _open_manifest_parent(target: Path, *, create: bool) -> int:
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
                    raise ResearchError(f"directorio padre de manifest no existe: {target.parent}") from exc
                with suppress(FileExistsError):
                    os.mkdir(component, 0o700, dir_fd=fd)
                child = os.open(component, flags, dir_fd=fd)
            except OSError as exc:
                raise ResearchError(f"directorio padre de manifest inseguro: {target.parent}: {exc}") from exc
            os.close(fd)
            fd = child
    except BaseException:
        with suppress(OSError):
            os.close(fd)
        raise
    return fd


def _target_stat(parent_fd: int, target: Path, *, allow_missing: bool) -> os.stat_result | None:
    try:
        info = os.lstat(target.name, dir_fd=parent_fd)
    except FileNotFoundError:
        if allow_missing:
            return None
        raise ResearchError(f"manifest no encontrado: {target}") from None
    except OSError as exc:
        raise ResearchError(f"manifest no se pudo inspeccionar: {target}: {exc}") from exc
    _validate_manifest_regular(info, target)
    return info


def _manifest_lock_name(target: Path) -> str:
    return f".{target.name}.lock"


def _manifest_run_id(payload: Mapping[str, Any]) -> str:
    value = payload.get("run_id")
    if not isinstance(value, str) or not value:
        raise ResearchError("manifest requiere run_id para identidad de escritura")
    return value


def _write_fd(fd: int, data: bytes) -> None:
    offset = 0
    while offset < len(data):
        written = os.write(fd, data[offset:])
        if written <= 0:
            raise OSError("escritura de manifest no progresó")
        offset += written


def _temp_manifest(parent_fd: int, target: Path, data: bytes, token: str) -> str:
    temporary_name = f".{target.name}.{os.getpid()}.{token}.tmp"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    fd = os.open(temporary_name, flags, 0o600, dir_fd=parent_fd)
    try:
        _write_fd(fd, data)
        os.fchmod(fd, 0o600)
        os.fsync(fd)
    except BaseException:
        with suppress(OSError):
            os.close(fd)
        with suppress(OSError):
            os.unlink(temporary_name, dir_fd=parent_fd)
        raise
    os.close(fd)
    return temporary_name


def _fsync_manifest(parent_fd: int, target: Path, *, allow_linked: bool = False) -> None:
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | getattr(os, "O_CLOEXEC", 0)
    fd = os.open(target.name, flags, dir_fd=parent_fd)
    try:
        info = os.fstat(fd)
        _validate_manifest_regular(info, target, allow_linked=allow_linked)
        os.fsync(fd)
    finally:
        os.close(fd)


def _lock_record(owner: _ManifestOwner) -> bytes:
    return (canonical_json({"run_id": owner.run_id, "token": owner.token, "pid": os.getpid()}) + "\n").encode("utf-8")


def _read_lock(parent_fd: int, target: Path) -> dict[str, Any]:
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | getattr(os, "O_CLOEXEC", 0)
    try:
        fd = os.open(_manifest_lock_name(target), flags, dir_fd=parent_fd)
    except OSError as exc:
        raise ResearchError(f"lock de manifest ilegible: {target}: {exc}") from exc
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise ResearchError(f"lock de manifest no es regular/privado: {target}")
        raw = b""
        while len(raw) <= 8192:
            chunk = os.read(fd, 8193 - len(raw))
            if not chunk:
                break
            raw += chunk
        if len(raw) > 8192:
            raise ResearchError(f"lock de manifest excede tamaño permitido: {target}")
    finally:
        os.close(fd)
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ResearchError(f"lock de manifest inválido: {target}") from exc
    if not isinstance(value, Mapping):
        raise ResearchError(f"lock de manifest inválido: {target}")
    return dict(value)


def _drop_manifest_owner(target: Path, parent_fd: int, owner: _ManifestOwner) -> None:
    current = _MANIFEST_OWNERS.get(target)
    if current is not owner:
        return
    try:
        lock = _read_lock(parent_fd, target)
    except ResearchError:
        return
    if lock.get("run_id") != owner.run_id or lock.get("token") != owner.token:
        return
    try:
        os.unlink(owner.lock_name, dir_fd=parent_fd)
        os.fsync(parent_fd)
    except OSError:
        return
    _MANIFEST_OWNERS.pop(target, None)


def _verify_manifest_owner(
    target: Path,
    parent_fd: int,
    payload: Mapping[str, Any],
    owner: _ManifestOwner,
    attempt_token: str | None,
) -> None:
    run_id = _manifest_run_id(payload)
    if owner.run_id != run_id or owner.target_stat is None or attempt_token != owner.token:
        raise ResearchError("manifest ya está reservado por otro intento")
    current = _target_stat(parent_fd, target, allow_missing=False)
    assert current is not None
    if (current.st_dev, current.st_ino) != owner.target_stat:
        raise ResearchError("identidad del manifest cambió; no se sustituye otro intento")
    lock = _read_lock(parent_fd, target)
    if lock.get("run_id") != owner.run_id or lock.get("token") != owner.token:
        raise ResearchError("lock de manifest no pertenece a este intento")
    _, current_payload = _read_manifest(target)
    if current_payload.get("run_id") != owner.run_id:
        raise ResearchError("manifest pertenece a otro intento")
    if current_payload.get("state") in {"COMPLETED", "FAILED"}:
        raise ResearchError("manifest ya está cerrado; no se reescribe")
    expected_hash = current_payload.get("integrity_hash")
    if not isinstance(expected_hash, str) or expected_hash != _manifest_integrity(current_payload):
        raise ResearchError("manifest en curso no conserva su identidad íntegra")


def _ensure_manifest_owner(
    target: Path,
    parent_fd: int,
    payload: Mapping[str, Any],
    *,
    attempt_token: str | None,
) -> tuple[_ManifestOwner, bool]:
    run_id = _manifest_run_id(payload)
    existing = _MANIFEST_OWNERS.get(target)
    if existing is not None:
        _verify_manifest_owner(target, parent_fd, payload, existing, attempt_token)
        return existing, False
    token = attempt_token or uuid.uuid4().hex
    owner = _ManifestOwner(run_id, token, _manifest_lock_name(target), None)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    try:
        fd = os.open(owner.lock_name, flags, 0o600, dir_fd=parent_fd)
    except FileExistsError as exc:
        raise ResearchError(f"manifest ya está reservado o tiene lock pendiente: {target}") from exc
    try:
        _write_fd(fd, _lock_record(owner))
        os.fchmod(fd, 0o600)
        os.fsync(fd)
    except BaseException:
        with suppress(OSError):
            os.close(fd)
        with suppress(OSError):
            os.unlink(owner.lock_name, dir_fd=parent_fd)
        raise
    os.close(fd)
    os.fsync(parent_fd)
    _MANIFEST_OWNERS[target] = owner
    try:
        if _target_stat(parent_fd, target, allow_missing=True) is not None:
            raise ResearchError(f"manifest ya existe; no se sobrescribe: {target}")
    except BaseException:
        _drop_manifest_owner(target, parent_fd, owner)
        raise
    return owner, True


def _publish_manifest_initial(parent_fd: int, target: Path, data: bytes, owner: _ManifestOwner) -> os.stat_result:
    temporary_name = _temp_manifest(parent_fd, target, data, owner.token)
    try:
        os.link(temporary_name, target.name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd, follow_symlinks=False)
        _fsync_manifest(parent_fd, target, allow_linked=True)
        os.fsync(parent_fd)
        os.unlink(temporary_name, dir_fd=parent_fd)
        os.fsync(parent_fd)
        info = _target_stat(parent_fd, target, allow_missing=False)
        assert info is not None
        return info
    except BaseException:
        with suppress(OSError):
            os.unlink(temporary_name, dir_fd=parent_fd)
        raise


def _publish_manifest_update(parent_fd: int, target: Path, data: bytes, owner: _ManifestOwner) -> os.stat_result:
    temporary_name = _temp_manifest(parent_fd, target, data, owner.token)
    try:
        os.replace(temporary_name, target.name, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
        _fsync_manifest(parent_fd, target)
        os.fsync(parent_fd)
        info = _target_stat(parent_fd, target, allow_missing=False)
        assert info is not None
        return info
    except BaseException:
        with suppress(OSError):
            os.unlink(temporary_name, dir_fd=parent_fd)
        raise


def _manifest_terminal(payload: Mapping[str, Any]) -> bool:
    return payload.get("state") in {"COMPLETED", "FAILED"}


def _write_manifest(path: Path, payload: Mapping[str, Any], *, attempt_token: str | None = None) -> None:
    target = _safe_manifest_path(path)
    data = (canonical_json(_jsonable(payload)) + "\n").encode("utf-8")
    parent_fd = _open_manifest_parent(target, create=True)
    owner: _ManifestOwner | None = None
    initial = False
    try:
        owner, initial = _ensure_manifest_owner(target, parent_fd, payload, attempt_token=attempt_token)
        info = (
            _publish_manifest_initial(parent_fd, target, data, owner)
            if initial
            else _publish_manifest_update(parent_fd, target, data, owner)
        )
        owner.target_stat = (info.st_dev, info.st_ino)
        if _manifest_terminal(payload):
            _drop_manifest_owner(target, parent_fd, owner)
    except ResearchError:
        if initial and owner is not None and owner.target_stat is None:
            with suppress(ResearchError):
                if _target_stat(parent_fd, target, allow_missing=True) is None:
                    _drop_manifest_owner(target, parent_fd, owner)
        raise
    except OSError as exc:
        if initial and owner is not None and owner.target_stat is None:
            with suppress(ResearchError):
                if _target_stat(parent_fd, target, allow_missing=True) is None:
                    _drop_manifest_owner(target, parent_fd, owner)
        raise ResearchError(f"no se pudo escribir manifest privado: {target}: {exc}") from exc
    finally:
        with suppress(OSError):
            os.close(parent_fd)


def _read_manifest(path: str | Path) -> tuple[Path, dict[str, Any]]:
    target = _safe_manifest_path(path)
    parent_fd = _open_manifest_parent(target, create=False)
    try:
        flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | getattr(os, "O_CLOEXEC", 0)
        fd = os.open(target.name, flags, dir_fd=parent_fd)
        try:
            info = os.fstat(fd)
            _validate_manifest_regular(info, target)
            with os.fdopen(fd, "r", encoding="utf-8") as stream:
                value = json.loads(stream.read())
            fd = -1
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ResearchError(f"manifest inválido o ilegible: {target}: {exc}") from exc
        finally:
            if fd >= 0:
                with suppress(OSError):
                    os.close(fd)
    finally:
        with suppress(OSError):
            os.close(parent_fd)
    if not isinstance(value, Mapping):
        raise ResearchError("manifest debe ser un objeto JSON")
    return target, dict(value)


def _load_research_config(path: str | Path | None) -> EffectiveConfig:
    target = path if path is not None else default_research_config_path()
    try:
        config = load_config(target)
    except (TypeError, ValueError, OSError) as exc:
        raise ResearchError(f"configuración de research inválida: {exc}") from exc
    base = str(config.price_base).strip().lower()
    if base == "trade":
        base = "traded"
    if base == "traded":
        base = "native"
    if base not in {"mid", "bid", "ask", "native"}:
        raise ResearchError(f"base de análisis no soportada para CFD: {config.price_base!r}")
    # Configuración de replay es una identidad nueva; no se modifica el TOML.
    data = dict(config.data)
    data.update({"mode": "REPLAY", "instrument": config.instrument, "price_base": base})
    return replace(config, mode="REPLAY", price_base=base, data=data)


def _real_capture_config(config: EffectiveConfig, *, config_path: str | Path | None) -> EffectiveConfig:
    """Require provenance-critical real-capture inputs without defaulting costs."""

    if config_path is None:
        raise ResearchError("las capturas no sintéticas requieren config_path explícito")
    required_spec = ("symbol_id", "digits", "pip_position", "price_scale")
    missing_spec = sorted(key for key in required_spec if key not in config.ctrader)
    if missing_spec:
        raise ResearchError(f"instrument_spec no observado/expreso en configuración: {missing_spec}")
    cfd = dict(config.cfd)
    # Missing catalogue facts are represented as unknown by the simulator;
    # they are never converted to zero-cost assumptions.
    cfd.setdefault("commission_known", False)
    cfd.setdefault("financing_required", True)
    if cfd.get("commission_known") is True and not any(
        key in cfd for key in ("commission_fixed", "commission_per_unit")
    ):
        cfd["commission_known"] = False
    return replace(config, cfd=cfd)


def _cost_contract(config: EffectiveConfig, *, fixture: bool) -> dict[str, Any]:
    cfd = config.cfd
    commission_fields = tuple(
        key for key in ("commission", "commission_fixed", "commission_per_unit", "commission_currency") if key in cfd
    )
    financing_fields = tuple(
        key for key in ("financing", "financing_required", "financing_rate_per_second") if key in cfd
    )
    commission_declared = cfd.get("commission_known") is True and bool(commission_fields)
    financing_declared = cfd.get("financing_required") is False or (
        cfd.get("financing_required") is True and "financing_rate_per_second" in cfd
    )
    return {
        "state": "KNOWN" if fixture or (commission_declared and financing_declared) else "UNKNOWN",
        "source": "synthetic_fixture" if fixture else "explicit_config_or_catalog",
        "commission_fields": list(commission_fields),
        "financing_fields": list(financing_fields),
        "missing_is_zero": False,
    }


def _normalise_variants(
    variants: Sequence[str | Mapping[str, Any] | ResearchVariant] | str | None,
) -> tuple[ResearchVariant, ...]:
    raw: Sequence[str | Mapping[str, Any] | ResearchVariant]
    if isinstance(variants, str):
        raw = tuple(item.strip() for item in variants.split(",") if item.strip())
    else:
        raw = (
            variants
            if variants is not None
            else (
                "trend_pullback_v1",
                "m1_trigger_reference",
                "donchian20_m5_v1",
            )
        )
    result: list[ResearchVariant] = []
    seen: set[str] = set()
    for item in raw:
        if isinstance(item, ResearchVariant):
            variant = item
        elif isinstance(item, Mapping):
            unknown = set(item) - {"name", "description"}
            if unknown:
                raise ResearchError(f"variant sólo admite name/description: {sorted(unknown)}")
            variant = ResearchVariant(str(item.get("name", "")), str(item.get("description", "")))
        else:
            variant = ResearchVariant(str(item))
        if variant.name in seen:
            raise ResearchError(f"variant duplicada: {variant.name}")
        seen.add(variant.name)
        result.append(variant)
    if not result:
        raise ResearchError("se requiere al menos una variant")
    return tuple(result)


def _validate_variant_implementations(variants: Sequence[ResearchVariant]) -> None:
    unsupported = sorted(item.name for item in variants if item.name not in SUPPORTED_VARIANTS)
    if unsupported:
        raise ResearchError(f"variant no implementada: {unsupported}; disponibles={sorted(SUPPORTED_VARIANTS)}")


def _supported_detector(value: Any) -> bool:
    return str(value) in {
        "TrendPullbackStrategy",
        "Donchian20M5Strategy",
        "M1ReferenceProjection",
        "multiple_strategy_variants",
    }


def _normalise_horizons(config: EffectiveConfig, values: Sequence[Any] | None) -> tuple[Decimal, ...]:
    raw = values if values is not None else config.cfd.get("horizons_seconds", (60, 180, 300))
    if isinstance(raw, str):
        raw = tuple(item.strip() for item in raw.split(",") if item.strip())
    if isinstance(raw, bytes) or not isinstance(raw, Sequence):
        raise ResearchError("horizons_seconds debe ser una secuencia")
    result = tuple(_decimal(item, name="horizon_seconds", positive=True) for item in raw)
    if not result:
        raise ResearchError("se requiere al menos un horizonte")
    if len(set(result)) != len(result):
        raise ResearchError("horizons_seconds no puede contener duplicados")
    return result


def default_research_config_path() -> Path:
    """Return the packaged fixture profile used only by explicit fixture runs."""

    return packaged_config_path(DEFAULT_CONFIG_NAME)


def _normalise_order(value: Any) -> ResearchOrder:
    text = str(value).strip().lower()
    if text not in {"as_observed", "market_time_corrected"}:
        raise ResearchError(f"order de captura no soportado: {value!r}")
    return cast(ResearchOrder, text)


def _code_hash() -> str:
    hasher = hashlib.sha256()
    roots = (_CHECKOUT / "mtf_lab",)
    paths = sorted(path for root in roots for path in root.rglob("*.py"))
    for path in paths:
        try:
            content = path.read_bytes()
        except OSError as exc:
            raise ResearchError(f"no se pudo leer una fuente para code_hash: {path}") from exc
        # Content only: paths and any textual metadata are deliberately not
        # included in the manifest hash.
        hasher.update(content)
        hasher.update(b"\n")
    return hasher.hexdigest()


def _trial_id(
    *,
    run_id: str,
    variant: ResearchVariant,
    horizon: Decimal,
    config: EffectiveConfig,
    seed: int,
    ordinal: int,
) -> str:
    return (
        "trial_"
        + _digest(
            {
                "run_id": run_id,
                "variant": variant.to_dict(),
                "horizon_seconds": str(horizon),
                "config_hash": config.config_hash,
                "seed": seed,
                "ordinal": ordinal,
            }
        )[:32]
    )


def _registered_trials(
    *,
    run_id: str,
    config: EffectiveConfig,
    variants: Sequence[ResearchVariant],
    horizons: Sequence[Decimal],
    seed: int,
    execution_plan: ExecutionPlan,
) -> list[dict[str, Any]]:
    trials: list[dict[str, Any]] = []
    ordinal = 0
    for variant in variants:
        for horizon in horizons:
            trials.append(
                {
                    "trial_id": _trial_id(
                        run_id=run_id,
                        variant=variant,
                        horizon=horizon,
                        config=config,
                        seed=seed,
                        ordinal=ordinal,
                    ),
                    "registration_order": ordinal,
                    "variant": variant.name,
                    "variant_description": variant.description,
                    "horizon_seconds": str(horizon),
                    "config_hash": config.config_hash,
                    "seed": seed,
                    "execution_model": execution_plan.scenario,
                    "execution_parameters": execution_plan.to_dict(),
                    "hypothesis": _variant_hypothesis(variant),
                    "decision": _pending_decision(),
                    "status": "REGISTERED",
                }
            )
            ordinal += 1
    return trials


def _not_assessed(reason: str, **extra: Any) -> dict[str, Any]:
    return {"status": "NOT_ASSESSED", "value": None, "reason": reason, **extra}


def _normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def _normal_inv_cdf(probability: float) -> float:
    if not 0.0 < probability < 1.0:
        raise ResearchError("normal inverse probability must be between zero and one")
    low, high = -10.0, 10.0
    for _ in range(80):
        middle = (low + high) / 2.0
        if _normal_cdf(middle) < probability:
            low = middle
        else:
            high = middle
    return (low + high) / 2.0


def _moments(values: Sequence[float]) -> tuple[float, float, float, float] | None:
    if len(values) < 2:
        return None
    centre = mean(values)
    second = mean((value - centre) ** 2 for value in values)
    if not math.isfinite(second) or second <= 0.0:
        return None
    deviation = math.sqrt(second)
    third = mean((value - centre) ** 3 for value in values) / deviation**3
    fourth = mean((value - centre) ** 4 for value in values) / deviation**4
    if not all(math.isfinite(item) for item in (centre, deviation, third, fourth)):
        return None
    return centre, deviation, third, fourth


def _sharpe(values: Sequence[float]) -> float | None:
    moments = _moments(values)
    return moments[0] / moments[1] if moments else None


def probabilistic_sharpe_ratio(values: Sequence[float], *, benchmark: float = 0.0) -> dict[str, Any]:
    """Return PSR with an explicit insufficient/degenerate state."""

    if len(values) < 3:
        return _not_assessed("insufficient_observations", observations=len(values), benchmark=benchmark)
    moments = _moments(values)
    if moments is None:
        return _not_assessed("constant_or_non_finite_returns", observations=len(values), benchmark=benchmark)
    _, deviation, skew, raw_kurtosis = moments
    observed = mean(values) / deviation
    denominator_squared = 1.0 - skew * observed + ((raw_kurtosis - 1.0) / 4.0) * observed**2
    if denominator_squared <= 0.0 or not math.isfinite(denominator_squared):
        return _not_assessed("invalid_psr_denominator", observations=len(values), benchmark=benchmark)
    statistic = (observed - benchmark) * math.sqrt(len(values) - 1) / math.sqrt(denominator_squared)
    return {
        "status": "ASSESSED",
        "value": _normal_cdf(statistic),
        "benchmark": benchmark,
        "observed_sharpe": observed,
        "observations": len(values),
        "skewness": skew,
        "kurtosis": raw_kurtosis,
        "assumption_scope": "IID_or_stationary_ergodic_approximation",
    }


def _expected_max_standard_normal(count: int) -> float:
    if count <= 1:
        return 0.0
    gamma = 0.5772156649015329
    first = _normal_inv_cdf(1.0 - 1.0 / count)
    second = _normal_inv_cdf(1.0 - 1.0 / (count * math.e))
    return (1.0 - gamma) * first + gamma * second


def deflated_sharpe_ratio(
    selected_values: Sequence[float],
    trial_values: Mapping[str, Sequence[float]],
    *,
    benchmark: float = 0.0,
) -> dict[str, Any]:
    """Deflate PSR using the conservative raw count of registered trials."""

    if len(trial_values) < 2:
        return _not_assessed(
            "fewer_than_two_registered_trials",
            effective_trials=len(trial_values),
            registry_scope="current_manifest",
            historical_attempts_included=False,
        )
    sharpes = [
        value
        for value in (_sharpe(series) for series in trial_values.values())
        if value is not None and math.isfinite(value)
    ]
    selected_sharpe = _sharpe(selected_values)
    if selected_sharpe is None or len(sharpes) < 2:
        return _not_assessed(
            "insufficient_non_degenerate_trial_returns",
            effective_trials=len(trial_values),
            registry_scope="current_manifest",
            historical_attempts_included=False,
        )
    dispersion = stdev(sharpes) if len(sharpes) > 1 else 0.0
    threshold = mean(sharpes) + dispersion * _expected_max_standard_normal(len(trial_values))
    psr = probabilistic_sharpe_ratio(selected_values, benchmark=threshold)
    if psr["status"] != "ASSESSED":
        return _not_assessed(
            "selected_series_not_evaluable",
            effective_trials=len(trial_values),
            expected_max_threshold=threshold,
            registry_scope="current_manifest",
            historical_attempts_included=False,
        )
    return {
        "status": "ASSESSED",
        "value": psr["value"],
        "benchmark": benchmark,
        "expected_max_threshold": threshold,
        "effective_trials": len(trial_values),
        "effective_trials_method": "conservative_registered_count",
        "registry_scope": "current_manifest",
        "historical_attempts_included": False,
        "trial_sharpe_dispersion": dispersion,
        "psr_at_deflated_threshold": psr,
    }


def _daily_pnl(ledger: Sequence[Mapping[str, Any]], *, observed_days: Sequence[str] = ()) -> tuple[list[float], int]:
    grouped: defaultdict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for day in observed_days:
        if isinstance(day, str) and day:
            grouped[day] += Decimal("0")
    unknown = 0
    for row in ledger:
        raw_value = row.get("net_pnl")
        close = row.get("close_available_at")
        if raw_value is None or not isinstance(close, str):
            unknown += 1
            continue
        try:
            value = Decimal(str(raw_value))
            parsed = datetime.fromisoformat(close.replace("Z", "+00:00"))
        except (InvalidOperation, TypeError, ValueError):
            unknown += 1
            continue
        grouped[parsed.astimezone(UTC).date().isoformat()] += value
    return [float(grouped[key]) for key in sorted(grouped)], unknown


def _drawdown(values: Sequence[float]) -> float:
    balance = 0.0
    peak = 0.0
    maximum = 0.0
    for value in values:
        balance += value
        peak = max(peak, balance)
        maximum = max(maximum, peak - balance)
    return maximum


def _quantile(values: Sequence[float], probability: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = probability * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _resample_blocks(values: Sequence[float], *, block_days: int, rng: Any) -> list[float]:
    if not values:
        return []
    block = max(1, min(int(block_days), len(values)))
    sampled: list[float] = []
    while len(sampled) < len(values):
        start = rng.randrange(0, len(values) - block + 1)
        sampled.extend(values[start : start + block])
    return sampled[: len(values)]


def bootstrap_daily(
    values: Sequence[float],
    *,
    iterations: int = 250,
    block_days: int = 5,
    seed: int = 42,
) -> dict[str, Any]:
    """Bootstrap daily return blocks with explicit insufficient-data states."""

    if len(values) < 4:
        return _not_assessed("fewer_than_four_daily_observations", observations=len(values), unit="daily")
    if isinstance(iterations, bool) or iterations <= 0 or isinstance(block_days, bool) or block_days <= 0:
        raise ResearchError("bootstrap iterations/block_days deben ser positivos")
    import random

    rng = random.Random(seed)
    means: list[float] = []
    drawdowns: list[float] = []
    for _ in range(iterations):
        sample = _resample_blocks(values, block_days=block_days, rng=rng)
        means.append(mean(sample))
        drawdowns.append(_drawdown(sample))
    return {
        "status": "ASSESSED",
        "unit": "daily",
        "iterations": iterations,
        "block_days": min(block_days, len(values)),
        "seed": seed,
        "mean_pnl": {"p05": _quantile(means, 0.05), "p50": _quantile(means, 0.50), "p95": _quantile(means, 0.95)},
        "max_drawdown": {
            "basis": "resampled_daily_settled_pnl",
            "p05": _quantile(drawdowns, 0.05),
            "p50": _quantile(drawdowns, 0.50),
            "p95": _quantile(drawdowns, 0.95),
        },
    }


def _raw_statistics(result: Mapping[str, Any]) -> tuple[dict[str, Any], list[float], int]:
    from .research_statistics import result_metrics

    metrics = result_metrics(result)
    ledger_value = result.get("ledger", ())
    ledger = ledger_value if isinstance(ledger_value, Sequence) else ()
    observed_days_value = result.get("observed_calendar_days", ())
    observed_days = observed_days_value if isinstance(observed_days_value, Sequence) else ()
    values, unknown = _daily_pnl(
        [row for row in ledger if isinstance(row, Mapping)],
        observed_days=observed_days,
    )
    equity_value = result.get("equity_summary")
    equity: Mapping[str, Any] = equity_value if isinstance(equity_value, Mapping) else {}
    return (
        {
            "status": "ASSESSED" if values and not unknown else "NOT_ASSESSED",
            "daily_observations": len(values),
            "unknown_trade_count": unknown,
            "net_pnl": equity.get("net_pnl"),
            "known_net_pnl_subtotal": equity.get("known_net_pnl_subtotal"),
            "economic_state": equity.get("economic_state"),
            "incomplete_economics": unknown > 0,
            "mean_daily_pnl": mean(values) if values else None,
            "median_daily_pnl": median(values) if values else None,
            "max_drawdown": metrics["max_drawdown_abs"],
            "max_drawdown_basis": metrics["max_drawdown_basis"],
            "risk_performance": metrics,
        },
        values,
        unknown,
    )


def _trade_interval(row: Mapping[str, Any]) -> tuple[datetime, datetime] | None:
    entry = row.get("entry_available_at")
    close = row.get("close_available_at") or row.get("close_target_at")
    if not isinstance(entry, str) or not isinstance(close, str):
        return None
    try:
        return _aware_timestamp(entry, name="entry_available_at"), _aware_timestamp(close, name="close_available_at")
    except ResearchError:
        return None


def _window_row_statistics(
    ledger: Sequence[Mapping[str, Any]], window: ResearchWindow
) -> tuple[list[float], int, int, int]:
    oos: list[float] = []
    purged = 0
    embargoed = 0
    unknown = 0
    embargo_end = window.test_end + timedelta(seconds=float(window.embargo_seconds))
    for row in ledger:
        interval = _trade_interval(row)
        status, _reason = assign_trade_window(row, window)
        if interval is None:
            unknown += 1
            continue
        entry, close = interval
        if window.test_end < entry <= embargo_end:
            embargoed += 1
            continue
        if status == "OOS" and row.get("net_pnl") is not None:
            try:
                oos.append(float(row["net_pnl"]))
            except (TypeError, ValueError):
                unknown += 1
        elif status == "PURGED" and entry <= window.test_end and close >= window.test_start:
            # The interval test, rather than a theoretical horizon, is the
            # source of truth for this count.
            purged += 1
    return oos, purged, embargoed, unknown


def _window_statistics(result: Mapping[str, Any], windows: Sequence[ResearchWindow]) -> dict[str, Any]:
    ledger_value = result.get("ledger", ())
    ledger = [row for row in ledger_value if isinstance(row, Mapping)] if isinstance(ledger_value, Sequence) else []
    reports: list[dict[str, Any]] = []
    for window in windows:
        oos, purged, embargoed, unknown = _window_row_statistics(ledger, window)
        reports.append(
            {
                **window.to_dict(),
                "oos_trade_count": len(oos),
                "purged_trade_count": purged,
                "embargoed_trade_count": embargoed,
                "unknown_trade_count": unknown,
                "purge": {
                    "applied": True,
                    "basis": "actual_trade_holding_interval",
                    "count": purged,
                },
                "embargo": {
                    "applied": window.embargo_seconds > 0,
                    "seconds": str(window.embargo_seconds),
                    "count": embargoed,
                },
                "oos_net_pnl": sum(oos) if oos else None,
                "oos_mean_trade_pnl": mean(oos) if oos else None,
                "oos_median_trade_pnl": median(oos) if oos else None,
                "oos_status": "ASSESSED" if oos else "NOT_ASSESSED",
            }
        )
    return {"windows": reports}


def _attach_statistics(
    results: list[dict[str, Any]],
    *,
    bootstrap_iterations: int,
    bootstrap_block_days: int,
    seed: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    raw_by_id: dict[str, tuple[dict[str, Any], int]] = {}
    holdout_by_id: dict[str, tuple[dict[str, float], int]] = {}
    for result in results:
        trial_id = str(result["trial_id"])
        raw, _values, unknown = _raw_statistics(result)
        raw_by_id[trial_id] = (raw, unknown)
        holdout = next(
            (
                row
                for row in result.get("validation", {}).get("windows", ())
                if isinstance(row, Mapping) and row.get("kind") == "holdout"
            ),
            None,
        )
        holdout_by_id[trial_id] = _holdout_daily_map(result, holdout)
    aligned, family_returns = _aligned_holdout_family(holdout_by_id)
    for result in results:
        trial_id = str(result["trial_id"])
        raw, _unknown = raw_by_id[trial_id]
        # Final statistical evidence is intentionally based on the locked
        # holdout.  A short fixture therefore remains NOT_ASSESSED instead of
        # silently falling back to development data.
        holdout = next(
            (
                row
                for row in result.get("validation", {}).get("windows", ())
                if isinstance(row, Mapping) and row.get("kind") == "holdout"
            ),
            None,
        )
        holdout_returns, holdout_unknown = _holdout_daily_data(result, holdout)
        inference_block = (
            _not_assessed("missing_economics", unknown_trades=holdout_unknown) if holdout_unknown else None
        )
        dsr = (
            inference_block
            if inference_block is not None
            else deflated_sharpe_ratio(holdout_returns, family_returns, benchmark=0.0)
            if aligned
            else _not_assessed(
                "holdout_family_not_aligned_or_unknown",
                registry_scope="current_manifest",
                effective_trials=len(family_returns),
            )
        )
        result["statistics"] = {
            "raw": raw,
            "holdout_basis": {
                "daily_observations": len(holdout_returns),
                "unknown_trade_count": holdout_unknown,
                "window": holdout.get("name") if isinstance(holdout, Mapping) else None,
            },
            "psr": inference_block or probabilistic_sharpe_ratio(holdout_returns, benchmark=0.0),
            "dsr": dsr,
            "bootstrap_daily": inference_block
            or bootstrap_daily(
                holdout_returns,
                iterations=bootstrap_iterations,
                block_days=bootstrap_block_days,
                seed=seed,
            ),
        }
    return results, {
        "trial_count": len(holdout_by_id),
        "evidence": "PSR_DSR_bootstrap_only",
        "holdout_family_aligned": aligned,
    }


def _aligned_holdout_family(
    values: Mapping[str, tuple[Mapping[str, float], int]],
) -> tuple[bool, dict[str, list[float]]]:
    if len(values) < 2 or any(unknown for _series, unknown in values.values()):
        return False, {}
    keys = {tuple(sorted(series)) for series, _unknown in values.values()}
    if len(keys) != 1 or not keys or not next(iter(keys)):
        return False, {}
    common = next(iter(keys))
    return True, {trial_id: [series[day] for day in common] for trial_id, (series, _unknown) in values.items()}


def _holdout_bounds(holdout: Mapping[str, Any] | None) -> tuple[datetime, datetime] | None:
    if holdout is None:
        return None
    test_start_raw = holdout.get("test_start")
    test_end_raw = holdout.get("test_end")
    if not isinstance(test_start_raw, str) or not isinstance(test_end_raw, str):
        return None
    try:
        test_start = _aware_timestamp(test_start_raw, name="holdout.test_start")
        test_end = _aware_timestamp(test_end_raw, name="holdout.test_end")
    except ResearchError:
        return None
    return test_start, test_end


def _holdout_membership(row: Mapping[str, Any], bounds: tuple[datetime, datetime]) -> tuple[bool, bool]:
    test_start, test_end = bounds
    interval = _trade_interval(row)
    if interval is None:
        entry = row.get("entry_available_at")
        if not isinstance(entry, str):
            return False, False
        try:
            return test_start <= _aware_timestamp(entry, name="entry_available_at") <= test_end, True
        except ResearchError:
            return False, False
    entry, close = interval
    if entry < test_start or close > test_end:
        return False, False
    return True, row.get("net_pnl") is None


def _holdout_row_value(
    row: Mapping[str, Any], bounds: tuple[datetime, datetime]
) -> tuple[str | None, Decimal | None, bool]:
    included, unknown = _holdout_membership(row, bounds)
    if not included:
        return None, None, False
    if unknown:
        return None, None, True
    close = row.get("close_available_at")
    raw_pnl = row.get("net_pnl")
    if not isinstance(close, str) or raw_pnl is None:
        return None, None, True
    try:
        timestamp = _aware_timestamp(close, name="close_available_at")
        value = Decimal(str(raw_pnl))
    except (InvalidOperation, TypeError, ValueError, ResearchError):
        return None, None, True
    return timestamp.astimezone(UTC).date().isoformat(), value, False


def _holdout_daily_map(result: Mapping[str, Any], holdout: Mapping[str, Any] | None) -> tuple[dict[str, float], int]:
    bounds = _holdout_bounds(holdout)
    if bounds is None:
        return {}, 0
    test_start, test_end = bounds
    observed_value = result.get("observed_calendar_days", ())
    observed_days = observed_value if isinstance(observed_value, Sequence) else ()
    grouped: defaultdict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for day in observed_days:
        if isinstance(day, str) and test_start.date().isoformat() <= day <= test_end.date().isoformat():
            grouped[day] += Decimal("0")
    unknown = 0
    ledger_value = result.get("ledger", ())
    ledger = ledger_value if isinstance(ledger_value, Sequence) else ()
    for row in ledger:
        if not isinstance(row, Mapping):
            continue
        date_key, value, unknown_row = _holdout_row_value(row, bounds)
        if unknown_row:
            unknown += 1
        elif date_key is not None and value is not None:
            grouped[date_key] += value
    return {key: float(grouped[key]) for key in sorted(grouped)}, unknown


def _holdout_daily_data(result: Mapping[str, Any], holdout: Mapping[str, Any] | None) -> tuple[list[float], int]:
    daily, unknown = _holdout_daily_map(result, holdout)
    return list(daily.values()), unknown


def _holdout_daily_returns(result: Mapping[str, Any], holdout: Mapping[str, Any] | None) -> list[float]:
    """Compatibility helper returning only the assessed holdout PnL series."""

    return _holdout_daily_data(result, holdout)[0]


def _base_manifest(
    *,
    config: EffectiveConfig,
    capture: Any,
    manifest_path: Path,
    trials: Sequence[Mapping[str, Any]],
    run_id: str,
    seed: int,
    fixture: bool,
    capture_path: str | Path | None,
    holdout_fraction: float,
    walkforward_folds: int,
    walkforward_train_fraction: float,
    walkforward_test_fraction: float,
    windows: Sequence[ResearchWindow],
    detectors: Sequence[str],
    execution_plan: ExecutionPlan,
) -> dict[str, Any]:
    detector_values = tuple(dict.fromkeys(str(item) for item in detectors))
    hypotheses = {
        str(item["variant"]): dict(item["hypothesis"]) for item in trials if isinstance(item.get("hypothesis"), Mapping)
    }
    decisions = {
        str(item["trial_id"]): dict(item["decision"]) for item in trials if isinstance(item.get("decision"), Mapping)
    }
    observed_times = tuple(event.effective_available_at for event in capture.quote_events) or tuple(
        timestamp
        for bar in capture.bars
        if bool(getattr(bar, "closed", True))
        for timestamp in (
            getattr(bar, "available_at", None) or getattr(bar, "interval_end", getattr(bar, "end", None)),
        )
        if timestamp is not None
    )
    return {
        "schema": RESEARCH_SCHEMA,
        "schema_version": RESEARCH_VERSION,
        "manifest_version": RESEARCH_VERSION,
        "state": "TRIALS_REGISTERED",
        "phase": "TRIALS_REGISTERED",
        "run_id": run_id,
        "product": PRODUCT,
        "detector": detector_values[0] if len(detector_values) == 1 else "multiple_strategy_variants",
        "detectors": list(detector_values),
        "instrument": config.instrument,
        "execution_model": execution_plan.to_dict(),
        "hypotheses": hypotheses,
        "decision_policy": dict(_DECISION_POLICY),
        "decisions": decisions,
        "data_hash": capture.capture_hash,
        "config_hash": config.config_hash,
        "manifest_path": str(manifest_path),
        "data": {
            "capture_hash": capture.capture_hash,
            "capture_id": capture.capture_id,
            "synthetic": bool(capture.provenance.get("synthetic", False)),
            "fixture_flag": fixture,
            "capture_path": str(Path(capture_path).expanduser()) if capture_path is not None else None,
            "coverage": capture.coverage.to_dict(),
            "issues": list(capture.issues),
            "observed_calendar_days": sorted(
                {timestamp.astimezone(UTC).date().isoformat() for timestamp in observed_times}
            ),
            "calendar_basis": "capture_quote_or_bar_availability",
            "weekends_fabricated": False,
        },
        "config": {
            "path": config.path,
            "hash": config.config_hash,
            "effective": config.to_dict(include_hash=False),
        },
        "instrument_spec": {
            "origin": "synthetic_fixture" if fixture else "explicit_config.ctrader",
            "observed": fixture
            or all(key in config.ctrader for key in ("symbol_id", "digits", "pip_position", "price_scale")),
            "fields": ["symbol_id", "digits", "pip_position", "price_scale"],
        },
        "costs_contract": _cost_contract(config, fixture=fixture),
        "code_hash": _code_hash(),
        "seed": seed,
        "policy": {
            "holdout_fraction": holdout_fraction,
            "walkforward_folds": walkforward_folds,
            "walkforward_train_fraction": walkforward_train_fraction,
            "walkforward_test_fraction": walkforward_test_fraction,
            "split_basis": "quote_available_at",
            "purge_basis": "actual_trade_holding_interval",
            "warmup_preserved": True,
            "bootstrap_unit": "daily",
            "execution_model": execution_plan.scenario,
            "execution_parameters": execution_plan.to_dict(),
            "execution_scenarios": {
                "full_fill": "MODELED_BY_CFDSIMULATOR",
                "ioc_partial": "MODELED_SYNTHETIC_HYPOTHESIS",
                "rejected": "MODELED_SYNTHETIC_LOCAL_REJECTION",
            },
        },
        "windows": [window.to_dict() for window in windows],
        "trials": [dict(item) for item in trials],
        "trial_registration": {
            "state": "COMPLETE",
            "count": len(trials),
            "before_results": True,
            "all_variants_and_horizons_registered": True,
            "registry_scope": "current_manifest",
            "historical_attempts_included": False,
            "historical_completeness": "NOT_VERIFIED",
            "hypotheses_preregistered": True,
            "decisions_preregistered": True,
        },
        "trials_registered_before_results": True,
        "results": [],
        "summary": {},
    }


def _manifest_integrity(payload: Mapping[str, Any]) -> str:
    material = dict(payload)
    material.pop("integrity_hash", None)
    return _digest(material)


def _prepare_research_context(
    capture_path: str | Path | None,
    *,
    manifest_path: str | Path | None,
    fixture: bool,
    fixture_count: int,
    config_path: str | Path | None,
    variants: Sequence[str | Mapping[str, Any] | ResearchVariant] | str | None,
    horizons_seconds: Sequence[Any] | None,
    seed: int,
    holdout_fraction: float,
    walkforward_folds: int,
    walkforward_train_fraction: float,
    walkforward_test_fraction: float,
    order: ResearchOrder,
    execution_model: ExecutionModel | str,
    fill_fraction: Decimal | int | str | None,
) -> _ResearchContext:
    """Validate and materialize the immutable inputs before registration."""

    target = _safe_manifest_path(manifest_path)
    if fixture and capture_path is not None:
        raise ResearchError("fixture=True y capture_path son incompatibles")
    if not fixture and capture_path is None:
        raise ResearchError("indique fixture=True o capture_path explícito")
    if not fixture and config_path is None:
        raise ResearchError("las capturas no sintéticas requieren config_path explícito")
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise ResearchError("seed debe ser entero")
    config = _load_research_config(config_path)
    if not fixture:
        config = _real_capture_config(config, config_path=config_path)
    requested_quantity = config.cfd.get("units", config.cfd.get("default_quantity", config.cfd.get("quantity", "1000")))
    execution_plan = build_execution_plan(
        requested_quantity,
        execution_model=execution_model,
        fill_fraction=fill_fraction,
    )
    selected_variants = _normalise_variants(variants)
    _validate_variant_implementations(selected_variants)
    selected_horizons = _normalise_horizons(config, horizons_seconds)
    try:
        capture = (
            synthetic_cfd_capture(config=config, count=fixture_count)
            if fixture
            else load_cfd_capture(capture_path or "", config=config, order=order)
        )
    except CFDBacktestError:
        raise
    except (TypeError, ValueError, OSError) as exc:
        raise ResearchError(f"captura CFD inválida: {exc}") from exc
    if not fixture and bool(capture.provenance.get("synthetic", False)):
        raise ResearchError("una captura sintética requiere fixture=True; no se publicita como real")
    max_horizon = max(selected_horizons)
    windows = build_research_windows(
        capture,
        holdout_fraction=holdout_fraction,
        walkforward_folds=walkforward_folds,
        walkforward_train_fraction=walkforward_train_fraction,
        walkforward_test_fraction=walkforward_test_fraction,
        purge_holding_seconds=max_horizon,
        embargo_seconds=max_horizon,
    )
    run_id = (
        "research_"
        + _digest(
            {
                "capture_hash": capture.capture_hash,
                "config_hash": config.config_hash,
                "variants": [item.to_dict() for item in selected_variants],
                "hypotheses": [_variant_hypothesis(item) for item in selected_variants],
                "horizons": [str(item) for item in selected_horizons],
                "seed": seed,
                "execution_model": execution_plan.to_dict(),
                "policy": [holdout_fraction, walkforward_folds, walkforward_train_fraction, walkforward_test_fraction],
            }
        )[:32]
    )
    trials = _registered_trials(
        run_id=run_id,
        config=config,
        variants=selected_variants,
        horizons=selected_horizons,
        seed=seed,
        execution_plan=execution_plan,
    )
    manifest = _base_manifest(
        config=config,
        capture=capture,
        manifest_path=target,
        trials=trials,
        run_id=run_id,
        seed=seed,
        fixture=fixture,
        capture_path=capture_path,
        holdout_fraction=holdout_fraction,
        walkforward_folds=walkforward_folds,
        walkforward_train_fraction=walkforward_train_fraction,
        walkforward_test_fraction=walkforward_test_fraction,
        windows=windows,
        detectors=tuple(_VARIANT_DETECTORS[item.name] for item in selected_variants),
        execution_plan=execution_plan,
    )
    return _ResearchContext(
        target=target,
        config=config,
        capture=capture,
        fixture=fixture,
        capture_path=capture_path,
        order=order,
        execution_plan=execution_plan,
        trials=trials,
        windows=windows,
        manifest=manifest,
    )


@decimal_context()
def run_research(
    capture_path: str | Path | None = None,
    *,
    manifest_path: str | Path | None = None,
    fixture: bool = False,
    fixture_count: int = 190,
    config_path: str | Path | None = None,
    variants: Sequence[str | Mapping[str, Any] | ResearchVariant] | str | None = None,
    horizons_seconds: Sequence[Any] | None = None,
    seed: int = 42,
    holdout_fraction: float = 0.20,
    walkforward_folds: int = 4,
    walkforward_train_fraction: float = 0.40,
    walkforward_test_fraction: float = 0.10,
    bootstrap_iterations: int = 250,
    bootstrap_block_days: int = 5,
    order: ResearchOrder = "as_observed",
    execution_model: ExecutionModel | str = "full_fill",
    fill_fraction: Decimal | int | str | None = None,
) -> dict[str, Any]:
    """Run all registered variant/horizon trials and atomically publish v1.

    Exactly one of ``fixture=True`` or ``capture_path`` is required.  The
    fixture path is deterministic and never reads the local corpus.  A real
    capture is accepted only through the explicit local path.
    """

    context = _prepare_research_context(
        capture_path,
        manifest_path=manifest_path,
        fixture=fixture,
        fixture_count=fixture_count,
        config_path=config_path,
        variants=variants,
        horizons_seconds=horizons_seconds,
        seed=seed,
        holdout_fraction=holdout_fraction,
        walkforward_folds=walkforward_folds,
        walkforward_train_fraction=walkforward_train_fraction,
        walkforward_test_fraction=walkforward_test_fraction,
        order=order,
        execution_model=execution_model,
        fill_fraction=fill_fraction,
    )
    target = context.target
    config = context.config
    capture = context.capture
    fixture = context.fixture
    capture_path = context.capture_path
    order = context.order
    execution_plan = context.execution_plan
    trials = context.trials
    windows = context.windows
    manifest = context.manifest
    attempt_token = uuid.uuid4().hex
    _write_manifest(target, {**manifest, "integrity_hash": _manifest_integrity(manifest)}, attempt_token=attempt_token)
    results: list[dict[str, Any]] = []
    try:
        for trial in trials:
            variant = ResearchVariant(str(trial["variant"]), str(trial.get("variant_description", "")))
            result = run_cfd_backtest(
                capture,
                config=config,
                variant=variant.name,
                horizon_seconds=str(trial["horizon_seconds"]),
                order=order,
                fixture=fixture,
                execution_model=execution_plan.scenario,
                fill_fraction=execution_plan.fill_fraction,
            )
            result["trial_id"] = trial["trial_id"]
            result["registration_order"] = trial["registration_order"]
            result["hypothesis"] = trial["hypothesis"]
            result["validation"] = _window_statistics(result, windows)
            results.append(result)
        results, family_summary = _attach_statistics(
            results,
            bootstrap_iterations=bootstrap_iterations,
            bootstrap_block_days=bootstrap_block_days,
            seed=seed,
        )
        _decisions, decision_counts = _commit_research_decisions(manifest, trials, results, fixture=fixture)
        manifest["results"] = results
        manifest["summary"] = {
            "result_count": len(results),
            "trial_count": len(trials),
            "trials_registered_before_results": True,
            "family": family_summary,
            "decision_counts": decision_counts,
            "decision_policy": dict(_DECISION_POLICY),
            "unknown_cost_results": sum(
                int(result.get("costs", {}).get("unknown_count", 0)) > 0
                for result in results
                if isinstance(result.get("costs"), Mapping)
            ),
            "leakage_violation_results": sum(bool(result.get("leakage_violations")) for result in results),
        }
        manifest["state"] = "COMPLETED"
        manifest["phase"] = "RESULTS_COMMITTED"
        manifest["integrity_hash"] = _manifest_integrity(manifest)
        _write_manifest(target, manifest, attempt_token=attempt_token)
        return manifest
    except Exception as exc:
        failed = dict(manifest)
        failed["state"] = "FAILED"
        failed["phase"] = "RESULTS_FAILED"
        failed["error"] = {"type": type(exc).__name__, "message": str(exc)}
        failed["results"] = results
        failed["summary"] = {
            "trial_count": len(trials),
            "result_count": len(results),
            "decision_counts": {"PENDING_RESULTS": len(trials)},
            "decision_policy": dict(_DECISION_POLICY),
        }
        failed["integrity_hash"] = _manifest_integrity(failed)
        _write_manifest(target, failed, attempt_token=attempt_token)
        raise


def _manifest_basic_identity_errors(manifest: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    if (
        manifest.get("schema") != RESEARCH_SCHEMA
        or manifest.get("schema_version") != RESEARCH_VERSION
        or manifest.get("manifest_version") != RESEARCH_VERSION
    ):
        errors.append("schema_version_incompatible")
    expected = manifest.get("integrity_hash")
    if not isinstance(expected, str) or expected != _manifest_integrity(manifest):
        errors.append("integrity_hash_mismatch")
    if manifest.get("product") != PRODUCT:
        errors.append("product_mismatch")
    if not _supported_detector(manifest.get("detector")):
        errors.append("detector_mismatch")
    if manifest.get("trials_registered_before_results") is not True:
        errors.append("trials_not_registered_before_results")
    return errors


def _manifest_hash_errors(manifest: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    code_hash = manifest.get("code_hash")
    if not isinstance(code_hash, str) or not code_hash:
        errors.append("code_hash_missing")
    if not isinstance(manifest.get("seed"), int) or isinstance(manifest.get("seed"), bool):
        errors.append("seed_missing")
    data = manifest.get("data")
    if not isinstance(data, Mapping) or not isinstance(data.get("capture_hash"), str) or not data.get("capture_hash"):
        errors.append("data_hash_missing")
    elif manifest.get("data_hash") != data.get("capture_hash"):
        errors.append("data_hash_alias_mismatch")
    config = manifest.get("config")
    if not isinstance(config, Mapping) or not isinstance(config.get("hash"), str) or not config.get("hash"):
        errors.append("config_hash_missing")
    elif manifest.get("config_hash") != config.get("hash"):
        errors.append("config_hash_alias_mismatch")
    return errors


def _manifest_identity_errors(manifest: Mapping[str, Any]) -> list[str]:
    return [*_manifest_basic_identity_errors(manifest), *_manifest_hash_errors(manifest)]


def _manifest_trials(manifest: Mapping[str, Any]) -> tuple[list[Mapping[str, Any]], list[Mapping[str, Any]], list[str]]:
    trials_value = manifest.get("trials")
    results_value = manifest.get("results")
    trials = [item for item in trials_value if isinstance(item, Mapping)] if isinstance(trials_value, Sequence) else []
    results = (
        [item for item in results_value if isinstance(item, Mapping)] if isinstance(results_value, Sequence) else []
    )
    trial_ids = [str(item.get("trial_id")) for item in trials]
    errors = []
    if len(set(trial_ids)) != len(trial_ids) or any(not item or item == "None" for item in trial_ids):
        errors.append("trial_ids_not_unique")
    registered = set(trial_ids)
    for result in results:
        trial_id = result.get("trial_id")
        if str(trial_id) not in registered:
            errors.append(f"unregistered_result:{trial_id}")
        if result.get("product") != PRODUCT or not _supported_detector(result.get("detector")):
            errors.append(f"result_identity_mismatch:{trial_id}")
        if result.get("leakage_violations"):
            errors.append(f"leakage_violation:{trial_id}")
        for key in ("ledger", "equity_mark_to_market", "exposure", "costs", "execution_model"):
            if key not in result:
                errors.append(f"result_missing:{key}:{trial_id}")
    return trials, results, errors


def _manifest_window_policy_errors(manifest: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    windows = manifest.get("windows")
    if not isinstance(windows, Sequence) or not windows:
        errors.append("windows_missing")
    else:
        for window in windows:
            if not isinstance(window, Mapping) or window.get("warmup_preserved") is not True:
                errors.append("warmup_not_preserved")
    policy = manifest.get("policy")
    expected = {
        "holdout_fraction": 0.2,
        "walkforward_folds": 4,
        "walkforward_train_fraction": 0.4,
        "walkforward_test_fraction": 0.1,
    }
    if not isinstance(policy, Mapping):
        return ["policy_missing"]
    for key, value in expected.items():
        if policy.get(key) != value:
            errors.append(f"{key}_policy_mismatch")
    return errors


def validate_research(manifest_path: str | Path, *, strict: bool = True) -> dict[str, Any]:
    """Validate the v1 manifest, trial ordering, hashes and no-leakage facts."""
    from .research_validation import validate_manifest_contract

    target, manifest = _read_manifest(manifest_path)
    errors = _manifest_identity_errors(manifest)
    registration = manifest.get("trial_registration")
    if not isinstance(registration, Mapping) or registration.get("before_results") is not True:
        errors.append("trials_not_registered_before_results")
    trials, results, trial_errors = _manifest_trials(manifest)
    errors.extend(trial_errors)
    errors.extend(_manifest_window_policy_errors(manifest))
    errors.extend(validate_manifest_contract(manifest))
    errors = sorted(set(errors))
    if strict and manifest.get("state") != "COMPLETED":
        errors.append("manifest_not_completed")
    return {
        "ok": not errors,
        "state": "VALID" if not errors else "INVALID",
        "manifest_path": str(target),
        "schema": manifest.get("schema"),
        "trial_count": len(trials),
        "result_count": len(results),
        "errors": errors,
    }


def compare_research(
    manifest_paths: Sequence[str | Path],
    *,
    strict: bool = True,
) -> dict[str, Any]:
    """Compare manifests only when their CFD product/data identity agrees."""

    if isinstance(manifest_paths, (str, bytes)) or len(manifest_paths) < 2:
        raise ResearchError("compare requiere al menos dos manifests")
    loaded = [_read_manifest(path) for path in manifest_paths]
    manifests = [item[1] for item in loaded]
    identities = {
        "product": {item.get("product") for item in manifests},
        "instrument": {item.get("instrument") for item in manifests},
        "capture_hash": {
            item.get("data", {}).get("capture_hash") for item in manifests if isinstance(item.get("data"), Mapping)
        },
        "detector": {
            tuple(item.get("detectors", (item.get("detector"),)))
            if isinstance(item.get("detectors", (item.get("detector"),)), Sequence)
            else (item.get("detector"),)
            for item in manifests
        },
        "execution_model": {_digest(item.get("execution_model", {})) for item in manifests},
    }
    # Different detectors are the intended comparison axis, not a product
    # mismatch. Their identities remain visible on the individual trial rows.
    mismatches = {
        key: sorted(str(value) for value in values)
        for key, values in identities.items()
        if len(values) != 1 and key != "detector"
    }
    if strict:
        for key in ("product", "instrument", "capture_hash", "execution_model"):
            if len(identities[key]) != 1:
                raise ResearchError(f"compare estricto rechaza {key} incompatible: {mismatches.get(key)}")
        invalid = [validate_research(path, strict=True) for path, _ in loaded]
        if any(not item["ok"] for item in invalid):
            raise ResearchError("compare estricto requiere manifests válidos/completos")
    rows: list[dict[str, Any]] = []
    for path, manifest in loaded:
        values = manifest.get("results", ())
        for result in values if isinstance(values, Sequence) else ():
            if not isinstance(result, Mapping):
                continue
            raw_stats = result.get("statistics")
            stats: Mapping[str, Any] = raw_stats if isinstance(raw_stats, Mapping) else {}
            raw_value = stats.get("raw")
            raw: Mapping[str, Any] = raw_value if isinstance(raw_value, Mapping) else {}
            equity_value = result.get("equity_summary")
            equity: Mapping[str, Any] = equity_value if isinstance(equity_value, Mapping) else {}
            execution_value = result.get("execution_model")
            execution: Mapping[str, Any] = execution_value if isinstance(execution_value, Mapping) else {}
            rows.append(
                {
                    "manifest_path": str(path),
                    "trial_id": result.get("trial_id"),
                    "variant": result.get("variant"),
                    "detector": result.get("detector"),
                    "horizon_seconds": result.get("horizon_seconds"),
                    "net_pnl": equity.get("net_pnl"),
                    "daily_observations": raw.get("daily_observations"),
                    "psr": stats.get("psr"),
                    "dsr": stats.get("dsr"),
                    "costs": result.get("costs"),
                    "execution_model": execution.get("scenario"),
                }
            )
    rows.sort(
        key=lambda row: (
            str(row.get("variant")),
            _decimal(row.get("horizon_seconds"), name="horizon_seconds"),
            str(row.get("trial_id")),
        )
    )
    return {
        "ok": not mismatches or not strict,
        "state": "COMPARED" if not mismatches or not strict else "INCOMPATIBLE",
        "strict": strict,
        "identity": {key: next(iter(values)) if len(values) == 1 else None for key, values in identities.items()},
        "mismatches": mismatches,
        "manifests": [str(path) for path, _ in loaded],
        "rows": rows,
    }


def research_command(args: Any) -> tuple[int, dict[str, Any]]:
    """Small adapter contract for the root CLI; it does not parse arguments."""

    action = str(getattr(args, "research_action", getattr(args, "action", getattr(args, "command", "run")))).lower()
    try:
        if action == "run":
            capture_value = getattr(args, "capture_path", None) or getattr(args, "input", None)
            manifest_value = getattr(args, "manifest_path", None) or getattr(args, "manifest", None)
            horizons_value = getattr(args, "horizons_seconds", None) or getattr(args, "horizons", None)
            payload = run_research(
                capture_value,
                manifest_path=manifest_value,
                fixture=bool(getattr(args, "fixture", False)),
                fixture_count=int(getattr(args, "fixture_count", 190)),
                config_path=getattr(args, "config", None),
                variants=getattr(args, "variants", None),
                horizons_seconds=horizons_value,
                seed=int(getattr(args, "seed", 42)),
                bootstrap_iterations=int(getattr(args, "bootstrap_iterations", 250)),
                bootstrap_block_days=int(getattr(args, "bootstrap_block_days", 5)),
                order=_normalise_order(getattr(args, "order", "as_observed")),
                execution_model=getattr(args, "execution_model", "full_fill"),
                fill_fraction=getattr(args, "fill_fraction", None),
            )
        elif action == "compare":
            manifest_args = cast(
                Sequence[str | Path], getattr(args, "manifests", None) or getattr(args, "manifest_paths", ())
            )
            payload = compare_research(
                manifest_args,
                strict=not bool(getattr(args, "non_strict", False)),
            )
        elif action == "validate":
            payload = validate_research(
                getattr(args, "manifest_path", getattr(args, "manifest", "")),
                strict=not bool(getattr(args, "non_strict", False)),
            )
        else:
            raise ResearchError(f"research action desconocida: {action}")
        return 0 if payload.get("ok", True) else 2, payload
    except (CFDBacktestError, ResearchError, TypeError, ValueError, OSError) as exc:
        return 2, {"ok": False, "state": type(exc).__name__, "error": str(exc)}


class ResearchService:
    """Named service facade for root adapters that prefer a class boundary."""

    run = staticmethod(run_research)
    compare = staticmethod(compare_research)
    validate = staticmethod(validate_research)
    command = staticmethod(research_command)


__all__ = [
    "DETECTOR",
    "ExecutionModel",
    "PRODUCT",
    "RESEARCH_SCHEMA",
    "RESEARCH_VERSION",
    "ResearchError",
    "ResearchService",
    "ResearchVariant",
    "bootstrap_daily",
    "compare_research",
    "deflated_sharpe_ratio",
    "probabilistic_sharpe_ratio",
    "research_command",
    "run_research",
    "validate_research",
]
