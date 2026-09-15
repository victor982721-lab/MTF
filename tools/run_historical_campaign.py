#!/usr/bin/env python3
"""Run one registered, development-only historical MTF campaign.

This is an intentionally small local boundary around the canonical historical
backtest.  It consumes an already acquired HistData manifest, validates that
manifest for one explicit half-open UTC window, records the attempt before
calling :func:`run_historical_backtest`, and retains enough source/input bytes
to replay the decision later.  It does not acquire data, open a database,
contact a provider, select a candidate, promote a result, or place an order.

The runner is a tool rather than a second backtest implementation.  All quote
processing, risk, exits, checkpoints, and result artifacts remain owned by
``mtf_lab.ops.historical_backtest``.  The only stream transformation here is
an explicit window/prefix bound, so a development run cannot accidentally
consume the reserved holdout.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import inspect
import json
import os
import platform
import stat
import sys
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, TypeAlias, cast

from mtf_lab.core.canonical import canonical_json, instant_text
from mtf_lab.data.historical import (
    DatasetManifest,
    DatasetValidation,
    HistoricalQuote,
    iter_quotes,
    read_manifest,
    validate_dataset,
)
from mtf_lab.data.walk_forward import (
    WalkForwardError,
    WalkForwardInput,
    WalkForwardManifest,
    WalkForwardStreamGuard,
    WalkForwardWindow,
    read_walk_forward_manifest,
)
from mtf_lab.ops.global_trial_registry import GlobalTrialRegistry, RegistryError
from mtf_lab.ops.historical_backtest import (
    HistoricalBacktestCheckpoint,
    HistoricalBacktestConfig,
    HistoricalBacktestError,
    run_historical_backtest,
)
from mtf_lab.ops.market_protocol import MarketProtocolError, ResearchProtocol, read_protocol

RUNNER_SCHEMA = "mtf-lab.historical-campaign-runner.v1"
DEVELOPMENT_START = datetime(2016, 1, 1, tzinfo=UTC)
DEVELOPMENT_END = datetime(2020, 1, 1, tzinfo=UTC)
WALK_FORWARD_START = DEVELOPMENT_END
WALK_FORWARD_END = datetime(2024, 1, 1, tzinfo=UTC)
HOLDOUT_START = WALK_FORWARD_END
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
_CODE_SUFFIXES = frozenset({".py", ".pyi"})
_SECRET_PARTS = ("token", "secret", "password", "credential", "private_key")

ManifestInput: TypeAlias = DatasetManifest | str | Path
WalkForwardManifestInput: TypeAlias = WalkForwardManifest | str | Path
WalkForwardInputValue: TypeAlias = WalkForwardInput | Mapping[str, Any] | str | Path
ProtocolInput: TypeAlias = ResearchProtocol | str | Path | None
RegistryInput: TypeAlias = GlobalTrialRegistry | str | Path
ValidationInput: TypeAlias = DatasetValidation | Mapping[str, Any] | str | Path | None
ResumeInput: TypeAlias = HistoricalBacktestCheckpoint | Mapping[str, Any] | str | Path
Runner: TypeAlias = Callable[..., Any]

_ARTIFACT_KINDS = frozenset({"ledger", "equity", "funnel"})


class HistoricalCampaignError(ValueError):
    """A local campaign request cannot satisfy its development-only contract."""


# Compatibility aliases make the tool easy to find for callers that use the
# shorter names from the surrounding research service.
CampaignRunnerError = HistoricalCampaignError
HistoricalCampaignRunnerError = HistoricalCampaignError


@dataclass(frozen=True, slots=True)
class WalkForwardContractBinding:
    """Read-only binding for the independent walk-forward contract.

    This is deliberately a preflight object, not a runnable backtest.  It
    binds the development manifest, the independent WF manifest, the frozen
    hash-only input and the causal stream guard without reading raw archives,
    writing a registry/output directory, or opening a WF/holdout run.
    """

    development_manifest: DatasetManifest
    walk_forward_manifest: WalkForwardManifest
    input_contract: WalkForwardInput
    window: WalkForwardWindow
    guard: WalkForwardStreamGuard

    def to_dict(self) -> dict[str, Any]:
        return {
            "stage": "walk-forward",
            "window": self.window.to_dict(),
            "input_contract": self.input_contract.to_dict(),
            "manifest_hashes": dict(self.guard.manifest_hashes),
            "holdout": "CLOSED",
            "selection_performed": False,
            "promotion_performed": False,
            "trading_enabled": False,
            "guard_snapshot": self.guard.snapshot(),
        }


def _jsonable(value: Any) -> Any:
    """Convert supported DTOs to bounded JSON values without guessing."""

    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(item) for item in value]
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return instant_text(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        return _jsonable(to_dict())
    raise HistoricalCampaignError(f"valor no serializable: {type(value).__name__}")


def _hash_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _hash_mapping(value: Mapping[str, Any]) -> str:
    return _hash_bytes(canonical_json(_jsonable(value)).encode("utf-8"))


def _absolute(path: str | Path, *, name: str) -> Path:
    target = Path(path).expanduser()
    if not target.is_absolute():
        raise HistoricalCampaignError(f"{name} debe ser una ruta absoluta")
    return Path(os.path.abspath(os.fspath(target)))


def _check_ancestors(target: Path, *, name: str, allow_missing: bool) -> None:
    """Reject symlink/non-directory ancestors before any write/read."""

    current = Path(target.anchor)
    for component in target.parent.parts:
        if component == target.anchor:
            continue
        current /= component
        try:
            info = os.lstat(current)
        except FileNotFoundError:
            if allow_missing:
                break
            raise HistoricalCampaignError(f"padre de {name} no existe: {current}") from None
        except OSError as exc:
            raise HistoricalCampaignError(f"padre de {name} ilegible: {current}") from exc
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise HistoricalCampaignError(f"padre de {name} inseguro: {current}")


def _safe_input_file(path: str | Path, *, name: str) -> Path:
    target = _absolute(path, name=name)
    _check_ancestors(target, name=name, allow_missing=False)
    try:
        info = os.lstat(target)
    except OSError as exc:
        raise HistoricalCampaignError(f"{name} no se pudo inspeccionar: {target}") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise HistoricalCampaignError(f"{name} debe ser un archivo regular exclusivo")
    return target


def _lstat_output_component(path: Path) -> os.stat_result:
    try:
        return os.lstat(path)
    except OSError as exc:
        raise HistoricalCampaignError(f"output_dir ilegible: {path}") from exc


def _inspect_output_component(path: Path) -> tuple[os.stat_result, bool]:
    """Inspect one component, creating it only when it is absent."""

    try:
        return os.lstat(path), False
    except FileNotFoundError:
        created = False
        try:
            os.mkdir(path, 0o700)
        except FileExistsError:
            pass
        except OSError as exc:
            raise HistoricalCampaignError(f"output_dir ilegible: {path}") from exc
        else:
            created = True
        return _lstat_output_component(path), created
    except OSError as exc:
        raise HistoricalCampaignError(f"output_dir ilegible: {path}") from exc


def _validate_output_component(
    path: Path,
    info: os.stat_result,
    *,
    current_uid: int,
    require_owner: bool = False,
) -> None:
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise HistoricalCampaignError(f"output_dir inseguro: {path}")
    if require_owner and info.st_uid != current_uid:
        raise HistoricalCampaignError(f"output_dir debe pertenecer al usuario actual: {path}")


def _make_output_component_private(path: Path, *, current_uid: int) -> None:
    try:
        os.chmod(path, 0o700)
    except OSError as exc:
        raise HistoricalCampaignError(f"output_dir no pudo hacerse privado: {path}") from exc
    info = _lstat_output_component(path)
    _validate_output_component(path, info, current_uid=current_uid, require_owner=True)
    if stat.S_IMODE(info.st_mode) != 0o700:
        raise HistoricalCampaignError(f"output_dir inseguro: {path}")


def _safe_output_dir(path: str | Path) -> Path:
    target = _absolute(path, name="output_dir")
    try:
        target.relative_to(REPOSITORY_ROOT)
    except ValueError:
        pass
    else:
        raise HistoricalCampaignError("output_dir debe estar fuera del checkout")

    # Existing ancestors belong to the filesystem layout (for example
    # ``/home`` or ``/tmp``), not to this invocation.  Inspect them without
    # changing their mode; only directories created here, or the requested
    # output directory itself, may be made private below.
    current_uid = os.getuid()
    current = Path(target.anchor)
    for component in target.parts:
        if component == target.anchor:
            continue
        current /= component
        info, created = _inspect_output_component(current)
        _validate_output_component(current, info, current_uid=current_uid, require_owner=created or current == target)
        if created or current == target:
            _make_output_component_private(current, current_uid=current_uid)
    return target


def _safe_existing_directory(path: str | Path, *, name: str) -> Path:
    """Inspect an already-created private directory without creating it."""

    target = _absolute(path, name=name)
    try:
        target.relative_to(REPOSITORY_ROOT)
    except ValueError:
        pass
    else:
        raise HistoricalCampaignError(f"{name} debe estar fuera del checkout")
    _check_ancestors(target, name=name, allow_missing=False)
    try:
        info = os.lstat(target)
    except OSError as exc:
        raise HistoricalCampaignError(f"{name} ilegible: {target}") from exc
    _validate_output_component(target, info, current_uid=os.getuid(), require_owner=True)
    if stat.S_IMODE(info.st_mode) & 0o077:
        raise HistoricalCampaignError(f"{name} debe permanecer privado: {target}")
    return target


def _validate_run_id(value: str, *, name: str = "run_id") -> str:
    selected = str(value).strip()
    if not selected or any(
        character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for character in selected
    ):
        raise HistoricalCampaignError(f"{name} contiene caracteres no permitidos")
    return selected


def _read_json_mapping(path: str | Path, *, name: str) -> tuple[dict[str, Any], Path]:
    """Read one private JSON object without following a symlink."""

    source = _safe_input_file(path, name=name)
    try:
        raw = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HistoricalCampaignError(f"{name} no contiene JSON legible") from exc
    if not isinstance(raw, Mapping):
        raise HistoricalCampaignError(f"{name} debe ser un objeto JSON")
    return cast(dict[str, Any], dict(raw)), source


def _optional_json_mapping(path: Path, *, name: str) -> tuple[dict[str, Any] | None, Path | None]:
    try:
        os.lstat(path)
    except FileNotFoundError:
        return None, None
    return _read_json_mapping(path, name=name)


def _private_artifact(path: Path, *, name: str) -> Path:
    """Validate a resumable artifact before the backtest is allowed to open it."""

    target = _safe_input_file(path, name=name)
    try:
        info = os.lstat(target)
    except OSError as exc:
        raise HistoricalCampaignError(f"{name} ilegible: {target}") from exc
    if stat.S_IMODE(info.st_mode) & 0o077:
        raise HistoricalCampaignError(f"{name} debe permanecer privado: {target}")
    return target


def _mkdir_private(path: Path) -> None:
    try:
        info = os.lstat(path)
    except FileNotFoundError:
        os.mkdir(path, 0o700)
        info = os.lstat(path)
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise HistoricalCampaignError(f"directorio privado inseguro: {path}")
    os.chmod(path, 0o700)


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _exclusive_bytes(path: Path, data: bytes, *, mode: int = 0o600) -> tuple[str, int]:
    """Write one new private file durably without replacing a prior artifact."""

    _check_ancestors(path, name="artifact", allow_missing=False)
    if path.exists() or os.path.lexists(path):
        raise HistoricalCampaignError(f"artifact ya existe: {path}")
    token = uuid.uuid4().hex
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{token}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
    fd = os.open(temporary, flags, mode)
    try:
        offset = 0
        while offset < len(data):
            written = os.write(fd, data[offset:])
            if written <= 0:
                raise OSError("escritura no progresó")
            offset += written
        os.fchmod(fd, mode)
        os.fsync(fd)
    except BaseException:
        try:
            os.close(fd)
        finally:
            with contextlib.suppress(FileNotFoundError):
                temporary.unlink()
        raise
    os.close(fd)
    try:
        os.link(temporary, path)
        temporary.unlink()
        _fsync_directory(path.parent)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()
        raise
    return _hash_bytes(data), len(data)


def _copy_exact(source: Path, destination: Path) -> tuple[str, int]:
    """Copy one regular source byte-for-byte into a new private artifact."""

    _safe_input_file(source, name="source")
    _check_ancestors(destination, name="copied artifact", allow_missing=False)
    if destination.exists() or os.path.lexists(destination):
        raise HistoricalCampaignError(f"copied artifact ya existe: {destination}")
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    source_fd = os.open(source, os.O_RDONLY | os.O_NOFOLLOW)
    destination_fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    digest = hashlib.sha256()
    size = 0
    try:
        while chunk := os.read(source_fd, 1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
            offset = 0
            while offset < len(chunk):
                written = os.write(destination_fd, chunk[offset:])
                if written <= 0:
                    raise OSError("copia no progresó")
                offset += written
        os.fchmod(destination_fd, 0o600)
        os.fsync(destination_fd)
    except BaseException:
        try:
            os.close(source_fd)
        finally:
            try:
                os.close(destination_fd)
            finally:
                with contextlib.suppress(FileNotFoundError):
                    temporary.unlink()
        raise
    os.close(source_fd)
    os.close(destination_fd)
    try:
        os.link(temporary, destination)
        temporary.unlink()
        _fsync_directory(destination.parent)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()
        raise
    return digest.hexdigest(), size


def _write_json(path: Path, value: Mapping[str, Any]) -> tuple[str, int]:
    payload = (canonical_json(_jsonable(value)) + "\n").encode("utf-8")
    return _exclusive_bytes(path, payload)


def _secret_free(value: Any, *, key: str = "") -> Any:
    """Validate a metadata mapping before it enters a registry/receipt."""

    lowered = key.lower()
    if any(part in lowered for part in _SECRET_PARTS):
        raise HistoricalCampaignError(f"campo sensible no permitido: {key}")
    if isinstance(value, Mapping):
        return {str(name): _secret_free(item, key=str(name)) for name, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_secret_free(item, key=key) for item in value]
    return _jsonable(value)


def _runtime_identity(value: Mapping[str, Any] | None) -> dict[str, Any]:
    if value is None:
        return {
            "component": "tools.run_historical_campaign",
            "operation": "historical_backtest",
            "execution": "local_offline",
            "python": platform.python_version(),
            "executable": sys.executable,
        }
    if not isinstance(value, Mapping) or len(value) > 32:
        raise HistoricalCampaignError("runtime_identity debe ser mapping de máximo 32 campos")
    result = _secret_free(value)
    if not isinstance(result, dict):
        raise HistoricalCampaignError("runtime_identity inválido")
    return cast(dict[str, Any], result)


def _mapping_value(value: Any, *, name: str) -> dict[str, Any]:
    if isinstance(value, Mapping):
        result = _secret_free(value)
        if not isinstance(result, dict):
            raise HistoricalCampaignError(f"{name} inválido")
        return cast(dict[str, Any], result)
    if isinstance(value, (str, Path)):
        source = _safe_input_file(value, name=name)
        try:
            raw = json.loads(source.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise HistoricalCampaignError(f"{name} no contiene JSON legible") from exc
        if not isinstance(raw, Mapping):
            raise HistoricalCampaignError(f"{name} debe ser un objeto JSON")
        result = _secret_free(raw)
        if not isinstance(result, dict):
            raise HistoricalCampaignError(f"{name} inválido")
        return cast(dict[str, Any], result)
    raise HistoricalCampaignError(f"{name} debe ser mapping o JSON local")


def _parse_utc(value: datetime | str | None, *, name: str) -> datetime:
    if value is None:
        raise HistoricalCampaignError(f"{name} es obligatorio; la ventana debe ser explícita")
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise HistoricalCampaignError(f"{name} debe ser ISO-8601 aware") from exc
    else:
        raise HistoricalCampaignError(f"{name} debe ser ISO-8601 aware")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise HistoricalCampaignError(f"{name} debe incluir zona horaria")
    return parsed.astimezone(UTC)


def _stage(value: str) -> str:
    normalized = str(value).strip().lower().replace("_", "-")
    if normalized in {"pilot-week", "pilot-month", "development"}:
        return normalized
    if normalized in {"walk-forward", "walkforward", "wf"}:
        return "walk-forward"
    if normalized == "holdout":
        raise HistoricalCampaignError("HOLDOUT_CLOSED: este runner sólo admite desarrollo/walk-forward")
    raise HistoricalCampaignError(f"stage no soportado para runner local: {value!r}")


def _validate_window(stage: str, start: datetime, end: datetime) -> None:
    if end <= start:
        raise HistoricalCampaignError("end debe ser posterior a start")
    if start >= HOLDOUT_START or end > HOLDOUT_START:
        raise HistoricalCampaignError("HOLDOUT_CLOSED: la ventana reservada no se puede abrir")
    if stage in {"pilot-week", "pilot-month", "development"}:
        if start < DEVELOPMENT_START or end > DEVELOPMENT_END:
            raise HistoricalCampaignError("DEVELOPMENT_WINDOW_ONLY: ventana fuera de 2016–2019")
        return
    if start < WALK_FORWARD_START or end > WALK_FORWARD_END:
        raise HistoricalCampaignError("WALK_FORWARD_WINDOW_ONLY: ventana fuera de 2020–2023")


def _prefix(value: int | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise HistoricalCampaignError("prefix_quotes debe ser entero positivo")
    return value


def _load_protocol(value: ProtocolInput) -> tuple[ResearchProtocol, Path | None]:
    if value is None:
        return ResearchProtocol.default(), None
    if isinstance(value, ResearchProtocol):
        return value, None
    source = _safe_input_file(value, name="protocol")
    try:
        return read_protocol(source), source
    except (MarketProtocolError, OSError, TypeError) as exc:
        raise HistoricalCampaignError(f"protocolo ilegible: {source}") from exc


def _load_manifest(value: ManifestInput) -> tuple[DatasetManifest, Path | None]:
    if isinstance(value, DatasetManifest):
        manifest = value
        source: Path | None = None
    else:
        source = _safe_input_file(value, name="dataset_manifest")
        try:
            loaded = read_manifest(source)
        except (OSError, TypeError, ValueError) as exc:
            raise HistoricalCampaignError(f"dataset manifest ilegible: {source}") from exc
        if not isinstance(loaded, DatasetManifest):
            raise HistoricalCampaignError("dataset_manifest debe ser el contrato HistData")
        manifest = loaded
    if manifest.provider != "histdata" or manifest.instrument != "EUR/USD":
        raise HistoricalCampaignError("sólo se admite el manifest HistData EUR/USD aprobado")
    return manifest, source


def _load_walk_forward_manifest(value: WalkForwardManifestInput) -> WalkForwardManifest:
    """Load the independent WF manifest without scanning its raw archives."""

    if isinstance(value, WalkForwardManifest):
        return value
    source = _safe_input_file(value, name="walk_forward_manifest")
    try:
        return read_walk_forward_manifest(source)
    except (WalkForwardError, OSError, TypeError, ValueError) as exc:
        raise HistoricalCampaignError(f"walk-forward manifest ilegible: {source}") from exc


def _load_walk_forward_input(value: WalkForwardInputValue) -> WalkForwardInput:
    """Load one hash-only WF input sidecar without touching market data."""

    if isinstance(value, WalkForwardInput):
        return value
    if isinstance(value, Mapping):
        raw = _jsonable(dict(value))
    else:
        source = _safe_input_file(value, name="walk_forward_input")
        try:
            raw = json.loads(source.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise HistoricalCampaignError(f"walk-forward input ilegible: {source}") from exc
    if not isinstance(raw, Mapping):
        raise HistoricalCampaignError("walk-forward input debe ser un objeto JSON")
    try:
        return WalkForwardInput.from_mapping(raw)
    except (WalkForwardError, KeyError, TypeError, ValueError) as exc:
        raise HistoricalCampaignError(f"walk-forward input inválido: {exc}") from exc


def bind_walk_forward_contract(
    development_manifest: ManifestInput,
    walk_forward_manifest: WalkForwardManifestInput,
    *,
    input_contract: WalkForwardInputValue,
    window: WalkForwardWindow | str = "WF_2020",
) -> WalkForwardContractBinding:
    """Bind the isolated WF contract and guard as a side-effect-free preflight.

    This function intentionally performs no raw archive scan.  It validates
    only the two manifest objects, the hash-only ``WalkForwardInput`` and the
    causal guard's source identities.  The canonical historical backtest
    still consumes a single ``DatasetManifest`` and therefore cannot safely
    execute this two-manifest warmup→WF stream yet.
    """

    if input_contract is None:
        raise HistoricalCampaignError("walk-forward input es obligatorio")
    selected_development, _ = _load_manifest(development_manifest)
    selected_walk_forward = _load_walk_forward_manifest(walk_forward_manifest)
    selected_input = _load_walk_forward_input(input_contract)
    try:
        guard = WalkForwardStreamGuard(
            selected_development,
            selected_walk_forward,
            window=window,
            input_contract=selected_input,
        )
    except (WalkForwardError, TypeError, ValueError) as exc:
        raise HistoricalCampaignError(f"contrato walk-forward inválido: {exc}") from exc
    return WalkForwardContractBinding(
        selected_development,
        selected_walk_forward,
        selected_input,
        guard.window,
        guard,
    )


def _validation_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        raw = _jsonable(dict(value))
    else:
        to_dict = getattr(value, "to_dict", None)
        raw = to_dict() if callable(to_dict) else value
        raw = _jsonable(raw)
    if not isinstance(raw, Mapping):
        raise HistoricalCampaignError("validación del manifest debe ser mapping")
    return cast(dict[str, Any], dict(raw))


def load_validated_manifest(
    manifest: ManifestInput,
    *,
    start: datetime,
    end: datetime,
    validation: ValidationInput = None,
) -> tuple[DatasetManifest, dict[str, Any], Path | None]:
    """Load a HistData manifest and require a successful matching validation."""

    selected, source = _load_manifest(manifest)
    if validation is None:
        checked = validate_dataset(selected, start, end)
        validation_payload = _validation_mapping(checked)
    elif isinstance(validation, (str, Path)):
        validation_payload = _mapping_value(validation, name="manifest_validation")
    else:
        validation_payload = _validation_mapping(validation)
    if validation_payload.get("ok") is not True:
        raise HistoricalCampaignError("manifest no está validado: la validación no es OK")
    declared_hash = validation_payload.get("content_hash")
    if declared_hash is not None and str(declared_hash) != selected.content_hash:
        raise HistoricalCampaignError("validación y manifest tienen content_hash distinto")
    requested_start = validation_payload.get("requested_start")
    requested_end = validation_payload.get("requested_end")
    if requested_start is not None and _parse_utc(str(requested_start), name="requested_start") != start:
        raise HistoricalCampaignError("validación no corresponde al inicio solicitado")
    if requested_end is not None and _parse_utc(str(requested_end), name="requested_end") != end:
        raise HistoricalCampaignError("validación no corresponde al fin solicitado")
    return selected, validation_payload, source


def _manifest_identity(manifest: DatasetManifest) -> dict[str, Any]:
    content_hash = str(manifest.content_hash).strip()
    dataset_id = str(manifest.dataset_id).strip()
    if not content_hash or not dataset_id:
        raise HistoricalCampaignError("manifest requiere dataset_id y content_hash")
    return {
        "dataset_id": dataset_id,
        "provider": manifest.provider,
        "instrument": manifest.instrument,
        "content_hash": content_hash,
        "coverage_start": instant_text(manifest.coverage_start) if manifest.coverage_start else None,
        "coverage_end": instant_text(manifest.coverage_end) if manifest.coverage_end else None,
        "schema_version": manifest.schema_version,
        "raw_partitions": [
            {
                "raw_archive": item.raw_archive,
                "raw_sha256": item.raw_sha256,
                "raw_size": item.raw_size,
                "members": list(item.members),
            }
            for item in manifest.partitions
        ],
    }


def _candidate_ids(protocol: ResearchProtocol, candidates: Sequence[str] | None) -> tuple[str, ...]:
    selected = protocol.candidate_ids if candidates is None else tuple(str(item).strip() for item in candidates)
    if not selected or len(set(selected)) != len(selected):
        raise HistoricalCampaignError("candidates debe contener IDs únicos no vacíos")
    if any(item not in protocol.candidate_ids for item in selected):
        raise HistoricalCampaignError("candidates contiene un ID no registrado")
    return selected


def _snapshot_code(source_root: Path, destination: Path) -> dict[str, Any]:
    root = _absolute(source_root, name="code_root")
    if root.is_symlink() or not root.is_dir():
        raise HistoricalCampaignError("code_root debe ser un directorio regular")
    _mkdir_private(destination)
    source_files: list[Path] = []
    for directory_name in ("mtf_lab", "tools"):
        directory = root / directory_name
        if not directory.is_dir() or directory.is_symlink():
            continue
        for candidate in directory.rglob("*"):
            if candidate.is_symlink() or not candidate.is_file():
                continue
            if candidate.suffix in _CODE_SUFFIXES and "__pycache__" not in candidate.parts:
                source_files.append(candidate)
    if not source_files:
        raise HistoricalCampaignError("code_root no contiene fuentes Python para preservar")
    entries: dict[str, dict[str, Any]] = {}
    for source in sorted(source_files, key=lambda item: item.relative_to(root).as_posix()):
        relative = source.relative_to(root)
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        _check_ancestors(target, name="code snapshot", allow_missing=False)
        digest, size = _copy_exact(source, target)
        entries[relative.as_posix()] = {"sha256": digest, "bytes": size}
    material = {"source_root": str(root), "files": entries}
    code_hash = _hash_mapping(material)
    manifest_path = destination / "code-manifest.json"
    _write_json(manifest_path, {**material, "code_hash": code_hash})
    return {
        "path": str(destination),
        "manifest_path": str(manifest_path),
        "source_root": str(root),
        "code_hash": code_hash,
        "file_count": len(entries),
        "files": entries,
    }


def _verify_code_snapshot(snapshot: Mapping[str, Any]) -> bool:  # noqa: C901 - immutable code identity scan
    root = Path(str(snapshot.get("source_root", "")))
    files = snapshot.get("files")
    if root.is_symlink() or not root.is_dir() or not isinstance(files, Mapping):
        return False
    try:
        _check_ancestors(root / "mtf_lab" / "placeholder.py", name="code snapshot root", allow_missing=True)
    except HistoricalCampaignError:
        return False
    expected_paths: set[str] = set()
    for relative, metadata in files.items():
        relative_path = Path(relative) if isinstance(relative, str) else None
        if (
            relative_path is None
            or relative_path.is_absolute()
            or ".." in relative_path.parts
            or not isinstance(metadata, Mapping)
        ):
            return False
        source = root / relative
        try:
            _check_ancestors(source, name="code snapshot source", allow_missing=False)
            info = os.lstat(source)
        except (HistoricalCampaignError, OSError):
            return False
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            return False
        expected_bytes = metadata.get("bytes")
        expected_sha = metadata.get("sha256")
        if (
            isinstance(expected_bytes, bool)
            or not isinstance(expected_bytes, int)
            or expected_bytes < 0
            or not isinstance(expected_sha, str)
        ):
            return False
        if info.st_size != expected_bytes:
            return False
        digest = _file_sha256(source)
        if digest != expected_sha:
            return False
        expected_paths.add(relative_path.as_posix())
    actual_paths: set[str] = set()
    for directory_name in ("mtf_lab", "tools"):
        directory = root / directory_name
        if directory.is_symlink() or not directory.is_dir():
            continue
        for candidate in directory.rglob("*"):
            if candidate.is_symlink() or not candidate.is_file() or "__pycache__" in candidate.parts:
                continue
            if candidate.suffix in _CODE_SUFFIXES:
                actual_paths.add(candidate.relative_to(root).as_posix())
    return actual_paths == expected_paths


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        while chunk := os.read(fd, 1024 * 1024):
            digest.update(chunk)
    finally:
        os.close(fd)
    return digest.hexdigest()


def _snapshot_inputs(
    destination: Path,
    *,
    manifest: DatasetManifest,
    manifest_source: Path | None,
    protocol: ResearchProtocol,
    protocol_source: Path | None,
    validation: Mapping[str, Any],
    request: Mapping[str, Any],
) -> dict[str, Any]:
    _mkdir_private(destination)
    files: dict[str, dict[str, Any]] = {}
    if manifest_source is not None:
        digest, size = _copy_exact(manifest_source, destination / "dataset-manifest.original.json")
    else:
        digest, size = _write_json(destination / "dataset-manifest.json", manifest.to_dict())
    files["dataset-manifest"] = {
        "path": str(
            destination / "dataset-manifest.original.json" if manifest_source else destination / "dataset-manifest.json"
        ),
        "sha256": digest,
        "bytes": size,
    }
    if protocol_source is not None:
        digest, size = _copy_exact(protocol_source, destination / "protocol.original.json")
    else:
        digest, size = _write_json(destination / "protocol.json", protocol.to_dict())
    files["protocol"] = {
        "path": str(destination / "protocol.original.json" if protocol_source else destination / "protocol.json"),
        "sha256": digest,
        "bytes": size,
    }
    digest, size = _write_json(destination / "validation.json", validation)
    files["validation"] = {"path": str(destination / "validation.json"), "sha256": digest, "bytes": size}
    digest, size = _write_json(destination / "request.json", request)
    files["request"] = {"path": str(destination / "request.json"), "sha256": digest, "bytes": size}
    return {
        "path": str(destination),
        "files": files,
        "input_hash": _hash_mapping({name: item["sha256"] for name, item in files.items()}),
        "raw_bytes_copied": False,
        "raw_identity_preserved_by_manifest": True,
    }


def _new_run_directory(output_root: Path, run_id: str | None) -> tuple[str, Path]:
    selected = _validate_run_id(run_id, name="run_id") if run_id is not None else uuid.uuid4().hex
    target = output_root / selected
    try:
        os.mkdir(target, 0o700)
    except FileExistsError as exc:
        raise HistoricalCampaignError(f"run_id ya existe: {selected}") from exc
    _mkdir_private(target)
    _fsync_directory(output_root)
    return selected, target


@dataclass(frozen=True, slots=True)
class _ResumeContext:
    """Durable state recovered from one prior campaign run directory."""

    run_id: str
    run_directory: Path
    checkpoint: HistoricalBacktestCheckpoint
    checkpoint_path: Path
    request: dict[str, Any]
    validation: dict[str, Any]
    input_snapshot: dict[str, Any]
    manifest_path: Path
    protocol_path: Path
    code_snapshot: dict[str, Any]
    receipt: dict[str, Any] | None
    receipt_path: Path | None


def _checkpoint_from_mapping(value: Mapping[str, Any], *, name: str) -> HistoricalBacktestCheckpoint:
    try:
        checkpoint = HistoricalBacktestCheckpoint.from_mapping(value)
    except (HistoricalBacktestError, KeyError, TypeError, ValueError, OverflowError) as exc:
        raise HistoricalCampaignError(f"{name} incompatible") from exc
    if checkpoint.finished or checkpoint.status != "CHECKPOINTED":
        raise HistoricalCampaignError(f"{name} debe ser un checkpoint parcial continuable")
    if checkpoint.processed_quotes <= 0:
        raise HistoricalCampaignError(f"{name} requiere processed_quotes positivo")
    return checkpoint


def _checkpoint_input(value: ResumeInput, *, name: str) -> tuple[HistoricalBacktestCheckpoint | None, Path | None]:
    if isinstance(value, HistoricalBacktestCheckpoint):
        return value, None
    if isinstance(value, Mapping):
        return _checkpoint_from_mapping(value, name=name), None
    source = _safe_input_file(value, name=name)
    raw, _ = _read_json_mapping(source, name=name)
    return _checkpoint_from_mapping(raw, name=name), source


def _choose_resume_file(directory: Path, names: Sequence[str], *, name: str) -> Path:
    for filename in names:
        candidate = directory / filename
        try:
            os.lstat(candidate)
        except FileNotFoundError:
            continue
        return _private_artifact(candidate, name=name)
    raise HistoricalCampaignError(f"falta {name} en el run_directory")


def _resume_run_paths(
    output_root: Path,
    *,
    checkpoint_input: ResumeInput | None,
    run_selector: str | None,
) -> tuple[str, Path, Path, HistoricalBacktestCheckpoint | None]:
    """Resolve a checkpoint and its owning run directory without creating files."""

    direct_checkpoint: HistoricalBacktestCheckpoint | None = None
    checkpoint_path: Path
    if checkpoint_input is not None and isinstance(checkpoint_input, (str, Path)):
        checkpoint_path = _safe_input_file(checkpoint_input, name="resume_checkpoint")
        if checkpoint_path.name != "checkpoint.json" or checkpoint_path.parent.name != "backtest":
            raise HistoricalCampaignError("resume_checkpoint debe ser backtest/checkpoint.json")
        run_directory = checkpoint_path.parent.parent
        if run_selector is not None and _validate_run_id(run_selector, name="resume_run_id") != run_directory.name:
            raise HistoricalCampaignError("resume_run_id no coincide con resume_checkpoint")
        selected_id = _validate_run_id(run_directory.name, name="resume_run_id")
    else:
        if run_selector is None:
            raise HistoricalCampaignError("resume requiere resume_checkpoint o resume_run_id")
        selected_id = _validate_run_id(run_selector, name="resume_run_id")
        run_directory = output_root / selected_id
        checkpoint_path = run_directory / "backtest" / "checkpoint.json"
        _safe_input_file(checkpoint_path, name="resume_checkpoint")
        if checkpoint_input is not None:
            direct_checkpoint, _ = _checkpoint_input(checkpoint_input, name="resume_checkpoint")

    try:
        relative = run_directory.relative_to(output_root)
    except ValueError as exc:
        raise HistoricalCampaignError("resume run_directory debe pertenecer a output_dir") from exc
    if len(relative.parts) != 1 or relative.parts[0] != selected_id:
        raise HistoricalCampaignError("resume run_directory inválido")
    _safe_existing_directory(run_directory, name="run_directory")
    _safe_existing_directory(run_directory / "backtest", name="backtest")
    checkpoint_path = _private_artifact(checkpoint_path, name="resume_checkpoint")
    raw, _ = _read_json_mapping(checkpoint_path, name="resume_checkpoint")
    checkpoint = _checkpoint_from_mapping(raw, name="resume_checkpoint")
    if direct_checkpoint is not None and canonical_json(_jsonable(direct_checkpoint.to_dict())) != canonical_json(
        _jsonable(checkpoint.to_dict())
    ):
        raise HistoricalCampaignError("resume_checkpoint no coincide con el checkpoint durable")
    return selected_id, run_directory, checkpoint_path, direct_checkpoint or checkpoint


def _code_snapshot_from_manifest(code_directory: Path) -> dict[str, Any]:  # noqa: C901 - one guarded loader
    manifest_path = _private_artifact(code_directory / "code-manifest.json", name="code-manifest")
    raw, _ = _read_json_mapping(manifest_path, name="code-manifest")
    source_root_value = raw.get("source_root")
    files = raw.get("files")
    code_hash = raw.get("code_hash")
    if not isinstance(source_root_value, str) or not source_root_value.strip():
        raise HistoricalCampaignError("code-manifest source_root inválido")
    if not isinstance(files, Mapping) or not isinstance(code_hash, str):
        raise HistoricalCampaignError("code-manifest incompleto")
    material = {"source_root": source_root_value, "files": dict(files)}
    if code_hash != _hash_mapping(material):
        raise HistoricalCampaignError("code-manifest code_hash inválido")
    snapshot: dict[str, Any] = {
        "path": str(code_directory),
        "manifest_path": str(manifest_path),
        "source_root": source_root_value,
        "code_hash": code_hash,
        "file_count": len(files),
        "files": dict(files),
    }
    if not _verify_code_snapshot(snapshot):
        raise HistoricalCampaignError("código fuente no coincide con el snapshot durable")
    for relative, metadata in files.items():
        if not isinstance(relative, str) or Path(relative).is_absolute() or ".." in Path(relative).parts:
            raise HistoricalCampaignError("code-manifest contiene una ruta insegura")
        if not isinstance(metadata, Mapping):
            raise HistoricalCampaignError("code-manifest contiene metadata inválida")
        expected_bytes = metadata.get("bytes")
        expected_sha = metadata.get("sha256")
        if (
            isinstance(expected_bytes, bool)
            or not isinstance(expected_bytes, int)
            or expected_bytes < 0
            or not isinstance(expected_sha, str)
        ):
            raise HistoricalCampaignError("code-manifest contiene metadata inválida")
        copied = _safe_input_file(code_directory / relative, name="code snapshot")
        try:
            copied_size = copied.stat().st_size
        except OSError as exc:
            raise HistoricalCampaignError("code snapshot ilegible") from exc
        if copied_size != expected_bytes or _file_sha256(copied) != expected_sha:
            raise HistoricalCampaignError("code snapshot alterado")
    return snapshot


def _resume_input_snapshot(
    run_directory: Path,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    Path,
    Path,
]:
    inputs_directory = _safe_existing_directory(run_directory / "inputs", name="inputs")
    manifest_path = _choose_resume_file(
        inputs_directory,
        ("dataset-manifest.original.json", "dataset-manifest.json"),
        name="dataset manifest snapshot",
    )
    protocol_path = _choose_resume_file(
        inputs_directory,
        ("protocol.original.json", "protocol.json"),
        name="protocol snapshot",
    )
    validation_path = _private_artifact(inputs_directory / "validation.json", name="manifest validation")
    request_path = _private_artifact(inputs_directory / "request.json", name="campaign request")
    validation, _ = _read_json_mapping(validation_path, name="manifest validation")
    request, _ = _read_json_mapping(request_path, name="campaign request")
    files: dict[str, dict[str, Any]] = {}
    for key, path in (
        ("dataset-manifest", manifest_path),
        ("protocol", protocol_path),
        ("validation", validation_path),
        ("request", request_path),
    ):
        info = os.lstat(path)
        files[key] = {"path": str(path), "sha256": _file_sha256(path), "bytes": int(info.st_size)}
    snapshot = {
        "path": str(inputs_directory),
        "files": files,
        "input_hash": _hash_mapping({key: value["sha256"] for key, value in files.items()}),
        "raw_bytes_copied": False,
        "raw_identity_preserved_by_manifest": True,
    }
    return snapshot, request, validation, manifest_path, protocol_path


def _load_resume_context(
    output_root: Path,
    *,
    checkpoint_input: ResumeInput | None,
    run_selector: str | None,
) -> _ResumeContext:
    run_id, run_directory, checkpoint_path, checkpoint = _resume_run_paths(
        output_root,
        checkpoint_input=checkpoint_input,
        run_selector=run_selector,
    )
    if checkpoint is None:
        raise HistoricalCampaignError("resume checkpoint faltante")
    input_snapshot, request, validation, manifest_path, protocol_path = _resume_input_snapshot(run_directory)
    code_directory = _safe_existing_directory(run_directory / "code", name="code")
    code_snapshot = _code_snapshot_from_manifest(code_directory)
    receipt, receipt_path = _optional_json_mapping(run_directory / "receipt.json", name="receipt")
    return _ResumeContext(
        run_id,
        run_directory,
        checkpoint,
        checkpoint_path,
        request,
        validation,
        input_snapshot,
        manifest_path,
        protocol_path,
        code_snapshot,
        receipt,
        receipt_path,
    )


def _same_json(left: Any, right: Any) -> bool:
    return canonical_json(_jsonable(left)) == canonical_json(_jsonable(right))


def _partial_controls(
    stop_after_quotes: int | None,
    checkpoint_after_quotes: int | None,
    checkpoint: HistoricalBacktestCheckpoint | None,
) -> tuple[int | None, int | None]:
    if (
        stop_after_quotes is not None
        and checkpoint_after_quotes is not None
        and stop_after_quotes != checkpoint_after_quotes
    ):
        raise HistoricalCampaignError("stop_after_quotes y checkpoint_after_quotes no coinciden")
    selected = stop_after_quotes if stop_after_quotes is not None else checkpoint_after_quotes
    if selected is not None:
        if isinstance(selected, bool) or not isinstance(selected, int) or selected <= 0:
            raise HistoricalCampaignError("stop_after_quotes debe ser entero positivo")
        if checkpoint is not None and selected <= checkpoint.processed_quotes:
            raise HistoricalCampaignError("stop_after_quotes debe avanzar el checkpoint")
    return stop_after_quotes, checkpoint_after_quotes


def _stored_prefix(request: Mapping[str, Any]) -> int | None:
    value = request.get("prefix_quotes")
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise HistoricalCampaignError("resume request prefix_quotes inválido")
    return _prefix(value)


def _validate_resume_offsets(context: _ResumeContext) -> None:
    offsets = dict(context.checkpoint.artifact_offsets)
    if set(offsets) != _ARTIFACT_KINDS:
        raise HistoricalCampaignError("resume artifact offsets incompletos")
    backtest_directory = context.run_directory / "backtest"
    for kind in sorted(_ARTIFACT_KINDS):
        offset = offsets[kind]
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
            raise HistoricalCampaignError(f"resume artifact offset inválido: {kind}")
        path = _private_artifact(backtest_directory / f"{kind}.jsonl", name=f"resume artifact {kind}")
        try:
            info = os.lstat(path)
        except OSError as exc:
            raise HistoricalCampaignError(f"resume artifact ilegible: {kind}") from exc
        if info.st_size < offset:
            raise HistoricalCampaignError(f"resume artifact offset excede el archivo: {kind}")
        if offset:
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
            try:
                if os.pread(fd, 1, offset - 1) != b"\n":
                    raise HistoricalCampaignError(f"resume artifact offset no termina una fila: {kind}")
            finally:
                os.close(fd)


def _source_digest(value: Mapping[str, Any]) -> str:
    return _hash_bytes((canonical_json(_jsonable(value)) + "\n").encode("utf-8"))


def _validate_resume_input_identity(  # noqa: C901 - one immutable resume identity gate
    context: _ResumeContext,
    *,
    selected_manifest: DatasetManifest,
    manifest_source: Path | None,
    selected_protocol: ResearchProtocol,
    protocol_source: Path | None,
    validation: Mapping[str, Any],
    request: Mapping[str, Any],
    config: HistoricalBacktestConfig,
    runtime_identity: Mapping[str, Any],
    code_snapshot: Mapping[str, Any],
) -> None:
    checkpoint = context.checkpoint
    if checkpoint.dataset_id != selected_manifest.dataset_id or checkpoint.data_hash != selected_manifest.content_hash:
        raise HistoricalCampaignError("resume manifest identity mismatch")
    if checkpoint.config_hash != config.config_hash:
        raise HistoricalCampaignError("resume config identity mismatch")
    if checkpoint.protocol_hash != selected_protocol.protocol_hash:
        raise HistoricalCampaignError("resume protocolo identity mismatch")
    if checkpoint.scenario != config.scenario or checkpoint.candidate_ids != config.candidate_ids:
        raise HistoricalCampaignError("resume config candidates/scenario mismatch")

    stored_request = context.request
    if stored_request.get("schema") != RUNNER_SCHEMA:
        raise HistoricalCampaignError("resume request schema incompatible")
    request_keys = (
        "stage",
        "window",
        "prefix_quotes",
        "scenario",
        "candidate_ids",
        "runtime_identity",
        "contract_spec",
        "calendar_state",
        "dataset",
        "protocol_hash",
        "config_hash",
    )
    for key in request_keys:
        if key not in stored_request or not _same_json(stored_request[key], request.get(key)):
            label = "runtime" if key == "runtime_identity" else key
            raise HistoricalCampaignError(f"resume {label} mismatch")
    stored_code_hash = stored_request.get("code_hash")
    if stored_code_hash is not None and stored_code_hash != code_snapshot.get("code_hash"):
        raise HistoricalCampaignError("resume código identity mismatch")
    if not _same_json(stored_request.get("runtime_identity"), runtime_identity):
        raise HistoricalCampaignError("resume runtime identity mismatch")

    try:
        stored_manifest = read_manifest(context.manifest_path)
    except (OSError, TypeError, ValueError) as exc:
        raise HistoricalCampaignError("resume manifest snapshot ilegible") from exc
    if not isinstance(stored_manifest, DatasetManifest) or not _same_json(
        _manifest_identity(stored_manifest), _manifest_identity(selected_manifest)
    ):
        raise HistoricalCampaignError("resume manifest snapshot mismatch")
    stored_manifest_digest = _file_sha256(context.manifest_path)
    if manifest_source is not None:
        current_manifest_path = _safe_input_file(manifest_source, name="dataset_manifest")
        if _file_sha256(current_manifest_path) != stored_manifest_digest:
            raise HistoricalCampaignError("resume manifest bytes changed")
    elif stored_manifest_digest != _source_digest(selected_manifest.to_dict()):
        raise HistoricalCampaignError("resume manifest object does not match snapshot")

    try:
        stored_protocol = read_protocol(context.protocol_path)
    except (MarketProtocolError, OSError, TypeError, ValueError) as exc:
        raise HistoricalCampaignError("resume protocolo snapshot ilegible") from exc
    if stored_protocol.protocol_hash != selected_protocol.protocol_hash:
        raise HistoricalCampaignError("resume protocolo snapshot mismatch")
    stored_protocol_digest = _file_sha256(context.protocol_path)
    if protocol_source is not None:
        current_protocol_path = _safe_input_file(protocol_source, name="protocol")
        if _file_sha256(current_protocol_path) != stored_protocol_digest:
            raise HistoricalCampaignError("resume protocolo bytes changed")
    elif stored_protocol_digest != _source_digest(selected_protocol.to_dict()):
        raise HistoricalCampaignError("resume protocolo object does not match snapshot")

    if not _same_json(context.validation, validation):
        raise HistoricalCampaignError("resume manifest validation mismatch")
    raw_files = context.input_snapshot.get("files")
    if not isinstance(raw_files, Mapping):
        raise HistoricalCampaignError("resume input snapshot incompleto")
    if set(raw_files) != {"dataset-manifest", "protocol", "validation", "request"}:
        raise HistoricalCampaignError("resume input snapshot incompleto")
    input_hash_material: dict[str, str] = {}
    for key, value in raw_files.items():
        if not isinstance(key, str) or not isinstance(value, Mapping) or not isinstance(value.get("sha256"), str):
            raise HistoricalCampaignError("resume input snapshot incompleto")
        input_hash_material[key] = str(value["sha256"])
    if context.input_snapshot.get("input_hash") != _hash_mapping(input_hash_material):
        raise HistoricalCampaignError("resume input snapshot hash inválido")
    if not _verify_code_snapshot(code_snapshot):
        raise HistoricalCampaignError("resume código fuente cambió")

    if context.receipt is not None:
        receipt = context.receipt
        for key, expected in (
            ("dataset", _manifest_identity(selected_manifest)),
            ("protocol_hash", selected_protocol.protocol_hash),
            ("config_hash", config.config_hash),
        ):
            if key in receipt and not _same_json(receipt[key], expected):
                raise HistoricalCampaignError(f"resume receipt {key} mismatch")
        receipt_config = receipt.get("config_snapshot")
        if receipt_config is not None and not _same_json(receipt_config, config.to_dict()):
            raise HistoricalCampaignError("resume receipt config mismatch")
        receipt_runtime = receipt.get("runtime_identity")
        if receipt_runtime is not None and not _same_json(receipt_runtime, runtime_identity):
            raise HistoricalCampaignError("resume receipt runtime mismatch")
        receipt_code = receipt.get("code_snapshot")
        if receipt_code is not None and (
            not isinstance(receipt_code, Mapping) or receipt_code.get("code_hash") != code_snapshot.get("code_hash")
        ):
            raise HistoricalCampaignError("resume receipt code mismatch")
        receipt_inputs = receipt.get("input_snapshot")
        if receipt_inputs is not None and (
            not isinstance(receipt_inputs, Mapping)
            or receipt_inputs.get("input_hash") != context.input_snapshot.get("input_hash")
        ):
            raise HistoricalCampaignError("resume receipt inputs mismatch")
    _validate_resume_offsets(context)


def _resume_attempt_ids(  # noqa: C901 - one registry identity gate
    context: _ResumeContext,
    registry: GlobalTrialRegistry,
    *,
    candidate_ids: Sequence[str],
    protocol: ResearchProtocol,
    manifest: DatasetManifest,
    runtime_identity: Mapping[str, Any],
    config_hash: str,
    code_hash: str,
    input_hash: str,
) -> tuple[str, ...]:
    try:
        registry_validation = registry.validate()
        records = registry.records()
    except (RegistryError, OSError, TypeError, ValueError) as exc:
        raise HistoricalCampaignError("resume registry ilegible") from exc
    if registry_validation.get("ok") is not True:
        raise HistoricalCampaignError("resume registry inválido")
    receipt_ids: tuple[str, ...] | None = None
    if context.receipt is not None:
        raw_registry = context.receipt.get("registry")
        if isinstance(raw_registry, Mapping):
            raw_path = raw_registry.get("path")
            if raw_path is not None and _absolute(raw_path, name="registry") != registry.path:
                raise HistoricalCampaignError("resume registry path mismatch")
            raw_ids = raw_registry.get("attempt_ids")
            if raw_ids is not None:
                if not isinstance(raw_ids, list) or any(not isinstance(item, str) or not item for item in raw_ids):
                    raise HistoricalCampaignError("resume attempt_ids inválidos")
                receipt_ids = tuple(raw_ids)
    registrations = [
        record
        for record in records
        if record.get("event") == "ATTEMPT_REGISTERED"
        and isinstance(record.get("scope"), Mapping)
        and cast(Mapping[str, Any], record["scope"]).get("run_id") == context.run_id
    ]
    if receipt_ids is not None:
        if len(receipt_ids) != len(candidate_ids) or len(set(receipt_ids)) != len(receipt_ids):
            raise HistoricalCampaignError("resume attempt_ids no coinciden con candidates")
        selected_records = [record for record in registrations if record.get("attempt_id") in receipt_ids]
        if len(selected_records) != len(receipt_ids):
            raise HistoricalCampaignError("resume intento registrado faltante")
    else:
        selected_records = registrations
    by_candidate: dict[str, list[Mapping[str, Any]]] = {candidate: [] for candidate in candidate_ids}
    for record in selected_records:
        candidate = str(record.get("candidate_id", ""))
        if candidate in by_candidate:
            by_candidate[candidate].append(record)
    if any(len(items) != 1 for items in by_candidate.values()) or len(selected_records) != len(candidate_ids):
        raise HistoricalCampaignError("resume requiere exactamente un intento registrado por candidato")
    for candidate in candidate_ids:
        matched_record = by_candidate[candidate][0]
        if (
            matched_record.get("protocol_hash") != protocol.protocol_hash
            or matched_record.get("dataset_hash") != manifest.content_hash
            or not _same_json(matched_record.get("runtime_identity"), runtime_identity)
        ):
            raise HistoricalCampaignError(f"resume registry identity mismatch: {candidate}")
        parameters = matched_record.get("parameters")
        if isinstance(parameters, Mapping):
            for key, expected in (("config_hash", config_hash), ("code_hash", code_hash), ("input_hash", input_hash)):
                if key in parameters and parameters.get(key) != expected:
                    raise HistoricalCampaignError(f"resume registry {key} mismatch: {candidate}")
    return tuple(str(by_candidate[candidate][0]["attempt_id"]) for candidate in candidate_ids)


def _receipt_destination(run_directory: Path, *, resumed: bool, failure: bool = False) -> Path:
    if not resumed:
        return run_directory / "receipt.json"
    prefix = "resume-failure" if failure else "resume-receipt"
    return run_directory / f"{prefix}-{uuid.uuid4().hex}.json"


def _result_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        mapped = _jsonable(dict(value))
    else:
        to_dict = getattr(value, "to_dict", None)
        mapped = _jsonable(to_dict()) if callable(to_dict) else None
    if not isinstance(mapped, Mapping):
        raise HistoricalCampaignError("run_historical_backtest devolvió un resultado no serializable")
    return cast(dict[str, Any], dict(mapped))


def _discard_sink(_kind: str, _row: Mapping[str, Any]) -> None:
    return None


def _invoke_runner(
    runner: Runner,
    stream: Iterator[HistoricalQuote],
    *,
    manifest: DatasetManifest,
    config: HistoricalBacktestConfig,
    output_dir: Path,
    resume: Any,
    stop_after_quotes: int | None = None,
    checkpoint_after_quotes: int | None = None,
) -> Any:
    """Invoke the canonical runner while keeping a testable adapter seam."""

    kwargs: dict[str, Any] = {
        "manifest": manifest,
        "config": config,
        "output_dir": output_dir,
        "sink": _discard_sink,
        "resume": resume,
        "quotes": stream,
        "quotes_or_factory": stream,
    }
    if stop_after_quotes is not None:
        kwargs["stop_after_quotes"] = stop_after_quotes
    if checkpoint_after_quotes is not None:
        kwargs["checkpoint_after_quotes"] = checkpoint_after_quotes
    try:
        parameters = inspect.signature(runner).parameters
    except (TypeError, ValueError):
        fallback_kwargs: dict[str, Any] = {
            "manifest": manifest,
            "config": config,
            "output_dir": output_dir,
            "sink": _discard_sink,
            "resume": resume,
        }
        if stop_after_quotes is not None:
            fallback_kwargs["stop_after_quotes"] = stop_after_quotes
        if checkpoint_after_quotes is not None:
            fallback_kwargs["checkpoint_after_quotes"] = checkpoint_after_quotes
        return runner(stream, **fallback_kwargs)
    if any(parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in parameters.values()):
        positional = [
            parameter
            for parameter in parameters.values()
            if parameter.kind in {inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD}
        ]
        if positional:
            first_name = positional[0].name
            if first_name in {"quotes", "quotes_or_factory"}:
                kwargs.pop("quotes", None)
                kwargs.pop("quotes_or_factory", None)
            return runner(stream, **kwargs)
        return runner(**kwargs)
    accepted = {
        name: item
        for name, item in kwargs.items()
        if name in parameters
        and parameters[name].kind in {inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY}
    }
    if "quotes" in accepted or "quotes_or_factory" in accepted:
        return runner(**accepted)
    return runner(stream, **accepted)


def _attempt_parameters(
    *,
    stage: str,
    start: datetime,
    end: datetime,
    prefix_quotes: int | None,
    scenario: str,
    candidate_ids: Sequence[str],
    config_hash: str,
    code_hash: str,
    input_hash: str,
) -> dict[str, Any]:
    return {
        "stage": stage,
        "window_start": instant_text(start),
        "window_end": instant_text(end),
        "prefix_quotes": prefix_quotes,
        "scenario": scenario,
        "candidate_ids": list(candidate_ids),
        "config_hash": config_hash,
        "code_hash": code_hash,
        "input_hash": input_hash,
        "selection_performed": False,
        "promotion_performed": False,
    }


def _base_receipt(
    *,
    run_id: str,
    stage: str,
    start: datetime,
    end: datetime,
    prefix_quotes: int | None,
    manifest: Mapping[str, Any],
    protocol: ResearchProtocol,
    config: HistoricalBacktestConfig,
    validation: Mapping[str, Any],
    code_snapshot: Mapping[str, Any],
    input_snapshot: Mapping[str, Any],
    attempt_ids: Sequence[str],
    registry_path: Path,
    runtime_identity: Mapping[str, Any],
    resumed: bool = False,
    resume_checkpoint: Path | None = None,
    previous_receipt: Path | None = None,
) -> dict[str, Any]:
    receipt: dict[str, Any] = {
        "schema": RUNNER_SCHEMA,
        "operation": "RUN_HISTORICAL_CAMPAIGN",
        "run_id": run_id,
        "status": "RUNNING",
        "stage": stage,
        "window": {"start": instant_text(start), "end": instant_text(end), "half_open": True},
        "prefix_quotes": prefix_quotes,
        "dataset": dict(manifest),
        "protocol_hash": protocol.protocol_hash,
        "config_hash": config.config_hash,
        "scenario": config.scenario,
        "candidate_ids": list(config.candidate_ids),
        "validation": dict(validation),
        "runtime_identity": dict(runtime_identity),
        "config_snapshot": config.to_dict(),
        "code_snapshot": dict(code_snapshot),
        "input_snapshot": dict(input_snapshot),
        "registry": {"path": str(registry_path), "attempt_ids": list(attempt_ids)},
        "network_performed": False,
        "data_acquisition": False,
        "database_writes": False,
        "holdout": "CLOSED",
        "selection_performed": False,
        "promotion_performed": False,
        "auto_promote": False,
        "promotable": False,
        "trading_enabled": False,
        "economic_conclusion": "NOT_ASSESSED",
        "profitability_claim": "NONE",
        "fills": "MODELLED_LOCAL_ONLY",
    }
    receipt["resume"] = {
        "requested": resumed,
        "checkpoint_path": str(resume_checkpoint) if resume_checkpoint is not None else None,
        "previous_receipt": str(previous_receipt) if previous_receipt is not None else None,
    }
    return receipt


def run_historical_campaign(  # noqa: C901 - one guarded orchestration boundary
    manifest: ManifestInput,
    *,
    registry: RegistryInput,
    output_dir: str | Path,
    start: datetime | str | None,
    end: datetime | str | None,
    protocol: ProtocolInput = None,
    stage: str = "development",
    walk_forward_manifest: WalkForwardManifestInput | None = None,
    walk_forward_input: WalkForwardInputValue | None = None,
    window: WalkForwardWindow | str = "WF_2020",
    scenario: str = "base",
    candidates: Sequence[str] | None = None,
    prefix_quotes: int | None = None,
    validation: ValidationInput = None,
    runtime_identity: Mapping[str, Any] | None = None,
    contract_spec: Mapping[str, Any] | None = None,
    calendar_state: Mapping[str, Any] | None = None,
    code_root: str | Path | None = None,
    source_root: str | Path | None = None,
    runner: Runner | None = None,
    resume: Any = None,
    run_id: str | None = None,
    stop_after_quotes: int | None = None,
    checkpoint_after_quotes: int | None = None,
    resume_checkpoint: ResumeInput | None = None,
    resume_run_id: str | None = None,
) -> dict[str, Any]:
    """Execute one registered, review-only historical development run.

    ``start`` and ``end`` are mandatory explicit UTC bounds.  ``prefix_quotes``
    optionally stops the streamed window after that many quotes; it never
    expands a window.  A holdout request fails before reading/validating a
    manifest or touching the registry.
    """

    selected_stage = _stage(stage)
    selected_start = _parse_utc(start, name="start")
    selected_end = _parse_utc(end, name="end")
    _validate_window(selected_stage, selected_start, selected_end)
    if selected_stage == "walk-forward":
        if walk_forward_manifest is None or walk_forward_input is None:
            raise HistoricalCampaignError(
                "WALK_FORWARD_CLOSED: requiere WalkForwardManifest y WalkForwardInput explícitos"
            )
        binding = bind_walk_forward_contract(
            manifest,
            walk_forward_manifest,
            input_contract=walk_forward_input,
            window=window,
        )
        if selected_start != binding.window.test_start or selected_end != binding.window.test_end:
            raise HistoricalCampaignError(
                "WALK_FORWARD_WINDOW_ONLY: start/end no coinciden con la ventana anual exacta"
            )
        raise HistoricalCampaignError(
            "WALK_FORWARD_CLOSED: el runner canónico aún no consume el flujo WARMUP_ONLY→WF; no se abrió WF/holdout"
        )
    if walk_forward_manifest is not None or walk_forward_input is not None:
        raise HistoricalCampaignError("walk-forward contract sólo aplica con stage=walk-forward")
    requested_prefix = _prefix(prefix_quotes)

    # ``resume`` predates the two explicit CLI selectors.  Keep it as a
    # programmatic compatibility seam, but normalize every form to one
    # checkpoint/run-directory pair before touching the source stream.
    if resume_checkpoint is not None and resume_run_id is not None:
        raise HistoricalCampaignError("resume_checkpoint y resume_run_id son mutuamente excluyentes")
    if resume is not None and resume_checkpoint is not None:
        raise HistoricalCampaignError("resume y resume_checkpoint son mutuamente excluyentes")
    normalized_checkpoint: ResumeInput | None = resume_checkpoint
    normalized_run_id = resume_run_id
    if resume is not None:
        normalized_checkpoint = cast(ResumeInput, resume)
        if normalized_run_id is None and run_id is not None and not isinstance(resume, (str, Path)):
            normalized_run_id = run_id
            run_id = None
        elif isinstance(resume, (str, Path)) and normalized_run_id is not None:
            raise HistoricalCampaignError("resume checkpoint y resume_run_id son mutuamente excluyentes")
        elif run_id is not None:
            raise HistoricalCampaignError("resume no admite un run_id adicional")
    legacy_direct_resume = resume is not None and not isinstance(resume, (str, Path))
    if normalized_checkpoint is not None and normalized_run_id is not None and not legacy_direct_resume:
        raise HistoricalCampaignError("resume_checkpoint y resume_run_id son mutuamente excluyentes")
    if normalized_checkpoint is not None and run_id is not None:
        raise HistoricalCampaignError("resume_checkpoint no admite --run-id")
    resume_requested = normalized_checkpoint is not None or normalized_run_id is not None

    if resume_requested:
        output_root = _safe_existing_directory(output_dir, name="output_dir")
        resume_context = _load_resume_context(
            output_root,
            checkpoint_input=normalized_checkpoint,
            run_selector=normalized_run_id,
        )
    else:
        output_root = _safe_output_dir(output_dir)
        resume_context = None

    selected_protocol, protocol_source = _load_protocol(protocol)
    selected_candidates = _candidate_ids(selected_protocol, candidates)
    selected_manifest, validation_payload, manifest_source = load_validated_manifest(
        manifest,
        start=selected_start,
        end=selected_end,
        validation=validation,
    )
    selected_runtime = _runtime_identity(runtime_identity)
    if contract_spec is not None:
        selected_contract = _secret_free(contract_spec)
        if not isinstance(selected_contract, Mapping):
            raise HistoricalCampaignError("contract_spec inválido")
    else:
        selected_contract = {}
    if calendar_state is not None:
        selected_calendar = _secret_free(calendar_state)
        if not isinstance(selected_calendar, Mapping):
            raise HistoricalCampaignError("calendar_state inválido")
    else:
        selected_calendar = None
    config = HistoricalBacktestConfig.from_protocol(
        selected_protocol,
        scenario=scenario,
        candidates=selected_candidates,
        contract_spec=cast(Mapping[str, Any], selected_contract),
        calendar_state=cast(Mapping[str, Any] | None, selected_calendar),
    )
    if resume_context is not None:
        stored_prefix = _stored_prefix(resume_context.request)
        if prefix_quotes is None:
            selected_prefix = stored_prefix
        elif requested_prefix != stored_prefix:
            raise HistoricalCampaignError("resume prefix_quotes mismatch")
        else:
            selected_prefix = requested_prefix
    else:
        selected_prefix = requested_prefix
    if (
        resume_context is not None
        and selected_prefix is not None
        and resume_context.checkpoint.processed_quotes > selected_prefix
    ):
        raise HistoricalCampaignError("resume prefix_quotes es menor al checkpoint")
    selected_stop_after, selected_checkpoint_after = _partial_controls(
        stop_after_quotes,
        checkpoint_after_quotes,
        resume_context.checkpoint if resume_context is not None else None,
    )
    identity = _manifest_identity(selected_manifest)

    if resume_context is None:
        selected_run_id, run_directory = _new_run_directory(output_root, run_id)
        code_destination = run_directory / "code"
        selected_code_root = source_root if source_root is not None else code_root
        code_snapshot = _snapshot_code(
            _absolute(selected_code_root, name="code_root") if selected_code_root is not None else REPOSITORY_ROOT,
            code_destination,
        )
    else:
        if run_id is not None:
            raise HistoricalCampaignError("resume_run_id no admite --run-id")
        selected_run_id = resume_context.run_id
        run_directory = resume_context.run_directory
        code_snapshot = resume_context.code_snapshot

    request_payload = {
        "schema": RUNNER_SCHEMA,
        "stage": selected_stage,
        "window": {"start": instant_text(selected_start), "end": instant_text(selected_end)},
        "prefix_quotes": selected_prefix,
        "scenario": scenario,
        "candidate_ids": list(selected_candidates),
        "runtime_identity": selected_runtime,
        "contract_spec": dict(selected_contract),
        "calendar_state": selected_calendar,
        "dataset": identity,
        "protocol_hash": selected_protocol.protocol_hash,
        "config_hash": config.config_hash,
        "code_hash": str(code_snapshot["code_hash"]),
    }
    if resume_context is None:
        input_snapshot = _snapshot_inputs(
            run_directory / "inputs",
            manifest=selected_manifest,
            manifest_source=manifest_source,
            protocol=selected_protocol,
            protocol_source=protocol_source,
            validation=validation_payload,
            request=request_payload,
        )
    else:
        input_snapshot = resume_context.input_snapshot
        _validate_resume_input_identity(
            resume_context,
            selected_manifest=selected_manifest,
            manifest_source=manifest_source,
            selected_protocol=selected_protocol,
            protocol_source=protocol_source,
            validation=validation_payload,
            request=request_payload,
            config=config,
            runtime_identity=selected_runtime,
            code_snapshot=code_snapshot,
        )

    if resume_context is not None:
        _safe_input_file(registry.path if isinstance(registry, GlobalTrialRegistry) else registry, name="registry")
    selected_registry = registry if isinstance(registry, GlobalTrialRegistry) else GlobalTrialRegistry(registry)
    parameters = _attempt_parameters(
        stage=selected_stage,
        start=selected_start,
        end=selected_end,
        prefix_quotes=selected_prefix,
        scenario=config.scenario,
        candidate_ids=selected_candidates,
        config_hash=config.config_hash,
        code_hash=str(code_snapshot["code_hash"]),
        input_hash=str(input_snapshot["input_hash"]),
    )
    attempt_ids: list[str]
    if resume_context is not None:
        attempt_ids = list(
            _resume_attempt_ids(
                resume_context,
                selected_registry,
                candidate_ids=selected_candidates,
                protocol=selected_protocol,
                manifest=selected_manifest,
                runtime_identity=selected_runtime,
                config_hash=config.config_hash,
                code_hash=str(code_snapshot["code_hash"]),
                input_hash=str(input_snapshot["input_hash"]),
            )
        )
        for attempt_id in attempt_ids:
            try:
                selected_registry.update_status(
                    attempt_id,
                    "RUNNING",
                    details={"run_id": selected_run_id, "resumed": True},
                )
            except (RegistryError, OSError, TypeError, ValueError) as exc:
                raise HistoricalCampaignError("no se pudo reactivar el intento histórico") from exc
    else:
        attempt_ids = []
        try:
            for candidate_id in selected_candidates:
                record = selected_registry.register_attempt(
                    candidate_id=candidate_id,
                    protocol_hash=selected_protocol.protocol_hash,
                    dataset_hash=selected_manifest.content_hash,
                    runtime_identity=selected_runtime,
                    data_identity=identity,
                    scope={
                        "stage": selected_stage,
                        "window_start": instant_text(selected_start),
                        "window_end": instant_text(selected_end),
                        "prefix_quotes": selected_prefix,
                        "run_id": selected_run_id,
                        "holdout": "CLOSED",
                        "selection_performed": False,
                        "promotion_performed": False,
                    },
                    mode="HISTORICAL",
                    parameters={**parameters, "candidate_id": candidate_id},
                )
                attempt_ids.append(str(record["attempt_id"]))
            for attempt_id in attempt_ids:
                selected_registry.update_status(attempt_id, "RUNNING", details={"run_id": selected_run_id})
        except (RegistryError, OSError, TypeError, ValueError) as exc:
            raise HistoricalCampaignError("no se pudo preregistrar el intento histórico") from exc

    receipt = _base_receipt(
        run_id=selected_run_id,
        stage=selected_stage,
        start=selected_start,
        end=selected_end,
        prefix_quotes=selected_prefix,
        manifest=identity,
        protocol=selected_protocol,
        config=config,
        validation=validation_payload,
        code_snapshot=code_snapshot,
        input_snapshot=input_snapshot,
        attempt_ids=attempt_ids,
        registry_path=selected_registry.path,
        runtime_identity=selected_runtime,
        resumed=resume_context is not None,
        resume_checkpoint=resume_context.checkpoint_path if resume_context is not None else None,
        previous_receipt=resume_context.receipt_path if resume_context is not None else None,
    )
    stream_statistics = {"quotes_emitted": 0, "factory_calls": 0}

    def bounded_stream() -> Iterator[HistoricalQuote]:
        stream_statistics["factory_calls"] += 1
        if stream_statistics["factory_calls"] != 1:
            raise HistoricalCampaignError("la fuente histórica sólo puede consumirse una vez")
        source_stream = iter_quotes(selected_manifest, selected_start, selected_end)
        try:
            for quote in source_stream:
                if selected_prefix is not None and stream_statistics["quotes_emitted"] >= selected_prefix:
                    break
                stream_statistics["quotes_emitted"] += 1
                yield quote
        finally:
            close = getattr(source_stream, "close", None)
            if callable(close):
                close()

    backtest_directory = run_directory / "backtest"
    if resume_context is None:
        _mkdir_private(backtest_directory)
    else:
        _safe_existing_directory(backtest_directory, name="backtest")
    selected_runner = runner or run_historical_backtest
    runner_resume = resume_context.checkpoint if resume_context is not None else None
    try:
        result = _invoke_runner(
            selected_runner,
            bounded_stream(),
            manifest=selected_manifest,
            config=config,
            output_dir=backtest_directory,
            resume=runner_resume,
            stop_after_quotes=selected_stop_after,
            checkpoint_after_quotes=selected_checkpoint_after,
        )
        result_payload = _result_mapping(result)
        if not _verify_code_snapshot(code_snapshot):
            raise HistoricalCampaignError("código fuente cambió durante la corrida; evidencia inválida")
        partial = result_payload.get("status") == "CHECKPOINTED" or result_payload.get("finished") is False
        receipt.update(
            {
                "status": "CHECKPOINTED" if partial else "COMPLETED",
                "state": "CHECKPOINTED_DEVELOPMENT_REVIEW_ONLY" if partial else "COMPLETED_DEVELOPMENT_REVIEW_ONLY",
                "stream": {
                    "quotes_emitted": stream_statistics["quotes_emitted"],
                    "prefix_applied": selected_prefix is not None,
                    "factory_calls": stream_statistics["factory_calls"],
                },
                "backtest": result_payload,
                "result_status": result_payload.get("status", "UNKNOWN"),
                "source_frozen_after_run": not partial,
                "resumable": partial,
            }
        )
        if partial:
            checkpoint_path = _private_artifact(backtest_directory / "checkpoint.json", name="checkpoint")
            receipt["checkpoint_path"] = str(checkpoint_path)
        else:
            receipt["registry_revision_before_terminal_status"] = selected_registry.revision
        receipt_path = _receipt_destination(run_directory, resumed=resume_context is not None)
        receipt["receipt_path"] = str(receipt_path)
        receipt_hash, _ = _write_json(receipt_path, receipt)
        status = "RUNNING" if partial else "COMPLETED"
        terminal_errors: list[str] = []
        for attempt_id in attempt_ids:
            try:
                selected_registry.update_status(
                    attempt_id,
                    status,
                    details={
                        "run_id": selected_run_id,
                        "receipt_sha256": receipt_hash,
                        "review_only": True,
                        "checkpointed": partial,
                    },
                )
            except RegistryError as exc:
                terminal_errors.append(str(exc))
        receipt["receipt_sha256"] = receipt_hash
        receipt["registry_revision"] = selected_registry.revision
        if terminal_errors:
            receipt["registry_terminal_errors"] = terminal_errors
        return receipt
    except BaseException as exc:
        failure = dict(receipt)
        failure.update(
            {
                "status": "FAILED",
                "state": "FAILED_NO_SELECTION",
                "error_type": type(exc).__name__,
                "error": str(exc),
                "stream": {
                    "quotes_emitted": stream_statistics["quotes_emitted"],
                    "prefix_applied": selected_prefix is not None,
                    "factory_calls": stream_statistics["factory_calls"],
                },
            }
        )
        receipt_path = _receipt_destination(run_directory, resumed=resume_context is not None, failure=True)
        failure["receipt_path"] = str(receipt_path)
        failure_hash: str | None = None
        with contextlib.suppress(BaseException):
            failure_hash, _ = _write_json(receipt_path, failure)
        for attempt_id in attempt_ids:
            with contextlib.suppress(RegistryError):
                selected_registry.update_status(
                    attempt_id,
                    "FAILED",
                    details={
                        "run_id": selected_run_id,
                        "error_type": type(exc).__name__,
                        "receipt_sha256": failure_hash,
                        "review_only": True,
                    },
                )
        if isinstance(exc, HistoricalCampaignError):
            raise
        if isinstance(exc, HistoricalBacktestError):
            raise HistoricalCampaignError(str(exc)) from exc
        raise HistoricalCampaignError("la corrida histórica falló") from exc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a registered development-only MTF historical backtest")
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--protocol", type=Path)
    parser.add_argument("--registry", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--start", required=True, help="UTC ISO start, inclusive")
    parser.add_argument("--end", required=True, help="UTC ISO end, exclusive")
    parser.add_argument(
        "--stage",
        default="development",
        choices=("pilot-week", "pilot-month", "development", "walk-forward", "holdout"),
    )
    parser.add_argument(
        "--walk-forward-manifest",
        type=Path,
        help="independent WalkForwardManifest sidecar (preflight only; no WF run)",
    )
    parser.add_argument(
        "--walk-forward-input",
        type=Path,
        help="hash-only WalkForwardInput sidecar (preflight only; no WF run)",
    )
    parser.add_argument(
        "--window",
        default="WF_2020",
        choices=("WF_2020", "WF_2021", "WF_2022", "WF_2023"),
        help="exact WF window used by the contract preflight",
    )
    parser.add_argument("--scenario", default="base", choices=("base", "adverse", "extreme"))
    parser.add_argument("--candidate", dest="candidates", action="append")
    parser.add_argument("--prefix-quotes", type=int)
    parser.add_argument("--validation", type=Path)
    parser.add_argument("--runtime-identity", type=Path)
    parser.add_argument("--contract-spec", type=Path)
    parser.add_argument("--calendar-state", type=Path)
    parser.add_argument("--code-root", type=Path)
    parser.add_argument("--run-id")
    parser.add_argument("--stop-after-quotes", type=int)
    parser.add_argument("--checkpoint-after-quotes", type=int)
    resume_group = parser.add_mutually_exclusive_group()
    resume_group.add_argument("--resume-checkpoint", type=Path)
    resume_group.add_argument("--resume-run-id")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        receipt = run_historical_campaign(
            args.manifest,
            registry=args.registry,
            output_dir=args.output_dir,
            start=args.start,
            end=args.end,
            protocol=args.protocol,
            stage=args.stage,
            walk_forward_manifest=args.walk_forward_manifest,
            walk_forward_input=args.walk_forward_input,
            window=args.window,
            scenario=args.scenario,
            candidates=args.candidates,
            prefix_quotes=args.prefix_quotes,
            validation=args.validation,
            runtime_identity=_mapping_value(args.runtime_identity, name="runtime_identity")
            if args.runtime_identity is not None
            else None,
            contract_spec=_mapping_value(args.contract_spec, name="contract_spec")
            if args.contract_spec is not None
            else None,
            calendar_state=_mapping_value(args.calendar_state, name="calendar_state")
            if args.calendar_state is not None
            else None,
            code_root=args.code_root,
            run_id=args.run_id,
            stop_after_quotes=args.stop_after_quotes,
            checkpoint_after_quotes=args.checkpoint_after_quotes,
            resume_checkpoint=args.resume_checkpoint,
            resume_run_id=args.resume_run_id,
        )
    except (HistoricalCampaignError, HistoricalBacktestError, RegistryError, OSError, TypeError, ValueError) as exc:
        print(f"historical campaign rejected: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(_jsonable(receipt), ensure_ascii=False, sort_keys=True))
    return 0


__all__ = [
    "CampaignRunnerError",
    "DEVELOPMENT_END",
    "DEVELOPMENT_START",
    "HOLDOUT_START",
    "HistoricalCampaignError",
    "HistoricalCampaignRunnerError",
    "RUNNER_SCHEMA",
    "WalkForwardContractBinding",
    "bind_walk_forward_contract",
    "load_validated_manifest",
    "main",
    "run_historical_campaign",
]


if __name__ == "__main__":
    raise SystemExit(main())
