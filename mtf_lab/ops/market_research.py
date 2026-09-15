"""Local service layer for the frozen historical-market campaign.

The service is deliberately narrower than a provider or a trading engine.  It
loads an already acquired :class:`DatasetManifest`, verifies identities, and
delegates streaming execution to the owner runner.  It never downloads data,
opens OAuth, places an order, or promotes a candidate.  A holdout run has a
separate one-use confirmatory access record in :class:`GlobalTrialRegistry`.

The public helpers are also the adapter seam used by the CLI.  They accept
plain mappings in focal tests, while the normal path keeps the canonical
``data.historical.DatasetManifest`` and ``ops.historical_backtest`` contracts
intact.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import os
import secrets
import stat
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Protocol, cast

from ..core.canonical import canonical_json, instant_text
from ..data.historical import DatasetManifest, HistoricalQuote, iter_quotes, read_manifest, validate_dataset
from .global_trial_registry import GlobalTrialRegistry, RegistryConflict, RegistryError
from .historical_assumptions import (
    HistoricalAssumptionError,
    HistoricalAssumptionsModel,
    HistoricalCalendarTemplate,
    calendar_template_for,
    historical_assumptions_for,
)
from .historical_backtest import SCENARIO_PARAMETERS as HISTORICAL_SCENARIO_PARAMETERS
from .historical_backtest import SCENARIOS as HISTORICAL_SCENARIOS
from .market_evidence import derive_net_r, evaluate_candidates, validate_evidence
from .market_protocol import (
    CANDIDATE_IDS,
    MarketProtocolError,
    ResearchProtocol,
    read_protocol,
    write_default_protocol,
)

MARKET_RESEARCH_SCHEMA = "mtf-lab.market-research-service.v1"
RUN_SCHEMA = "mtf-lab.market-research-run.v1"
STAGES = ("pilot-week", "pilot-month", "development", "walk-forward", "holdout", "forward")
SCENARIOS = HISTORICAL_SCENARIOS
SCENARIO_PARAMETERS = HISTORICAL_SCENARIO_PARAMETERS
_HOLDOUT_START = datetime(2024, 1, 1, tzinfo=UTC)
_HOLDOUT_END = datetime(2026, 1, 1, tzinfo=UTC)
HOLDOUT_WINDOW_START = instant_text(_HOLDOUT_START)
HOLDOUT_WINDOW_END = instant_text(_HOLDOUT_END)
FORWARD_INTERIM_EPISODES = 100
FORWARD_FINAL_EPISODES = 200
FORWARD_MIN_SESSIONS = 30
FORWARD_MIN_BLOCKS = 20
INITIAL_GATES = (
    "historical_dataset_and_2024_2025_coverage_verified",
    "calendar_coverage_and_exceptions_hash_verified",
    "cost_catalog_and_contract_spec_known",
    "holdout_single_use_confirmatory_access",
    "human_review_before_any_promotion_or_trading",
    "forward_30_100_200_observed_sessions",
)
_CHECKOUT = Path(__file__).resolve().parents[2]
_D0 = Decimal("0")


class MarketResearchError(ValueError):
    """The campaign request or its local evidence does not satisfy its gate."""


class HoldoutAccessError(MarketResearchError):
    """The locked holdout cannot be opened with the supplied identity/access."""


class ResearchRunner(Protocol):
    """Owner runner accepted by :func:`run_campaign`."""

    def __call__(self, **kwargs: Any) -> Any: ...


def _jsonable(value: Any) -> Any:
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
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return _jsonable(value.to_dict())
    return value


def _text(value: Any, *, name: str, allow_empty: bool = False) -> str:
    result = str(value).strip() if value is not None else ""
    if not result and not allow_empty:
        raise MarketResearchError(f"{name} es obligatorio")
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
            raise MarketResearchError(f"{name} debe ser ISO-8601 aware") from exc
    else:
        raise MarketResearchError(f"{name} debe ser datetime o ISO-8601")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise MarketResearchError(f"{name} debe incluir zona horaria")
    return parsed.astimezone(UTC)


def _decimal(value: Any, *, name: str) -> Decimal:
    if isinstance(value, bool):
        raise MarketResearchError(f"{name} no admite booleanos")
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise MarketResearchError(f"{name} no es Decimal válido") from exc
    if not parsed.is_finite():
        raise MarketResearchError(f"{name} debe ser finito")
    return parsed


def _safe_input_file(path: str | Path, *, name: str) -> Path:
    target = Path(path).expanduser()
    if not target.is_absolute():
        raise MarketResearchError(f"{name} debe ser una ruta absoluta")
    target = Path(os.path.abspath(os.fspath(target)))
    try:
        info = os.lstat(target)
    except OSError as exc:
        raise MarketResearchError(f"{name} no se pudo inspeccionar: {target}") from exc
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise MarketResearchError(f"{name} debe ser un archivo regular exclusivo")
    current = Path(target.anchor)
    for part in target.parent.parts:
        if part == target.anchor:
            continue
        current /= part
        try:
            parent_info = os.lstat(current)
        except OSError as exc:
            raise MarketResearchError(f"padre de {name} ilegible: {current}") from exc
        if stat.S_ISLNK(parent_info.st_mode) or not stat.S_ISDIR(parent_info.st_mode):
            raise MarketResearchError(f"padre de {name} inseguro: {current}")
    return target


def _safe_output_dir(path: str | Path) -> Path:
    target = Path(path).expanduser()
    if not target.is_absolute():
        raise MarketResearchError("output_dir debe ser una ruta absoluta")
    target = Path(os.path.abspath(os.fspath(target)))
    try:
        target.relative_to(_CHECKOUT)
    except ValueError:
        pass
    else:
        raise MarketResearchError("output_dir debe estar fuera del checkout")
    current = Path(target.anchor)
    for part in target.parts:
        if part == target.anchor:
            continue
        current /= part
        try:
            info = os.lstat(current)
        except FileNotFoundError:
            current.mkdir(mode=0o700)
            info = os.lstat(current)
        except OSError as exc:
            raise MarketResearchError(f"output_dir ilegible: {current}") from exc
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise MarketResearchError(f"output_dir inseguro: {current}")
    os.chmod(target, 0o700)
    return target


def _exclusive_payload(path: Path, payload: Mapping[str, Any]) -> Path:
    """Write a private JSON artifact once without replacing another run."""

    target = path
    token = uuid.uuid4().hex
    temporary = target.with_name(f".{target.name}.{os.getpid()}.{token}.tmp")
    data = (canonical_json(_jsonable(payload)) + "\n").encode("utf-8")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        offset = 0
        while offset < len(data):
            written = os.write(fd, data[offset:])
            if written <= 0:
                raise OSError("escritura de artifact no progresó")
            offset += written
        os.fchmod(fd, 0o600)
        os.fsync(fd)
    except BaseException:
        with suppress(OSError):
            os.close(fd)
        with suppress(OSError):
            temporary.unlink()
        raise
    os.close(fd)
    try:
        os.link(temporary, target)
        parent_fd = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
        temporary.unlink()
    except BaseException:
        with suppress(OSError):
            temporary.unlink()
        raise
    return target


def _load_protocol(value: ResearchProtocol | str | Path | None) -> ResearchProtocol:
    if value is None:
        return ResearchProtocol.default()
    if isinstance(value, ResearchProtocol):
        return value
    try:
        return read_protocol(value)
    except (MarketProtocolError, OSError, TypeError) as exc:
        raise MarketResearchError(f"protocolo ilegible: {value}") from exc


def _load_manifest(value: DatasetManifest | str | Path) -> DatasetManifest:
    if isinstance(value, DatasetManifest):
        return value
    try:
        loaded = read_manifest(_safe_input_file(value, name="dataset_manifest"))
        if not isinstance(loaded, DatasetManifest):
            raise MarketResearchError("dataset_manifest debe ser el contrato HistData DatasetManifest")
        return loaded
    except (OSError, TypeError, ValueError) as exc:
        raise MarketResearchError(f"dataset manifest ilegible: {value}") from exc


def _registry(value: GlobalTrialRegistry | str | Path) -> GlobalTrialRegistry:
    if isinstance(value, GlobalTrialRegistry):
        return value
    try:
        return GlobalTrialRegistry(value)
    except (RegistryError, OSError, TypeError) as exc:
        raise MarketResearchError(f"registry ilegible: {value}") from exc


def _manifest_identity(manifest: DatasetManifest) -> dict[str, Any]:
    content_hash = _text(getattr(manifest, "content_hash", None), name="dataset content_hash")
    dataset_id = _text(getattr(manifest, "dataset_id", None), name="dataset_id")
    return {
        "dataset_id": dataset_id,
        "provider": str(getattr(manifest, "provider", "")),
        "instrument": str(getattr(manifest, "instrument", "")),
        "content_hash": content_hash,
        "coverage_start": instant_text(manifest.coverage_start) if manifest.coverage_start else None,
        "coverage_end": instant_text(manifest.coverage_end) if manifest.coverage_end else None,
        "schema_version": getattr(manifest, "schema_version", None),
    }


def _identity_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(_jsonable(value)).encode("utf-8")).hexdigest()


def _campaign_key(identity: Mapping[str, Any], protocol: ResearchProtocol) -> str:
    """Stable holdout campaign identity independent of code/protocol revision."""

    material = {
        "dataset_id": identity.get("dataset_id"),
        "dataset_hash": identity.get("content_hash"),
        "instrument": identity.get("instrument"),
        "holdout_start_year": protocol.holdout_start_year,
        "holdout_end_year": protocol.holdout_end_year,
    }
    return f"holdout:{_identity_hash(material)}"


def _mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if hasattr(value, "to_dict") and callable(value.to_dict):
        result = value.to_dict()
        return result if isinstance(result, Mapping) else {}
    if dataclass_values := getattr(value, "__dict__", None):
        return dataclass_values if isinstance(dataclass_values, Mapping) else {}
    return {}


def _candidate_selection(protocol: ResearchProtocol, candidates: Sequence[str] | None) -> tuple[str, ...]:
    selected = tuple(protocol.candidate_ids) if candidates is None else tuple(str(item).strip() for item in candidates)
    if not selected:
        raise MarketResearchError("candidates no puede estar vacío")
    if len(set(selected)) != len(selected):
        raise MarketResearchError("candidates no puede contener duplicados")
    unknown = set(selected) - set(CANDIDATE_IDS)
    if unknown:
        raise MarketResearchError(f"candidate no registrado: {sorted(unknown)}")
    return selected


def _stage(value: str) -> str:
    normalized = str(value).strip().lower().replace("_", "-")
    if normalized not in STAGES:
        raise MarketResearchError(f"stage no soportado: {value!r}")
    return normalized


def _scenarios(values: Sequence[str] | None) -> tuple[str, ...]:
    selected = ("base",) if values is None else tuple(str(item).strip().lower() for item in values)
    if not selected or any(item not in SCENARIOS for item in selected):
        raise MarketResearchError(f"scenarios soportados: {SCENARIOS}")
    if len(set(selected)) != len(selected):
        raise MarketResearchError("scenarios no puede contener duplicados")
    return selected


def _runtime_identity(value: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(value, Mapping) or not value:
        raise MarketResearchError("runtime_identity explícito es obligatorio")
    return cast(dict[str, Any], _jsonable(dict(value)))


def _cost_identity(value: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(value, Mapping) or not value:
        raise MarketResearchError("cost_identity explícito es obligatorio")
    return cast(dict[str, Any], _jsonable(dict(value)))


def _assert_runtime_identity(runtime: Mapping[str, Any]) -> None:
    runtime_state = str(runtime.get("state", "")).upper()
    if runtime_state != "FROZEN":
        raise HoldoutAccessError("runtime_identity no está congelada/verificada")
    runtime_id = runtime.get("runtime_id", runtime.get("identity"))
    code_hash = runtime.get("code_hash")
    seal = runtime.get("identity_seal", runtime.get("seal"))
    runtime_version = runtime.get("runtime_version", runtime.get("python"))
    code_version = runtime.get("code_version", runtime.get("version"))
    if (
        not _text_or_none(runtime_id)
        or not _text_or_none(runtime_version)
        or not _text_or_none(code_version)
        or not _verified_sha(code_hash)
        or not _verified_sha(seal)
    ):
        raise HoldoutAccessError("runtime_identity requiere campos/versiones y code_hash/seal SHA-256 verificados")
    if runtime.get("sha256_verified") is not True and runtime.get("code_hash_verified") is not True:
        raise HoldoutAccessError("runtime_identity requiere verificación SHA-256 explícita")


def _assert_cost_identity(costs: Mapping[str, Any]) -> None:
    cost_state = str(costs.get("state", "")).upper()
    if cost_state != "KNOWN" or costs.get("costs_complete") is not True:
        raise HoldoutAccessError("cost_identity requiere costs_complete=true y estado KNOWN")
    if not _text_or_none(costs.get("known_source")):
        raise HoldoutAccessError("cost_identity requiere known_source")
    if not _text_or_none(costs.get("currency", costs.get("account_currency"))):
        raise HoldoutAccessError("cost_identity requiere currency/account_currency")
    if not _verified_sha(costs.get("cost_hash", costs.get("identity_seal"))):
        raise HoldoutAccessError("cost_identity requiere cost_hash/identity_seal SHA-256")


def _assert_calendar_identity(calendar: Mapping[str, Any], calendar_digest: str) -> None:
    if calendar_digest.upper() in {"UNDECLARED", "UNKNOWN", "NONE"}:
        raise HoldoutAccessError("calendar_hash no está congelado")
    coverage_start = calendar.get("coverage_start")
    coverage_end = calendar.get("coverage_end")
    if not _text_or_none(coverage_start) or not _text_or_none(coverage_end):
        raise HoldoutAccessError("calendar requiere coverage_start/coverage_end")
    try:
        parsed_end = _aware(coverage_end, name="calendar.coverage_end")
        parsed_start = _aware(coverage_start, name="calendar.coverage_start")
    except MarketResearchError as exc:
        raise HoldoutAccessError("calendar coverage debe ser UTC aware") from exc
    if parsed_end <= parsed_start:
        raise HoldoutAccessError("calendar coverage debe ser un intervalo positivo")
    if calendar.get("exceptions_known") is not True:
        raise HoldoutAccessError("calendar requiere exceptions_known=true")
    if not _verified_sha(calendar.get("calendar_hash", calendar.get("identity_seal", calendar_digest))):
        raise HoldoutAccessError("calendar requiere hash SHA-256 verificable")


def _assert_frozen_identities(
    *,
    runtime: Mapping[str, Any],
    costs: Mapping[str, Any],
    calendar_digest: str,
    calendar: Mapping[str, Any],
) -> None:
    _assert_runtime_identity(runtime)
    _assert_cost_identity(costs)
    _assert_calendar_identity(calendar, calendar_digest)


def _text_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _verified_sha(value: Any) -> bool:
    text = _text_or_none(value)
    if text is None or len(text) != 64:
        return False
    try:
        int(text, 16)
    except ValueError:
        return False
    return True


def _calendar_identity(value: str | Mapping[str, Any] | None) -> tuple[str, dict[str, Any]]:
    if value is None or value == "":
        raise MarketResearchError("calendar_hash explícito es obligatorio")
    if isinstance(value, Mapping):
        material = cast(dict[str, Any], _jsonable(dict(value)))
        return _identity_hash(material), material
    text = _text(value, name="calendar_hash")
    return text, {"calendar_hash": text}


@dataclass(frozen=True, slots=True)
class _AssumptionBinding:
    assumptions: HistoricalAssumptionsModel | None
    calendar: HistoricalCalendarTemplate | None
    contract_spec: Mapping[str, Any] | None
    calendar_state: Mapping[str, Any] | None

    @property
    def assumptions_model_id(self) -> str | None:
        return self.assumptions.model_id if self.assumptions is not None else None

    @property
    def assumption_hash(self) -> str | None:
        return self.assumptions.assumption_hash if self.assumptions is not None else None

    @property
    def calendar_model_id(self) -> str | None:
        return self.calendar.model_id if self.calendar is not None else None

    @property
    def calendar_hash(self) -> str | None:
        return self.calendar.calendar_hash if self.calendar is not None else None

    def to_dict(self) -> dict[str, Any]:
        return {
            "assumptions_model_id": self.assumptions_model_id,
            "assumption_hash": self.assumption_hash,
            "assumptions_model": (_jsonable(self.assumptions.to_dict()) if self.assumptions is not None else None),
            "calendar_model_id": self.calendar_model_id,
            "calendar_hash": self.calendar_hash,
            "calendar_model": _jsonable(self.calendar.to_dict()) if self.calendar is not None else None,
            "contract_spec": _jsonable(self.contract_spec),
            "calendar_state": _jsonable(self.calendar_state),
            "mode": "MODELED_NOT_OBSERVED" if self.calendar is not None else "UNSPECIFIED",
            "holdout_verified": False if self.calendar is not None else None,
        }

    @property
    def binding_hash(self) -> str:
        return _identity_hash(self.to_dict())


def _resolve_assumption_binding(
    *,
    assumptions_model_id: str | None,
    calendar_model_id: str | None,
    contract_spec: Mapping[str, Any] | None,
    calendar_state: Mapping[str, Any] | None,
) -> _AssumptionBinding:
    assumptions: HistoricalAssumptionsModel | None = None
    calendar: HistoricalCalendarTemplate | None = None
    if assumptions_model_id is not None:
        requested = _text(assumptions_model_id, name="assumptions_model_id")
        try:
            assumptions = historical_assumptions_for(requested)
        except HistoricalAssumptionError as exc:
            raise MarketResearchError(f"assumptions_model_id no soportado: {requested}") from exc
        if calendar_model_id is None:
            raise MarketResearchError("calendar_model_id explícito es obligatorio junto al assumptions_model_id")
    if calendar_model_id is not None:
        requested_calendar = _text(calendar_model_id, name="calendar_model_id")
        try:
            calendar = calendar_template_for(requested_calendar)
        except HistoricalAssumptionError as exc:
            raise MarketResearchError(f"calendar_model_id no soportado: {requested_calendar}") from exc
    if assumptions is not None and calendar is not None and assumptions.calendar.model_id != calendar.model_id:
        raise MarketResearchError("assumptions_model y calendar_model no comparten calendar_model_id")
    selected_contract = (
        dict(contract_spec)
        if contract_spec is not None
        else dict(assumptions.account.contract_spec)
        if assumptions is not None
        else None
    )
    selected_state = dict(calendar_state) if calendar_state is not None else None
    return _AssumptionBinding(assumptions, calendar, selected_contract, selected_state)


@dataclass(frozen=True, slots=True)
class ConfirmatoryAccess:
    """One-use opaque holdout access capability.

    The plaintext capability is held only by the caller.  The registry stores
    ``access_digest`` instead, so a local registry dump cannot be used as an
    access token.
    """

    attempt_id: str
    protocol_hash: str
    dataset_hash: str
    capability: str
    issued_at: str
    used: bool = False
    campaign_id: str = ""

    @property
    def access_digest(self) -> str:
        return _identity_hash(self.capability)

    @property
    def token(self) -> str:
        """Compatibility alias; the token is never serialized to an artifact."""

        return self.capability

    def to_dict(self) -> dict[str, Any]:
        return {
            "attempt_id": self.attempt_id,
            "protocol_hash": self.protocol_hash,
            "dataset_hash": self.dataset_hash,
            "access_digest": self.access_digest,
            "issued_at": self.issued_at,
            "used": self.used,
            "campaign_id": self.campaign_id,
        }


@dataclass(frozen=True, slots=True)
class HoldoutExecutionPermission:
    """Non-secret, run-scoped permit passed to an explicitly typed runner.

    ``ConfirmatoryAccess`` remains the one-use capability held by the caller.
    This DTO contains only the verified scope needed by a runner; it never
    contains the opaque capability itself.  A permission is created only
    after the capability has been consumed and the selected attempt set has
    been checked.
    """

    attempt_id: str
    candidate_id: str
    attempt_ids: tuple[str, ...]
    candidate_ids: tuple[str, ...]
    dataset_id: str
    dataset_hash: str
    protocol_hash: str
    scenarios: tuple[str, ...]
    access_digest: str
    campaign_id: str
    window_start: str = HOLDOUT_WINDOW_START
    window_end: str = HOLDOUT_WINDOW_END

    def __post_init__(self) -> None:
        if not _text_or_none(self.attempt_id) or not _text_or_none(self.candidate_id):
            raise HoldoutAccessError("holdout permission requiere attempt_id/candidate_id")
        if not self.attempt_ids or len(set(self.attempt_ids)) != len(self.attempt_ids):
            raise HoldoutAccessError("holdout permission requiere attempt_ids únicos")
        if not self.candidate_ids or len(set(self.candidate_ids)) != len(self.candidate_ids):
            raise HoldoutAccessError("holdout permission requiere candidate_ids únicos")
        if len(self.attempt_ids) != len(self.candidate_ids):
            raise HoldoutAccessError("holdout permission requiere candidatos e intentos alineados")
        if self.attempt_id not in self.attempt_ids or self.candidate_id not in self.candidate_ids:
            raise HoldoutAccessError("holdout permission no pertenece al conjunto seleccionado")
        if not _text_or_none(self.dataset_id) or not _verified_sha(self.dataset_hash):
            raise HoldoutAccessError("holdout permission requiere dataset verificado")
        if not _text_or_none(self.protocol_hash) or not _verified_sha(self.access_digest):
            raise HoldoutAccessError("holdout permission requiere protocolo/acceso verificados")
        if not self.scenarios or any(str(item) not in SCENARIOS for item in self.scenarios):
            raise HoldoutAccessError("holdout permission requiere escenarios registrados")
        if self.window_start != HOLDOUT_WINDOW_START or self.window_end != HOLDOUT_WINDOW_END:
            raise HoldoutAccessError("holdout permission sólo admite la ventana 2024-01-01..2026-01-01")

    @property
    def permission_hash(self) -> str:
        return _identity_hash(
            {
                "attempt_id": self.attempt_id,
                "candidate_id": self.candidate_id,
                "attempt_ids": list(self.attempt_ids),
                "candidate_ids": list(self.candidate_ids),
                "dataset_id": self.dataset_id,
                "dataset_hash": self.dataset_hash,
                "protocol_hash": self.protocol_hash,
                "scenarios": list(self.scenarios),
                "access_digest": self.access_digest,
                "campaign_id": self.campaign_id,
                "window_start": self.window_start,
                "window_end": self.window_end,
            }
        )

    def to_dict(self) -> dict[str, Any]:
        """Return the public scope; the one-use capability is never present."""

        return {
            "status": "CONSUMED",
            "used": True,
            "attempt_id": self.attempt_id,
            "candidate_id": self.candidate_id,
            "attempt_ids": list(self.attempt_ids),
            "candidate_ids": list(self.candidate_ids),
            "dataset_id": self.dataset_id,
            "dataset_hash": self.dataset_hash,
            "protocol_hash": self.protocol_hash,
            "scenarios": list(self.scenarios),
            "access_digest": self.access_digest,
            "campaign_id": self.campaign_id,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "permission_hash": self.permission_hash,
        }


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _find_attempt(records: Sequence[Mapping[str, Any]], attempt_id: str) -> Mapping[str, Any] | None:
    for record in records:
        if record.get("attempt_id") == attempt_id and record.get("event") == "ATTEMPT_REGISTERED":
            return record
    return None


def _attempt_status(records: Sequence[Mapping[str, Any]], attempt_id: str) -> str | None:
    status: str | None = None
    for record in records:
        if record.get("attempt_id") == attempt_id and isinstance(record.get("status"), str):
            status = str(record["status"])
    return status


def _check_access_registration(
    records: Sequence[Mapping[str, Any]],
    *,
    attempt_id: str,
    protocol_hash: str,
    dataset_hash: str,
    runtime: Mapping[str, Any],
    costs: Mapping[str, Any],
    calendar_digest: str,
    campaign_id: str,
) -> None:
    registration = _find_attempt(records, attempt_id)
    if registration is None:
        raise HoldoutAccessError(f"attempt_id no registrado: {attempt_id}")
    if registration.get("protocol_hash") != protocol_hash:
        raise HoldoutAccessError("protocol_hash del intento no coincide")
    if registration.get("dataset_hash") != dataset_hash:
        raise HoldoutAccessError("dataset_hash del intento no coincide")
    _check_access_scope(
        registration.get("scope"),
        campaign_id=campaign_id,
        calendar_digest=calendar_digest,
        cost_digest=_identity_hash(costs),
    )
    registered_runtime = registration.get("runtime_identity")
    if isinstance(registered_runtime, Mapping) and _identity_hash(registered_runtime) != _identity_hash(runtime):
        raise HoldoutAccessError("runtime_identity del intento no coincide")
    if _attempt_status(records, attempt_id) not in {"REGISTERED", "RUNNING"}:
        raise HoldoutAccessError("el intento no está abierto para acceso confirmatorio")


def _check_access_scope(scope: Any, *, campaign_id: str, calendar_digest: str, cost_digest: str) -> None:
    if not isinstance(scope, Mapping):
        raise HoldoutAccessError("el intento no tiene scope congelado")
    if scope.get("stage") != "holdout":
        raise HoldoutAccessError("el intento no fue preregistrado para holdout")
    if scope.get("campaign_id") not in {None, campaign_id}:
        raise HoldoutAccessError("campaign_id del intento no coincide")
    if scope.get("calendar_digest") not in {None, calendar_digest}:
        raise HoldoutAccessError("calendar_hash del intento no coincide")
    if scope.get("cost_digest") not in {None, cost_digest}:
        raise HoldoutAccessError("cost_identity del intento no coincide")
    if scope.get("window_start") != HOLDOUT_WINDOW_START or scope.get("window_end") != HOLDOUT_WINDOW_END:
        raise HoldoutAccessError("el intento no está ligado a la ventana 2024-01-01..2026-01-01")


def _check_holdout_dataset(protocol: ResearchProtocol, manifest: DatasetManifest, dataset_hash: str) -> None:
    # DatasetManifest is deliberately limited to the acquired HistData
    # 201603 partition.  A forged/future-looking coverage range on that
    # contract must not turn the March fixture into a 2024 holdout dataset.
    if isinstance(manifest, DatasetManifest):
        raise HoldoutAccessError(
            "holdout 2024-2025 no está habilitado: DatasetManifest sólo soporta HistData marzo-2016"
        )
    if not _verified_sha(dataset_hash):
        raise HoldoutAccessError("dataset content_hash no está verificado SHA-256")
    if protocol.holdout_locked is not True or protocol.holdout_open is True:
        raise HoldoutAccessError("el protocolo de holdout no está cerrado/congelado")
    if manifest.coverage_start is None or manifest.coverage_end is None:
        raise HoldoutAccessError("dataset coverage incompleta")
    validation = validate_dataset(manifest)
    if validation.ok is not True or validation.content_hash != dataset_hash:
        raise HoldoutAccessError("dataset raw/hash no pudo verificarse antes del holdout")
    if (
        manifest.coverage_start.year > protocol.holdout_start_year
        or manifest.coverage_end.year < protocol.holdout_end_year
    ):
        raise HoldoutAccessError("dataset no cubre íntegramente el holdout 2024-2025")


def _prior_campaign_access(records: Sequence[Mapping[str, Any]], campaign_id: str, dataset_hash: str) -> bool:
    return any(
        record.get("event") == "CONFIRMATORY_ACCESS_ISSUED"
        and (
            record.get("campaign_id") == campaign_id
            or (record.get("campaign_id") is None and record.get("dataset_hash") == dataset_hash)
        )
        for record in records
    )


def open_confirmatory_access(
    protocol: ResearchProtocol | str | Path,
    manifest: DatasetManifest | str | Path,
    registry: GlobalTrialRegistry | str | Path,
    *,
    attempt_id: str,
    calendar_hash: str | Mapping[str, Any],
    runtime_identity: Mapping[str, Any],
    cost_identity: Mapping[str, Any],
    assumptions_model_id: str | None = None,
    calendar_model_id: str | None = None,
    contract_spec: Mapping[str, Any] | None = None,
    calendar_state: Mapping[str, Any] | None = None,
) -> ConfirmatoryAccess:
    """Issue exactly one holdout capability after all identities are frozen."""

    selected_protocol = _load_protocol(protocol)
    selected_manifest = _load_manifest(manifest)
    selected_registry = _registry(registry)
    dataset_identity = _manifest_identity(selected_manifest)
    dataset_hash = str(dataset_identity["content_hash"])
    calendar_digest, calendar_material = _calendar_identity(calendar_hash)
    runtime = _runtime_identity(runtime_identity)
    costs = _cost_identity(cost_identity)
    binding = _resolve_assumption_binding(
        assumptions_model_id=assumptions_model_id,
        calendar_model_id=calendar_model_id,
        contract_spec=contract_spec,
        calendar_state=calendar_state,
    )
    _assert_frozen_identities(runtime=runtime, costs=costs, calendar_digest=calendar_digest, calendar=calendar_material)
    if binding.calendar is not None or binding.assumptions is not None:
        raise HoldoutAccessError("los modelos de assumptions/calendar son MODELED y nunca congelan holdout")
    records = selected_registry.records()
    campaign_id = _campaign_key(dataset_identity, selected_protocol)
    _check_holdout_dataset(selected_protocol, selected_manifest, dataset_hash)
    _check_access_registration(
        records,
        attempt_id=attempt_id,
        protocol_hash=selected_protocol.protocol_hash,
        dataset_hash=dataset_hash,
        runtime=runtime,
        costs=costs,
        calendar_digest=calendar_digest,
        campaign_id=campaign_id,
    )
    if not _attempts_match_binding(
        selected_registry,
        {"candidate": attempt_id},
        binding=binding,
        calendar_digest=calendar_digest,
        cost_identity=costs,
        scenarios=None,
        runtime_identity=runtime,
        run_parameters={"start": HOLDOUT_WINDOW_START, "end": HOLDOUT_WINDOW_END},
    ):
        raise HoldoutAccessError("assumptions/calendar/contract identity del intento no coincide")
    if _prior_campaign_access(records, campaign_id, dataset_hash):
        raise HoldoutAccessError("el acceso confirmatorio ya fue emitido")
    capability = secrets.token_urlsafe(32)
    access = ConfirmatoryAccess(
        attempt_id,
        selected_protocol.protocol_hash,
        dataset_hash,
        capability,
        _now(),
        False,
        campaign_id,
    )
    # Hashes are recorded, never credentials.  The explicitly supplied
    # runtime/cost/calendar identities bind the capability to this gate.
    try:
        selected_registry.append(
            {
                "event": "CONFIRMATORY_ACCESS_ISSUED",
                "attempt_id": attempt_id,
                "status": "RUNNING",
                "protocol_hash": selected_protocol.protocol_hash,
                "dataset_hash": dataset_hash,
                "campaign_id": campaign_id,
                "access_digest": access.access_digest,
                "calendar_digest": calendar_digest,
                "runtime_digest": _identity_hash(runtime),
                "cost_digest": _identity_hash(costs),
                "issued_at": access.issued_at,
            },
            expected_revision=len(records),
        )
    except RegistryConflict as exc:
        raise HoldoutAccessError("acceso confirmatorio disputado; no se reintenta") from exc
    return access


def consume_confirmatory_access(
    access: ConfirmatoryAccess | str,
    registry: GlobalTrialRegistry | str | Path,
    *,
    protocol_hash: str,
    dataset_hash: str,
) -> ConfirmatoryAccess:
    """Atomically mark one capability consumed; reuse is rejected."""

    selected_registry = _registry(registry)
    records = selected_registry.records()
    if isinstance(access, ConfirmatoryAccess):
        attempt_id = access.attempt_id
        capability = access.capability
        issued_protocol = access.protocol_hash
        issued_dataset = access.dataset_hash
    else:
        raise HoldoutAccessError(
            "el acceso confirmatorio debe conservar ConfirmatoryAccess; no se acepta token desnudo"
        )
    if access.used or issued_protocol != protocol_hash or issued_dataset != dataset_hash:
        raise HoldoutAccessError("identidad de acceso confirmatorio inválida")
    digest = access.access_digest
    issued = [
        record
        for record in records
        if record.get("attempt_id") == attempt_id
        and record.get("event") == "CONFIRMATORY_ACCESS_ISSUED"
        and record.get("access_digest") == digest
    ]
    if not issued:
        raise HoldoutAccessError("acceso confirmatorio no emitido por este registry")
    if any(
        record.get("attempt_id") == attempt_id and record.get("event") == "CONFIRMATORY_ACCESS_CONSUMED"
        for record in records
    ):
        raise HoldoutAccessError("acceso confirmatorio ya consumido")
    revision = len(records)
    try:
        selected_registry.append(
            {
                "event": "CONFIRMATORY_ACCESS_CONSUMED",
                "attempt_id": attempt_id,
                "status": "RUNNING",
                "protocol_hash": protocol_hash,
                "dataset_hash": dataset_hash,
                "access_digest": digest,
                "campaign_id": issued[0].get("campaign_id", ""),
                "consumed_at": _now(),
            },
            expected_revision=revision,
        )
    except RegistryConflict as exc:
        raise HoldoutAccessError("acceso confirmatorio disputado; no se reintenta") from exc
    return ConfirmatoryAccess(
        attempt_id,
        issued_protocol,
        issued_dataset,
        capability,
        str(issued[0].get("issued_at", "")),
        used=True,
        campaign_id=str(issued[0].get("campaign_id", "")),
    )


def register_campaign(
    protocol: ResearchProtocol | str | Path,
    manifest: DatasetManifest | str | Path,
    registry: GlobalTrialRegistry | str | Path,
    *,
    runtime_identity: Mapping[str, Any],
    calendar_hash: str | Mapping[str, Any],
    cost_identity: Mapping[str, Any],
    assumptions_model_id: str | None = None,
    calendar_model_id: str | None = None,
    contract_spec: Mapping[str, Any] | None = None,
    calendar_state: Mapping[str, Any] | None = None,
    candidates: Sequence[str] | None = None,
    scenarios: Sequence[str] | None = None,
    stage: str = "development",
    mode: str = "HISTORICAL",
    scope: Mapping[str, Any] | None = None,
    parameters: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Register candidate attempts before a runner is allowed to emit results."""

    selected_protocol = _load_protocol(protocol)
    selected_manifest = _load_manifest(manifest)
    selected_registry = _registry(registry)
    selected_stage = _stage(stage)
    selected_candidates = _candidate_selection(selected_protocol, candidates)
    requested_scope = dict(scope or {})
    selected_scenarios = _scenarios(scenarios or requested_scope.get("scenarios"))
    runtime = _runtime_identity(runtime_identity)
    costs = _cost_identity(cost_identity)
    calendar_digest, calendar_material = _calendar_identity(calendar_hash)
    binding = _resolve_assumption_binding(
        assumptions_model_id=assumptions_model_id,
        calendar_model_id=calendar_model_id,
        contract_spec=contract_spec,
        calendar_state=calendar_state,
    )
    identity = _manifest_identity(selected_manifest)
    dataset_hash = str(identity["content_hash"])
    campaign_id = _campaign_key(identity, selected_protocol)
    requested_campaign = requested_scope.get("campaign_id")
    if requested_campaign is not None and str(requested_campaign) != campaign_id:
        raise MarketResearchError("campaign_id debe ser el identificador congelado del holdout")
    extra_scope = {
        key: value
        for key, value in requested_scope.items()
        if key not in {"stage", "campaign_id", "calendar_digest", "cost_digest", "scenarios"}
    }
    shared_scope = {
        "stage": selected_stage,
        "campaign_id": campaign_id,
        "calendar_digest": calendar_digest,
        "cost_digest": _identity_hash(costs),
        "scenarios": list(selected_scenarios),
        "assumptions_model_id": binding.assumptions_model_id,
        "assumption_hash": binding.assumption_hash,
        "calendar_model_id": binding.calendar_model_id,
        "calendar_model_hash": binding.calendar_hash,
        "binding_hash": binding.binding_hash,
        "contract_spec_hash": _identity_hash(binding.contract_spec),
        "calendar_state_hash": _identity_hash(binding.calendar_state),
        **extra_scope,
    }
    if selected_stage == "holdout":
        if requested_scope.get("window_start", HOLDOUT_WINDOW_START) != HOLDOUT_WINDOW_START:
            raise MarketResearchError("holdout sólo admite window_start=2024-01-01T00:00:00Z")
        if requested_scope.get("window_end", HOLDOUT_WINDOW_END) != HOLDOUT_WINDOW_END:
            raise MarketResearchError("holdout sólo admite window_end=2026-01-01T00:00:00Z")
        shared_scope.update({"window_start": HOLDOUT_WINDOW_START, "window_end": HOLDOUT_WINDOW_END})
    shared_parameters: dict[str, Any] = {
        "calendar": calendar_material,
        "cost_identity": costs,
        "assumption_binding": binding.to_dict(),
    }
    if selected_stage == "holdout":
        shared_parameters.update({"start": HOLDOUT_WINDOW_START, "end": HOLDOUT_WINDOW_END})
    extra_parameters = {
        key: value
        for key, value in (parameters or {}).items()
        if key not in {"candidate", "calendar", "cost_identity", "assumption_binding"}
    }
    shared_parameters.update(extra_parameters)
    records: list[dict[str, Any]] = []
    for candidate_id in selected_candidates:
        records.append(
            selected_registry.register_attempt(
                candidate_id=candidate_id,
                protocol_hash=selected_protocol.protocol_hash,
                dataset_hash=dataset_hash,
                runtime_identity=runtime,
                data_identity=identity,
                scope={"candidate_id": candidate_id, **shared_scope},
                mode=mode,
                parameters={"candidate": selected_protocol.candidate(candidate_id).to_dict(), **shared_parameters},
            )
        )
    return {
        "schema": MARKET_RESEARCH_SCHEMA,
        "operation": "REGISTER",
        "protocol_hash": selected_protocol.protocol_hash,
        "dataset": identity,
        "campaign_id": campaign_id,
        "assumption_binding": binding.to_dict(),
        "stage": selected_stage,
        "scenarios": list(selected_scenarios),
        "candidate_ids": list(selected_candidates),
        "records": records,
        "registry_revision": selected_registry.revision,
        "registered_before_results": True,
        "holdout": "CLOSED",
        "auto_promote": False,
        "trading_enabled": False,
    }


def init_campaign(
    *,
    protocol_path: str | Path,
    registry_path: str | Path,
) -> dict[str, Any]:
    """Create the protocol and an empty private registry exactly once."""

    protocol_target = write_default_protocol(protocol_path)
    registry = GlobalTrialRegistry(registry_path)
    protocol = read_protocol(protocol_target)
    return {
        "schema": MARKET_RESEARCH_SCHEMA,
        "operation": "INIT",
        "protocol_path": str(protocol_target),
        "protocol_hash": protocol.protocol_hash,
        "registry_path": str(registry.path),
        "registry_revision": registry.revision,
        "holdout": {"locked": protocol.holdout_locked, "open": protocol.holdout_open, "access": "SINGLE"},
        "gates_remaining": list(INITIAL_GATES),
        "auto_promote": False,
        "trading_enabled": False,
    }


def _date_window(
    manifest: DatasetManifest,
    start: datetime | str | None,
    end: datetime | str | None,
) -> tuple[datetime | None, datetime | None]:
    parsed_start = _aware(start, name="start") if start is not None else None
    parsed_end = _aware(end, name="end") if end is not None else None
    if parsed_start is not None and parsed_end is not None and parsed_end <= parsed_start:
        raise MarketResearchError("end debe ser posterior a start")
    if parsed_start is not None and manifest.coverage_end is not None and parsed_start > manifest.coverage_end:
        raise MarketResearchError("start no intersecta la cobertura del dataset")
    if parsed_end is not None and manifest.coverage_start is not None and parsed_end <= manifest.coverage_start:
        raise MarketResearchError("end no intersecta la cobertura del dataset")
    return parsed_start, parsed_end


def _default_stream(
    manifest: DatasetManifest, start: datetime | None, end: datetime | None
) -> Callable[[], Iterator[HistoricalQuote]]:
    def factory() -> Iterator[HistoricalQuote]:
        yield from iter_quotes(manifest, start, end)

    return factory


def _invoke_runner(runner: Callable[..., Any], kwargs: Mapping[str, Any]) -> Any:
    """Pass only supported keyword arguments to custom runner adapters."""

    try:
        signature = inspect.signature(runner)
    except (TypeError, ValueError):
        return runner(**kwargs)
    parameters = signature.parameters
    if any(parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in parameters.values()):
        return runner(**kwargs)
    accepted = {
        name: value
        for name, value in kwargs.items()
        if name in parameters
        and parameters[name].kind in {inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY}
    }
    return runner(**accepted)


def _coerce_runner(runner: Any) -> Callable[..., Any]:
    method = getattr(runner, "run_historical_backtest", None)
    if callable(method):
        return cast(Callable[..., Any], method)
    if callable(runner):
        return cast(Callable[..., Any], runner)
    raise MarketResearchError("runner debe ser callable o exponer run_historical_backtest")


_HOLDOUT_PERMISSION_PARAMETER_NAMES = ("holdout_permission", "holdout_access")


def _holdout_permission_parameter(runner: Any) -> str:
    """Return the explicit non-secret permission parameter of a runner."""

    callable_runner = _coerce_runner(runner)
    try:
        parameters = inspect.signature(callable_runner).parameters
    except (TypeError, ValueError) as exc:
        raise HoldoutAccessError("runner holdout sin firma verificable") from exc
    for name in _HOLDOUT_PERMISSION_PARAMETER_NAMES:
        parameter = parameters.get(name)
        if parameter is not None and parameter.kind in {
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        }:
            return name
    raise HoldoutAccessError("runner holdout debe aceptar explícitamente holdout_permission/holdout_access")


def _assert_holdout_runner(stage: str, runner: Any) -> None:
    if stage != "holdout":
        return
    # The current owner historical runner intentionally rejects 2024+ and has
    # no typed permission seam.  Do not let a custom callable bypass either
    # fact by merely receiving stage/candidate kwargs.
    if runner is None:
        raise HoldoutAccessError("holdout permanece cerrado: no hay runner con permiso tipado")
    _holdout_permission_parameter(runner)


def _scenario_parameters(scenario: str) -> dict[str, str]:
    try:
        return dict(SCENARIO_PARAMETERS[scenario])
    except KeyError as exc:
        raise MarketResearchError(f"scenario no soportado: {scenario!r}") from exc


def _official_scenario_config(
    protocol: ResearchProtocol,
    scenario: str,
    *,
    candidates: Sequence[str],
    assumptions_model: HistoricalAssumptionsModel | None,
    calendar_model: HistoricalCalendarTemplate | None,
    contract_spec: Mapping[str, Any] | None,
    calendar_state: Mapping[str, Any] | None,
) -> Any:
    """Build one typed scenario through the owner runner's canonical factory."""

    from .historical_backtest import HistoricalBacktestConfig

    return HistoricalBacktestConfig.from_protocol(
        protocol,
        scenario=scenario,
        candidates=candidates,
        assumptions_model=assumptions_model,
        calendar_model=calendar_model,
        contract_spec=contract_spec,
        calendar_state=calendar_state,
    )


def _result_mapping(value: Any) -> dict[str, Any]:
    mapped = _mapping(value)
    if mapped:
        return cast(dict[str, Any], _jsonable(dict(mapped)))
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return {"episodes": [_jsonable(item) for item in value]}
    raise MarketResearchError("runner devolvió un resultado no serializable")


def _artifact_sha256(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    rows = 0
    with path.open("rb") as stream:
        for line in stream:
            digest.update(line)
            if line.strip():
                rows += 1
    return digest.hexdigest(), rows


def _snapshot_reference(value: Any, *, kind: str) -> dict[str, Any] | None:
    raw = _mapping(value)
    path_value = raw.get("path")
    if path_value is None:
        return None
    reference: dict[str, Any] = {
        "kind": kind,
        "path_reference": str(path_value),
        "pager": {"page_size": 256, "source": "complete_jsonl_artifact"},
        "retained_in_memory": 0,
    }
    try:
        target = _safe_input_file(path_value, name=f"{kind} artifact")
        actual_hash, actual_rows = _artifact_sha256(target)
    except (MarketResearchError, OSError) as exc:
        reference.update({"status": "NOT_ASSESSED", "reason": type(exc).__name__})
        return reference
    supplied_hash = raw.get("sha256", raw.get("content_hash", raw.get("hash")))
    reference.update(
        {
            "status": "ASSESSED",
            "sha256": actual_hash,
            "row_count": actual_rows,
            "complete": True,
        }
    )
    if supplied_hash is not None and str(supplied_hash) != actual_hash:
        reference.update({"status": "INVALID", "reason": "artifact_hash_mismatch"})
    declared = raw.get("count", raw.get("row_count"))
    if declared is not None and str(declared) != str(actual_rows):
        reference.update({"status": "INVALID", "reason": "artifact_row_count_mismatch"})
    return reference


def _snapshot_references(result: Mapping[str, Any]) -> dict[str, Any]:
    raw_artifacts = result.get("artifacts")
    refs: dict[str, Any] = {}
    if isinstance(raw_artifacts, Mapping):
        for kind, value in raw_artifacts.items():
            reference = _snapshot_reference(value, kind=str(kind))
            if reference is not None:
                refs[str(kind)] = reference
    for kind in ("ledger", "equity", "funnel", "episodes"):
        if kind in refs:
            continue
        reference = _snapshot_reference(result.get(kind), kind=kind)
        if reference is not None:
            refs[kind] = reference
    return refs


def _parse_snapshot_line(line: str, line_number: int) -> Mapping[str, Any] | None:
    if not line.strip():
        return None
    try:
        value = json.loads(line)
    except json.JSONDecodeError as exc:
        raise MarketResearchError(f"snapshot JSON inválido en línea {line_number}") from exc
    if not isinstance(value, Mapping):
        raise MarketResearchError(f"snapshot fila inválida en línea {line_number}")
    return value


def _snapshot_pages(target: Path, page_size: int) -> Iterator[tuple[Mapping[str, Any], ...]]:
    page: list[Mapping[str, Any]] = []
    try:
        with target.open("r", encoding="utf-8") as stream:
            for line_number, line in enumerate(stream, start=1):
                value = _parse_snapshot_line(line, line_number)
                if value is None:
                    continue
                page.append(value)
                if len(page) >= page_size:
                    yield tuple(page)
                    page.clear()
    except UnicodeDecodeError as exc:
        raise MarketResearchError("snapshot no es UTF-8") from exc
    if page:
        yield tuple(page)


def iter_snapshot_pages(
    reference: Mapping[str, Any], *, page_size: int = 256
) -> Iterator[tuple[Mapping[str, Any], ...]]:
    """Page a hashed complete JSONL snapshot without retaining the stream."""

    if not isinstance(reference, Mapping) or reference.get("status") != "ASSESSED":
        raise MarketResearchError("snapshot no está verificado")
    raw_path = reference.get("path_reference")
    if raw_path is None:
        raise MarketResearchError("snapshot carece de path_reference")
    if isinstance(page_size, bool) or not isinstance(page_size, int) or page_size <= 0:
        raise MarketResearchError("page_size debe ser entero positivo")
    target = _safe_input_file(raw_path, name="snapshot artifact")
    actual_hash, actual_count = _artifact_sha256(target)
    if reference.get("sha256") != actual_hash:
        raise MarketResearchError("snapshot hash mismatch")
    if reference.get("row_count") is not None and str(reference["row_count"]) != str(actual_count):
        raise MarketResearchError("snapshot row_count mismatch")
    yield from _snapshot_pages(target, page_size)


def _variant_rows(payload: Mapping[str, Any], candidate_ids: Sequence[str]) -> dict[str, dict[str, Any]]:
    raw = payload.get("variants")
    variants: list[Mapping[str, Any]] = []
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
        variants = [item for item in raw if isinstance(item, Mapping)]
    result: dict[str, dict[str, Any]] = {}
    for candidate_id in candidate_ids:
        matching = next(
            (item for item in variants if str(item.get("candidate_id", item.get("variant", ""))) == candidate_id),
            None,
        )
        if matching is not None:
            item = dict(payload)
            item.update(dict(matching))
            result[candidate_id] = item
            continue
        if str(payload.get("candidate_id", payload.get("variant", ""))) == candidate_id:
            result[candidate_id] = dict(payload)
    if not result and len(candidate_ids) == 1:
        result[candidate_ids[0]] = dict(payload)
    return result


def _decorate_result(
    result: Mapping[str, Any],
    *,
    candidate_id: str,
    stage: str,
    scenario: str,
    attempt_id: str,
    protocol: ResearchProtocol,
    dataset_identity: Mapping[str, Any],
    runtime_identity: Mapping[str, Any],
    cost_identity: Mapping[str, Any],
    calendar_digest: str,
    assumption_binding: _AssumptionBinding,
) -> dict[str, Any]:
    decorated = dict(_jsonable(dict(result)))
    candidate = protocol.candidate(candidate_id)
    metrics = _mapping(result.get("metrics"))
    scenario_applied = result.get("scenario_applied") is True or metrics.get("scenario_applied") is True
    if not scenario_applied:
        decorated["status"] = "NOT_ASSESSED"
        decorated["execution_state"] = "NOT_ASSESSED_SCENARIO_NOT_VERIFIED"
        decorated["execution_reason"] = "runner_no_confirmed_scenario_application"
    decorated.update(
        {
            "schema": RUN_SCHEMA,
            "candidate_id": candidate_id,
            "candidate": candidate.to_dict(),
            "stage": stage,
            "scenario": scenario,
            "attempt_id": attempt_id,
            "protocol_hash": protocol.protocol_hash,
            "dataset": dict(dataset_identity),
            "dataset_hash": dataset_identity["content_hash"],
            "runtime_identity_hash": _identity_hash(runtime_identity),
            "cost_identity_hash": _identity_hash(cost_identity),
            "calendar_hash": calendar_digest,
            "assumption_binding": assumption_binding.to_dict(),
            "assumptions_model_id": assumption_binding.assumptions_model_id,
            "assumption_hash": assumption_binding.assumption_hash,
            "calendar_model_id": assumption_binding.calendar_model_id,
            "calendar_model_hash": assumption_binding.calendar_hash,
            "scenario_parameters": _scenario_parameters(scenario),
            "scenario_applied": scenario_applied,
            "snapshot": _snapshot_references(result),
            "holdout_access": "CONFIRMATORY_SINGLE_USE" if stage == "holdout" else "CLOSED",
            "auto_promote": False,
            "trading_enabled": False,
        }
    )
    return decorated


def _attempts_for_stage(
    registry: GlobalTrialRegistry,
    *,
    protocol_hash: str,
    dataset_hash: str,
    stage: str,
    candidate_ids: Sequence[str],
) -> dict[str, str]:
    found: dict[str, str] = {}
    records = registry.records()
    for record in records:
        if record.get("event") != "ATTEMPT_REGISTERED":
            continue
        scope: Mapping[str, Any] = (
            cast(Mapping[str, Any], record.get("scope")) if isinstance(record.get("scope"), Mapping) else {}
        )
        if (
            record.get("protocol_hash") == protocol_hash
            and record.get("dataset_hash") == dataset_hash
            and scope.get("stage") == stage
            and record.get("candidate_id") in candidate_ids
        ):
            found[str(record["candidate_id"])] = str(record["attempt_id"])
    return found


def _attempts_support_scenarios(
    registry: GlobalTrialRegistry, attempts: Mapping[str, str], scenarios: Sequence[str]
) -> bool:
    by_id = {
        str(record.get("attempt_id")): record
        for record in registry.records()
        if record.get("event") == "ATTEMPT_REGISTERED"
    }
    for attempt_id in attempts.values():
        record = by_id.get(attempt_id)
        scope = record.get("scope") if isinstance(record, Mapping) else None
        declared = scope.get("scenarios") if isinstance(scope, Mapping) else None
        if not isinstance(declared, Sequence) or isinstance(declared, (str, bytes, bytearray)):
            return False
        if not set(scenarios).issubset({str(value) for value in declared}):
            return False
    return True


def _attempts_match_binding(
    registry: GlobalTrialRegistry,
    attempts: Mapping[str, str],
    *,
    binding: _AssumptionBinding,
    calendar_digest: str,
    cost_identity: Mapping[str, Any],
    scenarios: Sequence[str] | None,
    runtime_identity: Mapping[str, Any] | None = None,
    run_parameters: Mapping[str, Any] | None = None,
) -> bool:
    by_id = {
        str(record.get("attempt_id")): record
        for record in registry.records()
        if record.get("event") == "ATTEMPT_REGISTERED"
    }
    for attempt_id in attempts.values():
        record = by_id.get(attempt_id)
        if not _record_matches_binding(
            record,
            binding=binding,
            calendar_digest=calendar_digest,
            cost_identity=cost_identity,
            scenarios=scenarios,
            runtime_identity=runtime_identity,
            run_parameters=run_parameters,
        ):
            return False
    return True


def _record_matches_binding(
    record: Mapping[str, Any] | None,
    *,
    binding: _AssumptionBinding,
    calendar_digest: str,
    cost_identity: Mapping[str, Any],
    scenarios: Sequence[str] | None,
    runtime_identity: Mapping[str, Any] | None,
    run_parameters: Mapping[str, Any] | None,
) -> bool:
    if record is None:
        return False
    scope = record.get("scope")
    if not isinstance(scope, Mapping):
        return False
    expected_scope = {
        "calendar_digest": calendar_digest,
        "cost_digest": _identity_hash(cost_identity),
        "assumptions_model_id": binding.assumptions_model_id,
        "assumption_hash": binding.assumption_hash,
        "calendar_model_id": binding.calendar_model_id,
        "calendar_model_hash": binding.calendar_hash,
        "binding_hash": binding.binding_hash,
        "contract_spec_hash": _identity_hash(binding.contract_spec),
        "calendar_state_hash": _identity_hash(binding.calendar_state),
    }
    if any(scope.get(key) != value for key, value in expected_scope.items()):
        return False
    if runtime_identity is not None and record.get("runtime_identity") != _jsonable(dict(runtime_identity)):
        return False
    declared = scope.get("scenarios")
    if not isinstance(declared, Sequence) or isinstance(declared, (str, bytes, bytearray)):
        return False
    if scenarios is not None and set(scenarios) != {str(value) for value in declared}:
        return False
    record_parameters = record.get("parameters")
    if not isinstance(record_parameters, Mapping) or record_parameters.get("assumption_binding") != binding.to_dict():
        return False
    if run_parameters is not None:
        expected_parameters = _jsonable(dict(run_parameters))
        if any(record_parameters.get(key) != value for key, value in expected_parameters.items()):
            return False
    return True


def _mark_attempts(
    registry: GlobalTrialRegistry, attempts: Mapping[str, str], status: str, details: Mapping[str, Any]
) -> None:
    for attempt_id in attempts.values():
        try:
            registry.update_status(attempt_id, status, details=details)
        except RegistryError:
            # Preserve the runner result; validation exposes any registry
            # inconsistency rather than silently claiming a clean close.
            continue


def _discard_sink(_kind: str, _row: Mapping[str, Any]) -> None:
    return None


def _run_owner_runner(
    runner: Callable[..., Any] | None,
    *,
    quotes_or_factory: Iterable[HistoricalQuote] | Callable[[], Iterator[HistoricalQuote]],
    manifest: DatasetManifest,
    protocol: ResearchProtocol,
    output_dir: Path,
    sink: Any,
    candidate_id: str,
    candidates: Sequence[str] | None,
    stage: str,
    scenario: str,
    runtime_identity: Mapping[str, Any],
    cost_identity: Mapping[str, Any],
    calendar_hash: str,
    assumptions_model: HistoricalAssumptionsModel | None,
    calendar_model: HistoricalCalendarTemplate | None,
    contract_spec: Mapping[str, Any] | None,
    calendar_state: Mapping[str, Any] | None,
    start: datetime | None,
    end: datetime | None,
    resume: Any,
    holdout_permission: HoldoutExecutionPermission | None,
) -> Any:
    if runner is None:
        if stage == "holdout":
            raise HoldoutAccessError("holdout permanece cerrado: runner owner sin permiso tipado")
        from .historical_backtest import HistoricalBacktestConfig, run_historical_backtest

        config = _official_scenario_config(
            protocol,
            scenario,
            candidates=candidates or (candidate_id,),
            assumptions_model=assumptions_model,
            calendar_model=calendar_model,
            contract_spec=contract_spec,
            calendar_state=calendar_state,
        )
        assert isinstance(config, HistoricalBacktestConfig)
        return run_historical_backtest(
            quotes_or_factory,
            manifest=manifest,
            config=config,
            output_dir=output_dir,
            sink=sink,
            resume=resume,
        )
    callable_runner = _coerce_runner(runner)
    if stage == "holdout" and not isinstance(holdout_permission, HoldoutExecutionPermission):
        raise HoldoutAccessError("runner holdout requiere permiso tipado/verificado")
    return _invoke_runner(
        callable_runner,
        {
            "quotes_or_factory": quotes_or_factory,
            "quotes": quotes_or_factory,
            "manifest": manifest,
            "dataset": manifest,
            "protocol": protocol,
            "candidate_id": candidate_id,
            "stage": stage,
            "scenario": scenario,
            "scenario_parameters": _scenario_parameters(scenario),
            "runtime_identity": runtime_identity,
            "cost_identity": cost_identity,
            "calendar_hash": calendar_hash,
            "assumptions_model": assumptions_model,
            "calendar_model": calendar_model,
            "contract_spec": contract_spec,
            "calendar_state": calendar_state,
            "output_dir": output_dir,
            "sink": sink,
            "resume": resume,
            "resume_checkpoint": resume,
            "start": start,
            "end": end,
            "holdout_permission": holdout_permission,
            "holdout_access": holdout_permission,
        },
    )


def _execute_scenarios(
    *,
    runner: Callable[..., Any] | Any | None,
    stage: str,
    scenarios: Sequence[str],
    candidates: Sequence[str],
    attempts: Mapping[str, str],
    protocol: ResearchProtocol,
    manifest: DatasetManifest,
    dataset_identity: Mapping[str, Any],
    runtime_identity: Mapping[str, Any],
    cost_identity: Mapping[str, Any],
    calendar_digest: str,
    assumption_binding: _AssumptionBinding,
    assumptions_model: HistoricalAssumptionsModel | None,
    calendar_model: HistoricalCalendarTemplate | None,
    contract_spec: Mapping[str, Any] | None,
    calendar_state: Mapping[str, Any] | None,
    output_dir: Path,
    run_id: str,
    quotes_or_factory: Iterable[HistoricalQuote] | Callable[[], Iterator[HistoricalQuote]] | None,
    start: datetime | None,
    end: datetime | None,
    resume: Any,
    holdout_permission: HoldoutExecutionPermission | None,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for scenario in scenarios:
        scenario_dir = _safe_output_dir(output_dir / f"{run_id}-{scenario}")
        stream = quotes_or_factory if quotes_or_factory is not None else _default_stream(manifest, start, end)
        if runner is None:
            owner_result = _run_owner_runner(
                None,
                quotes_or_factory=stream,
                manifest=manifest,
                protocol=protocol,
                output_dir=scenario_dir,
                sink=_discard_sink,
                candidate_id=candidates[0],
                candidates=candidates,
                stage=stage,
                scenario=scenario,
                runtime_identity=runtime_identity,
                cost_identity=cost_identity,
                calendar_hash=calendar_digest,
                assumptions_model=assumptions_model,
                calendar_model=calendar_model,
                contract_spec=contract_spec,
                calendar_state=calendar_state,
                start=start,
                end=end,
                resume=resume,
                holdout_permission=holdout_permission,
            )
            payload = _result_mapping(owner_result)
            split = _variant_rows(payload, candidates)
            for candidate_id in candidates:
                candidate_result = split.get(
                    candidate_id,
                    {
                        "candidate_id": candidate_id,
                        "status": "NOT_ASSESSED",
                        "reason": "candidate_result_missing",
                        "scenario_applied": False,
                    },
                )
                results.append(
                    _decorate_result(
                        candidate_result,
                        candidate_id=candidate_id,
                        stage=stage,
                        scenario=scenario,
                        attempt_id=attempts[candidate_id],
                        protocol=protocol,
                        dataset_identity=dataset_identity,
                        runtime_identity=runtime_identity,
                        cost_identity=cost_identity,
                        calendar_digest=calendar_digest,
                        assumption_binding=assumption_binding,
                    )
                )
            continue
        callable_runner = _coerce_runner(runner)
        for candidate_id in candidates:
            owner_result = _run_owner_runner(
                callable_runner,
                quotes_or_factory=stream,
                manifest=manifest,
                protocol=protocol,
                output_dir=scenario_dir,
                sink=_discard_sink,
                candidate_id=candidate_id,
                candidates=(candidate_id,),
                stage=stage,
                scenario=scenario,
                runtime_identity=runtime_identity,
                cost_identity=cost_identity,
                calendar_hash=calendar_digest,
                assumptions_model=assumptions_model,
                calendar_model=calendar_model,
                contract_spec=contract_spec,
                calendar_state=calendar_state,
                start=start,
                end=end,
                resume=resume,
                holdout_permission=holdout_permission,
            )
            results.append(
                _decorate_result(
                    _result_mapping(owner_result),
                    candidate_id=candidate_id,
                    stage=stage,
                    scenario=scenario,
                    attempt_id=attempts[candidate_id],
                    protocol=protocol,
                    dataset_identity=dataset_identity,
                    runtime_identity=runtime_identity,
                    cost_identity=cost_identity,
                    calendar_digest=calendar_digest,
                    assumption_binding=assumption_binding,
                )
            )
    return results


def _prepare_holdout_access(
    *,
    stage: str,
    access: ConfirmatoryAccess | None,
    runtime: Mapping[str, Any],
    costs: Mapping[str, Any],
    calendar_digest: str,
    calendar_material: Mapping[str, Any],
    binding: _AssumptionBinding,
    registry: GlobalTrialRegistry,
    existing: Mapping[str, str],
    candidates: Sequence[str],
    scenarios: Sequence[str],
    protocol_hash: str,
    dataset_hash: str,
    dataset_id: str,
) -> HoldoutExecutionPermission | None:
    if stage != "holdout":
        return None
    if not isinstance(access, ConfirmatoryAccess):
        raise HoldoutAccessError("holdout requiere acceso confirmatorio único preemitido")
    _assert_frozen_identities(runtime=runtime, costs=costs, calendar_digest=calendar_digest, calendar=calendar_material)
    if binding.calendar is not None or binding.assumptions is not None:
        raise HoldoutAccessError("los modelos de assumptions/calendar son MODELED y nunca congelan holdout")
    if set(existing) != set(candidates):
        raise HoldoutAccessError("todos los candidatos deben estar preregistrados antes del holdout")
    if access.attempt_id not in set(existing.values()):
        raise HoldoutAccessError("el acceso confirmatorio no pertenece a los intentos seleccionados")
    if not _attempts_support_scenarios(registry, existing, scenarios):
        raise HoldoutAccessError("los escenarios deben estar preregistrados en la misma campaña")
    if not _attempts_match_binding(
        registry,
        existing,
        binding=binding,
        calendar_digest=calendar_digest,
        cost_identity=costs,
        scenarios=scenarios,
        runtime_identity=runtime,
        run_parameters={"start": HOLDOUT_WINDOW_START, "end": HOLDOUT_WINDOW_END},
    ):
        raise HoldoutAccessError("assumptions/calendar/contract identity no coincide con preregistro")
    consumed = consume_confirmatory_access(access, registry, protocol_hash=protocol_hash, dataset_hash=dataset_hash)
    attempt_candidates = {attempt_id: candidate_id for candidate_id, attempt_id in existing.items()}
    candidate_id = attempt_candidates.get(consumed.attempt_id)
    if candidate_id is None:
        raise HoldoutAccessError("el acceso consumido no pertenece a los candidatos seleccionados")
    return HoldoutExecutionPermission(
        attempt_id=consumed.attempt_id,
        candidate_id=candidate_id,
        attempt_ids=tuple(existing[candidate] for candidate in candidates),
        candidate_ids=tuple(candidates),
        dataset_id=dataset_id,
        dataset_hash=dataset_hash,
        protocol_hash=protocol_hash,
        scenarios=tuple(scenarios),
        access_digest=consumed.access_digest,
        campaign_id=consumed.campaign_id,
    )


def run_campaign(
    protocol: ResearchProtocol | str | Path,
    manifest: DatasetManifest | str | Path,
    registry: GlobalTrialRegistry | str | Path,
    *,
    output_dir: str | Path,
    stage: str,
    runtime_identity: Mapping[str, Any],
    calendar_hash: str | Mapping[str, Any],
    cost_identity: Mapping[str, Any],
    assumptions_model_id: str | None = None,
    calendar_model_id: str | None = None,
    contract_spec: Mapping[str, Any] | None = None,
    calendar_state: Mapping[str, Any] | None = None,
    candidates: Sequence[str] | None = None,
    scenarios: Sequence[str] | None = None,
    quotes_or_factory: Iterable[HistoricalQuote] | Callable[[], Iterator[HistoricalQuote]] | None = None,
    runner: Callable[..., Any] | Any | None = None,
    start: datetime | str | None = None,
    end: datetime | str | None = None,
    resume: Any = None,
    confirmatory_access: ConfirmatoryAccess | None = None,
) -> dict[str, Any]:
    """Run a bounded local stage and return a review-only campaign receipt.

    ``holdout`` is intentionally fail-closed.  The service consumes a
    pre-issued :class:`ConfirmatoryAccess` before invoking any runner and
    cannot issue a replacement after a runner error.
    """

    selected_protocol = _load_protocol(protocol)
    selected_manifest = _load_manifest(manifest)
    selected_registry = _registry(registry)
    selected_stage = _stage(stage)
    _assert_holdout_runner(selected_stage, runner)
    selected_candidates = _candidate_selection(selected_protocol, candidates)
    selected_scenarios = _scenarios(scenarios)
    runtime = _runtime_identity(runtime_identity)
    costs = _cost_identity(cost_identity)
    calendar_digest, calendar_material = _calendar_identity(calendar_hash)
    binding = _resolve_assumption_binding(
        assumptions_model_id=assumptions_model_id,
        calendar_model_id=calendar_model_id,
        contract_spec=contract_spec,
        calendar_state=calendar_state,
    )
    identity = _manifest_identity(selected_manifest)
    dataset_hash = str(identity["content_hash"])
    start_at, end_at = _date_window(selected_manifest, start, end)
    if selected_stage == "holdout":
        if start_at != _HOLDOUT_START or end_at != _HOLDOUT_END:
            raise HoldoutAccessError("holdout sólo admite la ventana 2024-01-01..2026-01-01")
        # Recheck at run time: an issued capability does not authorize
        # reinterpreting the current historical-data contract.
        _check_holdout_dataset(selected_protocol, selected_manifest, dataset_hash)
    existing = _attempts_for_stage(
        selected_registry,
        protocol_hash=selected_protocol.protocol_hash,
        dataset_hash=dataset_hash,
        stage=selected_stage,
        candidate_ids=selected_candidates,
    )
    access_receipt = _prepare_holdout_access(
        stage=selected_stage,
        access=confirmatory_access,
        runtime=runtime,
        costs=costs,
        calendar_digest=calendar_digest,
        calendar_material=calendar_material,
        binding=binding,
        registry=selected_registry,
        existing=existing,
        candidates=selected_candidates,
        scenarios=selected_scenarios,
        protocol_hash=selected_protocol.protocol_hash,
        dataset_hash=dataset_hash,
        dataset_id=str(identity["dataset_id"]),
    )
    holdout_permission = access_receipt
    if selected_stage == "holdout" and not isinstance(holdout_permission, HoldoutExecutionPermission):
        raise HoldoutAccessError("holdout no produjo un permiso tipado/verificado")
    access_receipt_dict = (
        holdout_permission.to_dict()
        if isinstance(holdout_permission, HoldoutExecutionPermission)
        else {"status": "CLOSED", "used": False}
    )
    if selected_stage != "holdout":
        registration = register_campaign(
            selected_protocol,
            selected_manifest,
            selected_registry,
            runtime_identity=runtime,
            calendar_hash=calendar_hash,
            cost_identity=costs,
            assumptions_model_id=assumptions_model_id,
            calendar_model_id=calendar_model_id,
            contract_spec=contract_spec,
            calendar_state=calendar_state,
            candidates=selected_candidates,
            scenarios=selected_scenarios,
            stage=selected_stage,
            parameters={"start": start_at, "end": end_at},
        )
        attempts = {
            str(record["candidate_id"]): str(record["attempt_id"])
            for record in registration["records"]
            if isinstance(record, Mapping)
        }
    else:
        attempts = existing

    _mark_attempts(selected_registry, attempts, "RUNNING", {"stage": selected_stage})
    target = _safe_output_dir(output_dir)
    run_id = uuid.uuid4().hex
    try:
        raw_results = _execute_scenarios(
            runner=runner,
            stage=selected_stage,
            scenarios=selected_scenarios,
            candidates=selected_candidates,
            attempts=attempts,
            protocol=selected_protocol,
            manifest=selected_manifest,
            dataset_identity=identity,
            runtime_identity=runtime,
            cost_identity=costs,
            calendar_digest=calendar_digest,
            assumption_binding=binding,
            assumptions_model=binding.assumptions,
            calendar_model=binding.calendar,
            contract_spec=binding.contract_spec,
            calendar_state=binding.calendar_state,
            output_dir=target,
            run_id=run_id,
            quotes_or_factory=quotes_or_factory,
            start=start_at,
            end=end_at,
            resume=resume,
            holdout_permission=holdout_permission
            if isinstance(holdout_permission, HoldoutExecutionPermission)
            else None,
        )
    except BaseException as exc:
        _mark_attempts(
            selected_registry,
            attempts,
            "FAILED",
            {"stage": selected_stage, "error": type(exc).__name__},
        )
        raise

    receipt: dict[str, Any] = {
        "schema": RUN_SCHEMA,
        "operation": "RUN",
        "run_id": run_id,
        "protocol_hash": selected_protocol.protocol_hash,
        "dataset": identity,
        "stage": selected_stage,
        "scenarios": list(selected_scenarios),
        "candidate_ids": list(selected_candidates),
        "results": raw_results,
        "confirmatory_access": access_receipt_dict,
        "registry_revision": selected_registry.revision,
        "registered_before_results": True,
        "holdout": "CONSUMED_SINGLE_USE" if selected_stage == "holdout" else "CLOSED",
        "auto_promote": False,
        "trading_enabled": False,
    }
    artifact = target / f"{run_id}.json"
    try:
        _exclusive_payload(artifact, receipt)
    except BaseException as exc:
        _mark_attempts(
            selected_registry,
            attempts,
            "FAILED",
            {"stage": selected_stage, "error": type(exc).__name__, "artifact": "NOT_COMMITTED"},
        )
        raise
    _mark_attempts(
        selected_registry,
        attempts,
        "COMPLETED",
        {"stage": selected_stage, "result_count": len(raw_results), "run_id": run_id},
    )
    receipt["artifact_path"] = str(artifact)
    return receipt


def _load_json_source(source: Mapping[str, Any] | str | Path) -> dict[str, Any]:
    if isinstance(source, Mapping):
        return cast(dict[str, Any], _jsonable(dict(source)))
    target = _safe_input_file(source, name="campaign artifact")
    try:
        value = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MarketResearchError(f"campaign artifact ilegible: {target}") from exc
    if not isinstance(value, Mapping):
        raise MarketResearchError("campaign artifact debe ser un objeto JSON")
    return cast(dict[str, Any], value)


def _result_by_candidate(payload: Mapping[str, Any], candidates: Sequence[str]) -> dict[str, Mapping[str, Any]]:
    raw = payload.get("results", payload.get("reports", ()))
    rows: list[Mapping[str, Any]] = []
    if isinstance(raw, Mapping):
        rows = [raw]
    elif isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
        rows = [item for item in raw if isinstance(item, Mapping)]
    grouped: dict[str, dict[str, Mapping[str, Any]]] = {}
    for row in rows:
        candidate_id = row.get("candidate_id", row.get("variant"))
        if candidate_id is not None and str(candidate_id) in candidates:
            grouped.setdefault(str(candidate_id), {})[str(row.get("scenario", "base"))] = row
    result: dict[str, Mapping[str, Any]] = {}
    for candidate_id, scenarios in grouped.items():
        base = scenarios.get("base") or next(iter(scenarios.values()))
        if len(scenarios) == 1:
            result[candidate_id] = base
            continue
        combined = dict(base)
        adverse = scenarios.get("adverse") or scenarios.get("extreme")
        if adverse is not None:
            combined["stress"] = dict(adverse)
        combined["scenario_results"] = {name: dict(value) for name, value in scenarios.items()}
        combined["scenario_set"] = sorted(scenarios)
        result[candidate_id] = combined
    return result


def report_campaign(
    source: Mapping[str, Any] | str | Path,
    *,
    protocol: ResearchProtocol | str | Path | None = None,
    output_dir: str | Path | None = None,
    criteria: Any | None = None,
) -> dict[str, Any]:
    """Build a bounded evidence report from an existing campaign artifact."""

    payload = _load_json_source(source)
    selected_protocol = _load_protocol(protocol)
    candidates = _result_by_candidate(payload, selected_protocol.candidate_ids)
    evidence = evaluate_candidates(candidates, protocol=selected_protocol, criteria=criteria)
    report: dict[str, Any] = {
        "schema": MARKET_RESEARCH_SCHEMA,
        "operation": "REPORT",
        "source_hash": _identity_hash(payload),
        "protocol_hash": selected_protocol.protocol_hash,
        "campaign": {
            "run_id": payload.get("run_id"),
            "stage": payload.get("stage"),
            "scenarios": payload.get("scenarios", []),
            "candidate_ids": list(candidates),
        },
        "evidence": evidence,
        "validation": validate_evidence(evidence),
        "auto_promote": False,
        "trading_enabled": False,
    }
    if output_dir is not None:
        target = _safe_output_dir(output_dir)
        artifact = target / f"evidence-report-{uuid.uuid4().hex}.json"
        _exclusive_payload(artifact, report)
        report["artifact_path"] = str(artifact)
    return report


def _validate_payload_results(payload: Mapping[str, Any], protocol: ResearchProtocol) -> list[str]:
    errors: list[str] = []
    results = payload.get("results")
    if not isinstance(results, Sequence) or isinstance(results, (str, bytes, bytearray)):
        return errors
    for index, item in enumerate(results):
        if not isinstance(item, Mapping):
            errors.append(f"result_not_mapping:{index}")
            continue
        if item.get("candidate_id") not in protocol.candidate_ids:
            errors.append(f"candidate_invalid:{index}")
        if item.get("protocol_hash") not in {None, protocol.protocol_hash}:
            errors.append(f"result_protocol_mismatch:{index}")
        if item.get("auto_promote") is not False or item.get("trading_enabled") is not False:
            errors.append(f"result_promotion_gate_invalid:{index}")
        snapshot = item.get("snapshot")
        if isinstance(snapshot, Mapping) and any(
            isinstance(reference, Mapping) and reference.get("status") == "INVALID" for reference in snapshot.values()
        ):
            errors.append(f"result_snapshot_invalid:{index}")
    return errors


def validate_campaign(
    source: Mapping[str, Any] | str | Path,
    *,
    protocol: ResearchProtocol | str | Path | None = None,
    manifest: DatasetManifest | str | Path | None = None,
    registry: GlobalTrialRegistry | str | Path | None = None,
) -> dict[str, Any]:
    """Validate identities, registry chain, and review-only promotion gates."""

    payload = _load_json_source(source)
    selected_protocol = _load_protocol(protocol)
    errors: list[str] = []
    if payload.get("schema") not in {RUN_SCHEMA, MARKET_RESEARCH_SCHEMA}:
        errors.append("schema_incompatible")
    if payload.get("protocol_hash") not in {None, selected_protocol.protocol_hash}:
        errors.append("protocol_hash_mismatch")
    if payload.get("auto_promote") is not False or payload.get("trading_enabled") is not False:
        errors.append("promotion_gate_invalid")
    if manifest is not None:
        identity = _manifest_identity(_load_manifest(manifest))
        dataset = payload.get("dataset")
        if not isinstance(dataset, Mapping) or dataset.get("content_hash") != identity["content_hash"]:
            errors.append("dataset_identity_mismatch")
    registry_result: dict[str, Any] | None = None
    if registry is not None:
        selected_registry = _registry(registry)
        registry_result = selected_registry.validate()
        if registry_result.get("ok") is not True:
            errors.append("registry_invalid")
    errors.extend(_validate_payload_results(payload, selected_protocol))
    evidence = payload.get("evidence")
    evidence_result = validate_evidence(evidence) if isinstance(evidence, Mapping) else None
    if evidence_result is not None and evidence_result.get("ok") is not True:
        errors.append("evidence_invalid")
    return {
        "schema": MARKET_RESEARCH_SCHEMA,
        "operation": "VALIDATE",
        "ok": not errors,
        "state": "VALID" if not errors else "INVALID",
        "errors": sorted(set(errors)),
        "protocol_hash": selected_protocol.protocol_hash,
        "registry": registry_result,
        "auto_promote": False,
        "trading_enabled": False,
    }


@dataclass(frozen=True, slots=True)
class ForwardMonitorPolicy:
    """Fixed forward-review thresholds; no auto-tuning or trading action."""

    interim_episodes: int = FORWARD_INTERIM_EPISODES
    final_episodes: int = FORWARD_FINAL_EPISODES
    minimum_sessions: int = FORWARD_MIN_SESSIONS
    minimum_blocks: int = FORWARD_MIN_BLOCKS
    downside_reference_r: Decimal = Decimal("0")
    allowance_r: Decimal = Decimal("0")
    threshold_r: Decimal = Decimal("5")

    def __post_init__(self) -> None:
        for name in ("interim_episodes", "final_episodes", "minimum_sessions", "minimum_blocks"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise MarketResearchError(f"{name} debe ser entero positivo")
        for name in ("downside_reference_r", "allowance_r", "threshold_r"):
            value = _decimal(getattr(self, name), name=name)
            if name != "threshold_r" and value < _D0:
                raise MarketResearchError(f"{name} debe ser no negativo")
            if name == "threshold_r" and value <= _D0:
                raise MarketResearchError("threshold_r debe ser positivo")
            object.__setattr__(self, name, value)

    def to_dict(self) -> dict[str, Any]:
        return {
            "interim_episodes": self.interim_episodes,
            "final_episodes": self.final_episodes,
            "minimum_sessions": self.minimum_sessions,
            "minimum_blocks": self.minimum_blocks,
            "downside_reference_r": format(self.downside_reference_r, "f"),
            "allowance_r": format(self.allowance_r, "f"),
            "threshold_r": format(self.threshold_r, "f"),
        }


def evaluate_forward_monitor(
    rows: Sequence[Mapping[str, Any]],
    *,
    policy: ForwardMonitorPolicy | None = None,
) -> dict[str, Any]:
    """Apply a fixed downside CUSUM to observed rows without issuing actions."""

    selected = policy or ForwardMonitorPolicy()
    observed: list[tuple[datetime, Decimal, str]] = []
    for index, row in enumerate(rows):
        raw_time = row.get("available_at", row.get("close_available_at", row.get("timestamp")))
        derived_r = derive_net_r(row)
        raw_r = derived_r.get("net_r") if derived_r.get("status") == "DERIVED" else None
        if raw_time is None or raw_r is None:
            continue
        try:
            observed.append((_aware(raw_time, name=f"rows[{index}].timestamp"), _decimal(raw_r, name="r"), str(index)))
        except MarketResearchError:
            continue
    observed.sort(key=lambda item: (item[0], item[2]))
    sessions = {item[0].date().isoformat() for item in observed}
    blocks = {(item[0].date() - datetime(1970, 1, 1, tzinfo=UTC).date()).days // 14 for item in observed}
    daily: dict[str, tuple[datetime, Decimal]] = {}
    for when, value, _ in observed:
        key = when.date().isoformat()
        previous = daily.get(key)
        daily[key] = (when, value if previous is None else previous[1] + value)
    cusum = _D0
    minimum = _D0
    breach_at: str | None = None
    reference = selected.downside_reference_r
    for when, value in (daily[key] for key in sorted(daily)):
        cusum = min(_D0, cusum + value - reference + selected.allowance_r)
        minimum = min(minimum, cusum)
        if breach_at is None and -cusum >= selected.threshold_r:
            breach_at = instant_text(when)
    count = len(observed)
    if count < selected.interim_episodes:
        state = "OBSERVE_INSUFFICIENT_SAMPLE"
    elif count < selected.final_episodes:
        state = "INTERIM_REVIEW"
    elif len(sessions) < selected.minimum_sessions or len(blocks) < selected.minimum_blocks:
        state = "FINAL_REVIEW_INSUFFICIENT_SESSIONS_OR_BLOCKS"
    else:
        state = "FINAL_REVIEW"
    return {
        "schema": MARKET_RESEARCH_SCHEMA,
        "state": state,
        "episodes": count,
        "daily_observations": len(daily),
        "sessions": len(sessions),
        "blocks": len(blocks),
        "policy": selected.to_dict(),
        "cusum": {
            "reference_r": format(reference, "f"),
            "minimum": format(minimum, "f"),
            "breach": breach_at is not None,
            "breach_at": breach_at,
            "action": "PAUSE_AND_HUMAN_REVIEW" if breach_at is not None else "NO_ACTION",
        },
        "auto_tune": False,
        "auto_promote": False,
        "trading_enabled": False,
    }


def _arg(args: Any, *names: str, default: Any = None) -> Any:
    for name in names:
        value = getattr(args, name, None)
        if value is not None:
            return value
    return default


def _arg_mapping(args: Any, *names: str, default: Mapping[str, Any] | None = None) -> dict[str, Any]:
    value = _arg(args, *names)
    if value is None:
        return dict(default or {})
    if isinstance(value, Mapping):
        return cast(dict[str, Any], _jsonable(dict(value)))
    if isinstance(value, (str, Path)):
        text = str(value)
        candidate = Path(text).expanduser()
        try:
            if candidate.is_absolute() and candidate.is_file():
                raw = json.loads(_safe_input_file(candidate, name="identity").read_text(encoding="utf-8"))
            else:
                raw = json.loads(text)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MarketResearchError(f"{names[0]} debe ser JSON local o mapping") from exc
        if isinstance(raw, Mapping):
            return cast(dict[str, Any], _jsonable(dict(raw)))
    raise MarketResearchError(f"{names[0]} debe ser mapping")


def _model_id_arg(args: Any, *names: str) -> str | None:
    value = _arg(args, *names)
    if value is None:
        return None
    candidate = Path(str(value)).expanduser()
    if isinstance(value, Path) or (candidate.is_absolute() and candidate.is_file()):
        body = _arg_mapping(args, *names)
        model_id = body.get("model_id", body.get("assumptions_model_id", body.get("calendar_model_id")))
        selected_id = _text(model_id, name=names[0])
        if "assum" in names[0]:
            try:
                selected = historical_assumptions_for(selected_id)
            except HistoricalAssumptionError as exc:
                raise MarketResearchError(f"{names[0]} no soportado: {selected_id}") from exc
            supplied_hash = body.get("assumption_hash")
            if supplied_hash is not None and str(supplied_hash) != selected.assumption_hash:
                raise MarketResearchError(f"{names[0]} assumption_hash no coincide")
        else:
            try:
                selected_calendar = calendar_template_for(selected_id)
            except HistoricalAssumptionError as exc:
                raise MarketResearchError(f"{names[0]} no soportado: {selected_id}") from exc
            supplied_hash = body.get("calendar_hash")
            if supplied_hash is not None and str(supplied_hash) != selected_calendar.calendar_hash:
                raise MarketResearchError(f"{names[0]} calendar_hash no coincide")
        return selected_id
    return _text(value, name=names[0])


def _calendar_identity_arg(args: Any) -> str | Mapping[str, Any]:
    identity = _arg(args, "calendar_identity")
    if identity is not None:
        return _arg_mapping(args, "calendar_identity")
    return _text(_arg(args, "calendar_hash", "calendar", default="UNKNOWN"), name="calendar_hash")


def _optional_mapping_arg(args: Any, *names: str) -> dict[str, Any] | None:
    if _arg(args, *names) is None:
        return None
    return _arg_mapping(args, *names)


class MarketResearchService:
    """Small object adapter for the campaign CLI and local callers."""

    def init(self, *, protocol_path: str | Path, registry_path: str | Path) -> dict[str, Any]:
        return init_campaign(protocol_path=protocol_path, registry_path=registry_path)

    def register(
        self,
        protocol: ResearchProtocol | str | Path,
        manifest: DatasetManifest | str | Path,
        registry: GlobalTrialRegistry | str | Path,
        **kwargs: Any,
    ) -> dict[str, Any]:
        return register_campaign(protocol, manifest, registry, **kwargs)

    def run(
        self,
        protocol: ResearchProtocol | str | Path,
        manifest: DatasetManifest | str | Path,
        registry: GlobalTrialRegistry | str | Path,
        **kwargs: Any,
    ) -> dict[str, Any]:
        return run_campaign(protocol, manifest, registry, **kwargs)

    def report(self, source: Mapping[str, Any] | str | Path, **kwargs: Any) -> dict[str, Any]:
        return report_campaign(source, **kwargs)

    def validate(self, source: Mapping[str, Any] | str | Path, **kwargs: Any) -> dict[str, Any]:
        return validate_campaign(source, **kwargs)


def _command_init(args: Any, service: MarketResearchService) -> tuple[int, dict[str, Any]]:
    protocol = _arg(args, "protocol", "protocol_path")
    registry = _arg(args, "registry", "registry_path")
    if protocol is None or registry is None:
        raise MarketResearchError("init requiere --protocol y --registry")
    return 0, service.init(protocol_path=protocol, registry_path=registry)


def _command_register(args: Any, service: MarketResearchService) -> tuple[int, dict[str, Any]]:
    protocol = _arg(args, "protocol", "protocol_path")
    registry = _arg(args, "registry", "registry_path")
    dataset = _arg(args, "dataset_manifest", "dataset", "manifest")
    if protocol is None or registry is None or dataset is None:
        raise MarketResearchError("register requiere --protocol, --registry y --dataset-manifest")
    runtime = _arg_mapping(args, "runtime_identity", default={"source": "cli", "state": "UNDECLARED"})
    costs = _arg_mapping(args, "cost_identity", "costs", default={"source": "cli", "state": "UNDECLARED"})
    calendar = _calendar_identity_arg(args)
    return 0, service.register(
        protocol,
        dataset,
        registry,
        runtime_identity=runtime,
        calendar_hash=calendar,
        cost_identity=costs,
        assumptions_model_id=_model_id_arg(args, "assumptions_model", "assumptions_model_id"),
        calendar_model_id=_model_id_arg(args, "calendar_model", "calendar_model_id"),
        contract_spec=_optional_mapping_arg(args, "contract_spec"),
        calendar_state=_optional_mapping_arg(args, "calendar_state"),
        candidates=_arg(args, "candidates"),
        scenarios=_arg(args, "scenarios"),
        stage=_arg(args, "stage", default="development"),
    )


def _command_run(args: Any, service: MarketResearchService) -> tuple[int, dict[str, Any]]:
    protocol = _arg(args, "protocol", "protocol_path")
    registry = _arg(args, "registry", "registry_path")
    dataset = _arg(args, "dataset_manifest", "dataset", "manifest")
    output = _arg(args, "output_dir", "output")
    if protocol is None or registry is None or dataset is None or output is None:
        raise MarketResearchError("run requiere --protocol, --registry, --dataset-manifest y --output-dir")
    runtime = _arg_mapping(args, "runtime_identity", default={"source": "cli", "state": "UNDECLARED"})
    costs = _arg_mapping(args, "cost_identity", "costs", default={"source": "cli", "state": "UNDECLARED"})
    calendar = _calendar_identity_arg(args)
    return 0, service.run(
        protocol,
        dataset,
        registry,
        output_dir=output,
        stage=_arg(args, "stage", default="development"),
        runtime_identity=runtime,
        calendar_hash=calendar,
        cost_identity=costs,
        assumptions_model_id=_model_id_arg(args, "assumptions_model", "assumptions_model_id"),
        calendar_model_id=_model_id_arg(args, "calendar_model", "calendar_model_id"),
        contract_spec=_optional_mapping_arg(args, "contract_spec"),
        calendar_state=_optional_mapping_arg(args, "calendar_state"),
        candidates=_arg(args, "candidates"),
        scenarios=_arg(args, "scenarios"),
        start=_arg(args, "start"),
        end=_arg(args, "end"),
    )


def _command_report(args: Any, service: MarketResearchService) -> tuple[int, dict[str, Any]]:
    source = _arg(args, "manifest", "source", "input", "report")
    if source is None:
        raise MarketResearchError("report requiere artifact de campaña")
    return 0, service.report(
        source,
        protocol=_arg(args, "protocol", "protocol_path"),
        output_dir=_arg(args, "output_dir", "output"),
    )


def _command_validate(args: Any, service: MarketResearchService) -> tuple[int, dict[str, Any]]:
    source = _arg(args, "manifest", "source", "input", "report")
    if source is None:
        raise MarketResearchError("validate requiere artifact de campaña")
    payload = service.validate(
        source,
        protocol=_arg(args, "protocol", "protocol_path"),
        manifest=_arg(args, "dataset_manifest"),
        registry=_arg(args, "registry", "registry_path"),
    )
    return (0 if payload.get("ok") is True else 2), payload


def market_research_command(args: Any) -> tuple[int, dict[str, Any]]:
    """CLI-neutral dispatcher returning ``(exit_code, JSON payload)``."""

    action = str(_arg(args, "market_research_action", "research_campaign_action", "action", default="")).strip().lower()
    service = MarketResearchService()
    try:
        handlers = {
            "init": _command_init,
            "register": _command_register,
            "run": _command_run,
            "report": _command_report,
            "validate": _command_validate,
        }
        handler = handlers.get(action)
        if handler is None:
            raise MarketResearchError("acción de campaign no soportada; use init/register/run/report/validate")
        return handler(args, service)
    except (MarketResearchError, MarketProtocolError, RegistryError, OSError, TypeError, ValueError) as exc:
        return 2, {
            "schema": MARKET_RESEARCH_SCHEMA,
            "state": "INVALID",
            "error": type(exc).__name__,
            "message": str(exc),
            "auto_promote": False,
            "trading_enabled": False,
        }


__all__ = [
    "ConfirmatoryAccess",
    "FORWARD_FINAL_EPISODES",
    "FORWARD_INTERIM_EPISODES",
    "FORWARD_MIN_BLOCKS",
    "FORWARD_MIN_SESSIONS",
    "ForwardMonitorPolicy",
    "HoldoutAccessError",
    "HoldoutExecutionPermission",
    "HOLDOUT_WINDOW_END",
    "HOLDOUT_WINDOW_START",
    "INITIAL_GATES",
    "MARKET_RESEARCH_SCHEMA",
    "MarketResearchError",
    "MarketResearchService",
    "ResearchRunner",
    "STAGES",
    "SCENARIOS",
    "consume_confirmatory_access",
    "evaluate_forward_monitor",
    "iter_snapshot_pages",
    "init_campaign",
    "market_research_command",
    "open_confirmatory_access",
    "register_campaign",
    "report_campaign",
    "run_campaign",
    "validate_campaign",
]
