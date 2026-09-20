"""Streaming historical BBO backtest composition for the local campaign.

This module is intentionally an orchestration layer.  It consumes the typed
historical data contract, feeds the existing :class:`IncrementalProcessor`
and detector, uses one shared :class:`CFDSimulator` for the legacy horizon
control and one policy-enabled instance for dynamic risk exits.  The latter
owns entry sizing, stops, targets, MFE/MAE and time exits through
``core.risk_exit``; no parallel position state is kept here.
It never downloads data, opens a provider, writes SQLite ticks, or constructs
a second indicator/strategy implementation.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
from collections import Counter, deque
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import MappingProxyType
from typing import Any, Protocol, cast

from ..core import (
    EventKind,
    IndicatorConfig,
    MarketEvent,
    OperationMode,
    PriceBase,
    Signal,
    StrategyConfig,
    parse_timeframe,
)
from ..core.canonical import canonical_json, fingerprint, instant_text
from ..core.cfd_simulation import (
    RISK_BAR_CLOCK_BASIS,
    CFDConfig,
    CFDQuote,
    CFDSignal,
    CFDSimulator,
    Direction,
)
from ..core.historical_calendar import HistoricalQuoteCalendar
from ..core.market_profiles import CANDIDATE_IDS, MARKET_PROFILES, MarketProfile, market_profile
from ..core.numeric import DEFAULT_DECIMAL_POLICY, quantize_decimal
from ..core.risk_exit import EntryPlan, RiskExitPolicy, plan_entry
from ..data.historical import DatasetManifest, HistoricalQuote
from ..data.historical_stream_guard import (
    HistoricalStreamIdentityError,
    HistoricalStreamIdentityGuard,
)
from ..runtime.consumers import RecordingSignalConsumer
from ..runtime.processor import DataPlaneResult, IncrementalProcessor, ProcessResult, SharedDataPlane
from .historical_assumptions import HistoricalAssumptionsModel, HistoricalCalendarTemplate, HistoricalEntryGate

HISTORICAL_SCHEMA_VERSION = 1
CHECKPOINT_VERSION = 1
BLOCK_SIZE = 2048
# Processing remains fixed at 2,048 quotes per block.  Durable snapshots are
# intentionally less frequent by default: every 32 blocks (65,536 quotes).
# The interval is part of the frozen configuration/checkpoint identity and
# can be lowered for an explicitly selected recovery window.
DEFAULT_CHECKPOINT_INTERVAL_BLOCKS = 32
RSS_TARGET_BYTES = int(1.5 * 1024**3)
LEGACY_HORIZONS = (Decimal("60"), Decimal("180"), Decimal("300"))
SCENARIOS = ("base", "adverse", "extreme")
HISTORICAL_SOURCE = "HISTORICAL_RESEARCH"
SCENARIO_PARAMETERS: dict[str, dict[str, str]] = {
    "base": {
        "decision_latency_seconds": "5",
        "slippage_pips": "0.1",
        "spread_multiplier": "1",
        "cost_multiplier": "1",
    },
    "adverse": {
        "decision_latency_seconds": "10",
        "slippage_pips": "0.2",
        "spread_multiplier": "1.5",
        "cost_multiplier": "1.5",
    },
    "extreme": {
        "decision_latency_seconds": "10",
        "slippage_pips": "0.2",
        "spread_multiplier": "2",
        "cost_multiplier": "2",
    },
}
_HISTORICAL_QUOTE_COVERAGE_MODE = "continuous_quotes"
_HISTORICAL_MAX_QUOTE_GAP_SECONDS = 90.0
_HISTORICAL_SHARED_PLANE_SOURCE = "historical:shared_data_plane"
_REPOSITORY = Path(__file__).resolve().parents[2]
_INDICATOR_CONFIG = IndicatorConfig(20, 50, 14, 14, True)
_HISTORICAL_CALENDAR = HistoricalQuoteCalendar()


class HistoricalBacktestError(ValueError):
    """The historical backtest cannot satisfy its explicit local contract."""


class ArtifactSink(Protocol):
    """Sink for one bounded result row; it must not retain the full stream."""

    def __call__(self, kind: str, row: Mapping[str, Any]) -> Any: ...


HistoricalProfile = MarketProfile
DEFAULT_PROFILES = MARKET_PROFILES


def _decimal(value: Any, name: str, *, allow_none: bool = False) -> Decimal | None:
    if value is None and allow_none:
        return None
    if isinstance(value, bool):
        raise HistoricalBacktestError(f"{name} no admite booleanos")
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise HistoricalBacktestError(f"{name} no es Decimal válido") from exc
    if not parsed.is_finite():
        raise HistoricalBacktestError(f"{name} debe ser finito")
    return parsed


def _utc(value: Any, name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except (TypeError, ValueError) as exc:
            raise HistoricalBacktestError(f"{name} debe ser timestamp ISO") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise HistoricalBacktestError(f"{name} debe incluir zona horaria")
    return parsed.astimezone(UTC)


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(item) for item in value]
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return instant_text(value)
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return _jsonable(value.to_dict())
    return value


def _hash_mapping(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(_jsonable(value)).encode("utf-8")).hexdigest()


def _validate_positive_int(value: Any, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise HistoricalBacktestError(f"{name} debe ser entero positivo")


def _normalize_guard_snapshot(value: Mapping[str, Any] | None) -> Mapping[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise HistoricalBacktestError("checkpoint guard_snapshot debe ser mapping")
    return cast(Mapping[str, Any], MappingProxyType(dict(value)))


def _infer_checkpoint_finished(value: Any) -> bool:
    """Infer lifecycle for checkpoints written before ``finished`` existed."""

    if not isinstance(value, Mapping):
        return True
    raw_profiles = value.get("profiles")
    if not isinstance(raw_profiles, Mapping):
        return True
    flags: list[bool] = []
    for raw_profile in raw_profiles.values():
        if not isinstance(raw_profile, Mapping):
            continue
        for simulator_name in ("legacy", "risk"):
            raw_simulator = raw_profile.get(simulator_name)
            if isinstance(raw_simulator, Mapping) and isinstance(raw_simulator.get("finished"), bool):
                flags.append(raw_simulator["finished"])
    return all(flags) if flags else True


def _validate_checkpoint_lifecycle(
    finished: bool,
    processed_quotes: int,
    cursor_sequence: int,
    artifact_offsets: Mapping[str, int],
) -> None:
    if not isinstance(finished, bool):
        raise HistoricalBacktestError("checkpoint.finished debe ser booleano")
    if (processed_quotes == 0) != (cursor_sequence < 0):
        raise HistoricalBacktestError("checkpoint cursor/processed_quotes inconsistente")
    if not finished and set(artifact_offsets) != {"ledger", "equity", "funnel"}:
        raise HistoricalBacktestError("checkpoint parcial requiere offsets completos")


def _manifest_identity(manifest: DatasetManifest) -> tuple[str, str]:
    if not isinstance(manifest, DatasetManifest):
        raise HistoricalBacktestError("manifest debe ser DatasetManifest")
    dataset_id = str(manifest.dataset_id).strip()
    if not dataset_id:
        raise HistoricalBacktestError("manifest.dataset_id es obligatorio")
    content_hash = manifest.content_hash
    if not isinstance(content_hash, str) or not content_hash.strip():
        raise HistoricalBacktestError("manifest requiere content_hash")
    return dataset_id, content_hash.strip()


def _protocol_hash(protocol: Any) -> str:
    if protocol is None:
        return ""
    value = getattr(protocol, "protocol_hash", None)
    if isinstance(value, str) and value:
        return value
    to_dict = getattr(protocol, "to_dict", None)
    if not callable(to_dict):
        raise HistoricalBacktestError("protocol requiere protocol_hash o to_dict()")
    raw = to_dict()
    if not isinstance(raw, Mapping):
        raise HistoricalBacktestError("protocol.to_dict() debe devolver mapping")
    return _hash_mapping(raw)


def _validate_historical_periods(config: HistoricalBacktestConfig) -> None:
    if (
        config.warmup_start_year,
        config.development_start_year,
        config.development_end_year,
        config.walkforward_start_year,
        config.walkforward_end_year,
        config.holdout_start_year,
        config.holdout_end_year,
    ) != (2015, 2016, 2019, 2020, 2023, 2024, 2025):
        raise HistoricalBacktestError("los periodos históricos están congelados")


def _scenario_name(value: Any) -> str:
    selected = str(value).strip().lower()
    if selected not in SCENARIOS:
        raise HistoricalBacktestError(f"scenario no soportado: {value!r}")
    return selected


def _scenario_parameters(value: str, raw: Mapping[str, Any] | None = None) -> dict[str, str]:
    selected = _scenario_name(value)
    expected = SCENARIO_PARAMETERS[selected]
    if raw is None:
        return dict(expected)
    normalized = {str(key): str(item) for key, item in raw.items()}
    if normalized != expected:
        raise HistoricalBacktestError(f"scenario_parameters no coincide con {selected}")
    return dict(expected)


def _scenario_policy(
    policy: RiskExitPolicy,
    scenario_value: Any,
    parameters: Mapping[str, Any] | None,
) -> tuple[str, dict[str, str], RiskExitPolicy]:
    scenario = _scenario_name(scenario_value)
    normalized = _scenario_parameters(scenario, parameters)
    latency = Decimal(normalized["decision_latency_seconds"])
    selected = policy if policy.exit_latency_seconds == latency else replace(policy, exit_latency_seconds=latency)
    return scenario, normalized, selected


def _selected_profiles(candidates: Sequence[str] | None) -> tuple[MarketProfile, ...]:
    if candidates is None:
        return MARKET_PROFILES
    raw = (candidates,) if isinstance(candidates, str) else tuple(candidates)
    normalized = tuple(str(item).strip() for item in raw)
    if not normalized or len(set(normalized)) != len(normalized):
        raise HistoricalBacktestError("candidates debe contener IDs únicos")
    if any(item not in CANDIDATE_IDS for item in normalized):
        raise HistoricalBacktestError("candidates contiene un ID no registrado")
    return tuple(market_profile(item) for item in normalized)


def _resolve_assumption_inputs(
    assumptions: HistoricalAssumptionsModel | None,
    calendar_model: HistoricalCalendarTemplate | None,
    contract_spec: Mapping[str, Any],
) -> tuple[HistoricalAssumptionsModel | None, HistoricalCalendarTemplate | None, Mapping[str, Any]]:
    if assumptions is not None and not isinstance(assumptions, HistoricalAssumptionsModel):
        raise HistoricalBacktestError("assumptions_model debe ser HistoricalAssumptionsModel")
    selected_calendar = calendar_model
    selected_spec: Mapping[str, Any] = contract_spec
    if assumptions is not None:
        if not selected_spec:
            selected_spec = assumptions.account.contract_spec
        if selected_calendar is None:
            selected_calendar = assumptions.calendar
    if selected_calendar is not None and not isinstance(selected_calendar, HistoricalCalendarTemplate):
        raise HistoricalBacktestError("calendar_model debe ser HistoricalCalendarTemplate")
    return assumptions, selected_calendar, selected_spec


@dataclass(frozen=True, slots=True)
class HistoricalBacktestConfig:
    """Frozen configuration for the registered profiles and one stress case."""

    protocol: Any | None = None
    profiles: tuple[MarketProfile, ...] = MARKET_PROFILES
    risk_policy: RiskExitPolicy = field(default_factory=RiskExitPolicy)
    scenario: str = "base"
    scenario_parameters: Mapping[str, Any] = field(default_factory=lambda: dict(SCENARIO_PARAMETERS["base"]))
    assumptions_model: HistoricalAssumptionsModel | None = None
    calendar_model: HistoricalCalendarTemplate | None = None
    contract_spec: Mapping[str, Any] = field(default_factory=dict)
    calendar_state: Mapping[str, Any] | None = None
    risk_state: Mapping[str, Any] = field(
        default_factory=lambda: {
            "daily_pnl": "0",
            "drawdown": "0",
            "positions": 0,
            "intents": 0,
            "costs_known": True,
        }
    )
    block_size: int = BLOCK_SIZE
    checkpoint_interval_blocks: int = DEFAULT_CHECKPOINT_INTERVAL_BLOCKS
    max_candles: int = 512
    max_retained_rows: int = 4096
    legacy_horizons: tuple[Decimal, ...] = LEGACY_HORIZONS
    warmup_start_year: int = 2015
    development_start_year: int = 2016
    development_end_year: int = 2019
    walkforward_start_year: int = 2020
    walkforward_end_year: int = 2023
    holdout_start_year: int = 2024
    holdout_end_year: int = 2025

    def __post_init__(self) -> None:
        profile_ids = tuple(item.candidate_id for item in self.profiles)
        if tuple(self.profiles) != _selected_profiles(profile_ids):
            raise HistoricalBacktestError("profiles debe usar las identidades canónicas registradas")
        if isinstance(self.block_size, bool) or not isinstance(self.block_size, int) or self.block_size != BLOCK_SIZE:
            raise HistoricalBacktestError(f"block_size debe ser exactamente {BLOCK_SIZE}")
        _validate_positive_int(self.checkpoint_interval_blocks, "checkpoint_interval_blocks")
        for name in ("max_candles", "max_retained_rows"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise HistoricalBacktestError(f"{name} debe ser entero positivo")
        if not isinstance(self.risk_policy, RiskExitPolicy):
            raise HistoricalBacktestError("risk_policy debe ser core.RiskExitPolicy")
        scenario, parameters, policy = _scenario_policy(self.risk_policy, self.scenario, self.scenario_parameters)
        assumptions, calendar_model, contract_spec = _resolve_assumption_inputs(
            self.assumptions_model,
            self.calendar_model,
            self.contract_spec,
        )
        object.__setattr__(self, "scenario", scenario)
        object.__setattr__(self, "scenario_parameters", MappingProxyType(parameters))
        object.__setattr__(self, "risk_policy", policy)
        object.__setattr__(self, "assumptions_model", assumptions)
        object.__setattr__(self, "calendar_model", calendar_model)
        object.__setattr__(self, "contract_spec", contract_spec)
        horizons = tuple(_decimal(value, "legacy_horizon") for value in self.legacy_horizons)
        if not horizons or any(value is None or value <= 0 for value in horizons):
            raise HistoricalBacktestError("legacy_horizons debe contener duraciones positivas")
        if not isinstance(self.contract_spec, Mapping):
            raise HistoricalBacktestError("contract_spec debe ser mapping")
        if self.calendar_state is not None and not isinstance(self.calendar_state, Mapping):
            raise HistoricalBacktestError("calendar_state debe ser mapping o None")
        if not isinstance(self.risk_state, Mapping):
            raise HistoricalBacktestError("risk_state debe ser mapping")
        _validate_historical_periods(self)
        object.__setattr__(self, "profiles", tuple(self.profiles))
        object.__setattr__(self, "contract_spec", MappingProxyType(dict(self.contract_spec)))
        object.__setattr__(
            self,
            "calendar_state",
            MappingProxyType(dict(self.calendar_state)) if self.calendar_state is not None else None,
        )
        object.__setattr__(self, "risk_state", MappingProxyType(dict(self.risk_state)))
        object.__setattr__(self, "legacy_horizons", tuple(cast(Decimal, value) for value in horizons))

    @classmethod
    def from_protocol(
        cls,
        protocol: Any,
        *,
        scenario: str = "base",
        candidates: Sequence[str] | None = None,
        checkpoint_interval_blocks: int = DEFAULT_CHECKPOINT_INTERVAL_BLOCKS,
        contract_spec: Mapping[str, Any] | None = None,
        calendar_state: Mapping[str, Any] | None = None,
        assumptions_model: HistoricalAssumptionsModel | None = None,
        calendar_model: HistoricalCalendarTemplate | None = None,
    ) -> HistoricalBacktestConfig:
        """Build one frozen scenario and optional registered candidate subset."""

        if protocol is None:
            raise HistoricalBacktestError("protocol es obligatorio")
        candidate_ids = getattr(protocol, "candidate_ids", None)
        if callable(candidate_ids):
            candidate_ids = candidate_ids()
        if candidate_ids is None:
            candidates = getattr(protocol, "candidates", None)
            candidate_iterable = candidates if isinstance(candidates, Iterable) else ()
            candidate_ids = tuple(getattr(item, "candidate_id", "") for item in candidate_iterable)
        if tuple(cast(Iterable[Any], candidate_ids)) != CANDIDATE_IDS:
            raise HistoricalBacktestError("protocol no contiene los seis candidatos exactos")
        if getattr(protocol, "holdout_locked", True) is not True or getattr(protocol, "holdout_open", False) is True:
            raise HistoricalBacktestError("el holdout debe permanecer cerrado")
        policy = getattr(protocol, "risk_policy", None)
        if not isinstance(policy, RiskExitPolicy):
            raise HistoricalBacktestError("protocol.risk_policy debe ser core.RiskExitPolicy")
        selected = _scenario_name(scenario)
        parameters = _scenario_parameters(selected)
        scenario_policy = replace(policy, exit_latency_seconds=Decimal(parameters["decision_latency_seconds"]))
        selected_spec = (
            contract_spec
            if contract_spec is not None
            else assumptions_model.account.contract_spec
            if assumptions_model is not None
            else {}
        )
        selected_calendar_model = calendar_model or (
            assumptions_model.calendar if assumptions_model is not None else None
        )
        return cls(
            protocol=protocol,
            profiles=_selected_profiles(candidates),
            risk_policy=scenario_policy,
            scenario=selected,
            scenario_parameters=parameters,
            assumptions_model=assumptions_model,
            calendar_model=selected_calendar_model,
            contract_spec=selected_spec,
            calendar_state=calendar_state,
            checkpoint_interval_blocks=checkpoint_interval_blocks,
        )

    @property
    def protocol_hash(self) -> str:
        return _protocol_hash(self.protocol)

    @property
    def config_hash(self) -> str:
        return _hash_mapping(self.to_dict(include_hash=False))

    @property
    def scenario_hash(self) -> str:
        return _hash_mapping(
            {
                "scenario": self.scenario,
                "scenario_parameters": dict(self.scenario_parameters),
                "scenario_mode": self.scenario_mode,
            }
        )

    @property
    def candidate_ids(self) -> tuple[str, ...]:
        return tuple(item.candidate_id for item in self.profiles)

    @property
    def checkpoint_interval_quotes(self) -> int:
        """Number of quotes between periodic snapshots.

        ``block_size`` stays fixed at :data:`BLOCK_SIZE`; this derived value
        makes the checkpoint cadence explicit without changing processing
        granularity.
        """

        return self.block_size * self.checkpoint_interval_blocks

    @property
    def candidates(self) -> tuple[str, ...]:
        """Selected candidate IDs in the declared deterministic order."""

        return self.candidate_ids

    @property
    def decision_latency_seconds(self) -> Decimal:
        return Decimal(self.scenario_parameters["decision_latency_seconds"])

    @property
    def slippage_pips(self) -> Decimal:
        return Decimal(self.scenario_parameters["slippage_pips"])

    @property
    def spread_multiplier(self) -> Decimal:
        return Decimal(self.scenario_parameters["spread_multiplier"])

    @property
    def cost_multiplier(self) -> Decimal:
        return Decimal(self.scenario_parameters["cost_multiplier"])

    @property
    def scenario_mode(self) -> str:
        # Historical research never authorizes an execution path.  Base,
        # adverse and extreme are statistical stress labels; all three remain
        # virtual diagnostics even when a future run has complete costs.
        return "VIRTUAL_DIAGNOSTIC"

    @property
    def mode(self) -> str:
        return self.scenario_mode

    @property
    def source(self) -> str:
        return HISTORICAL_SOURCE

    @property
    def assumptions_model_id(self) -> str | None:
        return self.assumptions_model.model_id if self.assumptions_model is not None else None

    @property
    def assumption_hash(self) -> str | None:
        return self.assumptions_model.assumption_hash if self.assumptions_model is not None else None

    @property
    def calendar_model_id(self) -> str | None:
        return self.calendar_model.model_id if self.calendar_model is not None else None

    def calendar_state_for_quote(self, at: datetime) -> Mapping[str, Any] | None:
        return self.calendar_model.state_for_quote(at) if self.calendar_model is not None else self.calendar_state

    def entry_gate_for(self, profile: MarketProfile, at: datetime) -> HistoricalEntryGate | None:
        if self.calendar_model is None:
            return None
        policy = _profile_policy(self, profile)
        preclose = (
            policy.intraday_max_minutes if profile.holding_profile == "INTRADAY" else policy.multiday_preclose_minutes
        )
        return self.calendar_model.entry_gate(
            at,
            preclose_minutes=preclose,
            include_daily_preclose=profile.holding_profile == "INTRADAY",
        )

    def to_dict(self, *, include_hash: bool = True) -> dict[str, Any]:
        value: dict[str, Any] = {
            "schema_version": HISTORICAL_SCHEMA_VERSION,
            "profiles": [item.to_dict() for item in self.profiles],
            "candidate_ids": list(self.candidate_ids),
            "scenario": self.scenario,
            "scenario_parameters": dict(self.scenario_parameters),
            "scenario_mode": self.scenario_mode,
            "mode": self.scenario_mode,
            "source": self.source,
            "scenario_hash": self.scenario_hash,
            "assumptions_model_id": self.assumptions_model_id,
            "assumption_hash": self.assumption_hash,
            "assumptions_model": (self.assumptions_model.to_dict() if self.assumptions_model is not None else None),
            "calendar_model_id": self.calendar_model_id,
            "calendar_model": self.calendar_model.to_dict() if self.calendar_model is not None else None,
            "risk_policy": self.risk_policy.serialize(),
            "contract_spec": _jsonable(self.contract_spec),
            "calendar_state": _jsonable(self.calendar_state),
            "risk_state": _jsonable(self.risk_state),
            "block_size": self.block_size,
            "checkpoint_interval_blocks": self.checkpoint_interval_blocks,
            "checkpoint_interval_quotes": self.checkpoint_interval_quotes,
            "max_candles": self.max_candles,
            "max_retained_rows": self.max_retained_rows,
            "legacy_horizons": [format(item, "f") for item in self.legacy_horizons],
            "periods": {
                "warmup": self.warmup_start_year,
                "development": [self.development_start_year, self.development_end_year],
                "walkforward": [self.walkforward_start_year, self.walkforward_end_year],
                "holdout": [self.holdout_start_year, self.holdout_end_year],
                "holdout_locked": True,
            },
            "protocol_hash": self.protocol_hash,
        }
        if include_hash:
            value["config_hash"] = _hash_mapping(value)
        return value


@dataclass(frozen=True, slots=True)
class ArtifactRows:
    """Bounded result view backed by a streamed JSONL artifact."""

    kind: str
    path: Path
    count: int
    retained: tuple[Mapping[str, Any], ...] = ()

    def __iter__(self) -> Iterator[Mapping[str, Any]]:
        return iter(self.retained)

    def iter_pages(self, *, page_size: int = 256) -> Iterator[tuple[Mapping[str, Any], ...]]:
        if isinstance(page_size, bool) or not isinstance(page_size, int) or page_size <= 0:
            raise ValueError("page_size debe ser entero positivo")
        if self.path.is_file():
            page: list[Mapping[str, Any]] = []
            with self.path.open("r", encoding="utf-8") as stream:
                for line in stream:
                    if not line.strip():
                        continue
                    value = json.loads(line)
                    if not isinstance(value, Mapping):
                        raise HistoricalBacktestError(f"artefacto {self.kind} contiene una fila inválida")
                    page.append(value)
                    if len(page) >= page_size:
                        yield tuple(page)
                        page.clear()
            if page:
                yield tuple(page)
            return
        for offset in range(0, len(self.retained), page_size):
            yield self.retained[offset : offset + page_size]

    def to_dict(self) -> dict[str, Any]:
        return {"kind": self.kind, "path": str(self.path), "count": self.count, "retained": len(self.retained)}


@dataclass(frozen=True, slots=True)
class HistoricalVariantResult:
    candidate_id: str
    strategy: str
    timeframes: tuple[str, ...]
    holding_profile: str
    metrics: Mapping[str, Any]
    legacy_control: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "strategy": self.strategy,
            "timeframes": list(self.timeframes),
            "holding_profile": self.holding_profile,
            "metrics": dict(self.metrics),
            "legacy_control": dict(self.legacy_control),
        }


@dataclass(frozen=True, slots=True)
class HistoricalBacktestCheckpoint:
    """Bounded resumable state fenced to dataset and configuration hashes.

    ``finished`` is deliberately part of the checkpoint contract rather than
    inferred from the presence of a file.  A checkpoint written while the
    source is still being consumed is therefore explicitly continuable and
    cannot be mistaken for a terminal result.  Older checkpoints did not carry
    this field; readers infer it from the owned simulator snapshots so an
    interrupted pre-change run remains continuable.
    """

    dataset_id: str
    data_hash: str
    config_hash: str
    protocol_hash: str
    cursor_sequence: int
    cursor_time: str
    processed_quotes: int
    state: Mapping[str, Any]
    schema_version: int = CHECKPOINT_VERSION
    scenario: str = "base"
    candidate_ids: tuple[str, ...] = CANDIDATE_IDS
    artifact_offsets: Mapping[str, int] = field(default_factory=dict)
    guard_snapshot: Mapping[str, Any] | None = None
    finished: bool = True
    checkpoint_interval_blocks: int = DEFAULT_CHECKPOINT_INTERVAL_BLOCKS

    def __post_init__(self) -> None:
        if self.schema_version != CHECKPOINT_VERSION:
            raise HistoricalBacktestError("checkpoint version incompatible")
        if not self.dataset_id or not self.data_hash or not self.config_hash:
            raise HistoricalBacktestError("checkpoint identity incompleta")
        if isinstance(self.cursor_sequence, bool) or self.cursor_sequence < -1:
            raise HistoricalBacktestError("cursor_sequence inválido")
        if isinstance(self.processed_quotes, bool) or self.processed_quotes < 0:
            raise HistoricalBacktestError("processed_quotes inválido")
        _validate_positive_int(self.checkpoint_interval_blocks, "checkpoint_interval_blocks")
        _utc(self.cursor_time, "cursor_time")
        if not isinstance(self.state, Mapping):
            raise HistoricalBacktestError("checkpoint.state debe ser mapping")
        scenario = _scenario_name(self.scenario)
        candidates = tuple(str(item) for item in self.candidate_ids)
        if tuple(item.candidate_id for item in _selected_profiles(candidates)) != candidates:
            raise HistoricalBacktestError("checkpoint candidate_ids no son canónicos")
        offsets = dict(self.artifact_offsets)
        if set(offsets) - {"ledger", "equity", "funnel"}:
            raise HistoricalBacktestError("checkpoint artifact_offsets contiene artefactos desconocidos")
        _validate_checkpoint_lifecycle(self.finished, self.processed_quotes, self.cursor_sequence, offsets)
        if any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in offsets.values()):
            raise HistoricalBacktestError("checkpoint artifact_offsets inválidos")
        object.__setattr__(self, "guard_snapshot", _normalize_guard_snapshot(self.guard_snapshot))
        object.__setattr__(self, "state", MappingProxyType(dict(self.state)))
        object.__setattr__(self, "scenario", scenario)
        object.__setattr__(self, "candidate_ids", candidates)
        object.__setattr__(self, "artifact_offsets", MappingProxyType(offsets))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "dataset_id": self.dataset_id,
            "data_hash": self.data_hash,
            "config_hash": self.config_hash,
            "protocol_hash": self.protocol_hash,
            "cursor_sequence": self.cursor_sequence,
            "cursor_time": self.cursor_time,
            "processed_quotes": self.processed_quotes,
            "status": self.status,
            "finished": self.finished,
            "scenario": self.scenario,
            "candidate_ids": list(self.candidate_ids),
            "artifact_offsets": dict(self.artifact_offsets),
            "guard_snapshot": _jsonable(self.guard_snapshot) if self.guard_snapshot is not None else None,
            "checkpoint_interval_blocks": self.checkpoint_interval_blocks,
            "state": _jsonable(self.state),
        }

    @property
    def status(self) -> str:
        """Durable lifecycle label corresponding to ``finished``."""

        return "COMPLETED" if self.finished else "CHECKPOINTED"

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> HistoricalBacktestCheckpoint:
        if not isinstance(value, Mapping):
            raise HistoricalBacktestError("resume debe ser checkpoint mapping")
        finished = value["finished"] if "finished" in value else _infer_checkpoint_finished(value.get("state"))
        return cls(
            dataset_id=str(value.get("dataset_id", "")),
            data_hash=str(value.get("data_hash", "")),
            config_hash=str(value.get("config_hash", "")),
            protocol_hash=str(value.get("protocol_hash", "")),
            cursor_sequence=int(value.get("cursor_sequence", -1)),
            cursor_time=str(value.get("cursor_time", "1970-01-01T00:00:00Z")),
            processed_quotes=int(value.get("processed_quotes", 0)),
            state=value.get("state", {}),
            schema_version=int(value.get("schema_version", 0)),
            scenario=str(value.get("scenario", "base")),
            candidate_ids=tuple(str(item) for item in value.get("candidate_ids", CANDIDATE_IDS)),
            artifact_offsets=(
                dict(value.get("artifact_offsets", {}))
                if isinstance(value.get("artifact_offsets", {}), Mapping)
                else {}
            ),
            guard_snapshot=dict(value["guard_snapshot"]) if isinstance(value.get("guard_snapshot"), Mapping) else None,
            finished=finished,
            checkpoint_interval_blocks=value.get("checkpoint_interval_blocks", DEFAULT_CHECKPOINT_INTERVAL_BLOCKS),
        )


@dataclass(frozen=True, slots=True)
class HistoricalBacktestResult:
    dataset_id: str
    data_hash: str
    config_hash: str
    protocol_hash: str
    status: str
    acceptance: bool
    processed_quotes: int
    variants: tuple[HistoricalVariantResult, ...]
    ledger: ArtifactRows
    equity: ArtifactRows
    funnel: ArtifactRows
    metrics: Mapping[str, Any]
    checkpoint: HistoricalBacktestCheckpoint
    scenario: str = "base"
    scenario_parameters: Mapping[str, Any] = field(default_factory=dict)
    candidate_ids: tuple[str, ...] = ()
    assumptions_model_id: str | None = None
    assumption_hash: str | None = None
    calendar_model_id: str | None = None
    finished: bool = True

    @property
    def scenario_mode(self) -> str:
        return str(self.metrics.get("scenario_mode", "VIRTUAL_DIAGNOSTIC"))

    @property
    def mode(self) -> str:
        return self.scenario_mode

    @property
    def source(self) -> str:
        return HISTORICAL_SOURCE

    @property
    def candidates(self) -> tuple[str, ...]:
        return self.candidate_ids

    @property
    def scenario_hash(self) -> str | None:
        value = self.metrics.get("scenario_hash")
        return str(value) if value is not None else None

    @property
    def capture_complete(self) -> bool:
        """Whether the source was drained and finalizers were run."""

        return self.finished

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": HISTORICAL_SCHEMA_VERSION,
            "dataset_id": self.dataset_id,
            "data_hash": self.data_hash,
            "config_hash": self.config_hash,
            "protocol_hash": self.protocol_hash,
            "scenario": self.scenario,
            "scenario_parameters": _jsonable(self.scenario_parameters),
            "candidate_ids": list(self.candidate_ids),
            "scenario_mode": self.metrics.get("scenario_mode", "VIRTUAL_DIAGNOSTIC"),
            "mode": self.metrics.get("scenario_mode", "VIRTUAL_DIAGNOSTIC"),
            "source": self.metrics.get("source", HISTORICAL_SOURCE),
            "scenario_hash": self.metrics.get("scenario_hash"),
            "assumptions_model_id": self.assumptions_model_id,
            "assumption_hash": self.assumption_hash,
            "calendar_model_id": self.calendar_model_id,
            "assumptions_model": self.metrics.get("assumptions_model"),
            "calendar_model": self.metrics.get("calendar_model"),
            "scenario_applied": self.metrics.get("scenario_applied") is True,
            "execution_state": "MODELLED_LOCAL_VIRTUAL",
            "costs_applied": self.metrics.get("costs_applied") is True,
            "gross_only": self.metrics.get("gross_only") is True,
            "net_status": self.metrics.get("net_status", "UNKNOWN_COSTS"),
            "status": self.status,
            "finished": self.finished,
            "capture_complete": self.capture_complete,
            "acceptance": self.acceptance,
            "promotable": False,
            "processed_quotes": self.processed_quotes,
            "variants": [item.to_dict() for item in self.variants],
            "ledger": self.ledger.to_dict(),
            "equity": self.equity.to_dict(),
            "funnel": self.funnel.to_dict(),
            "metrics": _jsonable(self.metrics),
            "checkpoint": self.checkpoint.to_dict(),
            "ticks": {"retained_in_memory": 0, "persisted_sqlite": False},
        }


def _strategy_config(profile: MarketProfile) -> StrategyConfig:
    if profile.strategy_impl != "TrendPullbackStrategy":
        raise HistoricalBacktestError("_strategy_config sólo aplica a TrendPullbackStrategy")
    return StrategyConfig(
        name=profile.candidate_id,
        context_timeframe=profile.timeframes[0],
        preparation_timeframe=profile.timeframes[1],
        trigger_timeframe=profile.timeframes[2],
        context_lookback=3,
        preparation_lookback=3,
        max_distance_atr=0.5,
        rsi_threshold=50.0,
        preparation_ttl_bars=3,
        require_closed=True,
        one_signal_per_episode=True,
        indicators=_INDICATOR_CONFIG,
        mode=OperationMode.REPLAY,
    )


_COST_SPEC_ALIASES: dict[str, tuple[str, ...]] = {
    "fixed": (
        "expected_cost_fixed",
        "expected_fixed_cost",
        "expected_commission_fixed",
        "expected_fee_fixed",
        "commission_fixed",
        "cost_fixed",
        "fixed_cost",
        "fee_fixed",
    ),
    "per_unit": (
        "expected_cost_per_unit",
        "expected_variable_cost_per_unit",
        "expected_commission_per_unit",
        "expected_fee_per_unit",
        "commission_per_unit",
        "cost_per_unit",
        "variable_cost_per_unit",
        "fee_per_unit",
    ),
    "currency": ("expected_cost_currency", "cost_currency", "commission_currency"),
    "source": ("expected_cost_source", "cost_estimate_source", "cost_source"),
}
_COST_SPEC_DECIMAL_FIELDS = frozenset(
    {
        *(_COST_SPEC_ALIASES["fixed"]),
        *(_COST_SPEC_ALIASES["per_unit"]),
        "expected_exit_slippage_per_unit",
        "expected_slippage_per_unit",
        "expected_exit_slippage",
        "expected_exit_slippage_price",
        "expected_slippage",
        "exit_slippage_per_unit",
        "exit_slippage",
        "slippage_per_unit",
        "expected_exit_slippage_pips",
        "expected_slippage_pips",
        "exit_slippage_pips",
        "slippage_pips",
        "financing_rate_per_second",
    }
)


def _explicit_cost_decimal(spec: Mapping[str, Any], keys: Sequence[str]) -> tuple[Decimal | None, bool]:
    """Read one cost component without treating an omitted value as zero."""

    supplied = [key for key in keys if key in spec]
    if not supplied:
        return None, False
    values: list[Decimal] = []
    for key in supplied:
        try:
            value = _decimal(spec[key], key)
        except HistoricalBacktestError:
            return None, False
        if value is None or value < Decimal("0"):
            return None, False
        values.append(value)
    if len(set(values)) != 1:
        return None, False
    return values[0], True


def _explicit_cost_text(spec: Mapping[str, Any], keys: Sequence[str], *, upper: bool = False) -> tuple[str, bool]:
    supplied = [key for key in keys if key in spec]
    if not supplied:
        return "", False
    values = [str(spec[key]).strip() for key in supplied]
    if not values or not values[0] or len(set(values)) != 1:
        return "", False
    value = values[0].upper() if upper else values[0]
    return value, bool(value)


def _effective_contract_spec(config: HistoricalBacktestConfig) -> Mapping[str, Any]:
    """Apply only the preregistered stress multiplier to explicit cost fields."""

    result = dict(config.contract_spec)
    multiplier = config.cost_multiplier
    if multiplier == Decimal("1"):
        return result
    for key in _COST_SPEC_DECIMAL_FIELDS:
        if key not in result:
            continue
        try:
            value = _decimal(result[key], key)
        except HistoricalBacktestError:
            continue
        if value is not None and value >= Decimal("0"):
            result[key] = value * multiplier
    return result


def _legacy_economic_inputs(config: HistoricalBacktestConfig) -> dict[str, Any]:
    """Project a known contract into CFD economics; unknowns stay unknown."""

    spec = _effective_contract_spec(config)
    known_spec = spec.get("known") is True and spec.get("fees_known") is True and spec.get("spread_known") is True
    if spec.get("commission_known") is False:
        known_spec = False
    if spec.get("expected_costs_known", spec.get("risk_envelope_known", True)) is not True:
        known_spec = False
    currency, currency_known = _explicit_cost_text(spec, _COST_SPEC_ALIASES["currency"], upper=True)
    source, source_known = _explicit_cost_text(spec, _COST_SPEC_ALIASES["source"])
    fixed, fixed_known = _explicit_cost_decimal(spec, _COST_SPEC_ALIASES["fixed"])
    per_unit, per_unit_known = _explicit_cost_decimal(spec, _COST_SPEC_ALIASES["per_unit"])
    account_currency = config.risk_policy.account_currency
    commission_known = bool(
        known_spec
        and currency_known
        and currency == account_currency
        and source_known
        and fixed_known
        and per_unit_known
    )
    financing_required = True
    financing_rate: Decimal | None = None
    declared_financing = spec.get("financing_required")
    if declared_financing is False:
        financing_required = False
    elif declared_financing is True and known_spec and currency_known and currency == account_currency:
        rate, rate_known = _explicit_cost_decimal(spec, ("financing_rate_per_second",))
        if rate_known and spec.get("financing_known", True) is not False:
            financing_rate = rate
    return {
        "commission_fixed": fixed if commission_known and fixed is not None else Decimal("0"),
        "commission_per_unit": per_unit if commission_known and per_unit is not None else Decimal("0"),
        "commission_known": commission_known,
        "financing_required": financing_required,
        "financing_rate_per_second": financing_rate,
    }


def _legacy_cfd_config(config: HistoricalBacktestConfig, instrument: str) -> CFDConfig:
    economics = _legacy_economic_inputs(config)
    return CFDConfig(
        instrument=instrument,
        units=Decimal("1000"),
        pip_size=Decimal("0.0001"),
        price_precision=5,
        horizons_seconds=config.legacy_horizons,
        decision_latency_seconds=config.decision_latency_seconds,
        entry_latency_seconds=Decimal("0"),
        close_latency_seconds=Decimal("0"),
        max_quote_age_seconds=Decimal("5"),
        commission_fixed=economics["commission_fixed"],
        commission_per_unit=economics["commission_per_unit"],
        commission_known=economics["commission_known"],
        financing_required=economics["financing_required"],
        financing_rate_per_second=economics["financing_rate_per_second"],
        slippage_pips=config.slippage_pips,
        terminal_retention=config.max_retained_rows,
        event_retention=config.max_retained_rows,
        quote_id_retention=config.max_retained_rows,
        max_active_trades=config.max_retained_rows,
    )


def _risk_cfd_config(
    config: HistoricalBacktestConfig,
    profile: HistoricalProfile,
    instrument: str,
) -> CFDConfig:
    """Build the single policy-enabled CFD state machine for one profile."""

    # RiskExit owns stops, targets, MFE/MAE and time exits.  Its legacy
    # horizon is retained only as a terminal safety bound; it never closes a
    # risk-managed trade before the shared policy decision.
    return replace(
        _legacy_cfd_config(config, instrument),
        horizons_seconds=(Decimal("259200"),),
        risk_exit_policy=_profile_policy(config, profile),
        risk_exit_contract_spec=dict(_effective_contract_spec(config)),
        risk_exit_calendar=dict(config.calendar_state) if config.calendar_state is not None else None,
        risk_exit_mode=config.scenario_mode,
        risk_trigger_timeframe=profile.trigger_timeframe,
        risk_bar_clock_basis=RISK_BAR_CLOCK_BASIS,
    )


def _historical_quote_to_quote(
    quote: HistoricalQuote,
    config: HistoricalBacktestConfig,
    *,
    gap: bool = False,
) -> CFDQuote:
    locator = quote.locator
    quote_id = str(quote.source_event_id)
    raw_mid = (quote.bid + quote.ask) / Decimal(2)
    half_spread = (quote.ask - quote.bid) * config.spread_multiplier / Decimal(2)
    bid = raw_mid - half_spread
    ask = raw_mid + half_spread
    metadata = {
        "historical": True,
        "availability_policy": quote.availability_basis,
        "locator": locator.to_dict(),
        "quote_contract_version": 2,
        "receipt_basis": "MODEL_NO_RECEIPT",
        "modelled_availability": True,
        "scenario": config.scenario,
        "scenario_parameters": dict(config.scenario_parameters),
        "raw_bid": quote.bid,
        "raw_ask": quote.ask,
        "raw_mid": raw_mid,
        "raw_spread": quote.ask - quote.bid,
        "spread_multiplier": config.spread_multiplier,
        "cost_multiplier": config.cost_multiplier,
        "bid_source_timestamp": quote.event_time,
        "ask_source_timestamp": quote.event_time,
        "bid_available_at": quote.available_at,
        "ask_available_at": quote.available_at,
        "bid_timestamp_known": True,
        "ask_timestamp_known": True,
        "updated_sides": ("bid", "ask"),
        "risk_gap": gap,
    }
    return CFDQuote(
        instrument=quote.instrument,
        market_time=quote.event_time,
        bid=bid,
        ask=ask,
        quote_id=quote_id,
        available_at=quote.available_at,
        source="historical_bbo_model_no_receipt",
        sequence=quote.sequence,
        quality="VALID",
        metadata=metadata,
        bid_source_timestamp=quote.event_time,
        ask_source_timestamp=quote.event_time,
        bid_available_at=quote.available_at,
        ask_available_at=quote.available_at,
        bid_timestamp_known=True,
        ask_timestamp_known=True,
        updated_sides=("bid", "ask"),
    )


def _historical_quote_to_event(quote: HistoricalQuote) -> MarketEvent:
    locator = quote.locator
    event_id = f"hist:{quote.source_event_id}"
    return MarketEvent(
        instrument=quote.instrument,
        event_time=quote.event_time,
        bid=float(quote.bid),
        ask=float(quote.ask),
        available_at=quote.available_at,
        source="historical_bbo",
        mode=OperationMode.REPLAY,
        price_base=PriceBase.MID,
        event_kind=EventKind.QUOTE,
        sequence=quote.sequence,
        source_event_id=event_id,
        event_id=event_id,
        metadata={
            "historical": True,
            "availability_policy": quote.availability_basis,
            "locator": locator.to_dict(),
            "bbo_mid_feature": str((quote.bid + quote.ask) / Decimal(2)),
        },
    )


def _quote_signal(
    signal: Signal,
    profile: HistoricalProfile,
    *,
    atr: Any | None = None,
    mode: str = "VIRTUAL_DIAGNOSTIC",
    scenario: str = "base",
    scenario_hash: str | None = None,
) -> CFDSignal:
    direction = str(signal.direction).upper()
    if direction in {"UP", "LONG", "BUY"}:
        side = Direction.LONG
    elif direction in {"DOWN", "SHORT", "SELL"}:
        side = Direction.SHORT
    else:
        raise HistoricalBacktestError(f"signal direction inválida: {signal.direction!r}")
    return CFDSignal(
        signal_id=f"{profile.candidate_id}:{signal.signal_id}",
        instrument=signal.instrument,
        direction=side,
        detected_at=signal.detected_at,
        available_at=signal.detected_at,
        strategy=profile.candidate_id,
        quality="VALID",
        metadata={
            "candidate_id": profile.candidate_id,
            "core_signal_id": signal.signal_id,
            "trigger_timeframe": profile.trigger_timeframe,
            "risk_atr": atr,
            "risk_exit_mode": mode,
            "mode": mode,
            "scenario": scenario,
            "scenario_hash": scenario_hash,
        },
    )


def _partition(timestamp: datetime, config: HistoricalBacktestConfig) -> str:
    year = timestamp.year
    if year < config.warmup_start_year or year > config.holdout_end_year:
        raise HistoricalBacktestError(f"historical timestamp fuera del protocolo: {timestamp.isoformat()}")
    if year >= config.holdout_start_year:
        raise HistoricalBacktestError("HOLDOUT_CLOSED")
    if year < config.development_start_year:
        return "WARMUP"
    if year <= config.development_end_year:
        return "DEV"
    if year <= config.walkforward_end_year:
        return f"WF_{year}"
    raise HistoricalBacktestError("periodo histórico no registrado")


def _profile_policy(config: HistoricalBacktestConfig, profile: MarketProfile) -> RiskExitPolicy:
    return replace(
        profile.policy_for(config.risk_policy),
        exit_latency_seconds=config.decision_latency_seconds,
    )


def _indicator_at(processor: IncrementalProcessor, timeframe: str, at: datetime) -> Any | None:
    return processor.latest_indicator_point(timeframe, at=at)


def _risk_state_for_plan(
    simulator: CFDSimulator,
    policy: RiskExitPolicy,
    quote: CFDQuote,
) -> dict[str, Any]:
    """Read the live virtual PAPER budget owned by ``CFDSimulator``."""

    raw = simulator.risk_state
    result = dict(raw) if isinstance(raw, Mapping) else {}
    equity = result.get("equity", policy.initial_equity)
    if equity is None:
        equity = policy.initial_equity
    result["equity"] = equity
    if result.get("daily_pnl") is None:
        result["daily_pnl"] = "0"
    if result.get("drawdown") is None:
        result["drawdown"] = "0"
    if result.get("daily_anchor_equity") is None:
        result["daily_anchor_equity"] = equity
    if result.get("high_water_equity") is None:
        result["high_water_equity"] = equity
    active = tuple(item for item in simulator.trades if not item.is_terminal)
    risk_intents = tuple(
        item for item in simulator.trades if not item.is_terminal and (item.risk_atr is not None or item.risk_exit_plan)
    )
    result["positions"] = sum(item.state.value == "FILLED" and item.risk_exit_plan is not None for item in active)
    result["intents"] = len(risk_intents)
    result["costs_known"] = bool(result.get("costs_known") and quote.spread is not None)
    return result


def _public_risk_state(simulator: CFDSimulator) -> dict[str, Any]:
    raw = simulator.risk_state
    result: dict[str, Any] = dict(raw) if isinstance(raw, Mapping) else {"status": "UNKNOWN"}
    if result.get("costs_known") is not True:
        for key in (
            "equity",
            "realized_pnl",
            "floating_pnl",
            "daily_pnl",
            "drawdown",
            "daily_anchor_equity",
            "high_water_equity",
        ):
            result[key] = None
        result["status"] = "UNKNOWN_COSTS"
    return result


def _effective_entry_price(signal: Signal, quote: CFDQuote, simulator: CFDSimulator) -> Decimal:
    direction = str(signal.direction).upper()
    if direction in {"UP", "LONG", "BUY"}:
        raw = quote.ask
        sign = Decimal("1")
    elif direction in {"DOWN", "SHORT", "SELL"}:
        raw = quote.bid
        sign = Decimal("-1")
    else:
        raise HistoricalBacktestError(f"signal direction inválida: {signal.direction!r}")
    if raw is None:
        raise HistoricalBacktestError("signal requiere una pierna BBO ejecutable")
    slipped = raw + sign * simulator.config.slippage_price
    quantum = Decimal(1).scaleb(-simulator.config.price_precision)
    return quantize_decimal(slipped, quantum, policy=DEFAULT_DECIMAL_POLICY)


def _plan_for_signal(
    config: HistoricalBacktestConfig,
    profile: HistoricalProfile,
    processor: IncrementalProcessor,
    signal: Signal,
    quote: CFDQuote,
    risk_simulator: CFDSimulator,
) -> EntryPlan:
    indicator = _indicator_at(processor, profile.trigger_timeframe, signal.trigger_end)
    atr = getattr(indicator, "atr", None) if indicator is not None else None
    entry_price = _effective_entry_price(signal, quote, risk_simulator)
    policy = _profile_policy(config, profile)
    risk_state = _risk_state_for_plan(risk_simulator, policy, quote)
    calendar_state = config.calendar_state_for_quote(quote.available_ts)
    return plan_entry(
        policy,
        direction=signal.direction,
        entry_price=entry_price,
        atr=atr,
        equity=risk_state["equity"],
        available_at=quote.available_ts,
        contract_spec=_effective_contract_spec(config),
        calendar_state=calendar_state,
        risk_state=risk_state,
        equity_source="VIRTUAL_PAPER_ONLY",
        mode="VIRTUAL_DIAGNOSTIC",
    )


class _ArtifactWriter:
    def __init__(
        self,
        output_dir: Path,
        sink: ArtifactSink,
        max_retained: int,
        *,
        resume: bool,
        truncate_offsets: Mapping[str, int] | None = None,
    ) -> None:
        self.output_dir = output_dir
        self.sink = sink
        self.max_retained = max_retained
        self._files = {kind: output_dir / f"{kind}.jsonl" for kind in ("ledger", "equity", "funnel")}
        self._streams: dict[str, Any] = {}
        self._counts: Counter[str] = Counter()
        self._retained: dict[str, deque[Mapping[str, Any]]] = {kind: deque(maxlen=max_retained) for kind in self._files}
        self._identity_limit = max(BLOCK_SIZE, max_retained)
        self._identities: dict[str, set[str]] = {kind: set() for kind in self._files}
        self._identity_order: dict[str, deque[str]] = {kind: deque() for kind in self._files}
        try:
            for kind, path in self._files.items():
                if resume and truncate_offsets and kind in truncate_offsets:
                    self._truncate_artifact(path, truncate_offsets[kind])
                self._streams[kind] = self._open_artifact(path, resume=resume)
                if resume and path.stat().st_size:
                    self._load_existing(kind, path)
        except BaseException:
            self.close()
            raise

    @staticmethod
    def _truncate_artifact(path: Path, offset: int) -> None:
        try:
            info = os.lstat(path)
        except FileNotFoundError as exc:
            raise HistoricalBacktestError(f"falta artefacto del checkpoint: {path}") from exc
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            raise HistoricalBacktestError(f"artefacto no es archivo regular: {path}")
        if info.st_uid != os.getuid() or info.st_nlink != 1 or info.st_size < offset:
            raise HistoricalBacktestError(f"artefacto incompatible con checkpoint: {path}")
        if offset:
            read_flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
            read_fd = os.open(path, read_flags)
            try:
                if os.pread(read_fd, 1, offset - 1) != b"\n":
                    raise HistoricalBacktestError(f"offset de artefacto no termina una fila: {path}")
            finally:
                os.close(read_fd)
        if info.st_size == offset:
            return
        fd = os.open(path, os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            os.ftruncate(fd, offset)
            os.fsync(fd)
        finally:
            os.close(fd)

    @staticmethod
    def _open_artifact(path: Path, *, resume: bool) -> Any:
        try:
            info = os.lstat(path)
        except FileNotFoundError:
            info = None
        if info is not None:
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
                raise HistoricalBacktestError(f"artefacto no es archivo regular: {path}")
            if info.st_uid != os.getuid() or info.st_nlink != 1:
                raise HistoricalBacktestError(f"artefacto no pertenece exclusivamente al usuario: {path}")
            if not resume and info.st_size:
                raise HistoricalBacktestError(f"artefacto existente sin resume: {path}")
        flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(path, flags, 0o600)
        try:
            os.fchmod(fd, stat.S_IRUSR | stat.S_IWUSR)
            checked = os.fstat(fd)
            if checked.st_uid != os.getuid() or checked.st_nlink != 1 or not stat.S_ISREG(checked.st_mode):
                raise HistoricalBacktestError(f"artefacto cambió durante apertura: {path}")
            return os.fdopen(fd, "a", encoding="utf-8")
        except BaseException:
            os.close(fd)
            raise

    def _load_existing(self, kind: str, path: Path) -> None:
        """Recover bounded artifact counters without replaying old sink rows."""

        with path.open("r", encoding="utf-8") as stream:
            for line_number, line in enumerate(stream, 1):
                if not line.strip():
                    continue
                try:
                    value = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise HistoricalBacktestError(f"artefacto {kind} inválido en línea {line_number}") from exc
                if not isinstance(value, Mapping):
                    raise HistoricalBacktestError(f"artefacto {kind} contiene una fila inválida")
                detached = cast(Mapping[str, Any], _jsonable(dict(value)))
                self._counts[kind] += 1
                self._retained[kind].append(detached)
                self._remember_identity(kind, detached)

    def _remember_identity(self, kind: str, row: Mapping[str, Any]) -> None:
        identity = row.get("evaluation_id")
        if identity is None:
            return
        value = str(identity)
        identities = self._identities[kind]
        if value in identities:
            return
        identities.add(value)
        order = self._identity_order[kind]
        order.append(value)
        while len(order) > self._identity_limit:
            identities.discard(order.popleft())

    def emit(self, kind: str, row: Mapping[str, Any]) -> None:
        if kind not in self._files:
            raise HistoricalBacktestError(f"tipo de artefacto no soportado: {kind}")
        detached = cast(Mapping[str, Any], _jsonable(dict(row)))
        identity = detached.get("evaluation_id")
        if identity is not None and str(identity) in self._identities[kind]:
            return
        stream = self._streams[kind]
        stream.write(canonical_json(dict(detached)) + "\n")
        stream.flush()
        self._counts[kind] += 1
        self._retained[kind].append(detached)
        self._remember_identity(kind, detached)
        self.sink(kind, detached)

    def artifact_offsets(self) -> dict[str, int]:
        """Flush and durably sync every artifact before exposing its offset.

        The checkpoint records byte offsets used to truncate a possible
        speculative suffix on resume.  Returning an offset before the JSONL
        file descriptor has reached stable storage would make that offset
        unusable after a crash, so the ordering is intentionally owned here:
        write -> flush -> fsync -> offset -> checkpoint.
        """

        offsets: dict[str, int] = {}
        for kind, stream in self._streams.items():
            stream.flush()
            os.fsync(stream.fileno())
            offsets[kind] = int(stream.tell())
        return offsets

    def close(self) -> None:
        for stream in self._streams.values():
            stream.close()

    def rows(self, kind: str) -> ArtifactRows:
        return ArtifactRows(kind, self._files[kind], self._counts[kind], tuple(self._retained[kind]))


@dataclass(slots=True)
class _ProfileState:
    profile: HistoricalProfile
    processor: IncrementalProcessor
    legacy: CFDSimulator
    risk: CFDSimulator
    bar_closes: int = 0
    quote_count: int = 0
    last_quote_time: datetime | None = None
    seen_signal_ids: set[str] = field(default_factory=set)
    risk_ledger_signatures: dict[str, str] = field(default_factory=dict)
    counters: Counter[str] = field(default_factory=Counter)
    partitions: Counter[str] = field(default_factory=Counter)

    def checkpoint(self) -> dict[str, Any]:
        # A shared plane is checkpointed once by the enclosing historical
        # runner.  Each profile therefore persists only its mutable strategy
        # and consumer state; the legacy path keeps its original full
        # processor snapshot for backwards compatibility.
        processor_checkpoint = (
            self.processor.strategy_checkpoint()
            if self.processor.data_plane is not None
            else self.processor.checkpoint()
        )
        return {
            "profile": self.profile.to_dict(),
            "processor": processor_checkpoint,
            "legacy": self.legacy.snapshot(),
            "risk": self.risk.snapshot(),
            "bar_closes": self.bar_closes,
            "quote_count": self.quote_count,
            "last_quote_time": instant_text(self.last_quote_time) if self.last_quote_time is not None else None,
            "seen_signal_ids": sorted(self.seen_signal_ids),
            "risk_ledger_signatures": dict(self.risk_ledger_signatures),
            "counters": dict(self.counters),
            "partitions": dict(self.partitions),
        }


def _shared_data_plane_timeframes(profiles: Sequence[HistoricalProfile]) -> tuple[str, ...]:
    """Return the deterministic union needed by the selected profiles."""

    names: set[str] = set()
    for profile in profiles:
        selected = _donchian_processor_timeframes(profile) if profile.is_donchian else profile.timeframes
        names.update(selected)
    return tuple(sorted(names, key=lambda name: parse_timeframe(name).seconds))


def _new_historical_data_plane(
    config: HistoricalBacktestConfig,
    instrument: str,
) -> SharedDataPlane:
    """Build the one data/indicator plane for this historical run."""

    return SharedDataPlane(
        timeframes=_shared_data_plane_timeframes(config.profiles),
        indicator_config=_INDICATOR_CONFIG,
        mode=OperationMode.REPLAY,
        instrument=instrument,
        source=_HISTORICAL_SHARED_PLANE_SOURCE,
        price_base=PriceBase.MID,
        max_candles=config.max_candles,
        coverage_mode=_HISTORICAL_QUOTE_COVERAGE_MODE,
        max_quote_gap_seconds=_HISTORICAL_MAX_QUOTE_GAP_SECONDS,
        historical_calendar=_HISTORICAL_CALENDAR,
    )


def _validate_historical_data_plane(
    data_plane: SharedDataPlane,
    config: HistoricalBacktestConfig,
    instrument: str,
) -> None:
    """Reject a checkpoint plane that is not the current frozen composition."""

    expected_timeframes = _shared_data_plane_timeframes(config.profiles)
    if tuple(item.name for item in data_plane.timeframes) != expected_timeframes:
        raise HistoricalBacktestError("data_plane temporalidades incompatibles")
    if data_plane.instrument != instrument:
        raise HistoricalBacktestError("data_plane instrument incompatible")
    if data_plane.mode is not OperationMode.REPLAY or data_plane.price_base is not PriceBase.MID:
        raise HistoricalBacktestError("data_plane mode/price_base incompatible")
    if data_plane.indicator_config != _INDICATOR_CONFIG:
        raise HistoricalBacktestError("data_plane indicator_config incompatible")
    if data_plane.max_candles != config.max_candles:
        raise HistoricalBacktestError("data_plane max_candles incompatible")
    if data_plane.quote_coverage_mode != _HISTORICAL_QUOTE_COVERAGE_MODE:
        raise HistoricalBacktestError("data_plane coverage_mode incompatible")
    if data_plane.max_quote_gap_seconds != _HISTORICAL_MAX_QUOTE_GAP_SECONDS:
        raise HistoricalBacktestError("data_plane max_quote_gap_seconds incompatible")
    expected_calendar = _HISTORICAL_CALENDAR.to_dict()
    actual_calendar = data_plane.historical_calendar.to_dict() if data_plane.historical_calendar is not None else None
    if actual_calendar != expected_calendar:
        raise HistoricalBacktestError("data_plane historical_calendar incompatible")


def _data_plane_from_checkpoint(
    checkpoint: HistoricalBacktestCheckpoint,
    config: HistoricalBacktestConfig,
    instrument: str,
) -> SharedDataPlane | None:
    raw_state = checkpoint.state.get("data_plane")
    if raw_state is None:
        return None
    if not isinstance(raw_state, Mapping):
        raise HistoricalBacktestError("checkpoint data_plane incompleto")
    try:
        data_plane = SharedDataPlane.from_checkpoint(raw_state)
    except (TypeError, ValueError) as exc:
        raise HistoricalBacktestError("checkpoint data_plane incompatible") from exc
    _validate_historical_data_plane(data_plane, config, instrument)
    return data_plane


def _profile_state(
    config: HistoricalBacktestConfig,
    profile: HistoricalProfile,
    instrument: str,
    *,
    data_plane: SharedDataPlane | None = None,
) -> _ProfileState:
    processor_timeframes = profile.timeframes if not profile.is_donchian else _donchian_processor_timeframes(profile)
    strategy = _strategy_config(profile) if not profile.is_donchian else StrategyConfig(mode=OperationMode.REPLAY)
    consumer = RecordingSignalConsumer(max_events=config.max_retained_rows, max_signals=config.max_retained_rows)
    processor = IncrementalProcessor(
        strategy=strategy,
        timeframes=processor_timeframes,
        mode=OperationMode.REPLAY,
        instrument=instrument,
        source=f"historical:{profile.candidate_id}",
        price_base=PriceBase.MID,
        max_candles=config.max_candles,
        signal_consumer=consumer,
        market_candidate_id=profile.candidate_id,
        data_plane=data_plane,
        quote_coverage_mode=_HISTORICAL_QUOTE_COVERAGE_MODE,
        max_quote_gap_seconds=_HISTORICAL_MAX_QUOTE_GAP_SECONDS,
        historical_calendar=_HISTORICAL_CALENDAR,
    )
    legacy = CFDSimulator(_legacy_cfd_config(config, instrument), max_active_trades=config.max_retained_rows)
    risk = CFDSimulator(_risk_cfd_config(config, profile, instrument), max_active_trades=config.max_retained_rows)
    return _ProfileState(
        profile,
        processor,
        legacy,
        risk,
    )


def _donchian_processor_timeframes(profile: HistoricalProfile) -> tuple[str, ...]:
    target = parse_timeframe(profile.timeframes[0])
    base = ("M1", "M5", "M15", "H1", "H4", "D1")
    return tuple(name for name in base if parse_timeframe(name).seconds <= target.seconds or name == target.name)


def _restore_profile_processor(
    raw_processor: Mapping[str, Any],
    *,
    profile_id: str,
    max_retained_rows: int,
    data_plane: SharedDataPlane | None,
) -> IncrementalProcessor:
    consumer = RecordingSignalConsumer(max_events=max_retained_rows, max_signals=max_retained_rows)
    if data_plane is not None:
        if raw_processor.get("checkpoint_scope") != "strategy":
            raise HistoricalBacktestError(f"checkpoint strategy incompleto: {profile_id}")
        try:
            return IncrementalProcessor.from_strategy_checkpoint(
                raw_processor,
                data_plane=data_plane,
                signal_consumer=consumer,
            )
        except (TypeError, ValueError) as exc:
            raise HistoricalBacktestError(f"checkpoint strategy incompatible: {profile_id}") from exc
    if raw_processor.get("checkpoint_scope") == "strategy":
        raise HistoricalBacktestError("checkpoint data_plane missing")
    return IncrementalProcessor.from_checkpoint(raw_processor, signal_consumer=consumer)


def _profile_from_state(
    config: HistoricalBacktestConfig,
    raw: Mapping[str, Any],
    instrument: str,
    *,
    data_plane: SharedDataPlane | None = None,
) -> _ProfileState:
    profile_id = str(cast(Mapping[str, Any], raw.get("profile", {})).get("candidate_id", ""))
    try:
        profile = market_profile(profile_id)
    except ValueError as exc:
        raise HistoricalBacktestError(f"checkpoint profile desconocido: {profile_id}") from exc
    if profile not in config.profiles:
        raise HistoricalBacktestError(f"checkpoint profile no pertenece al config: {profile_id}")
    raw_processor_value = raw.get("processor")
    if not isinstance(raw_processor_value, Mapping):
        raise HistoricalBacktestError(f"checkpoint processor incompleto: {profile_id}")
    raw_processor = raw_processor_value
    if str(raw_processor.get("market_candidate_id", "")) != profile.candidate_id:
        raise HistoricalBacktestError(f"checkpoint candidate_id incompatible: {profile_id}")
    processor = _restore_profile_processor(
        raw_processor,
        profile_id=profile_id,
        max_retained_rows=config.max_retained_rows,
        data_plane=data_plane,
    )
    legacy = CFDSimulator.from_snapshot(cast(Mapping[str, Any], raw["legacy"]))
    raw_risk = raw.get("risk")
    if not isinstance(raw_risk, Mapping):
        raise HistoricalBacktestError(f"checkpoint risk incompleto: {profile_id}")
    risk = CFDSimulator.from_snapshot(raw_risk)
    expected_risk = _risk_cfd_config(config, profile, instrument)
    if risk.config.config_hash != expected_risk.config_hash:
        raise HistoricalBacktestError(f"checkpoint risk config incompatible: {profile_id}")
    state = _ProfileState(profile, processor, legacy, risk)
    state.bar_closes = int(raw.get("bar_closes", 0))
    state.quote_count = int(raw.get("quote_count", 0))
    raw_last_quote = raw.get("last_quote_time")
    state.last_quote_time = _utc(raw_last_quote, "last_quote_time") if raw_last_quote is not None else None
    state.seen_signal_ids = {str(item) for item in raw.get("seen_signal_ids", ())}
    state.risk_ledger_signatures = {
        str(key): str(value) for key, value in dict(raw.get("risk_ledger_signatures", {})).items()
    }
    state.counters.update({str(key): int(value) for key, value in dict(raw.get("counters", {})).items()})
    state.partitions.update({str(key): int(value) for key, value in dict(raw.get("partitions", {})).items()})
    return state


def _write_checkpoint(output_dir: Path, checkpoint: HistoricalBacktestCheckpoint) -> None:
    path = output_dir / "checkpoint.json"
    try:
        existing = os.lstat(path)
    except FileNotFoundError:
        existing = None
    if existing is not None and (
        stat.S_ISLNK(existing.st_mode)
        or not stat.S_ISREG(existing.st_mode)
        or existing.st_uid != os.getuid()
        or existing.st_nlink != 1
    ):
        raise HistoricalBacktestError(f"checkpoint no es archivo regular exclusivo: {path}")
    temporary = output_dir / f".{path.name}.{os.getpid()}.tmp"
    payload = (canonical_json(checkpoint.to_dict()) + "\n").encode("utf-8")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        offset = 0
        while offset < len(payload):
            offset += os.write(fd, payload[offset:])
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(temporary, path)
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
    directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    directory_fd = os.open(output_dir, directory_flags)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _output_directory(value: str | Path) -> Path:
    target = Path(value).expanduser().absolute()
    resolved = target.resolve(strict=False)
    if resolved == _REPOSITORY or _REPOSITORY in resolved.parents:
        raise HistoricalBacktestError("output_dir debe estar fuera del checkout")
    if target.exists():
        if target.is_symlink():
            raise HistoricalBacktestError("output_dir no puede ser symlink")
        info = target.stat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
            raise HistoricalBacktestError("output_dir debe ser directorio del usuario")
    else:
        target.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(target, 0o700)
    return target


def _validate_checkpoint_profile_lifecycle(
    raw_profile: Mapping[str, Any],
    profile_id: str,
    *,
    processed_quotes: int,
    finished: bool,
) -> None:
    raw_count = raw_profile.get("quote_count")
    if isinstance(raw_count, bool) or not isinstance(raw_count, int) or raw_count != processed_quotes:
        raise HistoricalBacktestError(f"checkpoint quote_count incompatible: {profile_id}")
    for simulator_name in ("legacy", "risk"):
        raw_simulator = raw_profile.get(simulator_name)
        if not isinstance(raw_simulator, Mapping):
            raise HistoricalBacktestError(f"checkpoint {simulator_name} incompleto: {profile_id}")
        if raw_simulator.get("finished") is not finished:
            raise HistoricalBacktestError(f"checkpoint {simulator_name} lifecycle incompatible: {profile_id}")


def _validate_checkpoint_state_lifecycle(checkpoint: HistoricalBacktestCheckpoint) -> None:
    """Ensure the durable lifecycle flag agrees with every restored owner."""

    raw_profiles = checkpoint.state.get("profiles")
    if not isinstance(raw_profiles, Mapping):
        raise HistoricalBacktestError("checkpoint profiles incompletos")
    if set(raw_profiles) != set(checkpoint.candidate_ids):
        raise HistoricalBacktestError("checkpoint profiles no coinciden con candidate_ids")
    for profile_id in checkpoint.candidate_ids:
        raw_profile = raw_profiles.get(profile_id)
        if not isinstance(raw_profile, Mapping):
            raise HistoricalBacktestError(f"checkpoint sin profile: {profile_id}")
        _validate_checkpoint_profile_lifecycle(
            raw_profile,
            profile_id,
            processed_quotes=checkpoint.processed_quotes,
            finished=checkpoint.finished,
        )
    raw_plane = checkpoint.state.get("data_plane")
    if isinstance(raw_plane, Mapping):
        events_processed = raw_plane.get("events_processed")
        if isinstance(events_processed, bool) or not isinstance(events_processed, int):
            raise HistoricalBacktestError("checkpoint data_plane events_processed incompatible")
        if events_processed != checkpoint.processed_quotes:
            raise HistoricalBacktestError("checkpoint data_plane events_processed incompatible")


def _checkpoint_resume(
    value: HistoricalBacktestCheckpoint | Mapping[str, Any] | None,
    *,
    manifest: DatasetManifest,
    dataset_id: str,
    data_hash: str,
    config: HistoricalBacktestConfig,
) -> HistoricalBacktestCheckpoint | None:
    if value is None:
        return None
    checkpoint = (
        value if isinstance(value, HistoricalBacktestCheckpoint) else HistoricalBacktestCheckpoint.from_mapping(value)
    )
    if (
        checkpoint.dataset_id != dataset_id
        or checkpoint.data_hash != data_hash
        or checkpoint.config_hash != config.config_hash
        or checkpoint.protocol_hash != config.protocol_hash
        or checkpoint.scenario != config.scenario
        or checkpoint.candidate_ids != config.candidate_ids
        or checkpoint.checkpoint_interval_blocks != config.checkpoint_interval_blocks
    ):
        raise HistoricalBacktestError("resume identity mismatch")
    _validate_checkpoint_state_lifecycle(checkpoint)
    if checkpoint.guard_snapshot is None:
        raise HistoricalBacktestError("resume guard snapshot missing")
    try:
        guard = HistoricalStreamIdentityGuard.from_snapshot(manifest, checkpoint.guard_snapshot)
    except ValueError as exc:
        raise HistoricalBacktestError("resume guard snapshot incompatible") from exc
    if guard.manifest_hash != data_hash:
        raise HistoricalBacktestError("resume guard manifest hash mismatch")
    cursor = guard.cursor
    checkpoint_time = _utc(checkpoint.cursor_time, "checkpoint.cursor_time")
    if checkpoint.cursor_sequence < 0:
        if cursor is not None:
            raise HistoricalBacktestError("resume guard cursor conflicts with checkpoint")
    elif cursor is None or cursor.sequence != checkpoint.cursor_sequence or cursor.event_time != checkpoint_time:
        raise HistoricalBacktestError("resume guard cursor conflicts with checkpoint")
    return checkpoint


def _emit_funnel(
    writer: _ArtifactWriter,
    profile: HistoricalProfile,
    partition: str,
    evaluation: Any,
    *,
    scenario: str = "base",
    scenario_hash: str | None = None,
) -> None:
    raw = evaluation.as_dict() if hasattr(evaluation, "as_dict") else evaluation.to_dict()
    stage = str(raw.get("stage", "evaluation"))
    timeframe = profile.trigger_timeframe
    if stage == "preparation" and len(profile.timeframes) >= 2:
        timeframe = profile.timeframes[1]
    evaluation_id = fingerprint(
        {
            "candidate_id": profile.candidate_id,
            "partition": partition,
            "scenario": scenario,
            "evaluation": _jsonable(raw),
        }
    )
    writer.emit(
        "funnel",
        {
            "candidate_id": profile.candidate_id,
            "variant": profile.candidate_id,
            "source": HISTORICAL_SOURCE,
            "timeframe": timeframe,
            "stage": stage,
            "mode": "VIRTUAL_DIAGNOSTIC",
            "scenario": scenario,
            "scenario_hash": scenario_hash,
            "evaluation_id": evaluation_id,
            "partition": partition,
            "decision": raw.get("decision", raw.get("status", "UNKNOWN")),
            "reasons": list(raw.get("reasons", ())),
        },
    )


def _signal_atr_processor(profile: HistoricalProfile, state: _ProfileState, signal: Signal) -> Any | None:
    return _indicator_at(state.processor, profile.trigger_timeframe, signal.trigger_end)


def _handle_signal(
    config: HistoricalBacktestConfig,
    state: _ProfileState,
    signal: Signal,
    quote: CFDQuote,
    partition: str,
    writer: _ArtifactWriter,
) -> None:
    if signal.signal_id in state.seen_signal_ids:
        return
    state.seen_signal_ids.add(signal.signal_id)
    state.counters["signals"] += 1
    plan = _plan_for_signal(config, state.profile, state.processor, signal, quote, state.risk)
    entry_gate = config.entry_gate_for(state.profile, quote.available_ts)
    if entry_gate is not None and not entry_gate.allowed:
        gate_reason = entry_gate.reason or "CALENDAR_ENTRY_BLOCKED"
        plan = replace(
            plan,
            allowed=False,
            reasons=tuple(dict.fromkeys((*plan.reasons, gate_reason))),
            assumptions={**dict(plan.assumptions), "calendar_entry_gate": entry_gate.to_dict()},
            eligible_for_demo=False,
        )
        state.counters["calendar_entry_blocked"] += 1
    state.counters["entry_allowed" if plan.allowed else "entry_blocked"] += 1
    writer.emit(
        "funnel",
        {
            "candidate_id": state.profile.candidate_id,
            "variant": state.profile.candidate_id,
            "source": HISTORICAL_SOURCE,
            "timeframe": state.profile.trigger_timeframe,
            "stage": "entry_plan",
            "mode": config.scenario_mode,
            "scenario": config.scenario,
            "scenario_hash": config.scenario_hash,
            "evaluation_id": f"{state.profile.candidate_id}:{config.scenario}:{signal.signal_id}:entry",
            "partition": partition,
            "decision": "ALLOWED" if plan.allowed else "BLOCKED",
            "reasons": list(plan.reasons),
            "plan": plan.to_dict(),
        },
    )
    writer.emit(
        "ledger",
        {
            "path": "dynamic_risk",
            "event": "ENTRY_PLAN",
            "candidate_id": state.profile.candidate_id,
            "variant": state.profile.candidate_id,
            "timeframe": state.profile.trigger_timeframe,
            "stage": "entry",
            "mode": config.scenario_mode,
            "scenario": config.scenario,
            "scenario_hash": config.scenario_hash,
            "evaluation_id": f"{state.profile.candidate_id}:{config.scenario}:{signal.signal_id}:entry",
            "partition": partition,
            "signal": signal.as_dict(),
            "entry_plan": plan.to_dict(),
        },
    )
    if entry_gate is not None and not entry_gate.allowed:
        return
    legacy_signal = _quote_signal(
        signal,
        state.profile,
        atr=plan.atr,
        mode=config.scenario_mode,
        scenario=config.scenario,
        scenario_hash=config.scenario_hash,
    )
    for path, simulator in (
        ("legacy_horizon_control", state.legacy),
        ("dynamic_risk", state.risk),
    ):
        try:
            simulator.submit_all(legacy_signal)
        except Exception as exc:
            state.counters[f"{path}_errors"] += 1
            writer.emit(
                "ledger",
                {
                    "path": path,
                    "event": "CONTROL_ERROR" if path == "legacy_horizon_control" else "RISK_ERROR",
                    "candidate_id": state.profile.candidate_id,
                    "variant": state.profile.candidate_id,
                    "source": HISTORICAL_SOURCE,
                    "timeframe": state.profile.trigger_timeframe,
                    "stage": "legacy_control" if path == "legacy_horizon_control" else "risk",
                    "mode": config.scenario_mode,
                    "scenario": config.scenario,
                    "scenario_hash": config.scenario_hash,
                    "evaluation_id": f"{state.profile.candidate_id}:{config.scenario}:{signal.signal_id}:{path}",
                    "partition": partition,
                    "reason": type(exc).__name__,
                },
            )


def _risk_ledger_signature(payload: Mapping[str, Any]) -> str:
    decision = payload.get("risk_exit_decision")
    decision_map = decision if isinstance(decision, Mapping) else {}
    material = {
        "state": payload.get("state"),
        "entry_quote_id": payload.get("entry_quote_id"),
        "entry_price": payload.get("entry_price"),
        "close_quote_id": payload.get("close_quote_id"),
        "close_price": payload.get("close_price"),
        "reason": payload.get("reason"),
        "gross_pnl_quote": payload.get("gross_pnl_quote"),
        "gross_pnl_account": payload.get("gross_pnl_account"),
        "costs_account": payload.get("costs_account"),
        "net_pnl": payload.get("net_pnl"),
        "economic_state": (
            payload.get("economic_result", {}).get("state")
            if isinstance(payload.get("economic_result"), Mapping)
            else None
        ),
        "risk_exit_action": decision_map.get("action"),
        "risk_exit_reason": decision_map.get("reason"),
        "risk_exit_latency": decision_map.get("latency_seconds"),
        "risk_exit_due_at": payload.get("risk_exit_due_at"),
    }
    return fingerprint(_jsonable(material))


def _emit_risk_updates(
    config: HistoricalBacktestConfig,
    state: _ProfileState,
    changed: Iterable[Any],
    *,
    quote: CFDQuote,
    partition: str,
    writer: _ArtifactWriter,
    final: bool = False,
) -> None:
    for trade in changed:
        payload = trade.to_dict()
        trade_id = str(payload.get("trade_id", trade.identity))
        state_name = str(payload.get("state", "UNKNOWN"))
        signature = _risk_ledger_signature(payload)
        if not final and state.risk_ledger_signatures.get(trade_id) == signature:
            state.counters["risk_quote_marks_suppressed"] += 1
            continue
        state.risk_ledger_signatures[trade_id] = signature
        state.counters[f"risk_{state_name.lower()}"] += 1
        state.counters["risk_ledger_rows"] += 1
        quote_metadata = quote.metadata if isinstance(quote.metadata, Mapping) else {}
        writer.emit(
            "ledger",
            {
                "path": "dynamic_risk",
                "event": "RISK_FINAL" if final else "RISK_TRADE_UPDATE",
                "candidate_id": state.profile.candidate_id,
                "variant": state.profile.candidate_id,
                "source": HISTORICAL_SOURCE,
                "timeframe": state.profile.trigger_timeframe,
                "stage": "risk",
                "mode": "VIRTUAL_DIAGNOSTIC",
                "scenario": config.scenario,
                "scenario_hash": config.scenario_hash,
                "evaluation_id": f"{trade_id}:{config.scenario}:{quote.identity}:{'final' if final else 'quote'}",
                "partition": partition,
                "event_kind": "RISK_FINAL" if final else "RISK_TRADE_UPDATE",
                "observed_at": quote.available_ts,
                "trade_id": trade_id,
                "state": state_name,
                "trade": payload,
                "quote_id": quote.identity,
                "quote_locator": quote_metadata.get("locator"),
                "direction": payload.get("direction"),
                "entry_price": payload.get("entry_price"),
                "close_price": payload.get("close_price"),
                "entry_available_at": payload.get("entry_available_at"),
                "close_available_at": payload.get("close_available_at"),
                "net_pnl": payload.get("net_pnl"),
                "gross_pnl_quote": payload.get("gross_pnl_quote"),
                "gross_pnl_account": payload.get("gross_pnl_account"),
                "costs_account": payload.get("costs_account"),
                "mfe_price": payload.get("mfe_price"),
                "mae_price": payload.get("mae_price"),
                "r_multiple": payload.get("r_multiple"),
                "risk_exit_plan": payload.get("risk_exit_plan"),
                "risk_exit_decision": payload.get("risk_exit_decision"),
                "economic_result": payload.get("economic_result"),
                "quote_source": "HISTORICAL_BBO",
                "receipt_basis": "MODEL_NO_RECEIPT",
                "fill_source": "MODEL_FILL",
                "economics_state": _trade_economic_status(state.risk, trade),
            },
        )


def _gross_trade_mark(trade: Any, quote: CFDQuote) -> Decimal | None:
    if trade.state.value != "FILLED" or trade.entry_price is None:
        return Decimal("0") if trade.state.value != "FILLED" else None
    current = quote.bid if trade.direction.value == "LONG" else quote.ask
    if current is None:
        return None
    sign = Decimal("1") if trade.direction.value == "LONG" else Decimal("-1")
    return cast(Decimal, (current - trade.entry_price) * trade.units * sign)


def _trade_economic_status(simulator: CFDSimulator, trade: Any | None = None) -> str:
    """Label a ledger row without upgrading an unsettled/unknown net result."""

    if not _simulator_costs_known(simulator):
        return "UNKNOWN_COSTS"
    if trade is not None and getattr(trade, "state", None) is not None:
        state = getattr(trade.state, "value", str(trade.state))
        if state == "CLOSED" and getattr(trade, "net_pnl", None) is None:
            return "UNKNOWN_COSTS"
    return "ASSESSED_NET"


def _gross_equity_values(
    config: HistoricalBacktestConfig,
    state: _ProfileState,
    quote: CFDQuote,
) -> dict[str, Decimal | None]:
    realized = Decimal("0")
    unrealized = Decimal("0")
    exposure = Decimal("0")
    for trade in state.risk.trades:
        if trade.state.value == "CLOSED" or trade.close_observed:
            value = trade.gross_pnl_account or trade.gross_pnl_quote
            if value is not None:
                realized += value
        mark = _gross_trade_mark(trade, quote)
        if mark is not None and trade.state.value == "FILLED":
            unrealized += mark
            current = quote.bid if trade.direction.value == "LONG" else quote.ask
            if current is not None:
                exposure += abs(current * trade.units)
    return {
        "realized": realized,
        "unrealized": unrealized,
        "equity": config.risk_policy.initial_equity + realized + unrealized,
        "exposure": exposure,
    }


def _emit_equity(
    config: HistoricalBacktestConfig,
    state: _ProfileState,
    candle: Any,
    quote: CFDQuote,
    partition: str,
    writer: _ArtifactWriter,
) -> None:
    """Emit a bar-close equity observation without inventing PnL or fills."""

    observed_at = candle.available_at or candle.end
    risk_state = state.risk.risk_state or {}
    costs_known = risk_state.get("costs_known") is True
    gross = _gross_equity_values(config, state, quote)
    evaluation_id = fingerprint(
        {
            "candidate_id": state.profile.candidate_id,
            "timeframe": candle.timeframe_name,
            "candle_id": candle.candle_id,
            "partition": partition,
            "scenario": config.scenario,
        }
    )
    writer.emit(
        "equity",
        {
            "candidate_id": state.profile.candidate_id,
            "variant": state.profile.candidate_id,
            "source": HISTORICAL_SOURCE,
            "timeframe": candle.timeframe_name,
            "stage": "equity",
            "mode": "VIRTUAL_DIAGNOSTIC",
            "scenario": config.scenario,
            "scenario_hash": config.scenario_hash,
            "evaluation_id": evaluation_id,
            "partition": partition,
            "at": observed_at,
            "observed_at": observed_at,
            "equity": risk_state.get("equity") if costs_known else None,
            "realized": risk_state.get("realized_pnl") if costs_known else None,
            "unrealized": risk_state.get("floating_pnl") if costs_known else None,
            "net_equity": risk_state.get("equity") if costs_known else None,
            "net_realized": risk_state.get("realized_pnl") if costs_known else None,
            "net_unrealized": risk_state.get("floating_pnl") if costs_known else None,
            "gross_equity": gross["equity"],
            "gross_realized": gross["realized"],
            "gross_unrealized": gross["unrealized"],
            "gross_mark_to_market": gross["unrealized"],
            "gross_exposure": gross["exposure"],
            "gross_currency": config.risk_policy.account_currency,
            "open_positions": sum(
                item.state.value == "FILLED" and item.risk_exit_plan is not None for item in state.risk.trades
            ),
            "bar_close": candle.close,
            "economics_state": _trade_economic_status(state.risk),
            "equity_status": "MODELLED_VIRTUAL" if costs_known else "UNKNOWN_COSTS",
            "gross_status": "ASSESSED_GROSS_ONLY",
            "costs_known": costs_known,
            "basis": config.risk_policy.equity_basis,
            "quote_source": "HISTORICAL_BBO",
            "receipt_basis": "MODEL_NO_RECEIPT",
            "fill_source": "MODEL_FILL",
        },
    )
    state.counters["equity_updates"] += 1


def _process_result(
    config: HistoricalBacktestConfig,
    state: _ProfileState,
    result: ProcessResult,
    quote: CFDQuote,
    partition: str,
    writer: _ArtifactWriter,
) -> None:
    state.counters["processor_events"] += len(result.events)
    state.counters["processor_candles"] += len(result.candles)
    state.counters["processor_issues"] += len(result.issues)
    target = state.profile.trigger_timeframe
    closed_target_candles: list[Any] = []
    for candle in result.candles:
        if candle.timeframe_name == target and candle.closed:
            state.bar_closes += 1
            closed_target_candles.append(candle)
    for evaluation in result.evaluations:
        _emit_funnel(
            writer,
            state.profile,
            partition,
            evaluation,
            scenario=config.scenario,
            scenario_hash=config.scenario_hash,
        )
        state.counters["evaluations"] += 1
    for signal in result.signals:
        _handle_signal(config, state, signal, quote, partition, writer)
    for candle in closed_target_candles:
        _emit_equity(config, state, candle, quote, partition, writer)


def _finish_profile(
    config: HistoricalBacktestConfig,
    state: _ProfileState,
    last_quote: CFDQuote | None,
    partition: str,
    writer: _ArtifactWriter,
) -> None:
    final = state.processor.finalize(
        last_quote.available_ts if last_quote is not None else None,
        evaluate_strategy=partition != "WARMUP",
        capture_complete=True,
    )
    if last_quote is not None:
        _process_result(config, state, final, last_quote, partition, writer)
        if not state.risk.finished:
            risk_result = state.risk.finish(last_quote.available_ts, capture_complete=True)
            _emit_risk_updates(
                config,
                state,
                risk_result.trades,
                quote=last_quote,
                partition=partition,
                writer=writer,
                final=True,
            )
        if not state.legacy.finished:
            state.legacy.finish(last_quote.available_ts, capture_complete=True)


def _control_summary(state: _ProfileState, *, finalize: bool = True) -> dict[str, Any]:
    """Summarize the legacy control without finalizing partial state.

    A terminal result may close the legacy simulator to materialize its
    horizon control.  A partial checkpoint must not do that: the simulator's
    open quote/trade state is part of the continuation state and remains
    ``finished=False`` until the source is actually drained.
    """

    result = state.legacy.finish(capture_complete=True) if finalize else None
    trades = [trade.to_dict() for trade in (result.trades if result is not None else state.legacy.trades)]
    return {
        "path": "legacy_horizon_control",
        "promotable": False,
        "horizons_seconds": [format(item, "f") for item in state.legacy.config.horizons_seconds],
        "trade_count": len(trades),
        "terminal_count": sum(bool(item.get("state") in {"CLOSED", "REJECTED", "UNKNOWN"}) for item in trades),
        "unknown_count": sum(bool(item.get("state") == "UNKNOWN") for item in trades),
        "capture_complete": finalize,
        "finished": bool(result.finished) if result is not None else state.legacy.finished,
        "simulator_counters": dict(state.legacy.counters),
    }


def _variant_result(state: _ProfileState, control: Mapping[str, Any]) -> HistoricalVariantResult:
    risk_trades = tuple(state.risk.trades)
    active_risk = tuple(item for item in risk_trades if not item.is_terminal)
    metrics = {
        "quotes": state.quote_count,
        "processor_events": state.counters.get("processor_events", 0),
        "candles": state.counters.get("processor_candles", 0),
        "evaluations": state.counters.get("evaluations", 0),
        "signals": state.counters.get("signals", 0),
        "entry_allowed": state.counters.get("entry_allowed", 0),
        "entry_blocked": state.counters.get("entry_blocked", 0),
        "calendar_entry_blocked": state.counters.get("calendar_entry_blocked", 0),
        "equity_updates": state.counters.get("equity_updates", 0),
        "risk_trade_count": len(risk_trades),
        "risk_terminal_count": sum(item.is_terminal for item in risk_trades),
        "risk_active_count": len(active_risk),
        "risk_unknown_count": sum(item.state.value == "UNKNOWN" for item in risk_trades),
        "open_at_end": len(active_risk),
        "risk_ledger_rows": state.counters.get("risk_ledger_rows", 0),
        "risk_quote_marks_suppressed": state.counters.get("risk_quote_marks_suppressed", 0),
        "partitions": dict(state.partitions),
        "promotable": False,
        "economics": _trade_economic_status(state.risk),
    }
    return HistoricalVariantResult(
        state.profile.candidate_id,
        state.profile.strategy_impl,
        state.profile.timeframes,
        state.profile.holding_profile,
        metrics,
        control,
    )


def _restore_profiles(
    config: HistoricalBacktestConfig,
    checkpoint: HistoricalBacktestCheckpoint,
    instrument: str,
    *,
    data_plane: SharedDataPlane | None = None,
) -> dict[str, _ProfileState]:
    raw_profiles = checkpoint.state.get("profiles")
    if not isinstance(raw_profiles, Mapping):
        raise HistoricalBacktestError("checkpoint profiles incompletos")
    states: dict[str, _ProfileState] = {}
    for profile in config.profiles:
        raw = raw_profiles.get(profile.candidate_id)
        if not isinstance(raw, Mapping):
            raise HistoricalBacktestError(f"checkpoint sin profile: {profile.candidate_id}")
        states[profile.candidate_id] = _profile_from_state(
            config,
            raw,
            instrument,
            data_plane=data_plane,
        )
    return states


def _checkpoint_state(
    states: Mapping[str, _ProfileState],
    *,
    data_plane: SharedDataPlane | None = None,
) -> dict[str, Any]:
    selected_plane = data_plane
    if selected_plane is None:
        planes = [state.processor.data_plane for state in states.values() if state.processor.data_plane is not None]
        if planes:
            selected_plane = planes[0]
            if any(plane is not selected_plane for plane in planes[1:]):
                raise HistoricalBacktestError("profiles no comparten el mismo data_plane")
    if selected_plane is not None and any(
        state.processor.data_plane is not selected_plane for state in states.values()
    ):
        raise HistoricalBacktestError("profile sin el data_plane compartido")
    payload: dict[str, Any] = {"profiles": {key: value.checkpoint() for key, value in states.items()}}
    if selected_plane is not None:
        payload["data_plane"] = selected_plane.checkpoint()
    return payload


def _initial_profiles(
    config: HistoricalBacktestConfig,
    instrument: str,
    *,
    data_plane: SharedDataPlane | None = None,
) -> dict[str, _ProfileState]:
    return {
        profile.candidate_id: _profile_state(config, profile, instrument, data_plane=data_plane)
        for profile in config.profiles
    }


def _make_checkpoint(
    dataset_id: str,
    data_hash: str,
    config: HistoricalBacktestConfig,
    cursor_sequence: int,
    cursor_time: datetime,
    processed: int,
    states: Mapping[str, _ProfileState],
    writer: _ArtifactWriter,
    guard_snapshot: Mapping[str, Any],
    data_plane: SharedDataPlane | None = None,
    finished: bool = False,
) -> HistoricalBacktestCheckpoint:
    return HistoricalBacktestCheckpoint(
        dataset_id,
        data_hash,
        config.config_hash,
        config.protocol_hash,
        cursor_sequence,
        instant_text(cursor_time),
        processed,
        _checkpoint_state(states, data_plane=data_plane),
        scenario=config.scenario,
        candidate_ids=config.candidate_ids,
        artifact_offsets=writer.artifact_offsets(),
        guard_snapshot=guard_snapshot,
        finished=finished,
        checkpoint_interval_blocks=config.checkpoint_interval_blocks,
    )


def _risk_quote_for_state(
    quote: CFDQuote,
    state: _ProfileState,
    config: HistoricalBacktestConfig,
) -> CFDQuote:
    """Attach a UTC trigger-bar ordinal to a private per-profile quote."""

    metadata = dict(quote.metadata or {})
    timeframe = parse_timeframe(state.profile.trigger_timeframe)
    epoch = datetime.fromtimestamp(0, UTC)
    elapsed = quote.market_time.astimezone(UTC) - epoch
    elapsed_microseconds = (elapsed.days * 86_400 + elapsed.seconds) * 1_000_000 + elapsed.microseconds
    bar_microseconds = timeframe.seconds * 1_000_000
    metadata.update(
        {
            "risk_trigger_bar_count": elapsed_microseconds // bar_microseconds,
            "risk_trigger_timeframe": state.profile.trigger_timeframe,
            "risk_bar_clock_basis": "UTC_TIMEFRAME_BOUNDARIES_NO_OHLC_IMPUTATION",
        }
    )
    if (calendar_state := config.calendar_state_for_quote(quote.market_time)) is not None:
        metadata["risk_calendar"] = calendar_state
    return replace(quote, metadata=metadata)


def _consume_quote(
    config: HistoricalBacktestConfig,
    states: Mapping[str, _ProfileState],
    raw_quote: HistoricalQuote,
    *,
    partition: str,
    previous_time: datetime | None,
    writer: _ArtifactWriter,
    data_plane: SharedDataPlane | None = None,
) -> tuple[CFDQuote, datetime]:
    gap = previous_time is not None and raw_quote.event_time - previous_time > timedelta(minutes=1)
    quote = _historical_quote_to_quote(raw_quote, config, gap=gap)
    event = _historical_quote_to_event(raw_quote)
    precomputed: DataPlaneResult | None = data_plane.feed_event(event) if data_plane is not None else None
    for state in states.values():
        state.quote_count += 1
        state.partitions[partition] += 1
        result = (
            state.processor.feed_precomputed(
                precomputed,
                evaluate_strategy=partition != "WARMUP",
            )
            if precomputed is not None
            else state.processor.process_event(event, evaluate_strategy=partition != "WARMUP")
        )
        _process_result(config, state, result, quote, partition, writer)
        state.legacy.on_quote(quote, capture_complete=False)
        risk_quote = _risk_quote_for_state(quote, state, config)
        risk_changes = state.risk.on_quote(risk_quote, capture_complete=False)
        _emit_risk_updates(config, state, risk_changes, quote=quote, partition=partition, writer=writer)
        state.last_quote_time = raw_quote.event_time
    return quote, raw_quote.event_time


def _restored_last_quote(states: Mapping[str, _ProfileState]) -> CFDQuote | None:
    """Recover the last observed BBO from the legacy control state."""

    for state in states.values():
        quote = state.legacy.current_quote
        if quote is not None:
            return quote
    return None


def _validate_stream_quote(
    raw_quote: Any,
    previous_sequence: int | None,
    previous_time: datetime | None,
) -> HistoricalQuote:
    if not isinstance(raw_quote, HistoricalQuote):
        raise HistoricalBacktestError("quotes debe producir HistoricalQuote tipado")
    if previous_sequence is not None and raw_quote.sequence <= previous_sequence:
        raise HistoricalBacktestError("historical quote sequence no es estrictamente creciente")
    if previous_time is not None and raw_quote.event_time < previous_time:
        raise HistoricalBacktestError("historical quote time retrocede")
    return raw_quote


def _resume_quote_action(
    raw_quote: HistoricalQuote,
    checkpoint: HistoricalBacktestCheckpoint | None,
    cursor_sequence: int,
    cursor_time: datetime,
    resumed_new_quote: bool,
) -> tuple[bool, bool]:
    if checkpoint is None or raw_quote.sequence > cursor_sequence:
        if checkpoint is not None and not resumed_new_quote:
            if raw_quote.sequence != cursor_sequence + 1:
                raise HistoricalBacktestError("resume sequence no es contigua al cursor")
            if raw_quote.event_time < cursor_time:
                raise HistoricalBacktestError("resume time retrocede respecto al cursor")
            return False, True
        return False, resumed_new_quote
    return True, resumed_new_quote


def _partial_stop_requested(
    processed: int,
    *,
    stop_after_quotes: int | None,
    stop_requested: Callable[[], bool] | None,
) -> bool:
    """Evaluate the explicit, post-quote partial-checkpoint controls."""

    if stop_after_quotes is not None and processed >= stop_after_quotes:
        return True
    if stop_requested is None:
        return False
    requested = stop_requested()
    if not isinstance(requested, bool):
        raise HistoricalBacktestError("stop_requested debe devolver booleano")
    return requested


def _ensure_partial_checkpoint(
    checkpoint: HistoricalBacktestCheckpoint | None,
    *,
    dataset_id: str,
    data_hash: str,
    config: HistoricalBacktestConfig,
    cursor_sequence: int,
    cursor_time: datetime,
    processed: int,
    states: Mapping[str, _ProfileState],
    writer: _ArtifactWriter,
    guard_snapshot: Mapping[str, Any],
    data_plane: SharedDataPlane | None,
    target_dir: Path,
) -> HistoricalBacktestCheckpoint:
    """Persist the immediate stop snapshot, unless cadence just did so."""

    if checkpoint is not None and checkpoint.processed_quotes == processed and not checkpoint.finished:
        return checkpoint
    partial_checkpoint = _make_checkpoint(
        dataset_id,
        data_hash,
        config,
        cursor_sequence,
        cursor_time,
        processed,
        states,
        writer,
        guard_snapshot,
        data_plane=data_plane,
        finished=False,
    )
    _write_checkpoint(target_dir, partial_checkpoint)
    return partial_checkpoint


def _validate_stream_identity_and_resume(
    raw_quote: HistoricalQuote,
    *,
    prefix_guard: HistoricalStreamIdentityGuard,
    active_guard: HistoricalStreamIdentityGuard,
    checkpoint: HistoricalBacktestCheckpoint | None,
    cursor_sequence: int,
    cursor_time: datetime,
    resumed_new_quote: bool,
    resume_cursor_verified: bool,
    prefix_seen_count: int,
) -> tuple[bool, bool, bool, int]:
    """Validate one full-stream row before replay/consumption decisions."""

    try:
        prefix_guard.validate(raw_quote)
    except HistoricalStreamIdentityError as exc:
        raise HistoricalBacktestError("historical stream identity invalid") from exc
    prefix_seen_count += 1
    skip, resumed_new_quote = _resume_quote_action(
        raw_quote,
        checkpoint,
        cursor_sequence,
        cursor_time,
        resumed_new_quote,
    )
    if checkpoint is None:
        return skip, resumed_new_quote, resume_cursor_verified, prefix_seen_count
    if raw_quote.sequence == cursor_sequence:
        expected_snapshot = checkpoint.guard_snapshot
        if expected_snapshot is None or prefix_guard.snapshot() != dict(expected_snapshot):
            raise HistoricalBacktestError("resume guard cursor no coincide con el prefijo")
        if prefix_seen_count != checkpoint.processed_quotes:
            raise HistoricalBacktestError("resume guard prefix count no coincide con checkpoint")
        resume_cursor_verified = True
    if skip:
        return skip, resumed_new_quote, resume_cursor_verified, prefix_seen_count
    if not resume_cursor_verified:
        raise HistoricalBacktestError("resume guard cursor no aparece en el prefijo")
    if active_guard is prefix_guard:
        raise HistoricalBacktestError("resume guard no fue restaurado")
    # The restored guard starts at the checkpoint cursor.  Validate only new
    # records against it; skipped prefix records have passed through the fresh
    # full-prefix guard above.
    try:
        active_guard.validate(raw_quote)
    except HistoricalStreamIdentityError as exc:
        raise HistoricalBacktestError("historical stream identity invalid") from exc
    return skip, resumed_new_quote, resume_cursor_verified, prefix_seen_count


def _initialize_stream_state(
    manifest: DatasetManifest,
    config: HistoricalBacktestConfig,
    checkpoint: HistoricalBacktestCheckpoint | None,
) -> tuple[
    dict[str, _ProfileState],
    int,
    int,
    datetime,
    CFDQuote | None,
    HistoricalStreamIdentityGuard,
    HistoricalStreamIdentityGuard,
    SharedDataPlane | None,
]:
    processed = checkpoint.processed_quotes if checkpoint is not None else 0
    cursor_sequence = checkpoint.cursor_sequence if checkpoint is not None else -1
    cursor_time = (
        _utc(checkpoint.cursor_time, "checkpoint.cursor_time")
        if checkpoint is not None
        else datetime.fromtimestamp(0, UTC)
    )
    prefix_guard = HistoricalStreamIdentityGuard(manifest)
    active_guard = prefix_guard
    states: dict[str, _ProfileState] = {}
    instrument = str(manifest.instrument).strip().upper().replace("-", "/")
    data_plane: SharedDataPlane | None = None
    if checkpoint is not None:
        if checkpoint.guard_snapshot is None:
            raise HistoricalBacktestError("resume guard snapshot missing")
        try:
            active_guard = HistoricalStreamIdentityGuard.from_snapshot(
                manifest,
                checkpoint.guard_snapshot,
            )
        except ValueError as exc:
            raise HistoricalBacktestError("resume guard snapshot incompatible") from exc
        data_plane = _data_plane_from_checkpoint(checkpoint, config, instrument)
        states = _restore_profiles(config, checkpoint, instrument, data_plane=data_plane)
    else:
        data_plane = _new_historical_data_plane(config, instrument)
    last_quote = _restored_last_quote(states) if checkpoint is not None else None
    return states, processed, cursor_sequence, cursor_time, last_quote, prefix_guard, active_guard, data_plane


def _run_stream(
    quotes: Iterable[HistoricalQuote] | Callable[[], Iterator[HistoricalQuote]],
    *,
    manifest: DatasetManifest,
    config: HistoricalBacktestConfig,
    checkpoint: HistoricalBacktestCheckpoint | None,
    dataset_id: str,
    data_hash: str,
    target_dir: Path,
    writer: _ArtifactWriter,
    stop_after_quotes: int | None = None,
    stop_requested: Callable[[], bool] | None = None,
) -> tuple[
    dict[str, _ProfileState],
    int,
    int,
    datetime,
    CFDQuote | None,
    HistoricalStreamIdentityGuard,
    SharedDataPlane | None,
    HistoricalBacktestCheckpoint | None,
]:
    states, processed, cursor_sequence, cursor_time, last_quote, prefix_guard, active_guard, data_plane = (
        _initialize_stream_state(
            manifest,
            config,
            checkpoint,
        )
    )
    resume_checkpoint = checkpoint
    manifest_instrument = str(manifest.instrument).strip().upper().replace("-", "/")
    stream = quotes() if callable(quotes) else iter(quotes)
    stream_sequence: int | None = None
    stream_time: datetime | None = None
    resumed_new_quote = False
    resume_cursor_verified = checkpoint is None or checkpoint.cursor_sequence < 0
    prefix_seen_count = 0
    for candidate_quote in stream:
        prior_stream_time = stream_time
        raw_quote = _validate_stream_quote(candidate_quote, stream_sequence, prior_stream_time)
        if raw_quote.instrument != manifest_instrument:
            raise HistoricalBacktestError("quote instrument no coincide con manifest")
        # Validate the complete source prefix before deciding whether this
        # row is replayed or consumed.  The guard is transactional, so a
        # duplicate locator/source_event_id cannot advance the source cursor.
        skip, resumed_new_quote, resume_cursor_verified, prefix_seen_count = _validate_stream_identity_and_resume(
            raw_quote,
            prefix_guard=prefix_guard,
            active_guard=active_guard,
            checkpoint=resume_checkpoint,
            cursor_sequence=cursor_sequence,
            cursor_time=cursor_time,
            resumed_new_quote=resumed_new_quote,
            resume_cursor_verified=resume_cursor_verified,
            prefix_seen_count=prefix_seen_count,
        )
        stream_sequence = raw_quote.sequence
        stream_time = raw_quote.event_time
        if skip:
            continue
        if not states:
            states = _initial_profiles(config, raw_quote.instrument, data_plane=data_plane)
        partition = _partition(raw_quote.event_time, config)
        previous_time = prior_stream_time
        if previous_time is None:
            # ``stream_time`` is the current value for local ordering.  The
            # prior watermark is the cursor (new-only factory) or the last
            # skipped prefix row (full-stream factory).
            previous_time = cursor_time if checkpoint is not None else None
        last_quote, _ = _consume_quote(
            config,
            states,
            raw_quote,
            partition=partition,
            previous_time=previous_time,
            writer=writer,
            data_plane=data_plane,
        )
        processed += 1
        cursor_sequence = raw_quote.sequence
        cursor_time = raw_quote.event_time
        if processed % config.checkpoint_interval_quotes == 0:
            checkpoint = _make_checkpoint(
                dataset_id,
                data_hash,
                config,
                cursor_sequence,
                cursor_time,
                processed,
                states,
                writer,
                active_guard.snapshot(),
                data_plane=data_plane,
                finished=False,
            )
            _write_checkpoint(target_dir, checkpoint)
        if _partial_stop_requested(
            processed,
            stop_after_quotes=stop_after_quotes,
            stop_requested=stop_requested,
        ):
            # A periodic checkpoint at this exact quote is already the
            # immediate stop snapshot.  Reuse it instead of serializing and
            # fsyncing an identical non-terminal state twice.
            checkpoint = _ensure_partial_checkpoint(
                checkpoint,
                dataset_id=dataset_id,
                data_hash=data_hash,
                config=config,
                cursor_sequence=cursor_sequence,
                cursor_time=cursor_time,
                processed=processed,
                states=states,
                writer=writer,
                guard_snapshot=active_guard.snapshot(),
                data_plane=data_plane,
                target_dir=target_dir,
            )
            return (
                states,
                processed,
                cursor_sequence,
                cursor_time,
                last_quote,
                active_guard,
                data_plane,
                checkpoint,
            )
    if not states:
        raise HistoricalBacktestError("historical stream vacío")
    if resume_checkpoint is not None and not resume_cursor_verified:
        raise HistoricalBacktestError("resume guard cursor no aparece en el prefijo")
    return states, processed, cursor_sequence, cursor_time, last_quote, active_guard, data_plane, None


def _simulator_costs_known(simulator: CFDSimulator) -> bool:
    config = simulator.config
    return bool(
        config.commission_known
        and (not config.financing_required or config.financing_rate_per_second is not None)
        and (
            config.quote_currency is None
            or config.quote_currency == config.account_currency
            or config.conversion_rate is not None
        )
    )


def _historical_economic_metrics(states: Mapping[str, _ProfileState]) -> dict[str, Any]:
    """Expose net status only when every configured cost input is explicit."""

    simulators = tuple(simulator for state in states.values() for simulator in (state.legacy, state.risk))
    configured = bool(simulators) and all(_simulator_costs_known(simulator) for simulator in simulators)
    # ``CFDSimulator.trades`` is intentionally bounded.  Once a terminal
    # trade leaves that in-memory suffix, the historical result no longer has
    # complete evidence for an aggregate net statement.  The simulator
    # persists both ``archive_required`` and the monotonic ``terminal_evicted``
    # counter in its snapshot; fail closed on either signal rather than
    # allowing a retained suffix to upgrade the whole run to KNOWN.
    retention_incomplete = any(
        simulator.archive_required or int(simulator.counters.get("terminal_evicted", 0)) > 0 for simulator in simulators
    )
    unknown_terminal = any(
        trade.state.value == "UNKNOWN" or (trade.state.value == "CLOSED" and trade.net_pnl is None)
        for state in states.values()
        for trade in state.risk.trades
    )
    if not configured or unknown_terminal or retention_incomplete:
        return {
            "costs_applied": False,
            "costs_status": "UNKNOWN_NOT_ZERO",
            "gross_only": True,
            "net_status": "UNKNOWN_COSTS",
        }
    return {
        "costs_applied": True,
        "costs_status": "KNOWN",
        "gross_only": False,
        "net_status": "KNOWN",
    }


def _result_metrics(
    config: HistoricalBacktestConfig,
    states: Mapping[str, _ProfileState],
    processed: int,
    *,
    status: str,
    finished: bool,
) -> dict[str, Any]:
    """Build the common result envelope without finalizing any state."""

    metrics = {
        "schema_version": HISTORICAL_SCHEMA_VERSION,
        "streaming": True,
        "block_size": config.block_size,
        "checkpoint_interval_blocks": config.checkpoint_interval_blocks,
        "checkpoint_interval_quotes": config.checkpoint_interval_quotes,
        "workers": 1,
        "rss_target_bytes": RSS_TARGET_BYTES,
        "source": HISTORICAL_SOURCE,
        "processed_quotes": processed,
        "scenario": config.scenario,
        "scenario_parameters": dict(config.scenario_parameters),
        "scenario_mode": config.scenario_mode,
        "scenario_hash": config.scenario_hash,
        "candidate_ids": list(config.candidate_ids),
        "assumptions_model_id": config.assumptions_model_id,
        "assumption_hash": config.assumption_hash,
        "assumptions_model": (config.assumptions_model.to_dict() if config.assumptions_model is not None else None),
        "calendar_model_id": config.calendar_model_id,
        "calendar_model": config.calendar_model.to_dict() if config.calendar_model is not None else None,
        "calendar_observed": False,
        "scenario_applied": True,
        "holdout": "CLOSED",
        "partitions": {key: dict(value.partitions) for key, value in states.items()},
        "risk_state": {key: _public_risk_state(value.risk) for key, value in states.items()},
        "legacy_horizon_control": True,
        "promotable": False,
        "ticks_retained_in_memory": 0,
        "ticks_persisted_sqlite": False,
        "status": status,
        "finished": finished,
        "capture_complete": finished,
    }
    if finished:
        metrics.update(_historical_economic_metrics(states))
    else:
        # An incomplete capture cannot make a complete net statement, even
        # when a future/explicit cost specification is available.
        metrics.update(
            {
                "costs_applied": False,
                "costs_status": "UNKNOWN_NOT_ZERO",
                "gross_only": True,
                "net_status": "UNKNOWN_COSTS",
            }
        )
    return metrics


def _finish_run(
    *,
    states: Mapping[str, _ProfileState],
    last_quote: CFDQuote | None,
    processed: int,
    cursor_sequence: int,
    cursor_time: datetime,
    dataset_id: str,
    data_hash: str,
    config: HistoricalBacktestConfig,
    target_dir: Path,
    writer: _ArtifactWriter,
    guard: HistoricalStreamIdentityGuard,
    data_plane: SharedDataPlane | None = None,
) -> HistoricalBacktestResult:
    if last_quote is None:
        raise HistoricalBacktestError("resume no dejó una cotización final observable")
    final_partition = _partition(last_quote.available_ts, config)
    for state in states.values():
        _finish_profile(config, state, last_quote, final_partition, writer)
    checkpoint = _make_checkpoint(
        dataset_id,
        data_hash,
        config,
        cursor_sequence,
        cursor_time,
        processed,
        states,
        writer,
        guard.snapshot(),
        data_plane=data_plane,
        finished=True,
    )
    _write_checkpoint(target_dir, checkpoint)
    variants = tuple(_variant_result(state, _control_summary(state)) for state in states.values())
    metrics = _result_metrics(config, states, processed, status="COMPLETED", finished=True)
    return HistoricalBacktestResult(
        dataset_id,
        data_hash,
        config.config_hash,
        config.protocol_hash,
        "COMPLETED",
        False,
        processed,
        variants,
        writer.rows("ledger"),
        writer.rows("equity"),
        writer.rows("funnel"),
        metrics,
        checkpoint,
        config.scenario,
        config.scenario_parameters,
        config.candidate_ids,
        config.assumptions_model_id,
        config.assumption_hash,
        config.calendar_model_id,
        True,
    )


def _checkpointed_result(
    *,
    states: Mapping[str, _ProfileState],
    last_quote: CFDQuote | None,
    processed: int,
    config: HistoricalBacktestConfig,
    checkpoint: HistoricalBacktestCheckpoint,
    writer: _ArtifactWriter,
) -> HistoricalBacktestResult:
    """Return a continuable result without invoking any finalizer."""

    if last_quote is None or processed <= 0:
        raise HistoricalBacktestError("checkpoint parcial requiere una cotización observable")
    if checkpoint.finished or checkpoint.status != "CHECKPOINTED":
        raise HistoricalBacktestError("checkpoint parcial debe quedar sin finalizar")
    variants = tuple(_variant_result(state, _control_summary(state, finalize=False)) for state in states.values())
    metrics = _result_metrics(config, states, processed, status="CHECKPOINTED", finished=False)
    return HistoricalBacktestResult(
        checkpoint.dataset_id,
        checkpoint.data_hash,
        checkpoint.config_hash,
        checkpoint.protocol_hash,
        "CHECKPOINTED",
        False,
        processed,
        variants,
        writer.rows("ledger"),
        writer.rows("equity"),
        writer.rows("funnel"),
        metrics,
        checkpoint,
        config.scenario,
        config.scenario_parameters,
        config.candidate_ids,
        config.assumptions_model_id,
        config.assumption_hash,
        config.calendar_model_id,
        False,
    )


def _normalize_partial_controls(
    stop_after_quotes: int | None,
    checkpoint_after_quotes: int | None,
    stop_requested: Callable[[], bool] | None,
    checkpoint: HistoricalBacktestCheckpoint | None,
) -> int | None:
    """Validate the explicit controls for a non-terminal checkpoint."""

    if (
        stop_after_quotes is not None
        and checkpoint_after_quotes is not None
        and stop_after_quotes != checkpoint_after_quotes
    ):
        raise HistoricalBacktestError("stop_after_quotes y checkpoint_after_quotes no coinciden")
    selected = stop_after_quotes if stop_after_quotes is not None else checkpoint_after_quotes
    if selected is not None:
        if isinstance(selected, bool) or not isinstance(selected, int) or selected <= 0:
            raise HistoricalBacktestError("stop_after_quotes debe ser entero positivo")
        if checkpoint is not None and selected <= checkpoint.processed_quotes:
            raise HistoricalBacktestError("stop_after_quotes debe avanzar el checkpoint")
    if stop_requested is not None and not callable(stop_requested):
        raise HistoricalBacktestError("stop_requested debe ser callable")
    return selected


def run_historical_backtest(
    quotes: Iterable[HistoricalQuote] | Callable[[], Iterator[HistoricalQuote]],
    *,
    manifest: DatasetManifest,
    config: HistoricalBacktestConfig,
    output_dir: str | Path,
    sink: ArtifactSink,
    resume: HistoricalBacktestCheckpoint | Mapping[str, Any] | None = None,
    stop_after_quotes: int | None = None,
    checkpoint_after_quotes: int | None = None,
    stop_requested: Callable[[], bool] | None = None,
) -> HistoricalBacktestResult:
    """Run the registered candidates selected by the frozen config.

    The iterator is consumed once.  Only bounded processor/simulator state,
    result tails and metrics remain in memory; artifacts are emitted as JSONL
    rows through ``sink`` and ``output_dir``.  Holdout years are never opened
    by this API.  ``stop_after_quotes`` (or its readability alias
    ``checkpoint_after_quotes``) and ``stop_requested`` are explicit partial
    capture controls: they stop only after a quote has been fully consumed,
    persist a ``CHECKPOINTED`` result, and never call a finalizer.  Omitting
    them preserves the terminal prefix/full-stream behavior.
    """

    if not isinstance(config, HistoricalBacktestConfig):
        raise HistoricalBacktestError("config debe ser HistoricalBacktestConfig.from_protocol(...)")
    if not callable(sink):
        raise HistoricalBacktestError("sink debe ser callable(kind, row)")
    dataset_id, data_hash = _manifest_identity(manifest)
    checkpoint = _checkpoint_resume(
        resume,
        manifest=manifest,
        dataset_id=dataset_id,
        data_hash=data_hash,
        config=config,
    )
    selected_stop_after = _normalize_partial_controls(
        stop_after_quotes,
        checkpoint_after_quotes,
        stop_requested,
        checkpoint,
    )
    target_dir = _output_directory(output_dir)
    writer = _ArtifactWriter(
        target_dir,
        sink,
        config.max_retained_rows,
        resume=checkpoint is not None,
        truncate_offsets=checkpoint.artifact_offsets if checkpoint is not None else None,
    )
    try:
        (
            states,
            processed,
            cursor_sequence,
            cursor_time,
            last_quote,
            guard,
            data_plane,
            partial_checkpoint,
        ) = _run_stream(
            quotes,
            manifest=manifest,
            config=config,
            checkpoint=checkpoint,
            dataset_id=dataset_id,
            data_hash=data_hash,
            target_dir=target_dir,
            writer=writer,
            stop_after_quotes=selected_stop_after,
            stop_requested=stop_requested,
        )
        if partial_checkpoint is not None:
            return _checkpointed_result(
                states=states,
                last_quote=last_quote,
                processed=processed,
                config=config,
                checkpoint=partial_checkpoint,
                writer=writer,
            )
        return _finish_run(
            states=states,
            last_quote=last_quote,
            processed=processed,
            cursor_sequence=cursor_sequence,
            cursor_time=cursor_time,
            dataset_id=dataset_id,
            data_hash=data_hash,
            config=config,
            target_dir=target_dir,
            writer=writer,
            guard=guard,
            data_plane=data_plane,
        )
    finally:
        writer.close()


__all__ = [
    "ArtifactRows",
    "ArtifactSink",
    "BLOCK_SIZE",
    "DEFAULT_CHECKPOINT_INTERVAL_BLOCKS",
    "DEFAULT_PROFILES",
    "HistoricalBacktestCheckpoint",
    "HistoricalBacktestConfig",
    "HistoricalBacktestError",
    "HistoricalBacktestResult",
    "HistoricalProfile",
    "HistoricalVariantResult",
    "HISTORICAL_SOURCE",
    "LEGACY_HORIZONS",
    "RSS_TARGET_BYTES",
    "SCENARIOS",
    "SCENARIO_PARAMETERS",
    "run_historical_backtest",
]
