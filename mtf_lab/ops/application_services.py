"""Reusable application services for the MTF Lab command line.

The command line is an adapter, not the owner of market or persistence rules.
Services in this module compose the existing domain/runtime/adapters once and
return serialisable command results.  They deliberately do not import the
optional cTrader SDK at module import time.
"""

from __future__ import annotations

import argparse
import importlib
import importlib.util
import json
import os
import sys
import urllib.request
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, cast

from .. import __version__
from ..configuration import (
    ConfigError,
    EffectiveConfig,
    default_state_dir,
    load_config,
    normalize_simulation_mapping,
    packaged_config_path,
)
from ..core import assess_freshness, compute_indicators, parse_timeframe
from ..core.canonical import canonical_json
from ..core.reference import m1_reference_signals
from ..runtime import RuntimeCoordinator, capture_hash
from ..runtime.state import to_core_candle
from .backtest import BacktestRunner, VariantSpec
from .ctrader_capture import CTraderCapture
from .demo import run_demo
from .importer import ColumnMapping, LocalImporter
from .logging_state import OperationTelemetry
from .persistence import IdempotencyConflict, SQLiteStore, payload_hash
from .reporting import ReportBuilder
from .simulation import EvaluationSpec, VirtualContractSimulator

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class CommandResult:
    """A command response before the presentation layer renders it."""

    code: int = 0
    payload: Any = None
    text: str | None = None
    stderr: bool = False

    @classmethod
    def json(cls, payload: Any, *, code: int = 0, stderr: bool = False) -> CommandResult:
        return cls(code=code, payload=payload, stderr=stderr)

    @classmethod
    def plain(cls, text: str, *, code: int = 0, stderr: bool = False) -> CommandResult:
        return cls(code=code, text=text, stderr=stderr)

    def rendered(self) -> str:
        if self.text is not None:
            return self.text
        return json.dumps(self.payload, ensure_ascii=False, indent=2, default=str)


def default_db() -> Path:
    return Path(os.environ.get("MTF_LAB_DB", str(default_state_dir() / "mtf_lab.sqlite3"))).expanduser()


def default_logs(db: Path) -> Path:
    return db.with_suffix(".jsonl")


def default_config() -> Path:
    return packaged_config_path("default.toml")


def default_watch_config() -> Path:
    candidate = packaged_config_path("kraken.toml")
    return candidate if candidate.exists() else default_config()


def effective_config(path: str | Path | None = None) -> EffectiveConfig:
    """Load and normalise the one effective project configuration."""
    return load_config(path or default_config())


def _config_for(args: argparse.Namespace, *, watch: bool = False) -> EffectiveConfig:
    path = getattr(args, "config", None)
    return effective_config(path or (default_watch_config() if watch else default_config()))


def _db_for(args: argparse.Namespace, config: EffectiveConfig | None = None) -> Path:
    value = getattr(args, "db", None)
    if value is not None:
        return Path(value).expanduser()
    if config is not None and config.storage_db:
        return Path(config.storage_db).expanduser()
    return default_db()


def _log_for(args: argparse.Namespace, db: Path, config: EffectiveConfig | None = None) -> Path:
    value = getattr(args, "log", None)
    if value is not None:
        return Path(value).expanduser()
    if getattr(args, "db", None) is not None:
        return default_logs(db)
    if config is not None and config.storage_logs:
        return Path(config.storage_logs).expanduser()
    return default_logs(db)


def _canonical_base(value: Any) -> str:
    text = str(value or "traded").strip().lower()
    return "traded" if text in {"trade", "traded", "close"} else text


def _timeframe_name(value: Any) -> str:
    return parse_timeframe(value).name


def _int_or_default(value: Any, default: int) -> int:
    return default if value is None else int(value)


def _override_config(
    config: EffectiveConfig,
    *,
    instrument: str | None = None,
    price_base: str | None = None,
    mode: str | None = None,
) -> EffectiveConfig:
    """Adapt capture metadata without changing strategy or simulation rules."""
    values: dict[str, Any] = {}
    if instrument:
        values["instrument"] = str(instrument)
    if price_base:
        values["price_base"] = _canonical_base(price_base)
    if mode:
        values["mode"] = str(mode).upper()
    if values:
        data = dict(config.data)
        if instrument:
            data["instrument"] = str(instrument)
        if price_base:
            data["price_base"] = _canonical_base(price_base)
        if mode:
            data["mode"] = str(mode).upper()
        values["data"] = data
    return replace(config, **values) if values else config


def _load_toml(path: str | Path | None) -> dict[str, Any]:
    """Compatibility reader for integrators; commands use EffectiveConfig."""
    if path is None:
        return {}
    import tomllib

    with Path(path).expanduser().open("rb") as stream:
        value = tomllib.load(stream)
    if not isinstance(value, dict):
        raise ConfigError("la configuración TOML debe ser una tabla")
    return value


def _discover_engine(config: EffectiveConfig | Mapping[str, Any] | None = None) -> Any:
    """Build the one explicit strategy detector used by application flows."""
    from ..core.strategy import TrendPullbackStrategy

    if isinstance(config, EffectiveConfig):
        return TrendPullbackStrategy(config.strategy)
    if isinstance(config, Mapping):
        return TrendPullbackStrategy(config.get("strategy", config))
    return TrendPullbackStrategy()


def _sim_spec(config: EffectiveConfig | Mapping[str, Any] | None = None) -> EvaluationSpec:
    """Build ``EvaluationSpec`` through the central normalisation policy."""
    return EvaluationSpec(**normalize_simulation_mapping(config))


def _importer_identity(args: argparse.Namespace, config: EffectiveConfig | None) -> tuple[str, str, str]:
    instrument = getattr(args, "instrument", None) or (config.instrument if config else "UNKNOWN")
    timeframe = getattr(args, "timeframe", None) or (_timeframe_name(config.timeframes[0]) if config else "M1")
    base = getattr(args, "price_base", None) or (config.price_base if config else "close")
    return str(instrument), str(timeframe), "trade" if _canonical_base(base) == "traded" else str(base)


def _importer_options(args: argparse.Namespace, config: EffectiveConfig | None) -> dict[str, Any]:
    interval = getattr(args, "interval_seconds", None)
    return {
        "mapping": ColumnMapping.from_text(getattr(args, "mapping", None)),
        "timezone": getattr(args, "timezone", None),
        "timestamp_unit": getattr(args, "timestamp_unit", "iso8601"),
        "interval_seconds": float(interval) if interval is not None else None,
        "strict": not bool(getattr(args, "allow_issues", False)),
        "source": str(config.data.get("source")) if config is not None and config.data.get("source") else None,
        "allow_out_of_order": bool(getattr(args, "allow_out_of_order", False)),
        "allow_duplicates": bool(getattr(args, "allow_duplicates", False)),
    }


def _build_importer(args: argparse.Namespace, config: EffectiveConfig | None = None) -> LocalImporter:
    instrument, timeframe, base = _importer_identity(args, config)
    return LocalImporter(instrument=instrument, timeframe=timeframe, price_base=base, **_importer_options(args, config))


def _persist_import(
    db: Path,
    imported: Any,
    *,
    mode: str = "REPLAY",
    provider: str = "local-file",
    log_path: Path | None = None,
    session_id: str | None = None,
    config: EffectiveConfig | None = None,
    finish: bool = True,
) -> tuple[str, dict[str, Any]]:
    session_config = config.to_dict() if config is not None else {}
    session_config["import"] = imported.to_dict()
    session_config["capture_hash"] = _capture_hash(imported.records)
    metadata = {
        "import": imported.to_dict(),
        "capture_hash": session_config["capture_hash"],
        "log_path": str(log_path) if log_path else None,
    }
    with SQLiteStore(db) as store:
        sid = store.create_session(
            session_id=session_id,
            mode=mode,
            provider=provider,
            instrument=imported.instrument,
            code_version=__version__,
            dataset_ref=imported.source_path,
            config=session_config,
            metadata=metadata,
        )
        for ordinal, record in enumerate(imported.records):
            if "open" in record:
                store.save_candle(sid, record, ordinal=ordinal)
            else:
                store.save_event(sid, record, ordinal=ordinal)
        store.save_checkpoint(
            sid,
            "import",
            cursor={"source_path": imported.source_path, "last_row": len(imported.records)},
            events_processed=len(imported.records),
            state={
                "coverage_start": imported.coverage_start,
                "coverage_end": imported.coverage_end,
                "capture_hash": session_config["capture_hash"],
            },
        )
        if finish:
            store.finish_session(sid, status="COMPLETED")
    return sid, {
        "session_id": sid,
        "records": len(imported.records),
        "issues": [item.to_dict() for item in imported.issues],
        "coverage_start": imported.coverage_start,
        "coverage_end": imported.coverage_end,
        "db": str(db),
        "mode": mode,
        "provider": provider,
    }


def _stored_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    value = row.get("payload")
    if isinstance(value, Mapping):
        return dict(value)
    raw = row.get("payload_json")
    if raw:
        try:
            parsed = json.loads(str(raw))
        except (TypeError, json.JSONDecodeError):
            parsed = None
        if isinstance(parsed, Mapping):
            return dict(parsed)
    return {}


def _stored_records(store: SQLiteStore, session_id: str) -> list[dict[str, Any]]:
    """Recover original capture records, not derived read-model decisions."""
    session = store.get_session(session_id) or {}
    mode = str(session.get("mode", "REPLAY"))
    records: list[dict[str, Any]] = []
    for row in store.list_events(session_id):
        data = _stored_payload(row)
        if bool(data.get("derived_from_candle")):
            continue
        data.setdefault("event_id", row.get("event_id"))
        data.setdefault("source_event_id", row.get("event_id"))
        data.setdefault("source", row.get("source", "persisted"))
        data.setdefault("instrument", row.get("instrument", session.get("instrument", "unknown")))
        data.setdefault("event_ts", row.get("event_ts"))
        data.setdefault("received_ts", row.get("received_ts"))
        data.setdefault("available_ts", row.get("available_ts"))
        data.setdefault("kind", row.get("kind", "trade"))
        data.setdefault("price_base", row.get("price_base", "traded"))
        data.setdefault("mode", mode)
        data["_persisted_capture"] = True
        records.append(data)
    for row in store.list_candles(session_id):
        data = _stored_payload(row)
        raw_provenance = data.get("provenance")
        provenance: Mapping[str, Any] = raw_provenance if isinstance(raw_provenance, Mapping) else {}
        origin = str(data.get("origin", provenance.get("origin", "native"))).lower()
        source = str(row.get("source", data.get("source", ""))).lower()
        if (
            origin in {"aggregated", "derived", "resampled_ohlc", "runtime_aggregate"}
            or "aggregated" in source
            or "resampled" in source
        ):
            continue
        data.update(
            {
                "candle_id": row.get("candle_id"),
                "instrument": row.get("instrument", session.get("instrument", "unknown")),
                "timeframe": row.get("timeframe", "M1"),
                "start_ts": row.get("start_ts"),
                "end_ts": row.get("end_ts"),
                "available_ts": data.get("available_ts", row.get("available_ts")),
                "open": row.get("open"),
                "high": row.get("high"),
                "low": row.get("low"),
                "close": row.get("close"),
                "volume": row.get("volume"),
                "closed": bool(row.get("closed", True)),
                "source": row.get("source", "persisted"),
                "price_base": row.get("price_base", "traded"),
                "quality": row.get("quality", "UNKNOWN"),
                "revision": row.get("revision", 0),
                "mode": mode,
            }
        )
        data["_persisted_capture"] = True
        records.append(data)
    return records


def _latest_candles(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    latest: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (
            str(row.get("instrument", "unknown")),
            str(row.get("timeframe", "M1")),
            str(row.get("start_ts")),
        )
        candidate = dict(row)
        previous = latest.get(key)
        if previous is None or int(candidate.get("revision", 0) or 0) >= int(previous.get("revision", 0) or 0):
            latest[key] = candidate
    return sorted(latest.values(), key=lambda item: (str(item.get("start_ts", "")), str(item.get("timeframe", ""))))


def _core_candle(row: Mapping[str, Any], *, mode: str = "REPLAY") -> Any:
    """Decode a persisted candle while retaining quality/availability metadata."""
    data = _stored_payload(row)
    data.update(
        {
            "instrument": row.get("instrument", data.get("instrument", "unknown")),
            "timeframe": row.get("timeframe", data.get("timeframe", "M1")),
            "start_ts": row.get("start_ts", data.get("start_ts", data.get("start"))),
            "end_ts": row.get("end_ts", data.get("end_ts", data.get("end"))),
            "open": row.get("open", data.get("open")),
            "high": row.get("high", data.get("high")),
            "low": row.get("low", data.get("low")),
            "close": row.get("close", data.get("close")),
            "volume": row.get("volume", data.get("volume", 0.0)),
            "closed": bool(row.get("closed", data.get("closed", True))),
            "source": row.get("source", data.get("source", "persisted")),
            "price_base": row.get("price_base", data.get("price_base", "traded")),
            "quality": row.get("quality", data.get("quality", "UNKNOWN")),
            "candle_id": row.get("candle_id", data.get("candle_id")),
            "available_at": row.get("available_ts", data.get("available_at", data.get("available_ts"))),
            "received_at": data.get("received_at", row.get("available_ts")),
            "mode": mode,
        }
    )
    normalized_mode = str(mode).upper()
    if normalized_mode not in {"LIVE", "REPLAY", "SYNTHETIC"}:
        normalized_mode = "REPLAY"
    return to_core_candle(data, mode=normalized_mode)


def _capture_hash(records: Iterable[Any]) -> str:
    """Hash captures without internal read-model sentinels."""
    cleaned: list[Any] = []
    for record in records:
        if isinstance(record, Mapping):
            cleaned.append({key: value for key, value in record.items() if key != "_persisted_capture"})
        else:
            cleaned.append(record)
    return capture_hash(cleaned)


def _contract_hash(config: EffectiveConfig) -> str:
    from ..runtime.integration import runtime_simulation_config

    return payload_hash(runtime_simulation_config(config).to_dict())


def _analysis_for(
    store: SQLiteStore,
    session_id: str,
    *,
    dataset_hash: str,
    config: EffectiveConfig,
    variant: str,
    partition: str,
    metadata: Mapping[str, Any] | None = None,
    identity_extra: Mapping[str, Any] | None = None,
) -> str:
    return store.create_analysis(
        session_id,
        dataset_hash=dataset_hash,
        config_hash=config.config_hash,
        variant=variant,
        contract_hash=_contract_hash(config),
        partition=partition,
        code_version=config.version,
        metadata=metadata or {},
        identity_extra=identity_extra or {},
    )


def _signal_variant(row: Mapping[str, Any]) -> str:
    value = row.get("variant")
    if value:
        return str(value)
    payload = row.get("payload")
    if isinstance(payload, Mapping):
        return str(payload.get("variant") or payload.get("strategy") or "")
    return ""


def _persist_reference(
    store: SQLiteStore,
    session_id: str,
    config: EffectiveConfig,
    candles: Sequence[Mapping[str, Any]],
    *,
    dataset_hash: str,
    partition: str = "all",
    mode: str = "REPLAY",
    identity_extra: Mapping[str, Any] | None = None,
) -> tuple[list[Mapping[str, Any]], str]:
    core_rows = [
        _core_candle(row, mode=mode)
        for row in _latest_candles(candles)
        if str(row.get("timeframe", "")).upper() == _timeframe_name(config.strategy.trigger_timeframe)
    ]
    analysis_metadata = {"strategy": "m1_trigger_reference", "coverage": len(core_rows)}
    analysis_id = _analysis_for(
        store,
        session_id,
        dataset_hash=dataset_hash,
        config=config,
        variant="m1_trigger_reference",
        partition=partition,
        metadata=analysis_metadata,
        identity_extra=identity_extra,
    )
    if not core_rows:
        return [], analysis_id
    series = compute_indicators(core_rows, config.indicators)
    refs: list[Mapping[str, Any]] = [
        dict(signal)
        for signal in m1_reference_signals(
            series,
            rsi_threshold=config.strategy.rsi_threshold,
            mode=mode,
            identity_salt=config.config_hash,
        )
    ]
    for ordinal, signal in enumerate(refs):
        store.save_signal(
            session_id,
            signal,
            ordinal=ordinal,
            analysis_id=analysis_id,
            variant="m1_trigger_reference",
            analysis_config_hash=config.config_hash,
            contract_hash=_contract_hash(config),
            partition=partition,
        )
    return refs, analysis_id


class DoctorService:
    """Readiness diagnostics; network probing is opt-in and bounded."""

    @staticmethod
    def _module_lines(required: Mapping[str, str], optional: Mapping[str, str]) -> tuple[list[str], list[str]]:
        errors: list[str] = []
        lines: list[str] = []
        for module, label in required.items():
            ok = importlib.util.find_spec(module) is not None
            lines.append(f"dependencia {label}: {'OK' if ok else 'FALTA'} ({module})")
            if not ok:
                errors.append(f"dependencia requerida ausente: {module}")
        for module, label in optional.items():
            state = "OK" if importlib.util.find_spec(module) is not None else "omitido"
            lines.append(f"opcional {label}: {state} ({module})")
        return lines, errors

    @staticmethod
    def _config_line(args: argparse.Namespace) -> tuple[list[str], EffectiveConfig | None, list[str]]:
        try:
            config = _config_for(args)
            return (
                [
                    f"configuración: OK hash={config.config_hash}",
                    json.dumps({"effective_config": config.to_dict()}, ensure_ascii=False, sort_keys=True, default=str),
                ],
                config,
                [],
            )
        except Exception as exc:
            return [f"configuración: ERROR {type(exc).__name__}: {exc}"], None, [f"configuración: {exc}"]

    @staticmethod
    def _storage_line(db: Path) -> tuple[str, list[str]]:
        try:
            db.parent.mkdir(parents=True, exist_ok=True)
            with SQLiteStore(db) as store:
                return f"storage: OK {db} schema={store.schema_version}", []
        except Exception as exc:
            return "", [f"storage: {exc}"]

    @staticmethod
    def _network_line(args: argparse.Namespace) -> tuple[str, list[str]]:
        if not getattr(args, "connectivity", False):
            return "conectividad: omitida (use --connectivity explícitamente)", []
        try:
            with urllib.request.urlopen(
                "https://api.kraken.com/0/public/Time", timeout=float(args.timeout)
            ) as response:
                body = response.read(4096).decode("utf-8", "replace")
            return f"conectividad Kraken REST: OK ({response.status}) cuerpo={body[:160]}", []
        except Exception as exc:
            return f"conectividad Kraken REST: ERROR {type(exc).__name__}: {exc}", [
                "conectividad opcional no disponible"
            ]

    def run(self, args: argparse.Namespace) -> CommandResult:
        lines = [
            "MTF Lab doctor",
            f"python: {sys.version.split()[0]} ({sys.executable})",
            f"sqlite: {__import__('sqlite3').sqlite_version}",
            f"proyecto: {PROJECT_ROOT}",
        ]
        required = {
            "tomllib": "stdlib",
            "sqlite3": "stdlib",
            "mtf_lab.core": "núcleo",
            "mtf_lab.data": "datos",
            "mtf_lab.ops": "operaciones",
        }
        optional = {"websockets": "Kraken WebSocket", "requests": "HTTP opcional", "rich": "salida enriquecida"}
        module_lines, errors = self._module_lines(required, optional)
        lines.extend(module_lines)
        config_lines, config, config_errors = self._config_line(args)
        lines.extend(config_lines)
        errors.extend(config_errors)
        db = _db_for(args, config)
        storage_line, storage_errors = self._storage_line(db)
        if storage_line:
            lines.append(storage_line)
        errors.extend(storage_errors)
        network_line, network_errors = self._network_line(args)
        lines.append(network_line)
        errors.extend(network_errors)
        lines.extend(f"ERROR: {error}" for error in errors)
        return CommandResult.plain("\n".join(lines), code=2 if errors else 0, stderr=False)


class ImportService:
    """Validate a local input and persist its explicit capture envelope."""

    def run(self, args: argparse.Namespace) -> CommandResult:
        config = effective_config(args.config) if getattr(args, "config", None) else None
        imported = _build_importer(args, config).read(args.input, fmt=args.format)
        db = _db_for(args, config)
        provider = str(config.data.get("provider") or "local-file") if config is not None else "local-file"
        _sid, result = _persist_import(
            db,
            imported,
            mode="REPLAY",
            provider=provider,
            log_path=_log_for(args, db, config),
            config=config,
        )
        return CommandResult.json({"ok": True, **result})


class DemoService:
    def run(self, args: argparse.Namespace) -> CommandResult:
        config = _config_for(args)
        db = _db_for(args, config)
        result = run_demo(
            db,
            seed=int(args.seed),
            minutes=int(args.minutes),
            report_path=args.report,
            log_path=_log_for(args, db, config),
            config=config.to_dict(),
        )
        keys = ("session_id", "db_path", "report_path", "log_path", "dataset", "results")
        return CommandResult.json({"ok": True, **{key: result[key] for key in keys}})


class ReplayService:
    def run(self, args: argparse.Namespace) -> CommandResult:
        config = _config_for(args)
        db = _db_for(args, config)
        records, result, sid, config, capture_hint, replay_mode = self._capture(args, config, db)
        dataset_hash = capture_hint or _capture_hash(records)
        with SQLiteStore(db) as store:
            coordinator = RuntimeCoordinator(
                store,
                sid,
                config,
                mode=replay_mode,
                dataset_hash=dataset_hash,
                variant="trend_pullback_v1",
                partition=args.partition,
                checkpoint_name=args.checkpoint,
                checkpoint_every=args.checkpoint_every,
                source="replay",
                max_candles=args.max_candles,
                resume=args.resume,
            )
            replay_result = coordinator.replay(records, sort=not args.preserve_order, bootstrap=False, complete=True)
            refs, reference_id = coordinator.reference_signals()
            coordinator.finish(status="COMPLETED")
            status = coordinator.status()
            payload = {
                "ok": True,
                "mode": "REPLAY",
                **result,
                "capture_hash": dataset_hash,
                "analysis_id": coordinator.analysis_id,
                "reference_analysis_id": reference_id,
                "reference_signals": len(refs),
                "runtime": replay_result.to_dict(),
                "status": {
                    "connection": status.connection,
                    "analysis_enabled": status.analysis_enabled,
                    "blocked_reasons": status.analysis_blocked_reasons,
                    "pending_simulations": status.pending_simulations,
                    "completed_simulations": status.completed_simulations,
                },
                "persisted": store.status(sid)["counts"],
            }
        return CommandResult.json(payload)

    @staticmethod
    def _persisted_input(
        args: argparse.Namespace, config: EffectiveConfig, db: Path
    ) -> tuple[list[Any], dict[str, Any], str, EffectiveConfig, str | None, str]:
        imported = _build_importer(args, config).read(args.input, fmt=args.format)
        config = _override_config(config, instrument=imported.instrument, price_base=imported.price_base, mode="REPLAY")
        sid, result = _persist_import(
            db,
            imported,
            mode="REPLAY",
            provider="local-file",
            log_path=_log_for(args, db, config),
            config=config,
        )
        records = [{**record, "_persisted_capture": True} for record in imported.records]
        return records, result, sid, config, None, "REPLAY"

    @staticmethod
    def _session_input(
        args: argparse.Namespace, config: EffectiveConfig, db: Path
    ) -> tuple[list[dict[str, Any]], dict[str, Any], str, EffectiveConfig, str | None, str]:
        with SQLiteStore(db) as store:
            sessions = store.sessions(limit=100)
            if not sessions:
                raise ConfigError("No hay sesión para replay; indique --input.")
            sid = args.session or sessions[0]["session_id"]
            session = store.get_session(sid)
            if session is None:
                raise ConfigError(f"Sesión no encontrada: {sid}")
            records = _stored_records(store, sid)
            raw_stored_config = session.get("config")
            stored_config: Mapping[str, Any] = raw_stored_config if isinstance(raw_stored_config, Mapping) else {}
            hint = str(stored_config.get("capture_hash")) if stored_config.get("capture_hash") else None
            capture_base = next((record.get("price_base") for record in records if record.get("price_base")), None)
            replay_mode = str(session.get("mode", "REPLAY")).upper()
            config = _override_config(
                config,
                instrument=str(session.get("instrument") or config.instrument),
                price_base=str(capture_base) if capture_base else None,
                mode=replay_mode,
            )
            result = {"session_id": sid, "reused": True, "records": len(records)}
            return records, result, sid, config, hint, replay_mode

    @staticmethod
    def _capture(
        args: argparse.Namespace, config: EffectiveConfig, db: Path
    ) -> tuple[list[Any], dict[str, Any], str, EffectiveConfig, str | None, str]:
        if getattr(args, "input", None):
            return ReplayService._persisted_input(args, config, db)
        return ReplayService._session_input(args, config, db)


class BacktestService:
    def run(self, args: argparse.Namespace) -> CommandResult:
        config = _config_for(args)
        db = _db_for(args, config)
        with SQLiteStore(db) as store:
            sid, config, candle_rows, records, dataset_hash, mode, session_config = self._load(store, args, config)
            refs, reference_id = _persist_reference(
                store,
                sid,
                config,
                candle_rows,
                dataset_hash=dataset_hash,
                partition=args.partition,
                mode=mode,
                identity_extra={"boundary": args.boundary or ""},
            )
            primary, primary_analysis = self._primary_signals(
                store,
                sid,
                config,
                args,
                records,
                dataset_hash,
                mode,
                session_config,
            )
            points = self._points(candle_rows, args, config, mode)
            results = self._run_variants(
                store,
                sid,
                config,
                args,
                refs,
                primary,
                points,
                reference_id,
                primary_analysis,
                mode,
            )
            target = self._write_report(store, sid, args, db, results)
            payload = {
                "ok": True,
                "mode": "BACKTEST",
                "session_id": sid,
                "capture_hash": dataset_hash,
                "partition": args.partition,
                "boundary": args.boundary,
                "reference_signals": len(refs),
                "mtf_signals": len(primary),
                "reference_analysis_id": reference_id,
                "mtf_analysis_id": primary_analysis,
                "report": str(target),
                "results": [item.to_dict(include_simulations=False) for item in results],
                "persisted": store.status(sid)["counts"],
            }
        return CommandResult.json(payload)

    @staticmethod
    def _load(
        store: SQLiteStore, args: argparse.Namespace, config: EffectiveConfig
    ) -> tuple[str, EffectiveConfig, list[dict[str, Any]], list[dict[str, Any]], str, str, Mapping[str, Any]]:
        sessions = store.sessions(limit=100)
        sid = args.session or (sessions[0]["session_id"] if sessions else None)
        if not sid:
            raise ConfigError("No hay sesiones persistidas.")
        session = store.get_session(sid) or {}
        config = _override_config(config, instrument=str(session.get("instrument") or config.instrument))
        candle_rows = _latest_candles(store.list_candles(sid, closed_only=False))
        records = _stored_records(store, sid)
        raw_stored_config = session.get("config")
        session_config: Mapping[str, Any] = raw_stored_config if isinstance(raw_stored_config, Mapping) else {}
        dataset_hash = (
            str(session_config.get("capture_hash"))
            if session_config.get("capture_hash")
            else _capture_hash(records or candle_rows)
        )
        mode = str(session.get("mode", "REPLAY"))
        return sid, _override_config(config, mode=mode), candle_rows, records, dataset_hash, mode, session_config

    @staticmethod
    def _lineaged_primary(
        stored: Sequence[Mapping[str, Any]], config: EffectiveConfig, session_config: Mapping[str, Any]
    ) -> list[Mapping[str, Any]]:
        primary = [row for row in stored if _signal_variant(row) in {"", "trend_pullback_v1", "MULTITIMEFRAME"}]
        lineaged = [row for row in primary if row.get("analysis_config_hash")]
        if not lineaged:
            return primary
        session_hash = session_config.get("config_hash")
        if session_hash and str(session_hash) == str(config.config_hash):
            return lineaged
        return [row for row in lineaged if str(row.get("analysis_config_hash")) == str(config.config_hash)]

    @staticmethod
    def _recompute_primary(
        store: SQLiteStore,
        sid: str,
        config: EffectiveConfig,
        args: argparse.Namespace,
        records: list[dict[str, Any]],
        dataset_hash: str,
        mode: str,
    ) -> tuple[list[Mapping[str, Any]], str]:
        runtime = RuntimeCoordinator(
            store,
            sid,
            config,
            mode=mode,
            dataset_hash=dataset_hash,
            variant="trend_pullback_v1",
            partition=args.partition,
            checkpoint_name=f"backtest:{args.partition}:{args.boundary or 'none'}",
            checkpoint_every=max(1, min(500, len(records) or 1)),
            source="backtest-replay",
            max_candles=5000,
            resume=False,
            identity_extra={"boundary": args.boundary or ""},
        )
        runtime.replay(records, sort=True, bootstrap=False, complete=True)
        signals: list[Mapping[str, Any]] = [dict(signal.as_dict()) for signal in runtime.processor.signals]
        return signals, runtime.analysis_id

    @staticmethod
    def _primary_signals(
        store: SQLiteStore,
        sid: str,
        config: EffectiveConfig,
        args: argparse.Namespace,
        records: list[dict[str, Any]],
        dataset_hash: str,
        mode: str,
        session_config: Mapping[str, Any],
    ) -> tuple[list[Mapping[str, Any]], str]:
        primary = BacktestService._lineaged_primary(store.list_signals(sid), config, session_config)
        if not primary:
            return BacktestService._recompute_primary(store, sid, config, args, records, dataset_hash, mode)
        analysis_id = _analysis_for(
            store,
            sid,
            dataset_hash=dataset_hash,
            config=config,
            variant="trend_pullback_v1",
            partition=args.partition,
            metadata={"strategy": "trend_pullback_v1", "source_signal_count": len(primary)},
            identity_extra={"boundary": args.boundary or ""},
        )
        return primary, analysis_id

    @staticmethod
    def _points(
        candle_rows: Sequence[Mapping[str, Any]], args: argparse.Namespace, config: EffectiveConfig, mode: str
    ) -> list[Any]:
        from ..pipeline import points_from_candles

        point_tf = str(args.timeframe or _timeframe_name(config.strategy.trigger_timeframe)).upper()
        core_m1 = [
            _core_candle(row, mode=mode)
            for row in candle_rows
            if str(row.get("timeframe", "")).upper() == point_tf and bool(row.get("closed", True))
        ]
        return points_from_candles(core_m1)

    @staticmethod
    def _run_variants(
        store: SQLiteStore,
        sid: str,
        config: EffectiveConfig,
        args: argparse.Namespace,
        refs: list[Mapping[str, Any]],
        primary: list[Mapping[str, Any]],
        points: list[Any],
        reference_id: str,
        primary_analysis: str,
        mode: str,
    ) -> list[Any]:
        simulator = VirtualContractSimulator(spec=_sim_spec(config))
        runner = BacktestRunner(simulator=simulator, store=store, session_id=sid)
        baseline = runner.run(
            refs,
            points,
            variants=[
                VariantSpec(
                    "m1_trigger_reference",
                    "Referencia sólo M1",
                    {"context": False, "preparation": False},
                    mode="M1_REFERENCE",
                )
            ],
            boundary=args.boundary,
            partition=args.partition,
            data_quality=mode,
            resolution=str(args.timeframe or _timeframe_name(config.strategy.trigger_timeframe)).upper(),
            analysis_id=reference_id,
            analysis_name="m1_trigger_reference",
            analysis_config_hash=config.config_hash,
            contract_hash=_contract_hash(config),
        )
        mtf = runner.run(
            primary,
            points,
            variants=[
                VariantSpec(
                    "trend_pullback_v1",
                    "Contexto M15 + preparación M5 + disparador M1",
                    {"context": True, "preparation": True},
                    mode="MULTITIMEFRAME",
                )
            ],
            boundary=args.boundary,
            partition=args.partition,
            data_quality=mode,
            resolution=str(args.timeframe or _timeframe_name(config.strategy.trigger_timeframe)).upper(),
            analysis_id=primary_analysis,
            analysis_name="trend_pullback_v1",
            analysis_config_hash=config.config_hash,
            contract_hash=_contract_hash(config),
        )
        results = baseline + mtf
        for item in results:
            store.save_metric(
                sid,
                f"backtest:{item.variant}:{args.partition}",
                {"variant": item.variant, "partition": args.partition, "config_hash": config.config_hash},
                item.net_result,
                item.to_dict(include_simulations=False),
            )
        return results

    @staticmethod
    def _write_report(store: SQLiteStore, sid: str, args: argparse.Namespace, db: Path, results: list[Any]) -> Path:
        data = ReportBuilder(store, sid).summary()
        target = Path(args.report or db.with_name(f"{db.stem}-backtest.md")).expanduser()
        ReportBuilder(store, sid).write(target, format=args.format, data=data)
        return target


class ReportService:
    def run(self, args: argparse.Namespace) -> CommandResult:
        config = _config_for(args)
        db = _db_for(args, config)
        with SQLiteStore(db, read_only=True) as store:
            sessions = store.sessions(limit=100)
            sid = args.session or (sessions[0]["session_id"] if sessions else None)
            if not sid:
                raise ConfigError("No hay sesiones persistidas.")
            builder = ReportBuilder(store, sid)
            data = builder.summary()
            if args.output:
                return CommandResult.plain(str(builder.write(args.output, format=args.format, data=data)))
            if args.format == "json":
                return CommandResult.plain(builder.to_json(data))
            if args.format == "html":
                return CommandResult.plain(builder.to_html(data))
            return CommandResult.plain(builder.to_markdown(data))


def _kraken_bar_row(bar: Any, provenance: Any) -> dict[str, Any]:
    raw = bar.to_dict() if hasattr(bar, "to_dict") else dict(bar)
    return {
        "candle_id": raw.get("data_id", raw.get("candle_id", raw.get("source_record_id"))),
        "instrument": raw.get("instrument", "BTC/USD"),
        "timeframe": raw.get("resolution", raw.get("timeframe", raw.get("resolution_seconds"))),
        "start_ts": raw.get("interval_start", raw.get("start_ts", raw.get("start"))),
        "end_ts": raw.get("interval_end", raw.get("end_ts", raw.get("end"))),
        "available_ts": raw.get("available_at", raw.get("received_at")),
        "received_ts": raw.get("received_at"),
        "open": raw.get("open"),
        "high": raw.get("high"),
        "low": raw.get("low"),
        "close": raw.get("close"),
        "volume": raw.get("volume"),
        "closed": bool(raw.get("closed", True)),
        "source": raw.get("source", "kraken"),
        "price_base": raw.get("price_basis", "traded"),
        "quality": "PUBLIC_PROVIDER_CLOSED" if raw.get("closed", True) else "PUBLIC_PROVIDER_OPEN",
        "revision": raw.get("revision", 0),
        "source_record_id": raw.get("source_record_id"),
        "metadata": raw.get("metadata", {}),
        "provenance": provenance.to_dict() if hasattr(provenance, "to_dict") else provenance,
    }


def _freshness_state(
    config: EffectiveConfig, coordinator: RuntimeCoordinator, last_received_at: datetime | None
) -> tuple[str, list[str], Any]:
    closed_ends = [
        candle.end for values in coordinator.processor.candles.values() for candle in values if candle.closed
    ]
    last_closed_end = max(closed_ends, default=None)
    assessment = assess_freshness(
        now=datetime.now(UTC),
        last_received_at=last_received_at,
        last_closed_end=last_closed_end,
        max_feed_age_seconds=config.quality.max_feed_age_seconds,
        max_closed_candle_age_seconds=config.quality.max_closed_candle_age_seconds,
    )
    blocked: list[str] = []
    if assessment.quality.has("stale"):
        blocked.append("feed_stale")
    if assessment.quality.has("late"):
        blocked.append("closed_candle_late")
    return assessment.quality.status, blocked, assessment


def _save_open_bar(store: SQLiteStore, session_id: str, row: Mapping[str, Any], *, ordinal: int) -> None:
    """Persist a changing provider-open candle as an explicit revision."""
    try:
        store.save_candle(session_id, row, ordinal=ordinal)
        return
    except IdempotencyConflict:
        pass
    existing = [
        item
        for item in store.list_candles(session_id, timeframe=str(row.get("timeframe", "M1")))
        if item.get("start_ts") == str(row.get("start_ts")) and not bool(item.get("closed", True))
    ]
    revision = max((int(item.get("revision", 0) or 0) for item in existing), default=0) + 1
    updated = dict(row)
    base_id = str(updated.get("candle_id") or "open")
    updated["candle_id"] = f"{base_id}:r{revision}"
    updated["revision"] = revision
    store.save_candle(session_id, updated, ordinal=ordinal)


def _watch_intervals(config: EffectiveConfig) -> tuple[int, ...]:
    allowed = {1, 5, 15, 30, 60, 240, 1440, 10080, 21600}
    intervals = [tf.seconds // 60 for tf in config.timeframes if tf.seconds % 60 == 0 and tf.seconds // 60 in allowed]
    return tuple(dict.fromkeys(intervals or (1, 5, 15)))


@dataclass
class WatchContext:
    args: argparse.Namespace
    config: EffectiveConfig
    db: Path
    log_path: Path
    adapter: Any
    store: SQLiteStore
    session_id: str
    coordinator: RuntimeCoordinator
    telemetry: OperationTelemetry
    intervals: tuple[int, ...]
    rest_counts: dict[str, int]
    rest_errors: list[dict[str, Any]]
    rest_records: int = 0
    count: int = 0
    last_received_at: datetime | None = None
    recovery_in_progress: bool = False
    last_recovery_count: int = -1


class WatchService:
    """Compose the public Kraken watch path without embedding it in a handler."""

    def run(self, args: argparse.Namespace) -> CommandResult:
        if args.offline_demo:
            return DemoService().run(
                argparse.Namespace(
                    db=args.db,
                    seed=args.seed,
                    minutes=max(60, int(args.duration or 60)),
                    config=getattr(args, "config", None) or default_config(),
                    report=args.report,
                    log=args.log,
                )
            )
        config = _config_for(args, watch=True)
        config = _override_config(config, instrument=str(args.instrument or config.instrument), mode="LIVE")
        db = _db_for(args, config)
        try:
            adapter = self._adapter(config, args)
            with SQLiteStore(db) as store:
                ctx = self._context(args, config, db, adapter, store)
                self._bootstrap(ctx)
                self._stream(ctx)
                self._finish(ctx)
                return CommandResult.json(self._output(ctx))
        except Exception as exc:
            return CommandResult.plain(f"watch Kraken error: {type(exc).__name__}: {exc}", code=2, stderr=True)

    @staticmethod
    def _adapter_kwargs(config: EffectiveConfig) -> dict[str, Any]:
        provider_cfg = dict(config.provider)
        return {
            key: value
            for key, value in {
                "rest_endpoint": provider_cfg.get("rest_url"),
                "websocket_endpoint": provider_cfg.get("websocket_url"),
            }.items()
            if value
        }

    @staticmethod
    def _call_adapter(factory: Any, instrument: str, kwargs: Mapping[str, Any]) -> Any:
        named = "instrument" if getattr(factory, "__name__", "") == "KrakenAdapter" else "pair"
        try:
            return factory(**{named: instrument, **kwargs})
        except TypeError:
            return factory(**{named: instrument})

    @staticmethod
    def _adapter(config: EffectiveConfig, args: argparse.Namespace) -> Any:
        module = importlib.import_module("mtf_lab.data.kraken")
        factory = getattr(module, "KrakenAdapter", None) or getattr(module, "KrakenPublicAdapter", None)
        if factory is None:
            raise RuntimeError("el adaptador Kraken no expone KrakenAdapter/KrakenPublicAdapter")
        return WatchService._call_adapter(
            factory, str(args.instrument or config.instrument), WatchService._adapter_kwargs(config)
        )

    @staticmethod
    def _context(
        args: argparse.Namespace, config: EffectiveConfig, db: Path, adapter: Any, store: SQLiteStore
    ) -> WatchContext:
        instrument = str(args.instrument or config.instrument)
        if args.session and not args.instrument:
            existing = store.get_session(args.session)
            if existing and existing.get("instrument"):
                instrument = str(existing["instrument"])
                config = _override_config(config, instrument=instrument, mode="LIVE")
        session_id = args.session
        if session_id:
            session = store.get_session(session_id)
            if session is None:
                raise ConfigError(f"Sesión no encontrada: {session_id}")
            if str(session.get("mode", "")).upper() != "LIVE":
                raise ConfigError("La sesión de reanudación no es LIVE; no se reutiliza.")
            if str(session.get("instrument", "")).upper() != instrument.upper():
                raise ConfigError(
                    f"Instrumento incompatible con la sesión: {session.get('instrument')} != {instrument}"
                )
        else:
            session_id = store.create_session(
                mode="LIVE",
                provider="kraken-public-rest+websocket-v2",
                instrument=instrument,
                code_version=__version__,
                config={
                    **config.to_dict(),
                    "watch": {"checkpoint_every": args.checkpoint_every, "max_candles": args.max_candles},
                },
                metadata={
                    "public_only": True,
                    "synthetic": False,
                    "ws_endpoint": getattr(adapter, "websocket_endpoint", None),
                    "rest_endpoint": getattr(adapter, "rest_endpoint", None),
                },
            )
        coordinator = RuntimeCoordinator(
            store,
            session_id,
            config,
            mode="LIVE",
            dataset_hash=f"live:{session_id}",
            variant="trend_pullback_v1",
            partition="all",
            checkpoint_name=args.checkpoint,
            checkpoint_every=args.checkpoint_every,
            source="kraken-public",
            max_candles=args.max_candles,
            resume=args.resume,
        )
        log_path = _log_for(args, db, config)
        telemetry = OperationTelemetry(log_path=log_path, mode="LIVE", session_id=session_id, instrument=instrument)
        telemetry.state.update(
            connection="CONNECTING",
            data_quality="PUBLIC_PROVIDER",
            continuity="UNKNOWN",
            warmup_pending=coordinator.processor.status["warmup_pending"],
            reconciliation="PENDING",
        )
        intervals = _watch_intervals(config)
        telemetry.event(
            "watch_started", provider="kraken-public-rest+websocket-v2", intervals=list(intervals), resume=args.resume
        )
        return WatchContext(
            args, config, db, log_path, adapter, store, session_id, coordinator, telemetry, intervals, {}, []
        )

    def _fetch_bootstrap(self, ctx: WatchContext) -> dict[int, Any]:
        fetched_by_interval: dict[int, Any] = {}
        for interval in ctx.intervals:
            try:
                fetched = ctx.adapter.fetch_ohlc(interval=interval, include_open=False)
            except Exception as exc:
                detail = {"interval": interval, "type": type(exc).__name__, "error": str(exc)}
                ctx.rest_errors.append(detail)
                ctx.telemetry.logger.warning("rest_warmup_error", **detail)
                continue
            fetched_by_interval[interval] = fetched
            ctx.rest_counts[f"M{interval}"] = len(fetched.bars)
            ctx.telemetry.state.merge_nested(
                "coverage",
                {
                    f"M{interval}": {
                        "closed": len(fetched.bars),
                        "open": int(fetched.open_bar is not None),
                        "last": str(fetched.last_timestamp) if fetched.last_timestamp else None,
                    }
                },
            )
        return fetched_by_interval

    def _feed_bootstrap(self, ctx: WatchContext, fetched_by_interval: Mapping[int, Any]) -> None:
        for interval in sorted(fetched_by_interval, reverse=True):
            fetched = fetched_by_interval[interval]
            for bar in sorted(fetched.bars, key=lambda item: item.interval_start):
                ctx.coordinator.process(bar, bootstrap=True)
                self._record_received(ctx, getattr(bar, "received_at", None) or getattr(bar, "available_at", None))
                ctx.rest_records += 1
            if fetched.open_bar is not None:
                _save_open_bar(
                    ctx.store,
                    ctx.session_id,
                    _kraken_bar_row(fetched.open_bar, fetched.provenance),
                    ordinal=interval * 1_000_000,
                )

    @staticmethod
    def _bootstrap_complete(ctx: WatchContext, warmup: Mapping[str, Any]) -> bool:
        required = {
            _timeframe_name(ctx.config.strategy.context_timeframe),
            _timeframe_name(ctx.config.strategy.preparation_timeframe),
            _timeframe_name(ctx.config.strategy.trigger_timeframe),
        }
        configured = (_timeframe_name(tf) for tf in ctx.config.timeframes)
        return not ctx.rest_errors and all(warmup.get(tf, 1) == 0 for tf in configured if tf in required)

    def _bootstrap(self, ctx: WatchContext) -> None:
        self._feed_bootstrap(ctx, self._fetch_bootstrap(ctx))
        warmup = ctx.coordinator.processor.status["warmup_pending"]
        bootstrap_ok = self._bootstrap_complete(ctx, warmup)
        label, blocked, freshness = _freshness_state(ctx.config, ctx.coordinator, ctx.last_received_at)
        reasons = () if bootstrap_ok else ("bootstrap_incomplete",)
        ctx.coordinator.update_feed_state(
            connection="CONNECTED",
            reconciliation="BOOTSTRAP_VERIFIED" if bootstrap_ok else "BLOCKED",
            freshness=label,
            continuity="CONTINUOUS" if bootstrap_ok else "UNVERIFIED",
            blocked_reasons=tuple(dict.fromkeys((*reasons, *blocked))),
        )
        ctx.telemetry.state.update(
            connection="CONNECTED",
            warmup_pending=warmup,
            reconciliation="BOOTSTRAP_VERIFIED" if bootstrap_ok else "BLOCKED",
            continuity="CONTINUOUS" if bootstrap_ok else "UNKNOWN",
            data_quality=label,
            rest_records=ctx.rest_records,
            feed_age_seconds=freshness.feed_age_seconds,
            closed_candle_age_seconds=freshness.closed_candle_age_seconds,
        )
        ctx.telemetry.event(
            "bootstrap_complete", rest_counts=ctx.rest_counts, rest_errors=ctx.rest_errors, warmup_pending=warmup
        )

    @staticmethod
    def _record_received(ctx: WatchContext, received: datetime | None) -> None:
        if received is not None:
            ctx.last_received_at = received if ctx.last_received_at is None else max(ctx.last_received_at, received)

    def _recover_bars(self, ctx: WatchContext, since: datetime) -> list[dict[str, Any]]:
        errors: list[dict[str, Any]] = []
        for interval in ctx.intervals:
            try:
                fetched = ctx.adapter.recover_ohlc(interval=interval, since=since, include_open=False)
            except Exception as exc:
                errors.append({"interval": interval, "type": type(exc).__name__, "error": str(exc)})
                continue
            for bar in fetched.bars:
                ctx.coordinator.process(bar, bootstrap=True)
                self._record_received(ctx, getattr(bar, "received_at", None) or getattr(bar, "available_at", None))
        return errors

    def _recover(self, ctx: WatchContext, stream_status: Any) -> None:
        reconnect_count = int(getattr(stream_status, "reconnect_count", 0) or 0)
        if ctx.recovery_in_progress or reconnect_count <= ctx.last_recovery_count:
            return
        ctx.recovery_in_progress = True
        try:
            since = ctx.coordinator.processor.last_event_time or datetime.now(UTC)
            errors = self._recover_bars(ctx, since)
            ctx.last_recovery_count = reconnect_count
            reconciliation = "BLOCKED" if errors else "RECOVERED_BOUNDED"
            ctx.coordinator.update_feed_state(
                reconciliation=reconciliation,
                continuity="RECOVERED_BOUNDED" if not errors else None,
                blocked_reasons=("reconciliation_failed",) if errors else (),
            )
            ctx.telemetry.event("reconciliation_cycle", reconnect_count=reconnect_count, errors=errors, verified=False)
        finally:
            ctx.recovery_in_progress = False

    def _status_callback(self, ctx: WatchContext, status: Any) -> None:
        state = str(getattr(status, "state", "UNKNOWN")).upper()
        needs = bool(getattr(status, "needs_reconciliation", False))
        if needs and int(getattr(status, "reconnect_count", 0) or 0) > ctx.last_recovery_count:
            self._recover(ctx, status)
        reasons = ("feed_discontinuity",) if needs else ()
        reconciliation = ctx.coordinator.reconciliation_state
        if needs and reconciliation != "RECOVERED_BOUNDED":
            reconciliation = "NEEDS_RECONCILIATION"
        ctx.coordinator.update_feed_state(
            connection=state,
            reconciliation=reconciliation,
            blocked_reasons=tuple(dict.fromkeys((*ctx.coordinator.external_blocked_reasons, *reasons))),
            heartbeat_at=getattr(status, "last_message_at", None),
        )

    @staticmethod
    def _heartbeat_callback(ctx: WatchContext, now: datetime) -> None:
        if ctx.coordinator.connection_state not in {"DISCONNECTED", "ERROR"}:
            ctx.coordinator.heartbeat(now)

    def _stream(self, ctx: WatchContext) -> None:
        try:
            iterator = ctx.adapter.iter_trades(
                duration_seconds=ctx.args.duration,
                max_events=ctx.args.max_events,
                include_snapshot=not ctx.args.no_snapshot,
                status_callback=lambda status: self._status_callback(ctx, status),
                heartbeat_callback=lambda now: self._heartbeat_callback(ctx, now),
            )
        except TypeError:
            iterator = ctx.adapter.iter_trades(
                duration_seconds=ctx.args.duration,
                max_events=ctx.args.max_events,
                include_snapshot=not ctx.args.no_snapshot,
            )
        try:
            for event in iterator:
                self._process_event(ctx, event)
        finally:
            ctx.coordinator.advance(datetime.now(UTC), complete=False)

    def _process_event(self, ctx: WatchContext, event: Any) -> None:
        if bool(getattr(event, "is_snapshot", False)):
            ctx.coordinator.capture_only(event)
        else:
            ctx.coordinator.process(event)
        ctx.count += 1
        status = getattr(ctx.adapter, "status", None)
        needs = bool(getattr(status, "needs_reconciliation", False))
        state = str(getattr(status, "state", "CONNECTED"))
        self._record_received(ctx, getattr(event, "received_at", None) or getattr(event, "available_at", None))
        label, blocked, freshness = _freshness_state(ctx.config, ctx.coordinator, ctx.last_received_at)
        dynamic = ("feed_discontinuity",) if needs else ()
        reconciliation = (
            "NEEDS_RECONCILIATION"
            if needs and ctx.coordinator.reconciliation_state != "RECOVERED_BOUNDED"
            else ctx.coordinator.reconciliation_state
        )
        ctx.coordinator.update_feed_state(
            connection=state,
            reconciliation=reconciliation,
            freshness=label,
            blocked_reasons=tuple(dict.fromkeys((*dynamic, *blocked))),
        )
        proc = ctx.coordinator.processor.status
        ctx.telemetry.state.update(
            connection=state,
            last_received_ts=getattr(event, "received_at", None),
            last_available_ts=getattr(event, "available_at", None),
            events_processed=proc["events_processed"],
            candles_processed=proc["candles_processed"],
            signals=proc["signals"],
            errors=proc["errors"],
            warmup_pending=proc["warmup_pending"],
            reconciliation=ctx.coordinator.reconciliation_state,
            data_quality=label,
            feed_age_seconds=freshness.feed_age_seconds,
            closed_candle_age_seconds=freshness.closed_candle_age_seconds,
        )
        ctx.telemetry.state.update(feed_delay_ms=None)
        ctx.telemetry.reporter.emit()

    def _finish(self, ctx: WatchContext) -> None:
        status = getattr(ctx.adapter, "status", None)
        if bool(getattr(status, "needs_reconciliation", False)):
            self._reconcile_final(ctx)
        label, blocked, freshness = _freshness_state(ctx.config, ctx.coordinator, ctx.last_received_at)
        final_state = str(getattr(status, "state", "STOPPED")).upper()
        reasons = ("feed_disconnected", *blocked) if final_state in {"DISCONNECTED", "ERROR"} else tuple(blocked)
        ctx.coordinator.update_feed_state(
            connection=final_state, freshness=label, blocked_reasons=tuple(dict.fromkeys(reasons))
        )
        final_status = ctx.coordinator.status()
        ctx.telemetry.state.update(
            connection=final_state,
            warmup_pending=ctx.coordinator.processor.status["warmup_pending"],
            signals=len(ctx.coordinator.processor.signals),
            reconciliation=ctx.coordinator.reconciliation_state,
        )
        ctx.telemetry.event(
            "watch_complete",
            events=ctx.count,
            rest_records=ctx.rest_records,
            rest_errors=ctx.rest_errors,
            analysis_enabled=final_status.analysis_enabled,
            blocked_reasons=final_status.analysis_blocked_reasons,
            pending_simulations=final_status.pending_simulations,
        )
        ctx.telemetry.close()
        ctx.coordinator.finish(status="COMPLETED")

    def _reconcile_final(self, ctx: WatchContext) -> None:
        errors: list[dict[str, Any]] = []
        for interval in ctx.intervals:
            try:
                since = ctx.coordinator.processor.last_event_time or datetime.now(UTC)
                fetched = ctx.adapter.recover_ohlc(interval=interval, since=since, include_open=False)
                for bar in fetched.bars:
                    ctx.coordinator.process(bar, bootstrap=True)
            except Exception as exc:
                errors.append({"interval": interval, "type": type(exc).__name__, "error": str(exc)})
        if errors:
            ctx.coordinator.update_feed_state(reconciliation="BLOCKED", blocked_reasons=("reconciliation_failed",))
        else:
            ctx.coordinator.update_feed_state(reconciliation="RECOVERED_BOUNDED", blocked_reasons=())
        ctx.telemetry.event("reconciliation_complete", errors=errors, verified=False)

    @staticmethod
    def _output(ctx: WatchContext) -> dict[str, Any]:
        status = ctx.coordinator.status()
        provider_status = getattr(ctx.adapter, "status", None)
        label, _blocked, freshness = _freshness_state(ctx.config, ctx.coordinator, ctx.last_received_at)
        return {
            "ok": True,
            "mode": "LIVE",
            "session_id": ctx.session_id,
            "events": ctx.count,
            "rest_records": ctx.rest_records,
            "rest_counts": ctx.rest_counts,
            "rest_errors": ctx.rest_errors,
            "db": str(ctx.db),
            "log": str(ctx.log_path),
            "provider": "kraken-public-rest+websocket-v2",
            "analysis_id": ctx.coordinator.analysis_id,
            "analysis_enabled": status.analysis_enabled,
            "blocked_reasons": status.analysis_blocked_reasons,
            "pending_simulations": status.pending_simulations,
            "warmup_pending": ctx.coordinator.processor.status["warmup_pending"],
            "data_quality": label,
            "feed_age_seconds": freshness.feed_age_seconds,
            "closed_candle_age_seconds": freshness.closed_candle_age_seconds,
            "status": getattr(provider_status, "to_dict", lambda: {})(),
        }


class CfdPaperService:
    """Run the local cTrader normalisation and PAPER pipeline."""

    @staticmethod
    def _pipeline_config(config: EffectiveConfig) -> tuple[EffectiveConfig, str]:
        # Older profiles called native cTrader trendbars ``trade``/``traded``
        # (and some reused ``close``).  Preserve those profiles without
        # asserting an unproven trade stream: the explicit pipeline basis is
        # ``native``.  Bid/ask/mid continue to select quote events.
        base = str(config.price_base).strip().lower()
        if base in {"trade", "traded", "close", "native"}:
            return _override_config(config, price_base="native"), "native"
        return config, base

    @staticmethod
    def _document_values(parsed: Any) -> list[Any]:
        if not isinstance(parsed, Mapping):
            values = parsed
        elif isinstance(parsed.get("payloads"), list):
            values = parsed["payloads"]
        elif isinstance(parsed.get("envelopes"), list):
            values = parsed["envelopes"]
        elif isinstance(parsed.get("capture"), Mapping) and isinstance(parsed["capture"].get("payloads"), list):
            values = parsed["capture"]["payloads"]
        elif isinstance(parsed.get("capture"), Mapping) and isinstance(parsed["capture"].get("envelopes"), list):
            values = parsed["capture"]["envelopes"]
        else:
            values = parsed
        if not isinstance(values, list):
            raise ConfigError("captura JSON debe ser una lista o contener payloads[]")
        if not values:
            raise ConfigError("captura cTrader vacía")
        return values

    @staticmethod
    def _json_document(path: Path) -> list[Any]:
        try:
            with path.expanduser().open("r", encoding="utf-8") as stream:
                parsed = json.load(stream)
        except OSError as exc:
            raise ConfigError(f"no se pudo leer captura cTrader: {path}") from exc
        return CfdPaperService._document_values(parsed)

    @staticmethod
    def _jsonl_envelopes(path: Path, iter_jsonl: Any) -> Iterator[Any]:
        found = False
        for index, item in enumerate(iter_jsonl(path)):
            found = True
            yield CfdPaperService._capture_record(item, index=index)
        if not found:
            raise ConfigError("captura cTrader vacía")

    @staticmethod
    def _jsonl_input(path: Path) -> Iterable[Any]:
        try:
            from ..data.capture import iter_jsonl
        except ImportError:
            return CfdPaperService._legacy_jsonl(path)
        return CfdPaperService._jsonl_envelopes(path, iter_jsonl)

    @staticmethod
    def _capture_input(path: Path) -> Iterable[Any]:
        """Stream JSONL as versioned envelopes; arrays stay bounded by format."""
        if path.suffix.lower() in {".jsonl", ".ndjson"}:
            return CfdPaperService._jsonl_input(path)
        values = CfdPaperService._json_document(path)
        return (CfdPaperService._capture_record(item, index=index) for index, item in enumerate(values))

    @staticmethod
    def _legacy_jsonl(path: Path) -> Iterator[Any]:
        found = False
        try:
            stream = path.expanduser().open("r", encoding="utf-8")
        except OSError as exc:
            raise ConfigError(f"no se pudo leer captura cTrader: {path}") from exc
        with stream:
            for index, line in enumerate(stream):
                if not line.strip():
                    continue
                found = True
                try:
                    item = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ConfigError(f"payload cTrader {index} no es JSON válido") from exc
                yield CfdPaperService._capture_record(item, index=index)
        if not found:
            raise ConfigError("captura cTrader vacía")

    @staticmethod
    def _capture_record(item: Any, *, index: int | None = None) -> Any:
        if not isinstance(item, Mapping):
            suffix = f" {index}" if index is not None else ""
            raise ConfigError(f"payload cTrader{suffix} no es objeto")
        # Convert at the CLI input boundary so the pipeline receives durable
        # availability/ingest metadata instead of reconstructing it from a
        # market timestamp.  The fallback keeps the facade usable during a
        # partial installation where the capture module is not yet available.
        try:
            from ..data.capture import CaptureEnvelope, envelope_from_raw
        except ImportError:
            return dict(item)
        if isinstance(item, CaptureEnvelope):
            return item
        if "capture_schema" in item:
            return CaptureEnvelope.from_mapping(item)
        return envelope_from_raw(item, int(index or 0))

    @staticmethod
    def _durable_input(store: SQLiteStore, session_id: str) -> Iterator[Mapping[str, Any]]:
        """Replay the immutable capture archive, never a report payload copy."""
        found = False
        for row in store.iter_capture_envelopes(session_id):
            found = True
            yield row
        if not found:
            raise ConfigError(f"la sesión {session_id} no contiene envelopes de captura durables")

    @staticmethod
    def _instrument_spec_from_mapping(raw: Any) -> Any:
        """Build an instrument spec only from explicit, durable observations."""

        from ..data.ctrader import CTraderInstrumentSpec

        if not isinstance(raw, Mapping):
            raise ValueError("instrument_spec debe ser un mapping")
        symbol = raw.get("symbol")
        if symbol is None or not str(symbol).strip():
            raise ValueError("instrument_spec.symbol es obligatorio")
        fields = ("symbol_id", "digits", "pip_position", "price_scale")
        if any(field not in raw or isinstance(raw[field], bool) for field in fields):
            raise ValueError("instrument_spec requiere symbol_id/digits/pip_position/price_scale")
        return CTraderInstrumentSpec(
            symbol=str(symbol).strip().upper().replace("-", "/"),
            symbol_id=int(raw["symbol_id"]),
            digits=int(raw["digits"]),
            pip_position=int(raw["pip_position"]),
            price_scale=int(raw["price_scale"]),
        )

    @staticmethod
    def _session_capture_preflight(  # noqa: C901 - one guarded resume boundary
        store: SQLiteStore, session_id: str
    ) -> tuple[Mapping[str, Any], Mapping[str, Any] | None, Any | None] | CommandResult:
        """Inspect a durable session before constructing a new PAPER analysis.

        Historical pages carry the observed cTrader instrument specification in
        ``capture_provenance.instrument_spec``.  A fixture TOML intentionally
        has no broker ``symbol_id`` and must never replace that observation
        with its fallback.  Missing or conflicting evidence is rejected before
        ``CTraderPipeline`` can create another analysis identity.
        """

        from ..data.capture import MessageClass
        from .ctrader_history_export import CAPTURE_KIND

        session = store.get_session(session_id)
        if session is None:
            return CommandResult.json(
                {
                    "ok": False,
                    "state": "SESSION_NOT_FOUND",
                    "network_performed": False,
                    "session_id": session_id,
                    "next_action": "Use un session_id existente; no se creó una sesión ni un análisis PAPER.",
                },
                code=2,
                stderr=True,
            )

        found = False
        historical = False
        trendbar_pages = 0
        observed_raw: Mapping[str, Any] | None = None
        observed_spec: Any | None = None
        end_payload: Mapping[str, Any] | None = None
        for row in store.iter_capture_envelopes(session_id):
            found = True
            payload = row.get("payload")
            if not isinstance(payload, Mapping) or payload.get("capture_kind") != CAPTURE_KIND:
                continue
            historical = True
            message_class = str(row.get("message_class", ""))
            if message_class == MessageClass.END.value:
                end_payload = payload
                continue
            if message_class != MessageClass.TRENDBAR.value:
                continue
            trendbar_pages += 1
            provenance = payload.get("capture_provenance")
            raw = provenance.get("instrument_spec") if isinstance(provenance, Mapping) else None
            if not isinstance(raw, Mapping):
                return CommandResult.json(
                    {
                        "ok": False,
                        "state": "SESSION_CAPTURE_SPEC_REQUIRED",
                        "network_performed": False,
                        "session_id": session_id,
                        "capture_kind": CAPTURE_KIND,
                        "next_action": "La captura histórica durable no conserva instrument_spec observado; no se creó otro análisis.",
                    },
                    code=2,
                    stderr=True,
                )
            try:
                candidate = CfdPaperService._instrument_spec_from_mapping(raw)
            except (TypeError, ValueError) as exc:
                return CommandResult.json(
                    {
                        "ok": False,
                        "state": "SESSION_CAPTURE_SPEC_INVALID",
                        "network_performed": False,
                        "session_id": session_id,
                        "capture_kind": CAPTURE_KIND,
                        "error": type(exc).__name__,
                        "next_action": "La especificación durable no es utilizable; repita discovery/catálogo.",
                    },
                    code=2,
                    stderr=True,
                )
            if observed_spec is None:
                observed_spec = candidate
                observed_raw = dict(raw)
            elif candidate != observed_spec:
                return CommandResult.json(
                    {
                        "ok": False,
                        "state": "SESSION_CAPTURE_SPEC_CONFLICT",
                        "network_performed": False,
                        "session_id": session_id,
                        "capture_kind": CAPTURE_KIND,
                        "next_action": "Las páginas durables contienen especificaciones distintas; no se reanudó el análisis.",
                    },
                    code=2,
                    stderr=True,
                )

        if not found:
            return CommandResult.json(
                {
                    "ok": False,
                    "state": "SESSION_CAPTURE_REQUIRED",
                    "network_performed": False,
                    "session_id": session_id,
                    "next_action": "La sesión no contiene envelopes de captura durables; no se creó un análisis PAPER.",
                },
                code=2,
                stderr=True,
            )
        if not historical:
            # Synthetic/spot sessions retain the existing configured-spec
            # behavior; only historical cTrader pages need broker identity.
            return session, None, None
        if trendbar_pages == 0:
            return CommandResult.json(
                {
                    "ok": False,
                    "state": "SESSION_CAPTURE_REQUIRED",
                    "network_performed": False,
                    "session_id": session_id,
                    "capture_kind": CAPTURE_KIND,
                    "next_action": "La captura histórica no contiene páginas trendbar; no se creó un análisis PAPER.",
                },
                code=2,
                stderr=True,
            )
        if observed_spec is None or observed_raw is None:
            return CommandResult.json(
                {
                    "ok": False,
                    "state": "SESSION_CAPTURE_SPEC_REQUIRED",
                    "network_performed": False,
                    "session_id": session_id,
                    "capture_kind": CAPTURE_KIND,
                    "next_action": "La captura histórica durable no conserva instrument_spec observado; no se creó otro análisis.",
                },
                code=2,
                stderr=True,
            )
        metadata: dict[str, Any] = {
            "capture_kind": CAPTURE_KIND,
            "instrument_spec": observed_raw,
        }
        if end_payload is not None:
            for key in (
                "capture_order",
                "capture_status",
                "complete",
                "has_more",
                "issues",
                "history_complete",
                "continuity",
                "bounded_selection_verified",
            ):
                if key in end_payload:
                    metadata[key] = end_payload[key]
        return session, metadata, observed_spec

    @staticmethod
    def _historical_capture_preflight(
        config: EffectiveConfig, args: argparse.Namespace
    ) -> tuple[Mapping[str, Any] | None, Any | None, Path | None] | CommandResult:
        from ..data.ctrader import CTraderInstrumentSpec
        from .ctrader_history_export import CAPTURE_KIND, CAPTURE_ORDER, inspect_historical_capture

        input_path = Path(args.input).expanduser() if getattr(args, "input", None) else None
        if input_path is None:
            return None, None, None
        try:
            metadata = inspect_historical_capture(input_path)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            return CommandResult.json(
                {
                    "ok": False,
                    "state": "CAPTURE_METADATA_INVALID",
                    "network_performed": False,
                    "error": type(exc).__name__,
                    "next_action": "Revise la captura; no se abrió red ni se creó una sesión PAPER.",
                },
                code=2,
                stderr=True,
            )
        if metadata is None or metadata.get("capture_kind") != CAPTURE_KIND:
            return metadata, None, input_path
        if str(getattr(args, "price_base", None) or config.price_base).strip().lower() != "native":
            return CommandResult.json(
                {
                    "ok": False,
                    "state": "HISTORICAL_NATIVE_REQUIRED",
                    "network_performed": False,
                    "capture_kind": CAPTURE_KIND,
                    "next_action": "Use --price-base native; trendbars históricos no contienen bid/ask.",
                },
                code=2,
                stderr=True,
            )
        if (
            str(metadata.get("capture_order", "")) != CAPTURE_ORDER
            or str(getattr(args, "order", "as_observed")) != CAPTURE_ORDER
        ):
            return CommandResult.json(
                {
                    "ok": False,
                    "state": "HISTORICAL_ORDER_REQUIRED",
                    "network_performed": False,
                    "capture_kind": CAPTURE_KIND,
                    "next_action": "Use --order market_time_corrected; no se fingió recepción cronológica.",
                },
                code=2,
                stderr=True,
            )
        if (
            str(metadata.get("capture_status", "PARTIAL")) != "COMPLETE"
            or metadata.get("complete") is not True
            or metadata.get("has_more") is True
            or metadata.get("issues")
        ):
            return CommandResult.json(
                {
                    "ok": False,
                    "state": "PARTIAL_HISTORY_CAPTURE",
                    "network_performed": False,
                    "capture_kind": CAPTURE_KIND,
                    "complete": metadata.get("complete", False),
                    "has_more": metadata.get("has_more"),
                    "issues": list(metadata.get("issues", ())),
                    "next_action": "No se analiza una captura histórica parcial; repita la consulta y exportación.",
                },
                code=2,
                stderr=True,
            )
        spec_raw = metadata.get("instrument_spec")
        if not isinstance(spec_raw, Mapping):
            return CommandResult.json(
                {
                    "ok": False,
                    "state": "CAPTURE_SPEC_REQUIRED",
                    "network_performed": False,
                    "next_action": "La captura no contiene la especificación observada del instrumento.",
                },
                code=2,
                stderr=True,
            )
        observed_symbol = str(spec_raw.get("symbol", "")).strip().upper().replace("-", "/")
        configured_symbol = str(config.instrument).strip().upper().replace("-", "/")
        configured_cfd_symbol = str(config.cfd.get("instrument", config.instrument)).strip().upper().replace("-", "/")
        if not observed_symbol or configured_symbol != observed_symbol or configured_cfd_symbol != observed_symbol:
            return CommandResult.json(
                {
                    "ok": False,
                    "state": "CAPTURE_SPEC_MISMATCH",
                    "network_performed": False,
                    "observed_symbol": observed_symbol,
                    "configured_symbol": configured_symbol,
                    "next_action": "Use una configuración cuyo instrumento y CFD coincidan con la captura.",
                },
                code=2,
                stderr=True,
            )
        try:
            observed_spec = CTraderInstrumentSpec(
                symbol=observed_symbol,
                symbol_id=int(spec_raw["symbol_id"]),
                digits=int(spec_raw["digits"]),
                pip_position=int(spec_raw["pip_position"]),
                price_scale=int(spec_raw["price_scale"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            return CommandResult.json(
                {
                    "ok": False,
                    "state": "CAPTURE_SPEC_INVALID",
                    "network_performed": False,
                    "error": type(exc).__name__,
                    "next_action": "La especificación observada no es utilizable; repita discovery/catálogo.",
                },
                code=2,
                stderr=True,
            )
        return metadata, observed_spec, input_path

    def run(self, args: argparse.Namespace) -> CommandResult:  # noqa: C901 - compose one guarded PAPER command
        from ..data.ctrader import CTraderInstrumentSpec
        from .ctrader_pipeline import CTraderPipeline, synthetic_ctrader_capture

        config = _config_for(args)
        preflight = self._historical_capture_preflight(config, args)
        if isinstance(preflight, CommandResult):
            return preflight
        historical_metadata, observed_spec, input_path = preflight
        requested_price_base = getattr(args, "price_base", None)
        if requested_price_base:
            config = _override_config(config, price_base=str(requested_price_base), mode="REPLAY")
        config, analysis_basis = self._pipeline_config(config)
        capture_source = "local_file" if input_path is not None else "synthetic_fixture"
        db = _db_for(args, config)
        db.parent.mkdir(parents=True, exist_ok=True)
        with SQLiteStore(db) as store:
            session_metadata: Mapping[str, Any] | None = None
            session_record: Mapping[str, Any] = {}
            session_id = getattr(args, "session", None)
            if input_path is None and session_id:
                session_preflight = self._session_capture_preflight(store, str(session_id))
                if isinstance(session_preflight, CommandResult):
                    return session_preflight
                session_record, session_metadata, session_spec = session_preflight
                if session_spec is not None:
                    observed_spec = session_spec
                    historical_metadata = session_metadata
                    from .ctrader_history_export import CAPTURE_KIND, CAPTURE_ORDER

                    if str(config.price_base).strip().lower() != "native":
                        return CommandResult.json(
                            {
                                "ok": False,
                                "state": "HISTORICAL_NATIVE_REQUIRED",
                                "network_performed": False,
                                "session_id": session_id,
                                "capture_kind": CAPTURE_KIND,
                                "next_action": "Reanude con la misma configuración native; trendbars históricos no contienen bid/ask.",
                            },
                            code=2,
                            stderr=True,
                        )
                    if (
                        not isinstance(session_metadata, Mapping)
                        or str(session_metadata.get("capture_order", "")) != CAPTURE_ORDER
                        or str(getattr(args, "order", "as_observed")) != CAPTURE_ORDER
                    ):
                        return CommandResult.json(
                            {
                                "ok": False,
                                "state": "HISTORICAL_ORDER_REQUIRED",
                                "network_performed": False,
                                "session_id": session_id,
                                "capture_kind": CAPTURE_KIND,
                                "next_action": "Reanude con --order market_time_corrected; no se fingió recepción cronológica.",
                            },
                            code=2,
                            stderr=True,
                        )
                    if (
                        str(session_metadata.get("capture_status", "PARTIAL")) != "COMPLETE"
                        or session_metadata.get("complete") is not True
                        or session_metadata.get("has_more") is True
                        or session_metadata.get("issues")
                    ):
                        return CommandResult.json(
                            {
                                "ok": False,
                                "state": "PARTIAL_HISTORY_CAPTURE",
                                "network_performed": False,
                                "session_id": session_id,
                                "capture_kind": CAPTURE_KIND,
                                "complete": session_metadata.get("complete", False),
                                "has_more": session_metadata.get("has_more"),
                                "issues": list(session_metadata.get("issues", ())),
                                "next_action": "No se reanuda una captura histórica parcial; repita la consulta y exportación.",
                            },
                            code=2,
                            stderr=True,
                        )
                    configured_symbol = str(config.instrument).strip().upper().replace("-", "/")
                    configured_cfd_symbol = (
                        str(config.cfd.get("instrument", config.instrument)).strip().upper().replace("-", "/")
                    )
                    if observed_spec.symbol != configured_symbol or observed_spec.symbol != configured_cfd_symbol:
                        return CommandResult.json(
                            {
                                "ok": False,
                                "state": "CAPTURE_SPEC_MISMATCH",
                                "network_performed": False,
                                "session_id": session_id,
                                "observed_symbol": observed_spec.symbol,
                                "configured_symbol": configured_symbol,
                                "next_action": "Use una configuración cuyo instrumento y CFD coincidan con la captura.",
                            },
                            code=2,
                            stderr=True,
                        )
            if input_path is not None:
                capture_input: CTraderCapture | Iterable[Any] = self._capture_input(input_path)
            elif session_id:
                capture_source = "sqlite_capture_envelopes"
                capture_input = self._durable_input(store, str(session_id))
            else:
                symbol_id = (
                    int(observed_spec.symbol_id)
                    if observed_spec is not None
                    else _int_or_default(config.ctrader.get("symbol_id"), 99)
                )
                capture_input = synthetic_ctrader_capture(
                    start=datetime(2026, 1, 1, tzinfo=UTC),
                    symbol_id=symbol_id,
                    count=int(getattr(args, "count", 190)),
                    mode="REPLAY",
                )
            symbol_id = (
                int(observed_spec.symbol_id)
                if observed_spec is not None
                else _int_or_default(config.ctrader.get("symbol_id"), 99)
            )
            spec = observed_spec or CTraderInstrumentSpec(
                symbol=config.instrument,
                symbol_id=symbol_id,
                digits=int(config.ctrader.get("digits", 5)),
                pip_position=int(config.ctrader.get("pip_position", 4)),
                price_scale=int(config.ctrader.get("price_scale", 100_000)),
            )
            pipeline = CTraderPipeline(
                store, config, spec=spec, mode="REPLAY", max_candles=getattr(args, "max_candles", 256)
            )
            if session_id and session_metadata is not None:
                stored_config_hash = str(session_record.get("config_hash", ""))
                expected_config_hash = payload_hash(pipeline.session_config())
                if not stored_config_hash or stored_config_hash != expected_config_hash:
                    return CommandResult.json(
                        {
                            "ok": False,
                            "state": "SESSION_CONFIG_IDENTITY_MISMATCH",
                            "network_performed": False,
                            "session_id": session_id,
                            "next_action": "Reanude con la misma configuración y variante; no se creó otro análisis PAPER.",
                        },
                        code=2,
                        stderr=True,
                    )
                try:
                    from ..data.capture import CaptureIndex
                    from .ctrader_capture import capture_envelopes, capture_identity

                    with CaptureIndex(
                        capture_envelopes(self._durable_input(store, str(session_id))),
                        mode=cast(
                            Literal["as_observed", "market_time_corrected"], str(getattr(args, "order", "as_observed"))
                        ),
                    ) as index:
                        expected_dataset_ref = capture_identity(
                            index.capture_hash,
                            spec,
                            analysis_basis if analysis_basis in {"bid", "ask", "mid"} else "mid",
                        )
                except (TypeError, ValueError, OSError) as exc:
                    return CommandResult.json(
                        {
                            "ok": False,
                            "state": "SESSION_CAPTURE_INVALID",
                            "network_performed": False,
                            "session_id": session_id,
                            "error": type(exc).__name__,
                            "next_action": "La captura durable no puede reconstruirse con su contrato; no se creó otro análisis.",
                        },
                        code=2,
                        stderr=True,
                    )
                if str(session_record.get("dataset_ref", "")) != expected_dataset_ref:
                    return CommandResult.json(
                        {
                            "ok": False,
                            "state": "SESSION_DATASET_IDENTITY_MISMATCH",
                            "network_performed": False,
                            "session_id": session_id,
                            "next_action": "La identidad de la captura durable no coincide; no se creó otro análisis PAPER.",
                        },
                        code=2,
                        stderr=True,
                    )
            result = pipeline.run(
                capture_input,
                session_id=getattr(args, "session", None),
                capture_complete=not bool(getattr(args, "incomplete", False)),
                quote_basis=(analysis_basis if analysis_basis in {"bid", "ask", "mid"} else "mid"),
                chunk_size=int(getattr(args, "chunk_size", 128)),
                order=cast(Literal["as_observed", "market_time_corrected"], str(getattr(args, "order", "as_observed"))),
            )
            capture_view = result.capture.to_dict()
            if getattr(args, "include_payloads", False):
                capture_view["envelopes"] = list(store.iter_capture_envelopes(result.session_id))
        summary: dict[str, Any] = {
            "ok": True,
            "product": "FOREX_CFD_LOCAL_PAPER",
            "provider": "ctrader-open-api",
            "network_performed": False,
            "credentials_used": False,
            "capture_source": capture_source,
            "db": str(db),
            "session_id": result.session_id,
            "runtime_analysis_id": result.runtime_analysis_id,
            "paper_analysis_id": result.paper_analysis_id,
            "analysis_basis": result.analysis_basis,
            "capture": capture_view,
            "signals": len(result.signals),
            "trades": [trade.to_dict() for trade in result.trades],
            "paper_capture_complete": result.paper.capture_complete,
            "snapshot_hash": result.snapshot_hash,
            "next_action": "Revise la sesión en UI/reporte; las señales y fills son paper locales, no órdenes DEMO.",
        }
        if historical_metadata is not None:
            summary["historical_capture"] = {
                "capture_kind": historical_metadata.get("capture_kind"),
                "capture_order": historical_metadata.get("capture_order"),
                "observed_spec": historical_metadata.get("instrument_spec"),
            }
        if getattr(args, "report", None):
            target = Path(args.report).expanduser()
            target.parent.mkdir(parents=True, exist_ok=True)
            report_payload = result.to_dict()
            report_payload["capture"] = capture_view
            target.write_text(canonical_json(report_payload) + "\n", encoding="utf-8")
            summary["report_path"] = str(target)
        return CommandResult.json(summary)


__all__ = [
    "PROJECT_ROOT",
    "CommandResult",
    "default_db",
    "default_logs",
    "default_config",
    "default_watch_config",
    "effective_config",
    "_config_for",
    "_db_for",
    "_log_for",
    "_canonical_base",
    "_override_config",
    "_load_toml",
    "_sim_spec",
    "_discover_engine",
    "_build_importer",
    "_persist_import",
    "_stored_payload",
    "_stored_records",
    "_latest_candles",
    "_core_candle",
    "_capture_hash",
    "_contract_hash",
    "_analysis_for",
    "_signal_variant",
    "_persist_reference",
    "_kraken_bar_row",
    "_freshness_state",
    "_save_open_bar",
    "_watch_intervals",
    "_load_ctrader_capture_input",
    "DoctorService",
    "ImportService",
    "DemoService",
    "ReplayService",
    "BacktestService",
    "ReportService",
    "WatchService",
    "CfdPaperService",
]


# Compatibility name retained while the implementation lives in the service.
def _load_ctrader_capture_input(path: Path) -> Iterable[Any]:
    return CfdPaperService._capture_input(path)
