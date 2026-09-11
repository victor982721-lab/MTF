"""Command line entry points for the local MTF Lab operational layer.

The CLI is deliberately thin: ``RuntimeCoordinator`` is the shared path for
replay and observation, while reporting/UI consume only persisted read models.
There is no order, account or execution connector in this module.
"""
from __future__ import annotations

import argparse
from collections.abc import Iterable, Mapping
from dataclasses import replace
from datetime import UTC, datetime, timedelta
import importlib
import json
import os
import socket
import ssl
import sys
import tempfile
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from .. import __version__
from ..configuration import ConfigError, EffectiveConfig, load_config, normalize_simulation_mapping
from ..core import assess_freshness, compute_indicators
from ..pipeline import m1_reference_signals, points_from_candles
from ..runtime import RuntimeCoordinator, capture_hash
from ..runtime.state import to_core_candle
from .backtest import BacktestRunner, VariantSpec
from .demo import run_demo
from .importer import ColumnMapping, ImportValidationError, LocalImporter
from .logging_state import OperationTelemetry
from .persistence import IdempotencyConflict, SQLiteStore, payload_hash
from .reporting import ReportBuilder
from .simulation import EvaluationSpec, VirtualContractSimulator
from .ui import serve


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def default_db() -> Path:
    return Path(os.environ.get("MTF_LAB_DB", str(PROJECT_ROOT / "data" / "mtf_lab.sqlite3"))).expanduser()


def default_logs(db: Path) -> Path:
    return db.with_suffix(".jsonl")


def default_config() -> Path:
    return PROJECT_ROOT / "config" / "default.toml"


def default_watch_config() -> Path:
    candidate = PROJECT_ROOT / "config" / "kraken.toml"
    return candidate if candidate.exists() else default_config()


def effective_config(path: str | Path | None = None) -> EffectiveConfig:
    """Carga y normaliza la única configuración efectiva del proyecto."""
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


def _override_config(config: EffectiveConfig, *, instrument: str | None = None, price_base: str | None = None, mode: str | None = None) -> EffectiveConfig:
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
    """Compatibilidad de lectura para integradores; CLI usa EffectiveConfig."""
    if path is None:
        return {}
    import tomllib

    with Path(path).expanduser().open("rb") as stream:
        value = tomllib.load(stream)
    if not isinstance(value, dict):
        raise ConfigError("la configuración TOML debe ser una tabla")
    return value


def _sim_spec(config: EffectiveConfig | Mapping[str, Any] | None = None) -> EvaluationSpec:
    """Construye ``EvaluationSpec`` desde la normalización central."""
    return EvaluationSpec(**normalize_simulation_mapping(config))

def _build_importer(args: argparse.Namespace, config: EffectiveConfig | None = None) -> LocalImporter:
    instrument = getattr(args, "instrument", None) or (config.instrument if config else "UNKNOWN")
    timeframe = getattr(args, "timeframe", None) or (config.timeframes[0].name if config else "M1")
    base = getattr(args, "price_base", None) or (config.price_base if config else "close")
    # LocalImporter uses ``trade`` for its user-facing alias while core uses
    # ``traded`` internally; both mean the same explicitly declared base.
    if _canonical_base(base) == "traded":
        base = "trade"
    interval = getattr(args, "interval_seconds", None)
    mapping = ColumnMapping.from_text(getattr(args, "mapping", None))
    return LocalImporter(
        instrument=str(instrument), timeframe=str(timeframe), price_base=str(base), mapping=mapping,
        timezone=getattr(args, "timezone", None), timestamp_unit=getattr(args, "timestamp_unit", "iso8601"), interval_seconds=float(interval) if interval is not None else None,
        strict=not bool(getattr(args, "allow_issues", False)),
        source=(str(config.data.get("source")) if config is not None and config.data.get("source") else None),
        allow_out_of_order=bool(getattr(args, "allow_out_of_order", False)),
        allow_duplicates=bool(getattr(args, "allow_duplicates", False)),
    )


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
    metadata = {"import": imported.to_dict(), "capture_hash": session_config["capture_hash"], "log_path": str(log_path) if log_path else None}
    with SQLiteStore(db) as store:
        sid = store.create_session(
            session_id=session_id, mode=mode, provider=provider, instrument=imported.instrument,
            code_version=__version__, dataset_ref=imported.source_path, config=session_config, metadata=metadata,
        )
        for ordinal, record in enumerate(imported.records):
            if "open" in record:
                store.save_candle(sid, record, ordinal=ordinal)
            else:
                store.save_event(sid, record, ordinal=ordinal)
        store.save_checkpoint(
            sid, "import", cursor={"source_path": imported.source_path, "last_row": len(imported.records)},
            events_processed=len(imported.records), state={"coverage_start": imported.coverage_start, "coverage_end": imported.coverage_end, "capture_hash": session_config["capture_hash"]},
        )
        if finish:
            store.finish_session(sid, status="COMPLETED")
    return sid, {
        "session_id": sid, "records": len(imported.records), "issues": [x.to_dict() for x in imported.issues],
        "coverage_start": imported.coverage_start, "coverage_end": imported.coverage_end, "db": str(db), "mode": mode, "provider": provider,
    }


def cmd_doctor(args: argparse.Namespace) -> int:
    print("MTF Lab doctor")
    print(f"python: {sys.version.split()[0]} ({sys.executable})")
    print(f"sqlite: {__import__('sqlite3').sqlite_version}")
    print(f"proyecto: {PROJECT_ROOT}")
    import importlib.util

    errors: list[str] = []
    required = {"tomllib": "stdlib", "sqlite3": "stdlib", "mtf_lab.core": "núcleo", "mtf_lab.data": "datos", "mtf_lab.ops": "operaciones"}
    optional = {"websockets": "Kraken WebSocket", "requests": "HTTP opcional", "rich": "salida enriquecida"}
    for module, label in required.items():
        ok = importlib.util.find_spec(module) is not None
        print(f"dependencia {label}: {'OK' if ok else 'FALTA'} ({module})")
        if not ok:
            errors.append(f"dependencia requerida ausente: {module}")
    for module, label in optional.items():
        print(f"opcional {label}: {'OK' if importlib.util.find_spec(module) is not None else 'omitido'} ({module})")
    if sys.version_info < (3, 11):
        errors.append("Python >= 3.11 requerido")
    config: EffectiveConfig | None = None
    try:
        config = _config_for(args)
        print(f"configuración: OK hash={config.config_hash}")
        print(json.dumps({"effective_config": config.to_dict()}, ensure_ascii=False, sort_keys=True, default=str))
    except Exception as exc:
        print(f"configuración: ERROR {type(exc).__name__}: {exc}")
        errors.append(f"configuración: {exc}")
    db = _db_for(args, config)
    try:
        db.parent.mkdir(parents=True, exist_ok=True)
        with SQLiteStore(db) as store:
            print(f"storage: OK {db} schema={store.schema_version}")
    except Exception as exc:
        errors.append(f"storage: {exc}")
    if getattr(args, "connectivity", False):
        try:
            with urllib.request.urlopen("https://api.kraken.com/0/public/Time", timeout=float(args.timeout)) as response:
                body = response.read(4096).decode("utf-8", "replace")
            print(f"conectividad Kraken REST: OK ({response.status}) cuerpo={body[:160]}")
        except Exception as exc:
            print(f"conectividad Kraken REST: ERROR {type(exc).__name__}: {exc}")
            errors.append("conectividad opcional no disponible")
    else:
        print("conectividad: omitida (use --connectivity explícitamente)")
    for error in errors:
        print(f"ERROR: {error}")
    return 2 if errors else 0


def cmd_demo(args: argparse.Namespace) -> int:
    config = _config_for(args)
    db = _db_for(args, config)
    result = run_demo(db, seed=int(args.seed), minutes=int(args.minutes), report_path=args.report, log_path=_log_for(args, db, config), config=config)
    print(json.dumps({"ok": True, **{key: result[key] for key in ("session_id", "db_path", "report_path", "log_path", "dataset", "results")}}, ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_import(args: argparse.Namespace) -> int:
    config = effective_config(args.config) if getattr(args, "config", None) else None
    try:
        imported = _build_importer(args, config).read(args.input, fmt=args.format)
        db = _db_for(args, config)
        provider = str(config.data.get("provider") or "local-file") if config is not None else "local-file"
        sid, result = _persist_import(db, imported, mode="REPLAY", provider=provider, log_path=_log_for(args, db, config), config=config)
    except ImportValidationError as exc:
        print(json.dumps({"ok": False, "error": str(exc), "issues": [x.to_dict() for x in exc.issues]}, ensure_ascii=False, indent=2), file=sys.stderr)
        return 2
    print(json.dumps({"ok": True, **result}, ensure_ascii=False, indent=2))
    return 0


def _discover_engine(config: EffectiveConfig | Mapping[str, Any] | None = None) -> Any:
    """Factoría única y explícita del detector, sin fallback especulativo."""
    from ..core.strategy import TrendPullbackStrategy
    if isinstance(config, EffectiveConfig):
        return TrendPullbackStrategy(config.strategy)
    if isinstance(config, Mapping):
        value = config.get("strategy", config)
        return TrendPullbackStrategy(value)
    return TrendPullbackStrategy()


def _stored_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    value = row.get("payload")
    if isinstance(value, Mapping):
        return dict(value)
    raw = row.get("payload_json")
    if raw:
        try:
            parsed = json.loads(str(raw))
            if isinstance(parsed, Mapping):
                return dict(parsed)
        except (TypeError, json.JSONDecodeError):
            pass
    return {}


def _stored_records(store: SQLiteStore, session_id: str) -> list[dict[str, Any]]:
    """Recover the original capture contract, not derived decisions."""
    session = store.get_session(session_id) or {}
    mode = str(session.get("mode", "REPLAY"))
    records: list[dict[str, Any]] = []
    for row in store.list_events(session_id):
        data = _stored_payload(row)
        # Derived candle-close rows are read-model evidence, not source input.
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
        provenance = data.get("provenance") if isinstance(data.get("provenance"), Mapping) else {}
        origin = str(data.get("origin", provenance.get("origin", "native"))).lower()
        source = str(row.get("source", data.get("source", ""))).lower()
        if origin in {"aggregated", "derived", "resampled_ohlc", "runtime_aggregate"} or "aggregated" in source or "resampled" in source:
            continue
        data.update({
            "candle_id": row.get("candle_id"), "instrument": row.get("instrument", session.get("instrument", "unknown")),
            "timeframe": row.get("timeframe", "M1"), "start_ts": row.get("start_ts"), "end_ts": row.get("end_ts"),
            "available_ts": data.get("available_ts", row.get("available_ts")), "open": row.get("open"), "high": row.get("high"),
            "low": row.get("low"), "close": row.get("close"), "volume": row.get("volume"), "closed": bool(row.get("closed", True)),
            "source": row.get("source", "persisted"), "price_base": row.get("price_base", "traded"), "quality": row.get("quality", "UNKNOWN"),
            "revision": row.get("revision", 0), "mode": mode,
        })
        data["_persisted_capture"] = True
        records.append(data)
    return records


def _latest_candles(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    latest: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (str(row.get("instrument", "unknown")), str(row.get("timeframe", "M1")), str(row.get("start_ts")))
        candidate = dict(row)
        if key not in latest or int(candidate.get("revision", 0) or 0) >= int(latest[key].get("revision", 0) or 0):
            latest[key] = candidate
    return sorted(latest.values(), key=lambda item: (str(item.get("start_ts", "")), str(item.get("timeframe", ""))))


def _core_candle(row: Mapping[str, Any], *, mode: str = "REPLAY") -> Any:
    """Decode persisted candles while retaining quality/availability metadata."""
    data = _stored_payload(row)
    data.update({
        "instrument": row.get("instrument", data.get("instrument", "unknown")), "timeframe": row.get("timeframe", data.get("timeframe", "M1")),
        "start_ts": row.get("start_ts", data.get("start_ts", data.get("start"))), "end_ts": row.get("end_ts", data.get("end_ts", data.get("end"))),
        "open": row.get("open", data.get("open")), "high": row.get("high", data.get("high")), "low": row.get("low", data.get("low")),
        "close": row.get("close", data.get("close")), "volume": row.get("volume", data.get("volume", 0.0)), "closed": bool(row.get("closed", data.get("closed", True))),
        "source": row.get("source", data.get("source", "persisted")), "price_base": row.get("price_base", data.get("price_base", "traded")),
        "quality": row.get("quality", data.get("quality", "UNKNOWN")), "candle_id": row.get("candle_id", data.get("candle_id")),
        "available_at": row.get("available_ts", data.get("available_at", data.get("available_ts"))), "received_at": data.get("received_at", row.get("available_ts")),
        "mode": mode,
    })
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


def _analysis_for(store: SQLiteStore, session_id: str, *, dataset_hash: str, config: EffectiveConfig, variant: str, partition: str, metadata: Mapping[str, Any] | None = None, identity_extra: Mapping[str, Any] | None = None) -> str:
    return store.create_analysis(session_id, dataset_hash=dataset_hash, config_hash=config.config_hash, variant=variant, contract_hash=_contract_hash(config), partition=partition, code_version=config.version, metadata=metadata or {}, identity_extra=identity_extra or {})


def _signal_variant(row: Mapping[str, Any]) -> str:
    value = row.get("variant")
    if value:
        return str(value)
    payload = row.get("payload")
    if isinstance(payload, Mapping):
        return str(payload.get("variant") or payload.get("strategy") or "")
    return ""


def _persist_reference(store: SQLiteStore, session_id: str, config: EffectiveConfig, candles: list[Mapping[str, Any]], *, dataset_hash: str, partition: str = "all", mode: str = "REPLAY", identity_extra: Mapping[str, Any] | None = None) -> tuple[list[dict[str, Any]], str]:
    core_rows = [_core_candle(row, mode=mode) for row in _latest_candles(candles) if str(row.get("timeframe", "")).upper() == config.strategy.trigger_timeframe.name]
    if not core_rows:
        analysis_id = _analysis_for(store, session_id, dataset_hash=dataset_hash, config=config, variant="m1_trigger_reference", partition=partition, metadata={"strategy": "m1_trigger_reference", "coverage": 0}, identity_extra=identity_extra)
        return [], analysis_id
    series = compute_indicators(core_rows, config.indicators)
    refs = m1_reference_signals(series, rsi_threshold=config.strategy.rsi_threshold, mode=mode, identity_salt=config.config_hash)
    analysis_id = _analysis_for(store, session_id, dataset_hash=dataset_hash, config=config, variant="m1_trigger_reference", partition=partition, metadata={"strategy": "m1_trigger_reference", "coverage": len(core_rows)}, identity_extra=identity_extra)
    for ordinal, signal in enumerate(refs):
        store.save_signal(session_id, signal, ordinal=ordinal, analysis_id=analysis_id, variant="m1_trigger_reference", analysis_config_hash=config.config_hash, contract_hash=_contract_hash(config), partition=partition)
    return refs, analysis_id


def cmd_replay(args: argparse.Namespace) -> int:
    config = _config_for(args)
    db = _db_for(args, config)
    result: dict[str, Any]
    records: list[Any]
    capture_hash_hint: str | None = None
    replay_mode = "REPLAY"
    if getattr(args, "input", None):
        try:
            imported = _build_importer(args, config).read(args.input, fmt=args.format)
        except ImportValidationError as exc:
            print(json.dumps({"ok": False, "error": str(exc), "issues": [x.to_dict() for x in exc.issues]}, ensure_ascii=False, indent=2), file=sys.stderr)
            return 2
        config = _override_config(config, instrument=imported.instrument, price_base=imported.price_base, mode="REPLAY")
        sid, result = _persist_import(db, imported, mode="REPLAY", provider="local-file", log_path=_log_for(args, db, config), config=config)
        # The capture was already stored by import; mark the in-memory copy so
        # the runtime cannot rewrite its payload merely to attach indicators.
        records = [{**record, "_persisted_capture": True} for record in imported.records]
    else:
        with SQLiteStore(db) as store:
            sessions = store.sessions(limit=100)
            if not sessions:
                print("No hay sesión para replay; indique --input.", file=sys.stderr)
                return 2
            sid = args.session or sessions[0]["session_id"]
            session = store.get_session(sid)
            if session is None:
                print(f"Sesión no encontrada: {sid}", file=sys.stderr)
                return 2
            records = _stored_records(store, sid)
            stored_config = session.get("config") if isinstance(session.get("config"), Mapping) else {}
            capture_hash_hint = str(stored_config.get("capture_hash")) if stored_config.get("capture_hash") else None
            capture_base = next((record.get("price_base") for record in records if isinstance(record, Mapping) and record.get("price_base")), None)
            config = _override_config(config, instrument=str(session.get("instrument") or config.instrument), price_base=str(capture_base) if capture_base else None, mode=(replay_mode := str(session.get("mode", "REPLAY")).upper()))
            result = {"session_id": sid, "reused": True, "records": len(records)}
    dataset_hash = capture_hash_hint or _capture_hash(records)
    with SQLiteStore(db) as store:
        coordinator = RuntimeCoordinator(
            store, sid, config, mode=replay_mode, dataset_hash=dataset_hash, variant="trend_pullback_v1", partition=args.partition,
            checkpoint_name=args.checkpoint, checkpoint_every=args.checkpoint_every, source="replay", max_candles=args.max_candles, resume=args.resume,
        )
        replay_result = coordinator.replay(records, sort=not args.preserve_order, bootstrap=False, complete=True)
        refs, reference_id = coordinator.reference_signals()
        coordinator.finish(status="COMPLETED")
        status = coordinator.status()
        payload = {
            "ok": True, "mode": "REPLAY", **result, "capture_hash": dataset_hash, "analysis_id": coordinator.analysis_id,
            "reference_analysis_id": reference_id, "reference_signals": len(refs), "runtime": replay_result.to_dict(),
            "status": {"connection": status.connection, "analysis_enabled": status.analysis_enabled, "blocked_reasons": status.analysis_blocked_reasons, "pending_simulations": status.pending_simulations, "completed_simulations": status.completed_simulations},
            "persisted": store.status(sid)["counts"],
        }
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_backtest(args: argparse.Namespace) -> int:
    config = _config_for(args)
    db = _db_for(args, config)
    with SQLiteStore(db) as store:
        sessions = store.sessions(limit=100)
        sid = args.session or (sessions[0]["session_id"] if sessions else None)
        if not sid:
            print("No hay sesiones persistidas.", file=sys.stderr)
            return 2
        session = store.get_session(sid) or {}
        config = _override_config(config, instrument=str(session.get("instrument") or config.instrument))
        all_candle_rows = store.list_candles(sid, closed_only=False)
        candle_rows = _latest_candles(all_candle_rows)
        records = _stored_records(store, sid)
        session_config = session.get("config") if isinstance(session.get("config"), Mapping) else {}
        dataset_hash = str(session_config.get("capture_hash")) if session_config.get("capture_hash") else _capture_hash(records or candle_rows)
        mode = str(session.get("mode", "REPLAY"))
        config = _override_config(config, mode=mode)
        refs, reference_id = _persist_reference(store, sid, config, candle_rows, dataset_hash=dataset_hash, partition=args.partition, mode=mode, identity_extra={"boundary": args.boundary or ""})
        stored_signals = store.list_signals(sid)
        primary = [row for row in stored_signals if _signal_variant(row) in {"", "trend_pullback_v1", "MULTITIMEFRAME"}]
        # Prefer only the current effective config when lineage is present; an
        # old v1 capture without lineage remains usable and visible.
        lineaged_primary = [row for row in primary if row.get("analysis_config_hash")]
        if lineaged_primary:
            session_config_hash = session_config.get("config_hash")
            if session_config_hash and str(session_config_hash) == str(config.config_hash):
                # The capture was produced by this exact effective profile;
                # pipeline lineage may include the run-level seed/dataset hash.
                primary = lineaged_primary
            else:
                primary = [row for row in lineaged_primary if str(row.get("analysis_config_hash")) == str(config.config_hash)]
        primary_analysis: str
        if not primary:
            # No presentar ausencia de linaje como cero señales: ejecutar la
            # variante MTF solicitada sobre esta misma captura congelada.
            runtime = RuntimeCoordinator(
                store, sid, config, mode=mode, dataset_hash=dataset_hash,
                variant="trend_pullback_v1", partition=args.partition,
                checkpoint_name=f"backtest:{args.partition}:{args.boundary or 'none'}",
                checkpoint_every=max(1, min(500, len(records) or 1)),
                source="backtest-replay", max_candles=5000, resume=False, identity_extra={"boundary": args.boundary or ""},
            )
            runtime.replay(records, sort=True, bootstrap=False, complete=True)
            primary_analysis = runtime.analysis_id
            # Use the just-computed immutable signal objects, not the capture
            # level ``signals`` table (which may retain another membership).
            primary = [signal.as_dict() for signal in runtime.processor.signals]
        else:
            primary_analysis = _analysis_for(store, sid, dataset_hash=dataset_hash, config=config, variant="trend_pullback_v1", partition=args.partition, metadata={"strategy": "trend_pullback_v1", "source_signal_count": len(primary)}, identity_extra={"boundary": args.boundary or ""})
        # Las señales capturadas son inmutables y pueden pertenecer a un
        # análisis anterior; no se copian bajo otra identidad (la simulación
        # conserva su analysis_id propio y el payload referencia la señal).
        point_tf = str(args.timeframe or config.strategy.trigger_timeframe.name).upper()
        core_m1 = [_core_candle(row, mode=mode) for row in candle_rows if str(row.get("timeframe", "")).upper() == point_tf and bool(row.get("closed", True))]
        points = points_from_candles(core_m1)
        simulator = VirtualContractSimulator(spec=_sim_spec(config))
        runner = BacktestRunner(simulator=simulator, store=store, session_id=sid)
        boundary = args.boundary
        baseline_results = runner.run(refs, points, variants=[VariantSpec("m1_trigger_reference", "Referencia sólo M1", {"context": False, "preparation": False}, mode="M1_REFERENCE")], boundary=boundary, partition=args.partition, data_quality=mode, resolution=point_tf, analysis_id=reference_id, analysis_name="m1_trigger_reference", analysis_config_hash=config.config_hash, contract_hash=_contract_hash(config))
        mtf_results = runner.run(primary, points, variants=[VariantSpec("trend_pullback_v1", "Contexto M15 + preparación M5 + disparador M1", {"context": True, "preparation": True}, mode="MULTITIMEFRAME")], boundary=boundary, partition=args.partition, data_quality=mode, resolution=point_tf, analysis_id=primary_analysis, analysis_name="trend_pullback_v1", analysis_config_hash=config.config_hash, contract_hash=_contract_hash(config))
        results = baseline_results + mtf_results
        for item in results:
            store.save_metric(sid, f"backtest:{item.variant}:{args.partition}", {"variant": item.variant, "partition": args.partition, "config_hash": config.config_hash}, item.net_result, item.to_dict(include_simulations=False))
        report_data = ReportBuilder(store, sid).summary()
        target = Path(args.report or db.with_name(f"{db.stem}-backtest.md")).expanduser()
        ReportBuilder(store, sid).write(target, format=args.format, data=report_data)
        payload = {
            "ok": True, "mode": "BACKTEST", "session_id": sid, "capture_hash": dataset_hash,
            "partition": args.partition, "boundary": args.boundary, "reference_signals": len(refs), "mtf_signals": len(primary),
            "reference_analysis_id": reference_id, "mtf_analysis_id": primary_analysis, "report": str(target),
            "results": [item.to_dict(include_simulations=False) for item in results], "persisted": store.status(sid)["counts"],
        }
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_report(args: argparse.Namespace) -> int:
    config = _config_for(args)
    db = _db_for(args, config)
    with SQLiteStore(db, read_only=True) as store:
        sessions = store.sessions(limit=100)
        sid = args.session or (sessions[0]["session_id"] if sessions else None)
        if not sid:
            print("No hay sesiones persistidas.", file=sys.stderr)
            return 2
        builder = ReportBuilder(store, sid)
        data = builder.summary()
        if args.output:
            target = builder.write(args.output, format=args.format, data=data)
            print(str(target))
        elif args.format == "json":
            print(builder.to_json(data))
        elif args.format == "html":
            print(builder.to_html(data))
        else:
            print(builder.to_markdown(data))
    return 0


def _kraken_bar_row(bar: Any, provenance: Any) -> dict[str, Any]:
    raw = bar.to_dict() if hasattr(bar, "to_dict") else dict(bar)
    return {
        "candle_id": raw.get("data_id", raw.get("candle_id", raw.get("source_record_id"))),
        "instrument": raw.get("instrument", "BTC/USD"), "timeframe": raw.get("resolution", raw.get("timeframe", raw.get("resolution_seconds"))),
        "start_ts": raw.get("interval_start", raw.get("start_ts", raw.get("start"))), "end_ts": raw.get("interval_end", raw.get("end_ts", raw.get("end"))),
        "available_ts": raw.get("available_at", raw.get("received_at")), "received_ts": raw.get("received_at"),
        "open": raw.get("open"), "high": raw.get("high"), "low": raw.get("low"), "close": raw.get("close"), "volume": raw.get("volume"),
        "closed": bool(raw.get("closed", True)), "source": raw.get("source", "kraken"), "price_base": raw.get("price_basis", "traded"),
        "quality": "PUBLIC_PROVIDER_CLOSED" if raw.get("closed", True) else "PUBLIC_PROVIDER_OPEN", "revision": raw.get("revision", 0),
        "source_record_id": raw.get("source_record_id"), "metadata": raw.get("metadata", {}),
        "provenance": provenance.to_dict() if hasattr(provenance, "to_dict") else provenance,
    }


def _freshness_state(config: EffectiveConfig, coordinator: RuntimeCoordinator, last_received_at: datetime | None) -> tuple[str, list[str], Any]:
    closed_ends = [candle.end for values in coordinator.processor.candles.values() for candle in values if candle.closed]
    last_closed_end = max(closed_ends, default=None)
    assessment = assess_freshness(
        now=datetime.now(UTC), last_received_at=last_received_at, last_closed_end=last_closed_end,
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
    existing = [item for item in store.list_candles(session_id, timeframe=str(row.get("timeframe", "M1"))) if item.get("start_ts") == str(row.get("start_ts")) and not bool(item.get("closed", True))]
    revision = max((int(item.get("revision", 0) or 0) for item in existing), default=0) + 1
    updated = dict(row)
    base_id = str(updated.get("candle_id") or "open")
    updated["candle_id"] = f"{base_id}:r{revision}"
    updated["revision"] = revision
    store.save_candle(session_id, updated, ordinal=ordinal)


def _watch_intervals(config: EffectiveConfig) -> tuple[int, ...]:
    intervals: list[int] = []
    for tf in config.timeframes:
        if tf.seconds % 60 == 0 and tf.seconds // 60 in {1, 5, 15, 30, 60, 240, 1440, 10080, 21600}:
            intervals.append(tf.seconds // 60)
    return tuple(dict.fromkeys(intervals or (1, 5, 15)))


def cmd_watch(args: argparse.Namespace) -> int:
    if args.offline_demo:
        # Keep the explicit config path when supplied; the demo itself still
        # records SYNTHETIC provenance and never opens the provider.
        demo_args = argparse.Namespace(db=args.db, seed=args.seed, minutes=max(60, int(args.duration or 60)), config=getattr(args, "config", None) or default_config(), report=args.report, log=args.log)
        return cmd_demo(demo_args)
    config = _config_for(args, watch=True)
    if config.mode != "LIVE":
        # A public watch is explicitly live even if a caller supplied the
        # offline profile; the effective hash records this adaptation.
        config = _override_config(config, mode="LIVE")
    instrument = str(args.instrument or config.instrument)
    config = _override_config(config, instrument=instrument, mode="LIVE")
    db = _db_for(args, config)
    # A resumed session is authoritative for instrument identity unless the
    # caller explicitly supplied a different one (which would be a mismatch).
    if getattr(args, "session", None) and not getattr(args, "instrument", None):
        try:
            with SQLiteStore(db, read_only=True) as existing_store:
                existing_session = existing_store.get_session(args.session)
            if existing_session and existing_session.get("instrument"):
                instrument = str(existing_session["instrument"])
                config = _override_config(config, instrument=instrument, mode="LIVE")
        except Exception as exc:
            print(f"No se pudo leer la sesión de reanudación: {type(exc).__name__}: {exc}", file=sys.stderr)
            return 2
    log_path = _log_for(args, db, config)
    try:
        module = importlib.import_module("mtf_lab.data.kraken")
        factory = getattr(module, "KrakenAdapter", None) or getattr(module, "KrakenPublicAdapter", None)
        if factory is None:
            raise RuntimeError("el adaptador Kraken no expone KrakenAdapter/KrakenPublicAdapter")
        provider_cfg = dict(config.provider)
        adapter_kwargs = {"rest_endpoint": provider_cfg.get("rest_url"), "websocket_endpoint": provider_cfg.get("websocket_url")}
        adapter_kwargs = {key: value for key, value in adapter_kwargs.items() if value}
        try:
            adapter = factory(instrument=instrument, **adapter_kwargs) if getattr(factory, "__name__", "") == "KrakenAdapter" else factory(pair=instrument, **adapter_kwargs)
        except TypeError:
            # A test/different provider may expose only the pair argument; the
            # explicit configuration remains recorded in the session.
            adapter = factory(instrument=instrument) if getattr(factory, "__name__", "") == "KrakenAdapter" else factory(pair=instrument)
    except Exception as exc:
        print(f"No se pudo inicializar adaptador público Kraken: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    session_id = args.session
    try:
        with SQLiteStore(db) as store:
            if session_id:
                session = store.get_session(session_id)
                if session is None:
                    print(f"Sesión no encontrada: {session_id}", file=sys.stderr)
                    return 2
                if str(session.get("mode", "")).upper() != "LIVE":
                    print("La sesión de reanudación no es LIVE; no se reutiliza.", file=sys.stderr)
                    return 2
                if str(session.get("instrument", "")).upper() != instrument.upper():
                    print(f"Instrumento incompatible con la sesión: {session.get('instrument')} != {instrument}", file=sys.stderr)
                    return 2
            else:
                session_id = store.create_session(
                    mode="LIVE", provider="kraken-public-rest+websocket-v2", instrument=instrument, code_version=__version__,
                    config={**config.to_dict(), "watch": {"checkpoint_every": args.checkpoint_every, "max_candles": args.max_candles}},
                    metadata={"public_only": True, "synthetic": False, "ws_endpoint": getattr(adapter, "websocket_endpoint", None), "rest_endpoint": getattr(adapter, "rest_endpoint", None)},
                )
            coordinator = RuntimeCoordinator(
                store, session_id, config, mode="LIVE", dataset_hash=f"live:{session_id}", variant="trend_pullback_v1", partition="all",
                checkpoint_name=args.checkpoint, checkpoint_every=args.checkpoint_every, source="kraken-public", max_candles=args.max_candles, resume=args.resume,
            )
            telemetry = OperationTelemetry(log_path=log_path, mode="LIVE", session_id=session_id, instrument=instrument)
            telemetry.state.update(connection="CONNECTING", data_quality="PUBLIC_PROVIDER", continuity="UNKNOWN", warmup_pending=coordinator.processor.status["warmup_pending"], reconciliation="PENDING")
            telemetry.event("watch_started", provider="kraken-public-rest+websocket-v2", intervals=list(_watch_intervals(config)), resume=args.resume)
            rest_counts: dict[str, int] = {}; rest_errors: list[dict[str, Any]] = []; rest_records = 0
            last_received_at: datetime | None = None
            intervals = _watch_intervals(config)
            fetched_by_interval: dict[int, Any] = {}
            # First retain all native snapshots, then feed them from the
            # largest timeframe down. This gives native M15/M5 precedence over
            # any lower-timeframe OHLC derived by the runtime.
            for interval in intervals:
                try:
                    fetched = adapter.fetch_ohlc(interval=interval, include_open=False)
                    fetched_by_interval[interval] = fetched
                    rest_counts[f"M{interval}"] = len(fetched.bars)
                    telemetry.state.merge_nested("coverage", {f"M{interval}": {"closed": len(fetched.bars), "open": int(fetched.open_bar is not None), "last": str(fetched.last_timestamp) if fetched.last_timestamp else None}})
                except Exception as exc:
                    detail = {"interval": interval, "type": type(exc).__name__, "error": str(exc)}
                    rest_errors.append(detail); telemetry.logger.warning("rest_warmup_error", **detail)
            for interval in sorted(fetched_by_interval, reverse=True):
                fetched = fetched_by_interval[interval]
                for bar in sorted(fetched.bars, key=lambda item: item.interval_start):
                    coordinator.process(bar, bootstrap=True)
                    observed_received = getattr(bar, "received_at", None) or getattr(bar, "available_at", None)
                    if observed_received is not None:
                        last_received_at = observed_received if last_received_at is None else max(last_received_at, observed_received)
                    rest_records += 1
                # The provider's last row is explicitly open; retain it for
                # the viewer but never feed it into indicators/strategy.
                if fetched.open_bar is not None:
                    _save_open_bar(store, session_id, _kraken_bar_row(fetched.open_bar, fetched.provenance), ordinal=interval * 1_000_000)
            warmup = coordinator.processor.status["warmup_pending"]
            bootstrap_ok = not rest_errors and all(warmup.get(tf.name, 1) == 0 for tf in config.timeframes if tf.name in {config.strategy.context_timeframe.name, config.strategy.preparation_timeframe.name, config.strategy.trigger_timeframe.name})
            freshness_label, freshness_blocked, freshness = _freshness_state(config, coordinator, last_received_at)
            bootstrap_blocked = () if bootstrap_ok else ("bootstrap_incomplete",)
            coordinator.update_feed_state(
                connection="CONNECTED",
                reconciliation="BOOTSTRAP_VERIFIED" if bootstrap_ok else "BLOCKED",
                freshness=freshness_label,
                continuity="CONTINUOUS" if bootstrap_ok else "UNVERIFIED",
                blocked_reasons=tuple(dict.fromkeys((*bootstrap_blocked, *freshness_blocked))),
            )
            telemetry.state.update(connection="CONNECTED", warmup_pending=warmup, reconciliation="BOOTSTRAP_VERIFIED" if bootstrap_ok else "BLOCKED", continuity="CONTINUOUS" if bootstrap_ok else "UNKNOWN", data_quality=freshness_label, rest_records=rest_records, feed_age_seconds=freshness.feed_age_seconds, closed_candle_age_seconds=freshness.closed_candle_age_seconds)
            telemetry.event("bootstrap_complete", rest_counts=rest_counts, rest_errors=rest_errors, warmup_pending=warmup)
            count = 0
            recovery_in_progress = False
            last_recovery_count = -1

            def _recover_bounded(stream_status: Any) -> None:
                nonlocal recovery_in_progress, last_recovery_count, last_received_at
                reconnect_count = int(getattr(stream_status, "reconnect_count", 0) or 0)
                if recovery_in_progress or reconnect_count <= last_recovery_count:
                    return
                recovery_in_progress = True
                errors: list[dict[str, Any]] = []
                try:
                    since = coordinator.processor.last_event_time or datetime.now(UTC)
                    for interval in intervals:
                        try:
                            fetched = adapter.recover_ohlc(interval=interval, since=since, include_open=False)
                            for bar in fetched.bars:
                                coordinator.process(bar, bootstrap=True)
                                observed = getattr(bar, "received_at", None) or getattr(bar, "available_at", None)
                                if observed is not None:
                                    last_received_at = observed if last_received_at is None else max(last_received_at, observed)
                        except Exception as exc:
                            errors.append({"interval": interval, "type": type(exc).__name__, "error": str(exc)})
                    last_recovery_count = reconnect_count
                    if errors:
                        coordinator.update_feed_state(reconciliation="BLOCKED", blocked_reasons=("reconciliation_failed",))
                    else:
                        coordinator.update_feed_state(reconciliation="RECOVERED_BOUNDED", continuity="RECOVERED_BOUNDED", blocked_reasons=())
                    telemetry.event("reconciliation_cycle", reconnect_count=reconnect_count, errors=errors, verified=False)
                finally:
                    recovery_in_progress = False

            def _on_transport_status(stream_status: Any) -> None:
                state = str(getattr(stream_status, "state", "UNKNOWN")).upper()
                needs_reconciliation = bool(getattr(stream_status, "needs_reconciliation", False))
                if needs_reconciliation and int(getattr(stream_status, "reconnect_count", 0) or 0) > last_recovery_count:
                    _recover_bounded(stream_status)
                reasons = ("feed_discontinuity",) if needs_reconciliation else ()
                current_reconciliation = coordinator.reconciliation_state
                if needs_reconciliation and current_reconciliation != "RECOVERED_BOUNDED":
                    current_reconciliation = "NEEDS_RECONCILIATION"
                coordinator.update_feed_state(
                    connection=state,
                    reconciliation=current_reconciliation,
                    blocked_reasons=tuple(dict.fromkeys((*coordinator.external_blocked_reasons, *reasons))),
                    heartbeat_at=getattr(stream_status, "last_message_at", None),
                )

            def _on_transport_heartbeat(now: datetime) -> None:
                if coordinator.connection_state not in {"DISCONNECTED", "ERROR"}:
                    coordinator.heartbeat(now)

            try:
                iterator = adapter.iter_trades(
                    duration_seconds=args.duration, max_events=args.max_events,
                    include_snapshot=not args.no_snapshot,
                    status_callback=_on_transport_status, heartbeat_callback=_on_transport_heartbeat,
                )
            except TypeError:
                # Compatibility seam for test adapters that predate callbacks.
                iterator = adapter.iter_trades(duration_seconds=args.duration, max_events=args.max_events, include_snapshot=not args.no_snapshot)
            try:
                for event in iterator:
                    is_snapshot = bool(getattr(event, "is_snapshot", False))
                    if is_snapshot:
                        # Last-50 snapshot trades are capture evidence only;
                        # feeding them would falsely replay history before the
                        # REST watermark and could create duplicates.
                        coordinator.capture_only(event)
                    else:
                        coordinator.process(event)
                    count += 1
                    adapter_status = getattr(adapter, "status", None)
                    needs = bool(getattr(adapter_status, "needs_reconciliation", False))
                    state = str(getattr(adapter_status, "state", "CONNECTED"))
                    event_received = getattr(event, "received_at", None) or getattr(event, "available_at", None)
                    if event_received is not None:
                        last_received_at = event_received if last_received_at is None else max(last_received_at, event_received)
                    freshness_label, freshness_blocked, freshness = _freshness_state(config, coordinator, last_received_at)
                    dynamic_blocked = ("feed_discontinuity",) if needs else ()
                    coordinator.update_feed_state(
                        connection=state,
                        reconciliation=("NEEDS_RECONCILIATION" if needs and coordinator.reconciliation_state != "RECOVERED_BOUNDED" else coordinator.reconciliation_state),
                        freshness=freshness_label,
                        blocked_reasons=tuple(dict.fromkeys((*dynamic_blocked, *freshness_blocked))),
                    )
                    proc = coordinator.processor.status
                    telemetry.state.update(connection=state, last_received_ts=getattr(event, "received_at", None), last_available_ts=getattr(event, "available_at", None), events_processed=proc["events_processed"], candles_processed=proc["candles_processed"], signals=proc["signals"], errors=proc["errors"], warmup_pending=proc["warmup_pending"], reconciliation=coordinator.reconciliation_state, data_quality=freshness_label, feed_age_seconds=freshness.feed_age_seconds, closed_candle_age_seconds=freshness.closed_candle_age_seconds)
                    telemetry.state.update(feed_delay_ms=None)  # no false network-latency claim
                    telemetry.reporter.emit()
            finally:
                # A finite iterator can stop while old virtual horizons are
                # still pending. Advance by observed wall-clock availability,
                # but do not mark a live capture complete or invent a price.
                coordinator.advance(datetime.now(UTC), complete=False)
            status_obj = getattr(adapter, "status", None)
            if bool(getattr(status_obj, "needs_reconciliation", False)):
                recovery_errors: list[dict[str, Any]] = []
                for interval in intervals:
                    try:
                        since = coordinator.processor.last_event_time or datetime.now(UTC)
                        fetched = adapter.recover_ohlc(interval=interval, since=since, include_open=False)
                        for bar in fetched.bars:
                            coordinator.process(bar, bootstrap=True)
                    except Exception as exc:
                        recovery_errors.append({"interval": interval, "type": type(exc).__name__, "error": str(exc)})
                if recovery_errors:
                    coordinator.update_feed_state(reconciliation="BLOCKED", blocked_reasons=("reconciliation_failed",))
                else:
                    # Bounded OHLC overlap is useful evidence, but it is not a
                    # claim that every missed trade was recovered.
                    coordinator.update_feed_state(reconciliation="RECOVERED_BOUNDED", blocked_reasons=())
                telemetry.event("reconciliation_complete", errors=recovery_errors, verified=False)
            freshness_label, freshness_blocked, freshness = _freshness_state(config, coordinator, last_received_at)
            final_state = str(getattr(status_obj, "state", "STOPPED")).upper()
            if final_state in {"DISCONNECTED", "ERROR"}:
                coordinator.update_feed_state(connection=final_state, freshness=freshness_label, blocked_reasons=tuple(dict.fromkeys(("feed_disconnected", *freshness_blocked))))
            else:
                coordinator.update_feed_state(connection=final_state, freshness=freshness_label, blocked_reasons=tuple(freshness_blocked))
            final_status = coordinator.status()
            telemetry.state.update(connection=final_state, warmup_pending=coordinator.processor.status["warmup_pending"], signals=len(coordinator.processor.signals), reconciliation=coordinator.reconciliation_state)
            telemetry.event("watch_complete", events=count, rest_records=rest_records, rest_errors=rest_errors, analysis_enabled=final_status.analysis_enabled, blocked_reasons=final_status.analysis_blocked_reasons, pending_simulations=final_status.pending_simulations)
            telemetry.close()
            coordinator.finish(status="COMPLETED")
            output = {"ok": True, "mode": "LIVE", "session_id": session_id, "events": count, "rest_records": rest_records, "rest_counts": rest_counts, "rest_errors": rest_errors, "db": str(db), "log": str(log_path), "provider": "kraken-public-rest+websocket-v2", "analysis_id": coordinator.analysis_id, "analysis_enabled": final_status.analysis_enabled, "blocked_reasons": final_status.analysis_blocked_reasons, "pending_simulations": final_status.pending_simulations, "warmup_pending": coordinator.processor.status["warmup_pending"], "data_quality": freshness_label, "feed_age_seconds": freshness.feed_age_seconds, "closed_candle_age_seconds": freshness.closed_candle_age_seconds, "status": getattr(status_obj, "to_dict", lambda: {})()}
        print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
        return 0
    except Exception as exc:
        print(f"watch Kraken error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2


def _ctrader_activation_payload(config: EffectiveConfig) -> dict[str, Any]:
    return {"ctrader": dict(config.ctrader), "ctrader_oauth": dict(config.ctrader_oauth)}


def _selection_state_path(config: EffectiveConfig) -> Path:
    from .ctrader_activation import ActivationProfile, SecureTokenStore

    profile = ActivationProfile.from_mapping(dict(config.ctrader))
    return SecureTokenStore(profile.token_store_dir, project_root=PROJECT_ROOT).root / "selection.json"


def _discovery_state_path(config: EffectiveConfig) -> Path:
    return _selection_state_path(config).with_name("account-discovery.json")


def _secret_free(value: Any) -> Any:
    forbidden = {"access_token", "accesstoken", "refresh_token", "refreshtoken", "client_secret", "clientsecret", "password"}
    if isinstance(value, Mapping):
        return {
            str(key): _secret_free(item)
            for key, item in value.items()
            if str(key).lower() not in forbidden
        }
    if isinstance(value, (list, tuple)):
        return [_secret_free(item) for item in value]
    return value


def _persist_discovery(config: EffectiveConfig, *, token_ref: str, observed: Mapping[str, Any], observed_at: datetime | None = None) -> Path:
    """Persist private account metadata (never access/refresh tokens) atomically."""

    from .ctrader_activation import SecureTokenStore

    guard = SecureTokenStore(_discovery_state_path(config).parent, project_root=PROJECT_ROOT)
    guard._prepare()
    target = guard.root / "account-discovery.json"
    temporary = guard.root / f".account-discovery.{os.getpid()}.tmp"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(temporary, flags, 0o600)
    try:
        raw_records = observed.get("records", ()) if isinstance(observed, Mapping) else ()
        payload = {
            "version": 1,
            "source": "ctrader-open-api",
            "token_ref": str(token_ref),
            "observed_at": (observed_at or datetime.now(UTC)).astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "permissionScope": observed.get("permissionScope") if isinstance(observed, Mapping) else None,
            "accounts": [_secret_free(dict(item)) for item in raw_records if isinstance(item, Mapping)],
        }
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            fd = -1
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        os.chmod(target, 0o600)
        directory_fd = os.open(guard.root, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if fd >= 0:
            os.close(fd)
        if temporary.exists():
            temporary.unlink()
    return target


def _load_persisted_selection(config: EffectiveConfig) -> dict[str, Any] | None:
    """Read the explicit account selection from the private external store."""

    try:
        profile_token_ref = str(config.ctrader.get("token_ref", ""))
        path = _selection_state_path(config)
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(path, flags)
        try:
            if (os.fstat(fd).st_mode & 0o777) != 0o600:
                return None
            with os.fdopen(fd, "r", encoding="utf-8") as handle:
                fd = -1
                raw = json.load(handle)
        finally:
            if fd >= 0:
                os.close(fd)
        if not isinstance(raw, Mapping) or raw.get("version") != 1:
            return None
        if str(raw.get("token_ref", "")) != profile_token_ref:
            return None
        if str(raw.get("environment", "")).upper() != "DEMO":
            return None
        account_id = str(raw.get("account_id", "")).strip()
        if not account_id:
            return None
        return {"account_id": account_id, "account_selected": True, "environment": "DEMO"}
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return None


def _activation_payload_with_selection(config: EffectiveConfig) -> dict[str, Any]:
    payload = _ctrader_activation_payload(config)
    selected = _load_persisted_selection(config)
    if selected and not bool(payload["ctrader"].get("account_selected", False)):
        payload["ctrader"].update(selected)
    return payload


def _persist_selection(
    config: EffectiveConfig,
    *,
    account_id: str,
    token_ref: str,
    observed_at: datetime | None = None,
) -> Path:
    """Persist only the explicit DEMO selection atomically outside the repo."""

    from .ctrader_activation import SecureTokenStore

    guard = SecureTokenStore(_selection_state_path(config).parent, project_root=PROJECT_ROOT)
    guard._prepare()
    target = guard.root / "selection.json"
    temporary = guard.root / f".selection.{os.getpid()}.tmp"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(temporary, flags, 0o600)
    try:
        payload = {
            "version": 1,
            "token_ref": str(token_ref),
            "account_id": str(account_id),
            "environment": "DEMO",
            "observed_at": (observed_at or datetime.now(UTC)).astimezone(UTC).isoformat().replace("+00:00", "Z"),
        }
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            fd = -1
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        os.chmod(target, 0o600)
        directory_fd = os.open(guard.root, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if fd >= 0:
            os.close(fd)
        if temporary.exists():
            temporary.unlink()
    return target


def _ctrader_config(config: EffectiveConfig):
    """Map the profile section to the provider's strict, secret-free config."""
    from ..data.ctrader import CTraderConfig
    raw = dict(config.ctrader)
    allowed = {"environment", "host", "port", "symbol", "symbol_id", "account_id", "client_id", "client_secret_ref", "access_token_ref", "refresh_token_ref", "quote_basis", "timeframes", "digits", "price_scale", "pip_position", "request_timeout_seconds", "max_reconnects", "reconnect_backoff_seconds", "reconnect_backoff_max_seconds", "heartbeat_seconds", "queue_maxsize", "historical_count", "request_rate_limit", "historical_rate_limit"}
    values = {key: value for key, value in raw.items() if key in allowed}
    values.setdefault("environment", "demo")
    values.setdefault("symbol", config.instrument)
    values.setdefault("quote_basis", "mid")
    values.setdefault("timeframes", tuple(tf.name for tf in config.timeframes))
    if values.get("account_id") in {"", None}:
        values.pop("account_id", None)
    elif isinstance(values.get("account_id"), str) and values["account_id"].isdigit():
        values["account_id"] = int(values["account_id"])
    oauth = dict(config.ctrader_oauth)
    values.setdefault("client_id", os.environ.get(str(oauth.get("client_id_env", ""))) if oauth.get("client_id_env") else None)
    values.setdefault("client_secret_ref", str(oauth.get("client_secret_env", "")) if oauth.get("client_secret_env") else None)
    if raw.get("token_ref"):
        values.setdefault("access_token_ref", str(raw.get("token_ref")))
    return CTraderConfig.from_mapping(values)


def _token_metadata_for_config(config: EffectiveConfig):
    from ..ops.ctrader_activation import SecureTokenStore
    raw = dict(config.ctrader)
    token_ref = str(raw.get("token_ref", ""))
    token_dir = raw.get("token_store_dir")
    if not token_ref or not token_dir:
        return None
    try:
        root = SecureTokenStore(token_dir, project_root=PROJECT_ROOT)
        return root.metadata(token_ref)
    except Exception:
        return None


def _check_ctrader_connectivity(config: EffectiveConfig, *, timeout: float) -> dict[str, Any]:
    """Perform an explicitly requested, bounded DNS/TCP/TLS probe only."""

    provider_config = _ctrader_config(config)
    host = str(provider_config.host)
    port = int(provider_config.port)
    result: dict[str, Any] = {
        "requested": True,
        "endpoint": f"{host}:{port}",
        "dns": "NOT_CHECKED",
        "tcp": "NOT_CHECKED",
        "tls": "NOT_CHECKED",
        "account_authorized": False,
    }
    addresses = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    result["dns"] = "OK"
    result["address_count"] = len(addresses)
    raw = socket.create_connection((host, port), timeout=float(timeout))
    result["tcp"] = "OK"
    context = ssl.create_default_context()
    try:
        tls = context.wrap_socket(raw, server_hostname=host)
        try:
            result["tls"] = "OK"
            result["tls_version"] = tls.version()
        finally:
            tls.close()
    finally:
        try:
            raw.close()
        except OSError:
            pass
    result["note"] = "TLS de transporte solamente; no prueba OAuth, cuenta ni permisos."
    return result


def cmd_ctrader_doctor(args: argparse.Namespace) -> int:
    from ..data.ctrader import dependency_report
    from .ctrader_commands import status_command

    config = _config_for(args)
    report = dependency_report()
    output: dict[str, Any] = {
        "ok": True,
        "diagnostic_ok": True,
        "provider": "ctrader_open_api",
        "dependency": report.to_dict(),
        "config_path": config.path,
        "network_performed": False,
        "browser_opened": False,
        "connector_available": report.available,
        "connector_usable": False,
        "configuration_pending": False,
        "account_authorized": False,
        "connectivity": {"requested": False, "state": "NOT_CHECKED", "account_authorized": False},
    }
    if config.ctrader and config.ctrader_oauth:
        try:
            activation = status_command(
                _activation_payload_with_selection(config),
                token_metadata=_token_metadata_for_config(config),
                present_env_keys=set(os.environ),
                accounts=(),
                now=datetime.now(UTC),
            )
            output["activation"] = activation
            state = str(activation["status"]["state"])
            output["configuration_pending"] = state in {
                "APP_CREDENTIALS_REQUIRED",
                "ACCOUNTS_SCOPE_REQUIRED",
                "TOKEN_REFERENCE_REQUIRED",
                "TOKEN_EXPIRED",
            }
        except Exception as exc:
            output["activation"] = {"state": "INVALID_PROFILE", "ready": False, "next_action": "Corrija el perfil cTrader; no se modificó nada.", "error": type(exc).__name__}
            output["diagnostic_ok"] = False
    else:
        output["activation"] = {"state": "NOT_CONFIGURED", "ready": False, "next_action": "Use config/ctrader_query.toml o registre la aplicación; no se abrió navegador."}
        output["configuration_pending"] = True
    if getattr(args, "network", False):
        output["network_performed"] = True
        try:
            output["connectivity"] = _check_ctrader_connectivity(config, timeout=float(getattr(args, "timeout", 5.0)))
        except Exception as exc:
            output["connectivity"] = {
                "requested": True,
                "state": "ERROR",
                "account_authorized": False,
                "error": f"{type(exc).__name__}: {exc}",
                "note": "No se probó OAuth ni una cuenta; revise DNS/TCP/TLS.",
            }
            output["diagnostic_ok"] = False
    output["connector_usable"] = bool(
        report.available
        and isinstance(output.get("activation"), Mapping)
        and output["activation"].get("status", {}).get("ready", False)
        and output["connectivity"].get("tls") == "OK"
    )
    # `ok` means the diagnostic completed; connector availability and account
    # authorization are intentionally separate facts.
    output["ok"] = bool(output["diagnostic_ok"])
    print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
    return 0 if output["diagnostic_ok"] else 2


def cmd_ctrader_auth_url(args: argparse.Namespace) -> int:
    from .ctrader_activation import (
        ActivationProfile,
        LoopbackOAuthAssistant,
        OAuthAppConfig,
        OAuthAttemptStore,
        SecureTokenStore,
    )

    config = _config_for(args)
    if not config.ctrader_oauth or not config.ctrader:
        print(json.dumps({"ok": False, "state": "APP_CREDENTIALS_REQUIRED", "next_action": "Seleccione un perfil cTrader completo.", "browser_opened": False}, ensure_ascii=False, indent=2))
        return 2
    app = OAuthAppConfig.from_mapping(dict(config.ctrader_oauth))
    profile = ActivationProfile.from_mapping(dict(config.ctrader))
    if not profile.enabled:
        print(json.dumps({"ok": False, "state": "DISABLED", "network_performed": False, "browser_opened": False, "next_action": "Habilite explícitamente el perfil antes de iniciar OAuth."}, ensure_ascii=False, indent=2))
        return 2
    requested_scope = str(getattr(args, "scope", "") or ("trading" if profile.operation_mode.value == "DEMO" else "accounts")).strip().lower()
    if requested_scope not in profile.required_scopes:
        print(json.dumps({"ok": False, "state": "SCOPE_NOT_ALLOWED", "network_performed": False, "browser_opened": False, "next_action": "Solicite sólo un scope permitido por el perfil y autorícelo en una fase separada."}, ensure_ascii=False, indent=2))
        return 2
    client_id = os.environ.get(app.client_id_env, "")
    if not client_id:
        print(json.dumps({"ok": False, "state": "APP_CREDENTIALS_REQUIRED", "missing": [app.client_id_env], "next_action": "Defina client_id localmente; nunca lo guarde en TOML.", "browser_opened": False}, ensure_ascii=False, indent=2))
        return 2
    tokens = SecureTokenStore(profile.token_store_dir, project_root=PROJECT_ROOT)
    attempts = OAuthAttemptStore(profile.token_store_dir, project_root=PROJECT_ROOT)
    assistant = LoopbackOAuthAssistant(app, attempts=attempts, tokens=tokens)
    attempt = assistant.begin(
        client_id=client_id,
        requested_scopes=[requested_scope],
        open_browser=False,
    )
    handoff = assistant.resume_authorization(
        attempt.attempt_id,
        reveal_url=True,
        open_browser=bool(getattr(args, "open_browser", False)),
    )
    output = {
        "ok": True,
        "attempt": attempt.to_public_dict(),
        "authorization_url": handoff["authorization_url"],
        "browser_opened": handoff["browser_opened"],
        "network_performed": False,
        "requested_scope": requested_scope,
        "next_action": "Complete OAuth y ejecute callback-listen/token-exchange; accounts y trading se autorizan en fases separadas.",
    }
    print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_ctrader_callback_listen(args: argparse.Namespace) -> int:
    """Receive one OAuth callback through the registered local loopback server."""

    from .ctrader_activation import ActivationProfile, LoopbackOAuthAssistant, OAuthAppConfig, OAuthAttemptStore, SecureTokenStore

    config = _config_for(args)
    if not config.ctrader_oauth or not config.ctrader:
        print(json.dumps({"ok": False, "state": "APP_CREDENTIALS_REQUIRED", "network_performed": False}, ensure_ascii=False, indent=2))
        return 2
    if not getattr(args, "attempt_id", None):
        print(json.dumps({"ok": False, "state": "CALLBACK_REQUIRED", "network_performed": False, "next_action": "Proporcione --attempt-id devuelto por auth-url."}, ensure_ascii=False, indent=2))
        return 2
    app = OAuthAppConfig.from_mapping(dict(config.ctrader_oauth))
    profile = ActivationProfile.from_mapping(dict(config.ctrader))
    try:
        tokens = SecureTokenStore(profile.token_store_dir, project_root=PROJECT_ROOT)
        attempts = OAuthAttemptStore(profile.token_store_dir, project_root=PROJECT_ROOT)
        assistant = LoopbackOAuthAssistant(app, attempts=attempts, tokens=tokens)
        attempt = assistant.listen_callback(
            args.attempt_id,
            open_browser=bool(getattr(args, "open_browser", False)),
            timeout_seconds=float(getattr(args, "timeout", 600.0)),
        )
    except Exception as exc:
        print(json.dumps({"ok": False, "state": type(exc).__name__, "network_performed": False, "next_action": "No se recibió un callback válido; reanude el intento sin exponer el código.", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 2
    print(json.dumps({"ok": True, "attempt": attempt.to_public_dict(), "network_performed": False, "browser_opened": bool(getattr(args, "open_browser", False)), "next_action": "Ejecute token-exchange --attempt-id para intercambiar el código persistido; no se imprimió el código."}, ensure_ascii=False, indent=2, default=str))
    return 0


def _fixture_oauth_response(*, refreshed: bool = False):
    suffix = "refreshed" if refreshed else "exchange"
    return {
        "accessToken": f"fixture-access-{suffix}",
        "refreshToken": f"fixture-refresh-{suffix}",
        "expiresIn": 3600,
        "tokenType": "bearer",
    }


def _oauth_http_request(url: str, params: Mapping[str, str], timeout: float) -> Mapping[str, Any]:
    """POST form data to the validated OAuth endpoint without echoing secrets."""

    encoded = urllib.parse.urlencode(dict(params)).encode("utf-8")
    request = urllib.request.Request(
        str(url),
        data=encoded,
        method="POST",
        headers={"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        with urllib.request.urlopen(request, timeout=float(timeout)) as response:
            body = response.read(128 * 1024)
    except Exception as exc:
        raise RuntimeError("solicitud OAuth falló; revise conectividad y estado de la aplicación") from exc
    try:
        parsed = json.loads(body.decode("utf-8"))
    except Exception as exc:
        raise RuntimeError("respuesta OAuth no es JSON válido") from exc
    if not isinstance(parsed, Mapping):
        raise RuntimeError("respuesta OAuth no es un objeto")
    return parsed


def _read_callback_input(args: argparse.Namespace) -> str:
    """Read a callback URI from protected local input, never a process arg."""

    callback_file = getattr(args, "callback_file", None)
    use_stdin = bool(getattr(args, "callback_stdin", False))
    if bool(callback_file) == use_stdin:
        raise ValueError("proporcione exactamente --callback-file (0600) o --callback-stdin")
    if use_stdin:
        value = sys.stdin.read(16_384)
    else:
        path = Path(callback_file).expanduser()
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(path, flags)
        except OSError as exc:
            raise ValueError("no se pudo abrir callback-file de forma segura") from exc
        try:
            info = os.fstat(fd)
            mode = info.st_mode & 0o777
            if mode != 0o600 or info.st_uid != os.getuid():
                raise ValueError("callback-file debe ser privado, del usuario actual y tener permisos exactos 0600")
            with os.fdopen(fd, "r", encoding="utf-8") as handle:
                fd = -1
                value = handle.read(16_384)
        finally:
            if fd >= 0:
                os.close(fd)
    value = value.strip()
    if not value or len(value) > 16_384:
        raise ValueError("callback local vacío o demasiado grande")
    return value


def cmd_ctrader_token_exchange(args: argparse.Namespace) -> int:
    from .ctrader_activation import (
        ActivationProfile,
        LoopbackOAuthAssistant,
        OAuthAppConfig,
        OAuthAttemptPhase,
        OAuthAttemptStore,
        SecureTokenStore,
    )

    config = _config_for(args)
    if not config.ctrader_oauth or not config.ctrader:
        print(json.dumps({"ok": False, "state": "APP_CREDENTIALS_REQUIRED", "network_performed": False}, ensure_ascii=False, indent=2))
        return 2
    app = OAuthAppConfig.from_mapping(dict(config.ctrader_oauth))
    profile = ActivationProfile.from_mapping(dict(config.ctrader))
    if getattr(args, "fixture", False):
        with tempfile.TemporaryDirectory(prefix="mtf-lab-ctrader-fixture-") as tmp:
            fixture_root = Path(tmp)
            tokens = SecureTokenStore.for_fixture(fixture_root / "tokens", project_root=PROJECT_ROOT)
            attempts = OAuthAttemptStore.for_fixture(fixture_root / "attempts", project_root=PROJECT_ROOT)
            assistant = LoopbackOAuthAssistant(app, attempts=attempts, tokens=tokens, fixture_mode=True)
            fixture_scope = ("trading",) if "trading" in profile.required_scopes else ("accounts",)
            attempt = assistant.begin(
                client_id="fixture-client",
                requested_scopes=fixture_scope,
            )
            callback = f"{app.redirect_uri}?code=fixture-code&state={attempt.csrf_state}"
            assistant.receive_callback(attempt.attempt_id, callback)
            metadata = assistant.exchange(
                attempt.attempt_id,
                client_id="fixture-client",
                client_secret="fixture-runtime-only",
                token_ref=profile.token_ref,
                observed_scopes=profile.required_scopes,
                requester=lambda url, params, timeout: _fixture_oauth_response(),
            )
            output = {
                "ok": True,
                "fixture": True,
                "network_performed": False,
                "token": metadata.to_dict(),
                "secrets": "REDACTED",
                "real_token_store_touched": False,
            }
        output["fixture_store_removed"] = not fixture_root.exists()
        print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
        return 0
    if not getattr(args, "attempt_id", None):
        print(json.dumps({"ok": False, "state": "CALLBACK_REQUIRED", "network_performed": False, "next_action": "Use auth-url y entregue --attempt-id más --callback-file 0600 o --callback-stdin; no pase el código como argumento."}, ensure_ascii=False, indent=2))
        return 2
    if getattr(args, "callback_uri", None):
        print(json.dumps({"ok": False, "state": "CALLBACK_INPUT_UNSAFE", "network_performed": False, "next_action": "No pase el callback con código en argumentos; use --callback-file 0600 o --callback-stdin."}, ensure_ascii=False, indent=2))
        return 2
    client_id = os.environ.get(app.client_id_env, "")
    client_secret = os.environ.get(app.client_secret_env, "")
    if not client_id or not client_secret:
        print(json.dumps({"ok": False, "state": "APP_CREDENTIALS_REQUIRED", "network_performed": False, "next_action": "Defina las variables locales de aplicación; no se guardaron secretos."}, ensure_ascii=False, indent=2))
        return 2
    network_performed = False
    try:
        tokens = SecureTokenStore(profile.token_store_dir, project_root=PROJECT_ROOT)
        attempts = OAuthAttemptStore(profile.token_store_dir, project_root=PROJECT_ROOT)
        assistant = LoopbackOAuthAssistant(app, attempts=attempts, tokens=tokens)
        pending_attempt = attempts.load(args.attempt_id)
        if pending_attempt.phase is not OAuthAttemptPhase.CALLBACK_RECEIVED:
            callback_uri = _read_callback_input(args)
            assistant.receive_callback(args.attempt_id, callback_uri)
        network_performed = True
        metadata = assistant.exchange(
            args.attempt_id,
            client_id=client_id,
            client_secret=client_secret,
            token_ref=profile.token_ref,
            observed_scopes=None,
            requester=_oauth_http_request,
        )
    except Exception as exc:
        print(json.dumps({"ok": False, "state": type(exc).__name__, "network_performed": network_performed, "next_action": "No se guardó un token incompleto; revise callback, aplicación y código vigente.", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 2
    finally:
        client_secret = ""
    print(json.dumps({"ok": True, "network_performed": network_performed, "token": metadata.to_dict(), "secrets": "REDACTED", "next_action": "Descubra las cuentas autorizadas antes de seleccionar una cuenta DEMO."}, ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_ctrader_token_refresh(args: argparse.Namespace) -> int:
    from .ctrader_activation import (
        ActivationProfile,
        LoopbackOAuthAssistant,
        OAuthAppConfig,
        OAuthAttemptStore,
        SecureTokenStore,
    )

    config = _config_for(args)
    app = OAuthAppConfig.from_mapping(dict(config.ctrader_oauth))
    profile = ActivationProfile.from_mapping(dict(config.ctrader))
    if getattr(args, "fixture", False):
        with tempfile.TemporaryDirectory(prefix="mtf-lab-ctrader-fixture-") as tmp:
            fixture_root = Path(tmp)
            tokens = SecureTokenStore.for_fixture(fixture_root / "tokens", project_root=PROJECT_ROOT)
            attempts = OAuthAttemptStore.for_fixture(fixture_root / "attempts", project_root=PROJECT_ROOT)
            tokens.rotate(
                profile.token_ref,
                access_token="fixture-access-before-refresh",
                refresh_token="fixture-refresh-before-refresh",
                granted_scopes=profile.required_scopes,
                expires_at=datetime.now(UTC) + timedelta(minutes=5),
                fixture_payload=True,
            )
            assistant = LoopbackOAuthAssistant(app, attempts=attempts, tokens=tokens, fixture_mode=True)
            metadata = assistant.refresh(
                profile.token_ref,
                client_id="fixture-client",
                client_secret="fixture-runtime-only",
                observed_scopes=profile.required_scopes,
                requester=lambda url, params, timeout: _fixture_oauth_response(refreshed=True),
            )
            output = {
                "ok": True,
                "fixture": True,
                "network_performed": False,
                "token": metadata.to_dict(),
                "secrets": "REDACTED",
                "real_token_store_touched": False,
            }
        output["fixture_store_removed"] = not fixture_root.exists()
        print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
        return 0
    client_id = os.environ.get(app.client_id_env, "")
    client_secret = os.environ.get(app.client_secret_env, "")
    if not client_id or not client_secret:
        print(json.dumps({"ok": False, "state": "APP_CREDENTIALS_REQUIRED", "network_performed": False, "next_action": "Defina las variables locales de aplicación; no se guardaron secretos."}, ensure_ascii=False, indent=2))
        return 2
    network_performed = False
    try:
        tokens = SecureTokenStore(profile.token_store_dir, project_root=PROJECT_ROOT)
        attempts = OAuthAttemptStore(profile.token_store_dir, project_root=PROJECT_ROOT)
        assistant = LoopbackOAuthAssistant(app, attempts=attempts, tokens=tokens)
        existing = tokens.read(profile.token_ref)
        network_performed = True
        metadata = assistant.refresh(
            profile.token_ref,
            client_id=client_id,
            client_secret=client_secret,
            observed_scopes=existing.metadata.granted_scopes,
            requester=_oauth_http_request,
        )
    except Exception as exc:
        print(json.dumps({"ok": False, "state": type(exc).__name__, "network_performed": network_performed, "next_action": "No se modificó el token previo; revise conectividad y la autorización vigente.", "error": str(exc)}, ensure_ascii=False, indent=2))
        return 2
    finally:
        client_secret = ""
    print(json.dumps({"ok": True, "network_performed": network_performed, "token": metadata.to_dict(), "secrets": "REDACTED"}, ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_ctrader_select(args: argparse.Namespace) -> int:
    from .ctrader_commands import account_discovery_command, select_account_command

    config = _config_for(args)
    accounts_path = Path(args.accounts_file).expanduser() if args.accounts_file else _discovery_state_path(config)
    if not accounts_path.exists():
        print(json.dumps({"ok": False, "state": "ACCOUNT_DISCOVERY_REQUIRED", "next_action": "Ejecute query --network o proporcione --accounts-file con una respuesta observada; no se selecciona automáticamente."}, ensure_ascii=False, indent=2))
        return 2
    metadata = _token_metadata_for_config(config)
    if metadata is None:
        print(json.dumps({"ok": False, "state": "TOKEN_REFERENCE_REQUIRED", "network_performed": False, "next_action": "Active primero un token externo con scope accounts."}, ensure_ascii=False, indent=2))
        return 2
    raw = json.loads(accounts_path.read_text(encoding="utf-8"))
    accounts = (raw.get("accounts", raw.get("records")) if isinstance(raw, Mapping) else raw)
    if not isinstance(accounts, list):
        print(json.dumps({"ok": False, "state": "ACCOUNT_DISCOVERY_INVALID", "network_performed": False, "next_action": "El archivo debe contener una lista accounts observada."}, ensure_ascii=False, indent=2))
        return 2
    if isinstance(raw, Mapping) and raw.get("token_ref") not in {None, metadata.token_ref}:
        print(json.dumps({"ok": False, "state": "ACCOUNT_DISCOVERY_INVALID", "network_performed": False, "next_action": "El token_ref del descubrimiento no coincide con el perfil."}, ensure_ascii=False, indent=2))
        return 2
    observed_at = None
    if isinstance(raw, Mapping) and raw.get("observed_at"):
        observed_at = datetime.fromisoformat(str(raw["observed_at"]).replace("Z", "+00:00"))
    try:
        discovery = account_discovery_command(
            metadata,
            accounts,
            observed_at=observed_at,
            permission_scope=(raw.get("permissionScope", raw.get("permission_scope")) if isinstance(raw, Mapping) else None),
        )
    except Exception as exc:
        print(json.dumps({"ok": False, "state": "ACCOUNT_DISCOVERY_INVALID", "network_performed": False, "next_action": "La respuesta no contiene cuentas observadas válidas; repita discovery desde el servidor.", "error": type(exc).__name__}, ensure_ascii=False, indent=2))
        return 2
    output = select_account_command(
        _ctrader_activation_payload(config),
        discovery,
        account_id=args.account_id,
        environment="DEMO",
    )
    try:
        selection_path = _persist_selection(
            config,
            account_id=args.account_id,
            token_ref=metadata.token_ref,
            observed_at=discovery.observed_at,
        )
    except Exception as exc:
        print(json.dumps({"ok": False, "state": "SELECTION_PERSISTENCE_FAILED", "network_performed": False, "next_action": "No se modificó TOML; corrija el store externo y repita la selección.", "error": type(exc).__name__}, ensure_ascii=False, indent=2))
        return 2
    output["selection_persisted"] = True
    output["selection_path"] = str(selection_path)
    output["next_action"] = "Selección DEMO guardada fuera del repo; al conectar se volverá a validar contra el inventario del servidor."
    print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_ctrader_query(args: argparse.Namespace) -> int:
    """Connect, authenticate and discover accounts before any selection.

    Discovery is a server observation, not an activation proof.  The command
    deliberately authenticates the application and obtains the account list
    before it can authorize a selected account; an empty/unclassified list is
    reported as a gate rather than treated as DEMO.
    """

    config = _config_for(args)
    if getattr(args, "fixture", False):
        return cmd_ctrader_fixture(args)
    from .ctrader_activation import ActivationProfile, BrokerAccount, OAuthAppConfig, SecureTokenStore
    from .ctrader_commands import status_command

    metadata = _token_metadata_for_config(config)
    status_before = status_command(
        _activation_payload_with_selection(config),
        token_metadata=metadata,
        present_env_keys=set(os.environ),
        accounts=(),
        now=datetime.now(UTC),
    )
    if not getattr(args, "network", False):
        print(json.dumps({"ok": False, "state": status_before["status"]["state"], "network_performed": False, "activation": status_before, "next_action": "Use --fixture o --network sólo tras completar OAuth externo."}, ensure_ascii=False, indent=2, default=str))
        return 2
    activation_payload = _activation_payload_with_selection(config)
    profile = ActivationProfile.from_mapping(dict(activation_payload["ctrader"]))
    app = OAuthAppConfig.from_mapping(dict(config.ctrader_oauth))
    if profile.operation_mode.value != "QUERY" or metadata is None or metadata.is_expired(datetime.now(UTC)) or "accounts" not in metadata.granted_scopes:
        print(json.dumps({"ok": False, "state": status_before["status"]["state"], "network_performed": False, "activation": status_before, "next_action": "Complete un token accounts vigente antes de conectar."}, ensure_ascii=False, indent=2, default=str))
        return 2
    client_id = os.environ.get(app.client_id_env, "")
    client_secret = os.environ.get(app.client_secret_env, "")
    if not client_id or not client_secret:
        print(json.dumps({"ok": False, "state": "APP_CREDENTIALS_REQUIRED", "network_performed": False, "next_action": "Defina las referencias de aplicación en variables de entorno."}, ensure_ascii=False, indent=2))
        return 2
    store = SecureTokenStore(profile.token_store_dir, project_root=PROJECT_ROOT)
    lease = store.read(profile.token_ref)
    provider = None
    network_performed = False
    sequence: list[str] = []
    try:
        from ..data.ctrader import CTraderProvider

        provider = CTraderProvider(_ctrader_config(config))
        provider.connect()
        network_performed = True
        sequence.append("connect")

        def secret_provider(ref: str) -> str:
            if ref == app.client_secret_env:
                sequence.append("application_auth")
                return client_secret
            return ""

        def token_provider(ref: str) -> str:
            if ref == profile.token_ref:
                sequence.append("account_discovery")
                return lease.access_token
            return ""

        # Do not let a TOML account_id bypass the observed DEMO/environment
        # gate.  The client performs app auth and account discovery only.
        provider.authenticate(
            secret_provider=secret_provider,
            token_provider=token_provider,
            authorize_selected=False,
        )
        observed = provider.discover_accounts()
        raw_records = observed.get("records", ()) if isinstance(observed, Mapping) else ()
        accounts: list[BrokerAccount] = []
        for raw in raw_records:
            if not isinstance(raw, Mapping):
                continue
            account_id = raw.get("account_id", raw.get("ctidTraderAccountId"))
            if account_id is None:
                continue
            label = str(raw.get("brokerTitleShort", raw.get("label", "")))
            permissions = raw.get("permissions", raw.get("permission_scope", raw.get("permissionScope", ())))
            accounts.append(BrokerAccount(str(account_id), str(raw.get("environment", "UNKNOWN")), label, permissions))
        discovery_path = None
        try:
            discovery_path = _persist_discovery(
                config,
                token_ref=profile.token_ref,
                observed=observed,
                observed_at=datetime.now(UTC),
            )
        except Exception:
            # Query remains read-only if the optional local receipt cannot be
            # written; the server inventory is still returned and revalidated.
            discovery_path = None
        status_after_discovery = status_command(
            _activation_payload_with_selection(config),
            token_metadata=metadata,
            present_env_keys=set(os.environ),
            accounts=accounts,
            now=datetime.now(UTC),
        )
        if not status_after_discovery["status"].get("ready"):
            output = {
                "ok": False,
                "network_performed": True,
                "sequence": sequence,
                "state": status_after_discovery["status"]["state"],
                "accounts": [account.redacted() for account in accounts],
                "permission_scope": observed.get("permissionScope") if isinstance(observed, Mapping) else None,
                "discovery_path": str(discovery_path) if discovery_path else None,
                "activation": status_after_discovery,
                "status": provider.status.to_dict(),
                "next_action": status_after_discovery["status"]["next_action"],
            }
            print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
            return 2
        provider.authorize_account(
            int(profile.account_id),
            token_provider=lambda ref: lease.access_token if ref == profile.token_ref else "",
        )
        sequence.append("account_auth")
        catalog = provider.resolve_symbol()
        sequence.append("catalog")
        history = provider.fetch_history(
            "M1",
            count=int(config.ctrader.get("historical_count", 500)),
            max_pages=20,
        )
        sequence.append("history")
        output = {
            "ok": True,
            "network_performed": True,
            "sequence": sequence,
            "accounts": [account.redacted() for account in accounts],
            "permission_scope": observed.get("permissionScope") if isinstance(observed, Mapping) else None,
            "discovery_path": str(discovery_path) if discovery_path else None,
            "activation": status_after_discovery,
            "catalog": catalog.to_dict() if hasattr(catalog, "to_dict") else catalog,
            "history": history.to_dict() if hasattr(history, "to_dict") else history,
            "status": provider.status.to_dict(),
            "next_action": "Cuenta DEMO observada, catálogo e histórico obtenidos; las cotizaciones siguen siendo de consulta.",
        }
    except Exception as exc:
        provider_status = provider.status.to_dict() if provider is not None else {}
        action = provider_status.get("action") if isinstance(provider_status, Mapping) else None
        output = {
            "ok": False,
            "network_performed": network_performed,
            "sequence": sequence,
            "state": type(exc).__name__,
            "status": provider_status,
            "next_action": action or "Conecte, autentique la aplicación y descubra cuentas antes de seleccionar una cuenta DEMO.",
        }
        print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
        return 2
    finally:
        lease = None
        client_secret = ""
        if provider is not None:
            try:
                provider.close()
            except Exception:
                pass
    print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_ctrader_demo(args: argparse.Namespace) -> int:
    """Exercise demo executor only after an explicit local activation flag."""
    if not getattr(args, "activate", False):
        print(json.dumps({"ok": True, "enabled": False, "environment": "DEMO", "server_contacted": False, "next_action": "Use --activate only for the deterministic local fixture; no external account is activated."}, ensure_ascii=False, indent=2))
        return 0
    return cmd_ctrader_fixture(args)


def cmd_ctrader_fixture(args: argparse.Namespace) -> int:
    from .ctrader_demo import run_ctrader_fixture
    output = run_ctrader_fixture(args.report)
    print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
    return 0


def _load_ctrader_capture_input(path: Path) -> tuple[Mapping[str, Any], ...]:
    """Load a local JSON/JSONL capture without contacting a provider."""

    try:
        text = path.expanduser().read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"no se pudo leer captura cTrader: {path}") from exc
    if path.suffix.lower() in {".jsonl", ".ndjson"}:
        values = [json.loads(line) for line in text.splitlines() if line.strip()]
    else:
        parsed = json.loads(text)
        if isinstance(parsed, Mapping) and isinstance(parsed.get("payloads"), list):
            values = parsed["payloads"]
        elif isinstance(parsed, Mapping) and isinstance(parsed.get("capture"), Mapping) and isinstance(parsed["capture"].get("payloads"), list):
            values = parsed["capture"]["payloads"]
        elif isinstance(parsed, list):
            values = parsed
        else:
            raise ConfigError("captura JSON debe ser una lista o contener payloads[]")
    if not isinstance(values, list) or not values:
        raise ConfigError("captura cTrader vacía")
    records: list[Mapping[str, Any]] = []
    for index, item in enumerate(values):
        if not isinstance(item, Mapping):
            raise ConfigError(f"payload cTrader {index} no es objeto")
        records.append(dict(item))
    return tuple(records)


def cmd_cfd_paper(args: argparse.Namespace) -> int:
    """Run the real cTrader-normalization/runtime/CFD-paper path offline."""

    from .ctrader_pipeline import CTraderPipeline, synthetic_ctrader_capture
    from ..data.ctrader import CTraderInstrumentSpec
    from .persistence import SQLiteStore

    config = _config_for(args)
    if getattr(args, "input", None):
        capture_input = _load_ctrader_capture_input(Path(args.input))
    else:
        capture_input = synthetic_ctrader_capture(
            start=datetime(2026, 1, 1, tzinfo=UTC),
            symbol_id=int(config.ctrader.get("symbol_id", 99)) if config.ctrader.get("symbol_id") is not None else 99,
            count=int(getattr(args, "count", 190)),
            mode="REPLAY",
        )
    symbol_id = config.ctrader.get("symbol_id") if config.ctrader.get("symbol_id") is not None else 99
    spec = CTraderInstrumentSpec(
        symbol=config.instrument,
        symbol_id=int(symbol_id),
        digits=int(config.ctrader.get("digits", 5)),
        pip_position=int(config.ctrader.get("pip_position", 4)),
        price_scale=int(config.ctrader.get("price_scale", 100_000)),
    )
    db = _db_for(args, config)
    db.parent.mkdir(parents=True, exist_ok=True)
    with SQLiteStore(db) as store:
        pipeline = CTraderPipeline(store, config, spec=spec, mode="REPLAY", max_candles=getattr(args, "max_candles", 5000))
        result = pipeline.run(
            capture_input,
            session_id=getattr(args, "session", None),
            capture_complete=not bool(getattr(args, "incomplete", False)),
            quote_basis=("traded" if config.price_base in {"close", "traded"} else config.price_base),
        )
    summary: dict[str, Any] = {
        "ok": True,
        "product": "FOREX_CFD_LOCAL_PAPER",
        "provider": "ctrader-open-api",
        "network_performed": False,
        "credentials_used": False,
        "db": str(db),
        "session_id": result.session_id,
        "runtime_analysis_id": result.runtime_analysis_id,
        "paper_analysis_id": result.paper_analysis_id,
        "analysis_basis": result.analysis_basis,
        "capture": result.capture.to_dict(),
        "signals": len(result.signals),
        "trades": [trade.to_dict() for trade in result.trades],
        "paper_capture_complete": result.paper.capture_complete,
        "snapshot_hash": result.snapshot_hash,
        "next_action": "Revise la sesión en UI/reporte; las señales y fills son paper locales, no órdenes DEMO.",
    }
    if getattr(args, "report", None):
        target = Path(args.report).expanduser()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(result.to_dict(), ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
        summary["report_path"] = str(target)
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    return 0


def cmd_ui(args: argparse.Namespace) -> int:
    config = _config_for(args)
    db = _db_for(args, config)
    print(f"UI local: http://{args.host}:{args.port}/ (sólo lectura)", flush=True)
    serve(db, host=args.host, port=args.port, session_id=args.session, duration=args.duration)
    return 0


def _add_input_options(parser: argparse.ArgumentParser, *, optional: bool = False) -> None:
    if optional:
        parser.add_argument("--input", type=Path)
    else:
        parser.add_argument("input", type=Path)
    parser.add_argument("--format", choices=["csv", "jsonl", "json"])
    parser.add_argument("--instrument")
    parser.add_argument("--timeframe")
    parser.add_argument("--price-base", choices=["trade", "traded", "close", "mid", "bid", "ask"])
    parser.add_argument("--mapping", help="timestamp=ts,open=o,high=h,low=l,close=c,volume=v")
    parser.add_argument("--timezone", help="zona para timestamps sin offset")
    parser.add_argument("--timestamp-unit", choices=["iso8601", "s", "ms", "us", "ns"], default="iso8601", help="unidad explícita para timestamps numéricos")
    parser.add_argument("--interval-seconds", type=float)
    parser.add_argument("--allow-issues", action="store_true")
    parser.add_argument("--allow-out-of-order", action="store_true")
    parser.add_argument("--allow-duplicates", action="store_true")
    parser.add_argument("--db", type=Path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mtf-lab", description="MTF Lab — investigación cuantitativa local, virtual y reproducible")
    subs = parser.add_subparsers(dest="command", required=True)
    p = subs.add_parser("doctor", help="revisa Python, configuración, SQLite y opcionalmente conectividad")
    p.add_argument("--db", type=Path); p.add_argument("--config", type=Path); p.add_argument("--connectivity", "--check-network", dest="connectivity", action="store_true"); p.add_argument("--timeout", type=float, default=5); p.set_defaults(func=cmd_doctor)
    p = subs.add_parser("demo", help="recorrido offline sintético reproducible")
    p.add_argument("--db", type=Path); p.add_argument("--seed", type=int, default=42); p.add_argument("--minutes", type=int, default=720); p.add_argument("--config", type=Path); p.add_argument("--report", type=Path); p.add_argument("--log", type=Path); p.set_defaults(func=cmd_demo)
    p = subs.add_parser("import", help="importa CSV/JSONL local con validación explícita"); _add_input_options(p); p.add_argument("--config", type=Path); p.set_defaults(func=cmd_import)
    p = subs.add_parser("replay", help="reproduce una fuente local con agregación/indicadores incrementales"); _add_input_options(p, optional=True); p.add_argument("--session"); p.add_argument("--config", type=Path); p.add_argument("--checkpoint", default="runtime"); p.add_argument("--checkpoint-every", type=int, default=100); p.add_argument("--max-candles", type=int, default=5000); p.add_argument("--partition", choices=["all", "exploration", "evaluation"], default="all"); p.add_argument("--preserve-order", action="store_true"); p.add_argument("--no-resume", dest="resume", action="store_false"); p.set_defaults(resume=True, func=cmd_replay)
    p = subs.add_parser("backtest", help="compara referencia M1 y estrategia MTF con simulación virtual"); p.add_argument("--session"); p.add_argument("--db", type=Path); p.add_argument("--config", type=Path); p.add_argument("--timeframe"); p.add_argument("--format", choices=["markdown", "json", "html"], default="markdown"); p.add_argument("--report", type=Path); p.add_argument("--partition", choices=["all", "exploration", "evaluation"], default="all"); p.add_argument("--boundary", help="timestamp UTC de frontera cronológica"); p.set_defaults(func=cmd_backtest)
    p = subs.add_parser("report", help="genera o imprime informe de una sesión"); p.add_argument("--session"); p.add_argument("--latest", action="store_true", help="selecciona la sesión más reciente"); p.add_argument("--db", type=Path); p.add_argument("--config", type=Path); p.add_argument("--format", choices=["markdown", "json", "html"], default="markdown"); p.add_argument("--output", type=Path); p.set_defaults(func=cmd_report)
    p = subs.add_parser("watch", help="observación pública acotada/continua; no ejecuta órdenes"); p.add_argument("--db", type=Path); p.add_argument("--config", type=Path); p.add_argument("--instrument"); p.add_argument("--session"); p.add_argument("--duration", type=float); p.add_argument("--max-events", type=int); p.add_argument("--no-snapshot", action="store_true"); p.add_argument("--offline-demo", action="store_true"); p.add_argument("--seed", type=int, default=42); p.add_argument("--report", type=Path); p.add_argument("--log", type=Path); p.add_argument("--checkpoint", default="runtime"); p.add_argument("--checkpoint-every", type=int, default=100); p.add_argument("--max-candles", type=int, default=5000); p.add_argument("--no-resume", dest="resume", action="store_false"); p.set_defaults(resume=True, func=cmd_watch)
    p = subs.add_parser("ui", help="sirve interfaz local de sólo lectura"); p.add_argument("--db", type=Path); p.add_argument("--config", type=Path); p.add_argument("--session"); p.add_argument("--host", default="127.0.0.1"); p.add_argument("--port", type=int, default=8765); p.add_argument("--duration", type=float); p.set_defaults(func=cmd_ui)
    p = subs.add_parser("ctrader", help="consulta cTrader Open API; no envía operaciones por defecto")
    csubs = p.add_subparsers(dest="ctrader_command", required=True)
    q = csubs.add_parser("doctor", help="diagnostica SDK opcional, OAuth y gates de cuenta"); q.add_argument("--config", type=Path); q.add_argument("--network", "--check-network", dest="network", action="store_true", help="prueba explícitamente DNS/TCP/TLS DEMO; no autentica cuentas"); q.add_argument("--timeout", type=float, default=5.0); q.set_defaults(func=cmd_ctrader_doctor)
    q = csubs.add_parser("auth-url", help="inicia OAuth loopback reanudable; no abre navegador salvo flag"); q.add_argument("--config", type=Path, default=PROJECT_ROOT / "config" / "ctrader_query.toml"); q.add_argument("--scope", choices=["accounts", "trading"], help="un solo scope por intento; trading va después de accounts"); q.add_argument("--open-browser", action="store_true"); q.set_defaults(func=cmd_ctrader_auth_url)
    q = csubs.add_parser("callback-listen", help="recibe un callback en el loopback registrado; no imprime el código"); q.add_argument("--config", type=Path, default=PROJECT_ROOT / "config" / "ctrader_query.toml"); q.add_argument("--attempt-id", required=True); q.add_argument("--open-browser", action="store_true"); q.add_argument("--timeout", type=float, default=600.0); q.set_defaults(func=cmd_ctrader_callback_listen)
    q = csubs.add_parser("select", help="selecciona una cuenta DEMO desde un descubrimiento local observado"); q.add_argument("--config", type=Path, default=PROJECT_ROOT / "config" / "ctrader_query.toml"); q.add_argument("--accounts-file", type=Path); q.add_argument("--account-id", required=True); q.set_defaults(func=cmd_ctrader_select)
    q = csubs.add_parser("token-exchange", help="reanuda OAuth; entregue callback por archivo 0600/stdin, fixture usa store temporal"); q.add_argument("--config", type=Path, default=PROJECT_ROOT / "config" / "ctrader_query.toml"); q.add_argument("--attempt-id"); q.add_argument("--callback-file", type=Path); q.add_argument("--callback-stdin", action="store_true"); q.add_argument("--callback-uri", help=argparse.SUPPRESS); q.add_argument("--fixture", action="store_true"); q.set_defaults(func=cmd_ctrader_token_exchange)
    q = csubs.add_parser("token-refresh", help="rota token externo; fixture usa store temporal aislado"); q.add_argument("--config", type=Path, default=PROJECT_ROOT / "config" / "ctrader_query.toml"); q.add_argument("--fixture", action="store_true"); q.set_defaults(func=cmd_ctrader_token_refresh)
    q = csubs.add_parser("query", help="consulta cTrader tras autorización explícita o usa fixture offline"); q.add_argument("--config", type=Path, default=PROJECT_ROOT / "config" / "ctrader_query.toml"); q.add_argument("--fixture", action="store_true"); q.add_argument("--network", action="store_true"); q.add_argument("--report", type=Path); q.set_defaults(func=cmd_ctrader_query)
    q = csubs.add_parser("demo", help="ejecutor DEMO local: requiere --activate explícito y nunca usa servidor"); q.add_argument("--activate", action="store_true"); q.add_argument("--report", type=Path); q.set_defaults(func=cmd_ctrader_demo)
    q = csubs.add_parser("fixture", help="ejecuta fixture offline de consulta, CFD y ejecutor demo"); q.add_argument("--report", type=Path); q.set_defaults(func=cmd_ctrader_fixture)
    p = subs.add_parser("cfd-paper", help="pipeline cTrader→RuntimeCoordinator→CFD PAPER local"); p.add_argument("--config", type=Path, default=PROJECT_ROOT / "config" / "ctrader_pipeline_fixture.toml"); p.add_argument("--input", type=Path, help="captura JSON/JSONL local de payloads cTrader; sin red"); p.add_argument("--count", type=int, default=190, help="barras del fixture sintético cuando no se proporciona --input"); p.add_argument("--session"); p.add_argument("--db", type=Path); p.add_argument("--max-candles", type=int, default=5000); p.add_argument("--incomplete", action="store_true", help="conserva simulaciones futuras como PENDING"); p.add_argument("--report", type=Path); p.set_defaults(func=cmd_cfd_paper)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.func(args))
    except BrokenPipeError:
        return 0
    except Exception as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
