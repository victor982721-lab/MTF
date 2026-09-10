"""Command line entry points for the local MTF Lab operational layer."""

from __future__ import annotations

import argparse
import importlib
import json
import os
import socket
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from .. import __version__
from .backtest import BacktestRunner, VariantSpec
from .demo import run_demo
from .importer import ColumnMapping, ImportValidationError, LocalImporter
from .logging_state import JsonlLogger, OperationTelemetry
from .persistence import SQLiteStore
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


def _load_toml(path: str | Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    import tomllib

    target = Path(path).expanduser()
    with target.open("rb") as stream:
        value = tomllib.load(stream)
    if not isinstance(value, dict):
        raise ValueError("la configuración TOML debe ser una tabla")
    return value


def _validate_config(config: dict[str, Any]) -> list[str]:
    """Validate project TOML strictly enough to catch spelling mistakes."""
    allowed_sections = {"project", "storage", "data", "instrument", "timeframes", "indicators", "strategy", "quality", "simulation", "provider", "ui"}
    allowed: dict[str, set[str]] = {
        "project": {"name", "version", "mode"},
        "storage": {"db", "logs"},
        "data": {"provider", "instrument", "resolution", "source", "path", "price_base", "mode"},
        "instrument": {"symbol", "price_base"},
        "timeframes": {"base", "values", "closed_only"},
        "indicators": {"ema_fast", "ema_slow", "rsi_period", "atr_period", "wilder"},
        "strategy": {"name", "context_timeframe", "preparation_timeframe", "trigger_timeframe", "setup_timeframe", "context_lookback", "preparation_lookback", "lookback", "max_distance_atr", "setup_max_atr", "rsi_threshold", "preparation_ttl_bars", "preparation_ttl_minutes", "require_closed", "optional_filters", "indicators", "timeframes", "lookbacks", "conditions", "mode", "one_signal_per_episode"},
        "quality": {"max_feed_age_seconds", "max_closed_candle_age_seconds", "max_gap_minutes", "require_warmup"},
        "simulation": {"horizons_seconds", "horizons_minutes", "entry_latency_seconds", "stake", "max_price_age_seconds", "payout_net", "net_payout", "loss_amount", "tie_net", "tie_return", "costs", "tie_tolerance", "requested_base_price", "require_closed", "entry_rule", "exit_rule"},
        "provider": {"name", "rest_url", "websocket_url", "rest_max_records", "exclude_last_uncommitted", "trade_channel", "ohlc_channel"},
        "ui": {"host", "port"},
    }
    errors: list[str] = []
    errors.extend(f"clave desconocida: {x}" for x in sorted(set(config) - allowed_sections))
    for section, value in config.items():
        if section in allowed and isinstance(value, dict):
            errors.extend(f"{section}.clave desconocida: {x}" for x in sorted(set(value) - allowed[section]))
        elif section in allowed and section not in {"timeframes"} and not isinstance(value, dict):
            errors.append(f"{section} debe ser tabla")
    if isinstance(config.get("timeframes"), (dict, list)) is False and "timeframes" in config:
        errors.append("timeframes debe ser tabla o lista")
    if isinstance(config.get("strategy"), dict):
        tf = config["strategy"].get("timeframes")
        if tf is not None and not isinstance(tf, (dict, list)):
            errors.append("strategy.timeframes debe ser tabla o lista")
    return errors


def _sim_spec(config: dict[str, Any] | None = None) -> EvaluationSpec:
    """Translate the human-friendly TOML aliases to EvaluationSpec."""
    raw = dict((config or {}).get("simulation", {}))
    if "horizons_seconds" not in raw and "horizons_minutes" in raw:
        raw["horizons_seconds"] = tuple(float(x) * 60.0 for x in raw.pop("horizons_minutes"))
    else:
        raw.pop("horizons_minutes", None)
    if "payout_net" not in raw and "net_payout" in raw:
        raw["payout_net"] = raw.pop("net_payout")
    else:
        raw.pop("net_payout", None)
    if "tie_net" not in raw and "tie_return" in raw:
        raw["tie_net"] = raw.pop("tie_return")
    else:
        raw.pop("tie_return", None)
    # entry/exit labels document assumptions but are not constructor fields.
    raw.pop("entry_rule", None); raw.pop("exit_rule", None)
    allowed = {"horizons_seconds", "entry_latency_seconds", "stake", "max_price_age_seconds", "payout_net", "loss_amount", "tie_net", "costs", "tie_tolerance", "requested_base_price", "require_closed", "horizon_from"}
    unknown = set(raw) - allowed
    if unknown:
        raise ValueError(f"simulation.clave desconocida: {sorted(unknown)}")
    return EvaluationSpec(**raw)

def cmd_doctor(args: argparse.Namespace) -> int:
    print("MTF Lab doctor")
    print(f"python: {sys.version.split()[0]} ({sys.executable})")
    print(f"sqlite: {__import__('sqlite3').sqlite_version}")
    print(f"proyecto: {PROJECT_ROOT}")
    # Comprobación de dependencias efectivamente importables, sin instalar ni
    # tocar el entorno. Las opcionales sólo se reportan como tales.
    import importlib.util
    errors: list[str] = []
    required_modules = {"tomllib": "stdlib", "sqlite3": "stdlib", "mtf_lab.core": "núcleo", "mtf_lab.data": "datos", "mtf_lab.ops": "operaciones"}
    optional_modules = {"websockets": "Kraken WebSocket", "requests": "HTTP opcional", "rich": "salida enriquecida"}
    for module_name, label in required_modules.items():
        available = importlib.util.find_spec(module_name) is not None
        print(f"dependencia {label}: {'OK' if available else 'FALTA'} ({module_name})")
        if not available:
            errors.append(f"dependencia requerida ausente: {module_name}")
    for module_name, label in optional_modules.items():
        print(f"opcional {label}: {'OK' if importlib.util.find_spec(module_name) is not None else 'omitido'} ({module_name})")
    if sys.version_info < (3, 11):
        errors.append("Python >= 3.11 requerido")
    try:
        config_path = args.config or (default_config() if default_config().exists() else None)
        config = _load_toml(config_path)
        errors.extend(_validate_config(config))
        print(f"configuración: {'OK' if not errors else 'con errores'}")
    except Exception as exc:
        errors.append(f"configuración: {exc}")
    db = Path(args.db or default_db()).expanduser()
    try:
        db.parent.mkdir(parents=True, exist_ok=True)
        with SQLiteStore(db) as store:
            print(f"storage: OK {db} schema={store.schema_version}")
    except Exception as exc:
        errors.append(f"storage: {exc}")
    if args.connectivity:
        url = "https://api.kraken.com/0/public/Time"
        try:
            with urllib.request.urlopen(url, timeout=float(args.timeout)) as response:
                body = response.read(4096).decode("utf-8", "replace")
            print(f"conectividad Kraken REST: OK ({response.status}) cuerpo={body[:160]}")
        except Exception as exc:
            print(f"conectividad Kraken REST: ERROR {type(exc).__name__}: {exc}")
            errors.append("conectividad opcional no disponible")
    else:
        print("conectividad: omitida (use --connectivity explícitamente)")
    if errors:
        for error in errors: print(f"ERROR: {error}")
        return 2
    return 0


def cmd_demo(args: argparse.Namespace) -> int:
    db = Path(args.db or default_db()).expanduser()
    config_path = getattr(args, "config", None) or (default_config() if default_config().exists() else None)
    config = _load_toml(config_path) if config_path else None
    config_errors = _validate_config(config or {})
    if config_errors:
        raise ValueError("configuración inválida: " + "; ".join(config_errors))
    result = run_demo(db, seed=int(args.seed), minutes=int(args.minutes), report_path=args.report, log_path=args.log, config=config)
    print(json.dumps({"ok": True, **{key: result[key] for key in ("session_id", "db_path", "report_path", "log_path", "dataset", "results")}}, ensure_ascii=False, indent=2, default=str))
    return 0


def _build_importer(args: argparse.Namespace) -> LocalImporter:
    interval = float(args.interval_seconds) if args.interval_seconds is not None else None
    mapping = ColumnMapping.from_text(args.mapping)
    return LocalImporter(instrument=args.instrument, timeframe=args.timeframe, price_base=args.price_base, mapping=mapping, timezone=args.timezone, interval_seconds=interval, strict=not args.allow_issues, allow_out_of_order=args.allow_out_of_order, allow_duplicates=args.allow_duplicates)


def _persist_import(db: Path, imported: Any, *, mode: str = "REPLAY", provider: str = "local-file", log_path: Path | None = None, session_id: str | None = None) -> tuple[str, dict[str, Any]]:
    with SQLiteStore(db) as store:
        sid = store.create_session(session_id=session_id, mode=mode, provider=provider, instrument=imported.instrument, code_version=__version__, dataset_ref=imported.source_path, config={"timeframe": imported.timeframe, "price_base": imported.price_base, "import": imported.to_dict()}, metadata={"import": imported.to_dict()})
        for ordinal, record in enumerate(imported.records):
            if "open" in record:
                store.save_candle(sid, record, ordinal=ordinal)
            else:
                store.save_event(sid, record, ordinal=ordinal)
        store.save_checkpoint(sid, "import", cursor={"source_path": imported.source_path, "last_row": len(imported.records)}, events_processed=len(imported.records), last_event_id=None, state={"coverage_start": imported.coverage_start, "coverage_end": imported.coverage_end})
        store.finish_session(sid, status="COMPLETED")
        return sid, {"session_id": sid, "records": len(imported.records), "issues": [x.to_dict() for x in imported.issues], "coverage_start": imported.coverage_start, "coverage_end": imported.coverage_end, "db": str(db), "mode": mode, "provider": provider}


def cmd_import(args: argparse.Namespace) -> int:
    try:
        imported = _build_importer(args).read(args.input, fmt=args.format)
        sid, result = _persist_import(Path(args.db or default_db()).expanduser(), imported)
    except ImportValidationError as exc:
        print(json.dumps({"ok": False, "error": str(exc), "issues": [x.to_dict() for x in exc.issues]}, ensure_ascii=False, indent=2), file=sys.stderr)
        return 2
    print(json.dumps({"ok": True, **result}, ensure_ascii=False, indent=2))
    return 0


def _discover_engine(config: dict[str, Any] | None = None) -> Any | None:
    """Discover the provider-neutral core strategy without duplicating rules."""
    config = config or {}
    strategy_config = config.get("strategy", config)
    candidates = ["mtf_lab.core.engine", "mtf_lab.core.strategy", "mtf_lab.strategy", "mtf_lab.core"]
    for module_name in candidates:
        try:
            module = importlib.import_module(module_name)
        except ImportError:
            continue
        for name in ("create_engine", "Engine", "StrategyEngine", "TrendPullbackStrategy"):
            factory = getattr(module, name, None)
            if factory is None:
                continue
            try:
                return factory(strategy_config) if callable(factory) else factory
            except TypeError:
                try:
                    return factory()
                except Exception:
                    continue
    return None


def _core_candle(row: dict[str, Any], *, mode: str = "REPLAY") -> Any:
    """Convert a stored row back to the canonical core Candle model."""
    from datetime import datetime
    from mtf_lab.core.models import Candle, DataQuality, OperationMode, PriceBase

    def dt(value: Any) -> datetime:
        text = str(value).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
        return parsed

    quality = DataQuality.good(synthetic=mode.upper() == "SYNTHETIC")
    declared_base = str(row.get("price_base", "traded")).lower()
    # Una vela con columna ``close`` representa el cierre de la base declarada
    # por el importador; el modelo Candle usa ``traded`` como alias numérico
    # para el cierre, sin convertir bid/ask/mid entre sí.
    core_base = declared_base if declared_base in {"traded", "bid", "ask", "mid"} else "traded"
    return Candle(
        instrument=str(row["instrument"]),
        timeframe=str(row["timeframe"]),
        start=dt(row["start_ts"]),
        end=dt(row["end_ts"]),
        open=float(row["open"]),
        high=float(row["high"]),
        low=float(row["low"]),
        close=float(row["close"]),
        volume=float(row.get("volume") or 0.0),
        event_count=int(row.get("event_count") or 0),
        source=str(row.get("source") or "persisted"),
        mode=OperationMode.SYNTHETIC if mode.upper() == "SYNTHETIC" else OperationMode.REPLAY,
        price_base=PriceBase(core_base),
        closed=bool(row.get("closed", True)),
        available_at=dt(row["available_ts"]) if row.get("available_ts") else None,
        received_at=dt(row["available_ts"]) if row.get("available_ts") else None,
        quality=quality,
        candle_id=str(row.get("candle_id") or row.get("source_record_id") or ""),
        origin="persisted",
        metadata={"persisted": True, "quality": row.get("quality", "UNKNOWN"), "declared_price_base": declared_base},
    )


def _persist_core_result(store: SQLiteStore, sid: str, strategy_result: Any) -> tuple[list[Any], int]:
    signals = list(getattr(strategy_result, "signals", ()))
    evaluations = list(getattr(strategy_result, "evaluations", ()))
    for ordinal, evaluation in enumerate(evaluations):
        data = evaluation.as_dict() if hasattr(evaluation, "as_dict") else dict(evaluation)
        observed = data.get("timestamp") or data.get("available_at")
        stage = str(data.get("stage", "evaluation"))
        stable = f"decision:{stage}:{observed}"
        data["decision_id"] = stable
        store.save_decision(sid, data, ordinal=ordinal)
        decision = str(data.get("decision", data.get("status", ""))).lower()
        if decision in {"discarded", "blocked"}:
            reasons = data.get("reasons") or ["mandatory_condition_not_met"]
            reason = str(reasons[0])
            store.save_discard(
                sid,
                {
                    "discard_id": f"{sid}:discard:{stage}:{observed}:{reason}",
                    "decision_id": stable,
                    "observed_ts": observed,
                    "reason_code": reason,
                    "required": True,
                    "condition_status": decision.upper(),
                    "payload": data,
                },
                ordinal=ordinal,
            )
    for ordinal, signal in enumerate(signals):
        data = signal.as_dict() if hasattr(signal, "as_dict") else dict(signal)
        store.save_signal(sid, data, ordinal=ordinal)
    return signals, len(evaluations)


def _evaluate_core_strategy(store: SQLiteStore, sid: str, engine: Any, *, mode: str = "REPLAY") -> tuple[list[Any], int]:
    streams: dict[str, list[Any]] = {}
    for timeframe in ("M1", "M5", "M15"):
        rows = store.list_candles(sid, timeframe=timeframe, closed_only=False)
        streams[timeframe] = [_core_candle(row, mode=mode) for row in rows]
    if not callable(getattr(engine, "evaluate", None)):
        raise TypeError("core strategy must expose evaluate(streams)")
    result = engine.evaluate(streams)
    return _persist_core_result(store, sid, result)


def cmd_replay(args: argparse.Namespace) -> int:
    db = Path(args.db or default_db()).expanduser()
    config_path = args.config or (default_config() if default_config().exists() else None)
    config = _load_toml(config_path)
    config_errors = _validate_config(config)
    if config_errors:
        raise ValueError("configuración inválida: " + "; ".join(config_errors))
    if args.input:
        try:
            imported = _build_importer(args).read(args.input, fmt=args.format)
            sid, result = _persist_import(db, imported, mode="REPLAY", provider="local-file")
        except ImportValidationError as exc:
            print(json.dumps({"ok": False, "error": str(exc), "issues": [x.to_dict() for x in exc.issues]}, ensure_ascii=False, indent=2), file=sys.stderr); return 2
    else:
        with SQLiteStore(db) as store:
            sessions = store.sessions(limit=100)
            if not sessions:
                print("No hay sesión para replay; indique --input.", file=sys.stderr); return 2
            sid = args.session or sessions[0]["session_id"]; result = {"session_id": sid, "reused": True}
    with SQLiteStore(db) as store:
        events = store.list_events(sid)
        points = store.list_candles(sid, timeframe=args.timeframe, closed_only=True)
        engine = _discover_engine(config)
        if engine is None:
            print(json.dumps({"ok": True, "mode": "REPLAY", **result, "events": len(events), "candles": len(points), "signals": 0, "message": "núcleo de estrategia no disponible; no se inventaron señales"}, ensure_ascii=False, indent=2)); return 0
        signals_raw, evaluations_count = _evaluate_core_strategy(store, sid, engine, mode="REPLAY")
        signals_count = len(signals_raw) if not isinstance(signals_raw, int) else signals_raw
        sim = VirtualContractSimulator(spec=_sim_spec(config))
        runner = BacktestRunner(simulator=sim, store=store, session_id=sid)
        results = runner.run(store.list_signals(sid), points, variants=[VariantSpec("trend_pullback_v1", "Estrategia del núcleo")])
        print(json.dumps({"ok": True, "mode": "REPLAY", **result, "events": len(events), "candles": len(points), "signals": signals_count, "evaluations": evaluations_count, "results": [x.to_dict(include_simulations=False) for x in results]}, ensure_ascii=False, indent=2, default=str)); return 0


def cmd_backtest(args: argparse.Namespace) -> int:
    db = Path(args.db or default_db()).expanduser()
    with SQLiteStore(db) as store:
        sessions = store.sessions(limit=100)
        sid = args.session or (sessions[0]["session_id"] if sessions else None)
        if not sid: print("No hay sesiones persistidas.", file=sys.stderr); return 2
        signals = store.list_signals(sid)
        candles = store.list_candles(sid, timeframe=args.timeframe, closed_only=True)
        if not signals:
            print(json.dumps({"ok": True, "session_id": sid, "message": "No hay señales persistidas; no se fabricaron señales.", "signals": 0}, ensure_ascii=False, indent=2)); return 0
        cfg_path = args.config or (default_config() if default_config().exists() else None)
        cfg = _load_toml(cfg_path)
        config_errors = _validate_config(cfg)
        if config_errors:
            raise ValueError("configuración inválida: " + "; ".join(config_errors))
        sim = VirtualContractSimulator(_sim_spec(cfg))
        runner = BacktestRunner(simulator=sim, store=store, session_id=sid)
        variants = [
            VariantSpec("m1_trigger_reference", "Referencia M1; misma muestra y simulación", {"context": False, "preparation": False}, mode="M1_REFERENCE"),
            VariantSpec("trend_pullback_v1", "Contexto M15 + preparación M5 + disparador M1", {"context": True, "preparation": True}, mode="MULTITIMEFRAME"),
        ]
        results = runner.run(signals, candles, variants=variants, data_quality="PERSISTED", resolution=args.timeframe)
        # Las simulaciones ya quedaron persistidas por BacktestRunner; no se
        # agregan de nuevo al resumen.
        report = ReportBuilder(store, sid).summary()
        target = Path(args.report or db.with_name(f"{db.stem}-backtest.md")); ReportBuilder(store, sid).write(target, format=args.format, data=report)
        print(json.dumps({"ok": True, "session_id": sid, "report": str(target), "results": [x.to_dict(include_simulations=False) for x in results]}, ensure_ascii=False, indent=2, default=str)); return 0


def cmd_report(args: argparse.Namespace) -> int:
    db = Path(args.db or default_db()).expanduser()
    with SQLiteStore(db, read_only=True) as store:
        sessions = store.sessions(limit=100); sid = args.session or (sessions[0]["session_id"] if sessions else None)
        if not sid: print("No hay sesiones persistidas.", file=sys.stderr); return 2
        builder = ReportBuilder(store, sid); data = builder.summary()
        if args.output:
            target = builder.write(args.output, format=args.format, data=data); print(str(target))
        elif args.format == "json": print(builder.to_json(data))
        elif args.format == "html": print(builder.to_html(data))
        else: print(builder.to_markdown(data))
    return 0



def _kraken_bar_row(bar: Any, provenance: Any) -> dict[str, Any]:
    """Map a Kraken Bar to the storage candle contract explicitly."""
    raw = bar.to_dict() if hasattr(bar, "to_dict") else dict(bar)
    start = raw.get("interval_start", raw.get("start_ts", raw.get("start")));
    end = raw.get("interval_end", raw.get("end_ts", raw.get("end")))
    timeframe = raw.get("resolution", raw.get("timeframe", raw.get("resolution_seconds")))
    data_id = raw.get("data_id", raw.get("candle_id", raw.get("source_record_id")))
    return {
        "candle_id": data_id,
        "instrument": raw.get("instrument", "BTC/USD"),
        "timeframe": timeframe,
        "start_ts": start, "end_ts": end,
        "available_ts": raw.get("available_at", raw.get("received_at")),
        "open": raw.get("open"), "high": raw.get("high"), "low": raw.get("low"), "close": raw.get("close"),
        "volume": raw.get("volume"), "closed": bool(raw.get("closed", True)),
        "source": raw.get("source", "kraken"), "price_base": raw.get("price_basis", "traded"),
        "quality": "PUBLIC_PROVIDER_CLOSED" if raw.get("closed", True) else "PUBLIC_PROVIDER_OPEN",
        "revision": raw.get("revision", 0),
        "provenance": provenance.to_dict() if hasattr(provenance, "to_dict") else provenance,
    }

def cmd_watch(args: argparse.Namespace) -> int:
    # A watch without a provider is intentionally not replaced by synthetic
    # data.  Only the public, unauthenticated Kraken adapter is discovered.
    if args.offline_demo:
        return cmd_demo(argparse.Namespace(db=args.db, seed=args.seed, minutes=max(60, int(args.duration or 60)), config=None, report=args.report, log=args.log))
    try:
        module = importlib.import_module("mtf_lab.data.kraken")
    except ImportError as exc:
        print(f"No se encontró adaptador público Kraken: {exc}. Use --offline-demo para una prueba sintética explícita.", file=sys.stderr); return 2
    factory = getattr(module, "KrakenAdapter", None) or getattr(module, "KrakenPublicAdapter", None)
    if factory is None:
        print("El adaptador Kraken no expone KrakenAdapter/KrakenPublicAdapter.", file=sys.stderr); return 2
    db = Path(args.db or default_db()).expanduser()
    try:
        adapter = factory(instrument=args.instrument) if getattr(factory, "__name__", "") == "KrakenAdapter" else factory(pair=args.instrument)
        with SQLiteStore(db) as store:
            sid = store.create_session(mode="LIVE", provider="kraken-public-rest+websocket-v2", instrument=getattr(adapter, "pair", args.instrument), code_version=__version__, config={"channels": ["trade", "ohlc"], "rest_intervals": [1, 5, 15], "snapshot": not args.no_snapshot, "rest_max_records": 720, "exclude_last_uncommitted": True}, metadata={"public_only": True, "synthetic": False, "ws_endpoint": getattr(adapter, "websocket_endpoint", None), "rest_endpoint": getattr(adapter, "rest_endpoint", None)})
            log_path = Path(args.log or default_logs(db))
            telemetry = OperationTelemetry(log_path=log_path, mode="LIVE", session_id=sid, instrument=getattr(adapter, "pair", args.instrument))
            telemetry.state.update(connection="CONNECTING", data_quality="PUBLIC_PROVIDER", continuity="UNKNOWN", warmup_pending={"M1": "pending", "M5": "pending", "M15": "pending"})
            # REST es el arranque y la recuperación explícitos. La última fila
            # abierta queda separada; nunca se usa como vela cerrada.
            rest_records = 0
            rest_errors: list[dict[str, str]] = []
            rest_counts: dict[str, int] = {}
            for interval in (1, 5, 15):
                try:
                    fetched = adapter.fetch_ohlc(interval=interval, include_open=False)
                    rows = list(fetched.bars)
                    if fetched.open_bar is not None:
                        rows.append(fetched.open_bar)
                    inserted_interval = 0
                    for ordinal, bar in enumerate(rows):
                        inserted_interval += int(store.save_candle(sid, _kraken_bar_row(bar, fetched.provenance), ordinal=interval * 1000 + ordinal))
                    rest_counts[f"M{interval}"] = len(fetched.bars)
                    rest_records += inserted_interval
                    telemetry.state.merge_nested("coverage", {f"M{interval}": {"closed": len(fetched.bars), "open": int(fetched.open_bar is not None), "last": str(fetched.last_timestamp) if fetched.last_timestamp else None}})
                except Exception as exc:
                    detail = {"interval": f"M{interval}", "type": type(exc).__name__, "error": str(exc)}
                    rest_errors.append(detail)
                    telemetry.logger.warning("rest_warmup_error", **detail)
            telemetry.state.update(warmup_pending={f"M{i}": (0 if f"M{i}" in rest_counts and rest_counts[f"M{i}"] > 0 else "blocked") for i in (1, 5, 15)}, rest_records=rest_records)
            count = 0
            try:
                iterator = adapter.iter_trades(duration_seconds=args.duration, max_events=args.max_events, include_snapshot=not args.no_snapshot)
                for ordinal, event in enumerate(iterator):
                    if hasattr(event, "as_dict"):
                        data = event.as_dict()
                    elif hasattr(event, "to_dict"):
                        data = event.to_dict()
                    elif isinstance(event, dict):
                        data = dict(event)
                    else:
                        data = vars(event)
                    # Adapt provider-neutral names to the storage contract.
                    if "event_time" in data and "event_ts" not in data:
                        data["event_ts"] = data["event_time"]
                    if "price_basis" in data and "price_base" not in data:
                        data["price_base"] = data["price_basis"]
                    if "source_event_id" in data and "event_id" not in data:
                        data["event_id"] = data["source_event_id"]
                    inserted = store.save_event(sid, data, ordinal=ordinal)
                    count += int(inserted)
                    telemetry.state.update(connection=getattr(getattr(adapter, "status", None), "state", "CONNECTED"), last_received_ts=data.get("received_at"), last_available_ts=data.get("available_at"), continuity="NEEDS_RECONCILIATION" if getattr(getattr(adapter, "status", None), "needs_reconciliation", False) else "CONTINUOUS", data_quality="PUBLIC_PROVIDER")
                    telemetry.state.update(events_processed=count)
                    telemetry.reporter.emit()
                telemetry.state.update(connection=getattr(getattr(adapter, "status", None), "state", "STOPPED"), events_processed=count)
                telemetry.event("watch_complete", events=count, rest_records=rest_records, rest_errors=rest_errors, provider="kraken-public-rest+websocket-v2", synthetic=False)
                telemetry.close()
                store.save_checkpoint(sid, "watch", cursor={"last_event_count": count}, events_processed=count, state=telemetry.state.snapshot())
                store.finish_session(sid, status="COMPLETED")
            except Exception as exc:
                telemetry.state.increment("errors")
                telemetry.event("watch_error", level="ERROR", error_type=type(exc).__name__, error=str(exc), progress=False)
                telemetry.close()
                store.finish_session(sid, status="ERROR")
                raise
        print(json.dumps({"ok": True, "mode": "LIVE", "session_id": sid, "events": count, "rest_records": rest_records, "rest_counts": rest_counts, "rest_errors": rest_errors, "db": str(db), "provider": "kraken-public-rest+websocket-v2", "status": getattr(getattr(adapter, "status", None), "to_dict", lambda: {})()}, ensure_ascii=False, indent=2, default=str)); return 0
    except Exception as exc:
        print(f"watch Kraken error: {type(exc).__name__}: {exc}", file=sys.stderr); return 2


def cmd_ui(args: argparse.Namespace) -> int:
    db = Path(args.db or default_db()).expanduser()
    print(f"UI local: http://{args.host}:{args.port}/ (sólo lectura)", flush=True)
    serve(db, host=args.host, port=args.port, session_id=args.session, duration=args.duration)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mtf-lab", description="MTF Lab — investigación cuantitativa local, virtual y reproducible")
    subs = parser.add_subparsers(dest="command", required=True)
    p = subs.add_parser("doctor", help="revisa Python, configuración, SQLite y opcionalmente conectividad")
    p.add_argument("--db", type=Path); p.add_argument("--config", type=Path); p.add_argument("--connectivity", "--check-network", dest="connectivity", action="store_true"); p.add_argument("--timeout", type=float, default=5); p.set_defaults(func=cmd_doctor)
    p = subs.add_parser("demo", help="recorrido offline sintético reproducible")
    p.add_argument("--db", type=Path); p.add_argument("--seed", type=int, default=42); p.add_argument("--minutes", type=int, default=720); p.add_argument("--config", type=Path); p.add_argument("--report", type=Path); p.add_argument("--log", type=Path); p.set_defaults(func=cmd_demo)
    def add_input_options(p: argparse.ArgumentParser) -> None:
        p.add_argument("input", type=Path); p.add_argument("--format", choices=["csv", "jsonl", "json"]); p.add_argument("--instrument", default="UNKNOWN"); p.add_argument("--timeframe", default="M1"); p.add_argument("--price-base", choices=["trade", "close", "mid", "bid", "ask"], default="close"); p.add_argument("--mapping", help="timestamp=ts,open=o,high=h,low=l,close=c,volume=v"); p.add_argument("--timezone", help="zona para timestamps sin offset"); p.add_argument("--interval-seconds", type=float); p.add_argument("--allow-issues", action="store_true"); p.add_argument("--allow-out-of-order", action="store_true"); p.add_argument("--allow-duplicates", action="store_true"); p.add_argument("--db", type=Path)
    p = subs.add_parser("import", help="importa CSV/JSONL local con validación explícita"); add_input_options(p); p.set_defaults(func=cmd_import)
    p = subs.add_parser("replay", help="reproduce una fuente local sin fabricar señales"); p.add_argument("--input", type=Path); p.add_argument("--session"); p.add_argument("--db", type=Path); p.add_argument("--config", type=Path); p.add_argument("--format", choices=["csv", "jsonl", "json"]); p.add_argument("--instrument", default="UNKNOWN"); p.add_argument("--timeframe", default="M1"); p.add_argument("--price-base", choices=["trade", "close", "mid", "bid", "ask"], default="close"); p.add_argument("--mapping"); p.add_argument("--timezone"); p.add_argument("--interval-seconds", type=float); p.add_argument("--allow-issues", action="store_true"); p.add_argument("--allow-out-of-order", action="store_true"); p.add_argument("--allow-duplicates", action="store_true"); p.set_defaults(func=cmd_replay)
    p = subs.add_parser("backtest", help="evalúa señales persistidas con simulación virtual"); p.add_argument("--session"); p.add_argument("--db", type=Path); p.add_argument("--config", type=Path); p.add_argument("--timeframe", default="M1"); p.add_argument("--format", choices=["markdown", "json", "html"], default="markdown"); p.add_argument("--report", type=Path); p.set_defaults(func=cmd_backtest)
    p = subs.add_parser("report", help="genera o imprime informe de una sesión"); p.add_argument("--session"); p.add_argument("--latest", action="store_true", help="selecciona la sesión más reciente"); p.add_argument("--db", type=Path); p.add_argument("--format", choices=["markdown", "json", "html"], default="markdown"); p.add_argument("--output", type=Path); p.set_defaults(func=cmd_report)
    p = subs.add_parser("watch", help="observación acotada/continua; no sustituye feed real por sintético"); p.add_argument("--db", type=Path); p.add_argument("--instrument", default="BTC/USD"); p.add_argument("--duration", type=float); p.add_argument("--max-events", type=int); p.add_argument("--no-snapshot", action="store_true"); p.add_argument("--offline-demo", action="store_true"); p.add_argument("--seed", type=int, default=42); p.add_argument("--report", type=Path); p.add_argument("--log", type=Path); p.set_defaults(func=cmd_watch)
    p = subs.add_parser("ui", help="sirve interfaz local de sólo lectura"); p.add_argument("--db", type=Path); p.add_argument("--session"); p.add_argument("--host", default="127.0.0.1"); p.add_argument("--port", type=int, default=8765); p.add_argument("--duration", type=float); p.set_defaults(func=cmd_ui)
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
