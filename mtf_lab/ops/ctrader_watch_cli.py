"""CLI composition for bounded cTrader observation, never order execution."""

from __future__ import annotations

import argparse
import os
import signal
import stat
import sys
import threading
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ..configuration import ConfigError, EffectiveConfig
from .application_services import CommandResult, _config_for, _override_config
from .persistence import SQLiteStore

if TYPE_CHECKING:
    from ..data.ctrader import CTraderProvider
    from .ctrader_cli_services import CTraderCliService, QueryContext
    from .ctrader_watch import CTraderWatchOptions, CTraderWatchResult

_FIXTURE_ID = "mtf-ctrader-watch-spots-v1"
_FIXTURE_START = datetime(2026, 1, 1, tzinfo=UTC)
_FIXTURE_LIMIT = 5000


class _StopSignals:
    def __init__(self) -> None:
        self.event = threading.Event()
        self.previous: dict[int, Any] = {}

    def __enter__(self) -> threading.Event:
        if threading.current_thread() is threading.main_thread():
            for number in (signal.SIGINT, signal.SIGTERM):
                self.previous[number] = signal.getsignal(number)
                signal.signal(number, self._stop)
        return self.event

    def _stop(self, _number: int, _frame: Any) -> None:
        self.event.set()

    def __exit__(self, *_args: Any) -> None:
        for number, previous in self.previous.items():
            signal.signal(number, previous)


def _database_path(value: Any) -> Path:
    if value is None:
        raise ConfigError("watch requiere --db explícito")
    path = Path(value).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    fd = os.open(path, flags, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ConfigError("--db debe ser un archivo regular")
    finally:
        os.close(fd)
    return path


def _safe_spot_diagnostic(exc: BaseException) -> dict[str, Any] | None:
    """Expose only the bounded market diagnostic attached by the adapter."""

    value = getattr(exc, "diagnostic", None)
    if not isinstance(value, Mapping):
        return None
    allowed = (
        "event_time",
        "timestamp_original",
        "timestamp_unit",
        "received_at",
        "available_at",
        "bid_raw",
        "ask_raw",
        "price_scale",
        "digits",
        "fields_present",
        "updated_sides",
        "partial_update",
        "bid_ask_relation",
        "received_minus_event_seconds",
        "available_minus_event_seconds",
        "timing_status",
        "timing_attribution",
    )
    return {key: value[key] for key in allowed if key in value}


def _validate_args(args: argparse.Namespace, config: EffectiveConfig) -> CTraderWatchOptions:
    from .ctrader_watch import CTraderWatchOptions

    if bool(getattr(args, "fixture", False)) == bool(getattr(args, "network", False)):
        raise ConfigError("elija exactamente --fixture o --network")
    if bool(getattr(args, "session", None)) != bool(getattr(args, "resume", False)):
        raise ConfigError("--session y --resume se requieren juntos")
    if config.price_base not in {"mid", "bid", "ask"}:
        raise ConfigError("watch observa spots mid/bid/ask; use cfd-paper para histórico native")
    if str(config.ctrader.get("environment", "DEMO")).upper() != "DEMO":
        raise ConfigError("watch sólo admite DEMO")
    if config.execution.get("enabled", False) is not False:
        raise ConfigError("watch no admite ejecución habilitada")
    return CTraderWatchOptions(
        duration_seconds=getattr(args, "duration", 30.0),
        max_events=getattr(args, "max_events", 1000),
        idle_timeout_seconds=getattr(args, "idle_timeout", 5.0),
        checkpoint_every=getattr(args, "checkpoint_every", 100),
        resume=bool(getattr(args, "resume", False)),
    )


def _fixture_frontier(store: SQLiteStore, session_id: str | None, config: EffectiveConfig) -> tuple[int, bool]:
    if session_id is None:
        return 0, False
    session = store.get_session(session_id)
    checkpoints = store.list_checkpoints(session_id, "ctrader-watch", limit=2)
    if session is None or str(session.get("mode")) != "SYNTHETIC" or len(checkpoints) != 1:
        raise ConfigError("la sesión no tiene un checkpoint de fixture watch")
    checkpoint = checkpoints[0]
    state = checkpoint.get("state", {})
    if not isinstance(state, Mapping) or state.get("config_hash") != config.config_hash:
        raise ConfigError("el checkpoint pertenece a otra configuración")
    watch = state.get("watch", {})
    if not isinstance(watch, Mapping):
        raise ConfigError("checkpoint watch incompleto")
    count = watch.get("messages", 0)
    if isinstance(count, bool) or not isinstance(count, int) or not 0 <= count < _FIXTURE_LIMIT:
        raise ConfigError("cursor de fixture inválido o agotado")
    if not count:
        return 0, False
    sequence = watch.get("last_sequence")
    if not isinstance(sequence, int):
        raise ConfigError("checkpoint sin secuencia observable")
    rows = store.list_capture_envelopes(session_id, after_sequence=sequence - 1, limit=2)
    matching = [row for row in rows if row.get("ingest_sequence") == sequence]
    if len(rows) != 1 or len(matching) != 1:
        raise ConfigError("hay captura no conciliada después del checkpoint")
    payload = matching[0].get("payload", {})
    expected_time = int((_FIXTURE_START + timedelta(minutes=count)).timestamp() * 1000)
    if (
        not isinstance(payload, Mapping)
        or payload.get("sequence") != count - 1
        or payload.get("timestamp") != expected_time
    ):
        raise ConfigError("el cursor no coincide con la frontera de la fixture")
    operational = state.get("operational", {})
    continuous = isinstance(operational, Mapping) and operational.get("continuity_state") in {
        "CONTINUOUS",
        "RECOVERED_BOUNDED",
    }
    return count, continuous


def _fixture_provider(
    config: EffectiveConfig, start: int, count: int
) -> tuple[CTraderProvider, Callable[[], datetime]]:
    from ..data.ctrader import (
        PAYLOAD,
        CTraderClient,
        CTraderConfig,
        CTraderProvider,
        DeterministicTransport,
        WireMessage,
    )
    from ..data.paper_fixture import synthetic_ctrader_payloads

    if start + count > _FIXTURE_LIMIT:
        raise ConfigError("la fixture admite como máximo 5000 mensajes; reduzca el límite")
    payloads = synthetic_ctrader_payloads(symbol_id=99, count=max(130, start + count))[start : start + count]

    def handler(request: WireMessage) -> Any:
        if request.payload_type_id == PAYLOAD["PROTO_HEARTBEAT_EVENT"]:
            return None
        if request.payload_type_id != PAYLOAD["PROTO_OA_SUBSCRIBE_SPOTS_REQ"]:
            raise ConfigError("la fixture rechazó una solicitud no observacional")
        messages = [WireMessage("PROTO_OA_SUBSCRIBE_SPOTS_RES", {}, request.client_msg_id)]
        for item in payloads:
            payload = {key: value for key, value in item.items() if key not in {"trendbar", "trendbars"}}
            when = datetime.fromtimestamp(int(payload["timestamp"]) / 1000, UTC) + timedelta(seconds=1)
            messages.append(
                WireMessage(
                    "PROTO_OA_SPOT_EVENT",
                    payload,
                    is_event=True,
                    received_at=when,
                    available_at=when,
                    source_identity=f"{_FIXTURE_ID}:{payload['sequence']}",
                )
            )
        return messages

    transport = DeterministicTransport(handler, inbound_maxsize=count + 32)
    provider_config = CTraderConfig(
        symbol=config.instrument,
        symbol_id=99,
        account_id=7,
        quote_basis=config.price_base,
        queue_maxsize=count + 32,
        heartbeat_seconds=60.0,
    )
    client = CTraderClient(provider_config, transport=transport, wall_clock=lambda: _FIXTURE_START)

    def clock() -> datetime:
        return client.status.last_event_at or _FIXTURE_START

    provider = CTraderProvider(provider_config, client=client, clock=clock)
    try:
        client.connect()
        client.mark_authenticated(7)  # Controlled local transport, never an account at a broker.
        provider.subscribe(timeframes=())
    except BaseException:
        provider.close()
        raise
    return provider, clock


class CTraderWatchCliService:
    def __init__(self, query_service: CTraderCliService) -> None:
        self.query_service = query_service

    def run(self, args: argparse.Namespace) -> CommandResult:
        from .ctrader_cli_services import _safe_error_code, _write_query_report

        network_context: QueryContext | None = None
        try:
            config = _config_for(args)
            options = _validate_args(args, config)
            result: CTraderWatchResult | CommandResult
            if getattr(args, "fixture", False):
                result = self._fixture(args, _override_config(config, mode="SYNTHETIC"), options)
            else:
                prepared = self.query_service._prepare_query(args)
                if isinstance(prepared, CommandResult):
                    return prepared
                network_context = prepared
                result = self._network(args, prepared, options)
            if isinstance(result, CommandResult):
                return result
            output = {"ok": result.clean_stop, "read_only": True, **result.to_dict()}
            output["limit_unit"] = "wire_messages_per_run"
            report = getattr(args, "report", None)
            if report is not None:
                _write_query_report(report, output)
            return CommandResult.json(output)
        except KeyboardInterrupt:
            return CommandResult.json({"ok": False, "state": "INTERRUPTED"}, code=130, stderr=True)
        except Exception as exc:
            payload: dict[str, Any] = {
                "ok": False,
                "state": "WATCH_FAILED",
                "error": _safe_error_code(exc),
                "network_attempted": bool(network_context is not None and network_context.network_performed),
                "execution_enabled": False,
            }
            diagnostic = _safe_spot_diagnostic(exc)
            if diagnostic is not None:
                payload["spot_diagnostic"] = diagnostic
            return CommandResult.json(payload, code=2, stderr=True)

    def _fixture(
        self, args: argparse.Namespace, config: EffectiveConfig, options: CTraderWatchOptions
    ) -> CTraderWatchResult:
        from .ctrader_watch import CTraderWatchContext, run_ctrader_watch

        with SQLiteStore(_database_path(args.db)) as store:
            start, continuous = _fixture_frontier(store, getattr(args, "session", None), config)
            provider, clock = _fixture_provider(config, start, options.max_events or 1000)
            provenance = {
                "provider": "ctrader_open_api_fixture",
                "source_mode": "SYNTHETIC_FIXTURE",
                "synthetic": True,
                "environment": "OFFLINE",
                "network_performed": False,
                "execution_enabled": False,
                "data_identity": _FIXTURE_ID,
                "continuity_verified": continuous,
            }
            with _StopSignals() as stop:
                print("CTRADER_WATCH_STARTED fixture; no network; no orders", file=sys.stderr, flush=True)
                return run_ctrader_watch(
                    CTraderWatchContext(
                        provider,
                        config,
                        store,
                        provenance,
                        session_id=getattr(args, "session", None),
                        stop_event=stop,
                        clock=clock,
                    ),
                    options,
                )

    def _network(
        self, args: argparse.Namespace, context: Any, options: CTraderWatchOptions
    ) -> CTraderWatchResult | CommandResult:
        from .ctrader_watch import CTraderWatchContext, run_ctrader_watch

        provider = None
        handed_to_runner = False
        try:
            provider, observed = self.query_service._connect_and_discover(context)
            ready = self.query_service._authorize_readonly_provider(context, provider, observed)
            if isinstance(ready, CommandResult):
                return ready
            provider.subscribe(timeframes=())
            provenance = {
                "provider": "ctrader_open_api",
                "source_mode": "LIVE",
                "synthetic": False,
                "environment": "DEMO",
                "network_performed": True,
                "execution_enabled": False,
                "data_identity": {"account": context.profile.account_id, "instrument": context.config.instrument},
            }
            with SQLiteStore(_database_path(args.db)) as store, _StopSignals() as stop:
                print("CTRADER_WATCH_STARTED DEMO read-only; no orders", file=sys.stderr, flush=True)
                handed_to_runner = True
                return run_ctrader_watch(
                    CTraderWatchContext(
                        provider,
                        context.config,
                        store,
                        provenance,
                        session_id=getattr(args, "session", None),
                        stop_event=stop,
                    ),
                    options,
                )
        finally:
            context.client_secret = ""
            if provider is not None and not handed_to_runner:
                provider.close()
