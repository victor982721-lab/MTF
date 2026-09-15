"""Thin command handlers for the MTF Lab CLI.

Handlers only adapt ``argparse.Namespace`` values to application services and
render their response.  Business rules, persistence composition and provider
selection live in the service modules.
"""

from __future__ import annotations

import argparse
import sys

from ..configuration import ConfigError
from .application_services import (
    BacktestService,
    CfdPaperService,
    CommandResult,
    DemoService,
    DoctorService,
    ImportService,
    ReplayService,
    ReportService,
    WatchService,
)
from .importer import ImportValidationError


def _emit(result: CommandResult) -> int:
    print(result.rendered(), file=sys.stderr if result.stderr else sys.stdout)
    return int(result.code)


def _service_error(exc: Exception, *, json_output: bool = False) -> CommandResult:
    if json_output:
        return CommandResult.json(
            {"ok": False, "error": str(exc), "state": type(exc).__name__},
            code=2,
            stderr=True,
        )
    return CommandResult.plain(f"ERROR: {type(exc).__name__}: {exc}", code=2, stderr=True)


def cmd_doctor(args: argparse.Namespace) -> int:
    return _emit(DoctorService().run(args))


def cmd_demo(args: argparse.Namespace) -> int:
    return _emit(DemoService().run(args))


def cmd_import(args: argparse.Namespace) -> int:
    try:
        result = ImportService().run(args)
    except ImportValidationError as exc:
        result = CommandResult.json(
            {"ok": False, "error": str(exc), "issues": [item.to_dict() for item in exc.issues]},
            code=2,
            stderr=True,
        )
    return _emit(result)


def cmd_replay(args: argparse.Namespace) -> int:
    try:
        result = ReplayService().run(args)
    except ConfigError as exc:
        # Keep the command's historical concise diagnostics for missing or
        # invalid sessions instead of converting them to a traceback-like line.
        result = CommandResult.plain(str(exc), code=2, stderr=True)
    except ImportValidationError as exc:
        result = CommandResult.json(
            {"ok": False, "error": str(exc), "issues": [item.to_dict() for item in exc.issues]},
            code=2,
            stderr=True,
        )
    return _emit(result)


def cmd_backtest(args: argparse.Namespace) -> int:
    try:
        result = BacktestService().run(args)
    except ConfigError as exc:
        result = CommandResult.plain(str(exc), code=2, stderr=True)
    return _emit(result)


def cmd_report(args: argparse.Namespace) -> int:
    try:
        result = ReportService().run(args)
    except ConfigError as exc:
        result = CommandResult.plain(str(exc), code=2, stderr=True)
    return _emit(result)


def cmd_watch(args: argparse.Namespace) -> int:
    return _emit(WatchService().run(args))


def cmd_cfd_paper(args: argparse.Namespace) -> int:
    return _emit(CfdPaperService().run(args))


def cmd_research(args: argparse.Namespace) -> int:
    from .research import research_command

    code, payload = research_command(args)
    return _emit(CommandResult.json(payload, code=code, stderr=code != 0))


def cmd_market_data(args: argparse.Namespace) -> int:
    from .market_data_service import market_data_command

    code, payload = market_data_command(args)
    return _emit(CommandResult.json(payload, code=code, stderr=code != 0))


def cmd_market_research(args: argparse.Namespace) -> int:
    from .market_research import market_research_command

    code, payload = market_research_command(args)
    return _emit(CommandResult.json(payload, code=code, stderr=code != 0))


def cmd_ctrader_supervise(args: argparse.Namespace) -> int:
    from .supervision import CommandService

    return _emit(CommandService().run(args))


def cmd_ui(args: argparse.Namespace) -> int:
    from .application_services import _config_for, _db_for
    from .ui import serve

    snapshot = getattr(args, "snapshot", None)
    # Snapshot mode deliberately avoids configuration/database resolution.
    # The already-running writer owns publication; the UI only reads JSON.
    if snapshot is not None:
        if getattr(args, "db", None) is not None:
            raise ValueError("--snapshot y --db son incompatibles")
        db = None
    else:
        config = _config_for(args)
        db = _db_for(args, config)
    print(f"UI local: http://{args.host}:{args.port}/ (sólo lectura)", flush=True)
    serve(
        db,
        host=args.host,
        port=args.port,
        session_id=args.session,
        duration=args.duration,
        supervisor_state=getattr(args, "supervisor_state", None),
        snapshot_path=snapshot,
    )
    return 0


__all__ = [
    "cmd_doctor",
    "cmd_demo",
    "cmd_import",
    "cmd_replay",
    "cmd_backtest",
    "cmd_report",
    "cmd_watch",
    "cmd_cfd_paper",
    "cmd_research",
    "cmd_market_data",
    "cmd_market_research",
    "cmd_ctrader_supervise",
    "cmd_ui",
]


# cTrader handlers are kept here so parser callbacks have one presentation
# boundary, while their use cases remain in ctrader_cli_services.py.
def cmd_ctrader_doctor(args: argparse.Namespace) -> int:
    from .ctrader_cli_services import CTraderCliService

    return _emit(CTraderCliService().doctor(args))


def cmd_ctrader_auth_url(args: argparse.Namespace) -> int:
    from .ctrader_cli_services import CTraderCliService

    return _emit(CTraderCliService().auth_url(args))


def cmd_ctrader_callback_listen(args: argparse.Namespace) -> int:
    from .ctrader_cli_services import CTraderCliService

    return _emit(CTraderCliService().callback_listen(args))


def cmd_ctrader_token_exchange(args: argparse.Namespace) -> int:
    from .ctrader_cli_services import CTraderCliService

    return _emit(CTraderCliService().token_exchange(args))


def cmd_ctrader_token_refresh(args: argparse.Namespace) -> int:
    from .ctrader_cli_services import CTraderCliService

    return _emit(CTraderCliService().token_refresh(args))


def cmd_ctrader_select(args: argparse.Namespace) -> int:
    from .ctrader_cli_services import CTraderCliService

    return _emit(CTraderCliService().select(args))


def cmd_ctrader_query(args: argparse.Namespace) -> int:
    from .ctrader_cli_services import CTraderCliService

    return _emit(CTraderCliService().query(args))


def cmd_ctrader_watch(args: argparse.Namespace) -> int:
    from .ctrader_cli_services import CTraderCliService
    from .ctrader_watch_cli import CTraderWatchCliService

    return _emit(CTraderWatchCliService(CTraderCliService()).run(args))


def cmd_ctrader_demo(args: argparse.Namespace) -> int:
    from .ctrader_cli_services import CTraderCliService

    return _emit(CTraderCliService().demo(args))


def cmd_ctrader_fixture(args: argparse.Namespace) -> int:
    from .ctrader_cli_services import CTraderCliService

    return _emit(CTraderCliService().fixture(args))


__all__ += [
    "cmd_ctrader_doctor",
    "cmd_ctrader_auth_url",
    "cmd_ctrader_callback_listen",
    "cmd_ctrader_token_exchange",
    "cmd_ctrader_token_refresh",
    "cmd_ctrader_select",
    "cmd_ctrader_query",
    "cmd_ctrader_watch",
    "cmd_ctrader_demo",
    "cmd_ctrader_fixture",
]
