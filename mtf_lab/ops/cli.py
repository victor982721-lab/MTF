"""Compatibility facade for the local MTF Lab command line.

Argument registration, application services and cTrader activation services
are intentionally separate modules.  This facade keeps the historical
``mtf_lab.ops.cli`` import path stable for the console entry point and
integrators while exposing only one implementation of each command.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable

from .application_services import (
    PROJECT_ROOT,
    _analysis_for,
    _build_importer,
    _canonical_base,
    _capture_hash,
    _config_for,
    _contract_hash,
    _core_candle,
    _db_for,
    _discover_engine,
    _freshness_state,
    _kraken_bar_row,
    _latest_candles,
    _load_ctrader_capture_input,
    _load_toml,
    _log_for,
    _override_config,
    _persist_import,
    _persist_reference,
    _save_open_bar,
    _signal_variant,
    _sim_spec,
    _stored_payload,
    _stored_records,
    _watch_intervals,
    default_config,
    default_db,
    default_logs,
    default_watch_config,
    effective_config,
)
from .cli_arguments import _add_input_options
from .cli_arguments import build_parser as _build_parser
from .cli_handlers import (
    cmd_backtest,
    cmd_cfd_paper,
    cmd_ctrader_auth_url,
    cmd_ctrader_callback_listen,
    cmd_ctrader_demo,
    cmd_ctrader_doctor,
    cmd_ctrader_fixture,
    cmd_ctrader_query,
    cmd_ctrader_select,
    cmd_ctrader_supervise,
    cmd_ctrader_token_exchange,
    cmd_ctrader_token_refresh,
    cmd_ctrader_watch,
    cmd_demo,
    cmd_doctor,
    cmd_import,
    cmd_market_data,
    cmd_market_research,
    cmd_replay,
    cmd_report,
    cmd_research,
    cmd_ui,
    cmd_watch,
)
from .ctrader_cli_services import (
    _activation_payload_with_selection,
    _check_ctrader_connectivity,
    _ctrader_activation_payload,
    _ctrader_config,
    _discovery_state_path,
    _fixture_oauth_response,
    _load_persisted_selection,
    _oauth_http_request,
    _persist_discovery,
    _persist_selection,
    _read_callback_input,
    _secret_free,
    _selection_state_path,
    _token_metadata_for_config,
)


def _handler_map() -> dict[str, Callable[[argparse.Namespace], int]]:
    """Build callbacks from facade globals so legacy monkeypatching remains valid."""
    return {
        "doctor": cmd_doctor,
        "demo": cmd_demo,
        "import": cmd_import,
        "market_data": cmd_market_data,
        "market_research": cmd_market_research,
        "replay": cmd_replay,
        "backtest": cmd_backtest,
        "report": cmd_report,
        "watch": cmd_watch,
        "ui": cmd_ui,
        "ctrader_doctor": cmd_ctrader_doctor,
        "ctrader_auth_url": cmd_ctrader_auth_url,
        "ctrader_callback_listen": cmd_ctrader_callback_listen,
        "ctrader_select": cmd_ctrader_select,
        "ctrader_token_exchange": cmd_ctrader_token_exchange,
        "ctrader_token_refresh": cmd_ctrader_token_refresh,
        "ctrader_query": cmd_ctrader_query,
        "ctrader_watch": cmd_ctrader_watch,
        "ctrader_demo": cmd_ctrader_demo,
        "ctrader_fixture": cmd_ctrader_fixture,
        "cfd_paper": cmd_cfd_paper,
        "research": cmd_research,
        "ctrader_supervise": cmd_ctrader_supervise,
    }


def build_parser() -> argparse.ArgumentParser:
    """Return the complete parser while retaining facade callback identity."""
    return _build_parser(_handler_map())


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.func(args))
    except BrokenPipeError:
        return 0
    except Exception as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2


__all__ = [
    "PROJECT_ROOT",
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
    "_ctrader_activation_payload",
    "_selection_state_path",
    "_discovery_state_path",
    "_secret_free",
    "_persist_discovery",
    "_load_persisted_selection",
    "_activation_payload_with_selection",
    "_persist_selection",
    "_ctrader_config",
    "_token_metadata_for_config",
    "_check_ctrader_connectivity",
    "_fixture_oauth_response",
    "_oauth_http_request",
    "_read_callback_input",
    "_load_ctrader_capture_input",
    "cmd_doctor",
    "cmd_demo",
    "cmd_import",
    "cmd_replay",
    "cmd_backtest",
    "cmd_report",
    "cmd_watch",
    "cmd_ui",
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
    "cmd_cfd_paper",
    "cmd_research",
    "cmd_ctrader_supervise",
    "_add_input_options",
    "build_parser",
    "main",
]
