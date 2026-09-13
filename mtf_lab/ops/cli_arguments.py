"""Argument registration for the MTF Lab command line.

This module intentionally imports only ``argparse`` and ``pathlib`` at module
load.  Optional SDKs and application services are resolved when a command is
actually dispatched.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping
from pathlib import Path

Handler = Callable[[argparse.Namespace], int]


def _packaged_config_path(name: str) -> Path:
    """Resolve a source-checkout or wheel-bundled TOML without a fixed root."""
    from ..configuration import packaged_config_path

    return packaged_config_path(name)


def _default_handlers() -> dict[str, Handler]:
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
        cmd_replay,
        cmd_report,
        cmd_research,
        cmd_ui,
        cmd_watch,
    )

    return {
        "doctor": cmd_doctor,
        "demo": cmd_demo,
        "import": cmd_import,
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
    parser.add_argument(
        "--timestamp-unit",
        choices=["iso8601", "s", "ms", "us", "ns"],
        default="iso8601",
        help="unidad explícita para timestamps numéricos",
    )
    parser.add_argument("--interval-seconds", type=float)
    parser.add_argument("--allow-issues", action="store_true")
    parser.add_argument("--allow-out-of-order", action="store_true")
    parser.add_argument("--allow-duplicates", action="store_true")
    parser.add_argument("--db", type=Path)


def build_parser(handlers: Mapping[str, Handler] | None = None) -> argparse.ArgumentParser:
    """Build the complete parser, preserving command names and flags."""
    callbacks: Mapping[str, Handler] = handlers or _default_handlers()
    parser = argparse.ArgumentParser(
        prog="mtf-lab", description="MTF Lab — investigación cuantitativa local, virtual y reproducible"
    )
    subs = parser.add_subparsers(dest="command", required=True)

    p = subs.add_parser("doctor", help="revisa Python, configuración, SQLite y opcionalmente conectividad")
    p.add_argument("--db", type=Path)
    p.add_argument("--config", type=Path)
    p.add_argument("--connectivity", "--check-network", dest="connectivity", action="store_true")
    p.add_argument("--timeout", type=float, default=5)
    p.set_defaults(func=callbacks["doctor"])

    p = subs.add_parser("demo", help="recorrido offline sintético reproducible")
    p.add_argument("--db", type=Path)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--minutes", type=int, default=720)
    p.add_argument("--config", type=Path)
    p.add_argument("--report", type=Path)
    p.add_argument("--log", type=Path)
    p.set_defaults(func=callbacks["demo"])

    p = subs.add_parser("import", help="importa CSV/JSONL local con validación explícita")
    _add_input_options(p)
    p.add_argument("--config", type=Path)
    p.set_defaults(func=callbacks["import"])

    p = subs.add_parser("replay", help="reproduce una fuente local con agregación/indicadores incrementales")
    _add_input_options(p, optional=True)
    p.add_argument("--session")
    p.add_argument("--config", type=Path)
    p.add_argument("--checkpoint", default="runtime")
    p.add_argument("--checkpoint-every", type=int, default=100)
    p.add_argument("--max-candles", type=int, default=5000)
    p.add_argument("--partition", choices=["all", "exploration", "evaluation"], default="all")
    p.add_argument("--preserve-order", action="store_true")
    p.add_argument("--no-resume", dest="resume", action="store_false")
    p.set_defaults(resume=True, func=callbacks["replay"])

    p = subs.add_parser("backtest", help="compara referencia M1 y estrategia MTF con simulación virtual")
    p.add_argument("--session")
    p.add_argument("--db", type=Path)
    p.add_argument("--config", type=Path)
    p.add_argument("--timeframe")
    p.add_argument("--format", choices=["markdown", "json", "html"], default="markdown")
    p.add_argument("--report", type=Path)
    p.add_argument("--partition", choices=["all", "exploration", "evaluation"], default="all")
    p.add_argument("--boundary", help="timestamp UTC de frontera cronológica")
    p.set_defaults(func=callbacks["backtest"])

    p = subs.add_parser("research", help="investigación CFD explícita; no reutiliza payout binario")
    research = p.add_subparsers(dest="research_action", required=True)
    q = research.add_parser("run", help="registra todos los intentos y evalúa un replay local causal")
    source = q.add_mutually_exclusive_group(required=True)
    source.add_argument("--fixture", action="store_true", help="datos sintéticos; sin evidencia de mercado")
    source.add_argument("--input", type=Path, help="captura local autorizada JSON/JSONL")
    q.add_argument("--manifest", type=Path, required=True, help="manifiesto nuevo; no sobrescribe intentos")
    q.add_argument("--config", type=Path)
    q.add_argument("--fixture-count", type=int, default=190)
    q.add_argument("--variants", nargs="+", help="estrategias explícitas; nombres no soportados se rechazan")
    q.add_argument("--horizons-seconds", nargs="+", help="horizontes alternativos; nunca se suman entre sí")
    q.add_argument("--seed", type=int, default=42)
    q.add_argument("--bootstrap-iterations", type=int, default=250)
    q.add_argument("--bootstrap-block-days", type=int, default=5)
    q.add_argument(
        "--execution-model",
        choices=["full_fill", "ioc_partial", "rejected"],
        default="full_fill",
        help="hipótesis local de ejecución, no profundidad/fills observados del bróker",
    )
    q.add_argument("--fill-fraction", help="fracción Decimal entre 0 y 1 exclusiva de ioc_partial")
    q.add_argument("--order", choices=["as_observed", "event_time"], default="as_observed")
    q.set_defaults(func=callbacks["research"])
    q = research.add_parser("compare", help="compara manifiestos íntegros sin sumar productos/horizontes")
    q.add_argument("manifests", type=Path, nargs="+")
    q.set_defaults(func=callbacks["research"])
    q = research.add_parser("validate", help="verifica integridad, particiones, economía y límites de evidencia")
    q.add_argument("manifest", type=Path)
    q.set_defaults(func=callbacks["research"])

    p = subs.add_parser("report", help="genera o imprime informe de una sesión")
    p.add_argument("--session")
    p.add_argument("--latest", action="store_true", help="selecciona la sesión más reciente")
    p.add_argument("--db", type=Path)
    p.add_argument("--config", type=Path)
    p.add_argument("--format", choices=["markdown", "json", "html"], default="markdown")
    p.add_argument("--output", type=Path)
    p.set_defaults(func=callbacks["report"])

    p = subs.add_parser("watch", help="observación pública acotada/continua; no ejecuta órdenes")
    p.add_argument("--db", type=Path)
    p.add_argument("--config", type=Path)
    p.add_argument("--instrument")
    p.add_argument("--session")
    p.add_argument("--duration", type=float)
    p.add_argument("--max-events", type=int)
    p.add_argument("--no-snapshot", action="store_true")
    p.add_argument("--offline-demo", action="store_true")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--report", type=Path)
    p.add_argument("--log", type=Path)
    p.add_argument("--checkpoint", default="runtime")
    p.add_argument("--checkpoint-every", type=int, default=100)
    p.add_argument("--max-candles", type=int, default=5000)
    p.add_argument("--no-resume", dest="resume", action="store_false")
    p.set_defaults(resume=True, func=callbacks["watch"])

    p = subs.add_parser("ui", help="sirve interfaz local de sólo lectura")
    p.add_argument(
        "--supervisor-state", type=Path, help="snapshot privado del supervisor; readiness requiere identidad viva"
    )
    p.add_argument("--db", type=Path)
    p.add_argument("--config", type=Path)
    p.add_argument("--session")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--duration", type=float)
    p.set_defaults(func=callbacks["ui"])

    p = subs.add_parser("ctrader", help="consulta cTrader Open API; no envía operaciones por defecto")
    csubs = p.add_subparsers(dest="ctrader_command", required=True)

    q = csubs.add_parser("doctor", help="diagnostica SDK opcional, OAuth y gates de cuenta")
    q.add_argument("--config", type=Path)
    q.add_argument(
        "--network",
        "--check-network",
        dest="network",
        action="store_true",
        help="prueba explícitamente DNS/TCP/TLS DEMO; no autentica cuentas",
    )
    q.add_argument("--timeout", type=float, default=5.0)
    q.set_defaults(func=callbacks["ctrader_doctor"])

    q = csubs.add_parser("auth-url", help="inicia OAuth loopback reanudable; no abre navegador salvo flag")
    q.add_argument("--config", type=Path, default=_packaged_config_path("ctrader_query.toml"))
    q.add_argument(
        "--scope", choices=["accounts", "trading"], help="un solo scope por intento; trading va después de accounts"
    )
    q.add_argument("--open-browser", action="store_true")
    q.set_defaults(func=callbacks["ctrader_auth_url"])

    q = csubs.add_parser("callback-listen", help="recibe un callback en el loopback registrado; no imprime el código")
    q.add_argument("--config", type=Path, default=_packaged_config_path("ctrader_query.toml"))
    q.add_argument("--attempt-id", required=True)
    q.add_argument("--open-browser", action="store_true")
    q.add_argument("--timeout", type=float, default=600.0)
    q.set_defaults(func=callbacks["ctrader_callback_listen"])

    q = csubs.add_parser("select", help="selecciona una cuenta DEMO desde un descubrimiento local observado")
    q.add_argument("--config", type=Path, default=_packaged_config_path("ctrader_query.toml"))
    q.add_argument("--accounts-file", type=Path)
    q.add_argument("--account-id", required=True)
    q.set_defaults(func=callbacks["ctrader_select"])

    q = csubs.add_parser(
        "token-exchange", help="reanuda OAuth; entregue callback por archivo 0600/stdin, fixture usa store temporal"
    )
    q.add_argument("--config", type=Path, default=_packaged_config_path("ctrader_query.toml"))
    q.add_argument("--attempt-id")
    q.add_argument("--callback-file", type=Path)
    q.add_argument("--callback-stdin", action="store_true")
    q.add_argument("--callback-uri", help=argparse.SUPPRESS)
    q.add_argument("--fixture", action="store_true")
    q.set_defaults(func=callbacks["ctrader_token_exchange"])

    q = csubs.add_parser("token-refresh", help="rota token externo; fixture usa store temporal aislado")
    q.add_argument("--config", type=Path, default=_packaged_config_path("ctrader_query.toml"))
    q.add_argument("--fixture", action="store_true")
    q.set_defaults(func=callbacks["ctrader_token_refresh"])

    q = csubs.add_parser("query", help="consulta cTrader tras autorización explícita o usa fixture offline")
    q.add_argument("--config", type=Path, default=_packaged_config_path("ctrader_query.toml"))
    q.add_argument("--fixture", action="store_true")
    q.add_argument("--network", action="store_true")
    q.add_argument("--report", type=Path)
    q.add_argument(
        "--capture",
        "--capture-output",
        dest="capture",
        type=Path,
        help="exporta histórico nativo como captura versionada; no incluye bid/ask ni fills",
    )
    q.set_defaults(func=callbacks["ctrader_query"])

    q = csubs.add_parser("watch", help="observación continua cTrader de sólo lectura; nunca envía órdenes")
    q.add_argument("--config", type=Path, default=_packaged_config_path("ctrader_query.toml"))
    source = q.add_mutually_exclusive_group(required=True)
    source.add_argument("--fixture", action="store_true", help="transportes y precios sintéticos, sin red")
    source.add_argument("--network", action="store_true", help="sesión DEMO ya autorizada con scope accounts")
    q.add_argument("--db", type=Path, required=True, help="base de salida explícita; no usa la DB predeterminada")
    q.add_argument("--session", help="sesión existente; requiere --resume")
    q.add_argument("--resume", action="store_true", help="restaura checkpoint; no presume continuidad del feed")
    q.add_argument("--duration", type=float, default=30.0)
    q.add_argument(
        "--max-events",
        "--max-messages",
        dest="max_events",
        type=int,
        default=1000,
        help="mensajes atómicos del stream por corrida, no velas ni ticks normalizados",
    )
    q.add_argument("--idle-timeout", type=float, default=5.0)
    q.add_argument("--checkpoint-every", type=int, default=100)
    q.add_argument("--report", type=Path)
    q.set_defaults(func=callbacks["ctrader_watch"])

    q = csubs.add_parser("supervise", help="supervisor Linux local; sin trading por defecto")
    q.add_argument("--mode", choices=["observe", "shadow", "demo"], default="observe")
    q.add_argument(
        "--continuous", action="store_true", help="sin límite de tiempo/eventos; no elimina gates ni watchdog"
    )
    source = q.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--fixture", action="store_true", help="fuente sintética aislada; nunca satisface permiso externo"
    )
    source.add_argument("--network", action="store_true", help="fuente DEMO con autorización existente y verificada")
    q.add_argument("--activate", action="store_true", help="requiere modo DEMO y gates de operación aprobados")
    q.add_argument("--config", type=Path, default=_packaged_config_path("ctrader_query.toml"))
    q.add_argument("--state-dir", type=Path, required=True, help="estado privado fuera del checkout")
    q.add_argument("--db", type=Path, required=True)
    q.add_argument("--account-key", default="fixture", help="identidad local para el bloqueo de escritor")
    q.add_argument("--duration", type=float, default=30.0)
    q.add_argument("--max-events", type=int, default=100)
    q.add_argument("--idle-timeout", type=float, default=5.0)
    q.add_argument("--poll-timeout", type=float, default=0.25)
    q.add_argument("--checkpoint-every", type=int, default=100)
    q.add_argument("--max-candles", type=int, default=5000)
    q.add_argument("--max-reconnect-attempts", type=int, default=3)
    q.add_argument("--reconnect-backoff-seconds", type=float, default=0.25)
    q.add_argument("--stale-after-seconds", type=float, default=90.0)
    q.add_argument("--no-resume", dest="resume", action="store_false")
    q.add_argument("--watchdog", action="store_true", help="notificación local systemd de progreso útil")
    q.add_argument("--dbus-alerts", action="store_true", help="notificaciones locales de escritorio")
    q.add_argument("--report", type=Path)
    q.set_defaults(func=callbacks["ctrader_supervise"], resume=True)

    q = csubs.add_parser("demo", help="ejecutor DEMO local: requiere --activate explícito y nunca usa servidor")
    q.add_argument("--activate", action="store_true")
    q.add_argument("--report", type=Path)
    q.set_defaults(func=callbacks["ctrader_demo"])

    q = csubs.add_parser("fixture", help="ejecuta fixture offline de consulta, CFD y ejecutor demo")
    q.add_argument("--report", type=Path)
    q.set_defaults(func=callbacks["ctrader_fixture"])

    p = subs.add_parser("cfd-paper", help="pipeline cTrader→RuntimeCoordinator→CFD PAPER local")
    p.add_argument("--config", type=Path, default=_packaged_config_path("ctrader_pipeline_fixture.toml"))
    p.add_argument("--input", type=Path, help="captura JSON/JSONL local de payloads cTrader; sin red")
    p.add_argument(
        "--count", type=int, default=190, help="barras del fixture sintético cuando no se proporciona --input"
    )
    p.add_argument("--session")
    p.add_argument("--db", type=Path)
    p.add_argument("--max-candles", type=int, default=256)
    p.add_argument("--chunk-size", type=int, default=128, help="cantidad de sobres por commit incremental")
    p.add_argument(
        "--price-base",
        choices=["native", "bid", "ask", "mid"],
        help="base explícita para la captura; use native con exportaciones históricas de trendbars",
    )
    p.add_argument(
        "--order",
        choices=["as_observed", "market_time_corrected"],
        default="as_observed",
        help="orden causal; market_time_corrected es sólo inspección",
    )
    p.add_argument(
        "--incomplete",
        action="store_true",
        help="no declara liquidación final; las ventanas ya vencidas siguen siendo UNKNOWN",
    )
    p.add_argument(
        "--include-payloads", action="store_true", help="incluye payloads completos sólo en el reporte explícito"
    )
    p.add_argument("--report", type=Path)
    p.set_defaults(func=callbacks["cfd_paper"])
    return parser


__all__ = ["build_parser", "_add_input_options"]
