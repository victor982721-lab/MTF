"""Argument registration for the MTF Lab command line.

This module intentionally imports only ``argparse`` and ``pathlib`` at module
load.  Optional SDKs and application services are resolved when a command is
actually dispatched.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

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
        cmd_market_data,
        cmd_market_research,
        cmd_replay,
        cmd_report,
        cmd_research,
        cmd_runtime,
        cmd_ui,
        cmd_watch,
    )

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
        "runtime": cmd_runtime,
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


def _add_campaign_commands(parent: Any, handler: Handler) -> None:
    """Keep the historical campaign distinct from the legacy research runner."""
    from ..core.market_profiles import candidate_ids
    from .historical_assumptions import PEPPERSTONE_PUBLIC_CALENDAR_MODEL_ID, VIRTUAL_EURUSD_10K_MODEL_ID

    campaign = parent.add_parser("campaign", help="protocolo de mercado: preregistro, ejecución e informes")
    actions = campaign.add_subparsers(dest="market_research_action", required=True)
    init = actions.add_parser("init", help="crea protocolo y registro privados; no lee cotizaciones")
    init.add_argument("--protocol", type=Path, required=True)
    init.add_argument("--registry", type=Path, required=True)
    init.set_defaults(func=handler)
    for name in ("register", "run"):
        action = actions.add_parser(name, help="registra identidades antes de cualquier resultado")
        action.add_argument("--protocol", type=Path, required=True)
        action.add_argument("--registry", type=Path, required=True)
        action.add_argument("--dataset-manifest", type=Path, required=True)
        action.add_argument(
            "--stage", choices=["pilot-week", "pilot-month", "development", "walk-forward", "holdout"], required=True
        )
        action.add_argument("--candidates", choices=candidate_ids(), nargs="+")
        action.add_argument(
            "--runtime-identity", type=Path, help="JSON de runtime/código verificados; no cambia permisos"
        )
        action.add_argument("--cost-identity", type=Path, help="JSON de costes/procedencia; desconocidos no son cero")
        calendar_identity = action.add_mutually_exclusive_group()
        calendar_identity.add_argument(
            "--calendar-hash", default="UNKNOWN", help="identidad de calendario; no acredita vigencia por sí sola"
        )
        calendar_identity.add_argument(
            "--calendar-identity", type=Path, help="JSON de calendario/vigencia; un hash aislado no acredita cobertura"
        )
        action.add_argument(
            "--assumptions-model",
            choices=[VIRTUAL_EURUSD_10K_MODEL_ID],
            help="modelo virtual explícito; no acredita contrato, costes ni elegibilidad DEMO",
        )
        action.add_argument(
            "--calendar-model",
            choices=[PEPPERSTONE_PUBLIC_CALENDAR_MODEL_ID],
            help="retroproyección pública modelada; no acredita festivos ni horario histórico de la cuenta",
        )
        action.add_argument("--contract-spec", type=Path, help="JSON de especificación y procedencia; no abre cuentas")
        action.add_argument(
            "--calendar-state", type=Path, help="JSON de estado del calendario; desconocido no es abierto"
        )
        action.add_argument("--scenarios", choices=["base", "adverse", "extreme"], nargs="+", default=["base"])
        if name == "run":
            action.add_argument("--output-dir", type=Path, required=True)
            action.add_argument("--start", help="inicio ISO-8601 con zona, inclusivo")
            action.add_argument("--end", help="fin ISO-8601 con zona, exclusivo")
        action.set_defaults(func=handler)
    for name in ("report", "validate"):
        action = actions.add_parser(name, help="proyecta o valida evidencia existente sin ejecutar estrategias")
        action.add_argument("manifest", type=Path)
        action.add_argument("--protocol", type=Path)
        if name == "report":
            action.add_argument("--output-dir", type=Path, required=True)
        else:
            action.add_argument("--dataset-manifest", type=Path)
            action.add_argument("--registry", type=Path)
        action.set_defaults(func=handler)


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

    p = subs.add_parser("market-data", help="adquisición explícita y validación de ticks históricos auditables")
    market_data = p.add_subparsers(dest="market_data_action", required=True)
    q = market_data.add_parser(
        "acquire", help="adquisición gratuita HistData de un mes de desarrollo; requiere --terms-accepted"
    )
    q.add_argument("--provider", choices=["histdata"], default="histdata")
    q.add_argument("--instrument", choices=["EURUSD", "EUR/USD"], default="EURUSD")
    q.add_argument(
        "--month",
        choices=[f"{year}-{month:02d}" for year in range(2016, 2020) for month in range(1, 13)],
        default="2016-03",
        help="mes UTC de desarrollo permitido (2016-01..2019-12)",
    )
    q.add_argument("--data-root", type=Path, default=Path.home() / ".local/share/mtf-lab/market-data")
    q.add_argument("--terms-accepted", action="store_true", help="condiciones de acceso/uso resueltas previamente")
    q.add_argument("--timeout", type=float, default=120.0)
    q.set_defaults(func=callbacks["market_data"])
    q = market_data.add_parser("validate", help="verifica hashes, formato y cobertura sin descargar")
    q.add_argument("manifest", type=Path)
    q.add_argument("--start", help="inicio de ventana UTC inclusivo, con zona")
    q.add_argument("--end", help="fin de ventana UTC exclusivo, con zona")
    q.set_defaults(func=callbacks["market_data"])
    q = market_data.add_parser("describe", help="estructura descriptiva en desarrollo; no abre holdout")
    q.add_argument("manifest", type=Path)
    q.add_argument("--start", required=True, help="inicio ISO-8601 inclusivo con zona, 2016–2019")
    q.add_argument("--end", required=True, help="fin ISO-8601 exclusivo con zona, hasta 2020-01-01")
    q.add_argument("--output-dir", type=Path, help="HTML/JSON nuevo autocontenido; no sobrescribe informes")
    q.add_argument(
        "--registry",
        type=Path,
        help="registro global opcional de la corrida descriptiva; se crea sólo tras validar la ventana",
    )
    q.add_argument(
        "--weekly-calendar",
        choices=["strict", "modeled-fx"],
        default="strict",
        help="modelo semanal explícito; no acredita calendario ni festivos de cuenta",
    )
    q.set_defaults(func=callbacks["market_data"])

    p = subs.add_parser("research", help="investigación CFD explícita; no reutiliza payout binario")
    research = p.add_subparsers(dest="research_action", required=True)
    _add_campaign_commands(research, callbacks["market_research"])
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

    p = subs.add_parser("runtime", help="gestiona runtimes privados MTF sin tocar datos de investigación")
    runtime = p.add_subparsers(dest="runtime_action", required=True)
    q = runtime.add_parser("inspect", help="clasifica runtime activo, reviews, rollback y residuos")
    q.add_argument("--root", type=Path, default=Path.home() / ".local/share/mtf-lab")
    q.set_defaults(func=callbacks["runtime"])
    q = runtime.add_parser("gc", help="limpia sólo runtimes gestionados y obsoletos")
    q.add_argument("--root", type=Path, default=Path.home() / ".local/share/mtf-lab")
    q.add_argument("--dry-run", action="store_true", help="clasifica sin borrar")
    q.add_argument("--keep-reviews", type=int, default=1)
    q.add_argument("--keep-rollbacks", type=int, default=1)
    q.add_argument("--stale-after-seconds", type=float, default=3600.0)
    q.add_argument(
        "--purge-review-evidence",
        action="store_true",
        help="elimina también logs/validation heredados, sólo tras preservar su receipt",
    )
    q.set_defaults(func=callbacks["runtime"])
    q = runtime.add_parser("promote", help="promueve explícitamente un review validado")
    q.add_argument("--root", type=Path, default=Path.home() / ".local/share/mtf-lab")
    q.add_argument("--path", type=Path, required=True, help=".../runtime dentro de un review gestionado")
    q.add_argument("--keep-rollback", type=int, default=1)
    q.set_defaults(func=callbacks["runtime"])

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
    ui_source = p.add_mutually_exclusive_group()
    ui_source.add_argument("--db", type=Path)
    ui_source.add_argument(
        "--snapshot",
        type=Path,
        help="JSON privado publicado por el escritor; no abre SQLite durante corridas cercadas",
    )
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
