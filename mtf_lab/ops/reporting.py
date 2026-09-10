"""Auditable JSON/Markdown/HTML reports sourced from persisted records."""

from __future__ import annotations

import dataclasses
import html
import json
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def _json_default(value: Any) -> Any:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return dataclasses.asdict(value)
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


class ReportBuilder:
    """Build reports without adding calculations that are absent in storage."""

    def __init__(self, store: Any | None = None, session_id: str | None = None):
        self.store = store
        self.session_id = session_id

    def summary(self, *, session: Mapping[str, Any] | None = None, results: Iterable[Any] | None = None) -> dict[str, Any]:
        if session is None and self.store is not None and self.session_id:
            session = self.store.get_session(self.session_id)
        session = dict(session or {})
        if self.store is not None and self.session_id:
            status = self.store.status(self.session_id)
            signals = self.store.list_signals(self.session_id)
            discards = self.store.list_discards(self.session_id)
            simulations = self.store.list_simulations(self.session_id)
            candles = self.store.list_candles(self.session_id)
        else:
            status = dict(session)
            signals = []; discards = []; simulations = []; candles = []
        if results is not None:
            existing_ids = {str(item.get("simulation_id")) for item in simulations if item.get("simulation_id") is not None}
            for result in results:
                data = result.to_dict(include_simulations=True) if hasattr(result, "to_dict") else dict(result)
                for simulation in data.get("simulations", []):
                    simulation_id = simulation.get("simulation_id")
                    if simulation_id is None or str(simulation_id) not in existing_ids:
                        simulations.append(simulation)
                        if simulation_id is not None:
                            existing_ids.add(str(simulation_id))
        outcomes = Counter(str(x.get("outcome", "UNKNOWN")) for x in simulations)
        discard_reasons = Counter(str(x.get("reason_code", "UNKNOWN")) for x in discards)
        by_horizon: dict[str, dict[str, Any]] = {}
        grouped: defaultdict[str, Counter[str]] = defaultdict(Counter)
        grouped_net: defaultdict[str, float] = defaultdict(float)
        by_variant_grouped: defaultdict[str, Counter[str]] = defaultdict(Counter)
        by_variant_net: defaultdict[str, float] = defaultdict(float)
        variant_hashes: dict[str, str] = {}
        for sim in simulations:
            key = str(sim.get("horizon_seconds", "UNKNOWN"))
            outcome = str(sim.get("outcome", "UNKNOWN"))
            grouped[key][outcome] += 1
            if sim.get("net_result") is not None:
                grouped_net[key] += float(sim["net_result"])
            payload = sim.get("payload") if isinstance(sim.get("payload"), Mapping) else {}
            variant = str(sim.get("variant") or payload.get("variant") or str(sim.get("simulation_id", "UNKNOWN")).split(":", 1)[0])
            by_variant_grouped[variant][outcome] += 1
            if sim.get("net_result") is not None:
                by_variant_net[variant] += float(sim["net_result"])
            if sim.get("variant_config_hash") or payload.get("variant_config_hash"):
                variant_hashes[variant] = str(sim.get("variant_config_hash") or payload.get("variant_config_hash"))
        for key in sorted(grouped):
            by_horizon[key] = {"outcomes": dict(grouped[key]), "net_result": grouped_net[key]}
        by_variant = {key: {"outcomes": dict(by_variant_grouped[key]), "net_result": by_variant_net[key], "config_hash": variant_hashes.get(key)} for key in sorted(by_variant_grouped)}
        quality = Counter(str(x.get("quality", "UNKNOWN")) for x in candles)
        resolutions = Counter(str(x.get("timeframe", x.get("resolution", "UNKNOWN"))) for x in candles)
        mode = session.get("mode", status.get("mode", "UNKNOWN"))
        mode_label = {"SYNTHETIC": "SINTETICO", "LIVE": "OBSERVACIÓN EN DIRECTO", "REPLAY": "REPLAY", "BACKTEST": "BACKTEST"}.get(str(mode).upper(), str(mode))
        return {
            "generated_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
            "schema": "mtf-lab.report.v1",
            "session": session,
            "status": status,
            "mode": mode,
            "mode_label": mode_label,
            "provider": session.get("provider", status.get("provider", "UNKNOWN")),
            "instrument": session.get("instrument", status.get("instrument", "UNKNOWN")),
            "counts": {
                "signals": len(signals),
                "discards": len(discards),
                "simulations": len(simulations),
                "candles": len(candles),
            },
            "outcomes": dict(outcomes),
            "discard_reasons": dict(discard_reasons),
            "by_horizon": by_horizon,
            "by_variant": by_variant,
            "data_quality": dict(quality),
            "resolutions": dict(resolutions),
            "limitations": [
                "Los resultados son simulaciones virtuales y no constituyen órdenes ni recomendación.",
                "La tasa de acierto no es una probabilidad de éxito; señales cercanas pueden depender entre sí.",
                "Las velas no permiten inferir movimientos intrabar que no estén observados.",
                "La ausencia de una fuente de noticias o de un bróker no se interpreta como comprobación.",
            ],
        }

    def to_json(self, data: Mapping[str, Any], *, pretty: bool = True) -> str:
        return json.dumps(data, ensure_ascii=False, indent=2 if pretty else None, sort_keys=True, default=_json_default)

    def to_markdown(self, data: Mapping[str, Any]) -> str:
        status = data.get("status", {})
        counts = data.get("counts", {})
        lines = [
            "# MTF Lab — informe de ejecución",
            "",
            f"- Generado: `{data.get('generated_at', 'UNKNOWN')}`",
            f"- Modo: **{data.get('mode', 'UNKNOWN')}** (`{data.get('mode_label', data.get('mode', 'UNKNOWN'))}`)",
            f"- Proveedor: `{data.get('provider', 'UNKNOWN')}`",
            f"- Instrumento: `{data.get('instrument', 'UNKNOWN')}`",
            f"- Estado de sesión: `{status.get('status', 'UNKNOWN')}`",
            "",
            "## Conteos persistidos",
            "",
            "| Señales | Descartes | Simulaciones | Velas |",
            "|---:|---:|---:|---:|",
            f"| {counts.get('signals', 0)} | {counts.get('discards', 0)} | {counts.get('simulations', 0)} | {counts.get('candles', 0)} |",
            "",
            "## Resultados por horizonte",
            "",
            "| Horizonte (s) | Resultados | Neto virtual |",
            "|---:|---|---:|",
        ]
        for horizon, item in sorted((data.get("by_horizon") or {}).items(), key=lambda pair: pair[0]):
            lines.append(f"| {horizon} | `{json.dumps(item.get('outcomes', {}), ensure_ascii=False, sort_keys=True)}` | {item.get('net_result', 0):.4g} |")
        if not data.get("by_horizon"):
            lines.append("| — | Sin simulaciones | 0 |")
        lines.extend(["", "## Variantes controladas", ""])
        if data.get("by_variant"):
            lines.extend(["| Variante | Resultados | Neto virtual | Hash de configuración |", "|---|---|---:|---|"])
            for variant, item in sorted(data["by_variant"].items()):
                lines.append(f"| `{variant}` | `{json.dumps(item.get('outcomes', {}), ensure_ascii=False, sort_keys=True)}` | {item.get('net_result', 0):.4g} | `{item.get('config_hash') or '—'}` |")
        else:
            lines.append("- Sin variantes persistidas.")
        lines.extend(["", "## Motivos de descarte", ""])
        for reason, count in sorted((data.get("discard_reasons") or {}).items()):
            lines.append(f"- `{reason}`: {count}")
        if not data.get("discard_reasons"):
            lines.append("- Sin descartes registrados.")
        lines.extend(["", "## Calidad y límites", ""])
        lines.append(f"- Calidad: `{json.dumps(data.get('data_quality', {}), ensure_ascii=False, sort_keys=True)}`")
        lines.append(f"- Resoluciones: `{json.dumps(data.get('resolutions', {}), ensure_ascii=False, sort_keys=True)}`")
        for limitation in data.get("limitations", []):
            lines.append(f"- {limitation}")
        return "\n".join(lines) + "\n"

    def to_html(self, data: Mapping[str, Any]) -> str:
        # This is deliberately a static local report.  The interactive UI is
        # served separately and reads the same persisted tables.
        title = html.escape(f"MTF Lab — {data.get('instrument', 'sesión')}")
        md = html.escape(self.to_markdown(data))
        horizon_rows = "".join(
            f"<tr><td>{html.escape(str(h))}</td><td><code>{html.escape(json.dumps(v.get('outcomes', {}), ensure_ascii=False, sort_keys=True))}</code></td><td>{float(v.get('net_result', 0)):.4g}</td></tr>"
            for h, v in sorted((data.get("by_horizon") or {}).items())
        ) or '<tr><td colspan="3">Sin simulaciones</td></tr>'
        return f"""<!doctype html>
<html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title}</title><style>body{{font-family:system-ui,sans-serif;max-width:960px;margin:2rem auto;padding:0 1rem;color:#182230}}table{{border-collapse:collapse;width:100%}}th,td{{border:1px solid #ccd5df;padding:.45rem;text-align:left}}th{{background:#eef3f8}}.mode{{font-weight:700}}pre{{white-space:pre-wrap;background:#f7f8fa;padding:1rem}}</style></head>
<body><h1>{title}</h1><p class="mode">Modo: {html.escape(str(data.get('mode', 'UNKNOWN')))} · Proveedor: {html.escape(str(data.get('provider', 'UNKNOWN')))}</p>
<p>Generado: {html.escape(str(data.get('generated_at', 'UNKNOWN')))}</p><h2>Por horizonte</h2><table><thead><tr><th>Horizonte (s)</th><th>Resultados</th><th>Neto virtual</th></tr></thead><tbody>{horizon_rows}</tbody></table>
<h2>Informe legible</h2><pre>{md}</pre></body></html>"""

    def write(self, output: str | Path, *, format: str = "markdown", data: Mapping[str, Any] | None = None) -> Path:
        target = Path(output).expanduser()
        target.parent.mkdir(parents=True, exist_ok=True)
        data = dict(data or self.summary())
        fmt = format.lower()
        if fmt in {"json", ".json"}:
            content = self.to_json(data)
        elif fmt in {"html", ".html", "htm", ".htm"}:
            content = self.to_html(data)
        elif fmt in {"md", "markdown", ".md"}:
            content = self.to_markdown(data)
        else:
            raise ValueError("format must be json, markdown or html")
        target.write_text(content, encoding="utf-8")
        return target
