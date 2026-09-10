"""Auditable reports sourced from persisted records.

Reporting is a projection only: it groups already-settled virtual simulation
rows and displays their recorded assumptions.  It never chooses a price,
recalculates an outcome, or calls a strategy.  The segmented output keeps
analysis, variant, instrument, horizon, partition and contract dimensions
separate so comparisons do not silently mix unlike runs.
"""

from __future__ import annotations

import dataclasses
import html
import json
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


_MODE_LABELS = {
    "SYNTHETIC": "SINTETICO",
    "LIVE": "OBSERVACIÓN EN DIRECTO",
    "REPLAY": "REPLAY",
    "BACKTEST": "BACKTEST",
}


def _json_default(value: Any) -> Any:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return dataclasses.asdict(value)
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return dataclasses.asdict(value)
    if hasattr(value, "to_dict"):
        return dict(value.to_dict())
    if hasattr(value, "__dict__"):
        return {key: val for key, val in vars(value).items() if not key.startswith("_")}
    return {}


def _json(value: Any) -> Any:
    if isinstance(value, (dict, list, tuple)):
        return value
    try:
        return json.loads(value) if value is not None else {}
    except (TypeError, json.JSONDecodeError):
        return {}


def _horizon(value: Any) -> str:
    try:
        number = float(value)
        return str(int(number)) if number.is_integer() else f"{number:g}"
    except (TypeError, ValueError):
        return str(value or "UNKNOWN")


def _outcome(value: Any) -> str:
    return getattr(value, "value", str(value)).upper()


def _net(value: Any) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def _break_even(row: Mapping[str, Any]) -> float | None:
    assumptions = row.get("assumptions") if isinstance(row.get("assumptions"), Mapping) else _json(row.get("assumptions_json"))
    contract = assumptions.get("virtual_contract") if isinstance(assumptions, Mapping) and isinstance(assumptions.get("virtual_contract"), Mapping) else {}
    payout = contract.get("payout_net")
    loss_amount = contract.get("loss_amount", 1.0)
    stake = contract.get("stake", row.get("stake"))
    costs = contract.get("costs", 0)
    try:
        payout = float(payout); loss_amount = float(loss_amount); stake = float(stake); costs = float(costs)
    except (TypeError, ValueError):
        return None
    # Contract break-even with no ties: p*(stake*payout-costs) +
    # (1-p)*(-stake*loss-costs) = 0.  This reduces to 1/(1+payout)
    # for stake=loss=1 and zero costs, the documented illustrative case.
    denominator = stake * (payout + loss_amount)
    return (stake * loss_amount + costs) / denominator if denominator > 0 else None


def _aggregate(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    outcomes = Counter(_outcome(row.get("outcome", "UNKNOWN")) for row in rows)
    nets = [_net(row.get("net_result")) for row in rows]
    resolved = sum(outcomes.get(name, 0) for name in ("WIN", "LOSS", "TIE"))
    equity = 0.0; peak = 0.0; max_drawdown = 0.0
    for row in sorted(rows, key=lambda item: (str(item.get("detected_ts", "")), int(item.get("simulation_row_id", 0) or 0))):
        net = _net(row.get("net_result"))
        if net is None: continue
        equity += net; peak = max(peak, equity); max_drawdown = max(max_drawdown, peak - equity)
    be_values = [value for value in (_break_even(row) for row in rows) if value is not None]
    return {
        "sample_count": len(rows),
        "outcomes": {key: outcomes.get(key, 0) for key in ("WIN", "LOSS", "TIE", "INDETERMINATE", "PENDING") if outcomes.get(key, 0) or key in {"WIN", "LOSS", "TIE", "INDETERMINATE", "PENDING"}},
        "resolved_count": resolved,
        "indeterminate_count": outcomes.get("INDETERMINATE", 0) + outcomes.get("PENDING", 0),
        "pending_count": outcomes.get("PENDING", 0),
        "net_result": sum(value for value in nets if value is not None),
        "gross_wins": sum(value for value in nets if value is not None and value > 0),
        "gross_losses": sum(value for value in nets if value is not None and value < 0),
        "max_drawdown": max_drawdown if any(value is not None for value in nets) else None,
        "win_rate_on_resolved": outcomes.get("WIN", 0) / resolved if resolved else None,
        "break_even_probability": (sum(be_values) / len(be_values)) if be_values else None,
    }


class ReportBuilder:
    """Build JSON, Markdown and HTML reports from one SQLite read model."""

    def __init__(self, store: Any | None = None, session_id: str | None = None):
        self.store = store
        self.session_id = session_id

    def _dimensions(self, row: Mapping[str, Any], session: Mapping[str, Any], signal_instruments: Mapping[str, str]) -> dict[str, str]:
        payload = row.get("payload") if isinstance(row.get("payload"), Mapping) else _json(row.get("payload_json"))
        if isinstance(payload, Mapping) and not any(key in payload for key in ("analysis", "analysis_name", "variant", "variant_name", "instrument", "partition", "contract")) and isinstance(payload.get("payload"), Mapping):
            payload = payload["payload"]
        assumptions = row.get("assumptions") if isinstance(row.get("assumptions"), Mapping) else _json(row.get("assumptions_json"))
        config = session.get("config") if isinstance(session.get("config"), Mapping) else _json(session.get("config_json"))
        metadata = session.get("metadata") if isinstance(session.get("metadata"), Mapping) else _json(session.get("metadata_json"))
        variant = payload.get("variant") or payload.get("variant_name") or row.get("variant") or str(row.get("simulation_id", "UNKNOWN")).split(":", 1)[0]
        analysis = payload.get("analysis") or payload.get("analysis_name") or payload.get("strategy") or config.get("strategy") or "UNKNOWN"
        instrument = payload.get("instrument") or signal_instruments.get(str(row.get("signal_id"))) or row.get("instrument") or session.get("instrument") or "UNKNOWN"
        partition = payload.get("partition") or assumptions.get("partition") or metadata.get("partition") or "UNKNOWN"
        contract = payload.get("contract") or assumptions.get("contract")
        if isinstance(contract, Mapping):
            contract = contract.get("name") or contract.get("type")
        contract = contract or row.get("simulation_type") or "UNKNOWN"
        return {"analysis": str(analysis), "variant": str(variant), "instrument": str(instrument), "horizon": _horizon(row.get("horizon_seconds")), "partition": str(partition), "contract": str(contract)}

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
            status = dict(session); signals = []; discards = []; simulations = []; candles = []
        # A caller commonly passes freshly returned BacktestResult objects after
        # the runner has already persisted them.  Merge by identity, never by
        # list position, to keep counts and segments stable after a restart.
        if results is not None:
            existing = {str(item.get("simulation_id")) for item in simulations if item.get("simulation_id") is not None}
            for result in results:
                data = result.to_dict(include_simulations=True) if hasattr(result, "to_dict") else _mapping(result)
                for simulation in data.get("simulations", []):
                    sim = _mapping(simulation)
                    key = str(sim.get("simulation_id")) if sim.get("simulation_id") is not None else None
                    if key is None or key not in existing:
                        simulations.append(sim)
                        if key is not None: existing.add(key)
        signal_instruments = {str(row.get("signal_id")): str(row.get("instrument", "UNKNOWN")) for row in signals}
        segmented: defaultdict[tuple[str, str, str, str, str, str], list[Mapping[str, Any]]] = defaultdict(list)
        enriched: list[dict[str, Any]] = []
        for simulation in simulations:
            row = dict(simulation)
            row["dimensions"] = self._dimensions(row, session, signal_instruments)
            dimensions = row["dimensions"]
            segmented[tuple(dimensions[key] for key in ("analysis", "variant", "instrument", "horizon", "partition", "contract"))].append(row)
            enriched.append(row)
        segments: list[dict[str, Any]] = []
        for key in sorted(segmented):
            dimensions = dict(zip(("analysis", "variant", "instrument", "horizon", "partition", "contract"), key))
            segments.append({"dimensions": dimensions, **_aggregate(segmented[key])})
        all_aggregate = _aggregate(enriched)
        # Compatibility projections are still useful for terminal consumers,
        # but each is derived from the same six-dimensional rows above.
        def projection(index: str) -> dict[str, Any]:
            grouped: defaultdict[str, list[Mapping[str, Any]]] = defaultdict(list)
            for row in enriched: grouped[row["dimensions"][index]].append(row)
            return {name: _aggregate(values) for name, values in sorted(grouped.items())}
        outcomes = Counter(_outcome(x.get("outcome", "UNKNOWN")) for x in simulations)
        discard_reasons = Counter(str(x.get("reason_code", "UNKNOWN")) for x in discards)
        quality = Counter(str(x.get("quality", "UNKNOWN")) for x in candles)
        resolutions = Counter(str(x.get("timeframe", x.get("resolution", "UNKNOWN"))) for x in candles)
        mode = session.get("mode", status.get("mode", "UNKNOWN")); mode_label = _MODE_LABELS.get(str(mode).upper(), str(mode))
        return {
            "generated_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
            "schema": "mtf-lab.report.v1",
            "session": session,
            "status": status,
            "mode": mode,
            "mode_label": mode_label,
            "provider": session.get("provider", status.get("provider", "UNKNOWN")),
            "instrument": session.get("instrument", status.get("instrument", "UNKNOWN")),
            "counts": {"signals": len(signals), "discards": len(discards), "simulations": len(simulations), "candles": len(candles)},
            "outcomes": dict(outcomes),
            "discard_reasons": dict(discard_reasons),
            "aggregate": all_aggregate,
            "segments": segments,
            "by_analysis": projection("analysis"),
            "by_variant": projection("variant"),
            "by_instrument": projection("instrument"),
            "by_horizon": projection("horizon"),
            "by_partition": projection("partition"),
            "by_contract": projection("contract"),
            "data_quality": dict(quality),
            "resolutions": dict(resolutions),
            "limitations": [
                "Los resultados son simulaciones virtuales y no constituyen órdenes ni recomendación.",
                "La tasa de acierto no es una probabilidad de éxito; señales cercanas pueden depender entre sí.",
                "La muestra bruta y las particiones no eliminan dependencia entre señales cercanas.",
                "Las velas no permiten inferir movimientos intrabar que no estén observados.",
                "La ausencia de una fuente de noticias o de un bróker no se interpreta como comprobación.",
                "Una simulación INDETERMINATE o PENDING no se cuenta como ganancia, pérdida ni operación resuelta.",
            ],
        }

    def to_json(self, data: Mapping[str, Any], *, pretty: bool = True) -> str:
        return json.dumps(data, ensure_ascii=False, indent=2 if pretty else None, sort_keys=True, default=_json_default)

    def to_markdown(self, data: Mapping[str, Any]) -> str:
        status = data.get("status", {}); counts = data.get("counts", {}); aggregate = data.get("aggregate", {})
        lines = [
            "# MTF Lab — informe de ejecución", "",
            f"- Generado: `{data.get('generated_at', 'UNKNOWN')}`",
            f"- Modo: **{data.get('mode', 'UNKNOWN')}** (`{data.get('mode_label', data.get('mode', 'UNKNOWN'))}`)",
            f"- Proveedor: `{data.get('provider', 'UNKNOWN')}`", f"- Instrumento de sesión: `{data.get('instrument', 'UNKNOWN')}`",
            f"- Estado de sesión: `{status.get('status', 'UNKNOWN')}`", "",
            "## Conteos persistidos", "", "| Señales | Descartes | Simulaciones | Velas |", "|---:|---:|---:|---:|",
            f"| {counts.get('signals', 0)} | {counts.get('discards', 0)} | {counts.get('simulations', 0)} | {counts.get('candles', 0)} |", "",
            "## Agregado de simulaciones", "", f"- Resultado neto virtual: `{aggregate.get('net_result', 0):.6g}`", f"- Resueltos: `{aggregate.get('resolved_count', 0)}`; indeterminados/pending: `{aggregate.get('indeterminate_count', 0)}` (PENDING: `{aggregate.get('pending_count', 0)}`)", f"- Caída máxima de la secuencia registrada: `{aggregate.get('max_drawdown') if aggregate.get('max_drawdown') is not None else '—'}`", "",
            "## Segmentos controlados", "", "| Análisis | Variante | Instrumento | Horizonte s | Partición | Contrato | N | W/L/T/I/P | Neto | DD |", "|---|---|---|---:|---|---|---:|---|---:|---:|",
        ]
        for segment in data.get("segments", []):
            dim = segment.get("dimensions", {}); out = segment.get("outcomes", {})
            wlti = f"{out.get('WIN', 0)}/{out.get('LOSS', 0)}/{out.get('TIE', 0)}/{out.get('INDETERMINATE', 0)}/{out.get('PENDING', 0)}"
            dd = segment.get("max_drawdown"); dd_text = "—" if dd is None else f"{float(dd):.6g}"
            lines.append(f"| `{dim.get('analysis')}` | `{dim.get('variant')}` | `{dim.get('instrument')}` | {dim.get('horizon')} | `{dim.get('partition')}` | `{dim.get('contract')}` | {segment.get('sample_count', 0)} | {wlti} | {float(segment.get('net_result', 0)):.6g} | {dd_text} |")
        if not data.get("segments"): lines.append("| — | — | — | — | — | — | 0 | 0/0/0/0/0 | 0 | — |")
        lines.extend(["", "## Motivos de descarte", ""])
        for reason, count in sorted((data.get("discard_reasons") or {}).items()): lines.append(f"- `{reason}`: {count}")
        if not data.get("discard_reasons"): lines.append("- Sin descartes registrados.")
        lines.extend(["", "## Calidad y límites", "", f"- Calidad: `{json.dumps(data.get('data_quality', {}), ensure_ascii=False, sort_keys=True)}`", f"- Resoluciones: `{json.dumps(data.get('resolutions', {}), ensure_ascii=False, sort_keys=True)}`"])
        lines.extend(f"- {item}" for item in data.get("limitations", []))
        return "\n".join(lines) + "\n"

    def to_html(self, data: Mapping[str, Any]) -> str:
        title = html.escape(f"MTF Lab — {data.get('instrument', 'sesión')}")
        rows = []
        for segment in data.get("segments", []):
            dim = segment.get("dimensions", {}); out = segment.get("outcomes", {})
            rows.append("<tr>" + "".join(f"<td>{html.escape(str(value))}</td>" for value in (dim.get("analysis"), dim.get("variant"), dim.get("instrument"), dim.get("horizon"), dim.get("partition"), dim.get("contract"), segment.get("sample_count", 0), f"{out.get('WIN', 0)}/{out.get('LOSS', 0)}/{out.get('TIE', 0)}/{out.get('INDETERMINATE', 0)}/{out.get('PENDING', 0)}", f"{float(segment.get('net_result', 0)):.6g}")) + "</tr>")
        table = "".join(rows) or '<tr><td colspan="9">Sin simulaciones</td></tr>'
        md = html.escape(self.to_markdown(data))
        return f'''<!doctype html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{title}</title><style>body{{font-family:system-ui,sans-serif;max-width:1200px;margin:2rem auto;padding:0 1rem;color:#182230}}table{{border-collapse:collapse;width:100%;font-size:.9rem}}th,td{{border:1px solid #ccd5df;padding:.4rem;text-align:left}}th{{background:#eef3f8}}.mode{{font-weight:700}}pre{{white-space:pre-wrap;background:#f7f8fa;padding:1rem}}</style></head><body><h1>{title}</h1><p class="mode">Modo: {html.escape(str(data.get("mode_label", data.get("mode", "UNKNOWN"))))} · Proveedor: {html.escape(str(data.get("provider", "UNKNOWN")))}</p><p>Generado: {html.escape(str(data.get("generated_at", "UNKNOWN")))}</p><h2>Segmentos controlados</h2><table><thead><tr><th>Análisis</th><th>Variante</th><th>Instrumento</th><th>Horizonte s</th><th>Partición</th><th>Contrato</th><th>N</th><th>W/L/T/I/P</th><th>Neto</th></tr></thead><tbody>{table}</tbody></table><h2>Informe legible</h2><pre>{md}</pre></body></html>'''

    def write(self, output: str | Path, *, format: str = "markdown", data: Mapping[str, Any] | None = None) -> Path:
        target = Path(output).expanduser(); target.parent.mkdir(parents=True, exist_ok=True); data = dict(data or self.summary()); fmt = format.lower()
        if fmt in {"json", ".json"}: content = self.to_json(data)
        elif fmt in {"html", ".html", "htm", ".htm"}: content = self.to_html(data)
        elif fmt in {"md", "markdown", ".md"}: content = self.to_markdown(data)
        else: raise ValueError("format must be json, markdown or html")
        target.write_text(content, encoding="utf-8"); return target
