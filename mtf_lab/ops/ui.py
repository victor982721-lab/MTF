"""Read-only local HTTP UI for persisted MTF Lab data.

The UI delegates all ranges, pagination, revision selection, gap detection and
condition projection to :mod:`mtf_lab.ops.query`.  It contains no indicator or
settlement logic of its own and never writes to SQLite.
"""

from __future__ import annotations

import json
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse
from typing import Any

from .persistence import SQLiteStore
from .query import QueryPage, QueryService
from .reporting import ReportBuilder


INDEX_HTML = r'''<!doctype html>
<html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>MTF Lab</title><style>
body{font-family:system-ui,sans-serif;max-width:1280px;margin:1rem auto;padding:0 1rem;color:#182230;background:#fafbfc}
header{display:flex;gap:1rem;align-items:center;flex-wrap:wrap}h1{margin:.2rem 0}button,select,input{font:inherit;padding:.3rem}.pill{padding:.2rem .5rem;border-radius:1rem;background:#e8eef5}.warn{background:#fff2cf}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:.7rem}.card{background:white;border:1px solid #d7dfe8;border-radius:.5rem;padding:.7rem}.value{font-size:1.4rem;font-weight:700}table{border-collapse:collapse;width:100%;font-size:.88rem}th,td{border:1px solid #d7dfe8;padding:.35rem;text-align:left;vertical-align:top}th{background:#eef3f8;position:sticky;top:0}.chart{width:100%;height:180px;border:1px solid #d7dfe8;background:#fff}.line{fill:none;stroke-width:1.6}.close{stroke:#2b6cb0}.emaFast{stroke:#d97706}.emaSlow{stroke:#7c3aed}.rsi{stroke:#059669}.atr{stroke:#be123c}.scroll{overflow:auto;max-height:24rem}code{font-size:.82em}pre{white-space:pre-wrap;max-height:18rem;overflow:auto}.controls{display:flex;gap:.5rem;flex-wrap:wrap;align-items:end}.controls label{display:flex;flex-direction:column;font-size:.85rem}.section{margin-top:1.2rem}.muted{color:#657487}.pager{display:flex;gap:.5rem;align-items:center;margin:.4rem 0}.ok{color:#076b3b}.bad{color:#a32020}
</style></head><body><header><h1>MTF Lab</h1><span id="mode" class="pill">cargando…</span><label>Sesión <select id="session"></select></label></header>
<p class="muted">Consulta local de velas, indicadores, señales, condiciones, descartes, revisiones, huecos y simulaciones. Sólo lectura; no envía órdenes.</p>
<div class="controls"><label>Temporalidad<select id="timeframe"><option value="">Todas</option><option>M1</option><option>M5</option><option>M15</option></select></label><label>Desde UTC<input id="start" placeholder="2025-01-01T00:00:00Z"></label><label>Hasta UTC (exclusivo)<input id="end" placeholder="2025-01-02T00:00:00Z"></label><label>Velas<select id="revisions"><option value="latest">Última revisión</option><option value="all">Todas las revisiones</option></select></label><button id="apply">Actualizar</button></div>
<section id="summary" class="grid section"></section>
<section class="section"><h2>Gráficos sincronizados (datos persistidos)</h2><p class="muted">Las líneas se dibujan con los valores EMA/RSI/ATR registrados; el navegador no recalcula la estrategia.</p><div id="indicatorCharts" class="grid"></div></section>
<section class="section"><h2>Velas M1/M5/M15 e indicadores</h2><p class="muted">Se conserva la distinción entre vela cerrada/abierta y revisión; los indicadores provienen del registro persistido.</p><div id="candles" class="grid"></div><div id="candlePager" class="pager"></div></section>
<section class="section"><h2>Huecos y discontinuidades observados</h2><div id="gaps" class="scroll"></div></section>
<section class="section"><h2>Señales y condiciones</h2><div id="signals" class="scroll"></div><div id="conditions" class="scroll"></div></section>
<section class="section"><h2>Descartes</h2><div id="discards" class="scroll"></div></section>
<section class="section"><h2>Simulaciones virtuales segmentadas</h2><div id="sims" class="scroll"></div></section>
<script>
const esc=x=>String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const json=x=>typeof x==='object'?JSON.stringify(x):x;
async function get(path){let r=await fetch(path);if(!r.ok)throw Error(await r.text());return r.json()}
function rows(value){return Array.isArray(value)?value:(value&&Array.isArray(value.items)?value.items:[])}
function table(items, cols){if(!items.length)return '<p>Sin registros persistidos.</p>';return '<table><thead><tr>'+cols.map(c=>'<th>'+esc(c[1])+'</th>').join('')+'</tr></thead><tbody>'+items.map(x=>'<tr>'+cols.map(c=>'<td>'+esc(json(x[c[0]]))+'</td>').join('')+'</tr>').join('')+'</tbody></table>'}
function qs(extra={}){let sid=document.getElementById('session').value, q={session:sid,timeframe:document.getElementById('timeframe').value,start_ts:document.getElementById('start').value,end_ts:document.getElementById('end').value,revisions:document.getElementById('revisions').value,...extra};return Object.entries(q).filter(([,v])=>v!==''&&v!=null).map(([k,v])=>encodeURIComponent(k)+'='+encodeURIComponent(v)).join('&')}
function pathFor(points,key,w=520,h=150){let vals=points.map(x=>{let z=x.indicator_values&&x.indicator_values[key];return z==null?x[key]:z}).filter(v=>typeof v==='number'&&Number.isFinite(v));if(!vals.length)return '';let min=Math.min(...vals),max=Math.max(...vals);if(max===min)max=min+1;let n=points.length;return points.map((x,i)=>{let v=x.indicator_values&&x.indicator_values[key];if(v==null)v=x[key];if(typeof v!=='number'||!Number.isFinite(v))return '';let xx=n<=1?w/2:i*w/(n-1),yy=h-(v-min)/(max-min)*h;return (i?'L':'M')+xx.toFixed(1)+' '+yy.toFixed(1)}).filter(Boolean).join(' ')}
async function drawCharts(){let root=document.getElementById('indicatorCharts');let tfs=['M1','M5','M15'];let data=await Promise.all(tfs.map(async tf=>{try{return [tf,rows(await get('/api/indicators?'+qs({timeframe:tf,limit:200})))]}catch(e){return [tf,[]]}}));root.innerHTML=data.map(([tf,p])=>{let q=p.slice(-120),svg='<svg class="chart" viewBox="0 0 520 150" role="img" aria-label="'+esc(tf)+' EMA RSI ATR">';[['close','close'],['ema_fast','emaFast'],['ema_slow','emaSlow']].forEach(([k,c])=>{let d=pathFor(q,k);if(d)svg+='<path class="line '+c+'" d="'+d+'"/>'});svg+='</svg>';let osc='<svg class="chart" viewBox="0 0 520 90" role="img" aria-label="'+esc(tf)+' RSI ATR persistidos">';[['rsi','rsi'],['atr','atr']].forEach(([k,c])=>{let d=pathFor(q,k,520,80);if(d)osc+='<path class="line '+c+'" d="'+d+'"/>'});osc+='</svg><p class="muted">'+esc(tf)+' · precio/EMA y RSI/ATR persistidos</p>';return '<div class="card">'+svg+osc+'</div>'}).join('')||'<p>Sin indicadores persistidos.</p>'}

async function load(){let sid=document.getElementById('session').value;if(!sid)return;let s=await get('/api/status?session='+encodeURIComponent(sid));document.getElementById('mode').textContent=(s.mode_label||s.mode||'?')+' · '+(s.provider||'?')+' · '+(s.instrument||'?');let c=s.counts||{};document.getElementById('summary').innerHTML=Object.entries(c).map(([k,v])=>'<div class="card"><div>'+esc(k)+'</div><div class="value">'+esc(v)+'</div></div>').join('')+'<div class="card"><div>Conexión</div><div class="value">'+esc(s.connection||'—')+'</div></div><div class="card"><div>Análisis</div><div>'+esc(s.analysis_enabled?'habilitado':'bloqueado')+'</div><div>'+esc((s.analysis_blocked_reasons||[]).join(', ')||'—')+'</div></div><div class="card"><div>Calentamiento</div><div>'+esc(JSON.stringify(s.warmup_pending||{}))+'</div></div><div class="card"><div>Pendientes</div><div class="value">'+esc(s.pending_simulations||0)+'</div></div><div class="card"><div>Último evento</div><div>'+esc(s.last_event_ts||'—')+'</div></div><div class="card"><div>Gaps/revisiones</div><div class="value">'+esc((s.gaps||[]).length)+' / '+esc(s.revisions_count||0)+'</div></div>';
let cp=await get('/api/candles?'+qs({limit:200}));let candles=rows(cp), groups={};candles.forEach(x=>(groups[x.timeframe]??=[]).push(x));document.getElementById('candles').innerHTML=Object.entries(groups).map(([tf,a])=>'<div class="card"><h3>'+esc(tf)+'</h3>'+table(a.slice(-30),[['start_ts','Inicio'],['end_ts','Fin'],['close','Cierre'],['closed','Cerrada'],['revision','Rev.'],['is_latest_revision','Última'],['quality','Calidad'],['indicator_values','EMA/RSI/ATR']])+'</div>').join('')||'<p>Sin velas.</p>';document.getElementById('candlePager').innerHTML=cp.next_cursor?'<button id="moreCandles">Cargar más</button>':'<span class="muted">Fin de velas</span>';if(cp.next_cursor)document.getElementById('moreCandles').onclick=()=>loadMoreCandles(cp.next_cursor);await drawCharts();
let gaps=await get('/api/gaps?'+qs({}));document.getElementById('gaps').innerHTML=table(rows(gaps),[['timeframe','Temporalidad'],['gap_start','Inicio hueco'],['gap_end','Fin hueco'],['duration_seconds','Duración s'],['filled','Rellenado'],['quality','Calidad']]);
document.getElementById('signals').innerHTML=table(rows(await get('/api/signals?'+qs({limit:500})), [['detected_ts','Detectada'],['direction','Dirección'],['status','Estado'],['episode_id','Episodio'],['payload','Detalle']]));
document.getElementById('conditions').innerHTML='<h3>Condiciones</h3>'+table(rows(await get('/api/conditions?'+qs({limit:1000})), [['observed_ts','Observada'],['decision','Decisión'],['name','Condición'],['state','Estado'],['observed','Observado'],['expected','Esperado'],['reason','Razón'],['mandatory','Obligatoria']]));
document.getElementById('discards').innerHTML=table(rows(await get('/api/discards?'+qs({limit:500})), [['observed_ts','Observada'],['reason_code','Razón'],['required','Obligatoria'],['condition_status','Condición'],['payload','Detalle']]));
document.getElementById('sims').innerHTML=table(rows(await get('/api/simulations?'+qs({limit:1000})), [['detected_ts','Detectada'],['horizon_seconds','Horizonte s'],['direction','Dirección'],['outcome','Resultado'],['net_result','Neto virtual'],['quality','Calidad'],['dimensions','Análisis/variante/contrato']]));}
async function loadMoreCandles(cursor){let p=await get('/api/candles?'+qs({limit:200,cursor}));let old=window._candleItems||[];window._candleItems=old.concat(rows(p));document.getElementById('candlePager').innerHTML=p.next_cursor?'<button id="moreCandles">Cargar más</button>':'<span class="muted">Fin de velas</span>';if(p.next_cursor)document.getElementById('moreCandles').onclick=()=>loadMoreCandles(p.next_cursor);}
(async()=>{let a=await get('/api/sessions');let list=rows(a);let s=document.getElementById('session');s.innerHTML=list.map(x=>'<option value="'+esc(x.session_id)+'">'+esc(x.session_id.slice(0,12)+' · '+x.mode+' · '+x.instrument)+'</option>').join('');s.onchange=load;document.getElementById('apply').onclick=load;await load();setInterval(()=>load().catch(()=>{}),5000)})().catch(e=>document.body.insertAdjacentHTML('beforeend','<pre class="bad">'+esc(e)+'</pre>'));
</script></body></html>'''


def _json_bytes(data: Any) -> bytes:
    return json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")


def _bool_param(value: str | None) -> bool | None:
    if value is None or value == "":
        return None
    if value.lower() in {"1", "true", "yes", "si", "sí"}:
        return True
    if value.lower() in {"0", "false", "no"}:
        return False
    raise ValueError("parámetro booleano inválido")


class _Handler(BaseHTTPRequestHandler):
    server: "MTFHTTPServer"

    def log_message(self, fmt: str, *args: Any) -> None:
        return

    def _send(self, data: Any, *, status: int = HTTPStatus.OK, content_type: str = "application/json; charset=utf-8") -> None:
        body = data.encode("utf-8") if isinstance(data, str) else _json_bytes(data)
        self.send_response(status); self.send_header("Content-Type", content_type); self.send_header("Content-Length", str(len(body))); self.send_header("Cache-Control", "no-store"); self.end_headers(); self.wfile.write(body)

    def _common(self, query: dict[str, list[str]]) -> dict[str, Any]:
        def one(name: str, default: Any = None) -> Any: return query.get(name, [default])[0]
        try:
            requested_limit = int(one("limit", 100))
        except (TypeError, ValueError) as exc:
            raise ValueError("limit debe ser entero") from exc
        return {"start_ts": one("start_ts", one("start")), "end_ts": one("end_ts", one("end")), "instrument": one("instrument"), "timeframe": one("timeframe"), "limit": max(1, min(1000, requested_limit)), "recent": _bool_param(one("recent", "0")) or False, "cursor": one("cursor"), "revisions": one("revisions", "latest"), "closed": _bool_param(one("closed"))}

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
        parsed = urlparse(self.path); query = parse_qs(parsed.query, keep_blank_values=True)
        if parsed.path in {"/", "/index.html"}:
            self._send(INDEX_HTML, content_type="text/html; charset=utf-8"); return
        if parsed.path == "/api/health": self._send({"ok": True, "service": "mtf-lab-ui", "read_only": True}); return
        if parsed.path == "/api/sessions":
            try: session_limit = max(1, min(100, int(query.get("limit", [50])[0])))
            except (TypeError, ValueError): session_limit = 50
            self._send(self.server.store.sessions(limit=session_limit)); return
        sid = query.get("session", [self.server.default_session])[0]
        if not sid:
            self._send({"error": "session query parameter is required"}, status=HTTPStatus.BAD_REQUEST); return
        try:
            common = self._common(query)
            if parsed.path == "/api/status":
                data = self.server.queries.snapshot(sid)
                data["mode_label"] = {"SYNTHETIC": "SINTETICO", "LIVE": "OBSERVACIÓN EN DIRECTO", "REPLAY": "REPLAY", "BACKTEST": "BACKTEST"}.get(str(data.get("mode", "")).upper(), data.get("mode", "UNKNOWN"))
            elif parsed.path == "/api/candles": data = self.server.queries.query_candles(sid, **common).to_dict()
            elif parsed.path == "/api/indicators": data = self.server.queries.query_indicators(sid, **common).to_dict()
            elif parsed.path == "/api/revisions": data = self.server.queries.query_revisions(sid, **common).to_dict()
            elif parsed.path == "/api/events": data = self.server.queries.query_events(sid, **common).to_dict()
            elif parsed.path == "/api/signals": data = self.server.queries.query_signals(sid, **common).to_dict()
            elif parsed.path == "/api/decisions": data = self.server.queries.query_decisions(sid, **common).to_dict()
            elif parsed.path == "/api/discards": data = self.server.queries.query_discards(sid, **common).to_dict()
            elif parsed.path == "/api/conditions": data = self.server.queries.query_conditions(sid, start_ts=common["start_ts"], end_ts=common["end_ts"], recent=common["recent"], limit=common["limit"], cursor=common["cursor"]).to_dict()
            elif parsed.path == "/api/gaps": data = {"items": self.server.queries.query_gaps(sid, timeframe=common["timeframe"], instrument=common["instrument"], start_ts=common["start_ts"], end_ts=common["end_ts"], revisions=common["revisions"], include_open=common["closed"] is not True), "limit": common["limit"]}
            elif parsed.path == "/api/simulations":
                data = self.server.queries.query_simulations(sid, analysis=query.get("analysis", [None])[0], variant=query.get("variant", [None])[0], partition=query.get("partition", [None])[0], contract=query.get("contract", [None])[0], horizon_seconds=float(query["horizon_seconds"][0]) if query.get("horizon_seconds") else None, instrument=query.get("instrument", [None])[0], start_ts=common["start_ts"], end_ts=common["end_ts"], recent=common["recent"], limit=common["limit"], cursor=common["cursor"]).to_dict()
            elif parsed.path == "/api/poll": data = self.server.queries.poll(sid, limit=common["limit"])
            elif parsed.path == "/api/report": data = ReportBuilder(self.server.store, sid).summary()
            elif parsed.path == "/api/query":
                kind = query.get("kind", [""])[0]
                if kind == "simulations":
                    data = self.server.queries.query_simulations(sid, analysis=query.get("analysis", [None])[0], variant=query.get("variant", [None])[0], partition=query.get("partition", [None])[0], contract=query.get("contract", [None])[0], horizon_seconds=float(query["horizon_seconds"][0]) if query.get("horizon_seconds") else None, instrument=query.get("instrument", [None])[0], start_ts=common["start_ts"], end_ts=common["end_ts"], recent=common["recent"], limit=common["limit"], cursor=common["cursor"]).to_dict()
                else:
                    dispatch = {"events": self.server.queries.query_events, "candles": self.server.queries.query_candles, "signals": self.server.queries.query_signals, "decisions": self.server.queries.query_decisions, "discards": self.server.queries.query_discards}
                    if kind not in dispatch: raise ValueError("kind debe ser events/candles/signals/decisions/discards/simulations")
                    data = dispatch[kind](sid, **common).to_dict()
            else:
                self._send({"error": "not found"}, status=HTTPStatus.NOT_FOUND); return
        except (ValueError, TypeError) as exc:
            self._send({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST); return
        except Exception as exc:
            self._send({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR); return
        self._send(data)


class MTFHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], store: SQLiteStore, *, default_session: str | None = None):
        super().__init__(address, _Handler); self.store = store; self.queries = QueryService(store); self.default_session = default_session


def create_server(db_path: str | Path, *, host: str = "127.0.0.1", port: int = 8765, session_id: str | None = None) -> MTFHTTPServer:
    store = SQLiteStore(db_path, read_only=True)
    if session_id is None:
        sessions = store.sessions(limit=1); session_id = sessions[0]["session_id"] if sessions else None
    return MTFHTTPServer((host, int(port)), store, default_session=session_id)


def serve(db_path: str | Path, *, host: str = "127.0.0.1", port: int = 8765, session_id: str | None = None, duration: float | None = None) -> MTFHTTPServer:
    """Serve local read-only UI; optional duration makes smoke tests finite."""
    server = create_server(db_path, host=host, port=port, session_id=session_id)
    if duration is not None:
        timer = threading.Timer(max(0.0, float(duration)), server.shutdown); timer.daemon = True; timer.start()
    try: server.serve_forever(poll_interval=0.2)
    finally: server.server_close(); server.store.close()
    return server


__all__ = ["MTFHTTPServer", "create_server", "serve"]
