"""Read-only local HTTP UI for persisted MTF Lab results.

The UI has no strategy or indicator code.  Every table is retrieved from
SQLite through ``SQLiteStore`` and is marked with mode/provider/quality so a
synthetic run cannot look like a live quote feed.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Mapping
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from typing import Any

from .persistence import SQLiteStore
from .reporting import ReportBuilder


INDEX_HTML = """<!doctype html>
<html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>MTF Lab</title><style>
body{font-family:system-ui,sans-serif;max-width:1200px;margin:1rem auto;padding:0 1rem;color:#182230;background:#fafbfc}
header{display:flex;gap:1rem;align-items:center;flex-wrap:wrap}h1{margin:.2rem 0}button,select{font:inherit;padding:.3rem}.pill{padding:.2rem .5rem;border-radius:1rem;background:#e8eef5}.warn{background:#fff2cf}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:.7rem}.card{background:white;border:1px solid #d7dfe8;border-radius:.5rem;padding:.7rem}.value{font-size:1.4rem;font-weight:700}table{border-collapse:collapse;width:100%;font-size:.9rem}th,td{border:1px solid #d7dfe8;padding:.35rem;text-align:left}th{background:#eef3f8;position:sticky;top:0}.scroll{overflow:auto;max-height:24rem}code{font-size:.85em}pre{white-space:pre-wrap}
</style></head><body><header><h1>MTF Lab</h1><span id="mode" class="pill">cargando…</span><label>Sesión <select id="session"></select></label></header>
<p>Consulta local de velas, indicadores/señales, descartes y simulaciones. Sólo lectura; no envía órdenes.</p>
<section id="summary" class="grid"></section><h2>Velas por temporalidad</h2><div id="candles" class="grid"></div>
<h2>Señales</h2><div id="signals" class="scroll"></div><h2>Descartes</h2><div id="discards" class="scroll"></div><h2>Simulaciones virtuales</h2><div id="sims" class="scroll"></div>
<script>
const esc=x=>String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
async function get(path){let r=await fetch(path);if(!r.ok)throw Error(await r.text());return r.json()}
function table(rows, cols){if(!rows.length)return '<p>Sin registros persistidos.</p>';return '<table><thead><tr>'+cols.map(c=>'<th>'+esc(c[1])+'</th>').join('')+'</tr></thead><tbody>'+rows.map(x=>'<tr>'+cols.map(c=>'<td>'+esc(typeof x[c[0]]==='object'?JSON.stringify(x[c[0]]):x[c[0]])+'</td>').join('')+'</tr>').join('')+'</tbody></table>'}
async function load(){let sid=document.getElementById('session').value;if(!sid)return;let s=await get('/api/status?session='+encodeURIComponent(sid));document.getElementById('mode').textContent=esc((s.mode||'?')+' · '+(s.provider||'?')+' · '+(s.instrument||'?'));let c=s.counts||{};document.getElementById('summary').innerHTML=Object.entries(c).map(([k,v])=>'<div class="card"><div>'+esc(k)+'</div><div class="value">'+esc(v)+'</div></div>').join('')+'<div class="card"><div>Último dato</div><div>'+esc(s.last_event_ts||'—')+'</div></div>';
let candles=await get('/api/candles?session='+encodeURIComponent(sid));let groups={};candles.forEach(x=>(groups[x.timeframe]??=[]).push(x));document.getElementById('candles').innerHTML=Object.entries(groups).map(([tf,a])=>'<div class="card"><h3>'+esc(tf)+'</h3>'+table(a.slice(-20).map(x=>({...x,indicator_summary:(x.provenance&&x.provenance.indicators)||{}})),[['start_ts','Inicio'],['end_ts','Fin'],['close','Cierre'],['closed','Cerrada'],['quality','Calidad'],['indicator_summary','EMA/RSI/ATR']])+'</div>').join('')||'<p>Sin velas.</p>';
document.getElementById('signals').innerHTML=table((await get('/api/signals?session='+encodeURIComponent(sid))).slice(-100),[['detected_ts','Detectada'],['direction','Dirección'],['status','Estado'],['episode_id','Episodio'],['payload','Detalle']]);document.getElementById('discards').innerHTML=table((await get('/api/discards?session='+encodeURIComponent(sid))).slice(-100),[['observed_ts','Observada'],['reason_code','Razón'],['required','Obligatoria'],['condition_status','Condición'],['payload','Detalle']]);document.getElementById('sims').innerHTML=table((await get('/api/simulations?session='+encodeURIComponent(sid))).slice(-200),[['detected_ts','Detectada'],['horizon_seconds','Horizonte s'],['direction','Dirección'],['entry_price','Entrada'],['final_price','Final'],['outcome','Resultado'],['net_result','Neto'],['quality','Calidad']]);}
(async()=>{let a=await get('/api/sessions');let s=document.getElementById('session');s.innerHTML=a.map(x=>'<option value="'+esc(x.session_id)+'">'+esc(x.session_id.slice(0,12)+' · '+x.mode+' · '+x.instrument)+'</option>').join('');s.onchange=load;await load()})().catch(e=>document.body.insertAdjacentHTML('beforeend','<pre>'+esc(e)+'</pre>'));
</script></body></html>"""


def _json_bytes(data: Any) -> bytes:
    return json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")


class _Handler(BaseHTTPRequestHandler):
    server: "MTFHTTPServer"

    def log_message(self, fmt: str, *args: Any) -> None:
        # Keep the UI quiet by default; callers can wrap the server if they
        # need access logs.  JSONL operation logging remains separate.
        return

    def _send(self, data: Any, *, status: int = HTTPStatus.OK, content_type: str = "application/json; charset=utf-8") -> None:
        body = data.encode("utf-8") if isinstance(data, str) else _json_bytes(data)
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        if parsed.path in {"/", "/index.html"}:
            self._send(INDEX_HTML, content_type="text/html; charset=utf-8"); return
        if parsed.path == "/api/health":
            self._send({"ok": True, "service": "mtf-lab-ui", "read_only": True}); return
        session_id = query.get("session", [self.server.default_session])[0]
        if parsed.path == "/api/sessions":
            self._send(self.server.store.sessions()); return
        if not session_id:
            self._send({"error": "session query parameter is required"}, status=HTTPStatus.BAD_REQUEST); return
        try:
            if parsed.path == "/api/status": data = self.server.store.status(session_id)
            elif parsed.path == "/api/candles": data = self.server.store.list_candles(session_id, timeframe=query.get("timeframe", [None])[0], closed_only=query.get("closed", ["0"])[0] in {"1", "true"}, limit=2000)
            elif parsed.path == "/api/signals": data = self.server.store.list_signals(session_id, limit=1000)
            elif parsed.path == "/api/discards": data = self.server.store.list_discards(session_id, limit=1000)
            elif parsed.path == "/api/simulations": data = self.server.store.list_simulations(session_id, limit=2000)
            elif parsed.path == "/api/report": data = ReportBuilder(self.server.store, session_id).summary()
            else: self._send({"error": "not found"}, status=HTTPStatus.NOT_FOUND); return
        except Exception as exc:
            self._send({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR); return
        self._send(data)


class MTFHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], store: SQLiteStore, *, default_session: str | None = None):
        super().__init__(address, _Handler)
        self.store = store
        self.default_session = default_session


def create_server(db_path: str | Path, *, host: str = "127.0.0.1", port: int = 8765, session_id: str | None = None) -> MTFHTTPServer:
    store = SQLiteStore(db_path, read_only=True)
    if session_id is None:
        sessions = store.sessions(limit=1)
        session_id = sessions[0]["session_id"] if sessions else None
    return MTFHTTPServer((host, int(port)), store, default_session=session_id)


def serve(
    db_path: str | Path,
    *,
    host: str = "127.0.0.1",
    port: int = 8765,
    session_id: str | None = None,
    duration: float | None = None,
) -> MTFHTTPServer:
    """Serve local read-only UI; optional duration makes smoke tests finite."""
    server = create_server(db_path, host=host, port=port, session_id=session_id)
    if duration is not None:
        timer = threading.Timer(max(0.0, float(duration)), server.shutdown)
        timer.daemon = True; timer.start()
    try:
        server.serve_forever(poll_interval=0.2)
    finally:
        server.server_close(); server.store.close()
    return server
