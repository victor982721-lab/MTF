"""Read-only local HTTP UI for persisted MTF Lab data.

The UI delegates all ranges, pagination, revision selection, gap detection and
condition projection to :mod:`mtf_lab.ops.query`.  It contains no indicator or
settlement logic of its own and never writes to SQLite.
"""

from __future__ import annotations

import json
import os
import stat
import threading
from collections.abc import Mapping
from datetime import UTC, datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from socketserver import BaseServer
from typing import Any, cast
from urllib.parse import parse_qs, urlparse

from .persistence import SQLiteStore, canonical_json
from .query import QueryService
from .reporting import ReportBuilder

INDEX_HTML = r"""<!doctype html>
<html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>MTF Lab</title><style>
body{font-family:system-ui,sans-serif;max-width:1280px;margin:1rem auto;padding:0 1rem;color:#182230;background:#fafbfc}
header{display:flex;gap:1rem;align-items:center;flex-wrap:wrap}h1{margin:.2rem 0}button,select,input{font:inherit;padding:.3rem}.pill{padding:.2rem .5rem;border-radius:1rem;background:#e8eef5}.warn{background:#fff2cf}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:.7rem}.card{min-width:0;overflow-wrap:anywhere;background:white;border:1px solid #d7dfe8;border-radius:.5rem;padding:.7rem}.value{font-size:1.4rem;font-weight:700}table{border-collapse:collapse;width:100%;font-size:.88rem}th,td{border:1px solid #d7dfe8;padding:.35rem;text-align:left;vertical-align:top}th{background:#eef3f8;position:sticky;top:0}.chart{width:100%;height:180px;border:1px solid #d7dfe8;background:#fff}.line{fill:none;stroke-width:1.6}.close{stroke:#2b6cb0}.emaFast{stroke:#d97706}.emaSlow{stroke:#7c3aed}.rsi{stroke:#059669}.atr{stroke:#be123c}.scroll{overflow:auto;max-height:24rem}code{font-size:.82em}pre{white-space:pre-wrap;max-height:18rem;overflow:auto}.controls{display:flex;gap:.5rem;flex-wrap:wrap;align-items:end}.controls label{display:flex;flex-direction:column;font-size:.85rem}.section{margin-top:1.2rem}.muted{color:#657487}.pager{display:flex;gap:.5rem;align-items:center;margin:.4rem 0}.ok{color:#076b3b}.bad{color:#a32020}.stale{border-color:#d97706;background:#fffaf0}.metric-label{font-size:.82rem;color:#657487}.metric-value{font-weight:650;overflow-wrap:anywhere}.status-line{display:flex;gap:.5rem;align-items:center;flex-wrap:wrap}.status-dot{display:inline-block;width:.65rem;height:.65rem;border-radius:50%;background:#657487}.status-dot.ok{background:#078447}.status-dot.bad{background:#b42318}.status-dot.warn{background:#d97706}
</style></head><body><header><h1>MTF Lab</h1><span id="mode" class="pill">cargando…</span><label>Sesión <select id="session"></select></label></header>
<p class="muted">Consulta local de velas, indicadores, señales, condiciones, descartes, revisiones, huecos y simulaciones. Sólo lectura; no envía órdenes.</p>
<div class="controls"><label>Temporalidad<select id="timeframe"><option value="">Todas</option><option>M1</option><option>M5</option><option>M15</option></select></label><label>Desde UTC<input id="start" placeholder="2025-01-01T00:00:00Z"></label><label>Hasta UTC (exclusivo)<input id="end" placeholder="2025-01-02T00:00:00Z"></label><label>Velas<select id="revisions"><option value="latest">Última revisión</option><option value="all">Todas las revisiones</option></select></label><button id="apply">Actualizar</button></div>
<section id="summary" class="grid section"></section>
<section class="section" aria-labelledby="readiness-title"><h2 id="readiness-title">Capacidad efectiva del supervisor</h2><p class="muted">Salud HTTP no equivale a disponibilidad de análisis ni permiso de operar. La evidencia debe pertenecer a un proceso vivo y estar vigente.</p><div id="readiness" class="grid" aria-live="polite"></div></section>
<section class="section" aria-labelledby="observability-title"><h2 id="observability-title">Observabilidad: procedencia, proceso y frescura</h2><div id="observability" class="grid"></div></section>
<section class="section"><h2>Gráficos sincronizados (datos persistidos)</h2><p class="muted">Las líneas se dibujan con los valores EMA/RSI/ATR registrados; el navegador no recalcula la estrategia.</p><div id="indicatorCharts" class="grid"></div></section>
<section class="section"><h2>Velas M1/M5/M15 e indicadores</h2><p class="muted">Se conserva la distinción entre vela cerrada/abierta y revisión; los indicadores provienen del registro persistido.</p><div id="candles" class="grid"></div><div id="candlePager" class="pager"></div></section>
<section class="section"><h2>Huecos y discontinuidades observados</h2><div id="gaps" class="scroll"></div></section>
<section class="section"><h2>Señales y condiciones</h2><div id="signals" class="scroll"></div><div id="conditions" class="scroll"></div></section>
<section class="section"><h2>Descartes</h2><div id="discards" class="scroll"></div></section>
<section class="section"><h2>Simulaciones virtuales segmentadas</h2><div id="sims" class="scroll"></div></section>
<section class="section"><h2>Operaciones CFD PAPER (ciclo y economía separados)</h2><p class="muted">La columna Estado representa el ciclo de vida; Neto desconocido no se convierte en WIN/LOSS/TIE. En economía v2 el slippage ya está incluido en los fills; v1 conserva su cálculo histórico sin reinterpretación. Horizontes y variantes son escenarios alternativos, no posiciones reales que deban sumarse.</p><div id="cfdTrades" class="scroll"></div></section>
<script>
const esc=x=>String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const secretKey=/token|secret|password|authorization|api[_-]?key|refresh/i;
function scrub(value,key=''){if(key&&secretKey.test(key))return '[REDACTED]';if(Array.isArray(value))return value.map(item=>scrub(item));if(value&&typeof value==='object')return Object.fromEntries(Object.entries(value).map(([k,v])=>[k,scrub(v,k)]));return value}
const json=x=>{let value=scrub(x);return typeof value==='object'?JSON.stringify(value):value};
async function get(path){let r=await fetch(path);if(!r.ok)throw Error('HTTP '+String(r.status||'error'));return r.json()}
function rows(value){return Array.isArray(value)?value:(value&&Array.isArray(value.items)?value.items:[])}
function table(items, cols){if(!items.length)return '<p>Sin registros persistidos.</p>';return '<table><thead><tr>'+cols.map(c=>'<th>'+esc(c[1])+'</th>').join('')+'</tr></thead><tbody>'+items.map(x=>'<tr>'+cols.map(c=>'<td>'+esc(json(x[c[0]]))+'</td>').join('')+'</tr>').join('')+'</tbody></table>'}
function qs(extra={}){let sid=document.getElementById('session').value, q={session:sid,timeframe:document.getElementById('timeframe').value,start_ts:document.getElementById('start').value,end_ts:document.getElementById('end').value,revisions:document.getElementById('revisions').value,...extra};return Object.entries(q).filter(([,v])=>v!==''&&v!=null).map(([k,v])=>encodeURIComponent(k)+'='+encodeURIComponent(v)).join('&')}
const refreshBySession={};
function refreshState(sid){return refreshBySession[sid]??(refreshBySession[sid]={lastGoodAt:null,lastAttemptAt:null,error:null,status:null,partial:false})}
function shortError(){return 'No se pudo actualizar la consulta local; se conserva el último estado bueno.'}
function displayValue(value){if(value===null||value===undefined||value==='')return 'UNKNOWN';if(typeof value==='object')return JSON.stringify(value);return String(value)}
function metric(label,value){return '<div class="card"><div class="metric-label">'+esc(label)+'</div><div class="metric-value">'+esc(displayValue(value))+'</div></div>'}
function renderReadiness(value){
  const state=value||{}, operational=state.operational||{};
  document.getElementById('readiness').innerHTML=metric('Capacidad en el modo declarado',state.ready===true?'DISPONIBLE':'BLOQUEADA / NO VERIFICADA')+metric('Modo',state.mode||'UNKNOWN')+metric('Identidad viva',state.liveness||'UNVERIFIED')+metric('Motivos',(state.reasons||[]).join(', ')||'Ninguno observado')+Object.entries(operational).map(([key,item])=>metric(key,item)).join('');
}
async function loadReadiness(){try{renderReadiness(await get('/api/readiness'));}catch(error){renderReadiness({ready:false,reasons:['HTTP_READINESS_UNAVAILABLE']});}}
function freshnessMaxAge(value){if(typeof value==='number')return Number.isFinite(value)&&value>=0?value:null;if(typeof value==='string'&&value.trim()!==''){const number=Number(value);return Number.isFinite(number)&&number>=0?number:null;}return null}
function freshnessView(status){
  const raw=status&&status.freshness||{}, provenance=status&&status.provenance||{};
  const historical=['FIXTURE','HISTORICAL_REPLAY'].includes(String(provenance.source_class||''));
  const timestamp=raw.last_observed_at||raw.last_data_ts||(status&&status.last_data_ts);
  let age=null;
  if(timestamp){const parsed=Date.parse(String(timestamp));if(Number.isFinite(parsed))age=Math.max(0,(Date.now()-parsed)/1000);}
  let state=String(raw.state||'UNKNOWN').toUpperCase(), reason=String(raw.reason||'freshness_not_observed');
  const maxAge=freshnessMaxAge(raw.max_age_seconds);
  if(historical){state='NOT_APPLICABLE';reason='historical_or_offline_data';}
  else if(!timestamp){state='UNKNOWN';reason='no_data_observed';}
  else if((state==='FRESH'||state==='VALID')&&Number.isFinite(maxAge)&&age!==null&&age>maxAge){state='STALE';reason='data_age_exceeded';}
  return {state,reason,age,maxAge};
}
function renderObservability(status,sid){
  const state=refreshState(sid), p=status&&status.provenance||{}, process=status&& (status.process_status||status.process)||{}, fresh=freshnessView(status), coverage=status&&status.coverage_summary||{}, last=status&&status.last_data||{};
  const source=displayValue(p.source_class_label||p.source_class||'UNKNOWN');
  const range=(p.range&&((p.range.start_ts||'UNKNOWN')+' → '+(p.range.end_ts||'UNKNOWN')))||'UNKNOWN';
  const runtimeErrors=status&&status.errors||{};
  let refresh='<span class="status-line"><span class="status-dot '+(state.error?'bad':state.partial?'warn':'ok')+'"></span>'+esc(state.error?'DESACTUALIZADO · '+shortError():state.partial?'ACTUALIZADO PARCIAL · fallback de consulta':'ACTUALIZADO')+'</span>';
  if(state.lastGoodAt)refresh+='<div>Última consulta exitosa: '+esc(state.lastGoodAt)+'</div>';
  if(state.error)refresh+='<div class="bad">'+esc('El panel conserva datos anteriores; reintento automático activo.')+'</div>';
  const account=displayValue(p.account_environment||status.account_environment), observed=displayValue(p.observed_environment||p.environment||status.observation_environment);
  document.getElementById('observability').innerHTML=[
    '<div class="card '+(state.error?'stale':'')+'"><h3>Refresco de UI</h3><div>'+refresh+'</div></div>',
    metric('Procedencia respaldada',source),
    metric('Evidencia / causa',(p.evidence||[]).join('; ')||'UNKNOWN'),
    metric('source_mode / sintético',(p.source_mode||status.source_mode||'UNKNOWN')+' / '+displayValue(p.synthetic??status.synthetic)),
    metric('Proveedor / entorno observado',displayValue(p.provider||status.provider)+' / '+observed),
    metric('Cuenta / ejecución',account+' / '+displayValue(p.execution_enabled??status.execution_enabled)),
    metric('Instrumento',p.instrument||status.instrument||'UNKNOWN'),
    metric('dataset_ref',p.dataset_ref||'UNKNOWN'),
    metric('Hash dataset/captura',(p.dataset_hash||'UNKNOWN')+' / '+(p.capture_hash||'UNKNOWN')),
    metric('Rango de datos',range),
    metric('Estado de proceso',displayValue(process.status)+' · '+displayValue(process.capture_state)),
    metric('Feed activo',displayValue(process.feed_active)),
    metric('Conexión / continuidad',displayValue(process.connection)+' / '+displayValue(process.continuity)),
    metric('Último dato',last.timestamp||status.last_data_ts||'UNKNOWN'),
    metric('Frescura',displayValue(fresh.state)+' · '+displayValue(fresh.reason)+' · edad '+(fresh.age===null?'UNKNOWN':fresh.age.toFixed(1)+' s')),
    metric('Cobertura / huecos',(coverage.timeframes||[]).join(', ')+' / '+displayValue(coverage.gaps)),
    metric('Errores de proceso',displayValue(runtimeErrors.count||process.errors||0))
  ].join('');
}
function pathFor(points,key,w=520,h=150){
  const vals=points.map(x=>{let z=x.indicator_values&&x.indicator_values[key];return z==null?x[key]:z}).filter(v=>typeof v==='number'&&Number.isFinite(v));
  if(!vals.length)return '';
  let min=Math.min(...vals),max=Math.max(...vals);
  if(max===min)max=min+1;
  const n=points.length;
  let previousValid=false;
  const segments=[];
  points.forEach((x,i)=>{
    let v=x.indicator_values&&x.indicator_values[key];
    if(v==null)v=x[key];
    if(typeof v!=='number'||!Number.isFinite(v)){
      previousValid=false;
      return;
    }
    const xx=n<=1?w/2:i*w/(n-1);
    const yy=h-(v-min)/(max-min)*h;
    segments.push((previousValid?'L':'M')+xx.toFixed(1)+' '+yy.toFixed(1));
    previousValid=true;
  });
  return segments.join(' ');
}
async function drawCharts(){let root=document.getElementById('indicatorCharts');let tfs=['M1','M5','M15'];let data=await Promise.all(tfs.map(async tf=>[tf,rows(await get('/api/indicators?'+qs({timeframe:tf,limit:200})))]));root.innerHTML=data.map(([tf,p])=>{let q=p.slice(-120),svg='<svg class="chart" viewBox="0 0 520 150" role="img" aria-label="'+esc(tf)+' EMA RSI ATR">';[['close','close'],['ema_fast','emaFast'],['ema_slow','emaSlow']].forEach(([k,c])=>{let d=pathFor(q,k);if(d)svg+='<path class="line '+c+'" d="'+d+'"/>'});svg+='</svg>';let osc='<svg class="chart" viewBox="0 0 520 90" role="img" aria-label="'+esc(tf)+' RSI ATR persistidos">';[['rsi','rsi'],['atr','atr']].forEach(([k,c])=>{let d=pathFor(q,k,520,80);if(d)osc+='<path class="line '+c+'" d="'+d+'"/>'});osc+='</svg><p class="muted">'+esc(tf)+' · precio/EMA y RSI/ATR persistidos</p>';return '<div class="card">'+svg+osc+'</div>'}).join('')||'<p>Sin indicadores persistidos.</p>'}

function uiSnapshot(){
  const ids=['mode','summary','observability','indicatorCharts','candles','candlePager','gaps','signals','conditions','discards','sims','cfdTrades'];
  const result={};
  ids.forEach(id=>{const item=document.getElementById(id);result[id]={html:item.innerHTML,text:item.textContent};});
  return result;
}
function restoreUiSnapshot(snapshot){Object.entries(snapshot).forEach(([id,value])=>{document.getElementById(id).innerHTML=value.html;});}
async function load(){
  const sid=document.getElementById('session').value;
  if(!sid)return;
  const state=refreshState(sid), prior=uiSnapshot();
  state.lastAttemptAt=new Date().toISOString();
  let pollError=null;
  try{
    let poll=null;
    try{poll=await get('/api/poll?'+qs({limit:100}));}catch(error){pollError=error;}
    const s=(poll&&poll.status)||await get('/api/status?session='+encodeURIComponent(sid));
    const cp=await get('/api/candles?'+qs({limit:200}));
    const candles=rows(cp), groups={};
    candles.forEach(x=>(groups[x.timeframe]??=[]).push(x));
    const gaps=await get('/api/gaps?'+qs({}));
    const signals=await get('/api/signals?'+qs({limit:500}));
    const conditions=await get('/api/conditions?'+qs({limit:1000}));
    const discards=await get('/api/discards?'+qs({limit:500}));
    const sims=await get('/api/simulations?'+qs({limit:1000}));
    const cfd=await get('/api/cfd-trades?'+qs({limit:1000}));
    document.getElementById('mode').textContent=(s.mode_label||s.mode||'?')+' · '+(s.provider||'?')+' · '+(s.instrument||'?');
    const c=s.counts||{};
    document.getElementById('summary').innerHTML=Object.entries(c).map(([k,v])=>'<div class="card"><div>'+esc(k)+'</div><div class="value">'+esc(v)+'</div></div>').join('')+'<div class="card"><div>Conexión</div><div class="value">'+esc(s.connection||'—')+'</div></div><div class="card"><div>Proveedor/entorno</div><div>'+esc((s.provider||'—')+' / '+(s.provider_environment||'—'))+'</div><div>Cuenta: '+esc(s.provider_account_id||'—')+'</div><div>Símbolo: '+esc(s.provider_symbol||'—')+'</div></div><div class="card"><div>Destino ejecución</div><div>'+esc((s.execution_environment||'—')+' / '+(s.execution_destination||'—'))+'</div><div>Permisos: '+esc(JSON.stringify(s.permissions||{}))+'</div></div><div class="card"><div>Análisis</div><div>'+esc(s.analysis_enabled?'habilitado':'bloqueado')+'</div><div>'+esc((s.analysis_blocked_reasons||[]).join(', ')||'—')+'</div></div><div class="card"><div>Calentamiento</div><div>'+esc(JSON.stringify(s.warmup_pending||{}))+'</div></div><div class="card"><div>Pendientes</div><div class="value">'+esc(s.pending_simulations||0)+'</div></div><div class="card"><div>Último evento</div><div>'+esc(s.last_event_ts||'—')+'</div></div><div class="card"><div>Gaps/revisiones</div><div class="value">'+esc((s.gaps||[]).length)+' / '+esc(s.revisions_count||0)+'</div></div>';
    document.getElementById('candles').innerHTML=Object.entries(groups).map(([tf,a])=>'<div class="card"><h3>'+esc(tf)+'</h3>'+table(a.slice(-30),[['start_ts','Inicio'],['end_ts','Fin'],['close','Cierre'],['closed','Cerrada'],['revision','Rev.'],['is_latest_revision','Última'],['quality','Calidad'],['indicator_values','EMA/RSI/ATR']])+'</div>').join('')||'<p>Sin velas.</p>';
    document.getElementById('candlePager').innerHTML=cp.next_cursor?'<button id="moreCandles">Cargar más</button>':'<span class="muted">Fin de velas</span>';
    if(cp.next_cursor)document.getElementById('moreCandles').onclick=()=>loadMoreCandles(cp.next_cursor);
    await drawCharts();
    document.getElementById('gaps').innerHTML=table(rows(gaps),[['timeframe','Temporalidad'],['gap_start','Inicio hueco'],['gap_end','Fin hueco'],['duration_seconds','Duración s'],['filled','Rellenado'],['quality','Calidad']]);
    document.getElementById('signals').innerHTML=table(rows(signals), [['detected_ts','Detectada'],['direction','Dirección'],['status','Estado'],['episode_id','Episodio'],['payload','Detalle']]);
    document.getElementById('conditions').innerHTML='<h3>Condiciones</h3>'+table(rows(conditions), [['observed_ts','Observada'],['decision','Decisión'],['name','Condición'],['state','Estado'],['observed','Observado'],['expected','Esperado'],['reason','Razón'],['mandatory','Obligatoria']]);
    document.getElementById('discards').innerHTML=table(rows(discards), [['observed_ts','Observada'],['reason_code','Razón'],['required','Obligatoria'],['condition_status','Condición'],['payload','Detalle']]);
    document.getElementById('sims').innerHTML=table(rows(sims), [['detected_ts','Detectada'],['horizon_seconds','Horizonte s'],['direction','Dirección'],['outcome','Resultado'],['net_result','Neto virtual'],['quality','Calidad'],['dimensions','Análisis/variante/contrato']]);
    document.getElementById('cfdTrades').innerHTML=table(rows(cfd), [['detected_at','Detectada'],['horizon_seconds','Horizonte s'],['variant','Variante'],['economics_version','Economía versionada'],['trade_id','Operación'],['signal_id','Señal'],['direction','Dirección'],['units','Unidades'],['state','Estado'],['economic_state','Estado económico'],['economic_status','Economía'],['close_observed','Cierre observado'],['entry_price','Entrada'],['close_price','Cierre'],['gross_pnl_quote','Bruto ejecutado'],['slippage_quote','Slippage (desglose v2)'],['costs_account','Costes registrados'],['net_pnl','Neto'],['reason','Razón'] ]);
    state.lastGoodAt=new Date().toISOString();
    state.error=null;
    state.partial=Boolean(pollError);
    state.status=s;
    renderObservability(s,sid);
  }catch(error){
    restoreUiSnapshot(prior);
    state.error=true;
    state.partial=false;
    renderObservability(state.status||{},sid);
  }
}
async function loadMoreCandles(cursor){let p=await get('/api/candles?'+qs({limit:200,cursor}));let old=window._candleItems||[];window._candleItems=old.concat(rows(p));document.getElementById('candlePager').innerHTML=p.next_cursor?'<button id="moreCandles">Cargar más</button>':'<span class="muted">Fin de velas</span>';if(p.next_cursor)document.getElementById('moreCandles').onclick=()=>loadMoreCandles(p.next_cursor);}
(async()=>{await loadReadiness();setInterval(()=>loadReadiness(),5000);let a=await get('/api/sessions');let list=rows(a);let s=document.getElementById('session');s.innerHTML=list.map(x=>'<option value="'+esc(x.session_id)+'">'+esc(x.session_id.slice(0,12)+' · '+x.mode+' · '+x.instrument)+'</option>').join('');s.onchange=load;document.getElementById('apply').onclick=load;await load();setInterval(()=>load(),5000)})().catch(()=>document.body.insertAdjacentHTML('beforeend','<pre class="bad">No se pudo cargar la UI local.</pre>'));
</script></body></html>"""


# Snapshot mode deliberately has its own static page.  It never asks the
# browser to supply a database path or a session identifier; all data comes
# from the fixed, server-side snapshot selected by ``create_server``.
SNAPSHOT_INDEX_HTML = r"""<!doctype html>
<html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>MTF Lab — snapshot</title><style>
body{font-family:system-ui,sans-serif;max-width:1100px;margin:1rem auto;padding:0 1rem;color:#182230;background:#fafbfc}.card{border:1px solid #d7dfe8;border-radius:.5rem;background:#fff;padding:.7rem;margin:.6rem 0;overflow-wrap:anywhere}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:.7rem}.muted{color:#657487}.warn{color:#9a6700}.bad{color:#a32020}pre{white-space:pre-wrap;overflow:auto;max-height:28rem}table{border-collapse:collapse;width:100%}th,td{border:1px solid #d7dfe8;padding:.35rem;text-align:left}th{background:#eef3f8}
</style></head><body><h1>MTF Lab — snapshot de investigación</h1>
<p class="muted">Fuente fija de sólo lectura. Salud HTTP no equivale a readiness ni a permiso de operar.</p>
<div id="summary" class="grid"></div><section class="card"><h2>Procedencia y frescura</h2><div id="provenance"></div></section>
<section class="card"><h2>Datos publicados</h2><pre id="report">Cargando…</pre></section>
<script>(async()=>{const esc=x=>String(x??'UNKNOWN').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));const get=p=>fetch(p,{cache:'no-store'}).then(r=>{if(!r.ok)throw Error('HTTP '+r.status);return r.json()});try{const [s,h]=await Promise.all([get('/api/status'),get('/api/health')]);const c=s.counts||{};document.getElementById('summary').innerHTML=Object.entries(c).map(([k,v])=>'<div class="card"><b>'+esc(k)+'</b><div>'+esc(v)+'</div></div>').join('')+'<div class="card"><b>Modo</b><div>'+esc(s.mode_label||s.mode)+'</div></div><div class="card"><b>Health</b><div>'+esc(h.ok===true?'OK · read-only':'UNKNOWN')+'</div></div>';document.getElementById('provenance').innerHTML='<pre>'+esc(JSON.stringify({provenance:s.provenance,staleness:s.staleness,readiness:'separate endpoint'},null,2))+'</pre>';document.getElementById('report').textContent=JSON.stringify(await get('/api/report'),null,2)}catch(e){document.getElementById('report').textContent='SNAPSHOT_UNAVAILABLE';document.getElementById('report').className='bad'}})();</script></body></html>"""


SNAPSHOT_MAX_BYTES = 8 * 1024 * 1024
SNAPSHOT_DEFAULT_MAX_AGE_SECONDS = 3600.0
_SNAPSHOT_PRIVATE_KEYS = ("token", "secret", "password", "authorization", "api_key", "refresh")
_SNAPSHOT_IDENTIFIER_KEYS = (
    "account_id",
    "accountid",
    "account_key",
    "session_id",
    "email",
    "phone",
    "client_id",
    "path",
    "filename",
    "file_name",
)


class SnapshotSecurityError(ValueError):
    """The fixed dashboard snapshot is not a private regular file."""


def _snapshot_redact(value: Any, *, key: str = "", max_items: int = 256) -> Any:
    lowered = key.lower().replace("-", "_")
    if any(item in lowered for item in _SNAPSHOT_PRIVATE_KEYS + _SNAPSHOT_IDENTIFIER_KEYS):
        return "[REDACTED]"
    if isinstance(value, str) and any(
        marker in value.lower() for marker in ("token", "secret", "password", "api_key", "bearer ")
    ):
        return "[REDACTED]"
    if isinstance(value, Mapping):
        return {
            str(name): _snapshot_redact(item, key=str(name), max_items=max_items)
            for name, item in list(value.items())[:max_items]
        }
    if isinstance(value, (list, tuple)):
        return [_snapshot_redact(item, max_items=max_items) for item in list(value)[:max_items]]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if hasattr(value, "isoformat"):
        return str(value.isoformat())
    return str(value)


def _snapshot_path(value: str | Path) -> Path:
    if isinstance(value, str) and ("://" in value or value.lower().startswith(("http:", "https:", "file:"))):
        raise SnapshotSecurityError("snapshot JSON requiere una ruta fija local, no una URL")
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    if any(component in {".", ".."} for component in path.parts):
        raise SnapshotSecurityError("snapshot JSON requiere una ruta sin componentes relativos")
    # Reject symlinked parent components too.  A writer may publish an atomic
    # replacement, but the selected namespace itself must not be redirected.
    current = Path(path.anchor)
    for component in path.parts[1:-1]:
        current /= component
        try:
            info = os.lstat(current)
        except FileNotFoundError as exc:
            raise SnapshotSecurityError("directorio de snapshot inexistente") from exc
        if stat.S_ISLNK(info.st_mode):
            raise SnapshotSecurityError("el directorio de snapshot no puede ser symlink")
    return path


def _snapshot_timestamp(value: Mapping[str, Any]) -> datetime | None:
    sources: list[Mapping[str, Any]] = [value]
    for key in ("runtime_identity", "status"):
        nested = value.get(key)
        if isinstance(nested, Mapping):
            sources.append(cast(Mapping[str, Any], nested))
    for source in sources:
        for key in ("published_at", "generated_at", "snapshot_at", "updated_at", "captured_at"):
            candidate = source.get(key)
            if candidate is not None:
                text = str(candidate).strip().replace("Z", "+00:00")
                try:
                    parsed = datetime.fromisoformat(text)
                except (TypeError, ValueError, OverflowError):
                    continue
                if parsed.tzinfo is not None and parsed.utcoffset() is not None:
                    return parsed.astimezone(UTC)
    return None


def _snapshot_limit(value: Mapping[str, Any], default_max_age: float) -> tuple[float, bool]:
    raw_limit = value.get("valid_for_seconds", value.get("max_age_seconds"))
    if raw_limit is None and isinstance(value.get("runtime_identity"), Mapping):
        identity = cast(Mapping[str, Any], value["runtime_identity"])
        raw_limit = identity.get("valid_for_seconds", identity.get("max_age_seconds"))
    invalid_limit = raw_limit is not None
    try:
        limit = float(raw_limit) if raw_limit is not None and not isinstance(raw_limit, bool) else default_max_age
    except (TypeError, ValueError):
        limit = default_max_age
    if raw_limit is None:
        invalid_limit = False
    elif not (limit > 0) or limit != limit or limit == float("inf"):
        invalid_limit = True
        limit = default_max_age
    return limit, invalid_limit


def _snapshot_age(value: Mapping[str, Any], *, now: datetime, default_max_age: float) -> dict[str, Any]:
    timestamp = _snapshot_timestamp(value)
    limit, invalid_limit = _snapshot_limit(value, default_max_age)
    if timestamp is None:
        return {
            "state": "UNKNOWN",
            "stale": False,
            "age_seconds": None,
            "max_age_seconds": limit,
            "reason": "timestamp_missing_or_invalid",
        }
    if invalid_limit:
        return {
            "state": "UNKNOWN",
            "stale": False,
            "age_seconds": None,
            "max_age_seconds": limit,
            "reason": "valid_for_seconds_invalid",
            "published_at": timestamp.isoformat().replace("+00:00", "Z"),
        }
    raw_age = (now - timestamp).total_seconds()
    if raw_age < 0:
        return {
            "state": "UNKNOWN",
            "stale": False,
            "age_seconds": None,
            "max_age_seconds": limit,
            "reason": "timestamp_in_future_clock_skew",
            "published_at": timestamp.isoformat().replace("+00:00", "Z"),
        }
    age = raw_age
    stale = age > limit
    return {
        "state": "STALE" if stale else "FRESH",
        "stale": stale,
        "age_seconds": age,
        "max_age_seconds": limit,
        "reason": "max_age_exceeded" if stale else "within_declared_max_age",
        "published_at": timestamp.isoformat().replace("+00:00", "Z"),
    }


class SnapshotReader:
    """Read and validate one private fixed-path JSON snapshot per request."""

    def __init__(
        self,
        path: str | Path,
        *,
        max_bytes: int = SNAPSHOT_MAX_BYTES,
        max_age_seconds: float = SNAPSHOT_DEFAULT_MAX_AGE_SECONDS,
    ):
        self.path = _snapshot_path(path)
        self.max_bytes = max(1, min(SNAPSHOT_MAX_BYTES, int(max_bytes)))
        self.max_age_seconds = max(0.0, float(max_age_seconds))
        self._validate_path()

    def _validate_path(self) -> os.stat_result:
        try:
            info = os.lstat(self.path)
        except OSError as exc:
            raise SnapshotSecurityError("snapshot JSON no disponible") from exc
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            raise SnapshotSecurityError("snapshot JSON debe ser un archivo regular sin symlink")
        if info.st_uid != os.getuid() or info.st_nlink != 1:
            raise SnapshotSecurityError("snapshot JSON debe pertenecer al usuario y tener nlink=1")
        if info.st_size > self.max_bytes:
            raise SnapshotSecurityError("snapshot JSON excede el límite de tamaño")
        if stat.S_IMODE(info.st_mode) & 0o077:
            raise SnapshotSecurityError("snapshot JSON debe ser privado (sin permisos de grupo/otros)")
        return info

    def load(self) -> dict[str, Any]:
        expected = self._validate_path()
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(self.path, flags)
        except OSError as exc:
            raise SnapshotSecurityError("snapshot JSON no se pudo abrir de forma segura") from exc
        try:
            opened = os.fstat(fd)
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_dev != expected.st_dev
                or opened.st_ino != expected.st_ino
                or opened.st_uid != os.getuid()
                or opened.st_nlink != 1
                or opened.st_size > self.max_bytes
            ):
                raise SnapshotSecurityError("snapshot JSON cambió de identidad durante la lectura")
            chunks: list[bytes] = []
            total = 0
            while True:
                chunk = os.read(fd, min(64 * 1024, self.max_bytes - total + 1))
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
                if total > self.max_bytes:
                    raise SnapshotSecurityError("snapshot JSON excede el límite de tamaño")

            def reject_constant(value: str) -> None:
                raise ValueError(f"constante JSON no finita: {value}")

            try:
                value = json.loads(b"".join(chunks).decode("utf-8"), parse_constant=reject_constant)
            except (UnicodeDecodeError, ValueError) as exc:
                raise SnapshotSecurityError("snapshot JSON inválido") from exc
            final = os.fstat(fd)
            if final.st_nlink != 1 or final.st_uid != os.getuid() or not stat.S_ISREG(final.st_mode):
                raise SnapshotSecurityError("snapshot JSON perdió su identidad privada")
        finally:
            os.close(fd)
        if not isinstance(value, Mapping):
            raise SnapshotSecurityError("snapshot JSON debe contener un objeto")
        return cast(dict[str, Any], _snapshot_redact(value))

    def read(self) -> dict[str, Any]:
        """Compatibility name for callers that treat the reader as a source."""

        return self.load()

    def projection(self) -> dict[str, Any]:
        value = self.load()
        now = datetime.now(UTC)
        staleness = _snapshot_age(value, now=now, default_max_age=self.max_age_seconds)
        provenance = value.get("provenance")
        if not isinstance(provenance, Mapping):
            provenance = {"status": "UNKNOWN", "labels": ["UNKNOWN"]}
        return {
            "snapshot": value,
            "staleness": staleness,
            "provenance": _snapshot_redact(provenance),
            "redaction": {"applied": True, "private_keys": True, "raw_paths": True},
        }


def _snapshot_status(projection: Mapping[str, Any]) -> dict[str, Any]:
    value = projection.get("snapshot")
    raw = value if isinstance(value, Mapping) else {}
    raw_status = raw.get("status")
    raw_source = raw.get("source")
    raw_counts = raw.get("counts")
    status: Mapping[str, Any] = cast(Mapping[str, Any], raw_status) if isinstance(raw_status, Mapping) else {}
    source: Mapping[str, Any] = cast(Mapping[str, Any], raw_source) if isinstance(raw_source, Mapping) else {}
    counts: Mapping[str, Any] = cast(Mapping[str, Any], raw_counts) if isinstance(raw_counts, Mapping) else {}
    if not counts:
        counts = {
            name: raw.get(name)
            for name in ("messages", "events", "signals", "reconnects", "reconciliations")
            if raw.get(name) is not None
        }
    staleness = projection.get("staleness")
    stale = staleness.get("stale") if isinstance(staleness, Mapping) else False
    result = dict(status)
    result.update(
        {
            "mode": "SNAPSHOT",
            "mode_label": "SNAPSHOT DE INVESTIGACIÓN",
            "snapshot_mode": True,
            "read_only": True,
            "analysis_enabled": False,
            "execution_enabled": False,
            "ready": False,
            "readiness": "separate_endpoint",
            "counts": dict(counts),
            "provider": result.get(
                "provider", source.get("provider", raw.get("provider", raw.get("source", "UNKNOWN")))
            ),
            "instrument": result.get("instrument", source.get("instrument", raw.get("instrument", "UNKNOWN"))),
            "provenance": projection.get("provenance", {"status": "UNKNOWN", "labels": ["UNKNOWN"]}),
            "staleness": staleness or {"state": "UNKNOWN", "stale": False},
            "stale": stale is True,
            "redaction": projection.get("redaction", {"applied": True}),
            # A report snapshot is not runtime evidence.  Keep readiness
            # visibly blocked even if the payload contains a positive result.
            "readiness_verified": False,
        }
    )
    return result


def _snapshot_items(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        if isinstance(value.get("items"), list):
            return dict(value)
        return {"items": [dict(value)], "limit": 1}
    if isinstance(value, list):
        return {"items": value, "limit": len(value)}
    return {"items": [], "limit": 0}


def _snapshot_endpoint(projection: Mapping[str, Any], path: str) -> Any:
    """Project a fixed snapshot without accepting path/session query input."""

    value = projection.get("snapshot")
    raw = value if isinstance(value, Mapping) else {}
    if path == "/api/status":
        return _snapshot_status(projection)
    if path in {"/api/report", "/api/snapshot"}:
        return {**dict(raw), "staleness": projection.get("staleness"), "redaction": projection.get("redaction")}
    if path == "/api/poll":
        return {"status": _snapshot_status(projection), "items": [], "snapshot": True}
    if path == "/api/provenance":
        return {
            "provenance": projection.get("provenance", {"status": "UNKNOWN", "labels": ["UNKNOWN"]}),
            "staleness": projection.get("staleness", {"state": "UNKNOWN", "stale": False}),
            "redaction": projection.get("redaction", {"applied": True}),
        }
    if path == "/api/sessions":
        sessions = raw.get("sessions", raw.get("session"))
        return _snapshot_items(sessions).get("items", [])
    aliases = {
        "/api/candles": "candles",
        "/api/indicators": "indicators",
        "/api/revisions": "revisions",
        "/api/events": "events",
        "/api/signals": "signals",
        "/api/decisions": "decisions",
        "/api/discards": "discards",
        "/api/conditions": "conditions",
        "/api/gaps": "gaps",
        "/api/simulations": "simulations",
        "/api/cfd-trades": "cfd_trades",
        "/api/cfd_trades": "cfd_trades",
        "/api/captures": "captures",
        "/api/capture-envelopes": "capture_envelopes",
        "/api/capture_envelopes": "capture_envelopes",
        "/api/results": "results",
        "/api/evidence": "results",
    }
    if path in aliases:
        value = raw.get(aliases[path])
        if value is None and isinstance(raw.get("snapshot"), Mapping):
            value = raw["snapshot"].get(aliases[path])
        return _snapshot_items(value)
    if path == "/api/query":
        return {"items": [], "limit": 0, "snapshot": True}
    raise LookupError("not found")


def _json_bytes(data: Any) -> bytes:
    return str(canonical_json(data)).encode("utf-8")


def _bool_param(value: str | None) -> bool | None:
    if value is None or value == "":
        return None
    if value.lower() in {"1", "true", "yes", "si", "sí"}:
        return True
    if value.lower() in {"0", "false", "no"}:
        return False
    raise ValueError("parámetro booleano inválido")


class _Handler(BaseHTTPRequestHandler):
    server: BaseServer

    @property
    def _mtf_server(self) -> MTFHTTPServer:
        if not isinstance(self.server, MTFHTTPServer):
            raise RuntimeError("handler is attached to an unexpected server")
        return self.server

    @property
    def _snapshot_mode(self) -> bool:
        return self._mtf_server.snapshot_reader is not None

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _send(
        self, data: Any, *, status: int = HTTPStatus.OK, content_type: str = "application/json; charset=utf-8"
    ) -> None:
        body = data.encode("utf-8") if isinstance(data, str) else _json_bytes(data)
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _common(self, query: dict[str, list[str]]) -> dict[str, Any]:
        def one(name: str, default: Any = None) -> Any:
            return query.get(name, [default])[0]

        try:
            requested_limit = int(one("limit", 100))
        except (TypeError, ValueError) as exc:
            raise ValueError("limit debe ser entero") from exc
        return {
            "start_ts": one("start_ts", one("start")),
            "end_ts": one("end_ts", one("end")),
            "instrument": one("instrument"),
            "timeframe": one("timeframe"),
            "limit": max(1, min(1000, requested_limit)),
            "recent": _bool_param(one("recent", "0")) or False,
            "cursor": one("cursor"),
            "revisions": one("revisions", "latest"),
            "closed": _bool_param(one("closed")),
        }

    def _cfd_query(self, sid: str, query: dict[str, list[str]], common: dict[str, Any]) -> dict[str, Any]:
        analysis = query.get("analysis_id", query.get("analysis", [None]))[0]
        return self._mtf_server.queries.query_cfd_trades(
            sid,
            analysis_id=analysis,
            variant=query.get("variant", [None])[0],
            partition=query.get("partition", [None])[0],
            instrument=query.get("instrument", [None])[0],
            state=query.get("state", [None])[0],
            signal_id=query.get("signal_id", [None])[0],
            start_ts=common["start_ts"],
            end_ts=common["end_ts"],
            recent=common["recent"],
            limit=common["limit"],
            cursor=common["cursor"],
        ).to_dict()

    def _simulation_query(self, sid: str, query: dict[str, list[str]], common: dict[str, Any]) -> dict[str, Any]:
        horizon = float(query["horizon_seconds"][0]) if query.get("horizon_seconds") else None
        return self._mtf_server.queries.query_simulations(
            sid,
            analysis=query.get("analysis", [None])[0],
            variant=query.get("variant", [None])[0],
            partition=query.get("partition", [None])[0],
            contract=query.get("contract", [None])[0],
            horizon_seconds=horizon,
            instrument=query.get("instrument", [None])[0],
            start_ts=common["start_ts"],
            end_ts=common["end_ts"],
            recent=common["recent"],
            limit=common["limit"],
            cursor=common["cursor"],
        ).to_dict()

    def _query_kind(self, sid: str, query: dict[str, list[str]], common: dict[str, Any]) -> Any:
        kind = query.get("kind", [""])[0]
        if kind == "simulations":
            return self._simulation_query(sid, query, common)
        if kind in {"cfd_trades", "cfd-trades"}:
            return self._cfd_query(sid, query, common)
        if kind in {"capture_envelopes", "capture-envelopes", "captures"}:
            return self._mtf_server.queries.query_capture_envelopes(
                sid,
                start_ts=common["start_ts"],
                end_ts=common["end_ts"],
                limit=common["limit"],
                cursor=common["cursor"],
            ).to_dict()
        dispatch = {
            "events": self._mtf_server.queries.query_events,
            "candles": self._mtf_server.queries.query_candles,
            "signals": self._mtf_server.queries.query_signals,
            "decisions": self._mtf_server.queries.query_decisions,
            "discards": self._mtf_server.queries.query_discards,
        }
        if kind not in dispatch:
            raise ValueError(
                "kind debe ser events/candles/signals/decisions/discards/simulations/cfd_trades/capture_envelopes"
            )
        return dispatch[kind](sid, **common).to_dict()

    def _read_api(self, path: str, sid: str, query: dict[str, list[str]], common: dict[str, Any]) -> Any:
        if path == "/api/status":
            data = self._mtf_server.queries.snapshot(sid)
            data["mode_label"] = {
                "SYNTHETIC": "SINTETICO",
                "LIVE": "OBSERVACIÓN EN DIRECTO",
                "REPLAY": "REPLAY",
                "BACKTEST": "BACKTEST",
            }.get(str(data.get("mode", "")).upper(), data.get("mode", "UNKNOWN"))
            return data
        simple = {
            "/api/candles": lambda: self._mtf_server.queries.query_candles(sid, **common).to_dict(),
            "/api/indicators": lambda: self._mtf_server.queries.query_indicators(sid, **common).to_dict(),
            "/api/revisions": lambda: self._mtf_server.queries.query_revisions(sid, **common).to_dict(),
            "/api/events": lambda: self._mtf_server.queries.query_events(sid, **common).to_dict(),
            "/api/signals": lambda: self._mtf_server.queries.query_signals(sid, **common).to_dict(),
            "/api/decisions": lambda: self._mtf_server.queries.query_decisions(sid, **common).to_dict(),
            "/api/discards": lambda: self._mtf_server.queries.query_discards(sid, **common).to_dict(),
            "/api/poll": lambda: self._mtf_server.queries.poll(sid, limit=common["limit"]),
            "/api/report": lambda: ReportBuilder(self._mtf_server.store, sid).summary(),
        }
        if path in simple:
            return simple[path]()
        if path == "/api/conditions":
            return self._mtf_server.queries.query_conditions(
                sid,
                start_ts=common["start_ts"],
                end_ts=common["end_ts"],
                recent=common["recent"],
                limit=common["limit"],
                cursor=common["cursor"],
            ).to_dict()
        if path == "/api/gaps":
            return {
                "items": self._mtf_server.queries.query_gaps(
                    sid,
                    timeframe=common["timeframe"],
                    instrument=common["instrument"],
                    start_ts=common["start_ts"],
                    end_ts=common["end_ts"],
                    revisions=common["revisions"],
                    include_open=common["closed"] is not True,
                ),
                "limit": common["limit"],
            }
        if path == "/api/simulations":
            return self._simulation_query(sid, query, common)
        if path in {"/api/cfd-trades", "/api/cfd_trades"}:
            return self._cfd_query(sid, query, common)
        if path in {"/api/captures", "/api/capture-envelopes", "/api/capture_envelopes"}:
            return self._mtf_server.queries.query_capture_envelopes(
                sid,
                start_ts=common["start_ts"],
                end_ts=common["end_ts"],
                limit=common["limit"],
                cursor=common["cursor"],
            ).to_dict()
        if path == "/api/query":
            return self._query_kind(sid, query, common)
        raise LookupError("not found")

    def _handle_snapshot_api(self, path: str) -> None:
        reader = self._mtf_server.snapshot_reader
        if reader is None:
            raise RuntimeError("snapshot mode is not configured")
        try:
            data = _snapshot_endpoint(reader.projection(), path)
        except LookupError:
            self._send({"error": "not found"}, status=HTTPStatus.NOT_FOUND)
            return
        except (OSError, ValueError, TypeError) as exc:
            self._send({"error": "snapshot unavailable", "reason": str(exc)}, status=HTTPStatus.SERVICE_UNAVAILABLE)
            return
        self._send(data)

    def _session_list(self, query: dict[str, list[str]]) -> list[dict[str, Any]]:
        if self._snapshot_mode:
            reader = self._mtf_server.snapshot_reader
            if reader is None:
                return []
            value = _snapshot_endpoint(reader.projection(), "/api/sessions")
            return [dict(item) for item in value if isinstance(item, Mapping)] if isinstance(value, list) else []
        try:
            session_limit = max(1, min(100, int(query.get("limit", [50])[0])))
        except (TypeError, ValueError):
            session_limit = 50
        store = self._mtf_server.store
        if store is None:
            return []
        return store.sessions(limit=session_limit)

    def _handle_api(self, path: str, sid: str, query: dict[str, list[str]]) -> None:
        try:
            common = self._common(query)
            data = self._read_api(path, sid, query, common)
        except LookupError:
            self._send({"error": "not found"}, status=HTTPStatus.NOT_FOUND)
            return
        except (ValueError, TypeError) as exc:
            self._send({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        except Exception as exc:
            self._send({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self._send(data)

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query, keep_blank_values=True)
        if parsed.path in {"/", "/index.html"}:
            self._send(
                SNAPSHOT_INDEX_HTML if self._snapshot_mode else INDEX_HTML,
                content_type="text/html; charset=utf-8",
            )
            return
        if parsed.path == "/api/health":
            self._send(
                {
                    "ok": True,
                    "service": "mtf-lab-ui",
                    "read_only": True,
                    "snapshot_mode": self._snapshot_mode,
                    "readiness": "separate_endpoint",
                }
            )
            return
        if parsed.path == "/api/readiness":
            from .readiness import read_supervisor_readiness

            self._send(read_supervisor_readiness(self._mtf_server.supervisor_state))
            return
        if self._snapshot_mode:
            # Query/session values are intentionally ignored.  In snapshot
            # mode the path selected when the server was created is the only
            # data source; a URL cannot redirect it to another user file.
            self._handle_snapshot_api(parsed.path)
            return
        if parsed.path == "/api/sessions":
            self._send(self._session_list(query))
            return
        sid = query.get("session", [self._mtf_server.default_session])[0]
        if not sid:
            self._send({"error": "session query parameter is required"}, status=HTTPStatus.BAD_REQUEST)
            return
        self._handle_api(parsed.path, sid, query)


class MTFHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    supervisor_state: str | Path | None = None
    snapshot_reader: SnapshotReader | None = None
    snapshot_path: Path | None = None

    def __init__(
        self,
        address: tuple[str, int],
        store: SQLiteStore | None = None,
        *,
        default_session: str | None = None,
        snapshot_reader: SnapshotReader | None = None,
    ):
        super().__init__(address, _Handler)
        self.store = store
        # The handler only dereferences ``queries`` in DB mode.  Keep a
        # non-optional annotation so the legacy query path remains unchanged;
        # snapshot mode routes before reaching those methods.
        self.queries: QueryService = cast(QueryService, QueryService(store) if store is not None else None)
        self.default_session = default_session
        self.snapshot_reader = snapshot_reader
        self.snapshot_path = snapshot_reader.path if snapshot_reader is not None else None


def create_server(
    db_path: str | Path | None = None,
    *,
    host: str = "127.0.0.1",
    port: int = 8765,
    session_id: str | None = None,
    supervisor_state: str | Path | None = None,
    snapshot_path: str | Path | None = None,
    snapshot_json: str | Path | None = None,
    dashboard_snapshot: str | Path | None = None,
    snapshot_max_bytes: int = SNAPSHOT_MAX_BYTES,
    snapshot_max_age_seconds: float = SNAPSHOT_DEFAULT_MAX_AGE_SECONDS,
) -> MTFHTTPServer:
    requested_snapshots = [value for value in (snapshot_path, snapshot_json, dashboard_snapshot) if value is not None]
    if len(requested_snapshots) > 1:
        raise ValueError("use sólo un parámetro de snapshot")
    if requested_snapshots:
        if db_path is not None:
            raise ValueError("snapshot_path y db_path son modos mutuamente excluyentes")
        reader = SnapshotReader(
            requested_snapshots[0],
            max_bytes=snapshot_max_bytes,
            max_age_seconds=snapshot_max_age_seconds,
        )
        server = MTFHTTPServer((host, int(port)), None, default_session=None, snapshot_reader=reader)
        server.supervisor_state = supervisor_state
        return server
    if db_path is None:
        raise ValueError("db_path es requerido fuera del modo snapshot")
    store = SQLiteStore(db_path, read_only=True)
    if session_id is None:
        sessions = store.sessions(limit=1)
        session_id = sessions[0]["session_id"] if sessions else None
    server = MTFHTTPServer((host, int(port)), store, default_session=session_id)
    server.supervisor_state = supervisor_state
    return server


def serve(
    db_path: str | Path | None = None,
    *,
    host: str = "127.0.0.1",
    port: int = 8765,
    session_id: str | None = None,
    duration: float | None = None,
    supervisor_state: str | Path | None = None,
    snapshot_path: str | Path | None = None,
    snapshot_json: str | Path | None = None,
    dashboard_snapshot: str | Path | None = None,
    snapshot_max_bytes: int = SNAPSHOT_MAX_BYTES,
    snapshot_max_age_seconds: float = SNAPSHOT_DEFAULT_MAX_AGE_SECONDS,
) -> MTFHTTPServer:
    """Serve local read-only UI; optional duration makes smoke tests finite."""
    server = create_server(
        db_path,
        host=host,
        port=port,
        session_id=session_id,
        supervisor_state=supervisor_state,
        snapshot_path=snapshot_path,
        snapshot_json=snapshot_json,
        dashboard_snapshot=dashboard_snapshot,
        snapshot_max_bytes=snapshot_max_bytes,
        snapshot_max_age_seconds=snapshot_max_age_seconds,
    )
    if duration is not None:
        timer = threading.Timer(max(0.0, float(duration)), server.shutdown)
        timer.daemon = True
        timer.start()
    try:
        server.serve_forever(poll_interval=0.2)
    finally:
        server.server_close()
        if server.store is not None:
            server.store.close()
    return server


__all__ = [
    "MTFHTTPServer",
    "SNAPSHOT_DEFAULT_MAX_AGE_SECONDS",
    "SNAPSHOT_INDEX_HTML",
    "SNAPSHOT_MAX_BYTES",
    "SnapshotReader",
    "SnapshotSecurityError",
    "create_server",
    "serve",
]
