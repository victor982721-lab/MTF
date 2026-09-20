"""Offline browser behavior: immutable queries and visible, coherent pagination."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import unittest
from pathlib import Path

from mtf_lab.ops.ui import INDEX_HTML

_HARNESS = r"""
const vm = require('vm');
const fs = require('fs');
const input = JSON.parse(fs.readFileSync(0, 'utf8'));
const elements = new Map();
function element(id) {
  if (!elements.has(id)) elements.set(id, {value: id === 'session' ? 'A' : '', innerHTML: '', textContent: '', onclick: null});
  return elements.get(id);
}
const calls = [];
const candle = (label, tf='M1') => ({timeframe: tf, start_ts: label, end_ts: label, close: 1, closed: true});
const normal = path => {
  const [route, query] = path.split('?');
  const params = new URLSearchParams(query);
  const session = params.get('session');
  if (route === '/api/poll' || route === '/api/status') {
    const status = {mode: 'SESSION_' + session, provider: 'fixture', instrument: 'synthetic', counts: {candles: 1}};
    return route === '/api/poll' ? {status} : status;
  }
  if (route === '/api/candles') return {items: [candle('ROW_' + session)], next_cursor: 'page-2-' + session};
  return {items: [], next_cursor: null};
};
const deferred = () => {let resolve, reject; const promise = new Promise((yes, no) => {resolve=yes;reject=no;});return {promise,resolve,reject};};
let handler = normal;
const context = vm.createContext({
  document: {getElementById: element},
  fetch: async path => {
    calls.push(path);
    const body = await handler(path);
    return {ok: true, json: async () => body};
  },
  element, calls, candle, normal, deferred, URLSearchParams,
  setHandler: fn => {handler=fn;},
  tick: () => new Promise(resolve => setImmediate(resolve)),
});
vm.runInContext(input.source.split('(async()=>{await loadReadiness()')[0], context);
(async () => {
  const result = await vm.runInContext('(async()=>{' + input.scenario + '})()', context);
  process.stdout.write(JSON.stringify(result));
})().catch(error => {process.stderr.write(String(error.stack || error));process.exitCode=1;});
"""


@unittest.skipUnless(os.environ.get("MTF_UI_JS_DEV") == "1", "dev-only: real Node UI behavior gate")
class UIQueryConsistencyTests(unittest.TestCase):
    def run_scenario(self, scenario: str) -> dict[str, object]:
        node = os.environ.get("MTF_NODE_BIN") or shutil.which("node")
        self.assertTrue(node and Path(node).is_file(), "UI development gate requires Node")
        start = INDEX_HTML.index("<script>") + len("<script>")
        source = INDEX_HTML[start : INDEX_HTML.index("</script>", start)]
        result = subprocess.run(
            [str(node), "-e", _HARNESS],
            input=json.dumps({"source": source, "scenario": scenario}),
            text=True,
            capture_output=True,
            check=False,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertIsInstance(payload, dict)
        return payload

    def test_refresh_uses_one_query_snapshot_and_does_not_publish_changed_filters(self) -> None:
        result = self.run_scenario(r"""
element('start').value='START_A';element('end').value='END_A';element('revisions').value='all';
setHandler(path => {if(path.startsWith('/api/poll?')){element('session').value='B';element('start').value='START_B';}return normal(path);});
await load();
return {sessions:calls.map(path=>new URLSearchParams(path.split('?')[1]).get('session')),
        starts:calls.filter(path=>!path.startsWith('/api/status?')).map(path=>new URLSearchParams(path.split('?')[1]).get('start_ts')),
        html:element('candles').innerHTML, good:refreshState('A').lastGoodAt};
""")
        self.assertEqual(set(result["sessions"]), {"A"})  # type: ignore[arg-type]
        self.assertEqual(set(result["starts"]), {"START_A"})  # type: ignore[arg-type]
        self.assertEqual(result["html"], "")
        self.assertIsNone(result["good"])

    def test_old_success_cannot_replace_new_session(self) -> None:
        result = self.run_scenario(r"""
const wait=deferred();setHandler(path=>path.startsWith('/api/poll?')&&path.includes('session=A')?wait.promise:normal(path));
const old=load();await tick();element('session').value='B';await load();
const before=element('candles').innerHTML;wait.resolve({status:{mode:'OLD_A'}});await old;
return {mode:element('mode').textContent, before, after:element('candles').innerHTML,
        goodA:refreshState('A').lastGoodAt, goodB:refreshState('B').lastGoodAt};
""")
        self.assertTrue(str(result["mode"]).startswith("SESSION_B"))
        self.assertEqual(result["before"], result["after"])
        self.assertIsNone(result["goodA"])
        self.assertIsNotNone(result["goodB"])

    def test_old_error_cannot_restore_previous_snapshot_over_new_refresh(self) -> None:
        result = self.run_scenario(r"""
await load();const wait=deferred();let first=true;
setHandler(path=>{if(first&&path.startsWith('/api/candles?')){first=false;return wait.promise;}return normal(path);});
const old=load();await tick();await load();const good=refreshState('A').lastGoodAt;
wait.reject(Error('old request failed'));await old;
return {error:refreshState('A').error, good:good===refreshState('A').lastGoodAt, html:element('candles').innerHTML};
""")
        self.assertIsNone(result["error"])
        self.assertTrue(result["good"])
        self.assertIn("ROW_A", str(result["html"]))

    def test_pagination_renders_initial_and_new_rows_once_with_single_inflight_page(self) -> None:
        result = self.run_scenario(r"""
const wait=deferred();
setHandler(path=>{if(path.startsWith('/api/candles?'))return path.includes('cursor=')?wait.promise:{items:Array.from({length:35},(_,i)=>candle('FIRST_'+i)),next_cursor:'NEXT'};return normal(path);});
await load();const first=loadMoreCandles('NEXT');const duplicate=loadMoreCandles('NEXT');
wait.resolve({items:[candle('SECOND_0'),candle('SECOND_1')],next_cursor:null});await Promise.all([first,duplicate]);
return {html:element('candles').innerHTML, count:candleView.items.length,
        requests:calls.filter(path=>path.includes('cursor=')).length, pager:element('candlePager').innerHTML};
""")
        self.assertEqual(result["count"], 37)
        self.assertEqual(result["requests"], 1)
        for label in ("FIRST_0", "FIRST_34", "SECOND_0", "SECOND_1"):
            self.assertIn(label, str(result["html"]))
        self.assertIn("Fin de velas", str(result["pager"]))

    def test_old_page_and_page_error_are_ignored_after_filter_change(self) -> None:
        for fails in (False, True):
            with self.subTest(fails=fails):
                result = self.run_scenario(
                    r"""
const wait=deferred();setHandler(path=>path.includes('cursor=')?wait.promise:normal(path));
await load();const page=loadMoreCandles('page-2-A');await tick();
element('session').value='B';await load();const before=element('candles').innerHTML;
"""
                    + (
                        "wait.reject(Error('stale page'));"
                        if fails
                        else "wait.resolve({items:[candle('STALE_A')],next_cursor:null});"
                    )
                    + r"""
await page;return {same:before===element('candles').innerHTML, html:element('candles').innerHTML,
                  pager:element('candlePager').innerHTML, query:candleView.query.session};
"""
                )
                self.assertTrue(result["same"])
                self.assertEqual(result["query"], "B")
                self.assertNotIn("STALE_A", str(result["html"]))
                self.assertNotIn('class="bad"', str(result["pager"]))

    def test_page_error_preserves_data_and_cursor_for_retry(self) -> None:
        result = self.run_scenario(r"""
await load();const before=element('candles').innerHTML;
setHandler(path=>{if(path.includes('cursor='))throw Error('page error');return normal(path);});
await loadMoreCandles('page-2-A');const unchanged=before===element('candles').innerHTML;
const requestCount=calls.length;await autoRefresh();const autoPreserved=calls.length===requestCount;
const cursor=candleView.nextCursor;setHandler(path=>path.includes('cursor=')?{items:[candle('RETRY')],next_cursor:null}:normal(path));
await loadMoreCandles(cursor);return {unchanged,autoPreserved,cursor,html:element('candles').innerHTML};
""")
        self.assertTrue(result["unchanged"])
        self.assertTrue(result["autoPreserved"])
        self.assertEqual(result["cursor"], "page-2-A")
        self.assertIn("ROW_A", str(result["html"]))
        self.assertIn("RETRY", str(result["html"]))

    def test_automatic_refresh_preserves_exploration_until_explicit_refresh(self) -> None:
        result = self.run_scenario(r"""
const wait=deferred();setHandler(path=>path.includes('cursor=')?wait.promise:normal(path));
await load();const page=loadMoreCandles('page-2-A');await tick();
const inflightCount=calls.length;await autoRefresh();const pendingPreserved=calls.length===inflightCount;
wait.resolve({items:[candle('PAGE_TWO')],next_cursor:null});await page;
const loadedCount=calls.length;await autoRefresh();const loadedPreserved=calls.length===loadedCount;
const pausedLabel=element('candlePager').innerHTML.includes('automática en pausa');
const explored=element('candles').innerHTML.includes('PAGE_TWO');await load();
const reset=!element('candles').innerHTML.includes('PAGE_TWO');const refreshCount=calls.length;await autoRefresh();
return {pendingPreserved,loadedPreserved,pausedLabel,explored,reset,automaticResumed:calls.length>refreshCount};
""")
        self.assertTrue(all(result.values()), result)

    def test_timer_neither_supersedes_pending_refresh_nor_applies_unsubmitted_filters(self) -> None:
        result = self.run_scenario(r"""
const wait=deferred();setHandler(path=>path.startsWith('/api/poll?')?wait.promise:normal(path));
const pending=load();await tick();const firstCount=calls.length;await autoRefresh();
const noOverlap=calls.length===firstCount;wait.resolve({status:{mode:'FIXTURE'}});await pending;
setHandler(normal);element('timeframe').value='M5';const before=calls.length;await autoRefresh();
const unsubmittedPreserved=before===calls.length;await load();
return {noOverlap,unsubmittedPreserved,explicitApplied:candleView.query.timeframe==='M5'};
""")
        self.assertTrue(all(result.values()), result)

    def test_filter_edit_during_first_request_requires_explicit_apply(self) -> None:
        result = self.run_scenario(r"""
const wait=deferred();setHandler(path=>path.startsWith('/api/poll?')?wait.promise:normal(path));
const pending=load();await tick();element('timeframe').value='M5';
wait.resolve({status:{mode:'FIXTURE'}});await pending;
const before=calls.length;await autoRefresh();const noRequest=before===calls.length;
const noCommit=committedQuery===null;setHandler(normal);await load();
return {noRequest,noCommit,explicitApplied:candleView.query.timeframe==='M5'};
""")
        self.assertTrue(all(result.values()), result)

    def test_failed_refresh_preserves_page_state_but_changed_filters_cannot_reuse_cursor(self) -> None:
        result = self.run_scenario(r"""
await load();setHandler(path=>{if(path.startsWith('/api/indicators?'))throw Error('chart error');return normal(path);});
await load();const before=element('candles').innerHTML;setHandler(normal);
await loadMoreCandles('page-2-A');const count=candleView.items.length;
element('timeframe').value='M5';const requestCount=calls.length;await loadMoreCandles(candleView.nextCursor);
return {preserved:before.includes('ROW_A'),count,noRequest:requestCount===calls.length};
""")
        self.assertTrue(result["preserved"])
        self.assertEqual(result["count"], 2)
        self.assertTrue(result["noRequest"])
