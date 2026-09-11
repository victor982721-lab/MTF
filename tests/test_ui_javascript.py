"""Opt-in development gates for the inline UI JavaScript.

The normal offline suite does not acquire a Node dependency. Set
``MTF_UI_JS_DEV=1`` (and optionally ``MTF_NODE_BIN``) to make these checks a
hard gate: an unavailable Node runtime is a failure, never a silent skip.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from mtf_lab.ops.ui import INDEX_HTML

_DEV_GATE = os.environ.get("MTF_UI_JS_DEV") == "1"


def _node_path() -> Path | None:
    configured = os.environ.get("MTF_NODE_BIN")
    if configured:
        return Path(configured)
    discovered = shutil.which("node")
    return Path(discovered) if discovered else None


def _extract_javascript(html: str) -> str:
    start = html.index("<script>") + len("<script>")
    end = html.index("</script>", start)
    return html[start:end]


def _extract_css(html: str) -> str:
    start = html.index("<style>") + len("<style>")
    end = html.index("</style>", start)
    return html[start:end]


class UIJavaScriptDevelopmentTests(unittest.TestCase):
    @unittest.skipUnless(
        _DEV_GATE,
        "dev-only: set MTF_UI_JS_DEV=1 to require the CSS containment gate",
    )
    def test_card_css_contains_long_text_without_hiding_data(self) -> None:
        css = _extract_css(INDEX_HTML)
        self.assertIn(".card{min-width:0;overflow-wrap:anywhere;", css)
        self.assertIn(".scroll{overflow:auto;", css)
        self.assertNotIn(".card{overflow:hidden", css)

    def _require_node(self) -> Path:
        node = _node_path()
        self.assertIsNotNone(
            node,
            "MTF_UI_JS_DEV=1 requires Node; set MTF_NODE_BIN to the validated runtime",
        )
        assert node is not None
        self.assertTrue(node.is_file(), f"Node runtime no encontrado: {node}")
        return node

    @unittest.skipUnless(
        _DEV_GATE,
        "dev-only: set MTF_UI_JS_DEV=1 to require the real Node syntax gate",
    )
    def test_index_html_compiles_with_real_node(self) -> None:
        node = self._require_node()
        with tempfile.TemporaryDirectory(prefix="mtf-ui-js-") as directory:
            source = Path(directory) / "index.js"
            source.write_text(_extract_javascript(INDEX_HTML), encoding="utf-8")
            result = subprocess.run(
                [str(node), "--check", str(source)],
                capture_output=True,
                text=True,
                check=False,
                timeout=30,
            )
        self.assertEqual(result.returncode, 0, result.stderr or result.stdout)

    @unittest.skipUnless(
        _DEV_GATE,
        "dev-only: set MTF_UI_JS_DEV=1 to require the VM runtime gate",
    )
    def test_index_html_runtime_renders_all_terminal_tables_without_errors(self) -> None:
        node = self._require_node()
        runner = r"""
'use strict';
const fs = require('fs');
const vm = require('vm');
const source = fs.readFileSync(0, 'utf8');
const elements = new Map();
const errors = [];
const unhandled = [];
function element(id) {
  if (!elements.has(id)) {
    const item = {id, value: id === 'session' ? 's1' : '', textContent: '', _html: '', onclick: null, onchange: null};
    Object.defineProperty(item, 'innerHTML', {get() { return this._html; }, set(value) { this._html = String(value); }});
    elements.set(id, item);
  }
  return elements.get(id);
}
const document = {getElementById: element, body: {insertAdjacentHTML(_where, html) { errors.push(String(html)); }}};
const fixtures = {
  '/api/sessions': [{session_id: 's1', mode: 'REPLAY', instrument: 'EUR/USD'}],
  '/api/status': {mode: 'REPLAY', provider: 'fixture', instrument: 'EUR/USD', counts: {}, connection: 'OFFLINE', analysis_enabled: false, permissions: {}, warmup_pending: {}, gaps: []},
  '/api/candles': {items: [{timeframe: 'M1', start_ts: '2026-01-01T00:00:00Z', end_ts: '2026-01-01T00:01:00Z', close: 1.1, closed: true, revision: 0, indicator_values: {}}], next_cursor: null},
  '/api/indicators': {items: [{close: 1.1, ema_fast: 1.1, ema_slow: 1.1, rsi: 50, atr: 0.01}]},
  '/api/gaps': {items: []},
  '/api/signals': {items: [{detected_ts: '2026-01-01T00:00:00Z', direction: 'UP', status: 'VALID'}]},
  '/api/conditions': {items: [{observed_ts: '2026-01-01T00:00:00Z', name: 'context', state: 'fulfilled'}]},
  '/api/discards': {items: [{observed_ts: '2026-01-01T00:00:00Z', reason_code: 'none'}]},
  '/api/simulations': {items: [{detected_ts: '2026-01-01T00:00:00Z', outcome: 'PENDING', net_result: null}]},
  '/api/cfd-trades': {items: [{detected_at: '2026-01-01T00:00:00Z', trade_id: 'cfd-1', state: 'CLOSED', economic_state: 'INDETERMINATE', net_pnl: null}]},
};
function fetch(url) {
  const path = String(url).split('?')[0];
  const body = fixtures[path];
  if (body === undefined) return Promise.reject(new Error(`unexpected fetch: ${path}`));
  return Promise.resolve({ok: true, text: async () => JSON.stringify(body), json: async () => body});
}
process.on('unhandledRejection', error => unhandled.push(String(error && (error.stack || error))));
process.on('uncaughtException', error => unhandled.push(String(error && (error.stack || error))));
const context = {document, fetch, console, Promise, Error, JSON, Math, Number, String, Object, Array, encodeURIComponent, decodeURIComponent, setInterval: () => 1, clearInterval: () => {}, setTimeout, clearTimeout, setImmediate};
try {
  vm.runInNewContext(source, context, {filename: 'index-inline.js'});
} catch (error) {
  unhandled.push(String(error && (error.stack || error)));
}
(async () => {
  for (let i = 0; i < 4; i += 1) await new Promise(resolve => setImmediate(resolve));
  const ids = ['signals', 'conditions', 'discards', 'sims', 'cfdTrades'];
  const tables = Object.fromEntries(ids.map(id => [id, element(id).innerHTML.includes('<table>')]));
  const bad = [...elements.values()].filter(item => item._html.includes('class="bad"') || item.textContent.includes('bad')).map(item => item.id);
  const prefix = context.pathFor([{close: null}, {close: 2}, {close: 3}], 'close');
  const gap = context.pathFor([{close: 1}, {close: null}, {close: 3}], 'close');
  const result = {errors, unhandled, tables, bad, mode: element('mode').textContent, paths: {prefix, gap}};
  process.stdout.write(JSON.stringify(result));
  if (errors.length || unhandled.length || bad.length || Object.values(tables).some(value => !value) || prefix !== 'M260.0 150.0 L520.0 0.0' || gap !== 'M0.0 150.0 M520.0 0.0') process.exitCode = 1;
})().catch(error => { process.stderr.write(String(error && (error.stack || error))); process.exitCode = 2; });
"""
        result = subprocess.run(
            [str(node), "-e", runner],
            input=_extract_javascript(INDEX_HTML),
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stderr or result.stdout)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["errors"], [])
        self.assertEqual(payload["unhandled"], [])
        self.assertEqual(payload["bad"], [])
        self.assertEqual(
            payload["tables"],
            {"signals": True, "conditions": True, "discards": True, "sims": True, "cfdTrades": True},
        )
        self.assertEqual(payload["paths"], {"prefix": "M260.0 150.0 L520.0 0.0", "gap": "M0.0 150.0 M520.0 0.0"})


if __name__ == "__main__":
    unittest.main()
