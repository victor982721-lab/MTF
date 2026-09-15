"""Fail-closed gates for bounded historical cTrader PAPER replay."""

from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mtf_lab.data.capture import CaptureEnvelope, MessageClass
from mtf_lab.data.ctrader import (
    CTraderHistoryResult,
    CTraderInstrumentSpec,
    normalize_trendbar,
    synthetic_trendbar,
)
from mtf_lab.ops.ctrader_capture import CausalNormalizer
from mtf_lab.ops.ctrader_history_export import export_history_capture, select_bounded_history_window
from mtf_lab.ops.ctrader_pipeline import CTraderPipeline
from mtf_lab.ops.persistence import SQLiteStore
from tests.test_ctrader_pipeline import pipeline_config

BASE = datetime(2026, 1, 1, tzinfo=UTC)
FIXTURE_ACCOUNT_ID = "fixture-demo-account"


class HistoricalPaperGateTests(unittest.TestCase):
    @staticmethod
    def _catalog() -> dict[str, object]:
        return {
            "requested_symbol": "EUR/USD",
            "selected": 99,
            "symbols": [{"symbol_id": 99, "name": "EUR/USD", "digits": 5, "pip_position": 4}],
        }

    @staticmethod
    def _source_history(
        *, gap: bool = False, complete: bool = False
    ) -> tuple[CTraderHistoryResult, CTraderInstrumentSpec]:
        spec = CTraderInstrumentSpec(symbol="EUR/USD", symbol_id=99)
        starts = [BASE + timedelta(minutes=index) for index in range(4)]
        if gap:
            starts = [starts[0], starts[1], starts[3]]
        raw_bars = [synthetic_trendbar(timestamp_minutes=int(start.timestamp() // 60), period="M1") for start in starts]
        # Source pages are intentionally in recent-to-old order, as cTrader
        # returns them.  The selector creates a new bounded view in market time.
        split = max(1, len(raw_bars) // 2)
        raw_pages = (
            {"symbolId": 99, "period": 1, "hasMore": True, "trendbar": raw_bars[split:]},
            {"symbolId": 99, "period": 1, "hasMore": False, "trendbar": raw_bars[:split]},
        )
        receipt = BASE + timedelta(hours=1)
        metadata = tuple(
            {
                "received_at": receipt,
                "available_at": receipt,
                "ingest_sequence": index,
                "connection_generation": 1,
                "source_identity": f"history-page-{index}",
                "request": {
                    "payload_type": "PROTO_OA_GET_TRENDBARS_REQ",
                    "payload": {"symbolId": 99, "period": 1},
                },
            }
            for index in range(len(raw_pages))
        )
        bars = tuple(
            normalize_trendbar(
                raw,
                spec=spec,
                received_at=receipt,
                available_at=receipt,
                requested_period=1,
                response_period=1,
            )
            for raw in raw_bars
        )
        return (
            CTraderHistoryResult(
                bars=bars,
                timeframe="M1",
                pages=len(raw_pages),
                complete=complete,
                has_more=not complete,
                raw_pages=raw_pages,
                page_metadata=metadata,
            ),
            spec,
        )

    def _export(
        self, target: Path, history: CTraderHistoryResult, spec: CTraderInstrumentSpec
    ) -> list[CaptureEnvelope]:
        result = export_history_capture(
            target,
            history=history,
            spec=spec,
            catalog=self._catalog(),
            environment="DEMO",
            account_id=FIXTURE_ACCOUNT_ID,
            endpoint="demo.ctraderapi.com:5035",
            permission_scope="SCOPE_VIEW",
        )
        self.assertTrue(result["complete"])
        return [CaptureEnvelope.from_mapping(json.loads(line)) for line in target.read_text().splitlines()]

    def test_bounded_complete_export_admits_market_coverage_but_not_quotes(self) -> None:
        source, spec = self._source_history()
        selection = select_bounded_history_window(source, start=BASE, end=BASE + timedelta(minutes=4))

        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "bounded.jsonl"
            envelopes = self._export(target, selection.history, spec)
            end = envelopes[-1]
            self.assertEqual(end.message_class, MessageClass.END)
            self.assertEqual(end.payload["continuity"], "CONTINUOUS")
            self.assertTrue(end.payload["bounded_selection_verified"])

            config = replace(pipeline_config(), price_base="native")
            with SQLiteStore(Path(tmp) / "paper.sqlite3") as store:
                result = CTraderPipeline(store, config, spec=spec).run(
                    envelopes,
                    session_id="bounded-history",
                    order="market_time_corrected",
                )

            coverage = result.capture.coverage.to_dict()
            self.assertTrue(coverage["coverage_satisfied"])
            self.assertEqual(coverage["observed_start"], "2026-01-01T00:00:00.000000Z")
            self.assertEqual(coverage["observed_end"], "2026-01-01T00:04:00.000000Z")
            self.assertNotIn("quality_blocked", result.capture.issues)
            self.assertEqual(
                result.capture.provenance,
                {
                    "provider": "ctrader-open-api",
                    "instrument": "EUR/USD",
                    "mode": "REPLAY",
                    "order": "market_time_corrected",
                    "bounded_selection_verified": True,
                },
            )
            self.assertTrue(result.paper.finished)
            self.assertTrue(result.paper.capture_complete)
            self.assertEqual(result.trades, ())
            self.assertEqual(result.cfd_signals, ())

    def test_historical_event_time_with_receipt_is_not_quality_blocked(self) -> None:
        source, spec = self._source_history()
        selection = select_bounded_history_window(source, start=BASE, end=BASE + timedelta(minutes=4))
        with tempfile.TemporaryDirectory() as tmp:
            envelopes = self._export(Path(tmp) / "bounded.jsonl", selection.history, spec)
        normalizer = CausalNormalizer(spec)
        normalized = tuple(
            bar
            for envelope in envelopes
            if envelope.message_class is MessageClass.TRENDBAR
            for bar in normalizer.normalize(envelope).bars
        )
        self.assertEqual(len(normalized), 4)
        metadata = normalized[0].metadata
        self.assertNotIn("insufficient", metadata.get("quality_flags", ()))
        self.assertNotIn("original_receipt_unknown", metadata.get("quality_reasons", ()))
        self.assertTrue(normalized[0].closed)
        self.assertEqual(normalized[0].received_at, envelopes[0].received_at)

        without_receipt = replace(envelopes[0], received_at=None)
        unknown = CausalNormalizer(spec).normalize(without_receipt)
        self.assertIn("insufficient", unknown.bars[0].metadata["quality_flags"])
        self.assertIn("original_receipt_unknown", unknown.bars[0].metadata["quality_reasons"])

    def test_complete_unbounded_response_does_not_claim_continuity(self) -> None:
        source, spec = self._source_history(complete=True)
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "unbounded.jsonl"
            envelopes = self._export(target, source, spec)
            self.assertEqual(envelopes[-1].payload["continuity"], "UNKNOWN")
            config = replace(pipeline_config(), price_base="native")
            with SQLiteStore(Path(tmp) / "paper.sqlite3") as store:
                result = CTraderPipeline(store, config, spec=spec).run(
                    envelopes,
                    session_id="unbounded-history",
                    order="market_time_corrected",
                )
            self.assertFalse(result.capture.coverage.complete)
            self.assertEqual(result.capture.coverage.continuity, "UNKNOWN")

    def test_marked_complete_gap_does_not_claim_continuity(self) -> None:
        source, spec = self._source_history(gap=True, complete=True)
        marker = {
            "requested_start": "2026-01-01T00:00:00Z",
            "requested_end": "2026-01-01T00:04:00Z",
            "source_has_more": False,
        }
        marked = replace(
            source,
            raw_pages=tuple({**page, "bounded_selection": marker} for page in source.raw_pages),
            page_metadata=tuple({**metadata, "bounded_selection": True} for metadata in source.page_metadata),
        )
        with tempfile.TemporaryDirectory() as tmp:
            envelopes = self._export(Path(tmp) / "gap.jsonl", marked, spec)
        self.assertEqual(envelopes[-1].payload["bounded_selection_verified"], True)
        self.assertEqual(envelopes[-1].payload["continuity"], "UNKNOWN")

    def test_first_bounded_v22_export_remains_readable_without_end_marker(self) -> None:
        source, spec = self._source_history()
        selection = select_bounded_history_window(source, start=BASE, end=BASE + timedelta(minutes=4))
        with tempfile.TemporaryDirectory() as tmp:
            envelopes = self._export(Path(tmp) / "bounded.jsonl", selection.history, spec)
            legacy_end = dict(envelopes[-1].payload)
            legacy_end.pop("bounded_selection_verified")
            legacy_end["continuity"] = "UNKNOWN"
            envelopes[-1] = replace(envelopes[-1], payload=legacy_end)
            config = replace(pipeline_config(), price_base="native")
            with SQLiteStore(Path(tmp) / "paper.sqlite3") as store:
                result = CTraderPipeline(store, config, spec=spec).run(
                    envelopes,
                    session_id="bounded-v22-legacy",
                    order="market_time_corrected",
                )
        self.assertTrue(result.capture.coverage.complete)
        self.assertEqual(result.capture.coverage.continuity, "CONTINUOUS")
        self.assertTrue(result.paper.capture_complete)

    def test_bounded_selection_preserves_partial_source_provenance(self) -> None:
        source, spec = self._source_history()
        selection = select_bounded_history_window(source, start=BASE, end=BASE + timedelta(minutes=4))
        # The selector's output is complete, but this test documents the source
        # partial state separately: a source result is never relabeled in place.
        self.assertFalse(selection.source_complete)
        self.assertTrue(selection.source_has_more)
        self.assertTrue(selection.history.complete)


if __name__ == "__main__":
    unittest.main()
