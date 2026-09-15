"""Regresiones del contexto de periodo al reanudar capturas históricas."""

from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mtf_lab.data.capture import CaptureEnvelope, MessageClass, iter_jsonl
from mtf_lab.data.ctrader import (
    CTraderHistoryResult,
    CTraderInstrumentSpec,
    normalize_trendbar,
    synthetic_trendbar,
)
from mtf_lab.ops.ctrader_capture import CausalNormalizer, normalize_ctrader_capture
from mtf_lab.ops.ctrader_history_export import export_history_capture

BASE = datetime(2026, 1, 1, tzinfo=UTC)
FIXTURE_ACCOUNT_ID = "fixture-demo-account"


class CTraderCapturePeriodTests(unittest.TestCase):
    @staticmethod
    def _periodless_bar(offset_minutes: int = 0) -> dict[str, object]:
        raw = synthetic_trendbar(timestamp_minutes=int(BASE.timestamp() // 60) + offset_minutes, period="M5")
        del raw["period"]
        return raw

    @staticmethod
    def _envelope(payload: dict[str, object], *, sequence: int = 0) -> CaptureEnvelope:
        received = BASE + timedelta(hours=1)
        return CaptureEnvelope(
            BASE,
            received,
            received,
            sequence,
            1,
            MessageClass.TRENDBAR,
            payload,
            source_identity=f"history-page-{sequence}",
            availability_policy="historical_event_time",
        )

    def _export_periodless(self, target: Path) -> tuple[CaptureEnvelope, CTraderInstrumentSpec]:
        spec = CTraderInstrumentSpec(symbol="EUR/USD", symbol_id=99)
        raw = self._periodless_bar()
        receipt = BASE + timedelta(hours=1)
        response = {"symbolId": 99, "period": 5, "trendbar": [raw], "hasMore": False}
        history = CTraderHistoryResult(
            bars=(
                normalize_trendbar(
                    raw,
                    spec=spec,
                    requested_period=5,
                    response_period=5,
                    received_at=receipt,
                    available_at=receipt,
                ),
            ),
            timeframe="M5",
            pages=1,
            complete=True,
            has_more=False,
            raw_pages=(response,),
            page_metadata=(
                {
                    "received_at": receipt,
                    "available_at": receipt,
                    "ingest_sequence": 0,
                    "connection_generation": 1,
                    "source_identity": "response-page-0",
                    "response_type": "PROTO_OA_GET_TRENDBARS_RES",
                    "request": {
                        "payload_type": "PROTO_OA_GET_TRENDBARS_REQ",
                        "payload": {"symbolId": 99, "period": 5},
                    },
                },
            ),
        )
        catalog = {
            "requested_symbol": "EUR/USD",
            "selected": 99,
            "symbols": [{"symbol_id": 99, "name": "EUR/USD", "digits": 5, "pip_position": 4}],
        }
        exported = export_history_capture(
            target,
            history=history,
            spec=spec,
            catalog=catalog,
            environment="DEMO",
            account_id=FIXTURE_ACCOUNT_ID,
            endpoint="demo.ctraderapi.com:5035",
            permission_scope="SCOPE_VIEW",
        )
        self.assertTrue(exported["complete"])
        row = next(iter(iter_jsonl(target)))
        return CaptureEnvelope.from_mapping(row), spec

    def test_periodless_export_replays_to_one_native_bar(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            envelope, spec = self._export_periodless(Path(tmp) / "history.jsonl")

        result = CausalNormalizer(spec).normalize(envelope)
        capture = normalize_ctrader_capture([envelope], spec=spec)

        self.assertEqual(len(result.bars), 1)
        self.assertGreater(capture.to_dict()["bar_count"], 0)
        self.assertEqual(capture.to_dict()["bar_count"], 1)
        self.assertEqual(result.bars[0].resolution, "M5")
        self.assertEqual(result.bars[0].metadata["requested_period"], 5)
        self.assertEqual(result.bars[0].metadata["response_period"], 5)
        self.assertEqual(result.issues, ())
        self.assertEqual(result.quote_events, ())

    def test_request_response_discrepancy_rejects_without_relabeling(self) -> None:
        raw = self._periodless_bar()
        envelope = self._envelope(
            {
                "period": 7,
                "trendbar": [raw],
                "capture_provenance": {"requested_period": 5, "response_period": 7},
            }
        )
        result = CausalNormalizer(CTraderInstrumentSpec(symbol_id=99)).normalize(envelope)
        capture = normalize_ctrader_capture([envelope], spec=CTraderInstrumentSpec(symbol_id=99))

        self.assertEqual(result.bars, ())
        self.assertEqual(capture.to_dict()["bar_count"], 0)
        self.assertTrue(any("requested=5, response=7" in issue for issue in result.issues))

    def test_periodless_child_without_context_stays_fail_closed(self) -> None:
        result = CausalNormalizer(CTraderInstrumentSpec(symbol_id=99)).normalize(
            self._envelope({"trendbar": [self._periodless_bar()]})
        )

        self.assertEqual(result.bars, ())
        self.assertTrue(any("period no soportado: None" in issue for issue in result.issues))

    def test_response_level_period_is_valid_context_without_provenance(self) -> None:
        result = CausalNormalizer(CTraderInstrumentSpec(symbol_id=99)).normalize(
            self._envelope({"period": "M5", "trendbar": [self._periodless_bar()]})
        )

        self.assertEqual(len(result.bars), 1)
        self.assertEqual(result.bars[0].resolution, "M5")
        self.assertEqual(result.issues, ())

    def test_live_periodless_child_remains_fail_closed(self) -> None:
        received = BASE + timedelta(hours=1)
        payload = {
            "symbolId": 99,
            "timestamp": int(BASE.timestamp() * 1000),
            "bid": 110000,
            "ask": 110020,
            "trendbar": [self._periodless_bar()],
        }
        envelope = CaptureEnvelope(
            BASE,
            received,
            received,
            0,
            1,
            MessageClass.SPOT,
            payload,
        )

        result = CausalNormalizer(CTraderInstrumentSpec(symbol_id=99)).normalize(envelope)

        self.assertEqual(result.bars, ())
        self.assertTrue(any("period no soportado: None" in issue for issue in result.issues))


if __name__ == "__main__":
    unittest.main()
