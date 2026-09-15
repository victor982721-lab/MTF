"""Regresiones de periodo para la exportación histórica cTrader."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mtf_lab.data.ctrader import CTraderHistoryResult, CTraderInstrumentSpec, normalize_trendbar, synthetic_trendbar
from mtf_lab.ops.ctrader_history_export import HistoryCaptureError, export_history_capture

BASE = datetime(2026, 1, 1, tzinfo=UTC)
FIXTURE_ACCOUNT_ID = "fixture-demo-account"


class CTraderHistoryExportPeriodTests(unittest.TestCase):
    @staticmethod
    def _periodless_bar(offset_minutes: int) -> dict[str, object]:
        raw = synthetic_trendbar(timestamp_minutes=int(BASE.timestamp() // 60) + offset_minutes, period="M1")
        del raw["period"]
        return raw

    @staticmethod
    def _history(
        *,
        timeframe: str,
        requested_period: int,
        response_period: int | None,
        bars: list[dict[str, object]],
    ) -> tuple[CTraderHistoryResult, CTraderInstrumentSpec, dict[str, object]]:
        spec = CTraderInstrumentSpec(symbol="EUR/USD", symbol_id=99)
        response: dict[str, object] = {"symbolId": 99, "trendbar": bars, "hasMore": False}
        if response_period is not None:
            response["period"] = response_period
        receipt = BASE + timedelta(hours=1)
        normalized = tuple(
            normalize_trendbar(
                raw,
                spec=spec,
                requested_period=requested_period,
                response_period=response_period,
                received_at=receipt,
                available_at=receipt,
            )
            for raw in bars
        )
        history = CTraderHistoryResult(
            bars=normalized,
            timeframe=timeframe,
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
                        "payload": {"symbolId": 99, "period": requested_period},
                    },
                },
            ),
        )
        catalog = {
            "requested_symbol": "EUR/USD",
            "selected": 99,
            "symbols": [{"symbol_id": 99, "name": "EUR/USD", "digits": 5, "pip_position": 4}],
        }
        return history, spec, catalog

    def test_periodless_bars_export_complete_with_page_request_response_context(self) -> None:
        bars = [self._periodless_bar(0), self._periodless_bar(5)]
        history, spec, catalog = self._history(timeframe="M5", requested_period=5, response_period=5, bars=bars)

        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "history.jsonl"
            result = export_history_capture(
                target,
                history=history,
                spec=spec,
                catalog=catalog,
                environment="DEMO",
                account_id=FIXTURE_ACCOUNT_ID,
                endpoint="demo.ctraderapi.com:5035",
                permission_scope="SCOPE_VIEW",
            )
            self.assertTrue(result["complete"])
            self.assertEqual(result["raw_bars"], 2)
            self.assertEqual(result["native_bars"], 2)
            self.assertEqual(result["timeframe"], "M5")
            self.assertEqual(result["requested_period"], 5)

            first = json.loads(target.read_text(encoding="utf-8").splitlines()[0])
            payload = first["payload"]
            provenance = payload["capture_provenance"]
            self.assertEqual(provenance["requested_period"], 5)
            self.assertEqual(provenance["response_period"], 5)
            self.assertEqual(
                provenance["period_context"],
                {"requested_period": 5, "response_period": 5, "child_period_optional": True},
            )
            self.assertNotIn("period", payload["trendbar"][0])

    def test_periodless_bars_use_request_when_response_period_is_absent(self) -> None:
        history, spec, catalog = self._history(
            timeframe="M15", requested_period=7, response_period=None, bars=[self._periodless_bar(0)]
        )

        with tempfile.TemporaryDirectory() as tmp:
            result = export_history_capture(
                Path(tmp) / "history.jsonl",
                history=history,
                spec=spec,
                catalog=catalog,
                environment="DEMO",
                account_id=FIXTURE_ACCOUNT_ID,
                endpoint="demo.ctraderapi.com:5035",
                permission_scope="SCOPE_VIEW",
            )
        self.assertTrue(result["complete"])
        self.assertEqual(result["issues"], [])
        self.assertEqual(result["requested_period"], 7)

    def test_response_period_discrepancy_is_rejected_before_capture_publish(self) -> None:
        history, spec, catalog = self._history(
            timeframe="M5", requested_period=5, response_period=5, bars=[self._periodless_bar(0)]
        )
        raw_page = dict(history.raw_pages[0])
        raw_page["period"] = 7
        history = CTraderHistoryResult(
            history.bars,
            history.timeframe,
            history.pages,
            history.complete,
            history.has_more,
            history.issues,
            (raw_page,),
            history.page_metadata,
        )

        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "mismatch.jsonl"
            with self.assertRaisesRegex(HistoryCaptureError, "periodo discrepante") as caught:
                export_history_capture(
                    target,
                    history=history,
                    spec=spec,
                    catalog=catalog,
                    environment="DEMO",
                    account_id=FIXTURE_ACCOUNT_ID,
                    endpoint="demo.ctraderapi.com:5035",
                    permission_scope="SCOPE_VIEW",
                )
            self.assertTrue(any("requested=5, response=7" in issue for issue in caught.exception.issues))
            self.assertFalse(target.exists())

    def test_request_period_discrepancy_is_rejected_without_defaulting_to_m1(self) -> None:
        history, spec, catalog = self._history(
            timeframe="M5", requested_period=7, response_period=7, bars=[self._periodless_bar(0)]
        )
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(HistoryCaptureError, "periodo solicitado inconsistente") as caught:
                export_history_capture(
                    Path(tmp) / "mismatch.jsonl",
                    history=history,
                    spec=spec,
                    catalog=catalog,
                    environment="DEMO",
                    account_id=FIXTURE_ACCOUNT_ID,
                    endpoint="demo.ctraderapi.com:5035",
                    permission_scope="SCOPE_VIEW",
                )
            self.assertTrue(any("history=5, request=7" in issue for issue in caught.exception.issues))


if __name__ == "__main__":
    unittest.main()
