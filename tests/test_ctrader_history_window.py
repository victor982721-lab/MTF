"""Bounded explicit-window selection for paginated native cTrader history."""

from __future__ import annotations

import json
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from mtf_lab.data.ctrader import CTraderHistoryResult, CTraderInstrumentSpec, normalize_trendbar, synthetic_trendbar
from mtf_lab.ops.ctrader_history_export import (
    HistoryCaptureError,
    export_history_capture,
    inspect_historical_capture,
    select_bounded_history_window,
)

BASE = datetime(2026, 1, 1, tzinfo=UTC)


class BoundedHistoryWindowTests(unittest.TestCase):
    @staticmethod
    def _page(starts: tuple[datetime, ...], *, has_more: bool = True) -> dict[str, object]:
        return {
            "symbolId": 99,
            "period": 1,
            "hasMore": has_more,
            "trendbar": [
                synthetic_trendbar(timestamp_minutes=int(item.timestamp() // 60), period="M1") for item in starts
            ],
        }

    def _history(self, *, gap: bool = False, issues: tuple[str, ...] = ()) -> CTraderHistoryResult:
        spec = CTraderInstrumentSpec(symbol_id=99)
        received = BASE + timedelta(hours=1)
        page0 = self._page((BASE + timedelta(minutes=3), BASE + timedelta(minutes=2)))
        page1_starts = (BASE, BASE - timedelta(minutes=1)) if gap else (BASE + timedelta(minutes=1), BASE)
        page1 = self._page(page1_starts, has_more=True)
        pages = (page0, page1)
        metadata = tuple(
            {
                "received_at": received,
                "available_at": received,
                "ingest_sequence": index,
                "connection_generation": 1,
                "source_identity": f"history-page-{index}",
                "response_type": "PROTO_OA_GET_TRENDBARS_RES",
                "request": {
                    "payload_type": "PROTO_OA_GET_TRENDBARS_REQ",
                    "payload": {"period": 1, "symbolId": 99},
                },
            }
            for index in range(len(pages))
        )
        bars = tuple(
            normalize_trendbar(
                raw,
                spec=spec,
                requested_period=1,
                response_period=1,
                received_at=received,
                available_at=received,
            )
            for page in pages
            for raw in page["trendbar"]  # type: ignore[index]
        )
        return CTraderHistoryResult(
            bars=bars,
            timeframe="M1",
            pages=len(pages),
            complete=False,
            has_more=True,
            issues=issues,
            raw_pages=pages,
            page_metadata=metadata,
        )

    def test_selection_is_new_bounded_view_and_export_is_complete(self) -> None:
        history = self._history()
        original_count = len(history.raw_pages[0]["trendbar"])  # type: ignore[index]
        selection = select_bounded_history_window(
            history,
            start=BASE,
            end=BASE + timedelta(minutes=4),
        )
        self.assertTrue(selection.history.complete)
        self.assertFalse(selection.history.has_more)
        self.assertTrue(selection.source_has_more)
        self.assertFalse(selection.source_complete)
        self.assertEqual(selection.source_pages, 2)
        self.assertEqual(selection.selected_bars, 4)
        self.assertEqual(len(selection.history.bars), 4)
        self.assertEqual(len(history.raw_pages[0]["trendbar"]), original_count)  # type: ignore[index]
        self.assertTrue(history.has_more)

        with TemporaryDirectory(prefix="mtf-history-window-") as directory:
            target = Path(directory) / "bounded.jsonl"
            exported = export_history_capture(
                target,
                history=selection.history,
                spec=CTraderInstrumentSpec(symbol_id=99),
                catalog={
                    "requested_symbol": "EUR/USD",
                    "selected": 99,
                    "symbols": [{"symbol_id": 99, "name": "EUR/USD", "digits": 5, "pip_position": 4}],
                },
                environment="DEMO",
                account_id=7,
                endpoint="demo.ctraderapi.com:5035",
                permission_scope="SCOPE_VIEW",
            )
            self.assertTrue(exported["complete"])
            self.assertFalse(exported["has_more"])
            metadata = inspect_historical_capture(target)
            self.assertIsNotNone(metadata)
            assert metadata is not None
            self.assertTrue(metadata["complete"])
            self.assertFalse(metadata["has_more"])
            rows = [json.loads(line) for line in target.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(
                sum(
                    len(row.get("payload", {}).get("trendbar", ()))
                    for row in rows
                    if row["message_class"] == "trendbar"
                ),
                4,
            )
            self.assertEqual(rows[-1]["message_class"], "end")

    def test_gap_is_not_reclassified_as_complete(self) -> None:
        with self.assertRaisesRegex(HistoryCaptureError, "contiene gaps"):
            select_bounded_history_window(
                self._history(gap=True),
                start=BASE,
                end=BASE + timedelta(minutes=4),
            )

    def test_source_issue_is_preserved_as_blocking(self) -> None:
        with self.assertRaisesRegex(HistoryCaptureError, "incidencias"):
            select_bounded_history_window(
                self._history(issues=("source pagination issue",)),
                start=BASE,
                end=BASE + timedelta(minutes=4),
            )


if __name__ == "__main__":
    unittest.main()
