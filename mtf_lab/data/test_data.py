"""Offline smoke tests for the provider-neutral data layer.

These tests intentionally avoid network access.  They exercise the contract
that a CLI/core can rely on and leave the live Kraken test as an explicit,
optional connectivity check.
"""

from __future__ import annotations

import csv
import io
import json
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from .importer import ColumnMapping, ImportConfig, import_csv, import_jsonl
from .kraken import KrakenAPIError, KrakenPublicAdapter
from .synthetic import SyntheticGenerator


class SyntheticTests(unittest.TestCase):
    def test_same_seed_is_identical_and_tagged(self) -> None:
        left = SyntheticGenerator(seed=99).generate(periods=24, scenario="pullback")
        right = SyntheticGenerator(seed=99).generate(periods=24, scenario="pullback")
        self.assertEqual([item.to_dict() for item in left], [item.to_dict() for item in right])
        self.assertTrue(left.provenance.synthetic)
        self.assertIn("SINTETICO", left.provenance.notes[0])

    def test_problem_fixture_reports_gap_duplicate_order(self) -> None:
        result = SyntheticGenerator(seed=4).generate(
            periods=24,
            scenario="volatility_change",
            anomalies=("gap", "duplicate", "out_of_order"),
        )
        self.assertGreaterEqual(result.quality.gap_count, 1)
        self.assertGreaterEqual(result.quality.duplicate_count, 1)
        self.assertGreaterEqual(result.quality.out_of_order_count, 1)


class ImportTests(unittest.TestCase):
    def test_csv_and_jsonl_keep_explicit_price_basis(self) -> None:
        with TemporaryDirectory() as temp:
            root = Path(temp)
            csv_path = root / "bars.csv"
            csv_path.write_text(
                "timestamp,instrument,resolution,open,high,low,close\n"
                "2024-01-01T00:00:00Z,SYNTH/USD,M1,10,12,9,11\n",
                encoding="utf-8",
            )
            bars = import_csv(csv_path)
            self.assertEqual(bars[0].price_basis, "traded")
            json_path = root / "events.jsonl"
            json_path.write_text(
                json.dumps({"ts": 1704067200, "bid": 10, "ask": 12, "mid": 11, "id": "e1"}) + "\n",
                encoding="utf-8",
            )
            mapping = ColumnMapping.from_dict(
                {
                    "record_kind": "event",
                    "timestamp": "ts",
                    "timestamp_unit": "s",
                    "instrument": None,
                    "resolution": None,
                    "bid": "bid",
                    "ask": "ask",
                    "mid": "mid",
                    "event_id": "id",
                    "price": None,
                }
            )
            events = import_jsonl(json_path, mapping, config=ImportConfig(instrument="SYNTH/USD", price_basis="mid"))
            self.assertEqual(events[0].price, 11.0)
            self.assertEqual(events[0].price_basis, "mid")
            self.assertEqual(events[0].bid, 10.0)
            self.assertEqual(events[0].ask, 12.0)

    def test_naive_timestamp_requires_explicit_timezone(self) -> None:
        with TemporaryDirectory() as temp:
            path = Path(temp) / "naive.csv"
            path.write_text(
                "timestamp,instrument,resolution,open,high,low,close\n"
                "2024-01-01T00:00:00,SYNTH/USD,M1,10,12,9,11\n",
                encoding="utf-8",
            )
            with self.assertRaises(ValueError):
                import_csv(path)


class KrakenTests(unittest.TestCase):
    def test_rest_separates_provider_open_row(self) -> None:
        payload = {
            "error": [],
            "result": {
                "BTC/USD": [
                    [1704067200, "10", "12", "9", "11", "10.5", "3", 4],
                    [1704067260, "11", "13", "10", "12", "11.5", "4", 5],
                ],
                "last": 1704067320,
            },
        }

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self):
                return json.dumps(payload).encode()

        with patch("mtf_lab.data.kraken.urlopen", return_value=Response()):
            result = KrakenPublicAdapter(rest_min_interval_seconds=0).fetch_ohlc(1)
        self.assertEqual(len(result), 1)
        self.assertIsNotNone(result.open_bar)
        self.assertFalse(result.open_bar.closed)
        self.assertEqual(result[0].resolution, "M1")

    def test_rest_body_error_is_not_ignored(self) -> None:
        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self):
                return json.dumps({"error": ["EGeneral:Invalid arguments"], "result": {}}).encode()

        with patch("mtf_lab.data.kraken.urlopen", return_value=Response()):
            with self.assertRaises(KrakenAPIError):
                KrakenPublicAdapter(rest_min_interval_seconds=0).fetch_ohlc(1)

    def test_trade_wire_payload_normalizes_multiple_required_fields(self) -> None:
        event = KrakenPublicAdapter(rest_min_interval_seconds=0)._normalize_trade(
            {
                "symbol": "BTC/USD",
                "side": "sell",
                "qty": 0.5,
                "price": 100.25,
                "trade_id": 7,
                "timestamp": "2024-01-01T00:00:00.123Z",
            },
            is_snapshot=True,
        )
        self.assertEqual(event.source_event_id, "7")
        self.assertTrue(event.is_snapshot)
        self.assertEqual(event.price_basis, "traded")


if __name__ == "__main__":
    unittest.main()
