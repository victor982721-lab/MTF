"""Regression coverage for the data-layer refactor.

These tests deliberately stay offline: Kraken responses and sockets are local
fakes, while importer inputs are temporary files.  They exercise the public
contracts after the parsing/validation helpers were split out of the original
high-complexity functions.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from mtf_lab.core.models import EventKind, MarketEvent, OperationMode, PriceBase
from mtf_lab.core.quality import DataQuality as CoreQuality
from mtf_lab.data.importer import ColumnMapping, ImportConfig, ImportConfigurationError, import_csv, import_jsonl
from mtf_lab.data.kraken import KrakenAPIError, KrakenConfigurationError, KrakenPublicAdapter, normalize_pair
from mtf_lab.data.models import Bar, DataQuality, DataSet, Event, Provenance, ValidationIssue
from mtf_lab.data.translation import TranslationError, core_event_to_data, data_event_to_core, reconcile_native_candles

T0 = datetime(2024, 1, 2, 12, 0, tzinfo=UTC)


class ImporterRefactorTests(unittest.TestCase):
    def test_csv_candle_defaults_and_gap_warning(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "bars.csv"
            path.write_text(
                "timestamp,instrument,resolution,open,high,low,close,volume,trade_count\n"
                "2024-01-02T12:00:00Z,TEST/USD,M1,100,102,99,101,2,3\n"
                "2024-01-02T12:02:00Z,TEST/USD,M1,101,103,100,102,4,5\n",
                encoding="utf-8",
            )
            dataset = import_csv(path)

        self.assertEqual(len(dataset.bars), 2)
        self.assertEqual(dataset.bars[0].resolution, "M1")
        self.assertEqual(dataset.bars[0].metadata["source_row"], 1)
        self.assertEqual(dataset.quality.gap_count, 1)
        self.assertTrue(any(issue.code == "GAP" for issue in dataset.issues))

    def test_jsonl_event_reorder_and_duplicate_policy(self) -> None:
        mapping = ColumnMapping(record_kind="event")
        config = ImportConfig(strict=False, reorder=True, drop_duplicates=True, source="fixture")
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "events.jsonl"
            path.write_text(
                json.dumps(
                    {
                        "timestamp": (T0 + timedelta(seconds=2)).isoformat(),
                        "instrument": "TEST/USD",
                        "price": 102,
                        "event_id": "e2",
                    }
                )
                + "\n"
                + json.dumps(
                    {
                        "timestamp": (T0 + timedelta(seconds=1)).isoformat(),
                        "instrument": "TEST/USD",
                        "price": 101,
                        "event_id": "e1",
                    }
                )
                + "\n"
                + json.dumps(
                    {
                        "timestamp": (T0 + timedelta(seconds=1)).isoformat(),
                        "instrument": "TEST/USD",
                        "price": 101,
                        "event_id": "e1",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            dataset = import_jsonl(path, mapping=mapping, config=config)

        self.assertEqual([event.source_event_id for event in dataset.events], ["e1", "e2"])
        self.assertEqual({issue.code for issue in dataset.issues}, {"OUT_OF_ORDER", "DUPLICATE"})
        self.assertTrue(all(issue.severity == "WARNING" for issue in dataset.issues))

    def test_jsonl_lenient_keeps_structural_issue_and_valid_record(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "events.jsonl"
            path.write_text(
                "not-json\n" + json.dumps({"timestamp": T0.isoformat(), "instrument": "TEST/USD", "price": 100}) + "\n",
                encoding="utf-8",
            )
            dataset = import_jsonl(path, mapping={"record_kind": "event"}, config=ImportConfig(strict=False))

        self.assertEqual(len(dataset.events), 1)
        self.assertEqual(dataset.issues[0].code, "PARSE_ERROR")
        self.assertFalse(dataset.quality.valid)

    def test_invalid_mapping_and_missing_header_fail_explicitly(self) -> None:
        with self.assertRaises(ImportConfigurationError):
            ColumnMapping.from_dict({"unknown": "value"})
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "missing.csv"
            path.write_text("timestamp,price\n2024-01-02T12:00:00Z,100\n", encoding="utf-8")
            with self.assertRaises(ValueError) as error:
                import_csv(path, mapping={"record_kind": "event"})
        self.assertIn("invalid local data", str(error.exception))


class KrakenRefactorTests(unittest.TestCase):
    class _StubAdapter(KrakenPublicAdapter):
        def __init__(self, payload: object) -> None:
            super().__init__(rest_min_interval_seconds=0)
            self.payload = payload
            self.requested_uri = ""

        def _get(self, uri: str) -> bytes:
            self.requested_uri = uri
            return json.dumps(self.payload).encode("utf-8")

    def test_rest_parser_separates_open_bar_and_keeps_cursor(self) -> None:
        epoch = int(T0.timestamp())
        payload = {
            "error": [],
            "result": {
                "BTC/USD": [
                    [epoch, "100", "102", "99", "101", "100.5", "2", "3"],
                    [epoch + 60, "101", "103", "100", "102", "101.5", "4", "5"],
                ],
                "last": epoch + 120,
            },
        }
        adapter = self._StubAdapter(payload)
        result = adapter.fetch_ohlc(since=T0, now=T0 + timedelta(minutes=3))

        self.assertEqual(len(result), 1)
        self.assertEqual(result.bars[0].closed, True)
        self.assertEqual(result.open_bar.closed, False)
        self.assertEqual(result.last_timestamp, T0 + timedelta(minutes=2))
        self.assertEqual(parse_qs(urlsplit(adapter.requested_uri).query)["since"], [str(epoch)])
        self.assertEqual(result.provenance.notes[0], "Kraken REST público sin autenticación")

    def test_trade_message_deduplicates_provider_identity(self) -> None:
        adapter = self._StubAdapter({"error": [], "result": {"BTC/USD": []}})
        trade = {
            "symbol": "BTC/USD",
            "timestamp": T0.isoformat().replace("+00:00", "Z"),
            "trade_id": 7,
            "price": "100",
            "qty": "0.5",
            "side": "buy",
        }
        events = list(adapter._message_trade_events({"type": "snapshot", "data": [trade, dict(trade)]}))

        self.assertEqual(len(events), 1)
        self.assertTrue(adapter.status.snapshot_seen)
        self.assertEqual(adapter.status.duplicate_events, 1)
        self.assertEqual(events[0].side, "buy")

    def test_rest_errors_and_pair_aliases_are_explicit(self) -> None:
        self.assertEqual(normalize_pair("XXBTZUSD"), "BTC/USD")
        self.assertEqual(normalize_pair("xbt-usd"), "BTC/USD")
        with self.assertRaises(KrakenConfigurationError):
            normalize_pair("BTCUSD")
        adapter = self._StubAdapter({"error": ["EAPI:Invalid key"], "result": {}})
        with self.assertRaises(KrakenAPIError):
            adapter.fetch_ohlc()


class ModelsRefactorTests(unittest.TestCase):
    def test_models_normalize_values_and_detach_metadata(self) -> None:
        metadata = {"nested": {"value": 1}}
        event = Event(" TEST/USD ", T0, 100, quantity=2, source_event_id=" e1 ", side=" BUY ", metadata=metadata)
        bar = Bar(" TEST/USD ", T0, T0 + timedelta(minutes=1), 100, 102, 99, 101, 60, volume=3, trade_count=4)
        metadata["nested"]["value"] = 9

        self.assertEqual(event.instrument, "TEST/USD")
        self.assertEqual(event.source_event_id, "e1")
        self.assertEqual(event.metadata["nested"]["value"], 1)
        self.assertEqual(bar.volume, 3.0)
        self.assertEqual(bar.trade_count, 4)

    def test_model_validation_and_serialized_dataset_contract(self) -> None:
        with self.assertRaises(ValueError):
            Event("TEST/USD", T0, 0)
        with self.assertRaises(ValueError):
            Bar("TEST/USD", T0, T0 + timedelta(minutes=1), 100, 99, 100, 101, 60)
        provenance = Provenance(" fixture ", "REPLAY", " TEST/USD ", resolutions=[" M1 "], notes=["ok"])
        quality = DataQuality(True, 1, coverage_start=T0, coverage_end=T0)
        dataset = DataSet(
            (Event("TEST/USD", T0, 100),), provenance, quality, (ValidationIssue("NOTE", "ok", "WARNING"),)
        )

        self.assertEqual(provenance.provider, "fixture")
        self.assertEqual(provenance.resolutions, ("M1",))
        self.assertEqual(dataset.to_dict()["issues"][0]["severity"], "WARNING")


class TranslationRefactorTests(unittest.TestCase):
    def test_mid_roundtrip_preserves_explicit_quote_components(self) -> None:
        event = Event("TEST/USD", T0, 100.5, bid=100, ask=101, mid=100.5, price_basis="mid", source="fixture")
        core = data_event_to_core(event)
        restored = core_event_to_data(core)

        self.assertEqual(core.event_kind, EventKind.TRADE)
        self.assertEqual(core.price_base, PriceBase.MID)
        self.assertEqual(restored.mid, 100.5)
        self.assertEqual(restored.price_basis, "mid")

    def test_core_unknown_quality_and_mixed_instruments_remain_blocking(self) -> None:
        core = MarketEvent(
            instrument="TEST/USD",
            event_time=T0,
            price=100,
            source="fixture",
            mode=OperationMode.REPLAY,
            price_base=PriceBase.TRADED,
            quality=CoreQuality.good(),
        )
        foreign = MarketEvent(
            instrument="OTHER/USD",
            event_time=T0,
            price=100,
            source="fixture",
            mode=OperationMode.REPLAY,
            price_base=PriceBase.TRADED,
            quality=CoreQuality.good(),
        )
        result = reconcile_native_candles([], [core, foreign], "M1")

        self.assertTrue(result.blocked)
        self.assertTrue(any("instrumentos mezclados" in reason for reason in result.blocking_reasons))
        with self.assertRaises(TranslationError):
            reconcile_native_candles([], [], "M1", tolerance=float("nan"))


if __name__ == "__main__":
    unittest.main()
