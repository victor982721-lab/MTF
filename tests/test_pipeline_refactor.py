from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from mtf_lab.core import DataQuality, EventKind, MarketEvent, OperationMode, PriceBase
from mtf_lab.data import SyntheticGenerator
from mtf_lab.data.translation import TranslationError
from mtf_lab.ops.persistence import SQLiteStore
from mtf_lab.pipeline import (
    PipelineResult,
    persist_pipeline,
    provider_bar_to_candle,
    provider_event_to_core,
    run_incremental_dataset,
)


class ProviderAdapterRefactorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.start = datetime(2026, 1, 1, tzinfo=UTC)
        self.end = self.start + timedelta(minutes=1)

    def test_mapping_bar_preserves_explicit_basis_and_quality(self) -> None:
        row = {
            "start_ts": self.start.isoformat().replace("+00:00", "Z"),
            "end_ts": self.end.isoformat().replace("+00:00", "Z"),
            "timeframe": "M1",
            "instrument": "TEST/USD",
            "open": 100,
            "high": 102,
            "low": 99,
            "close": 101,
            "volume": 12,
            "event_count": 3,
            "price_type": "close",
            "source": "fixture",
            "mode": "SYNTHETIC",
            "synthetic": True,
            "closed": True,
            "available_at": self.end,
            "received_at": self.end,
            "quality": {"status": "valid"},
        }

        candle = provider_bar_to_candle(row, mode=OperationMode.SYNTHETIC)

        self.assertEqual(candle.instrument, "TEST/USD")
        self.assertEqual(candle.timeframe_name, "M1")
        self.assertEqual(candle.price_base, PriceBase.TRADED)
        self.assertEqual(candle.mode, OperationMode.SYNTHETIC)
        self.assertTrue(candle.quality.has("synthetic"))
        self.assertEqual(candle.event_count, 3)

    def test_attribute_bar_and_existing_candle_are_supported(self) -> None:
        source = SimpleNamespace(
            start=self.start,
            end=self.end,
            resolution="M1",
            instrument="TEST/USD",
            open=100,
            high=102,
            low=99,
            close=101,
            price_basis="traded",
            volume=1,
            trade_count=1,
            source="fixture",
            closed=True,
            available_at=self.end,
            received_at=self.end,
            quality=DataQuality.good(),
        )

        candle = provider_bar_to_candle(source)

        self.assertEqual(candle.source, "fixture")
        self.assertIs(provider_bar_to_candle(candle), candle)

    def test_bar_without_basis_fails_closed(self) -> None:
        row = {
            "start": self.start,
            "end": self.end,
            "resolution": "M1",
            "instrument": "TEST/USD",
            "open": 100,
            "high": 102,
            "low": 99,
            "close": 101,
        }

        with self.assertRaisesRegex(TranslationError, "base de precio"):
            provider_bar_to_candle(row)

    def test_event_adapter_keeps_trade_and_quote_bases_explicit(self) -> None:
        trade = SimpleNamespace(
            instrument="TEST/USD",
            event_time=self.start.isoformat().replace("+00:00", "Z"),
            price=101,
            quantity=2,
            price_basis="close",
            source="fixture",
            event_kind="trade",
            received_at=self.start,
            available_at=self.start,
            source_sequence=7,
        )
        quote = SimpleNamespace(
            instrument="TEST/USD",
            event_time=self.start,
            bid=100,
            ask=102,
            mid=101,
            price_basis="mid",
            source="fixture",
            event_kind="quote",
            received_at=self.start,
            available_at=self.start,
        )

        trade_core = provider_event_to_core(trade)
        quote_core = provider_event_to_core(quote)

        self.assertEqual(trade_core.price_base, PriceBase.TRADED)
        self.assertEqual(trade_core.price, 101)
        self.assertEqual(trade_core.sequence, 7)
        self.assertEqual(quote_core.price_base, PriceBase.MID)
        self.assertIsNone(quote_core.price)
        self.assertEqual(quote_core.selected_price, 101)
        self.assertEqual(quote_core.event_kind, EventKind.QUOTE)

    def test_event_without_basis_or_with_unknown_kind_is_rejected(self) -> None:
        missing_basis = SimpleNamespace(instrument="TEST/USD", event_time=self.start, price=101)
        unknown_kind = SimpleNamespace(
            instrument="TEST/USD",
            event_time=self.start,
            price=101,
            price_basis="traded",
            event_kind="not-a-kind",
        )

        with self.assertRaisesRegex(TranslationError, "base de precio"):
            provider_event_to_core(missing_basis)
        with self.assertRaisesRegex(TranslationError, "event_kind desconocido"):
            provider_event_to_core(unknown_kind)

    def test_existing_core_event_is_not_rebuilt(self) -> None:
        event = MarketEvent(
            "TEST/USD",
            self.start,
            price=101,
            mode=OperationMode.REPLAY,
            source_event_id="fixture-1",
        )

        self.assertIs(provider_event_to_core(event, mode=OperationMode.SYNTHETIC), event)


class PipelinePersistenceRefactorTests(unittest.TestCase):
    def test_persist_without_backtest_keeps_session_and_source_events(self) -> None:
        dataset = SyntheticGenerator(seed=7, instrument="SYNTH/USD").generate_scenarios(periods_each=3)
        streams, indicators, strategy, _processor = run_incremental_dataset(dataset, config={})
        result = PipelineResult(dataset, streams, indicators, strategy)

        with TemporaryDirectory() as tmp:
            db = Path(tmp) / "pipeline.sqlite3"
            persisted = persist_pipeline(result, db, seed=7, config={}, run_backtest=False)

            self.assertIsNotNone(persisted.session_id)
            self.assertEqual(persisted.simulations, [])
            self.assertIsNotNone(persisted.report)
            session_id = persisted.session_id
            assert session_id is not None
            with SQLiteStore(db, read_only=True) as store:
                status = store.status(session_id)
                self.assertEqual(status["status"], "COMPLETED")
                self.assertEqual(status["counts"]["events"], len(streams.get("M1", [])))
                self.assertEqual(status["counts"]["candles"], sum(map(len, streams.values())))


if __name__ == "__main__":
    unittest.main()
