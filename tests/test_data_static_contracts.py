"""Static and offline contract checks for the data facade and pipeline.

The checks use only in-memory records or temporary paths.  They intentionally
do not import test fixtures into production modules or contact provider APIs.
"""

from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

import mtf_lab.data as data
from mtf_lab.core import OperationMode, PriceBase
from mtf_lab.data.kraken import KrakenFetchResult
from mtf_lab.data.models import Bar, DataQuality, DataSet, Event, Provenance
from mtf_lab.pipeline import (
    build_streams,
    points_from_candles,
    provider_bar_to_candle,
    provider_event_to_core,
    resample_candles,
)

T0 = datetime(2024, 1, 2, 12, 0, tzinfo=UTC)


def _bar(start: datetime, index: int = 0) -> Bar:
    end = start + timedelta(minutes=1)
    price = 100.0 + index
    return Bar(
        instrument="TEST/USD",
        interval_start=start,
        interval_end=end,
        open=price,
        high=price + 1,
        low=price - 1,
        close=price + 0.5,
        resolution_seconds=60,
        volume=1.0,
        trade_count=1,
        source="fixture",
        source_record_id=f"bar-{index}",
        received_at=end,
        available_at=end,
    )


class DataFacadeStaticContractTests(unittest.TestCase):
    def test_facade_exports_are_resolvable_without_test_dependencies(self) -> None:
        self.assertTrue(data.__all__)
        for name in data.__all__:
            self.assertTrue(hasattr(data, name), name)
        for path in Path("mtf_lab/data").glob("*.py"):
            source = path.read_text(encoding="utf-8")
            self.assertNotIn("tests.test_ctrader_pipeline", source)
            self.assertNotIn("from tests", source)

    def test_sequence_contracts_support_index_and_slice(self) -> None:
        bar = _bar(T0)
        event = Event("TEST/USD", T0, 100.5, source="fixture", source_event_id="event-0")
        provenance = Provenance("fixture", "REPLAY", "TEST/USD", resolutions=("M1",))
        quality = DataQuality(True, 2, coverage_start=T0, coverage_end=T0 + timedelta(minutes=1))
        dataset = DataSet((bar, event), provenance, quality)
        fetch = KrakenFetchResult((bar,), None, provenance, quality, T0, "fixture://bars", "hash")

        self.assertIs(dataset[0], bar)
        self.assertEqual(dataset[1:], (event,))
        self.assertIs(fetch[0], bar)
        self.assertEqual(fetch[:], (bar,))

    def test_pipeline_resamples_complete_bars_and_reports_missing_bucket(self) -> None:
        bars = [_bar(T0 + timedelta(minutes=index), index) for index in range(5)]
        candles, gaps = resample_candles([provider_bar_to_candle(bar) for bar in bars], "M5")
        self.assertEqual(len(candles), 1)
        self.assertEqual(gaps, [])
        self.assertEqual(candles[0].open, 100.0)
        self.assertEqual(candles[0].close, 104.5)
        self.assertEqual(points_from_candles(candles)[0]["available_at"], candles[0].available_at)

        incomplete, incomplete_gaps = resample_candles([provider_bar_to_candle(bar) for bar in bars[:-1]], "M5")
        self.assertEqual(incomplete, [])
        self.assertEqual(incomplete_gaps, [T0.isoformat().replace("+00:00", "Z")])

    def test_public_provider_adapters_preserve_explicit_causal_fields(self) -> None:
        bar = _bar(T0)
        candle = provider_bar_to_candle(bar, mode=OperationMode.REPLAY)
        event = Event(
            "TEST/USD",
            T0 + timedelta(seconds=5),
            100.5,
            source="fixture",
            source_event_id="event-1",
            received_at=T0 + timedelta(seconds=6),
            available_at=T0 + timedelta(seconds=7),
        )
        market_event = provider_event_to_core(event, mode=OperationMode.REPLAY)

        self.assertEqual(candle.start, T0)
        self.assertEqual(candle.end, T0 + timedelta(minutes=1))
        self.assertEqual(candle.available_at, bar.available_at)
        self.assertEqual(market_event.event_time, event.event_time)
        self.assertEqual(market_event.received_at, event.received_at)
        self.assertEqual(market_event.available_at, event.available_at)
        self.assertEqual(market_event.price_base, PriceBase.TRADED)

    def test_build_streams_is_offline_and_keeps_base_stream(self) -> None:
        bars = [_bar(T0 + timedelta(minutes=index), index) for index in range(5)]
        streams, gaps = build_streams(bars, mode=OperationMode.REPLAY, targets=("M1", "M5"))

        self.assertEqual(len(streams["M1"]), 5)
        self.assertEqual(len(streams["M5"]), 1)
        self.assertEqual(gaps["M5"], [])


if __name__ == "__main__":
    unittest.main()
