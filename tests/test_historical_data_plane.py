"""Offline contracts for the shared historical aggregation/data plane."""

from __future__ import annotations

import json
import unittest
from datetime import UTC, datetime, timedelta
from typing import Any

from mtf_lab.core import EventKind, MarketEvent, OperationMode, PriceBase
from mtf_lab.data.synthetic import SyntheticGenerator
from mtf_lab.runtime.consumers import RecordingSignalConsumer
from mtf_lab.runtime.processor import IncrementalProcessor, SharedDataPlane

BASE = datetime(2024, 1, 1, tzinfo=UTC)


def quote(index: int, *, price: float | None = None) -> MarketEvent:
    event_time = BASE.replace(minute=0, second=0) + timedelta(minutes=index)
    value = 100.0 + index if price is None else price
    return MarketEvent(
        instrument="TEST/USD",
        event_time=event_time,
        received_at=event_time,
        available_at=event_time,
        bid=value,
        ask=value + 0.01,
        source="data-plane-fixture",
        mode=OperationMode.REPLAY,
        price_base=PriceBase.MID,
        event_kind=EventKind.QUOTE,
        sequence=index,
        source_event_id=f"quote:{index}",
    )


def quote_at(index: int, seconds: int, *, price: float | None = None) -> MarketEvent:
    event_time = BASE + timedelta(seconds=seconds)
    value = 100.0 + index if price is None else price
    return MarketEvent(
        instrument="TEST/USD",
        event_time=event_time,
        received_at=event_time,
        available_at=event_time,
        bid=value,
        ask=value + 0.01,
        source="data-plane-fixture",
        mode=OperationMode.REPLAY,
        price_base=PriceBase.MID,
        event_kind=EventKind.QUOTE,
        sequence=index,
        source_event_id=f"quote-at:{index}",
    )


def generated_quotes(count: int = 900) -> list[MarketEvent]:
    bars = SyntheticGenerator(seed=42, instrument="TEST/USD").generate(periods=count, scenario="sideways").bars
    return [
        MarketEvent(
            instrument="TEST/USD",
            event_time=bar.interval_start,
            received_at=bar.interval_end,
            available_at=bar.interval_end,
            bid=bar.close,
            ask=bar.close + 0.01,
            source="data-plane-synthetic-fixture",
            mode=OperationMode.REPLAY,
            price_base=PriceBase.MID,
            event_kind=EventKind.QUOTE,
            sequence=index,
            source_event_id=f"synthetic-quote:{index}",
        )
        for index, bar in enumerate(bars)
    ]


def make_plane(coverage_mode: str, *, max_candles: int | None = 128) -> SharedDataPlane:
    kwargs: dict[str, Any] = {"max_quote_gap_seconds": 90.0} if coverage_mode == "continuous_quotes" else {}
    return SharedDataPlane(
        timeframes=("M1", "M5", "M15"),
        instrument="TEST/USD",
        price_base=PriceBase.MID,
        coverage_mode=coverage_mode,
        max_candles=max_candles,
        **kwargs,
    )


def feed_plane(plane: SharedDataPlane, records: list[MarketEvent]) -> None:
    for record in records:
        result = plane.feed_event(record)
        if not result.accepted:
            raise AssertionError(f"fixture rejected: {result.issues}")


class SharedDataPlaneTests(unittest.TestCase):
    def test_strict_checkpoint_keeps_event_backed_bucket_and_roundtrips(self) -> None:
        plane = make_plane("strict")
        feed_plane(plane, [quote_at(0, 0), quote_at(1, 30)])

        snapshot = plane.checkpoint()
        bucket = snapshot["aggregators"]["M1"]["bucket"]
        self.assertIn("events", bucket)
        self.assertEqual(len(bucket["events"]), 2)
        restored = SharedDataPlane.from_checkpoint(json.loads(json.dumps(snapshot)))

        self.assertEqual(restored.checkpoint(), snapshot)
        bucket = restored.aggregators["M1"]._bucket
        assert bucket is not None
        self.assertEqual(len(bucket.events), 2)

    def test_continuous_checkpoint_uses_bounded_bucket_state_and_roundtrips(self) -> None:
        plane = make_plane("continuous_quotes")
        feed_plane(plane, [quote_at(0, 0), quote_at(1, 30)])

        snapshot = plane.checkpoint()
        bucket = snapshot["aggregators"]["M1"]["bucket"]
        self.assertNotIn("events", bucket)
        self.assertEqual(bucket["stats"]["event_count"], 2)
        restored = SharedDataPlane.from_checkpoint(json.loads(json.dumps(snapshot)))

        self.assertEqual(restored.checkpoint(), snapshot)
        self.assertEqual(restored.aggregators["M1"].export_bucket_state(), bucket)

    def test_full_and_chunk_restore_are_equivalent_for_strict_and_continuous(self) -> None:
        records = [quote(index) for index in range(180)]
        for coverage_mode in ("strict", "continuous_quotes"):
            full = make_plane(coverage_mode)
            feed_plane(full, records)

            chunk = make_plane(coverage_mode)
            feed_plane(chunk, records[:91])
            checkpoint = json.loads(json.dumps(chunk.checkpoint()))
            resumed = SharedDataPlane.from_checkpoint(checkpoint)
            feed_plane(resumed, records[91:])

            self.assertEqual(resumed.checkpoint(), full.checkpoint(), coverage_mode)

    def test_processors_keep_mutable_strategy_state_independent_and_reject_config_mismatch(self) -> None:
        plane = make_plane("strict")
        first = IncrementalProcessor(
            data_plane=plane,
            price_base=PriceBase.MID,
            signal_consumer=RecordingSignalConsumer(),
        )
        second = IncrementalProcessor(
            data_plane=plane,
            price_base=PriceBase.MID,
            signal_consumer=RecordingSignalConsumer(),
        )

        self.assertIs(first.indicator_engines["M1"], second.indicator_engines["M1"])
        self.assertIsNot(first.candles, second.candles)
        self.assertIsNot(first.candles["M1"], second.candles["M1"])
        self.assertIsNot(first._events, second._events)
        first.episodes["local-only"] = object()
        self.assertNotIn("local-only", second.episodes)

        with self.assertRaisesRegex(ValueError, "indicator_config"):
            IncrementalProcessor(
                data_plane=plane,
                price_base=PriceBase.MID,
                strategy={
                    "indicators": {
                        "ema_fast": 5,
                        "ema_slow": 6,
                        "rsi_period": 2,
                        "atr_period": 2,
                    }
                },
            )
        with self.assertRaises(ValueError):
            IncrementalProcessor(data_plane=plane, timeframes=("M1", "H1"), price_base=PriceBase.MID)

    def test_chunk_restore_preserves_signals_and_consumer_funnel(self) -> None:
        records = generated_quotes()
        full_plane = make_plane("continuous_quotes", max_candles=256)
        full_consumer = RecordingSignalConsumer(max_events=5000, max_signals=256)
        full_processor = IncrementalProcessor(
            data_plane=full_plane,
            price_base=PriceBase.MID,
            signal_consumer=full_consumer,
        )
        for record in records[:450]:
            full_processor.feed_precomputed(full_plane.feed_event(record))

        plane_checkpoint = json.loads(json.dumps(full_plane.checkpoint()))
        strategy_checkpoint = json.loads(json.dumps(full_processor.strategy_checkpoint()))
        resumed_plane = SharedDataPlane.from_checkpoint(plane_checkpoint)
        resumed_consumer = RecordingSignalConsumer(max_events=5000, max_signals=256)
        resumed_processor = IncrementalProcessor.from_strategy_checkpoint(
            strategy_checkpoint,
            data_plane=resumed_plane,
            signal_consumer=resumed_consumer,
        )

        for record in records[450:]:
            full_processor.feed_precomputed(full_plane.feed_event(record))
            resumed_processor.feed_precomputed(resumed_plane.feed_event(record))

        self.assertEqual(
            [item.signal_id for item in resumed_processor.signals],
            [item.signal_id for item in full_processor.signals],
        )
        self.assertEqual(
            [item.as_dict() for item in resumed_processor.evaluations],
            [item.as_dict() for item in full_processor.evaluations],
        )
        self.assertEqual(resumed_processor.consumer_checkpoint, full_processor.consumer_checkpoint)
        self.assertEqual(
            [item.to_dict() for item in resumed_processor.consumer_events],
            [item.to_dict() for item in full_processor.consumer_events],
        )
        self.assertEqual(resumed_plane.checkpoint(), full_plane.checkpoint())

    def test_shared_donchian_does_not_evaluate_partial_trigger_bars(self) -> None:
        for candidate_id, timeframe, boundary_seconds in (
            ("dc_m5_v1", "M5", 300),
            ("dc_m15_v1", "M15", 900),
            ("dc_h1_v1", "H1", 3600),
        ):
            first = quote_at(0, 34)
            boundary = quote_at(1, boundary_seconds)
            plane = SharedDataPlane(
                timeframes=(timeframe,),
                instrument="TEST/USD",
                price_base=PriceBase.MID,
                coverage_mode="continuous_quotes",
                max_quote_gap_seconds=90.0,
                max_candles=64,
            )
            shared = IncrementalProcessor(
                data_plane=plane,
                timeframes=(timeframe,),
                instrument="TEST/USD",
                price_base=PriceBase.MID,
                market_candidate_id=candidate_id,
                signal_consumer=RecordingSignalConsumer(),
            )
            legacy = IncrementalProcessor(
                timeframes=(timeframe,),
                instrument="TEST/USD",
                price_base=PriceBase.MID,
                market_candidate_id=candidate_id,
                quote_coverage_mode="continuous_quotes",
                max_quote_gap_seconds=90.0,
                signal_consumer=RecordingSignalConsumer(),
            )
            shared.feed_precomputed(plane.feed_event(first))
            legacy.process_event(first)
            shared_result = shared.feed_precomputed(plane.feed_event(boundary))
            legacy_result = legacy.process_event(boundary)

            self.assertIn("quality_blocked", {issue.code for issue in shared_result.issues}, candidate_id)
            self.assertEqual(shared_result.evaluations, legacy_result.evaluations, candidate_id)
            self.assertEqual(shared_result.signals, legacy_result.signals, candidate_id)
            self.assertFalse(
                any("bar_not_available" in evaluation.reasons for evaluation in shared_result.evaluations),
                candidate_id,
            )


if __name__ == "__main__":
    unittest.main()
