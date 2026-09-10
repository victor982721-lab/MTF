"""Pruebas del runtime incremental y de reanudación causal."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import unittest

from mtf_lab.core import Candle, DataQuality, IndicatorPoint, Signal
from mtf_lab.data.models import Bar as ProviderBar, Event as ProviderEvent
from mtf_lab.runtime import IncrementalProcessor, SimulationConfig
from mtf_lab.runtime.state import quality_label_is_usable


UTC = timezone.utc
BASE = datetime(2026, 1, 1, tzinfo=UTC)


def event(minute: int, *, seconds: int = 0, price: float | None = None, event_id: str | None = None) -> ProviderEvent:
    ts = BASE + timedelta(minutes=minute, seconds=seconds)
    return ProviderEvent(
        instrument="TEST/USD",
        event_time=ts,
        price=100.0 + minute if price is None else price,
        quantity=1.0,
        source="fixture",
        source_event_id=event_id or f"e-{minute}-{seconds}",
        source_sequence=f"{minute}-{seconds}",
        received_at=ts + timedelta(milliseconds=10),
        available_at=ts + timedelta(milliseconds=10),
    )


def native_bar(start_minute: int, *, close: float = 100.0) -> ProviderBar:
    start = BASE + timedelta(minutes=start_minute)
    return ProviderBar(
        instrument="TEST/USD",
        interval_start=start,
        interval_end=start + timedelta(minutes=1),
        open=close,
        high=close + 1,
        low=close - 1,
        close=close,
        resolution_seconds=60,
        volume=1.0,
        trade_count=1,
        price_basis="traded",
        source="native-fixture",
        source_record_id=f"native-{start_minute}",
        available_at=start + timedelta(minutes=1),
        received_at=start + timedelta(minutes=1),
        closed=True,
    )


class RuntimeIncrementalTests(unittest.TestCase):
    def test_events_close_temporalities_in_deterministic_order_without_empty_bars(self) -> None:
        processor = IncrementalProcessor(instrument="TEST/USD", timeframes=("M1", "M5", "M15"))
        emitted = []
        for minute in range(17):
            emitted.extend(processor.process_event(event(minute)).candles)
        names_at_5 = [bar.timeframe.name for bar in emitted if bar.end == BASE + timedelta(minutes=5)]
        names_at_15 = [bar.timeframe.name for bar in emitted if bar.end == BASE + timedelta(minutes=15)]
        self.assertEqual(names_at_5, ["M5", "M1"])
        self.assertEqual(names_at_15, ["M15", "M5", "M1"])
        # [17,18) remains open until an explicit watermark; no synthetic empty
        # candles are inserted for a skipped interval.
        result = processor.finalize(BASE + timedelta(minutes=31))
        self.assertEqual([bar.timeframe.name for bar in result.candles], ["M15", "M5", "M1"])
        self.assertEqual(len(processor.candles["M1"]), 17)

    def test_duplicate_event_and_native_aggregate_interval_are_idempotent(self) -> None:
        processor = IncrementalProcessor(instrument="TEST/USD")
        first = processor.process_event(event(0))
        duplicate = processor.process_event(event(0))
        self.assertTrue(first.accepted)
        self.assertFalse(duplicate.accepted)
        self.assertTrue(any(issue.code == "duplicate_event" for issue in duplicate.issues))
        # Native M1 takes the logical interval before the derived event candle;
        # only one effective M1 survives.
        native = processor.process_bar(native_bar(0, close=100.25))
        self.assertTrue(native.accepted)
        emitted = processor.process_event(event(1, price=101.0))
        self.assertEqual(len([bar for bar in processor.candles["M1"] if bar.start == BASE]), 1)
        self.assertEqual(processor.candles["M1"][0].source, "native-fixture")
        self.assertTrue(any(issue.code == "duplicate_candle" for issue in emitted.issues))

    def test_checkpoint_json_roundtrip_does_not_duplicate_events_or_candles(self) -> None:
        original = IncrementalProcessor(instrument="TEST/USD")
        for minute in range(8):
            original.process_event(event(minute))
        snapshot = original.checkpoint()
        encoded = original.checkpoint_json()
        self.assertEqual(snapshot, json.loads(encoded))
        resumed = IncrementalProcessor.from_checkpoint(encoded)
        # Replay of an already consumed event is explicitly idempotent.
        duplicate = resumed.process_event(event(7))
        self.assertFalse(duplicate.accepted)
        for minute in range(8, 18):
            original.process_event(event(minute))
            resumed.process_event(event(minute))
        original.finalize(BASE + timedelta(minutes=20))
        resumed.finalize(BASE + timedelta(minutes=20))
        self.assertEqual(original.status["candles"], resumed.status["candles"])
        self.assertEqual([bar.candle_id for bar in original.candles["M5"]], [bar.candle_id for bar in resumed.candles["M5"]])
        self.assertEqual([signal.signal_id for signal in original.signals], [signal.signal_id for signal in resumed.signals])
        self.assertEqual(len(resumed._seen_event_ids), resumed.events_processed)

    def test_strategy_window_is_bounded_and_indicators_are_not_recomputed(self) -> None:
        processor = IncrementalProcessor(instrument="TEST/USD", max_candles=32)
        for minute in range(120):
            processor.process_event(event(minute))
        status = processor.status
        # A trigger call receives only the causal tail (previous trigger,
        # current context/preparation lookbacks and active episode), not the
        # 120-candle historical arrays.
        self.assertLessEqual(status["last_strategy_window_sizes"].get("M1", 0), 2)
        self.assertLessEqual(status["last_strategy_window_sizes"].get("M5", 0), 9)
        self.assertLessEqual(status["last_strategy_window_sizes"].get("M15", 0), 4)
        # Update counters are monotonic audit counters; stored candle arrays
        # are bounded by max_candles.
        self.assertGreaterEqual(status["indicator_updates"]["M1"], status["candles"]["M1"])
        self.assertGreaterEqual(status["indicator_updates"]["M5"], status["candles"]["M5"])
        self.assertGreaterEqual(status["indicator_updates"]["M15"], status["candles"]["M15"])
        self.assertLess(status["strategy_evaluations"], status["indicator_updates"]["M1"])
        self.assertGreaterEqual(status["strategy_evaluations"], 0)

    def test_native_bars_build_missing_higher_timeframes_from_ohlc_only(self) -> None:
        processor = IncrementalProcessor(instrument="TEST/USD")
        emitted = []
        for minute in range(15):
            emitted.extend(processor.process_bar(native_bar(minute, close=100.0 + minute)).candles)
        self.assertEqual(len(processor.candles["M1"]), 15)
        self.assertEqual(len(processor.candles["M5"]), 3)
        self.assertEqual(len(processor.candles["M15"]), 1)
        higher = processor.candles["M15"][0]
        self.assertEqual(higher.origin, "resampled_ohlc")
        self.assertTrue(higher.metadata["no_ticks_invented"])
        self.assertEqual(higher.open, 100.0)
        self.assertEqual(higher.close, 114.0)

    def test_native_preference_and_price_base_are_explicit(self) -> None:
        derived_first = IncrementalProcessor(instrument="TEST/USD")
        derived_first.process_event(event(0, price=100.0))
        derived_first.process_event(event(1, price=101.0))
        replacement = derived_first.process_bar(native_bar(0, close=100.25))
        self.assertTrue(replacement.accepted)
        self.assertEqual(derived_first.candles["M1"][0].source, "native-fixture")
        self.assertTrue(any(issue.code == "native_preferred" for issue in replacement.issues))

        bid_processor = IncrementalProcessor(instrument="TEST/USD", price_base="bid")
        bid_processor.process_event(
            ProviderEvent("TEST/USD", BASE, price=99.0, bid=99.0, ask=100.0, price_basis="bid", source="quotes", source_event_id="bid-0")
        )
        result = bid_processor.process_event(
            ProviderEvent("TEST/USD", BASE + timedelta(minutes=1), price=98.0, bid=98.0, ask=99.0, price_basis="bid", source="quotes", source_event_id="bid-1")
        )
        self.assertTrue(result.accepted)
        self.assertEqual(bid_processor.candles["M1"][0].price_base.value, "bid")
        self.assertEqual(bid_processor.candles["M1"][0].close, 99.0)

    def test_public_quality_labels_block_only_unknown_or_bad_states(self) -> None:
        for label in ("VALID", "SYNTHETIC_VALIDATED", "DATA_QUALITY_VALIDATED", "OK"):
            self.assertTrue(quality_label_is_usable(label), label)
        for label in ("UNKNOWN", "DISCONNECTED", "STALE", "GAP", "INVALID", "SYNTHETIC_ANOMALY"):
            self.assertFalse(quality_label_is_usable(label), label)

    def test_max_candles_bounds_query_history_but_keeps_monotonic_indicators(self) -> None:
        processor = IncrementalProcessor(instrument="TEST/USD", max_candles=4)
        for minute in range(20):
            processor.process_event(event(minute))
        self.assertTrue(all(len(values) <= 4 for values in processor.candles.values()))
        self.assertTrue(all(len(values) <= 4 for values in processor.indicator_points.values()))
        self.assertGreater(processor.status["indicator_updates"]["M1"], len(processor.candles["M1"]))

    def test_replay_mixed_native_and_event_records_is_deterministic(self) -> None:
        records = [event(1), native_bar(0, close=100.5), event(0, price=100.0), event(2, price=102.0)]
        one = IncrementalProcessor(instrument="TEST/USD")
        two = IncrementalProcessor(instrument="TEST/USD")
        result_one = one.replay(records)
        result_two = two.replay(list(reversed(records)))
        self.assertEqual(one.status["candles"], two.status["candles"])
        self.assertEqual([bar.candle_id for bar in one.candles["M1"]], [bar.candle_id for bar in two.candles["M1"]])
        self.assertEqual(result_one.candles, result_two.candles)

    def test_pending_virtual_simulation_uses_later_observation_and_can_remain_indeterminate(self) -> None:
        processor = IncrementalProcessor(
            instrument="TEST/USD",
            simulation=SimulationConfig(horizons_seconds=(60,), entry_latency_seconds=1, max_price_age_seconds=2),
        )
        signal = Signal(
            signal_id="sig-fixture",
            instrument="TEST/USD",
            direction="UP",
            detected_at=BASE,
            context_start=BASE - timedelta(minutes=15),
            preparation_start=BASE - timedelta(minutes=5),
            trigger_start=BASE,
            trigger_end=BASE + timedelta(minutes=1),
            episode_id="episode-fixture",
        )
        pending = processor.register_signal(signal)
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0].status, "PENDING")
        # Entry at t=1 s and final reference at t=60 s, never t=0 signal
        # close; explicit horizon is from entry by default.
        processor.process_event(event(0, seconds=1, price=100.0))
        processor.process_event(event(0, seconds=60, price=101.0))
        self.assertEqual(len(processor.pending_simulations), 1)
        completed = processor.finalize(BASE + timedelta(seconds=63)).completed_simulations
        self.assertEqual(len(completed), 1)
        self.assertEqual(completed[0].outcome, "WIN")
        self.assertAlmostEqual(completed[0].net_result or 0.0, 0.8)


if __name__ == "__main__":
    unittest.main()
