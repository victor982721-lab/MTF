"""Offline contracts for detector/product composition."""

from __future__ import annotations

import os
import unittest
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from mtf_lab.core import (
    DataQuality,
    EventKind,
    IndicatorConfig,
    IndicatorPoint,
    IndicatorSeries,
    MarketEvent,
    PriceBase,
    Signal,
)
from mtf_lab.core.reference import m1_reference_signals
from mtf_lab.data.models import Event
from mtf_lab.ops.simulation import PricePoint, select_price_point
from mtf_lab.runtime import IncrementalProcessor
from mtf_lab.runtime.consumers import (
    BinarySimulationConsumer,
    CFDSignalConsumer,
    RecordingSignalConsumer,
)
from mtf_lab.runtime.state import PriceObservation, SimulationConfig, event_dict, event_from_dict

BASE = datetime(2026, 1, 1, tzinfo=UTC)


@contextmanager
def isolated_environment():
    with TemporaryDirectory() as tmp:
        root = Path(tmp)
        with patch.dict(
            os.environ,
            {
                "HOME": str(root / "home"),
                "XDG_CONFIG_HOME": str(root / "config"),
                "XDG_CACHE_HOME": str(root / "cache"),
                "XDG_STATE_HOME": str(root / "state"),
            },
            clear=False,
        ):
            yield root


def signal() -> Signal:
    return Signal(
        signal_id="composition-signal",
        instrument="TEST/USD",
        direction="UP",
        detected_at=BASE,
        context_start=BASE,
        preparation_start=BASE,
        trigger_start=BASE,
        trigger_end=BASE + timedelta(minutes=1),
        episode_id="composition-episode",
    )


def event(second: int, price: float, event_id: str) -> Event:
    timestamp = BASE + timedelta(seconds=second)
    return Event(
        instrument="TEST/USD",
        event_time=timestamp,
        price=price,
        quantity=1.0,
        source="offline-fixture",
        source_event_id=event_id,
        received_at=timestamp,
        available_at=timestamp,
    )


class SignalConsumerCompositionTests(unittest.TestCase):
    def test_shared_selector_accepts_pricepoint_and_runtime_observation(self) -> None:
        with isolated_environment():
            target = BASE + timedelta(seconds=60)
            price_point = PricePoint(
                target + timedelta(seconds=1),
                101.0,
                available_at=target + timedelta(seconds=1),
                source="fixture",
                base_price="traded",
                quality="VALID",
                resolution="M1",
                instrument="TEST/USD",
                point_id="point-contract",
            )
            runtime_observation = PriceObservation(
                target + timedelta(seconds=1),
                target + timedelta(seconds=1),
                101.0,
                "traded",
                "fixture",
                "M1",
                instrument="TEST/USD",
                observation_id="point-contract",
            )
            selected_point, point_reason = select_price_point(
                [price_point],
                target,
                rule="first_observation_at_or_after",
                requested_base_price="traded",
                instrument="TEST/USD",
            )
            selected_observation, observation_reason = select_price_point(
                [runtime_observation],
                target,
                rule="first_observation_at_or_after",
                requested_base_price="traded",
                instrument="TEST/USD",
            )
            self.assertIsNone(point_reason)
            self.assertIsNone(observation_reason)
            self.assertIsNotNone(selected_point)
            self.assertIsNotNone(selected_observation)
            assert selected_point is not None and selected_observation is not None
            self.assertEqual(selected_point.use_time, selected_observation.use_time)
            self.assertEqual(selected_point.point.price, selected_observation.point.price)
            self.assertEqual(selected_point.point.identity, selected_observation.point.identity)

    def test_reference_projection_keeps_original_expected_identity(self) -> None:
        with isolated_environment():
            points = []
            for index, (close, ema, rsi) in enumerate(((99, 100, 45), (101, 100, 55))):
                start = BASE + timedelta(minutes=index)
                points.append(
                    IndicatorPoint(
                        start,
                        start + timedelta(minutes=1),
                        start + timedelta(minutes=1),
                        close,
                        ema,
                        ema,
                        rsi,
                        1.0,
                        True,
                        DataQuality.good(),
                        f"point-{index}",
                        index,
                    )
                )
            series = IndicatorSeries("M1", "TEST/USD", IndicatorConfig(), tuple(points))
            rows = m1_reference_signals(series, identity_salt="fixture")
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["signal_id"], "m1ref_93d1e0e394199830ba13cc9c68733810")
            self.assertEqual(rows[0]["direction"], "UP")
            self.assertEqual(rows[0]["status"], "REFERENCE_M1")

    def test_default_consumer_preserves_binary_virtual_results(self) -> None:
        with isolated_environment():
            processor = IncrementalProcessor(
                instrument="TEST/USD",
                simulation=SimulationConfig(horizons_seconds=(60,)),
            )
            pending = processor.register_signal(signal())
            self.assertIsInstance(processor.signal_consumer, BinarySimulationConsumer)
            self.assertEqual(len(pending), 1)
            self.assertEqual(len(processor.pending_simulations), 1)
            self.assertEqual(processor.status["consumer_type"], "binary_simulation")

    def test_recording_consumer_has_no_binary_product_state(self) -> None:
        with isolated_environment():
            consumer = RecordingSignalConsumer(max_events=8, max_signals=4)
            processor = IncrementalProcessor(
                instrument="TEST/USD",
                simulation=SimulationConfig(horizons_seconds=(60,)),
                signal_consumer=consumer,
            )
            self.assertEqual(processor.register_signal(signal()), ())
            result = processor.process_event(event(0, 100.0, "event-0"))
            self.assertEqual(result.completed_simulations, ())
            self.assertEqual(processor.pending_simulations, ())
            self.assertIsNone(getattr(processor, "_simulation_book", None))
            self.assertEqual(consumer.signal_count, 1)
            self.assertEqual(consumer.observation_count, 1)
            self.assertEqual(processor.status["consumer_type"], "recording")

    def test_cfd_consumer_adapts_signal_without_constructing_binary_book(self) -> None:
        with isolated_environment():
            received: list[Signal] = []
            consumer = CFDSignalConsumer(max_events=8, signal_sink=received.append)
            processor = IncrementalProcessor(
                instrument="TEST/USD",
                simulation=SimulationConfig(horizons_seconds=(60,)),
                signal_consumer=consumer,
            )
            processor.register_signal(signal())
            processor.process_event(event(0, 100.0, "event-0"))
            self.assertEqual(processor.pending_simulations, ())
            self.assertEqual(processor.completed_simulations, [])
            self.assertIsNone(getattr(processor, "_simulation_book", None))
            self.assertEqual([item.signal_id for item in received], [signal().signal_id])
            self.assertFalse(hasattr(consumer, "signals"))
            self.assertEqual(processor.consumer_checkpoint["consumer_type"], "cfd_signal")

    def test_same_detector_input_is_independent_of_consumer(self) -> None:
        with isolated_environment():
            consumers = (None, RecordingSignalConsumer(), CFDSignalConsumer())
            processors = [
                IncrementalProcessor(instrument="TEST/USD", signal_consumer=consumer) for consumer in consumers
            ]
            records = [event(index * 60, 100.0 + index * 0.1, f"event-{index}") for index in range(20)]
            results = []
            for processor in processors:
                results.append(processor.replay(records, sort=False))
            status_keys = (
                "mode",
                "instrument",
                "price_base",
                "timeframes",
                "events_processed",
                "candles_processed",
                "candles",
                "signals",
                "evaluations",
                "warmup_pending",
            )
            baseline_status = {key: processors[0].status[key] for key in status_keys}
            for processor in processors[1:]:
                self.assertEqual({key: processor.status[key] for key in status_keys}, baseline_status)
            self.assertEqual(
                [item.signal_id for item in processors[0].signals], [item.signal_id for item in processors[1].signals]
            )
            self.assertEqual(
                [item.signal_id for item in processors[0].signals], [item.signal_id for item in processors[2].signals]
            )
            self.assertEqual([item.evaluations for item in results], [results[0].evaluations] * 3)
            self.assertEqual([item.signals for item in results], [results[0].signals] * 3)

    def test_event_checkpoint_roundtrip_preserves_quality_source_and_metadata(self) -> None:
        with isolated_environment():
            original = MarketEvent(
                instrument="TEST/USD",
                event_time=BASE,
                bid=100.0,
                ask=100.2,
                price_base=PriceBase.MID,
                event_kind=EventKind.TRADE,
                source="offline-feed",
                event_id="mid-event",
                quality=DataQuality.good(source="provider-quality"),
                metadata={"provider_marker": "kept"},
            )
            restored = event_from_dict(event_dict(original))
            self.assertEqual(restored.metadata, original.metadata)
            self.assertEqual(restored.quality, original.quality)
            self.assertEqual(restored.quality.source, "provider-quality")

    def test_processor_checkpoint_restore_is_exact_for_recording_consumer(self) -> None:
        with isolated_environment():
            processor = IncrementalProcessor(
                instrument="TEST/USD",
                signal_consumer=RecordingSignalConsumer(max_events=8, max_signals=4),
            )
            processor.process_event(event(0, 100.0, "event-0"))
            snapshot = processor.checkpoint()
            resumed = IncrementalProcessor.from_checkpoint(snapshot)
            self.assertEqual(resumed.checkpoint(), snapshot)
            self.assertEqual(resumed.consumer_checkpoint["consumer_type"], "recording")

    def test_cfd_checkpoint_does_not_invent_a_sink(self) -> None:
        with isolated_environment():
            received: list[Signal] = []
            original = CFDSignalConsumer(signal_sink=received.append)
            original.on_signal(signal())
            snapshot = original.checkpoint()
            restored = CFDSignalConsumer()
            restored.restore(snapshot)
            restored.on_signal(
                Signal(
                    signal_id="second",
                    instrument="TEST/USD",
                    direction="DOWN",
                    detected_at=BASE,
                    context_start=BASE,
                    preparation_start=BASE,
                    trigger_start=BASE,
                    trigger_end=BASE + timedelta(minutes=1),
                    episode_id="second-episode",
                )
            )
            self.assertEqual([item.signal_id for item in received], [signal().signal_id])
            self.assertEqual(restored.signal_count, 2)
            self.assertNotIn("cfd_signals", snapshot.state)

    def test_consumer_checkpoint_roundtrip_restores_type_and_causal_events(self) -> None:
        with isolated_environment():
            original = IncrementalProcessor(
                instrument="TEST/USD",
                signal_consumer=RecordingSignalConsumer(max_events=4, max_signals=2),
            )
            original.register_signal(signal())
            original.process_event(event(0, 100.0, "event-0"))
            snapshot = original.checkpoint()
            resumed = IncrementalProcessor.from_checkpoint(snapshot)
            self.assertIsInstance(resumed.signal_consumer, RecordingSignalConsumer)
            self.assertEqual([item.signal_id for item in resumed.signal_consumer.signals], [signal().signal_id])
            self.assertEqual(resumed.consumer_checkpoint["consumer_type"], "recording")
            self.assertEqual(len(resumed.consumer_events), len(original.consumer_events))
            duplicate = resumed.process_event(event(0, 100.0, "event-0"))
            self.assertFalse(duplicate.accepted)
            self.assertEqual(duplicate.signals, ())


if __name__ == "__main__":
    unittest.main()
