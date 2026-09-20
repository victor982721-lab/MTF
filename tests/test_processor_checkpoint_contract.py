"""Contratos offline del codec de checkpoints del procesador incremental."""

from __future__ import annotations

import json
import os
import unittest
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from mtf_lab.core import EventKind, MarketEvent, OperationMode, PriceBase
from mtf_lab.runtime import IncrementalProcessor
from mtf_lab.runtime.consumers import RecordingSignalConsumer
from mtf_lab.runtime.processor import SharedDataPlane

BASE = datetime(2024, 1, 1, tzinfo=UTC)


@contextmanager
def isolated_environment() -> Iterator[Path]:
    """Keep this codec regression independent of user/runtime state."""

    with TemporaryDirectory(prefix="mtf-processor-checkpoint-") as directory:
        root = Path(directory)
        paths = {
            "HOME": root / "home",
            "XDG_STATE_HOME": root / "xdg-state",
            "XDG_CONFIG_HOME": root / "xdg-config",
            "XDG_DATA_HOME": root / "xdg-data",
            "XDG_CACHE_HOME": root / "xdg-cache",
            "TMPDIR": root / "tmp",
        }
        for path in paths.values():
            path.mkdir(mode=0o700)
        with mock.patch.dict(
            os.environ,
            {key: str(path) for key, path in paths.items()},
            clear=False,
        ):
            yield root


def event(index: int) -> MarketEvent:
    timestamp = BASE + timedelta(minutes=index)
    return MarketEvent(
        instrument="TEST/USD",
        event_time=timestamp,
        price=100.0 + index,
        source="checkpoint-fixture",
        mode=OperationMode.REPLAY,
        received_at=timestamp,
        available_at=timestamp,
        source_event_id=f"event:{index}",
    )


def quote(index: int) -> MarketEvent:
    timestamp = BASE + timedelta(minutes=index)
    value = 100.0 + index
    return MarketEvent(
        instrument="TEST/USD",
        event_time=timestamp,
        received_at=timestamp,
        available_at=timestamp,
        bid=value,
        ask=value + 0.01,
        source="checkpoint-shared-fixture",
        mode=OperationMode.REPLAY,
        price_base=PriceBase.MID,
        event_kind=EventKind.QUOTE,
        sequence=index,
        source_event_id=f"quote:{index}",
    )


class ProcessorCheckpointContractTests(unittest.TestCase):
    def test_standalone_v1_shape_order_and_json_roundtrip_are_unchanged(self) -> None:
        with isolated_environment():
            processor = IncrementalProcessor(instrument="TEST/USD")
            for index in range(8):
                self.assertTrue(processor.process_event(event(index)).accepted)

            snapshot = processor.checkpoint()
            self.assertEqual(
                list(snapshot),
                [
                    "checkpoint_version",
                    "config_hash",
                    "mode",
                    "instrument",
                    "source",
                    "price_base",
                    "consumer_type",
                    "consumer_product",
                    "timeframes",
                    "strategy",
                    "simulation",
                    "max_candles",
                    "seen_event_ids",
                    "events",
                    "aggregators",
                    "resample_buffers",
                    "candles",
                    "indicator_points",
                    "indicator_engines",
                    "evaluations",
                    "signals",
                    "decision_id_order",
                    "signal_id_order",
                    "episodes",
                    "context",
                    "pending_simulations",
                    "completed_simulations",
                    "completed_simulation_ids",
                    "simulation_observations",
                    "consumer",
                    "consumer_events",
                    "consumer_event_count",
                    "last_event_time",
                    "last_event_id",
                    "last_available_at",
                    "events_processed",
                    "candles_processed",
                    "strategy_evaluations",
                    "strategy_skipped",
                    "strategy_dirty",
                    "last_strategy_window_sizes",
                    "indicator_updates",
                    "issues",
                ],
            )
            encoded = processor.checkpoint_json()
            self.assertEqual(snapshot, json.loads(encoded))

            restored = IncrementalProcessor.from_checkpoint(json.loads(json.dumps(snapshot)))
            self.assertEqual(restored.checkpoint(), snapshot)
            self.assertEqual(restored.checkpoint_json(), encoded)

    def test_shared_plane_v1_snapshot_and_strategy_only_resume_are_exact(self) -> None:
        with isolated_environment():
            plane = SharedDataPlane(
                timeframes=("M1", "M5", "M15"),
                instrument="TEST/USD",
                price_base=PriceBase.MID,
                max_candles=64,
            )
            processor = IncrementalProcessor(
                data_plane=plane,
                price_base=PriceBase.MID,
                signal_consumer=RecordingSignalConsumer(max_events=64, max_signals=16),
            )
            for index in range(12):
                processor.feed_precomputed(plane.feed_event(quote(index)))

            snapshot = processor.checkpoint()
            self.assertEqual(snapshot["checkpoint_scope"], "strategy")
            self.assertIn("data_plane", snapshot)
            self.assertEqual(
                list(snapshot),
                [
                    "checkpoint_version",
                    "checkpoint_scope",
                    "config_hash",
                    "data_plane_config_hash",
                    "mode",
                    "instrument",
                    "source",
                    "price_base",
                    "consumer_type",
                    "consumer_product",
                    "timeframes",
                    "strategy",
                    "simulation",
                    "market_candidate_id",
                    "max_candles",
                    "evaluations",
                    "signals",
                    "decision_id_order",
                    "signal_id_order",
                    "episodes",
                    "context",
                    "pending_simulations",
                    "completed_simulations",
                    "completed_simulation_ids",
                    "simulation_observations",
                    "consumer",
                    "consumer_events",
                    "consumer_event_count",
                    "last_event_time",
                    "last_event_id",
                    "last_available_at",
                    "events_processed",
                    "candles_processed",
                    "strategy_evaluations",
                    "strategy_skipped",
                    "strategy_dirty",
                    "last_strategy_window_sizes",
                    "indicator_updates",
                    "issues",
                    "data_plane",
                ],
            )
            restored = IncrementalProcessor.from_checkpoint(json.loads(json.dumps(snapshot)))
            self.assertEqual(restored.checkpoint(), snapshot)

            plane_only = json.loads(json.dumps(plane.checkpoint()))
            strategy_only = json.loads(json.dumps(processor.strategy_checkpoint()))
            restored_plane = SharedDataPlane.from_checkpoint(plane_only)
            restored_strategy = IncrementalProcessor.from_strategy_checkpoint(
                strategy_only,
                data_plane=restored_plane,
                signal_consumer=RecordingSignalConsumer(max_events=64, max_signals=16),
            )
            self.assertEqual(restored_strategy.strategy_checkpoint(), strategy_only)
            self.assertEqual(restored_plane.checkpoint(), plane_only)

    def test_config_mismatch_and_corrupt_v1_payloads_fail_closed(self) -> None:
        with isolated_environment():
            processor = IncrementalProcessor(instrument="TEST/USD")
            processor.process_event(event(0))
            snapshot = processor.checkpoint()

            mismatch = dict(snapshot)
            mismatch["config_hash"] = "0" * 64
            with self.assertRaisesRegex(ValueError, "config_hash"):
                IncrementalProcessor.from_checkpoint(mismatch)

            unsupported = dict(snapshot)
            unsupported["checkpoint_version"] = 2
            with self.assertRaisesRegex(ValueError, "versión"):
                IncrementalProcessor.from_checkpoint(unsupported)

            corrupt_plane = dict(snapshot)
            corrupt_plane["data_plane"] = ["not", "a", "mapping"]
            with self.assertRaisesRegex(ValueError, "data_plane checkpoint"):
                IncrementalProcessor.from_checkpoint(corrupt_plane)

            malformed_bucket = json.loads(json.dumps(snapshot))
            malformed_bucket["aggregators"]["M1"]["bucket"] = {"events": []}
            with self.assertRaisesRegex(ValueError, "bucket de checkpoint"):
                IncrementalProcessor.from_checkpoint(malformed_bucket)

            plane = SharedDataPlane(timeframes=("M1",), instrument="TEST/USD", price_base=PriceBase.MID, max_candles=16)
            shared_processor = IncrementalProcessor(data_plane=plane, timeframes=("M1",), price_base=PriceBase.MID)
            shared = json.loads(json.dumps(shared_processor.strategy_checkpoint()))
            shared["data_plane_config_hash"] = "0" * 64
            with self.assertRaisesRegex(ValueError, "data_plane_config_hash"):
                IncrementalProcessor.from_strategy_checkpoint(shared, data_plane=plane)


if __name__ == "__main__":
    unittest.main()
