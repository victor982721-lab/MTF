from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from mtf_lab.configuration import load_config
from mtf_lab.core import Signal
from mtf_lab.data.models import Event
from mtf_lab.ops.persistence import SQLiteStore
from mtf_lab.runtime import IncrementalProcessor, RuntimeCoordinator, SimulationConfig

BASE = datetime(2026, 1, 1, tzinfo=UTC)


def fixture_event(seconds: int, price: float, event_id: str) -> Event:
    timestamp = BASE + timedelta(seconds=seconds)
    return Event(
        instrument="TEST/USD",
        event_time=timestamp,
        price=price,
        quantity=1.0,
        source="fixture",
        source_event_id=event_id,
        received_at=timestamp,
        available_at=timestamp,
    )


class RuntimeOperationalTests(unittest.TestCase):
    def test_open_capture_keeps_missing_final_pending_until_complete(self) -> None:
        processor = IncrementalProcessor(
            instrument="TEST/USD",
            simulation=SimulationConfig(horizons_seconds=(60,), entry_latency_seconds=1, max_price_age_seconds=2),
        )
        processor.register_signal(
            Signal(
                signal_id="sig",
                instrument="TEST/USD",
                direction="UP",
                detected_at=BASE,
                context_start=BASE,
                preparation_start=BASE,
                trigger_start=BASE,
                trigger_end=BASE + timedelta(minutes=1),
                episode_id="episode",
            )
        )
        processor.process_event(fixture_event(1, 100.0, "entry"))
        open_result = processor.finalize(BASE + timedelta(seconds=63), capture_complete=False)
        self.assertEqual(open_result.completed_simulations, ())
        self.assertEqual(len(processor.pending_simulations), 1)
        complete_result = processor.finalize(BASE + timedelta(seconds=63), capture_complete=True)
        self.assertEqual(len(complete_result.completed_simulations), 1)
        self.assertEqual(complete_result.completed_simulations[0].outcome, "INDETERMINATE")

    def test_checkpoint_restores_operational_gate_and_observation_book(self) -> None:
        config = load_config()
        live_config = replace(config, mode="LIVE")

        def clock() -> datetime:
            return BASE

        with TemporaryDirectory() as tmp, SQLiteStore(Path(tmp) / "runtime.sqlite3") as store:
            session_id = store.create_session(
                mode="LIVE", provider="fixture", instrument=live_config.instrument, config=live_config.to_dict()
            )
            coordinator = RuntimeCoordinator(
                store, session_id, live_config, mode="LIVE", dataset_hash="fixture", clock=clock
            )
            coordinator.update_feed_state(
                connection="DISCONNECTED",
                reconciliation="NEEDS_RECONCILIATION",
                freshness="STALE",
                blocked_reasons=("feed_stale",),
                block_details={"feed_stale": {"source": "fixture"}},
            )
            coordinator.checkpoint()
            resumed = RuntimeCoordinator(
                store, session_id, live_config, mode="LIVE", dataset_hash="fixture", clock=clock
            )
            status = resumed.status()
            self.assertFalse(status.analysis_enabled)
            self.assertIn("feed_stale", status.analysis_blocked_reasons)
            self.assertEqual(status.freshness_state, "STALE")

    def test_max_candles_bounds_runtime_dedupe_and_indicator_buffers(self) -> None:
        processor = IncrementalProcessor(instrument="TEST/USD", max_candles=8)
        for index in range(120):
            processor.process_event(fixture_event(index * 60, 100.0 + index, f"event-{index}"))
        self.assertLessEqual(len(processor.indicator_engines["M1"]._points), 8)
        self.assertLessEqual(len(processor.aggregators["M1"]._seen_event_ids), 8)
        self.assertLessEqual(len(processor._simulation_book.observations), 4096)


if __name__ == "__main__":
    unittest.main()
