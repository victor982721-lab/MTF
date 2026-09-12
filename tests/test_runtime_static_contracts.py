"""Focused offline contracts for the runtime static-cleanup tranche."""

from __future__ import annotations

import contextlib
import os
import unittest
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import mock

from mtf_lab.configuration import load_config
from mtf_lab.core import Candle, DataQuality, IndicatorPoint, MarketEvent, OperationMode, QualityFlag
from mtf_lab.ops.persistence import SQLiteStore
from mtf_lab.runtime import IncrementalProcessor, RuntimeCoordinator
from mtf_lab.runtime.processor import ProcessResult, RuntimeIssue, point_available
from mtf_lab.runtime.state import quality_from

BASE = datetime(2026, 1, 1, tzinfo=UTC)


@contextlib.contextmanager
def isolated_runtime_environment() -> Iterator[Path]:
    with TemporaryDirectory(prefix="mtf-runtime-static-") as directory:
        root = Path(directory)
        with mock.patch.dict(
            os.environ,
            {
                "HOME": str(root / "home"),
                "XDG_STATE_HOME": str(root / "xdg-state"),
                "MTF_LAB_STATE_DIR": str(root / "state"),
            },
            clear=False,
        ):
            yield root


def fixture_event(seconds: int, event_id: str, price: float = 100.0) -> MarketEvent:
    timestamp = BASE + timedelta(seconds=seconds)
    return MarketEvent(
        instrument="SYNTH/USD",
        event_time=timestamp,
        price=price,
        source="fixture",
        mode=OperationMode.REPLAY,
        received_at=timestamp,
        available_at=timestamp,
        source_event_id=event_id,
    )


class RuntimeStaticContractTests(unittest.TestCase):
    def test_quality_boundary_accepts_legacy_and_structured_inputs(self) -> None:
        invalid_legacy = quality_from(SimpleNamespace(valid=False), synthetic=False)
        structured = quality_from({"flags": ["gap"], "reasons": ["missing interval"], "source": "fixture"})

        self.assertFalse(invalid_legacy.usable)
        self.assertEqual(invalid_legacy.status, "invalid")
        self.assertFalse(structured.usable)
        self.assertIn("gap", {flag.value for flag in structured.flags})
        self.assertEqual(structured.source, "fixture")

    def test_checkpoint_roundtrip_preserves_event_identity_and_lifecycle(self) -> None:
        processor = IncrementalProcessor(instrument="TEST/USD")
        event = replace_event_instrument(fixture_event(1, "event-1"), "TEST/USD")

        result = processor.process_event(event)
        snapshot = processor.checkpoint()
        restored = IncrementalProcessor.from_checkpoint(snapshot)

        self.assertTrue(result.accepted)
        self.assertEqual(restored.events_processed, processor.events_processed)
        self.assertEqual(restored.last_event_id, processor.last_event_id)
        self.assertEqual(restored.last_event_time, processor.last_event_time)
        self.assertEqual(restored.status["price_base"], processor.status["price_base"])

    def test_point_available_rejects_open_or_naive_points_without_lookahead(self) -> None:
        point = IndicatorPoint(BASE, BASE + timedelta(minutes=1), None, 100, None, None, None, None)

        self.assertIsNone(point_available(replace(point, closed=False)))
        self.assertIsNone(point_available(replace(point, end=datetime(2026, 1, 1))))
        self.assertIsNotNone(point_available(point))

    def test_coordinator_process_uses_isolated_state_and_persists_capture(self) -> None:
        with isolated_runtime_environment() as root:
            config = load_config()
            database = root / "runtime.sqlite3"
            with SQLiteStore(database) as store:
                session_id = store.create_session(
                    mode="REPLAY",
                    provider="fixture",
                    instrument=config.instrument,
                    config=config.to_dict(),
                )
                coordinator = RuntimeCoordinator(
                    store,
                    session_id,
                    config,
                    mode="REPLAY",
                    dataset_hash="fixture",
                    checkpoint_every=1000,
                )
                result = coordinator.process(fixture_event(1, "event-3"))

                self.assertTrue(result.accepted)
                self.assertEqual(len(store.list_events(session_id)), 1)
                self.assertNotIn("site-packages", str(database))

    def test_startup_clip_preserves_evidence_and_does_not_ignore_operational_blocks(self) -> None:
        with isolated_runtime_environment() as root:
            config = load_config()
            with SQLiteStore(root / "gates.sqlite3") as store:
                session_id = store.create_session(
                    mode="REPLAY", provider="fixture", instrument=config.instrument, config=config.to_dict()
                )
                coordinator = RuntimeCoordinator(
                    store, session_id, config, mode="REPLAY", dataset_hash="fixture", resume=False
                )
                # Explicit broken state blocks even before the first event.
                with mock.patch.object(coordinator, "_processor_blocked_reasons", return_value=[]):
                    coordinator.update_feed_state(continuity="BROKEN", blocked_reasons=())
                    self.assertFalse(coordinator.can_emit_signals())
                coordinator.resolve_continuity(reason="test-only-start")
                first = fixture_event(1, "first")
                self.assertTrue(coordinator.process(first).accepted)
                self.assertTrue(coordinator.process(fixture_event(60, "boundary")).accepted)
                self.assertFalse(coordinator._runtime_block_details)
                history = coordinator.status().block_history
                self.assertTrue(any(item.get("startup_partial") for item in history))
                self.assertTrue(any(issue.code == "partial_bucket" for issue in coordinator.processor.issues))
                self.assertFalse(coordinator.can_emit_signals(), "indicator warmup still applies")
                snapshot = coordinator.export_state()
                coordinator.restore_state(snapshot)
                self.assertEqual(coordinator._startup_event_time, first.event_time)
                self.assertEqual(coordinator._startup_event_id, first.event_id)
                # Isolate operational gating from warmup for a precise negative
                # check; complete warmed replay is covered by paper_session.
                with mock.patch.object(coordinator, "_processor_blocked_reasons", return_value=[]):
                    self.assertTrue(coordinator.can_emit_signals())
                    for continuity in ("BROKEN", "UNKNOWN", "UNVERIFIED", "BLOCKED"):
                        coordinator.update_feed_state(continuity=continuity, blocked_reasons=())
                        self.assertFalse(coordinator.can_emit_signals(), continuity)
                    coordinator.update_feed_state(continuity="CONTINUOUS", blocked_reasons=("feed_stale",))
                    self.assertFalse(coordinator.can_emit_signals())
                    coordinator.update_feed_state(continuity="CONTINUOUS", blocked_reasons=())
                    coordinator.mode = OperationMode.LIVE
                    self.assertFalse(coordinator.can_emit_signals())

    def test_later_partials_native_quality_and_true_gaps_are_not_startup(self) -> None:
        with isolated_runtime_environment() as root:
            config = load_config()
            with SQLiteStore(root / "gates.sqlite3") as store:
                session_id = store.create_session(mode="REPLAY", provider="fixture", instrument=config.instrument)
                coordinator = RuntimeCoordinator(
                    store, session_id, config, mode="REPLAY", dataset_hash="fixture", resume=False
                )
                first = fixture_event(1, "first")
                coordinator.process(first)
                later = Candle(
                    config.instrument,
                    "M1",
                    BASE + timedelta(minutes=1),
                    BASE + timedelta(minutes=2),
                    1,
                    1,
                    1,
                    1,
                    quality=DataQuality.from_flag(QualityFlag.PARTIAL),
                    metadata={"partial": True, "coverage_start": (BASE + timedelta(seconds=61)).isoformat()},
                )
                issue = RuntimeIssue("quality_blocked", "partial", later.end, later.candle_id)
                coordinator._runtime_block_from_result(
                    ProcessResult(True, candles=(later,), issues=(issue,)), aggregated_input=True
                )
                self.assertIn("continuity:quality_blocked", coordinator._runtime_block_details)
                gap = RuntimeIssue("gap", "missing interval", BASE + timedelta(minutes=5))
                coordinator._runtime_block_from_result(ProcessResult(True, issues=(gap,)), aggregated_input=True)
                self.assertIn("continuity:gap", coordinator._runtime_block_details)
                self.assertFalse(coordinator.can_emit_signals())
                # Input bars cannot claim to be a trusted aggregator startup.
                coordinator.resolve_continuity(reason="test-only-explicit-resolution")
                native = Candle(
                    config.instrument,
                    "M1",
                    BASE,
                    BASE + timedelta(minutes=1),
                    1,
                    1,
                    1,
                    1,
                    quality=DataQuality.from_flag(QualityFlag.PARTIAL),
                    metadata={"partial": True, "coverage_start": first.event_time.isoformat()},
                )
                native_issue = RuntimeIssue("quality_blocked", "partial", native.end, native.candle_id)
                coordinator._runtime_block_from_result(ProcessResult(True, candles=(native,), issues=(native_issue,)))
                self.assertIn("continuity:quality_blocked", coordinator._runtime_block_details)


def replace_event_instrument(event: MarketEvent, instrument: str) -> MarketEvent:
    return MarketEvent(
        instrument=instrument,
        event_time=event.event_time,
        price=event.price,
        source=event.source,
        mode=event.mode,
        received_at=event.received_at,
        available_at=event.available_at,
        source_event_id=event.source_event_id,
    )


if __name__ == "__main__":
    unittest.main()
