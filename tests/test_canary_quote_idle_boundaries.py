"""Wall-clock idleness must not manufacture technical quote candle boundaries."""

from __future__ import annotations

import os
import tempfile
import unittest
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from mtf_lab.ops.ctrader_canary_inputs import collect_canary_inputs
from tests.test_canary_positive_async_quotes import (
    BASE as PROVIDER_BASE,
)
from tests.test_canary_positive_async_quotes import (
    _ProviderFixture,
    _spot_messages,
    _technical_config,
)
from tests.test_canary_runtime_quote_coverage import BASE, _coordinator, _quote


class CanaryQuoteIdleBoundaryTests(unittest.TestCase):
    def _observed_stream_with_idle(self, count: int):
        coordinator, _ = _coordinator(technical_gap=30)
        market_candles = []
        previous = None
        for index in range(count):
            source_time = BASE + timedelta(seconds=10 * index)
            available = source_time + timedelta(milliseconds=200)
            if previous is not None:
                current = previous + timedelta(seconds=1)
                while current < available:
                    before = tuple(coordinator.processor.indicator_points["M1"])
                    tick = coordinator.tick(current)
                    self.assertEqual(tick.candles, (), "idle time cannot close an observed quote bucket")
                    self.assertEqual(tuple(coordinator.processor.indicator_points["M1"]), before)
                    current += timedelta(seconds=1)
            event = replace(_quote(source_time, index), received_at=available, available_at=available)
            result = coordinator.process(event)
            self.assertTrue(result.accepted)
            market_candles.extend(result.candles)
            previous = available
        return coordinator, market_candles

    def test_idle_across_two_m1_boundaries_preserves_only_startup_partial(self) -> None:
        coordinator, candles = self._observed_stream_with_idle(15)
        m1 = [candle for candle in candles if candle.timeframe.name == "M1"]
        self.assertEqual(len(m1), 2)
        self.assertTrue(m1[0].metadata.get("partial"))
        self.assertFalse(m1[0].quality.valid)
        self.assertFalse(m1[1].metadata.get("partial"))
        self.assertTrue(m1[1].quality.valid)
        self.assertEqual(coordinator.continuity_state, "CONTINUOUS")
        self.assertFalse(coordinator.status().block_details)
        self.assertEqual(coordinator.processor.events_processed, 15)
        self.assertFalse(coordinator.signals)
        self.assertTrue(all(point.atr is None for point in coordinator.processor.indicator_points["M1"]))

    def test_atr14_requires_real_closed_m1_quotes_even_with_idle_ticks(self) -> None:
        coordinator, candles = self._observed_stream_with_idle(104)
        m1 = [candle for candle in candles if candle.timeframe.name == "M1"]
        self.assertEqual(sum(bool(candle.metadata.get("partial")) for candle in m1), 1)
        points = tuple(coordinator.processor.indicator_points["M1"])
        valid_atr = [point for point in points if point.quality.valid and point.atr is not None]
        self.assertTrue(valid_atr)
        first = valid_atr[0]
        self.assertGreaterEqual(sum(point.quality.valid and point.end <= first.end for point in points), 14)
        self.assertGreater(first.atr, 0)
        self.assertEqual(coordinator.continuity_state, "CONTINUOUS")
        self.assertEqual(coordinator.processor.events_processed, 104)
        self.assertFalse(coordinator.signals)

    def test_gap_beyond_thirty_seconds_and_wall_clock_staleness_still_block(self) -> None:
        coordinator, _ = _coordinator(technical_gap=30)
        coordinator.process(_quote(BASE, 0))
        stale_at = BASE + timedelta(seconds=max(70, coordinator.config.quality.max_feed_age_seconds + 1))
        idle = coordinator.tick(stale_at)
        self.assertEqual(idle.candles, ())
        self.assertEqual(coordinator.status().freshness_state, "STALE")
        self.assertIn("feed_stale", coordinator.status().analysis_blocked_reasons)
        result = coordinator.process(_quote(stale_at + timedelta(seconds=1), 1))
        self.assertTrue(any(issue.code in {"partial_bucket", "gap", "quality_blocked"} for issue in result.issues))
        self.assertEqual(coordinator.continuity_state, "BROKEN")
        self.assertFalse(coordinator.can_emit_signals())

    def test_default_live_tick_keeps_its_existing_bucket_closure(self) -> None:
        coordinator, _ = _coordinator(technical_gap=None)
        coordinator.process(_quote(BASE, 0))
        result = coordinator.tick(BASE.replace(second=0) + timedelta(minutes=1, seconds=1))
        self.assertTrue(any(candle.timeframe.name == "M1" for candle in result.candles))
        self.assertEqual(coordinator.processor.quote_coverage_mode, "strict")

    def test_technical_tick_still_advances_consumers_and_checkpoint_cadence(self) -> None:
        coordinator, _ = _coordinator(technical_gap=30)
        coordinator.process(_quote(BASE, 0))
        now = BASE + timedelta(seconds=20)
        with (
            patch.object(
                coordinator.processor, "_dispatch_advance", wraps=coordinator.processor._dispatch_advance
            ) as dispatch,
            patch.object(coordinator, "_checkpoint_if_due", wraps=coordinator._checkpoint_if_due) as checkpoint,
        ):
            result = coordinator.tick(now, force_checkpoint=True)
        self.assertEqual(result.candles, ())
        dispatch.assert_called_once_with(now, capture_complete=False)
        checkpoint.assert_called_once_with(force=True)

    def _collect_with_real_idle_polls(self, *, large_gap: bool = False):
        fixture = _ProviderFixture()
        config = _technical_config()
        messages = _spot_messages(PROVIDER_BASE + timedelta(seconds=5), count=15)
        if large_gap:
            # The second observed quote is beyond the existing idle bound.
            messages = [messages[0], *messages[4:]]
        for message in messages:
            fixture.transport.push(message)
        original_poll = fixture.client.poll_event
        cursor = 0
        empty_polls = 0

        def poll_event(timeout_seconds: float | None = None):
            nonlocal cursor, empty_polls
            target = messages[cursor].available_at
            assert target is not None
            if fixture.clock_value[0] < target:
                step = timedelta(seconds=min(0.25, timeout_seconds or 0.25))
                fixture.clock_value[0] = min(target, fixture.clock_value[0] + step)
                empty_polls += 1
                return None
            cursor += 1
            return original_poll(0)

        fixture.client.poll_event = poll_event
        try:
            with (
                tempfile.TemporaryDirectory() as state,
                tempfile.TemporaryDirectory() as home,
                patch.dict(os.environ, {"HOME": home, "XDG_STATE_HOME": str(Path(home) / "state")}),
            ):
                result = collect_canary_inputs(
                    fixture.provider,
                    config,
                    network=True,
                    deadline=PROVIDER_BASE + timedelta(minutes=25),
                    max_events=len(messages),
                    window_start=PROVIDER_BASE,
                    window_end=PROVIDER_BASE + timedelta(minutes=25),
                    preparation_start=PROVIDER_BASE,
                    session_evidence=fixture.evidence,
                    runtime_observer=lambda snapshot: dict(snapshot),
                    technical_only=True,
                    fetch_warmup=False,
                    market_window_state=lambda *_args: "OPEN",
                    isolated_state_dir=state,
                    clock=lambda: fixture.clock_value[0],
                    monotonic=lambda: (fixture.clock_value[0] - PROVIDER_BASE).total_seconds(),
                    close_provider=False,
                )
        finally:
            fixture.close()
        self.assertGreater(empty_polls, 100)
        return result

    def test_collector_exercises_none_polls_and_reports_healthy_short_warmup(self) -> None:
        result = self._collect_with_real_idle_polls()
        self.assertFalse(result.ok, "a two-minute capture must not invent ATR14 readiness")
        self.assertIn("ATR", result.reason or "")
        self.assertEqual(result.gates["watch_stop_reason"], "MAX_EVENTS")
        self.assertEqual(result.gates["watch_event_count"], 15)
        self.assertEqual(result.gates["watch_freshness_state"], "VALID")
        self.assertEqual(result.gates["watch_continuity_state"], "CONTINUOUS")
        self.assertEqual(result.gates["watch_block_details"], {})
        self.assertEqual(result.gates["watch_bbo_rejections"], [])
        self.assertFalse(result.gates["orders_attempted"])

    def test_collector_idle_limit_is_not_relaxed_for_the_boundary_fix(self) -> None:
        result = self._collect_with_real_idle_polls(large_gap=True)
        self.assertFalse(result.ok)
        self.assertEqual(result.gates["watch_stop_reason"], "IDLE_TIMEOUT")
        self.assertEqual(result.gates["watch_event_count"], 1)
        self.assertFalse(result.gates["orders_attempted"])


if __name__ == "__main__":
    unittest.main()
