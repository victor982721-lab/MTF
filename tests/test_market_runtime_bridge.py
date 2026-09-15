"""The supervisor consumes causal profile context, not retained-list length."""

from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch

from mtf_lab.core.aggregation import BAR_CLOCK_BASIS, bar_clock_ordinal
from mtf_lab.core.models import EventKind, MarketEvent, OperationMode, PriceBase
from mtf_lab.ops.ctrader_watch import CTraderWatchRunner
from mtf_lab.ops.supervision import _SupervisedWatchRunner
from mtf_lab.runtime.integration import RuntimeCoordinator
from mtf_lab.runtime.processor import IncrementalProcessor


class MarketRuntimeBridgeTests(unittest.TestCase):
    def test_snapshot_uses_existing_point_and_nominal_clock_without_io(self) -> None:
        coordinator = object.__new__(RuntimeCoordinator)
        coordinator.config = SimpleNamespace(execution={"market_candidate_id": "tp_fast_v1"})
        coordinator.mode = OperationMode.REPLAY
        coordinator.processor = IncrementalProcessor(
            instrument="EUR/USD",
            price_base="mid",
            max_candles=128,
            market_candidate_id="tp_fast_v1",
            quote_coverage_mode="continuous_quotes",
            max_quote_gap_seconds=90,
        )
        start = datetime(2016, 3, 7, tzinfo=UTC)
        for index in range(81):
            when = start + timedelta(seconds=index * 30)
            coordinator.processor.process_event(
                MarketEvent(
                    instrument="EUR/USD",
                    event_time=when,
                    available_at=when,
                    bid=1.1 + index * 0.000001,
                    ask=1.1001 + index * 0.000001,
                    event_kind=EventKind.QUOTE,
                    price_base=PriceBase.MID,
                    sequence=index,
                    source_event_id=str(index),
                )
            )
        with patch.object(coordinator.processor, "process_event", side_effect=AssertionError("recalculation")):
            snapshot = coordinator.market_profile_snapshot()
        assert snapshot is not None
        self.assertEqual(snapshot["market_candidate_id"], "tp_fast_v1")
        self.assertEqual(snapshot["trigger_bar_count"], bar_clock_ordinal(when, "M1"))
        self.assertEqual(snapshot["risk_bar_clock_basis"], BAR_CLOCK_BASIS)
        self.assertIsNotNone(snapshot["atr"])
        self.assertEqual(snapshot["data_mode"], "REPLAY")
        self.assertTrue(snapshot["read_only_memory_projection"])

    def test_bounded_signal_history_does_not_drop_delta_and_context_precedes_dispatch(self) -> None:
        runner = object.__new__(_SupervisedWatchRunner)
        runner._coordinator = SimpleNamespace(signals=("old-retained",))
        order = []
        runner._record_callback = lambda _record: order.append("record")
        runner._status_callback = lambda _coordinator: order.append("runtime")
        runner._signal_callback = lambda signal: order.append(signal)

        def transition(self, record, synthetic):
            self._coordinator.signals = ("new-signal",)
            self._last_record_signals = ("new-signal",)
            return True

        with patch.object(CTraderWatchRunner, "_process_record", transition):
            self.assertTrue(runner._process_record(object(), False))
        self.assertEqual(order, ["record", "runtime", "new-signal"])


if __name__ == "__main__":
    unittest.main()
