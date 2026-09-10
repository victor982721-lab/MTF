from __future__ import annotations

from datetime import UTC, datetime, timedelta
import unittest

from mtf_lab.ops.simulation import (
    DirectionalEvaluator,
    EvaluationSpec,
    Outcome,
    PricePoint,
    normalize_completion,
    normalize_price_base,
    select_price_point,
)
from mtf_lab.runtime.state import PendingSimulation, PriceObservation, SimulationConfig


BASE = datetime(2026, 1, 1, tzinfo=UTC)


class SimulationParityTests(unittest.TestCase):
    def test_price_base_aliases_are_shared_and_unknown_is_rejected(self) -> None:
        self.assertEqual(normalize_price_base("trade"), "traded")
        self.assertEqual(normalize_price_base("close"), "traded")
        self.assertEqual(normalize_price_base("BID"), "bid")
        with self.assertRaises(ValueError):
            normalize_price_base("future")
        with self.assertRaises(ValueError):
            SimulationConfig(requested_base_price="future")

    def test_observation_and_pending_keep_identity_instrument_and_market_times(self) -> None:
        observation = PriceObservation(
            timestamp=BASE,
            available_at=BASE + timedelta(seconds=2),
            price=100.0,
            base_price="trade",
            source="fixture",
            resolution="M1",
            instrument="TEST/USD",
            observation_id="obs-1",
            source_ordinal=4,
        )
        self.assertEqual(observation.base_price, "traded")
        self.assertEqual(observation.identity, "obs-1")
        self.assertEqual(observation.instrument, "TEST/USD")
        restored_observation = PriceObservation.from_dict(observation.to_dict())
        self.assertEqual(restored_observation.identity, observation.identity)
        self.assertEqual(restored_observation.available_at, observation.available_at)

        pending = PendingSimulation(
            simulation_id="sim-1",
            signal_id="sig-1",
            instrument="TEST/USD",
            direction="UP",
            horizon_seconds=60,
            detected_at=BASE,
            detection_available_at=BASE + timedelta(seconds=1),
            entry_due_at=BASE + timedelta(seconds=5),
            expiry_at=BASE + timedelta(seconds=65),
            price_base="close",
            capture_complete=False,
        )
        restored_pending = PendingSimulation.from_dict(pending.to_dict())
        self.assertEqual(restored_pending.identity, "sim-1")
        self.assertEqual(restored_pending.instrument, "TEST/USD")
        self.assertEqual(restored_pending.price_base, "traded")
        self.assertEqual(restored_pending.detection_available_at, BASE + timedelta(seconds=1))
        self.assertEqual(restored_pending.status, "PENDING")

    def test_as_of_excludes_late_final_and_completeness_controls_pending(self) -> None:
        points = [
            PricePoint(BASE + timedelta(seconds=2), 100, available_at=BASE + timedelta(seconds=2), source="f", base_price="close", quality="VALID", resolution="M1", instrument="TEST/USD", point_id="entry"),
            PricePoint(BASE + timedelta(seconds=62), 101, available_at=BASE + timedelta(seconds=62), source="f", base_price="traded", quality="VALID", resolution="M1", instrument="TEST/USD", point_id="final"),
        ]
        spec = EvaluationSpec(horizons_seconds=(60,), entry_latency_seconds=1, max_price_age_seconds=5, requested_base_price="close")
        evaluator = DirectionalEvaluator(spec)
        signal = {"signal_id": "sig", "instrument": "TEST/USD", "direction": "UP", "detected_ts": BASE}
        pending = evaluator.evaluate(signal, points, capture_complete=False, as_of=BASE + timedelta(seconds=60))
        complete = evaluator.evaluate(signal, points, capture_complete=True, as_of=BASE + timedelta(seconds=60))
        self.assertEqual(pending.outcome, Outcome.PENDING)
        self.assertEqual(complete.outcome, Outcome.INDETERMINATE)
        resolved = evaluator.evaluate(signal, points, capture_complete=True, as_of=BASE + timedelta(seconds=62))
        self.assertEqual(resolved.outcome, Outcome.WIN)
        self.assertEqual(resolved.entry_market_ts, "2026-01-01T00:00:02.000000Z")
        self.assertEqual(resolved.final_point_id, "final")

    def test_shared_selector_uses_availability_and_instrument(self) -> None:
        late = PricePoint(BASE + timedelta(seconds=2), 100, available_at=BASE + timedelta(seconds=20), source="f", base_price="traded", quality="VALID", resolution="M1", instrument="TEST/USD", point_id="late")
        selection, reason = select_price_point([late], BASE + timedelta(seconds=1), rule="first_observation_at_or_after", requested_base_price="trade", max_price_age_seconds=5)
        self.assertIsNone(selection)
        self.assertEqual(reason, "MAX_PRICE_AGE_EXCEEDED")
        selection, reason = select_price_point([late], BASE + timedelta(seconds=1), rule="first_observation_at_or_after", requested_base_price="traded", max_price_age_seconds=30, instrument="OTHER/USD")
        self.assertIsNone(selection)
        self.assertEqual(reason, "PRICE_NOT_AVAILABLE")

    def test_completion_transition_is_separate_from_watermark(self) -> None:
        self.assertFalse(normalize_completion(False))
        self.assertTrue(normalize_completion(capture_complete=True))
        with self.assertRaises(ValueError):
            normalize_completion(False, capture_complete=True)
        runtime = SimulationConfig()
        self.assertEqual(runtime.missing_outcome("no final", capture_complete=False), ("PENDING", "no final"))
        self.assertEqual(runtime.missing_outcome("no final", capture_complete=True), ("INDETERMINATE", "no final"))


if __name__ == "__main__":
    unittest.main()
