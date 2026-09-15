"""Offline causal warmup→WF fixture and resume regressions."""

from __future__ import annotations

import json
import unittest
from dataclasses import replace
from datetime import UTC, datetime

from mtf_lab.data.walk_forward import (
    WALK_FORWARD_END,
    WALK_FORWARD_WINDOW_IDS,
    WalkForwardError,
    WalkForwardIdentityError,
)
from mtf_lab.data.walk_forward_fixture import (
    WALK_FORWARD_FIXTURE_DATA_ROOT,
    WalkForwardFixtureError,
    make_walk_forward_fixture,
    run_walk_forward_fixture,
)


class WalkForwardWarmupResumeTests(unittest.TestCase):
    def test_fixture_covers_each_exact_annual_window_and_keeps_warmup_non_operational(self) -> None:
        for window_id in WALK_FORWARD_WINDOW_IDS:
            with self.subTest(window_id=window_id):
                fixture = make_walk_forward_fixture(window=window_id)
                self.assertEqual(len(fixture.walk_forward_manifest.partitions), 48)
                self.assertEqual(fixture.walk_forward_manifest.data_root, WALK_FORWARD_FIXTURE_DATA_ROOT)
                self.assertEqual(fixture.to_dict()["holdout"], "CLOSED")
                self.assertFalse(fixture.to_dict()["network_performed"])
                self.assertFalse(fixture.to_dict()["data_acquisition"])
                self.assertFalse(fixture.to_dict()["trading_enabled"])

                result = run_walk_forward_fixture(fixture)
                self.assertTrue(result.finished)
                self.assertEqual(fixture.window.window_id, window_id)
                self.assertEqual(result.window_id, window_id)
                self.assertEqual(result.to_dict()["window_id"], window_id)
                self.assertEqual(result.processed_quotes, 13)
                self.assertEqual(result.phases[0], "WARMUP_ONLY")
                self.assertTrue(all(phase == "WF_EVALUATION" for phase in result.phases[1:]))
                self.assertNotIn("HOLDOUT_REJECTED", result.phases)

    def test_resume_from_warmup_and_wf_checkpoints_is_byte_equivalent(self) -> None:
        fixture = make_walk_forward_fixture(window="WF_2023")
        uninterrupted = run_walk_forward_fixture(fixture)

        for cut in (1, 5, 12):
            with self.subTest(cut=cut):
                partial = run_walk_forward_fixture(fixture, stop_after_quotes=cut)
                self.assertEqual(partial.status, "CHECKPOINTED")
                self.assertFalse(partial.finished)
                self.assertEqual(partial.window_id, "WF_2023")
                cursor = partial.checkpoint["cursor"]
                self.assertIsInstance(cursor, dict)
                self.assertEqual(cursor["accepted_count"], cut)
                checkpoint = json.loads(partial.checkpoint_bytes.decode("utf-8"))
                resumed = run_walk_forward_fixture(
                    fixture,
                    resume=checkpoint,
                    prefix=partial.output,
                )

                self.assertTrue(resumed.finished)
                self.assertEqual(resumed.output, uninterrupted.output)
                self.assertEqual(resumed.output_sha256, uninterrupted.output_sha256)
                self.assertEqual(resumed.checkpoint, uninterrupted.checkpoint)
                self.assertEqual(resumed.processed_quotes, uninterrupted.processed_quotes)

                with self.assertRaisesRegex(WalkForwardFixtureError, "prefix"):
                    run_walk_forward_fixture(
                        fixture,
                        resume=checkpoint,
                        prefix=partial.output + b"garbage\n",
                    )
                with self.assertRaisesRegex(WalkForwardFixtureError, "prefix"):
                    run_walk_forward_fixture(fixture, resume=checkpoint)

                bad_cursor = json.loads(partial.checkpoint_bytes.decode("utf-8"))
                bad_cursor["cursor"]["accepted_count"] = cut + 1
                with self.assertRaisesRegex(WalkForwardFixtureError, "accepted_count"):
                    run_walk_forward_fixture(fixture, resume=bad_cursor, prefix=partial.output)

    def test_resume_identity_and_closed_holdout_fail_before_consuming_fixture(self) -> None:
        fixture = make_walk_forward_fixture(window="WF_2020")
        partial = run_walk_forward_fixture(fixture, stop_after_quotes=1)

        tampered = dict(partial.checkpoint)
        tampered["input_hash"] = "0" * 64
        with self.assertRaisesRegex(WalkForwardIdentityError, "input_hash"):
            run_walk_forward_fixture(fixture, resume=tampered, prefix=partial.output)

        holdout_time = WALK_FORWARD_END
        holdout_quote = replace(
            fixture.quotes[-1],
            event_time=holdout_time,
            available_at=holdout_time,
        )
        with self.assertRaisesRegex(WalkForwardError, "holdout is CLOSED"):
            fixture.new_guard().validate(holdout_quote)

    def test_fixture_is_deterministic_and_does_not_require_real_dates_or_storage(self) -> None:
        first = make_walk_forward_fixture(window="WF_2021")
        second = make_walk_forward_fixture(window="WF_2021")
        first_run = run_walk_forward_fixture(first)
        second_run = run_walk_forward_fixture(second)

        self.assertEqual(first.to_dict(), second.to_dict())
        self.assertEqual(first_run.output, second_run.output)
        self.assertEqual(first_run.checkpoint_bytes, second_run.checkpoint_bytes)
        self.assertEqual(first_run.output_sha256, second_run.output_sha256)
        self.assertNotIn("2024", first_run.output.decode("utf-8"))
        self.assertLess(first.quotes[0].event_time, datetime(2021, 1, 1, tzinfo=UTC))


if __name__ == "__main__":
    unittest.main()
