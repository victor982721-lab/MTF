from __future__ import annotations

import os
import tempfile
import unittest
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from importlib import import_module
from pathlib import Path
from typing import Any
from unittest.mock import patch

from test_ctrader_canary_inputs import NOW, _FakeExecutor  # type: ignore[import-not-found]

from mtf_lab.configuration import load_config
from mtf_lab.ops.ctrader_canary_inputs import collect_canary_inputs
from mtf_lab.ops.persistence import SQLiteStore
from mtf_lab.ops.supervision import CTraderSupervisor, ExecutionCallbacks, SupervisorContext, SupervisorOptions

REAUDIT_NOW: datetime = datetime(2026, 1, 1, 12, tzinfo=UTC)


def _make_supervisor(
    root: Path,
    *,
    base: datetime,
    monotonic: Callable[[], float],
) -> tuple[CTraderSupervisor, SQLiteStore]:
    config = load_config(Path("config/ctrader_query.toml"))
    config = replace(config, mode="SYNTHETIC", data={**config.data, "mode": "SYNTHETIC"})

    class Provider:
        generation = 1
        status = {"connection": "CONNECTED"}

        def close(self) -> None:
            return None

    store = SQLiteStore(root / "runtime.sqlite3")
    supervisor = CTraderSupervisor(
        SupervisorContext(
            Provider(),
            config,
            store,
            provenance={
                "source_mode": "SYNTHETIC_FIXTURE",
                "synthetic": True,
                "environment": "OFFLINE",
                "network_performed": False,
                "execution_enabled": False,
            },
            clock=lambda: base,
            monotonic=monotonic,
            callbacks=ExecutionCallbacks(),
        ),
        SupervisorOptions(mode="observe", fixture=True, state_dir=root / "state"),
    )
    return supervisor, store


class BootAwareSupervisorTests(unittest.TestCase):
    def test_verified_boot_change_only_resets_clock_baseline(self) -> None:
        base = datetime(2026, 1, 1, tzinfo=UTC)

        with tempfile.TemporaryDirectory(prefix="mtf-reaudit-boot-") as directory:
            root = Path(directory)
            supervisor, store = _make_supervisor(root, base=base, monotonic=lambda: 1.0)
            try:
                supervisor.state.previous_wall_at = (base - timedelta(seconds=1)).isoformat()
                supervisor.state.previous_mono = 100.0
                supervisor.state.runtime_identity = {"boot_id": "old-boot"}
                supervisor.state.risk_blocked_reasons = ("existing-risk-block",)
                supervisor.state.fatal_latched = False
                supervisor.state.latch_reason = None

                with patch("mtf_lab.ops.supervision._boot_id", return_value="new-boot"):
                    supervisor._now()

                self.assertFalse(supervisor.state.fatal_latched)
                self.assertFalse(supervisor.state.clock_anomaly)
                self.assertEqual(supervisor.state.risk_blocked_reasons, ("existing-risk-block",))
                self.assertEqual(supervisor.state.previous_mono, 1.0)

                supervisor.state.fatal_latched = True
                supervisor.state.latch_reason = "legitimate-existing-latch"
                with patch("mtf_lab.ops.supervision._boot_id", return_value="new-boot"):
                    supervisor._now()
                self.assertTrue(supervisor.state.fatal_latched)
                self.assertEqual(supervisor.state.latch_reason, "legitimate-existing-latch")
            finally:
                store.close()

    def test_same_boot_rollback_still_latches(self) -> None:
        base = datetime(2026, 1, 1, tzinfo=UTC)

        with tempfile.TemporaryDirectory(prefix="mtf-reaudit-boot-") as directory:
            root = Path(directory)
            supervisor, store = _make_supervisor(root, base=base, monotonic=lambda: 1.0)
            try:
                supervisor.state.previous_wall_at = (base - timedelta(seconds=1)).isoformat()
                supervisor.state.previous_mono = 100.0
                supervisor.state.runtime_identity = {"boot_id": "same-boot"}

                with patch("mtf_lab.ops.supervision._boot_id", return_value="same-boot"):
                    supervisor._now()

                self.assertTrue(supervisor.state.fatal_latched)
                self.assertEqual(supervisor.state.latch_reason, "monotonic_went_backwards")
            finally:
                store.close()

    def test_boot_change_does_not_hide_wall_clock_rollback(self) -> None:
        base = datetime(2026, 1, 1, tzinfo=UTC)
        with tempfile.TemporaryDirectory(prefix="mtf-reaudit-boot-") as directory:
            root = Path(directory)
            supervisor, store = _make_supervisor(root, base=base, monotonic=lambda: 1.0)
            try:
                supervisor.state.previous_wall_at = (base + timedelta(seconds=10)).isoformat()
                supervisor.state.previous_mono = 100.0
                supervisor.state.runtime_identity = {"boot_id": "old-boot"}
                with patch("mtf_lab.ops.supervision._boot_id", return_value="new-boot"):
                    supervisor._now()
                self.assertTrue(supervisor.state.fatal_latched)
                self.assertEqual(supervisor.state.latch_reason, "clock_went_backwards")
            finally:
                store.close()

    def test_second_clock_sample_after_boot_transition_checks_new_baseline(self) -> None:
        base = datetime(2026, 1, 1, tzinfo=UTC)
        mono = [1.0]
        with tempfile.TemporaryDirectory(prefix="mtf-reaudit-boot-") as directory:
            root = Path(directory)
            supervisor, store = _make_supervisor(root, base=base, monotonic=lambda: mono[0])
            try:
                supervisor.state.previous_wall_at = (base - timedelta(seconds=1)).isoformat()
                supervisor.state.previous_mono = 100.0
                supervisor.state.runtime_identity = {"boot_id": "old-boot"}
                with patch("mtf_lab.ops.supervision._boot_id", return_value="new-boot"):
                    supervisor._now()
                    mono[0] = -1.0
                    supervisor._now()
                self.assertTrue(supervisor.state.fatal_latched)
                self.assertEqual(supervisor.state.latch_reason, "monotonic_went_backwards")
                self.assertEqual(supervisor.state.runtime_identity["boot_id"], "old-boot")
            finally:
                store.close()

    def test_unverified_boot_does_not_reset_strict_monotonic_guard(self) -> None:
        base = datetime(2026, 1, 1, tzinfo=UTC)
        with tempfile.TemporaryDirectory(prefix="mtf-reaudit-boot-") as directory:
            root = Path(directory)
            supervisor, store = _make_supervisor(root, base=base, monotonic=lambda: 1.0)
            try:
                supervisor.state.previous_wall_at = (base - timedelta(seconds=1)).isoformat()
                supervisor.state.previous_mono = 100.0
                supervisor.state.runtime_identity = {"boot_id": "old-boot"}
                with patch("mtf_lab.ops.supervision._boot_id", return_value=None):
                    supervisor._now()
                self.assertTrue(supervisor.state.fatal_latched)
                self.assertEqual(supervisor.state.latch_reason, "monotonic_went_backwards")
            finally:
                store.close()

    def test_persisted_state_reloads_with_boot_aware_baseline(self) -> None:
        base = datetime(2026, 1, 1, tzinfo=UTC)
        with tempfile.TemporaryDirectory(prefix="mtf-reaudit-boot-") as directory:
            root = Path(directory)
            first, first_store = _make_supervisor(root, base=base, monotonic=lambda: 1.0)
            first.state.previous_wall_at = (base - timedelta(seconds=1)).isoformat()
            first.state.previous_mono = 100.0
            first.state.runtime_identity = {"boot_id": "old-boot"}
            first.state_store.save(first.state)
            first_store.close()

            restarted, restarted_store = _make_supervisor(root, base=base, monotonic=lambda: 1.0)
            try:
                with patch("mtf_lab.ops.supervision._boot_id", return_value="new-boot"):
                    restarted._load_state()
                    restarted._now()
                self.assertFalse(restarted.state.fatal_latched)
                self.assertEqual(restarted.state.previous_mono, 1.0)
                self.assertEqual(restarted.state.runtime_identity["boot_id"], "old-boot")
            finally:
                restarted_store.close()


class AbsoluteDeadlineCollectorTests(unittest.TestCase):
    def setUp(self) -> None:
        fixture_module = import_module("test_ctrader_canary_inputs")
        self.fixture = fixture_module.CanaryInputTests("test_ready_context_uses_runtime_profile_and_manual_directions")
        self.fixture.setUp()

    def tearDown(self) -> None:
        self.fixture.tearDown()

    def _collect(
        self,
        *,
        deadline: datetime,
        window_end: datetime,
        clock: Callable[[], datetime],
        market_window: Callable[..., Any],
        observer: Any | None = None,
    ) -> Any:
        config = replace(
            self.fixture.config,
            execution={**self.fixture.config.execution, "max_price_age_seconds": "300"},
        )
        self.fixture._push_quote()
        observer = observer or _FakeExecutor()
        with (
            tempfile.TemporaryDirectory(prefix="mtf-reaudit-state-") as state_dir,
            tempfile.TemporaryDirectory(prefix="mtf-reaudit-home-") as home,
        ):
            (Path(home) / "tmp").mkdir()
            with patch.dict(
                os.environ,
                {
                    "HOME": home,
                    "XDG_CONFIG_HOME": str(Path(home) / "config"),
                    "XDG_STATE_HOME": str(Path(home) / "state"),
                    "XDG_CACHE_HOME": str(Path(home) / "cache"),
                    "XDG_DATA_HOME": str(Path(home) / "data"),
                    "TMPDIR": str(Path(home) / "tmp"),
                },
                clear=False,
            ):
                return collect_canary_inputs(
                    self.fixture.provider,
                    config,
                    network=True,
                    deadline=deadline,
                    max_events=1,
                    window_start=NOW - timedelta(minutes=1),
                    window_end=window_end,
                    session_evidence=self.fixture.evidence,
                    executor=observer,
                    warmup=self.fixture.warmup,
                    fetch_warmup=False,
                    market_window_state=market_window,
                    isolated_state_dir=state_dir,
                    clock=clock,
                )

    def test_slow_preparation_recomputes_deadline(self) -> None:
        now_value: list[datetime] = [REAUDIT_NOW]
        advanced = [False]

        def clock() -> datetime:
            return now_value[0]

        def market_window(_provider: Any, _start: datetime, _end: datetime) -> str:
            if not advanced[0]:
                advanced[0] = True
                now_value[0] = REAUDIT_NOW + timedelta(seconds=40)
            return "OPEN"

        result = self._collect(
            deadline=NOW + timedelta(seconds=10),
            window_end=NOW + timedelta(minutes=10),
            clock=clock,
            market_window=market_window,
        )
        self.assertFalse(result.ok)
        self.assertEqual(result.reason, "DEADLINE_EXPIRED_AFTER_PREPARATION")

    def test_late_capture_is_blocked_before_ready_even_before_window_end(self) -> None:
        now_value: list[datetime] = [REAUDIT_NOW]

        def clock() -> datetime:
            return now_value[0]

        original_poll = self.fixture.provider.client.poll_event

        def late_poll(timeout_seconds: float) -> Any:
            message = original_poll(timeout_seconds)
            if message is not None:
                now_value[0] = REAUDIT_NOW + timedelta(seconds=20)
            return message

        self.fixture.provider.client.poll_event = late_poll

        result = self._collect(
            deadline=NOW + timedelta(seconds=10),
            window_end=NOW + timedelta(minutes=10),
            clock=clock,
            market_window=lambda *_args: "OPEN",
            observer=_FakeExecutor(),
        )
        self.assertFalse(result.ok)
        self.assertEqual(result.reason, "DEADLINE_EXPIRED_DURING_CAPTURE")

    def test_nominal_capture_remains_ready(self) -> None:
        result = self._collect(
            deadline=NOW + timedelta(minutes=5),
            window_end=NOW + timedelta(minutes=10),
            clock=lambda: NOW,
            market_window=lambda *_args: "OPEN",
        )
        self.assertTrue(result.ok, result.reason)
        self.assertIsNotNone(result.context)


if __name__ == "__main__":
    unittest.main()
