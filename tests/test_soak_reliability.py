"""Pruebas focales del harness de resistencia local; nunca esperan 72 horas."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from tools.soak_reliability import (
    MIN_SOAK_SECONDS,
    CycleResult,
    SoakCheckpoint,
    SoakConfig,
    _acceptance_allowed,
    _default_fence_paths,
    _SoakState,
    compute_code_hash,
    run_soak,
)

BASE = datetime(2026, 1, 1, tzinfo=UTC)


class _FakeTime:
    def __init__(self) -> None:
        self.wall = BASE
        self.mono = 0.0

    def clock(self) -> datetime:
        return self.wall

    def monotonic(self) -> float:
        return self.mono

    def sleep(self, seconds: float) -> None:
        self.wall += timedelta(seconds=seconds)
        self.mono += seconds


class _Runner:
    def __init__(self, fake_time: _FakeTime | None = None, *, wall_jump: float = 0.0, mono_jump: float = 0.0) -> None:
        self.fake_time = fake_time
        self.wall_jump = wall_jump
        self.mono_jump = mono_jump
        self.calls: list[int] = []

    def run_cycle(self, cycle_index: int, _config: SoakConfig) -> CycleResult:
        self.calls.append(cycle_index)
        if self.fake_time is not None:
            self.fake_time.wall += timedelta(seconds=self.wall_jump)
            self.fake_time.mono += self.mono_jump
        return CycleResult(
            ok=True,
            status="REPLAY_SUPERVISOR_RESTORE",
            messages=1,
            events=1,
            progress=True,
            replay_hash=f"replay-{cycle_index}",
            supervisor_hash=f"supervisor-{cycle_index}",
        )


class _Watchdog:
    def __init__(self) -> None:
        self.calls: list[tuple[int, str]] = []

    def notify_progress(self, progress: int, status: str) -> bool:
        self.calls.append((progress, status))
        return True


def _config(root: Path, *, duration: float = 3.0, **kwargs: Any) -> SoakConfig:
    values: dict[str, Any] = {
        "duration_seconds": duration,
        "cycle_interval_seconds": 1.0,
        "state_dir": root / "state",
        "data_dir": root / "data",
        "checkpoint_path": root / "state" / "checkpoint.json",
        "smoke": True,
    }
    values.update(kwargs)
    return SoakConfig(**values)


def _fence_fixture() -> tuple[tempfile.TemporaryDirectory[str], Path, Path]:
    temporary = tempfile.TemporaryDirectory(prefix="mtf-soak-fence-repo-")
    root = Path(temporary.name)
    source = root / "mtf_lab" / "core" / "cfd_simulation.py"
    source.parent.mkdir(parents=True)
    (root / "mtf_lab" / "data" / "protobuf_generated").mkdir(parents=True)
    source.write_text("VALUE = 1\n", encoding="utf-8")
    subprocess.run(["git", "init", "--quiet", str(root)], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(root), "add", "mtf_lab/core/cfd_simulation.py"], check=True, capture_output=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(root),
            "-c",
            "user.name=MTF fixture",
            "-c",
            "user.email=mtf-fixture@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "fixture",
        ],
        check=True,
        capture_output=True,
    )
    return temporary, root, source


class SoakReliabilityTests(unittest.TestCase):
    def test_default_code_fence_covers_sources_generated_codec_metadata_and_excludes_derivatives(self) -> None:
        root = Path(__file__).resolve().parents[1]
        paths = set(_default_fence_paths(root))
        self.assertIn("mtf_lab/core/cfd_simulation.py", paths)
        self.assertIn("mtf_lab/ops/research.py", paths)
        self.assertIn("mtf_lab/ops/ctrader_account_risk.py", paths)
        self.assertIn("mtf_lab/ops/supervision_demo.py", paths)
        self.assertIn("mtf_lab/data/protobuf_generated/OpenApiMessages_pb2.py", paths)
        self.assertIn("tools/soak_reliability.py", paths)
        self.assertIn("config/ctrader_pipeline_fixture.toml", paths)
        self.assertIn("requirements-ctrader-codegen.lock", paths)
        self.assertIn("pyproject.toml", paths)
        self.assertFalse(any(item.startswith(("runtime/", "data/", "reports/")) for item in paths))
        self.assertFalse(
            any("__pycache__" in item or item.endswith((".pyc", ".db", ".sqlite3", ".log")) for item in paths)
        )

    def test_dirty_cfd_source_changes_default_code_fence(self) -> None:
        temporary, root, source = _fence_fixture()
        self.addCleanup(temporary.cleanup)
        original = source.read_bytes()
        mode = source.stat().st_mode & 0o777
        before = compute_code_hash(repo_root=root)
        try:
            source.write_bytes(original + b"\n# temporary soak fence probe\n")
            changed = compute_code_hash(repo_root=root)
        finally:
            source.write_bytes(original)
            os.chmod(source, mode)
        restored = compute_code_hash(repo_root=root)
        temporary.cleanup()
        self.assertNotEqual(before, changed)
        self.assertEqual(before, restored)

    def test_deleted_tracked_source_changes_default_code_fence(self) -> None:
        temporary, root, source = _fence_fixture()
        self.addCleanup(temporary.cleanup)
        original = source.read_bytes()
        mode = source.stat().st_mode & 0o777
        before = compute_code_hash(repo_root=root)
        try:
            source.unlink()
            deleted = compute_code_hash(repo_root=root)
        finally:
            source.write_bytes(original)
            os.chmod(source, mode)
        restored = compute_code_hash(repo_root=root)
        temporary.cleanup()
        self.assertNotEqual(before, deleted)
        self.assertEqual(before, restored)

    def test_added_generated_source_changes_default_code_fence(self) -> None:
        temporary, root, _source = _fence_fixture()
        generated = root / "mtf_lab" / "data" / "protobuf_generated" / "newgenerated.py"
        try:
            self.assertFalse(generated.exists())
            before = compute_code_hash(repo_root=root)
            generated.write_text("# temporary generated codec probe\n", encoding="utf-8")
            added = compute_code_hash(repo_root=root)
        finally:
            generated.unlink(missing_ok=True)
        restored = compute_code_hash(repo_root=root)
        temporary.cleanup()
        self.assertNotEqual(before, added)
        self.assertEqual(before, restored)

    def test_default_code_fence_rejects_directory_alias(self) -> None:
        temporary, root, _source = _fence_fixture()
        target = Path(tempfile.mkdtemp(prefix="soak-fence-target-"))
        alias = root / "mtf_lab" / "diralias"
        self.assertFalse(alias.exists())
        try:
            alias.symlink_to(target, target_is_directory=True)
            with self.assertRaises(ValueError):
                compute_code_hash(repo_root=root)
        finally:
            alias.unlink(missing_ok=True)
            target.rmdir()
            temporary.cleanup()

    def test_explicit_code_fence_rejects_file_alias_and_path_alias(self) -> None:
        temporary, root, target = _fence_fixture()
        alias = root / "mtf_lab" / "filealias.py"
        self.assertFalse(alias.exists())
        try:
            alias.symlink_to(target)
            with self.assertRaises(ValueError):
                compute_code_hash(repo_root=root, paths=("mtf_lab/filealias.py",))
        finally:
            alias.unlink(missing_ok=True)
            temporary.cleanup()
        temporary, root, _source = _fence_fixture()
        try:
            with self.assertRaises(ValueError):
                compute_code_hash(repo_root=root, paths=("mtf_lab/../mtf_lab/core/cfd_simulation.py",))
        finally:
            temporary.cleanup()

    def test_reduced_code_fence_can_run_but_never_accepts_72_hour_soak(self) -> None:
        config = SoakConfig(duration_seconds=MIN_SOAK_SECONDS, code_paths=("mtf_lab/core/cfd_simulation.py",))
        state = _SoakState(
            run_id="test",
            code_hash="code",
            input_hash="input",
            active_wall=MIN_SOAK_SECONDS,
            active_mono=MIN_SOAK_SECONDS,
            progress_counter=1,
            last_status="PASS",
            watchdog_state={"ok": True},
            disk_state={"ok": True},
        )
        self.assertFalse(_acceptance_allowed(state, config, status="PASS", control_injected=False))

    def test_default_duration_is_72_hours_and_non_smoke_short_duration_is_rejected(self) -> None:
        self.assertEqual(SoakConfig().duration_seconds, MIN_SOAK_SECONDS)
        with self.assertRaises(ValueError):
            SoakConfig(duration_seconds=1.0)

    def test_dry_run_does_not_create_state_or_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = _config(root, duration=MIN_SOAK_SECONDS, dry_run=True, smoke=False)
            receipt = run_soak(config)
            self.assertEqual(receipt.status, "DRY_RUN")
            self.assertFalse(receipt.acceptance)
            self.assertFalse((root / "state").exists())
            self.assertFalse((root / "data").exists())

    def test_injected_smoke_runs_replay_supervisor_restore_and_is_never_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fake = _FakeTime()
            runner = _Runner()
            progress: list[dict[str, Any]] = []
            receipt = run_soak(
                _config(root),
                cycle_runner=runner,
                clock=fake.clock,
                monotonic=fake.monotonic,
                sleep=fake.sleep,
                progress_sink=lambda row: progress.append(dict(row)),
            )
            self.assertEqual(receipt.status, "PASS")
            self.assertFalse(receipt.acceptance)
            self.assertEqual(receipt.clock_mode, "injected")
            self.assertEqual(receipt.runner_mode, "injected_test_double")
            self.assertGreaterEqual(receipt.cycles, 3)
            self.assertEqual(receipt.last_replay_hash, "replay-3")
            self.assertTrue(receipt.last_restore_hash)
            self.assertTrue(receipt.compare_hash)
            self.assertEqual(progress[0]["state"], "STARTED")
            self.assertEqual(progress[-1]["state"], "PASS")
            self.assertEqual((root / "state" / "checkpoint.json").stat().st_mode & 0o777, 0o600)
            raw = json.loads((root / "state" / "checkpoint.json").read_text(encoding="utf-8"))
            self.assertEqual(SoakCheckpoint.from_mapping(raw).restore_hash, receipt.last_restore_hash)

    def test_resume_accounts_wall_gap_as_downtime_not_active_duration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fake = _FakeTime()
            config = _config(root)
            first = run_soak(
                config,
                cycle_runner=_Runner(),
                clock=fake.clock,
                monotonic=fake.monotonic,
                sleep=fake.sleep,
                stop_after_cycles=1,
            )
            self.assertEqual(first.status, "PAUSED")
            active_before = first.monotonic_elapsed_seconds
            fake.wall += timedelta(seconds=100)
            resumed = run_soak(
                config,
                cycle_runner=_Runner(),
                clock=fake.clock,
                monotonic=fake.monotonic,
                sleep=fake.sleep,
            )
            self.assertEqual(resumed.status, "PASS")
            self.assertFalse(resumed.acceptance)
            self.assertGreaterEqual(resumed.downtime_seconds, 100.0)
            self.assertGreater(resumed.monotonic_elapsed_seconds, active_before)
            self.assertLess(resumed.monotonic_elapsed_seconds, 10.0)

    def test_hash_fence_rejects_input_mismatch_without_running_new_cycle(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fake = _FakeTime()
            first_config = _config(root, input_identity="fixture-a")
            run_soak(
                first_config,
                cycle_runner=_Runner(),
                clock=fake.clock,
                monotonic=fake.monotonic,
                sleep=fake.sleep,
                stop_after_cycles=1,
            )
            runner = _Runner()
            second = run_soak(
                _config(root, input_identity="fixture-b"),
                cycle_runner=runner,
                clock=fake.clock,
                monotonic=fake.monotonic,
                sleep=fake.sleep,
            )
            self.assertEqual(second.status, "FAILED")
            self.assertFalse(second.acceptance)
            self.assertIn("HASH_FENCE_MISMATCH", second.failed_reasons)
            self.assertEqual(runner.calls, [])

    def test_suspension_gap_is_discounted_and_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fake = _FakeTime()
            receipt = run_soak(
                _config(root, duration=20.0, suspension_threshold_seconds=5.0),
                cycle_runner=_Runner(fake, wall_jump=20.0, mono_jump=1.0),
                clock=fake.clock,
                monotonic=fake.monotonic,
                sleep=fake.sleep,
            )
            self.assertEqual(receipt.status, "FAILED")
            self.assertFalse(receipt.acceptance)
            self.assertIn("SUSPENSION_OR_CLOCK_GAP", receipt.failed_reasons)
            self.assertGreaterEqual(receipt.downtime_seconds, 19.0)
            self.assertLessEqual(receipt.monotonic_elapsed_seconds, 1.0)

    def test_failed_checkpoint_is_not_automatically_rearmed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fake = _FakeTime()
            config = _config(root, duration=20.0, suspension_threshold_seconds=5.0)
            failed = run_soak(
                config,
                cycle_runner=_Runner(fake, wall_jump=20.0, mono_jump=1.0),
                clock=fake.clock,
                monotonic=fake.monotonic,
                sleep=fake.sleep,
            )
            self.assertEqual(failed.status, "FAILED")
            resumed = run_soak(
                config,
                cycle_runner=_Runner(),
                clock=fake.clock,
                monotonic=fake.monotonic,
                sleep=fake.sleep,
            )
            self.assertEqual(resumed.status, "FAILED")
            self.assertIn("CHECKPOINT_HAS_FAILURES", resumed.failed_reasons)

    def test_disk_failure_never_approves_or_writes_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            receipt = run_soak(
                _config(root),
                cycle_runner=_Runner(),
                disk_probe=lambda _path, _minimum: False,
            )
            self.assertEqual(receipt.status, "FAILED")
            self.assertFalse(receipt.acceptance)
            self.assertIn("DISK_SPACE_FAILED", receipt.failed_reasons)
            self.assertFalse((root / "state" / "checkpoint.json").exists())

    def test_watchdog_fake_is_labeled_and_progress_is_received(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fake = _FakeTime()
            watchdog = _Watchdog()
            receipt = run_soak(
                _config(root, duration=1.0, watchdog_enabled=True),
                cycle_runner=_Runner(),
                clock=fake.clock,
                monotonic=fake.monotonic,
                sleep=fake.sleep,
                watchdog=watchdog,
            )
            self.assertEqual(receipt.status, "PASS")
            self.assertFalse(receipt.acceptance)
            self.assertEqual(receipt.watchdog["status"], "ACTIVE")
            self.assertGreaterEqual(len(watchdog.calls), 1)

    def test_local_supervisor_fixture_runner_is_offline_and_not_market_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            receipt = run_soak(
                _config(root, duration=0.1, cycle_interval_seconds=0.1, max_events_per_cycle=4),
            )
            self.assertEqual(receipt.status, "PASS", receipt.to_dict())
            self.assertFalse(receipt.acceptance)
            self.assertEqual(receipt.runner_mode, "local_supervisor_fixture")
            self.assertIn("not_market_evidence", receipt.observation_source)
            self.assertFalse(receipt.to_dict()["network_attempted"])
            self.assertFalse(receipt.to_dict()["execution_enabled"])
            self.assertEqual(receipt.restore_scope["restore_model"]["scope"], "restore_model")
            self.assertEqual(receipt.restore_scope["restore_model"]["status"], "VERIFIED")
            self.assertTrue(receipt.restore_scope["restore_model"]["corrupt_checkpoint_rejected"])
            self.assertEqual(receipt.restore_scope["bookkeeping"]["scope"], "bookkeeping")
            self.assertGreater(receipt.progress_counter, 0)


if __name__ == "__main__":
    unittest.main()
