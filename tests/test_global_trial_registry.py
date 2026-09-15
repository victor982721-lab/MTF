from __future__ import annotations

import os
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from mtf_lab.ops.global_trial_registry import GlobalTrialRegistry, RegistryConflict, RegistryError


def _register(registry: GlobalTrialRegistry, candidate: str) -> dict[str, object]:
    return registry.register_attempt(
        candidate_id=candidate,
        protocol_hash="p" * 64,
        dataset_hash="d" * 64,
        runtime_identity={"python": "3.14", "mode": "offline"},
        data_identity={"source": "local", "scope": "historical"},
        scope={"candidate": candidate, "market": "EUR/USD"},
        parameters={"threshold": "frozen"},
    )


class GlobalTrialRegistryTests(unittest.TestCase):
    def test_registry_is_append_only_hashed_and_tracks_terminal_status(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-global-registry-") as directory:
            path = Path(directory) / "trials.jsonl"
            registry = GlobalTrialRegistry(path)
            registered = _register(registry, "tp_fast_v1")
            self.assertEqual(registry.revision, 1)
            running = registry.update_status(str(registered["attempt_id"]), "RUNNING")
            finished = registry.update_status(
                str(registered["attempt_id"]), "FAILED", details={"reason": "insufficient"}
            )
            self.assertEqual(running["status"], "RUNNING")
            self.assertEqual(finished["status"], "FAILED")
            self.assertTrue(registry.validate()["ok"])
            records = registry.records()
            self.assertEqual(len(records), 3)
            self.assertEqual(records[0]["previous_record_hash"], None)
            self.assertEqual(records[1]["previous_record_hash"], records[0]["record_hash"])
            self.assertEqual(records[2]["previous_record_hash"], records[1]["record_hash"])
            self.assertEqual(registry.validate()["attempt_count"], 1)

    def test_registry_cas_and_sensitive_fields_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-global-registry-") as directory:
            registry = GlobalTrialRegistry(Path(directory) / "trials.jsonl")
            _register(registry, "tp_fast_v1")
            with self.assertRaises(RegistryConflict):
                _register_with_revision(registry, expected_revision=0)
            with self.assertRaises(RegistryError):
                registry.append({"event": "BAD", "access_token": "secret"})

    def test_concurrent_append_preserves_every_revision(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-global-registry-") as directory:
            registry = GlobalTrialRegistry(Path(directory) / "trials.jsonl")

            def append(index: int) -> dict[str, object]:
                return _register(registry, f"candidate-{index}")

            with ThreadPoolExecutor(max_workers=4) as pool:
                results = list(pool.map(append, range(8)))
            self.assertEqual(len(results), 8)
            self.assertEqual(registry.revision, 8)
            self.assertTrue(registry.validate()["ok"])
            self.assertEqual([item["registry_revision"] for item in registry.records()], list(range(1, 9)))

    def test_symlink_hardlink_and_fifo_targets_are_rejected_without_blocking(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-global-registry-") as directory:
            root = Path(directory)
            real = root / "real"
            real.mkdir()
            parent_link = root / "parent-link"
            parent_link.symlink_to(real, target_is_directory=True)
            with self.assertRaises(RegistryError):
                GlobalTrialRegistry(parent_link / "trials.jsonl")

            source = root / "source"
            source.write_bytes(b"preserve")
            hardlink = root / "hardlink"
            os.link(source, hardlink)
            with self.assertRaises(RegistryError):
                GlobalTrialRegistry(hardlink)
            self.assertEqual(source.read_bytes(), b"preserve")

            fifo = root / "trials.fifo"
            os.mkfifo(fifo)
            try:
                with self.assertRaises(RegistryError):
                    GlobalTrialRegistry(fifo)
            finally:
                fifo.unlink()


def _register_with_revision(registry: GlobalTrialRegistry, *, expected_revision: int) -> dict[str, object]:
    return registry.register_attempt(
        candidate_id="tp_fast_v1",
        protocol_hash="p" * 64,
        dataset_hash="d" * 64,
        runtime_identity={"python": "3.14"},
        expected_revision=expected_revision,
    )


if __name__ == "__main__":
    unittest.main()
