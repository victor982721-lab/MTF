"""Offline filesystem contracts for the historical campaign runner."""

from __future__ import annotations

import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from tools import run_historical_campaign as campaign


class HistoricalCampaignOutputDirectoryTests(unittest.TestCase):
    def test_tempfile_output_creates_private_dirs_without_chmod_ancestors(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-campaign-output-") as directory:
            parent = Path(directory)
            os.chmod(parent, 0o755)
            target = parent / "nested" / "output"
            chmod_calls: list[Path] = []
            real_chmod = os.chmod

            def record_chmod(path: str | os.PathLike[str], mode: int) -> None:
                chmod_calls.append(Path(path))
                real_chmod(path, mode)

            with mock.patch.object(campaign.os, "chmod", side_effect=record_chmod):
                result = campaign._safe_output_dir(target)

            self.assertEqual(result, target)
            self.assertEqual(stat.S_IMODE(parent.stat().st_mode), 0o755)
            self.assertEqual(stat.S_IMODE((parent / "nested").stat().st_mode), 0o700)
            self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o700)
            self.assertEqual(set(chmod_calls), {parent / "nested", target})
            self.assertNotIn(parent.parent, chmod_calls)

    def test_home_state_ancestors_are_not_chmodded(self) -> None:
        home = Path.home()
        # The offline runner redirects XDG state into its isolated HOME.  Use
        # the effective XDG root so the fake filesystem models the actual
        # target's existing ancestors rather than attempting to create `/tmp`
        # and the runner's temporary parent.
        state_root = Path(os.environ.get("XDG_STATE_HOME", str(home / ".local" / "state")))
        target = state_root / "mtf-campaign-output"
        existing = {item for item in state_root.parents if item != Path(state_root.anchor)} | {state_root}
        created: set[Path] = set()
        chmod_calls: list[Path] = []

        def directory_stat(mode: int) -> os.stat_result:
            return os.stat_result((stat.S_IFDIR | mode, 1, 1, 1, os.getuid(), os.getgid(), 0, 0, 0, 0))

        def fake_lstat(path: str | os.PathLike[str]) -> os.stat_result:
            candidate = Path(path)
            if candidate in existing:
                return directory_stat(0o755)
            if candidate in created:
                return directory_stat(0o700)
            raise FileNotFoundError(candidate)

        def fake_mkdir(path: str | os.PathLike[str], _mode: int) -> None:
            created.add(Path(path))

        def record_chmod(path: str | os.PathLike[str], _mode: int) -> None:
            chmod_calls.append(Path(path))

        with (
            mock.patch.object(campaign.os, "lstat", side_effect=fake_lstat),
            mock.patch.object(campaign.os, "mkdir", side_effect=fake_mkdir),
            mock.patch.object(campaign.os, "chmod", side_effect=record_chmod),
        ):
            result = campaign._safe_output_dir(target)

        self.assertEqual(result, target)
        self.assertEqual(created, {target})
        self.assertEqual(chmod_calls, [target])
        self.assertTrue(set(state_root.parents).isdisjoint(chmod_calls))
        self.assertNotIn(state_root, chmod_calls)

    def test_existing_output_dir_is_owned_and_only_leaf_is_restricted(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-campaign-output-") as directory:
            parent = Path(directory)
            target = parent / "output"
            target.mkdir(mode=0o755)
            os.chmod(target, 0o755)
            chmod_calls: list[Path] = []
            real_chmod = os.chmod

            def record_chmod(path: str | os.PathLike[str], mode: int) -> None:
                chmod_calls.append(Path(path))
                real_chmod(path, mode)

            with mock.patch.object(campaign.os, "chmod", side_effect=record_chmod):
                result = campaign._safe_output_dir(target)

            self.assertEqual(result, target)
            self.assertEqual(stat.S_IMODE(parent.stat().st_mode), 0o700)
            self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o700)
            self.assertEqual(chmod_calls, [target])

    def test_existing_output_dir_with_foreign_owner_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-campaign-output-") as directory:
            target = Path(directory) / "output"
            target.mkdir()
            real_lstat = os.lstat
            chmod_calls: list[Path] = []

            def fake_lstat(path: str | os.PathLike[str]) -> os.stat_result:
                info = real_lstat(path)
                if Path(path) == target:
                    values = list(info)
                    values[4] = os.getuid() + 1
                    return os.stat_result(values)
                return info

            def record_chmod(path: str | os.PathLike[str], _mode: int) -> None:
                chmod_calls.append(Path(path))

            with (
                mock.patch.object(campaign.os, "lstat", side_effect=fake_lstat),
                mock.patch.object(campaign.os, "chmod", side_effect=record_chmod),
                self.assertRaisesRegex(campaign.HistoricalCampaignError, "pertenecer al usuario actual"),
            ):
                campaign._safe_output_dir(target)

            self.assertEqual(chmod_calls, [])

    def test_symlink_ancestor_is_rejected_before_creation(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-campaign-output-") as directory:
            root = Path(directory)
            real = root / "real"
            real.mkdir()
            link = root / "link"
            link.symlink_to(real, target_is_directory=True)

            with self.assertRaisesRegex(campaign.HistoricalCampaignError, "output_dir inseguro"):
                campaign._safe_output_dir(link / "output")
            self.assertFalse((real / "output").exists())


if __name__ == "__main__":
    unittest.main()
