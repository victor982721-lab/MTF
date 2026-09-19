"""Isolated credential-boundary tests; never touch the user's stores."""

from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools import demo_canary_launcher as launcher
from tools.ctrader_query_launcher import LauncherError


class CanaryLauncherTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="mtf-canary-launcher-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        (self.root / "tools").mkdir(mode=0o700)
        (self.root / "tools/demo_canary.py").write_text("# controller fixture\n", encoding="utf-8")
        self.config = self.private_file("canary.toml", "# fixture\n")
        self.approval = self.private_file("approval.json", "{}")
        self.runtime = self.private_file("python", "#!/bin/sh\nexit 0\n")
        self.runtime.chmod(0o700)
        self.credentials = self.private_file(
            "ctrader-app-fixture.credentials.json",
            json.dumps({"client_id": "fixture-client", "client_secret": "fixture-secret"}),
        )

    def private_file(self, name: str, text: str) -> Path:
        path = self.root / name
        path.write_text(text, encoding="utf-8")
        path.chmod(0o600)
        return path

    def build(self, extra: list[str], env: dict[str, str] | None = None) -> tuple[Path, list[str], dict[str, str]]:
        return launcher.build_exec(
            ["--config", str(self.config), *extra],
            root=self.root,
            runtime_path=self.runtime,
            inherited=env or {},
        )

    def test_offline_default_never_reads_or_inherits_credentials(self) -> None:
        with patch.object(launcher, "_credentials", side_effect=AssertionError("must not read credentials")):
            _, command, env = self.build([], {"CTRADER_CLIENT_SECRET": "ambient", "PYTHONPATH": "/hostile"})
        self.assertIn("--preflight", command)
        self.assertNotIn("--network", command)
        self.assertNotIn("--execute", command)
        self.assertNotIn("CTRADER_CLIENT_SECRET", env)
        self.assertNotIn("PYTHONPATH", env)
        self.assertEqual(env["PATH"], "/usr/bin:/bin")

    def test_network_only_child_has_private_credentials(self) -> None:
        inherited = {"MTF_LAB_CTRADER_CREDENTIALS_FILE": str(self.credentials), "PYTHONHOME": "/hostile"}
        _, command, env = self.build(["--network", "--state-dir", str(self.root)], inherited)
        self.assertEqual(env["CTRADER_CLIENT_SECRET"], "fixture-secret")
        self.assertEqual(env["CTRADER_CLIENT_ID"], "fixture-client")
        self.assertNotIn("fixture-secret", " ".join(command))
        self.assertNotIn("--execute", command)
        self.assertNotIn("PYTHONHOME", env)
        self.assertNotIn("MTF_LAB_CTRADER_CREDENTIALS_FILE", env)
        self.assertNotIn("CTRADER_CLIENT_SECRET", inherited)

    def test_execute_requires_every_explicit_gate_flag(self) -> None:
        cases = (["--execute"], ["--execute", "--network"], ["--execute", "--approval-file", str(self.approval)])
        for extra in cases:
            with self.subTest(extra=extra), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit), patch.object(launcher, "_credentials") as credentials:
                    self.build(list(extra))
                credentials.assert_not_called()

    def test_execute_forwards_but_does_not_mutate(self) -> None:
        extra = [
            "--network",
            "--execute",
            "--state-dir",
            str(self.root),
            "--approval-file",
            str(self.approval),
            "--max-events",
            "1000",
        ]
        with patch.object(os, "execve") as execute:
            _, command, _ = self.build(extra, {"MTF_LAB_CTRADER_CREDENTIALS_FILE": str(self.credentials)})
        execute.assert_not_called()
        self.assertEqual(command.count("--execute"), 1)
        self.assertEqual(command[1:3], ["-I", "-B"])

    def test_help_and_unsupported_flags_do_not_read_private_files(self) -> None:
        for args in (["--help"], ["--client-secret", "not-accepted"]):
            with (
                self.subTest(args=args),
                contextlib.redirect_stdout(io.StringIO()),
                contextlib.redirect_stderr(io.StringIO()),
            ):
                with patch.object(launcher, "build_exec") as builder, self.assertRaises(SystemExit):
                    launcher.main(args)
                builder.assert_not_called()

    def test_private_paths_and_permissions_are_enforced(self) -> None:
        self.config.chmod(0o644)
        with self.assertRaises(LauncherError):
            self.build([])
        self.config.chmod(0o600)
        self.config.unlink()
        self.config.symlink_to(self.approval)
        with self.assertRaises(LauncherError):
            self.build([])

    def test_missing_credential_reference_does_not_fall_back_to_environment(self) -> None:
        with self.assertRaises(LauncherError):
            self.build(["--network", "--state-dir", str(self.root)], {"CTRADER_CLIENT_SECRET": "ambient"})


if __name__ == "__main__":
    unittest.main()
