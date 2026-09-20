"""Focused contracts for the Python write-path audit in the offline runner."""

from __future__ import annotations

import importlib.util
import os
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

from tools import offline_tests

ROOT = Path(__file__).resolve().parents[1]


def _coverage_site() -> Path:
    spec = importlib.util.find_spec("coverage")
    if spec is None or spec.origin is None:
        raise unittest.SkipTest("Coverage.py no está instalado en el intérprete de QA")
    return Path(spec.origin).resolve().parent.parent


class OfflineWriteAuditTests(unittest.TestCase):
    def _synthetic_root(self, body: str) -> tuple[tempfile.TemporaryDirectory[str], Path]:
        temporary = tempfile.TemporaryDirectory(prefix="mtf-write-audit-root-")
        root = Path(temporary.name)
        (root / "tests").mkdir()
        (root / "tests" / "test_synthetic.py").write_text(
            "import unittest\n" + textwrap.dedent(body) + "\n",
            encoding="utf-8",
        )
        return temporary, root

    def test_stateful_suite_allows_isolated_fixture_and_state_writes_with_audit(self) -> None:
        body = r"""
        import os
        import subprocess
        import sys
        import unittest
        from pathlib import Path

        class SyntheticWrites(unittest.TestCase):
            def test_allowed_paths(self):
                roots = [
                    Path(os.environ["HOME"]),
                    Path(os.environ["XDG_CONFIG_HOME"]),
                    Path(os.environ["XDG_CACHE_HOME"]),
                    Path(os.environ["XDG_DATA_HOME"]),
                    Path(os.environ["XDG_STATE_HOME"]),
                    Path(os.environ["TMPDIR"]),
                ]
                for index, root in enumerate(roots):
                    root.mkdir(parents=True, exist_ok=True)
                    (root / ("fixture-" + str(index))).write_text("fixture", encoding="utf-8")

                temp_root = Path(os.environ["TMPDIR"])
                nested = temp_root / "nested" / "state"
                nested.mkdir(parents=True)
                source = nested / "source"
                source.write_bytes(b"state")
                target = nested / "target"
                source.rename(target)
                replacement = nested / "replacement"
                target.replace(replacement)

                fd_root = os.open(nested, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    fd = os.open("fd-state", os.O_CREAT | os.O_WRONLY, 0o600, dir_fd=fd_root)
                    os.write(fd, b"fd")
                    os.close(fd)
                    os.rename("fd-state", "fd-state-renamed", src_dir_fd=fd_root, dst_dir_fd=fd_root)
                    os.unlink("fd-state-renamed", dir_fd=fd_root)
                finally:
                    os.close(fd_root)

                link_target = nested / "link-target"
                link_target.write_text("link", encoding="utf-8")
                link = nested / "link"
                link.symlink_to(link_target)
                link.write_text("link-updated", encoding="utf-8")
                link.unlink()
                hardlink = nested / "hardlink"
                os.link(link_target, hardlink)
                hardlink.unlink()
                os.truncate(link_target, 4)
                os.utime(link_target, None)
                os.chmod(link_target, 0o600)
                os.remove(link_target)
                replacement.unlink()
                nested.rmdir()

                child = (
                    "from pathlib import Path; import os; "
                    "Path(os.environ['TMPDIR']).joinpath('python-child').write_text('child')"
                )
                subprocess.run([sys.executable, "-c", child], check=True)
                subprocess.run([sys.executable, "-c", "pass"], input="", text=True, check=True)
        """
        temporary, root = self._synthetic_root(body)
        try:
            result = offline_tests.run_suite(root, timeout=30)
            self.assertTrue(result["summary"]["successful"], result)
            self.assertEqual([], result["write_attempts"], result)
            self.assertTrue(result["guard_evidence"]["write_audit"], result)
            audit = result["write_audit"]
            self.assertTrue(audit["enabled"], audit)
            self.assertGreater(audit["allowed_writes"], 0, audit)
            self.assertEqual(0, audit["blocked_writes"], audit)
            self.assertEqual([], audit["native_subprocesses_unverified"], audit)
            self.assertTrue(any(event["kind"] == "os.open" for event in audit["events"]), audit)
            self.assertTrue(any(event["kind"].startswith("os.rename") for event in audit["events"]), audit)
        finally:
            temporary.cleanup()

    def test_external_sentinel_and_symlink_escape_are_blocked_and_preserved(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-write-audit-sentinel-") as sentinel_name:
            sentinel_dir = Path(sentinel_name)
            sentinel = sentinel_dir / "sentinel.txt"
            sentinel.write_text("original", encoding="utf-8")
            named_fifo = sentinel_dir / "named-fifo"
            os.mkfifo(named_fifo)
            body = f"""
            import io
            import json
            import os
            import subprocess
            import sys
            import unittest
            from pathlib import Path

            class SyntheticEscape(unittest.TestCase):
                def test_external_paths_are_rejected(self):
                    outside = Path({str(sentinel)!r})
                    try:
                        outside.write_text("direct", encoding="utf-8")
                    except RuntimeError:
                        pass
                    else:
                        self.fail("external direct write was not rejected")

                    alias = Path(os.environ["TMPDIR"]) / "external-alias"
                    alias.symlink_to(outside)
                    try:
                        alias.write_text("through-alias", encoding="utf-8")
                    except RuntimeError:
                        pass
                    else:
                        self.fail("symlink escape was not rejected")
                    fd = os.open({str(sentinel_dir)!r}, os.O_RDONLY | os.O_DIRECTORY)
                    try:
                        try:
                            os.open("dir-fd-child", os.O_CREAT | os.O_WRONLY, 0o600, dir_fd=fd)
                        except RuntimeError:
                            pass
                        else:
                            self.fail("dir_fd escape was not rejected")
                        try:
                            os.symlink("sentinel.txt", "dir-fd-alias", dir_fd=fd)
                        except RuntimeError:
                            pass
                        else:
                            self.fail("symlink dir_fd escape was not rejected")
                    finally:
                        os.close(fd)

                    regular_fd = os.open(outside, os.O_RDONLY)
                    try:
                        try:
                            io.open(regular_fd, "wb", closefd=False)
                        except RuntimeError:
                            pass
                        else:
                            self.fail("regular external fd was treated as an allowed pipe")
                    finally:
                        os.close(regular_fd)

                    fifo_fd = os.open({str(named_fifo)!r}, os.O_RDONLY | os.O_NONBLOCK)
                    try:
                        try:
                            io.open(fifo_fd, "wb", closefd=False)
                        except RuntimeError:
                            pass
                        else:
                            self.fail("named FIFO external fd was treated as an anonymous pipe")
                    finally:
                        os.close(fifo_fd)

                    child_env = os.environ.copy()
                    child_env["MTF_OFFLINE_WRITE_ROOTS"] = json.dumps([{str(sentinel_dir)!r}])
                    try:
                        subprocess.run([sys.executable, "-c", "pass"], env=child_env, check=True)
                    except RuntimeError:
                        pass
                    else:
                        self.fail("child process broadened the inherited write roots")

                    child = (
                        "from pathlib import Path; "
                        "Path({str(sentinel)!r}).write_text('subprocess', encoding='utf-8')"
                    )
                    result = subprocess.run([sys.executable, "-c", child], check=False)
                    self.assertNotEqual(0, result.returncode)
            """
            temporary, root = self._synthetic_root(body)
            try:
                result = offline_tests.run_suite(root, timeout=30)
                self.assertTrue(result["summary"]["successful"], result)
                self.assertEqual(6, len(result["write_attempts"]), result)
                audit = result["write_audit"]
                self.assertEqual(6, audit["blocked_writes"], audit)
                self.assertTrue(any(event["allowed"] is False for event in audit["events"]), audit)
                self.assertEqual("original", sentinel.read_text(encoding="utf-8"))
            finally:
                temporary.cleanup()
                named_fifo.unlink(missing_ok=True)

    def test_strict_smoke_still_blocks_even_an_isolated_write(self) -> None:
        script = 'from pathlib import Path; import os; Path(os.environ["TMPDIR"]).joinpath("blocked").write_text("x")'
        with tempfile.TemporaryDirectory(prefix="mtf-write-audit-smoke-") as name:
            child_root = Path(name) / "child"
            result = offline_tests._child_command(
                [sys.executable, "-c", script],
                root=ROOT,
                temp_root=child_root,
                block_writes=True,
                timeout=10,
            )
            self.assertNotEqual(0, result["returncode"])
            self.assertFalse((child_root / "tmp" / "blocked").exists())
            self.assertEqual("block_all_writes", result["write_audit"]["enforcement"])
            self.assertEqual(1, result["write_audit"]["blocked_writes"])
            self.assertEqual(1, len(result["write_attempts"]), result)

    def test_native_subprocess_is_recorded_as_unverified(self) -> None:
        body = """
        import os
        import subprocess
        import unittest

        class NativeProcess(unittest.TestCase):
            def test_read_only_probe(self):
                subprocess.run(["/bin/true"], check=True)
                self.assertEqual(0, os.system(":"))
        """
        temporary, root = self._synthetic_root(body)
        try:
            result = offline_tests.run_suite(root, timeout=30)
            self.assertTrue(result["summary"]["successful"], result)
            audit = result["write_audit"]
            self.assertEqual(2, len(audit["native_subprocesses_unverified"]), audit)
            self.assertTrue(any(item["command"] == "os.system" for item in audit["native_subprocesses_unverified"]))
            self.assertIn("native filesystem I/O and child diagnostics are not aggregated", audit["scope"])
        finally:
            temporary.cleanup()

    def test_coverage_output_outside_child_temp_uses_exact_paths(self) -> None:
        body = """
        import unittest

        class Tiny(unittest.TestCase):
            def test_ok(self):
                self.assertTrue(True)
        """
        temporary, root = self._synthetic_root(body)
        try:
            with tempfile.TemporaryDirectory(prefix="mtf-write-audit-coverage-") as output_name:
                coverage_file = Path(output_name) / ".coverage"
                result = offline_tests.run_suite(
                    root,
                    timeout=30,
                    coverage_file=coverage_file,
                    coverage_site=_coverage_site(),
                )
                self.assertTrue(result["summary"]["successful"], result)
                self.assertTrue(result["coverage"]["data_file_exists"], result)
                audit = result["write_audit"]
                self.assertIn(str(coverage_file), audit["authorized_paths"], audit)
                self.assertNotIn(str(Path(output_name)), audit["allowed_roots"], audit)

                script = """
                import json
                import os
                from pathlib import Path

                paths = json.loads(os.environ["MTF_OFFLINE_WRITE_PATHS"])
                allowed = Path(paths[0])
                allowed.write_text("coverage", encoding="utf-8")
                sibling = allowed.with_name("not-coverage")
                try:
                    sibling.write_text("forbidden", encoding="utf-8")
                except RuntimeError:
                    pass
                else:
                    raise AssertionError("coverage parent was treated as a general write root")
                """
                with tempfile.TemporaryDirectory(prefix="mtf-write-audit-child-") as child_name:
                    child_result = offline_tests._child_command(
                        [sys.executable, "-c", textwrap.dedent(script)],
                        root=ROOT,
                        temp_root=Path(child_name),
                        block_writes=False,
                        timeout=10,
                        write_paths=(coverage_file,),
                        write_dirs=(coverage_file.parent,),
                    )
                self.assertEqual(0, child_result["returncode"], child_result)
                self.assertEqual(1, child_result["write_audit"]["allowed_writes"], child_result)
                self.assertEqual(1, child_result["write_audit"]["blocked_writes"], child_result)
                self.assertFalse((coverage_file.parent / "not-coverage").exists())
        finally:
            temporary.cleanup()

    def test_coverage_symlink_is_rejected_before_child_starts(self) -> None:
        body = """
        import unittest

        class Tiny(unittest.TestCase):
            def test_ok(self):
                self.assertTrue(True)
        """
        temporary, root = self._synthetic_root(body)
        try:
            with tempfile.TemporaryDirectory(prefix="mtf-write-audit-symlink-") as output_name:
                target = Path(output_name) / "real-coverage"
                target.write_text("preserve", encoding="utf-8")
                link = Path(output_name) / ".coverage"
                link.symlink_to(target)
                with self.assertRaisesRegex(ValueError, "symlink"):
                    offline_tests.run_suite(
                        root,
                        timeout=30,
                        coverage_file=link,
                        coverage_site=_coverage_site(),
                    )
                self.assertEqual("preserve", target.read_text(encoding="utf-8"))

                nested_target = Path(output_name) / "nested-target"
                nested_target.mkdir()
                nested_alias = Path(output_name) / "nested-alias"
                nested_alias.symlink_to(nested_target, target_is_directory=True)
                nested_file = nested_alias / "created" / ".coverage"
                with self.assertRaisesRegex(ValueError, "symlink"):
                    offline_tests.run_suite(
                        root,
                        timeout=30,
                        coverage_file=nested_file,
                        coverage_site=_coverage_site(),
                    )
                self.assertFalse((nested_target / "created").exists())
        finally:
            temporary.cleanup()


if __name__ == "__main__":
    unittest.main()
