"""Regression tests for the stdlib-only engineering audit."""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))

from engineering_audit import analyze_repository  # noqa: E402


class EngineeringAuditTests(unittest.TestCase):
    def test_current_package_is_parseable_and_core_has_no_adapter_edges(self) -> None:
        result = analyze_repository(ROOT)
        self.assertEqual([], result["parse_errors"])
        self.assertIn("mtf_lab.core", result["modules"])
        forbidden_prefixes = ("mtf_lab.data", "mtf_lab.ops", "mtf_lab.runtime")
        for module, dependencies in result["dependencies"].items():
            if module == "mtf_lab.core" or module.startswith("mtf_lab.core."):
                self.assertFalse(
                    any(dependency.startswith(forbidden_prefixes) for dependency in dependencies),
                    msg=f"{module} has an adapter dependency: {dependencies}",
                )

    def test_cycle_and_forbidden_core_dependency_are_visible(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-audit-cycle-") as name:
            root = Path(name)
            package = root / "mtf_lab" / "core"
            package.mkdir(parents=True)
            (root / "mtf_lab" / "__init__.py").write_text("\n", encoding="utf-8")
            (package / "__init__.py").write_text("\n", encoding="utf-8")
            (package / "a.py").write_text("import socket\nfrom . import b\n", encoding="utf-8")
            (package / "b.py").write_text("from . import a\n", encoding="utf-8")
            result = analyze_repository(root)
        self.assertTrue(any(set(cycle) == {"mtf_lab.core.a", "mtf_lab.core.b"} for cycle in result["cycles"]))
        self.assertEqual("socket", result["forbidden_core_imports"][0]["target"])
        self.assertTrue(any(item["kind"] == "cycle" for item in result["strict_violations"]))

    def test_type_checking_dependency_is_not_a_runtime_edge(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-audit-typing-") as name:
            root = Path(name)
            package = root / "mtf_lab" / "core"
            package.mkdir(parents=True)
            (root / "mtf_lab" / "__init__.py").write_text("\n", encoding="utf-8")
            (package / "__init__.py").write_text("\n", encoding="utf-8")
            (package / "a.py").write_text(
                "from typing import TYPE_CHECKING\nif TYPE_CHECKING:\n    from ..data import Event\n",
                encoding="utf-8",
            )
            data = root / "mtf_lab" / "data"
            data.mkdir()
            (data / "__init__.py").write_text("\n", encoding="utf-8")
            result = analyze_repository(root)
        self.assertNotIn("mtf_lab.data", result["dependencies"]["mtf_lab.core.a"])
        self.assertEqual(1, len(result["typing_only_forbidden_core_imports"]))
        self.assertEqual([], result["forbidden_core_imports"])

    def test_package_symbol_import_does_not_add_unrelated_child_edges(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-audit-symbol-") as name:
            root = Path(name)
            package = root / "mtf_lab"
            ops = package / "ops"
            ops.mkdir(parents=True)
            (package / "__init__.py").write_text("__version__ = '1'\n", encoding="utf-8")
            (package / "__main__.py").write_text("raise SystemExit\n", encoding="utf-8")
            (ops / "__init__.py").write_text("\n", encoding="utf-8")
            (ops / "application.py").write_text("from .. import __version__\n", encoding="utf-8")
            result = analyze_repository(root)
        dependencies = result["dependencies"]["mtf_lab.ops.application"]
        self.assertIn("mtf_lab", dependencies)
        self.assertNotIn("mtf_lab.__main__", dependencies)

    def test_complexity_and_cross_object_private_state_are_measured(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-audit-metrics-") as name:
            root = Path(name)
            package = root / "mtf_lab" / "core"
            package.mkdir(parents=True)
            (root / "mtf_lab" / "__init__.py").write_text("\n", encoding="utf-8")
            (package / "__init__.py").write_text("\n", encoding="utf-8")
            (package / "metrics.py").write_text(
                "def sample(items, obj):\n"
                "    if items and len(items) > 0:\n"
                "        for item in items:\n"
                "            obj._state = item\n"
                "    return obj._state\n",
                encoding="utf-8",
            )
            result = analyze_repository(root)
        function = next(item for item in result["complexity"]["functions"] if item["name"] == "sample")
        self.assertEqual(4, function["complexity"])
        self.assertTrue(
            any(item["base"] == "obj" and item["access"] == "write" for item in result["private_state"]["cross_object"])
        )

    def test_top_level_io_is_a_strict_import_side_effect(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-audit-side-effect-") as name:
            root = Path(name)
            package = root / "mtf_lab" / "core"
            package.mkdir(parents=True)
            (root / "mtf_lab" / "__init__.py").write_text("\n", encoding="utf-8")
            (package / "__init__.py").write_text("open('state.json', 'w')\n", encoding="utf-8")
            result = analyze_repository(root)
        self.assertEqual(1, len(result["import_time_side_effects"]))
        self.assertEqual("open", result["import_time_side_effects"][0]["call"])
        self.assertTrue(any(item["kind"] == "import_time_side_effect" for item in result["strict_violations"]))


if __name__ == "__main__":
    unittest.main()
