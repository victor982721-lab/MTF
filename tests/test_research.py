from __future__ import annotations

import json
import os
import stat
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

from mtf_lab.configuration import load_config
from mtf_lab.ops.cfd_backtest import synthetic_cfd_capture
from mtf_lab.ops.research import (
    bootstrap_daily,
    compare_research,
    deflated_sharpe_ratio,
    probabilistic_sharpe_ratio,
    research_command,
    run_research,
    validate_research,
)


class ResearchServiceTests(unittest.TestCase):
    def test_run_registers_all_trials_before_results_and_validates(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-manifest-") as tmp:
            manifest_path = Path(tmp) / "research.json"
            result = run_research(
                manifest_path=manifest_path,
                fixture=True,
                variants=("trend_pullback_v1",),
                horizons_seconds=(60, 180),
                bootstrap_iterations=8,
            )
            self.assertEqual(result["schema"], "mtf-lab.research-manifest.v1")
            self.assertEqual(result["state"], "COMPLETED")
            self.assertTrue(result["trial_registration"]["before_results"])
            self.assertEqual(len(result["trials"]), 2)
            self.assertEqual(len(result["results"]), 2)
            self.assertTrue(all("ledger" in item and "equity_mark_to_market" in item for item in result["results"]))
            self.assertTrue(validate_research(manifest_path)["ok"])

    def test_manifest_is_private_and_tampering_is_detected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-manifest-") as tmp:
            path = Path(tmp) / "research.json"
            run_research(
                manifest_path=path,
                fixture=True,
                variants=("trend_pullback_v1",),
                bootstrap_iterations=4,
            )
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["state"] = "TAMPERED"
            path.write_text(json.dumps(payload), encoding="utf-8")
            validation = validate_research(path)
            self.assertFalse(validation["ok"])
            self.assertIn("integrity_hash_mismatch", validation["errors"])

    def test_manifest_existing_is_not_overwritten_and_bytes_are_preserved(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-manifest-") as tmp:
            path = Path(tmp) / "research.json"
            first = run_research(
                manifest_path=path,
                fixture=True,
                fixture_count=130,
                variants=("trend_pullback_v1",),
                horizons_seconds=(60,),
                bootstrap_iterations=1,
            )
            before = path.read_bytes()
            with self.assertRaises(ValueError):
                run_research(
                    manifest_path=path,
                    fixture=True,
                    fixture_count=130,
                    variants=("trend_pullback_v1",),
                    horizons_seconds=(60,),
                    bootstrap_iterations=1,
                )
            self.assertEqual(path.read_bytes(), before)
            self.assertEqual(json.loads(before)["run_id"], first["run_id"])
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            self.assertFalse(path.with_name(f".{path.name}.lock").exists())

    def test_manifest_first_publication_is_exclusive_under_concurrent_runs(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-manifest-") as tmp:
            path = Path(tmp) / "nested" / "research.json"

            def launch() -> object:
                try:
                    return run_research(
                        manifest_path=path,
                        fixture=True,
                        fixture_count=130,
                        variants=("trend_pullback_v1",),
                        horizons_seconds=(60,),
                        bootstrap_iterations=1,
                    )
                except Exception as exc:  # noqa: BLE001 - capture the losing race explicitly
                    return exc

            with ThreadPoolExecutor(max_workers=2) as pool:
                outcomes = list(pool.map(lambda _item: launch(), (0, 1)))
            self.assertEqual(sum(isinstance(item, dict) for item in outcomes), 1)
            self.assertEqual(sum(isinstance(item, Exception) for item in outcomes), 1)
            self.assertTrue(validate_research(path)["ok"])
            self.assertEqual(stat.S_IMODE(path.parent.stat().st_mode), 0o700)
            self.assertFalse(path.with_name(f".{path.name}.lock").exists())

    def test_manifest_rejects_symlink_parent_special_target_and_hardlink(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-manifest-") as tmp:
            root = Path(tmp)
            real_parent = root / "real"
            real_parent.mkdir()
            parent_link = root / "parent-link"
            parent_link.symlink_to(real_parent, target_is_directory=True)
            with self.assertRaises(ValueError):
                run_research(
                    manifest_path=parent_link / "research.json",
                    fixture=True,
                    fixture_count=130,
                    variants=("trend_pullback_v1",),
                    horizons_seconds=(60,),
                    bootstrap_iterations=1,
                )
            self.assertFalse((real_parent / "research.json").exists())

            existing_target = root / "existing.json"
            existing_target.write_bytes(b"target-bytes")
            final_link = root / "final-link.json"
            final_link.symlink_to(existing_target)
            with self.assertRaises(ValueError):
                run_research(
                    manifest_path=final_link,
                    fixture=True,
                    fixture_count=130,
                    variants=("trend_pullback_v1",),
                    horizons_seconds=(60,),
                    bootstrap_iterations=1,
                )
            self.assertEqual(existing_target.read_bytes(), b"target-bytes")

            fifo = root / "manifest.fifo"
            os.mkfifo(fifo)
            try:
                with self.assertRaises(ValueError):
                    validate_research(fifo)
            finally:
                fifo.unlink()

            source = root / "source.json"
            source.write_bytes(b"preserve")
            hardlink = root / "hardlink.json"
            os.link(source, hardlink)
            with self.assertRaises(ValueError):
                run_research(
                    manifest_path=hardlink,
                    fixture=True,
                    fixture_count=130,
                    variants=("trend_pullback_v1",),
                    horizons_seconds=(60,),
                    bootstrap_iterations=1,
                )
            self.assertEqual(source.read_bytes(), b"preserve")
            self.assertEqual(hardlink.read_bytes(), b"preserve")

            directory = root / "manifest-dir"
            directory.mkdir()
            with self.assertRaises(ValueError):
                run_research(
                    manifest_path=directory,
                    fixture=True,
                    fixture_count=130,
                    variants=("trend_pullback_v1",),
                    horizons_seconds=(60,),
                    bootstrap_iterations=1,
                )

    def test_real_capture_requires_explicit_spec_config_and_does_not_default_fixture_costs(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-manifest-") as tmp:
            capture_path = Path(tmp) / "capture.json"
            config = load_config("config/ctrader_pipeline_fixture.toml")
            capture = synthetic_cfd_capture(config=config)
            capture_path.write_text(
                json.dumps({"envelopes": [item.to_dict() for item in capture.envelopes]}),
                encoding="utf-8",
            )
            with self.assertRaises(ValueError):
                run_research(capture_path=capture_path, manifest_path=Path(tmp) / "real.json")
            with self.assertRaises(ValueError):
                run_research(
                    capture_path=capture_path,
                    manifest_path=Path(tmp) / "real-explicit.json",
                    config_path="config/ctrader_pipeline_fixture.toml",
                )

    def test_statistical_helpers_use_explicit_not_assessed_states(self) -> None:
        self.assertEqual(probabilistic_sharpe_ratio((1.0, 1.0))["status"], "NOT_ASSESSED")
        values = {
            "a": [0.1, -0.02, 0.03, 0.05, -0.01, 0.04],
            "b": [0.02, 0.01, -0.01, 0.02, 0.0, 0.01],
        }
        self.assertEqual(deflated_sharpe_ratio(values["a"], values)["status"], "ASSESSED")
        self.assertEqual(bootstrap_daily(values["a"], iterations=8, block_days=2)["status"], "ASSESSED")

    def test_compare_is_strict_about_product_identity(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-manifest-") as tmp:
            first = Path(tmp) / "one.json"
            second = Path(tmp) / "two.json"
            run_research(
                manifest_path=first,
                fixture=True,
                variants=("trend_pullback_v1",),
                bootstrap_iterations=4,
            )
            payload = json.loads(first.read_text(encoding="utf-8"))
            payload["product"] = "OTHER_PRODUCT"
            second.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaises(ValueError):
                compare_research((first, second), strict=True)

    def test_root_cli_adapter_contract(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-manifest-") as tmp:
            path = Path(tmp) / "command.json"
            code, result = research_command(
                SimpleNamespace(
                    research_action="run",
                    capture_path=None,
                    manifest_path=str(path),
                    fixture=True,
                    fixture_count=190,
                    config=None,
                    variants=None,
                    horizons_seconds=(60,),
                    seed=42,
                    bootstrap_iterations=4,
                    bootstrap_block_days=2,
                    order="as_observed",
                )
            )
            self.assertEqual(code, 0)
            self.assertEqual(result["state"], "COMPLETED")
            self.assertEqual(
                {item["variant"] for item in result["results"]},
                {
                    "trend_pullback_v1",
                    "donchian20_m5_v1",
                    "m1_trigger_reference",
                },
            )

    def test_execution_scenario_and_parameters_are_registered_with_trials(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-manifest-") as tmp:
            path = Path(tmp) / "ioc.json"
            result = run_research(
                manifest_path=path,
                fixture=True,
                variants=("trend_pullback_v1",),
                horizons_seconds=(60,),
                bootstrap_iterations=4,
                execution_model="ioc_partial",
                fill_fraction="0.25",
            )
            self.assertEqual(result["execution_model"]["scenario"], "ioc_partial")
            self.assertEqual(result["execution_model"]["fill_fraction"], "0.25")
            self.assertEqual(result["trials"][0]["execution_model"], "ioc_partial")
            self.assertEqual(result["trials"][0]["execution_parameters"]["effective_quantity"], "250.00")
            self.assertEqual(result["results"][0]["execution_model"]["scenario"], "ioc_partial")
            self.assertTrue(validate_research(path)["ok"])

    def test_hypotheses_are_preregistered_and_decisions_never_promote(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-manifest-") as tmp:
            result = run_research(
                manifest_path=Path(tmp) / "hypothesis.json",
                fixture=True,
                variants=("trend_pullback_v1", "m1_trigger_reference", "donchian20_m5_v1"),
                horizons_seconds=(60,),
                bootstrap_iterations=1,
            )
            self.assertEqual(result["hypotheses"]["trend_pullback_v1"]["role"], "BASELINE_FROZEN")
            self.assertEqual(result["hypotheses"]["m1_trigger_reference"]["role"], "DIAGNOSTIC_CONTROL")
            self.assertEqual(result["hypotheses"]["donchian20_m5_v1"]["role"], "DONCHIAN20_M5_IMPLEMENTED_CHALLENGER")
            self.assertTrue(all(item["decision"]["promote"] is False for item in result["trials"]))
            self.assertTrue(all(item["decision"]["promote"] is False for item in result["results"]))
            self.assertEqual(result["summary"]["decision_counts"], {"EVIDENCE_INSUFFICIENT": 3})


if __name__ == "__main__":
    unittest.main()
