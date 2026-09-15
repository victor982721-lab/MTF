"""Regresiones offline para el preflight WF del runner histórico.

El runner canónico todavía consume un único ``DatasetManifest``.  Estas
pruebas fijan el comportamiento seguro mientras se termina el consumidor
warmup→WF: los contratos se pueden enlazar sin leer raws, pero ``stage``
walk-forward no crea una corrida ni registra intentos.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mtf_lab.data.walk_forward import WalkForwardInput
from tests.test_walk_forward_contract import WalkForwardFixtureMixin
from tools import run_historical_campaign as campaign


class HistoricalCampaignWalkForwardContractTests(WalkForwardFixtureMixin, unittest.TestCase):
    def test_binding_is_side_effect_free_and_uses_the_causal_guard(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-campaign-wf-binding-") as directory:
            root = Path(directory)
            development = self.make_development(root)
            walk_forward = self.make_wf(root)
            input_contract = self.make_input(development, walk_forward)

            with patch.object(
                campaign,
                "run_historical_backtest",
                side_effect=AssertionError("no se debe abrir el backtest"),
            ):
                binding = campaign.bind_walk_forward_contract(
                    development,
                    walk_forward,
                    input_contract=input_contract,
                    window="WF_2020",
                )

            self.assertEqual(binding.window.window_id, "WF_2020")
            self.assertEqual(binding.input_contract.identity_hash, input_contract.identity_hash)
            self.assertEqual(binding.guard.manifest_hashes["development"], development.content_hash)
            self.assertEqual(binding.guard.manifest_hashes["walk-forward"], walk_forward.content_hash)
            self.assertEqual(binding.to_dict()["holdout"], "CLOSED")

    def test_walk_forward_stage_fails_closed_before_output_registry_or_source(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-campaign-wf-closed-") as directory:
            root = Path(directory)
            development = self.make_development(root)
            walk_forward = self.make_wf(root)
            input_contract = self.make_input(development, walk_forward)
            registry = root / "registry.jsonl"
            output = root / "runs"

            with (
                patch.object(campaign, "iter_quotes", side_effect=AssertionError("no source read")),
                patch.object(campaign, "GlobalTrialRegistry", side_effect=AssertionError("no registry open")),
                self.assertRaisesRegex(campaign.HistoricalCampaignError, "WARMUP_ONLY.*WF"),
            ):
                campaign.run_historical_campaign(
                    development,
                    registry=registry,
                    output_dir=output,
                    start="2020-01-01T00:00:00Z",
                    end="2021-01-01T00:00:00Z",
                    stage="walk-forward",
                    walk_forward_manifest=walk_forward,
                    walk_forward_input=input_contract,
                    window="WF_2020",
                )

            self.assertFalse(registry.exists())
            self.assertFalse(output.exists())

    def test_walk_forward_input_mismatch_is_rejected_without_creating_artifacts(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-campaign-wf-mismatch-") as directory:
            root = Path(directory)
            development = self.make_development(root)
            walk_forward = self.make_wf(root)
            input_contract = self.make_input(development, walk_forward)
            raw = input_contract.to_dict()
            raw["development_manifest_hash"] = "0" * 64

            with self.assertRaisesRegex(campaign.HistoricalCampaignError, "input_hash"):
                campaign.run_historical_campaign(
                    development,
                    registry=root / "registry.jsonl",
                    output_dir=root / "runs",
                    start="2020-01-01T00:00:00Z",
                    end="2021-01-01T00:00:00Z",
                    stage="walk-forward",
                    walk_forward_manifest=walk_forward,
                    walk_forward_input=raw,
                )
            self.assertFalse((root / "registry.jsonl").exists())
            self.assertFalse((root / "runs").exists())

    def test_plain_development_stage_rejects_wf_contract_arguments(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-campaign-wf-scope-") as directory:
            root = Path(directory)
            development = self.make_development(root)
            walk_forward = self.make_wf(root)
            input_contract: WalkForwardInput = self.make_input(development, walk_forward)
            with self.assertRaisesRegex(campaign.HistoricalCampaignError, "sólo aplica"):
                campaign.run_historical_campaign(
                    development,
                    registry=root / "registry.jsonl",
                    output_dir=root / "runs",
                    start="2019-01-01T00:00:00Z",
                    end="2020-01-01T00:00:00Z",
                    stage="development",
                    walk_forward_manifest=walk_forward,
                    walk_forward_input=input_contract,
                )
            self.assertFalse((root / "registry.jsonl").exists())
            self.assertFalse((root / "runs").exists())


if __name__ == "__main__":
    unittest.main()
