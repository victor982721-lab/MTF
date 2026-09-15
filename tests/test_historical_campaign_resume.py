"""Regresiones offline para reanudar campañas históricas sin duplicar intentos."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from test_historical_backtest import manifest, quotes

from mtf_lab.data.historical import DatasetManifest
from tools import run_historical_campaign as campaign

START = "2016-03-07T05:00:00Z"
END = "2016-03-07T05:32:00Z"
PROJECT_ROOT = Path(__file__).resolve().parents[1]


class HistoricalCampaignResumeTests(unittest.TestCase):
    def _validation(self, data: DatasetManifest) -> dict[str, object]:
        selected = data
        return {
            "ok": True,
            "content_hash": selected.content_hash,
            "requested_start": START,
            "requested_end": END,
        }

    def _run_partial(self, root: Path, *, count: int = 32) -> dict[str, object]:
        dataset = manifest(root)
        fixture = quotes(count)
        validation = self._validation(dataset)
        with (
            patch.object(campaign, "load_validated_manifest", return_value=(dataset, validation, None)),
            patch.object(campaign, "iter_quotes", side_effect=lambda *_args: iter(fixture)),
        ):
            return campaign.run_historical_campaign(
                dataset,
                registry=root / "registry.jsonl",
                output_dir=root / "runs",
                start=START,
                end=END,
                checkpoint_after_quotes=5,
                code_root=PROJECT_ROOT,
            )

    def _resume(
        self,
        root: Path,
        partial: dict[str, object],
        *,
        by_checkpoint: bool = False,
        **kwargs: object,
    ) -> dict[str, object]:
        dataset = manifest(root)
        fixture = quotes(32)
        validation = self._validation(dataset)
        selector: dict[str, object] = (
            {"resume_checkpoint": Path(str(partial["checkpoint_path"]))}
            if by_checkpoint
            else {"resume_run_id": str(partial["run_id"])}
        )
        with (
            patch.object(campaign, "load_validated_manifest", return_value=(dataset, validation, None)),
            patch.object(campaign, "iter_quotes", side_effect=lambda *_args: iter(fixture)),
        ):
            return campaign.run_historical_campaign(
                dataset,
                registry=root / "registry.jsonl",
                output_dir=root / "runs",
                start=START,
                end=END,
                code_root=PROJECT_ROOT,
                **selector,
                **kwargs,
            )

    def test_partial_then_resume_run_id_reuses_attempts_and_receipts(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-campaign-resume-") as directory:
            root = Path(directory)
            partial = self._run_partial(root)
            self.assertEqual(partial["status"], "CHECKPOINTED")
            self.assertTrue(partial["resumable"])
            original_receipt = Path(str(partial["receipt_path"]))
            original_bytes = original_receipt.read_bytes()

            completed = self._resume(root, partial)

            self.assertEqual(completed["status"], "COMPLETED")
            self.assertEqual(completed["backtest"]["processed_quotes"], 32)
            self.assertTrue(completed["resume"]["requested"])
            self.assertEqual(original_receipt.read_bytes(), original_bytes)
            self.assertNotEqual(Path(str(completed["receipt_path"])), original_receipt)
            records = [json.loads(line) for line in (root / "registry.jsonl").read_text().splitlines()]
            self.assertEqual(
                sum(record.get("event") == "ATTEMPT_REGISTERED" for record in records),
                6,
            )
            self.assertEqual(
                sum(
                    record.get("event") == "ATTEMPT_STATUS" and record.get("status") == "COMPLETED"
                    for record in records
                ),
                6,
            )

    def test_resume_checkpoint_and_partial_resume_use_same_run(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-campaign-resume-checkpoint-") as directory:
            root = Path(directory)
            partial = self._run_partial(root)

            continued = self._resume(root, partial, by_checkpoint=True, stop_after_quotes=8)
            self.assertEqual(continued["status"], "CHECKPOINTED")
            self.assertEqual(continued["backtest"]["processed_quotes"], 8)

            finished = self._resume(root, partial, by_checkpoint=True)
            self.assertEqual(finished["status"], "COMPLETED")
            records = [json.loads(line) for line in (root / "registry.jsonl").read_text().splitlines()]
            self.assertEqual(
                sum(record.get("event") == "ATTEMPT_REGISTERED" for record in records),
                6,
            )

    def test_resume_rejects_mutated_runtime_and_offset_without_touching_artifacts(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-campaign-resume-identity-") as directory:
            root = Path(directory)
            partial = self._run_partial(root)
            run_directory = root / "runs" / str(partial["run_id"])
            receipt = Path(str(partial["receipt_path"]))
            receipt_bytes = receipt.read_bytes()
            request_path = run_directory / "inputs" / "request.json"
            request_bytes = request_path.read_bytes()
            request = json.loads(request_path.read_text())
            request["runtime_identity"] = {"python": "tampered"}
            request_path.write_text(json.dumps(request, sort_keys=True) + "\n")

            with self.assertRaisesRegex(campaign.HistoricalCampaignError, "resume runtime"):
                self._resume(root, partial)
            self.assertEqual(receipt.read_bytes(), receipt_bytes)

            # Restore only the request, then tamper the durable offset.  The
            # runner must reject it before the canonical writer can truncate.
            request_path.write_bytes(request_bytes)
            checkpoint_path = Path(str(partial["checkpoint_path"]))
            checkpoint = json.loads(checkpoint_path.read_text())
            checkpoint["artifact_offsets"]["ledger"] = 10**9
            checkpoint_path.write_text(json.dumps(checkpoint, sort_keys=True) + "\n")
            with self.assertRaisesRegex(campaign.HistoricalCampaignError, "offset"):
                self._resume(root, partial)

    def test_parser_makes_resume_selectors_mutually_exclusive(self) -> None:
        with self.assertRaises(SystemExit):
            campaign._parser().parse_args(["--resume-checkpoint", "a", "--resume-run-id", "b"])


if __name__ == "__main__":
    unittest.main()
