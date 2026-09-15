"""Consistent result flags for the cTrader -> local PAPER pipeline."""

from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from mtf_lab.data.ctrader import CTraderInstrumentSpec
from mtf_lab.ops.ctrader_pipeline import CTraderPipeline, synthetic_ctrader_capture
from mtf_lab.ops.persistence import SQLiteStore
from tests.test_ctrader_pipeline import BASE, pipeline_config


class CTraderPipelineResultConsistencyTests(unittest.TestCase):
    def test_complete_pipeline_result_marks_paper_finished_and_complete(self) -> None:
        capture = synthetic_ctrader_capture(start=BASE, symbol_id=99, count=190)
        with TemporaryDirectory() as tmp, SQLiteStore(Path(tmp) / "pipeline.sqlite3") as store:
            result = CTraderPipeline(
                store, pipeline_config(), spec=CTraderInstrumentSpec(symbol="EUR/USD", symbol_id=99)
            ).run(capture, session_id="complete")

            self.assertTrue(result.paper.capture_complete)
            self.assertTrue(result.paper.finished)
            paper_view = result.paper.to_dict()
            self.assertTrue(paper_view["capture_complete"])
            self.assertTrue(paper_view["finished"])
            self.assertTrue(result.snapshot["paper"]["finished"])
            self.assertTrue(result.snapshot["paper"]["capture_complete"])

    def test_unfinished_pipeline_cannot_report_complete_paper(self) -> None:
        capture = synthetic_ctrader_capture(start=BASE, symbol_id=99, count=190)
        with TemporaryDirectory() as tmp, SQLiteStore(Path(tmp) / "pipeline.sqlite3") as store:
            result = CTraderPipeline(
                store, pipeline_config(), spec=CTraderInstrumentSpec(symbol="EUR/USD", symbol_id=99)
            ).run(capture, session_id="unfinished", finish_session=False)

            self.assertFalse(result.paper.capture_complete)
            self.assertFalse(result.paper.finished)
            self.assertFalse(result.paper.to_dict()["capture_complete"])
            self.assertFalse(result.paper.to_dict()["finished"])


if __name__ == "__main__":
    unittest.main()
