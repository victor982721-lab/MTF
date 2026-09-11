"""One application algorithm across whole replay, batches and durable restart."""

import random
import unittest
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from mtf_lab.data.capture import CaptureContractError
from mtf_lab.data.ctrader import CTraderInstrumentSpec
from mtf_lab.ops.ctrader_pipeline import CaptureCoverage, CTraderPipeline, synthetic_ctrader_capture
from mtf_lab.ops.persistence import SQLiteStore
from tests.test_ctrader_pipeline import BASE, pipeline_config


class PaperSessionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.capture = synthetic_ctrader_capture(start=BASE, symbol_id=99, count=190)

    def pipeline(self, store):
        return CTraderPipeline(store, pipeline_config(), spec=CTraderInstrumentSpec(symbol_id=99), max_candles=256)

    def session(self, pipeline, name):
        return pipeline.open_session(
            dataset_id=self.capture.capture_hash,
            session_id=name,
            coverage=self.capture.coverage,
            provenance=self.capture.provenance,
        )

    def test_batches_and_restart_mid_position_match_complete_replay(self):
        with TemporaryDirectory() as tmp:
            with SQLiteStore(Path(tmp) / "whole.sqlite3") as store:
                whole = self.pipeline(store).run(self.capture, session_id="whole")
            db = Path(tmp) / "chunks.sqlite3"
            with SQLiteStore(db) as store:
                pipeline = self.pipeline(store)
                session = self.session(pipeline, "chunks")
                session.ingest_many(self.capture.envelopes[:128], chunk_size=17)
                self.assertTrue(any(trade.state.value == "FILLED" for trade in session.simulator.trades))
                snapshot = session.snapshot()
                self.assertEqual(snapshot["stats"]["envelopes"], 128)
            with SQLiteStore(db) as store:
                session = self.session(self.pipeline(store), "chunks")
                self.assertEqual(session.snapshot()["snapshot_hash"], snapshot["snapshot_hash"])
                # Seed retained so a failed partition property is reproducible.
                randomizer = random.Random(7321)
                index = 128
                while index < len(self.capture.envelopes):
                    size = randomizer.randint(1, 19)
                    session.ingest_many(self.capture.envelopes[index : index + size], chunk_size=size)
                    index += size
                chunked = session.finish()
                self.assertEqual(
                    [item.as_dict() for item in chunked.signals], [item.as_dict() for item in whole.signals]
                )
                self.assertEqual([item.to_dict() for item in chunked.trades], [item.to_dict() for item in whole.trades])
                self.assertEqual(chunked.snapshot_hash, whole.snapshot_hash)
                rows = store.list_cfd_trades("chunks", chunked.paper_analysis_id)
                self.assertEqual(len(rows), len(chunked.trades))
                self.assertEqual(store.list_simulations("chunks"), [])

    def test_repeated_committed_suffix_is_noop_and_cannot_resurrect(self):
        with TemporaryDirectory() as tmp, SQLiteStore(Path(tmp) / "paper.sqlite3") as store:
            session = self.session(self.pipeline(store), "repeat")
            session.ingest_many(self.capture.envelopes[:128])
            old = session.snapshot()
            session.ingest_many(self.capture.envelopes[128:])
            before = session.finish()
            session.ingest_many(self.capture.envelopes[-20:], chunk_size=3)
            after = session.result()
            self.assertEqual(before.snapshot_hash, after.snapshot_hash)
            with self.assertRaisesRegex(CaptureContractError, "rewind"):
                session.restore(old)

    def test_batch_failure_rolls_back_both_durable_and_memory_state(self):
        with TemporaryDirectory() as tmp, SQLiteStore(Path(tmp) / "paper.sqlite3") as store:
            session = self.session(self.pipeline(store), "atomic")
            session.ingest_many(self.capture.envelopes[:10])
            before = session.snapshot()
            original = store.save_checkpoint

            def fail_pipeline(*args, **kwargs):
                if args[1] == "pipeline":
                    raise OSError("controlled checkpoint failure")
                return original(*args, **kwargs)

            with (
                patch.object(store, "save_checkpoint", side_effect=fail_pipeline),
                self.assertRaisesRegex(OSError, "controlled"),
            ):
                session.ingest_many(self.capture.envelopes[10:13])
            self.assertEqual(session.snapshot()["snapshot_hash"], before["snapshot_hash"])
            self.assertEqual(len(tuple(store.iter_capture_envelopes("atomic"))), 10)
            session.ingest_many(self.capture.envelopes[10:13])
            self.assertEqual(len(tuple(store.iter_capture_envelopes("atomic"))), 13)

    def test_incomplete_request_does_not_finalize_despite_fixture_end_marker(self):
        with TemporaryDirectory() as tmp, SQLiteStore(Path(tmp) / "paper.sqlite3") as store:
            result = self.pipeline(store).run(self.capture, session_id="incomplete", capture_complete=False)
            self.assertFalse(result.paper.capture_complete)
            self.assertFalse(result.snapshot["finished"])

    def test_unprocessed_session_has_no_observed_coverage_or_fake_content_hash(self):
        with TemporaryDirectory() as tmp, SQLiteStore(Path(tmp) / "paper.sqlite3") as store:
            session = self.session(self.pipeline(store), "empty")
            self.assertIsNone(session.coverage.observed_start)
            self.assertFalse(session.coverage.complete)
            self.assertEqual(len(session.capture_hash), 64)
            self.assertNotEqual(session.capture_hash, session.dataset_id)

    def test_evicted_terminal_signal_resolves_from_durable_product_log(self):
        with TemporaryDirectory() as tmp, SQLiteStore(Path(tmp) / "paper.sqlite3") as store:
            pipeline = self.pipeline(store)
            pipeline.cfd_config = replace(pipeline.cfd_config, terminal_retention=1)
            session = self.session(pipeline, "evicted")
            session.ingest_many(self.capture.envelopes)
            result = session.finish(finish_session=False)
            self.assertEqual(len(session.simulator.trades), 1)
            self.assertGreater(len(store.list_cfd_trades("evicted", session.paper_analysis_id)), 1)
            resolved = session.simulator.submit_all(result.cfd_signals[0])
            self.assertTrue(all(trade.state.value == "CLOSED" for trade in resolved))
            self.assertEqual(len(session.simulator.trades), 1)

    def test_unknown_continuity_is_inspectable_not_admitted_as_definitive_paper(self):
        with TemporaryDirectory() as tmp, SQLiteStore(Path(tmp) / "paper.sqlite3") as store:
            session = self.pipeline(store).open_session(
                dataset_id="inspect", session_id="inspect", coverage=CaptureCoverage()
            )
            session.ingest_many(self.capture.envelopes[:-1])
            result = session.finish()
            self.assertTrue(result.signals)
            self.assertEqual(result.trades, ())
            self.assertFalse(result.paper.capture_complete)
            self.assertEqual(result.capture.coverage.continuity, "UNKNOWN")

    def test_clock_is_a_durable_event_and_checkpoint_integrity_is_enforced(self):
        with TemporaryDirectory() as tmp, SQLiteStore(Path(tmp) / "paper.sqlite3") as store:
            session = self.session(self.pipeline(store), "clock")
            session.ingest_many(self.capture.envelopes[:3])
            session.advance(self.capture.envelopes[2].available_at + timedelta(seconds=10))
            last = tuple(store.iter_capture_envelopes("clock"))[-1]
            self.assertEqual(last["message_class"], "clock")
            tampered = dict(session.snapshot())
            tampered["finished"] = True
            with self.assertRaisesRegex(CaptureContractError, "integrity"):
                session.restore(tampered)

    def test_live_local_controls_separate_socket_liveness_and_quote_freshness(self):
        from mtf_lab.data.capture import CaptureEnvelope, MessageClass

        with TemporaryDirectory() as tmp, SQLiteStore(Path(tmp) / "paper.sqlite3") as store:
            pipeline = CTraderPipeline(store, pipeline_config(), spec=CTraderInstrumentSpec(symbol_id=99), mode="LIVE")
            session = pipeline.open_session(dataset_id="local-live-controls", session_id="local-live-controls")
            for sequence, state in enumerate(("CONNECTED", "RECONCILED")):
                when = BASE + timedelta(seconds=sequence)
                session.ingest(
                    CaptureEnvelope(when, when, when, sequence, 1, MessageClass.CONNECTION, {"state": state})
                )
            for sequence in (2, 3):
                when = BASE + timedelta(seconds=sequence)
                session.ingest(
                    CaptureEnvelope(
                        when,
                        when,
                        when,
                        sequence,
                        1,
                        MessageClass.SPOT,
                        {"symbolId": 99, "timestamp": int(when.timestamp() * 1000), "bid": 110000, "ask": 110020},
                    )
                )
            self.assertEqual(session.coordinator.status().freshness_state, "FRESH")
            later = BASE + timedelta(seconds=183)
            session.ingest(CaptureEnvelope(later, later, later, 4, 1, MessageClass.CLOCK, {"clock_kind": "heartbeat"}))
            status = session.coordinator.status()
            self.assertEqual(status.connection, "CONNECTED")
            self.assertEqual(status.freshness_state, "STALE")
            self.assertFalse(status.analysis_enabled)
            self.assertIsNotNone(status.last_heartbeat_at)

    def test_crossed_quote_is_retained_but_never_sent_to_fill(self):
        from mtf_lab.data.capture import CaptureEnvelope, MessageClass

        with TemporaryDirectory() as tmp, SQLiteStore(Path(tmp) / "paper.sqlite3") as store:
            session = self.session(self.pipeline(store), "crossed")
            wire = CaptureEnvelope(
                BASE,
                BASE,
                BASE,
                0,
                0,
                MessageClass.SPOT,
                {"timestamp": int(BASE.timestamp() * 1000), "symbolId": 99, "bid": 110020, "ask": 110000},
            )
            session.ingest(wire)
            self.assertEqual(len(tuple(store.iter_capture_envelopes("crossed"))), 1)
            self.assertEqual(session.simulator.trades, ())
            self.assertEqual(session.snapshot()["stats"]["quote_rejections"], 1)
            self.assertIn("cfd_quote_rejected:CROSSED_QUOTE", session.snapshot()["capture"]["issues"])

    def test_late_availability_cannot_change_an_earlier_detector_decision(self):
        from mtf_lab.ops.ctrader_capture import normalize_ctrader_capture

        with TemporaryDirectory() as tmp:
            with SQLiteStore(Path(tmp) / "baseline.sqlite3") as store:
                baseline = self.pipeline(store).run(self.capture, session_id="baseline")
            entry_id = baseline.trades[0].entry_quote_id
            index = next(i for i, item in enumerate(self.capture.envelopes) if item.observation_id == entry_id)
            delayed_rows = list(self.capture.envelopes)
            delayed_rows[index] = replace(
                delayed_rows[index], available_at=delayed_rows[index].available_at + timedelta(seconds=180)
            )
            delayed_capture = normalize_ctrader_capture(
                delayed_rows, spec=CTraderInstrumentSpec(symbol_id=99), coverage=self.capture.coverage
            )
            with SQLiteStore(Path(tmp) / "delayed.sqlite3") as store:
                delayed = self.pipeline(store).run(delayed_capture, session_id="delayed")
            # The decision before this update is immutable. Eligibility of the
            # subsequent entry changes, and no retrospective fill is fabricated.
            self.assertEqual(baseline.signals[0].as_dict(), delayed.signals[0].as_dict())
            self.assertEqual(baseline.trades[0].state.value, "CLOSED")
            self.assertEqual(delayed.trades[0].state.value, "UNKNOWN")
            self.assertIsNone(delayed.trades[0].entry_price)

    def test_clock_sequence_uses_global_max_not_last_availability_tie(self):
        from mtf_lab.data.capture import CaptureEnvelope, MessageClass

        with TemporaryDirectory() as tmp, SQLiteStore(Path(tmp) / "clock.sqlite3") as store:
            session = self.pipeline(store).open_session(dataset_id="sequence", session_id="sequence")
            for sequence, receipt, available in ((8, 1, 2), (3, 0, 3)):
                when = BASE + timedelta(seconds=receipt)
                usable = BASE + timedelta(seconds=available)
                session.ingest(CaptureEnvelope(when, when, usable, sequence, 0, MessageClass.CLOCK, {}))
            session.advance(BASE + timedelta(seconds=4))
            rows = tuple(store.iter_capture_envelopes("sequence"))
            self.assertEqual(rows[-1]["ingest_sequence"], 9)

    def test_new_generation_blocks_product_until_explicit_reconciliation(self):
        from mtf_lab.data.capture import CaptureEnvelope, MessageClass

        with TemporaryDirectory() as tmp, SQLiteStore(Path(tmp) / "generation.sqlite3") as store:
            session = self.session(self.pipeline(store), "generation")
            session.ingest_many(self.capture.envelopes[:128])
            self.assertTrue(any(trade.state.value == "FILLED" for trade in session.simulator.trades))
            session.ingest(replace(self.capture.envelopes[128], connection_generation=1))
            self.assertFalse(session.snapshot()["paper"]["connected"])
            self.assertEqual(session.coverage.continuity, "DISCONTINUOUS")
            seed = session.reader_resume_arguments()
            self.assertEqual(seed, {"next_ingest_sequence": 129, "initial_generation": 1})
            at = self.capture.envelopes[128].available_at + timedelta(seconds=1)
            session.ingest(CaptureEnvelope(at, at, at, 129, 1, MessageClass.CONNECTION, {"state": "RECONCILED"}))
            self.assertTrue(session.snapshot()["paper"]["connected"])
            self.assertEqual(session.coverage.continuity, "CONTINUOUS")

    def test_cfd_session_never_constructs_binary_consumer(self):
        with (
            TemporaryDirectory() as tmp,
            SQLiteStore(Path(tmp) / "paper.sqlite3") as store,
            patch(
                "mtf_lab.runtime.consumers.BinarySimulationConsumer.__init__",
                side_effect=AssertionError("binary product constructed"),
            ),
        ):
            session = self.session(self.pipeline(store), "no-binary")
            session.ingest_many(self.capture.envelopes[:130])
            self.assertEqual(session.coordinator.processor.status["consumer_type"], "cfd_signal")
            self.assertEqual(store.list_simulations("no-binary"), [])
