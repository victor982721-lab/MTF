from __future__ import annotations

import json
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

from mtf_lab.ops.backtest import BacktestRunner, VariantSpec
from mtf_lab.ops.importer import ColumnMapping, LocalImporter
from mtf_lab.ops.persistence import SQLiteStore
from mtf_lab.ops.query import QueryService
from mtf_lab.ops.reporting import ReportBuilder
from mtf_lab.ops.simulation import (
    DirectionalEvaluator,
    EvaluationSpec,
    Outcome,
    normalize_points,
    select_price_point,
)

BASE = datetime(2025, 1, 1, tzinfo=UTC)


def _point(
    offset: float,
    price: float,
    ordinal: int,
    *,
    available_offset: float | None = None,
    quality: str = "VALID",
    closed: bool = True,
    instrument: str = "TEST/USD",
    base: str = "close",
) -> dict[str, object]:
    timestamp = BASE + timedelta(seconds=offset)
    available = BASE + timedelta(seconds=available_offset if available_offset is not None else offset)
    return {
        "timestamp": timestamp.isoformat().replace("+00:00", "Z"),
        "available_at": available.isoformat().replace("+00:00", "Z"),
        "price": price,
        "price_base": base,
        "quality": quality,
        "closed": closed,
        "source_ordinal": ordinal,
        "instrument": instrument,
        "resolution": "M1",
    }


class SimulationRefactorTests(unittest.TestCase):
    def test_selector_keeps_causal_filters_and_reports_reasons(self) -> None:
        target = BASE + timedelta(seconds=10)
        points = [
            _point(11, 101, 1, available_offset=20, quality="STALE"),
            _point(12, 102, 2, quality="STALE"),
            _point(13, 103, 3, closed=False),
        ]
        selected, reason = select_price_point(
            normalize_points(points),
            target,
            rule="first_observation_at_or_after",
            requested_base_price="trade",
            max_price_age_seconds=5,
        )
        self.assertIsNone(selected)
        self.assertEqual(reason, "PRICE_NOT_AVAILABLE")

        selected, reason = select_price_point(
            normalize_points([_point(30, 100, 0)]),
            target,
            rule="first_observation_at_or_after",
            requested_base_price="close",
            max_price_age_seconds=5,
        )
        self.assertIsNone(selected)
        self.assertEqual(reason, "MAX_PRICE_AGE_EXCEEDED")
        with self.assertRaises(ValueError):
            select_price_point([], target, rule="unsupported")

    def test_evaluator_refactored_paths_preserve_pending_and_resolution(self) -> None:
        spec = EvaluationSpec(
            horizons_seconds=(60,),
            entry_latency_seconds=1,
            max_price_age_seconds=5,
            requested_base_price="close",
        )
        evaluator = DirectionalEvaluator(spec)
        invalid = evaluator.evaluate({"signal_id": "invalid", "detected_ts": BASE, "direction": "SIDEWAYS"}, [])
        self.assertEqual(invalid.outcome, Outcome.INDETERMINATE)
        pending = evaluator.evaluate(
            {"signal_id": "empty", "detected_ts": BASE, "direction": "UP"},
            [],
            data_complete=False,
        )
        self.assertEqual(pending.outcome, Outcome.PENDING)
        mismatch = evaluator.evaluate(
            {"signal_id": "mismatch", "detected_ts": BASE, "direction": "UP", "instrument": "OTHER/USD"},
            [_point(2, 100, 0), _point(62, 101, 1)],
        )
        self.assertEqual(mismatch.reason, "INSTRUMENT_MISMATCH")
        missing_entry = evaluator.evaluate(
            {"signal_id": "late", "detected_ts": BASE, "direction": "UP"},
            [_point(30, 100, 0)],
        )
        self.assertEqual(missing_entry.reason, "MAX_PRICE_AGE_EXCEEDED")
        open_pending = evaluator.evaluate(
            {"signal_id": "open", "detected_ts": BASE, "direction": "UP"},
            [_point(2, 100, 0), _point(62, 101, 1)],
            data_complete=False,
            as_of=BASE + timedelta(seconds=63),
        )
        self.assertEqual(open_pending.reason, "FINAL_PRICE_NOT_YET_DUE")
        resolved = evaluator.evaluate(
            {"signal_id": "resolved", "detected_ts": BASE, "direction": "UP"},
            [_point(2, 100, 0), _point(62, 101, 1)],
        )
        self.assertEqual(resolved.outcome, Outcome.WIN)


class ImporterRefactorTests(unittest.TestCase):
    def test_csv_permissive_mode_collects_order_and_duplicate_issues(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "candles.csv"
            path.write_text(
                "timestamp,open,high,low,close\n"
                "2025-01-01T00:01:00Z,2,3,1,2.5\n"
                "2025-01-01T00:00:00Z,1,2,0,1.5\n"
                "2025-01-01T00:00:00Z,1,2,0,1.5\n",
                encoding="utf-8",
            )
            result = LocalImporter(
                instrument="TEST/USD",
                timeframe="M1",
                price_base="close",
                strict=False,
                allow_out_of_order=True,
                allow_duplicates=True,
            ).read(path)
        self.assertEqual(len(result.records), 3)
        self.assertEqual(result.coverage_start, "2025-01-01T00:00:00.000000Z")
        self.assertEqual(result.coverage_end, "2025-01-01T00:02:00.000000Z")
        self.assertEqual({issue.code for issue in result.issues}, {"OUT_OF_ORDER", "DUPLICATE"})

    def test_jsonl_event_mapping_and_numeric_timestamp_unit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "events.jsonl"
            path.write_text(
                json.dumps({"kind": "trade", "event_ts": "2025-01-01T00:00:00Z", "id": "t1", "price": 10}) + "\n",
                encoding="utf-8",
            )
            mapping = ColumnMapping(
                timestamp="timestamp",
                event_timestamp="event_ts",
                open=None,
                high=None,
                low=None,
                close=None,
                volume=None,
                price="price",
                event_id="id",
            )
            result = LocalImporter(
                instrument="TEST/USD",
                timeframe="M1",
                price_base="trade",
                mapping=mapping,
            ).read(path)
            self.assertEqual(result.records[0]["event_id"], "t1")
            self.assertEqual(result.records[0]["price"], 10.0)

            numeric = Path(tmp) / "numeric.json"
            numeric.write_text(
                json.dumps([{"timestamp": 1735689600000, "price": 11, "id": "t2"}]),
                encoding="utf-8",
            )
            numeric_result = LocalImporter(
                instrument="TEST/USD",
                timeframe="M1",
                price_base="trade",
                mapping=ColumnMapping(
                    open=None,
                    high=None,
                    low=None,
                    close=None,
                    volume=None,
                    event_id="id",
                ),
                timestamp_unit="ms",
            ).read(numeric)
            self.assertEqual(numeric_result.format, "json")
            self.assertEqual(numeric_result.records[0]["price"], 11.0)


class BacktestRefactorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spec = EvaluationSpec(horizons_seconds=(60,), entry_latency_seconds=1, max_price_age_seconds=120)
        self.points = [_point(offset, 100 + offset / 100, index) for index, offset in enumerate((2, 12, 62, 72))]
        self.signals = [
            {"signal_id": "s1", "detected_ts": BASE.isoformat(), "direction": "UP"},
            {"signal_id": "s2", "detected_ts": (BASE + timedelta(seconds=10)).isoformat(), "direction": "UP"},
        ]

    def test_variants_and_engine_adapter_use_one_simulation_path(self) -> None:
        runner = BacktestRunner(spec=self.spec)
        results = runner.run(
            self.signals,
            self.points,
            variants=[
                VariantSpec("all", "all"),
                VariantSpec("first", "first", signal_filter=lambda row: row["signal_id"] == "s1"),
            ],
            persist=False,
        )
        self.assertEqual([result.variant for result in results], ["all", "first"])
        self.assertEqual(results[0].signal_count, 2)
        self.assertEqual(results[1].signal_count, 1)
        self.assertEqual(len(results[0].simulations), 2)

        class Engine:
            def update(self, event: dict[str, object]) -> list[dict[str, object]]:
                return [cast(dict[str, object], event["signal"])]

        engine_results = runner.run_from_engine(
            [{"signal": self.signals[0]}], Engine(), self.points, persist_decisions=False
        )
        self.assertEqual(engine_results[0].signal_count, 1)

    def test_portfolio_settles_at_expiry_and_skips_overlap(self) -> None:
        portfolio = BacktestRunner(spec=self.spec).run_portfolio(
            self.signals,
            self.points,
            max_positions=1,
        )
        self.assertEqual(portfolio.accepted_count, 1)
        self.assertEqual(portfolio.skipped_overlap, 1)
        self.assertGreater(portfolio.balance, 0)

    def test_persistence_and_report_merge_are_identity_based(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, SQLiteStore(Path(tmp) / "ops.sqlite3") as store:
            session_id = store.create_session(
                mode="BACKTEST", provider="fixture", instrument="TEST/USD", config={"strategy": "test"}
            )
            runner = BacktestRunner(spec=self.spec, store=store, session_id=session_id)
            result = runner.run(self.signals[:1], self.points)[0]
            self.assertEqual(len(store.list_simulations(session_id)), 1)
            data = ReportBuilder(store, session_id).summary(results=[result, result])
            self.assertEqual(data["counts"]["simulations"], 1)


class QueryAndReportingRefactorTests(unittest.TestCase):
    def test_decoders_and_read_only_summary_without_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, SQLiteStore(Path(tmp) / "query.sqlite3") as store:
            session_id = store.create_session(mode="REPLAY", provider="fixture", instrument="TEST/USD")
            query = QueryService(store)
            decoded = query._decode_row(
                "cfd_trades",
                {
                    "state": "PENDING",
                    "terminal": 0,
                    "close_observed": 0,
                    "net_pnl": None,
                    "lineage_json": "{}",
                    "payload_json": "{}",
                },
            )
            self.assertEqual(decoded["economic_state"], "NOT_SETTLED")
            envelope = query._decode_row(
                "capture_envelopes",
                {"envelope_json": json.dumps({"payload": {"x": 1}, "message_class": "QUOTE"})},
            )
            self.assertEqual(envelope["payload"], {"x": 1})
            self.assertEqual(envelope["message_class"], "QUOTE")
            self.assertEqual(query.snapshot(session_id)["schema_version"], 4)

        evaluator = DirectionalEvaluator(EvaluationSpec(horizons_seconds=(60,), max_price_age_seconds=120))
        backtest_result = BacktestRunner(spec=evaluator.spec).run(
            [{"signal_id": "s", "detected_ts": BASE, "direction": "UP"}],
            [_point(2, 100, 0), _point(62, 101, 1)],
        )[0]
        report = ReportBuilder().summary(
            session={"mode": "REPLAY", "provider": "fixture", "instrument": "TEST/USD"},
            results=[backtest_result],
        )
        self.assertEqual(report["counts"]["simulations"], 1)
        self.assertEqual(report["aggregate"]["outcomes"]["WIN"], 1)

    def test_report_writers_keep_explicit_formats(self) -> None:
        builder = ReportBuilder()
        data = builder.summary(session={"mode": "REPLAY", "instrument": "TEST/USD"})
        with tempfile.TemporaryDirectory() as tmp:
            json_path = builder.write(Path(tmp) / "report.json", format="json", data=data)
            html_path = builder.write(Path(tmp) / "report.html", format="html", data=data)
            self.assertEqual(json.loads(json_path.read_text(encoding="utf-8"))["schema"], "mtf-lab.report.v1")
            self.assertIn("<!doctype html>", html_path.read_text(encoding="utf-8"))
            with self.assertRaises(ValueError):
                builder.write(Path(tmp) / "report.txt", format="text", data=data)


if __name__ == "__main__":
    unittest.main()
