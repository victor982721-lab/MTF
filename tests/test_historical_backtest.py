"""Pruebas focales del runner histórico acotado y su reanudación."""

from __future__ import annotations

import hashlib
import tempfile
import unittest
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from mtf_lab.core import IndicatorPoint, OperationMode, Signal
from mtf_lab.core.quality import DataQuality
from mtf_lab.core.risk_exit import RiskExitPolicy, evaluate_exit, plan_entry
from mtf_lab.data.historical import DatasetManifest, DatasetPartition, HistoricalQuote, SourceLocator
from mtf_lab.ops.historical_assumptions import VIRTUAL_EURUSD_10K_MODEL_ID, historical_assumptions_for
from mtf_lab.ops.historical_backtest import (
    BLOCK_SIZE,
    HistoricalBacktestConfig,
    HistoricalBacktestError,
    _ArtifactWriter,
    _emit_risk_updates,
    _gross_equity_values,
    _handle_signal,
    _historical_quote_to_quote,
    _profile_state,
    _risk_quote_for_state,
    run_historical_backtest,
)
from mtf_lab.ops.market_protocol import MarketProtocolError, ResearchProtocol

BASE = datetime(2016, 3, 7, 5, tzinfo=UTC)


def manifest(root: Path) -> DatasetManifest:
    raw_digest = "a" * 64
    partition = DatasetPartition(
        partition_id="201603",
        raw_archive="raw/fixture.zip",
        raw_sha256=raw_digest,
        raw_size=1,
        members=("quotes.csv",),
        source_uri="local-fixture",
        terms_uri="local-fixture",
        acquired_at=datetime(2026, 9, 13, tzinfo=UTC),
    )
    return DatasetManifest(
        dataset_id=f"histdata:EUR/USD:201603:{raw_digest[:32]}",
        provider="histdata",
        instrument="EUR/USD",
        partitions=(partition,),
        coverage_start=None,
        coverage_end=None,
        content_hash=hashlib.sha256(b"201603\0" + raw_digest.encode() + b"\0" + b"1\0quotes.csv").hexdigest(),
        data_root=str(root),
    )


def quotes(count: int, *, start: datetime = BASE) -> tuple[HistoricalQuote, ...]:
    digest = "a" * 64
    return tuple(
        HistoricalQuote(
            instrument="EUR/USD",
            event_time=start + timedelta(minutes=index),
            available_at=start + timedelta(minutes=index),
            bid=Decimal("1.10000") + Decimal(index) / Decimal("1000000"),
            ask=Decimal("1.10020") + Decimal(index) / Decimal("1000000"),
            locator=SourceLocator("raw/fixture.zip", "quotes.csv", digest, index, index + 1),
            sequence=index,
            source_event_id=f"{digest}:quotes.csv:{index + 1}:{index}",
        )
        for index in range(count)
    )


def quote_at(index: int, at: datetime, *, bid: str = "1.10000", ask: str = "1.10020") -> HistoricalQuote:
    digest = "a" * 64
    return HistoricalQuote(
        instrument="EUR/USD",
        event_time=at,
        available_at=at,
        bid=Decimal(bid),
        ask=Decimal(ask),
        locator=SourceLocator("raw/fixture.zip", "quotes.csv", digest, index, index + 1),
        sequence=index,
        source_event_id=f"{digest}:quotes.csv:{index + 1}:{index}",
    )


class HistoricalBacktestTests(unittest.TestCase):
    def test_config_is_derived_from_closed_protocol(self) -> None:
        config = HistoricalBacktestConfig.from_protocol(ResearchProtocol.default())
        self.assertEqual(config.block_size, BLOCK_SIZE)
        self.assertEqual(len(config.config_hash), 64)
        self.assertEqual(tuple(item.candidate_id for item in config.profiles), ResearchProtocol.default().candidate_ids)
        self.assertTrue(config.to_dict()["periods"]["holdout_locked"])

        with self.assertRaises(MarketProtocolError):
            HistoricalBacktestConfig.from_protocol(
                ResearchProtocol(holdout_locked=False),
            )

    def test_scenario_and_candidate_selection_are_part_of_the_config_identity(self) -> None:
        protocol = ResearchProtocol.default()
        base = HistoricalBacktestConfig.from_protocol(protocol, scenario="base")
        adverse = HistoricalBacktestConfig.from_protocol(protocol, scenario="adverse")
        extreme = HistoricalBacktestConfig.from_protocol(protocol, scenario="extreme")
        selected = HistoricalBacktestConfig.from_protocol(
            protocol,
            scenario="adverse",
            candidates=("tp_fast_v1", "dc_m5_v1"),
        )
        self.assertEqual(base.decision_latency_seconds, Decimal("5"))
        self.assertEqual(base.slippage_pips, Decimal("0.1"))
        self.assertEqual(base.spread_multiplier, Decimal("1"))
        self.assertEqual(adverse.decision_latency_seconds, Decimal("10"))
        self.assertEqual(adverse.slippage_pips, Decimal("0.2"))
        self.assertEqual(adverse.spread_multiplier, Decimal("1.5"))
        self.assertEqual(base.scenario_mode, "VIRTUAL_DIAGNOSTIC")
        self.assertEqual(adverse.scenario_mode, "VIRTUAL_DIAGNOSTIC")
        self.assertEqual(extreme.scenario_mode, "VIRTUAL_DIAGNOSTIC")
        self.assertEqual(adverse.risk_policy.exit_latency_seconds, Decimal("10"))
        self.assertNotEqual(base.config_hash, adverse.config_hash)
        self.assertNotEqual(adverse.config_hash, selected.config_hash)
        self.assertEqual(selected.candidate_ids, ("tp_fast_v1", "dc_m5_v1"))
        reordered = HistoricalBacktestConfig.from_protocol(
            protocol,
            candidates=("dc_m5_v1", "tp_fast_v1"),
        )
        self.assertEqual(reordered.candidate_ids, ("dc_m5_v1", "tp_fast_v1"))

    def test_explicit_assumption_model_projects_dynamic_calendar_and_contract(self) -> None:
        assumptions = historical_assumptions_for(VIRTUAL_EURUSD_10K_MODEL_ID)
        config = HistoricalBacktestConfig.from_protocol(
            ResearchProtocol.default(),
            candidates=("tp_fast_v1",),
            assumptions_model=assumptions,
        )
        self.assertEqual(config.assumptions_model_id, VIRTUAL_EURUSD_10K_MODEL_ID)
        self.assertEqual(config.contract_spec["unit_value"], Decimal("1"))
        self.assertIsNone(config.contract_spec["margin_per_unit"])
        self.assertEqual(config.calendar_model_id, assumptions.calendar.model_id)
        raw = quotes(1)[0]
        modeled = _historical_quote_to_quote(raw, config)
        state = _profile_state(config, config.profiles[0], "EUR/USD")
        risk_quote = _risk_quote_for_state(modeled, state, config)
        assert risk_quote.metadata is not None
        calendar = risk_quote.metadata["risk_calendar"]
        assert isinstance(calendar, Mapping)
        self.assertTrue(calendar["known"])
        self.assertFalse(calendar["financing_known"])
        self.assertFalse(calendar["calendar_observed"])

    def test_calendar_gates_respect_intraday_vs_multiday_preclose_windows(self) -> None:
        assumptions = historical_assumptions_for(VIRTUAL_EURUSD_10K_MODEL_ID)
        config = HistoricalBacktestConfig.from_protocol(
            ResearchProtocol.default(),
            assumptions_model=assumptions,
        )
        # Monday 16:30 New York is 30 minutes before the modeled financing
        # event and 29 minutes before the daily break.  Only intraday gets a
        # daily pre-close gate; MULTIDAY still cannot enter during the actual
        # break, but is not forced out before it.
        transition = datetime(2026, 9, 14, 20, 30, tzinfo=UTC)
        intraday = next(item for item in config.profiles if item.holding_profile == "INTRADAY")
        multiday = next(item for item in config.profiles if item.holding_profile == "MULTIDAY")
        intraday_gate = config.entry_gate_for(intraday, transition)
        multiday_gate = config.entry_gate_for(multiday, transition)
        assert intraday_gate is not None
        assert multiday_gate is not None
        self.assertFalse(intraday_gate.allowed)
        self.assertEqual(intraday_gate.reason, "PRE_FINANCING")
        self.assertTrue(multiday_gate.allowed)

        daily_break = datetime(2026, 9, 14, 21, 0, tzinfo=UTC)
        intraday_break = config.entry_gate_for(intraday, daily_break)
        multiday_break = config.entry_gate_for(multiday, daily_break)
        assert intraday_break is not None
        assert multiday_break is not None
        self.assertEqual(intraday_break.reason, "DAILY_BREAK")
        self.assertEqual(multiday_break.reason, "DAILY_BREAK")

    def test_calendar_exit_routing_keeps_daily_break_out_of_multiday_and_preserves_latency(self) -> None:
        assumptions = historical_assumptions_for(VIRTUAL_EURUSD_10K_MODEL_ID)
        transition = datetime(2026, 9, 14, 20, 30, tzinfo=UTC)
        # This is a timing-routing probe only.  The production model keeps
        # financing_amount_known=False; the local copy marks it known so the
        # test can observe whether a daily field is treated as a MULTIDAY cut.
        known_calendar = {
            **assumptions.calendar.state_for_quote(transition),
            "financing_known": True,
        }
        contract_spec = {
            **dict(assumptions.account.contract_spec),
            "known": True,
            "fees_known": True,
            "spread_known": True,
            "expected_costs_known": True,
            "risk_envelope_known": True,
            "expected_commission_fixed": "0",
            "expected_commission_per_unit": "0",
            "expected_exit_slippage_pips": "0",
            "expected_cost_currency": "USD",
        }
        risk_state = {
            "daily_pnl": "0",
            "drawdown": "0",
            "daily_anchor_equity": "10000",
            "high_water_equity": "10000",
            "positions": 0,
            "intents": 0,
            "costs_known": True,
            "equity_source": "VIRTUAL_PAPER_ONLY",
            "equity": "10000",
            "margin_available": "10000",
            "margin_required": "1",
            "bar_clock_known": True,
        }
        for latency in ("5", "10"):
            with self.subTest(latency=latency):
                multiday_policy = RiskExitPolicy(holding_profile="MULTIDAY", exit_latency_seconds=latency)
                multiday_plan = plan_entry(
                    multiday_policy,
                    direction="LONG",
                    entry_price="1.10000",
                    atr="0.00100",
                    equity="10000",
                    equity_source="VIRTUAL_PAPER_ONLY",
                    available_at=transition,
                    contract_spec=contract_spec,
                    calendar_state=known_calendar,
                    risk_state=risk_state,
                    mode="VIRTUAL_DIAGNOSTIC",
                )
                before_break = evaluate_exit(
                    multiday_policy,
                    multiday_plan,
                    current_price="1.10010",
                    executable_price="1.10010",
                    observed_at=transition + timedelta(minutes=1),
                    entry_at=transition,
                    calendar_state=known_calendar,
                )
                self.assertEqual(before_break.action, "NONE")
                self.assertEqual(before_break.reason, "NO_EXIT")

                during_break_at = datetime(2026, 9, 14, 21, 0, tzinfo=UTC)
                during_break_calendar = {
                    **assumptions.calendar.state_for_quote(during_break_at),
                    "financing_known": True,
                }
                during_break = evaluate_exit(
                    multiday_policy,
                    multiday_plan,
                    current_price="1.10010",
                    executable_price="1.10010",
                    observed_at=during_break_at,
                    entry_at=transition,
                    calendar_state=during_break_calendar,
                )
                self.assertEqual(during_break.action, "NONE")
                self.assertEqual(during_break.reason, "NO_EXIT")

                friday_at = datetime(2026, 9, 18, 20, 0, tzinfo=UTC)
                friday_calendar = {
                    **assumptions.calendar.state_for_quote(friday_at),
                    "financing_known": True,
                }
                friday_plan = plan_entry(
                    multiday_policy,
                    direction="LONG",
                    entry_price="1.10000",
                    atr="0.00100",
                    equity="10000",
                    equity_source="VIRTUAL_PAPER_ONLY",
                    available_at=friday_at,
                    contract_spec=contract_spec,
                    calendar_state=friday_calendar,
                    risk_state=risk_state,
                    mode="VIRTUAL_DIAGNOSTIC",
                )
                friday_exit = evaluate_exit(
                    multiday_policy,
                    friday_plan,
                    current_price="1.10010",
                    executable_price="1.10010",
                    observed_at=friday_at + timedelta(minutes=1),
                    entry_at=friday_at,
                    calendar_state=friday_calendar,
                )
                self.assertEqual(friday_exit.action, "TIME_EXIT")
                self.assertEqual(friday_exit.reason, "PRE_WEEKLY_CLOSE_AT")
                self.assertEqual(friday_exit.latency_seconds, Decimal(latency))

                intraday_policy = RiskExitPolicy(holding_profile="INTRADAY", exit_latency_seconds=latency)
                intraday_plan = plan_entry(
                    intraday_policy,
                    direction="LONG",
                    entry_price="1.10000",
                    atr="0.00100",
                    equity="10000",
                    equity_source="VIRTUAL_PAPER_ONLY",
                    available_at=transition,
                    contract_spec=contract_spec,
                    calendar_state=known_calendar,
                    risk_state=risk_state,
                    mode="VIRTUAL_DIAGNOSTIC",
                )
                intraday_exit = evaluate_exit(
                    intraday_policy,
                    intraday_plan,
                    current_price="1.10010",
                    executable_price="1.10010",
                    observed_at=transition + timedelta(minutes=1),
                    entry_at=transition,
                    calendar_state=known_calendar,
                )
                self.assertEqual(intraday_exit.action, "TIME_EXIT")
                self.assertEqual(intraday_exit.reason, "PRE_FINANCING_AT")
                self.assertEqual(intraday_exit.latency_seconds, Decimal(latency))

                stop = evaluate_exit(
                    intraday_policy,
                    intraday_plan,
                    current_price="1.09800",
                    executable_price="1.09800",
                    observed_at=transition + timedelta(minutes=1),
                    entry_at=transition,
                    calendar_state=known_calendar,
                    server_side_stop=True,
                )
                take_profit = evaluate_exit(
                    intraday_policy,
                    intraday_plan,
                    current_price="1.10300",
                    executable_price="1.10300",
                    observed_at=transition + timedelta(minutes=1),
                    entry_at=transition,
                    calendar_state=known_calendar,
                    server_side_stop=True,
                )
                self.assertEqual(stop.action, "STOP_LOSS")
                self.assertEqual(take_profit.action, "TAKE_PROFIT")
                self.assertEqual(stop.latency_seconds, Decimal("0"))
                self.assertEqual(take_profit.latency_seconds, Decimal("0"))

    def test_holdout_is_rejected_before_any_candidate_is_processed(self) -> None:
        with tempfile.TemporaryDirectory(prefix="mtf-historical-holdout-") as directory:
            root = Path(directory)
            late = quotes(1, start=datetime(2024, 1, 1, tzinfo=UTC))
            with self.assertRaisesRegex(HistoricalBacktestError, "HOLDOUT_CLOSED"):
                run_historical_backtest(
                    late,
                    manifest=manifest(root),
                    config=HistoricalBacktestConfig.from_protocol(ResearchProtocol.default()),
                    output_dir=root / "output",
                    sink=lambda kind, row: None,
                )

    def test_stress_spread_is_modeled_around_mid_without_mutating_raw_quote(self) -> None:
        raw = quotes(1)[0]
        adverse = HistoricalBacktestConfig.from_protocol(ResearchProtocol.default(), scenario="adverse")
        modeled = _historical_quote_to_quote(raw, adverse)
        self.assertEqual(raw.bid, Decimal("1.10000"))
        self.assertEqual(raw.ask, Decimal("1.10020"))
        self.assertEqual(modeled.mid, Decimal("1.10010"))
        self.assertEqual(modeled.spread, Decimal("0.00030"))
        assert modeled.metadata is not None
        self.assertEqual(modeled.metadata["receipt_basis"], "MODEL_NO_RECEIPT")
        self.assertEqual(modeled.metadata["raw_spread"], Decimal("0.00020"))
        self.assertEqual(modeled.metadata["cost_multiplier"], Decimal("1.5"))

    def test_risk_quote_bar_watermark_is_profile_local(self) -> None:
        config = HistoricalBacktestConfig.from_protocol(ResearchProtocol.default())
        state = _profile_state(config, config.profiles[0], "EUR/USD")
        state.bar_closes = 5
        raw = _historical_quote_to_quote(quotes(1)[0], config)
        risk_quote = _risk_quote_for_state(raw, state, config)
        assert risk_quote.metadata is not None
        epoch = datetime.fromtimestamp(0, UTC)
        elapsed = risk_quote.market_time - epoch
        expected = ((elapsed.days * 86_400 + elapsed.seconds) * 1_000_000 + elapsed.microseconds) // 60_000_000
        self.assertEqual(risk_quote.metadata["risk_trigger_bar_count"], expected)
        self.assertEqual(risk_quote.metadata["risk_trigger_timeframe"], "M1")
        self.assertEqual(
            risk_quote.metadata["risk_bar_clock_basis"],
            "UTC_TIMEFRAME_BOUNDARIES_NO_OHLC_IMPUTATION",
        )
        assert raw.metadata is not None
        self.assertNotIn("risk_trigger_bar_count", raw.metadata)

    def test_risk_bar_clock_does_not_consume_ticks_or_fill_gap_ohlc(self) -> None:
        config = HistoricalBacktestConfig.from_protocol(
            ResearchProtocol.default(),
            candidates=("dc_m5_v1",),
        )
        state = _profile_state(config, config.profiles[0], "EUR/USD")
        times = (
            BASE,
            BASE + timedelta(seconds=59),
            BASE + timedelta(minutes=5),
            BASE + timedelta(minutes=35),
        )
        ordinals: list[int] = []
        for timestamp in times:
            raw = quotes(1, start=timestamp)[0]
            modeled = _historical_quote_to_quote(raw, config)
            risk_quote = _risk_quote_for_state(modeled, state, config)
            assert risk_quote.metadata is not None
            ordinals.append(int(risk_quote.metadata["risk_trigger_bar_count"]))
            self.assertEqual(risk_quote.metadata["risk_trigger_timeframe"], "M5")
            self.assertEqual(risk_quote.sequence, 0)
        self.assertEqual(ordinals[1], ordinals[0])
        self.assertEqual(ordinals[2], ordinals[0] + 1)
        self.assertEqual(ordinals[3], ordinals[2] + 6)

    def test_risk_ledger_suppresses_quote_marks_but_keeps_final_gross_result(self) -> None:
        config = HistoricalBacktestConfig.from_protocol(
            ResearchProtocol.default(),
            candidates=("tp_fast_v1",),
            assumptions_model=historical_assumptions_for(VIRTUAL_EURUSD_10K_MODEL_ID),
        )
        state = _profile_state(config, config.profiles[0], "EUR/USD")
        # 05:00 UTC in January is midnight America/New_York and is open under
        # the server-derived 16:59-17:01 New York daily-break template.
        trade_base = BASE
        point = IndicatorPoint(
            trade_base,
            trade_base + timedelta(minutes=1),
            trade_base + timedelta(minutes=1),
            1.1,
            1.1,
            1.1,
            50.0,
            Decimal("0.001"),
            True,
            DataQuality.good(),
            "indicator-fixture",
            0,
        )
        signal = Signal(
            "risk-ledger-signal",
            "EUR/USD",
            "UP",
            trade_base,
            trade_base,
            trade_base,
            trade_base,
            trade_base + timedelta(minutes=1),
            "risk-ledger-episode",
            mode=OperationMode.REPLAY,
        )
        with tempfile.TemporaryDirectory(prefix="mtf-risk-ledger-") as directory:
            writer = _ArtifactWriter(Path(directory), lambda kind, row: None, 128, resume=False)
            with patch.object(state.processor, "latest_indicator_point", return_value=point):
                initial = _historical_quote_to_quote(quote_at(0, trade_base), config)
                _handle_signal(config, state, signal, initial, "DEV", writer)
            last_risk_quote = initial
            for index in range(200):
                timestamp = trade_base + timedelta(seconds=index)
                bid, ask = ("1.10100", "1.10120") if index == 50 else ("1.10000", "1.10020")
                if index == 199:
                    bid, ask = "1.09800", "1.09820"
                last_risk_quote = _risk_quote_for_state(
                    _historical_quote_to_quote(quote_at(index + 1, timestamp, bid=bid, ask=ask), config),
                    state,
                    config,
                )
                changes = state.risk.on_quote(last_risk_quote, capture_complete=False)
                _emit_risk_updates(
                    config,
                    state,
                    changes,
                    quote=last_risk_quote,
                    partition="DEV",
                    writer=writer,
                )
            final = state.risk.finish(last_risk_quote.available_ts, capture_complete=True)
            _emit_risk_updates(
                config,
                state,
                final.trades,
                quote=last_risk_quote,
                partition="DEV",
                writer=writer,
                final=True,
            )
            ledger = writer.rows("ledger")
            gross = _gross_equity_values(config, state, last_risk_quote)
            writer.close()
        final_rows = [row for row in ledger.retained if row.get("event") == "RISK_FINAL"]
        self.assertEqual(len(final_rows), 1)
        final_row = final_rows[0]
        self.assertLess(Decimal(str(final_row["gross_pnl_account"])), Decimal("0"))
        self.assertIsNone(final_row["net_pnl"])
        self.assertGreater(Decimal(str(final_row["mfe_price"])), Decimal("0"))
        self.assertGreater(Decimal(str(final_row["mae_price"])), Decimal("0"))
        self.assertLess(ledger.count, 20)
        self.assertGreater(state.counters["risk_quote_marks_suppressed"], 150)
        self.assertLess(gross["realized"] or Decimal("0"), Decimal("0"))
        self.assertLess(gross["equity"] or Decimal("0"), config.risk_policy.initial_equity)

    def test_stream_result_has_bounded_artifacts_and_resume_identity(self) -> None:
        fixture = quotes(32)
        with tempfile.TemporaryDirectory(prefix="mtf-historical-run-") as directory:
            root = Path(directory)
            output = root / "output"
            rows: list[tuple[str, dict[str, object]]] = []
            config = HistoricalBacktestConfig.from_protocol(ResearchProtocol.default())
            result = run_historical_backtest(
                fixture,
                manifest=manifest(root),
                config=config,
                output_dir=output,
                sink=lambda kind, row: rows.append((kind, dict(row))),
            )
            self.assertEqual(result.status, "COMPLETED")
            self.assertFalse(result.acceptance)
            self.assertEqual(result.processed_quotes, len(fixture))
            self.assertTrue(result.to_dict()["scenario_applied"])
            self.assertEqual(result.to_dict()["scenario_mode"], "VIRTUAL_DIAGNOSTIC")
            self.assertEqual(
                tuple(item.candidate_id for item in result.variants),
                tuple(item.candidate_id for item in config.profiles),
            )
            self.assertGreater(result.equity.count, 0)
            self.assertIsNone(result.equity.retained[0]["equity"])
            self.assertFalse(result.equity.retained[0]["costs_known"])
            self.assertEqual(result.metrics["ticks_persisted_sqlite"], False)
            self.assertEqual(result.checkpoint.cursor_sequence, len(fixture) - 1)
            self.assertTrue(rows)
            self.assertEqual(result.ledger.path, output / "ledger.jsonl")
            self.assertTrue((output / "checkpoint.json").is_file())
            profile_state = result.checkpoint.state["profiles"]["tp_fast_v1"]
            self.assertIn("risk", profile_state)
            self.assertNotIn("positions", profile_state)

            resumed_rows: list[tuple[str, dict[str, object]]] = []
            resumed = run_historical_backtest(
                fixture,
                manifest=manifest(root),
                config=config,
                output_dir=output,
                sink=lambda kind, row: resumed_rows.append((kind, dict(row))),
                resume=result.checkpoint,
            )
            self.assertEqual(resumed.processed_quotes, result.processed_quotes)
            self.assertEqual(resumed.ledger.count, result.ledger.count)
            self.assertEqual(resumed.equity.count, result.equity.count)
            self.assertEqual(resumed.funnel.count, result.funnel.count)
            self.assertFalse(resumed_rows)


if __name__ == "__main__":
    unittest.main()
