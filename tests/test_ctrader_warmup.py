"""Causal cTrader history bootstrap and one-reader watch regressions."""

from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from mtf_lab.configuration import load_config
from mtf_lab.core.models import OperationMode
from mtf_lab.core.strategy import Signal
from mtf_lab.data.ctrader import (
    CTraderClient,
    CTraderConfig,
    CTraderHistoryResult,
    CTraderInstrumentSpec,
    CTraderProvider,
    DeterministicTransport,
    WireMessage,
    normalize_trendbar,
    synthetic_spot_event,
    synthetic_trendbar,
)
from mtf_lab.data.models import Bar
from mtf_lab.ops.ctrader_warmup import (
    CTraderWarmupError,
    fetch_causal_warmup,
    validate_history_result,
    warmup_requirements,
)
from mtf_lab.ops.ctrader_watch import (
    CTraderWatchContext,
    CTraderWatchOptions,
    CTraderWatchRunner,
    run_ctrader_watch,
)
from mtf_lab.ops.ctrader_watch_cli import _fixture_provider
from mtf_lab.ops.persistence import SQLiteStore

ROOT = Path(__file__).parents[1]


def _bar(start: datetime, timeframe: str, *, instrument: str = "EUR/USD") -> Bar:
    minutes = {"M1": 1, "M5": 5, "M15": 15}[timeframe]
    end = start + timedelta(minutes=minutes)
    price = 1.10 + start.minute / 100_000
    return Bar(
        instrument=instrument,
        interval_start=start,
        interval_end=end,
        open=price,
        high=price + 0.0001,
        low=price - 0.0001,
        close=price,
        resolution_seconds=minutes * 60,
        price_basis="native",
        source="ctrader-open-api",
        received_at=end + timedelta(seconds=1),
        available_at=end + timedelta(seconds=1),
        closed=True,
    )


class CTraderWarmupTests(unittest.TestCase):
    def test_requirements_and_suffix_reject_gap_or_future_bar(self) -> None:
        config = load_config(ROOT / "config/ctrader_query.toml")
        required = warmup_requirements(config)
        self.assertEqual(required, {"M15": 53, "M5": 53, "M1": 52})
        start = datetime(2026, 1, 1, tzinfo=UTC)
        bars = tuple(_bar(start + timedelta(minutes=index), "M1") for index in range(3))
        result = CTraderHistoryResult(bars, "M1", 1, True, False)
        self.assertEqual(
            len(
                validate_history_result(
                    result,
                    timeframe="M1",
                    required=3,
                    instrument="EUR/USD",
                    cutoff=start + timedelta(minutes=4),
                )
            ),
            3,
        )
        with self.assertRaisesRegex(CTraderWarmupError, "barras contiguas"):
            validate_history_result(
                CTraderHistoryResult((bars[0], bars[2]), "M1", 1, True, False),
                timeframe="M1",
                required=2,
                instrument="EUR/USD",
                cutoff=start + timedelta(minutes=4),
            )
        with self.assertRaisesRegex(CTraderWarmupError, "barras contiguas"):
            validate_history_result(
                CTraderHistoryResult((*bars, _bar(start + timedelta(minutes=10), "M1")), "M1", 1, True, False),
                timeframe="M1",
                required=4,
                instrument="EUR/USD",
                cutoff=start + timedelta(minutes=20),
            )
        with self.assertRaisesRegex(CTraderWarmupError, "posterior al cutoff"):
            validate_history_result(
                CTraderHistoryResult((*bars, _bar(start + timedelta(minutes=20), "M1")), "M1", 1, True, False),
                timeframe="M1",
                required=3,
                instrument="EUR/USD",
                cutoff=start + timedelta(minutes=20),
            )

    def test_fetch_keeps_source_has_more_as_provenance(self) -> None:
        config = load_config(ROOT / "config/ctrader_query.toml")
        required = warmup_requirements(config)
        cutoff = datetime(2026, 1, 2, tzinfo=UTC)

        class Provider:
            def fetch_history(
                self,
                timeframe: str,
                *,
                count: int,
                to_timestamp: datetime,
                max_pages: int,
                stop_after_bars: int,
            ) -> CTraderHistoryResult:
                del count, max_pages, stop_after_bars, to_timestamp
                minutes = {"M1": 1, "M5": 5, "M15": 15}[timeframe]
                values = tuple(
                    _bar(cutoff - timedelta(minutes=minutes * index + minutes), timeframe)
                    for index in range(required[timeframe] + 2)
                )
                return CTraderHistoryResult(tuple(reversed(values)), timeframe, 1, False, True)

        result = fetch_causal_warmup(Provider(), config, cutoff=cutoff)
        self.assertEqual(result.cutoff, cutoff)
        self.assertTrue(all(len(result.bars[key]) >= value for key, value in required.items()))
        self.assertTrue(all(item["source_has_more"] for item in result.source.values()))

    def test_periodless_live_trendbar_uses_explicit_payload_context(self) -> None:
        provider = CTraderProvider(CTraderConfig(symbol="EUR/USD", symbol_id=99, quote_basis="mid"))
        start = datetime(2026, 1, 1, tzinfo=UTC)
        raw = dict(synthetic_trendbar(timestamp_minutes=int(start.timestamp() // 60), period="M1"))
        raw.pop("period")
        payload = dict(
            synthetic_spot_event(
                timestamp_ms=int((start + timedelta(minutes=1)).timestamp() * 1000),
                symbol_id=99,
                bid_relative=110000,
                ask_relative=110020,
                trendbars=(raw,),
            )
        )
        payload["period"] = 1
        result = provider.normalize_spot(payload, received_at=start + timedelta(minutes=2))
        self.assertEqual(len(result.bars), 1)
        self.assertEqual(result.bars[0].timeframe, "M1")

    def test_normalization_issue_does_not_forward_quote_to_paper(self) -> None:
        config = load_config(ROOT / "config/fixture_cfd.toml")
        transport = DeterministicTransport()
        ctrader_config = CTraderConfig(symbol="EUR/USD", symbol_id=99, account_id=7, quote_basis="mid")
        client = CTraderClient(ctrader_config, transport=transport)
        client.connect()
        client.mark_authenticated(7)
        provider = CTraderProvider(ctrader_config, client=client)
        try:
            with TemporaryDirectory() as name, SQLiteStore(Path(name) / "watch.sqlite3") as store:
                runner = CTraderWatchRunner(
                    CTraderWatchContext(
                        provider=provider,
                        config=config,
                        store=store,
                        provenance={
                            "provider": "ctrader_open_api_fixture",
                            "source_mode": "SYNTHETIC_FIXTURE",
                            "synthetic": True,
                            "environment": "OFFLINE",
                            "network_performed": False,
                            "execution_enabled": False,
                            "data_identity": "normalization-issue-fixture",
                        },
                        close_provider=False,
                    ),
                    CTraderWatchOptions(max_events=1),
                )
                runner._prepare()
                signal = Signal(
                    signal_id="warmup-issue-signal",
                    instrument="EUR/USD",
                    direction="UP",
                    detected_at=datetime(2026, 1, 1, tzinfo=UTC),
                    context_start=datetime(2025, 12, 31, 23, 55, tzinfo=UTC),
                    preparation_start=datetime(2025, 12, 31, 23, 58, tzinfo=UTC),
                    trigger_start=datetime(2025, 12, 31, 23, 59, tzinfo=UTC),
                    trigger_end=datetime(2026, 1, 1, tzinfo=UTC),
                    episode_id="warmup-issue-episode",
                    mode=OperationMode.SYNTHETIC,
                )
                assert runner.paper is not None
                runner.paper.on_signal(signal, capture_hash="prefix")
                raw = dict(
                    synthetic_trendbar(timestamp_minutes=int(datetime(2026, 1, 1, tzinfo=UTC).timestamp() // 60))
                )
                raw["period"] = 999
                payload = dict(
                    synthetic_spot_event(
                        timestamp_ms=int(datetime(2026, 1, 1, 0, 1, tzinfo=UTC).timestamp() * 1000),
                        symbol_id=99,
                        bid_relative=110000,
                        ask_relative=110020,
                        trendbars=(raw,),
                    )
                )
                runner._process_message(
                    WireMessage(
                        "PROTO_OA_SPOT_EVENT",
                        payload,
                        ingest_sequence=0,
                        connection_generation=1,
                        is_event=True,
                    )
                )
                self.assertEqual(runner.paper.simulator.counters["fills"], 0)
                self.assertIn("normalization_error", runner.coordinator.external_blocked_reasons)
        finally:
            provider.close()

    def test_unstamped_message_is_durable_gate_before_analysis(self) -> None:
        config = load_config(ROOT / "config/fixture_cfd.toml")
        transport = DeterministicTransport()
        ctrader_config = CTraderConfig(symbol="EUR/USD", symbol_id=99, account_id=7, quote_basis="mid")
        client = CTraderClient(ctrader_config, transport=transport)
        client.connect()
        client.mark_authenticated(7)
        provider = CTraderProvider(ctrader_config, client=client)
        try:
            payload = dict(
                synthetic_spot_event(
                    timestamp_ms=int(datetime(2026, 1, 1, tzinfo=UTC).timestamp() * 1000),
                    symbol_id=99,
                    bid_relative=110000,
                    ask_relative=110020,
                )
            )
            with TemporaryDirectory() as name, SQLiteStore(Path(name) / "watch.sqlite3") as store:
                runner = CTraderWatchRunner(
                    CTraderWatchContext(
                        provider=provider,
                        config=config,
                        store=store,
                        provenance={
                            "provider": "ctrader_open_api_fixture",
                            "source_mode": "SYNTHETIC_FIXTURE",
                            "synthetic": True,
                            "environment": "OFFLINE",
                            "network_performed": False,
                            "execution_enabled": False,
                            "data_identity": "unstamped-fixture",
                        },
                        close_provider=False,
                    ),
                    CTraderWatchOptions(max_events=1, idle_timeout_seconds=0.1),
                )
                runner._prepare()
                runner._process_message(WireMessage("PROTO_OA_SPOT_EVENT", payload, is_event=True))
                self.assertEqual(runner.stats.messages, 1)
                self.assertEqual(runner.stats.events, 0)
                self.assertGreaterEqual(runner.stats.ignored_messages, 1)
                self.assertEqual(store.list_capture_envelopes(runner.coordinator.session_id), [])
        finally:
            provider.close()

    def test_runner_consumes_bootstrap_before_one_reader_spots(self) -> None:
        source_config = load_config(ROOT / "config/fixture_cfd.toml")
        provider, clock = _fixture_provider(source_config, 0, 180, include_trendbars=True)
        # cTrader historical trendbars are native.  The explicit native
        # analysis basis lets the same live SpotEvents remain available to
        # the PAPER sink without pretending that bars contain bid/ask legs.
        config = replace(source_config, price_base="native")
        spec = CTraderInstrumentSpec(symbol="EUR/USD", symbol_id=99, digits=5, pip_position=4, price_scale=100_000)
        bootstrap: dict[str, tuple[Bar, ...]] = {}
        requirements = warmup_requirements(config)
        boundary = datetime(2026, 1, 1, tzinfo=UTC)
        codes = {"M1": 1, "M5": 5, "M15": 7}
        for timeframe, count in requirements.items():
            period = codes[timeframe]
            minutes = {"M1": 1, "M5": 5, "M15": 15}[timeframe]
            rows = []
            for index in range(count):
                start = boundary - timedelta(minutes=minutes * (count - index))
                raw = synthetic_trendbar(timestamp_minutes=int(start.timestamp() // 60), period=timeframe)
                rows.append(
                    normalize_trendbar(
                        raw,
                        spec=spec,
                        received_at=boundary,
                        available_at=boundary,
                        requested_period=period,
                        response_period=period,
                        mode="SYNTHETIC",
                    )
                )
            bootstrap[timeframe] = tuple(rows)
        try:
            with TemporaryDirectory() as name, SQLiteStore(Path(name) / "watch.sqlite3") as store:
                result = run_ctrader_watch(
                    CTraderWatchContext(
                        provider=provider,
                        config=config,
                        store=store,
                        provenance={
                            "provider": "ctrader_open_api_fixture",
                            "source_mode": "SYNTHETIC_FIXTURE",
                            "synthetic": True,
                            "environment": "OFFLINE",
                            "network_performed": False,
                            "execution_enabled": False,
                            "data_identity": "warmup-fixture",
                        },
                        clock=clock,
                        bootstrap_bars=bootstrap,
                        bootstrap_metadata={"cutoff": boundary.isoformat()},
                    ),
                    CTraderWatchOptions(max_events=180, duration_seconds=5, idle_timeout_seconds=0.1),
                )
            self.assertEqual(result.status["warmup_pending"], {"M1": 0, "M5": 0, "M15": 0})
            self.assertEqual(result.status["capture_state"], "PAUSED")
            self.assertEqual(result.status["analysis_blocked_reasons"], [])
            self.assertEqual(result.status["paper"]["enabled"], True)
            self.assertGreater(result.bars, result.status["bootstrap_bars"])
        finally:
            provider.close()

    def test_reader_health_disconnects_paper_and_blocks_continuity(self) -> None:
        config = load_config(ROOT / "config/fixture_cfd.toml")
        provider, _clock = _fixture_provider(config, 0, 130)
        try:
            with TemporaryDirectory() as name, SQLiteStore(Path(name) / "watch.sqlite3") as store:
                from mtf_lab.ops.ctrader_watch import CTraderWatchRunner

                runner = CTraderWatchRunner(
                    CTraderWatchContext(
                        provider=provider,
                        config=config,
                        store=store,
                        provenance={
                            "provider": "ctrader_open_api_fixture",
                            "source_mode": "SYNTHETIC_FIXTURE",
                            "synthetic": True,
                            "environment": "OFFLINE",
                            "network_performed": False,
                            "execution_enabled": False,
                            "data_identity": "health-fixture",
                        },
                    ),
                    CTraderWatchOptions(max_events=1),
                )
                runner._prepare()
                provider.client._set_status(needs_reconciliation=True, dropped_messages=1)
                runner._observe_provider_health()
                self.assertFalse(runner.paper.simulator._connected)
                self.assertEqual(runner.coordinator.continuity_state, "BROKEN")
                self.assertIn("market_backpressure", runner.coordinator.external_blocked_reasons)
        finally:
            provider.close()


if __name__ == "__main__":
    unittest.main()
