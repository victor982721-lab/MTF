"""Focused contracts for the cTrader watch -> local CFD PAPER sink."""

from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from mtf_lab.configuration import load_config
from mtf_lab.core import IndicatorConfig
from mtf_lab.core.models import OperationMode
from mtf_lab.core.strategy import Signal
from mtf_lab.data.ctrader import (
    CTraderClient,
    CTraderConfig,
    CTraderProvider,
    DeterministicTransport,
    WireMessage,
)
from mtf_lab.data.paper_fixture import synthetic_ctrader_payloads
from mtf_lab.ops.ctrader_executor import RiskLimitRejected
from mtf_lab.ops.ctrader_watch import (
    CTraderWatchContext,
    CTraderWatchOptions,
    CTraderWatchRunner,
    _WatchPaper,
    run_ctrader_watch,
)
from mtf_lab.ops.persistence import SQLiteStore
from mtf_lab.ops.supervision_demo import _book_legs

BASE = datetime(2026, 1, 1, tzinfo=UTC)


def _signal() -> Signal:
    return Signal(
        signal_id="signal-paper-watch",
        instrument="EUR/USD",
        direction="UP",
        detected_at=BASE,
        context_start=BASE - timedelta(minutes=5),
        preparation_start=BASE - timedelta(minutes=2),
        trigger_start=BASE - timedelta(minutes=1),
        trigger_end=BASE,
        episode_id="episode-paper-watch",
        mode=OperationMode.SYNTHETIC,
    )


class CTraderWatchPaperTests(unittest.TestCase):
    def _provider(self) -> CTraderProvider:
        return CTraderProvider(
            CTraderConfig(symbol="EUR/USD", symbol_id=99, quote_basis="mid"),
            transport=DeterministicTransport(),
        )

    @staticmethod
    def _event(provider: CTraderProvider, when: datetime, *, bid: int, ask: int):
        result = provider.normalize_spot(
            {
                "symbolId": 99,
                "timestamp": int(when.timestamp() * 1000),
                "timestamp_unit": "ms",
                "bid": bid,
                "ask": ask,
            },
            received_at=when,
            available_at=when,
            sequence=int((when - BASE).total_seconds()),
        )
        assert result.quote_events
        return result.quote_events[0]

    def _paper(self, store: SQLiteStore) -> _WatchPaper:
        config = load_config(Path("config/fixture_cfd.toml"))
        cfd = {**dict(config.cfd), "horizons_seconds": (60,), "max_quote_age_seconds": 5}
        paper = _WatchPaper(
            store=store,
            session_id="watch-paper",
            config=config,
            paper_config=cfd,
            semantic_identity="semantic-paper-watch",
            runtime_analysis_id="runtime-paper-watch",
        )
        return paper

    def test_valid_bid_ask_fills_and_closes_deterministically_and_persists(self) -> None:
        provider = self._provider()
        with TemporaryDirectory() as directory, SQLiteStore(Path(directory) / "paper.sqlite3") as store:
            store.create_session(
                session_id="watch-paper",
                mode="SYNTHETIC",
                provider="ctrader",
                instrument="EUR/USD",
                config=load_config(Path("config/fixture_cfd.toml")).to_dict(),
            )
            paper = self._paper(store)
            paper.on_signal(_signal(), capture_hash="prefix-1")
            paper.on_event(self._event(provider, BASE, bid=110000, ask=110020), capture_hash="prefix-1")
            paper.on_event(
                self._event(provider, BASE + timedelta(seconds=60), bid=110010, ask=110030),
                capture_hash="prefix-2",
            )
            self.assertEqual(len(paper.simulator.trades), 1)
            trade = paper.simulator.trades[0]
            self.assertEqual(trade.state.value, "CLOSED")
            self.assertEqual(paper.simulator.counters["fills"], 1)
            self.assertEqual(paper.simulator.counters["closures"], 1)
            rows = store.list_cfd_trades("watch-paper", paper.analysis_id)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["state"], "CLOSED")
            self.assertEqual(rows[0]["payload"]["entry_quote_id"], trade.entry_quote_id)

    def test_equal_crossed_missing_and_partial_quotes_never_fill(self) -> None:
        provider = self._provider()
        with TemporaryDirectory() as directory, SQLiteStore(Path(directory) / "paper.sqlite3") as store:
            store.create_session(
                session_id="watch-paper",
                mode="SYNTHETIC",
                provider="ctrader",
                instrument="EUR/USD",
                config=load_config(Path("config/fixture_cfd.toml")).to_dict(),
            )
            paper = self._paper(store)
            paper.on_signal(_signal(), capture_hash="prefix")
            equal = self._event(provider, BASE, bid=110000, ask=110000)
            paper.on_event(equal, capture_hash="prefix")
            self.assertEqual(paper.simulator.counters["fills"], 0)
            partial = self._event(provider, BASE + timedelta(seconds=1), bid=110001, ask=110021)
            partial = replace(partial, metadata={**partial.metadata, "partial_update": True})
            paper.on_event(partial, capture_hash="prefix")
            self.assertEqual(paper.simulator.counters["fills"], 0)
            missing = replace(partial, ask=None, metadata={**partial.metadata, "partial_update": False})
            paper.on_event(missing, capture_hash="prefix")
            self.assertEqual(paper.simulator.counters["fills"], 0)
            self.assertEqual(paper.simulator.trades[0].state.value, "PENDING")
            self.assertGreaterEqual(len(paper.projection()["issues"]), 3)

    def test_resume_restores_trades_but_forces_fresh_quote_baseline(self) -> None:
        provider = self._provider()
        with TemporaryDirectory() as directory, SQLiteStore(Path(directory) / "paper.sqlite3") as store:
            store.create_session(
                session_id="watch-paper",
                mode="SYNTHETIC",
                provider="ctrader",
                instrument="EUR/USD",
                config=load_config(Path("config/fixture_cfd.toml")).to_dict(),
            )
            first = self._paper(store)
            first.on_signal(_signal(), capture_hash="prefix")
            first.on_event(self._event(provider, BASE, bid=110000, ask=110020), capture_hash="prefix")
            checkpoint = first.checkpoint()
            second = self._paper(store)
            second.restore(checkpoint, generation=0)
            # The restored filled trade is retained, but the first quote from
            # the new process is a baseline and cannot close it.
            baseline = self._event(provider, BASE + timedelta(seconds=60), bid=110010, ask=110030)
            second.on_event(
                replace(baseline, source_sequence=0),
                capture_hash="prefix-next",
            )
            self.assertEqual(second.simulator.counters["closures"], 0)
            self.assertEqual(second.simulator.trades[0].state.value, "FILLED")
            next_quote = self._event(provider, BASE + timedelta(seconds=61), bid=110020, ask=110040)
            second.on_event(replace(next_quote, source_sequence=1), capture_hash="prefix-next")
            self.assertEqual(second.simulator.counters["closures"], 1)

    def test_watch_routes_detector_signal_to_paper_and_persists_fills(self) -> None:
        base = load_config(Path("config/fixture_cfd.toml"))
        indicators = IndicatorConfig(ema_fast=2, ema_slow=3, rsi_period=2, atr_period=2)
        strategy = replace(
            base.strategy,
            indicators=indicators,
            context_lookback=1,
            preparation_lookback=3,
            preparation_ttl_bars=2,
            max_distance_atr=2.0,
        )
        config = replace(base, mode="SYNTHETIC", indicators=indicators, strategy=strategy)
        transport = DeterministicTransport(inbound_maxsize=256)
        ctrader_config = CTraderConfig(
            symbol="EUR/USD", symbol_id=99, account_id=7, quote_basis="mid", queue_maxsize=256
        )
        client = CTraderClient(ctrader_config, transport=transport, wall_clock=lambda: BASE)
        client.connect()
        client.mark_authenticated(7)
        provider = CTraderProvider(ctrader_config, client=client, clock=lambda: BASE)
        for index, payload in enumerate(synthetic_ctrader_payloads(start=BASE, symbol_id=99, count=190)):
            when = BASE + timedelta(minutes=index + 1)
            transport.push(
                WireMessage(
                    "PROTO_OA_SPOT_EVENT",
                    payload,
                    is_event=True,
                    received_at=when,
                    available_at=when,
                    ingest_sequence=index,
                    connection_generation=1,
                    source_identity=f"paper-watch:{index}",
                )
            )
        provenance = {
            "source_mode": "SYNTHETIC_FIXTURE",
            "synthetic": True,
            "provider": "ctrader_open_api_fixture",
            "environment": "OFFLINE",
            "network_performed": False,
            "execution_enabled": False,
            "data_identity": "watch-paper-e2e-v1",
        }
        with TemporaryDirectory() as directory, SQLiteStore(Path(directory) / "watch.sqlite3") as store:
            result = run_ctrader_watch(
                CTraderWatchContext(
                    provider,
                    config,
                    store,
                    provenance=provenance,
                    clock=lambda: BASE + timedelta(minutes=191),
                ),
                # Coverage-instrumented full-gate runs can spend more than
                # half a second committing one SQLite batch.  The queue is
                # already bounded by max_events, so a wider idle guard keeps
                # the test focused on the route rather than scheduler jitter.
                CTraderWatchOptions(max_events=190, idle_timeout_seconds=5.0, poll_timeout_seconds=0.01),
            )
            self.assertGreater(result.paper["counters"]["fills"], 0)
            self.assertEqual(result.paper["counters"]["fills"], result.paper["counters"]["closures"])
            rows = store.list_cfd_trades(result.session_id, result.paper_analysis_id)
            self.assertEqual(len(rows), len(result.paper["trades"]))
            self.assertTrue(rows)
            self.assertTrue(all(row["state"] == "CLOSED" for row in rows))
            self.assertTrue(all(row["product"] == "FOREX_CFD_LOCAL_PAPER" for row in rows))

    def test_demo_quote_projection_rejects_nonordered_or_nonvalid_legs(self) -> None:
        def snapshot(bid: str, ask: str, *, state: str | None = None) -> dict[str, object]:
            def leg(price: str) -> dict[str, object]:
                return {
                    "price": price,
                    "event_time": "2026-01-01T00:00:00Z",
                    "available_at": "2026-01-01T00:00:01Z",
                    "timestamp_missing": False,
                    **({"state": state} if state is not None else {}),
                }

            return {"symbols": {"99": {"bid": leg(bid), "ask": leg(ask)}}}

        with self.assertRaisesRegex(RiskLimitRejected, "cruzada"):
            _book_legs(snapshot("1.1000", "1.1000"), 99)
        with self.assertRaisesRegex(RiskLimitRejected, "cruzada"):
            _book_legs(snapshot("1.1002", "1.1000"), 99)
        with self.assertRaisesRegex(RiskLimitRejected, "calidad"):
            _book_legs(snapshot("1.1000", "1.1002", state="STALE"), 99)

    def test_live_freshness_does_not_promote_an_invalid_spot_event(self) -> None:
        config = replace(load_config(Path("config/fixture_cfd.toml")), mode="LIVE")
        provider = self._provider()
        provenance = {
            "source_mode": "DEMO_OBSERVED",
            "synthetic": False,
            "provider": "ctrader_open_api",
            "environment": "DEMO",
            "network_performed": True,
            "execution_enabled": False,
            "data_identity": "live-quality-test",
        }
        with TemporaryDirectory() as directory, SQLiteStore(Path(directory) / "watch.sqlite3") as store:
            runner = CTraderWatchRunner(
                CTraderWatchContext(
                    provider,
                    config,
                    store,
                    provenance=provenance,
                    close_provider=False,
                    clock=lambda: BASE,
                ),
                CTraderWatchOptions(max_events=1),
            )
            runner._prepare()
            valid = self._event(provider, BASE, bid=110000, ask=110020)
            runner._process_record(valid, False)
            self.assertEqual(runner.coordinator.status().freshness_state, "VALID")
            invalid = self._event(provider, BASE + timedelta(seconds=1), bid=110020, ask=110020)
            runner._process_record(invalid, False)
            self.assertEqual(runner.coordinator.status().freshness_state, "BLOCKED")


if __name__ == "__main__":
    unittest.main()
