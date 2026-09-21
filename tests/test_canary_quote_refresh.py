"""Focused offline contracts for the bounded DEMO canary quote refresh."""

from __future__ import annotations

import copy
import unittest
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

from tools.demo_canary import CanaryGateError, _refresh_canary_quotes

BASE = datetime(2026, 9, 21, 16, 30, tzinfo=UTC)
GENERATION = "g1"
SYMBOL_ID = 11


def _leg(
    event_time: datetime,
    available_at: datetime,
    *,
    price: str = "1.1000",
    generation: str = GENERATION,
    state: str | None = None,
    reasons: tuple[str, ...] = (),
) -> dict[str, Any]:
    leg = {
        "price": price,
        "event_time": event_time.isoformat(),
        "available_at": available_at.isoformat(),
        "received_at": available_at.isoformat(),
        "generation": generation,
        "reasons": list(reasons),
        "timestamp_missing": False,
    }
    if state is not None:
        leg["state"] = state
    return leg


def _book(bid: dict[str, Any] | None, ask: dict[str, Any] | None) -> dict[str, Any]:
    legs: dict[str, Any] = {}
    if bid is not None:
        legs["bid"] = bid
    if ask is not None:
        legs["ask"] = ask
    return {
        "schema_version": 1,
        "generation": GENERATION,
        "symbols": {str(SYMBOL_ID): legs},
    }


def _record(side: str, leg: dict[str, Any]) -> dict[str, Any]:
    return {"side": side, "leg": copy.deepcopy(leg)}


class _Clock:
    def __init__(self, current: datetime, *future_values: datetime) -> None:
        self.current = current
        self._values = (current, *future_values)
        self.calls = 0

    def __call__(self) -> datetime:
        index = min(self.calls, len(self._values) - 1)
        value = self._values[index]
        self.calls += 1
        return value

    def advance(self, seconds: float) -> None:
        self.current += timedelta(seconds=seconds)
        self._values = self._values[: self.calls] + (self.current,)


_STOP = object()


class FakeProvider:
    """Same-provider/client quote-book double; it never opens a real session."""

    generation = GENERATION

    def __init__(
        self,
        snapshot: dict[str, Any],
        records: list[object] | None = None,
        *,
        clock: _Clock | None = None,
        quiet_gap_seconds: float = 0.0,
    ) -> None:
        self.spec = SimpleNamespace(symbol="EUR/USD", symbol_id=SYMBOL_ID)
        self.client = object()
        self._snapshot = copy.deepcopy(snapshot)
        self._records = list(records or ())
        self._clock = clock
        self._quiet_gap_seconds = quiet_gap_seconds
        self.snapshot_calls = 0
        self.stream_calls: list[dict[str, object]] = []
        self.consumed = 0

    def snapshot_quote_state(self) -> dict[str, Any]:
        self.snapshot_calls += 1
        return copy.deepcopy(self._snapshot)

    def stream(
        self,
        *,
        max_events: int,
        duration_seconds: float,
        timeout_seconds: float,
    ):
        self.stream_calls.append(
            {
                "max_events": max_events,
                "duration_seconds": duration_seconds,
                "timeout_seconds": timeout_seconds,
            }
        )
        if self._quiet_gap_seconds and self._clock is not None:
            self._clock.advance(self._quiet_gap_seconds)
        for index, record in enumerate(self._records):
            if index >= max_events:
                break
            self.consumed += 1
            if record is _STOP:
                raise AssertionError("refresh consumed a record after the first fresh BBO")
            if isinstance(record, dict):
                side = record.get("side")
                leg = record.get("leg")
                if isinstance(side, str) and isinstance(leg, dict):
                    symbols = self._snapshot["symbols"]
                    symbols[str(SYMBOL_ID)][side] = copy.deepcopy(leg)
            yield record

    # Any use of an execution/session path is a test failure, not a fallback.
    def reconnect(self) -> None:
        raise AssertionError("quote refresh must not reconnect")

    def authenticate(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("quote refresh must not authenticate")

    def submit_signal(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("quote refresh must not submit")


def _stale_book() -> dict[str, Any]:
    old = BASE - timedelta(seconds=5)
    return _book(_leg(old, old, price="1.0990"), _leg(old, old, price="1.0992"))


def _fresh_pair(*, at: datetime = BASE) -> tuple[dict[str, Any], dict[str, Any]]:
    return (
        _leg(at - timedelta(milliseconds=400), at, price="1.1001"),
        _leg(at - timedelta(milliseconds=300), at, price="1.1003"),
    )


class CanaryQuoteRefreshTests(unittest.TestCase):
    def test_six_stale_quotes_then_fresh_preserves_raw_before_after_and_stops(self) -> None:
        stale = _stale_book()
        fresh_bid, fresh_ask = _fresh_pair()
        records: list[object] = [
            _record("bid", stale["symbols"][str(SYMBOL_ID)]["bid"]),
            _record("ask", stale["symbols"][str(SYMBOL_ID)]["ask"]),
            _record("bid", _leg(BASE - timedelta(seconds=4), BASE - timedelta(seconds=4), price="1.0991")),
            _record("ask", _leg(BASE - timedelta(seconds=4), BASE - timedelta(seconds=4), price="1.0993")),
            _record("bid", fresh_bid),
            _record("ask", fresh_ask),
            _STOP,
        ]
        clock = _Clock(BASE)
        provider = FakeProvider(stale, records, clock=clock)
        raw_before = provider.snapshot_quote_state()

        _refresh_canary_quotes(provider, deadline=BASE + timedelta(seconds=45), max_events=8, clock=clock)

        raw_after = provider.snapshot_quote_state()
        self.assertEqual(raw_before, stale)
        self.assertEqual(raw_after["symbols"][str(SYMBOL_ID)]["bid"], fresh_bid)
        self.assertEqual(raw_after["symbols"][str(SYMBOL_ID)]["ask"], fresh_ask)
        self.assertEqual(provider.consumed, 6)
        self.assertEqual(
            provider.stream_calls,
            [{"max_events": 8, "duration_seconds": 30.0, "timeout_seconds": 0.25}],
        )

    def test_fresh_entry_returns_without_polling(self) -> None:
        bid, ask = _fresh_pair()
        initial = _book(bid, ask)
        clock = _Clock(BASE)
        provider = FakeProvider(initial, [_STOP], clock=clock)
        raw_before = provider.snapshot_quote_state()

        _refresh_canary_quotes(provider, deadline=BASE + timedelta(seconds=10), max_events=4, clock=clock)

        self.assertEqual(provider.stream_calls, [])
        self.assertEqual(provider.consumed, 0)
        self.assertEqual(provider.snapshot_quote_state(), raw_before)

    def test_quiet_gap_then_fresh_pair_completes_within_thirty_seconds(self) -> None:
        fresh_bid, fresh_ask = _fresh_pair(at=BASE + timedelta(seconds=5))
        clock = _Clock(BASE)
        provider = FakeProvider(
            _stale_book(),
            [_record("bid", fresh_bid), _record("ask", fresh_ask), _STOP],
            clock=clock,
            quiet_gap_seconds=5,
        )

        _refresh_canary_quotes(provider, deadline=BASE + timedelta(seconds=45), max_events=5, clock=clock)

        self.assertEqual(provider.consumed, 2)
        self.assertEqual(provider.stream_calls[0]["duration_seconds"], 30.0)
        self.assertEqual(clock.current, BASE + timedelta(seconds=5))

    def test_fresh_bid_does_not_rejuvenate_old_ask(self) -> None:
        stale = _stale_book()
        fresh_bid, _fresh_ask = _fresh_pair()
        provider = FakeProvider(stale, [_record("bid", fresh_bid)])

        with self.assertRaises(CanaryGateError):
            _refresh_canary_quotes(provider, deadline=BASE + timedelta(seconds=10), max_events=2, clock=lambda: BASE)

        after = provider.snapshot_quote_state()["symbols"][str(SYMBOL_ID)]
        self.assertEqual(after["bid"], fresh_bid)
        self.assertEqual(after["ask"], stale["symbols"][str(SYMBOL_ID)]["ask"])

    def test_missing_future_noncausal_generation_and_quality_are_rejected(self) -> None:
        valid_bid, valid_ask = _fresh_pair()
        cases = {
            "missing_book": {"schema_version": 1, "generation": GENERATION, "symbols": {}},
            "missing_ask": _book(valid_bid, None),
            "future": _book(valid_bid, _leg(BASE, BASE + timedelta(seconds=1), price="1.1003")),
            "noncausal": _book(valid_bid, _leg(BASE, BASE - timedelta(seconds=1), price="1.1003")),
            "generation": _book(valid_bid, _leg(BASE - timedelta(milliseconds=300), BASE, generation="g2")),
            "quality_state": _book(valid_bid, _leg(BASE - timedelta(milliseconds=300), BASE, state="STALE")),
            "quality_reason": _book(
                valid_bid,
                _leg(BASE - timedelta(milliseconds=300), BASE, reasons=("STALE_ASK",)),
            ),
        }
        for name, snapshot in cases.items():
            with self.subTest(case=name):
                provider = FakeProvider(snapshot)
                with self.assertRaises(CanaryGateError):
                    _refresh_canary_quotes(
                        provider,
                        deadline=BASE + timedelta(seconds=10),
                        max_events=2,
                        clock=lambda: BASE,
                    )

    def test_deadline_and_event_cap_bound_consumption(self) -> None:
        stale = _stale_book()
        clock = _Clock(BASE)
        provider = FakeProvider(
            stale,
            [
                _record("bid", stale["symbols"][str(SYMBOL_ID)]["bid"]),
                _record("ask", stale["symbols"][str(SYMBOL_ID)]["ask"]),
                _record("bid", stale["symbols"][str(SYMBOL_ID)]["bid"]),
            ],
            clock=clock,
        )
        with self.assertRaises(CanaryGateError):
            _refresh_canary_quotes(provider, deadline=BASE + timedelta(seconds=10), max_events=2, clock=clock)
        self.assertEqual(provider.consumed, 2)
        self.assertEqual(provider.stream_calls[0]["max_events"], 2)
        self.assertEqual(provider.stream_calls[0]["duration_seconds"], 10.0)

        at_deadline = FakeProvider(stale)
        with self.assertRaises(CanaryGateError):
            _refresh_canary_quotes(at_deadline, deadline=BASE, max_events=2, clock=lambda: BASE)
        self.assertEqual(at_deadline.stream_calls, [])

    def test_clock_rollback_fails_closed(self) -> None:
        stale = _stale_book()
        clock = _Clock(BASE, BASE, BASE - timedelta(seconds=1))
        provider = FakeProvider(stale, [_record("bid", stale["symbols"][str(SYMBOL_ID)]["bid"])])

        with self.assertRaises(CanaryGateError):
            _refresh_canary_quotes(provider, deadline=BASE + timedelta(seconds=10), max_events=2, clock=clock)

    def test_missing_reader_or_book_snapshotter_is_rejected_without_execution_path(self) -> None:
        for missing in ("stream", "snapshot_quote_state"):
            with self.subTest(missing=missing):
                provider = FakeProvider(_stale_book())
                setattr(provider, missing, None)
                with self.assertRaises(CanaryGateError):
                    _refresh_canary_quotes(
                        provider,
                        deadline=BASE + timedelta(seconds=10),
                        max_events=2,
                        clock=lambda: BASE,
                    )


if __name__ == "__main__":
    unittest.main()
