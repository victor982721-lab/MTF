"""Regresiones para el contexto de periodo de trendbars cTrader."""

from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta

from mtf_lab.data.ctrader import (
    CTraderClient,
    CTraderConfig,
    CTraderDataError,
    CTraderInstrumentSpec,
    CTraderProvider,
    DeterministicTransport,
    WireMessage,
    normalize_trendbar,
    synthetic_trendbar,
)

BASE = datetime(2026, 1, 1, tzinfo=UTC)


class CTraderTrendbarPeriodTests(unittest.TestCase):
    @staticmethod
    def _periodless_bar(*, timestamp_minutes: int) -> dict[str, object]:
        raw = synthetic_trendbar(timestamp_minutes=timestamp_minutes, period="M1")
        del raw["period"]
        return raw

    def _provider(self, response_payload: dict[str, object]) -> CTraderProvider:
        def handler(request: WireMessage) -> WireMessage:
            self.assertEqual(request.payload_type_name, "PROTO_OA_GET_TRENDBARS_REQ")
            return WireMessage("PROTO_OA_GET_TRENDBARS_RES", response_payload, request.client_msg_id)

        config = CTraderConfig(symbol_id=99, account_id=7)
        transport = DeterministicTransport(handler)
        client = CTraderClient(config, transport=transport)
        client.connect()
        client.mark_authenticated(7)
        return CTraderProvider(config, client=client, clock=lambda: BASE + timedelta(hours=1))

    def test_standalone_periodless_trendbar_stays_fail_closed(self) -> None:
        raw = self._periodless_bar(timestamp_minutes=int(BASE.timestamp() // 60))
        with self.assertRaisesRegex(CTraderDataError, r"period no soportado: None"):
            normalize_trendbar(raw, spec=CTraderInstrumentSpec(symbol_id=99), received_at=BASE)

    def test_fetch_uses_response_period_for_periodless_child_and_is_complete(self) -> None:
        raw = self._periodless_bar(timestamp_minutes=int(BASE.timestamp() // 60))
        provider = self._provider(
            {
                "period": 5,
                "symbolId": 99,
                "trendbar": [raw],
                "hasMore": False,
            }
        )

        result = provider.fetch("M5", count=1)
        self.assertEqual(len(result.bars), 1)
        self.assertEqual(result.bars[0].resolution, "M5")
        self.assertEqual(result.bars[0].metadata["period_source"], "response")
        self.assertEqual(result.bars[0].metadata["requested_period"], 5)
        self.assertEqual(result.bars[0].metadata["response_period"], 5)
        self.assertIsNone(result.bars[0].metadata["bar_period_observed"])
        self.assertEqual(result.issues, ())

        history = provider.fetch_history("M5", count=1, max_pages=1)
        self.assertEqual(len(history.bars), 1)
        self.assertTrue(history.complete)
        self.assertFalse(history.has_more)
        self.assertEqual(history.pages, 1)
        self.assertEqual(history.issues, ())

    def test_fetch_uses_requested_period_when_response_context_is_absent(self) -> None:
        raw = self._periodless_bar(timestamp_minutes=int(BASE.timestamp() // 60))
        provider = self._provider(
            {
                "symbolId": 99,
                "trendbar": [raw],
                "hasMore": False,
            }
        )

        result = provider.fetch("M15", count=1)
        self.assertEqual(len(result.bars), 1)
        self.assertEqual(result.bars[0].resolution, "M15")
        self.assertEqual(result.bars[0].metadata["period_source"], "request")
        self.assertEqual(result.bars[0].metadata["requested_period"], 7)
        self.assertIsNone(result.bars[0].metadata["response_period"])
        self.assertEqual(result.issues, ())

    def test_fetch_rejects_response_period_discrepancy_without_relabeling(self) -> None:
        raw = self._periodless_bar(timestamp_minutes=int(BASE.timestamp() // 60))
        provider = self._provider(
            {
                "period": 7,
                "symbolId": 99,
                "trendbar": [raw],
                "hasMore": False,
            }
        )

        result = provider.fetch("M5", count=1)
        self.assertEqual(result.bars, ())
        self.assertTrue(any("requested=5, response=7" in issue for issue in result.issues))

        history = provider.fetch_history("M5", count=1, max_pages=1)
        self.assertEqual(history.bars, ())
        self.assertFalse(history.complete)
        self.assertTrue(any("cobertura incompleta" in issue for issue in history.issues))

    def test_fetch_rejects_observed_bar_period_discrepancy(self) -> None:
        raw = synthetic_trendbar(timestamp_minutes=int(BASE.timestamp() // 60), period="M15")
        provider = self._provider(
            {
                "period": 5,
                "symbolId": 99,
                "trendbar": [raw],
                "hasMore": False,
            }
        )

        result = provider.fetch("M5", count=1)
        self.assertEqual(result.bars, ())
        self.assertTrue(any("requested=5, bar=7" in issue for issue in result.issues))

        with self.assertRaisesRegex(CTraderDataError, r"response=5, bar=7"):
            normalize_trendbar(
                raw,
                spec=CTraderInstrumentSpec(symbol_id=99),
                response_period="M5",
                received_at=BASE,
            )


if __name__ == "__main__":
    unittest.main()
