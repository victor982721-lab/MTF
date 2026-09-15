"""Descriptive market measurements never imply fills, full sessions, or edge."""

from __future__ import annotations

import tempfile
import unittest
import zipfile
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from mtf_lab.data.historical import HistoricalQuote, iter_quotes, manifest_from_histdata_archive
from mtf_lab.ops.market_structure import BoundedDistribution, describe_market_structure, quote_event


class _OnePass:
    def __init__(self, values: Iterator[HistoricalQuote]) -> None:
        self.values = values
        self.calls = 0

    def __iter__(self) -> Iterator[HistoricalQuote]:
        self.calls += 1
        if self.calls > 1:
            raise AssertionError("the historical stream was reread")
        yield from self.values


class MarketStructureTests(unittest.TestCase):
    def test_quantiles_are_intervals_and_overflow_is_not_a_false_point(self) -> None:
        distribution = BoundedDistribution(width=Decimal(".1"), bins=10)
        distribution.add(Decimal(".15"))
        distribution.add(Decimal(".25"))
        distribution.add(Decimal("8"))
        result = distribution.to_dict()
        self.assertEqual(result["n"], 3)
        self.assertEqual(result["quantiles"]["p50"]["lower"], "0.2")
        self.assertIsNone(result["quantiles"]["p99"]["upper_exclusive"])
        self.assertEqual(result["overflow_count"], 1)
        self.assertEqual(distribution.histogram.total(), 3)

    def test_real_adapter_shape_is_streamed_once_and_unknowns_stay_unknown(self) -> None:
        start = datetime(2016, 3, 7)
        rows = []
        for index in range(90):
            when = start + timedelta(seconds=index * 30, milliseconds=100)
            price = Decimal("1.1") + Decimal(index % 5) / 100000
            rows.append(f"{when:%Y%m%d %H%M%S}{when.microsecond // 1000:03d},{price},{price + Decimal('.0001')},0\n")
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            raw = root / "raw"
            raw.mkdir()
            archive = raw / "HISTDATA_COM_ASCII_EURUSD_T_201603.zip"
            with zipfile.ZipFile(archive, "w") as output:
                output.writestr("DAT_ASCII_EURUSD_T_201603.csv", "".join(rows))
            manifest = manifest_from_histdata_archive(
                archive,
                data_root=root,
                acquired_at=datetime(2026, 9, 13, tzinfo=UTC),
                source_uri="https://www.histdata.com/",
                terms_uri="https://www.histdata.com/f-a-q/",
            )
            stream = _OnePass(iter_quotes(manifest))
            report = describe_market_structure(stream, manifest=manifest)
            first = next(iter_quotes(manifest))
        self.assertEqual(stream.calls, 1)
        self.assertEqual(report["quote_statistics"]["quote_count"], 90)
        self.assertEqual(report["quote_statistics"]["spread_tick_weighted"]["mean"], "1.0")
        self.assertEqual(report["quote_statistics"]["days_with_quotes"][0]["full_session"], "UNKNOWN")
        self.assertIsNone(report["volume"])
        self.assertEqual(report["profitability_claim"], "NONE")
        self.assertIsNone(quote_event(first).received_at)
        self.assertIsNone(quote_event(first).quantity)
        self.assertGreater(report["timeframes"]["M1"]["coverage_valid_bars"], 0)
        self.assertGreater(report["timeframes"]["M1"]["spread_atr"]["n"], 0)
        self.assertEqual(report["timeframes"]["D1"]["spread_atr"]["n"], 0)
        self.assertIsNone(report["timeframes"]["D1"]["returns"]["sqrt_sum_squared_log_returns"])


if __name__ == "__main__":
    unittest.main()
