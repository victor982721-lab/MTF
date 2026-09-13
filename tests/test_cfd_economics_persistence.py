"""Focused persistence/query contracts for versioned CFD economics."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from mtf_lab.core.canonical import canonical_json, fingerprint
from mtf_lab.core.cfd_simulation import (
    CFD_ECONOMICS_LEGACY_VERSION,
    CFD_ECONOMICS_VERSION,
    CFDConfig,
    CFDQuote,
    CFDSignal,
    CFDSimulator,
    CFDTrade,
    Direction,
)
from mtf_lab.ops.persistence import (
    _CFD_LEGACY_SEMANTIC_FIELDS,
    IdempotencyConflict,
    SQLiteStore,
)
from mtf_lab.ops.query import QueryService

BASE = datetime(2026, 1, 1, tzinfo=UTC)


def v2_trade() -> CFDTrade:
    config = CFDConfig(
        instrument="EUR/USD",
        units=Decimal("1000"),
        horizons_seconds=(Decimal("60"),),
        commission_fixed=Decimal("0.05"),
        slippage_pips=Decimal("1"),
        economics_version=CFD_ECONOMICS_VERSION,
    )
    signal = CFDSignal("persist-v2", "EUR/USD", Direction.LONG, BASE)
    quotes = (
        CFDQuote("EUR/USD", BASE, Decimal("1.1000"), Decimal("1.1002"), "entry"),
        CFDQuote(
            "EUR/USD",
            BASE + timedelta(seconds=60),
            Decimal("1.1005"),
            Decimal("1.1007"),
            "close",
        ),
    )
    return CFDSimulator(config).replay([signal], quotes).trades[0]


class CFDEconomicsPersistenceTests(unittest.TestCase):
    def test_v2_fields_are_kept_in_payload_and_explicit_projections(self) -> None:
        with (
            tempfile.TemporaryDirectory(prefix="mtf-cfd-persistence-") as tmp,
            SQLiteStore(Path(tmp) / "isolated.sqlite3") as store,
        ):
            session_id = store.create_session(
                mode="REPLAY", provider="fixture", instrument="EUR/USD", config={"fixture": True}
            )
            analysis_id = store.create_analysis(
                session_id, dataset_hash="dataset-v2", config_hash="config-v2", variant="paper"
            )
            trade = v2_trade()
            self.assertTrue(
                store.save_cfd_trade(
                    session_id,
                    analysis_id,
                    trade.to_dict(),
                    variant="paper",
                    partition="research",
                )
            )
            raw_payload = store.conn.execute(
                "SELECT payload_json FROM cfd_trades WHERE session_id=? AND analysis_id=? AND trade_id=?",
                (session_id, analysis_id, trade.trade_id),
            ).fetchone()[0]
            payload = json.loads(raw_payload)
            self.assertEqual(payload["economics_version"], CFD_ECONOMICS_VERSION)
            self.assertEqual(payload["entry_reference_price"], "1.10020")
            self.assertEqual(payload["close_reference_price"], "1.10050")
            self.assertEqual(payload["reference_gross_pnl_quote"], "0.30000")
            self.assertEqual(payload["slippage_quote"], "0.20000")

            persisted = store.list_cfd_trades(session_id, analysis_id)[0]
            self.assertEqual(persisted["economics_version"], CFD_ECONOMICS_VERSION)
            self.assertEqual(persisted["entry_reference_price"], "1.10020")
            self.assertEqual(persisted["close_reference_price"], "1.10050")
            self.assertEqual(persisted["reference_gross_pnl_quote"], "0.30000")
            self.assertEqual(persisted["slippage_quote"], "0.20000")
            self.assertEqual(persisted["economic_result"]["economics_version"], CFD_ECONOMICS_VERSION)

            page = QueryService(store).query_cfd_trades(session_id, analysis_id=analysis_id)
            projected = page.items[0]
            self.assertEqual(projected["economics_version"], CFD_ECONOMICS_VERSION)
            self.assertEqual(projected["entry_reference_price"], "1.10020")
            self.assertEqual(projected["close_reference_price"], "1.10050")
            self.assertEqual(projected["reference_gross_pnl_quote"], "0.30000")
            self.assertEqual(projected["slippage_quote"], "0.20000")

    def test_pre_h1_payload_is_read_as_legacy_without_rewriting_or_hash_upgrade(self) -> None:
        config = CFDConfig(
            instrument="EUR/USD",
            units=Decimal("1000"),
            horizons_seconds=(Decimal("60"),),
            economics_version=CFD_ECONOMICS_LEGACY_VERSION,
        )
        signal = CFDSignal("persist-legacy", "EUR/USD", Direction.LONG, BASE)
        pending = CFDSimulator(config).submit(signal)

        with (
            tempfile.TemporaryDirectory(prefix="mtf-cfd-legacy-") as tmp,
            SQLiteStore(Path(tmp) / "isolated.sqlite3") as store,
        ):
            session_id = store.create_session(mode="REPLAY", provider="fixture", instrument="EUR/USD")
            analysis_id = store.create_analysis(
                session_id, dataset_hash="dataset-legacy", config_hash="config-legacy", variant="paper"
            )
            self.assertTrue(store.save_cfd_trade(session_id, analysis_id, pending.to_dict()))
            row = store.conn.execute(
                "SELECT payload_json FROM cfd_trades WHERE session_id=? AND analysis_id=? AND trade_id=?",
                (session_id, analysis_id, pending.trade_id),
            ).fetchone()
            self.assertIsNotNone(row)
            raw = json.loads(row[0])
            for field in (
                "economics_version",
                "entry_reference_price",
                "close_reference_price",
                "reference_gross_pnl_quote",
            ):
                raw.pop(field, None)
            lineage = raw.get("lineage")
            if isinstance(lineage, dict):
                lineage.pop("economics_version", None)
            legacy_semantic = {field: raw.get(field) for field in _CFD_LEGACY_SEMANTIC_FIELDS}
            raw_text = canonical_json(raw)
            legacy_hash = fingerprint(legacy_semantic)
            store.conn.execute(
                "UPDATE cfd_trades SET payload_json=?, semantic_hash=? WHERE session_id=? AND analysis_id=? AND trade_id=?",
                (raw_text, legacy_hash, session_id, analysis_id, pending.trade_id),
            )
            store.conn.commit()

            before = store.conn.execute(
                "SELECT payload_json,semantic_hash FROM cfd_trades WHERE session_id=? AND analysis_id=? AND trade_id=?",
                (session_id, analysis_id, pending.trade_id),
            ).fetchone()
            listed = store.list_cfd_trades(session_id, analysis_id)[0]
            self.assertEqual(listed["economics_version"], CFD_ECONOMICS_LEGACY_VERSION)
            self.assertIsNone(listed["entry_reference_price"])
            self.assertIsNone(listed["close_reference_price"])
            self.assertIsNone(listed["reference_gross_pnl_quote"])
            after_read = store.conn.execute(
                "SELECT payload_json,semantic_hash FROM cfd_trades WHERE session_id=? AND analysis_id=? AND trade_id=?",
                (session_id, analysis_id, pending.trade_id),
            ).fetchone()
            self.assertEqual(tuple(after_read), tuple(before))

            self.assertFalse(store.save_cfd_trade(session_id, analysis_id, pending.to_dict()))
            after_duplicate = store.conn.execute(
                "SELECT payload_json,semantic_hash FROM cfd_trades WHERE session_id=? AND analysis_id=? AND trade_id=?",
                (session_id, analysis_id, pending.trade_id),
            ).fetchone()
            self.assertEqual(tuple(after_duplicate), tuple(before))

            v2_mapping = pending.to_dict()
            v2_mapping["economics_version"] = CFD_ECONOMICS_VERSION
            with self.assertRaises(IdempotencyConflict):
                store.save_cfd_trade(session_id, analysis_id, v2_mapping)


if __name__ == "__main__":
    unittest.main()
