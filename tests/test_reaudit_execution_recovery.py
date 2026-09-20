from __future__ import annotations

import dataclasses
import json
import tempfile
import unittest
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from mtf_lab.ops.ctrader_account_risk import AccountRiskObserver
from mtf_lab.ops.ctrader_demo_composition import OfflineCompositionError, recover_execution_intents
from mtf_lab.ops.ctrader_executor import (
    CorrelationError,
    CTraderDemoExecutor,
    DecimalValue,
    DemoAccount,
    DemoTransport,
    ExecutionIntent,
    ExecutionPolicy,
    Fill,
    MemoryIntentStore,
    OrderResult,
    OrderState,
    Quote,
    RiskLimitRejected,
    Side,
)

NOW = datetime(2026, 9, 13, 12, tzinfo=UTC)


class ReauditExecutionRecoveryTests(unittest.TestCase):
    def test_cashflow_zero_money_digits_is_valid_and_missing_digits_stays_unknown(self) -> None:
        raw: dict[str, Any] = {
            "delta": 100,
            "changeBalanceTimestamp": int(NOW.timestamp() * 1000),
            "operationType": "DEPOSIT",
            "moneyDigits": 0,
            "balance": 1000,
        }
        record, _digest, signed, reasons = AccountRiskObserver._cashflow_record(
            raw,
            cashflow_id="cashflow-zero-digits",
            day_start=datetime(2026, 9, 13, tzinfo=UTC),
            now=NOW,
            trader_digits=2,
        )
        self.assertEqual(signed, Decimal("100"))
        self.assertIsNotNone(record)
        assert record is not None
        self.assertEqual(record["balance"], "1000")
        self.assertEqual(reasons, [])

        missing = dict(raw)
        missing.pop("moneyDigits")
        record, _digest, signed, reasons = AccountRiskObserver._cashflow_record(
            missing,
            cashflow_id="cashflow-fallback",
            day_start=datetime(2026, 9, 13, tzinfo=UTC),
            now=NOW,
            trader_digits=2,
        )
        self.assertEqual(signed, Decimal("1.00"))
        self.assertIsNotNone(record)
        self.assertEqual(reasons, [])

        no_digits = dict(missing)
        record, _digest, signed, reasons = AccountRiskObserver._cashflow_record(
            no_digits,
            cashflow_id="cashflow-unknown",
            day_start=datetime(2026, 9, 13, tzinfo=UTC),
            now=NOW,
            trader_digits=None,
        )
        self.assertIsNone(record)
        self.assertIsNone(signed)
        self.assertIn("cashflow_money_digits_unobserved", reasons)

    def _legacy_executor(self, *, max_positions: int, max_exposure: int) -> tuple[CTraderDemoExecutor, DemoTransport]:
        account = DemoAccount(
            "demo-1",
            "DEMO",
            "demo://ctrader",
            frozenset({"trading"}),
            selected=True,
            verified=True,
        )
        transport = DemoTransport(
            account_id=account.account_id,
            endpoint=account.endpoint,
            scopes=account.scopes,
            default_behavior="partial",
            clock=lambda: NOW,
        )
        executor = CTraderDemoExecutor(
            account,
            policy=ExecutionPolicy(
                max_quantity=DecimalValue("1"),
                fixed_quantity=DecimalValue("1"),
                max_positions=max_positions,
                max_inflight_intents=1,
                max_exposure=DecimalValue(str(max_exposure)),
                max_spread=DecimalValue("0.2"),
                max_price_age_seconds=DecimalValue("10"),
                timeout_seconds=DecimalValue("1"),
            ),
            transport=transport,
            clock=lambda: NOW,
        )
        executor.activate()
        return executor, transport

    @staticmethod
    def _quote() -> Quote:
        return Quote(
            "EUR/USD",
            DecimalValue("99.9"),
            DecimalValue("100.1"),
            NOW,
            NOW,
            quality="VALID",
            source="fixture",
        )

    @staticmethod
    def _signal(signal_id: str) -> dict[str, str]:
        return {"signal_id": signal_id, "instrument": "EUR/USD", "direction": "UP", "mode": "DEMO"}

    def test_partial_result_reserves_entry_slot_until_reconciled(self) -> None:
        executor, transport = self._legacy_executor(max_positions=2, max_exposure=151)
        first = executor.submit_signal(self._signal("partial-one"), self._quote())
        self.assertEqual(first.state, OrderState.PARTIAL)
        with self.assertRaisesRegex(RiskLimitRejected, "unresolved order"):
            executor.submit_signal(self._signal("partial-two"), self._quote())

        transport.advance_partial(first.intent.intent_id)
        reconciled = executor.manage()
        self.assertEqual(len(reconciled), 1)
        self.assertEqual(reconciled[0].state, OrderState.FILLED)

    def test_current_one_position_policy_still_blocks_second_entry(self) -> None:
        executor, _transport = self._legacy_executor(max_positions=1, max_exposure=1000)
        first = executor.submit_signal(self._signal("canary-one"), self._quote())
        self.assertEqual(first.state, OrderState.PARTIAL)
        with self.assertRaises(RiskLimitRejected):
            executor.submit_signal(self._signal("canary-two"), self._quote())

    @staticmethod
    def _journal_intent() -> ExecutionIntent:
        return ExecutionIntent(
            "intent-recovery",
            "signal-recovery",
            "EUR/USD",
            Side.BUY,
            DecimalValue("1"),
            DecimalValue("1.1001"),
            NOW,
            "demo-1",
            metadata={
                "risk_entry_bar_count": 100,
                "risk_entry_bar_count_frozen": False,
                "order_options": {"stop_loss": "1.0990", "take_profit": "1.1010"},
            },
        )

    @classmethod
    def _journal_update(cls, *, updated: bool = True) -> tuple[ExecutionIntent, dict[str, Any]]:
        original = cls._journal_intent()
        metadata = dict(original.metadata)
        metadata["risk_entry_bar_count"] = 105 if updated else 100
        metadata["risk_entry_bar_count_frozen"] = updated
        metadata["risk_entry_bar_count_source"] = (
            "RUNTIME_UTC_ORDINAL_AT_FILL" if updated else "RUNTIME_UTC_ORDINAL_AT_INTENT"
        )
        updated_intent = dataclasses.replace(original, metadata=metadata)
        fill = Fill("fill-recovery", DecimalValue("1"), DecimalValue("1.1001"), NOW)
        result = OrderResult(
            updated_intent,
            OrderState.FILLED,
            "order-recovery",
            DecimalValue("1"),
            (fill,),
            position_ids=("position-recovery",),
            stop_loss=DecimalValue("1.0990"),
            take_profit=DecimalValue("1.1010"),
        )
        return original, {"journal_type": "update", "intent_id": original.intent_id, **result.to_dict()}

    @staticmethod
    def _recover(path: Path) -> CTraderDemoExecutor:
        account = DemoAccount(
            "demo-1",
            "DEMO",
            "demo://ctrader",
            frozenset({"trading"}),
            selected=True,
            verified=True,
        )
        return CTraderDemoExecutor(
            account,
            policy=ExecutionPolicy(
                max_positions=2,
                max_exposure=DecimalValue("1000"),
                max_spread=DecimalValue("1"),
                max_price_age_seconds=DecimalValue("10"),
            ),
            transport=DemoTransport(account_id=account.account_id, endpoint=account.endpoint, scopes=account.scopes),
            intent_store=MemoryIntentStore(),
        )

    def test_recovery_restores_mutable_intent_and_protection_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            path = Path(raw_tmp) / "intents.jsonl"
            original, update = self._journal_update()
            path.write_text(
                json.dumps({"journal_type": "intent", **original.to_dict()}) + "\n" + json.dumps(update) + "\n",
                encoding="utf-8",
            )
            executor = self._recover(path)
            recovered = recover_execution_intents(path, executor)
            self.assertEqual(recovered[0].metadata["risk_entry_bar_count"], 105)
            result = executor.result(original.intent_id)
            assert result is not None
            self.assertTrue(result.intent.metadata["risk_entry_bar_count_frozen"])
            self.assertEqual(result.protection_state, "OBSERVED")
            self.assertEqual(result.stop_loss, DecimalValue("1.0990"))
            self.assertEqual(result.take_profit, DecimalValue("1.1010"))

    def test_recovery_keeps_legacy_update_without_nested_intent_compatible(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            path = Path(raw_tmp) / "legacy-update.jsonl"
            original, update = self._journal_update()
            update.pop("intent")
            path.write_text(
                json.dumps({"journal_type": "intent", **original.to_dict()}) + "\n" + json.dumps(update) + "\n",
                encoding="utf-8",
            )
            executor = self._recover(path)
            recover_execution_intents(path, executor)
            result = executor.result(original.intent_id)
            assert result is not None
            self.assertEqual(result.intent.metadata["risk_entry_bar_count"], 100)
            self.assertEqual(result.protection_state, "OBSERVED")

    def test_recovery_rejects_immutable_intent_mutation_and_protection_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            root = Path(raw_tmp)
            original, update = self._journal_update()
            tampered_intent = dict(update["intent"])
            tampered_intent["quantity"] = "2"
            tampered = {**update, "intent": tampered_intent}
            path = root / "bad-identity.jsonl"
            path.write_text(
                json.dumps({"journal_type": "intent", **original.to_dict()}) + "\n" + json.dumps(tampered) + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(OfflineCompositionError, "immutable intent field changed"):
                recover_execution_intents(path, self._recover(path))

            bad_protection = dict(update)
            bad_protection["stop_loss"] = "1.0980"
            path = root / "bad-protection.jsonl"
            path.write_text(
                json.dumps({"journal_type": "intent", **original.to_dict()}) + "\n" + json.dumps(bad_protection) + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(OfflineCompositionError, "SL/TP mismatch"):
                recover_execution_intents(path, self._recover(path))

    def test_recovery_rejects_fill_overflow_and_duplicate_ids(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            root = Path(raw_tmp)
            original, update = self._journal_update()
            fill = dict(update["fills"][0])
            overflow = {**update, "filled_quantity": "1", "fills": [fill, {**fill, "fill_id": "fill-two"}]}
            path = root / "fill-overflow.jsonl"
            path.write_text(
                json.dumps({"journal_type": "intent", **original.to_dict()}) + "\n" + json.dumps(overflow) + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(OfflineCompositionError, "invalid outcome update"):
                recover_execution_intents(path, self._recover(path))

            regressed = {**update, "filled_quantity": "0.5", "fills": []}
            path = root / "fill-regressed.jsonl"
            path.write_text(
                json.dumps({"journal_type": "intent", **original.to_dict()})
                + "\n"
                + json.dumps(update)
                + "\n"
                + json.dumps(regressed)
                + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(OfflineCompositionError, "filled quantity regressed"):
                recover_execution_intents(path, self._recover(path))

            zero_terminal = {**update, "filled_quantity": "0", "fills": [], "order_id": "order-recovery"}
            path = root / "zero-terminal.jsonl"
            path.write_text(
                json.dumps({"journal_type": "intent", **original.to_dict()}) + "\n" + json.dumps(zero_terminal) + "\n",
                encoding="utf-8",
            )
            executor = self._recover(path)
            recover_execution_intents(path, executor)
            recovered = executor.result(original.intent_id)
            assert recovered is not None
            self.assertEqual(recovered.state, OrderState.UNKNOWN)

            duplicate = {**update, "filled_quantity": "1", "fills": [fill, fill]}
            path = root / "fill-duplicate.jsonl"
            path.write_text(
                json.dumps({"journal_type": "intent", **original.to_dict()}) + "\n" + json.dumps(duplicate) + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(OfflineCompositionError, "invalid outcome update"):
                recover_execution_intents(path, self._recover(path))

    def test_recovery_rejects_created_at_and_risk_plan_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            root = Path(raw_tmp)
            original, update = self._journal_update()
            created_at_intent = dict(update["intent"])
            created_at_intent["created_at"] = "2025-09-13T12:00:00.000Z"
            path = root / "created-at.jsonl"
            path.write_text(
                json.dumps({"journal_type": "intent", **original.to_dict()})
                + "\n"
                + json.dumps({**update, "intent": created_at_intent})
                + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(OfflineCompositionError, "created_at"):
                recover_execution_intents(path, self._recover(path))

            original_metadata = dict(original.metadata)
            original_metadata["risk_exit_plan"] = {
                "allowed": False,
                "eligible_for_demo": False,
                "mode": "DEMO_GATED",
                "policy_hash": "policy-original",
                "initial_stop": "1.0990",
                "take_profit": "1.1010",
            }
            original = dataclasses.replace(original, metadata=original_metadata)
            updated_metadata = dict(original.metadata)
            updated_plan = dict(updated_metadata["risk_exit_plan"])
            updated_plan["allowed"] = True
            updated_plan["eligible_for_demo"] = True
            updated_metadata["risk_exit_plan"] = updated_plan
            updated_intent = dataclasses.replace(original, metadata=updated_metadata)
            fill = Fill("fill-plan", DecimalValue("1"), DecimalValue("1.1001"), NOW)
            result = OrderResult(
                updated_intent,
                OrderState.FILLED,
                "order-plan",
                DecimalValue("1"),
                (fill,),
                position_ids=("position-plan",),
                stop_loss=DecimalValue("1.0990"),
                take_profit=DecimalValue("1.1010"),
            )
            path = root / "risk-plan.jsonl"
            path.write_text(
                json.dumps({"journal_type": "intent", **original.to_dict()})
                + "\n"
                + json.dumps({"journal_type": "update", "intent_id": original.intent_id, **result.to_dict()})
                + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(OfflineCompositionError, "immutable intent metadata changed: risk_exit_plan"):
                recover_execution_intents(path, self._recover(path))

    def test_restore_intent_rejects_result_symbol_or_signal_mismatch(self) -> None:
        original = self._journal_intent()
        executor = self._recover(Path("/tmp/unused-recovery-test-path"))
        for field, value in (("symbol", "GBP/USD"), ("signal_id", "other-signal")):
            with self.subTest(field=field):
                result_intent = (
                    dataclasses.replace(original, symbol=value)
                    if field == "symbol"
                    else dataclasses.replace(original, signal_id=value)
                )
                result = OrderResult(result_intent, OrderState.UNKNOWN)
                with self.assertRaises(CorrelationError):
                    executor.restore_intent(original, result=result)
                self.assertNotIn(original.intent_id, executor._intents)


if __name__ == "__main__":
    unittest.main()
