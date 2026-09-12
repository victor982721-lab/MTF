from __future__ import annotations

import unittest
from collections.abc import Set
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, cast

from mtf_lab.core import OperationMode, Signal
from mtf_lab.ops.ctrader_activation import (
    ActivationMode,
    ActivationProfile,
    ActivationState,
    ActivationStatus,
    BrokerAccount,
    LoopbackOAuthAssistant,
    OAuthAppConfig,
    OAuthAttemptStore,
    RealAccountForbidden,
    SecureTokenStore,
    TokenMetadata,
    evaluate_activation,
)
from mtf_lab.ops.ctrader_executor import (
    CTraderDemoExecutor,
    DecimalValue,
    DemoAccount,
    Quote,
    _as_mapping,
)
from mtf_lab.ops.ctrader_executor import (
    RealAccountForbidden as ExecutorRealAccountForbidden,
)

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def app_config() -> OAuthAppConfig:
    return OAuthAppConfig(
        client_id_env="CTRADER_CLIENT_ID",
        client_secret_env="CTRADER_CLIENT_SECRET",
        redirect_uri="http://127.0.0.1:8765/callback",
        authorization_url="https://id.example.test/oauth/authorize",
        token_url="https://id.example.test/oauth/token",
    )


def profile(mode: ActivationMode, *, selected: bool = True, token_ref: str = "fixture-token") -> ActivationProfile:
    scopes = {"accounts"} if mode is ActivationMode.QUERY else {"accounts", "trading"}
    return ActivationProfile(
        enabled=True,
        operation_mode=mode,
        environment="DEMO",
        required_scopes=frozenset(scopes),
        account_id="demo-1" if selected else "",
        account_selected=selected,
        token_ref=token_ref,
    )


def token(
    *,
    scopes: Set[str] = frozenset({"accounts", "trading"}),
    token_ref: str = "fixture-token",
    expires_at: datetime = NOW + timedelta(hours=1),
) -> TokenMetadata:
    return TokenMetadata(token_ref, frozenset(scopes), expires_at, NOW, 1)


def demo_account(*, environment: str = "DEMO") -> BrokerAccount:
    return BrokerAccount("demo-1", environment, "Fixture Demo", frozenset({"accounts", "trading"}))


class ActivationDecisionTableTests(unittest.TestCase):
    def assert_state(
        self,
        expected: ActivationState,
        current: ActivationStatus,
        *,
        ready: bool = False,
    ) -> None:
        self.assertEqual(current.state, expected)
        self.assertEqual(current.ready, ready)

    def evaluate(
        self,
        mode: ActivationMode = ActivationMode.QUERY,
        *,
        current_token: TokenMetadata | None = None,
        accounts: tuple[BrokerAccount, ...] = (),
        selected: bool = True,
        token_ref: str = "fixture-token",
        present: Set[str] = frozenset({"CTRADER_CLIENT_ID", "CTRADER_CLIENT_SECRET"}),
    ) -> ActivationStatus:
        return evaluate_activation(
            profile(mode, selected=selected, token_ref=token_ref),
            token=current_token,
            accounts=accounts,
            present_env_keys=present,
            app=app_config(),
            now=NOW,
        )

    def test_preflight_and_account_gates_are_fail_closed(self) -> None:
        disabled = evaluate_activation(
            ActivationProfile(
                enabled=False,
                operation_mode=ActivationMode.QUERY,
                environment="DEMO",
                required_scopes=frozenset({"accounts"}),
            ),
            token=None,
            accounts=(),
            present_env_keys=frozenset(),
            app=app_config(),
            now=NOW,
        )
        self.assert_state(ActivationState.DISABLED, disabled)
        self.assert_state(ActivationState.APP_CREDENTIALS_REQUIRED, self.evaluate(present=frozenset()))
        self.assert_state(ActivationState.ACCOUNTS_SCOPE_REQUIRED, self.evaluate())
        self.assert_state(
            ActivationState.TOKEN_REFERENCE_REQUIRED,
            self.evaluate(current_token=token(), token_ref="other-token"),
        )
        self.assert_state(
            ActivationState.TOKEN_EXPIRED,
            self.evaluate(current_token=expires_at_token()),
        )
        self.assert_state(
            ActivationState.ACCOUNTS_SCOPE_REQUIRED,
            self.evaluate(current_token=token(scopes=frozenset({"trading"}))),
        )
        self.assert_state(
            ActivationState.ACCOUNT_DISCOVERY_REQUIRED,
            self.evaluate(current_token=token(), accounts=()),
        )
        self.assert_state(
            ActivationState.DEMO_ACCOUNT_SELECTION_REQUIRED,
            self.evaluate(current_token=token(), accounts=(demo_account(),), selected=False),
        )
        self.assert_state(
            ActivationState.ACCOUNT_SELECTION_INVALID,
            self.evaluate(current_token=token(), accounts=(BrokerAccount("other", "DEMO"),)),
        )

    def test_explicit_real_account_is_never_accepted(self) -> None:
        with self.assertRaises(RealAccountForbidden):
            from mtf_lab.ops.ctrader_activation import select_demo_account

            select_demo_account((demo_account(environment="REAL"),), "demo-1", environment="DEMO")

        self.assert_state(
            ActivationState.REAL_ACCOUNT_FORBIDDEN,
            self.evaluate(current_token=token(), accounts=(demo_account(environment="REAL"),)),
        )

    def test_query_and_demo_readiness_require_their_exact_scopes(self) -> None:
        query_ready = self.evaluate(current_token=token(scopes=frozenset({"accounts"})), accounts=(demo_account(),))
        self.assert_state(ActivationState.QUERY_READY, query_ready, ready=True)

        missing_trading = self.evaluate(
            ActivationMode.DEMO,
            current_token=token(scopes=frozenset({"accounts"})),
            accounts=(demo_account(),),
        )
        self.assert_state(ActivationState.TRADING_SCOPE_REQUIRED, missing_trading)

        demo_ready = self.evaluate(ActivationMode.DEMO, current_token=token(), accounts=(demo_account(),))
        self.assert_state(ActivationState.DEMO_READY, demo_ready, ready=True)

    def test_status_never_contains_token_values(self) -> None:
        status = self.evaluate(current_token=token(scopes=frozenset({"accounts"})), accounts=(demo_account(),))
        rendered = str(status.to_dict())
        self.assertNotIn("access", rendered.lower())
        self.assertNotIn("refresh", rendered.lower())
        self.assertNotIn("fixture-token-secret", rendered)

    def test_callback_listener_rejects_invalid_timeout_before_binding(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            tokens = SecureTokenStore.for_fixture(root / "tokens", project_root=Path.cwd())
            attempts = OAuthAttemptStore.for_fixture(root / "attempts", project_root=Path.cwd())
            assistant = LoopbackOAuthAssistant(app_config(), attempts=attempts, tokens=tokens, fixture_mode=True)
            attempt = assistant.begin(
                client_id="fixture-client",
                requested_scopes=("accounts",),
                now=NOW,
            )
            with self.assertRaisesRegex(Exception, "timeout_seconds"):
                assistant.listen_callback(attempt.attempt_id, timeout_seconds=0, now=NOW)


class DecimalOperatorContractTests(unittest.TestCase):
    def test_unsupported_operands_return_notimplemented_and_bool_is_rejected(self) -> None:
        value = DecimalValue("2")
        operations = (
            value.__add__,
            value.__sub__,
            value.__rsub__,
            value.__mul__,
            value.__truediv__,
            value.__rtruediv__,
        )
        for operation in operations:
            with self.subTest(operation=operation.__name__):
                self.assertIs(operation(object()), NotImplemented)
        with self.assertRaisesRegex(TypeError, "boolean"):
            DecimalValue._operand(True)

    def test_unsupported_addition_keeps_reverse_dispatch(self) -> None:
        class ReverseOperand:
            def __radd__(self, other: Any) -> str:
                return "reverse-dispatched"

        result = cast(Any, DecimalValue("2")) + ReverseOperand()
        self.assertEqual(result, "reverse-dispatched")

    def test_signal_serializer_normalizes_live_mode_before_executor_gate(self) -> None:
        signal = Signal(
            "signal-live",
            "EUR/USD",
            "UP",
            NOW,
            NOW,
            NOW,
            NOW,
            NOW + timedelta(minutes=1),
            "episode-live",
            mode=OperationMode.LIVE,
        )
        self.assertEqual(_as_mapping(signal)["mode"], "LIVE")
        executor = CTraderDemoExecutor(
            DemoAccount("demo-1", "DEMO", "demo://ctrader", frozenset({"trading"}), selected=True, verified=True)
        )
        quote = Quote("EUR/USD", DecimalValue("1.1000"), DecimalValue("1.1002"), NOW)
        with self.assertRaises(ExecutorRealAccountForbidden):
            executor._make_intent(signal, quote, None)
        for index, mode in enumerate((OperationMode.LIVE, "  LIVE  ")):
            with self.subTest(mapping=index), self.assertRaises(ExecutorRealAccountForbidden):
                executor._make_intent(
                    {
                        "signal_id": f"legacy-live-{index}",
                        "instrument": "EUR/USD",
                        "direction": "UP",
                        "mode": mode,
                    },
                    quote,
                    None,
                )


def expires_at_token() -> TokenMetadata:
    return token(expires_at=NOW - timedelta(seconds=1))


if __name__ == "__main__":
    unittest.main()
