"""Operational services for MTF Lab.

This package deliberately contains no market-specific calculations.  It stores
the observations produced by the core, evaluates virtual outcomes using an
explicit data contract, and exposes reporting/CLI/UI adapters.  The public
classes accept ordinary mappings as well as dataclass-like objects so that the
core can evolve without making persistence a second implementation of it.
"""

from .backtest import BacktestResult, BacktestRunner, PortfolioResult, VariantSpec
from .persistence import SQLiteStore
from .reporting import ReportBuilder
from .cfd_simulation import (
    CFDConfig, CFDQuote, CFDReplayResult, CFDSignal, CFDSimulationError, CFDSimulator,
    CFDTrade, Direction, TradeState, ForexCFDConfig, ForexCFDQuote, ForexCFDSignal,
    ForexCFDSimulator, ForexCFDTrade, known_fixture_eurusd_long,
)
from .ctrader_executor import (
    CTraderDemoExecutor, DemoAccount, DemoTransport, ExecutionPolicy, ExecutionIntent,
    ExecutionEvent, Fill, IntentStore, MemoryIntentStore, JsonlIntentStore, OrderResult,
    OrderSnapshot, OrderState, Position, Quote, Side, verify_demo_account,
)
from .ctrader_demo_transport import (
    CTraderDemoTransport, CTraderOfficialDemoTransport, CTraderDemoTransportConfig,
    DEMO_PROTOBUF_ENDPOINT, LIVE_PROTOBUF_ENDPOINT, ServerAccountObservation, OfficialCorrelationError,
    OfficialGateway, OfficialMessageError, OfficialSDKUnavailable, OfficialTransportError,
    VOLUME_SCALE, load_official_proto,
)
from .ctrader_pipeline import (
    CTraderCapture, CTraderPipeline, CTraderPipelineError, CTraderPipelineResult,
    PAPER_PRODUCT, PAPER_VARIANT, normalize_ctrader_capture, signal_to_cfd_signal,
    spot_event_to_cfd_quote, synthetic_ctrader_capture, synthetic_ctrader_payloads,
)
from .ctrader_demo import run_ctrader_fixture
from .ctrader_activation import (
    AccountDiscovery, ActivationProfile, ActivationMode, ActivationState, ActivationStatus, BrokerAccount,
    LoopbackOAuthAssistant, OAuthAttempt, OAuthAttemptPhase, OAuthAttemptStore, OAuthAppConfig, OAuthTokenPayload, SecureTokenStore, TokenLease, TokenMetadata, UnsafeTokenStore,
    build_authorization_url, open_authorization_browser, parse_callback_uri, exchange_authorization_code, refresh_access_token,
    evaluate_activation, record_account_discovery, select_demo_account, select_discovered_demo_account,
)

from .simulation import (
    DirectionalEvaluator,
    EvaluationSpec,
    Outcome,
    VirtualContract,
    VirtualContractSimulator,
)

__all__ = [
    "BacktestResult",
    "BacktestRunner",
    "PortfolioResult",
    "DirectionalEvaluator",
    "EvaluationSpec",
    "Outcome",
    "ReportBuilder",
    "SQLiteStore",
    "VariantSpec",
    "VirtualContract",
    "VirtualContractSimulator",
    "CFDConfig", "CFDQuote", "CFDReplayResult", "CFDSignal", "CFDSimulationError",
    "CFDSimulator", "CFDTrade", "Direction", "TradeState", "ForexCFDConfig",
    "ForexCFDQuote", "ForexCFDSignal", "ForexCFDSimulator", "ForexCFDTrade",
    "known_fixture_eurusd_long", "run_ctrader_fixture", "CTraderDemoTransport", "CTraderOfficialDemoTransport", "CTraderDemoTransportConfig", "DEMO_PROTOBUF_ENDPOINT", "LIVE_PROTOBUF_ENDPOINT", "OfficialCorrelationError", "OfficialGateway", "OfficialMessageError", "OfficialSDKUnavailable", "OfficialTransportError", "VOLUME_SCALE", "load_official_proto", "CTraderCapture", "CTraderPipeline", "CTraderPipelineError", "CTraderPipelineResult", "PAPER_PRODUCT", "PAPER_VARIANT", "normalize_ctrader_capture", "signal_to_cfd_signal", "spot_event_to_cfd_quote", "synthetic_ctrader_capture", "synthetic_ctrader_payloads", "CTraderDemoExecutor", "DemoAccount", "DemoTransport",
    "ExecutionPolicy", "ExecutionIntent", "ExecutionEvent", "Fill", "IntentStore",
    "MemoryIntentStore", "JsonlIntentStore", "OrderResult", "OrderSnapshot", "OrderState",
    "Position", "Quote", "Side", "verify_demo_account", "ActivationProfile",
    "ActivationMode", "ActivationState", "ActivationStatus", "BrokerAccount",
    "OAuthAppConfig", "OAuthTokenPayload", "SecureTokenStore", "TokenLease", "TokenMetadata",
    "build_authorization_url", "parse_callback_uri", "exchange_authorization_code",
    "refresh_access_token", "evaluate_activation", "select_demo_account",
]
