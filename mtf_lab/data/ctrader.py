"""Public cTrader adapter facade.

The implementation is intentionally split into stable layers:

* :mod:`ctrader_protocol` owns payload IDs, Protobuf presence and codec use;
* :mod:`ctrader_config` owns immutable configuration;
* :mod:`ctrader_transport` owns framing and local transports;
* :mod:`ctrader_session` owns one reader, request correlation and auth;
* :mod:`ctrader_market` owns normalized market records and quote quality;
* :mod:`ctrader_fixtures` owns deterministic offline payloads.

This module contains no second implementation.  It only preserves the public
imports used by the existing CLI/pipeline and exposes the central Protobuf
presence helpers for other adapters.
"""

from .ctrader_errors import (
    AuthState,
    CTraderAuthError,
    CTraderConfigurationError,
    CTraderDataError,
    CTraderDependencyError,
    CTraderError,
    CTraderProtocolError,
    CTraderRequestCancelled,
    CTraderRequestTimeout,
    CTraderTransportError,
    ConnectionState,
    DependencyState,
    RequestPhase,
)
from .ctrader_protocol import (
    CTraderCodec,
    DependencyReport,
    MAX_FRAME_LENGTH,
    MESSAGE_PAYLOAD_TYPES,
    PAYLOAD,
    PAYLOAD_NAMES,
    SDK_MODULE,
    SESSION_CONTROL_PAYLOAD_TYPES,
    SESSION_CONTROL_TYPE_NAMES,
    PROTOBUF_MODULE,
    SdkProtobufCodec,
    TREND_PERIODS,
    TREND_PERIOD_NAMES,
    WIRE_CLASS_NAMES,
    WireMessage,
    canonical_json,
    capture_envelope,
    dependency_report,
    enum_name,
    field_present,
    has_field,
    jsonable,
    message_payload_type,
    message_to_mapping,
    read_enum,
    read_field,
    read_optional,
    read_repeated,
    redact_value,
    stable_hash,
)
from .ctrader_config import CTraderConfig, normalize_symbol_name
from .ctrader_transport import (
    CTraderRateLimiter,
    CTraderTransport,
    DeterministicTransport,
    TcpTlsTransport,
)
from .ctrader_session import (
    AuthenticatedSessionEvidence,
    CancellationToken,
    Clock,
    CTraderClient,
    CTraderSession,
    CTraderStatus,
    EventScheduler,
    RequestRecord,
    Scheduler,
    SessionEvidence,
    WallClock,
    replace_status,
)
from .ctrader_market import (
    CTraderFetchResult,
    CTraderHistoryResult,
    CTraderInstrumentSpec,
    CTraderMarketCalendar,
    CTraderNormalizationResult,
    CTraderProvider,
    CTraderSessionWindow,
    CTraderSymbol,
    QuoteLegQuality,
    QuoteQuality,
    QuoteQualityReason,
    QuoteQualityState,
    SymbolCatalog,
    normalize_account_payload,
    normalize_spot_event,
    normalize_trendbar,
    _as_sequence,
    _mapping,
    _timestamp_ms,
    _unix_ms,
)
from .ctrader_fixtures import synthetic_spot_event, synthetic_trendbar

# Compatibility aliases used by integrators that identify the adapter by
# protocol, while retaining one implementation.
CTraderAdapter = CTraderProvider
CTraderOpenAPIProvider = CTraderProvider

# Explicit private compatibility aliases for older local callers.  New code
# should import the public names from ctrader_protocol/ctrader_market instead.
_field = read_field

__all__ = [
    "AuthState",
    "AuthenticatedSessionEvidence",
    "CancellationToken",
    "Clock",
    "CTraderAdapter",
    "CTraderAuthError",
    "CTraderClient",
    "CTraderCodec",
    "CTraderConfig",
    "CTraderConfigurationError",
    "CTraderDataError",
    "CTraderDependencyError",
    "CTraderError",
    "CTraderFetchResult",
    "CTraderHistoryResult",
    "CTraderInstrumentSpec",
    "CTraderMarketCalendar",
    "CTraderNormalizationResult",
    "CTraderOpenAPIProvider",
    "CTraderProtocolError",
    "CTraderProvider",
    "CTraderRateLimiter",
    "CTraderRequestCancelled",
    "CTraderRequestTimeout",
    "CTraderSessionWindow",
    "CTraderSession",
    "CTraderStatus",
    "CTraderSymbol",
    "CTraderTransport",
    "CTraderTransportError",
    "ConnectionState",
    "DependencyReport",
    "DependencyState",
    "DeterministicTransport",
    "EventScheduler",
    "MAX_FRAME_LENGTH",
    "MESSAGE_PAYLOAD_TYPES",
    "PAYLOAD",
    "PAYLOAD_NAMES",
    "PROTOBUF_MODULE",
    "QuoteLegQuality",
    "QuoteQuality",
    "QuoteQualityReason",
    "QuoteQualityState",
    "RequestPhase",
    "RequestRecord",
    "SDK_MODULE",
    "SESSION_CONTROL_PAYLOAD_TYPES",
    "SESSION_CONTROL_TYPE_NAMES",
    "Scheduler",
    "SdkProtobufCodec",
    "SessionEvidence",
    "SymbolCatalog",
    "TREND_PERIODS",
    "TREND_PERIOD_NAMES",
    "TcpTlsTransport",
    "WIRE_CLASS_NAMES",
    "WallClock",
    "WireMessage",
    "canonical_json",
    "capture_envelope",
    "dependency_report",
    "enum_name",
    "field_present",
    "has_field",
    "jsonable",
    "message_payload_type",
    "message_to_mapping",
    "normalize_account_payload",
    "normalize_spot_event",
    "normalize_symbol_name",
    "normalize_trendbar",
    "read_enum",
    "read_field",
    "read_optional",
    "read_repeated",
    "redact_value",
    "stable_hash",
    "synthetic_spot_event",
    "synthetic_trendbar",
]
