"""Compatibility facade for the canonical CFD domain.

The implementation lives in mtf_lab.core.cfd_simulation; this module keeps the
pre-existing operational imports stable while avoiding a second CFD state
machine in the orchestration layer.
"""

from ..core.cfd_quality import QuoteAssessment, QuoteLeg, QuoteLegAssessment, QuoteQuality, QuoteReason, QuoteSide
from ..core.cfd_simulation import (
    CFD_ECONOMICS_LEGACY_VERSION,
    CFD_ECONOMICS_VERSION,
    CFD_PRODUCT,
    CFD_SNAPSHOT_VERSION,
    CFDConfig,
    CFDEconomicResult,
    CFDQuote,
    CFDReplayResult,
    CFDSignal,
    CFDSimulationError,
    CFDSimulator,
    CFDTrade,
    Direction,
    EconomicResult,
    EconomicState,
    ForexCFDConfig,
    ForexCFDQuote,
    ForexCFDSignal,
    ForexCFDSimulator,
    ForexCFDTrade,
    TradeState,
    decimal,
    known_fixture_eurusd_long,
)

__all__ = [
    "CFD_ECONOMICS_LEGACY_VERSION",
    "CFD_ECONOMICS_VERSION",
    "CFDConfig",
    "CFDEconomicResult",
    "CFD_PRODUCT",
    "CFDQuote",
    "CFDReplayResult",
    "CFDSignal",
    "CFDSimulationError",
    "CFDSimulator",
    "CFDTrade",
    "CFD_SNAPSHOT_VERSION",
    "Direction",
    "EconomicResult",
    "EconomicState",
    "ForexCFDConfig",
    "ForexCFDQuote",
    "ForexCFDSignal",
    "ForexCFDSimulator",
    "ForexCFDTrade",
    "QuoteAssessment",
    "QuoteLeg",
    "QuoteLegAssessment",
    "QuoteQuality",
    "QuoteReason",
    "QuoteSide",
    "TradeState",
    "decimal",
    "known_fixture_eurusd_long",
]
