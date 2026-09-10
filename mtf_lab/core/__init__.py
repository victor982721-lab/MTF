"""Núcleo determinista de MTF Lab.

Las interfaces públicas se reexportan aquí para que CLI, replay, backtest y
la interfaz local compartan exactamente los mismos cálculos y reglas.
"""

from .aggregation import AggregationResult, CandleAggregator, aggregate_events
from .indicators import (
    IndicatorConfig,
    IndicatorPoint,
    IndicatorSeries,
    IncrementalIndicatorEngine,
    compute_indicators,
    compute_indicators_incremental,
)
from .models import (
    Bar,
    Candle,
    Event,
    EventKind,
    MarketEvent,
    Mode,
    OperationMode,
    PriceBase,
    Timeframe,
    parse_timeframe,
)
from .quality import (
    DataQuality,
    FreshnessAssessment,
    QualityFlag,
    QualityIssue,
    QualityReport,
    ValidationResult,
    assess_freshness,
    validate_candle,
    validate_event,
)
from .strategy import (
    ConditionResult,
    ConditionState,
    DecisionKind,
    Evaluation,
    PreparationEpisode,
    Signal,
    StrategyConfig,
    StrategyResult,
    TrendPullbackStrategy,
)

__all__ = [
    "AggregationResult",
    "Bar",
    "Candle",
    "CandleAggregator",
    "ConditionResult",
    "ConditionState",
    "DataQuality",
    "FreshnessAssessment",
    "DecisionKind",
    "Event",
    "EventKind",
    "Evaluation",
    "IncrementalIndicatorEngine",
    "IndicatorConfig",
    "IndicatorPoint",
    "IndicatorSeries",
    "MarketEvent",
    "Mode",
    "OperationMode",
    "PreparationEpisode",
    "PriceBase",
    "QualityFlag",
    "QualityIssue",
    "QualityReport",
    "Signal",
    "StrategyConfig",
    "StrategyResult",
    "Timeframe",
    "TrendPullbackStrategy",
    "ValidationResult",
    "aggregate_events",
    "assess_freshness",
    "compute_indicators",
    "compute_indicators_incremental",
    "parse_timeframe",
    "validate_candle",
    "validate_event",
]
