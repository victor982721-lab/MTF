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
]
