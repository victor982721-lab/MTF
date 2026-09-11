"""Runtime incremental y reanudable de MTF Lab."""

# Processor/state are imported before the integration facade so callers can
# import runtime while application adapters are being initialized.
from .consumers import (
    BinarySimulationConsumer,
    CFDSignalConsumer,
    RecordingSignalConsumer,
    SignalConsumer,
    SignalConsumerCheckpoint,
    SignalConsumerEvent,
    SignalConsumerResult,
    consumer_from_checkpoint,
)
from .processor import (
    IncrementalProcessor,
    IncrementalRuntime,
    ProcessResult,
    ReplayResult,
    RuntimeIssue,
    RuntimeService,
)
from .state import (
    PendingSimulation,
    PriceObservation,
    SimulationConfig,
    to_core_candle,
    to_core_event,
)
from .integration import CoordinatorStatus, RuntimeCoordinator, capture_hash, runtime_simulation_config

__all__ = [
    "BinarySimulationConsumer",
    "CFDSignalConsumer",
    "CoordinatorStatus",
    "IncrementalProcessor",
    "IncrementalRuntime",
    "PendingSimulation",
    "PriceObservation",
    "ProcessResult",
    "RecordingSignalConsumer",
    "ReplayResult",
    "RuntimeCoordinator",
    "RuntimeIssue",
    "RuntimeService",
    "SignalConsumer",
    "SignalConsumerCheckpoint",
    "SignalConsumerEvent",
    "SignalConsumerResult",
    "SimulationConfig",
    "capture_hash",
    "consumer_from_checkpoint",
    "runtime_simulation_config",
    "to_core_candle",
    "to_core_event",
]
