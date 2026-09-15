"""Canonical strategy profiles shared by research and execution composition.

Profiles describe only identity, temporalities and the fixed risk-policy
projection.  They do not evaluate signals or talk to a provider.  Keeping the
six identities here prevents the historical runner and configuration loader
from maintaining competing profile tables.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

from .models import parse_timeframe
from .risk_exit import RiskExitPolicy

CANDIDATE_IDS: tuple[str, ...] = (
    "tp_fast_v1",
    "tp_intraday_slow_v1",
    "tp_multiday_v1",
    "dc_m5_v1",
    "dc_m15_v1",
    "dc_h1_v1",
)


@dataclass(frozen=True, slots=True)
class MarketProfile:
    """One preregistered strategy identity and its causal timeframes."""

    candidate_id: str
    strategy_impl: str
    timeframes: tuple[str, ...]
    family: str
    holding_profile: str
    trigger_timeframe: str
    role: str

    def __post_init__(self) -> None:
        candidate_id = str(self.candidate_id).strip()
        if candidate_id not in CANDIDATE_IDS:
            raise ValueError(f"candidate_id no soportado: {candidate_id!r}")
        strategy_impl = str(self.strategy_impl).strip()
        family = str(self.family).strip().upper()
        holding = str(self.holding_profile).strip().upper()
        if not strategy_impl or family not in {"INTRADAY", "MULTIDAY"} or holding != family:
            raise ValueError("profile strategy/family/holding_profile inválidos")
        parsed = tuple(parse_timeframe(value).name for value in self.timeframes)
        if not parsed or len(set(parsed)) != len(parsed):
            raise ValueError("profile requiere temporalidades únicas")
        trigger = parse_timeframe(self.trigger_timeframe).name
        if trigger not in parsed:
            raise ValueError("trigger_timeframe debe pertenecer a timeframes")
        if self.strategy_impl == "TrendPullbackStrategy":
            if (
                len(parsed) != 3
                or trigger != parsed[-1]
                or not (
                    parse_timeframe(parsed[0]).seconds
                    > parse_timeframe(parsed[1]).seconds
                    > parse_timeframe(parsed[2]).seconds
                )
            ):
                raise ValueError("baseline requiere contexto > preparación > disparador")
        elif not self.strategy_impl.startswith("Donchian20"):
            raise ValueError(f"strategy_impl no soportada: {self.strategy_impl!r}")
        object.__setattr__(self, "candidate_id", candidate_id)
        object.__setattr__(self, "strategy_impl", strategy_impl)
        object.__setattr__(self, "timeframes", parsed)
        object.__setattr__(self, "family", family)
        object.__setattr__(self, "holding_profile", holding)
        object.__setattr__(self, "trigger_timeframe", trigger)
        object.__setattr__(self, "role", str(self.role).strip())

    @property
    def context_timeframe(self) -> str | None:
        return self.timeframes[0] if self.strategy_impl == "TrendPullbackStrategy" else None

    @property
    def preparation_timeframe(self) -> str | None:
        return self.timeframes[1] if self.strategy_impl == "TrendPullbackStrategy" else None

    @property
    def is_donchian(self) -> bool:
        return self.strategy_impl.startswith("Donchian20")

    def policy_for(self, base: RiskExitPolicy) -> RiskExitPolicy:
        """Project the frozen holding limits for this profile.

        Intraday time exits are five *trigger bars*, not a copied minute
        constant.  Multiday profiles use the fixed 72-hour ceiling.  Other
        policy values remain owned by ``RiskExitPolicy``.
        """

        if not isinstance(base, RiskExitPolicy):
            raise TypeError("base debe ser RiskExitPolicy")
        if self.holding_profile == "INTRADAY":
            # ``intraday_max_minutes`` is the broker/calendar pre-close
            # margin, not the holding duration.  The duration is measured in
            # trigger bars by the core evaluator.
            return replace(base, holding_profile="INTRADAY", intraday_max_bars=5)
        return replace(base, holding_profile="MULTIDAY")

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "strategy_impl": self.strategy_impl,
            "timeframes": list(self.timeframes),
            "family": self.family,
            "holding_profile": self.holding_profile,
            "trigger_timeframe": self.trigger_timeframe,
            "role": self.role,
        }


MARKET_PROFILES: tuple[MarketProfile, ...] = (
    MarketProfile(
        "tp_fast_v1",
        "TrendPullbackStrategy",
        ("M15", "M5", "M1"),
        "INTRADAY",
        "INTRADAY",
        "M1",
        "BASELINE_FROZEN_INTRADAY_FAST",
    ),
    MarketProfile(
        "tp_intraday_slow_v1",
        "TrendPullbackStrategy",
        ("H4", "H1", "M15"),
        "INTRADAY",
        "INTRADAY",
        "M15",
        "BASELINE_FROZEN_INTRADAY_SLOW",
    ),
    MarketProfile(
        "tp_multiday_v1",
        "TrendPullbackStrategy",
        ("D1", "H4", "H1"),
        "MULTIDAY",
        "MULTIDAY",
        "H1",
        "BASELINE_FROZEN_MULTIDAY",
    ),
    MarketProfile("dc_m5_v1", "Donchian20M5Strategy", ("M5",), "INTRADAY", "INTRADAY", "M5", "DONCHIAN20_M5"),
    MarketProfile("dc_m15_v1", "Donchian20M15Strategy", ("M15",), "INTRADAY", "INTRADAY", "M15", "DONCHIAN20_M15"),
    MarketProfile("dc_h1_v1", "Donchian20H1Strategy", ("H1",), "MULTIDAY", "MULTIDAY", "H1", "DONCHIAN20_H1"),
)


_PROFILE_BY_ID = {item.candidate_id: item for item in MARKET_PROFILES}


def market_profile(candidate_id: str) -> MarketProfile:
    try:
        return _PROFILE_BY_ID[str(candidate_id).strip()]
    except KeyError as exc:
        raise ValueError(f"candidate_id no registrado: {candidate_id!r}") from exc


def candidate_ids() -> tuple[str, ...]:
    return CANDIDATE_IDS


__all__ = ["CANDIDATE_IDS", "MARKET_PROFILES", "MarketProfile", "candidate_ids", "market_profile"]
