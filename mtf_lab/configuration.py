"""Carga y normalización única de la configuración efectiva de MTF Lab.

Todos los comandos deben consumir :class:`EffectiveConfig`; el TOML se valida
antes de crear sesiones o motores. Los aliases se convierten una sola vez y
quedan incluidos en el hash efectivo.
"""

from __future__ import annotations

import hashlib
import math
import os
import tomllib
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, field, fields
from pathlib import Path
from types import MappingProxyType
from typing import Any

from .core import IndicatorConfig, StrategyConfig, Timeframe, parse_timeframe
from .core.canonical import canonical_json, canonical_value

PROJECT_ROOT = Path(__file__).resolve().parents[1]
_BUNDLED_CONFIG_ROOT = Path(__file__).resolve().parent / "resources" / "config"


def _is_source_checkout(root: Path) -> bool:
    """Accept a root only when its project identity is explicitly MTF Lab."""
    manifest = root / "pyproject.toml"
    config = root / "config"
    if not config.is_dir() or not manifest.is_file():
        return False
    try:
        metadata = tomllib.loads(manifest.read_text(encoding="utf-8")).get("project", {})
    except (OSError, tomllib.TOMLDecodeError):
        return False
    return isinstance(metadata, dict) and metadata.get("name") == "mtf-lab"


def packaged_config_path(name: str = "default.toml") -> Path:
    """Return a checked-in config path for source and wheel installations.

    A source checkout keeps ``config/`` as the canonical editable location.
    Wheels carry byte-for-byte copies below ``mtf_lab/resources/config`` so a
    regular (non-editable) install does not reach back into the checkout.
    """
    relative = Path(name)
    if relative.is_absolute() or relative.parent != Path("."):
        raise ValueError(f"nombre de configuración no válido: {name!r}")
    source = PROJECT_ROOT / "config" / relative
    if _is_source_checkout(PROJECT_ROOT) and source.is_file():
        return source
    bundled = _BUNDLED_CONFIG_ROOT / relative
    if bundled.is_file():
        return bundled
    # Keep a useful, deterministic path in the error emitted by load_config.
    return source


def default_state_dir() -> Path:
    """Return a writable state directory without using site-packages.

    Source checkouts preserve the historical ``data/`` location.  A regular
    wheel has no repository root, so it uses the explicit override or the
    user's XDG state directory instead of attempting to write beside code.
    """
    explicit = os.environ.get("MTF_LAB_STATE_DIR")
    if explicit:
        return Path(explicit).expanduser()
    if _is_source_checkout(PROJECT_ROOT):
        return PROJECT_ROOT / "data"
    xdg_state = os.environ.get("XDG_STATE_HOME")
    base = Path(xdg_state).expanduser() if xdg_state else Path.home() / ".local" / "state"
    return base / "mtf-lab"


class ConfigError(ValueError):
    """Configuración ausente, desconocida o incompatible."""


def _finite(value: Any, name: str, *, minimum: float | None = None) -> float:
    if isinstance(value, bool):
        raise ConfigError(f"{name} debe ser numérico")
    try:
        result = float(value)
    except (TypeError, ValueError):
        raise ConfigError(f"{name} debe ser numérico") from None
    if not math.isfinite(result) or (minimum is not None and result < minimum):
        raise ConfigError(f"{name} debe ser finito y >= {minimum if minimum is not None else '-inf'}")
    return result


def _canonical(value: Any) -> Any:
    """Return strict JSON-compatible values for configuration hashes."""

    return canonical_value(value)


def _detach(value: Any) -> Any:
    """Detach nested mapping proxies while preserving JSON container shape."""

    if isinstance(value, Mapping):
        return {key: _detach(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_detach(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_detach(item) for item in value)
    if isinstance(value, set):
        return {_detach(item) for item in value}
    return deepcopy(value)


def _freeze_mapping(value: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if value is None:
        return MappingProxyType({})
    if not isinstance(value, Mapping):
        raise ConfigError("las secciones de configuración deben ser tablas")
    # EffectiveConfig is frozen, but nested TOML/provider mappings are still
    # mutable unless ownership is detached here. Keep nested list/dict shape
    # for public JSON compatibility while preventing caller-side mutation.
    return MappingProxyType(_detach(value))


def _thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    if isinstance(value, list):
        return [_thaw(item) for item in value]
    return value


def _section(raw: Mapping[str, Any], name: str, allowed: set[str], *, optional: bool = True) -> dict[str, Any]:
    value = raw.get(name, {})
    if value is None and optional:
        return {}
    if not isinstance(value, Mapping):
        raise ConfigError(f"[{name}] debe ser una tabla")
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ConfigError(f"{name}: claves desconocidas: {unknown}")
    return dict(value)


_TOP_LEVEL_KEYS = frozenset(
    {
        "project",
        "storage",
        "data",
        "instrument",
        "timeframes",
        "indicators",
        "strategy",
        "quality",
        "simulation",
        "provider",
        "ui",
        "ctrader",
        "ctrader_oauth",
        "execution",
        "cfd",
    }
)

_SECTION_KEYS: dict[str, set[str]] = {
    "project": {"name", "version", "mode"},
    "instrument": {"symbol", "price_base"},
    "storage": {"db", "logs"},
    "data": {"provider", "instrument", "resolution", "source", "path", "price_base", "mode"},
    "timeframes": {"base", "values", "closed_only"},
    "indicators": {"ema_fast", "ema_slow", "rsi_period", "atr_period", "wilder"},
    "strategy": {
        "name",
        "context_timeframe",
        "preparation_timeframe",
        "trigger_timeframe",
        "setup_timeframe",
        "context_lookback",
        "preparation_lookback",
        "lookback",
        "max_distance_atr",
        "setup_max_atr",
        "rsi_threshold",
        "preparation_ttl_bars",
        "preparation_ttl_minutes",
        "require_closed",
        "one_signal_per_episode",
        "optional_filters",
        "indicators",
        "timeframes",
        "lookbacks",
        "conditions",
        "mode",
    },
    "quality": {"max_feed_age_seconds", "max_closed_candle_age_seconds", "max_gap_minutes", "require_warmup"},
    "simulation": {
        "horizons_seconds",
        "horizons_minutes",
        "entry_latency_seconds",
        "entry_rule",
        "exit_rule",
        "horizon_from",
        "stake",
        "max_price_age_seconds",
        "payout_net",
        "net_payout",
        "loss_amount",
        "tie_net",
        "tie_return",
        "costs",
        "tie_tolerance",
        "requested_base_price",
        "require_closed",
    },
    "provider": {
        "name",
        "rest_url",
        "websocket_url",
        "rest_max_records",
        "exclude_last_uncommitted",
        "trade_channel",
        "ohlc_channel",
        "environment",
        "protocol",
        "endpoint",
        "account_id",
        "symbol",
        "historical_rate_limit",
        "request_rate_limit",
    },
    "ctrader": {
        "enabled",
        "operation_mode",
        "environment",
        "required_scopes",
        "account_id",
        "account_selected",
        "token_ref",
        "token_store_dir",
        "protocol",
        "host",
        "port",
        "endpoint",
        "symbol",
        "redirect_uri",
        "scope",
        "client_id_env",
        "client_secret_env",
        "token_path",
        "request_rate_limit",
        "historical_rate_limit",
        "heartbeat_seconds",
        "max_queue",
        "max_reconnects",
        "reconnect_backoff_seconds",
        "allow_network",
        "provider",
    },
    "ctrader_oauth": {"client_id_env", "client_secret_env", "redirect_uri", "authorization_url", "token_url"},
    "execution": {
        "enabled",
        "environment",
        "endpoint",
        "account_id",
        "scope",
        "activation_required",
        "max_quantity",
        "fixed_quantity",
        "max_exposure",
        "max_positions",
        "max_spread",
        "max_price_age_seconds",
        "allowed_symbols",
        "paused",
        "transport",
        "token_ref",
        "close_only_own_positions",
        "no_martingale",
        "timeout_seconds",
    },
    "cfd": {
        "instrument",
        "account_currency",
        "default_quantity",
        "quantity_unit",
        "units",
        "commission",
        "commission_currency",
        "commission_fixed",
        "commission_per_unit",
        "commission_known",
        "terminal_retention",
        "event_retention",
        "quote_id_retention",
        "max_active_trades",
        "slippage",
        "slippage_pips",
        "financing",
        "financing_required",
        "financing_rate_per_second",
        "conversion_rate",
        "fill_policy",
        "close_policy",
        "horizons_seconds",
        "entry_latency_seconds",
        "decision_latency_seconds",
        "close_latency_seconds",
        "max_spread",
        "max_quote_age_seconds",
        "max_price_age_seconds",
        "market_calendar",
        "pip_size",
        "price_precision",
        "digits",
        "lot_size",
        "min_quantity",
        "max_quantity",
        "step_quantity",
    },
    "ui": {"host", "port"},
}


def _read_toml(path: str | Path | None) -> tuple[Path, Mapping[str, Any]]:
    target = Path(path).expanduser() if path is not None else packaged_config_path()
    if not target.exists():
        raise ConfigError(f"configuración no encontrada: {target}")
    try:
        raw = tomllib.loads(target.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ConfigError(f"no se pudo leer TOML {target}: {exc}") from exc
    if not isinstance(raw, Mapping):
        raise ConfigError("el TOML debe ser una tabla")
    unknown_top = sorted(set(raw) - _TOP_LEVEL_KEYS)
    if unknown_top:
        raise ConfigError(f"claves raíz desconocidas: {unknown_top}")
    return target, raw


def _read_sections(raw: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {name: _section(raw, name, allowed) for name, allowed in _SECTION_KEYS.items()}


def _provider_and_ui(sections: Mapping[str, Mapping[str, Any]]) -> tuple[dict[str, Any], dict[str, Any]]:
    provider = dict(sections["provider"])
    provider.setdefault("name", "kraken_public")
    if not str(provider.get("name", "")).lower().startswith(("ctrader", "fixture")):
        provider.setdefault("rest_url", "https://api.kraken.com/0/public/OHLC")
        provider.setdefault("websocket_url", "wss://ws.kraken.com/v2")
        provider.setdefault("rest_max_records", 720)
        provider.setdefault("exclude_last_uncommitted", True)
        provider.setdefault("trade_channel", True)
        provider.setdefault("ohlc_channel", True)
    ui = dict(sections["ui"])
    ui.setdefault("host", "127.0.0.1")
    ui.setdefault("port", 8765)
    _validate_provider_ui(provider, ui)
    return provider, ui


def _validate_provider_ui(provider: Mapping[str, Any], ui: Mapping[str, Any]) -> None:
    if not isinstance(provider.get("name"), str) or not str(provider.get("name")).strip():
        raise ConfigError("provider.name debe ser texto no vacío")
    rest_max_records = provider.get("rest_max_records")
    if rest_max_records is not None and (
        isinstance(rest_max_records, bool) or not isinstance(rest_max_records, int) or int(rest_max_records) < 1
    ):
        raise ConfigError("provider.rest_max_records debe ser entero positivo")
    port = ui.get("port")
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= int(port) <= 65535:
        raise ConfigError("ui.port debe estar entre 1 y 65535")
    if not isinstance(ui.get("host"), str) or not str(ui.get("host")).strip():
        raise ConfigError("ui.host debe ser texto no vacío")


def _resolve_mode(project: Mapping[str, Any], data: Mapping[str, Any]) -> str:
    mode = _mode(project.get("mode", "offline"))
    if data.get("mode") is not None and _mode(data.get("mode")) != mode:
        raise ConfigError("data.mode y project.mode son incompatibles")
    return mode


def _resolve_instrument(instrument: Mapping[str, Any], data: Mapping[str, Any]) -> tuple[str, str]:
    if (
        instrument.get("symbol") is not None
        and data.get("instrument") is not None
        and str(instrument.get("symbol")).strip() != str(data.get("instrument")).strip()
    ):
        raise ConfigError("instrument.symbol y data.instrument discrepan")
    if (
        instrument.get("price_base") is not None
        and data.get("price_base") is not None
        and str(instrument.get("price_base")).lower() != str(data.get("price_base")).lower()
    ):
        raise ConfigError("instrument.price_base y data.price_base discrepan")
    symbol = instrument.get("symbol", data.get("instrument", "SYNTH/USD"))
    if not isinstance(symbol, str) or not symbol.strip():
        raise ConfigError("instrument.symbol debe ser texto no vacío")
    price_base = str(instrument.get("price_base", data.get("price_base", "traded"))).lower()
    if price_base == "trade":
        price_base = "traded"
    if price_base not in {"traded", "close", "bid", "ask", "mid", "native"}:
        raise ConfigError(f"instrument.price_base no soportada: {price_base!r}")
    return str(symbol).strip(), price_base


def _resolve_timeframes(section: Mapping[str, Any]) -> tuple[tuple[Timeframe, ...], bool]:
    values = section.get("values", ("M1", "M5", "M15"))
    if not isinstance(values, (list, tuple)) or isinstance(values, (str, bytes)):
        raise ConfigError("timeframes.values debe ser una lista")
    try:
        parsed_values = tuple(parse_timeframe(value) for value in values)
        if len({tf.name for tf in parsed_values}) != len(parsed_values):
            raise ConfigError("timeframes.values no puede contener duplicados")
        timeframes = tuple(sorted(parsed_values, key=lambda tf: tf.seconds))
        base_tf = parse_timeframe(section.get("base", timeframes[0] if timeframes else "M1"))
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"timeframes inválidas: {exc}") from exc
    if not timeframes or base_tf not in timeframes:
        raise ConfigError("timeframes.base debe estar en timeframes.values")
    closed_only = section.get("closed_only", True)
    if not isinstance(closed_only, bool):
        raise ConfigError("timeframes.closed_only debe ser booleano")
    return timeframes, closed_only


def _resolve_indicators(ind_section: Mapping[str, Any], strategy_section: Mapping[str, Any]) -> IndicatorConfig:
    ind_data = dict(ind_section)
    if ind_data.pop("wilder", True) is not True:
        raise ConfigError("sólo se admite indicators.wilder=true en esta versión")
    try:
        indicators = IndicatorConfig.from_mapping(ind_data or None)
    except (TypeError, ValueError) as exc:
        raise ConfigError(str(exc)) from exc
    if "indicators" in strategy_section:
        nested = strategy_section["indicators"]
        if not isinstance(nested, Mapping):
            raise ConfigError("strategy.indicators debe ser tabla")
        nested_cfg = IndicatorConfig.from_mapping(nested)
        if nested_cfg != indicators:
            raise ConfigError("indicators raíz y strategy.indicators discrepan")
    return indicators


def _resolve_strategy(
    section: Mapping[str, Any], indicators: IndicatorConfig, mode: str, timeframes: tuple[Timeframe, ...]
) -> StrategyConfig:
    strategy_data = dict(section)
    strategy_data.pop("indicators", None)
    if strategy_data.get("mode") is not None and _mode(strategy_data.get("mode")) != mode:
        raise ConfigError("strategy.mode y project.mode son incompatibles")
    strategy_data["indicators"] = {
        "ema_fast": indicators.ema_fast,
        "ema_slow": indicators.ema_slow,
        "rsi_period": indicators.rsi_period,
        "atr_period": indicators.atr_period,
    }
    strategy_data["mode"] = mode
    if not any(
        key in strategy_data
        for key in ("context_timeframe", "preparation_timeframe", "trigger_timeframe", "setup_timeframe", "timeframes")
    ):
        if len(timeframes) < 3:
            raise ConfigError("se requieren al menos tres temporalidades para trend_pullback_v1")
        strategy_data.update(
            {
                "trigger_timeframe": timeframes[0],
                "preparation_timeframe": timeframes[1],
                "context_timeframe": timeframes[2],
            }
        )
    _validate_optional_filters(strategy_data.get("optional_filters", {}))
    try:
        return StrategyConfig.from_mapping(strategy_data)
    except (TypeError, ValueError) as exc:
        raise ConfigError(str(exc)) from exc


def _validate_optional_filters(optional_filters: Any) -> None:
    if not optional_filters:
        return
    if not isinstance(optional_filters, Mapping) or any(
        not isinstance(value, bool) for value in optional_filters.values()
    ):
        raise ConfigError("strategy.optional_filters debe mapear nombres a booleanos")
    unsupported = [name for name, enabled in optional_filters.items() if enabled]
    if unsupported:
        raise ConfigError(f"filtros opcionales solicitados pero no implementados: {unsupported}")


def _resolve_simulation(section: Mapping[str, Any]) -> SimulationConfig:
    sim_data = dict(section)
    _validate_simulation_aliases(sim_data)
    if "horizons_minutes" in sim_data:
        sim_data["horizons_seconds"] = tuple(
            _finite(item, "simulation.horizons_minutes", minimum=0.000001) * 60.0
            for item in sim_data.pop("horizons_minutes")
        )
    if "net_payout" in sim_data:
        sim_data["payout_net"] = sim_data.pop("net_payout")
    if "tie_return" in sim_data:
        sim_data["tie_net"] = sim_data.pop("tie_return")
    try:
        return SimulationConfig(**sim_data)
    except TypeError as exc:
        raise ConfigError(str(exc)) from exc


def _validate_simulation_aliases(values: Mapping[str, Any]) -> None:
    if "horizons_seconds" in values and "horizons_minutes" in values:
        raise ConfigError("simulation.horizons_seconds y horizons_minutes son incompatibles")
    if "payout_net" in values and "net_payout" in values:
        raise ConfigError("simulation.payout_net y net_payout son incompatibles")
    if "tie_net" in values and "tie_return" in values:
        raise ConfigError("simulation.tie_net y tie_return son incompatibles")


def _resolve_storage_data(
    project: Mapping[str, Any],
    storage: Mapping[str, Any],
    data: Mapping[str, Any],
    provider: Mapping[str, Any],
    symbol: str,
    price_base: str,
    mode: str,
    timeframes: tuple[Timeframe, ...],
) -> tuple[str, str, str, str, dict[str, Any]]:
    project_name = str(project.get("name", "MTF Lab"))
    version = str(project.get("version", "0.1.0"))
    db = Path(storage.get("db", default_state_dir() / "mtf_lab.sqlite3")).expanduser()
    logs = Path(storage.get("logs", db.with_suffix(".jsonl"))).expanduser()
    data_effective = dict(data)
    data_effective.setdefault("provider", provider.get("name"))
    data_effective.setdefault("instrument", symbol)
    data_effective.setdefault("resolution", timeframes[0].name)
    data_effective.setdefault("price_base", price_base)
    data_effective.setdefault("mode", mode)
    return project_name, version, str(db), str(logs), data_effective


def _simulation_horizons(values: Any) -> tuple[float, ...]:
    horizons = tuple(_finite(item, "simulation.horizons_seconds", minimum=0.000001) for item in values)
    if not horizons:
        raise ConfigError("simulation.horizons_seconds no puede estar vacío")
    return horizons


def _normalize_simulation_numbers(config: SimulationConfig) -> None:
    for name in (
        "entry_latency_seconds",
        "stake",
        "max_price_age_seconds",
        "payout_net",
        "loss_amount",
        "costs",
        "tie_tolerance",
    ):
        object.__setattr__(config, name, _finite(getattr(config, name), f"simulation.{name}", minimum=0.0))
    if config.stake <= 0:
        raise ConfigError("simulation.stake debe ser > 0")


def _validate_simulation_rules(config: SimulationConfig) -> None:
    if config.entry_rule not in {"first_observation_at_or_after"}:
        raise ConfigError(f"simulation.entry_rule no soportada: {config.entry_rule!r}")
    if config.exit_rule not in {"last_observation_at_or_before", "first_observation_at_or_after"}:
        raise ConfigError(f"simulation.exit_rule no soportada: {config.exit_rule!r}")
    if config.horizon_from not in {"entry", "detection"}:
        raise ConfigError("simulation.horizon_from debe ser entry o detection")
    if not isinstance(config.require_closed, bool):
        raise ConfigError("simulation.require_closed debe ser booleano")


def _simulation_price_base(value: Any) -> str | None:
    if value is None:
        return None
    raw_base = value.value if hasattr(value, "value") else value
    if not isinstance(raw_base, str):
        raise ConfigError(f"simulation.requested_base_price no soportada: {value!r}")
    base = raw_base.strip().lower()
    if base == "trade":
        base = "traded"
    if base not in {"traded", "close", "bid", "ask", "mid", "native"}:
        raise ConfigError(f"simulation.requested_base_price no soportada: {value!r}")
    return base


def _freeze_effective_mappings(config: EffectiveConfig) -> None:
    for name in ("data", "provider", "ui", "ctrader", "execution", "cfd", "ctrader_oauth"):
        object.__setattr__(config, name, _freeze_mapping(getattr(config, name)))


def _effective_price_base(value: str) -> str:
    if value == "close":
        # Alias histórico de una vela OHLC; no se aplica a native.
        value = "traded"
    if value not in {"traded", "bid", "ask", "mid", "native"}:
        raise ConfigError(f"instrument.price_base no soportada: {value!r}")
    return value


@dataclass(frozen=True, slots=True)
class QualityConfig:
    max_feed_age_seconds: float = 90.0
    max_closed_candle_age_seconds: float = 900.0
    max_gap_minutes: float = 3.0
    require_warmup: bool = True

    def __post_init__(self) -> None:
        for name in ("max_feed_age_seconds", "max_closed_candle_age_seconds", "max_gap_minutes"):
            object.__setattr__(self, name, _finite(getattr(self, name), name, minimum=0.0))
        if not isinstance(self.require_warmup, bool):
            raise ConfigError("quality.require_warmup debe ser booleano")

    def to_dict(self) -> dict[str, Any]:
        return {field.name: getattr(self, field.name) for field in fields(self)}


@dataclass(frozen=True, slots=True)
class SimulationConfig:
    horizons_seconds: tuple[float, ...] = (60.0, 180.0, 300.0)
    entry_latency_seconds: float = 5.0
    entry_rule: str = "first_observation_at_or_after"
    exit_rule: str = "last_observation_at_or_before"
    horizon_from: str = "entry"
    stake: float = 1.0
    max_price_age_seconds: float = 90.0
    payout_net: float = 0.80
    loss_amount: float = 1.0
    tie_net: float = 0.0
    costs: float = 0.0
    tie_tolerance: float = 0.0
    requested_base_price: str | None = None
    require_closed: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "horizons_seconds", _simulation_horizons(self.horizons_seconds))
        _normalize_simulation_numbers(self)
        _validate_simulation_rules(self)
        object.__setattr__(self, "requested_base_price", _simulation_price_base(self.requested_base_price))

    def to_dict(self) -> dict[str, Any]:
        return {field.name: _canonical(getattr(self, field.name)) for field in fields(self)}


@dataclass(frozen=True, slots=True)
class EffectiveConfig:
    path: str
    project_name: str
    version: str
    mode: str
    instrument: str
    price_base: str
    timeframes: tuple[Timeframe, ...]
    closed_only: bool
    indicators: IndicatorConfig
    strategy: StrategyConfig
    quality: QualityConfig
    simulation: SimulationConfig
    storage_db: str
    storage_logs: str
    data: Mapping[str, Any]
    provider: Mapping[str, Any]
    ui: Mapping[str, Any]
    ctrader: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))
    execution: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))
    cfd: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))
    ctrader_oauth: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))

    def __post_init__(self) -> None:
        _freeze_effective_mappings(self)
        object.__setattr__(self, "price_base", _effective_price_base(self.price_base))

    @property
    def config_hash(self) -> str:
        encoded = canonical_json(self.to_dict(include_hash=False)).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def to_dict(self, *, include_hash: bool = True) -> dict[str, Any]:
        strategy = {
            "name": self.strategy.name,
            "context_timeframe": parse_timeframe(self.strategy.context_timeframe).name,
            "preparation_timeframe": parse_timeframe(self.strategy.preparation_timeframe).name,
            "trigger_timeframe": parse_timeframe(self.strategy.trigger_timeframe).name,
            "context_lookback": self.strategy.context_lookback,
            "preparation_lookback": self.strategy.preparation_lookback,
            "max_distance_atr": self.strategy.max_distance_atr,
            "rsi_threshold": self.strategy.rsi_threshold,
            "preparation_ttl_bars": self.strategy.preparation_ttl_bars,
            "require_closed": self.strategy.require_closed,
            "one_signal_per_episode": self.strategy.one_signal_per_episode,
            "optional_filters": dict(self.strategy.optional_filters),
            "mode": self.strategy.mode.value,
        }
        result: dict[str, Any] = {
            "project": {"name": self.project_name, "version": self.version, "mode": self.mode},
            "instrument": {"symbol": self.instrument, "price_base": self.price_base},
            "timeframes": {
                "base": self.timeframes[0].name,
                "values": [tf.name for tf in self.timeframes],
                "closed_only": self.closed_only,
            },
            "indicators": {
                "ema_fast": self.indicators.ema_fast,
                "ema_slow": self.indicators.ema_slow,
                "rsi_period": self.indicators.rsi_period,
                "atr_period": self.indicators.atr_period,
                "wilder": True,
            },
            "strategy": strategy,
            "quality": self.quality.to_dict(),
            "simulation": self.simulation.to_dict(),
            "storage": {"db": self.storage_db, "logs": self.storage_logs},
            "data": _thaw(self.data),
            "provider": _thaw(self.provider),
            "ui": _thaw(self.ui),
            "ctrader": _thaw(self.ctrader),
            "execution": _thaw(self.execution),
            "cfd": _thaw(self.cfd),
            "ctrader_oauth": _thaw(self.ctrader_oauth),
        }
        if include_hash:
            result["config_hash"] = self.config_hash
        return result


def _mode(value: Any) -> str:
    text = str(value or "offline").strip().upper()
    aliases = {
        "OFFLINE": "SYNTHETIC",
        "SINTETICO": "SYNTHETIC",
        "SINTÉTICO": "SYNTHETIC",
        "SYNTHETIC": "SYNTHETIC",
        "LIVE": "LIVE",
        "OBSERVACIÓN EN DIRECTO": "LIVE",
        "OBSERVACION EN DIRECTO": "LIVE",
        "OBSERVATION_EN_DIRECTO": "LIVE",
        "REPLAY": "REPLAY",
    }
    if text not in aliases:
        raise ConfigError(f"project.mode no soportado: {value!r}")
    return aliases[text]


def normalize_simulation_mapping(config: EffectiveConfig | Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Return one canonical simulation mapping for CLI, pipeline and runtime."""
    if isinstance(config, EffectiveConfig):
        return config.simulation.to_dict()
    if config is None:
        return load_config().simulation.to_dict()
    raw_section = config.get("simulation", {}) if isinstance(config, Mapping) else {}
    if not isinstance(raw_section, Mapping):
        raise ConfigError("simulation debe ser tabla")
    raw = dict(raw_section)
    if "horizons_seconds" in raw and "horizons_minutes" in raw:
        raise ConfigError("simulation.horizons_seconds y horizons_minutes son incompatibles")
    if "horizons_minutes" in raw:
        raw["horizons_seconds"] = tuple(
            _finite(item, "simulation.horizons_minutes", minimum=0.000001) * 60.0
            for item in raw.pop("horizons_minutes")
        )
    for alias, canonical in (("net_payout", "payout_net"), ("tie_return", "tie_net")):
        if alias in raw:
            if canonical in raw:
                raise ConfigError(f"simulation.{alias} y {canonical} son incompatibles")
            raw[canonical] = raw.pop(alias)
    allowed = {
        "horizons_seconds",
        "entry_latency_seconds",
        "entry_rule",
        "exit_rule",
        "horizon_from",
        "stake",
        "max_price_age_seconds",
        "payout_net",
        "loss_amount",
        "tie_net",
        "costs",
        "tie_tolerance",
        "requested_base_price",
        "require_closed",
    }
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise ConfigError(f"simulation: claves desconocidas {unknown}")
    return raw


def load_config(path: str | Path | None = None) -> EffectiveConfig:
    target, raw = _read_toml(path)
    sections = _read_sections(raw)
    project = sections["project"]
    data_section = sections["data"]
    mode = _resolve_mode(project, data_section)
    provider, ui = _provider_and_ui(sections)
    symbol, price_base = _resolve_instrument(sections["instrument"], data_section)
    timeframes, closed_only = _resolve_timeframes(sections["timeframes"])
    indicators = _resolve_indicators(sections["indicators"], sections["strategy"])
    strategy = _resolve_strategy(sections["strategy"], indicators, mode, timeframes)
    simulation = _resolve_simulation(sections["simulation"])
    quality = QualityConfig(**sections["quality"])
    required_strategy_tfs = {
        parse_timeframe(strategy.context_timeframe).name,
        parse_timeframe(strategy.preparation_timeframe).name,
        parse_timeframe(strategy.trigger_timeframe).name,
    }
    if not required_strategy_tfs.issubset({tf.name for tf in timeframes}):
        raise ConfigError("strategy requiere temporalidades presentes en timeframes.values")
    project_name, version, db, logs, data_effective = _resolve_storage_data(
        project,
        sections["storage"],
        data_section,
        provider,
        symbol,
        price_base,
        mode,
        timeframes,
    )
    return EffectiveConfig(
        str(target.resolve()),
        project_name,
        version,
        mode,
        symbol,
        price_base,
        timeframes,
        closed_only,
        indicators,
        strategy,
        quality,
        simulation,
        db,
        logs,
        data_effective,
        provider,
        ui,
        sections["ctrader"],
        sections["execution"],
        sections["cfd"],
        sections["ctrader_oauth"],
    )


__all__ = [
    "ConfigError",
    "EffectiveConfig",
    "QualityConfig",
    "SimulationConfig",
    "default_state_dir",
    "load_config",
    "normalize_simulation_mapping",
    "packaged_config_path",
]
