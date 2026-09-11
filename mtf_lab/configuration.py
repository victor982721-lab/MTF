"""Carga y normalización única de la configuración efectiva de MTF Lab.

Todos los comandos deben consumir :class:`EffectiveConfig`; el TOML se valida
antes de crear sesiones o motores. Los aliases se convierten una sola vez y
quedan incluidos en el hash efectivo.
"""
from __future__ import annotations

import hashlib
import math
import os
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
        import tomllib

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
        horizons = tuple(_finite(item, "simulation.horizons_seconds", minimum=0.000001) for item in self.horizons_seconds)
        if not horizons:
            raise ConfigError("simulation.horizons_seconds no puede estar vacío")
        object.__setattr__(self, "horizons_seconds", horizons)
        for name in ("entry_latency_seconds", "stake", "max_price_age_seconds", "payout_net", "loss_amount", "costs", "tie_tolerance"):
            object.__setattr__(self, name, _finite(getattr(self, name), f"simulation.{name}", minimum=0.0))
        if self.stake <= 0:
            raise ConfigError("simulation.stake debe ser > 0")
        if self.entry_rule not in {"first_observation_at_or_after"}:
            raise ConfigError(f"simulation.entry_rule no soportada: {self.entry_rule!r}")
        if self.exit_rule not in {"last_observation_at_or_before", "first_observation_at_or_after"}:
            raise ConfigError(f"simulation.exit_rule no soportada: {self.exit_rule!r}")
        if self.horizon_from not in {"entry", "detection"}:
            raise ConfigError("simulation.horizon_from debe ser entry o detection")
        if not isinstance(self.require_closed, bool):
            raise ConfigError("simulation.require_closed debe ser booleano")
        if self.requested_base_price is not None:
            raw_base = self.requested_base_price.value if hasattr(self.requested_base_price, "value") else self.requested_base_price
            if not isinstance(raw_base, str):
                raise ConfigError(f"simulation.requested_base_price no soportada: {self.requested_base_price!r}")
            base = raw_base.strip().lower()
            if base == "trade":
                base = "traded"
            if base not in {"traded", "close", "bid", "ask", "mid", "native"}:
                raise ConfigError(f"simulation.requested_base_price no soportada: {self.requested_base_price!r}")
            object.__setattr__(self, "requested_base_price", base)

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
        for name in ("data", "provider", "ui", "ctrader", "execution", "cfd", "ctrader_oauth"):
            object.__setattr__(self, name, _freeze_mapping(getattr(self, name)))
        if self.price_base == "close":
            # Alias histórico de una vela OHLC; no se aplica a native.
            object.__setattr__(self, "price_base", "traded")
        if self.price_base not in {"traded", "bid", "ask", "mid", "native"}:
            raise ConfigError(f"instrument.price_base no soportada: {self.price_base!r}")

    @property
    def config_hash(self) -> str:
        encoded = canonical_json(self.to_dict(include_hash=False)).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def to_dict(self, *, include_hash: bool = True) -> dict[str, Any]:
        strategy = {
            "name": self.strategy.name,
            "context_timeframe": self.strategy.context_timeframe.name,
            "preparation_timeframe": self.strategy.preparation_timeframe.name,
            "trigger_timeframe": self.strategy.trigger_timeframe.name,
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
            "timeframes": {"base": self.timeframes[0].name, "values": [tf.name for tf in self.timeframes], "closed_only": self.closed_only},
            "indicators": {"ema_fast": self.indicators.ema_fast, "ema_slow": self.indicators.ema_slow, "rsi_period": self.indicators.rsi_period, "atr_period": self.indicators.atr_period, "wilder": True},
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
    aliases = {"OFFLINE": "SYNTHETIC", "SINTETICO": "SYNTHETIC", "SINTÉTICO": "SYNTHETIC", "SYNTHETIC": "SYNTHETIC", "LIVE": "LIVE", "OBSERVACIÓN EN DIRECTO": "LIVE", "OBSERVACION EN DIRECTO": "LIVE", "OBSERVATION_EN_DIRECTO": "LIVE", "REPLAY": "REPLAY"}
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
        raw["horizons_seconds"] = tuple(_finite(item, "simulation.horizons_minutes", minimum=0.000001) * 60.0 for item in raw.pop("horizons_minutes"))
    for alias, canonical in (("net_payout", "payout_net"), ("tie_return", "tie_net")):
        if alias in raw:
            if canonical in raw:
                raise ConfigError(f"simulation.{alias} y {canonical} son incompatibles")
            raw[canonical] = raw.pop(alias)
    allowed = {"horizons_seconds", "entry_latency_seconds", "entry_rule", "exit_rule", "horizon_from", "stake", "max_price_age_seconds", "payout_net", "loss_amount", "tie_net", "costs", "tie_tolerance", "requested_base_price", "require_closed"}
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise ConfigError(f"simulation: claves desconocidas {unknown}")
    return raw


def load_config(path: str | Path | None = None) -> EffectiveConfig:
    import tomllib
    target = Path(path).expanduser() if path is not None else packaged_config_path()
    if not target.exists():
        raise ConfigError(f"configuración no encontrada: {target}")
    try:
        raw = tomllib.loads(target.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ConfigError(f"no se pudo leer TOML {target}: {exc}") from exc
    if not isinstance(raw, Mapping):
        raise ConfigError("el TOML debe ser una tabla")
    top_allowed = {"project", "storage", "data", "instrument", "timeframes", "indicators", "strategy", "quality", "simulation", "provider", "ui", "ctrader", "ctrader_oauth", "execution", "cfd"}
    unknown_top = sorted(set(raw) - top_allowed)
    if unknown_top:
        raise ConfigError(f"claves raíz desconocidas: {unknown_top}")
    project = _section(raw, "project", {"name", "version", "mode"})
    instrument_section = _section(raw, "instrument", {"symbol", "price_base"})
    storage = _section(raw, "storage", {"db", "logs"})
    data_section = _section(raw, "data", {"provider", "instrument", "resolution", "source", "path", "price_base", "mode"})
    tf_section = _section(raw, "timeframes", {"base", "values", "closed_only"})
    ind_section = _section(raw, "indicators", {"ema_fast", "ema_slow", "rsi_period", "atr_period", "wilder"})
    strategy_section = _section(raw, "strategy", {"name", "context_timeframe", "preparation_timeframe", "trigger_timeframe", "setup_timeframe", "context_lookback", "preparation_lookback", "lookback", "max_distance_atr", "setup_max_atr", "rsi_threshold", "preparation_ttl_bars", "preparation_ttl_minutes", "require_closed", "one_signal_per_episode", "optional_filters", "indicators", "timeframes", "lookbacks", "conditions", "mode"})
    quality_section = _section(raw, "quality", {"max_feed_age_seconds", "max_closed_candle_age_seconds", "max_gap_minutes", "require_warmup"})
    sim_section = _section(raw, "simulation", {"horizons_seconds", "horizons_minutes", "entry_latency_seconds", "entry_rule", "exit_rule", "horizon_from", "stake", "max_price_age_seconds", "payout_net", "net_payout", "loss_amount", "tie_net", "tie_return", "costs", "tie_tolerance", "requested_base_price", "require_closed"})
    provider = _section(raw, "provider", {"name", "rest_url", "websocket_url", "rest_max_records", "exclude_last_uncommitted", "trade_channel", "ohlc_channel", "environment", "protocol", "endpoint", "account_id", "symbol", "historical_rate_limit", "request_rate_limit"})
    ctrader = _section(raw, "ctrader", {"enabled", "operation_mode", "environment", "required_scopes", "account_id", "account_selected", "token_ref", "token_store_dir", "protocol", "host", "port", "endpoint", "symbol", "redirect_uri", "scope", "client_id_env", "client_secret_env", "token_path", "request_rate_limit", "historical_rate_limit", "heartbeat_seconds", "max_queue", "max_reconnects", "reconnect_backoff_seconds", "allow_network", "provider"})
    ctrader_oauth = _section(raw, "ctrader_oauth", {"client_id_env", "client_secret_env", "redirect_uri", "authorization_url", "token_url"})
    execution = _section(raw, "execution", {"enabled", "environment", "endpoint", "account_id", "scope", "activation_required", "max_quantity", "fixed_quantity", "max_exposure", "max_positions", "max_spread", "max_price_age_seconds", "allowed_symbols", "paused", "transport", "token_ref", "close_only_own_positions", "no_martingale", "timeout_seconds"})
    cfd = _section(raw, "cfd", {"instrument", "account_currency", "default_quantity", "quantity_unit", "units", "commission", "commission_currency", "commission_fixed", "commission_per_unit", "commission_known", "terminal_retention", "event_retention", "quote_id_retention", "max_active_trades", "slippage", "slippage_pips", "financing", "financing_required", "financing_rate_per_second", "conversion_rate", "fill_policy", "close_policy", "horizons_seconds", "entry_latency_seconds", "decision_latency_seconds", "close_latency_seconds", "max_spread", "max_quote_age_seconds", "max_price_age_seconds", "market_calendar", "pip_size", "price_precision", "digits", "lot_size", "min_quantity", "max_quantity", "step_quantity"})
    provider.setdefault("name", "kraken_public")
    if not str(provider.get("name", "")).lower().startswith(("ctrader", "fixture")):
        provider.setdefault("rest_url", "https://api.kraken.com/0/public/OHLC")
        provider.setdefault("websocket_url", "wss://ws.kraken.com/v2")
        provider.setdefault("rest_max_records", 720)
        provider.setdefault("exclude_last_uncommitted", True)
        provider.setdefault("trade_channel", True); provider.setdefault("ohlc_channel", True)
    ui = _section(raw, "ui", {"host", "port"})
    ui.setdefault("host", "127.0.0.1"); ui.setdefault("port", 8765)
    if not isinstance(provider.get("name"), str) or not str(provider.get("name")).strip():
        raise ConfigError("provider.name debe ser texto no vacío")
    if provider.get("rest_max_records") is not None and (isinstance(provider.get("rest_max_records"), bool) or not isinstance(provider.get("rest_max_records"), int) or int(provider.get("rest_max_records")) < 1):
        raise ConfigError("provider.rest_max_records debe ser entero positivo")
    if isinstance(ui.get("port"), bool) or not isinstance(ui.get("port"), int) or not 1 <= int(ui.get("port")) <= 65535:
        raise ConfigError("ui.port debe estar entre 1 y 65535")
    if not isinstance(ui.get("host"), str) or not str(ui.get("host")).strip():
        raise ConfigError("ui.host debe ser texto no vacío")
    mode = _mode(project.get("mode", "offline"))
    if data_section.get("mode") is not None and _mode(data_section.get("mode")) != mode:
        raise ConfigError("data.mode y project.mode son incompatibles")
    if instrument_section.get("symbol") is not None and data_section.get("instrument") is not None and str(instrument_section.get("symbol")).strip() != str(data_section.get("instrument")).strip():
        raise ConfigError("instrument.symbol y data.instrument discrepan")
    if instrument_section.get("price_base") is not None and data_section.get("price_base") is not None and str(instrument_section.get("price_base")).lower() != str(data_section.get("price_base")).lower():
        raise ConfigError("instrument.price_base y data.price_base discrepan")
    symbol = instrument_section.get("symbol", data_section.get("instrument", "SYNTH/USD"))
    if not isinstance(symbol, str) or not symbol.strip():
        raise ConfigError("instrument.symbol debe ser texto no vacío")
    price_base = str(instrument_section.get("price_base", data_section.get("price_base", "traded"))).lower()
    if price_base == "trade":
        price_base = "traded"
    if price_base not in {"traded", "close", "bid", "ask", "mid", "native"}:
        raise ConfigError(f"instrument.price_base no soportada: {price_base!r}")
    values = tf_section.get("values", ("M1", "M5", "M15"))
    if not isinstance(values, (list, tuple)) or isinstance(values, (str, bytes)):
        raise ConfigError("timeframes.values debe ser una lista")
    try:
        parsed_values = tuple(parse_timeframe(value) for value in values)
        if len({tf.name for tf in parsed_values}) != len(parsed_values):
            raise ConfigError("timeframes.values no puede contener duplicados")
        timeframes = tuple(sorted(parsed_values, key=lambda tf: tf.seconds))
        base_tf = parse_timeframe(tf_section.get("base", timeframes[0] if timeframes else "M1"))
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"timeframes inválidas: {exc}") from exc
    if not timeframes or base_tf not in timeframes:
        raise ConfigError("timeframes.base debe estar en timeframes.values")
    if not isinstance(tf_section.get("closed_only", True), bool):
        raise ConfigError("timeframes.closed_only debe ser booleano")
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
    strategy_data = dict(strategy_section)
    strategy_data.pop("indicators", None)
    if strategy_data.get("mode") is not None and _mode(strategy_data.get("mode")) != mode:
        raise ConfigError("strategy.mode y project.mode son incompatibles")
    strategy_data["indicators"] = {"ema_fast": indicators.ema_fast, "ema_slow": indicators.ema_slow, "rsi_period": indicators.rsi_period, "atr_period": indicators.atr_period}
    strategy_data["mode"] = mode
    # Si se declaran temporalidades sólo en la sección común, las tres
    # primeras ordenadas alimentan contexto/preparación/disparador.
    if not any(key in strategy_data for key in ("context_timeframe", "preparation_timeframe", "trigger_timeframe", "setup_timeframe", "timeframes")):
        if len(timeframes) < 3:
            raise ConfigError("se requieren al menos tres temporalidades para trend_pullback_v1")
        strategy_data.update({"trigger_timeframe": timeframes[0], "preparation_timeframe": timeframes[1], "context_timeframe": timeframes[2]})
    optional_filters = strategy_data.get("optional_filters", {})
    if optional_filters:
        if not isinstance(optional_filters, Mapping) or any(not isinstance(value, bool) for value in optional_filters.values()):
            raise ConfigError("strategy.optional_filters debe mapear nombres a booleanos")
        unsupported = [name for name, enabled in optional_filters.items() if enabled]
        if unsupported:
            raise ConfigError(f"filtros opcionales solicitados pero no implementados: {unsupported}")
    try:
        strategy = StrategyConfig.from_mapping(strategy_data)
    except (TypeError, ValueError) as exc:
        raise ConfigError(str(exc)) from exc
    # Simulation aliases are normalized once; they cannot coexist.
    if "horizons_seconds" in sim_section and "horizons_minutes" in sim_section:
        raise ConfigError("simulation.horizons_seconds y horizons_minutes son incompatibles")
    if "payout_net" in sim_section and "net_payout" in sim_section:
        raise ConfigError("simulation.payout_net y net_payout son incompatibles")
    if "tie_net" in sim_section and "tie_return" in sim_section:
        raise ConfigError("simulation.tie_net y tie_return son incompatibles")
    sim_data = dict(sim_section)
    if "horizons_minutes" in sim_data:
        sim_data["horizons_seconds"] = tuple(_finite(item, "simulation.horizons_minutes", minimum=0.000001) * 60.0 for item in sim_data.pop("horizons_minutes"))
    if "net_payout" in sim_data:
        sim_data["payout_net"] = sim_data.pop("net_payout")
    if "tie_return" in sim_data:
        sim_data["tie_net"] = sim_data.pop("tie_return")
    try:
        simulation = SimulationConfig(**sim_data)
    except TypeError as exc:
        raise ConfigError(str(exc)) from exc
    quality = QualityConfig(**quality_section)
    required_strategy_tfs = {strategy.context_timeframe.name, strategy.preparation_timeframe.name, strategy.trigger_timeframe.name}
    if not required_strategy_tfs.issubset({tf.name for tf in timeframes}):
        raise ConfigError("strategy requiere temporalidades presentes en timeframes.values")
    project_name = str(project.get("name", "MTF Lab")); version = str(project.get("version", "0.1.0"))
    db = Path(storage.get("db", default_state_dir() / "mtf_lab.sqlite3")).expanduser()
    logs = Path(storage.get("logs", db.with_suffix(".jsonl"))).expanduser()
    data_effective = dict(data_section)
    data_effective.setdefault("provider", provider.get("name"))
    data_effective.setdefault("instrument", str(symbol).strip())
    data_effective.setdefault("resolution", timeframes[0].name)
    data_effective.setdefault("price_base", price_base)
    data_effective.setdefault("mode", mode)
    return EffectiveConfig(str(target.resolve()), project_name, version, mode, str(symbol).strip(), price_base, timeframes, bool(tf_section.get("closed_only", True)), indicators, strategy, quality, simulation, str(db), str(logs), MappingProxyType(data_effective), MappingProxyType(dict(provider)), MappingProxyType(dict(ui)), MappingProxyType(dict(ctrader)), MappingProxyType(dict(execution)), MappingProxyType(dict(cfd)), MappingProxyType(dict(ctrader_oauth)))

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
