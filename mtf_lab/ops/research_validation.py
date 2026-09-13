"""Pure semantic validation for v1 research manifests.

The research service owns manifest creation; this module is an independent
validator used by later composition roots.  It accepts an already-loaded
mapping only: it never opens SQLite, reads a capture, contacts a broker or
signs an authorization.  Integrity hashes are checked as one signal, but the
semantic checks below intentionally remain effective when a caller recomputes
the manifest hash after tampering.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from ..core.canonical import canonical_json
from ..core.numeric import decimal_context

_SCHEMA = "mtf-lab.research-manifest.v1"
_VERSION = 1
_PRODUCT = "FOREX_CFD_LOCAL_PAPER"
_ECONOMICS_V2 = "cfd-economics-v2"
_EXECUTION_MODELS = {"full_fill", "ioc_partial", "rejected"}
_VARIANT_DETECTORS = {
    "trend_pullback_v1": "TrendPullbackStrategy",
    "donchian20_m5_v1": "Donchian20M5Strategy",
    "m1_trigger_reference": "M1ReferenceProjection",
}
_HYPOTHESIS_ROLES = {
    "BASELINE_FROZEN",
    "DIAGNOSTIC_CONTROL",
    "DONCHIAN20_M5_IMPLEMENTED_CHALLENGER",
}
_DECISION_STATUSES = {
    "PENDING_RESULTS",
    "EVIDENCE_INSUFFICIENT",
    "NOT_ASSESSED",
    "REQUIRES_HUMAN_REVIEW",
}
_WINDOW_TOLERANCE = timedelta(microseconds=1)
_DECIMAL_TOLERANCE = Decimal("0.0000001")


def _mapping(value: Any) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _sequence(value: Any) -> Sequence[Any] | None:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return value
    return None


def _aware(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)


def _decimal(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def _positive_decimal(value: Any) -> Decimal | None:
    result = _decimal(value)
    return result if result is not None and result > 0 else None


def _nonnegative_decimal(value: Any) -> Decimal | None:
    result = _decimal(value)
    return result if result is not None and result >= 0 else None


def _is_int(value: Any, *, positive: bool = False) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and (value > 0 if positive else value >= 0)


def _same_decimal(left: Decimal, right: Decimal) -> bool:
    return abs(left - right) <= _DECIMAL_TOLERANCE


def _hash_without_integrity(manifest: Mapping[str, Any]) -> str:
    material = dict(manifest)
    material.pop("integrity_hash", None)
    return hashlib.sha256(canonical_json(material).encode("utf-8")).hexdigest()


def _add(errors: list[str], condition: bool, code: str) -> None:
    if condition and code not in errors:
        errors.append(code)


def _validate_identity(manifest: Mapping[str, Any], errors: list[str]) -> None:
    _add(
        errors,
        manifest.get("schema") != _SCHEMA
        or manifest.get("schema_version") != _VERSION
        or manifest.get("manifest_version") != _VERSION,
        "schema_version_incompatible",
    )
    expected = manifest.get("integrity_hash")
    try:
        actual = _hash_without_integrity(manifest)
    except (TypeError, ValueError, OverflowError):
        actual = None
        errors.append("integrity_hash_uncomputable")
    _add(errors, not isinstance(expected, str) or actual is None or expected != actual, "integrity_hash_mismatch")
    _add(errors, manifest.get("product") != _PRODUCT, "product_mismatch")
    _add(errors, manifest.get("state") != "COMPLETED", "manifest_not_completed")
    detector = manifest.get("detector")
    _add(
        errors,
        not isinstance(detector, str)
        or detector not in {"multiple_strategy_variants", *set(_VARIANT_DETECTORS.values())},
        "detector_mismatch",
    )
    data = _mapping(manifest.get("data"))
    config = _mapping(manifest.get("config"))
    _add(
        errors,
        data is None or not isinstance(data.get("capture_hash"), str) or not data.get("capture_hash"),
        "data_hash_missing",
    )
    _add(
        errors,
        config is None or not isinstance(config.get("hash"), str) or not config.get("hash"),
        "config_hash_missing",
    )
    if data is not None and manifest.get("data_hash") != data.get("capture_hash"):
        errors.append("data_hash_alias_mismatch")
    if config is not None and manifest.get("config_hash") != config.get("hash"):
        errors.append("config_hash_alias_mismatch")
    _add(errors, not isinstance(manifest.get("code_hash"), str) or not manifest.get("code_hash"), "code_hash_missing")
    _add(errors, not _is_int(manifest.get("seed")), "seed_missing")


def _validate_registration(
    manifest: Mapping[str, Any], errors: list[str]
) -> tuple[list[Mapping[str, Any]], list[Mapping[str, Any]]]:
    trials_value = _sequence(manifest.get("trials"))
    results_value = _sequence(manifest.get("results"))
    if trials_value is None:
        errors.append("trials_missing")
        trials: list[Mapping[str, Any]] = []
    else:
        trials = [item for item in trials_value if isinstance(item, Mapping)]
        if len(trials) != len(trials_value):
            errors.append("trial_not_mapping")
    if results_value is None:
        errors.append("results_missing")
        results: list[Mapping[str, Any]] = []
    else:
        results = [item for item in results_value if isinstance(item, Mapping)]
        if len(results) != len(results_value):
            errors.append("result_not_mapping")
    _add(errors, len(trials) != len(results), "trial_result_count_mismatch")
    registration = _mapping(manifest.get("trial_registration"))
    if registration is None:
        errors.append("trial_registration_missing")
    else:
        _add(errors, registration.get("count") != len(trials), "trial_registration_count_mismatch")
        _add(errors, registration.get("before_results") is not True, "trials_not_registered_before_results")
        _add(errors, registration.get("all_variants_and_horizons_registered") is not True, "trial_registry_incomplete")
    _add(errors, manifest.get("trials_registered_before_results") is not True, "trials_not_registered_before_results")
    trial_ids = [str(item.get("trial_id", "")) for item in trials]
    result_ids = [str(item.get("trial_id", "")) for item in results]
    _add(errors, any(not value for value in trial_ids), "trial_id_missing")
    _add(errors, any(not item.get("trial_id") for item in results), "result_id_missing")
    _add(errors, len(set(trial_ids)) != len(trial_ids), "trial_ids_not_unique")
    _add(errors, len(set(result_ids)) != len(result_ids), "result_ids_not_unique")
    _add(errors, set(trial_ids) != set(result_ids), "trial_result_ids_not_one_to_one")
    orders = [item.get("registration_order") for item in trials]
    _add(errors, orders != list(range(len(trials))), "registration_order_invalid")
    summary = _mapping(manifest.get("summary"))
    if summary is not None:
        for key, count, code in (
            ("trial_count", len(trials), "summary_trial_count_mismatch"),
            ("result_count", len(results), "summary_result_count_mismatch"),
        ):
            observed = summary.get(key)
            _add(errors, observed is not None and observed != count, code)
    return trials, results


def _validate_trial_hypothesis(
    trial: Mapping[str, Any], hypotheses: Mapping[str, Any] | None, errors: list[str]
) -> None:
    variant = str(trial.get("variant", ""))
    expected_value = hypotheses.get(variant) if hypotheses is not None else None
    expected = expected_value if isinstance(expected_value, Mapping) else None
    observed = _mapping(trial.get("hypothesis"))
    _add(errors, observed is None, "trial_hypothesis_missing")
    _add(errors, expected is None, "variant_hypothesis_missing")
    if observed is not None:
        _add(errors, str(observed.get("role")) not in _HYPOTHESIS_ROLES, "hypothesis_role_invalid")
        _add(errors, observed.get("profitability_claim") != "NONE", "profitability_claim_present")
        if expected is not None:
            _add(
                errors,
                any(
                    observed.get(key) != expected.get(key)
                    for key in ("role", "hypothesis", "profitability_claim", "description")
                ),
                "hypothesis_mismatch",
            )
    decision = _mapping(trial.get("decision"))
    _add(errors, decision is None, "trial_decision_missing")
    if decision is not None:
        _add(errors, str(decision.get("status")) not in _DECISION_STATUSES, "decision_status_invalid")
        _add(errors, decision.get("promote") is not False, "decision_promotion_not_prohibited")


def _validate_result_hypothesis_decision(
    result: Mapping[str, Any],
    trial_by_id: Mapping[str, Mapping[str, Any]],
    decisions: Mapping[str, Any] | None,
    fixture: bool,
    errors: list[str],
) -> None:
    trial_id = str(result.get("trial_id", ""))
    trial = trial_by_id.get(trial_id)
    result_hypothesis = _mapping(result.get("hypothesis"))
    _add(errors, result_hypothesis is None, "result_hypothesis_missing")
    if trial is not None:
        trial_hypothesis = _mapping(trial.get("hypothesis"))
        _add(errors, result_hypothesis != trial_hypothesis, "result_hypothesis_mismatch")
    decision = _mapping(result.get("decision"))
    _add(errors, decision is None, "result_decision_missing")
    if decision is not None:
        status = str(decision.get("status"))
        _add(errors, status not in _DECISION_STATUSES - {"PENDING_RESULTS"}, "result_decision_status_invalid")
        _add(errors, decision.get("promote") is not False, "result_decision_promotion_not_prohibited")
        _add(errors, fixture and status != "EVIDENCE_INSUFFICIENT", "fixture_decision_status_invalid")
        if decisions is not None:
            _add(errors, decisions.get(trial_id) != dict(decision), "manifest_result_decision_mismatch")


def _validate_decision_summary(manifest: Mapping[str, Any], result_count: int, errors: list[str]) -> None:
    summary = _mapping(manifest.get("summary"))
    counts = _mapping(summary.get("decision_counts")) if summary is not None else None
    _add(errors, counts is None, "decision_summary_missing")
    if counts is not None:
        total = sum(int(value) for value in counts.values() if isinstance(value, int) and not isinstance(value, bool))
        _add(errors, total != result_count, "decision_summary_count_mismatch")


def _validate_hypothesis_decisions(
    manifest: Mapping[str, Any],
    trials: Sequence[Mapping[str, Any]],
    results: Sequence[Mapping[str, Any]],
    errors: list[str],
) -> None:
    hypotheses = _mapping(manifest.get("hypotheses"))
    policy = _mapping(manifest.get("decision_policy"))
    decisions = _mapping(manifest.get("decisions"))
    _add(errors, hypotheses is None, "hypotheses_preregistration_missing")
    _add(errors, policy is None, "decision_policy_missing")
    _add(errors, decisions is None, "decisions_missing")
    if policy is not None:
        _add(errors, policy.get("auto_accept") != "PROHIBITED", "auto_accept_policy_invalid")
        _add(errors, policy.get("auto_promote") != "PROHIBITED", "auto_promote_policy_invalid")
        _add(errors, policy.get("basis") != "evidence_gates_only_not_pnl_or_sharpe", "decision_basis_invalid")
    for trial in trials:
        _validate_trial_hypothesis(trial, hypotheses, errors)
    trial_by_id = {str(item.get("trial_id")): item for item in trials}
    fixture = _manifest_fixture(manifest)
    for result in results:
        _validate_result_hypothesis_decision(result, trial_by_id, decisions, fixture, errors)
    _validate_decision_summary(manifest, len(results), errors)


def _manifest_fixture(manifest: Mapping[str, Any]) -> bool:
    data = _mapping(manifest.get("data")) or {}
    return (
        data.get("synthetic") is True
        or data.get("fixture_flag") is True
        or manifest.get("synthetic") is True
        or manifest.get("fixture") is True
    )


def _validate_trial_results(
    manifest: Mapping[str, Any],
    trials: Sequence[Mapping[str, Any]],
    results: Sequence[Mapping[str, Any]],
    errors: list[str],
) -> None:
    data = _mapping(manifest.get("data")) or {}
    config = _mapping(manifest.get("config")) or {}
    manifest_data_hash = data.get("capture_hash")
    manifest_config_hash = config.get("hash")
    manifest_instrument = str(manifest.get("instrument", ""))
    coverage = _mapping(data.get("coverage")) or {}
    capture_end = _aware(coverage.get("observed_end", coverage.get("requested_end")))
    detectors = {str(item) for item in (_sequence(manifest.get("detectors")) or ())}
    fixture = _manifest_fixture(manifest)
    costs_contract = _mapping(manifest.get("costs_contract"))
    if costs_contract is None:
        errors.append("costs_contract_missing")
    else:
        _add(errors, costs_contract.get("missing_is_zero") is True, "economic_uncertainty_default_zero")
        _add(
            errors,
            str(costs_contract.get("state", "")).upper() not in {"KNOWN", "UNKNOWN", "PARTIAL_UNKNOWN"},
            "costs_contract_state_invalid",
        )
    _add(errors, fixture and manifest.get("execution_enabled") is True, "fixture_broker_evidence_present")
    _add(errors, fixture and manifest.get("network_performed") is True, "fixture_broker_evidence_present")
    _add(errors, fixture and data.get("network_performed") is True, "fixture_broker_evidence_present")
    data_marker = data.get("broker_evidence")
    _add(errors, fixture and data_marker is not None and data_marker is not False, "fixture_broker_evidence_present")
    if fixture:
        _add(
            errors,
            data.get("synthetic") is not True or data.get("fixture_flag") is not True,
            "fixture_identity_incomplete",
        )
        _add(errors, data.get("weekends_fabricated") is True, "fixture_calendar_fabricated")
        marker = manifest.get("broker_evidence")
        _add(errors, marker is not None and marker is not False, "fixture_broker_evidence_present")
    trial_by_id = {str(item.get("trial_id")): item for item in trials}
    for result in results:
        trial_id = str(result.get("trial_id", ""))
        trial = trial_by_id.get(trial_id)
        if trial is None:
            continue
        variant = str(trial.get("variant", ""))
        expected_detector = _VARIANT_DETECTORS.get(variant)
        _add(
            errors,
            result.get("registration_order") != trial.get("registration_order"),
            "result_registration_order_mismatch",
        )
        _add(errors, result.get("variant") != variant, "result_variant_mismatch")
        _add(
            errors,
            _decimal(result.get("horizon_seconds")) != _decimal(trial.get("horizon_seconds")),
            "result_horizon_mismatch",
        )
        _add(
            errors,
            result.get("config_hash") != trial.get("config_hash") or result.get("config_hash") != manifest_config_hash,
            "result_config_mismatch",
        )
        _add(errors, result.get("capture_hash") != manifest_data_hash, "result_data_mismatch")
        _add(
            errors,
            result.get("product") != manifest.get("product") or result.get("product") != _PRODUCT,
            "result_product_mismatch",
        )
        _add(errors, expected_detector is None, "unknown_variant")
        if expected_detector is not None:
            _add(errors, result.get("detector") != expected_detector, "result_detector_mismatch")
            if detectors:
                _add(errors, expected_detector not in detectors, "manifest_detector_registry_mismatch")
        _validate_result_shape(result, manifest_instrument, errors)
        _validate_broker_marker(result, fixture, errors)
        _validate_execution_contract(manifest, trial, result, errors)
        _validate_ledger(result, manifest_instrument, capture_end, errors)
        _validate_cost_summary(result, errors)


def _validate_result_shape(result: Mapping[str, Any], instrument: str, errors: list[str]) -> None:
    ledger = _sequence(result.get("ledger"))
    _add(errors, ledger is None, "result_ledger_missing")
    if ledger is None:
        return
    trades = result.get("trades")
    _add(errors, not _is_int(trades) or trades != len(ledger), "result_trade_count_mismatch")
    _add(errors, not _is_int(result.get("signals")), "result_signal_count_invalid")
    for row in ledger:
        if isinstance(row, Mapping):
            _add(errors, row.get("product") != _PRODUCT, "ledger_product_mismatch")
            _add(
                errors, bool(instrument) and str(row.get("instrument", "")) != instrument, "ledger_instrument_mismatch"
            )
        else:
            errors.append("ledger_row_not_mapping")


def _validate_broker_marker(result: Mapping[str, Any], fixture: bool, errors: list[str]) -> None:
    if not fixture:
        return
    marker = result.get("broker_evidence")
    _add(errors, marker is not None and marker is not False, "fixture_broker_evidence_present")
    execution = _mapping(result.get("execution_model"))
    if execution is None:
        errors.append("execution_model_missing")
        return
    external = str(execution.get("external_broker_fills", "")).strip().upper()
    _add(errors, external not in {"NOT_OBSERVED", "NONE", "FALSE", "ABSENT"}, "fixture_broker_evidence_present")


def _validate_execution_contract(
    manifest: Mapping[str, Any], trial: Mapping[str, Any], result: Mapping[str, Any], errors: list[str]
) -> None:
    manifest_execution = _mapping(manifest.get("execution_model")) or {}
    policy = _mapping(manifest.get("policy")) or {}
    policy_execution = _mapping(policy.get("execution_parameters")) or {}
    trial_execution = str(trial.get("execution_model", "")).lower()
    result_execution = _mapping(result.get("execution_model"))
    scenario = str(manifest_execution.get("scenario", manifest_execution.get("model", ""))).lower()
    _add(errors, scenario not in _EXECUTION_MODELS, "execution_model_invalid")
    _add(errors, trial_execution != scenario, "trial_execution_model_mismatch")
    _add(
        errors,
        not manifest_execution.get("parameters") and not policy_execution,
        "execution_parameters_missing",
    )
    _add(errors, not trial.get("execution_parameters"), "trial_execution_parameters_missing")
    if policy.get("execution_model") is not None:
        _add(errors, str(policy.get("execution_model")).lower() != scenario, "policy_execution_model_mismatch")
    if policy_execution:
        _add(
            errors,
            str(policy_execution.get("scenario", "")).lower() != scenario,
            "policy_execution_parameters_mismatch",
        )
    if result_execution is None:
        errors.append("result_execution_model_missing")
        return
    _add(
        errors,
        str(result_execution.get("scenario", result_execution.get("model", ""))).lower() != scenario,
        "result_execution_model_mismatch",
    )
    orders = result_execution.get("orders")
    _add(errors, orders is not None and orders != result.get("trades"), "execution_order_count_mismatch")
    _add(errors, result_execution.get("synthetic_hypothesis") is not True, "execution_hypothesis_unlabelled")
    parameters = _mapping(result_execution.get("parameters")) or {}
    if parameters:
        _add(
            errors,
            str(parameters.get("scenario", parameters.get("model", ""))).lower() != scenario,
            "execution_parameters_mismatch",
        )
    _validate_execution_parameters(manifest_execution, trial, result_execution, errors)
    ledger = _sequence(result.get("ledger")) or ()
    requested_total = _nonnegative_decimal(result_execution.get("requested_quantity"))
    filled_total = _nonnegative_decimal(result_execution.get("filled_quantity"))
    cancelled_total = _nonnegative_decimal(result_execution.get("cancelled_quantity"))
    rejected_total = _nonnegative_decimal(result_execution.get("rejected_quantity"))
    if None in {requested_total, filled_total, cancelled_total, rejected_total}:
        errors.append("execution_quantity_invalid")
    else:
        assert (
            requested_total is not None
            and filled_total is not None
            and cancelled_total is not None
            and rejected_total is not None
        )
        _add(
            errors,
            requested_total < 0 or filled_total + cancelled_total + rejected_total != requested_total,
            "execution_quantity_not_reconcilable",
        )
    for row in ledger:
        if isinstance(row, Mapping):
            _validate_execution_row(row, scenario, errors)


def _validate_execution_row(row: Mapping[str, Any], scenario: str, errors: list[str]) -> None:
    quantities = {
        name: _nonnegative_decimal(row.get(name))
        for name in ("requested_quantity", "filled_quantity", "cancelled_quantity", "rejected_quantity")
    }
    if any(value is None for value in quantities.values()):
        errors.append("execution_quantity_invalid")
        return
    requested = quantities["requested_quantity"]
    filled = quantities["filled_quantity"]
    cancelled = quantities["cancelled_quantity"]
    rejected = quantities["rejected_quantity"]
    assert requested is not None and filled is not None and cancelled is not None and rejected is not None
    _add(errors, requested <= 0, "execution_requested_quantity_invalid")
    _add(errors, filled + cancelled + rejected != requested, "execution_quantity_not_reconcilable")
    executed = row.get("executed")
    _add(errors, not isinstance(executed, bool), "execution_executed_flag_invalid")
    if isinstance(executed, bool):
        _add(errors, executed != (filled > 0), "execution_executed_flag_mismatch")
    if filled > 0:
        units = _positive_decimal(row.get("units"))
        _add(errors, units is None or units != filled, "execution_units_mismatch")
    if scenario == "rejected":
        _add(errors, str(row.get("state", "")).upper() != "REJECTED", "rejected_state_mismatch")
        _add(
            errors,
            filled != 0
            or rejected != requested
            or any(row.get(name) is not None for name in ("entry_price", "entry_available_at", "entry_quote_id")),
            "rejected_execution_has_fill",
        )
    elif scenario == "full_fill":
        _add(errors, filled != requested or cancelled != 0 or rejected != 0, "full_fill_quantity_mismatch")
    elif scenario == "ioc_partial":
        _add(errors, filled <= 0 or cancelled <= 0 or rejected != 0, "ioc_partial_quantity_mismatch")
        _add(errors, row.get("cancelled_on_first_fill") is not True, "ioc_cancel_metadata_missing")


def _validate_execution_parameters(
    manifest_execution: Mapping[str, Any],
    trial: Mapping[str, Any],
    result_execution: Mapping[str, Any],
    errors: list[str],
) -> None:
    manifest_parameters = _mapping(manifest_execution.get("parameters")) or manifest_execution
    trial_parameters = _mapping(trial.get("execution_parameters")) or {}
    result_parameters = _mapping(result_execution.get("parameters")) or {}
    for key in (
        "scenario",
        "fill_fraction",
        "requested_quantity",
        "effective_quantity",
        "cancelled_quantity",
        "rejected_quantity",
    ):
        expected = manifest_parameters.get(key, trial_parameters.get(key))
        observed = result_parameters.get(key)
        if key in manifest_parameters and key in trial_parameters:
            manifest_value = manifest_parameters[key]
            trial_value = trial_parameters[key]
            if key == "scenario":
                _add(errors, str(manifest_value).lower() != str(trial_value).lower(), "execution_parameters_mismatch")
            elif manifest_value is None or trial_value is None:
                _add(errors, manifest_value != trial_value, "execution_parameters_mismatch")
            else:
                left = _decimal(manifest_value)
                right = _decimal(trial_value)
                _add(errors, left is None or right is None or left != right, "execution_parameters_mismatch")
        if expected is None or observed is None:
            continue
        if key == "scenario":
            matches = str(expected).lower() == str(observed).lower()
        else:
            expected_decimal = _decimal(expected)
            observed_decimal = _decimal(observed)
            matches = (
                expected_decimal is not None and observed_decimal is not None and expected_decimal == observed_decimal
            )
        _add(errors, not matches, "execution_parameters_mismatch")


def _validate_ledger(
    result: Mapping[str, Any], instrument: str, capture_end: datetime | None, errors: list[str]
) -> None:
    ledger_value = _sequence(result.get("ledger")) or ()
    for row in ledger_value:
        if not isinstance(row, Mapping):
            continue
        parsed = _validate_ledger_times(row, errors)
        if result.get("capture_complete") is True and capture_end is not None:
            observed = {name: value for name, value in parsed.items() if "target" not in name}
            _add(errors, any(value > capture_end for value in observed.values()), "ledger_timestamp_outside_capture")
        _validate_economics(row, errors)


def _validate_cost_summary(result: Mapping[str, Any], errors: list[str]) -> None:
    summary = _mapping(result.get("costs"))
    if summary is None:
        errors.append("result_costs_summary_missing")
        return
    ledger = _sequence(result.get("ledger")) or ()
    unknown_rows = 0
    for row in ledger:
        if not isinstance(row, Mapping):
            continue
        state = str(row.get("state", "")).upper()
        if state in {"CLOSED", "UNKNOWN", "FILLED"} and row.get("net_pnl") is None:
            unknown_rows += 1
    reported = summary.get("unknown_count")
    _add(errors, not _is_int(reported) or reported != unknown_rows, "cost_summary_unknown_count_mismatch")
    state = str(summary.get("state", "")).upper()
    expected_state = "PARTIAL_UNKNOWN" if unknown_rows else "KNOWN"
    _add(errors, state != expected_state, "cost_summary_state_mismatch")
    reasons = _sequence(summary.get("unknown_reasons"))
    _add(errors, unknown_rows > 0 and (reasons is None or not reasons), "cost_summary_unknown_reason_missing")


def _validate_ledger_times(row: Mapping[str, Any], errors: list[str]) -> dict[str, datetime]:
    parsed = _parse_ledger_times(row, errors)
    required = ("detected_at", "signal_available_at", "decision_at", "entry_target_at")
    if len(parsed) >= len(required) and all(name in parsed for name in required):
        _add(
            errors,
            not (
                parsed["detected_at"]
                <= parsed["signal_available_at"]
                <= parsed["decision_at"]
                <= parsed["entry_target_at"]
            ),
            "ledger_causality_violation",
        )
    _validate_optional_ledger_order(parsed, errors)
    return parsed


def _parse_ledger_times(row: Mapping[str, Any], errors: list[str]) -> dict[str, datetime]:
    required = ("detected_at", "signal_available_at", "decision_at", "entry_target_at")
    parsed: dict[str, datetime] = {}
    for name in required:
        timestamp = _aware(row.get(name))
        _add(errors, timestamp is None, "ledger_timestamp_invalid")
        if timestamp is not None:
            parsed[name] = timestamp
    optional_names = (
        "entry_market_at",
        "entry_available_at",
        "close_target_at",
        "close_market_at",
        "close_available_at",
    )
    for name in optional_names:
        value = row.get(name)
        if value is not None:
            timestamp = _aware(value)
            _add(errors, timestamp is None, "ledger_timestamp_invalid")
            if timestamp is not None:
                parsed[name] = timestamp
    return parsed


def _validate_optional_ledger_order(parsed: Mapping[str, datetime], errors: list[str]) -> None:
    _validate_entry_order(parsed, errors)
    _validate_close_order(parsed, errors)


def _validate_entry_order(parsed: Mapping[str, datetime], errors: list[str]) -> None:
    if "entry_available_at" in parsed:
        _add(
            errors,
            parsed["entry_available_at"] < parsed.get("entry_target_at", parsed["entry_available_at"]),
            "ledger_causality_violation",
        )
        if "signal_available_at" in parsed:
            _add(errors, parsed["entry_available_at"] < parsed["signal_available_at"], "ledger_causality_violation")
        if "entry_market_at" in parsed:
            _add(errors, parsed["entry_market_at"] > parsed["entry_available_at"], "ledger_causality_violation")
    if "entry_market_at" in parsed:
        for name in ("signal_available_at", "decision_at", "entry_target_at"):
            if name in parsed:
                _add(errors, parsed["entry_market_at"] < parsed[name], "ledger_causality_violation")


def _validate_close_order(parsed: Mapping[str, datetime], errors: list[str]) -> None:
    if "close_available_at" in parsed:
        for name in ("entry_available_at", "close_target_at", "close_market_at"):
            if name in parsed:
                _add(errors, parsed[name] > parsed["close_available_at"], "ledger_causality_violation")
    if "close_market_at" in parsed and "entry_market_at" in parsed:
        _add(errors, parsed["close_market_at"] < parsed["entry_market_at"], "ledger_causality_violation")
    if "close_target_at" in parsed and "entry_available_at" in parsed:
        _add(errors, parsed["close_target_at"] < parsed["entry_available_at"], "ledger_causality_violation")
    if "close_target_at" in parsed and "entry_target_at" in parsed:
        _add(errors, parsed["close_target_at"] < parsed["entry_target_at"], "ledger_causality_violation")


def _validate_economics(row: Mapping[str, Any], errors: list[str]) -> None:
    economic = _mapping(row.get("economic_result")) or {}
    determined = _validate_economic_status(row, economic, errors)
    if determined:
        _validate_economic_math(row, economic, errors)


def _validate_economic_status(row: Mapping[str, Any], economic: Mapping[str, Any], errors: list[str]) -> bool:
    version = row.get("economics_version")
    state = str(row.get("state", "")).upper()
    economic_state = str(economic.get("state", economic.get("economic_state", ""))).upper()
    values_present = any(
        row.get(name) is not None for name in ("entry_price", "close_price", "pips", "gross_pnl_quote", "net_pnl")
    )
    if values_present or state == "CLOSED":
        _add(errors, version != _ECONOMICS_V2, "economics_version_not_v2")
    determined = state == "CLOSED" and row.get("net_pnl") is not None and economic_state in {"", "DETERMINED"}
    if economic_state == "DETERMINED" and row.get("net_pnl") is None:
        errors.append("economic_state_without_net")
    if economic_state in {"INDETERMINATE", "NOT_SETTLED"} and row.get("net_pnl") is not None:
        errors.append("economic_uncertainty_hidden")
    costs_known = row.get("costs_known")
    unknown_reason = str(row.get("costs_unknown_reason", "")).strip()
    no_entry_declared = (
        state == "REJECTED"
        and row.get("entry_price") is None
        and bool(str(row.get("reason", "")).strip() or unknown_reason)
    )
    _add(errors, costs_known is not None and not isinstance(costs_known, bool), "economic_uncertainty_unlabelled")
    _add(errors, determined and costs_known is not True, "economic_uncertainty_unlabelled")
    if costs_known is False:
        _add(errors, not unknown_reason, "economic_uncertainty_unlabelled")
        if row.get("net_pnl") is not None:
            errors.append("economic_uncertainty_hidden")
        row_costs = _decimal(row.get("costs_account"))
        economic_costs = _decimal(economic.get("costs_account"))
        if row_costs == Decimal("0") and economic_costs == Decimal("0") and not no_entry_declared:
            errors.append("economic_uncertainty_default_zero")
    if (
        state == "UNKNOWN"
        and not str(row.get("reason", "")).strip()
        and not str(economic.get("reason", economic.get("economic_reason", ""))).strip()
    ):
        errors.append("economic_uncertainty_unlabelled")
    return determined


def _validate_economic_math(row: Mapping[str, Any], economic: Mapping[str, Any], errors: list[str]) -> None:
    direction = str(row.get("direction", "")).upper()
    sign = Decimal("1") if direction == "LONG" else Decimal("-1") if direction == "SHORT" else None
    units = _positive_decimal(row.get("units"))
    entry = _decimal(row.get("entry_price"))
    close = _decimal(row.get("close_price"))
    pip_size = _positive_decimal(row.get("pip_size"))
    gross_quote = _decimal(row.get("gross_pnl_quote"))
    pips = _decimal(row.get("pips"))
    if (
        sign is None
        or units is None
        or entry is None
        or close is None
        or pip_size is None
        or gross_quote is None
        or pips is None
    ):
        errors.append("economic_inputs_missing")
        return
    expected_gross = (close - entry) * units * sign
    expected_pips = (close - entry) / pip_size * sign
    _add(errors, not _same_decimal(gross_quote, expected_gross), "economic_gross_not_reconstructible")
    _add(errors, not _same_decimal(pips, expected_pips), "economic_pips_not_reconstructible")
    commission = _fee_value(row.get("commission_quote"), "commission_quote", errors)
    financing = _fee_value(row.get("financing_quote"), "financing_quote", errors)
    slippage = _fee_value(row.get("slippage_quote"), "slippage_quote", errors)
    if commission is None or financing is None or slippage is None:
        errors.append("economic_fee_inputs_missing")
        return
    expected_costs_quote = commission + financing
    economic_gross_quote = _decimal(economic.get("gross_pnl_quote"))
    economic_costs_quote = _decimal(economic.get("costs_quote"))
    _add(
        errors,
        economic_gross_quote is None or not _same_decimal(economic_gross_quote, expected_gross),
        "economic_result_gross_mismatch",
    )
    _add(
        errors,
        economic_costs_quote is None or not _same_decimal(economic_costs_quote, expected_costs_quote),
        "economic_costs_not_reconstructible",
    )
    gross_account = _decimal(row.get("gross_pnl_account"))
    costs_account = _decimal(row.get("costs_account"))
    net = _decimal(row.get("net_pnl"))
    if gross_account is None or costs_account is None or net is None:
        errors.append("economic_accounting_inputs_missing")
        return
    quote_currency = str(row.get("quote_currency", "") or "").upper()
    account_currency = str(row.get("account_currency", "") or "").upper()
    conversion = _positive_decimal(row.get("conversion_rate"))
    expected_gross_account = expected_gross
    if quote_currency and account_currency and quote_currency != account_currency:
        if conversion is None:
            errors.append("conversion_rate_missing")
            return
        expected_gross_account *= conversion
        expected_costs_account = expected_costs_quote * conversion
    else:
        expected_costs_account = expected_costs_quote
    _add(errors, not _same_decimal(gross_account, expected_gross_account), "economic_gross_account_not_reconstructible")
    _add(errors, not _same_decimal(costs_account, expected_costs_account), "economic_costs_account_not_reconstructible")
    economic_gross_account = _decimal(economic.get("gross_pnl_account"))
    economic_costs_account = _decimal(economic.get("costs_account"))
    _add(
        errors,
        economic_gross_account is None or not _same_decimal(economic_gross_account, gross_account),
        "economic_result_gross_account_mismatch",
    )
    _add(
        errors,
        economic_costs_account is None or not _same_decimal(economic_costs_account, costs_account),
        "economic_result_costs_account_mismatch",
    )
    economic_net = _decimal(economic.get("net_pnl"))
    _add(errors, economic_net is not None and not _same_decimal(economic_net, net), "economic_result_net_mismatch")
    _add(errors, not _same_decimal(net, gross_account - costs_account), "economic_net_not_reconstructible")
    _validate_v2_slippage(row, expected_gross, errors)


def _validate_v2_slippage(row: Mapping[str, Any], expected_gross: Decimal, errors: list[str]) -> None:
    slippage = _nonnegative_decimal(row.get("slippage_quote"))
    reference_gross = _decimal(row.get("reference_gross_pnl_quote"))
    if row.get("slippage_quote") is not None and slippage is None:
        errors.append("economic_slippage_invalid")
    if slippage is not None and reference_gross is not None:
        _add(
            errors,
            not _same_decimal(reference_gross - expected_gross, slippage),
            "economic_slippage_not_reconstructible",
        )


def _fee_value(value: Any, name: str, errors: list[str]) -> Decimal | None:
    if value is None:
        return None
    result = _nonnegative_decimal(value)
    _add(errors, result is None, f"economic_{name}_invalid")
    return result


def _validate_windows(manifest: Mapping[str, Any], errors: list[str]) -> None:
    windows_value = _sequence(manifest.get("windows"))
    if windows_value is None:
        errors.append("windows_missing")
        return
    _add(errors, len(windows_value) != 5, "windows_count_mismatch")
    data = _mapping(manifest.get("data")) or {}
    coverage = _mapping(data.get("coverage")) or {}
    range_start = _aware(coverage.get("observed_start", coverage.get("requested_start")))
    range_end = _aware(coverage.get("observed_end", coverage.get("requested_end")))
    rows = [item for item in windows_value if isinstance(item, Mapping)]
    if len(rows) != len(windows_value):
        errors.append("window_not_mapping")
    if range_start is None or range_end is None or range_end <= range_start:
        errors.append("window_range_missing")
        return
    policy = _mapping(manifest.get("policy"))
    _validate_window_policy(policy, errors)
    if len(rows) != 5:
        return
    expected_names = [f"walkforward_{index}" for index in range(1, 5)] + ["holdout"]
    if [str(row.get("name")) for row in rows] != expected_names:
        errors.append("window_order_invalid")
    span = range_end - range_start

    def at(fraction: Decimal) -> datetime:
        return range_start + timedelta(seconds=span.total_seconds() * float(fraction))

    for index, row in enumerate(rows[:4]):
        _add(errors, row.get("kind") != "walkforward" or row.get("index") != index, "window_kind_or_index_invalid")
        _validate_window_counts(row, errors)
        start = _window_time(row, "warmup_start", errors)
        train_start = _window_time(row, "train_start", errors)
        train_end = _window_time(row, "train_end", errors)
        test_start = _window_time(row, "test_start", errors)
        test_end = _window_time(row, "test_end", errors)
        if None in {start, train_start, train_end, test_start, test_end}:
            continue
        assert (
            start is not None
            and train_start is not None
            and train_end is not None
            and test_start is not None
            and test_end is not None
        )
        expected_train_end = at(Decimal("0.4") + Decimal(index) * Decimal("0.1"))
        expected_test_end = at(Decimal("0.5") + Decimal(index) * Decimal("0.1"))
        _add(errors, start != range_start or train_start != range_start, "window_warmup_range_invalid")
        _add(errors, train_end != test_start, "window_train_test_boundary_invalid")
        _add(errors, abs(train_end - expected_train_end) > _WINDOW_TOLERANCE, "window_train_fraction_invalid")
        _add(errors, abs(test_end - expected_test_end) > _WINDOW_TOLERANCE, "window_test_fraction_invalid")
        _add(
            errors,
            not range_start <= train_start <= train_end <= test_start <= test_end <= range_end,
            "window_future_or_overlap",
        )
    row = rows[4]
    _add(errors, row.get("kind") != "holdout" or row.get("index") != 0, "window_kind_or_index_invalid")
    _validate_window_counts(row, errors)
    start = _window_time(row, "warmup_start", errors)
    train_start = _window_time(row, "train_start", errors)
    train_end = _window_time(row, "train_end", errors)
    test_start = _window_time(row, "test_start", errors)
    test_end = _window_time(row, "test_end", errors)
    if None not in {start, train_start, train_end, test_start, test_end}:
        assert (
            start is not None
            and train_start is not None
            and train_end is not None
            and test_start is not None
            and test_end is not None
        )
        _add(errors, start != range_start or train_start != range_start, "window_warmup_range_invalid")
        _add(errors, train_end != test_start, "window_train_test_boundary_invalid")
        _add(errors, abs(train_end - at(Decimal("0.8"))) > _WINDOW_TOLERANCE, "holdout_fraction_invalid")
        _add(errors, test_end != range_end, "holdout_range_invalid")
        _add(
            errors,
            not range_start <= train_start <= train_end <= test_start <= test_end <= range_end,
            "window_future_or_overlap",
        )
    previous_end = _aware(rows[3].get("test_end"))
    holdout_train_end = _aware(row.get("train_end"))
    _add(
        errors,
        previous_end is None or holdout_train_end is None or previous_end != holdout_train_end,
        "walkforward_holdout_gap_invalid",
    )


def _validate_window_policy(policy: Mapping[str, Any] | None, errors: list[str]) -> None:
    if policy is None:
        errors.append("policy_missing")
        return
    expected = {
        "holdout_fraction": 0.2,
        "walkforward_folds": 4,
        "walkforward_train_fraction": 0.4,
        "walkforward_test_fraction": 0.1,
    }
    for key, value in expected.items():
        observed = policy.get(key)
        try:
            observed_decimal = _decimal(observed)
            matches = observed_decimal is not None and math.isclose(
                float(observed_decimal), float(value), rel_tol=0.0, abs_tol=1e-12
            )
        except (TypeError, ValueError):
            matches = False
        _add(errors, not matches, f"{key}_policy_mismatch")
    _add(errors, policy.get("warmup_preserved") is not True, "warmup_not_preserved")


def _validate_window_counts(row: Mapping[str, Any], errors: list[str]) -> None:
    _add(errors, row.get("warmup_preserved") is not True, "warmup_not_preserved")
    for name in ("warmup_records", "train_records", "test_records", "embargo_records"):
        _add(errors, not _is_int(row.get(name)), "window_count_invalid")
    for name in ("purge_holding_seconds", "embargo_seconds"):
        _add(errors, _nonnegative_decimal(row.get(name)) is None, "window_duration_invalid")


def _window_time(row: Mapping[str, Any], name: str, errors: list[str]) -> datetime | None:
    value = _aware(row.get(name))
    _add(errors, value is None, "window_timestamp_invalid")
    return value


def _validate_holdout_finality(manifest: Mapping[str, Any], errors: list[str]) -> None:
    policy = _mapping(manifest.get("policy")) or {}
    summary = _mapping(manifest.get("summary")) or {}
    used_values = (
        policy.get("holdout_used_for_tuning"),
        policy.get("holdout_used_for_adjustment"),
        policy.get("final_holdout_used_for_tuning"),
        summary.get("holdout_used_for_tuning"),
        summary.get("holdout_used_for_adjustment"),
        summary.get("final_holdout_used_for_tuning"),
    )
    used = any(value is True for value in used_values)
    approved = policy.get(
        "final_holdout_approved",
        policy.get("final_approval", summary.get("final_holdout_approved", summary.get("holdout_approval"))),
    )
    status = str(summary.get("final_status", summary.get("status", summary.get("promotion_status", "")))).upper()
    declared_final_dataset = str(
        summary.get("final_dataset", summary.get("final_evaluation_dataset", policy.get("final_dataset", "")))
    ).lower()
    if used is True and (approved is True or status in {"APPROVED", "PROMOTED", "ACCEPTED"}):
        errors.append("holdout_used_for_tuning_cannot_be_final_approval")
    if used is True and declared_final_dataset in {"holdout", "final_holdout"}:
        errors.append("holdout_used_for_tuning_cannot_be_final_approval")


@decimal_context()
def validate_manifest_contract(manifest: Mapping[str, Any]) -> list[str]:
    """Return stable semantic contract errors for one loaded manifest.

    The return value is intentionally a list of codes rather than a signed
    attestation. Empty means that this bounded validator found no violation;
    it does not prove profitability, broker authorization, or completeness of
    unobserved historical attempts.
    """

    if not isinstance(manifest, Mapping):
        return ["manifest_not_mapping"]
    errors: list[str] = []
    _validate_identity(manifest, errors)
    trials, results = _validate_registration(manifest, errors)
    _validate_hypothesis_decisions(manifest, trials, results, errors)
    _validate_trial_results(manifest, trials, results, errors)
    _validate_windows(manifest, errors)
    _validate_holdout_finality(manifest, errors)
    return list(dict.fromkeys(errors))


__all__ = ["validate_manifest_contract"]
