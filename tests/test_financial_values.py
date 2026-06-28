from __future__ import annotations

import json
import math

import pytest

from app.financial_values import (
    MAX_ABS_MONEY,
    InvalidFinancialValueError,
    parse_optional_confidence,
    parse_optional_money,
    sanitize_nonfinite_json_values,
    validate_optional_confidence,
    validate_optional_money,
    validate_strict_json_value,
)


@pytest.mark.parametrize("value", [None, "", "   "])
def test_optional_financial_parsers_treat_blank_as_missing(value) -> None:
    assert parse_optional_money(value) is None
    assert parse_optional_confidence(value) is None


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (f"-{MAX_ABS_MONEY:.0f}", -MAX_ABS_MONEY),
        (f"{MAX_ABS_MONEY:.0f}", MAX_ABS_MONEY),
        ("0", 0.0),
        ("12.5", 12.5),
    ],
)
def test_parse_optional_money_accepts_supported_finite_bounds(value, expected) -> None:
    assert parse_optional_money(value) == expected


@pytest.mark.parametrize(
    "value",
    ["nan", "NaN", "inf", "+inf", "-inf", "Infinity", "1e309", "1000000000.01", "not-money"],
)
def test_parse_optional_money_rejects_nonfinite_overflow_and_out_of_range(value) -> None:
    with pytest.raises(InvalidFinancialValueError):
        parse_optional_money(value)


@pytest.mark.parametrize(
    "value",
    [
        False,
        True,
        "1",
        object(),
        complex(1, 0),
        math.nan,
        math.inf,
        -math.inf,
        1_000_000_000.01,
        pytest.param(10**10_000, id="huge-int"),
    ],
)
def test_validate_optional_money_rejects_bool_strings_nonnumeric_and_unsafe_numbers(value) -> None:
    with pytest.raises(InvalidFinancialValueError):
        validate_optional_money(value)


@pytest.mark.parametrize("value", [-MAX_ABS_MONEY, 0, 12.5, MAX_ABS_MONEY])
def test_validate_optional_money_accepts_numeric_supported_bounds(value) -> None:
    assert validate_optional_money(value) == float(value)


@pytest.mark.parametrize("value", ["0", "1", "0.5"])
def test_parse_optional_confidence_accepts_inclusive_bounds(value) -> None:
    assert parse_optional_confidence(value) == float(value)


@pytest.mark.parametrize("value", ["nan", "inf", "-inf", "1e309", "-0.0001", "1.0001", "not-confidence"])
def test_parse_optional_confidence_rejects_nonfinite_and_out_of_range(value) -> None:
    with pytest.raises(InvalidFinancialValueError):
        parse_optional_confidence(value)


@pytest.mark.parametrize("value", [False, True, "0.5", object(), math.nan, math.inf, -0.01, 1.01])
def test_validate_optional_confidence_rejects_bool_strings_nonnumeric_and_unsafe_numbers(value) -> None:
    with pytest.raises(InvalidFinancialValueError):
        validate_optional_confidence(value)


@pytest.mark.parametrize("value", [0, 0.5, 1])
def test_validate_optional_confidence_accepts_numeric_inclusive_bounds(value) -> None:
    assert validate_optional_confidence(value) == float(value)


@pytest.mark.parametrize(
    "value",
    [
        {"nested": [float("nan")]},
        {"nested": {"value": float("inf")}},
        {"nested": {"value": -float("inf")}},
        {"unsupported": object()},
    ],
)
def test_validate_strict_json_value_rejects_nested_non_json_values(value) -> None:
    with pytest.raises(InvalidFinancialValueError, match="strict JSON"):
        validate_strict_json_value(value, field_name="parser result")


def test_validate_strict_json_value_accepts_nested_standard_json() -> None:
    value = {"items": [{"name": "Card", "value": 12.5}], "ok": True, "note": None}
    assert validate_strict_json_value(value) is value


def test_sanitize_nonfinite_json_values_uses_strict_json_evidence_markers() -> None:
    sanitized = sanitize_nonfinite_json_values(
        {"values": [float("nan"), float("inf"), -float("inf"), 12.5]}
    )

    assert sanitized == {
        "values": [
            {"invalid_numeric_value": "nan"},
            {"invalid_numeric_value": "positive_infinity"},
            {"invalid_numeric_value": "negative_infinity"},
            12.5,
        ]
    }
    json.dumps(sanitized, allow_nan=False)
