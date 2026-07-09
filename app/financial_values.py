"""Boundary validation for financial values.

Keep this module dependency-free so ingestion, workers, and HTTP routes can use
the same rules without importing database or application state.
"""

from __future__ import annotations

import math
import json
from numbers import Real
from typing import Any


MAX_ABS_MONEY = 1_000_000_000.0


class InvalidFinancialValueError(ValueError):
    """Raised when an untrusted financial value is unsafe to persist."""


def sanitize_nonfinite_json_values(value: Any) -> Any:
    """Preserve legacy invalid-number evidence using strict-JSON markers."""

    if isinstance(value, float) and not math.isfinite(value):
        if math.isnan(value):
            label = "nan"
        elif value > 0:
            label = "positive_infinity"
        else:
            label = "negative_infinity"
        return {"invalid_numeric_value": label}
    if isinstance(value, dict):
        return {
            str(key): sanitize_nonfinite_json_values(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [sanitize_nonfinite_json_values(item) for item in value]
    return value


def validate_strict_json_value(
    value: Any,
    *,
    field_name: str = "value",
) -> Any:
    """Reject values that Python would serialize as non-standard JSON.

    Parser responses are copied into logs and JSON-backed columns in several
    places.  Python's default encoder silently emits ``NaN`` and ``Infinity``;
    validating the complete result before any logging or mutation keeps those
    tokens out of durable state, including nested metadata.
    """

    try:
        json.dumps(value, allow_nan=False)
    except (TypeError, ValueError, OverflowError) as exc:
        raise InvalidFinancialValueError(
            f"{field_name} must contain only strict JSON values"
        ) from exc
    return value


def _validate_real_number(value: Any, *, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise InvalidFinancialValueError(f"{field_name} must be a number")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise InvalidFinancialValueError(f"{field_name} must be a number") from exc
    if not math.isfinite(number):
        raise InvalidFinancialValueError(f"{field_name} must be finite")
    return number


def validate_optional_money(
    value: Any,
    *,
    field_name: str = "money value",
) -> float | None:
    """Validate an already-numeric optional money value without rounding it."""

    if value is None:
        return None
    number = _validate_real_number(value, field_name=field_name)
    if abs(number) > MAX_ABS_MONEY:
        raise InvalidFinancialValueError(
            f"{field_name} must be between {-MAX_ABS_MONEY:.0f} and {MAX_ABS_MONEY:.0f}"
        )
    return number


def validate_optional_confidence(
    value: Any,
    *,
    field_name: str = "confidence",
) -> float | None:
    """Validate an already-numeric optional confidence score."""

    if value is None:
        return None
    number = _validate_real_number(value, field_name=field_name)
    if not 0.0 <= number <= 1.0:
        raise InvalidFinancialValueError(f"{field_name} must be between 0 and 1")
    return number


def _parse_optional_number(value: Any, *, field_name: str) -> float | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise InvalidFinancialValueError(f"{field_name} must be a number")
    text = value.strip()
    if not text:
        return None
    try:
        return float(text)
    except (TypeError, ValueError, OverflowError) as exc:
        raise InvalidFinancialValueError(f"{field_name} must be a number") from exc


def parse_optional_money(
    value: Any,
    *,
    field_name: str = "amount",
) -> float | None:
    """Parse and validate an optional money string from an input boundary."""

    parsed = _parse_optional_number(value, field_name=field_name)
    return validate_optional_money(parsed, field_name=field_name)


def parse_optional_confidence(
    value: Any,
    *,
    field_name: str = "confidence",
) -> float | None:
    """Parse and validate an optional confidence string from an input boundary."""

    parsed = _parse_optional_number(value, field_name=field_name)
    return validate_optional_confidence(parsed, field_name=field_name)
