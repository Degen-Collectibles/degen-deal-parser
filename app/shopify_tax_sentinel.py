from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import json
import math
from typing import Any

from sqlmodel import select

from .config import get_settings
from .db import managed_session
from .inventory.shopify import (
    get_shopify_locations,
    resolve_shopify_access_token,
    shopify_admin_configured,
)
from .models import AppSetting, ShopifyOrder, ShopifySyncIssue, utcnow
from .runtime_logging import structured_log_line
from .shopify_tax_sources import (
    CDTFA_CITY_RATES_URL,
    fetch_cdtfa_city_rate,
    fetch_non_taxable_physical_variants,
)

from .shopify_sync import (
    SHOPIFY_SYNC_ISSUE_OPEN,
    SHOPIFY_SYNC_ISSUE_RESOLVED,
    SHOPIFY_SYNC_ISSUE_SYNC_ERROR,
    SHOPIFY_TAX_ISSUE_OFFICIAL_RATE_CHANGED,
    SHOPIFY_TAX_ISSUE_OFFICIAL_SOURCE_UNAVAILABLE,
    SHOPIFY_TAX_ISSUE_NON_POS_ORDER,
    SHOPIFY_TAX_ISSUE_POS_LOCATION_MISMATCH,
    SHOPIFY_TAX_ISSUE_POS_TAX_LINES_MISSING,
    SHOPIFY_TAX_ISSUE_POS_TAX_OVERRIDE,
    SHOPIFY_TAX_ISSUE_POS_TAX_RATE_MISMATCH,
    SHOPIFY_TAX_ISSUE_TAXABLE_VARIANT_DISABLED,
    record_shopify_sync_issue,
    resolve_unobserved_shopify_sync_issues,
)


settings = get_settings()
SHOPIFY_POS_TAX_SENTINEL_INTERVAL_SECONDS = 24 * 60 * 60
OFFICIAL_CHECK_AT_KEY = "shopify_pos_tax_sentinel.official_checked_at"
OFFICIAL_RATE_KEY = "shopify_pos_tax_sentinel.official_rate"
OFFICIAL_EFFECTIVE_KEY = "shopify_pos_tax_sentinel.official_effective_label"
LOCATION_CHECK_AT_KEY = "shopify_pos_tax_sentinel.location_checked_at"
CATALOG_CHECK_AT_KEY = "shopify_pos_tax_sentinel.catalog_checked_at"
ORDER_CHECK_AT_KEY = "shopify_pos_tax_sentinel.order_checked_at"
MAX_ORDER_LOOKBACK_DAYS = 365
MAX_LOG_CHECK_ERRORS = 8

_OFFICIAL_CHECK_MAX_AGE = timedelta(days=7)
_SOURCE_ISSUE_PRODUCT_ID = "shopify_pos_tax_sentinel"
_SOURCE_ISSUE_TITLE_PREFIX = "tax-sentinel:"
_LOCATION_CONFIG_TITLE = f"{_SOURCE_ISSUE_TITLE_PREFIX}location-config"
_ORDER_ISSUE_TYPES = frozenset(
    {
        SHOPIFY_TAX_ISSUE_NON_POS_ORDER,
        SHOPIFY_TAX_ISSUE_POS_LOCATION_MISMATCH,
        SHOPIFY_TAX_ISSUE_POS_TAX_LINES_MISSING,
        SHOPIFY_TAX_ISSUE_POS_TAX_OVERRIDE,
        SHOPIFY_TAX_ISSUE_POS_TAX_RATE_MISMATCH,
    }
)


@dataclass(frozen=True)
class SentinelRunSummary:
    success: bool
    variants_checked: int
    orders_checked: int
    findings_recorded: int
    findings_resolved: int
    errors: tuple[str, ...]


@dataclass(frozen=True)
class TaxFinding:
    issue_type: str
    severity: str
    message: str
    order_id: str
    order_number: str
    location_id: str
    payload: dict[str, Any]


def shopify_pos_tax_sentinel_configured(settings_obj: Any) -> bool:
    location_id = getattr(settings_obj, "shopify_pos_location_id", "")
    location_text = location_id.strip() if isinstance(location_id, str) else ""
    return bool(
        getattr(settings_obj, "shopify_pos_tax_sentinel_enabled", False)
        and location_text
        and shopify_admin_configured(settings_obj)
    )


def shopify_pos_tax_runtime_name(settings_obj: Any) -> str:
    raw_name = getattr(settings_obj, "runtime_name", "")
    runtime_name = raw_name.strip() if isinstance(raw_name, str) else ""
    return f"{runtime_name or 'app'}_shopify_tax"


def _decimal(value: Any) -> Decimal:
    try:
        if type(value) is Decimal:
            result = value
        elif type(value) is int:
            result = Decimal(value)
        elif type(value) is float:
            if not math.isfinite(value):
                raise ValueError
            result = Decimal(str(value))
        elif type(value) is str:
            if not value.strip():
                raise ValueError
            result = Decimal(value)
        else:
            raise ValueError
    except (InvalidOperation, ValueError, TypeError, OverflowError):
        raise ValueError("Invalid decimal evidence") from None

    if not result.is_finite():
        raise ValueError("Invalid decimal evidence")
    return result


def _object_rows(payload: dict[str, Any], field: str) -> list[dict[str, Any]]:
    if field not in payload or payload[field] is None:
        return []

    rows = payload[field]
    if not isinstance(rows, list):
        raise ValueError(f"Shopify order {field} is not a list")
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError(
                f"Shopify order {field} entry at index {index} is not an object"
            )
    return rows


def _line_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    line_items = _object_rows(payload, "line_items")
    if not line_items:
        raise ValueError("Shopify order line_items is missing or empty")
    for index, line_item in enumerate(line_items):
        if "taxable" not in line_item or not isinstance(line_item["taxable"], bool):
            raise ValueError(
                f"Shopify order line_items entry at index {index} has invalid taxable"
            )
    return line_items


def _taxable_lines(payload: dict[str, Any]) -> list[dict[str, Any]]:
    line_items = _line_items(payload)
    return [line_item for line_item in line_items if line_item["taxable"] is True]


def _order_tax_lines(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return _object_rows(payload, "tax_lines")


def _line_tax_lines(
    line_item: dict[str, Any],
    *,
    line_index: int,
) -> list[dict[str, Any]]:
    if "tax_lines" not in line_item or line_item["tax_lines"] is None:
        raise ValueError(
            f"Shopify order taxable line at index {line_index} is missing tax_lines"
        )
    tax_lines = line_item["tax_lines"]
    if not isinstance(tax_lines, list):
        raise ValueError(
            f"Shopify order taxable line at index {line_index} tax_lines is not a list"
        )
    for tax_index, tax_line in enumerate(tax_lines):
        if not isinstance(tax_line, dict):
            raise ValueError(
                "Shopify order taxable line tax_lines entry at "
                f"index {line_index}:{tax_index} is not an object"
            )
    return tax_lines


def _tax_rate_sum(tax_lines: list[dict[str, Any]], *, field: str) -> Decimal:
    component_rates = []
    for index, tax_line in enumerate(tax_lines):
        if "rate" not in tax_line:
            raise ValueError(
                f"Shopify order {field} entry at index {index} is missing rate"
            )
        rate = _decimal(tax_line["rate"])
        if rate < Decimal("0") or rate > Decimal("1"):
            raise ValueError(
                f"Shopify order {field} entry at index {index} has invalid rate"
            )
        component_rates.append(rate)
    return sum(component_rates, Decimal("0"))


_EVIDENCE_TAX_LINE_KEYS = ("title", "rate", "price", "channel_liable")
_UNSAFE_EVIDENCE_VALUE = object()


def _evidence_scalar(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value) if value.is_finite() else _UNSAFE_EVIDENCE_VALUE
    if isinstance(value, float):
        return value if math.isfinite(value) else _UNSAFE_EVIDENCE_VALUE
    if value is None or isinstance(value, (str, int, bool)):
        return value
    return _UNSAFE_EVIDENCE_VALUE


def _project_tax_lines(tax_lines: list[dict[str, Any]]) -> list[dict[str, Any]]:
    projected_lines = []
    for tax_line in tax_lines:
        projected_line = {}
        for key in _EVIDENCE_TAX_LINE_KEYS:
            if key not in tax_line:
                continue
            value = _evidence_scalar(tax_line[key])
            if value is not _UNSAFE_EVIDENCE_VALUE:
                projected_line[key] = value
        projected_lines.append(projected_line)
    return projected_lines


def _identifier(value: Any, *, field: str) -> str:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError(f"Shopify order {field} is missing or invalid")
    normalized = str(value).strip()
    if not normalized:
        raise ValueError(f"Shopify order {field} is missing or invalid")
    return normalized


def _order_id(payload: dict[str, Any]) -> str:
    return _identifier(payload.get("id"), field="id")


def _order_number(payload: dict[str, Any], order_id: str) -> str:
    for field in ("name", "order_number"):
        try:
            return _identifier(payload.get(field), field=field)
        except ValueError:
            pass
    return order_id


def _redacted_identifier(value: Any, *, field: str) -> str:
    try:
        return _identifier(value, field=field)
    except ValueError:
        return ""


def _boundary_tax_line_evidence(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return _project_tax_lines([row for row in value if isinstance(row, dict)])


def _evidence(
    payload: dict[str, Any],
    *,
    source_name: str,
    location_id: str,
    tax_lines: list[dict[str, Any]],
) -> dict[str, Any]:
    total_tax_value = payload.get("total_tax")
    try:
        _decimal(total_tax_value)
    except ValueError:
        total_tax = None
    else:
        total_tax = _evidence_scalar(total_tax_value)
        if total_tax is _UNSAFE_EVIDENCE_VALUE:
            total_tax = None
    return {
        "source_name": source_name,
        "location_id": location_id,
        "total_tax": total_tax,
        "tax_lines": _project_tax_lines(tax_lines),
    }


def _finding(
    *,
    issue_type: str,
    severity: str,
    message: str,
    order_id: str,
    order_number: str,
    location_id: str,
    evidence: dict[str, Any],
) -> TaxFinding:
    return TaxFinding(
        issue_type=issue_type,
        severity=severity,
        message=message,
        order_id=order_id,
        order_number=order_number,
        location_id=location_id,
        payload=evidence,
    )


def evaluate_shopify_order(
    payload: dict[str, Any],
    *,
    expected_location_id: str,
    expected_rate: Decimal,
    pos_only: bool,
) -> list[TaxFinding]:
    source_value = payload.get("source_name")
    source_name = source_value.strip().lower() if isinstance(source_value, str) else ""

    if source_name != "pos":
        if not pos_only:
            return []
        order_id = _order_id(payload)
        order_number = _order_number(payload, order_id)
        location_id = _redacted_identifier(
            payload.get("location_id"),
            field="location_id",
        )
        evidence = _evidence(
            payload,
            source_name=source_name,
            location_id=location_id,
            tax_lines=_boundary_tax_line_evidence(payload.get("tax_lines")),
        )
        return [
            _finding(
                issue_type=SHOPIFY_TAX_ISSUE_NON_POS_ORDER,
                severity="critical",
                message=(
                    f"Shopify order {order_number} came from "
                    f"source {source_name!r}; expected POS."
                ),
                order_id=order_id,
                order_number=order_number,
                location_id=location_id,
                evidence=evidence,
            )
        ]

    financial_status_value = payload.get("financial_status")
    financial_status = (
        financial_status_value.strip().lower()
        if isinstance(financial_status_value, str)
        else ""
    )
    if financial_status != "paid":
        return []

    order_id = _order_id(payload)
    order_number = _order_number(payload, order_id)
    location_id = _redacted_identifier(
        payload.get("location_id"),
        field="location_id",
    )
    expected_location = _identifier(
        expected_location_id,
        field="expected location id",
    )
    if location_id != expected_location:
        evidence = _evidence(
            payload,
            source_name=source_name,
            location_id=location_id,
            tax_lines=_boundary_tax_line_evidence(payload.get("tax_lines")),
        )
        return [
            _finding(
                issue_type=SHOPIFY_TAX_ISSUE_POS_LOCATION_MISMATCH,
                severity="critical",
                message=(
                    f"Paid POS Shopify order {order_number or order_id} used "
                    f"location {location_id!r}; expected {expected_location!r}."
                ),
                order_id=order_id,
                order_number=order_number,
                location_id=location_id,
                evidence=evidence,
            )
        ]

    normalized_expected_rate = _decimal(expected_rate)
    line_items = _line_items(payload)
    line_observed_rates: list[str] = []
    line_mismatch_rates: list[str] = []
    line_override_indexes: list[int] = []
    taxable_line_count = 0
    for line_index, line_item in enumerate(line_items):
        if line_item["taxable"] is False:
            for field in ("requires_shipping", "gift_card"):
                if field not in line_item or not isinstance(line_item[field], bool):
                    raise ValueError(
                        "Shopify order line_items entry at "
                        f"index {line_index} has invalid {field}"
                    )
            if line_item["requires_shipping"] and not line_item["gift_card"]:
                line_override_indexes.append(line_index)
            continue
        taxable_line_count += 1
        line_tax_lines = _line_tax_lines(line_item, line_index=line_index)
        if not line_tax_lines:
            line_override_indexes.append(line_index)
            continue
        line_rate = _tax_rate_sum(
            line_tax_lines,
            field=f"line_items[{line_index}].tax_lines",
        )
        line_rate_text = str(line_rate)
        line_observed_rates.append(line_rate_text)
        if abs(line_rate - normalized_expected_rate) > Decimal("0.0001"):
            line_mismatch_rates.append(line_rate_text)

    tax_lines = _order_tax_lines(payload)
    evidence = _evidence(
        payload,
        source_name=source_name,
        location_id=location_id,
        tax_lines=tax_lines,
    )

    total_tax = _decimal(payload.get("total_tax"))
    if total_tax < Decimal("0"):
        raise ValueError("Shopify order total_tax must be non-negative")

    findings: list[TaxFinding] = []
    override_observed = bool(line_override_indexes) or (
        taxable_line_count > 0 and total_tax == Decimal("0") and not tax_lines
    )
    if override_observed:
        override_evidence = {
            **evidence,
            "line_override_indexes": line_override_indexes,
        }
        findings.append(
            _finding(
                issue_type=SHOPIFY_TAX_ISSUE_POS_TAX_OVERRIDE,
                severity="warning",
                message=(
                    f"Paid POS Shopify order {order_number or order_id} has one or "
                    "more line items with no recorded tax or an explicit tax override."
                ),
                order_id=order_id,
                order_number=order_number,
                location_id=location_id,
                evidence=override_evidence,
            )
        )

    if total_tax > Decimal("0") and not tax_lines:
        findings.append(
            _finding(
                issue_type=SHOPIFY_TAX_ISSUE_POS_TAX_LINES_MISSING,
                severity="warning",
                message=(
                    f"Paid POS Shopify order {order_number or order_id} has positive "
                    "total tax but no order tax lines."
                ),
                order_id=order_id,
                order_number=order_number,
                location_id=location_id,
                evidence=evidence,
            )
        )

    order_observed_rate: Decimal | None = None
    order_rate_mismatch = False
    if tax_lines:
        order_observed_rate = _tax_rate_sum(tax_lines, field="tax_lines")
        order_rate_mismatch = (
            abs(order_observed_rate - normalized_expected_rate) > Decimal("0.0001")
        )

    if order_rate_mismatch or line_mismatch_rates:
        mismatch_evidence = {
            **evidence,
            "expected_rate": str(normalized_expected_rate),
        }
        observed_descriptions = []
        if order_rate_mismatch and order_observed_rate is not None:
            mismatch_evidence["observed_rate"] = str(order_observed_rate)
            observed_descriptions.append(f"order rate {order_observed_rate}")
        if line_mismatch_rates:
            mismatch_evidence["line_observed_rates"] = line_observed_rates
            observed_descriptions.append(
                f"taxable line rate(s) {', '.join(line_mismatch_rates)}"
            )
        findings.append(
            _finding(
                issue_type=SHOPIFY_TAX_ISSUE_POS_TAX_RATE_MISMATCH,
                severity="critical",
                message=(
                    f"Paid POS Shopify order {order_number or order_id} has "
                    f"{' and '.join(observed_descriptions)}; expected "
                    f"{normalized_expected_rate}."
                ),
                order_id=order_id,
                order_number=order_number,
                location_id=location_id,
                evidence=mismatch_evidence,
            )
        )

    return findings


def parse_order_payload(raw_payload: str) -> dict[str, Any]:
    error_message = "Shopify order raw payload is not a JSON object"

    def reject_nonstandard_number(_value: str) -> None:
        raise ValueError(error_message)

    try:
        payload = json.loads(raw_payload, parse_constant=reject_nonstandard_number)
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        raise ValueError(error_message) from exc

    if not isinstance(payload, dict):
        raise ValueError(error_message)
    return payload


def _utc_now(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if not isinstance(value, datetime):
        raise ValueError("Sentinel now must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _setting_text(settings_obj: Any, name: str, default: str = "") -> str:
    value = getattr(settings_obj, name, default)
    return value.strip() if isinstance(value, str) else ""


def _positive_lookback_days(settings_obj: Any) -> int:
    value = getattr(settings_obj, "shopify_pos_tax_order_lookback_days", 7)
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError("Invalid Shopify POS tax sentinel configuration") from None
    if (
        not decimal_value.is_finite()
        or decimal_value <= 0
        or decimal_value > MAX_ORDER_LOOKBACK_DAYS
        or decimal_value != decimal_value.to_integral_value()
    ):
        raise ValueError("Invalid Shopify POS tax sentinel configuration")
    return int(decimal_value)


def _sentinel_configuration(
    settings_obj: Any,
) -> tuple[Decimal, str, str, str, int, bool, str]:
    raw_rate = getattr(settings_obj, "shopify_pos_expected_tax_rate", None)
    try:
        expected_rate = Decimal(str(raw_rate))
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError("Invalid Shopify POS tax sentinel configuration") from None
    if not expected_rate.is_finite() or not Decimal("0") < expected_rate < Decimal("1"):
        raise ValueError("Invalid Shopify POS tax sentinel configuration")

    raw_location_id = getattr(settings_obj, "shopify_pos_location_id", None)
    if isinstance(raw_location_id, bool) or raw_location_id is None:
        expected_location_id = ""
    else:
        expected_location_id = str(raw_location_id).strip()
    if not expected_location_id:
        raise ValueError("Invalid Shopify POS tax sentinel configuration")

    city = _setting_text(settings_obj, "shopify_pos_tax_city", "San Jose")
    county = _setting_text(
        settings_obj,
        "shopify_pos_tax_county",
        "Santa Clara",
    )
    if not city or not county:
        raise ValueError("Invalid Shopify POS tax sentinel configuration")

    pos_only = getattr(settings_obj, "shopify_pos_only", True)
    if not isinstance(pos_only, bool):
        raise ValueError("Invalid Shopify POS tax sentinel configuration")

    return (
        expected_rate,
        expected_location_id,
        city,
        county,
        _positive_lookback_days(settings_obj),
        pos_only,
        _setting_text(settings_obj, "shopify_store_domain"),
    )


def _error_type(exc: BaseException) -> str:
    name = type(exc).__name__
    sanitized = "".join(
        character
        for character in name
        if character.isalnum() or character == "_"
    )
    return sanitized[:64] or "Error"


def _check_error(check: str, exc: BaseException) -> str:
    return f"{check} check failed ({_error_type(exc)})"


def _sanitized_check_errors(errors: Any) -> list[str]:
    allowed_checks = (
        "configuration",
        "location",
        "catalog",
        "official",
        "order",
        "runtime",
    )
    sanitized_errors: list[str] = []
    seen: set[str] = set()
    if not isinstance(errors, (list, tuple)):
        return sanitized_errors
    for error in errors:
        if not isinstance(error, str):
            continue
        for check in allowed_checks:
            prefix = f"{check} check failed ("
            if not error.startswith(prefix) or not error.endswith(")"):
                continue
            error_type = error[len(prefix) : -1]
            if (
                not error_type
                or len(error_type) > 64
                or any(
                    not (character.isalnum() or character == "_")
                    for character in error_type
                )
            ):
                break
            identity = f"{check} check failed ({error_type})"
            if identity not in seen:
                seen.add(identity)
                sanitized_errors.append(identity)
            break
        if len(sanitized_errors) >= MAX_LOG_CHECK_ERRORS:
            break
    return sanitized_errors


def _record_source_error(session: Any, *, check: str, exc: BaseException) -> Any:
    error_type = _error_type(exc)
    return record_shopify_sync_issue(
        session,
        issue_type=SHOPIFY_SYNC_ISSUE_SYNC_ERROR,
        severity="critical",
        message=f"Shopify POS tax sentinel {check} check failed ({error_type}).",
        shopify_product_id=_SOURCE_ISSUE_PRODUCT_ID,
        shopify_title=f"{_SOURCE_ISSUE_TITLE_PREFIX}{check}",
        payload={"check": check, "error_type": error_type},
    )


def _owned_issue_key(issue_type: str, title: str) -> str:
    return f"{issue_type}:{_SOURCE_ISSUE_PRODUCT_ID}:{title}"


def _resolve_exact_owned_issue(
    session: Any,
    *,
    issue_key: str,
    resolution_note: str,
) -> int:
    issue = session.exec(
        select(ShopifySyncIssue).where(
            ShopifySyncIssue.issue_key == issue_key,
            ShopifySyncIssue.status == SHOPIFY_SYNC_ISSUE_OPEN,
        )
    ).first()
    if issue is None:
        return 0
    resolved_at = utcnow()
    issue.status = SHOPIFY_SYNC_ISSUE_RESOLVED
    issue.resolution_note = resolution_note
    issue.resolved_by = "shopify_pos_tax_sentinel"
    issue.resolved_at = resolved_at
    issue.last_seen_at = resolved_at
    session.add(issue)
    return 1


def _resolve_source_error(session: Any, *, check: str) -> int:
    title = f"{_SOURCE_ISSUE_TITLE_PREFIX}{check}"
    return _resolve_exact_owned_issue(
        session,
        issue_key=_owned_issue_key(SHOPIFY_SYNC_ISSUE_SYNC_ERROR, title),
        resolution_note=f"The complete {check} check succeeded.",
    )


def _resolve_evaluated_order_issues(
    session: Any,
    *,
    evaluated_order_ids: set[str],
    observed_issue_keys: dict[str, set[str]],
) -> int:
    if not evaluated_order_ids:
        return 0
    issues = session.exec(
        select(ShopifySyncIssue).where(
            ShopifySyncIssue.issue_type.in_(_ORDER_ISSUE_TYPES),
            ShopifySyncIssue.status == SHOPIFY_SYNC_ISSUE_OPEN,
            ShopifySyncIssue.shopify_order_id.in_(evaluated_order_ids),
        )
    ).all()
    resolved_at = utcnow()
    resolved_count = 0
    for issue in issues:
        if not issue.shopify_order_id:
            continue
        if issue.issue_key in observed_issue_keys.get(issue.issue_type, set()):
            continue
        issue.status = SHOPIFY_SYNC_ISSUE_RESOLVED
        issue.resolution_note = (
            "No longer observed for the successfully evaluated Shopify order."
        )
        issue.resolved_by = "shopify_pos_tax_sentinel"
        issue.resolved_at = resolved_at
        issue.last_seen_at = resolved_at
        session.add(issue)
        resolved_count += 1
    return resolved_count


def _upsert_setting(session: Any, *, key: str, value: str) -> None:
    row = session.get(AppSetting, key)
    if row is None:
        row = AppSetting(key=key, value=value)
    else:
        row.value = value
    session.add(row)


def _setting_value(session: Any, key: str) -> str | None:
    row = session.get(AppSetting, key)
    if row is None or not isinstance(row.value, str):
        return None
    value = row.value.strip()
    return value or None


def _prior_official_timestamp(value: str | None) -> str | None:
    if value is None:
        return None
    timestamp = value
    if timestamp.endswith("Z"):
        timestamp = f"{timestamp[:-1]}+00:00"
    try:
        checked_at = datetime.fromisoformat(timestamp)
    except ValueError:
        return None
    if checked_at.tzinfo is None or checked_at.utcoffset() is None:
        return None
    return checked_at.astimezone(timezone.utc).isoformat()


def _prior_official_rate(value: str | None) -> str | None:
    try:
        rate = _decimal(value)
    except ValueError:
        return None
    if not Decimal("0") <= rate <= Decimal("1"):
        return None
    return str(rate)


def _prior_official_effective_label(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = " ".join(value.split())
    if not normalized or len(normalized) > 64:
        return None
    try:
        effective_date = datetime.strptime(normalized, "%B %d, %Y")
    except ValueError:
        return None
    return f"{effective_date:%B} {effective_date.day}, {effective_date.year}"


def _prior_official_observation(session: Any) -> dict[str, Any]:
    checked_at = _prior_official_timestamp(
        _setting_value(session, OFFICIAL_CHECK_AT_KEY)
    )
    rate = _prior_official_rate(_setting_value(session, OFFICIAL_RATE_KEY))
    effective_label = _prior_official_effective_label(
        _setting_value(session, OFFICIAL_EFFECTIVE_KEY)
    )
    if checked_at is None or rate is None or effective_label is None:
        checked_at = None
        rate = None
        effective_label = None
    return {
        "stale": True,
        "prior_checked_at": checked_at,
        "prior_rate": rate,
        "prior_effective_label": effective_label,
    }


def _prior_official_message(observation: dict[str, Any]) -> str:
    checked_at = observation["prior_checked_at"]
    rate = observation["prior_rate"]
    effective_label = observation["prior_effective_label"]
    if checked_at is None or rate is None or effective_label is None:
        return " No prior successful official observation is available."
    return (
        " Stale prior successful official observation: "
        f"checked at {checked_at}, rate {rate}, effective {effective_label}."
    )


def _official_check_due(session: Any, *, now: datetime, forced: bool) -> bool:
    if forced:
        return True
    row = session.get(AppSetting, OFFICIAL_CHECK_AT_KEY)
    if row is None or not isinstance(row.value, str) or not row.value.strip():
        return True
    timestamp = row.value.strip()
    if timestamp.endswith("Z"):
        timestamp = f"{timestamp[:-1]}+00:00"
    try:
        checked_at = datetime.fromisoformat(timestamp)
    except ValueError:
        return True
    if checked_at.tzinfo is None or checked_at.utcoffset() is None:
        return True
    checked_at_utc = checked_at.astimezone(timezone.utc)
    if checked_at_utc > now:
        return True
    return now - checked_at_utc > _OFFICIAL_CHECK_MAX_AGE


def _location_identifier(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return ""
    return str(value).strip()


def _location_text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _location_province_code(location: dict[str, Any]) -> str:
    for field in ("province_code", "state_code"):
        value = _location_text(location.get(field))
        if value:
            return value
    return ""


def _message_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "<missing>"
    if not isinstance(value, (str, int)):
        return "<invalid>"
    normalized = " ".join(str(value).split())
    return normalized[:128] or "<missing>"


def _location_observation(
    locations: list[dict[str, Any]],
    *,
    expected_location_id: str,
    expected_city: str,
) -> tuple[bool, dict[str, Any]]:
    matching = next(
        (
            location
            for location in locations
            if _location_identifier(location.get("id")) == expected_location_id
        ),
        None,
    )
    observed_id = _location_identifier((matching or {}).get("id"))
    observed_city = _location_text((matching or {}).get("city"))
    observed_province = _location_province_code(matching or {})
    observed_active = (matching or {}).get("active")
    valid = bool(
        matching is not None
        and observed_active is True
        and observed_city.casefold() == expected_city.casefold()
        and observed_province.casefold() == "ca"
    )
    evidence = {
        "expected_location_id": expected_location_id,
        "expected_city": expected_city,
        "expected_province_code": "CA",
        "observed_location_id": observed_id,
        "observed_active": (
            observed_active if isinstance(observed_active, bool) else None
        ),
        "observed_city": observed_city,
        "observed_province_code": observed_province,
    }
    return valid, evidence


def _official_issue_identifier(city: str, county: str) -> str:
    normalized = "-".join(f"{city}-{county}".casefold().split())
    return normalized or "configured-city"


def _stored_order_identifier(order: ShopifyOrder) -> str:
    value = str(order.shopify_order_id or "").strip()
    if value:
        return value
    return f"stored-order-{order.id or 'unknown'}"


def _stored_order_number(order: ShopifyOrder, order_id: str) -> str:
    value = str(order.order_number or "").strip()
    return value or order_id


def _emit_summary(
    summary: SentinelRunSummary,
    *,
    settings_obj: Any,
    started_at: datetime,
) -> None:
    completed_at = datetime.now(timezone.utc)
    print(
        structured_log_line(
            runtime=shopify_pos_tax_runtime_name(settings_obj),
            action="audit_once",
            success=summary.success,
            variants_checked=summary.variants_checked,
            orders_checked=summary.orders_checked,
            findings_recorded=summary.findings_recorded,
            findings_resolved=summary.findings_resolved,
            errors_count=len(summary.errors),
            check_errors=_sanitized_check_errors(summary.errors),
            started_at=started_at.astimezone(timezone.utc).isoformat(),
            completed_at=completed_at.isoformat(),
        )
    )


async def run_shopify_tax_sentinel_once(
    *,
    settings_obj: Any,
    session_factory=managed_session,
    now: datetime | None = None,
    force_official_check: bool = False,
) -> SentinelRunSummary:
    started_at = datetime.now(timezone.utc)
    now_utc = _utc_now(now)
    variants_checked = 0
    orders_checked = 0
    findings_recorded = 0
    findings_resolved = 0
    errors: list[str] = []
    observed: dict[str, set[str]] = {
        SHOPIFY_TAX_ISSUE_TAXABLE_VARIANT_DISABLED: set(),
        SHOPIFY_TAX_ISSUE_POS_TAX_RATE_MISMATCH: set(),
        SHOPIFY_TAX_ISSUE_POS_TAX_LINES_MISSING: set(),
        SHOPIFY_TAX_ISSUE_POS_LOCATION_MISMATCH: set(),
        SHOPIFY_TAX_ISSUE_POS_TAX_OVERRIDE: set(),
        SHOPIFY_TAX_ISSUE_NON_POS_ORDER: set(),
        SHOPIFY_TAX_ISSUE_OFFICIAL_RATE_CHANGED: set(),
        SHOPIFY_TAX_ISSUE_OFFICIAL_SOURCE_UNAVAILABLE: set(),
    }

    with session_factory() as session:
        try:
            try:
                (
                    expected_rate,
                    expected_location_id,
                    city,
                    county,
                    lookback_days,
                    pos_only,
                    store_domain,
                ) = _sentinel_configuration(settings_obj)
            except ValueError as exc:
                errors.append(_check_error("configuration", exc))
                _record_source_error(session, check="configuration", exc=exc)
                findings_recorded += 1
                session.commit()
                summary = SentinelRunSummary(
                    success=False,
                    variants_checked=0,
                    orders_checked=0,
                    findings_recorded=findings_recorded,
                    findings_resolved=0,
                    errors=tuple(errors),
                )
                _emit_summary(
                    summary,
                    settings_obj=settings_obj,
                    started_at=started_at,
                )
                return summary

            findings_resolved += _resolve_source_error(
                session,
                check="configuration",
            )

            token_error: BaseException | None = None
            try:
                access_token = resolve_shopify_access_token(settings_obj)
            except Exception as exc:
                access_token = ""
                token_error = exc

            location_error: BaseException | None = None
            try:
                if token_error is not None:
                    raise token_error
                if not store_domain or not access_token:
                    raise ValueError("Shopify Admin configuration is unavailable")
                locations = await get_shopify_locations(
                    store_domain=store_domain,
                    access_token=access_token,
                )
                location_valid, location_evidence = _location_observation(
                    locations,
                    expected_location_id=expected_location_id,
                    expected_city=city,
                )
            except Exception as exc:
                location_error = exc

            if location_error is not None:
                errors.append(_check_error("location", location_error))
                _record_source_error(
                    session,
                    check="location",
                    exc=location_error,
                )
                findings_recorded += 1
            else:
                if not location_valid:
                    expected_location_message = _message_value(
                        location_evidence["expected_location_id"]
                    )
                    expected_city_message = _message_value(
                        location_evidence["expected_city"]
                    )
                    expected_province_message = _message_value(
                        location_evidence["expected_province_code"]
                    )
                    observed_location_message = _message_value(
                        location_evidence["observed_location_id"]
                    )
                    observed_city_message = _message_value(
                        location_evidence["observed_city"]
                    )
                    observed_province_message = _message_value(
                        location_evidence["observed_province_code"]
                    )
                    observed_active_message = _message_value(
                        location_evidence["observed_active"]
                    )
                    issue = record_shopify_sync_issue(
                        session,
                        issue_type=SHOPIFY_TAX_ISSUE_POS_LOCATION_MISMATCH,
                        severity="critical",
                        message=(
                            "Configured Shopify POS location mismatch: expected "
                            f"ID {expected_location_message}, city "
                            f"{expected_city_message}, province "
                            f"{expected_province_message}; observed ID "
                            f"{observed_location_message}, city "
                            f"{observed_city_message}, province "
                            f"{observed_province_message}, active "
                            f"{observed_active_message}."
                        ),
                        shopify_product_id=_SOURCE_ISSUE_PRODUCT_ID,
                        shopify_title=_LOCATION_CONFIG_TITLE,
                        shopify_location_id=expected_location_id,
                        payload=location_evidence,
                    )
                    observed[SHOPIFY_TAX_ISSUE_POS_LOCATION_MISMATCH].add(
                        issue.issue_key
                    )
                    findings_recorded += 1
                else:
                    findings_resolved += _resolve_exact_owned_issue(
                        session,
                        issue_key=_owned_issue_key(
                            SHOPIFY_TAX_ISSUE_POS_LOCATION_MISMATCH,
                            _LOCATION_CONFIG_TITLE,
                        ),
                        resolution_note=(
                            "The configured Shopify POS location passed the "
                            "complete location check."
                        ),
                    )
                findings_resolved += _resolve_source_error(
                    session,
                    check="location",
                )
                _upsert_setting(
                    session,
                    key=LOCATION_CHECK_AT_KEY,
                    value=now_utc.isoformat(),
                )

            catalog_complete = False
            catalog_error: BaseException | None = None
            try:
                if token_error is not None:
                    raise token_error
                if not store_domain or not access_token:
                    raise ValueError("Shopify Admin configuration is unavailable")
                variants = await fetch_non_taxable_physical_variants(
                    store_domain=store_domain,
                    access_token=access_token,
                )
                variants_checked = len(variants)
                variant_rows = [
                    {
                        "product_id": variant.product_id,
                        "product_title": variant.product_title,
                        "variant_id": variant.variant_id,
                        "variant_title": variant.variant_title,
                        "sku": variant.sku,
                    }
                    for variant in variants
                ]
            except Exception as exc:
                catalog_error = exc

            if catalog_error is not None:
                errors.append(_check_error("catalog", catalog_error))
                _record_source_error(
                    session,
                    check="catalog",
                    exc=catalog_error,
                )
                findings_recorded += 1
            else:
                for variant in variant_rows:
                    issue = record_shopify_sync_issue(
                        session,
                        issue_type=SHOPIFY_TAX_ISSUE_TAXABLE_VARIANT_DISABLED,
                        severity="warning",
                        message=(
                            "Active physical Shopify variant has tax collection "
                            "disabled."
                        ),
                        shopify_product_id=variant["product_id"],
                        shopify_variant_id=variant["variant_id"],
                        shopify_sku=variant["sku"],
                        shopify_title=(
                            f"{variant['product_title']} / {variant['variant_title']}"
                        ),
                        payload=variant,
                    )
                    observed[SHOPIFY_TAX_ISSUE_TAXABLE_VARIANT_DISABLED].add(
                        issue.issue_key
                    )
                    findings_recorded += 1
                findings_resolved += _resolve_source_error(
                    session,
                    check="catalog",
                )
                catalog_complete = True

            official_complete = False
            if _official_check_due(
                session,
                now=now_utc,
                forced=force_official_check,
            ):
                official_error: BaseException | None = None
                try:
                    official = await fetch_cdtfa_city_rate(
                        city=city,
                        county=county,
                    )
                    official_rate = _decimal(str(official.rate))
                    if not Decimal("0") <= official_rate <= Decimal("1"):
                        raise ValueError("Invalid official tax rate")
                    effective_label = (
                        official.effective_label.strip()
                        if isinstance(official.effective_label, str)
                        else ""
                    )
                    if not effective_label:
                        raise ValueError("Invalid official tax effective label")
                except Exception as exc:
                    official_error = exc

                if official_error is not None:
                    errors.append(_check_error("official", official_error))
                    official_error_type = _error_type(official_error)
                    prior_observation = _prior_official_observation(session)
                    issue = record_shopify_sync_issue(
                        session,
                        issue_type=SHOPIFY_TAX_ISSUE_OFFICIAL_SOURCE_UNAVAILABLE,
                        severity="critical",
                        message=(
                            "Official CDTFA tax rate source check failed for "
                            f"{_message_value(city)}, {_message_value(county)} "
                            f"County ({official_error_type})."
                            f"{_prior_official_message(prior_observation)}"
                        ),
                        shopify_product_id="cdtfa",
                        shopify_variant_id=_official_issue_identifier(city, county),
                        payload={
                            "source_url": CDTFA_CITY_RATES_URL,
                            "city": city,
                            "county": county,
                            "error_type": official_error_type,
                            **prior_observation,
                        },
                    )
                    observed[SHOPIFY_TAX_ISSUE_OFFICIAL_SOURCE_UNAVAILABLE].add(
                        issue.issue_key
                    )
                    findings_recorded += 1
                else:
                    _upsert_setting(
                        session,
                        key=OFFICIAL_CHECK_AT_KEY,
                        value=now_utc.isoformat(),
                    )
                    _upsert_setting(
                        session,
                        key=OFFICIAL_RATE_KEY,
                        value=str(official_rate),
                    )
                    _upsert_setting(
                        session,
                        key=OFFICIAL_EFFECTIVE_KEY,
                        value=effective_label,
                    )
                    if official_rate != expected_rate:
                        issue = record_shopify_sync_issue(
                            session,
                            issue_type=SHOPIFY_TAX_ISSUE_OFFICIAL_RATE_CHANGED,
                            severity="critical",
                            message=(
                                f"Official CDTFA tax rate for {_message_value(city)}, "
                                f"{_message_value(county)} County is {official_rate} "
                                f"(effective {_message_value(effective_label)}); "
                                f"configured Shopify POS rate is {expected_rate}."
                            ),
                            shopify_product_id="cdtfa",
                            shopify_variant_id=_official_issue_identifier(
                                city,
                                county,
                            ),
                            payload={
                                "source_url": CDTFA_CITY_RATES_URL,
                                "city": city,
                                "county": county,
                                "official_rate": str(official_rate),
                                "expected_rate": str(expected_rate),
                                "effective_label": effective_label,
                            },
                        )
                        observed[SHOPIFY_TAX_ISSUE_OFFICIAL_RATE_CHANGED].add(
                            issue.issue_key
                        )
                        findings_recorded += 1
                    official_complete = True

            order_complete = True
            evaluated_order_ids: set[str] = set()
            cutoff = now_utc - timedelta(days=lookback_days)
            orders = session.exec(
                select(ShopifyOrder)
                .where(
                    ShopifyOrder.created_at >= cutoff,
                    ShopifyOrder.created_at <= now_utc,
                )
                .order_by(ShopifyOrder.created_at, ShopifyOrder.shopify_order_id)
            ).all()
            for order in orders:
                orders_checked += 1
                stored_order_id = _stored_order_identifier(order)
                stored_order_number = _stored_order_number(order, stored_order_id)
                try:
                    payload = parse_order_payload(order.raw_payload)
                    findings = evaluate_shopify_order(
                        payload,
                        expected_location_id=expected_location_id,
                        expected_rate=expected_rate,
                        pos_only=pos_only,
                    )
                except ValueError as exc:
                    order_complete = False
                    errors.append(_check_error("order", exc))
                    issue = record_shopify_sync_issue(
                        session,
                        issue_type=SHOPIFY_TAX_ISSUE_POS_TAX_LINES_MISSING,
                        severity="warning",
                        message=(
                            f"Stored Shopify order {stored_order_number} could not "
                            "be evaluated for POS tax evidence."
                        ),
                        shopify_order_id=stored_order_id,
                        shopify_order_number=stored_order_number,
                        payload={
                            "check": "order",
                            "error_type": _error_type(exc),
                        },
                    )
                    observed[SHOPIFY_TAX_ISSUE_POS_TAX_LINES_MISSING].add(
                        issue.issue_key
                    )
                    findings_recorded += 1
                else:
                    evaluated_order_ids.add(stored_order_id)
                    for finding in findings:
                        issue = record_shopify_sync_issue(
                            session,
                            issue_type=finding.issue_type,
                            severity=finding.severity,
                            message=finding.message,
                            shopify_order_id=finding.order_id,
                            shopify_order_number=finding.order_number,
                            shopify_location_id=finding.location_id or None,
                            payload=finding.payload,
                        )
                        observed[finding.issue_type].add(issue.issue_key)
                        findings_recorded += 1

            if catalog_complete:
                findings_resolved += resolve_unobserved_shopify_sync_issues(
                    session,
                    issue_type=SHOPIFY_TAX_ISSUE_TAXABLE_VARIANT_DISABLED,
                    observed_issue_keys=observed[
                        SHOPIFY_TAX_ISSUE_TAXABLE_VARIANT_DISABLED
                    ],
                    resolution_note=(
                        "No longer observed by the complete Shopify catalog tax check."
                    ),
                )
                _upsert_setting(
                    session,
                    key=CATALOG_CHECK_AT_KEY,
                    value=now_utc.isoformat(),
                )

            if official_complete:
                for issue_type in (
                    SHOPIFY_TAX_ISSUE_OFFICIAL_RATE_CHANGED,
                    SHOPIFY_TAX_ISSUE_OFFICIAL_SOURCE_UNAVAILABLE,
                ):
                    findings_resolved += resolve_unobserved_shopify_sync_issues(
                        session,
                        issue_type=issue_type,
                        observed_issue_keys=observed[issue_type],
                        resolution_note=(
                            "No longer observed by the complete official tax rate check."
                        ),
                    )

            if order_complete:
                findings_resolved += _resolve_evaluated_order_issues(
                    session,
                    evaluated_order_ids=evaluated_order_ids,
                    observed_issue_keys=observed,
                )
                _upsert_setting(
                    session,
                    key=ORDER_CHECK_AT_KEY,
                    value=now_utc.isoformat(),
                )

            session.commit()
        except Exception:
            session.rollback()
            raise

    summary = SentinelRunSummary(
        success=not errors,
        variants_checked=variants_checked,
        orders_checked=orders_checked,
        findings_recorded=findings_recorded,
        findings_resolved=findings_resolved,
        errors=tuple(errors),
    )
    _emit_summary(
        summary,
        settings_obj=settings_obj,
        started_at=started_at,
    )
    return summary


async def periodic_shopify_pos_tax_sentinel_loop(
    stop_event: asyncio.Event,
) -> None:
    runtime_name = shopify_pos_tax_runtime_name(settings)
    while not stop_event.is_set():
        started_at = datetime.now(timezone.utc)
        try:
            await run_shopify_tax_sentinel_once(settings_obj=settings)
        except Exception as exc:
            completed_at = datetime.now(timezone.utc)
            check_error = _check_error("runtime", exc)
            print(
                structured_log_line(
                    runtime=runtime_name,
                    action="shopify.pos_tax_sentinel.failed",
                    success=False,
                    error_type=_error_type(exc),
                    check_errors=_sanitized_check_errors([check_error]),
                    started_at=started_at.isoformat(),
                    completed_at=completed_at.isoformat(),
                )
            )
        if stop_event.is_set():
            break
        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=SHOPIFY_POS_TAX_SENTINEL_INTERVAL_SECONDS,
            )
        except TimeoutError:
            continue
        break
