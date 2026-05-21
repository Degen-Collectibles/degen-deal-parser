from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional


ENTRY_KINDS = {
    "sale",
    "buy",
    "trade",
    "expense",
    "unknown",
    "loan_draw",
    "loan_repayment",
    "transfer",
}
NON_OPERATING_ENTRY_KINDS = {"loan_draw", "loan_repayment", "transfer"}
PARSED_EXPENSE_CATEGORIES = {
    "insurance",
    "loan_interest",
    "loan_owner_payments",
    "other_business_expense",
    "payroll",
    "rent_facilities",
    "show_fees",
    "taxes_licenses",
    "transfers",
    "uncategorized",
}

INVENTORY_HINTS: tuple[str, ...] = (
    "single",
    "singles",
    "card",
    "cards",
    "slab",
    "slabs",
    "pack",
    "packs",
    "box",
    "boxes",
    "sealed",
    "collection",
    "binder",
    "bulk",
)

EXPENSE_PATTERNS: list[tuple[str, tuple[str, ...]]] = [
    ("rent", ("rent", "lease")),
    ("utilities", ("electric", "electricity", "water bill", "internet", "wifi", "utility", "utilities")),
    ("software", ("software", "subscription", "quickbooks", "shopify", "canva", "adobe", "discord boost")),
    ("fees", ("vendor fee", "table fee", "booth fee", "event fee", "processing fee", "stripe fee", "square fee")),
    ("shipping", ("shipping", "postage", "usps", "ups", "fedex", "label cost")),
    ("travel", (
        "gas", "parking", "hotel", "mileage", "uber", "lyft",
        "flight", "flights", "airfare", "plane ticket", "airline",
        "baggage", "checked bag", "rental car", "rideshare",
    )),
    ("food", ("food", "lunch", "dinner", "snacks", "coffee")),
    ("payroll", ("payroll", "wages", "salary", "commission", "paid staff")),
    ("tax", ("tax", "sales tax", "franchise tax", "license renewal")),
    ("insurance", ("insurance",)),
    ("maintenance", ("repair", "maintenance", "cleaning", "printer ink")),
    ("supplies", ("supplies", "paper", "tape", "bubble mailer", "mailer", "shipping supplies")),
]

@dataclass
class FinancialSummary:
    entry_kind: str
    money_in: float
    money_out: float
    expense_category: Optional[str]
    requires_review: bool = False


def normalize_payment_amount(amount: Optional[float]) -> float:
    if amount is None:
        return 0.0
    return round(float(amount), 2)


def detect_expense_category(message_text: str) -> Optional[str]:
    lower = (message_text or "").lower()
    if not lower:
        return None

    if any(token in lower for token in INVENTORY_HINTS):
        return None

    for category, keywords in EXPENSE_PATTERNS:
        if any(keyword in lower for keyword in keywords):
            return category

    if re.search(r"\b(paid|expense|spent)\b", lower) and re.search(r"\b(store|shop|booth|vendor|business)\b", lower):
        return "other"

    return None


def derive_entry_kind(
    parsed_type: Optional[str],
    parsed_category: Optional[str],
    cash_direction: Optional[str],
    message_text: str,
) -> tuple[str, Optional[str]]:
    if parsed_type == "loan_draw":
        return "loan_draw", "loan_owner_payments"
    if parsed_type == "loan_repayment":
        return "loan_repayment", "loan_owner_payments"
    if parsed_type == "transfer":
        return "transfer", parsed_category or "transfers"
    if parsed_type == "expense":
        if parsed_category and parsed_category in PARSED_EXPENSE_CATEGORIES:
            return "expense", parsed_category
        expense_category = detect_expense_category(message_text)
        return "expense", expense_category or parsed_category or "uncategorized"
    if parsed_type in {None, "unknown"} and parsed_category in PARSED_EXPENSE_CATEGORIES:
        return "unknown", parsed_category

    expense_category = detect_expense_category(message_text)
    if expense_category and parsed_type in {None, "unknown", "buy"}:
        return "expense", expense_category

    if parsed_type == "sell":
        return "sale", "inventory"
    if parsed_type == "buy":
        return "buy", "inventory"
    if parsed_type == "trade":
        return "trade", "inventory"

    return "unknown", expense_category


def compute_financials(
    *,
    parsed_type: Optional[str],
    parsed_category: Optional[str],
    amount: Optional[float],
    cash_direction: Optional[str],
    message_text: str,
) -> FinancialSummary:
    entry_kind, expense_category = derive_entry_kind(
        parsed_type=parsed_type,
        parsed_category=parsed_category,
        cash_direction=cash_direction,
        message_text=message_text,
    )

    normalized_amount = normalize_payment_amount(amount)
    money_in = 0.0
    money_out = 0.0
    requires_review = False

    if entry_kind == "sale":
        money_in = normalized_amount
    elif entry_kind in {"buy", "expense"}:
        money_out = normalized_amount
    elif entry_kind == "loan_draw":
        money_in = normalized_amount
    elif entry_kind in {"loan_repayment", "transfer"}:
        money_out = normalized_amount
    elif entry_kind == "trade":
        if cash_direction == "to_store":
            money_in = normalized_amount
        elif cash_direction == "from_store":
            money_out = normalized_amount
        elif normalized_amount:
            requires_review = True

    if amount is None and entry_kind in {"sale", "buy", "expense"}:
        requires_review = True

    return FinancialSummary(
        entry_kind=entry_kind,
        money_in=money_in,
        money_out=money_out,
        expense_category=expense_category,
        requires_review=requires_review,
    )
