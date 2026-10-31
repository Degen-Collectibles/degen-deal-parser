"""Audited, cent-exact bank splits; never modify the original cash movement."""
from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation

from .models import BankTransaction, utcnow
from .financial_audit import record_financial_audit
from .bank_overlap import set_reconciliation_note, validate_reconciliation_note

EMPLOYEE_CATEGORIES = {"employee_advances", "employee_reimbursements"}


def money_cents(value) -> int:
    try:
        number = Decimal(str(value))
        if not number.is_finite() or number * 100 != (number * 100).to_integral_value():
            raise ValueError("Enter a finite amount with at most two decimal places.")
        return int(number * 100)
    except (InvalidOperation, TypeError) as exc:
        raise ValueError("Invalid allocation amount.") from exc


def bank_allocations(row: BankTransaction) -> list[dict]:
    from .discord.bank_reconciliation import EXPENSE_CATEGORY_LABELS

    if getattr(row, "allocation_error", None):
        raise ValueError(row.allocation_error)
    try:
        parts = json.loads(getattr(row, "allocations_json", None) or "[]")
    except (ValueError, TypeError) as exc:
        raise ValueError(f"Invalid allocation on bank row {row.id}") from exc
    if not isinstance(parts, list):
        raise ValueError(f"Invalid allocation on bank row {row.id}")
    if not parts:
        return []
    for part in parts:
        if (not isinstance(part, dict)
                or type(part.get("amount_cents")) is not int
                or part["amount_cents"] <= 0
                or part.get("category") not in EXPENSE_CATEGORY_LABELS):
            raise ValueError(f"Invalid allocation on bank row {row.id}")
        if part["category"] == "employee_advances" and row.amount >= 0:
            raise ValueError("Employee advances must be cash outflows.")
        if part["category"] == "employee_reimbursements" and row.amount <= 0:
            raise ValueError("Employee reimbursements must be cash receipts.")
    if sum(p["amount_cents"] for p in parts) != abs(money_cents(row.amount)):
        raise ValueError(f"Allocation no longer equals bank row {row.id}; review before reporting.")
    if row.matched_transaction_id or str(row.classification or "").startswith("logged_in_discord"):
        raise ValueError(f"Split bank row {row.id} is also matched to Discord; resolve overlap first.")
    return parts


def expand_bank_allocations(rows: list[BankTransaction], *, errors: list | None = None) -> list[BankTransaction]:
    expanded = []
    for row in rows:
        try:
            parts = bank_allocations(row)
        except ValueError as exc:
            if errors is None:
                raise
            errors.append({"id": row.id, "error": str(exc)})
            expanded.append(row.model_copy(update={"expense_category": "uncategorized", "expense_subcategory": str(exc)}))
            continue
        if not parts:
            expanded.append(row)
            continue
        for part in parts:
            expanded.append(row.model_copy(update={
                "amount": (-1 if row.amount < 0 else 1) * part["amount_cents"] / 100,
                "expense_category": part["category"],
                "expense_subcategory": ("Estimated split: " if part.get("estimated") else "Split: ") + str(part.get("note", "")),
                "allocations_json": "[]",
            }))
    return expanded


def _set_employee_allocation_one(session, row: BankTransaction, *, employee_amount,
                            business_category: str, estimated: bool, note: str,
                            actor_label: str):
    from .discord.bank_reconciliation import EXPENSE_CATEGORY_LABELS, base_classification

    if row.pending or row.is_removed or not row.amount:
        raise ValueError("Only posted, active bank movements can be allocated.")
    if money_cents(employee_amount) and (row.matched_transaction_id or str(row.classification or "").startswith("logged_in_discord")):
        raise ValueError("Resolve the existing Discord match and adjust/exclude its expense before adding a split.")
    cents = money_cents(employee_amount)
    total = abs(money_cents(row.amount))
    if not 0 <= cents <= total:
        raise ValueError("Employee share must be between zero and the bank amount.")
    if business_category not in EXPENSE_CATEGORY_LABELS or business_category in EMPLOYEE_CATEGORIES:
        raise ValueError("Choose the category for the business remainder.")
    if not note.strip():
        raise ValueError("An evidence or allocation note is required.")
    validate_reconciliation_note(note)
    before = row.model_dump(mode="json")
    parts = []
    if cents:
        parts.append({"category": "employee_advances" if row.amount < 0 else "employee_reimbursements",
                      "amount_cents": cents, "estimated": bool(estimated), "note": note.strip()})
        if cents < total:
            parts.append({"category": business_category, "amount_cents": total - cents,
                          "estimated": bool(estimated), "note": note.strip()})
    row.allocations_json = json.dumps(parts, sort_keys=True)
    row.expense_category = business_category
    row.category_confidence = "manual"
    row.category_reason = "Manual allocation: " + note.strip()
    row.match_override_status = "force_unmatched"
    set_reconciliation_note(session, row, "Manual split; bank allocations are the reporting source. " + note.strip(), actor_label=actor_label)
    row.match_override_at = utcnow()
    row.match_override_by = actor_label
    if not row.matched_transaction_id:
        row.classification = base_classification(row.description, row.amount)
    row.review_status = "open"  # An allocation is not proof of reimbursement.
    row.expense_subcategory = "Employee allocation; see split details" if parts else "Employee allocation cleared"
    row.review_note = (row.review_note or "") + "\nSupersedes prior allocation notes: " + note.strip()
    row.updated_at = utcnow()
    bank_allocations(row)
    session.add(row)
    record_financial_audit(session, action="financial.bank_allocation.edit",
                           resource_key=f"bank_transactions:{row.id}", before=before,
                           after=row.model_dump(mode="json"), actor_label=actor_label, note=note)
    return row


def set_employee_allocation(session, row: BankTransaction, **kwargs):
    """Lock and edit all relink copies together, including explicit clears."""
    if row.id is None:
        return _set_employee_allocation_one(session, row, **kwargs)
    from sqlmodel import select
    from .discord.bank_reconciliation import bank_reporting_groups
    candidates = list(session.exec(select(BankTransaction).where(
        BankTransaction.amount == row.amount, BankTransaction.is_removed == False
    ).order_by(BankTransaction.id).with_for_update()).all())
    group = next((g for g in bank_reporting_groups(candidates) if any(r.id == row.id for r in g)), [row])
    cents = money_cents(kwargs["employee_amount"])
    if cents and any(r.matched_transaction_id or str(r.classification or "").startswith("logged_in_discord") for r in group):
        raise ValueError("Resolve Discord matches on all duplicate copies before allocating; adjust the Discord expense too.")
    if not cents and all((r.allocations_json or "[]") == "[]" for r in group):
        return row
    for copy in group:
        _set_employee_allocation_one(session, copy, **kwargs)
    return row
