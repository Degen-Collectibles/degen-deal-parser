from __future__ import annotations

import csv
import hashlib
import json
import re
from collections import Counter
from datetime import date, datetime, time, timedelta, timezone
from io import StringIO
from typing import Any, Optional

from sqlmodel import Session, delete, select

from .models import BankFeedAccount, BankStatementImport, BankTransaction, Transaction, normalize_money_value, utcnow
from .transactions import transaction_base_query


CLASSIFICATION_LABELS = {
    "logged_in_discord_strong": "Logged in Discord",
    "logged_in_discord_possible": "Possible Discord match",
    "shopify_payout": "Shopify payout",
    "tiktok_payout": "TikTok payout",
    "processor_payout": "Processor payout",
    "paypal_payout": "PayPal payout",
    "direct_customer_payment_needs_log_check": "Direct customer payment",
    "direct_payment_out_needs_log_check": "Direct payment out",
    "cash_deposit_needs_source": "Cash deposit",
    "transfer_or_card_payment": "Transfer/card payment",
    "transfer_or_possible_processor_sweep": "Transfer/processor sweep",
    "credit_needs_review": "Credit needs review",
    "expense_or_purchase_needs_review": "Expense/purchase review",
}

ATTENTION_CLASSIFICATIONS = {
    "logged_in_discord_possible",
    "direct_customer_payment_needs_log_check",
    "direct_payment_out_needs_log_check",
    "cash_deposit_needs_source",
    "transfer_or_possible_processor_sweep",
    "credit_needs_review",
    "expense_or_purchase_needs_review",
}

HIGH_CONFIDENCE_CLASSIFICATIONS = {
    "logged_in_discord_strong",
    "shopify_payout",
    "tiktok_payout",
    "processor_payout",
    "paypal_payout",
    "transfer_or_card_payment",
}

DISCORD_LOGGED_CLASSIFICATIONS = {
    "logged_in_discord_strong",
    "logged_in_discord_possible",
}

EXPENSE_CATEGORY_LABELS = {
    "inventory_purchases": "Inventory purchases",
    "cash_inventory_purchases": "Cash inventory purchases",
    "grading_fees": "Grading fees",
    "shipping_postage": "Shipping/postage",
    "supplies_packaging": "Supplies/packaging",
    "show_fees": "Card show fees",
    "payroll": "Payroll",
    "taxes_licenses": "Taxes/licenses",
    "rent_facilities": "Rent/facilities",
    "software_subscriptions": "Software/subscriptions",
    "travel_airfare": "Travel - airfare",
    "travel_lodging": "Travel - lodging",
    "travel_ground_transport": "Travel - ground/fuel/parking",
    "meals_entertainment": "Meals/entertainment",
    "partner_paybacks": "Partner paybacks",
    "loan_owner_payments": "Loans/owner payments",
    "bank_fees": "Bank/finance fees",
    "transfers": "Bank/credit-card transfers",
    "platform_payouts": "Platform payouts",
    "sales_collections": "Sales collections",
    "cash_deposits": "Cash deposits",
    "other_business_expense": "Other business expense",
    "uncategorized": "Uncategorized",
}

NON_OPERATING_EXPENSE_CATEGORIES = {"transfers", "loan_owner_payments", "partner_paybacks"}
BANK_ACCOUNT_FILTERS = {"all", "checking", "credit_card"}
BANK_ACCOUNT_FILTER_LABELS = {
    "all": "All bank accounts",
    "checking": "Checking",
    "credit_card": "Credit card",
}


def all_classification_choices() -> list[dict[str, str]]:
    return [
        {"value": value, "label": label}
        for value, label in sorted(CLASSIFICATION_LABELS.items(), key=lambda item: item[1].lower())
    ]


def all_expense_category_choices() -> list[dict[str, str]]:
    return [
        {"value": value, "label": label}
        for value, label in sorted(EXPENSE_CATEGORY_LABELS.items(), key=lambda item: item[1].lower())
    ]


def expense_category_label(category: str) -> str:
    return EXPENSE_CATEGORY_LABELS.get(category or "", (category or "uncategorized").replace("_", " ").title())


def parse_bank_date(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(text, fmt).date()
            return datetime.combine(parsed, time(hour=12), tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def parse_bank_amount(value: Any) -> float:
    if value is None:
        return 0.0
    text = str(value).strip()
    is_negative = text.startswith("(") and text.endswith(")")
    text = text.replace("$", "").replace(",", "").replace("(", "").replace(")", "").strip()
    if not text:
        return 0.0
    try:
        parsed = float(text)
    except ValueError:
        return 0.0
    if is_negative and parsed > 0:
        parsed *= -1
    return round(parsed, 2)


def normalize_description_stem(description: str, *, limit: int = 90) -> str:
    text = re.sub(r"\d+", "#", (description or "").upper())
    text = re.sub(r"\s+", " ", text).strip()
    return text[:limit]


def infer_account_type(headers: list[str], explicit: str | None = None) -> str:
    normalized = (explicit or "").strip().lower()
    if normalized in {"checking", "credit_card", "bank", "savings"}:
        return "checking" if normalized == "bank" else normalized
    lowered = {header.strip().lower() for header in headers}
    if {"card", "transaction date", "post date", "category"}.issubset(lowered):
        return "credit_card"
    return "checking"


def find_header(headers: list[str], candidates: tuple[str, ...]) -> Optional[str]:
    normalized = {header.strip().lower(): header for header in headers}
    for candidate in candidates:
        if candidate.lower() in normalized:
            return normalized[candidate.lower()]
    for header in headers:
        lowered = header.strip().lower()
        if any(candidate.lower() in lowered for candidate in candidates):
            return header
    return None


def payout_classification(description: str, amount: float) -> str:
    if amount <= 0:
        return ""
    lower = (description or "").lower()
    if "tiktok" in lower:
        return "tiktok_payout"
    if "shopify" in lower:
        return "shopify_payout"
    if any(token in lower for token in ("stripe", "square inc", "merchant services", "chase payment solutions")):
        return "processor_payout"
    if "paypal" in lower and "transfer" in lower:
        return "paypal_payout"
    return ""


def base_classification(description: str, amount: float) -> str:
    lower = (description or "").lower()
    payout = payout_classification(description, amount)
    if payout:
        return payout
    if any(
        token in lower
        for token in (
            "online payment",
            "autopay",
            "payment thank you",
            "payment to chase card",
            "chase credit crd",
            "cardmember services",
            "epay",
            "online transfer to chk",
            "online transfer from chk",
        )
    ):
        return "transfer_or_card_payment"
    if any(token in lower for token in ("transfer from", "transfer to", "online transfer", "ach transfer", "real time transfer")):
        return "transfer_or_possible_processor_sweep" if amount > 0 else "transfer_or_card_payment"
    if any(token in lower for token in ("zelle payment from", "quickpay with zelle payment from")):
        return "direct_customer_payment_needs_log_check"
    if any(token in lower for token in ("zelle payment to", "venmo", "cash app", "paypal inst xfer")):
        return "direct_payment_out_needs_log_check"
    if amount > 0 and any(token in lower for token in ("deposit", "remote online deposit", "atm cash deposit", "cash deposit")):
        return "cash_deposit_needs_source"
    return "credit_needs_review" if amount > 0 else "expense_or_purchase_needs_review"


def classification_confidence(classification: str) -> str:
    if classification in HIGH_CONFIDENCE_CLASSIFICATIONS:
        return "high"
    if classification in {
        "logged_in_discord_possible",
        "direct_customer_payment_needs_log_check",
        "direct_payment_out_needs_log_check",
        "cash_deposit_needs_source",
    }:
        return "medium"
    return "low"


def classification_label(classification: str) -> str:
    return CLASSIFICATION_LABELS.get(classification, classification.replace("_", " ").title())


def _raw_row_value(raw_row_json: str | None, key: str) -> str:
    if not raw_row_json:
        return ""
    try:
        payload = json.loads(raw_row_json)
    except Exception:
        return ""
    value = payload.get(key)
    return str(value or "").strip()


def _category_result(
    category: str,
    subcategory: str,
    confidence: str,
    reason: str,
) -> dict[str, str]:
    return {
        "expense_category": category,
        "expense_subcategory": subcategory,
        "category_confidence": confidence,
        "category_reason": reason,
    }


def _contains_any(text: str, tokens: tuple[str, ...]) -> bool:
    return any(token in text for token in tokens)


def normalize_bank_account_filter(value: str | None) -> str:
    normalized = (value or "").strip().lower()
    return normalized if normalized in BANK_ACCOUNT_FILTERS else "all"


def bank_account_filter_label(value: str | None) -> str:
    return BANK_ACCOUNT_FILTER_LABELS.get(normalize_bank_account_filter(value), "All bank accounts")


def is_partner_payback_description(description: str) -> bool:
    normalized = re.sub(r"[^a-z0-9]+", " ", (description or "").lower())
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return any(
        name in normalized
        for name in (
            "chia hua wang",
            "chia wang",
            "jeffrey lee",
        )
    )


def bank_row_is_discord_logged(row: Any) -> bool:
    matched_id = getattr(row, "matched_transaction_id", None)
    classification = str(getattr(row, "classification", "") or "")
    return bool(matched_id) or classification in DISCORD_LOGGED_CLASSIFICATIONS


def _category_from_matched_transaction(row: Transaction, amount: float) -> Optional[dict[str, str]]:
    entry_kind = (row.entry_kind or "").lower()
    tx_category = (row.expense_category or row.category or "").lower()
    if amount >= 0:
        return None
    if tx_category == "inventory" or entry_kind in {"buy", "trade"}:
        return _category_result(
            "inventory_purchases",
            "Matched app inventory transaction",
            "high",
            "Matched a normalized Discord/app inventory buy, trade, or inventory expense.",
        )
    if entry_kind == "expense" and tx_category:
        if tx_category in EXPENSE_CATEGORY_LABELS:
            return _category_result(tx_category, "Matched app expense", "high", "Matched an app expense category.")
        if tx_category in {"shipping", "postage"}:
            return _category_result("shipping_postage", "Matched app shipping expense", "high", "Matched an app shipping expense.")
        if tx_category in {"supplies", "office"}:
            return _category_result("supplies_packaging", "Matched app supplies expense", "high", "Matched an app supplies expense.")
    return None


def categorize_bank_payload(payload: dict[str, Any], matched_transaction: Optional[Transaction] = None) -> dict[str, str]:
    amount = float(payload.get("amount") or 0.0)
    classification = str(payload.get("classification") or "")
    description = str(payload.get("description") or "")
    raw_type = str(payload.get("raw_type") or "")
    details = str(payload.get("details") or "")
    raw_row_json = str(payload.get("raw_row_json") or "")
    chase_category = _raw_row_value(raw_row_json, "Category")
    text = " ".join([description, raw_type, details, chase_category]).lower()

    if amount < 0 and is_partner_payback_description(description):
        return _category_result("partner_paybacks", "Partner payback", "high", "Payee is Chia Hua Wang, Chia Wang, or Jeffrey Lee.")

    if matched_transaction:
        matched_category = _category_from_matched_transaction(matched_transaction, amount)
        if matched_category:
            return matched_category

    if amount >= 0:
        if classification in {"shopify_payout", "tiktok_payout", "processor_payout", "paypal_payout"}:
            return _category_result("platform_payouts", classification_label(classification), "high", "Recognized platform payout deposit.")
        if classification == "direct_customer_payment_needs_log_check":
            return _category_result("sales_collections", "Direct customer payment", "medium", "Incoming Zelle/Venmo/PayPal-style customer collection.")
        if classification == "cash_deposit_needs_source":
            return _category_result("cash_deposits", "Cash deposit", "medium", "Incoming cash deposit.")
        if classification in {"transfer_or_card_payment", "transfer_or_possible_processor_sweep"}:
            return _category_result("transfers", "Transfer", "high", "Bank/card transfer, not an operating expense.")
        return _category_result("sales_collections", "Other incoming funds", "low", "Incoming bank activity that was not a platform payout.")

    if classification == "transfer_or_card_payment":
        return _category_result("transfers", "Credit-card or bank transfer", "high", "Card payment or internal bank transfer, not a new expense.")

    if _contains_any(text, ("ca dept tax fee", "cdtfa", "franchise tax", "irs ", "tax payment", "sales tax")):
        return _category_result("taxes_licenses", "Sales/payroll/state tax", "high", "Tax authority or tax-payment descriptor.")
    if "payroll service" in text or "payroll" in text:
        return _category_result("payroll", "Payroll service", "high", "Payroll processor descriptor.")
    if _contains_any(text, ("www.psacard.com", "psa card", "psacard")):
        return _category_result("grading_fees", "PSA grading", "high", "PSA grading charge.")
    if _contains_any(text, ("stamps.com", "shippingeasy", "shipping easy", "fedex", "dhl", "usps", "ups store", "postal")):
        return _category_result("shipping_postage", "Shipping label/postage", "high", "Carrier, postage, or shipping software descriptor.")
    if _contains_any(text, ("pacificchasegroup", "rent", "lease payment", "office rent")):
        return _category_result("rent_facilities", "Rent/facilities", "medium", "Facility, rent, or landlord-style descriptor.")
    if _contains_any(text, ("canva", "sortswift", "shopify*", "google *youtube", "google workspace", "openai", "chatgpt", "adobe", "notion")):
        return _category_result("software_subscriptions", "Software/subscription", "medium", "Software or recurring subscription descriptor.")
    if _contains_any(text, ("front row card show", "genesis card show", "orange county fair", "card show")):
        return _category_result("show_fees", "Card show/event", "medium", "Card show or event descriptor.")
    if _contains_any(text, ("airbnb", "expedia", "hotel", "lodging", "homes to suites", "town and country")):
        return _category_result("travel_lodging", "Hotel/lodging", "high", "Hotel, Airbnb, or lodging descriptor.")
    if _contains_any(text, ("alaska air", "southwes", "southwest", "frontier ai", "swa*earlybrd", "airline")):
        return _category_result("travel_airfare", "Airfare", "high", "Airline descriptor.")
    if _contains_any(text, ("turo", "uber", "parking", "clear *clearme", "chevron", "shell", "valero", "gas")):
        return _category_result("travel_ground_transport", "Ground/fuel/parking", "high", "Vehicle, rideshare, fuel, parking, or airport-service descriptor.")
    if chase_category.lower() == "food & drink" or _contains_any(
        text,
        (
            "doordash",
            "dd *",
            "starbucks",
            "chipotle",
            "restaurant",
            "grill",
            "benihana",
            "javiers",
            "chilis",
            "supermarket",
            "casino",
        ),
    ):
        return _category_result("meals_entertainment", "Meals/entertainment", "medium", "Food, restaurant, or entertainment descriptor.")
    if _contains_any(text, ("vault x", "storage standard", "amazon", "target", "alibaba", "temu", "yami.com", "bric`s", "office & shipping")):
        return _category_result("supplies_packaging", "Supplies/packaging", "medium", "Marketplace, storage, or packaging-supply descriptor.")
    if is_partner_payback_description(description):
        return _category_result("partner_paybacks", "Partner payback", "high", "Payee is Chia Hua Wang, Chia Wang, or Jeffrey Lee.")
    if _contains_any(text, ("zelle payment to", "venmo", "paypal", "apple cash sent", "apple cash balance", "ebay", "wise inc", "wise us")):
        return _category_result("inventory_purchases", "Direct seller/payment marketplace", "medium", "Direct seller, marketplace, or payment-app outflow typically used for inventory buys.")
    if "online domestic wire transfer" in text:
        if is_partner_payback_description(description):
            return _category_result("partner_paybacks", "Partner payback", "high", "Payee is Chia Hua Wang, Chia Wang, or Jeffrey Lee.")
        if "loan" in text:
            return _category_result("loan_owner_payments", "Loan/owner payment", "medium", "Wire memo references a loan/payment.")
        return _category_result("inventory_purchases", "Wire seller payment", "medium", "Outgoing wire to a named seller/vendor.")
    if "withdrawal" in text:
        return _category_result("cash_inventory_purchases", "Cash withdrawal", "low", "Cash withdrawal; likely inventory cash, but the bank line has no payee.")
    if "check" in text:
        return _category_result("uncategorized", "Paper check", "low", "Paper check descriptor does not include enough payee detail.")
    if _contains_any(text, ("fee", "interest", "finance charge")):
        return _category_result("bank_fees", "Bank/finance fee", "medium", "Bank fee or finance-charge descriptor.")
    if chase_category.lower() == "merchandise & inventory":
        return _category_result("supplies_packaging", "Chase merchandise/inventory", "low", "Chase merchant category was Merchandise & Inventory.")
    if chase_category.lower() == "travel":
        return _category_result("travel_ground_transport", "Chase travel", "low", "Chase merchant category was Travel.")
    if chase_category.lower() == "gas":
        return _category_result("travel_ground_transport", "Fuel", "medium", "Chase merchant category was Gas.")
    if chase_category.lower() == "office & shipping":
        return _category_result("shipping_postage", "Office/shipping", "low", "Chase merchant category was Office & Shipping.")
    return _category_result("other_business_expense", "Other business expense", "low", "No stronger category rule matched.")


def parse_bank_csv(content: bytes, *, account_label: str, account_type: str | None = None) -> tuple[list[dict[str, Any]], str]:
    text = content.decode("utf-8-sig")
    reader = csv.DictReader(StringIO(text))
    headers = reader.fieldnames or []
    if not headers:
        raise ValueError("CSV file has no header row")

    resolved_account_type = infer_account_type(headers, account_type)
    date_header = find_header(headers, ("Posting Date", "Post Date", "Date"))
    transaction_date_header = find_header(headers, ("Transaction Date",))
    description_header = find_header(headers, ("Description", "Memo", "Payee"))
    amount_header = find_header(headers, ("Amount",))
    type_header = find_header(headers, ("Type", "Category", "Details"))
    details_header = find_header(headers, ("Details",))
    balance_header = find_header(headers, ("Balance",))
    check_header = find_header(headers, ("Check or Slip #", "Check", "Check Number"))

    if not date_header or not description_header or not amount_header:
        raise ValueError("CSV must include date, description, and amount columns")

    parsed_rows: list[dict[str, Any]] = []
    for row_index, row in enumerate(reader, start=2):
        posted_at = parse_bank_date(row.get(date_header))
        transaction_at = parse_bank_date(row.get(transaction_date_header)) if transaction_date_header else None
        description = str(row.get(description_header) or "").strip()
        amount = parse_bank_amount(row.get(amount_header))
        parsed_rows.append(
            {
                "row_index": row_index,
                "account_label": account_label,
                "account_type": resolved_account_type,
                "posted_at": posted_at,
                "transaction_at": transaction_at,
                "description": description,
                "description_stem": normalize_description_stem(description),
                "details": str(row.get(details_header) or "").strip() if details_header else None,
                "raw_type": str(row.get(type_header) or "").strip() if type_header else None,
                "amount": amount,
                "balance": parse_bank_amount(row.get(balance_header)) if balance_header and row.get(balance_header) not in (None, "") else None,
                "check_or_slip": str(row.get(check_header) or "").strip() if check_header else None,
                "raw_row_json": json.dumps(row, default=str),
            }
        )
    return parsed_rows, resolved_account_type


def _date_from_datetime(value: Optional[datetime]) -> Optional[date]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.date()


def load_matchable_transactions(session: Session, rows: list[dict[str, Any]]) -> list[Transaction]:
    dates = [_date_from_datetime(row.get("posted_at")) for row in rows if row.get("posted_at")]
    if not dates:
        return []
    start = datetime.combine(min(dates) - timedelta(days=7), time.min, tzinfo=timezone.utc)
    end = datetime.combine(max(dates) + timedelta(days=7), time.max, tzinfo=timezone.utc)
    return list(session.exec(transaction_base_query(start=start, end=end)).all())


def _transaction_match_amount(row: Transaction) -> float:
    if row.amount is not None:
        return abs(normalize_money_value(row.amount))
    return abs(normalize_money_value(row.money_in) or normalize_money_value(row.money_out))


def _transaction_signed_amount(row: Transaction) -> float:
    return round(normalize_money_value(row.money_in) - normalize_money_value(row.money_out), 2)


def match_bank_rows_to_transactions(
    bank_rows: list[dict[str, Any]],
    transactions: list[Transaction],
) -> None:
    used_transaction_ids: set[int] = set()
    for bank_row in bank_rows:
        posted_date = _date_from_datetime(bank_row.get("posted_at"))
        amount = float(bank_row.get("amount") or 0.0)
        candidates: list[tuple[int, int, Transaction]] = []
        for tx in transactions:
            if tx.id in used_transaction_ids:
                continue
            if abs(_transaction_match_amount(tx) - abs(amount)) > 0.01:
                continue
            tx_date = _date_from_datetime(tx.occurred_at)
            if not tx_date or not posted_date:
                continue
            day_delta = abs((tx_date - posted_date).days)
            if day_delta > 5:
                continue
            score = 100 - (day_delta * 10)
            payment_method = (tx.payment_method or "").lower()
            description = (bank_row.get("description") or "").lower()
            signed = _transaction_signed_amount(tx)
            if payment_method and payment_method in description:
                score += 15
            if amount > 0 and signed > 0:
                score += 15
            if amount < 0 and signed < 0:
                score += 15
            if (tx.entry_kind or "") == "sale" and amount > 0:
                score += 5
            if (tx.entry_kind or "") in {"buy", "expense", "trade"} and amount < 0:
                score += 5
            candidates.append((score, day_delta, tx))

        if not candidates:
            classification = base_classification(str(bank_row.get("description") or ""), amount)
            bank_row.update(
                {
                    "classification": classification,
                    "confidence": classification_confidence(classification),
                    "match_reason": "No exact app transaction amount/date match found.",
                    "matched_transaction_id": None,
                    "matched_source_message_id": None,
                    "matched_platform": (
                        classification.split("_", 1)[0]
                        if classification in {"shopify_payout", "tiktok_payout", "processor_payout", "paypal_payout"}
                        else None
                    ),
                }
            )
            bank_row.update(categorize_bank_payload(bank_row))
            continue

        candidates.sort(key=lambda item: (-item[0], item[1], item[2].id or 0))
        score, day_delta, tx = candidates[0]
        if tx.id is not None:
            used_transaction_ids.add(tx.id)
        classification = "logged_in_discord_strong" if score >= 110 and day_delta <= 2 else "logged_in_discord_possible"
        bank_row.update(
            {
                "classification": classification,
                "confidence": "high" if classification == "logged_in_discord_strong" else "medium",
                "match_reason": f"Amount/date match to app transaction; date delta {day_delta} day(s), score {score}.",
                "matched_transaction_id": tx.id,
                "matched_source_message_id": tx.source_message_id,
                "matched_platform": "discord",
            }
        )
        bank_row.update(categorize_bank_payload(bank_row, tx))


def import_bank_statement_file(
    session: Session,
    *,
    filename: str,
    content: bytes,
    account_label: str,
    account_type: str | None = None,
) -> BankStatementImport:
    if not filename.lower().endswith(".csv"):
        raise ValueError("Bank reconciliation currently supports CSV exports")

    account_label = (account_label or "").strip() or "Bank account"
    file_hash = hashlib.sha256(content).hexdigest()
    existing = session.exec(
        select(BankStatementImport).where(BankStatementImport.file_hash == file_hash)
    ).first()
    if existing:
        rerun_bank_reconciliation(session, existing.id)
        return existing

    rows, resolved_account_type = parse_bank_csv(content, account_label=account_label, account_type=account_type)
    if not rows:
        raise ValueError("CSV did not contain any transaction rows")

    match_bank_rows_to_transactions(rows, load_matchable_transactions(session, rows))

    dates = [row["posted_at"] for row in rows if row.get("posted_at")]
    total_credits = round(sum(float(row["amount"]) for row in rows if float(row["amount"]) > 0), 2)
    total_debits = round(sum(float(row["amount"]) for row in rows if float(row["amount"]) < 0), 2)
    import_row = BankStatementImport(
        label=f"{account_label} - {filename}",
        account_label=account_label,
        account_type=resolved_account_type,
        source_name=filename,
        file_hash=file_hash,
        row_count=len(rows),
        range_start=min(dates) if dates else None,
        range_end=max(dates) if dates else None,
        total_credits=total_credits,
        total_debits=total_debits,
        net_amount=round(total_credits + total_debits, 2),
    )
    session.add(import_row)
    session.commit()
    session.refresh(import_row)

    for row in rows:
        session.add(
            BankTransaction(
                import_id=import_row.id,
                row_index=int(row["row_index"]),
                account_label=account_label,
                account_type=resolved_account_type,
                posted_at=row.get("posted_at"),
                transaction_at=row.get("transaction_at"),
                description=str(row.get("description") or ""),
                description_stem=str(row.get("description_stem") or ""),
                details=row.get("details") or None,
                raw_type=row.get("raw_type") or None,
                amount=float(row.get("amount") or 0.0),
                balance=row.get("balance"),
                check_or_slip=row.get("check_or_slip") or None,
                classification=str(row.get("classification") or "needs_review"),
                confidence=str(row.get("confidence") or "low"),
                expense_category=str(row.get("expense_category") or "uncategorized"),
                expense_subcategory=row.get("expense_subcategory") or None,
                category_confidence=str(row.get("category_confidence") or "low"),
                category_reason=str(row.get("category_reason") or ""),
                match_reason=str(row.get("match_reason") or ""),
                matched_transaction_id=row.get("matched_transaction_id"),
                matched_source_message_id=row.get("matched_source_message_id"),
                matched_platform=row.get("matched_platform"),
                raw_row_json=str(row.get("raw_row_json") or "{}"),
            )
        )
    session.commit()
    return import_row


def rerun_bank_reconciliation(session: Session, import_id: int) -> BankStatementImport:
    import_row = session.get(BankStatementImport, import_id)
    if not import_row:
        raise ValueError("Bank import not found")
    rows = session.exec(
        select(BankTransaction)
        .where(BankTransaction.import_id == import_id)
        .order_by(BankTransaction.row_index)
    ).all()
    payloads: list[dict[str, Any]] = []
    for row in rows:
        payloads.append(
            {
                "row_index": row.row_index,
                "posted_at": row.posted_at,
                "transaction_at": row.transaction_at,
                "description": row.description,
                "description_stem": row.description_stem,
                "details": row.details,
                "raw_type": row.raw_type,
                "amount": row.amount,
                "balance": row.balance,
                "check_or_slip": row.check_or_slip,
                "raw_row_json": row.raw_row_json,
            }
        )
    match_bank_rows_to_transactions(payloads, load_matchable_transactions(session, payloads))

    by_index = {row.row_index: row for row in rows}
    now = utcnow()
    for payload in payloads:
        row = by_index.get(int(payload["row_index"]))
        if not row:
            continue
        row.classification = str(payload.get("classification") or row.classification)
        row.confidence = str(payload.get("confidence") or row.confidence)
        row.expense_category = str(payload.get("expense_category") or row.expense_category or "uncategorized")
        row.expense_subcategory = payload.get("expense_subcategory") or None
        row.category_confidence = str(payload.get("category_confidence") or row.category_confidence or "low")
        row.category_reason = str(payload.get("category_reason") or "")
        row.match_reason = str(payload.get("match_reason") or "")
        row.matched_transaction_id = payload.get("matched_transaction_id")
        row.matched_source_message_id = payload.get("matched_source_message_id")
        row.matched_platform = payload.get("matched_platform")
        row.updated_at = now
        session.add(row)
    session.commit()
    return import_row


def list_bank_statement_imports(session: Session) -> list[BankStatementImport]:
    return list(
        session.exec(
            select(BankStatementImport).order_by(
                BankStatementImport.created_at.desc(),
                BankStatementImport.id.desc(),
            )
        ).all()
    )


def get_bank_transactions(
    session: Session,
    *,
    import_id: int,
    classification: str = "",
    expense_category: str = "",
    review_status: str = "",
    search: str = "",
    attention_only: bool = False,
    expenses_only: bool = False,
) -> list[BankTransaction]:
    stmt = select(BankTransaction).where(BankTransaction.import_id == import_id)
    rows = [row for row in session.exec(stmt).all() if not row.is_removed]
    classification = (classification or "").strip()
    expense_category = (expense_category or "").strip()
    review_status = (review_status or "").strip()
    search_text = (search or "").strip().lower()
    if classification:
        rows = [row for row in rows if row.classification == classification]
    if expense_category:
        rows = [row for row in rows if (row.expense_category or "uncategorized") == expense_category]
    if expenses_only:
        rows = [row for row in rows if float(row.amount or 0.0) < 0]
    if review_status:
        rows = [row for row in rows if row.review_status == review_status]
    if attention_only:
        rows = [
            row
            for row in rows
            if row.classification in ATTENTION_CLASSIFICATIONS and row.review_status == "open"
        ]
    if search_text:
        rows = [
            row
            for row in rows
            if search_text in (row.description or "").lower()
            or search_text in (row.description_stem or "").lower()
            or search_text in (row.raw_type or "").lower()
            or search_text in (row.expense_category or "").lower()
            or search_text in (row.expense_subcategory or "").lower()
        ]

    return sorted(rows, key=lambda row: (row.row_index, row.id or 0))


def summarize_bank_transactions(rows: list[BankTransaction]) -> dict[str, Any]:
    counts = Counter(row.classification for row in rows)
    category_counts = Counter(row.expense_category or "uncategorized" for row in rows if float(row.amount or 0.0) < 0)
    open_attention = [
        row
        for row in rows
        if row.review_status == "open" and row.classification in ATTENTION_CLASSIFICATIONS
    ]
    credits = round(sum(float(row.amount or 0.0) for row in rows if float(row.amount or 0.0) > 0), 2)
    debits = round(sum(float(row.amount or 0.0) for row in rows if float(row.amount or 0.0) < 0), 2)
    operating_expense_rows = [
        row
        for row in rows
        if float(row.amount or 0.0) < 0
        and (row.expense_category or "uncategorized") not in NON_OPERATING_EXPENSE_CATEGORIES
    ]
    non_operating_debits = [
        row
        for row in rows
        if float(row.amount or 0.0) < 0
        and (row.expense_category or "uncategorized") in NON_OPERATING_EXPENSE_CATEGORIES
    ]
    uncategorized_expenses = [
        row
        for row in rows
        if float(row.amount or 0.0) < 0 and (row.expense_category or "uncategorized") == "uncategorized"
    ]
    return {
        "rows": len(rows),
        "credits": credits,
        "debits": debits,
        "expense_total": round(sum(abs(float(row.amount or 0.0)) for row in operating_expense_rows), 2),
        "non_operating_debits": round(sum(abs(float(row.amount or 0.0)) for row in non_operating_debits), 2),
        "net": round(credits + debits, 2),
        "matched": counts["logged_in_discord_strong"] + counts["logged_in_discord_possible"],
        "payouts": counts["shopify_payout"] + counts["tiktok_payout"] + counts["processor_payout"] + counts["paypal_payout"],
        "attention": len(open_attention),
        "expense_categories": len(category_counts),
        "uncategorized_expenses": len(uncategorized_expenses),
        "classification_counts": counts,
        "category_counts": category_counts,
    }


def build_classification_options(rows: list[BankTransaction]) -> list[dict[str, Any]]:
    counts = Counter(row.classification for row in rows)
    return [
        {
            "value": classification,
            "label": classification_label(classification),
            "count": count,
        }
        for classification, count in sorted(
            counts.items(),
            key=lambda item: (-item[1], classification_label(item[0]).lower()),
        )
    ]


def build_expense_category_options(rows: list[BankTransaction]) -> list[dict[str, Any]]:
    totals: dict[str, dict[str, Any]] = {}
    for row in rows:
        if float(row.amount or 0.0) >= 0:
            continue
        category = row.expense_category or "uncategorized"
        if category not in totals:
            totals[category] = {
                "value": category,
                "label": expense_category_label(category),
                "count": 0,
                "total": 0.0,
            }
        totals[category]["count"] += 1
        totals[category]["total"] += abs(float(row.amount or 0.0))
    return sorted(
        totals.values(),
        key=lambda item: (
            -float(item["total"]),
            str(item["label"]).lower(),
        ),
    )


def _bank_category_group(category: str) -> str:
    if category in {"inventory_purchases", "cash_inventory_purchases", "grading_fees"}:
        return "inventory"
    if category == "partner_paybacks":
        return "partner_paybacks"
    if category in NON_OPERATING_EXPENSE_CATEGORIES:
        return "non_operating"
    if category == "uncategorized":
        return "uncategorized"
    return "operating"


def _bank_day_key(value: Optional[datetime]) -> str:
    if value is None:
        return ""
    return value.date().isoformat()


def _bank_day_label(day_key: str) -> str:
    parsed = date.fromisoformat(day_key)
    return f"{parsed.strftime('%b')} {parsed.day}"


def build_finance_bank_expense_data(
    session: Session,
    *,
    start: datetime,
    end: datetime,
    account_filter: str = "all",
) -> dict[str, Any]:
    selected_account = normalize_bank_account_filter(account_filter)
    stmt = (
        select(BankTransaction)
        .where(BankTransaction.amount < 0)
        .where(BankTransaction.is_removed == False)  # noqa: E712
        .where(BankTransaction.posted_at >= start)
        .where(BankTransaction.posted_at <= end)
    )
    if selected_account != "all":
        stmt = stmt.where(BankTransaction.account_type == selected_account)
    rows = [row for row in session.exec(stmt).all() if not row.is_removed]

    category_totals: dict[str, dict[str, Any]] = {}
    account_totals: dict[str, dict[str, Any]] = {}
    daily_totals: dict[str, dict[str, Any]] = {}
    gross_outflow_total = 0.0
    bank_only_total = 0.0
    discord_logged_total = 0.0
    discord_logged_count = 0
    operating_total = 0.0
    non_operating_total = 0.0
    inventory_total = 0.0
    partner_paybacks_total = 0.0
    uncategorized_total = 0.0
    uncategorized_count = 0

    for row in rows:
        amount = abs(float(row.amount or 0.0))
        category = row.expense_category or "uncategorized"
        category_group = _bank_category_group(category)
        is_non_operating = category in NON_OPERATING_EXPENSE_CATEGORIES
        is_operating = not is_non_operating
        is_discord_logged = bank_row_is_discord_logged(row)
        gross_outflow_total += amount
        if is_discord_logged:
            discord_logged_total += amount
            discord_logged_count += 1
        else:
            bank_only_total += amount
            if is_operating:
                operating_total += amount
            else:
                non_operating_total += amount
            if category_group == "inventory":
                inventory_total += amount
            elif category_group == "partner_paybacks":
                partner_paybacks_total += amount
            elif category_group == "uncategorized":
                uncategorized_total += amount
                uncategorized_count += 1

        category_bucket = category_totals.setdefault(
            category,
            {
                "category": category,
                "label": expense_category_label(category),
                "count": 0,
                "bank_only_count": 0,
                "discord_logged_count": 0,
                "total": 0.0,
                "bank_only_total": 0.0,
                "discord_logged_total": 0.0,
                "operating_total": 0.0,
                "non_operating_total": 0.0,
                "group": category_group,
            },
        )
        category_bucket["count"] = int(category_bucket["count"]) + 1
        category_bucket["total"] = float(category_bucket["total"]) + amount
        if is_discord_logged:
            category_bucket["discord_logged_count"] = int(category_bucket["discord_logged_count"]) + 1
            category_bucket["discord_logged_total"] = float(category_bucket["discord_logged_total"]) + amount
        else:
            category_bucket["bank_only_count"] = int(category_bucket["bank_only_count"]) + 1
            category_bucket["bank_only_total"] = float(category_bucket["bank_only_total"]) + amount
            if is_operating:
                category_bucket["operating_total"] = float(category_bucket["operating_total"]) + amount
            else:
                category_bucket["non_operating_total"] = float(category_bucket["non_operating_total"]) + amount

        account_key = row.account_label or row.account_type or "Unknown account"
        account_bucket = account_totals.setdefault(
            account_key,
            {
                "label": account_key,
                "account_type": row.account_type or "unknown",
                "count": 0,
                "bank_only_count": 0,
                "discord_logged_count": 0,
                "total": 0.0,
                "bank_only_total": 0.0,
                "discord_logged_total": 0.0,
                "operating_total": 0.0,
                "non_operating_total": 0.0,
            },
        )
        account_bucket["count"] = int(account_bucket["count"]) + 1
        account_bucket["total"] = float(account_bucket["total"]) + amount
        if is_discord_logged:
            account_bucket["discord_logged_count"] = int(account_bucket["discord_logged_count"]) + 1
            account_bucket["discord_logged_total"] = float(account_bucket["discord_logged_total"]) + amount
        else:
            account_bucket["bank_only_count"] = int(account_bucket["bank_only_count"]) + 1
            account_bucket["bank_only_total"] = float(account_bucket["bank_only_total"]) + amount
            if is_operating:
                account_bucket["operating_total"] = float(account_bucket["operating_total"]) + amount
            else:
                account_bucket["non_operating_total"] = float(account_bucket["non_operating_total"]) + amount

        day_key = _bank_day_key(row.posted_at)
        if day_key:
            daily_bucket = daily_totals.setdefault(
                day_key,
                {
                    "date": day_key,
                    "label": _bank_day_label(day_key),
                    "operating": 0.0,
                    "inventory": 0.0,
                    "partner_paybacks": 0.0,
                    "non_operating": 0.0,
                    "uncategorized": 0.0,
                    "already_logged": 0.0,
                    "total": 0.0,
                    "bank_only_total": 0.0,
                },
            )
            daily_bucket["total"] = float(daily_bucket["total"]) + amount
            if is_discord_logged:
                daily_bucket["already_logged"] = float(daily_bucket["already_logged"]) + amount
            else:
                daily_bucket["bank_only_total"] = float(daily_bucket["bank_only_total"]) + amount
                if category_group == "inventory":
                    daily_bucket["inventory"] = float(daily_bucket["inventory"]) + amount
                elif category_group == "partner_paybacks":
                    daily_bucket["partner_paybacks"] = float(daily_bucket["partner_paybacks"]) + amount
                elif category_group == "uncategorized":
                    daily_bucket["uncategorized"] = float(daily_bucket["uncategorized"]) + amount
                elif is_non_operating:
                    daily_bucket["non_operating"] = float(daily_bucket["non_operating"]) + amount
                else:
                    daily_bucket["operating"] = float(daily_bucket["operating"]) + amount

    category_rows = sorted(
        [
            {
                **bucket,
                "total": round(float(bucket["total"]), 2),
                "bank_only_total": round(float(bucket["bank_only_total"]), 2),
                "discord_logged_total": round(float(bucket["discord_logged_total"]), 2),
                "operating_total": round(float(bucket["operating_total"]), 2),
                "non_operating_total": round(float(bucket["non_operating_total"]), 2),
                "share_pct": round((float(bucket["bank_only_total"]) / bank_only_total) * 100.0, 1)
                if bank_only_total
                else 0.0,
            }
            for bucket in category_totals.values()
        ],
        key=lambda item: (
            -float(item["bank_only_total"]),
            -float(item["total"]),
            str(item["label"]).lower(),
        ),
    )
    account_rows = sorted(
        [
            {
                **bucket,
                "total": round(float(bucket["total"]), 2),
                "bank_only_total": round(float(bucket["bank_only_total"]), 2),
                "discord_logged_total": round(float(bucket["discord_logged_total"]), 2),
                "operating_total": round(float(bucket["operating_total"]), 2),
                "non_operating_total": round(float(bucket["non_operating_total"]), 2),
                "share_pct": round((float(bucket["bank_only_total"]) / bank_only_total) * 100.0, 1)
                if bank_only_total
                else 0.0,
            }
            for bucket in account_totals.values()
        ],
        key=lambda item: (-float(item["total"]), str(item["label"]).lower()),
    )
    daily_rows = [
        {
            **bucket,
            "operating": round(float(bucket["operating"]), 2),
            "inventory": round(float(bucket["inventory"]), 2),
            "partner_paybacks": round(float(bucket["partner_paybacks"]), 2),
            "non_operating": round(float(bucket["non_operating"]), 2),
            "uncategorized": round(float(bucket["uncategorized"]), 2),
            "already_logged": round(float(bucket["already_logged"]), 2),
            "total": round(float(bucket["total"]), 2),
            "bank_only_total": round(float(bucket["bank_only_total"]), 2),
        }
        for _, bucket in sorted(daily_totals.items())
    ]

    return {
        "account_filter": selected_account,
        "account_filter_label": bank_account_filter_label(selected_account),
        "row_count": len(rows),
        "gross_outflow_total": round(gross_outflow_total, 2),
        "bank_only_total": round(bank_only_total, 2),
        "bank_only_count": len(rows) - discord_logged_count,
        "discord_logged_total": round(discord_logged_total, 2),
        "discord_logged_count": discord_logged_count,
        "operating_total": round(operating_total, 2),
        "non_operating_total": round(non_operating_total, 2),
        "inventory_total": round(inventory_total, 2),
        "partner_paybacks_total": round(partner_paybacks_total, 2),
        "uncategorized_total": round(uncategorized_total, 2),
        "uncategorized_count": uncategorized_count,
        "category_rows": category_rows,
        "account_rows": account_rows,
        "daily_rows": daily_rows,
    }


def delete_bank_import(session: Session, import_id: int) -> None:
    feed_accounts = session.exec(
        select(BankFeedAccount).where(BankFeedAccount.bank_import_id == import_id)
    ).all()
    for account in feed_accounts:
        account.bank_import_id = None
        account.updated_at = utcnow()
        session.add(account)
    session.exec(delete(BankTransaction).where(BankTransaction.import_id == import_id))
    import_row = session.get(BankStatementImport, import_id)
    if import_row:
        session.delete(import_row)
    session.commit()
