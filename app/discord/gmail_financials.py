from __future__ import annotations

import base64
import hashlib
import html
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from html.parser import HTMLParser
from typing import Any, Optional
from urllib.parse import urlencode

import httpx
from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, delete, select

from ..config import get_settings
from ..models import (
    BankTransaction,
    DiscordMessage,
    GmailConnection,
    GmailEvidenceLink,
    GmailReceipt,
    GmailReceiptLineItem,
    GmailSyncRun,
    PARSE_REVIEW_REQUIRED,
    Transaction,
    TransactionItem,
    normalize_money_value,
    utcnow,
)

GMAIL_READONLY_SCOPE = "https://www.googleapis.com/auth/gmail.readonly"
GMAIL_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GMAIL_TOKEN_URL = "https://oauth2.googleapis.com/token"
GMAIL_API_BASE = "https://gmail.googleapis.com/gmail/v1"
DEFAULT_GMAIL_ACCOUNT = "degencollectiblesllc@gmail.com"


class GmailConfigurationError(ValueError):
    pass


class GmailAPIError(RuntimeError):
    pass


@dataclass
class SortSwiftParseResult:
    is_sortswift: bool
    vendor: str = ""
    receipt_type: str = "receipt"
    total_cash: Optional[float] = None
    total_credit: Optional[float] = None
    actual_tender: Optional[str] = None
    actual_tender_amount: Optional[float] = None
    quantity_total: int = 0
    line_items: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


class _TableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.rows: list[list[str]] = []
        self._current_row: Optional[list[str]] = None
        self._current_cell: Optional[list[str]] = None
        self._in_cell = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        if tag.lower() == "tr":
            self._current_row = []
        elif tag.lower() in {"td", "th"} and self._current_row is not None:
            self._current_cell = []
            self._in_cell = True

    def handle_data(self, data: str) -> None:
        if self._in_cell and self._current_cell is not None:
            self._current_cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        lowered = tag.lower()
        if lowered in {"td", "th"} and self._current_row is not None and self._current_cell is not None:
            text = html.unescape(" ".join(self._current_cell))
            self._current_row.append(re.sub(r"\s+", " ", text).strip())
            self._current_cell = None
            self._in_cell = False
        elif lowered == "tr" and self._current_row is not None:
            if any(cell for cell in self._current_row):
                self.rows.append(self._current_row)
            self._current_row = None


def _fernet() -> Fernet:
    settings = get_settings()
    secret = (
        (settings.bank_feed_encryption_key or "").strip()
        or (settings.session_secret or "").strip()
        or (settings.google_gmail_client_secret or "").strip()
    )
    if not secret:
        raise GmailConfigurationError("Set BANK_FEED_ENCRYPTION_KEY before connecting Gmail")
    key = base64.urlsafe_b64encode(hashlib.sha256(secret.encode("utf-8")).digest())
    return Fernet(key)


def encrypt_gmail_token(token: str) -> bytes:
    return _fernet().encrypt(token.encode("utf-8"))


def decrypt_gmail_token(blob: Optional[bytes]) -> str:
    if not blob:
        raise GmailConfigurationError("Gmail connection has no stored token")
    try:
        return _fernet().decrypt(bytes(blob)).decode("utf-8")
    except InvalidToken as exc:
        raise GmailConfigurationError("Stored Gmail token could not be decrypted") from exc


def gmail_config_status() -> dict[str, Any]:
    settings = get_settings()
    missing: list[str] = []
    if not (settings.google_gmail_client_id or "").strip():
        missing.append("GOOGLE_GMAIL_CLIENT_ID")
    if not (settings.google_gmail_client_secret or "").strip():
        missing.append("GOOGLE_GMAIL_CLIENT_SECRET")
    if not effective_gmail_redirect_uri():
        missing.append("GOOGLE_GMAIL_REDIRECT_URI or PUBLIC_BASE_URL")
    return {
        "configured": not missing,
        "missing": missing,
        "account": settings.google_gmail_account or DEFAULT_GMAIL_ACCOUNT,
        "scope": GMAIL_READONLY_SCOPE,
        "redirect_uri": effective_gmail_redirect_uri(),
        "restricted_scope": True,
    }


def effective_gmail_redirect_uri() -> str:
    settings = get_settings()
    explicit = (settings.google_gmail_redirect_uri or "").strip()
    if explicit:
        return explicit
    base = (settings.public_base_url or "").strip().rstrip("/")
    return f"{base}/bookkeeping/gmail/callback" if base else ""


def build_gmail_oauth_url(state: str) -> str:
    settings = get_settings()
    status = gmail_config_status()
    if not status["configured"]:
        raise GmailConfigurationError("Gmail is not configured: " + ", ".join(status["missing"]))
    params = {
        "client_id": settings.google_gmail_client_id,
        "redirect_uri": status["redirect_uri"],
        "response_type": "code",
        "scope": GMAIL_READONLY_SCOPE,
        "access_type": "offline",
        "prompt": "consent",
        "state": state,
        "login_hint": status["account"],
    }
    return f"{GMAIL_AUTH_URL}?{urlencode(params)}"


def build_gmail_financial_search_query(*, days: int = 180) -> str:
    safe_days = max(1, min(int(days or 180), 3650))
    terms = (
        "invoice OR receipt OR \"order confirmation\" OR \"payment received\" OR "
        "\"your bill\" OR \"tax invoice\" OR \"purchase confirmation\" OR "
        "\"Buylist Confirmation\" OR from:no-reply@mail.sortswift.com"
    )
    return f"newer_than:{safe_days}d ({terms}) -category:promotions -category:social"


def exchange_gmail_oauth_code(code: str) -> dict[str, Any]:
    settings = get_settings()
    status = gmail_config_status()
    if not status["configured"]:
        raise GmailConfigurationError("Gmail is not configured: " + ", ".join(status["missing"]))
    payload = {
        "code": code,
        "client_id": settings.google_gmail_client_id,
        "client_secret": settings.google_gmail_client_secret,
        "redirect_uri": status["redirect_uri"],
        "grant_type": "authorization_code",
    }
    response = httpx.post(GMAIL_TOKEN_URL, data=payload, timeout=30.0)
    data = response.json() if response.content else {}
    if response.status_code >= 400:
        raise GmailAPIError(str(data.get("error_description") or data.get("error") or response.text))
    return data


def refresh_gmail_access_token(session: Session, connection: GmailConnection) -> str:
    settings = get_settings()
    refresh_token = decrypt_gmail_token(connection.refresh_token_enc)
    payload = {
        "client_id": settings.google_gmail_client_id,
        "client_secret": settings.google_gmail_client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }
    response = httpx.post(GMAIL_TOKEN_URL, data=payload, timeout=30.0)
    data = response.json() if response.content else {}
    if response.status_code >= 400:
        raise GmailAPIError(str(data.get("error_description") or data.get("error") or response.text))
    access_token = str(data.get("access_token") or "")
    if not access_token:
        raise GmailAPIError("Gmail refresh did not return an access token")
    connection.access_token_enc = encrypt_gmail_token(access_token)
    expires_in = int(data.get("expires_in") or 0)
    if expires_in:
        connection.access_token_expires_at = utcnow() + timedelta(seconds=max(expires_in - 60, 0))
    connection.updated_at = utcnow()
    session.add(connection)
    session.flush()
    return access_token


def access_token_for_gmail_connection(session: Session, connection: GmailConnection) -> str:
    expires_at = connection.access_token_expires_at
    if expires_at and expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    if not connection.access_token_enc or (expires_at and expires_at <= utcnow() + timedelta(minutes=5)):
        return refresh_gmail_access_token(session, connection)
    return decrypt_gmail_token(connection.access_token_enc)


def upsert_gmail_connection_from_oauth(session: Session, token_payload: dict[str, Any]) -> GmailConnection:
    settings = get_settings()
    email = (settings.google_gmail_account or DEFAULT_GMAIL_ACCOUNT).strip().lower()
    connection = session.exec(select(GmailConnection).where(GmailConnection.email_address == email)).first()
    if connection is None:
        connection = GmailConnection(email_address=email)
    access_token = str(token_payload.get("access_token") or "")
    refresh_token = str(token_payload.get("refresh_token") or "")
    if access_token:
        connection.access_token_enc = encrypt_gmail_token(access_token)
    if refresh_token:
        connection.refresh_token_enc = encrypt_gmail_token(refresh_token)
    expires_in = int(token_payload.get("expires_in") or 0)
    if expires_in:
        connection.access_token_expires_at = utcnow() + timedelta(seconds=max(expires_in - 60, 0))
    connection.scopes_json = json.dumps([GMAIL_READONLY_SCOPE])
    connection.status = "active"
    connection.sync_query = build_gmail_financial_search_query(days=settings.gmail_sync_days)
    connection.updated_at = utcnow()
    session.add(connection)
    session.commit()
    session.refresh(connection)
    return connection


def _money(text: Any) -> Optional[float]:
    cleaned = str(text or "").replace(",", "")
    match = re.search(r"(?:USD\s*)?\$?\s*(-?\d+(?:\.\d+)?)", cleaned, re.I)
    if not match:
        return None
    return float(Decimal(match.group(1)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _int_value(text: Any) -> int:
    match = re.search(r"\d+", str(text or ""))
    return int(match.group(0)) if match else 0


def _plain_text_from_html(value: str) -> str:
    text = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", value or "")
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</(p|div|tr|h\d)>", "\n", text)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", html.unescape(text)).strip()


def parse_sortswift_buylist_email(html_body: str) -> SortSwiftParseResult:
    body = html_body or ""
    lowered = body.lower()
    if "sortswift" not in lowered and "buylist confirmation" not in lowered:
        return SortSwiftParseResult(is_sortswift=False)

    parser = _TableParser()
    parser.feed(body)
    rows = parser.rows
    header_index = next(
        (
            idx
            for idx, row in enumerate(rows)
            if {"name", "set", "qty", "cash"}.issubset({cell.strip().lower() for cell in row})
        ),
        -1,
    )
    if header_index < 0:
        return SortSwiftParseResult(is_sortswift=True, vendor="SortSwift", receipt_type="sortswift_buylist")

    headers = [cell.strip().lower() for cell in rows[header_index]]
    line_items: list[dict[str, Any]] = []
    total_cash: Optional[float] = None
    total_credit: Optional[float] = None
    quantity_total = 0

    def cell(row: list[str], name: str) -> str:
        try:
            idx = headers.index(name)
        except ValueError:
            return ""
        return row[idx] if idx < len(row) else ""

    for row in rows[header_index + 1 :]:
        joined = " ".join(row).strip()
        if not joined:
            continue
        if "totals:" in joined.lower():
            total_cash = _money(cell(row, "cash"))
            total_credit = _money(cell(row, "credit"))
            quantity_total = _int_value(cell(row, "qty"))
            money_values = [
                _money(cell_value)
                for cell_value in row
                if re.search(r"\$|USD", str(cell_value or ""), re.I) and _money(cell_value) is not None
            ]
            if total_cash is None and money_values:
                total_cash = money_values[0]
            if total_credit is None and len(money_values) >= 2:
                total_credit = money_values[1]
            if quantity_total <= 0:
                non_money_cells = [cell_value for cell_value in row if _money(cell_value) is None]
                for cell_value in reversed(non_money_cells):
                    quantity_total = _int_value(cell_value)
                    if quantity_total:
                        break
            continue
        name = cell(row, "name")
        if not name:
            continue
        quantity = _int_value(cell(row, "qty")) or 1
        cash_amount = _money(cell(row, "cash"))
        credit_amount = _money(cell(row, "credit"))
        line_items.append(
            {
                "name": name,
                "set": cell(row, "set"),
                "number": cell(row, "number"),
                "condition": cell(row, "cond"),
                "language": cell(row, "lang"),
                "print": cell(row, "print"),
                "quantity": quantity,
                "cash": cash_amount,
                "credit": credit_amount,
                "notes": cell(row, "notes"),
            }
        )

    if quantity_total <= 0:
        quantity_total = sum(int(item.get("quantity") or 0) for item in line_items)
    if total_cash is None:
        total_cash = normalize_money_value(sum(float(item.get("cash") or 0.0) for item in line_items))
    if total_credit is None:
        total_credit = normalize_money_value(sum(float(item.get("credit") or 0.0) for item in line_items))

    warnings: list[str] = []
    actual_tender = None
    actual_tender_amount = None
    if total_cash and not total_credit:
        actual_tender = "cash"
        actual_tender_amount = total_cash
    elif total_credit and not total_cash:
        actual_tender = "store_credit"
        actual_tender_amount = total_credit
    elif total_cash and total_credit:
        warnings.append("SortSwift receipt includes both cash and store credit totals; actual tender needs review.")
        actual_tender_amount = normalize_money_value(total_cash + total_credit)
    else:
        warnings.append("SortSwift receipt did not include a usable cash or credit total.")

    return SortSwiftParseResult(
        is_sortswift=True,
        vendor="SortSwift",
        receipt_type="sortswift_buylist",
        total_cash=total_cash,
        total_credit=total_credit,
        actual_tender=actual_tender,
        actual_tender_amount=actual_tender_amount,
        quantity_total=quantity_total,
        line_items=line_items,
        warnings=warnings,
    )


def _sender_vendor(sender: str) -> str:
    sender = (sender or "").strip()
    if "<" in sender:
        sender = sender.split("<", 1)[0].strip()
    return sender or "Unknown vendor"


def _generic_total(text: str) -> Optional[float]:
    for pattern in (r"(?:total|amount|paid|charged)[^\d$-]{0,24}((?:USD\s*)?\$?\s*-?\d+(?:\.\d+)?)", r"((?:USD\s*)?\$?\s*-?\d+\.\d{2})"):
        match = re.search(pattern, text or "", re.I)
        if match:
            return _money(match.group(1))
    return None


def _receipt_dedupe_hash(*, gmail_message_id: str, vendor: str, amount: Optional[float], received_at: Optional[datetime]) -> str:
    payload = "|".join(
        [
            gmail_message_id.strip(),
            vendor.strip().lower(),
            f"{float(amount or 0.0):.2f}",
            received_at.date().isoformat() if received_at else "",
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _synthetic_gmail_message(session: Session, receipt: GmailReceipt, *, body_text: str) -> DiscordMessage:
    external_id = f"gmail:{receipt.gmail_message_id}"
    row = session.exec(select(DiscordMessage).where(DiscordMessage.discord_message_id == external_id)).first()
    if row is None:
        row = DiscordMessage(
            discord_message_id=external_id,
            channel_id="gmail",
            channel_name="Gmail / SortSwift",
            author_id="gmail",
            created_at=receipt.received_at or utcnow(),
        )
    row.author_name = receipt.sender or "Gmail"
    row.content = body_text[:8000]
    row.parse_status = PARSE_REVIEW_REQUIRED
    row.needs_review = True
    row.entry_kind = "buy"
    row.money_in = 0.0
    row.money_out = receipt.total_amount or 0.0
    row.amount = receipt.total_amount
    row.payment_method = receipt.tender or None
    row.expense_category = "inventory"
    row.category = "inventory"
    row.notes = f"Gmail SortSwift receipt: {receipt.subject}"
    session.add(row)
    session.flush()
    return row


def _upsert_sortswift_transaction(session: Session, receipt: GmailReceipt, parsed: SortSwiftParseResult, *, body_text: str) -> Transaction:
    source_row = _synthetic_gmail_message(session, receipt, body_text=body_text)
    tx = session.exec(select(Transaction).where(Transaction.source_external_id == receipt.gmail_message_id)).first()
    if tx is None:
        tx = session.exec(select(Transaction).where(Transaction.source_message_id == source_row.id)).first()
    if tx is None:
        tx = Transaction(source_message_id=source_row.id or 0, occurred_at=receipt.received_at or utcnow())

    tx.source_kind = "gmail_sortswift"
    tx.source_external_id = receipt.gmail_message_id
    tx.discord_message_id = source_row.discord_message_id
    tx.channel_id = "gmail"
    tx.channel_name = "Gmail / SortSwift"
    tx.author_name = receipt.sender
    tx.occurred_at = receipt.received_at or utcnow()
    tx.parse_status = PARSE_REVIEW_REQUIRED
    tx.deal_type = "buy"
    tx.entry_kind = "buy"
    tx.payment_method = receipt.tender
    tx.cash_direction = "out"
    tx.category = "inventory"
    tx.expense_category = "inventory"
    tx.amount = receipt.total_amount
    tx.money_in = 0.0
    tx.money_out = receipt.total_amount or 0.0
    tx.needs_review = True
    tx.confidence = 0.9 if not parsed.warnings else 0.6
    warning_text = " ".join(parsed.warnings)
    tx.notes = f"Gmail SortSwift buylist receipt. {warning_text}".strip()
    tx.source_content = body_text[:8000]
    tx.updated_at = utcnow()
    session.add(tx)
    session.flush()

    session.exec(delete(TransactionItem).where(TransactionItem.transaction_id == tx.id))
    for item in parsed.line_items:
        item_name = f"{item.get('name') or ''}"
        set_name = str(item.get("set") or "").strip()
        number = str(item.get("number") or "").strip()
        suffix = " ".join(part for part in [set_name, number] if part)
        if suffix:
            item_name = f"{item_name} ({suffix})"
        session.add(TransactionItem(transaction_id=tx.id or 0, direction="in", item_name=item_name))
    receipt.transaction_id = tx.id
    receipt.status = "transaction_created"
    session.add(receipt)
    return tx


def upsert_gmail_receipt_from_message(
    session: Session,
    *,
    gmail_message_id: str,
    thread_id: Optional[str],
    sender: str,
    subject: str,
    received_at: Optional[datetime],
    html_body: str,
    snippet: str = "",
    connection_id: Optional[int] = None,
) -> GmailReceipt:
    if not gmail_message_id:
        raise ValueError("gmail_message_id is required")

    body_text = _plain_text_from_html(html_body)
    sortswift = parse_sortswift_buylist_email(f"{subject}\n{sender}\n{html_body}")
    vendor = sortswift.vendor if sortswift.is_sortswift else _sender_vendor(sender)
    receipt_type = sortswift.receipt_type if sortswift.is_sortswift else "invoice_or_receipt"
    total = sortswift.actual_tender_amount if sortswift.is_sortswift else _generic_total(body_text)
    tender = sortswift.actual_tender if sortswift.is_sortswift else None
    dedupe_hash = _receipt_dedupe_hash(
        gmail_message_id=gmail_message_id,
        vendor=vendor,
        amount=total,
        received_at=received_at,
    )

    with session.no_autoflush:
        receipt = session.exec(select(GmailReceipt).where(GmailReceipt.gmail_message_id == gmail_message_id)).first()
    if receipt is None:
        try:
            with session.begin_nested():
                receipt = GmailReceipt(gmail_message_id=gmail_message_id)
                session.add(receipt)
                session.flush()
        except IntegrityError:
            with session.no_autoflush:
                receipt = session.exec(
                    select(GmailReceipt).where(GmailReceipt.gmail_message_id == gmail_message_id)
                ).first()
            if receipt is None:
                raise

    receipt.connection_id = connection_id
    receipt.thread_id = thread_id
    receipt.sender = sender or ""
    receipt.subject = subject or ""
    receipt.received_at = received_at
    receipt.detected_vendor = vendor
    receipt.detected_type = receipt_type
    receipt.total_amount = total
    receipt.tender = tender
    receipt.confidence = "high" if sortswift.is_sortswift and total else "medium" if total else "low"
    receipt.snippet = (snippet or body_text)[:1000]
    receipt.raw_text = (html_body or body_text)[:12000]
    receipt.dedupe_hash = dedupe_hash
    receipt.parsed_json = json.dumps(
        {
            "vendor": vendor,
            "type": receipt_type,
            "total_cash": sortswift.total_cash,
            "total_credit": sortswift.total_credit,
            "actual_tender": tender,
            "quantity_total": sortswift.quantity_total,
            "warnings": sortswift.warnings,
        },
        sort_keys=True,
    )
    receipt.updated_at = utcnow()
    if receipt.status not in {"matched", "ignored", "transaction_created"}:
        receipt.status = "unmatched"
    session.add(receipt)
    session.flush()

    session.exec(delete(GmailReceiptLineItem).where(GmailReceiptLineItem.gmail_receipt_id == receipt.id))
    for idx, item in enumerate(sortswift.line_items, start=1):
        session.add(
            GmailReceiptLineItem(
                gmail_receipt_id=receipt.id or 0,
                row_index=idx,
                name=str(item.get("name") or ""),
                set_name=str(item.get("set") or "") or None,
                card_number=str(item.get("number") or "") or None,
                condition=str(item.get("condition") or "") or None,
                language=str(item.get("language") or "") or None,
                print_type=str(item.get("print") or "") or None,
                quantity=int(item.get("quantity") or 1),
                cash_amount=item.get("cash"),
                credit_amount=item.get("credit"),
                notes=str(item.get("notes") or "") or None,
                raw_json=json.dumps(item, sort_keys=True),
            )
        )

    if sortswift.is_sortswift:
        _upsert_sortswift_transaction(session, receipt, sortswift, body_text=body_text)
    return receipt


def link_gmail_evidence_to_bank_row(
    session: Session,
    gmail_receipt_id: int,
    bank_transaction_id: int,
    *,
    linked_by: Optional[str] = None,
) -> GmailEvidenceLink:
    receipt = session.get(GmailReceipt, gmail_receipt_id)
    bank_row = session.get(BankTransaction, bank_transaction_id)
    if not receipt or not bank_row:
        raise ValueError("Gmail receipt and bank row are required")
    link = session.exec(
        select(GmailEvidenceLink).where(
            GmailEvidenceLink.gmail_receipt_id == gmail_receipt_id,
            GmailEvidenceLink.bank_transaction_id == bank_transaction_id,
        )
    ).first()
    if link is None:
        link = GmailEvidenceLink(gmail_receipt_id=gmail_receipt_id, bank_transaction_id=bank_transaction_id)
    link.linked_by = linked_by
    link.linked_at = utcnow()
    link.link_status = "linked"
    link.note = f"Gmail evidence: {receipt.detected_vendor} {receipt.subject}".strip()
    receipt.status = "matched"
    note = f"Gmail evidence: {receipt.detected_vendor} {receipt.subject}".strip()
    if note not in (bank_row.review_note or ""):
        bank_row.review_note = "\n".join(part for part in [bank_row.review_note, note] if part)
    bank_row.updated_at = utcnow()
    session.add(link)
    session.add(receipt)
    session.add(bank_row)
    session.flush()
    return link


def list_gmail_connections(session: Session) -> list[GmailConnection]:
    return list(session.exec(select(GmailConnection).order_by(GmailConnection.created_at.desc())).all())


def list_gmail_receipts(session: Session, *, limit: int = 25) -> list[GmailReceipt]:
    return list(
        session.exec(
            select(GmailReceipt)
            .order_by(GmailReceipt.received_at.desc(), GmailReceipt.id.desc())
            .limit(max(1, min(limit, 200)))
        ).all()
    )


def _gmail_get(access_token: str, path: str, *, params: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    response = httpx.get(
        f"{GMAIL_API_BASE}{path}",
        params=params or {},
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=30.0,
    )
    data = response.json() if response.content else {}
    if response.status_code >= 400:
        raise GmailAPIError(str(data.get("error", {}).get("message") or response.text))
    return data


def _message_header(payload: dict[str, Any], name: str) -> str:
    headers = payload.get("headers") if isinstance(payload.get("headers"), list) else []
    for header in headers:
        if str(header.get("name") or "").lower() == name.lower():
            return str(header.get("value") or "")
    return ""


def _decode_part_body(part: dict[str, Any]) -> str:
    body = part.get("body") if isinstance(part.get("body"), dict) else {}
    data = body.get("data")
    if not data:
        return ""
    padded = str(data) + ("=" * (-len(str(data)) % 4))
    try:
        return base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8", errors="replace")
    except Exception:
        return ""


def _message_html(payload: dict[str, Any]) -> str:
    if str(payload.get("mimeType") or "").lower() == "text/html":
        return _decode_part_body(payload)
    parts = payload.get("parts") if isinstance(payload.get("parts"), list) else []
    for part in parts:
        if str(part.get("mimeType") or "").lower() == "text/html":
            return _decode_part_body(part)
    text_parts = [_decode_part_body(part) for part in parts if str(part.get("mimeType") or "").lower() == "text/plain"]
    return "\n".join(part for part in text_parts if part) or _decode_part_body(payload)


def sync_gmail_connection(session: Session, connection_id: int, *, limit: Optional[int] = None) -> dict[str, Any]:
    settings = get_settings()
    connection = session.get(GmailConnection, connection_id)
    if not connection:
        raise ValueError("Gmail connection not found")
    access_token = access_token_for_gmail_connection(session, connection)
    query = connection.sync_query or build_gmail_financial_search_query(days=settings.gmail_sync_days)
    run = GmailSyncRun(connection_id=connection.id or 0, query=query)
    session.add(run)
    session.flush()
    imported = 0
    transaction_count = 0
    scanned = 0
    try:
        search = _gmail_get(
            access_token,
            "/users/me/messages",
            params={"q": query, "maxResults": max(1, min(limit or settings.gmail_sync_limit, 100))},
        )
        messages = search.get("messages") if isinstance(search.get("messages"), list) else []
        for item in messages:
            message_id = str(item.get("id") or "")
            if not message_id:
                continue
            scanned += 1
            message = _gmail_get(access_token, f"/users/me/messages/{message_id}", params={"format": "full"})
            payload = message.get("payload") if isinstance(message.get("payload"), dict) else {}
            received_at = None
            internal_date = message.get("internalDate")
            if internal_date:
                received_at = datetime.fromtimestamp(int(internal_date) / 1000, tz=timezone.utc)
            receipt = upsert_gmail_receipt_from_message(
                session,
                gmail_message_id=message_id,
                thread_id=str(message.get("threadId") or ""),
                sender=_message_header(payload, "From"),
                subject=_message_header(payload, "Subject"),
                received_at=received_at,
                html_body=_message_html(payload),
                snippet=str(message.get("snippet") or ""),
                connection_id=connection.id,
            )
            imported += 1
            if receipt.transaction_id:
                transaction_count += 1
        connection.last_sync_at = utcnow()
        connection.last_sync_error = None
        connection.updated_at = utcnow()
        run.status = "completed"
        return {"scanned": scanned, "imported": imported, "transactions": transaction_count}
    except Exception as exc:
        connection.last_sync_error = str(exc)
        run.status = "failed"
        run.error = str(exc)
        raise
    finally:
        run.scanned_count = scanned
        run.imported_count = imported
        run.transaction_count = transaction_count
        run.finished_at = utcnow()
        session.add(connection)
        session.add(run)
        session.commit()
