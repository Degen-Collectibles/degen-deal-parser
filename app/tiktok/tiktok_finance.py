"""TikTok Shop finance sync: statements, fee breakdowns, payouts, returns.

Read-only toward TikTok. Writes only the tiktok_statements,
tiktok_statement_transactions, tiktok_payments and tiktok_returns tables.
Finance/ledger totals do not read these tables yet.

A statement is "reconciled" once its stored transactions add up to TikTok's
settlement_amount (within one cent) and the transaction count matches.
"""
from __future__ import annotations

import asyncio
import json
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

import httpx
from sqlmodel import Session, select

from ..config import get_settings
from ..models import TikTokPayment, TikTokReturn, TikTokStatement, TikTokStatementTransaction, utcnow

DEFAULT_SHOP_API_BASE_URL = "https://open-api.tiktokglobalshop.com"
STATEMENTS_PATH = "/finance/202309/statements"
PAYMENTS_PATH = "/finance/202309/payments"
RETURNS_SEARCH_PATH = "/return_refund/202309/returns/search"
CANCELLATIONS_SEARCH_PATH = "/return_refund/202309/cancellations/search"
STATEMENT_TRANSACTIONS_VERSION = "202501"
WINDOW_DAYS = 30
MAX_PAGES = 200
# Statements can still change (adjustments, late refunds) for a while after they are issued.
STATEMENT_RECHECK_DAYS = 7
RECONCILE_TOLERANCE = 0.01
RESERVE_TRANSACTION_TYPE = "RESERVE"
RETRY_ATTEMPTS = 4
# 66007001 "Rpc error", 36009003 "Internal error. Retry later."
TRANSIENT_ERROR_CODES = {"66007001", "36009003"}


class TikTokFinanceError(RuntimeError):
    pass


def _money(value: Any) -> float:
    if isinstance(value, dict):
        value = value.get("value")
    try:
        return round(float(value or 0), 2)
    except (TypeError, ValueError):
        return 0.0


def _epoch_to_utc(value: Any) -> Optional[datetime]:
    try:
        seconds = int(value or 0)
    except (TypeError, ValueError):
        return None
    if seconds <= 0:
        return None
    if seconds > 10_000_000_000:
        seconds //= 1000
    return datetime.fromtimestamp(seconds, tz=timezone.utc)


def _last4(value: Any) -> Optional[str]:
    digits = re.sub(r"\D", "", str(value or ""))
    return digits[-4:] if digits else None


def _dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


class TikTokFinanceApi:
    """Signed TikTok Shop API calls for the finance/return endpoints."""

    def __init__(
        self,
        *,
        app_key: str,
        app_secret: str,
        access_token: str,
        shop_cipher: str,
        base_url: str = DEFAULT_SHOP_API_BASE_URL,
        http: Optional[httpx.Client] = None,
        pause_seconds: float = 0.2,
        retry_backoff_seconds: float = 2.0,
    ) -> None:
        self.app_key = app_key
        self.app_secret = app_secret
        self.access_token = access_token
        self.shop_cipher = shop_cipher
        self.base_url = base_url or DEFAULT_SHOP_API_BASE_URL
        self.http = http or httpx.Client(timeout=30.0)
        self.pause_seconds = pause_seconds
        self.retry_backoff_seconds = retry_backoff_seconds

    def call(
        self,
        method: str,
        path: str,
        *,
        query: Optional[dict[str, Any]] = None,
        body: Optional[dict[str, Any]] = None,
        version: str = "202309",
    ) -> dict[str, Any]:
        from scripts.tiktok_backfill import build_tiktok_request

        url, body_json, headers = build_tiktok_request(
            base_url=self.base_url,
            path=path,
            app_key=self.app_key,
            app_secret=self.app_secret,
            shop_id="",
            shop_cipher=self.shop_cipher,
            access_token=self.access_token,
            body=body if method == "POST" else None,
            extra_query=query,
            api_version=version,
        )
        headers["Content-Type"] = "application/json"
        last_error = ""
        for attempt in range(RETRY_ATTEMPTS):
            if attempt:
                time.sleep(self.retry_backoff_seconds * attempt)
            elif self.pause_seconds:
                time.sleep(self.pause_seconds)
            response = self.http.request(method, url, headers=headers, content=body_json if method == "POST" else None)
            try:
                payload = response.json()
            except ValueError:
                payload = {}
            if response.status_code == 429 or response.status_code >= 500 or str(payload.get("code")) in TRANSIENT_ERROR_CODES:
                last_error = f"{path}: transient TikTok error ({payload.get('code') or response.status_code}) after {attempt + 1} attempts"
                continue
            if payload.get("code") not in (0, "0"):
                raise TikTokFinanceError(f"{path}: {payload.get('code')} {payload.get('message') or response.status_code}")
            return payload.get("data") or {}
        raise TikTokFinanceError(last_error)


def _paged(
    api: TikTokFinanceApi,
    method: str,
    path: str,
    items_key: str,
    *,
    query: dict[str, Any],
    body: Optional[dict[str, Any]] = None,
    version: str = "202309",
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    items: list[dict[str, Any]] = []
    first_envelope: dict[str, Any] = {}
    token = ""
    seen_tokens: set[str] = set()
    for _ in range(MAX_PAGES):
        page_query = dict(query)
        if token:
            page_query["page_token"] = token
        data = api.call(method, path, query=page_query, body=body, version=version)
        if not first_envelope:
            first_envelope = {key: value for key, value in data.items() if key != items_key}
        page = list(data.get(items_key) or [])
        items.extend(page)
        token = str(data.get("next_page_token") or "")
        if not page or not token or token in seen_tokens:
            return items, first_envelope
        seen_tokens.add(token)
    raise TikTokFinanceError(f"{path}: more than {MAX_PAGES} pages")


def _windows(start: datetime, end: datetime) -> list[tuple[int, int]]:
    windows = []
    cursor = start
    while cursor < end:
        window_end = min(cursor + timedelta(days=WINDOW_DAYS), end)
        windows.append((int(cursor.timestamp()), int(window_end.timestamp())))
        cursor = window_end
    return windows


def fetch_statements(api: TikTokFinanceApi, start: datetime, end: datetime) -> list[dict[str, Any]]:
    statements: list[dict[str, Any]] = []
    for lo, hi in _windows(start, end):
        page, _ = _paged(api, "GET", STATEMENTS_PATH, "statements", query={
            "page_size": 100, "sort_field": "statement_time", "statement_time_ge": lo, "statement_time_lt": hi,
        })
        statements.extend(page)
    return statements


def fetch_statement_transactions(api: TikTokFinanceApi, statement_id: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    items, envelope = _paged(
        api,
        "GET",
        f"/finance/{STATEMENT_TRANSACTIONS_VERSION}/statements/{statement_id}/statement_transactions",
        "transactions",
        query={"page_size": 50, "sort_field": "order_create_time"},
        version=STATEMENT_TRANSACTIONS_VERSION,
    )
    return envelope, items


def fetch_payments(api: TikTokFinanceApi, start: datetime, end: datetime) -> list[dict[str, Any]]:
    payments: list[dict[str, Any]] = []
    for lo, hi in _windows(start, end):
        page, _ = _paged(api, "GET", PAYMENTS_PATH, "payments", query={
            "page_size": 100, "sort_field": "create_time", "create_time_ge": lo, "create_time_lt": hi,
        })
        payments.extend(page)
    return payments


def fetch_returns(api: TikTokFinanceApi, start: datetime, end: datetime) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for lo, hi in _windows(start, end):
        page, _ = _paged(api, "POST", RETURNS_SEARCH_PATH, "return_orders", query={"page_size": 50},
                         body={"update_time_ge": lo, "update_time_lt": hi})
        items.extend(page)
    return items


def fetch_cancellations(api: TikTokFinanceApi, start: datetime, end: datetime) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for lo, hi in _windows(start, end):
        page, _ = _paged(api, "POST", CANCELLATIONS_SEARCH_PATH, "cancellations", query={"page_size": 50},
                         body={"update_time_ge": lo, "update_time_lt": hi})
        items.extend(page)
    return items


def _upsert(session: Session, model: type, key_field: str, key: str, values: dict[str, Any]):
    row = session.exec(select(model).where(getattr(model, key_field) == key)).first()
    if row is None:
        row = model(**{key_field: key}, **values)
    else:
        for field, value in values.items():
            setattr(row, field, value)
    session.add(row)
    return row


def upsert_statement(session: Session, payload: dict[str, Any]) -> TikTokStatement:
    statement_id = str(payload["id"])
    existing = session.exec(select(TikTokStatement).where(TikTokStatement.statement_id == statement_id)).first()
    if existing is not None and abs(existing.settlement_amount - _money(payload.get("settlement_amount"))) > RECONCILE_TOLERANCE:
        # TikTok changed the statement total; re-pull its transactions.
        existing.reconciled = False
    return _upsert(session, TikTokStatement, "statement_id", statement_id, {
        "statement_time": _epoch_to_utc(payload.get("statement_time")) or utcnow(),
        "currency": str(payload.get("currency") or "USD"),
        "revenue_amount": _money(payload.get("revenue_amount")),
        "fee_amount": _money(payload.get("fee_amount")),
        "shipping_cost_amount": _money(payload.get("shipping_cost_amount")),
        "adjustment_amount": _money(payload.get("adjustment_amount")),
        "net_sales_amount": _money(payload.get("net_sales_amount")),
        "settlement_amount": _money(payload.get("settlement_amount")),
        "payment_id": str(payload.get("payment_id") or "") or None,
        "payment_status": str(payload.get("payment_status") or ""),
        "payment_time": _epoch_to_utc(payload.get("payment_time")),
        "raw_json": _dumps(payload),
        "synced_at": utcnow(),
    })


def upsert_statement_transactions(
    session: Session,
    statement: TikTokStatement,
    envelope: dict[str, Any],
    items: list[dict[str, Any]],
) -> TikTokStatement:
    settlement_sum = 0.0
    for item in items:
        settlement = _money(item.get("settlement_amount"))
        # RESERVE lines hold back or release money in the payout; they are not
        # part of the statement's settlement_amount (payable = settlement + reserve).
        if str(item.get("type") or "").upper() != RESERVE_TRANSACTION_TYPE:
            settlement_sum += settlement
        _upsert(session, TikTokStatementTransaction, "transaction_id", str(item["id"]), {
            "statement_id": statement.statement_id,
            "transaction_type": str(item.get("type") or ""),
            "order_id": str(item.get("order_id") or "") or None,
            "order_create_time": _epoch_to_utc(item.get("order_create_time")),
            "currency": str(envelope.get("currency") or statement.currency or "USD"),
            "revenue_amount": _money(item.get("revenue_amount")),
            "fee_tax_amount": _money(item.get("fee_tax_amount")),
            "shipping_cost_amount": _money(item.get("shipping_cost_amount")),
            "adjustment_amount": _money(item.get("adjustment_amount")),
            "settlement_amount": settlement,
            "fee_breakdown_json": _dumps(item.get("fee_tax_breakdown") or {}),
            "raw_json": _dumps(item),
            "synced_at": utcnow(),
        })
    total_count = int(envelope.get("total_count") or len(items))
    statement.transaction_count = len(items)
    statement.transaction_settlement_sum = round(settlement_sum, 2)
    statement.reconciled = (
        len(items) == total_count
        and abs(statement.transaction_settlement_sum - statement.settlement_amount) <= RECONCILE_TOLERANCE
    )
    session.add(statement)
    return statement


def upsert_payment(session: Session, payload: dict[str, Any]) -> TikTokPayment:
    masked = dict(payload)
    last4 = _last4(payload.get("bank_account"))
    masked["bank_account"] = f"****{last4}" if last4 else None
    return _upsert(session, TikTokPayment, "payment_id", str(payload["id"]), {
        "create_time": _epoch_to_utc(payload.get("create_time")) or utcnow(),
        "paid_time": _epoch_to_utc(payload.get("paid_time")),
        "status": str(payload.get("status") or ""),
        "currency": str((payload.get("amount") or {}).get("currency") or "USD"),
        "amount": _money(payload.get("amount")),
        "settlement_amount": _money(payload.get("settlement_amount")),
        "reserve_amount": _money(payload.get("reserve_amount")),
        "bank_account_last4": last4,
        "raw_json": _dumps(masked),
        "synced_at": utcnow(),
    })


def upsert_return(session: Session, kind: str, payload: dict[str, Any]) -> TikTokReturn:
    prefix = "return" if kind == "return" else "cancel"
    external_id = str(payload.get(f"{prefix}_id") or "")
    refund = payload.get("refund_amount") or {}
    line_items = payload.get("return_line_items") if kind == "return" else payload.get("cancel_line_items")
    return _upsert(session, TikTokReturn, "record_key", f"{kind}:{external_id}", {
        "kind": kind,
        "external_id": external_id,
        "order_id": str(payload.get("order_id") or "") or None,
        "status": str(payload.get(f"{prefix}_status") or ""),
        "request_type": str(payload.get(f"{prefix}_type") or ""),
        "reason": str(payload.get(f"{prefix}_reason") or ""),
        "reason_text": str(payload.get(f"{prefix}_reason_text") or ""),
        "initiated_by": str(payload.get("role") or ""),
        "currency": str(refund.get("currency") or "USD"),
        "refund_total": _money(refund.get("refund_total")),
        "create_time": _epoch_to_utc(payload.get("create_time")),
        "update_time": _epoch_to_utc(payload.get("update_time")),
        "line_items_json": _dumps(line_items or []),
        "raw_json": _dumps(payload),
        "synced_at": utcnow(),
    })


def _needs_transactions(statement: TikTokStatement, now: datetime) -> bool:
    statement_time = statement.statement_time
    if statement_time.tzinfo is None:
        statement_time = statement_time.replace(tzinfo=timezone.utc)
    return (
        not statement.reconciled
        or statement.payment_status.upper() != "PAID"
        or now - statement_time <= timedelta(days=STATEMENT_RECHECK_DAYS)
    )


def sync_tiktok_finance(
    session: Session,
    api: TikTokFinanceApi,
    *,
    start: datetime,
    end: datetime,
    now: Optional[datetime] = None,
    commit_each: Callable[[], None] | None = None,
) -> dict[str, Any]:
    """Pull statements (+ transactions), payments and returns for [start, end)."""
    now = now or datetime.now(timezone.utc)
    summary: dict[str, Any] = {
        "statements": 0, "transactions_fetched_for": 0, "reconciled": 0, "unreconciled": [],
        "payments": 0, "returns": 0, "cancellations": 0, "errors": [],
    }
    for payload in fetch_statements(api, start, end):
        statement = upsert_statement(session, payload)
        summary["statements"] += 1
        if _needs_transactions(statement, now):
            try:
                envelope, items = fetch_statement_transactions(api, statement.statement_id)
            except TikTokFinanceError as exc:
                # Leave it unreconciled so the next sync retries it.
                statement.reconciled = False
                session.add(statement)
                summary["errors"].append(f"{statement.statement_id}: {exc}"[:300])
            else:
                upsert_statement_transactions(session, statement, envelope, items)
                summary["transactions_fetched_for"] += 1
        if statement.reconciled:
            summary["reconciled"] += 1
        else:
            summary["unreconciled"].append(statement.statement_id)
        if commit_each:
            commit_each()
    for payload in fetch_payments(api, start, end):
        upsert_payment(session, payload)
        summary["payments"] += 1
    for payload in fetch_returns(api, start, end):
        upsert_return(session, "return", payload)
        summary["returns"] += 1
    for payload in fetch_cancellations(api, start, end):
        upsert_return(session, "cancellation", payload)
        summary["cancellations"] += 1
    return summary


def build_finance_api(session: Session) -> Optional[TikTokFinanceApi]:
    from ..shared import _resolve_tiktok_pull_credentials, get_latest_tiktok_auth_row

    settings = get_settings()
    _shop_id, shop_cipher, access_token = _resolve_tiktok_pull_credentials(get_latest_tiktok_auth_row(session))
    app_key = (settings.tiktok_app_key or "").strip()
    app_secret = (settings.tiktok_app_secret or "").strip()
    if not (shop_cipher and access_token and app_key and app_secret):
        return None
    return TikTokFinanceApi(
        app_key=app_key,
        app_secret=app_secret,
        access_token=access_token,
        shop_cipher=shop_cipher,
        base_url=(settings.tiktok_shop_api_base_url or "").strip() or DEFAULT_SHOP_API_BASE_URL,
    )


def run_tiktok_finance_sync_once() -> dict[str, Any]:
    from ..db import managed_session

    settings = get_settings()
    now = datetime.now(timezone.utc)
    with managed_session() as session:
        api = build_finance_api(session)
        if api is None:
            return {"status": "skipped", "reason": "TikTok credentials incomplete"}
        summary = sync_tiktok_finance(
            session,
            api,
            start=now - timedelta(days=max(int(settings.tiktok_finance_sync_lookback_days), 1)),
            end=now + timedelta(days=1),
            now=now,
        )
        session.commit()
    return {"status": "ok", **summary}


async def tiktok_finance_sync_loop(stop_event: asyncio.Event) -> None:
    from ..runtime_logging import structured_log_line

    settings = get_settings()
    interval_seconds = max(float(settings.tiktok_finance_sync_interval_minutes), 5.0) * 60
    while not stop_event.is_set():
        try:
            result = await asyncio.to_thread(run_tiktok_finance_sync_once)
            print(structured_log_line(
                runtime=f"{settings.runtime_name}_tiktok_finance",
                action="tiktok.finance.sync",
                success=result.get("status") == "ok",
                status=result.get("status"),
                statements=result.get("statements"),
                unreconciled=len(result.get("unreconciled") or []),
                payments=result.get("payments"),
                returns=result.get("returns"),
                cancellations=result.get("cancellations"),
            ))
        except Exception as exc:
            print(structured_log_line(
                runtime=f"{settings.runtime_name}_tiktok_finance",
                action="tiktok.finance.sync_failed",
                success=False,
                error=str(exc)[:400],
            ))
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
        except asyncio.TimeoutError:
            continue
