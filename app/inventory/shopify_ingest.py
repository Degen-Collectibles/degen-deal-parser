from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from threading import Lock
from typing import Any, Optional

import httpx
from sqlalchemy.exc import IntegrityError, OperationalError, ProgrammingError
from sqlmodel import Session, select

from ..models import ShopifyOrder, InventoryItem, InventoryStockMovement, INVENTORY_SOLD, utcnow
from ..runtime_logging import structured_log_line
from ..shopify_api import SHOPIFY_API_VERSION
from ..shopify_sync import (
    SHOPIFY_SYNC_ISSUE_INVALID_ORDER_IDENTITY,
    SHOPIFY_SYNC_ISSUE_INVALID_ORDER_QUANTITY,
    SHOPIFY_SYNC_ISSUE_ORDER_QUANTITY_CHANGED,
    SHOPIFY_SYNC_ISSUE_UNKNOWN_SKU,
    enqueue_shopify_sync_job,
    record_shopify_sync_issue,
)

SHOPIFY_ORDERS_PATH = f"/admin/api/{SHOPIFY_API_VERSION}/orders.json"
SHOPIFY_PROGRESS_INTERVAL = 50
_backfill_state_lock = Lock()
_backfill_state = {
    "is_running": False,
    "last_started_at": None,
    "last_finished_at": None,
    "last_since": None,
    "last_limit": None,
    "last_summary": None,
    "last_error": None,
}


@dataclass
class ShopifyBackfillSummary:
    fetched: int = 0
    inserted: int = 0
    updated: int = 0
    failed: int = 0


def read_shopify_backfill_state() -> dict[str, Any]:
    with _backfill_state_lock:
        return dict(_backfill_state)


def update_shopify_backfill_state(**changes: Any) -> dict[str, Any]:
    with _backfill_state_lock:
        _backfill_state.update(changes)
        return dict(_backfill_state)


def parse_shopify_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").strip()
        if not text:
            return utcnow()
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def money_to_float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    try:
        return round(float(Decimal(str(value))), 2)
    except (InvalidOperation, ValueError, TypeError):
        return 0.0


def json_dumps(value: Any) -> str:
    return json.dumps(value, default=str, sort_keys=True, separators=(",", ":"))


def normalize_shopify_order_identity(payload: dict[str, Any]) -> str:
    raw_identity = payload.get("id")
    if isinstance(raw_identity, bool) or raw_identity is None:
        return ""
    if isinstance(raw_identity, str):
        normalized = raw_identity.strip()
        if not normalized:
            return ""
        try:
            numeric_identity = Decimal(normalized)
        except InvalidOperation:
            return normalized
        if (
            not numeric_identity.is_finite()
            or numeric_identity <= 0
            or numeric_identity != numeric_identity.to_integral_value()
        ):
            return ""
        return normalized
    if isinstance(raw_identity, int):
        return str(raw_identity) if raw_identity > 0 else ""
    if isinstance(raw_identity, (float, Decimal)):
        numeric_identity = Decimal(str(raw_identity))
        if (
            not numeric_identity.is_finite()
            or numeric_identity <= 0
            or numeric_identity != numeric_identity.to_integral_value()
        ):
            return ""
        return str(int(numeric_identity))
    return ""


def _normalize_shopify_line_quantity(raw_quantity: Any) -> Optional[int]:
    if isinstance(raw_quantity, bool) or raw_quantity is None:
        return None
    if isinstance(raw_quantity, int):
        return raw_quantity if raw_quantity >= 0 else None
    if isinstance(raw_quantity, str):
        normalized = raw_quantity.strip()
        if not normalized:
            return None
        try:
            numeric_quantity = Decimal(normalized)
        except InvalidOperation:
            return None
    elif isinstance(raw_quantity, (float, Decimal)):
        numeric_quantity = Decimal(str(raw_quantity))
    else:
        return None
    if (
        not numeric_quantity.is_finite()
        or numeric_quantity < 0
        or numeric_quantity != numeric_quantity.to_integral_value()
    ):
        return None
    return int(numeric_quantity)


def _safe_json_list(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if not value:
        return []
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError:
        return []
    if not isinstance(loaded, list):
        return []
    return [item for item in loaded if isinstance(item, dict)]


def normalize_shopify_line_items(line_items: Any) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for item in _safe_json_list(line_items):
        title = str(item.get("title") or "").strip()
        if not title:
            continue

        quantity = (
            _normalize_shopify_line_quantity(item.get("quantity"))
            if "quantity" in item
            else None
        )

        normalized.append(
            {
                "title": title,
                "quantity": quantity if quantity is not None else 0,
                "sku": str(item.get("sku") or "").strip() or None,
                "product_id": str(item.get("product_id") or "").strip() or None,
                "variant_id": str(item.get("variant_id") or "").strip() or None,
                "unit_price": money_to_float(item.get("price")),
            }
        )
    return normalized


def build_shopify_reconciliation_snapshot(record: dict[str, Any]) -> dict[str, Any]:
    normalized_line_items = normalize_shopify_line_items(record.get("line_items_json"))
    return {
        "shopify_order_id": record.get("shopify_order_id"),
        "order_number": record.get("order_number"),
        "created_at": record.get("created_at"),
        "updated_at": record.get("updated_at"),
        "customer_name": record.get("customer_name"),
        "customer_email": record.get("customer_email"),
        "financial_status": record.get("financial_status"),
        "total_price": record.get("total_price"),
        "subtotal_ex_tax": record.get("subtotal_ex_tax"),
        "line_item_count": len(normalized_line_items),
        "line_items": normalized_line_items,
    }


def normalize_customer_name(payload: dict[str, Any]) -> Optional[str]:
    customer = payload.get("customer") or {}
    billing_address = payload.get("billing_address") or {}
    shipping_address = payload.get("shipping_address") or {}

    first_name = (
        customer.get("first_name")
        or billing_address.get("first_name")
        or shipping_address.get("first_name")
        or ""
    ).strip()
    last_name = (
        customer.get("last_name")
        or billing_address.get("last_name")
        or shipping_address.get("last_name")
        or ""
    ).strip()
    full_name = " ".join(part for part in (first_name, last_name) if part).strip()
    return full_name or None


def normalize_customer_email(payload: dict[str, Any]) -> Optional[str]:
    customer = payload.get("customer") or {}
    return (
        customer.get("email")
        or payload.get("email")
        or (payload.get("contact_email") or "")
    ) or None


def normalize_order_number(payload: dict[str, Any]) -> str:
    name = str(payload.get("name") or "").strip()
    if name:
        return name
    number = payload.get("order_number")
    if number not in (None, ""):
        return f"#{number}"
    return str(payload.get("id") or "")


def extract_order_tax_fields(payload: dict[str, Any]) -> tuple[Optional[float], Optional[float], bool]:
    total_price = money_to_float(payload.get("total_price"))
    raw_total_tax = payload.get("total_tax")
    if raw_total_tax not in (None, ""):
        total_tax = money_to_float(raw_total_tax)
        return total_tax, round(total_price - total_tax, 2), False

    tax_lines = payload.get("tax_lines") or []
    if isinstance(tax_lines, list) and tax_lines:
        tax_total = round(
            sum(money_to_float(line.get("price")) for line in tax_lines if isinstance(line, dict)),
            2,
        )
        return tax_total, round(total_price - tax_total, 2), False

    return None, None, True


def validate_shopify_webhook(*, raw_body: bytes, shared_secret: str, received_hmac: Optional[str]) -> bool:
    if not shared_secret or not received_hmac:
        return False
    digest = hmac.new(shared_secret.encode("utf-8"), raw_body, hashlib.sha256).digest()
    encoded_digest = base64.b64encode(digest).decode("utf-8")
    return hmac.compare_digest(encoded_digest, received_hmac.strip())


def order_record_from_payload(
    payload: dict[str, Any],
    *,
    source: str,
    received_at: Optional[datetime] = None,
    runtime_name: str = "shopify_ingest",
) -> dict[str, Any]:
    order_id = normalize_shopify_order_identity(payload)
    if not order_id:
        raise ValueError("Shopify payload has no usable immutable id")

    created_at = parse_shopify_datetime(payload.get("created_at"))
    updated_at = parse_shopify_datetime(payload.get("updated_at") or payload.get("created_at"))
    total_tax, subtotal_ex_tax, missing_tax = extract_order_tax_fields(payload)
    if missing_tax:
        print(
            structured_log_line(
                runtime=runtime_name,
                action="shopify.tax_fields.missing",
                success=False,
                error="No tax field available on Shopify order payload",
                shopify_order_id=payload.get("id"),
                order_number=payload.get("name") or payload.get("order_number"),
            )
        )
    return {
        "shopify_order_id": order_id,
        "order_number": normalize_order_number(payload),
        "created_at": created_at,
        "updated_at": updated_at,
        "customer_name": normalize_customer_name(payload),
        "customer_email": normalize_customer_email(payload),
        "total_price": money_to_float(payload.get("total_price")),
        "subtotal_price": money_to_float(payload.get("subtotal_price")),
        "total_tax": total_tax,
        "subtotal_ex_tax": subtotal_ex_tax,
        "financial_status": str(payload.get("financial_status") or "").strip(),
        "fulfillment_status": str(payload.get("fulfillment_status") or "").strip() or None,
        "line_items_json": json_dumps(payload.get("line_items") or []),
        "line_items_summary_json": json_dumps(
            normalize_shopify_line_items(payload.get("line_items") or [])
        ),
        "raw_payload": json_dumps(payload),
        "source": source,
        "received_at": received_at or utcnow(),
    }


def upsert_shopify_order(
    session: Session,
    payload: dict[str, Any],
    *,
    source: str,
    received_at: Optional[datetime] = None,
    dry_run: bool = False,
    runtime_name: str = "shopify_ingest",
) -> str:
    record = order_record_from_payload(
        payload,
        source=source,
        received_at=received_at,
        runtime_name=runtime_name,
    )
    existing = session.exec(
        select(ShopifyOrder).where(ShopifyOrder.shopify_order_id == record["shopify_order_id"])
    ).first()

    if existing is None:
        if not dry_run:
            session.add(ShopifyOrder(**record))
        return "inserted"

    for field_name, value in record.items():
        setattr(existing, field_name, value)
    if not dry_run:
        session.add(existing)
    return "updated"


def build_shopify_orders_url(store_domain: str) -> str:
    normalized = (store_domain or "").strip().rstrip("/")
    if normalized.startswith("https://"):
        return f"{normalized}{SHOPIFY_ORDERS_PATH}"
    if normalized.startswith("http://"):
        normalized = normalized.replace("http://", "https://", 1)
        return f"{normalized}{SHOPIFY_ORDERS_PATH}"
    return f"https://{normalized}{SHOPIFY_ORDERS_PATH}"


def parse_shopify_link_header(link_header: str | None) -> dict[str, str]:
    links: dict[str, str] = {}
    if not link_header:
        return links

    for raw_part in link_header.split(","):
        part = raw_part.strip()
        if not part.startswith("<") or ">;" not in part:
            continue
        url_part, _, meta_part = part.partition(">;")
        url = url_part[1:]
        rel = ""
        for item in meta_part.split(";"):
            key, _, value = item.strip().partition("=")
            if key == "rel":
                rel = value.strip().strip('"')
                break
        if rel:
            links[rel] = url
    return links


def extract_next_page_info(link_header: str | None) -> Optional[str]:
    links = parse_shopify_link_header(link_header)
    next_url = links.get("next")
    if not next_url or "page_info=" not in next_url:
        return None
    _, _, tail = next_url.partition("page_info=")
    return tail.split("&", 1)[0]


def fetch_shopify_orders_page(
    client: httpx.Client,
    *,
    store_domain: str,
    api_key: str,
    since: Optional[str] = None,
    page_info: Optional[str] = None,
    limit: int = 250,
) -> tuple[list[dict[str, Any]], Optional[str]]:
    headers = {
        "X-Shopify-Access-Token": api_key,
        "Accept": "application/json",
    }
    params: dict[str, Any] = {
        "status": "any",
        "limit": max(1, min(limit, 250)),
    }
    if page_info:
        params = {
            "limit": max(1, min(limit, 250)),
            "page_info": page_info,
        }
    elif since:
        params["created_at_min"] = since

    url = build_shopify_orders_url(store_domain)
    max_attempts = 3
    backoff = 0.5
    response: httpx.Response | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = client.get(url, headers=headers, params=params)
            if response.status_code == 429 or response.status_code >= 500:
                wait_s = backoff
                ra = response.headers.get("Retry-After")
                if ra:
                    try:
                        wait_s = max(wait_s, float(ra))
                    except ValueError:
                        pass
                if attempt < max_attempts:
                    time.sleep(wait_s)
                    backoff *= 2
                    continue
            response.raise_for_status()
            break
        except (httpx.TimeoutException, httpx.TransportError):
            if attempt >= max_attempts:
                raise
            time.sleep(backoff)
            backoff *= 2
    if response is None:
        raise RuntimeError("Shopify fetch failed without response")
    payload = response.json()
    orders = payload.get("orders") or []
    if not isinstance(orders, list):
        raise ValueError("Shopify API response did not include an orders list")
    next_page_info = extract_next_page_info(response.headers.get("Link"))
    return orders, next_page_info


def backfill_shopify_orders(
    session: Session,
    *,
    store_domain: str,
    api_key: str,
    since: Optional[str] = None,
    limit: Optional[int] = None,
    dry_run: bool = False,
    runtime_name: str = "shopify_backfill",
) -> ShopifyBackfillSummary:
    summary = ShopifyBackfillSummary()
    if limit == 0:
        return summary
    page_info: Optional[str] = None
    remaining = limit if limit and limit > 0 else None

    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        while True:
            page_limit = min(250, remaining) if remaining else 250
            orders, page_info = fetch_shopify_orders_page(
                client,
                store_domain=store_domain,
                api_key=api_key,
                since=since,
                page_info=page_info,
                limit=page_limit,
            )
            if not orders:
                break

            for payload in orders:
                if remaining is not None and remaining <= 0:
                    break
                summary.fetched += 1
                try:
                    result = upsert_shopify_order(
                        session,
                        payload,
                        source="backfill",
                        received_at=utcnow(),
                        dry_run=dry_run,
                        runtime_name=runtime_name,
                    )
                    if result == "inserted":
                        summary.inserted += 1
                    else:
                        summary.updated += 1
                    if not dry_run:
                        session.commit()
                    elif session.in_transaction():
                        session.rollback()
                except Exception as exc:
                    summary.failed += 1
                    if session.in_transaction():
                        session.rollback()
                    print(
                        structured_log_line(
                            runtime=runtime_name,
                            action="shopify.backfill.order_failed",
                            success=False,
                            error=str(exc),
                            shopify_order_id=payload.get("id"),
                            order_number=payload.get("name") or payload.get("order_number"),
                        )
                    )

                if summary.fetched % SHOPIFY_PROGRESS_INTERVAL == 0:
                    print(
                        structured_log_line(
                            runtime=runtime_name,
                            action="shopify.backfill.progress",
                            success=True,
                            fetched=summary.fetched,
                            inserted=summary.inserted,
                            updated=summary.updated,
                            failed=summary.failed,
                            dry_run=dry_run,
                        )
                    )
                if remaining is not None:
                    remaining -= 1

            if remaining is not None and remaining <= 0:
                break
            if not page_info:
                break

    return summary


def repair_shopify_tax_fields(session: Session) -> int:
    rows = session.exec(
        select(ShopifyOrder).where(
            (ShopifyOrder.total_tax == None) | (ShopifyOrder.subtotal_ex_tax == None)
        )
    ).all()
    updated = 0
    for row in rows:
        try:
            payload = json.loads(row.raw_payload or "{}")
        except json.JSONDecodeError:
            continue
        total_tax, subtotal_ex_tax, missing_tax = extract_order_tax_fields(payload)
        if missing_tax:
            continue
        row.total_tax = total_tax
        row.subtotal_ex_tax = subtotal_ex_tax
        session.add(row)
        updated += 1
    if updated:
        session.commit()
    return updated


def repair_shopify_line_item_summaries(session: Session) -> int:
    rows = session.exec(
        select(ShopifyOrder).where(ShopifyOrder.line_items_summary_json == "[]")
    ).all()
    updated = 0
    for row in rows:
        try:
            payload = json.loads(row.raw_payload or "{}")
        except json.JSONDecodeError:
            continue
        normalized = normalize_shopify_line_items(payload.get("line_items") or row.line_items_json)
        if not normalized:
            continue
        row.line_items_summary_json = json_dumps(normalized)
        session.add(row)
        updated += 1
    if updated:
        session.commit()
    return updated


_STOCK_MOVEMENT_DEDUPE_INDEX = "idx_inventory_stock_movements_dedupe_key"
_SQLITE_STOCK_MOVEMENT_DEDUPE_ERROR = (
    "UNIQUE constraint failed: inventory_stock_movements.dedupe_key"
)


def _is_stock_movement_dedupe_conflict(exc: IntegrityError) -> bool:
    original = exc.orig
    diagnostic = getattr(original, "diag", None)
    constraint_name = getattr(diagnostic, "constraint_name", None) or getattr(
        original, "constraint_name", None
    )
    if constraint_name == _STOCK_MOVEMENT_DEDUPE_INDEX:
        return True
    normalized_original = " ".join(str(original).strip().split())
    return normalized_original == _SQLITE_STOCK_MOVEMENT_DEDUPE_ERROR


def _log_stock_movement_schema_unavailable(
    exc: OperationalError | ProgrammingError,
    *,
    runtime_name: str,
    order_id: str,
    sku: str,
    required_column: str,
) -> None:
    print(
        structured_log_line(
            runtime=runtime_name,
            action="inventory.shopify_order.schema_unavailable",
            success=False,
            error=str(exc),
            shopify_order_id=order_id,
            barcode=sku,
            required_column=required_column,
        )
    )


def _missing_stock_movement_schema_column(
    exc: OperationalError | ProgrammingError,
) -> Optional[str]:
    original = exc.orig
    diagnostic = getattr(original, "diag", None)
    sqlstate = (
        getattr(original, "sqlstate", None)
        or getattr(original, "pgcode", None)
        or getattr(diagnostic, "sqlstate", None)
    )
    message = str(original).strip().lower()
    is_missing_column = sqlstate == "42703" or "no such column" in message
    if not is_missing_column:
        return None
    if "requested_quantity" in message:
        return "inventory_stock_movements.requested_quantity"
    if "dedupe_key" in message:
        return "inventory_stock_movements.dedupe_key"
    return None


def _find_stock_movement_by_dedupe_key(
    session: Session,
    dedupe_key: str,
    *,
    runtime_name: str,
    order_id: str,
    sku: str,
) -> Optional[InventoryStockMovement]:
    try:
        return session.exec(
            select(InventoryStockMovement).where(
                InventoryStockMovement.dedupe_key == dedupe_key
            )
        ).first()
    except (OperationalError, ProgrammingError) as exc:
        required_column = _missing_stock_movement_schema_column(exc)
        if required_column is not None:
            _log_stock_movement_schema_unavailable(
                exc,
                runtime_name=runtime_name,
                order_id=order_id,
                sku=sku,
                required_column=required_column,
            )
        raise


def _begin_stock_mutation_transaction(session: Session) -> bool:
    """Acquire SQLite's write lock and report whether this function owns the transaction."""
    session_transaction_preexisting = session.in_transaction()
    bind = session.get_bind()
    bind_in_transaction = getattr(bind, "in_transaction", None)
    external_transaction_preexisting = bool(
        bind_in_transaction() if callable(bind_in_transaction) else False
    )
    connection = session.connection()
    if connection.dialect.name == "sqlite":
        pooled_connection = connection.connection
        driver_connection = getattr(pooled_connection, "dbapi_connection", None)
        if driver_connection is not None and not driver_connection.in_transaction:
            connection.exec_driver_sql("BEGIN IMMEDIATE")
    return not session_transaction_preexisting and not external_transaction_preexisting


def mark_inventory_sold_from_shopify_order(
    session: Session,
    payload: dict[str, Any],
    *,
    runtime_name: str = "shopify_ingest",
) -> int:
    """
    Check line items in a Shopify order payload for SKUs matching inventory barcodes
    (format: DGN-XXXXXX). For each match, mark the InventoryItem as sold.

    Returns the count of items marked sold.
    """
    financial_status = str(payload.get("financial_status") or "").strip().lower()
    order_number = str(payload.get("name") or payload.get("order_number") or "").strip()
    order_identity = normalize_shopify_order_identity(payload)
    if not order_identity:
        payload_fingerprint = hashlib.sha256(json_dumps(payload).encode("utf-8")).hexdigest()
        issue = record_shopify_sync_issue(
            session,
            issue_type=SHOPIFY_SYNC_ISSUE_INVALID_ORDER_IDENTITY,
            shopify_order_number=order_number or None,
            message=(
                "Shopify order payload did not include a usable immutable order id; "
                "local inventory was not changed."
            ),
            payload=payload,
            severity="error",
            issue_fingerprint=payload_fingerprint,
        )
        session.commit()
        print(
            structured_log_line(
                runtime=runtime_name,
                action="inventory.shopify_order.invalid_identity",
                success=False,
                error="Shopify order payload is missing usable immutable id.",
                financial_status=financial_status,
                shopify_order_number=order_number,
                issue_id=issue.id,
                payload_fingerprint=payload_fingerprint,
            )
        )
        return 0

    if financial_status != "paid":
        print(
            structured_log_line(
                runtime=runtime_name,
                action="inventory.shopify_order.stock_mutation_deferred",
                success=True,
                financial_status=financial_status,
                reason="Shopify order is not paid; local inventory was not changed.",
                shopify_order_id=payload.get("id"),
                shopify_order_number=payload.get("name") or payload.get("order_number"),
            )
        )
        return 0

    line_items = payload.get("line_items") or []
    if not isinstance(line_items, list):
        return 0

    sold_by_sku: dict[str, dict[str, Any]] = {}
    for item in line_items:
        if not isinstance(item, dict):
            continue
        sku = str(item.get("sku") or "").strip()
        if not sku:
            continue
        quantity_field = (
            "current_quantity" if "current_quantity" in item else "quantity"
        )
        quantity_present = quantity_field in item
        quantity_raw = item.get(quantity_field) if quantity_present else None
        normalized_quantity = (
            _normalize_shopify_line_quantity(quantity_raw)
            if quantity_present
            else None
        )
        quantity_valid = normalized_quantity is not None
        quantity = normalized_quantity if normalized_quantity is not None else 0
        row = sold_by_sku.setdefault(
            sku,
            {
                "quantity": 0,
                "price": 0.0,
                "title": str(item.get("title") or item.get("name") or ""),
                "variant_id": str(item.get("variant_id") or ""),
                "payloads": [],
                "invalid_quantity_values": [],
            },
        )
        row["quantity"] = int(row["quantity"]) + quantity
        row["price"] = money_to_float(item.get("price"))
        row["payloads"].append(item)
        if not quantity_valid:
            row["invalid_quantity_values"].append(
                {
                    "field": quantity_field,
                    "present": quantity_present,
                    "value": quantity_raw,
                }
            )

    if not sold_by_sku:
        return 0

    marked = 0
    issues_recorded = 0
    movement_claims_recorded = 0
    prepared_sales: list[dict[str, Any]] = []

    def movement_quantities(
        movement: InventoryStockMovement,
    ) -> tuple[int, Optional[int], int]:
        applied_quantity = max(
            0,
            int(movement.quantity_before or 0) - int(movement.quantity_after or 0),
        )
        requested_quantity = movement.requested_quantity
        comparison_quantity = (
            max(0, int(requested_quantity))
            if requested_quantity is not None
            else applied_quantity
        )
        return applied_quantity, requested_quantity, comparison_quantity

    def log_replay(
        *,
        sku: str,
        inventory_item_id: int,
        applied_quantity: int,
        requested_quantity: Optional[int],
    ) -> None:
        print(
            structured_log_line(
                runtime=runtime_name,
                action="inventory.shopify_order.replay_skipped",
                success=True,
                shopify_order_id=order_identity,
                inventory_item_id=inventory_item_id,
                barcode=sku,
                applied_quantity=applied_quantity,
                requested_quantity=requested_quantity,
            )
        )

    def record_quantity_change(
        *,
        sku: str,
        sale: dict[str, Any],
        sale_price: float,
        sold_quantity: int,
        inventory_item_id: int,
        card_name: str,
        dedupe_key: str,
        applied_quantity: int,
        requested_quantity: Optional[int],
        comparison_quantity: int,
    ) -> None:
        nonlocal issues_recorded
        message = (
            f"Shopify order {order_identity} SKU {sku} previously requested quantity "
            f"{comparison_quantity}; applied quantity {applied_quantity} locally; "
            f"current paid quantity {sold_quantity}. "
            "Local inventory was not changed automatically."
        )
        record_shopify_sync_issue(
            session,
            issue_type=SHOPIFY_SYNC_ISSUE_ORDER_QUANTITY_CHANGED,
            shopify_sku=sku,
            shopify_title=str(sale.get("title") or card_name or ""),
            shopify_order_id=order_identity,
            shopify_order_number=order_number,
            shopify_variant_id=str(sale.get("variant_id") or "") or None,
            inventory_item_id=inventory_item_id,
            quantity=sold_quantity,
            unit_price=sale_price if sale_price > 0 else None,
            message=message,
            payload={
                "shopify_order_identity": order_identity,
                "inventory_item_id": inventory_item_id,
                "sku": sku,
                "dedupe_key": dedupe_key,
                "applied_quantity": applied_quantity,
                "requested_quantity": comparison_quantity,
                "requested_quantity_recorded": requested_quantity,
                "current_quantity": sold_quantity,
                "line_items": sale.get("payloads") or [],
            },
            severity="error",
        )
        issues_recorded += 1
        print(
            structured_log_line(
                runtime=runtime_name,
                action="inventory.shopify_order.quantity_changed",
                success=False,
                error=message,
                shopify_order_id=order_identity,
                inventory_item_id=inventory_item_id,
                barcode=sku,
                applied_quantity=applied_quantity,
                requested_quantity=comparison_quantity,
                current_quantity=sold_quantity,
            )
        )

    # PostgreSQL locks matching rows via FOR UPDATE. SQLite ignores FOR UPDATE,
    # so reserve its write lock before reading any quantity used for a decrement.
    transaction_started_here = _begin_stock_mutation_transaction(session)
    for sku in sorted(sold_by_sku):
        sale = sold_by_sku[sku]
        sale_price = float(sale.get("price") or 0)
        sold_quantity = max(0, int(sale.get("quantity") or 0))
        inv_item = session.exec(
            select(InventoryItem)
            .where(InventoryItem.barcode == sku)
            .with_for_update()
        ).first()
        invalid_quantity_values = sale.get("invalid_quantity_values") or []
        if invalid_quantity_values:
            message = (
                f"Shopify order {order_identity} SKU {sku} included an invalid "
                "paid quantity; local inventory was not changed."
            )
            issue = record_shopify_sync_issue(
                session,
                issue_type=SHOPIFY_SYNC_ISSUE_INVALID_ORDER_QUANTITY,
                shopify_sku=sku,
                shopify_title=str(sale.get("title") or ""),
                shopify_order_id=order_identity,
                shopify_order_number=order_number,
                shopify_variant_id=str(sale.get("variant_id") or "") or None,
                inventory_item_id=inv_item.id if inv_item is not None else None,
                quantity=0,
                unit_price=sale_price if sale_price > 0 else None,
                message=message,
                payload={
                    "shopify_order_identity": order_identity,
                    "inventory_item_id": inv_item.id if inv_item is not None else None,
                    "sku": sku,
                    "invalid_quantity_values": invalid_quantity_values,
                    "line_items": sale.get("payloads") or [],
                },
                severity="error",
            )
            issues_recorded += 1
            print(
                structured_log_line(
                    runtime=runtime_name,
                    action="inventory.shopify_order.invalid_quantity",
                    success=False,
                    error=message,
                    shopify_order_id=order_identity,
                    inventory_item_id=inv_item.id if inv_item is not None else None,
                    barcode=sku,
                    issue_id=issue.id,
                )
            )
            continue
        if inv_item is None or inv_item.archived_at is not None:
            if sold_quantity == 0:
                continue
            record_shopify_sync_issue(
                session,
                issue_type=SHOPIFY_SYNC_ISSUE_UNKNOWN_SKU,
                shopify_sku=sku,
                shopify_title=str(sale.get("title") or ""),
                shopify_order_id=order_identity,
                shopify_order_number=order_number,
                shopify_variant_id=str(sale.get("variant_id") or "") or None,
                quantity=sold_quantity,
                unit_price=sale_price if sale_price > 0 else None,
                message=(
                    f"Shopify order {order_identity} used SKU {sku}, "
                    "but no active Degen inventory item matched it."
                ),
                payload={"line_items": sale.get("payloads") or []},
            )
            issues_recorded += 1
            continue

        dedupe_key = f"shopify-order:{order_identity}:inventory-item:{inv_item.id}"
        movement_notes = f"Shopify order {order_identity}"
        with session.no_autoflush:
            existing_movement = _find_stock_movement_by_dedupe_key(
                session,
                dedupe_key,
                runtime_name=runtime_name,
                order_id=order_identity,
                sku=sku,
            )
            if existing_movement is None:
                existing_movement = session.exec(
                    select(InventoryStockMovement).where(
                        InventoryStockMovement.item_id == inv_item.id,
                        InventoryStockMovement.reason == "sale",
                        InventoryStockMovement.source == "Shopify",
                        InventoryStockMovement.notes == movement_notes,
                    )
                ).first()

        if existing_movement is not None:
            applied_quantity, requested_quantity, comparison_quantity = (
                movement_quantities(existing_movement)
            )
            if comparison_quantity == sold_quantity:
                log_replay(
                    sku=sku,
                    inventory_item_id=inv_item.id,
                    applied_quantity=applied_quantity,
                    requested_quantity=requested_quantity,
                )
            else:
                record_quantity_change(
                    sku=sku,
                    sale=sale,
                    sale_price=sale_price,
                    sold_quantity=sold_quantity,
                    inventory_item_id=inv_item.id,
                    card_name=str(inv_item.card_name or ""),
                    dedupe_key=dedupe_key,
                    applied_quantity=applied_quantity,
                    requested_quantity=requested_quantity,
                    comparison_quantity=comparison_quantity,
                )
            continue
        if sold_quantity == 0:
            continue

        prepared_sales.append(
            {
                "sku": sku,
                "sale": sale,
                "sale_price": sale_price,
                "sold_quantity": sold_quantity,
                "inv_item": inv_item,
                "dedupe_key": dedupe_key,
                "movement_notes": movement_notes,
            }
        )

    if issues_recorded:
        session.commit()
        return 0

    for prepared_sale in prepared_sales:
        sku = prepared_sale["sku"]
        sale = prepared_sale["sale"]
        sale_price = prepared_sale["sale_price"]
        sold_quantity = prepared_sale["sold_quantity"]
        inv_item = prepared_sale["inv_item"]
        dedupe_key = prepared_sale["dedupe_key"]
        movement_notes = prepared_sale["movement_notes"]
        before_qty = max(0, inv_item.quantity or 0)
        after_qty = max(0, before_qty - sold_quantity)
        movement = InventoryStockMovement(
            item_id=inv_item.id,
            reason="sale",
            quantity_delta=after_qty - before_qty,
            quantity_before=before_qty,
            quantity_after=after_qty,
            source="Shopify",
            notes=movement_notes,
            dedupe_key=dedupe_key,
            requested_quantity=sold_quantity,
            created_by=runtime_name,
            created_at=utcnow(),
        )
        try:
            with session.begin_nested():
                session.add(movement)
                session.flush()
                movement_claims_recorded += 1
        except (OperationalError, ProgrammingError) as exc:
            required_column = _missing_stock_movement_schema_column(exc)
            if required_column is not None:
                _log_stock_movement_schema_unavailable(
                    exc,
                    runtime_name=runtime_name,
                    order_id=order_identity,
                    sku=sku,
                    required_column=required_column,
                )
            raise
        except IntegrityError as exc:
            if not _is_stock_movement_dedupe_conflict(exc):
                raise
            with session.no_autoflush:
                concurrent_movement = _find_stock_movement_by_dedupe_key(
                    session,
                    dedupe_key,
                    runtime_name=runtime_name,
                    order_id=order_identity,
                    sku=sku,
                )
            if concurrent_movement is None:
                raise
            applied_quantity, requested_quantity, comparison_quantity = (
                movement_quantities(concurrent_movement)
            )
            if comparison_quantity == sold_quantity:
                log_replay(
                    sku=sku,
                    inventory_item_id=inv_item.id,
                    applied_quantity=applied_quantity,
                    requested_quantity=requested_quantity,
                )
                continue

            # A conflicting claim appeared after preflight. Roll back every
            # mutation for this order, then persist only the durable issue.
            inventory_item_id = inv_item.id
            card_name = str(inv_item.card_name or "")
            session.rollback()
            issues_recorded = 0
            record_quantity_change(
                sku=sku,
                sale=sale,
                sale_price=sale_price,
                sold_quantity=sold_quantity,
                inventory_item_id=inventory_item_id,
                card_name=card_name,
                dedupe_key=dedupe_key,
                applied_quantity=applied_quantity,
                requested_quantity=requested_quantity,
                comparison_quantity=comparison_quantity,
            )
            session.commit()
            return 0

        if after_qty == before_qty:
            print(
                structured_log_line(
                    runtime=runtime_name,
                    action="inventory.shopify_order.zero_stock_claimed",
                    success=True,
                    shopify_order_id=order_identity,
                    inventory_item_id=inv_item.id,
                    barcode=sku,
                    requested_quantity=sold_quantity,
                    quantity_before=before_qty,
                    quantity_after=after_qty,
                )
            )
            continue

        inv_item.quantity = after_qty
        if after_qty == 0:
            inv_item.status = INVENTORY_SOLD
            inv_item.sold_at = utcnow()
            inv_item.sold_price = sale_price if sale_price > 0 else None
        inv_item.updated_at = utcnow()
        session.add(inv_item)
        enqueue_shopify_sync_job(
            session,
            inv_item,
            action="quantity",
            source="Shopify order webhook",
            payload={
                "shopify_order_id": order_identity,
                "shopify_order_number": order_number,
                "sku": sku,
                "quantity_after": after_qty,
            },
        )
        marked += 1
        print(
            structured_log_line(
                runtime=runtime_name,
                action="inventory.item.sold_via_shopify",
                success=True,
                barcode=sku,
                inventory_item_id=inv_item.id,
                card_name=inv_item.card_name,
                sold_price=sale_price,
                sold_quantity=sold_quantity,
                quantity_before=before_qty,
                quantity_after=after_qty,
                shopify_order_id=order_identity,
            )
        )

    if marked or issues_recorded or movement_claims_recorded:
        session.commit()
    elif transaction_started_here:
        session.rollback()

    return marked
