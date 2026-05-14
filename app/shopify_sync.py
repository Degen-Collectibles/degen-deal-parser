from __future__ import annotations

import json
from typing import Any, Optional

from sqlmodel import Session, select

from .models import InventoryItem, ShopifySyncIssue, ShopifySyncJob, utcnow

SHOPIFY_SYNC_NOT_SYNCED = "not_synced"
SHOPIFY_SYNC_LINKED = "linked"
SHOPIFY_SYNC_PENDING = "pending"
SHOPIFY_SYNC_SYNCED = "synced"
SHOPIFY_SYNC_ERROR = "error"

SHOPIFY_SYNC_JOB_PENDING = "pending"
SHOPIFY_SYNC_JOB_DONE = "done"
SHOPIFY_SYNC_JOB_ERROR = "error"

SHOPIFY_SYNC_ISSUE_OPEN = "open"
SHOPIFY_SYNC_ISSUE_RESOLVED = "resolved"
SHOPIFY_SYNC_ISSUE_IGNORED = "ignored"
SHOPIFY_SYNC_ISSUE_LINKED = "linked"

SHOPIFY_SYNC_ISSUE_UNKNOWN_SKU = "unknown_sku"
SHOPIFY_SYNC_ISSUE_UNLINKED_PRODUCT = "unlinked_product"
SHOPIFY_SYNC_ISSUE_SYNC_ERROR = "sync_error"


def _json_dumps(value: Any) -> str:
    return json.dumps(value or {}, default=str, sort_keys=True, separators=(",", ":"))


def _issue_key(
    *,
    issue_type: str,
    shopify_order_id: Optional[str] = None,
    shopify_product_id: Optional[str] = None,
    shopify_variant_id: Optional[str] = None,
    shopify_sku: Optional[str] = None,
    shopify_title: Optional[str] = None,
) -> str:
    if issue_type == SHOPIFY_SYNC_ISSUE_UNKNOWN_SKU:
        return ":".join(
            [
                issue_type,
                str(shopify_order_id or "unknown-order"),
                str(shopify_variant_id or shopify_sku or shopify_title or "unknown-line"),
            ]
        )
    if issue_type == SHOPIFY_SYNC_ISSUE_UNLINKED_PRODUCT:
        return ":".join(
            [
                issue_type,
                str(shopify_product_id or "unknown-product"),
                str(shopify_variant_id or shopify_sku or shopify_title or "unknown-variant"),
            ]
        )
    return ":".join(
        [
            issue_type,
            str(shopify_product_id or shopify_order_id or "unknown"),
            str(shopify_variant_id or shopify_sku or shopify_title or "unknown"),
        ]
    )


def record_shopify_sync_issue(
    session: Session,
    *,
    issue_type: str,
    message: str,
    shopify_sku: Optional[str] = None,
    shopify_title: Optional[str] = None,
    shopify_order_id: Optional[str] = None,
    shopify_order_number: Optional[str] = None,
    shopify_product_id: Optional[str] = None,
    shopify_variant_id: Optional[str] = None,
    shopify_inventory_item_id: Optional[str] = None,
    shopify_location_id: Optional[str] = None,
    inventory_item_id: Optional[int] = None,
    quantity: int = 1,
    unit_price: Optional[float] = None,
    payload: Optional[dict[str, Any]] = None,
    severity: str = "warning",
) -> ShopifySyncIssue:
    key = _issue_key(
        issue_type=issue_type,
        shopify_order_id=shopify_order_id,
        shopify_product_id=shopify_product_id,
        shopify_variant_id=shopify_variant_id,
        shopify_sku=shopify_sku,
        shopify_title=shopify_title,
    )
    issue = session.exec(
        select(ShopifySyncIssue).where(ShopifySyncIssue.issue_key == key)
    ).first()
    now = utcnow()
    if issue is None:
        issue = ShopifySyncIssue(
            issue_key=key,
            issue_type=issue_type,
            status=SHOPIFY_SYNC_ISSUE_OPEN,
            first_seen_at=now,
        )
    else:
        issue.occurrence_count = max(1, issue.occurrence_count or 1) + 1
        if issue.status in {SHOPIFY_SYNC_ISSUE_RESOLVED, SHOPIFY_SYNC_ISSUE_LINKED}:
            issue.status = SHOPIFY_SYNC_ISSUE_OPEN
    issue.severity = severity
    issue.inventory_item_id = inventory_item_id or issue.inventory_item_id
    issue.shopify_order_id = shopify_order_id or issue.shopify_order_id
    issue.shopify_order_number = shopify_order_number or issue.shopify_order_number
    issue.shopify_product_id = shopify_product_id or issue.shopify_product_id
    issue.shopify_variant_id = shopify_variant_id or issue.shopify_variant_id
    issue.shopify_inventory_item_id = shopify_inventory_item_id or issue.shopify_inventory_item_id
    issue.shopify_location_id = shopify_location_id or issue.shopify_location_id
    issue.shopify_sku = shopify_sku or issue.shopify_sku
    issue.shopify_title = shopify_title or issue.shopify_title
    issue.quantity = max(1, int(quantity or 1))
    issue.unit_price = unit_price if unit_price is not None else issue.unit_price
    issue.message = message
    issue.raw_payload_json = _json_dumps(payload)
    issue.last_seen_at = now
    session.add(issue)
    return issue


def enqueue_shopify_sync_job(
    session: Session,
    item: InventoryItem,
    *,
    action: str = "sync",
    source: str = "",
    payload: Optional[dict[str, Any]] = None,
) -> Optional[ShopifySyncJob]:
    if item.id is None or not item.shopify_sync_enabled:
        return None
    job = ShopifySyncJob(
        item_id=item.id,
        action=action,
        status=SHOPIFY_SYNC_JOB_PENDING,
        source=source or None,
        payload_json=_json_dumps(payload),
        created_at=utcnow(),
        updated_at=utcnow(),
    )
    item.shopify_sync_status = SHOPIFY_SYNC_PENDING
    item.shopify_sync_error = None
    item.updated_at = utcnow()
    session.add(item)
    session.add(job)
    return job


def mark_shopify_item_synced(
    session: Session,
    item: InventoryItem,
    *,
    status: str = SHOPIFY_SYNC_SYNCED,
    error: Optional[str] = None,
    payload: Optional[dict[str, Any]] = None,
) -> None:
    item.shopify_sync_status = status
    item.shopify_sync_error = error
    item.shopify_synced_at = utcnow() if error is None else item.shopify_synced_at
    if payload is not None:
        item.shopify_last_payload_json = _json_dumps(payload)
    item.updated_at = utcnow()
    session.add(item)
