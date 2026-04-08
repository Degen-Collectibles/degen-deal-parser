"""
Outbound Shopify Admin API integration for inventory.

Handles creating Shopify products from inventory items, updating prices,
and marking items sold when a Shopify order arrives with a matching SKU.

Requires SHOPIFY_ACCESS_TOKEN (private app token) with write_products and
write_inventory scopes. SHOPIFY_STORE_DOMAIN must also be set.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

import httpx

from .models import InventoryItem, ITEM_TYPE_SLAB, utcnow
from .inventory_pricing import effective_price

logger = logging.getLogger(__name__)

SHOPIFY_API_VERSION = "2024-01"


def _shopify_headers(access_token: str) -> dict[str, str]:
    return {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _shopify_base(store_domain: str) -> str:
    domain = (store_domain or "").strip().rstrip("/")
    if not domain.startswith("http"):
        domain = f"https://{domain}"
    return f"{domain}/admin/api/{SHOPIFY_API_VERSION}"


def _build_product_title(item: InventoryItem) -> str:
    parts = [item.card_name]
    if item.set_name:
        parts.append(item.set_name)
    if item.item_type == ITEM_TYPE_SLAB and item.grading_company and item.grade:
        parts.append(f"{item.grading_company} {item.grade}")
    elif item.condition:
        parts.append(item.condition)
    if item.language and item.language != "English":
        parts.append(item.language)
    return " — ".join(parts)


def _build_product_body(item: InventoryItem) -> str:
    lines = []
    if item.game:
        lines.append(f"Game: {item.game}")
    if item.set_name:
        lines.append(f"Set: {item.set_name}")
    if item.card_number:
        lines.append(f"Card #: {item.card_number}")
    if item.item_type == ITEM_TYPE_SLAB:
        if item.grading_company:
            lines.append(f"Grading Company: {item.grading_company}")
        if item.grade:
            lines.append(f"Grade: {item.grade}")
        if item.cert_number:
            lines.append(f"Cert #: {item.cert_number}")
    else:
        if item.condition:
            lines.append(f"Condition: {item.condition}")
    if item.language and item.language != "English":
        lines.append(f"Language: {item.language}")
    if item.notes:
        lines.append(f"Notes: {item.notes}")
    return "<br>".join(lines)


def _build_product_tags(item: InventoryItem) -> list[str]:
    tags = [item.game, item.item_type]
    if item.item_type == ITEM_TYPE_SLAB:
        if item.grading_company:
            tags.append(item.grading_company)
        if item.grade:
            tags.append(f"Grade {item.grade}")
    else:
        if item.condition:
            tags.append(item.condition)
    if item.set_name:
        tags.append(item.set_name)
    return [t for t in tags if t]


def build_shopify_product_payload(item: InventoryItem) -> dict[str, Any]:
    price = effective_price(item)
    price_str = f"{price:.2f}" if price is not None else "0.00"
    return {
        "product": {
            "title": _build_product_title(item),
            "body_html": _build_product_body(item),
            "product_type": "Slabs" if item.item_type == ITEM_TYPE_SLAB else "Singles",
            "tags": ", ".join(_build_product_tags(item)),
            "variants": [
                {
                    "price": price_str,
                    "sku": item.barcode,
                    "inventory_quantity": item.quantity,
                    "inventory_management": "shopify",
                    "fulfillment_service": "manual",
                }
            ],
        }
    }


async def push_item_to_shopify(
    item: InventoryItem,
    *,
    store_domain: str,
    access_token: str,
) -> Optional[dict[str, Any]]:
    """
    Create a new Shopify product for the inventory item.

    Returns a dict with shopify_product_id and shopify_variant_id on success,
    or None on failure.
    """
    if not store_domain or not access_token:
        logger.warning("[shopify-inventory] SHOPIFY_STORE_DOMAIN or SHOPIFY_ACCESS_TOKEN not set")
        return None

    url = f"{_shopify_base(store_domain)}/products.json"
    payload = build_shopify_product_payload(item)

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.post(
                url,
                json=payload,
                headers=_shopify_headers(access_token),
            )
            resp.raise_for_status()
            data = resp.json()
            product = data.get("product") or {}
            variants = product.get("variants") or [{}]
            return {
                "shopify_product_id": str(product.get("id") or ""),
                "shopify_variant_id": str(variants[0].get("id") or ""),
            }
    except httpx.HTTPStatusError as exc:
        logger.error(
            "[shopify-inventory] create product failed for item %s: %s %s",
            item.barcode,
            exc.response.status_code,
            exc.response.text[:200],
        )
        return None
    except Exception as exc:
        logger.error("[shopify-inventory] create product error for item %s: %s", item.barcode, exc)
        return None


async def update_shopify_variant_price(
    item: InventoryItem,
    *,
    store_domain: str,
    access_token: str,
) -> bool:
    """Push the current effective_price to the Shopify variant. Returns True on success."""
    if not item.shopify_variant_id or not store_domain or not access_token:
        return False

    price = effective_price(item)
    if price is None:
        return False

    url = f"{_shopify_base(store_domain)}/variants/{item.shopify_variant_id}.json"
    payload = {"variant": {"id": item.shopify_variant_id, "price": f"{price:.2f}"}}

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.put(
                url,
                json=payload,
                headers=_shopify_headers(access_token),
            )
            resp.raise_for_status()
            return True
    except Exception as exc:
        logger.error(
            "[shopify-inventory] price update failed for variant %s: %s",
            item.shopify_variant_id,
            exc,
        )
        return False
