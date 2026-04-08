"""
Auto-pricing adapters for inventory items.

Singles (MTG / Pokemon): Scrydex subscription API.
  - Configure SCRYDEX_API_KEY and SCRYDEX_BASE_URL in .env
  - If Scrydex uses a Scryfall-compatible endpoint, set SCRYDEX_BASE_URL to
    https://api.scryfall.com and leave SCRYDEX_API_KEY empty.

Slabs (PSA / BGS / CGC): 130point → Alt → Card Ladder fallback chain.
  - These services do not have official public APIs; requests target their
    public search/data endpoints. Update the URL constants below when their
    endpoints change.

Usage:
    from .inventory_pricing import fetch_price_for_item
    result = await fetch_price_for_item(item, client)
    # result is None on failure, or:
    # {"source": "scrydex", "market_price": 42.0, "low_price": 38.0, "high_price": 50.0, "raw": {...}}
"""
from __future__ import annotations

import json
import logging
from typing import Any, Optional

import httpx

from .models import InventoryItem, ITEM_TYPE_SLAB

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 130point search endpoint (graded cards)
# Example: https://www.130point.com/sales/search?q=Charizard+PSA+10&output=json
# ---------------------------------------------------------------------------
POINT130_SEARCH_URL = "https://www.130point.com/sales/search"

# ---------------------------------------------------------------------------
# Alt.gg API (graded cards marketplace)
# Example: https://alt.gg/api/search?q=Charizard+PSA+10
# ---------------------------------------------------------------------------
ALT_SEARCH_URL = "https://alt.gg/api/search"

# ---------------------------------------------------------------------------
# Card Ladder (price history for graded cards)
# Example: https://www.cardladder.com/api/search?q=Charizard+PSA+10
# ---------------------------------------------------------------------------
CARD_LADDER_SEARCH_URL = "https://www.cardladder.com/api/search"


# ---------------------------------------------------------------------------
# Scryfall fallback (free MTG API — used when SCRYDEX_BASE_URL is unset)
# ---------------------------------------------------------------------------
SCRYFALL_NAMED_URL = "https://api.scryfall.com/cards/named"


def effective_price(item: InventoryItem) -> Optional[float]:
    """Return the price to use: list_price overrides auto_price."""
    if item.list_price is not None:
        return round(item.list_price, 2)
    if item.auto_price is not None:
        return round(item.auto_price, 2)
    return None


# ---------------------------------------------------------------------------
# Singles pricing (Scrydex / Scryfall)
# ---------------------------------------------------------------------------

async def fetch_single_price(
    item: InventoryItem,
    client: httpx.AsyncClient,
    *,
    api_key: str = "",
    base_url: str = "",
) -> Optional[dict[str, Any]]:
    """
    Fetch market price for a single card via Scrydex (or Scryfall as fallback).

    Scrydex API format is not yet publicly documented; this implementation targets
    their expected REST interface. Update the request construction below once you
    have API access and can confirm the exact endpoints and response shape.

    When base_url is empty or points to api.scryfall.com, falls back to the free
    Scryfall API (MTG only).
    """
    resolved_url = (base_url or "").rstrip("/")

    # Scryfall-compatible path: used when no Scrydex URL is configured, or when
    # the operator explicitly sets SCRYDEX_BASE_URL=https://api.scryfall.com
    if not resolved_url or "scryfall.com" in resolved_url:
        return await _fetch_scryfall_price(item, client)

    # Scrydex — adapt the path/params/headers once API docs are available.
    # Current implementation uses a reasonable convention; update as needed.
    endpoint = f"{resolved_url}/v1/prices"
    params: dict[str, str] = {"name": item.card_name}
    if item.set_code:
        params["set"] = item.set_code
    if item.card_number:
        params["number"] = item.card_number
    if item.game:
        params["game"] = item.game.lower()
    if item.condition:
        params["condition"] = item.condition.lower()

    headers: dict[str, str] = {"Accept": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    try:
        resp = await client.get(endpoint, params=params, headers=headers, timeout=15.0)
        resp.raise_for_status()
        data = resp.json()
        market = _safe_float(data.get("market_price") or data.get("market") or data.get("price"))
        low = _safe_float(data.get("low_price") or data.get("low"))
        high = _safe_float(data.get("high_price") or data.get("high"))
        if market is None and low is None:
            logger.warning("[pricing] scrydex returned no price for %s", item.card_name)
            return None
        return {
            "source": "scrydex",
            "market_price": market,
            "low_price": low,
            "high_price": high,
            "raw": data,
        }
    except Exception as exc:
        logger.warning("[pricing] scrydex request failed for %s: %s", item.card_name, exc)
        return None


async def _fetch_scryfall_price(
    item: InventoryItem,
    client: httpx.AsyncClient,
) -> Optional[dict[str, Any]]:
    """Fetch price from Scryfall (free MTG-only API)."""
    params: dict[str, str] = {"exact": item.card_name}
    if item.set_code:
        params["set"] = item.set_code

    try:
        resp = await client.get(SCRYFALL_NAMED_URL, params=params, timeout=10.0)
        if resp.status_code == 404:
            logger.info("[pricing] scryfall: card not found: %s", item.card_name)
            return None
        resp.raise_for_status()
        data = resp.json()
        prices = data.get("prices") or {}
        market = _safe_float(prices.get("usd"))
        low = _safe_float(prices.get("usd_foil")) if item.game == "MTG" else None
        if market is None:
            return None
        return {
            "source": "scrydex",   # report as scrydex since that is the configured source
            "market_price": market,
            "low_price": low,
            "high_price": None,
            "raw": {"prices": prices, "name": data.get("name"), "set": data.get("set_name")},
        }
    except Exception as exc:
        logger.warning("[pricing] scryfall request failed for %s: %s", item.card_name, exc)
        return None


# ---------------------------------------------------------------------------
# Slab pricing (130point → Alt → Card Ladder)
# ---------------------------------------------------------------------------

def _slab_query(item: InventoryItem) -> str:
    """Build a search query string for a graded slab."""
    parts = [item.card_name]
    if item.grading_company:
        parts.append(item.grading_company)
    if item.grade:
        parts.append(item.grade)
    return " ".join(parts)


async def fetch_slab_price(
    item: InventoryItem,
    client: httpx.AsyncClient,
) -> Optional[dict[str, Any]]:
    """
    Try slab pricing sources in order: 130point → Alt → Card Ladder.
    Returns the first successful result, or None if all fail.
    """
    query = _slab_query(item)

    result = await _fetch_130point_price(item, client, query)
    if result:
        return result

    result = await _fetch_alt_price(item, client, query)
    if result:
        return result

    result = await _fetch_card_ladder_price(item, client, query)
    return result


async def _fetch_130point_price(
    item: InventoryItem,
    client: httpx.AsyncClient,
    query: str,
) -> Optional[dict[str, Any]]:
    """
    130point.com graded card sales data.

    The endpoint returns a JSON array of recent sales. We compute median price
    from the top results. Update URL/params if the site structure changes.
    """
    try:
        params = {"q": query, "output": "json"}
        resp = await client.get(POINT130_SEARCH_URL, params=params, timeout=15.0,
                                headers={"User-Agent": "DegenCollectibles/1.0"})
        if resp.status_code != 200:
            return None
        data = resp.json()
        # Expected: list of sale records with "price" or "sale_price" field
        sales = data if isinstance(data, list) else (data.get("results") or data.get("sales") or [])
        prices = [_safe_float(s.get("price") or s.get("sale_price")) for s in sales[:20]]
        prices = [p for p in prices if p is not None and p > 0]
        if not prices:
            return None
        prices_sorted = sorted(prices)
        mid = len(prices_sorted) // 2
        median = prices_sorted[mid]
        return {
            "source": "130point",
            "market_price": round(median, 2),
            "low_price": round(min(prices_sorted), 2),
            "high_price": round(max(prices_sorted), 2),
            "raw": {"query": query, "sample_count": len(prices_sorted)},
        }
    except Exception as exc:
        logger.debug("[pricing] 130point failed for %s: %s", query, exc)
        return None


async def _fetch_alt_price(
    item: InventoryItem,
    client: httpx.AsyncClient,
    query: str,
) -> Optional[dict[str, Any]]:
    """Alt.gg graded card marketplace pricing."""
    try:
        params = {"q": query}
        resp = await client.get(ALT_SEARCH_URL, params=params, timeout=15.0,
                                headers={"User-Agent": "DegenCollectibles/1.0"})
        if resp.status_code != 200:
            return None
        data = resp.json()
        listings = data if isinstance(data, list) else (data.get("results") or data.get("listings") or [])
        prices = [_safe_float(s.get("price") or s.get("list_price")) for s in listings[:20]]
        prices = [p for p in prices if p is not None and p > 0]
        if not prices:
            return None
        prices_sorted = sorted(prices)
        mid = len(prices_sorted) // 2
        median = prices_sorted[mid]
        return {
            "source": "alt",
            "market_price": round(median, 2),
            "low_price": round(min(prices_sorted), 2),
            "high_price": round(max(prices_sorted), 2),
            "raw": {"query": query, "sample_count": len(prices_sorted)},
        }
    except Exception as exc:
        logger.debug("[pricing] alt failed for %s: %s", query, exc)
        return None


async def _fetch_card_ladder_price(
    item: InventoryItem,
    client: httpx.AsyncClient,
    query: str,
) -> Optional[dict[str, Any]]:
    """Card Ladder graded card price history."""
    try:
        params = {"q": query}
        resp = await client.get(CARD_LADDER_SEARCH_URL, params=params, timeout=15.0,
                                headers={"User-Agent": "DegenCollectibles/1.0"})
        if resp.status_code != 200:
            return None
        data = resp.json()
        results = data if isinstance(data, list) else (data.get("results") or [])
        if not results:
            return None
        first = results[0] if results else {}
        market = _safe_float(first.get("market_price") or first.get("avg_price") or first.get("price"))
        low = _safe_float(first.get("low_price") or first.get("low"))
        high = _safe_float(first.get("high_price") or first.get("high"))
        if market is None:
            return None
        return {
            "source": "card_ladder",
            "market_price": round(market, 2),
            "low_price": round(low, 2) if low else None,
            "high_price": round(high, 2) if high else None,
            "raw": {"query": query},
        }
    except Exception as exc:
        logger.debug("[pricing] card_ladder failed for %s: %s", query, exc)
        return None


# ---------------------------------------------------------------------------
# Unified entry point
# ---------------------------------------------------------------------------

async def fetch_price_for_item(
    item: InventoryItem,
    client: httpx.AsyncClient,
    *,
    api_key: str = "",
    base_url: str = "",
) -> Optional[dict[str, Any]]:
    """Dispatch to the correct pricing source based on item_type."""
    if item.item_type == ITEM_TYPE_SLAB:
        return await fetch_slab_price(item, client)
    return await fetch_single_price(item, client, api_key=api_key, base_url=base_url)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        f = float(value)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


def price_result_to_json(result: dict[str, Any]) -> str:
    """Serialise a price result dict to JSON string for storage in raw_response_json."""
    return json.dumps(result.get("raw") or result, default=str, separators=(",", ":"))
