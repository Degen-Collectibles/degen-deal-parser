"""
Auto-pricing adapters for inventory items.

Singles (MTG / Pokemon): Scrydex subscription API.
  - Configure SCRYDEX_API_KEY and SCRYDEX_BASE_URL in .env
  - If Scrydex uses a Scryfall-compatible endpoint, set SCRYDEX_BASE_URL to
    https://api.scryfall.com and leave SCRYDEX_API_KEY empty.

Slabs (PSA / BGS / CGC): Card Ladder last-solds first, then 130point / Alt fallback.
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
import re
import html as html_lib
import asyncio
import csv
import io
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urljoin, urlencode

import httpx

from .models import InventoryItem, ITEM_TYPE_SLAB, utcnow

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 130point search endpoint (graded cards)
# Example: https://www.130point.com/sales/search?q=Charizard+PSA+10&output=json
# ---------------------------------------------------------------------------
POINT130_SEARCH_URL = "https://www.130point.com/sales/search"

# ---------------------------------------------------------------------------
# ALT sold-listing search (graded cards marketplace)
# scripts/alt_cli.py fetches ALT's current web-app search config, then queries
# the sold-listing Typesense collection.
# ---------------------------------------------------------------------------
ALT_BROWSE_URL = "https://alt.xyz/browse"
PRICECHARTING_GAME_URL = "https://www.pricecharting.com/game"
MYSLABS_ARCHIVE_SEARCH_URL = "https://myslabs.com/search/archive/"

# ---------------------------------------------------------------------------
# Card Ladder (price history for graded cards)
# Example: https://www.cardladder.com/api/search?q=Charizard+PSA+10
# ---------------------------------------------------------------------------
CARD_LADDER_SEARCH_URL = "https://www.cardladder.com/api/search"
CARD_LADDER_APP_SALES_HISTORY_URL = "https://app.cardladder.com/sales-history"
CARD_LADDER_CARDS_SEARCH_URL = "https://www.cardladder.com/cards/search"
OTHER_GRADERS = ("PSA", "BGS", "CGC", "SGC", "BECKETT")
CARD_LADDER_NOISE_EXCLUSIONS = (
    "Autograph",
    "Auto",
    "Signed",
    "Lot",
    "Reprint",
    "Proxy",
    "Custom",
    "Checklist",
)
STALE_SLAB_COMP_DAYS = 30
SLAB_PRICE_SOURCE_OPTIONS = ("all", "alt", "pricecharting", "myslabs", "card_ladder", "130point")
SLAB_PRICE_SOURCE_ALIASES = {
    "": "all",
    "all_sources": "all",
    "all sources": "all",
    "slab_comps": "all",
    "slab comps": "all",
    "price_charting": "pricecharting",
    "price charting": "pricecharting",
    "pc": "pricecharting",
    "cardladder": "card_ladder",
    "card ladder": "card_ladder",
    "cl": "card_ladder",
    "point130": "130point",
    "130 point": "130point",
    "my_slabs": "myslabs",
    "my slabs": "myslabs",
}


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
# Slab pricing (Card Ladder -> 130point -> Alt)
# ---------------------------------------------------------------------------

def _slab_query(item: InventoryItem) -> str:
    """Build a search query string for a graded slab."""
    parts = [item.card_name]
    if item.grading_company:
        parts.append(item.grading_company)
    if item.grade:
        parts.append(item.grade)
    return " ".join(parts)


def build_card_ladder_slab_query(item: InventoryItem, *, strict: bool = True) -> str:
    """Build a Card Ladder Sales History query for one graded slab."""
    parts: list[str] = []
    seen: set[str] = set()
    for value in (
        item.card_name,
        item.set_name,
        item.card_number,
        item.grading_company,
        item.grade,
    ):
        cleaned = str(value or "").strip()
        key = cleaned.lower()
        if cleaned and key not in seen:
            parts.append(cleaned)
            seen.add(key)

    if strict:
        grader = (item.grading_company or "").strip().upper()
        for other in OTHER_GRADERS:
            if grader and other == grader:
                continue
            parts.append(f"-{other}")
        if grader and item.grade:
            grade = _safe_float(item.grade)
            if grade is not None:
                for nearby in (grade - 1, grade + 1):
                    if nearby > 0:
                        parts.append(f"-({grader} {nearby:g})")
        for term in CARD_LADDER_NOISE_EXCLUSIONS:
            parts.append(f"-{term}")

    return " ".join(part for part in parts if part).strip()


def build_card_ladder_cli_query(item: InventoryItem, *, strict: bool = True) -> str:
    """Build the exact query key used by scripts/cardladder_cli.py."""
    try:
        from scripts import cardladder_cli
    except Exception:
        return build_card_ladder_slab_query(item, strict=strict)
    return cardladder_cli.build_slab_query(
        _card_ladder_cli_base_query(item),
        grader=(item.grading_company or "").strip().upper(),
        grade=str(item.grade or "").strip(),
        cert=str(item.cert_number or "").strip(),
        strict=strict,
    )


def _card_ladder_cli_base_query(item: InventoryItem) -> str:
    return " ".join(
        part
        for part in (
            str(item.card_name or "").strip(),
            str(item.set_name or "").strip(),
            str(item.card_number or "").strip(),
        )
        if part
    )


def card_ladder_sales_history_url(query: str) -> str:
    params = urlencode({"sort": "date", "direction": "desc", "q": query})
    return f"{CARD_LADDER_APP_SALES_HISTORY_URL}?{params}"


def card_ladder_cli_status() -> dict[str, Any]:
    try:
        from scripts import cardladder_cli
    except Exception as exc:
        return {"available": False, "error": str(exc)}
    profile_dir = cardladder_cli.default_profile_dir()
    cache_db = cardladder_cli.default_cache_path()
    return {
        "available": True,
        "profile_dir": str(profile_dir),
        "profile_exists": profile_dir.exists(),
        "cache_db": str(cache_db),
        "cache_exists": cache_db.exists(),
        "login_command": ".\\.venv\\Scripts\\python.exe scripts\\cardladder_cli.py login",
    }


def _fetch_card_ladder_cli_cache(
    item: InventoryItem,
    *,
    query: str = "",
    cache_path: str | Path | None = None,
    limit: int = 20,
) -> Optional[dict[str, Any]]:
    """Read saved comps from the browser-backed Card Ladder CLI cache."""
    try:
        from scripts import cardladder_cli
    except Exception as exc:
        logger.debug("[pricing] card_ladder_cli unavailable: %s", exc)
        return None

    cache_db = Path(cache_path) if cache_path else cardladder_cli.default_cache_path()
    if not cache_db.exists():
        return None

    resolved_query = query or build_card_ladder_cli_query(item)
    base_query = _card_ladder_cli_base_query(item)
    grader = (item.grading_company or "").strip().upper()
    grade = str(item.grade or "").strip()
    cert = str(item.cert_number or "").strip()

    try:
        records = cardladder_cli.load_cached_records(
            cache_db,
            query=resolved_query,
            limit=limit,
        )
        if not records and base_query:
            records = cardladder_cli.load_cached_records(
                cache_db,
                text=base_query,
                grader=grader,
                grade=grade,
                cert=cert,
                limit=limit,
            )
    except Exception as exc:
        logger.debug("[pricing] card_ladder_cli cache read failed for %s: %s", resolved_query, exc)
        return None

    sales = _card_ladder_sales_from_cli_records(records)
    if not sales:
        return None
    prices = [float(sale["price"]) for sale in sales if _safe_float(sale.get("price")) is not None]
    suggested = _suggest_price_from_sales(sales)
    if suggested is None:
        return None
    return {
        "source": "card_ladder",
        "market_price": suggested,
        "low_price": round(min(prices), 2) if prices else None,
        "high_price": round(max(prices), 2) if prices else None,
        "raw": {
            "query": resolved_query,
            "sales_history_url": card_ladder_sales_history_url(resolved_query),
            "source_detail": "card_ladder_cli_cache",
            "cache_db": str(cache_db),
            "sample_count": len(prices),
            "sales": sales[:20],
        },
    }


def _card_ladder_sales_from_cli_records(records: list[Any]) -> list[dict[str, Any]]:
    sales: list[dict[str, Any]] = []
    seen: set[tuple[str, float, str]] = set()
    for record in records:
        price = _safe_float(getattr(record, "price", None))
        if price is None:
            continue
        sale = {
            "title": str(getattr(record, "title", "") or "").strip(),
            "price": round(price, 2),
            "sold_date": _normalize_sale_date(getattr(record, "sold_date", "")),
            "platform": str(getattr(record, "platform", "") or "").strip(),
            "sale_type": str(getattr(record, "sale_type", "") or "").strip(),
            "url": str(getattr(record, "url", "") or "").strip(),
            "image_url": str(getattr(record, "image_url", "") or "").strip(),
        }
        key = (sale["sold_date"], sale["price"], sale["title"])
        if key in seen:
            continue
        seen.add(key)
        sales.append(sale)
        if len(sales) >= 50:
            break
    return sales


def build_alt_cli_query(item: InventoryItem) -> str:
    try:
        from scripts import alt_cli
    except Exception:
        return _slab_query(item)
    return alt_cli.build_slab_query(
        _card_ladder_cli_base_query(item),
        grader=(item.grading_company or "").strip().upper(),
        grade=str(item.grade or "").strip(),
        cert=str(item.cert_number or "").strip(),
    )


def alt_cli_status() -> dict[str, Any]:
    try:
        from scripts import alt_cli
    except Exception as exc:
        return {"available": False, "error": str(exc)}
    cache_db = alt_cli.default_cache_path()
    return {
        "available": True,
        "cache_db": str(cache_db),
        "cache_exists": cache_db.exists(),
    }


def _alt_card_number_filter(item: InventoryItem) -> str:
    raw = str(item.card_number or "").strip()
    if not raw:
        return ""
    return raw.split("/", 1)[0].strip()


def _fetch_alt_cli_cache(
    item: InventoryItem,
    *,
    query: str = "",
    cache_path: str | Path | None = None,
    limit: int = 20,
) -> Optional[dict[str, Any]]:
    try:
        from scripts import alt_cli
    except Exception as exc:
        logger.debug("[pricing] alt_cli unavailable: %s", exc)
        return None

    cache_db = Path(cache_path) if cache_path else alt_cli.default_cache_path()
    if not cache_db.exists():
        return None
    resolved_query = query or build_alt_cli_query(item)
    try:
        records = alt_cli.load_cached_records(
            cache_db,
            query=resolved_query,
            limit=limit,
        )
        if not records:
            records = alt_cli.load_cached_records(
                cache_db,
                text=_card_ladder_cli_base_query(item),
                grader=(item.grading_company or "").strip().upper(),
                grade=str(item.grade or "").strip(),
                limit=limit,
            )
    except Exception as exc:
        logger.debug("[pricing] alt_cli cache read failed for %s: %s", resolved_query, exc)
        return None
    return _alt_result_from_records(
        resolved_query,
        records,
        source_detail="alt_cli_cache",
        cache_db=str(cache_db),
    )


async def sync_alt_cli_for_item(
    item: InventoryItem,
    *,
    limit: int = 20,
) -> dict[str, Any]:
    try:
        from scripts import alt_cli
    except Exception as exc:
        raise RuntimeError(f"ALT CLI is unavailable: {exc}") from exc

    query = build_alt_cli_query(item)
    if not query:
        raise RuntimeError("Card name or slab details are required before refreshing ALT.")
    records = await asyncio.to_thread(
        alt_cli.fetch_records,
        query,
        grader=(item.grading_company or "").strip().upper(),
        grade=str(item.grade or "").strip(),
        card_number=_alt_card_number_filter(item),
        limit=limit,
    )
    if not records:
        raise RuntimeError("ALT returned no sold comps for this slab query.")
    await asyncio.to_thread(alt_cli.cache_records, alt_cli.default_cache_path(), query, records)
    result = _alt_result_from_records(query, records, source_detail="alt_typesense_live")
    if not result:
        raise RuntimeError("ALT returned sold rows, but none had usable prices.")
    return result


def _alt_result_from_records(
    query: str,
    records: list[Any],
    *,
    source_detail: str,
    cache_db: str = "",
) -> Optional[dict[str, Any]]:
    return _result_from_comp_records(
        query,
        records,
        source="alt",
        source_detail=source_detail,
        sales_history_url=alt_sales_history_url(query),
        cache_db=cache_db,
    )


def _sales_from_comp_records(records: list[Any]) -> list[dict[str, Any]]:
    sales: list[dict[str, Any]] = []
    seen: set[tuple[str, float, str]] = set()
    for record in records:
        price = _safe_float(getattr(record, "price", None))
        if price is None:
            continue
        sale = {
            "title": str(getattr(record, "title", "") or "").strip(),
            "price": round(price, 2),
            "sold_date": _normalize_sale_date(getattr(record, "sold_date", "")),
            "platform": str(getattr(record, "platform", "") or "").strip(),
            "sale_type": str(getattr(record, "sale_type", "") or "").strip(),
            "url": str(getattr(record, "url", "") or "").strip(),
            "image_url": str(getattr(record, "image_url", "") or "").strip(),
        }
        key = (sale["sold_date"], sale["price"], sale["title"])
        if key in seen:
            continue
        seen.add(key)
        sales.append(sale)
        if len(sales) >= 50:
            break
    return sales


def alt_sales_history_url(query: str) -> str:
    params = urlencode({"query": query, "tab": "sold"})
    return f"{ALT_BROWSE_URL}?{params}"


def build_130point_cli_query(item: InventoryItem) -> str:
    try:
        from scripts import point130_cli
    except Exception:
        return _slab_query(item)
    return point130_cli.build_slab_query(
        _card_ladder_cli_base_query(item),
        grader=(item.grading_company or "").strip().upper(),
        grade=str(item.grade or "").strip(),
        cert=str(item.cert_number or "").strip(),
    )


def point130_cli_status() -> dict[str, Any]:
    try:
        from scripts import point130_cli
    except Exception as exc:
        return {"available": False, "error": str(exc)}
    cache_db = point130_cli.default_cache_path()
    return {
        "available": True,
        "cache_db": str(cache_db),
        "cache_exists": cache_db.exists(),
    }


def _fetch_130point_cli_cache(
    item: InventoryItem,
    *,
    query: str = "",
    cache_path: str | Path | None = None,
    limit: int = 20,
) -> Optional[dict[str, Any]]:
    try:
        from scripts import point130_cli
    except Exception as exc:
        logger.debug("[pricing] point130_cli unavailable: %s", exc)
        return None

    cache_db = Path(cache_path) if cache_path else point130_cli.default_cache_path()
    if not cache_db.exists():
        return None
    resolved_query = query or build_130point_cli_query(item)
    try:
        records = point130_cli.load_cached_records(
            cache_db,
            query=resolved_query,
            limit=limit,
        )
        if not records:
            records = point130_cli.load_cached_records(
                cache_db,
                text=_card_ladder_cli_base_query(item),
                grader=(item.grading_company or "").strip().upper(),
                grade=str(item.grade or "").strip(),
                limit=limit,
            )
    except Exception as exc:
        logger.debug("[pricing] point130_cli cache read failed for %s: %s", resolved_query, exc)
        return None
    return _result_from_comp_records(
        resolved_query,
        records,
        source="130point",
        source_detail="130point_cli_cache",
        sales_history_url=point130_sales_history_url(resolved_query),
        cache_db=str(cache_db),
    )


def point130_sales_history_url(query: str) -> str:
    try:
        from scripts import point130_cli
        return point130_cli.sales_search_url(query)
    except Exception:
        params = urlencode({"q": query})
        return f"{POINT130_SEARCH_URL}?{params}"


def _result_from_comp_records(
    query: str,
    records: list[Any],
    *,
    source: str,
    source_detail: str,
    sales_history_url: str,
    cache_db: str = "",
) -> Optional[dict[str, Any]]:
    sales = _sales_from_comp_records(records)
    if not sales:
        return None
    prices = [float(sale["price"]) for sale in sales if _safe_float(sale.get("price")) is not None]
    suggested = _suggest_price_from_sales(sales)
    if suggested is None:
        return None
    for sale in sales:
        sale.setdefault("source", source)
        sale.setdefault("sources", [source])
        sale.setdefault("source_details", [source_detail])
    raw: dict[str, Any] = {
        "query": query,
        "sales_history_url": sales_history_url,
        "source_detail": source_detail,
        "sample_count": len(prices),
        "sales": sales[:20],
    }
    if cache_db:
        raw["cache_db"] = cache_db
    return {
        "source": source,
        "market_price": suggested,
        "low_price": round(min(prices), 2) if prices else None,
        "high_price": round(max(prices), 2) if prices else None,
        "raw": raw,
    }


async def sync_card_ladder_cli_for_item(
    item: InventoryItem,
    *,
    timeout_seconds: int = 120,
    limit: int = 25,
    headless: bool = True,
) -> dict[str, Any]:
    """Refresh one slab query through the Card Ladder CLI and return cached comps."""
    try:
        from scripts import cardladder_cli
    except Exception as exc:
        raise RuntimeError(f"Card Ladder CLI is unavailable: {exc}") from exc

    repo_root = Path(__file__).resolve().parent.parent
    script_path = repo_root / "scripts" / "cardladder_cli.py"
    if not script_path.exists():
        raise RuntimeError(f"Card Ladder CLI script is missing: {script_path}")

    base_query = _card_ladder_cli_base_query(item)
    grader = (item.grading_company or "").strip().upper()
    grade = str(item.grade or "").strip()
    cert = str(item.cert_number or "").strip()
    if not base_query and not cert:
        raise RuntimeError("Card name or cert number is required before refreshing Card Ladder.")

    cmd = [sys.executable, str(script_path), "sync"]
    if base_query:
        cmd.append(base_query)
    if grader:
        cmd.extend(["--grader", grader])
    if grade:
        cmd.extend(["--grade", grade])
    if cert:
        cmd.extend(["--cert", cert])
    cmd.extend(["--limit", str(max(1, int(limit)))])
    if headless:
        cmd.append("--headless")
    cmd.append("--no-prompt")

    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=str(repo_root),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout_bytes, stderr_bytes = await asyncio.wait_for(
            process.communicate(),
            timeout=max(5, int(timeout_seconds)),
        )
    except asyncio.TimeoutError as exc:
        raise RuntimeError("Card Ladder refresh timed out. Run the CLI login command, then try again.") from exc

    stdout = stdout_bytes.decode("utf-8", errors="replace").strip()
    stderr = stderr_bytes.decode("utf-8", errors="replace").strip()
    if process.returncode != 0:
        detail = stderr or stdout or f"Card Ladder CLI exited with code {process.returncode}."
        raise RuntimeError(detail[:1000])

    query = cardladder_cli.build_slab_query(
        base_query,
        grader=grader,
        grade=grade,
        cert=cert,
        strict=True,
    )
    result = _fetch_card_ladder_cli_cache(item, query=query, limit=limit)
    if not result:
        raise RuntimeError("Card Ladder CLI finished, but no comps were saved for this slab query.")
    return result


def import_card_ladder_cli_records_for_item(
    item: InventoryItem,
    *,
    text: str,
    query: str = "",
    cache_path: str | Path | None = None,
) -> dict[str, Any]:
    """Parse pasted/exported sold rows into the Card Ladder CLI cache."""
    try:
        from scripts import cardladder_cli
    except Exception as exc:
        raise RuntimeError(f"Card Ladder CLI parser is unavailable: {exc}") from exc

    records = _card_ladder_records_from_text(text)
    if not records:
        raise RuntimeError("No sold comps were recognized in the pasted text.")

    grader = (item.grading_company or "").strip().upper()
    grade = str(item.grade or "").strip()
    cert = str(item.cert_number or "").strip()
    for record in records:
        if grader and not getattr(record, "grader", ""):
            record.grader = grader
        if grade and not getattr(record, "grade", ""):
            record.grade = grade
        if cert and not getattr(record, "cert", ""):
            record.cert = cert

    resolved_query = query or build_card_ladder_cli_query(item)
    cache_db = Path(cache_path) if cache_path else cardladder_cli.default_cache_path()
    saved = cardladder_cli.cache_records(cache_db, resolved_query, records)
    result = _fetch_card_ladder_cli_cache(
        item,
        query=resolved_query,
        cache_path=cache_db,
        limit=max(20, len(records)),
    )
    if not result:
        raise RuntimeError("Comps were parsed, but could not be read back from the Card Ladder cache.")
    raw = result.setdefault("raw", {})
    if isinstance(raw, dict):
        raw["source_detail"] = "card_ladder_manual_import"
        raw["imported_count"] = saved
    return result


def _card_ladder_records_from_text(text: str) -> list[Any]:
    try:
        from scripts import cardladder_cli
    except Exception as exc:
        raise RuntimeError(f"Card Ladder CLI parser is unavailable: {exc}") from exc

    raw = (text or "").strip()
    if not raw:
        return []

    records: list[Any] = []
    records.extend(_card_ladder_records_from_csv(raw, cardladder_cli))

    chunks = [chunk.strip() for chunk in re.split(r"\n\s*\n", raw) if chunk.strip()]
    for chunk in chunks:
        record = cardladder_cli.record_from_text(chunk)
        if record:
            records.append(record)

    if not records:
        lines = [line.strip() for line in raw.splitlines() if line.strip()]
        for chunk in _sliding_text_chunks(lines):
            record = cardladder_cli.record_from_text(chunk)
            if record:
                records.append(record)

    return cardladder_cli.dedupe_records(records)


def _card_ladder_records_from_csv(text: str, cardladder_cli: Any) -> list[Any]:
    first_line = text.splitlines()[0] if text.splitlines() else ""
    if "," not in first_line and "\t" not in first_line:
        return []
    try:
        sample = text[:2048]
        dialect = csv.Sniffer().sniff(sample, delimiters=",\t")
    except Exception:
        dialect = csv.excel_tab if "\t" in first_line else csv.excel
    try:
        reader = csv.DictReader(io.StringIO(text), dialect=dialect)
        records = []
        for row in reader:
            record = cardladder_cli.record_from_json(dict(row))
            if record:
                records.append(record)
        return records
    except Exception:
        return []


def _sliding_text_chunks(lines: list[str]) -> list[str]:
    chunks: list[str] = []
    if not lines:
        return chunks
    for size in range(3, min(8, len(lines)) + 1):
        for index in range(0, len(lines) - size + 1):
            window = lines[index:index + size]
            if any("$" in line for line in window):
                chunks.append("\n".join(window))
    if not chunks:
        chunks.extend(lines)
    return chunks


async def fetch_slab_price(
    item: InventoryItem,
    client: httpx.AsyncClient,
    *,
    source_filter: str = "all",
) -> Optional[dict[str, Any]]:
    """
    Collect slab comps from every available source and dedupe overlapping solds.
    """
    selected_source = normalize_slab_price_source(source_filter)
    results: list[dict[str, Any]] = []

    if _wants_slab_source(selected_source, "card_ladder"):
        cli_query = build_card_ladder_cli_query(item)
        result = _fetch_card_ladder_cli_cache(item, query=cli_query)
        if result:
            results.append(result)

        if not result or selected_source == "all":
            card_ladder_query = build_card_ladder_slab_query(item)
            result = await _fetch_card_ladder_price(item, client, card_ladder_query)
            if result:
                results.append(result)

    query = _slab_query(item)
    if _wants_slab_source(selected_source, "alt"):
        result = await _fetch_alt_price(item, client, query)
        if result:
            results.append(result)

    if _wants_slab_source(selected_source, "130point"):
        result = await _fetch_130point_price(item, client, query)
        if result:
            results.append(result)

    if _wants_slab_source(selected_source, "myslabs"):
        result = await _fetch_myslabs_price(item, client, query)
        if result:
            results.append(result)

    if _wants_slab_source(selected_source, "pricecharting"):
        result = await _fetch_pricecharting_price(item, client, query)
        if result:
            results.append(result)

    if selected_source != "all":
        return results[0] if results else None

    return combine_slab_price_results(item, results)


def normalize_slab_price_source(source: Any) -> str:
    cleaned = str(source or "").strip().lower().replace("-", "_")
    normalized = SLAB_PRICE_SOURCE_ALIASES.get(cleaned, cleaned)
    return normalized if normalized in SLAB_PRICE_SOURCE_OPTIONS else "all"


def _wants_slab_source(selected_source: str, source: str) -> bool:
    return selected_source == "all" or selected_source == source


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
    point_query = build_130point_cli_query(item)
    cached = _fetch_130point_cli_cache(item, query=point_query)
    if cached:
        return cached

    try:
        from scripts import point130_cli

        records = await asyncio.to_thread(point130_cli.fetch_records, point_query, limit=20)
        if records:
            await asyncio.to_thread(point130_cli.cache_records, point130_cli.default_cache_path(), point_query, records)
        return _result_from_comp_records(
            point_query,
            records,
            source="130point",
            source_detail="130point_live",
            sales_history_url=point130_sales_history_url(point_query),
        )
    except Exception as exc:
        logger.debug("[pricing] 130point failed for %s: %s", point_query or query, exc)
        return None


async def _fetch_alt_price(
    item: InventoryItem,
    client: httpx.AsyncClient,
    query: str,
) -> Optional[dict[str, Any]]:
    """ALT sold-listing search using the same web-app Typesense service."""
    alt_query = build_alt_cli_query(item)
    cached = _fetch_alt_cli_cache(item, query=alt_query)
    if cached:
        return cached

    try:
        return await sync_alt_cli_for_item(item, limit=20)
    except Exception as exc:
        logger.debug("[pricing] alt failed for %s: %s", alt_query or query, exc)
        return None


async def _fetch_myslabs_price(
    item: InventoryItem,
    client: httpx.AsyncClient,
    query: str,
) -> Optional[dict[str, Any]]:
    """MySlabs public sold archive for slabbed card sales."""
    myslabs_query = build_myslabs_query(item)
    try:
        resp = await client.get(
            MYSLABS_ARCHIVE_SEARCH_URL,
            params={"publish_type": "0", "q": myslabs_query, "o": "created_desc"},
            timeout=20.0,
            follow_redirects=True,
            headers={
                "Accept": "text/html,application/xhtml+xml",
                "User-Agent": "Mozilla/5.0 (compatible; DegenCollectibles/1.0)",
            },
        )
        if resp.status_code != 200:
            return None
        sales = _myslabs_sales_from_html(resp.text, limit=20)
        if not sales:
            return None
        prices = [float(sale["price"]) for sale in sales if _safe_float(sale.get("price")) is not None]
        suggested = _suggest_price_from_sales(sales)
        if suggested is None:
            return None
        return {
            "source": "myslabs",
            "market_price": suggested,
            "low_price": round(min(prices), 2) if prices else None,
            "high_price": round(max(prices), 2) if prices else None,
            "raw": {
                "query": myslabs_query,
                "sales_history_url": str(resp.url),
                "source_detail": "myslabs_archive",
                "sample_count": len(prices),
                "sales": sales[:20],
            },
        }
    except Exception as exc:
        logger.debug("[pricing] myslabs failed for %s: %s", myslabs_query or query, exc)
        return None


def build_myslabs_query(item: InventoryItem) -> str:
    card_number = str(item.card_number or "").strip()
    if "/" in card_number:
        card_number = card_number.split("/", 1)[0].strip()
    parts = [
        item.card_name or "",
        item.set_name or "",
        card_number,
        item.grading_company or "",
        item.grade or "",
    ]
    return " ".join(str(part).strip() for part in parts if str(part or "").strip())


def _myslabs_sales_from_html(text: str, *, limit: int = 20) -> list[dict[str, Any]]:
    sales: list[dict[str, Any]] = []
    for block in re.split(r'(?=<div class="slab_item\b)', text or "")[1:]:
        end = block.find('<script type="application/ld+json"')
        if end != -1:
            block = block[:end]
        title_m = re.search(r'<div class="slab-title">\s*(.*?)\s*</div>', block, flags=re.IGNORECASE | re.DOTALL)
        price_m = re.search(r'<div class="item-price">\s*\$([^<]+)', block, flags=re.IGNORECASE | re.DOTALL)
        date_m = re.search(r'<small class="[^"]*">\s*([^<]+?)\s*</small>', block, flags=re.IGNORECASE | re.DOTALL)
        href_m = re.search(r'<a href="([^"]+)"', block, flags=re.IGNORECASE)
        image_m = re.search(r'(?:data-src|src)="([^"]+)"', block, flags=re.IGNORECASE)
        if not title_m or not price_m or not date_m:
            continue
        price = _safe_float(price_m.group(1))
        if price is None:
            continue
        title = html_lib.unescape(re.sub(r"<[^>]+>", " ", title_m.group(1)))
        title = re.sub(r"\s+", " ", title).strip()
        sold_date = _myslabs_sale_date(date_m.group(1))
        url = urljoin("https://myslabs.com", html_lib.unescape(href_m.group(1)).strip()) if href_m else ""
        image_url = html_lib.unescape(image_m.group(1)).strip() if image_m else ""
        sales.append(
            {
                "title": title,
                "price": round(price, 2),
                "sold_date": sold_date,
                "platform": "MySlabs",
                "sale_type": "",
                "url": url,
                "image_url": image_url,
                "sources": ["myslabs"],
                "source_details": ["myslabs_archive"],
            }
        )
        if len(sales) >= limit:
            break
    return sales


def _myslabs_sale_date(value: Any) -> str:
    raw = html_lib.unescape(str(value or "")).strip()
    for fmt in ("%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue
    return _normalize_sale_date(raw)


async def _fetch_pricecharting_price(
    item: InventoryItem,
    client: httpx.AsyncClient,
    query: str,
) -> Optional[dict[str, Any]]:
    """PriceCharting public Pokemon page fallback for graded slab sold listings."""
    product_url = _pricecharting_product_url(item)
    if not product_url:
        return None
    try:
        resp = await client.get(
            product_url,
            timeout=20.0,
            follow_redirects=True,
            headers={
                "Accept": "text/html,application/xhtml+xml",
                "User-Agent": "Mozilla/5.0 (compatible; DegenCollectibles/1.0)",
            },
        )
        if resp.status_code != 200:
            return None
        sales = _pricecharting_sales_from_html(resp.text, item)
        if not sales:
            return None
        prices = [float(s["price"]) for s in sales if _safe_float(s.get("price")) is not None]
        suggested = _suggest_price_from_sales(sales)
        if suggested is None:
            return None
        return {
            "source": "pricecharting",
            "market_price": suggested,
            "low_price": round(min(prices), 2) if prices else None,
            "high_price": round(max(prices), 2) if prices else None,
            "raw": {
                "query": query,
                "product_url": str(resp.url),
                "source_detail": "pricecharting",
                "sample_count": len(prices),
                "sales": sales[:20],
            },
        }
    except Exception as exc:
        logger.debug("[pricing] pricecharting failed for %s: %s", query, exc)
        return None


def _pricecharting_product_url(item: InventoryItem) -> Optional[str]:
    if (item.game or "").strip().lower() not in {"pokemon", "pokémon"}:
        return None
    if not item.card_name or not item.set_name or not item.card_number:
        return None
    set_slug = _slugify_for_pricecharting(item.set_name)
    name_slug = _slugify_for_pricecharting(item.card_name)
    number = str(item.card_number or "").split("/", 1)[0].strip()
    number_slug = _slugify_for_pricecharting(number)
    if not set_slug or not name_slug or not number_slug:
        return None
    return f"{PRICECHARTING_GAME_URL}/pokemon-{set_slug}/{name_slug}-{number_slug}"


def _slugify_for_pricecharting(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", str(value or "").lower()).strip("-")
    return re.sub(r"-{2,}", "-", cleaned)


def _pricecharting_sales_from_html(text: str, item: InventoryItem) -> list[dict[str, Any]]:
    label = _pricecharting_grade_label(item)
    completed_class = _pricecharting_completed_class_for_label(text, label)
    if not completed_class and label.startswith("Grade "):
        completed_class = _pricecharting_completed_class_for_label(text, label.replace("Grade ", ""))
    if not completed_class:
        return []
    block = _pricecharting_completed_block(text, completed_class)
    if not block:
        return []

    sales: list[dict[str, Any]] = []
    for row in re.findall(r"<tr\b[^>]*>(.*?)</tr>", block, flags=re.IGNORECASE | re.DOTALL):
        date_m = re.search(r'<td[^>]*class="[^"]*\bdate\b[^"]*"[^>]*>\s*([^<]+)', row, flags=re.IGNORECASE)
        price_m = re.search(r'<span[^>]*class="[^"]*\bjs-price\b[^"]*"[^>]*>\s*([^<]+)', row, flags=re.IGNORECASE)
        title_td = re.search(r'<td[^>]*class="[^"]*\btitle\b[^"]*"[^>]*>(.*?)</td>', row, flags=re.IGNORECASE | re.DOTALL)
        if not date_m or not price_m or not title_td:
            continue
        price = _safe_float(price_m.group(1))
        if price is None:
            continue
        title_html = title_td.group(1)
        href_m = re.search(r'href="([^"]+)"', title_html, flags=re.IGNORECASE)
        platform_m = re.search(r"\[([^\]]+)\]", title_html)
        title = re.sub(r"<[^>]+>", " ", title_html)
        title = re.sub(r"\[[^\]]+\]", " ", title)
        title = html_lib.unescape(re.sub(r"\s+", " ", title)).strip()
        sales.append(
            {
                "title": title,
                "price": round(price, 2),
                "sold_date": _normalize_sale_date(date_m.group(1).strip()),
                "platform": platform_m.group(1).strip() if platform_m else "PriceCharting",
                "sale_type": "",
                "url": html_lib.unescape(href_m.group(1)).strip() if href_m else "",
            }
        )
        if len(sales) >= 20:
            break
    return sales


def _pricecharting_grade_label(item: InventoryItem) -> str:
    grader = (item.grading_company or "").strip().upper()
    grade = str(item.grade or "").strip()
    if grader and grade == "10":
        return f"{grader} 10"
    return f"Grade {grade}" if grade else ""


def _pricecharting_completed_class_for_label(text: str, target_label: str) -> str:
    if not target_label:
        return ""
    select_m = re.search(
        r'<select[^>]+id="completed-auctions-condition"[^>]*>(.*?)</select>',
        text or "",
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not select_m:
        return ""
    target = target_label.strip().lower()
    for value, label in re.findall(r'<option[^>]+value="([^"]*)"[^>]*>\s*([^<]+)</option>', select_m.group(1)):
        display = html_lib.unescape(label).strip()
        display = re.sub(r"\s+\(\d+\)$", "", display).strip()
        if display.lower() == target:
            return value.strip()
    return ""


def _pricecharting_completed_block(text: str, completed_class: str) -> str:
    class_pat = re.escape(completed_class)
    start_matches = list(re.finditer(
        rf'<div[^>]+class="[^"]*\b{class_pat}\b[^"]*"[^>]*>',
        text or "",
        flags=re.IGNORECASE,
    ))
    for start_m in start_matches:
        table_end = (text or "").find("</table>", start_m.end())
        if table_end == -1:
            continue
        block = (text or "")[start_m.end():table_end]
        if re.search(r'<td[^>]*class="[^"]*\bdate\b', block, flags=re.IGNORECASE) and "js-price" in block:
            return block
    return ""


async def _fetch_card_ladder_price(
    item: InventoryItem,
    client: httpx.AsyncClient,
    query: str,
) -> Optional[dict[str, Any]]:
    """Card Ladder graded card price history."""
    headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "DegenCollectibles/1.0",
    }
    sales_url = card_ladder_sales_history_url(query)
    try:
        for endpoint, params in (
            (CARD_LADDER_SEARCH_URL, {"q": query}),
            (CARD_LADDER_CARDS_SEARCH_URL, {"q": query, "limit": "20"}),
        ):
            resp = await client.get(endpoint, params=params, timeout=15.0, headers=headers)
            if resp.status_code != 200:
                continue
            data: Any
            try:
                data = resp.json()
            except Exception:
                data = resp.text
            result = _card_ladder_result_from_payload(query, data, sales_url=sales_url)
            if result:
                return result

        resp = await client.get(
            CARD_LADDER_APP_SALES_HISTORY_URL,
            params={"sort": "date", "direction": "desc", "q": query},
            timeout=15.0,
            headers={"Accept": "text/html,application/xhtml+xml", "User-Agent": "DegenCollectibles/1.0"},
        )
        if resp.status_code == 200:
            return _card_ladder_result_from_payload(query, resp.text, sales_url=sales_url)
    except Exception as exc:
        logger.debug("[pricing] card_ladder failed for %s: %s", query, exc)
    return None


def _card_ladder_result_from_payload(
    query: str,
    payload: Any,
    *,
    sales_url: str,
) -> Optional[dict[str, Any]]:
    sales = _card_ladder_sales_from_payload(payload)
    if sales:
        prices = [float(s["price"]) for s in sales if _safe_float(s.get("price")) is not None]
        suggested = _suggest_price_from_sales(sales)
        if suggested is None:
            return None
        return {
            "source": "card_ladder",
            "market_price": suggested,
            "low_price": round(min(prices), 2) if prices else None,
            "high_price": round(max(prices), 2) if prices else None,
            "raw": {
                "query": query,
                "sales_history_url": sales_url,
                "sample_count": len(prices),
                "sales": sales[:20],
            },
        }

    market_row = _card_ladder_market_row(payload)
    if not market_row:
        return None
    market = _safe_float(
        market_row.get("market_price")
        or market_row.get("avg_price")
        or market_row.get("average_price")
        or market_row.get("price")
    )
    if market is None:
        return None
    low = _safe_float(market_row.get("low_price") or market_row.get("low"))
    high = _safe_float(market_row.get("high_price") or market_row.get("high"))
    return {
        "source": "card_ladder",
        "market_price": round(market, 2),
        "low_price": round(low, 2) if low else None,
        "high_price": round(high, 2) if high else None,
        "raw": {
            "query": query,
            "sales_history_url": sales_url,
            "sample_count": 0,
            "market_row": {
                key: market_row.get(key)
                for key in ("title", "name", "market_price", "avg_price", "price", "low_price", "high_price")
                if key in market_row
            },
        },
    }


def _card_ladder_sales_from_payload(payload: Any) -> list[dict[str, Any]]:
    sales: list[dict[str, Any]] = []
    seen: set[tuple[str, float, str]] = set()
    for item in _walk_payload_mappings(payload):
        sale = _card_ladder_sale_from_mapping(item)
        if not sale:
            continue
        key = (str(sale.get("sold_date") or ""), float(sale["price"]), str(sale.get("title") or ""))
        if key in seen:
            continue
        seen.add(key)
        sales.append(sale)
        if len(sales) >= 50:
            break
    return sales


def _card_ladder_sale_from_mapping(item: dict[str, Any]) -> Optional[dict[str, Any]]:
    price = _safe_float(
        item.get("price")
        or item.get("sale_price")
        or item.get("sold_price")
        or item.get("soldPrice")
        or item.get("amount")
    )
    sold_date = (
        item.get("sold_date")
        or item.get("soldDate")
        or item.get("date_sold")
        or item.get("dateSold")
        or item.get("sold_at")
        or item.get("date")
    )
    if price is None or not sold_date:
        return None
    title = (
        item.get("title")
        or item.get("listing_title")
        or item.get("listingTitle")
        or item.get("name")
        or item.get("card_name")
        or ""
    )
    return {
        "title": str(title).strip(),
        "price": round(price, 2),
        "sold_date": _normalize_sale_date(sold_date),
        "platform": str(item.get("platform") or item.get("source") or "").strip(),
        "sale_type": str(item.get("sale_type") or item.get("type") or item.get("listing_type") or "").strip(),
        "url": str(item.get("url") or item.get("href") or item.get("link") or "").strip(),
    }


def _card_ladder_market_row(payload: Any) -> Optional[dict[str, Any]]:
    for item in _walk_payload_mappings(payload):
        if not any(key in item for key in ("market_price", "avg_price", "average_price", "price")):
            continue
        if _safe_float(item.get("market_price") or item.get("avg_price") or item.get("average_price") or item.get("price")):
            return item
    return None


def _walk_payload_mappings(payload: Any, *, depth: int = 0) -> list[dict[str, Any]]:
    if depth > 8:
        return []
    if isinstance(payload, str):
        return _walk_payload_mappings(_json_candidates_from_text(payload), depth=depth + 1)
    if isinstance(payload, dict):
        rows = [payload]
        for value in payload.values():
            rows.extend(_walk_payload_mappings(value, depth=depth + 1))
        return rows
    if isinstance(payload, list):
        rows: list[dict[str, Any]] = []
        for value in payload:
            rows.extend(_walk_payload_mappings(value, depth=depth + 1))
        return rows
    return []


def _json_candidates_from_text(text: str) -> list[Any]:
    candidates: list[Any] = []
    for pattern in (
        r'<script[^>]+id="__NEXT_DATA__"[^>]*>\s*(\{.*?\})\s*</script>',
        r'<script[^>]+type="application/json"[^>]*>\s*(\{.*?\})\s*</script>',
    ):
        for match in re.finditer(pattern, text or "", re.IGNORECASE | re.DOTALL):
            try:
                candidates.append(json.loads(match.group(1)))
            except Exception:
                continue
    return candidates


def _normalize_sale_date(value: Any) -> str:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    raw = str(value or "").strip()
    iso_match = re.match(r"^(\d{4}-\d{2}-\d{2})", raw)
    if iso_match:
        return iso_match.group(1)
    return raw


def _sale_date(value: Any) -> Optional[date]:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raw = str(value or "").strip()
    iso_match = re.match(r"^(\d{4}-\d{2}-\d{2})", raw)
    if iso_match:
        try:
            return date.fromisoformat(iso_match.group(1))
        except ValueError:
            return None
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def _suggest_price_from_sales(sales: list[dict[str, Any]]) -> Optional[float]:
    if not sales:
        return None
    for sale in sales:
        latest_price = _safe_float(sale.get("price"))
        if latest_price is None:
            continue
        latest_date = _sale_date(sale.get("sold_date") or sale.get("date"))
        if latest_date and (utcnow().date() - latest_date).days > STALE_SLAB_COMP_DAYS:
            return round(latest_price, 2)
        break
    weighted: list[float] = []
    for index, sale in enumerate(sales[:5]):
        price = _safe_float(sale.get("price"))
        if price is None:
            continue
        weighted.extend([price] * (2 if index == 0 else 1))
    if not weighted:
        return None
    weighted.sort()
    mid = len(weighted) // 2
    if len(weighted) % 2 == 0:
        return round((weighted[mid - 1] + weighted[mid]) / 2, 2)
    return round(weighted[mid], 2)


def combine_slab_price_results(
    item: InventoryItem,
    results: list[dict[str, Any]],
) -> Optional[dict[str, Any]]:
    usable = [result for result in results if isinstance(result, dict)]
    if not usable:
        return None

    all_sales: list[dict[str, Any]] = []
    source_results: list[dict[str, Any]] = []
    source_urls: dict[str, str] = {}
    for result in usable:
        source = str(result.get("source") or "unknown")
        raw = result.get("raw") if isinstance(result.get("raw"), dict) else {}
        source_detail = str(raw.get("source_detail") or source)
        source_results.append(
            {
                "source": source,
                "source_detail": source_detail,
                "market_price": result.get("market_price"),
                "sample_count": raw.get("sample_count"),
                "query": raw.get("query"),
            }
        )
        url = str(raw.get("sales_history_url") or raw.get("product_url") or "")
        if url:
            source_urls[source] = url
        for sale in _sales_from_price_result(result):
            sale_sources = _clean_sources(sale.get("sources")) or [source]
            if source not in sale_sources:
                sale_sources.append(source)
            details = _clean_sources(sale.get("source_details")) or [source_detail]
            if source_detail not in details:
                details.append(source_detail)
            sale["sources"] = sale_sources
            sale["source_details"] = details
            sale["source"] = "+".join(sale_sources)
            all_sales.append(sale)

    merged_sales = _dedupe_slab_sales(all_sales)
    if not merged_sales:
        return None
    prices = [float(sale["price"]) for sale in merged_sales if _safe_float(sale.get("price")) is not None]
    suggested = _suggest_price_from_sales(merged_sales)
    if suggested is None:
        return None
    sources = sorted({source for sale in merged_sales for source in _clean_sources(sale.get("sources"))})
    return {
        "source": "slab_comps",
        "market_price": suggested,
        "low_price": round(min(prices), 2) if prices else None,
        "high_price": round(max(prices), 2) if prices else None,
        "raw": {
            "query": build_alt_cli_query(item),
            "source_detail": "multi_source_comps",
            "sources": sources,
            "source_urls": source_urls,
            "source_results": source_results,
            "sales_history_url": next(iter(source_urls.values()), ""),
            "sample_count": len(prices),
            "sales": merged_sales[:30],
        },
    }


def _sales_from_price_result(result: dict[str, Any]) -> list[dict[str, Any]]:
    source = str(result.get("source") or "")
    raw = result.get("raw") if isinstance(result.get("raw"), dict) else {}
    sales = raw.get("sales")
    if not isinstance(sales, list):
        return []
    out: list[dict[str, Any]] = []
    for sale in sales:
        if not isinstance(sale, dict):
            continue
        price = _safe_float(sale.get("price"))
        if price is None:
            continue
        copied = dict(sale)
        copied["price"] = round(price, 2)
        copied["sold_date"] = _normalize_sale_date(copied.get("sold_date") or copied.get("date"))
        copied.setdefault("source", source)
        copied.setdefault("sources", [source] if source else [])
        out.append(copied)
    return out


def _clean_sources(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(source).strip() for source in value if str(source or "").strip()]
    if isinstance(value, str) and value.strip():
        return [part.strip() for part in value.split("+") if part.strip()]
    return []


def _dedupe_slab_sales(sales: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[tuple[str, ...], dict[str, Any]] = {}
    order: list[tuple[str, ...]] = []
    for sale in sales:
        key = _slab_sale_dedupe_key(sale)
        if key not in merged:
            merged[key] = dict(sale)
            merged[key]["sources"] = _clean_sources(sale.get("sources"))
            merged[key]["source_details"] = _clean_sources(sale.get("source_details"))
            order.append(key)
            continue
        current = merged[key]
        current["sources"] = sorted(set(_clean_sources(current.get("sources")) + _clean_sources(sale.get("sources"))))
        current["source_details"] = sorted(
            set(_clean_sources(current.get("source_details")) + _clean_sources(sale.get("source_details")))
        )
        current["source"] = "+".join(current["sources"])
        if not current.get("url") and sale.get("url"):
            current["url"] = sale.get("url")
        if not current.get("image_url") and sale.get("image_url"):
            current["image_url"] = sale.get("image_url")
        if len(str(sale.get("title") or "")) > len(str(current.get("title") or "")):
            current["title"] = sale.get("title")
    rows = [merged[key] for key in order]
    rows.sort(key=_slab_sale_sort_key, reverse=True)
    for row in rows:
        row["sources"] = sorted(set(_clean_sources(row.get("sources"))))
        row["source_details"] = sorted(set(_clean_sources(row.get("source_details"))))
        row["source"] = "+".join(row["sources"])
    return rows


def _slab_sale_dedupe_key(sale: dict[str, Any]) -> tuple[str, ...]:
    url_key = _normalized_listing_url(sale.get("url"))
    if url_key:
        return ("url", url_key)
    date = str(sale.get("sold_date") or sale.get("date") or "").strip().lower()
    price = _safe_float(sale.get("price"))
    price_key = f"{price:.2f}" if price is not None else ""
    title_key = _normalized_sale_title(sale.get("title"))
    platform = str(sale.get("platform") or "").strip().lower()
    return ("listing", date, price_key, platform, title_key)


def _normalized_listing_url(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    ebay = re.search(r"(?:itm/|item=)(\d{9,15})", raw, flags=re.IGNORECASE)
    if ebay:
        return f"ebay:{ebay.group(1)}"
    cleaned = re.sub(r"[?#].*$", "", raw.lower())
    cleaned = cleaned.replace("https://", "").replace("http://", "").replace("www.", "")
    return cleaned.rstrip("/")


def _normalized_sale_title(value: Any) -> str:
    text = html_lib.unescape(str(value or "").lower())
    text = text.replace("opens in a new window or tab", " ")
    text = re.sub(r"\bnew listing\b", " ", text)
    tokens = re.findall(r"[a-z0-9]+", text)
    return " ".join(tokens[:14])


def _slab_sale_sort_key(sale: dict[str, Any]) -> tuple[str, float]:
    date = str(sale.get("sold_date") or sale.get("date") or "")
    iso = re.match(r"^(\d{4}-\d{2}-\d{2})", date)
    date_key = iso.group(1) if iso else date
    return (date_key, float(_safe_float(sale.get("price")) or 0.0))


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
        cleaned = re.sub(r"[^\d.]", "", str(value))
        if not cleaned:
            return None
        f = float(cleaned)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


def price_result_to_json(result: dict[str, Any]) -> str:
    """Serialise a price result dict to JSON string for storage in raw_response_json."""
    return json.dumps(result.get("raw") or result, default=str, separators=(",", ":"))


def apply_slab_resticker_alert(
    item: InventoryItem,
    *,
    suggested_price: Any,
    previous_effective_price: Optional[float] = None,
    min_percent: float = 10.0,
    min_dollars: float = 10.0,
    source: str = "card_ladder",
) -> str:
    """
    Mutate a slab item with a resticker alert when market moves above sticker.

    Returns "created", "updated", "cleared", or "none" so callers can decide
    whether to emit notifications.
    """
    if item.item_type != ITEM_TYPE_SLAB:
        return "none"
    target = _safe_float(suggested_price)
    if target is None:
        return "none"
    dismissed_target = _safe_float(item.resticker_alert_price)
    if (
        not item.resticker_alert_active
        and item.resticker_resolved_at is not None
        and dismissed_target is not None
        and target - dismissed_target < max(0.0, min_dollars)
    ):
        return "none"

    reference = _sticker_reference_price(item, previous_effective_price=previous_effective_price)
    if reference is None:
        return "none"

    increase = round(target - reference, 2)
    percent = (increase / reference * 100.0) if reference > 0 else 0.0
    if increase < max(0.0, min_dollars) or percent < max(0.0, min_percent):
        if item.resticker_alert_active and target <= reference:
            clear_slab_resticker_alert(item, reason="Sticker price caught up to the latest slab comp.")
            return "cleared"
        return "none"

    was_active = bool(item.resticker_alert_active)
    previous_alert_price = _safe_float(item.resticker_alert_price)
    should_notify_update = (
        was_active
        and previous_alert_price is not None
        and target - previous_alert_price >= max(0.0, min_dollars)
    )

    item.resticker_alert_active = True
    item.resticker_alerted_at = utcnow()
    item.resticker_resolved_at = None
    item.resticker_reference_price = round(reference, 2)
    item.resticker_alert_price = round(target, 2)
    item.resticker_alert_reason = (
        f"{source.replace('_', ' ').title()} suggested ${target:,.2f}, "
        f"up ${increase:,.2f} ({percent:.1f}%) from sticker ${reference:,.2f}."
    )
    if not was_active:
        return "created"
    return "updated" if should_notify_update else "none"


def clear_slab_resticker_alert(item: InventoryItem, *, reason: str = "") -> None:
    item.resticker_alert_active = False
    item.resticker_resolved_at = utcnow()
    if reason:
        item.resticker_alert_reason = reason


def _sticker_reference_price(
    item: InventoryItem,
    *,
    previous_effective_price: Optional[float],
) -> Optional[float]:
    for candidate in (item.list_price, previous_effective_price, item.resticker_reference_price):
        value = _safe_float(candidate)
        if value is not None:
            return round(value, 2)
    return None
