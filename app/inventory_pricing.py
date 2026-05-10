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
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode

import httpx

from .models import InventoryItem, ITEM_TYPE_SLAB, utcnow

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
PRICECHARTING_GAME_URL = "https://www.pricecharting.com/game"

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
) -> Optional[dict[str, Any]]:
    """
    Try slab pricing sources in order: Card Ladder -> 130point -> Alt.
    Returns the first successful result, or None if all fail.
    """
    cli_query = build_card_ladder_cli_query(item)
    result = _fetch_card_ladder_cli_cache(item, query=cli_query)
    if result:
        return result

    card_ladder_query = build_card_ladder_slab_query(item)
    result = await _fetch_card_ladder_price(item, client, card_ladder_query)
    if result:
        return result

    query = _slab_query(item)
    result = await _fetch_130point_price(item, client, query)
    if result:
        return result

    result = await _fetch_alt_price(item, client, query)
    if result:
        return result

    result = await _fetch_pricecharting_price(item, client, query)
    if result:
        return result

    return None


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


def _suggest_price_from_sales(sales: list[dict[str, Any]]) -> Optional[float]:
    if not sales:
        return None
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
