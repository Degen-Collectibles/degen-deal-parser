"""130point sold-comps CLI with local cache support."""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Iterable

import httpx

try:
    from scripts import cardladder_cli as comp_cache
except ModuleNotFoundError:  # pragma: no cover - direct script execution.
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import cardladder_cli as comp_cache  # type: ignore[no-redef]

POINT130_SEARCH_URL = "https://www.130point.com/sales/search"
GRADERS = ("PSA", "BGS", "SGC", "CGC")


def default_cache_path() -> Path:
    env = os.environ.get("POINT130_CACHE_DB")
    if env:
        return Path(env)
    local_app_data = os.environ.get("LOCALAPPDATA")
    if local_app_data:
        return Path(local_app_data) / "DegenCollectibles" / "130point-cli" / "comps.sqlite"
    return Path.home() / ".degen-collectibles" / "130point-cli" / "comps.sqlite"


def build_slab_query(
    query: str = "",
    *,
    grader: str = "",
    grade: str = "",
    cert: str = "",
    card_number: str = "",
) -> str:
    parts: list[str] = []
    seen: set[str] = set()
    for value in (query, card_number, grader.strip().upper(), grade.strip(), cert.strip()):
        cleaned = str(value or "").strip()
        key = cleaned.lower()
        if cleaned and key not in seen:
            parts.append(cleaned)
            seen.add(key)
    return " ".join(parts)


def sales_search_url(query: str) -> str:
    from urllib.parse import urlencode

    return f"{POINT130_SEARCH_URL}?{urlencode({'q': query})}"


def fetch_records(query: str, *, limit: int = 20, client: httpx.Client | None = None) -> list[comp_cache.CompRecord]:
    close_client = client is None
    client = client or httpx.Client(timeout=25.0, follow_redirects=True)
    try:
        resp = client.get(
            POINT130_SEARCH_URL,
            params={"q": query, "output": "json"},
            headers={
                "Accept": "application/json, text/plain, */*",
                "User-Agent": "Mozilla/5.0 (compatible; DegenCollectibles/1.0)",
            },
        )
        if resp.status_code == 403 and "cloudflare" in resp.text.lower():
            raise RuntimeError("130point returned 403 Cloudflare verification.")
        resp.raise_for_status()
        try:
            payload: Any = resp.json()
        except Exception:
            payload = resp.text
        records = records_from_payload(payload)
        return comp_cache.dedupe_records(records)[: max(1, int(limit))]
    finally:
        if close_client:
            client.close()


def records_from_payload(payload: Any) -> list[comp_cache.CompRecord]:
    records: list[comp_cache.CompRecord] = []
    if isinstance(payload, str):
        records.extend(records_from_html(payload))
        return records
    for row in walk_mappings(payload):
        record = record_from_mapping(row)
        if record:
            records.append(record)
    return records


def records_from_html(text: str) -> list[comp_cache.CompRecord]:
    chunks = re.findall(r"<tr\b[^>]*>(.*?)</tr>", text or "", flags=re.IGNORECASE | re.DOTALL)
    records: list[comp_cache.CompRecord] = []
    for chunk in chunks:
        plain = re.sub(r"<[^>]+>", "\n", chunk)
        plain = re.sub(r"\n{2,}", "\n", plain)
        record = comp_cache.record_from_text(plain)
        if record:
            href = re.search(r'href="([^"]+)"', chunk, flags=re.IGNORECASE)
            if href and not record.url:
                record.url = href.group(1)
            records.append(record)
    if not records:
        for chunk in [part.strip() for part in re.split(r"\n\s*\n", text or "") if part.strip()]:
            record = comp_cache.record_from_text(chunk)
            if record:
                records.append(record)
    return records


def walk_mappings(payload: Any, *, depth: int = 0) -> list[dict[str, Any]]:
    if depth > 8:
        return []
    if isinstance(payload, dict):
        rows = [payload]
        for value in payload.values():
            rows.extend(walk_mappings(value, depth=depth + 1))
        return rows
    if isinstance(payload, list):
        rows: list[dict[str, Any]] = []
        for value in payload:
            rows.extend(walk_mappings(value, depth=depth + 1))
        return rows
    return []


def first_value(row: dict[str, Any], *names: str) -> Any:
    compact = {re.sub(r"[^a-z0-9]", "", key.lower()): value for key, value in row.items()}
    for name in names:
        key = re.sub(r"[^a-z0-9]", "", name.lower())
        if key in compact:
            return compact[key]
    for key, value in row.items():
        compact_key = re.sub(r"[^a-z0-9]", "", key.lower())
        if any(re.sub(r"[^a-z0-9]", "", name.lower()) in compact_key for name in names):
            return value
    return None


def record_from_mapping(row: dict[str, Any]) -> comp_cache.CompRecord | None:
    price = comp_cache.parse_price(first_value(row, "price", "sold_price", "sale_price", "best_offer", "amount"))
    title = str(first_value(row, "title", "listing_title", "name", "item") or "").strip()
    sold_date = str(first_value(row, "sold_date", "soldDate", "date_sold", "date", "end_date") or "").strip()
    url = str(first_value(row, "url", "href", "link", "item_url") or "").strip()
    if price is None or not (title or url):
        return None
    return comp_cache.CompRecord(
        title=title,
        price=price,
        sold_date=sold_date,
        platform=str(first_value(row, "platform", "source", "marketplace") or "130point").strip(),
        sale_type=str(first_value(row, "sale_type", "type", "listing_type") or "").strip(),
        grader=str(first_value(row, "grader", "grading_company") or "").strip(),
        grade=str(first_value(row, "grade") or "").strip(),
        cert=str(first_value(row, "cert", "cert_number") or "").strip(),
        url=url,
        image_url=str(first_value(row, "image", "image_url", "thumbnail") or "").strip(),
        raw=row,
    )


def cache_records(cache_path: str | Path, query: str, records: Iterable[comp_cache.CompRecord]) -> int:
    return comp_cache.cache_records(cache_path, query, records)


def load_cached_records(cache_path: str | Path, **kwargs: Any) -> list[comp_cache.CompRecord]:
    return comp_cache.load_cached_records(cache_path, **kwargs)


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="130point sold comps CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    build = sub.add_parser("build-query")
    add_query_args(build)

    sync = sub.add_parser("sync")
    add_query_args(sync)
    add_cache_args(sync)
    sync.add_argument("--limit", type=int, default=20)

    comps = sub.add_parser("comps")
    add_query_args(comps)
    add_cache_args(comps)
    comps.add_argument("--limit", type=int, default=20)
    comps.add_argument("--format", choices=("table", "json", "csv"), default="table")
    comps.add_argument("--include-raw", action="store_true")
    comps.add_argument("--data-source", choices=("live", "local"), default="live")

    local = sub.add_parser("search")
    add_cache_args(local)
    local.add_argument("text", nargs="?", default="")
    local.add_argument("--grader", default="")
    local.add_argument("--grade", default="")
    local.add_argument("--limit", type=int, default=20)
    local.add_argument("--format", choices=("table", "json", "csv"), default="table")
    local.add_argument("--include-raw", action="store_true")
    return parser


def add_cache_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--cache-db", default=str(default_cache_path()))


def add_query_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("query", nargs="?", default="")
    parser.add_argument("--grader", default="", choices=("", *GRADERS))
    parser.add_argument("--grade", default="")
    parser.add_argument("--cert", default="")
    parser.add_argument("--card-number", default="")


def main(argv: list[str] | None = None) -> int:
    args = make_parser().parse_args(argv)
    if args.command == "build-query":
        print(build_slab_query(args.query, grader=args.grader, grade=args.grade, cert=args.cert, card_number=args.card_number))
        return 0
    if args.command == "sync":
        query = build_slab_query(args.query, grader=args.grader, grade=args.grade, cert=args.cert, card_number=args.card_number)
        records = fetch_records(query, limit=args.limit)
        saved = cache_records(args.cache_db, query, records)
        print(f"Saved {saved} comps to {args.cache_db}")
        return 0 if records else 2
    if args.command == "comps":
        query = build_slab_query(args.query, grader=args.grader, grade=args.grade, cert=args.cert, card_number=args.card_number)
        records = (
            load_cached_records(args.cache_db, query=query, limit=args.limit)
            if args.data_source == "local"
            else fetch_records(query, limit=args.limit)
        )
        if args.data_source == "live" and records:
            cache_records(args.cache_db, query, records)
        comp_cache.print_records(records, args.format, include_raw=args.include_raw)
        return 0 if records else 2
    if args.command == "search":
        records = load_cached_records(
            args.cache_db,
            text=args.text,
            grader=args.grader,
            grade=args.grade,
            limit=args.limit,
        )
        comp_cache.print_records(records, args.format, include_raw=args.include_raw)
        return 0 if records else 2
    return 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
