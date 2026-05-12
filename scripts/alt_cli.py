"""
ALT sold-comps CLI.

ALT's web app publishes a short-lived search configuration through its own
GraphQL endpoint, then searches sold listings through Typesense. This CLI uses
that same public web-app path and stores results in a local SQLite cache.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Optional

import httpx

ALT_GRAPHQL_URL = "https://alt-platform-server.production.internal.onlyalt.com/graphql/SearchServiceConfig"
ALT_BROWSE_URL = "https://alt.xyz/browse"
QUERY_BY = "name,rawName,subject,brand,cardNumber,gradeKey,gradingCompany,grade"
DEFAULT_LIMIT = 20

GRADERS = ("PSA", "BGS", "SGC", "CGC")

SEARCH_CONFIG_QUERY = """
query SearchServiceConfig {
  serviceConfig {
    search {
      soldListingSearch {
        clientConfig {
          nodes { host port protocol }
          apiKey
        }
        collectionName
        expiresAt
      }
    }
  }
}
"""


@dataclass
class AltCompRecord:
    title: str = ""
    price: Optional[float] = None
    sold_date: str = ""
    platform: str = ""
    sale_type: str = ""
    grader: str = ""
    grade: str = ""
    cert: str = ""
    url: str = ""
    image_url: str = ""
    raw: Optional[dict[str, Any]] = None

    def stable_key(self) -> tuple[Any, ...]:
        return (
            self.title.strip().lower(),
            self.price,
            self.sold_date.strip().lower(),
            self.platform.strip().lower(),
            self.sale_type.strip().lower(),
            self.url.strip().lower(),
        )

    def as_dict(self, *, include_raw: bool = False) -> dict[str, Any]:
        row = {
            "title": self.title,
            "price": self.price,
            "sold_date": self.sold_date,
            "platform": self.platform,
            "sale_type": self.sale_type,
            "grader": self.grader,
            "grade": self.grade,
            "cert": self.cert,
            "url": self.url,
            "image_url": self.image_url,
        }
        if include_raw:
            row["raw"] = self.raw or {}
        return row


def default_cache_path() -> Path:
    env = os.environ.get("ALT_CACHE_DB")
    if env:
        return Path(env)
    local_app_data = os.environ.get("LOCALAPPDATA")
    if local_app_data:
        return Path(local_app_data) / "DegenCollectibles" / "alt-cli" / "comps.sqlite"
    return Path.home() / ".degen-collectibles" / "alt-cli" / "comps.sqlite"


def build_slab_query(
    query: str = "",
    *,
    grader: str = "",
    grade: str = "",
    cert: str = "",
    card_number: str = "",
) -> str:
    parts: list[str] = []
    for value in (query, card_number, grader.strip().upper(), grade.strip(), cert.strip()):
        cleaned = str(value or "").strip()
        if cleaned and cleaned.lower() not in {part.lower() for part in parts}:
            parts.append(cleaned)
    return " ".join(parts)


def alt_browse_url(query: str) -> str:
    from urllib.parse import urlencode

    return f"{ALT_BROWSE_URL}?{urlencode({'query': query, 'tab': 'sold'})}"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def normalise_query_key(query: str) -> str:
    return re.sub(r"\s+", " ", query.strip().lower())


def record_source_hash(record: AltCompRecord) -> str:
    material = json.dumps(
        {
            "title": record.title.strip().lower(),
            "price": record.price,
            "sold_date": record.sold_date.strip().lower(),
            "platform": record.platform.strip().lower(),
            "sale_type": record.sale_type.strip().lower(),
            "url": record.url.strip().lower(),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def ensure_cache_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS comps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query TEXT NOT NULL,
            query_key TEXT NOT NULL,
            source_hash TEXT NOT NULL,
            title TEXT NOT NULL DEFAULT '',
            price REAL,
            sold_date TEXT NOT NULL DEFAULT '',
            platform TEXT NOT NULL DEFAULT '',
            sale_type TEXT NOT NULL DEFAULT '',
            grader TEXT NOT NULL DEFAULT '',
            grade TEXT NOT NULL DEFAULT '',
            cert TEXT NOT NULL DEFAULT '',
            url TEXT NOT NULL DEFAULT '',
            image_url TEXT NOT NULL DEFAULT '',
            raw_json TEXT NOT NULL DEFAULT '{}',
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            UNIQUE(query_key, source_hash)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_alt_comps_query_key ON comps(query_key)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_alt_comps_grade ON comps(grader, grade)")
    conn.commit()


def open_cache(cache_path: str | Path, *, read_only: bool = False) -> sqlite3.Connection:
    path = Path(cache_path)
    if read_only:
        if not path.exists():
            raise RuntimeError(f"Cache database does not exist: {path}")
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    if not read_only:
        ensure_cache_schema(conn)
    return conn


def record_from_cache_row(row: sqlite3.Row) -> AltCompRecord:
    try:
        raw = json.loads(row["raw_json"] or "{}")
    except json.JSONDecodeError:
        raw = {}
    return AltCompRecord(
        title=row["title"] or "",
        price=row["price"],
        sold_date=row["sold_date"] or "",
        platform=row["platform"] or "",
        sale_type=row["sale_type"] or "",
        grader=row["grader"] or "",
        grade=row["grade"] or "",
        cert=row["cert"] or "",
        url=row["url"] or "",
        image_url=row["image_url"] or "",
        raw=raw,
    )


def cache_records(cache_path: str | Path, query: str, records: Iterable[AltCompRecord]) -> int:
    conn = open_cache(cache_path)
    now = utc_now_iso()
    query_key = normalise_query_key(query)
    saved = 0
    try:
        for record in records:
            source_hash = record_source_hash(record)
            raw_json = json.dumps(record.raw or {}, sort_keys=True)
            existing = conn.execute(
                "SELECT id, first_seen_at FROM comps WHERE query_key = ? AND source_hash = ?",
                (query_key, source_hash),
            ).fetchone()
            if existing:
                conn.execute(
                    """
                    UPDATE comps
                    SET query = ?, title = ?, price = ?, sold_date = ?, platform = ?,
                        sale_type = ?, grader = ?, grade = ?, cert = ?, url = ?, image_url = ?,
                        raw_json = ?, first_seen_at = ?, last_seen_at = ?
                    WHERE id = ?
                    """,
                    (
                        query,
                        record.title,
                        record.price,
                        record.sold_date,
                        record.platform,
                        record.sale_type,
                        record.grader,
                        record.grade,
                        record.cert,
                        record.url,
                        record.image_url,
                        raw_json,
                        existing["first_seen_at"],
                        now,
                        int(existing["id"]),
                    ),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO comps (
                        query, query_key, source_hash, title, price, sold_date,
                        platform, sale_type, grader, grade, cert, url, image_url,
                        raw_json, first_seen_at, last_seen_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        query,
                        query_key,
                        source_hash,
                        record.title,
                        record.price,
                        record.sold_date,
                        record.platform,
                        record.sale_type,
                        record.grader,
                        record.grade,
                        record.cert,
                        record.url,
                        record.image_url,
                        raw_json,
                        now,
                        now,
                    ),
                )
            saved += 1
        conn.commit()
        return saved
    finally:
        conn.close()


def load_cached_records(
    cache_path: str | Path,
    *,
    query: str = "",
    text: str = "",
    grader: str = "",
    grade: str = "",
    limit: int = DEFAULT_LIMIT,
) -> list[AltCompRecord]:
    conn = open_cache(cache_path, read_only=True)
    where: list[str] = []
    params: list[Any] = []
    try:
        if query:
            where.append("query_key = ?")
            params.append(normalise_query_key(query))
        if text:
            for token in re.findall(r"[A-Za-z0-9]+", text):
                where.append("(title LIKE ? OR query LIKE ? OR platform LIKE ? OR sale_type LIKE ?)")
                like = f"%{token}%"
                params.extend([like, like, like, like])
        if grader:
            where.append("UPPER(grader) = ?")
            params.append(grader.upper())
        if grade:
            where.append("grade = ?")
            params.append(grade)
        sql = "SELECT * FROM comps"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY last_seen_at DESC, sold_date DESC LIMIT ?"
        params.append(max(1, int(limit)))
        return [record_from_cache_row(row) for row in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()


def dedupe_records(records: Iterable[AltCompRecord]) -> list[AltCompRecord]:
    seen: set[tuple[Any, ...]] = set()
    out: list[AltCompRecord] = []
    for record in records:
        key = record.stable_key()
        if key in seen:
            continue
        seen.add(key)
        out.append(record)
    return out


def search_config(client: httpx.Client | None = None) -> dict[str, Any]:
    close_client = client is None
    client = client or httpx.Client(timeout=20.0)
    try:
        resp = client.post(
            ALT_GRAPHQL_URL,
            json={
                "operationName": "SearchServiceConfig",
                "variables": {},
                "query": SEARCH_CONFIG_QUERY,
            },
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Origin": "https://alt.xyz",
                "Referer": "https://alt.xyz/",
                "User-Agent": "DegenCollectibles/1.0",
            },
        )
        resp.raise_for_status()
        data = resp.json()
        config = (((data.get("data") or {}).get("serviceConfig") or {}).get("search") or {}).get("soldListingSearch")
        if not config:
            raise RuntimeError("ALT search config was not present in GraphQL response.")
        return config
    finally:
        if close_client:
            client.close()


def _card_number_filter(card_number: str) -> str:
    raw = str(card_number or "").strip()
    if not raw:
        return ""
    first = raw.split("/", 1)[0].strip()
    if not first:
        return ""
    safe = re.sub(r"[^A-Za-z0-9-]", "", first)
    return f"cardNumber:={safe}" if safe else ""


def _grade_filter(grader: str, grade: str) -> str:
    grader = grader.strip().upper()
    grade = grade.strip()
    if grader and grade:
        return f"gradeKey:={grader}-{grade}"
    if grader:
        return f"gradingCompany:={grader}"
    if grade:
        return f"grade:={grade}"
    return ""


def _filter_by(*parts: str) -> str:
    return " && ".join(part for part in parts if part)


def fetch_records(
    query: str,
    *,
    grader: str = "",
    grade: str = "",
    card_number: str = "",
    limit: int = DEFAULT_LIMIT,
    client: httpx.Client | None = None,
) -> list[AltCompRecord]:
    close_client = client is None
    client = client or httpx.Client(timeout=25.0)
    try:
        config = search_config(client)
        node = config["clientConfig"]["nodes"][0]
        base_url = f"{node['protocol']}://{node['host']}:{node['port']}"
        endpoint = f"{base_url}/collections/{config['collectionName']}/documents/search"
        params = {
            "q": query,
            "query_by": QUERY_BY,
            "per_page": str(max(1, int(limit))),
            "sort_by": "timeStampForSorting:desc",
        }
        filter_by = _filter_by(_grade_filter(grader, grade), _card_number_filter(card_number))
        if filter_by:
            params["filter_by"] = filter_by
        resp = client.get(
            endpoint,
            params=params,
            headers={
                "Accept": "application/json",
                "X-TYPESENSE-API-KEY": config["clientConfig"]["apiKey"],
                "User-Agent": "DegenCollectibles/1.0",
            },
        )
        resp.raise_for_status()
        hits = (resp.json() or {}).get("hits") or []
        return dedupe_records(record_from_hit(hit) for hit in hits if record_from_hit(hit))
    finally:
        if close_client:
            client.close()


def record_from_hit(hit: dict[str, Any]) -> Optional[AltCompRecord]:
    doc = hit.get("document") if isinstance(hit, dict) else None
    if not isinstance(doc, dict):
        return None
    price = doc.get("price")
    try:
        price_value = round(float(price), 2) if price is not None else None
    except (TypeError, ValueError):
        price_value = None
    if price_value is None or price_value <= 0:
        return None
    images = doc.get("images") if isinstance(doc.get("images"), list) else []
    image_url = ""
    for image in images:
        if isinstance(image, dict) and image.get("url"):
            image_url = str(image["url"])
            break
    return AltCompRecord(
        title=str(doc.get("rawName") or doc.get("name") or "").replace("Opens in a new window or tab", "").strip(),
        price=price_value,
        sold_date=str(doc.get("soldDate") or ""),
        platform=str(doc.get("auctionHouse") or "ALT").strip(),
        sale_type=str(doc.get("auctionType") or doc.get("listingType") or "").strip(),
        grader=str(doc.get("gradingCompany") or "").strip(),
        grade=str(doc.get("grade") or "").strip(),
        url=str(doc.get("url") or ""),
        image_url=image_url,
        raw=doc,
    )


def summarize_records(records: list[AltCompRecord]) -> dict[str, Any]:
    prices = [float(record.price) for record in records if record.price is not None]
    return {
        "count": len(records),
        "priced_count": len(prices),
        "low": round(min(prices), 2) if prices else None,
        "high": round(max(prices), 2) if prices else None,
        "median": round(float(median(prices)), 2) if prices else None,
        "average": round(sum(prices) / len(prices), 2) if prices else None,
    }


def print_records(records: list[AltCompRecord], output_format: str, *, include_raw: bool = False) -> None:
    if output_format == "json":
        print(json.dumps([record.as_dict(include_raw=include_raw) for record in records], indent=2))
        return
    if output_format == "csv":
        writer = csv.DictWriter(
            sys.stdout,
            fieldnames=["sold_date", "price", "platform", "sale_type", "grader", "grade", "title", "url", "image_url"],
            extrasaction="ignore",
        )
        writer.writeheader()
        for record in records:
            writer.writerow(record.as_dict())
        return
    if not records:
        print("No comps found.")
        return
    headers = ("Date", "Price", "Platform", "Type", "Grade", "Title")
    rows = [
        (
            record.sold_date[:12],
            f"${record.price:,.2f}" if record.price is not None else "",
            record.platform[:14],
            record.sale_type[:14],
            " ".join(part for part in (record.grader, record.grade) if part)[:10],
            (record.title or record.url)[:82],
        )
        for record in records
    ]
    widths = [max(len(headers[index]), *(len(row[index]) for row in rows)) for index in range(len(headers))]
    print("  ".join(headers[index].ljust(widths[index]) for index in range(len(headers))))
    print("  ".join("-" * width for width in widths))
    for row in rows:
        print("  ".join(row[index].ljust(widths[index]) for index in range(len(headers))))


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ALT sold comps CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    build = sub.add_parser("build-query", help="Print the ALT query this CLI would use")
    add_query_args(build)

    sync = sub.add_parser("sync", help="Fetch ALT sold comps and store them in the local cache")
    add_query_args(sync)
    add_cache_args(sync)
    sync.add_argument("--limit", type=int, default=DEFAULT_LIMIT)

    comps = sub.add_parser("comps", help="Fetch or read ALT sold comps")
    add_query_args(comps)
    add_cache_args(comps)
    comps.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    comps.add_argument("--format", choices=("table", "json", "csv"), default="table")
    comps.add_argument("--include-raw", action="store_true")
    comps.add_argument("--data-source", choices=("live", "local"), default="live")

    local = sub.add_parser("search", help="Search cached ALT comps")
    add_cache_args(local)
    local.add_argument("text", nargs="?", default="")
    local.add_argument("--grader", default="")
    local.add_argument("--grade", default="")
    local.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
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
        if not query:
            raise RuntimeError("Provide a query or slab filters.")
        records = fetch_records(query, grader=args.grader, grade=args.grade, card_number=args.card_number, limit=args.limit)
        saved = cache_records(args.cache_db, query, records)
        print(f"Saved {saved} comps to {args.cache_db}")
        return 0 if records else 2
    if args.command == "comps":
        query = build_slab_query(args.query, grader=args.grader, grade=args.grade, cert=args.cert, card_number=args.card_number)
        if not query:
            raise RuntimeError("Provide a query or slab filters.")
        if args.data_source == "local":
            records = load_cached_records(args.cache_db, query=query, limit=args.limit)
        else:
            records = fetch_records(query, grader=args.grader, grade=args.grade, card_number=args.card_number, limit=args.limit)
            if records:
                cache_records(args.cache_db, query, records)
        print_records(records, args.format, include_raw=args.include_raw)
        return 0 if records else 2
    if args.command == "search":
        records = load_cached_records(args.cache_db, text=args.text, grader=args.grader, grade=args.grade, limit=args.limit)
        print_records(records, args.format, include_raw=args.include_raw)
        return 0 if records else 2
    return 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
