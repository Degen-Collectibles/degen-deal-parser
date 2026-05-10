"""
Authenticated Card Ladder comps CLI.

This intentionally uses a persistent local browser profile instead of storing a
Card Ladder password or trying to bypass their bot protection. Run `login` once,
complete Card Ladder's normal login in the browser, then use `comps` for slab
sales-history lookups.

Examples:
    .\\.venv\\Scripts\\python.exe scripts\\cardladder_cli.py login
    .\\.venv\\Scripts\\python.exe scripts\\cardladder_cli.py comps "1986 Fleer Jordan" --grader PSA --grade 9
    .\\.venv\\Scripts\\python.exe scripts\\cardladder_cli.py sync "1986 Fleer Jordan" --grader PSA --grade 9
    .\\.venv\\Scripts\\python.exe scripts\\cardladder_cli.py search Jordan --grader PSA --grade 9
    .\\.venv\\Scripts\\python.exe scripts\\cardladder_cli.py summary "1986 Fleer Jordan" --grader PSA --grade 9
    .\\.venv\\Scripts\\python.exe scripts\\cardladder_cli.py comps --cert 12345678 --grader PSA --format json
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import os
import re
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from statistics import median
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import urlencode

try:
    from playwright.async_api import (
        BrowserContext,
        Page,
        Response,
        TimeoutError as PlaywrightTimeoutError,
        async_playwright,
    )
except ImportError:  # pragma: no cover - import guard for local operator use.
    print("Playwright is required. Install it with:")
    print("  .\\.venv\\Scripts\\python.exe -m pip install playwright")
    print("  .\\.venv\\Scripts\\python.exe -m playwright install chrome")
    sys.exit(1)


CARDLADDER_HOME_URL = "https://www.cardladder.com/"
CARDLADDER_LOGIN_URL = "https://www.cardladder.com/login"
CARDLADDER_SALES_HISTORY_URLS = (
    "https://www.cardladder.com/sales-history",
    "https://app.cardladder.com/sales-history",
)
CARDLADDER_APP_SALES_HISTORY_URL = "https://app.cardladder.com/sales-history"
DEFAULT_CACHE_MAX_AGE_HOURS = 24.0

GRADERS = ("PSA", "BGS", "SGC", "CGC")
OTHER_GRADERS = (
    "PSA",
    "BGS",
    "SGC",
    "CGC",
    "HGA",
    "CSG",
    "GMA",
    "SCD",
    "GAI",
    "CEX",
    "FGS",
    "PGI",
    "CSA",
    "KSA",
    "EGS",
    "BSG",
    "TFA",
    "PGA",
    "AGA",
    "DCS",
    "CGI",
    "ISA",
    "PBI",
    "RCG",
    "SBC",
    "DGA",
    "TGA",
    "WCG",
    "CCG",
    "DSG",
    "VGT",
    "PCA",
    "MGC",
    "CGA",
    "AGC",
    "PGS",
    "TAG",
    "AGS",
    "FCG",
    "CGS",
    "BGN",
    "DCI",
)
NOISE_EXCLUSIONS = (
    "Auto",
    "Autos",
    "Autograph",
    "Autographs",
    "Autographed",
    "Signed",
    "Signature",
    "Signatures",
    "Signing",
    "Signings",
    "DNA",
    "JSA",
    "BAS",
    "COA",
    "Worn",
    "Patch",
    "Patches",
    "Swatch",
    "Swatches",
    "Material",
    "Materials",
    "Relic",
    "Relics",
    "Fabric",
    "Fabrics",
    "Lot",
    "Lots",
    "Bonus",
    "Extras",
    "Extra",
    "More",
    "Also",
    "Additional",
    "Bundle",
    "Sealed",
    "Unopened",
    "Pick",
    "Choose",
    "Box",
    "Boxes",
    "Reprint",
    "Reprints",
    "Re-Print",
    "Re-Prints",
    "RP",
    "RPs",
    "Reproduction",
    "Reproductions",
    "Repro",
    "Repros",
    "Replica",
    "Counterfeit",
    "Counterfeits",
    "Full",
    "Entire",
    "Complete",
    "Partial",
    "Assorted",
    "MC",
    "MK",
    "OC",
    "PD",
    "Checklist",
    "Checklists",
    "Chklists",
    "Chklist",
    "Chklsts",
    "Chklst",
    "Lists",
    "List",
    "CLs",
    "CL",
    "Illustrated",
    "23kt",
    "23k",
    "24kt",
    "24k",
    "Karat",
)
LISTING_TYPES = ("Auction", "Best Offer", "Fixed Price")
VERIFIED_FILTERS = ("All", "Verified", "Unverified")
SORT_FIELDS = {
    "date": "date",
    "price": "price",
}
SORT_DIRECTIONS = ("asc", "desc")
GRADER_DIALOG_LABELS = {
    "PSA": "PSA",
    "BGS": "BECKETT",
    "SGC": "SGC",
    "CGC": "CGC",
}

PRICE_KEY_HINTS = (
    "price",
    "soldprice",
    "saleprice",
    "purchaseprice",
    "amount",
    "finalprice",
    "value",
)
DATE_KEY_HINTS = ("date", "solddate", "saledate", "enddate", "orderdate", "createdat")
TITLE_KEY_HINTS = ("title", "cardtitle", "itemtitle", "description", "name")
PLATFORM_KEY_HINTS = ("platform", "marketplace", "source", "venue")
TYPE_KEY_HINTS = ("type", "listingtype", "sale_type", "saletype")
URL_KEY_HINTS = ("url", "href", "link")
IMAGE_KEY_HINTS = ("image", "imageurl", "thumbnail", "photo")
GRADER_KEY_HINTS = ("grader", "gradingcompany", "company")
GRADE_KEY_HINTS = ("grade", "numericgrade")
CERT_KEY_HINTS = ("cert", "certnumber", "certificationnumber", "certificate")

PRICE_RE = re.compile(r"\$\s?[\d,]+(?:\.\d{2})?")
DATE_RE = re.compile(
    r"\b(?:"
    r"\d{4}-\d{1,2}-\d{1,2}|"
    r"\d{1,2}/\d{1,2}/\d{2,4}|"
    r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2},?\s+\d{2,4}"
    r")\b",
    flags=re.IGNORECASE,
)


@dataclass
class CompRecord:
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


def default_profile_dir() -> Path:
    env = os.environ.get("CARDLADDER_PROFILE_DIR")
    if env:
        return Path(env)
    local_app_data = os.environ.get("LOCALAPPDATA")
    if local_app_data:
        return Path(local_app_data) / "DegenCollectibles" / "cardladder-cli" / "chrome-profile"
    return Path.home() / ".degen-collectibles" / "cardladder-cli" / "chrome-profile"


def default_cache_path() -> Path:
    env = os.environ.get("CARDLADDER_CACHE_DB")
    if env:
        return Path(env)
    local_app_data = os.environ.get("LOCALAPPDATA")
    if local_app_data:
        return Path(local_app_data) / "DegenCollectibles" / "cardladder-cli" / "comps.sqlite"
    return Path.home() / ".degen-collectibles" / "cardladder-cli" / "comps.sqlite"


def _compact_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def _first_value(row: dict[str, Any], hints: Iterable[str]) -> Any:
    keyed = {_compact_key(key): value for key, value in row.items()}
    for hint in hints:
        compact_hint = _compact_key(hint)
        if compact_hint in keyed:
            return keyed[compact_hint]
    for key, value in row.items():
        compact_key = _compact_key(key)
        if any(_compact_key(hint) in compact_key for hint in hints):
            return value
    return None


def parse_price(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)):
        number = float(value)
        return round(number, 2) if number > 0 else None
    text = str(value or "")
    match = PRICE_RE.search(text)
    if match:
        text = match.group(0)
    cleaned = re.sub(r"[^0-9.]", "", text)
    if not cleaned:
        return None
    try:
        number = float(cleaned)
    except ValueError:
        return None
    return round(number, 2) if number > 0 else None


def _string_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (str, int, float)):
        return str(value).strip()
    return ""


def _looks_like_sale(row: dict[str, Any]) -> bool:
    price = parse_price(_first_value(row, PRICE_KEY_HINTS))
    title = _string_value(_first_value(row, TITLE_KEY_HINTS))
    sold_date = _string_value(_first_value(row, DATE_KEY_HINTS))
    url = _string_value(_first_value(row, URL_KEY_HINTS))
    return bool(price and (title or url) and (sold_date or title))


def record_from_json(row: dict[str, Any]) -> Optional[CompRecord]:
    if not _looks_like_sale(row):
        return None
    return CompRecord(
        title=_string_value(_first_value(row, TITLE_KEY_HINTS)),
        price=parse_price(_first_value(row, PRICE_KEY_HINTS)),
        sold_date=_string_value(_first_value(row, DATE_KEY_HINTS)),
        platform=_string_value(_first_value(row, PLATFORM_KEY_HINTS)),
        sale_type=_string_value(_first_value(row, TYPE_KEY_HINTS)),
        grader=_string_value(_first_value(row, GRADER_KEY_HINTS)),
        grade=_string_value(_first_value(row, GRADE_KEY_HINTS)),
        cert=_string_value(_first_value(row, CERT_KEY_HINTS)),
        url=_string_value(_first_value(row, URL_KEY_HINTS)),
        image_url=_string_value(_first_value(row, IMAGE_KEY_HINTS)),
        raw=row,
    )


def walk_json_records(payload: Any) -> list[CompRecord]:
    records: list[CompRecord] = []
    seen_ids: set[int] = set()

    def visit(value: Any) -> None:
        if id(value) in seen_ids:
            return
        seen_ids.add(id(value))
        if isinstance(value, dict):
            record = record_from_json(value)
            if record:
                records.append(record)
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(payload)
    return records


def record_from_text(chunk: str) -> Optional[CompRecord]:
    text = re.sub(r"\n{3,}", "\n\n", chunk.strip())
    if not text:
        return None
    price = parse_price(text)
    if price is None:
        return None
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    label_map: dict[str, str] = {}
    for index, line in enumerate(lines[:-1]):
        key = line.strip().lower().replace(" ", "_")
        if key in {"date_sold", "type", "price"}:
            label_map[key] = lines[index + 1]
    sold_date = label_map.get("date_sold", "")
    if not sold_date:
        date_match = DATE_RE.search(text)
        sold_date = date_match.group(0) if date_match else ""
    sale_type = label_map.get("type", "")
    platform = ""
    title = ""
    skip_next = False
    label_words = {"date sold", "type", "price"}
    for line in lines:
        lower = line.lower()
        if skip_next:
            skip_next = False
            continue
        if lower in label_words:
            skip_next = True
            continue
        if PRICE_RE.search(line) or DATE_RE.search(line):
            continue
        if not platform and (
            " - " in line
            or line.upper() == line
            or line.lower().startswith(("ebay", "alt ", "fanatics", "goldin", "heritage"))
        ):
            platform = line
            continue
        title = line
        break
    return CompRecord(
        title=title or (lines[1] if len(lines) > 1 else lines[0]),
        price=price,
        sold_date=sold_date,
        platform=platform,
        sale_type=sale_type,
        raw={"text": text},
    )


def dedupe_records(records: Iterable[CompRecord]) -> list[CompRecord]:
    seen: set[tuple[Any, ...]] = set()
    unique: list[CompRecord] = []
    for record in records:
        key = record.stable_key()
        if key in seen:
            continue
        seen.add(key)
        unique.append(record)
    return unique


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def normalise_query_key(query: str) -> str:
    return re.sub(r"\s+", " ", query.strip().lower())


def record_source_hash(record: CompRecord) -> str:
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
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def ensure_cache_schema(conn: sqlite3.Connection) -> bool:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS comps (
            id INTEGER PRIMARY KEY,
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
    conn.execute("CREATE INDEX IF NOT EXISTS idx_comps_query_key ON comps(query_key)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_comps_grade ON comps(grader, grade)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_comps_cert ON comps(cert)")
    existing_columns = {row[1] for row in conn.execute("PRAGMA table_info(comps)").fetchall()}
    if "sale_type" not in existing_columns:
        conn.execute("ALTER TABLE comps ADD COLUMN sale_type TEXT NOT NULL DEFAULT ''")
    fts_available = True
    try:
        conn.execute(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS comps_fts
            USING fts5(title, platform, sale_type, grader, grade, cert, query)
            """
        )
        fts_columns = {row[1] for row in conn.execute("PRAGMA table_info(comps_fts)").fetchall()}
        if "sale_type" not in fts_columns:
            conn.execute("DROP TABLE comps_fts")
            conn.execute(
                """
                CREATE VIRTUAL TABLE comps_fts
                USING fts5(title, platform, sale_type, grader, grade, cert, query)
                """
            )
    except sqlite3.OperationalError:
        fts_available = False
    conn.commit()
    return fts_available


def open_cache(cache_path: str | Path, *, read_only: bool = False) -> tuple[sqlite3.Connection, bool]:
    path = Path(cache_path)
    if read_only:
        if not path.exists():
            raise RuntimeError(f"Cache database does not exist: {path}")
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        return conn, _cache_has_fts(conn)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn, ensure_cache_schema(conn)


def _cache_has_fts(conn: sqlite3.Connection) -> bool:
    try:
        conn.execute("SELECT rowid FROM comps_fts LIMIT 1")
        return True
    except sqlite3.Error:
        return False


def record_from_cache_row(row: sqlite3.Row) -> CompRecord:
    raw_text = row["raw_json"] if "raw_json" in row.keys() else "{}"
    try:
        raw = json.loads(raw_text or "{}")
    except json.JSONDecodeError:
        raw = {}
    return CompRecord(
        title=row["title"] or "",
        price=row["price"],
        sold_date=row["sold_date"] or "",
        platform=row["platform"] or "",
        sale_type=row["sale_type"] if "sale_type" in row.keys() else "",
        grader=row["grader"] or "",
        grade=row["grade"] or "",
        cert=row["cert"] or "",
        url=row["url"] or "",
        image_url=row["image_url"] or "",
        raw=raw,
    )


def cache_records(cache_path: str | Path, query: str, records: Iterable[CompRecord]) -> int:
    conn, fts_available = open_cache(cache_path)
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
                comp_id = int(existing["id"])
                first_seen_at = existing["first_seen_at"]
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
                        first_seen_at,
                        now,
                        comp_id,
                    ),
                )
            else:
                cur = conn.execute(
                    """
                    INSERT INTO comps (
                        query, query_key, source_hash, title, price, sold_date,
                        platform, sale_type, grader, grade, cert, url, image_url, raw_json,
                        first_seen_at, last_seen_at
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
                comp_id = int(cur.lastrowid)
            if fts_available:
                conn.execute("DELETE FROM comps_fts WHERE rowid = ?", (comp_id,))
                conn.execute(
                    """
                    INSERT INTO comps_fts(rowid, title, platform, sale_type, grader, grade, cert, query)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        comp_id,
                        record.title,
                        record.platform,
                        record.sale_type,
                        record.grader,
                        record.grade,
                        record.cert,
                        query,
                    ),
                )
            saved += 1
        conn.commit()
        return saved
    finally:
        conn.close()


def _append_cache_filters(
    where: list[str],
    params: list[Any],
    *,
    grader: str = "",
    grade: str = "",
    cert: str = "",
    table_prefix: str = "",
) -> None:
    if grader:
        where.append(f"UPPER({table_prefix}grader) = ?")
        params.append(grader.upper())
    if grade:
        where.append(f"{table_prefix}grade = ?")
        params.append(grade)
    if cert:
        where.append(f"{table_prefix}cert = ?")
        params.append(cert)


def _fts_query(text: str) -> str:
    tokens = re.findall(r"[A-Za-z0-9]+", text)
    return " ".join(f"{token}*" for token in tokens)


def load_cached_records(
    cache_path: str | Path,
    *,
    query: str = "",
    text: str = "",
    grader: str = "",
    grade: str = "",
    cert: str = "",
    limit: int = 20,
) -> list[CompRecord]:
    conn, fts_available = open_cache(cache_path, read_only=True)
    where: list[str] = []
    params: list[Any] = []
    try:
        if query:
            where.append("query_key = ?")
            params.append(normalise_query_key(query))
        if text:
            fts = _fts_query(text)
            if fts and fts_available:
                sql = "SELECT c.* FROM comps c JOIN comps_fts f ON f.rowid = c.id WHERE comps_fts MATCH ?"
                fts_params: list[Any] = [fts]
                _append_cache_filters(where, params, grader=grader, grade=grade, cert=cert, table_prefix="c.")
                if where:
                    sql += " AND " + " AND ".join(where)
                sql += " ORDER BY c.last_seen_at DESC, c.sold_date DESC LIMIT ?"
                fts_params.extend(params)
                fts_params.append(max(1, int(limit)))
                return [record_from_cache_row(row) for row in conn.execute(sql, fts_params).fetchall()]
            for token in re.findall(r"[A-Za-z0-9]+", text):
                where.append("(title LIKE ? OR query LIKE ? OR platform LIKE ? OR sale_type LIKE ?)")
                like = f"%{token}%"
                params.extend([like, like, like, like])
        _append_cache_filters(where, params, grader=grader, grade=grade, cert=cert)
        sql = "SELECT * FROM comps"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY last_seen_at DESC, sold_date DESC LIMIT ?"
        params.append(max(1, int(limit)))
        return [record_from_cache_row(row) for row in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()


def cached_query_age_hours(cache_path: str | Path, query: str) -> Optional[float]:
    try:
        conn, _ = open_cache(cache_path, read_only=True)
    except RuntimeError:
        return None
    try:
        row = conn.execute(
            "SELECT MAX(last_seen_at) AS last_seen_at FROM comps WHERE query_key = ?",
            (normalise_query_key(query),),
        ).fetchone()
        if not row or not row["last_seen_at"]:
            return None
        last_seen = datetime.fromisoformat(str(row["last_seen_at"]))
        return (datetime.now(timezone.utc) - last_seen).total_seconds() / 3600
    finally:
        conn.close()


def summarize_records(records: list[CompRecord]) -> dict[str, Any]:
    prices = [float(record.price) for record in records if record.price is not None]
    platforms: dict[str, int] = {}
    for record in records:
        if record.platform:
            platforms[record.platform] = platforms.get(record.platform, 0) + 1
    summary: dict[str, Any] = {
        "count": len(records),
        "priced_count": len(prices),
        "low": round(min(prices), 2) if prices else None,
        "high": round(max(prices), 2) if prices else None,
        "median": round(float(median(prices)), 2) if prices else None,
        "average": round(sum(prices) / len(prices), 2) if prices else None,
        "platforms": platforms,
    }
    if records:
        summary["latest_seen_sale"] = records[0].sold_date
        summary["latest_seen_title"] = records[0].title
    return summary


def print_summary(summary: dict[str, Any]) -> None:
    if not summary["count"]:
        print("No comps found.")
        return
    print(f"Comps: {summary['count']} ({summary['priced_count']} with prices)")
    print(f"Median: {_money(summary['median'])}  Avg: {_money(summary['average'])}")
    print(f"Low:    {_money(summary['low'])}  High: {_money(summary['high'])}")
    if summary.get("platforms"):
        platform_text = ", ".join(f"{name}={count}" for name, count in sorted(summary["platforms"].items()))
        print(f"Platforms: {platform_text}")
    if summary.get("latest_seen_sale") or summary.get("latest_seen_title"):
        print(f"Latest row: {summary.get('latest_seen_sale', '')}  {summary.get('latest_seen_title', '')}")


def _money(value: Any) -> str:
    return f"${value:,.2f}" if isinstance(value, (int, float)) else "-"


def query_from_args(args: argparse.Namespace) -> str:
    return build_slab_query(
        args.query or "",
        grader=args.grader or "",
        grade=args.grade or "",
        cert=args.cert or "",
        advanced=args.advanced,
        strict=not args.loose,
        extra_exclusions=getattr(args, "exclude", ()) or (),
    )


def sql_rows(cache_path: str | Path, sql: str) -> tuple[list[str], list[sqlite3.Row]]:
    stripped = sql.strip().rstrip(";")
    first_word = stripped.split(None, 1)[0].lower() if stripped else ""
    if first_word not in {"select", "with", "pragma"}:
        raise RuntimeError("Only read-only SELECT, WITH, and PRAGMA queries are allowed")
    conn, _ = open_cache(cache_path, read_only=True)
    try:
        cursor = conn.execute(stripped)
        columns = [desc[0] for desc in cursor.description or []]
        return columns, cursor.fetchall()
    finally:
        conn.close()


def print_sql_table(columns: list[str], rows: list[sqlite3.Row]) -> None:
    if not rows:
        print("No rows.")
        return
    rendered = [[_string_value(row[col]) for col in columns] for row in rows]
    widths = [
        min(80, max(len(columns[index]), *(len(row[index]) for row in rendered)))
        for index in range(len(columns))
    ]
    print("  ".join(columns[index].ljust(widths[index]) for index in range(len(columns))))
    print("  ".join("-" * width for width in widths))
    for row in rendered:
        print("  ".join(row[index][: widths[index]].ljust(widths[index]) for index in range(len(columns))))


def build_slab_query(
    query: str = "",
    *,
    grader: str = "",
    grade: str = "",
    cert: str = "",
    advanced: bool = False,
    strict: bool = True,
    extra_exclusions: Iterable[str] = (),
) -> str:
    parts: list[str] = []
    base = query.strip()
    if base:
        parts.append(base)
    grader = grader.strip().upper()
    grade = grade.strip()
    cert = cert.strip()
    if grader and grader not in parts:
        parts.append(grader)
    if grade:
        parts.append(grade)
    if cert and not base:
        parts.append(cert)

    if strict:
        for other in OTHER_GRADERS:
            if other != grader:
                parts.append(f"-{other}")
        if grader and grade:
            selected = grade.rstrip(".0") if grade.endswith(".0") else grade
            for value in (
                "10",
                "9.5",
                "9",
                "8.5",
                "8",
                "7.5",
                "7",
                "6.5",
                "6",
                "5.5",
                "5",
                "4.5",
                "4",
                "3.5",
                "3",
                "2.5",
                "2",
                "1.5",
                "1",
            ):
                if value != selected:
                    parts.append(f"-({grader} {value})")
        for word in NOISE_EXCLUSIONS:
            parts.append(f"-{word}")
        for word in extra_exclusions:
            cleaned = str(word or "").strip()
            if cleaned:
                parts.append(f"-{cleaned}")

    built = " ".join(part for part in parts if part).strip()
    if advanced and built and not built.startswith("!"):
        built = "!" + built
    return built


async def launch_cardladder_context(
    profile_dir: Path,
    *,
    headless: bool,
    timeout_ms: int,
) -> BrowserContext:
    profile_dir.mkdir(parents=True, exist_ok=True)
    pw = await async_playwright().start()
    try:
        try:
            context = await pw.chromium.launch_persistent_context(
                user_data_dir=str(profile_dir),
                channel="chrome",
                headless=headless,
                viewport={"width": 1440, "height": 1000},
                timeout=timeout_ms,
            )
        except Exception:
            context = await pw.chromium.launch_persistent_context(
                user_data_dir=str(profile_dir),
                headless=headless,
                viewport={"width": 1440, "height": 1000},
                timeout=timeout_ms,
            )
        setattr(context, "_cardladder_playwright", pw)
        return context
    except Exception:
        await pw.stop()
        raise


async def close_context(context: BrowserContext) -> None:
    pw = getattr(context, "_cardladder_playwright", None)
    await context.close()
    if pw:
        await pw.stop()


async def body_text(page: Page) -> str:
    try:
        return await page.inner_text("body", timeout=5000)
    except Exception:
        return ""


async def wait_for_operator_if_needed(page: Page, *, no_prompt: bool, reason: str) -> None:
    text = await body_text(page)
    loginish = any(term in text.lower() for term in ("log in", "sign in", "email", "password"))
    cloudflare = "performing security verification" in text.lower() or "just a moment" in (await page.title()).lower()
    if not (loginish or cloudflare):
        return
    if no_prompt:
        raise RuntimeError(f"Card Ladder needs operator action: {reason}")
    print()
    print(reason)
    print(f"Current URL: {page.url}")
    print("Use the opened browser to finish verification/login, then press Enter here.")
    await asyncio.to_thread(input, "> ")


async def goto_sales_history(page: Page, *, timeout_ms: int, no_prompt: bool) -> None:
    last_error: Optional[Exception] = None
    for url in CARDLADDER_SALES_HISTORY_URLS:
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            await page.wait_for_timeout(1500)
            await wait_for_operator_if_needed(
                page,
                no_prompt=no_prompt,
                reason="Card Ladder stopped at login or security verification.",
            )
            return
        except Exception as exc:
            last_error = exc
    if last_error:
        raise last_error


async def click_first(page: Page, selectors: Iterable[str], *, timeout_ms: int = 1500) -> bool:
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            await locator.click(timeout=timeout_ms)
            return True
        except Exception:
            continue
    return False


async def fill_first(page: Page, selectors: Iterable[str], value: str, *, timeout_ms: int = 2000) -> bool:
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            await locator.fill(value, timeout=timeout_ms)
            return True
        except Exception:
            continue
    return False


def sales_history_url(query: str, *, sort: str = "date", direction: str = "desc") -> str:
    params = {
        "sort": SORT_FIELDS.get(sort, "date"),
        "direction": direction if direction in SORT_DIRECTIONS else "desc",
    }
    if query:
        params["q"] = query
    return f"{CARDLADDER_APP_SALES_HISTORY_URL}?{urlencode(params)}"


def _has_sales_filters(args: argparse.Namespace) -> bool:
    return any(
        (
            getattr(args, "min_price", ""),
            getattr(args, "max_price", ""),
            getattr(args, "min_date", ""),
            getattr(args, "max_date", ""),
            getattr(args, "platform", None),
            getattr(args, "listing_type", ""),
            getattr(args, "seller", ""),
            getattr(args, "verified", "All") != "All",
        )
    )


async def fill_labeled_input(page: Page, label: str, value: str) -> bool:
    if not value:
        return False
    selectors = (
        f"xpath=//*[normalize-space()='{label}']/following::input[1]",
        f"xpath=//*[normalize-space()='{label}']/following::textarea[1]",
    )
    return await fill_first(page, selectors, value)


async def click_labeled_dropdown_value(page: Page, label: str, value: str) -> bool:
    if not value:
        return False
    opened = await click_first(
        page,
        (
            f"xpath=//*[normalize-space()='{label}']/following::*[contains(normalize-space(), 'expand_more')][1]",
            f"xpath=//*[normalize-space()='{label}']/following::button[1]",
            f"text={label}",
        ),
        timeout_ms=2000,
    )
    if not opened:
        return False
    await page.wait_for_timeout(300)
    return await click_first(
        page,
        (
            f"text={value}",
            f"[role='option']:has-text('{value}')",
            f"[role='menuitem']:has-text('{value}')",
        ),
        timeout_ms=2000,
    )


async def apply_sales_filters(page: Page, args: argparse.Namespace) -> None:
    if not _has_sales_filters(args):
        return
    opened = await click_first(
        page,
        (
            "button:has-text('tune')",
            "[aria-label*='filter' i]",
            "text=FILTER",
        ),
        timeout_ms=2500,
    )
    if not opened:
        raise RuntimeError("Could not open Card Ladder Sales History filters")
    await page.wait_for_timeout(500)
    await fill_labeled_input(page, "Min Price", str(args.min_price or ""))
    await fill_labeled_input(page, "Max Price", str(args.max_price or ""))
    await fill_labeled_input(page, "Min Date", str(args.min_date or ""))
    await fill_labeled_input(page, "Max Date", str(args.max_date or ""))
    await fill_labeled_input(page, "Seller ID", str(args.seller or ""))
    for platform in getattr(args, "platform", []) or []:
        if await fill_labeled_input(page, "Platforms", platform):
            await page.wait_for_timeout(400)
            await click_first(
                page,
                (
                    f"text={platform}",
                    f"[role='option']:has-text('{platform}')",
                    f"[role='menuitem']:has-text('{platform}')",
                ),
                timeout_ms=1500,
            )
    if getattr(args, "listing_type", ""):
        await click_labeled_dropdown_value(page, "Listing Type", args.listing_type)
    if getattr(args, "verified", "All") != "All":
        await click_labeled_dropdown_value(page, "Verified", args.verified)
    clicked = await click_first(page, ("button:has-text('Apply')", "text=Apply"), timeout_ms=2500)
    if not clicked:
        raise RuntimeError("Could not apply Card Ladder Sales History filters")
    await page.wait_for_timeout(2500)


async def run_text_search(page: Page, query: str, args: argparse.Namespace) -> None:
    await page.goto(
        sales_history_url(
            query,
            sort=getattr(args, "sort", "date"),
            direction=getattr(args, "direction", "desc"),
        ),
        wait_until="domcontentloaded",
        timeout=args.timeout_ms,
    )
    await page.wait_for_timeout(2000)
    await apply_sales_filters(page, args)
    if query:
        return
    selectors = (
        "input[type='search']",
        "input[placeholder*='Search' i]",
        "textarea[placeholder*='Search' i]",
        "input[name*='search' i]",
        "input",
        "textarea",
    )
    if not await fill_first(page, selectors, query):
        raise RuntimeError("Could not find Card Ladder sales-history search box")
    await page.keyboard.press("Enter")
    await page.wait_for_timeout(2500)


async def run_cert_search(page: Page, *, grader: str, cert: str, fallback_query: str, args: argparse.Namespace) -> None:
    opened = await click_first(
        page,
        (
            "button:has-text('tag')",
            "button:has-text('#')",
            "button:has-text('Cert')",
            "[aria-label*='cert' i]",
            "text=Search by Cert",
            "text=Cert #",
        ),
    )
    if opened:
        await page.wait_for_timeout(500)
        await fill_first(
            page,
            (
                "input[placeholder*='cert' i]",
                "input[name*='cert' i]",
                "[role='dialog'] input",
                "input",
            ),
            cert,
        )
        if grader:
            dialog_label = GRADER_DIALOG_LABELS.get(grader.upper(), grader.upper())
            try:
                await page.select_option("select", label=dialog_label, timeout=1000)
            except Exception:
                await click_first(
                    page,
                    (
                        f"text={dialog_label}",
                        f"button:has-text('{dialog_label}')",
                        "xpath=//*[normalize-space()='Grader']/following::*[contains(normalize-space(), 'expand_more')][1]",
                    ),
                )
                if dialog_label not in {"", "PSA"}:
                    await click_first(page, (f"text={dialog_label}", f"[role='option']:has-text('{dialog_label}')"))
        if getattr(args, "cert_all_grades", False):
            try:
                checked = await page.locator("[role='dialog'] input[type='checkbox'], input[type='checkbox']").first.is_checked(timeout=1000)
                if checked:
                    await page.locator("[role='dialog'] input[type='checkbox'], input[type='checkbox']").first.uncheck(timeout=1000)
            except Exception:
                await click_first(page, ("text=Show sales from this grade only",))
        clicked = await click_first(
            page,
            (
                "[role='dialog'] button:has-text('Search')",
                "button:has-text('Search')",
                "button:has-text('Submit')",
                "button:has-text('Apply')",
            ),
        )
        if clicked:
            await page.wait_for_timeout(3000)
            await apply_sales_filters(page, args)
            return
    await run_text_search(page, fallback_query, args)


async def dom_records(page: Page) -> list[CompRecord]:
    chunks = await page.evaluate(
        """
        () => Array.from(document.querySelectorAll(
          '[role="row"], tr, article, [class*="result" i], [class*="sale" i], [class*="row" i]'
        ))
          .map(el => (el.innerText || '').trim())
          .filter(text => text && /\\$\\s?\\d/.test(text))
          .slice(0, 80)
        """
    )
    return [record for record in (record_from_text(chunk) for chunk in chunks) if record]


async def capture_json_response(
    response: Response,
    payloads: list[dict[str, Any] | list[Any]],
    tasks: set[asyncio.Task[Any]],
) -> None:
    try:
        url = response.url.lower()
        if "cardladder" not in url:
            return
        request_type = response.request.resource_type
        content_type = response.headers.get("content-type", "")
        if request_type not in {"xhr", "fetch"} and "json" not in content_type.lower():
            return
        parsed = await response.json()
        payloads.append(parsed)
    except Exception:
        return
    finally:
        task = asyncio.current_task()
        if task is not None:
            tasks.discard(task)


async def collect_comps(args: argparse.Namespace) -> list[CompRecord]:
    query = query_from_args(args)
    if not query:
        raise RuntimeError("Provide a query or --cert/--grader/--grade")

    context = await launch_cardladder_context(
        Path(args.profile_dir),
        headless=bool(args.headless),
        timeout_ms=args.timeout_ms,
    )
    payloads: list[dict[str, Any] | list[Any]] = []
    tasks: set[asyncio.Task[Any]] = set()
    try:
        page = context.pages[0] if context.pages else await context.new_page()

        def on_response(response: Response) -> None:
            task = asyncio.create_task(capture_json_response(response, payloads, tasks))
            tasks.add(task)

        page.on("response", on_response)
        await goto_sales_history(page, timeout_ms=args.timeout_ms, no_prompt=args.no_prompt)
        if args.cert:
            await run_cert_search(page, grader=args.grader or "", cert=args.cert, fallback_query=query, args=args)
        else:
            await run_text_search(page, query, args)
        try:
            await page.wait_for_load_state("networkidle", timeout=5000)
        except PlaywrightTimeoutError:
            pass
        if tasks:
            await asyncio.wait(tasks, timeout=3)
        records: list[CompRecord] = []
        for payload in payloads:
            records.extend(walk_json_records(payload))
        if not records:
            records.extend(await dom_records(page))
        if args.debug_dir:
            debug_dir = Path(args.debug_dir)
            debug_dir.mkdir(parents=True, exist_ok=True)
            (debug_dir / "cardladder_payloads.json").write_text(json.dumps(payloads, indent=2), encoding="utf-8")
            (debug_dir / "cardladder_page.txt").write_text(await body_text(page), encoding="utf-8")
            await page.screenshot(path=str(debug_dir / "cardladder_page.png"), full_page=True)
        return dedupe_records(records)[: max(1, int(args.limit or 10))]
    finally:
        await close_context(context)


async def run_login(args: argparse.Namespace) -> int:
    context = await launch_cardladder_context(
        Path(args.profile_dir),
        headless=False,
        timeout_ms=args.timeout_ms,
    )
    try:
        page = context.pages[0] if context.pages else await context.new_page()
        await page.goto(args.url or CARDLADDER_LOGIN_URL, wait_until="domcontentloaded", timeout=args.timeout_ms)
        print(f"Profile: {args.profile_dir}")
        print("Log in to Card Ladder in the opened browser.")
        print("When Sales History or your dashboard is loaded, press Enter here.")
        await asyncio.to_thread(input, "> ")
        return 0
    finally:
        await close_context(context)


def print_table(records: list[CompRecord]) -> None:
    if not records:
        print("No comps found.")
        return
    headers = ("Date", "Price", "Platform", "Type", "Grade", "Title")
    rows = []
    for record in records:
        grade = " ".join(part for part in (record.grader, record.grade) if part)
        rows.append(
            (
                record.sold_date[:12],
                f"${record.price:,.2f}" if record.price is not None else "",
                record.platform[:14],
                record.sale_type[:12],
                grade[:10],
                (record.title or record.url)[:82],
            )
        )
    widths = [max(len(headers[index]), *(len(row[index]) for row in rows)) for index in range(len(headers))]
    print("  ".join(headers[index].ljust(widths[index]) for index in range(len(headers))))
    print("  ".join("-" * width for width in widths))
    for row in rows:
        print("  ".join(row[index].ljust(widths[index]) for index in range(len(headers))))


def print_csv(records: list[CompRecord]) -> None:
    fieldnames = ["sold_date", "price", "platform", "sale_type", "grader", "grade", "cert", "title", "url", "image_url"]
    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for record in records:
        writer.writerow(record.as_dict())


def print_records(records: list[CompRecord], output_format: str, *, include_raw: bool = False) -> None:
    if output_format == "json":
        print(json.dumps([record.as_dict(include_raw=include_raw) for record in records], indent=2))
    elif output_format == "csv":
        print_csv(records)
    else:
        print_table(records)


def print_sql_json(columns: list[str], rows: list[sqlite3.Row]) -> None:
    print(json.dumps([{column: row[column] for column in columns} for row in rows], indent=2))


def print_sql_csv(columns: list[str], rows: list[sqlite3.Row]) -> None:
    writer = csv.DictWriter(sys.stdout, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({column: row[column] for column in columns})


def add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--profile-dir",
        default=str(default_profile_dir()),
        help="Persistent browser profile directory. Defaults outside the repo under LOCALAPPDATA.",
    )
    parser.add_argument("--timeout-ms", type=int, default=30000)


def add_slab_query_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("query", nargs="?", default="")
    parser.add_argument("--grader", default="", choices=("", *GRADERS))
    parser.add_argument("--grade", default="")
    parser.add_argument("--cert", default="")
    parser.add_argument("--advanced", action="store_true", help="Prefix query with ! to disable typo/synonym expansion")
    parser.add_argument("--loose", action="store_true", help="Skip common slab/noise exclusions")
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        help="Add an extra Card Ladder negative term. Repeat for multiple exclusions.",
    )


def add_cache_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--cache-db",
        default=str(default_cache_path()),
        help="SQLite cache used for saved comps and local search.",
    )


def add_sales_filter_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--min-price", default="", help="Card Ladder Sales History minimum sale price")
    parser.add_argument("--max-price", default="", help="Card Ladder Sales History maximum sale price")
    parser.add_argument("--min-date", default="", help="Card Ladder Sales History minimum sold date, e.g. 01/01/2026")
    parser.add_argument("--max-date", default="", help="Card Ladder Sales History maximum sold date, e.g. 05/10/2026")
    parser.add_argument("--platform", action="append", default=[], help="Filter by marketplace/platform. Repeatable.")
    parser.add_argument("--listing-type", default="", choices=("", *LISTING_TYPES))
    parser.add_argument("--seller", default="", help="Filter by eBay seller ID")
    parser.add_argument("--verified", default="All", choices=VERIFIED_FILTERS)
    parser.add_argument("--sort", default="date", choices=tuple(SORT_FIELDS))
    parser.add_argument("--direction", default="desc", choices=SORT_DIRECTIONS)
    parser.add_argument(
        "--cert-all-grades",
        action="store_true",
        help="For --cert searches, uncheck Card Ladder's 'show sales from this grade only' option.",
    )


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Card Ladder slab comps CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    login = sub.add_parser("login", help="Open Card Ladder and persist your local browser login session")
    add_common_args(login)
    login.add_argument("--url", default=CARDLADDER_LOGIN_URL)

    query = sub.add_parser("build-query", help="Print the Card Ladder sales-history query this CLI would use")
    add_slab_query_args(query)

    comps = sub.add_parser("comps", help="Search Card Ladder Sales History for slab comps")
    add_common_args(comps)
    add_cache_args(comps)
    add_slab_query_args(comps)
    add_sales_filter_args(comps)
    comps.add_argument("--limit", type=int, default=10)
    comps.add_argument("--format", choices=("table", "json", "csv"), default="table")
    comps.add_argument("--include-raw", action="store_true", help="Include raw JSON object in --format json output")
    comps.add_argument(
        "--data-source",
        choices=("live", "local", "auto"),
        default="live",
        help="live opens Card Ladder, local reads the SQLite cache, auto uses fresh cache then falls back live.",
    )
    comps.add_argument(
        "--cache-max-age-hours",
        type=float,
        default=DEFAULT_CACHE_MAX_AGE_HOURS,
        help="Freshness threshold for --data-source auto.",
    )
    comps.add_argument("--no-cache-write", action="store_true", help="Do not save live results to the local SQLite cache")
    comps.add_argument("--headless", action="store_true", help="Run without opening a visible browser")
    comps.add_argument("--no-prompt", action="store_true", help="Fail instead of waiting for login/security verification")
    comps.add_argument("--debug-dir", default="", help="Save captured payloads, page text, and screenshot")

    sync = sub.add_parser("sync", help="Fetch live Card Ladder comps and store them in the local SQLite cache")
    add_common_args(sync)
    add_cache_args(sync)
    add_slab_query_args(sync)
    add_sales_filter_args(sync)
    sync.add_argument("--limit", type=int, default=25)
    sync.add_argument("--headless", action="store_true", help="Run without opening a visible browser")
    sync.add_argument("--no-prompt", action="store_true", help="Fail instead of waiting for login/security verification")
    sync.add_argument("--debug-dir", default="", help="Save captured payloads, page text, and screenshot")

    local = sub.add_parser("search", help="Search cached comps without opening Card Ladder")
    add_cache_args(local)
    local.add_argument("text", nargs="?", default="")
    local.add_argument("--grader", default="")
    local.add_argument("--grade", default="")
    local.add_argument("--cert", default="")
    local.add_argument("--limit", type=int, default=20)
    local.add_argument("--format", choices=("table", "json", "csv"), default="table")
    local.add_argument("--include-raw", action="store_true", help="Include raw JSON object in --format json output")

    summary = sub.add_parser("summary", help="Summarize slab comps with median, average, low, and high")
    add_common_args(summary)
    add_cache_args(summary)
    add_slab_query_args(summary)
    add_sales_filter_args(summary)
    summary.add_argument("--limit", type=int, default=25)
    summary.add_argument("--format", choices=("table", "json"), default="table")
    summary.add_argument("--data-source", choices=("live", "local", "auto"), default="auto")
    summary.add_argument("--cache-max-age-hours", type=float, default=DEFAULT_CACHE_MAX_AGE_HOURS)
    summary.add_argument("--no-cache-write", action="store_true", help="Do not save live results to the local SQLite cache")
    summary.add_argument("--headless", action="store_true", help="Run without opening a visible browser")
    summary.add_argument("--no-prompt", action="store_true", help="Fail instead of waiting for login/security verification")
    summary.add_argument("--debug-dir", default="", help="Save captured payloads, page text, and screenshot")

    sql = sub.add_parser("sql", help="Run a read-only SQL query against the local comps cache")
    add_cache_args(sql)
    sql.add_argument("sql")
    sql.add_argument("--format", choices=("table", "json", "csv"), default="table")
    return parser


async def async_main(argv: list[str] | None = None) -> int:
    parser = make_parser()
    args = parser.parse_args(argv)
    if args.command == "login":
        return await run_login(args)
    if args.command == "build-query":
        print(
            build_slab_query(
                args.query,
                grader=args.grader,
                grade=args.grade,
                cert=args.cert,
                advanced=args.advanced,
                strict=not args.loose,
                extra_exclusions=args.exclude or (),
            )
        )
        return 0
    if args.command == "comps":
        query = query_from_args(args)
        if not query:
            raise RuntimeError("Provide a query or --cert/--grader/--grade")
        records: list[CompRecord] = []
        if args.data_source in {"local", "auto"}:
            age_hours = cached_query_age_hours(args.cache_db, query)
            use_cache = args.data_source == "local" or (
                age_hours is not None and age_hours <= float(args.cache_max_age_hours)
            )
            if use_cache:
                records = load_cached_records(args.cache_db, query=query, limit=args.limit)
        if not records and args.data_source != "local":
            records = await collect_comps(args)
            if records and not args.no_cache_write:
                cache_records(args.cache_db, query, records)
        print_records(records, args.format, include_raw=args.include_raw)
        return 0 if records else 2
    if args.command == "sync":
        query = query_from_args(args)
        if not query:
            raise RuntimeError("Provide a query or --cert/--grader/--grade")
        records = await collect_comps(args)
        saved = cache_records(args.cache_db, query, records)
        print(f"Saved {saved} comps to {args.cache_db}")
        return 0 if records else 2
    if args.command == "search":
        records = load_cached_records(
            args.cache_db,
            text=args.text,
            grader=args.grader,
            grade=args.grade,
            cert=args.cert,
            limit=args.limit,
        )
        print_records(records, args.format, include_raw=args.include_raw)
        return 0 if records else 2
    if args.command == "summary":
        query = query_from_args(args)
        if not query:
            raise RuntimeError("Provide a query or --cert/--grader/--grade")
        records: list[CompRecord] = []
        if args.data_source in {"local", "auto"}:
            age_hours = cached_query_age_hours(args.cache_db, query)
            use_cache = args.data_source == "local" or (
                age_hours is not None and age_hours <= float(args.cache_max_age_hours)
            )
            if use_cache:
                records = load_cached_records(args.cache_db, query=query, limit=args.limit)
        if not records and args.data_source != "local":
            records = await collect_comps(args)
            if records and not args.no_cache_write:
                cache_records(args.cache_db, query, records)
        summary = summarize_records(records)
        if args.format == "json":
            print(json.dumps(summary, indent=2))
        else:
            print_summary(summary)
        return 0 if records else 2
    if args.command == "sql":
        columns, rows = sql_rows(args.cache_db, args.sql)
        if args.format == "json":
            print_sql_json(columns, rows)
        elif args.format == "csv":
            print_sql_csv(columns, rows)
        else:
            print_sql_table(columns, rows)
        return 0
    parser.error("Unknown command")
    return 2


def main() -> None:
    try:
        raise SystemExit(asyncio.run(async_main()))
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
    except KeyboardInterrupt:
        print("\nAborted.", file=sys.stderr)
        raise SystemExit(130)


if __name__ == "__main__":
    main()
