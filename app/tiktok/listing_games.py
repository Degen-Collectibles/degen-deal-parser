"""Game choices for listings, with exact catalog identity and an offline fallback."""
from __future__ import annotations

import logging
import re
import threading
import time

import httpx

PRIMARY_GAMES = (
    ("Pokemon", ("3", "85")),
    ("Magic", ("1",)),
    ("Yu-Gi-Oh", ("2",)),
    ("One Piece", ("68",)),
    ("Lorcana", ("71",)),
    ("Riftbound", ("89",)),
    ("Dragon Ball Super: Fusion World", ("80",)),
)
CATALOG_URL = "https://openapi.tcgtracking.com/v1/categories"
# Catalog categories that describe supplies or lots rather than a game.
SUPPLY_IDS = {"31", "32", "35", "49", "50", "56"}
_cache: list[dict] = []
_expires = 0.0
_lock = threading.Lock()


def normalize(value):
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower().replace("é", "e")).strip()


def primary(value):
    clean = normalize(value)
    aliases = {"mtg": "magic", "magic the gathering": "magic", "yugioh": "yu gi oh",
               "one piece card game": "one piece", "disney lorcana": "lorcana",
               "riftbound league of legends trading card game": "riftbound",
               "fusion world": "dragon ball super fusion world", "dbs fusion world": "dragon ball super fusion world"}
    clean = aliases.get(clean, clean)
    for name, ids in PRIMARY_GAMES:
        if clean == normalize(name):
            return {"id": name, "name": name, "category_ids": list(ids)}
    return None


def catalog():
    global _cache, _expires
    with _lock:
        if _cache and time.monotonic() < _expires:
            return _cache, ""
        try:
            with httpx.Client(timeout=12, follow_redirects=False, trust_env=False) as client:
                response = client.get(CATALOG_URL)
                response.raise_for_status()
                rows = response.json()["categories"]
            if not isinstance(rows, list):
                raise ValueError("Invalid game catalog")
            games = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                key = str(row.get("id", ""))
                name = str(row.get("display_name") or row.get("name") or "").strip()
                if not key.isdigit() or key in SUPPLY_IDS or not name or len(name) > 180:
                    continue
                games.append({"id": "catalog:" + key, "name": name, "category_ids": [key]})
            if not games:
                raise ValueError("Empty game catalog")
            _cache = sorted(games, key=lambda g: g["name"].casefold())
            _expires = time.monotonic() + 3600
            return _cache, ""
        except (httpx.HTTPError, ValueError, KeyError, TypeError):
            logging.getLogger(__name__).warning("Listing game catalog lookup unavailable")
            warning = "Game catalog is unavailable right now. Try again, choose a listed game, or upload a product image and enter its details manually."
            return _cache, warning


def search(query):
    if len(query) > 180:
        raise ValueError("Enter a game name up to 180 characters.")
    games, warning = catalog()
    words = normalize(query).split()
    return {"games": [dict(g) for g in games if all(w in normalize(g["name"]) for w in words)], "warning": warning}


def resolve(value):
    known = primary(value)
    if known:
        return known
    if not str(value).startswith("catalog:"):
        raise ValueError("Choose a listed game, or use Other game to search and select its catalog.")
    games, warning = catalog()
    match = next((g for g in games if g["id"] == value), None)
    if not match:
        raise ValueError(warning or "This game is not in the catalog. Search again or upload a product image and enter its details manually.")
    return dict(match)


def matches_title(title, game):
    """Do not borrow defaults across distinct games, especially DBS catalogs."""
    text, target = normalize(title), normalize(game)
    if not target:
        return False
    if target == "dragon ball super fusion world":
        return "fusion world" in text and "masters" not in text
    if "dragon ball" in target:
        return target in text and ("fusion world" in target) == ("fusion world" in text)
    return bool(re.search(r"(?:^| )" + re.escape(target) + r"(?: |$)", text))
