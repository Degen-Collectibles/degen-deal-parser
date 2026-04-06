import time
from typing import Any, Optional

_cache: dict[str, tuple[float, Any]] = {}


def cache_get(key: str, ttl: float = 60.0) -> Optional[Any]:
    entry = _cache.get(key)
    if entry is None:
        return None
    stored_at, value = entry
    if time.monotonic() - stored_at > ttl:
        del _cache[key]
        return None
    return value


def cache_set(key: str, value: Any) -> None:
    _cache[key] = (time.monotonic(), value)


def cache_invalidate(prefix: str) -> None:
    keys = [k for k in list(_cache) if k.startswith(prefix)]
    for k in keys:
        del _cache[k]
