from __future__ import annotations

import hashlib
import hmac
from typing import Optional

from ..config import Settings, get_settings


def keyed_fingerprint(
    value: str,
    *,
    namespace: str,
    length: int = 16,
    settings: Optional[Settings] = None,
) -> str:
    settings = settings or get_settings()
    key_text = (getattr(settings, "employee_token_hmac_key", "") or "").strip()
    if not key_text:
        raise ValueError("EMPLOYEE_TOKEN_HMAC_KEY is required for audit fingerprints")
    payload = f"{namespace}:{value or ''}".encode("utf-8")
    return hmac.new(key_text.encode("utf-8"), payload, hashlib.sha256).hexdigest()[:length]
