from __future__ import annotations

import json
from typing import Any

from sqlmodel import Session

from .models import AuditLog


def _clean_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _clean_payload(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_clean_payload(item) for item in value]
    return value


def record_financial_audit(
    session: Session,
    *,
    action: str,
    resource_key: str,
    before: dict[str, Any] | None = None,
    after: dict[str, Any] | None = None,
    actor_user_id: int | None = None,
    actor_label: str = "",
    note: str = "",
) -> AuditLog:
    details = {
        "actor": actor_label or "system",
        "before": _clean_payload(before or {}),
        "after": _clean_payload(after or {}),
    }
    if note:
        details["note"] = note
    row = AuditLog(
        actor_user_id=actor_user_id,
        action=action,
        resource_key=resource_key,
        details_json=json.dumps(details, sort_keys=True, default=str),
    )
    session.add(row)
    return row
