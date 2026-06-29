"""Authenticated encryption and migration helpers for TikTok OAuth tokens."""

from __future__ import annotations

import base64
import hashlib
import json
import re
from typing import Any

from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy import Text, text
from sqlalchemy.types import TypeDecorator
from sqlmodel import Session


TIKTOK_TOKEN_CIPHERTEXT_PREFIX = "enc:v1:"
TIKTOK_TOKEN_KEY_MIN_CHARS = 32
_SENSITIVE_PAYLOAD_KEYS = frozenset(
    {
        "accesstoken",
        "refreshtoken",
        "creatoraccesstoken",
        "creatorrefreshtoken",
        "authorization",
        "xttsaccesstoken",
    }
)


class TikTokTokenConfigurationError(RuntimeError):
    pass


class TikTokTokenMigrationError(RuntimeError):
    """Token-free startup migration failure safe to surface in logs."""

    pass


class TikTokTokenMigrationConflict(TikTokTokenMigrationError):
    """A token row changed after migration read it; no stale value was written."""

    pass


def configured_tiktok_token_keys() -> tuple[str, ...]:
    from ..config import get_settings

    settings = get_settings()
    raw = str(getattr(settings, "tiktok_token_encryption_keys", "") or "")
    keys = tuple(part.strip() for part in raw.split(",") if part.strip())
    if any(len(key) < TIKTOK_TOKEN_KEY_MIN_CHARS for key in keys):
        raise TikTokTokenConfigurationError(
            "Every TIKTOK_TOKEN_ENCRYPTION_KEYS entry must be at least 32 characters."
        )
    session_secret = str(getattr(settings, "session_secret", "") or "").strip()
    if session_secret and session_secret in keys:
        raise TikTokTokenConfigurationError(
            "TIKTOK_TOKEN_ENCRYPTION_KEYS must not reuse SESSION_SECRET."
        )
    return keys


def _fernet(secret: str) -> Fernet:
    key = base64.urlsafe_b64encode(hashlib.sha256(secret.encode("utf-8")).digest())
    return Fernet(key)


def _encrypt_with_key(plaintext: str, key: str) -> str:
    ciphertext = _fernet(key).encrypt(plaintext.encode("utf-8")).decode("ascii")
    return f"{TIKTOK_TOKEN_CIPHERTEXT_PREFIX}{ciphertext}"


def _decrypt_with_keys(stored: str, keys: tuple[str, ...]) -> str:
    if not stored or not stored.startswith(TIKTOK_TOKEN_CIPHERTEXT_PREFIX):
        return stored
    if not keys:
        raise TikTokTokenConfigurationError(
            "Set TIKTOK_TOKEN_ENCRYPTION_KEYS before reading TikTok OAuth tokens."
        )
    ciphertext = stored[len(TIKTOK_TOKEN_CIPHERTEXT_PREFIX) :].encode("ascii")
    for key in keys:
        try:
            return _fernet(key).decrypt(ciphertext).decode("utf-8")
        except (InvalidToken, UnicodeDecodeError):
            continue
    raise TikTokTokenConfigurationError(
        "Stored TikTok token could not be decrypted with the configured key ring."
    )


def _protect_stored_token(stored_value: Any, keys: tuple[str, ...]) -> str | None:
    """Return current-key ciphertext while preserving already-current bytes."""
    if stored_value in (None, ""):
        return stored_value
    stored = str(stored_value)
    if not keys:
        raise TikTokTokenConfigurationError(
            "Set TIKTOK_TOKEN_ENCRYPTION_KEYS before storing TikTok OAuth tokens."
        )
    if stored.startswith(TIKTOK_TOKEN_CIPHERTEXT_PREFIX):
        ciphertext = stored[len(TIKTOK_TOKEN_CIPHERTEXT_PREFIX) :].encode("ascii")
        try:
            _fernet(keys[0]).decrypt(ciphertext).decode("utf-8")
            return stored
        except (InvalidToken, UnicodeDecodeError):
            pass
        plaintext = _decrypt_with_keys(stored, keys)
    else:
        plaintext = stored
    return _encrypt_with_key(plaintext, keys[0])


def encrypt_tiktok_token(value: str) -> str:
    plaintext = str(value or "")
    if not plaintext:
        return plaintext
    if plaintext.startswith(TIKTOK_TOKEN_CIPHERTEXT_PREFIX):
        plaintext = decrypt_tiktok_token(plaintext)
    keys = configured_tiktok_token_keys()
    if not keys:
        raise TikTokTokenConfigurationError(
            "Set TIKTOK_TOKEN_ENCRYPTION_KEYS before storing TikTok OAuth tokens."
        )
    return _encrypt_with_key(plaintext, keys[0])


def decrypt_tiktok_token(value: str) -> str:
    stored = str(value or "")
    keys = configured_tiktok_token_keys()
    return _decrypt_with_keys(stored, keys)


class EncryptedTikTokToken(TypeDecorator[str]):
    """TEXT-compatible encrypted field with transparent ORM plaintext access."""

    impl = Text
    cache_ok = True

    def process_bind_param(self, value: str | None, dialect: Any) -> str | None:
        if value in (None, ""):
            return value
        return encrypt_tiktok_token(str(value))

    def process_result_value(self, value: str | None, dialect: Any) -> str | None:
        if value in (None, ""):
            return value
        return decrypt_tiktok_token(str(value))


def _normalized_payload_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def sanitize_tiktok_token_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): sanitize_tiktok_token_payload(item)
            for key, item in value.items()
            if _normalized_payload_key(key) not in _SENSITIVE_PAYLOAD_KEYS
        }
    if isinstance(value, list):
        return [sanitize_tiktok_token_payload(item) for item in value]
    if isinstance(value, tuple):
        return [sanitize_tiktok_token_payload(item) for item in value]
    return value


def sanitize_tiktok_raw_payload_json(value: Any) -> str:
    payload = value
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8", errors="ignore")
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except (TypeError, ValueError):
            payload = {}
    if not isinstance(payload, (dict, list)):
        payload = {}
    return json.dumps(
        sanitize_tiktok_token_payload(payload),
        default=str,
        sort_keys=True,
        separators=(",", ":"),
    )


_TOKEN_TABLE_SPECS: tuple[tuple[str, tuple[str, ...], str], ...] = (
    (
        "tiktok_auth",
        (
            "access_token",
            "refresh_token",
            "creator_access_token",
            "creator_refresh_token",
        ),
        "seller_rows",
    ),
    (
        "tiktok_creator_auth",
        ("access_token", "refresh_token"),
        "creator_rows",
    ),
)


def _migration_rows(
    session: Session,
    table_name: str,
    token_fields: tuple[str, ...],
) -> list[dict[str, Any]]:
    columns = ", ".join(("id", *token_fields, "raw_payload"))
    lock_clause = " FOR UPDATE" if session.get_bind().dialect.name == "postgresql" else ""
    result = session.execute(text(f"SELECT {columns} FROM {table_name}{lock_clause}"))
    return [dict(row) for row in result.mappings().all()]


def _cas_update_migration_row(
    session: Session,
    *,
    table_name: str,
    row: dict[str, Any],
    values: dict[str, Any],
) -> None:
    changed_fields = tuple(values)
    guarded_fields = tuple(field for field in row if field != "id")
    set_sql = ", ".join(f"{field} = :new_{field}" for field in changed_fields)
    where_sql = " AND ".join(
        f"(({field} = :old_{field}) OR ({field} IS NULL AND :old_{field} IS NULL))"
        for field in guarded_fields
    )
    parameters: dict[str, Any] = {"row_id": row["id"]}
    parameters.update({f"new_{field}": value for field, value in values.items()})
    parameters.update({f"old_{field}": row[field] for field in guarded_fields})
    try:
        result = session.execute(
            text(f"UPDATE {table_name} SET {set_sql} WHERE id = :row_id AND {where_sql}"),
            parameters,
        )
    except Exception:
        raise TikTokTokenMigrationError(
            "TikTok OAuth token startup migration failed; stored values were not logged."
        ) from None
    if result.rowcount != 1:
        raise TikTokTokenMigrationConflict(
            "TikTok OAuth token row changed during startup migration; retry startup."
        )


def migrate_tiktok_token_storage(session: Session) -> dict[str, int]:
    """Encrypt/rotate token columns and scrub raw JSON without stale overwrites."""
    keys = configured_tiktok_token_keys()
    rows_by_table = {
        table_name: _migration_rows(session, table_name, token_fields)
        for table_name, token_fields, _counter_name in _TOKEN_TABLE_SPECS
    }
    has_tokens = any(
        row.get(field_name) not in (None, "")
        for table_name, token_fields, _counter_name in _TOKEN_TABLE_SPECS
        for row in rows_by_table[table_name]
        for field_name in token_fields
    )
    if has_tokens and not keys:
        session.rollback()
        raise TikTokTokenConfigurationError(
            "Set TIKTOK_TOKEN_ENCRYPTION_KEYS before starting with stored TikTok OAuth tokens."
        )

    migrated = {"seller_rows": 0, "creator_rows": 0}
    try:
        for table_name, token_fields, counter_name in _TOKEN_TABLE_SPECS:
            for row in rows_by_table[table_name]:
                candidate_values = {
                    field_name: _protect_stored_token(row.get(field_name), keys)
                    for field_name in token_fields
                }
                candidate_values["raw_payload"] = sanitize_tiktok_raw_payload_json(
                    row.get("raw_payload")
                )
                changed_values = {
                    field_name: value
                    for field_name, value in candidate_values.items()
                    if value != row.get(field_name)
                }
                if not changed_values:
                    continue
                _cas_update_migration_row(
                    session,
                    table_name=table_name,
                    row=row,
                    values=changed_values,
                )
                migrated[counter_name] += 1
        session.commit()
        session.expire_all()
    except (TikTokTokenConfigurationError, TikTokTokenMigrationError):
        session.rollback()
        raise
    except Exception:
        session.rollback()
        raise TikTokTokenMigrationError(
            "TikTok OAuth token startup migration failed; stored values were not logged."
        ) from None
    return migrated
