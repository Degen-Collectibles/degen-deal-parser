from __future__ import annotations

import json
import traceback
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import StatementError
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine, select

from app.models import TikTokAuth, TikTokCreatorAuth
from app.tiktok import token_storage
from app.tiktok.tiktok_ingest import (
    TikTokTokenExchangeResult,
    build_tiktok_auth_record,
    build_tiktok_creator_auth_record,
)


def _engine():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


def _raw_auth_tokens(engine, table: str, row_id: int) -> tuple:
    columns = (
        "access_token, refresh_token, creator_access_token, creator_refresh_token"
        if table == "tiktok_auth"
        else "access_token, refresh_token"
    )
    with engine.connect() as connection:
        return tuple(
            connection.exec_driver_sql(
                f"SELECT {columns} FROM {table} WHERE id = ?",
                (row_id,),
            ).one()
        )


def test_tiktok_tokens_are_encrypted_at_rest_and_transparently_decrypted():
    engine = _engine()
    with Session(engine) as session:
        seller = TikTokAuth(
            tiktok_shop_id="shop-secure",
            access_token="seller-access-secret",
            refresh_token="seller-refresh-secret",
            creator_access_token="creator-access-secret",
            creator_refresh_token="creator-refresh-secret",
        )
        creator = TikTokCreatorAuth(
            creator_username="creator-secure",
            access_token="standalone-access-secret",
            refresh_token="standalone-refresh-secret",
        )
        session.add(seller)
        session.add(creator)
        session.commit()
        session.refresh(seller)
        session.refresh(creator)

        seller_raw = _raw_auth_tokens(engine, "tiktok_auth", seller.id)
        creator_raw = _raw_auth_tokens(engine, "tiktok_creator_auth", creator.id)

        for stored in (*seller_raw, *creator_raw):
            assert stored.startswith(token_storage.TIKTOK_TOKEN_CIPHERTEXT_PREFIX)
            assert "secret" not in stored
        assert seller.access_token == "seller-access-secret"
        assert seller.creator_refresh_token == "creator-refresh-secret"
        assert creator.access_token == "standalone-access-secret"


def test_application_engine_hides_bound_parameters_in_database_errors():
    from app import db

    assert db.engine.hide_parameters is True


def test_token_payload_sanitizer_removes_nested_credentials_but_keeps_expiry_metadata():
    payload = {
        "access_token": "top-secret",
        "access_token_expire_in": 3600,
        "data": {
            "refreshToken": "nested-secret",
            "refresh_token_expires_in": 7200,
            "shop_id": "shop-1",
        },
        "rows": [{"creator_access_token": "creator-secret", "scope": "orders.read"}],
    }

    sanitized = token_storage.sanitize_tiktok_token_payload(payload)

    assert "access_token" not in sanitized
    assert sanitized["access_token_expire_in"] == 3600
    assert "refreshToken" not in sanitized["data"]
    assert sanitized["data"]["refresh_token_expires_in"] == 7200
    assert "creator_access_token" not in sanitized["rows"][0]
    assert sanitized["rows"][0]["scope"] == "orders.read"


def test_auth_record_builders_do_not_duplicate_tokens_in_raw_payload():
    token_result = TikTokTokenExchangeResult(
        access_token="record-access-secret",
        refresh_token="record-refresh-secret",
        shop_id="shop-record",
        open_id="open-record",
        raw_payload={
            "access_token": "record-access-secret",
            "refresh_token": "record-refresh-secret",
            "shop_id": "shop-record",
            "access_token_expire_in": 3600,
        },
    )

    seller = build_tiktok_auth_record(
        token_result,
        app_key="app-key",
        redirect_uri="https://example.test/callback",
    )
    creator = build_tiktok_creator_auth_record(
        token_result,
        creator_username="creator-record",
        app_key="app-key",
    )

    for record in (seller, creator):
        raw = json.loads(record["raw_payload"])
        assert "access_token" not in raw
        assert "refresh_token" not in raw
        assert "record-access-secret" not in record["raw_payload"]
        assert raw["access_token_expire_in"] == 3600


def test_migration_encrypts_legacy_plaintext_and_scrubs_raw_payload():
    engine = _engine()
    with Session(engine) as session:
        row = TikTokAuth(tiktok_shop_id="legacy-shop")
        creator = TikTokCreatorAuth(creator_username="legacy-creator")
        session.add(row)
        session.add(creator)
        session.commit()
        session.refresh(row)
        session.refresh(creator)
        with engine.begin() as connection:
            connection.exec_driver_sql(
                "UPDATE tiktok_auth SET access_token=?, refresh_token=?, "
                "creator_access_token=?, creator_refresh_token=?, raw_payload=? WHERE id=?",
                (
                    "legacy-access-secret",
                    "legacy-refresh-secret",
                    "legacy-creator-access-secret",
                    "legacy-creator-refresh-secret",
                    json.dumps(
                        {
                            "access_token": "legacy-access-secret",
                            "refresh_token": "legacy-refresh-secret",
                            "shop_id": "legacy-shop",
                        }
                    ),
                    row.id,
                ),
            )
            connection.exec_driver_sql(
                "UPDATE tiktok_creator_auth SET access_token=?, refresh_token=?, raw_payload=? WHERE id=?",
                (
                    "legacy-standalone-access-secret",
                    "legacy-standalone-refresh-secret",
                    json.dumps(
                        {
                            "accessToken": "legacy-standalone-access-secret",
                            "refreshToken": "legacy-standalone-refresh-secret",
                            "creator_username": "legacy-creator",
                        }
                    ),
                    creator.id,
                ),
            )

        migrated = token_storage.migrate_tiktok_token_storage(session)
        session.expire_all()
        refreshed = session.get(TikTokAuth, row.id)
        refreshed_creator = session.get(TikTokCreatorAuth, creator.id)
        raw_tokens = _raw_auth_tokens(engine, "tiktok_auth", row.id)
        raw_creator_tokens = _raw_auth_tokens(engine, "tiktok_creator_auth", creator.id)

        assert migrated["seller_rows"] == 1
        assert migrated["creator_rows"] == 1
        assert all(
            value.startswith(token_storage.TIKTOK_TOKEN_CIPHERTEXT_PREFIX)
            for value in (*raw_tokens, *raw_creator_tokens)
        )
        assert refreshed.access_token == "legacy-access-secret"
        assert refreshed.creator_refresh_token == "legacy-creator-refresh-secret"
        assert refreshed_creator.access_token == "legacy-standalone-access-secret"
        assert "legacy-access-secret" not in refreshed.raw_payload
        assert "legacy-standalone-access-secret" not in refreshed_creator.raw_payload
        assert json.loads(refreshed.raw_payload) == {"shop_id": "legacy-shop"}
        assert json.loads(refreshed_creator.raw_payload) == {
            "creator_username": "legacy-creator"
        }


def test_missing_encryption_key_fails_closed_for_new_token_writes(monkeypatch):
    engine = _engine()
    monkeypatch.setattr(token_storage, "configured_tiktok_token_keys", lambda: ())

    with Session(engine) as session:
        session.add(
            TikTokAuth(
                tiktok_shop_id="missing-key-shop",
                access_token="must-not-write-plaintext",
            )
        )
        with pytest.raises(StatementError):
            session.commit()
        session.rollback()


def test_legacy_token_migration_refuses_to_start_without_key(monkeypatch):
    engine = _engine()
    with Session(engine) as session:
        row = TikTokAuth(tiktok_shop_id="legacy-missing-key")
        session.add(row)
        session.commit()
        session.refresh(row)
        with engine.begin() as connection:
            connection.exec_driver_sql(
                "UPDATE tiktok_auth SET access_token=? WHERE id=?",
                ("legacy-plaintext-must-not-run", row.id),
            )
        monkeypatch.setattr(token_storage, "configured_tiktok_token_keys", lambda: ())

        with pytest.raises(token_storage.TikTokTokenConfigurationError):
            token_storage.migrate_tiktok_token_storage(session)

        with engine.connect() as connection:
            stored = connection.exec_driver_sql(
                "SELECT access_token FROM tiktok_auth WHERE id=?",
                (row.id,),
            ).scalar_one()
        assert stored == "legacy-plaintext-must-not-run"


def test_token_key_must_not_reuse_session_secret(monkeypatch):
    from app import config

    shared_secret = "shared-session-and-token-secret-000000000000000001"
    monkeypatch.setattr(
        config,
        "get_settings",
        lambda: SimpleNamespace(
            tiktok_token_encryption_keys=shared_secret,
            session_secret=shared_secret,
        ),
    )

    with pytest.raises(token_storage.TikTokTokenConfigurationError):
        token_storage.configured_tiktok_token_keys()


def test_key_ring_decrypts_previous_key_and_reencrypts_with_current(monkeypatch):
    old_key = "old-tiktok-token-encryption-key-000000000000000001"
    new_key = "new-tiktok-token-encryption-key-000000000000000001"
    engine = _engine()
    monkeypatch.setattr(token_storage, "configured_tiktok_token_keys", lambda: (old_key,))
    with Session(engine) as session:
        row = TikTokAuth(tiktok_shop_id="rotate-shop", access_token="rotate-secret")
        session.add(row)
        session.commit()
        session.refresh(row)
        old_ciphertext = _raw_auth_tokens(engine, "tiktok_auth", row.id)[0]

        monkeypatch.setattr(
            token_storage,
            "configured_tiktok_token_keys",
            lambda: (new_key, old_key),
        )
        session.expire_all()
        assert session.get(TikTokAuth, row.id).access_token == "rotate-secret"
        token_storage.migrate_tiktok_token_storage(session)
        new_ciphertext = _raw_auth_tokens(engine, "tiktok_auth", row.id)[0]

        assert new_ciphertext != old_ciphertext
        assert token_storage.decrypt_tiktok_token(new_ciphertext) == "rotate-secret"


def test_migration_does_not_rewrite_ciphertext_already_using_current_key():
    engine = _engine()
    with Session(engine) as session:
        row = TikTokAuth(
            tiktok_shop_id="idempotent-shop",
            access_token="stable-access-secret",
            refresh_token="stable-refresh-secret",
            raw_payload='{"shop_id":"idempotent-shop"}',
        )
        session.add(row)
        session.commit()
        session.refresh(row)
        before = _raw_auth_tokens(engine, "tiktok_auth", row.id)

        migrated = token_storage.migrate_tiktok_token_storage(session)
        after = _raw_auth_tokens(engine, "tiktok_auth", row.id)

        assert migrated == {"seller_rows": 0, "creator_rows": 0}
        assert after == before


def test_migration_never_overwrites_token_refresh_committed_after_read(
    tmp_path,
    monkeypatch,
):
    engine = create_engine(
        f"sqlite:///{(tmp_path / 'tiktok-token-race.sqlite3').as_posix()}",
        connect_args={"check_same_thread": False},
    )
    SQLModel.metadata.create_all(engine)
    with engine.begin() as connection:
        connection.exec_driver_sql("PRAGMA journal_mode=WAL")

    with Session(engine) as session:
        row = TikTokAuth(tiktok_shop_id="race-shop", raw_payload='{"shop_id":"race-shop"}')
        session.add(row)
        session.commit()
        session.refresh(row)
        row_id = row.id
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "UPDATE tiktok_auth SET access_token=?, refresh_token=? WHERE id=?",
            ("old-access-token", "old-refresh-token", row_id),
        )

    original_sanitizer = token_storage.sanitize_tiktok_raw_payload_json
    refresh_committed = False

    def commit_refresh_after_migration_read(value):
        nonlocal refresh_committed
        if not refresh_committed:
            refresh_committed = True
            with Session(engine) as refresh_session:
                refreshed = refresh_session.get(TikTokAuth, row_id)
                refreshed.access_token = "newly-refreshed-token"
                refreshed.refresh_token = "newly-refreshed-refresh"
                refresh_session.add(refreshed)
                refresh_session.commit()
        return original_sanitizer(value)

    monkeypatch.setattr(
        token_storage,
        "sanitize_tiktok_raw_payload_json",
        commit_refresh_after_migration_read,
    )

    with Session(engine) as migration_session:
        with pytest.raises(token_storage.TikTokTokenMigrationConflict):
            token_storage.migrate_tiktok_token_storage(migration_session)

    with Session(engine) as verification_session:
        refreshed = verification_session.get(TikTokAuth, row_id)
        assert refreshed.access_token == "newly-refreshed-token"
        assert refreshed.refresh_token == "newly-refreshed-refresh"


def test_migration_database_failure_never_exposes_plaintext_parameters():
    access_canary = "LEGACY_ACCESS_CANARY"
    refresh_canary = "LEGACY_REFRESH_CANARY"
    engine = _engine()
    with Session(engine) as session:
        row = TikTokAuth(tiktok_shop_id="failure-shop")
        session.add(row)
        session.commit()
        session.refresh(row)
        row_id = row.id
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "UPDATE tiktok_auth SET access_token=?, refresh_token=? WHERE id=?",
            (access_canary, refresh_canary, row_id),
        )
        connection.exec_driver_sql(
            "CREATE TRIGGER fail_tiktok_token_migration "
            "BEFORE UPDATE ON tiktok_auth BEGIN "
            "SELECT RAISE(ABORT, 'forced token migration failure'); END"
        )

    with Session(engine) as session:
        with pytest.raises(token_storage.TikTokTokenMigrationError) as exc_info:
            token_storage.migrate_tiktok_token_storage(session)

    rendered = "".join(
        traceback.format_exception(
            type(exc_info.value),
            exc_info.value,
            exc_info.value.__traceback__,
        )
    )
    for canary in (access_canary, refresh_canary):
        assert canary not in str(exc_info.value)
        assert canary not in repr(exc_info.value)
        assert canary not in rendered
