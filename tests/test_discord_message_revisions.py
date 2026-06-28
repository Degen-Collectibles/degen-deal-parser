from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime, timezone
import hashlib
import struct
from unittest.mock import patch

import pytest
from sqlalchemy import event, inspect, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.pool import StaticPool
from fastapi import HTTPException
from sqlmodel import Session, SQLModel, create_engine, select

from app import db
from app.discord import message_revisions
from app.discord.message_revisions import (
    compute_message_snapshot_hash,
    ensure_message_revision,
    get_latest_message_revision,
)
from app.models import (
    DISCORD_SOURCE_REFRESH_REQUIRED_ERROR,
    DiscordMessage,
    DiscordMessageRevision,
    Transaction,
    TransactionSourceRevision,
)
from app.routers.admin_actions import (
    _bulk_clear_all_discord_messages,
    _bulk_clear_channel_discord_messages,
    clear_all_messages,
    clear_all_messages_form,
    clear_channel_messages,
    clear_channel_messages_form,
)


def _message(
    *,
    content: str,
    attachment_urls_json: str,
    discord_message_id: str = "discord-123",
) -> DiscordMessage:
    return DiscordMessage(
        discord_message_id=discord_message_id,
        channel_id="channel-1",
        content=content,
        attachment_urls_json=attachment_urls_json,
        created_at=datetime(2026, 6, 27, 12, 0, tzinfo=timezone.utc),
    )


def _revision_engine(*, include_transactions: bool = False):
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def _enable_foreign_keys(dbapi_connection, _connection_record):
        dbapi_connection.execute("PRAGMA foreign_keys=ON")

    DiscordMessage.__table__.create(engine)
    DiscordMessageRevision.__table__.create(engine)
    if include_transactions:
        Transaction.__table__.create(engine)
        TransactionSourceRevision.__table__.create(engine)
    return engine


def _file_revision_engine(db_path):
    engine = create_engine(
        f"sqlite:///{db_path.as_posix()}",
        connect_args={"check_same_thread": False, "timeout": 0.1},
    )

    @event.listens_for(engine, "connect")
    def _enable_foreign_keys(dbapi_connection, _connection_record):
        dbapi_connection.execute("PRAGMA foreign_keys=ON")

    DiscordMessage.__table__.create(engine)
    DiscordMessageRevision.__table__.create(engine)
    return engine


def _all_revisions(session: Session, message_id: int) -> list[DiscordMessageRevision]:
    return list(
        session.exec(
            select(DiscordMessageRevision)
            .where(DiscordMessageRevision.message_id == message_id)
            .order_by(DiscordMessageRevision.revision_number)
        ).all()
    )


def test_fresh_message_captures_revision_one_from_exact_projection() -> None:
    engine = _revision_engine()
    raw_attachments = '[{"url":"https://example.test/a.png"}]'

    with Session(engine) as session:
        message = _message(content="Buy 450 cash", attachment_urls_json=raw_attachments)
        session.add(message)

        revision = ensure_message_revision(session, message)

        assert message.id is not None
        assert revision.id is not None
        assert revision.message_id == message.id
        assert revision.revision_number == 1
        assert revision.content == "Buy 450 cash"
        assert revision.attachment_urls_json == raw_attachments
        assert revision.source_edited_at is None
        assert revision.snapshot_hash == compute_message_snapshot_hash(
            "Buy 450 cash", raw_attachments
        )
        assert message.current_revision_id == revision.id
        assert get_latest_message_revision(session, message.id).id == revision.id

    engine.dispose()


def test_changed_projection_appends_revision_without_mutating_original() -> None:
    engine = _revision_engine()
    original_attachments = '["https://example.test/original.png"]'
    edited_attachments = '["https://example.test/edited.png"]'
    edited_at = datetime(2026, 6, 27, 12, 5, tzinfo=timezone.utc)

    with Session(engine) as session:
        message = _message(content="Sell 100 cash", attachment_urls_json=original_attachments)
        session.add(message)
        original = ensure_message_revision(session, message)
        original_id = original.id
        original_hash = original.snapshot_hash

        message.content = "Sell 125 zelle"
        message.attachment_urls_json = edited_attachments
        message.edited_at = edited_at
        edited = ensure_message_revision(session, message)

        assert edited.revision_number == 2
        assert edited.content == "Sell 125 zelle"
        assert edited.attachment_urls_json == edited_attachments
        assert edited.source_edited_at == edited_at
        assert message.current_revision_id == edited.id

        unchanged_original = session.get(DiscordMessageRevision, original_id)
        assert unchanged_original is not None
        assert unchanged_original.revision_number == 1
        assert unchanged_original.content == "Sell 100 cash"
        assert unchanged_original.attachment_urls_json == original_attachments
        assert unchanged_original.snapshot_hash == original_hash
        assert [row.revision_number for row in _all_revisions(session, message.id)] == [1, 2]

    engine.dispose()


def test_identical_snapshot_dedupes_and_repairs_null_current_pointer() -> None:
    engine = _revision_engine()

    with Session(engine) as session:
        message = _message(content="Buy 10", attachment_urls_json="not valid json [")
        session.add(message)
        original = ensure_message_revision(session, message)

        message.current_revision_id = None
        session.flush()
        repeated = ensure_message_revision(session, message)

        assert repeated.id == original.id
        assert message.current_revision_id == original.id
        assert len(_all_revisions(session, message.id)) == 1

    engine.dispose()


def test_legacy_projection_is_captured_before_edited_revision() -> None:
    engine = _revision_engine()

    with Session(engine) as session:
        legacy = _message(content="old source", attachment_urls_json="legacy malformed ]")
        session.add(legacy)
        session.flush()
        assert legacy.current_revision_id is None

        pre_edit = ensure_message_revision(session, legacy)
        legacy.content = "new source"
        legacy.attachment_urls_json = '["new"]'
        post_edit = ensure_message_revision(session, legacy)

        revisions = _all_revisions(session, legacy.id)
        assert pre_edit.revision_number == 1
        assert post_edit.revision_number == 2
        assert [(row.content, row.attachment_urls_json) for row in revisions] == [
            ("old source", "legacy malformed ]"),
            ("new source", '["new"]'),
        ]

    engine.dispose()


def test_snapshot_hash_is_sha256_with_unambiguous_exact_string_framing() -> None:
    content = "a\u0000b \U0001f600"
    raw_attachments = '[ {"url": "x"} ]'
    content_bytes = content.encode("utf-8")
    attachment_bytes = raw_attachments.encode("utf-8")
    expected = hashlib.sha256(
        b"discord-message-revision-v1\x00"
        + struct.pack(">Q", len(content_bytes))
        + content_bytes
        + struct.pack(">Q", len(attachment_bytes))
        + attachment_bytes
    ).hexdigest()

    assert compute_message_snapshot_hash(content, raw_attachments) == expected
    assert compute_message_snapshot_hash(content, raw_attachments) == expected
    assert compute_message_snapshot_hash("a", "bc") != compute_message_snapshot_hash("ab", "c")
    assert compute_message_snapshot_hash(content, raw_attachments) != compute_message_snapshot_hash(
        content, '[{"url":"x"}]'
    )


def test_revision_model_has_stable_table_constraints_and_revision_foreign_keys() -> None:
    assert DiscordMessageRevision.__tablename__ == "discord_message_revisions"
    assert {column.name for column in DiscordMessageRevision.__table__.columns} == {
        "id",
        "message_id",
        "revision_number",
        "content",
        "attachment_urls_json",
        "source_edited_at",
        "captured_at",
        "snapshot_hash",
    }

    unique_columns = {
        tuple(column.name for column in constraint.columns)
        for constraint in DiscordMessageRevision.__table__.constraints
        if constraint.__class__.__name__ == "UniqueConstraint"
    }
    assert ("message_id", "revision_number") in unique_columns
    assert ("message_id", "id") in unique_columns
    assert {
        foreign_key.target_fullname
        for foreign_key in DiscordMessageRevision.__table__.c.message_id.foreign_keys
    } == {"discordmessage.id"}
    assert {
        foreign_key.target_fullname
        for foreign_key in DiscordMessage.__table__.c.current_revision_id.foreign_keys
    } == {"discord_message_revisions.id"}
    assert {
        foreign_key.target_fullname
        for foreign_key in Transaction.__table__.c.source_revision_id.foreign_keys
    } == {"discord_message_revisions.id"}
    assert DiscordMessage.__table__.c.current_revision_id.nullable
    assert DiscordMessage.__table__.c.active_parse_attempt_id.nullable
    assert Transaction.__table__.c.source_revision_id.nullable


def test_discord_source_refresh_quarantine_is_persisted_and_migrated() -> None:
    assert "source_refresh_required" in DiscordMessage.__table__.c
    assert (
        db.SQLITE_ADDITIVE_MIGRATIONS["discordmessage"]["source_refresh_required"]
        == "BOOLEAN DEFAULT 0"
    )
    assert (
        db.POSTGRES_ADDITIVE_MIGRATIONS["discordmessage"]["source_refresh_required"]
        == "BOOLEAN DEFAULT FALSE"
    )

    discord_revision_foreign_keys = {
        tuple(
            (element.parent.name, element.target_fullname)
            for element in constraint.elements
        )
        for constraint in DiscordMessage.__table__.foreign_key_constraints
    }
    transaction_revision_foreign_keys = {
        tuple(
            (element.parent.name, element.target_fullname)
            for element in constraint.elements
        )
        for constraint in Transaction.__table__.foreign_key_constraints
    }
    assert (
        ("id", "discord_message_revisions.message_id"),
        ("current_revision_id", "discord_message_revisions.id"),
    ) in discord_revision_foreign_keys
    assert (
        ("source_message_id", "discord_message_revisions.message_id"),
        ("source_revision_id", "discord_message_revisions.id"),
    ) in transaction_revision_foreign_keys

    assert TransactionSourceRevision.__tablename__ == "transaction_source_revisions"
    assert {column.name for column in TransactionSourceRevision.__table__.columns} == {
        "id",
        "transaction_id",
        "message_id",
        "revision_id",
        "source_position",
    }
    association_unique_columns = {
        tuple(column.name for column in constraint.columns)
        for constraint in TransactionSourceRevision.__table__.constraints
        if constraint.__class__.__name__ == "UniqueConstraint"
    }
    assert ("transaction_id", "message_id") in association_unique_columns
    assert ("transaction_id", "source_position") in association_unique_columns
    association_foreign_keys = {
        tuple(
            (element.parent.name, element.target_fullname)
            for element in constraint.elements
        )
        for constraint in TransactionSourceRevision.__table__.foreign_key_constraints
    }
    assert (
        ("message_id", "discord_message_revisions.message_id"),
        ("revision_id", "discord_message_revisions.id"),
    ) in association_foreign_keys
    assert {
        foreign_key.target_fullname
        for foreign_key in TransactionSourceRevision.__table__.c.transaction_id.foreign_keys
    } == {"transaction.id"}


def test_fresh_schema_rejects_cross_message_transaction_source_revision_links() -> None:
    engine = _revision_engine(include_transactions=True)

    with Session(engine) as session:
        first_message = _message(
            content="first",
            attachment_urls_json="[]",
            discord_message_id="association-first",
        )
        second_message = _message(
            content="second",
            attachment_urls_json="[]",
            discord_message_id="association-second",
        )
        session.add(first_message)
        session.add(second_message)
        first_revision = ensure_message_revision(session, first_message)
        second_revision = ensure_message_revision(session, second_message)
        transaction = Transaction(
            source_message_id=first_message.id,
            source_revision_id=first_revision.id,
            occurred_at=first_message.created_at,
        )
        session.add(transaction)
        session.flush()

        session.add(
            TransactionSourceRevision(
                transaction_id=transaction.id,
                message_id=first_message.id,
                revision_id=second_revision.id,
                source_position=0,
            )
        )
        with pytest.raises(IntegrityError):
            session.flush()


def test_admin_physical_clear_routes_refuse_messages_with_immutable_revisions() -> None:
    engine = _revision_engine()
    with Session(engine) as session:
        message = _message(content="immutable", attachment_urls_json="[]")
        session.add(message)
        ensure_message_revision(session, message)
        session.commit()

        with pytest.raises(HTTPException) as all_conflict:
            _bulk_clear_all_discord_messages(session)
        assert all_conflict.value.status_code == 409

        with pytest.raises(HTTPException) as channel_conflict:
            _bulk_clear_channel_discord_messages(session, message.channel_id)
        assert channel_conflict.value.status_code == 409
        assert session.get(DiscordMessage, message.id) is not None


def test_actual_admin_json_and_form_clear_routes_return_409_without_deleting_evidence() -> None:
    engine = _revision_engine(include_transactions=True)
    with Session(engine) as session:
        message = _message(content="immutable route evidence", attachment_urls_json="[]")
        session.add(message)
        revision = ensure_message_revision(session, message)
        transaction = Transaction(
            source_message_id=message.id,
            source_revision_id=revision.id,
            occurred_at=message.created_at,
        )
        session.add(transaction)
        session.flush()
        association = TransactionSourceRevision(
            transaction_id=transaction.id,
            message_id=message.id,
            revision_id=revision.id,
            source_position=0,
        )
        session.add(association)
        session.commit()

        with patch(
            "app.routers.admin_actions.require_role_response",
            return_value=None,
        ), patch(
            "app.routers.admin_actions.managed_session",
            side_effect=lambda: nullcontext(session),
        ):
            with pytest.raises(HTTPException) as json_conflict:
                clear_all_messages(request=object())
            with pytest.raises(HTTPException) as form_conflict:
                clear_all_messages_form(request=object())

        assert json_conflict.value.status_code == 409
        assert form_conflict.value.status_code == 409
        assert session.get(DiscordMessage, message.id) is not None
        assert session.get(DiscordMessageRevision, revision.id) is not None
        assert session.get(Transaction, transaction.id) is not None
        assert session.get(TransactionSourceRevision, association.id) is not None


def test_actual_admin_channel_json_and_form_clear_routes_return_409_without_deleting_evidence() -> None:
    engine = _revision_engine(include_transactions=True)
    with Session(engine) as session:
        message = _message(content="immutable channel evidence", attachment_urls_json="[]")
        session.add(message)
        revision = ensure_message_revision(session, message)
        transaction = Transaction(
            source_message_id=message.id,
            source_revision_id=revision.id,
            occurred_at=message.created_at,
        )
        session.add(transaction)
        session.flush()
        association = TransactionSourceRevision(
            transaction_id=transaction.id,
            message_id=message.id,
            revision_id=revision.id,
            source_position=0,
        )
        session.add(association)
        session.commit()

        with patch(
            "app.routers.admin_actions.require_role_response",
            return_value=None,
        ), patch(
            "app.routers.admin_actions.managed_session",
            side_effect=lambda: nullcontext(session),
        ):
            with pytest.raises(HTTPException) as json_conflict:
                clear_channel_messages(
                    request=object(),
                    channel_id=message.channel_id,
                )
            with pytest.raises(HTTPException) as form_conflict:
                clear_channel_messages_form(
                    request=object(),
                    channel_id=message.channel_id,
                )

        assert json_conflict.value.status_code == 409
        assert form_conflict.value.status_code == 409
        assert session.get(DiscordMessage, message.id) is not None
        assert session.get(DiscordMessageRevision, revision.id) is not None
        assert session.get(Transaction, transaction.id) is not None
        assert session.get(TransactionSourceRevision, association.id) is not None


def test_fresh_schema_rejects_cross_message_revision_links() -> None:
    engine = _revision_engine(include_transactions=True)

    with Session(engine) as session:
        first_message = _message(
            content="first",
            attachment_urls_json="[]",
            discord_message_id="discord-first",
        )
        second_message = _message(
            content="second",
            attachment_urls_json="[]",
            discord_message_id="discord-second",
        )
        first_revision = ensure_message_revision(session, first_message)
        second_revision = ensure_message_revision(session, second_message)
        session.commit()
        first_message_id = first_message.id
        first_revision_id = first_revision.id
        second_revision_id = second_revision.id

    with Session(engine) as session:
        first_message = session.get(DiscordMessage, first_message_id)
        assert first_message is not None
        first_message.current_revision_id = second_revision_id
        with pytest.raises(IntegrityError, match="FOREIGN KEY"):
            session.flush()
        session.rollback()

    with Session(engine) as session:
        cross_linked = Transaction(
            source_message_id=first_message_id,
            source_revision_id=second_revision_id,
            occurred_at=datetime(2026, 6, 27, 12, 0, tzinfo=timezone.utc),
        )
        session.add(cross_linked)
        with pytest.raises(IntegrityError, match="FOREIGN KEY"):
            session.flush()
        session.rollback()

        correctly_linked = Transaction(
            source_message_id=first_message_id,
            source_revision_id=first_revision_id,
            occurred_at=datetime(2026, 6, 27, 12, 0, tzinfo=timezone.utc),
        )
        session.add(correctly_linked)
        session.commit()

    engine.dispose()


def test_revision_helper_exposes_no_update_or_delete_api() -> None:
    assert not hasattr(message_revisions, "update_message_revision")
    assert not hasattr(message_revisions, "delete_message_revision")


def test_orm_rejects_revision_updates_after_insert() -> None:
    engine = _revision_engine()

    with Session(engine) as session:
        message = _message(content="original", attachment_urls_json="[]")
        revision = ensure_message_revision(session, message)
        session.commit()

        revision.content = "mutated"
        with pytest.raises(RuntimeError, match="append-only"):
            session.flush()
        session.rollback()

        stored = session.get(DiscordMessageRevision, revision.id)
        assert stored is not None
        assert stored.content == "original"

    engine.dispose()


def test_orm_rejects_revision_deletes_after_insert() -> None:
    engine = _revision_engine()

    with Session(engine) as session:
        message = _message(content="preserved", attachment_urls_json="[]")
        revision = ensure_message_revision(session, message)
        session.commit()
        revision_id = revision.id

        session.delete(revision)
        with pytest.raises(RuntimeError, match="append-only"):
            session.flush()
        session.rollback()

        assert session.get(DiscordMessageRevision, revision_id) is not None

    engine.dispose()


def test_sqlite_trigger_allows_revision_inserts_and_rejects_update_delete() -> None:
    engine = _revision_engine()

    with Session(engine) as session:
        message = _message(content="revision one", attachment_urls_json="[]")
        first = ensure_message_revision(session, message)
        session.commit()
        message_id = message.id
        first_id = first.id

    with engine.begin() as connection:
        db.install_sqlite_discord_revision_guards(connection)
        db.install_sqlite_discord_revision_guards(connection)
        connection.execute(
            text(
                "INSERT INTO discord_message_revisions "
                "(message_id, revision_number, content, attachment_urls_json, "
                "captured_at, snapshot_hash) "
                "VALUES (:message_id, 2, 'revision two', '[]', "
                "CURRENT_TIMESTAMP, :snapshot_hash)"
            ),
            {
                "message_id": message_id,
                "snapshot_hash": compute_message_snapshot_hash("revision two", "[]"),
            },
        )

    with pytest.raises(IntegrityError, match="append-only"):
        with engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE discord_message_revisions SET content = 'tampered' "
                    "WHERE revision_number = 2"
                )
            )

    with pytest.raises(IntegrityError, match="append-only"):
        with engine.begin() as connection:
            connection.execute(
                text("DELETE FROM discord_message_revisions WHERE revision_number = 2")
            )

    with engine.connect() as connection:
        rows = connection.execute(
            text(
                "SELECT id, revision_number, content FROM discord_message_revisions "
                "ORDER BY revision_number"
            )
        ).all()
        assert rows == [(first_id, 1, "revision one"), (rows[1][0], 2, "revision two")]

    engine.dispose()


def test_sqlite_trigger_rejects_insert_or_replace_revision_collisions() -> None:
    engine = _revision_engine()

    with Session(engine) as session:
        message = _message(content="original", attachment_urls_json="[]")
        revision = ensure_message_revision(session, message)
        session.commit()
        message_id = message.id
        revision_id = revision.id

    with engine.begin() as connection:
        db.install_sqlite_discord_revision_guards(connection)

    collision_statements = (
        (
            "INSERT OR REPLACE INTO discord_message_revisions "
            "(id, message_id, revision_number, content, attachment_urls_json, "
            "captured_at, snapshot_hash) "
            "VALUES (:revision_id, :message_id, 1, 'replaced by id', '[]', "
            "CURRENT_TIMESTAMP, 'replacement-hash')",
            revision_id,
        ),
        (
            "INSERT OR REPLACE INTO discord_message_revisions "
            "(id, message_id, revision_number, content, attachment_urls_json, "
            "captured_at, snapshot_hash) "
            "VALUES (:revision_id, :message_id, 1, 'replaced by number', '[]', "
            "CURRENT_TIMESTAMP, 'replacement-hash')",
            revision_id + 100,
        ),
    )
    for statement, colliding_id in collision_statements:
        with pytest.raises(IntegrityError, match="append-only"):
            with engine.begin() as connection:
                connection.execute(
                    text(statement),
                    {
                        "revision_id": colliding_id,
                        "message_id": message_id,
                    },
                )

    with engine.connect() as connection:
        assert connection.execute(
            text(
                "SELECT id, message_id, revision_number, content, snapshot_hash "
                "FROM discord_message_revisions"
            )
        ).one() == (
            revision_id,
            message_id,
            1,
            "original",
            compute_message_snapshot_hash("original", "[]"),
        )

    engine.dispose()


def test_revision_helper_does_not_commit_callers_transaction() -> None:
    engine = _revision_engine()
    session = Session(engine)
    message = _message(content="uncommitted", attachment_urls_json="[]")
    session.add(message)
    ensure_message_revision(session, message)
    session.close()

    with Session(engine) as verification_session:
        assert verification_session.exec(select(DiscordMessage)).all() == []
        assert verification_session.exec(select(DiscordMessageRevision)).all() == []

    engine.dispose()


def _seed_and_detach_message(engine):
    with Session(engine) as session:
        message = _message(
            content="revision one",
            attachment_urls_json='["one"]',
            discord_message_id="race-target",
        )
        revision = ensure_message_revision(session, message)
        session.commit()
        message_id = message.id
        revision_id = revision.id

    with Session(engine) as session:
        message = session.get(DiscordMessage, message_id)
        revision = session.get(DiscordMessageRevision, revision_id)
        assert message is not None
        assert revision is not None
        session.expunge(message)
        session.expunge(revision)

    return message, revision


def _run_competing_revision_race(
    *,
    engine,
    monkeypatch,
    target_content: str,
    competing_content: str,
    unrelated_discord_id: str,
):
    target_message, stale_revision = _seed_and_detach_message(engine)
    target_message_id = target_message.id
    target_message.content = target_content
    target_message.attachment_urls_json = '["target"]'
    real_get_latest = message_revisions.get_latest_message_revision
    competing_revision_ids: list[int] = []
    first_lookup = True

    def stale_then_compete(session, message_id):
        nonlocal first_lookup
        if first_lookup:
            first_lookup = False
            with Session(engine) as competing_session:
                competing_revision = DiscordMessageRevision(
                    message_id=message_id,
                    revision_number=2,
                    content=competing_content,
                    attachment_urls_json='["target"]',
                    snapshot_hash=compute_message_snapshot_hash(
                        competing_content,
                        '["target"]',
                    ),
                )
                competing_session.add(competing_revision)
                competing_session.commit()
                assert competing_revision.id is not None
                competing_revision_ids.append(competing_revision.id)
            return stale_revision
        return real_get_latest(session, message_id)

    monkeypatch.setattr(
        message_revisions,
        "get_latest_message_revision",
        stale_then_compete,
    )

    with Session(engine) as session:
        unrelated = _message(
            content="outer transaction work",
            attachment_urls_json="[]",
            discord_message_id=unrelated_discord_id,
        )
        session.add(unrelated)
        resolved_revision = ensure_message_revision(session, target_message)
        session.commit()
        resolved_revision_id = resolved_revision.id

    with Session(engine) as session:
        stored_message = session.get(DiscordMessage, target_message_id)
        assert stored_message is not None
        revisions = _all_revisions(session, target_message_id)
        unrelated = session.exec(
            select(DiscordMessage).where(
                DiscordMessage.discord_message_id == unrelated_discord_id
            )
        ).one()

        return {
            "competing_revision_id": competing_revision_ids[0],
            "resolved_revision_id": resolved_revision_id,
            "current_revision_id": stored_message.current_revision_id,
            "revisions": revisions,
            "unrelated_content": unrelated.content,
        }


def test_unique_race_dedupes_identical_competing_revision_and_preserves_outer_work(
    tmp_path,
    monkeypatch,
) -> None:
    engine = _file_revision_engine(tmp_path / "identical-race.sqlite3")

    result = _run_competing_revision_race(
        engine=engine,
        monkeypatch=monkeypatch,
        target_content="same revision two",
        competing_content="same revision two",
        unrelated_discord_id="outer-identical",
    )

    assert result["resolved_revision_id"] == result["competing_revision_id"]
    assert result["current_revision_id"] == result["competing_revision_id"]
    assert [revision.revision_number for revision in result["revisions"]] == [1, 2]
    assert result["unrelated_content"] == "outer transaction work"
    engine.dispose()


def test_unique_race_appends_after_distinct_competing_revision_and_preserves_outer_work(
    tmp_path,
    monkeypatch,
) -> None:
    engine = _file_revision_engine(tmp_path / "distinct-race.sqlite3")

    result = _run_competing_revision_race(
        engine=engine,
        monkeypatch=monkeypatch,
        target_content="our revision",
        competing_content="competing revision",
        unrelated_discord_id="outer-distinct",
    )

    assert result["resolved_revision_id"] != result["competing_revision_id"]
    assert result["current_revision_id"] == result["resolved_revision_id"]
    assert [revision.revision_number for revision in result["revisions"]] == [1, 2, 3]
    assert [revision.content for revision in result["revisions"]] == [
        "revision one",
        "competing revision",
        "our revision",
    ]
    assert result["unrelated_content"] == "outer transaction work"
    engine.dispose()


def test_retry_exhaustion_preserves_outer_transaction_via_caller_savepoint(
    tmp_path,
    monkeypatch,
) -> None:
    engine = _file_revision_engine(tmp_path / "retry-exhaustion.sqlite3")
    target_message, stale_revision = _seed_and_detach_message(engine)
    target_message_id = target_message.id
    stale_revision_id = stale_revision.id

    with Session(engine) as competing_session:
        competing_revision = DiscordMessageRevision(
            message_id=target_message.id,
            revision_number=2,
            content="already claimed",
            attachment_urls_json="[]",
            snapshot_hash=compute_message_snapshot_hash("already claimed", "[]"),
        )
        competing_session.add(competing_revision)
        competing_session.commit()

    monkeypatch.setattr(
        message_revisions,
        "get_latest_message_revision",
        lambda _session, _message_id: stale_revision,
    )

    with Session(engine) as session:
        unrelated = _message(
            content="survives retry exhaustion",
            attachment_urls_json="[]",
            discord_message_id="outer-exhaustion",
        )
        session.add(unrelated)
        session.flush()

        target_savepoint = session.begin_nested()
        target_message.content = "cannot append"
        target_message.attachment_urls_json = "[]"
        with pytest.raises(RuntimeError, match="after 3 attempts") as raised:
            ensure_message_revision(session, target_message)
        assert isinstance(raised.value.__cause__, IntegrityError)
        target_savepoint.rollback()
        session.commit()

    with Session(engine) as session:
        stored_target = session.get(DiscordMessage, target_message_id)
        assert stored_target is not None
        assert stored_target.content == "revision one"
        assert stored_target.current_revision_id == stale_revision_id
        assert session.exec(
            select(DiscordMessage).where(
                DiscordMessage.discord_message_id == "outer-exhaustion"
            )
        ).one().content == "survives retry exhaustion"
        assert [
            revision.revision_number
            for revision in _all_revisions(session, target_message_id)
        ] == [1, 2]

    engine.dispose()


def test_revision_columns_and_indexes_are_declared_for_both_engines() -> None:
    assert db.SQLITE_ADDITIVE_MIGRATIONS["discordmessage"]["current_revision_id"] == "INTEGER"
    assert db.SQLITE_ADDITIVE_MIGRATIONS["transaction"]["source_revision_id"] == "INTEGER"
    assert db.POSTGRES_ADDITIVE_MIGRATIONS["discordmessage"]["current_revision_id"] == "INTEGER"
    assert db.POSTGRES_ADDITIVE_MIGRATIONS["transaction"]["source_revision_id"] == "INTEGER"
    assert db.SQLITE_ADDITIVE_MIGRATIONS["discordmessage"]["active_parse_attempt_id"] == "INTEGER"
    assert db.POSTGRES_ADDITIVE_MIGRATIONS["discordmessage"]["active_parse_attempt_id"] == "INTEGER"

    sqlite_indexes = set(db.SQLITE_INDEX_MIGRATIONS)
    postgres_indexes = set(db.POSTGRES_INDEX_MIGRATIONS)
    expected_indexes = {
        "CREATE INDEX IF NOT EXISTS idx_discordmessage_current_revision_id "
        "ON discordmessage (current_revision_id)",
        'CREATE INDEX IF NOT EXISTS idx_transaction_source_revision_id '
        'ON "transaction" (source_revision_id)',
        "CREATE INDEX IF NOT EXISTS idx_discordmessage_active_parse_attempt_id "
        "ON discordmessage (active_parse_attempt_id)",
        "CREATE INDEX IF NOT EXISTS idx_transaction_source_revisions_message_id "
        "ON transaction_source_revisions (message_id)",
        "CREATE INDEX IF NOT EXISTS idx_transaction_source_revisions_revision_id "
        "ON transaction_source_revisions (revision_id)",
    }
    assert expected_indexes <= sqlite_indexes
    assert expected_indexes <= postgres_indexes


def test_ensure_sqlite_schema_adds_revision_columns_without_rewriting_rows(
    tmp_path, monkeypatch
) -> None:
    test_engine = create_engine(
        f"sqlite:///{(tmp_path / 'legacy-revisions.sqlite3').as_posix()}",
        connect_args={"check_same_thread": False},
    )
    with test_engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE discordmessage "
                "(id INTEGER PRIMARY KEY, discord_message_id TEXT, content TEXT, last_error TEXT)"
            )
        )
        connection.execute(
            text(
                'CREATE TABLE "transaction" '
                "(id INTEGER PRIMARY KEY, source_message_id INTEGER, source_content TEXT)"
            )
        )
        connection.execute(
            text(
                "CREATE TABLE discord_message_revisions ("
                "id INTEGER PRIMARY KEY, message_id INTEGER NOT NULL, "
                "revision_number INTEGER NOT NULL, content TEXT NOT NULL, "
                "attachment_urls_json TEXT NOT NULL, source_edited_at TIMESTAMP, "
                "captured_at TIMESTAMP NOT NULL, snapshot_hash TEXT NOT NULL, "
                "UNIQUE(message_id, revision_number), UNIQUE(message_id, id))"
            )
        )
        connection.execute(
            text(
                "INSERT INTO discordmessage (id, discord_message_id, content, last_error) "
                "VALUES (7, 'legacy-7', 'preserve me', :last_error)"
            ),
            {"last_error": DISCORD_SOURCE_REFRESH_REQUIRED_ERROR},
        )
        connection.execute(
            text(
                "INSERT INTO discordmessage (id, discord_message_id, content) "
                "VALUES (8, 'legacy-8', 'other message')"
            )
        )
        connection.execute(
            text(
                'INSERT INTO "transaction" (id, source_message_id, source_content) '
                "VALUES (9, 7, 'preserve transaction')"
            )
        )
        connection.execute(
            text(
                "INSERT INTO discord_message_revisions "
                "(id, message_id, revision_number, content, attachment_urls_json, "
                "captured_at, snapshot_hash) VALUES "
                "(70, 7, 1, 'preserve me', '[]', CURRENT_TIMESTAMP, 'hash-70'), "
                "(80, 8, 1, 'other message', '[]', CURRENT_TIMESTAMP, 'hash-80')"
            )
        )

    sqlite_columns = {
        "discordmessage": {
            "current_revision_id": "INTEGER",
            "source_refresh_required": "BOOLEAN DEFAULT 0",
        },
        "transaction": {"source_revision_id": "INTEGER"},
    }
    sqlite_indexes = [
        "CREATE INDEX IF NOT EXISTS idx_discordmessage_current_revision_id "
        "ON discordmessage (current_revision_id)",
        'CREATE INDEX IF NOT EXISTS idx_transaction_source_revision_id '
        'ON "transaction" (source_revision_id)',
    ]
    monkeypatch.setattr(db, "engine", test_engine)
    monkeypatch.setattr(db, "database_url", str(test_engine.url))
    monkeypatch.setattr(db, "SQLITE_ADDITIVE_MIGRATIONS", sqlite_columns)
    monkeypatch.setattr(db, "SQLITE_INDEX_MIGRATIONS", sqlite_indexes)
    monkeypatch.setattr(db, "migrate_legacy_sqlite_shopify_orders", lambda _connection: None)
    monkeypatch.setattr(db, "_migrate_shift_entry_drop_unique", lambda _connection: None)
    monkeypatch.setattr(
        db, "_migrate_schedule_roster_member_calendar_unique", lambda _connection: None
    )

    db.ensure_sqlite_schema()

    inspector = inspect(test_engine)
    assert {column["name"] for column in inspector.get_columns("discordmessage")} >= {
        "current_revision_id"
    }
    assert {column["name"] for column in inspector.get_columns("transaction")} >= {
        "source_revision_id"
    }
    assert {index["name"] for index in inspector.get_indexes("discordmessage")} >= {
        "idx_discordmessage_current_revision_id"
    }
    assert {index["name"] for index in inspector.get_indexes("transaction")} >= {
        "idx_transaction_source_revision_id"
    }
    with test_engine.connect() as connection:
        assert connection.execute(
            text(
                "SELECT id, discord_message_id, content, current_revision_id, "
                "source_refresh_required "
                "FROM discordmessage WHERE id = 7"
            )
        ).one() == (7, "legacy-7", "preserve me", None, 1)
        assert connection.execute(
            text(
                'SELECT id, source_message_id, source_content, source_revision_id '
                'FROM "transaction"'
            )
        ).one() == (9, 7, "preserve transaction", None)

    with test_engine.begin() as connection:
        connection.execute(
            text("UPDATE discordmessage SET current_revision_id = 70 WHERE id = 7")
        )
        connection.execute(
            text('UPDATE "transaction" SET source_revision_id = 70 WHERE id = 9')
        )

    with pytest.raises(IntegrityError, match="revision provenance"):
        with test_engine.begin() as connection:
            connection.execute(
                text("UPDATE discordmessage SET current_revision_id = 80 WHERE id = 7")
            )

    with pytest.raises(IntegrityError, match="revision provenance"):
        with test_engine.begin() as connection:
            connection.execute(
                text('UPDATE "transaction" SET source_revision_id = 999 WHERE id = 9')
            )

    with pytest.raises(IntegrityError, match="revision provenance"):
        with test_engine.begin() as connection:
            connection.execute(
                text(
                    'INSERT INTO "transaction" '
                    "(id, source_message_id, source_content, source_revision_id) "
                    "VALUES (10, 7, 'cross-linked', 80)"
                )
            )

    with pytest.raises(IntegrityError, match="revision provenance"):
        with test_engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO discordmessage "
                    "(id, discord_message_id, content, current_revision_id) "
                    "VALUES (10, 'dangling', 'dangling', 999)"
                )
            )

    with test_engine.connect() as connection:
        assert connection.execute(
            text("SELECT current_revision_id FROM discordmessage WHERE id = 7")
        ).scalar_one() == 70
        assert connection.execute(
            text('SELECT source_revision_id FROM "transaction" WHERE id = 9')
        ).scalar_one() == 70

    test_engine.dispose()


def test_postgres_schema_generation_includes_revision_columns_and_indexes(
    monkeypatch,
) -> None:
    statements: list[str] = []

    class FakeEngine:
        def begin(self):
            return nullcontext(object())

    monkeypatch.setattr(db, "database_url", "postgresql+psycopg://test/revisions")
    monkeypatch.setattr(db, "engine", FakeEngine())
    monkeypatch.setattr(
        db,
        "_pg_migrate_statement",
        lambda statement, _label: statements.append(statement),
    )
    monkeypatch.setattr(
        db,
        "_pg_security_migrate_statement",
        lambda statement, _label: statements.append(statement),
    )
    monkeypatch.setattr(db, "migrate_legacy_postgres_shopify_orders", lambda _connection: None)

    db.ensure_postgres_schema()

    normalized_statements = [" ".join(statement.split()) for statement in statements]

    assert (
        "ALTER TABLE discordmessage ADD COLUMN IF NOT EXISTS current_revision_id INTEGER"
        in statements
    )
    assert (
        "ALTER TABLE transaction ADD COLUMN IF NOT EXISTS source_revision_id INTEGER" in statements
    )
    assert (
        "CREATE INDEX IF NOT EXISTS idx_discordmessage_current_revision_id "
        "ON discordmessage (current_revision_id)"
        in statements
    )
    assert (
        'CREATE INDEX IF NOT EXISTS idx_transaction_source_revision_id '
        'ON "transaction" (source_revision_id)'
        in statements
    )
    assert any(
        "CREATE OR REPLACE FUNCTION reject_discord_message_revision_mutation()"
        in statement
        for statement in normalized_statements
    )
    assert any(
        "CREATE UNIQUE INDEX IF NOT EXISTS "
        "uq_discord_message_revisions_message_id_id "
        "ON discord_message_revisions (message_id, id)"
        in statement
        for statement in normalized_statements
    )
    assert any(
        "ADD CONSTRAINT fk_discordmessage_current_revision_provenance "
        "FOREIGN KEY (id, current_revision_id) "
        "REFERENCES discord_message_revisions (message_id, id) NOT VALID"
        in statement
        for statement in normalized_statements
    )
    assert any(
        "ADD CONSTRAINT fk_transaction_source_revision_provenance "
        "FOREIGN KEY (source_message_id, source_revision_id) "
        "REFERENCES discord_message_revisions (message_id, id) NOT VALID"
        in statement
        for statement in normalized_statements
    )
    assert any(
        "CREATE TRIGGER trg_discord_message_revisions_append_only "
        "BEFORE UPDATE OR DELETE ON discord_message_revisions"
        in statement
        for statement in normalized_statements
    )
    assert any(
        "CREATE TABLE IF NOT EXISTS transaction_source_revisions "
        in statement
        and "FOREIGN KEY (message_id, revision_id) REFERENCES "
        "discord_message_revisions (message_id, id)" in statement
        and "UNIQUE (transaction_id, message_id)" in statement
        and "UNIQUE (transaction_id, source_position)" in statement
        for statement in normalized_statements
    )
    revision_unique_position = next(
        index
        for index, statement in enumerate(normalized_statements)
        if "uq_discord_message_revisions_message_id_id" in statement
    )
    association_create_position = next(
        index
        for index, statement in enumerate(normalized_statements)
        if "CREATE TABLE IF NOT EXISTS transaction_source_revisions" in statement
    )
    association_index_position = next(
        index
        for index, statement in enumerate(normalized_statements)
        if "idx_transaction_source_revisions_message_id" in statement
    )
    assert revision_unique_position < association_create_position < association_index_position


def test_ensure_sqlite_schema_creates_transaction_source_revisions_for_legacy_database(
    tmp_path, monkeypatch
) -> None:
    test_engine = create_engine(
        f"sqlite:///{(tmp_path / 'legacy-source-associations.sqlite3').as_posix()}",
        connect_args={"check_same_thread": False},
    )
    with test_engine.begin() as connection:
        connection.execute(text("CREATE TABLE discordmessage (id INTEGER PRIMARY KEY)"))
        connection.execute(text('CREATE TABLE "transaction" (id INTEGER PRIMARY KEY)'))
        connection.execute(
            text(
                "CREATE TABLE discord_message_revisions ("
                "id INTEGER PRIMARY KEY, message_id INTEGER NOT NULL, "
                "UNIQUE(message_id, id))"
            )
        )

    monkeypatch.setattr(db, "engine", test_engine)
    monkeypatch.setattr(db, "database_url", str(test_engine.url))
    monkeypatch.setattr(db, "SQLITE_ADDITIVE_MIGRATIONS", {})
    monkeypatch.setattr(
        db,
        "SQLITE_INDEX_MIGRATIONS",
        [
            "CREATE INDEX IF NOT EXISTS idx_transaction_source_revisions_message_id "
            "ON transaction_source_revisions (message_id)",
            "CREATE INDEX IF NOT EXISTS idx_transaction_source_revisions_revision_id "
            "ON transaction_source_revisions (revision_id)",
        ],
    )
    monkeypatch.setattr(db, "install_sqlite_discord_revision_guards", lambda _connection: None)
    monkeypatch.setattr(db, "migrate_legacy_sqlite_shopify_orders", lambda _connection: None)
    monkeypatch.setattr(db, "_migrate_shift_entry_drop_unique", lambda _connection: None)
    monkeypatch.setattr(
        db, "_migrate_schedule_roster_member_calendar_unique", lambda _connection: None
    )

    db.ensure_sqlite_schema()

    inspector = inspect(test_engine)
    assert "transaction_source_revisions" in inspector.get_table_names()
    assert {column["name"] for column in inspector.get_columns("transaction_source_revisions")} == {
        "id",
        "transaction_id",
        "message_id",
        "revision_id",
        "source_position",
    }
    unique_sets = {
        tuple(constraint["column_names"])
        for constraint in inspector.get_unique_constraints("transaction_source_revisions")
    }
    assert ("transaction_id", "message_id") in unique_sets
    assert ("transaction_id", "source_position") in unique_sets
    foreign_keys = {
        (
            tuple(foreign_key["constrained_columns"]),
            foreign_key["referred_table"],
            tuple(foreign_key["referred_columns"]),
        )
        for foreign_key in inspector.get_foreign_keys("transaction_source_revisions")
    }
    assert (
        ("message_id", "revision_id"),
        "discord_message_revisions",
        ("message_id", "id"),
    ) in foreign_keys
    assert {index["name"] for index in inspector.get_indexes("transaction_source_revisions")} >= {
        "idx_transaction_source_revisions_message_id",
        "idx_transaction_source_revisions_revision_id",
    }
    test_engine.dispose()


def test_postgres_revision_security_migrations_fail_closed_with_lock_timeout(
    monkeypatch,
) -> None:
    executed: list[str] = []

    class FailingConnection:
        def execute(self, statement):
            rendered = str(statement)
            executed.append(rendered)
            if "SET LOCAL lock_timeout" in rendered:
                return None
            raise RuntimeError("permission denied")

    class FailingEngine:
        def begin(self):
            return nullcontext(FailingConnection())

    monkeypatch.setattr(db, "engine", FailingEngine())

    with pytest.raises(RuntimeError, match="required security migration failed"):
        db._pg_security_migrate_statement(
            "CREATE TRIGGER required_guard BEFORE UPDATE ON protected_table",
            "protected_table.required_guard",
        )

    assert "SET LOCAL lock_timeout = '5s'" in executed[0]
    assert "CREATE TRIGGER required_guard" in executed[1]


def test_postgres_revision_guard_installer_propagates_security_ddl_failure(
    monkeypatch,
) -> None:
    def fail_required_migration(_statement, label):
        raise RuntimeError(f"blocked {label}")

    monkeypatch.setattr(
        db,
        "_pg_security_migrate_statement",
        fail_required_migration,
    )

    with pytest.raises(RuntimeError, match="message_id_id_unique"):
        db.install_postgres_discord_revision_guards()
