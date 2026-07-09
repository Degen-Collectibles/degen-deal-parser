from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi import HTTPException
from sqlalchemy import event
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine, select

from app.discord.discord_ingest import insert_or_update_message, mark_message_deleted_row
from app.discord.message_revisions import ensure_message_revision
from app.discord.reparse import reparse_message_rows
from app.discord.transactions import (
    MAX_TRANSACTION_SOURCE_ROWS,
    StaleSourceRevisionError,
    invalidate_transactions_for_message,
    rebuild_transactions,
    sync_transaction_from_message,
    transaction_base_query,
)
from app.models import (
    DISCORD_SOURCE_REFRESH_REQUIRED_ERROR,
    DiscordMessage,
    DiscordMessageRevision,
    PARSE_IGNORED,
    PARSE_PARSED,
    PARSE_PENDING,
    Transaction,
    TransactionItem,
    TransactionSourceRevision,
)


def _engine(*, enforce_foreign_keys: bool = True):
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def _foreign_keys(connection, _record):
        if enforce_foreign_keys:
            connection.execute("PRAGMA foreign_keys=ON")

    SQLModel.metadata.create_all(engine)
    return engine


def _message(discord_id: str, content: str, *, created_second: int) -> DiscordMessage:
    return DiscordMessage(
        discord_message_id=discord_id,
        channel_id="999",
        channel_name="deals",
        author_id="7",
        author_name="Trader",
        content=content,
        attachment_urls_json="[]",
        created_at=datetime(2026, 6, 28, 12, 0, created_second, tzinfo=timezone.utc),
        parse_status=PARSE_PARSED,
        deal_type="buy",
        entry_kind="buy",
        amount=50.0,
        money_out=50.0,
    )


def _fake_discord_message(row: DiscordMessage, content: str):
    class Author:
        id = 7
        bot = False

        def __str__(self):
            return "Trader"

    author = Author()
    return SimpleNamespace(
        id=int(row.discord_message_id),
        content=content,
        channel=SimpleNamespace(id=999, name="deals"),
        guild=SimpleNamespace(id=8),
        author=author,
        attachments=[],
        created_at=row.created_at,
        edited_at=datetime.now(timezone.utc),
    )


def _seed_stitched_transaction(session: Session):
    primary = _message("1001", "Photo", created_second=1)
    child = _message("1002", "Buy 50 cash", created_second=2)
    session.add(primary)
    session.add(child)
    session.flush()
    primary.stitched_group_id = "group-1"
    primary.stitched_primary = True
    child.stitched_group_id = "group-1"
    child.stitched_primary = False
    child.parse_status = PARSE_IGNORED
    ids = json.dumps([primary.id, child.id])
    primary.stitched_message_ids_json = ids
    child.stitched_message_ids_json = ids
    ensure_message_revision(session, primary)
    ensure_message_revision(session, child)
    transaction = sync_transaction_from_message(
        session,
        primary,
        source_rows=[primary, child],
        source_content="Message 1: Photo\nMessage 2: Buy 50 cash",
    )
    session.commit()
    return primary.id, child.id, transaction.id


def test_successful_group_sync_binds_all_revisions_and_atomically_replaces_sources() -> None:
    engine = _engine()
    with Session(engine) as session:
        primary_id, child_id, transaction_id = _seed_stitched_transaction(session)
        associations = session.exec(
            select(TransactionSourceRevision)
            .where(TransactionSourceRevision.transaction_id == transaction_id)
            .order_by(TransactionSourceRevision.source_position)
        ).all()
        transaction = session.get(Transaction, transaction_id)
        assert [(row.message_id, row.source_position) for row in associations] == [
            (primary_id, 0),
            (child_id, 1),
        ]
        assert transaction.source_content == "Message 1: Photo\nMessage 2: Buy 50 cash"

        primary = session.get(DiscordMessage, primary_id)
        primary.stitched_group_id = None
        primary.stitched_primary = False
        sync_transaction_from_message(
            session,
            primary,
            source_rows=[primary],
            source_content=primary.content,
        )
        session.commit()
        replaced = session.exec(
            select(TransactionSourceRevision).where(
                TransactionSourceRevision.transaction_id == transaction_id
            )
        ).all()
        assert [(row.message_id, row.source_position) for row in replaced] == [(primary_id, 0)]
    engine.dispose()


def test_explicit_sync_refuses_oversized_existing_associations_before_deletion() -> None:
    engine = _engine()
    with Session(engine) as session:
        source_rows = [
            _message(
                f"oversized-existing-{index}",
                "Buy 50 cash" if index == 0 else f"fragment {index}",
                created_second=1,
            )
            for index in range(MAX_TRANSACTION_SOURCE_ROWS + 1)
        ]
        session.add_all(source_rows)
        session.flush()
        revisions = [ensure_message_revision(session, source_row) for source_row in source_rows]
        primary = source_rows[0]
        transaction = Transaction(
            source_message_id=primary.id,
            source_revision_id=revisions[0].id,
            occurred_at=primary.created_at,
            parse_status=PARSE_PARSED,
            amount=50,
            money_out=50,
            source_content="oversized original evidence",
        )
        session.add(transaction)
        session.flush()
        for position, (source_row, revision) in enumerate(zip(source_rows, revisions)):
            session.add(
                TransactionSourceRevision(
                    transaction_id=transaction.id,
                    message_id=source_row.id,
                    revision_id=revision.id,
                    source_position=position,
                )
            )
        session.commit()
        transaction_id = transaction.id

        with pytest.raises(StaleSourceRevisionError, match="association.*oversized"):
            sync_transaction_from_message(
                session,
                primary,
                source_rows=[primary],
                source_content=primary.content,
            )

        transaction = session.get(Transaction, transaction_id)
        associations = session.exec(
            select(TransactionSourceRevision)
            .where(TransactionSourceRevision.transaction_id == transaction_id)
            .order_by(TransactionSourceRevision.source_position)
        ).all()
        assert transaction.source_content == "oversized original evidence"
        assert len(associations) == MAX_TRANSACTION_SOURCE_ROWS + 1
        assert [association.source_position for association in associations] == list(
            range(MAX_TRANSACTION_SOURCE_ROWS + 1)
        )
        session.rollback()
    engine.dispose()


def test_valid_ignored_child_tombstones_only_its_standalone_transaction() -> None:
    engine = _engine()
    with Session(engine) as session:
        primary = _message("standalone-primary", "Photo", created_second=1)
        child = _message("standalone-child", "Buy 50 cash", created_second=2)
        session.add(primary)
        session.add(child)
        session.flush()
        ensure_message_revision(session, primary)
        ensure_message_revision(session, child)

        child_transaction = sync_transaction_from_message(session, child)
        session.add(
            TransactionItem(
                transaction_id=child_transaction.id,
                direction="named",
                item_name="Old standalone evidence",
            )
        )
        session.flush()
        child_transaction_id = child_transaction.id
        child_source_revision_id = child_transaction.source_revision_id
        child_source_content = child_transaction.source_content
        child_association_before = session.exec(
            select(TransactionSourceRevision).where(
                TransactionSourceRevision.transaction_id == child_transaction_id
            )
        ).one()
        child_association_tuple = (
            child_association_before.id,
            child_association_before.message_id,
            child_association_before.revision_id,
            child_association_before.source_position,
        )

        membership = json.dumps([primary.id, child.id])
        primary.stitched_group_id = "standalone-to-child-group"
        primary.stitched_primary = True
        primary.stitched_message_ids_json = membership
        child.stitched_group_id = "standalone-to-child-group"
        child.stitched_primary = False
        child.stitched_message_ids_json = membership
        child.parse_status = PARSE_IGNORED
        primary_transaction = sync_transaction_from_message(
            session,
            primary,
            source_rows=[primary, child],
            source_content="Message 1: Photo\nMessage 2: Buy 50 cash",
        )
        session.commit()
        primary_transaction_id = primary_transaction.id
        primary_associations_before = [
            (association.id, association.message_id, association.revision_id, association.source_position)
            for association in session.exec(
                select(TransactionSourceRevision)
                .where(TransactionSourceRevision.transaction_id == primary_transaction_id)
                .order_by(TransactionSourceRevision.source_position)
            ).all()
        ]

        sync_transaction_from_message(session, child)
        session.commit()

        child_transaction = session.get(Transaction, child_transaction_id)
        primary_transaction = session.get(Transaction, primary_transaction_id)
        child_association_after = session.exec(
            select(TransactionSourceRevision).where(
                TransactionSourceRevision.transaction_id == child_transaction_id
            )
        ).one()
        primary_associations_after = [
            (association.id, association.message_id, association.revision_id, association.source_position)
            for association in session.exec(
                select(TransactionSourceRevision)
                .where(TransactionSourceRevision.transaction_id == primary_transaction_id)
                .order_by(TransactionSourceRevision.source_position)
            ).all()
        ]
        child_items = session.exec(
            select(TransactionItem).where(TransactionItem.transaction_id == child_transaction_id)
        ).all()

        assert child_transaction.is_deleted is True
        assert child_transaction.parse_status == PARSE_IGNORED
        assert child_transaction.source_revision_id == child_source_revision_id
        assert child_transaction.source_content == child_source_content
        assert (
            child_association_after.id,
            child_association_after.message_id,
            child_association_after.revision_id,
            child_association_after.source_position,
        ) == child_association_tuple
        assert child_items == []
        assert primary_transaction.is_deleted is False
        assert primary_associations_after == primary_associations_before
    engine.dispose()


@pytest.mark.parametrize(
    ("field_name", "new_value"),
    [
        ("content", "Mutated without a revision"),
        ("attachment_urls_json", '["https://cdn.example/mutated.jpg"]'),
    ],
)
def test_implicit_bound_sync_rejects_projection_snapshot_mismatch(
    field_name: str,
    new_value: str,
) -> None:
    engine = _engine()
    with Session(engine) as session:
        primary_id, _child_id, transaction_id = _seed_stitched_transaction(session)
        transaction = session.get(Transaction, transaction_id)
        source_revision_id = transaction.source_revision_id
        source_content = transaction.source_content
        association_tuples = [
            (association.id, association.message_id, association.revision_id, association.source_position)
            for association in session.exec(
                select(TransactionSourceRevision)
                .where(TransactionSourceRevision.transaction_id == transaction_id)
                .order_by(TransactionSourceRevision.source_position)
            ).all()
        ]
        revision_count = len(session.exec(select(DiscordMessageRevision)).all())

        primary = session.get(DiscordMessage, primary_id)
        setattr(primary, field_name, new_value)
        session.add(primary)
        session.flush()

        with pytest.raises(StaleSourceRevisionError, match="bound source projection"):
            sync_transaction_from_message(session, primary)
        session.rollback()

        transaction = session.get(Transaction, transaction_id)
        assert transaction.source_revision_id == source_revision_id
        assert transaction.source_content == source_content
        assert len(session.exec(select(DiscordMessageRevision)).all()) == revision_count
        assert [
            (association.id, association.message_id, association.revision_id, association.source_position)
            for association in session.exec(
                select(TransactionSourceRevision)
                .where(TransactionSourceRevision.transaction_id == transaction_id)
                .order_by(TransactionSourceRevision.source_position)
            ).all()
        ] == association_tuples
    engine.dispose()


def test_implicit_legacy_group_sync_rejects_child_projection_mismatch_without_bindings() -> None:
    engine = _engine()
    with Session(engine) as session:
        primary = _message("legacy-implicit-primary", "Photo", created_second=1)
        child = _message("legacy-implicit-child", "Buy 50 cash", created_second=2)
        child.parse_status = PARSE_IGNORED
        session.add(primary)
        session.add(child)
        session.flush()
        membership = json.dumps([primary.id, child.id])
        primary.stitched_group_id = "legacy-implicit-group"
        primary.stitched_primary = True
        primary.stitched_message_ids_json = membership
        child.stitched_group_id = "legacy-implicit-group"
        child.stitched_primary = False
        child.stitched_message_ids_json = membership
        primary_revision = ensure_message_revision(session, primary)
        ensure_message_revision(session, child)
        transaction = Transaction(
            source_message_id=primary.id,
            source_revision_id=primary_revision.id,
            occurred_at=primary.created_at,
            parse_status=PARSE_PARSED,
            amount=50,
            money_out=50,
            source_content="legacy source evidence",
            is_deleted=True,
        )
        session.add(transaction)
        session.commit()
        transaction_id = transaction.id
        revision_count = len(session.exec(select(DiscordMessageRevision)).all())

        child.content = "Buy 75 cash without revision"
        session.add(child)
        session.commit()

        with pytest.raises(StaleSourceRevisionError, match="current source projection"):
            sync_transaction_from_message(session, primary)
        session.rollback()

        transaction = session.get(Transaction, transaction_id)
        associations = session.exec(
            select(TransactionSourceRevision).where(
                TransactionSourceRevision.transaction_id == transaction_id
            )
        ).all()
        assert transaction.is_deleted is True
        assert transaction.source_content == "legacy source evidence"
        assert associations == []
        assert len(session.exec(select(DiscordMessageRevision)).all()) == revision_count
    engine.dispose()


def test_child_edit_invalidates_primary_money_and_preserves_bound_associations() -> None:
    engine = _engine()

    @contextmanager
    def managed_session():
        with Session(engine) as session:
            yield session

    with Session(engine) as session:
        _primary_id, child_id, transaction_id = _seed_stitched_transaction(session)
        child = session.get(DiscordMessage, child_id)
        edited_message = _fake_discord_message(child, "Buy 75 cash")

    with (
        patch("app.discord.discord_ingest.managed_session", managed_session),
        patch("app.discord.discord_ingest.sync_attachment_assets"),
        patch("app.discord.discord_ingest.ingest_log"),
    ):
        tracked, action = insert_or_update_message(
            edited_message,
            is_edit=True,
            watched_channel_ids=set(),
        )

    assert (tracked, action) == (True, "updated")
    with Session(engine) as session:
        child = session.get(DiscordMessage, child_id)
        transaction = session.get(Transaction, transaction_id)
        associations = session.exec(
            select(TransactionSourceRevision).where(
                TransactionSourceRevision.transaction_id == transaction_id
            )
        ).all()
        assert child.parse_status == PARSE_PENDING
        assert transaction.is_deleted is True
        assert transaction.parse_status == PARSE_PENDING
        assert {row.message_id for row in associations} == {
            transaction.source_message_id,
            child_id,
        }
    engine.dispose()


def test_child_delete_invalidates_primary_and_legacy_stitched_fallback() -> None:
    engine = _engine()
    with Session(engine) as session:
        primary_id, child_id, transaction_id = _seed_stitched_transaction(session)
        child = session.get(DiscordMessage, child_id)
        assert mark_message_deleted_row(session, child)
        transaction = session.get(Transaction, transaction_id)
        assert transaction.is_deleted is True

        # A transaction created before the association migration still follows
        # the persisted stitch group back to its primary source.
        legacy_primary = _message("2001", "Legacy photo", created_second=3)
        legacy_child = _message("2002", "Buy 20", created_second=4)
        session.add(legacy_primary)
        session.add(legacy_child)
        session.flush()
        legacy_primary.stitched_group_id = "legacy-group"
        legacy_primary.stitched_primary = True
        legacy_child.stitched_group_id = "legacy-group"
        legacy_child.stitched_primary = False
        legacy_transaction = Transaction(
            source_message_id=legacy_primary.id,
            occurred_at=legacy_primary.created_at,
            parse_status=PARSE_PARSED,
            amount=20,
            money_out=20,
        )
        session.add(legacy_transaction)
        session.commit()

        legacy_child = session.get(DiscordMessage, legacy_child.id)
        assert mark_message_deleted_row(session, legacy_child)
        legacy_transaction = session.get(Transaction, legacy_transaction.id)
        assert legacy_transaction.is_deleted is True
    engine.dispose()


def test_oversized_legacy_group_delete_quarantines_source_and_hides_live_money() -> None:
    engine = _engine()
    with Session(engine) as session:
        source_rows = [
            _message(
                f"oversized-legacy-delete-{index}",
                "Buy 50 cash" if index == 0 else f"fragment {index}",
                created_second=index,
            )
            for index in range(MAX_TRANSACTION_SOURCE_ROWS + 1)
        ]
        for source_row in source_rows[1:]:
            source_row.parse_status = PARSE_IGNORED
        session.add_all(source_rows)
        session.flush()
        membership = json.dumps([source_row.id for source_row in source_rows])
        for index, source_row in enumerate(source_rows):
            source_row.stitched_group_id = "oversized-legacy-delete"
            source_row.stitched_primary = index == 0
            source_row.stitched_message_ids_json = membership
            session.add(source_row)
        primary = source_rows[0]
        deleted_child = source_rows[-1]
        transaction = Transaction(
            source_message_id=primary.id,
            occurred_at=primary.created_at,
            parse_status=PARSE_PARSED,
            entry_kind="buy",
            payment_method="cash",
            amount=50,
            money_out=50,
            source_content="oversized legacy source evidence",
        )
        session.add(transaction)
        session.commit()
        transaction_id = transaction.id
        deleted_child_id = deleted_child.id

        assert transaction_id in {
            row.id for row in session.exec(transaction_base_query()).all()
        }
        assert mark_message_deleted_row(session, deleted_child)

        session.expire_all()
        deleted_child = session.get(DiscordMessage, deleted_child_id)
        transaction = session.get(Transaction, transaction_id)
        assert deleted_child.is_deleted is True
        assert deleted_child.source_refresh_required is True
        assert DISCORD_SOURCE_REFRESH_REQUIRED_ERROR in (deleted_child.last_error or "")
        assert transaction.is_deleted is False
        assert transaction.source_content == "oversized legacy source evidence"
        assert transaction_id not in {
            row.id for row in session.exec(transaction_base_query()).all()
        }
    engine.dispose()


def test_legacy_report_query_excludes_deleted_group_member_without_quarantine() -> None:
    engine = _engine()
    with Session(engine) as session:
        primary = _message("legacy-deleted-primary", "Buy 50 cash", created_second=1)
        child = _message("legacy-deleted-child", "fragment", created_second=2)
        child.parse_status = PARSE_IGNORED
        child.is_deleted = True
        child.deleted_at = datetime(2026, 6, 28, 12, 1, tzinfo=timezone.utc)
        session.add(primary)
        session.add(child)
        session.flush()
        membership = json.dumps([primary.id, child.id])
        primary.stitched_group_id = "legacy-deleted-member"
        primary.stitched_primary = True
        primary.stitched_message_ids_json = membership
        child.stitched_group_id = "legacy-deleted-member"
        child.stitched_primary = False
        child.stitched_message_ids_json = membership
        transaction = Transaction(
            source_message_id=primary.id,
            occurred_at=primary.created_at,
            parse_status=PARSE_PARSED,
            amount=50,
            money_out=50,
            source_content="legacy source with deleted member",
        )
        session.add(transaction)
        session.commit()

        assert transaction.id not in {
            row.id for row in session.exec(transaction_base_query()).all()
        }
    engine.dispose()


def test_legacy_fallback_does_not_tombstone_association_backed_nonmember_transaction() -> None:
    engine = _engine()
    with Session(engine) as session:
        primary_id, child_id, transaction_id = _seed_stitched_transaction(session)
        primary = session.get(DiscordMessage, primary_id)
        rogue = _message("same-group-nonmember", "rogue fragment", created_second=3)
        rogue.parse_status = PARSE_IGNORED
        rogue.stitched_group_id = primary.stitched_group_id
        rogue.stitched_primary = False
        rogue.source_refresh_required = True
        session.add(rogue)
        session.commit()

        assert invalidate_transactions_for_message(
            session,
            rogue,
            reason="association-backed nonmember regression",
        ) == []
        session.commit()

        transaction = session.get(Transaction, transaction_id)
        remaining_associations = session.exec(
            select(TransactionSourceRevision).where(
                TransactionSourceRevision.transaction_id == transaction_id
            )
        ).all()
        assert transaction.is_deleted is False
        assert [(association.message_id, association.source_position) for association in remaining_associations] == [
            (primary_id, 0),
            (child_id, 1),
        ]
        assert transaction_id in {
            row.id for row in session.exec(transaction_base_query()).all()
        }
    engine.dispose()


@pytest.mark.parametrize(
    "corruption",
    ["oversized_members", "ambiguous_primaries"],
)
def test_legacy_stitched_invalidation_fails_closed_on_corrupt_fanout(
    corruption: str,
) -> None:
    engine = _engine()
    with Session(engine) as session:
        primary = _message("legacy-primary", "Legacy photo", created_second=5)
        child = _message("legacy-child", "Buy 20", created_second=5)
        primary.stitched_group_id = "legacy-corrupt-group"
        primary.stitched_primary = True
        child.stitched_group_id = "legacy-corrupt-group"
        child.stitched_primary = False
        session.add(primary)
        session.add(child)
        session.flush()

        if corruption == "oversized_members":
            for index in range(MAX_TRANSACTION_SOURCE_ROWS - 1):
                extra = _message(
                    f"legacy-extra-{index}",
                    "fragment",
                    created_second=5,
                )
                extra.stitched_group_id = "legacy-corrupt-group"
                extra.stitched_primary = False
                session.add(extra)
        elif corruption == "ambiguous_primaries":
            second_primary = _message(
                "legacy-second-primary",
                "Second primary",
                created_second=5,
            )
            second_primary.stitched_group_id = "legacy-corrupt-group"
            second_primary.stitched_primary = True
            session.add(second_primary)

        transactions = []
        transaction = Transaction(
            source_message_id=primary.id,
            occurred_at=primary.created_at,
            parse_status=PARSE_PARSED,
            amount=20,
            money_out=20,
        )
        session.add(transaction)
        transactions.append(transaction)
        session.commit()

        with pytest.raises(StaleSourceRevisionError, match="legacy stitched"):
            invalidate_transactions_for_message(
                session,
                child,
                reason="corrupt legacy fallback regression",
            )
        session.rollback()
        assert all(
            session.get(Transaction, transaction.id).is_deleted is False
            for transaction in transactions
        )
    engine.dispose()


def test_invalidation_fails_closed_when_one_message_has_excessive_association_fanout() -> None:
    engine = _engine()
    with Session(engine) as session:
        message = _message("association-fanout", "shared child", created_second=5)
        session.add(message)
        child_revision = ensure_message_revision(session, message)
        transactions = []
        for index in range(MAX_TRANSACTION_SOURCE_ROWS + 1):
            primary = _message(
                f"association-primary-{index}",
                "Buy 20",
                created_second=5,
            )
            session.add(primary)
            primary_revision = ensure_message_revision(session, primary)
            transaction = Transaction(
                source_message_id=primary.id,
                source_revision_id=primary_revision.id,
                occurred_at=primary.created_at,
                parse_status=PARSE_PARSED,
                amount=20,
                money_out=20,
            )
            session.add(transaction)
            session.flush()
            session.add(
                TransactionSourceRevision(
                    transaction_id=transaction.id,
                    message_id=primary.id,
                    revision_id=primary_revision.id,
                    source_position=0,
                )
            )
            session.add(
                TransactionSourceRevision(
                    transaction_id=transaction.id,
                    message_id=message.id,
                    revision_id=child_revision.id,
                    source_position=1,
                )
            )
            transactions.append(transaction)
        session.commit()

        with pytest.raises(StaleSourceRevisionError, match="association fanout"):
            invalidate_transactions_for_message(
                session,
                message,
                reason="association fanout regression",
            )
        session.rollback()
        assert all(
            session.get(Transaction, transaction.id).is_deleted is False
            for transaction in transactions
        )
    engine.dispose()


def test_report_query_excludes_any_association_current_revision_mismatch() -> None:
    engine = _engine()
    with Session(engine) as session:
        _primary_id, child_id, transaction_id = _seed_stitched_transaction(session)
        assert {row.id for row in session.exec(transaction_base_query()).all()} == {
            transaction_id
        }

        child = session.get(DiscordMessage, child_id)
        child.content = "Buy 75 cash"
        ensure_message_revision(session, child)
        session.commit()

        assert transaction_id not in {
            row.id for row in session.exec(transaction_base_query()).all()
        }
    engine.dispose()


def test_report_query_excludes_nonterminal_primary_on_same_revision_but_allows_ignored_child() -> None:
    engine = _engine()
    with Session(engine) as session:
        primary_id, child_id, transaction_id = _seed_stitched_transaction(session)
        child = session.get(DiscordMessage, child_id)
        child.parse_status = "ignored"
        session.add(child)
        session.commit()
        assert transaction_id in {
            row.id for row in session.exec(transaction_base_query()).all()
        }

        primary = session.get(DiscordMessage, primary_id)
        primary.parse_status = PARSE_PENDING
        session.add(primary)
        session.commit()

        assert transaction_id not in {
            row.id for row in session.exec(transaction_base_query()).all()
        }
    engine.dispose()


@pytest.mark.parametrize("corruption", ["revision_pointer", "content", "attachments"])
def test_report_query_excludes_direct_source_revision_or_snapshot_mismatch(
    corruption: str,
) -> None:
    engine = _engine()
    with Session(engine) as session:
        primary_id, _child_id, transaction_id = _seed_stitched_transaction(session)
        primary = session.get(DiscordMessage, primary_id)
        transaction = session.get(Transaction, transaction_id)
        original_revision_id = primary.current_revision_id

        if corruption == "revision_pointer":
            primary.content = "Temporary second revision"
            second_revision = ensure_message_revision(session, primary)
            session.flush()
            primary.content = "Photo"
            primary.current_revision_id = original_revision_id
            transaction.source_revision_id = second_revision.id
            session.add(transaction)
        elif corruption == "content":
            primary.content = "Projection changed without revision"
        else:
            primary.attachment_urls_json = '["https://cdn.example/mismatch.jpg"]'
        session.add(primary)
        session.commit()

        assert transaction_id not in {
            row.id for row in session.exec(transaction_base_query()).all()
        }
    engine.dispose()


def test_report_query_excludes_missing_direct_source_revision_when_id_is_present() -> None:
    engine = _engine(enforce_foreign_keys=False)
    with Session(engine) as session:
        _primary_id, _child_id, transaction_id = _seed_stitched_transaction(session)
        transaction = session.get(Transaction, transaction_id)
        transaction.source_revision_id = 999_999
        session.add(transaction)
        session.commit()

        assert transaction_id not in {
            row.id for row in session.exec(transaction_base_query()).all()
        }
    engine.dispose()


@pytest.mark.parametrize("quarantined_source", ["primary", "bound_child"])
def test_report_query_excludes_quarantined_primary_or_bound_source(
    quarantined_source: str,
) -> None:
    engine = _engine()
    with Session(engine) as session:
        primary_id, child_id, transaction_id = _seed_stitched_transaction(session)
        primary = session.get(DiscordMessage, primary_id)
        child = session.get(DiscordMessage, child_id)
        if quarantined_source == "primary":
            primary.source_refresh_required = True
            session.add(primary)
        elif quarantined_source == "bound_child":
            child.last_error = DISCORD_SOURCE_REFRESH_REQUIRED_ERROR
            session.add(child)
        session.commit()

        assert transaction_id not in {
            row.id for row in session.exec(transaction_base_query()).all()
        }
    engine.dispose()


def test_report_query_excludes_quarantined_legacy_group_constituent() -> None:
    engine = _engine()
    with Session(engine) as session:
        primary = _message("legacy-report-primary", "Photo", created_second=1)
        child = _message("legacy-report-child", "Buy 50 cash", created_second=2)
        child.parse_status = PARSE_IGNORED
        child.source_refresh_required = True
        session.add(primary)
        session.add(child)
        session.flush()
        membership = json.dumps([primary.id, child.id])
        primary.stitched_group_id = "legacy-report-group"
        primary.stitched_primary = True
        primary.stitched_message_ids_json = membership
        child.stitched_group_id = "legacy-report-group"
        child.stitched_primary = False
        child.stitched_message_ids_json = membership
        primary_revision = ensure_message_revision(session, primary)
        ensure_message_revision(session, child)
        transaction = Transaction(
            source_message_id=primary.id,
            source_revision_id=primary_revision.id,
            occurred_at=primary.created_at,
            parse_status=PARSE_PARSED,
            amount=50,
            money_out=50,
            source_content="legacy grouped evidence",
        )
        session.add(transaction)
        session.commit()

        assert transaction.id not in {
            row.id for row in session.exec(transaction_base_query()).all()
        }
    engine.dispose()


@pytest.mark.parametrize("corruption", ["status", "projection", "missing_revision"])
def test_report_query_excludes_invalid_bound_revision_snapshot_or_status(
    corruption: str,
) -> None:
    engine = _engine(enforce_foreign_keys=corruption != "missing_revision")
    with Session(engine) as session:
        _primary_id, child_id, transaction_id = _seed_stitched_transaction(session)
        child = session.get(DiscordMessage, child_id)
        child_association = session.exec(
            select(TransactionSourceRevision)
            .where(TransactionSourceRevision.transaction_id == transaction_id)
            .where(TransactionSourceRevision.message_id == child_id)
        ).one()
        if corruption == "status":
            child.parse_status = PARSE_PENDING
        elif corruption == "projection":
            child.content = "Bound child changed without revision"
        else:
            child.current_revision_id = 999_998
            child_association.revision_id = 999_998
            session.add(child_association)
        session.add(child)
        session.commit()

        assert transaction_id not in {
            row.id for row in session.exec(transaction_base_query()).all()
        }
    engine.dispose()


def test_report_query_excludes_gapped_source_association_positions() -> None:
    engine = _engine()
    with Session(engine) as session:
        _primary_id, child_id, transaction_id = _seed_stitched_transaction(session)
        child_association = session.exec(
            select(TransactionSourceRevision)
            .where(TransactionSourceRevision.transaction_id == transaction_id)
            .where(TransactionSourceRevision.message_id == child_id)
        ).one()
        child_association.source_position = 2
        session.add(child_association)
        session.commit()

        assert transaction_id not in {
            row.id for row in session.exec(transaction_base_query()).all()
        }
    engine.dispose()


def test_implicit_sync_refuses_child_revision_mismatch_and_preserves_tombstone_bindings() -> None:
    engine = _engine()

    @contextmanager
    def managed_session():
        with Session(engine) as session:
            yield session

    with Session(engine) as session:
        primary_id, child_id, transaction_id = _seed_stitched_transaction(session)
        child = session.get(DiscordMessage, child_id)
        edited_message = _fake_discord_message(child, "Buy 75 cash")

    with patch("app.discord.discord_ingest.managed_session", managed_session), patch(
        "app.discord.discord_ingest.sync_attachment_assets",
    ):
        assert insert_or_update_message(
            edited_message,
            is_edit=True,
            watched_channel_ids=set(),
        ) == (True, "updated")

    with Session(engine) as session:
        primary = session.get(DiscordMessage, primary_id)
        with pytest.raises(StaleSourceRevisionError, match="source revision"):
            sync_transaction_from_message(session, primary)
        session.rollback()

    with Session(engine) as session:
        transaction = session.get(Transaction, transaction_id)
        associations = session.exec(
            select(TransactionSourceRevision)
            .where(TransactionSourceRevision.transaction_id == transaction_id)
            .order_by(TransactionSourceRevision.source_position)
        ).all()
        assert transaction.is_deleted is True
        assert transaction.source_content == "Message 1: Photo\nMessage 2: Buy 50 cash"
        assert [(row.message_id, row.source_position) for row in associations] == [
            (primary_id, 0),
            (child_id, 1),
        ]
    engine.dispose()


def test_rebuild_skips_stale_stitched_transaction_without_collapsing_provenance() -> None:
    engine = _engine()
    with Session(engine) as session:
        primary_id, child_id, transaction_id = _seed_stitched_transaction(session)
        child = session.get(DiscordMessage, child_id)
        child.content = "Buy 75 cash"
        child.parse_status = PARSE_PENDING
        ensure_message_revision(session, child)
        transaction = session.get(Transaction, transaction_id)
        transaction.is_deleted = True
        session.add(transaction)
        session.commit()

        assert rebuild_transactions(session) == 0

        transaction = session.get(Transaction, transaction_id)
        associations = session.exec(
            select(TransactionSourceRevision).where(
                TransactionSourceRevision.transaction_id == transaction_id
            )
        ).all()
        assert transaction.is_deleted is True
        assert {row.message_id for row in associations} == {primary_id, child_id}
    engine.dispose()


@pytest.mark.parametrize("stale_cause", ["projection", "quarantine"])
def test_rebuild_tombstones_stale_source_and_preserves_other_row_savepoints(
    stale_cause: str,
) -> None:
    engine = _engine()
    with Session(engine) as session:
        primary = _message("rebuild-stale", "Buy 50 cash", created_second=1)
        session.add(primary)
        session.flush()
        transaction = sync_transaction_from_message(session, primary)
        session.commit()
        primary_id = primary.id
        transaction_id = transaction.id
        source_content = transaction.source_content
        association_tuples = [
            (association.id, association.message_id, association.revision_id, association.source_position)
            for association in session.exec(
                select(TransactionSourceRevision)
                .where(TransactionSourceRevision.transaction_id == transaction_id)
                .order_by(TransactionSourceRevision.source_position)
            ).all()
        ]
        primary = session.get(DiscordMessage, primary_id)
        if stale_cause == "projection":
            primary.content = "Projection mismatch without revision"
        else:
            primary.source_refresh_required = True
        earlier = _message("rebuild-earlier", "Buy 5 cash", created_second=0)
        earlier.amount = 5
        earlier.money_out = 5
        later = _message("rebuild-later", "Buy 10 cash", created_second=4)
        later.amount = 10
        later.money_out = 10
        session.add(primary)
        session.add(earlier)
        session.add(later)
        session.commit()
        earlier_id = earlier.id
        later_id = later.id

        assert rebuild_transactions(session) == 2

        transaction = session.get(Transaction, transaction_id)
        failed_source = session.get(DiscordMessage, primary_id)
        earlier_transaction = session.exec(
            select(Transaction).where(Transaction.source_message_id == earlier_id)
        ).one()
        later_transaction = session.exec(
            select(Transaction).where(Transaction.source_message_id == later_id)
        ).one()
        assert transaction.is_deleted is True
        assert failed_source.source_refresh_required is True
        assert DISCORD_SOURCE_REFRESH_REQUIRED_ERROR in (failed_source.last_error or "")
        assert transaction.source_content == source_content
        assert [
            (association.id, association.message_id, association.revision_id, association.source_position)
            for association in session.exec(
                select(TransactionSourceRevision)
                .where(TransactionSourceRevision.transaction_id == transaction_id)
                .order_by(TransactionSourceRevision.source_position)
            ).all()
        ] == association_tuples
        assert earlier_transaction.is_deleted is False
        assert later_transaction.is_deleted is False
    engine.dispose()


def test_approve_route_returns_409_for_stale_stitched_source_without_reviving_money() -> None:
    from app.routers.messages import approve_message

    engine = _engine()
    with Session(engine) as session:
        primary_id, child_id, transaction_id = _seed_stitched_transaction(session)
        child = session.get(DiscordMessage, child_id)
        child.content = "Buy 75 cash"
        child.parse_status = PARSE_PENDING
        ensure_message_revision(session, child)
        transaction = session.get(Transaction, transaction_id)
        transaction.is_deleted = True
        session.add(transaction)
        session.commit()

        with patch("app.routers.messages.require_role_response", return_value=None):
            with pytest.raises(HTTPException) as exc_info:
                approve_message(request=object(), message_id=primary_id, session=session)
        assert exc_info.value.status_code == 409
        session.rollback()

    with Session(engine) as session:
        transaction = session.get(Transaction, transaction_id)
        associations = session.exec(
            select(TransactionSourceRevision).where(
                TransactionSourceRevision.transaction_id == transaction_id
            )
        ).all()
        assert transaction.is_deleted is True
        assert {row.message_id for row in associations} == {primary_id, child_id}
    engine.dispose()


def test_retry_stale_stitched_source_queues_reparse_without_collapsing_provenance() -> None:
    from app.routers.messages import retry_message

    engine = _engine()
    with Session(engine) as session:
        primary_id, child_id, transaction_id = _seed_stitched_transaction(session)
        child = session.get(DiscordMessage, child_id)
        child.content = "Buy 75 cash"
        child.parse_status = PARSE_PENDING
        ensure_message_revision(session, child)
        session.commit()

        with patch("app.routers.messages.require_role_response", return_value=None):
            result = retry_message(request=object(), message_id=primary_id, session=session)
        assert result["ok"] is True

    with Session(engine) as session:
        primary = session.get(DiscordMessage, primary_id)
        transaction = session.get(Transaction, transaction_id)
        associations = session.exec(
            select(TransactionSourceRevision)
            .where(TransactionSourceRevision.transaction_id == transaction_id)
            .order_by(TransactionSourceRevision.source_position)
        ).all()
        assert primary.parse_status == PARSE_PENDING
        assert transaction.is_deleted is True
        assert [(row.message_id, row.source_position) for row in associations] == [
            (primary_id, 0),
            (child_id, 1),
        ]
    engine.dispose()


def test_reparse_tombstones_transaction_and_preserves_existing_source_evidence() -> None:
    engine = _engine()
    with Session(engine) as session:
        primary_id, child_id, transaction_id = _seed_stitched_transaction(session)
        primary = session.get(DiscordMessage, primary_id)

        assert reparse_message_rows(
            session,
            [primary],
            reason="manual regression reparse",
            reset_attempts=True,
        ) == 1

        transaction = session.get(Transaction, transaction_id)
        associations = session.exec(
            select(TransactionSourceRevision)
            .where(TransactionSourceRevision.transaction_id == transaction_id)
            .order_by(TransactionSourceRevision.source_position)
        ).all()
        assert transaction is not None
        assert transaction.is_deleted is True
        assert transaction.source_content == "Message 1: Photo\nMessage 2: Buy 50 cash"
        assert [(row.message_id, row.source_position) for row in associations] == [
            (primary_id, 0),
            (child_id, 1),
        ]
    engine.dispose()


def test_reparse_stitched_child_tombstones_bound_primary_without_collapsing_provenance() -> None:
    engine = _engine()
    with Session(engine) as session:
        primary_id, child_id, transaction_id = _seed_stitched_transaction(session)
        child = session.get(DiscordMessage, child_id)

        assert reparse_message_rows(
            session,
            [child],
            reason="manual child reparse",
            reset_attempts=True,
        ) == 1

        transaction = session.get(Transaction, transaction_id)
        associations = session.exec(
            select(TransactionSourceRevision)
            .where(TransactionSourceRevision.transaction_id == transaction_id)
            .order_by(TransactionSourceRevision.source_position)
        ).all()
        assert transaction.is_deleted is True
        assert transaction.parse_status == PARSE_PENDING
        assert transaction.source_content == "Message 1: Photo\nMessage 2: Buy 50 cash"
        assert [(row.message_id, row.source_position) for row in associations] == [
            (primary_id, 0),
            (child_id, 1),
        ]
    engine.dispose()


def test_valid_recompute_preserves_exact_existing_association_rows() -> None:
    from app.shared import recompute_financial_fields

    engine = _engine()
    with Session(engine) as session:
        primary_id, _child_id, transaction_id = _seed_stitched_transaction(session)
        before = session.exec(
            select(TransactionSourceRevision)
            .where(TransactionSourceRevision.transaction_id == transaction_id)
            .order_by(TransactionSourceRevision.source_position)
        ).all()
        before_tuples = [
            (association.id, association.message_id, association.revision_id, association.source_position)
            for association in before
        ]

        sentinel = _message("sentinel", "Buy 1 cash", created_second=5)
        session.add(sentinel)
        session.flush()
        ensure_message_revision(session, sentinel)
        sentinel_transaction = sync_transaction_from_message(session, sentinel)
        sentinel_association = session.exec(
            select(TransactionSourceRevision).where(
                TransactionSourceRevision.transaction_id == sentinel_transaction.id
            )
        ).one()
        sentinel_association.id = 100
        session.add(sentinel_association)
        session.commit()

        assert recompute_financial_fields(session) == 2

        after = session.exec(
            select(TransactionSourceRevision)
            .where(TransactionSourceRevision.transaction_id == transaction_id)
            .order_by(TransactionSourceRevision.source_position)
        ).all()
        after_tuples = [
            (association.id, association.message_id, association.revision_id, association.source_position)
            for association in after
        ]
        assert after_tuples == before_tuples
    engine.dispose()


def test_implicit_sync_rejects_bound_primary_association_mismatch() -> None:
    engine = _engine()
    with Session(engine) as session:
        primary_id, _child_id, transaction_id = _seed_stitched_transaction(session)
        transaction = session.get(Transaction, transaction_id)
        associations = session.exec(
            select(TransactionSourceRevision)
            .where(TransactionSourceRevision.transaction_id == transaction_id)
            .order_by(TransactionSourceRevision.source_position)
        ).all()
        primary_association, child_association = associations

        primary_association.source_position = 2
        session.add(primary_association)
        session.flush()
        child_association.source_position = 0
        session.add(child_association)
        session.flush()
        primary_association.source_position = 1
        session.add(primary_association)
        session.commit()

        primary = session.get(DiscordMessage, primary_id)
        with pytest.raises(StaleSourceRevisionError, match="bound transaction primary"):
            sync_transaction_from_message(session, primary)
    engine.dispose()


def test_recompute_skips_stale_stitched_tombstone_without_changing_evidence() -> None:
    from app.shared import recompute_financial_fields

    engine = _engine()
    with Session(engine) as session:
        primary_id, child_id, transaction_id = _seed_stitched_transaction(session)
        child = session.get(DiscordMessage, child_id)
        child.content = "Buy 75 cash"
        child.parse_status = PARSE_PENDING
        ensure_message_revision(session, child)
        transaction = session.get(Transaction, transaction_id)
        transaction.is_deleted = True
        session.add(transaction)
        session.commit()

        before = session.exec(
            select(TransactionSourceRevision)
            .where(TransactionSourceRevision.transaction_id == transaction_id)
            .order_by(TransactionSourceRevision.source_position)
        ).all()
        before_tuples = [
            (association.id, association.message_id, association.revision_id, association.source_position)
            for association in before
        ]
        before_content = transaction.source_content

        assert recompute_financial_fields(session) == 0

        transaction = session.get(Transaction, transaction_id)
        after = session.exec(
            select(TransactionSourceRevision)
            .where(TransactionSourceRevision.transaction_id == transaction_id)
            .order_by(TransactionSourceRevision.source_position)
        ).all()
        assert transaction.is_deleted is True
        assert transaction.source_content == before_content
        assert [
            (association.id, association.message_id, association.revision_id, association.source_position)
            for association in after
        ] == before_tuples
    engine.dispose()


def test_rebuild_creates_complete_stitched_provenance_without_existing_bindings() -> None:
    engine = _engine()
    with Session(engine) as session:
        primary = _message("3001", "Photo", created_second=6)
        child = _message("3002", "Buy 50 cash", created_second=6)
        child.parse_status = PARSE_IGNORED
        session.add(primary)
        session.add(child)
        session.flush()
        ordered_ids = [primary.id, child.id]
        membership = json.dumps(ordered_ids)
        primary.stitched_group_id = "group-without-bindings"
        primary.stitched_primary = True
        primary.stitched_message_ids_json = membership
        child.stitched_group_id = "group-without-bindings"
        child.stitched_primary = False
        child.stitched_message_ids_json = membership
        ensure_message_revision(session, primary)
        ensure_message_revision(session, child)
        session.commit()

        assert rebuild_transactions(session) == 1

        transaction = session.exec(
            select(Transaction).where(Transaction.source_message_id == primary.id)
        ).one()
        associations = session.exec(
            select(TransactionSourceRevision)
            .where(TransactionSourceRevision.transaction_id == transaction.id)
            .order_by(TransactionSourceRevision.source_position)
        ).all()
        assert [(association.message_id, association.source_position) for association in associations] == [
            (primary.id, 0),
            (child.id, 1),
        ]
        assert transaction.source_content == "Message 1: Photo\n\nMessage 2: Buy 50 cash"
    engine.dispose()


@pytest.mark.parametrize(
    ("membership", "error_match"),
    [
        ("not-json", "malformed"),
        (json.dumps(list(range(1, 34))), "oversized"),
    ],
)
def test_implicit_sync_fails_closed_for_corrupt_or_oversized_stitch_metadata(
    membership: str,
    error_match: str,
) -> None:
    engine = _engine()
    with Session(engine) as session:
        primary = _message("4001", "Buy 50 cash", created_second=7)
        primary.stitched_group_id = "corrupt-group"
        primary.stitched_primary = True
        primary.stitched_message_ids_json = membership
        session.add(primary)
        ensure_message_revision(session, primary)
        session.commit()

        with pytest.raises(StaleSourceRevisionError, match=error_match):
            sync_transaction_from_message(session, primary)
        session.rollback()
        assert session.exec(
            select(Transaction).where(Transaction.source_message_id == primary.id)
        ).first() is None
    engine.dispose()
