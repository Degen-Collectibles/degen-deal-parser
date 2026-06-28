from __future__ import annotations

import asyncio
import json
from contextlib import contextmanager
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from sqlalchemy import event
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine, select

from app.discord import worker as worker_module
from app.discord.discord_ingest import (
    block_message_after_raw_edit_fetch_failure,
    insert_or_update_message,
    revoke_parse_claim_for_source_change,
)
from app.discord.message_revisions import ensure_message_revision
from app.discord.transactions import (
    invalidate_transactions_for_message,
    sync_transaction_from_message,
    transaction_base_query,
)
from app.discord.worker import claim_message_for_parse, process_row
from app.models import (
    AuditLog,
    DiscordMessage,
    DiscordMessageRevision,
    OperationsLog,
    PARSE_FAILED,
    PARSE_IGNORED,
    PARSE_PARSED,
    PARSE_PENDING,
    PARSE_PROCESSING,
    ParseAttempt,
    ReviewCorrection,
    Transaction,
    TransactionSourceRevision,
    utcnow,
)


def _engine():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def _foreign_keys(connection, _record):
        connection.execute("PRAGMA foreign_keys=ON")

    SQLModel.metadata.create_all(engine)
    return engine


def _parse_result(*, ignore: bool = False, amount: float = 50.0) -> dict:
    return {
        "parsed_type": "buy",
        "parsed_amount": amount,
        "parsed_payment_method": "cash",
        "parsed_cash_direction": None,
        "parsed_category": "inventory",
        "parsed_items": ["Card"],
        "parsed_items_in": ["Card"],
        "parsed_items_out": [],
        "parsed_trade_summary": None,
        "parsed_notes": None,
        "image_summary": None,
        "confidence": 0.95,
        "needs_review": False,
        "ignore_message": ignore,
    }


class GroupFixture:
    def __init__(self):
        self.engine = _engine()
        with Session(self.engine) as session:
            primary = DiscordMessage(
                discord_message_id="group-primary",
                channel_id="chan",
                channel_name="deals",
                author_name="seller",
                content="Photo",
                attachment_urls_json='["https://cdn.example/card.jpg"]',
                created_at=utcnow(),
                parse_status=PARSE_PARSED,
                deal_type="buy",
                entry_kind="buy",
                amount=50,
                money_out=50,
            )
            child = DiscordMessage(
                discord_message_id="group-child",
                channel_id="chan",
                channel_name="deals",
                author_name="seller",
                content="Buy 50 cash",
                attachment_urls_json="[]",
                created_at=utcnow() + timedelta(seconds=1),
                parse_status=PARSE_IGNORED,
            )
            session.add(primary)
            session.add(child)
            session.flush()
            primary.stitched_group_id = "group"
            primary.stitched_primary = True
            child.stitched_group_id = "group"
            child.stitched_primary = False
            primary.stitched_message_ids_json = f"[{primary.id}, {child.id}]"
            child.stitched_message_ids_json = primary.stitched_message_ids_json
            ensure_message_revision(session, primary)
            ensure_message_revision(session, child)
            transaction = sync_transaction_from_message(
                session,
                primary,
                source_rows=[primary, child],
                source_content="Message 1: Photo\nMessage 2: Buy 50 cash",
            )
            primary.parse_status = PARSE_PENDING
            session.add(primary)
            session.commit()
            self.primary_id = primary.id
            self.child_id = child.id
            self.transaction_id = transaction.id

    @contextmanager
    def managed_session(self):
        with Session(self.engine) as session:
            yield session

    def stitch_group(self, *, session, **_kwargs):
        return [
            session.get(DiscordMessage, self.primary_id),
            session.get(DiscordMessage, self.child_id),
        ]

    def claim(self) -> int:
        with Session(self.engine) as session:
            attempt = claim_message_for_parse(session, self.primary_id)
            assert attempt is not None
            attempt_id = attempt.id
            session.commit()
            return attempt_id

    def edit_child_and_revoke(self, *, content: str = "Buy 75 cash") -> None:
        with Session(self.engine) as session:
            child = session.get(DiscordMessage, self.child_id)
            revoke_parse_claim_for_source_change(
                session,
                child,
                reason="child edited during parse",
            )
            child.content = content
            child.parse_status = PARSE_PENDING
            ensure_message_revision(session, child)
            invalidate_transactions_for_message(
                session,
                child,
                reason="child edited during parse",
            )
            session.add(child)
            session.commit()


def test_overlapping_primary_and_child_claims_have_one_group_owner() -> None:
    fixture = GroupFixture()
    with patch("app.discord.worker.build_stitch_group", side_effect=fixture.stitch_group):
        first_attempt_id = fixture.claim()
        with Session(fixture.engine) as session:
            competing = claim_message_for_parse(session, fixture.child_id)
            session.commit()
            assert competing is None
            rows = session.exec(
                select(DiscordMessage).where(
                    DiscordMessage.id.in_([fixture.primary_id, fixture.child_id])
                )
            ).all()
            assert {row.active_parse_attempt_id for row in rows} == {first_attempt_id}
            assert session.get(DiscordMessage, fixture.primary_id).parse_status == PARSE_PROCESSING
    fixture.engine.dispose()


def test_claim_cas_rejects_manual_edit_to_stale_ignored_constituent() -> None:
    fixture = GroupFixture()
    original_parse_claim_rows = worker_module._parse_claim_rows
    manual_reviewed_at = utcnow()

    def edit_child_after_worker_preread(session, row):
        stale_rows = original_parse_claim_rows(session, row)
        with Session(fixture.engine) as edit_session:
            child = edit_session.get(DiscordMessage, fixture.child_id)
            child.amount = 75
            child.notes = "manual child edit won"
            child.money_in = 75
            child.money_out = 0
            child.needs_review = True
            child.reviewed_by = "manual-reviewer"
            child.reviewed_at = manual_reviewed_at
            edit_session.add(child)
            sync_transaction_from_message(edit_session, child)
            edit_session.commit()
        return stale_rows

    with patch("app.discord.worker.build_stitch_group", side_effect=fixture.stitch_group), patch(
        "app.discord.worker._parse_claim_rows",
        side_effect=edit_child_after_worker_preread,
    ):
        with Session(fixture.engine) as session:
            attempt = claim_message_for_parse(session, fixture.primary_id)
            session.commit()
            assert attempt is None

    with Session(fixture.engine) as session:
        primary = session.get(DiscordMessage, fixture.primary_id)
        child = session.get(DiscordMessage, fixture.child_id)
        transaction = session.get(Transaction, fixture.transaction_id)
        attempts = session.exec(select(ParseAttempt)).all()

        assert attempts == []
        assert primary.active_parse_attempt_id is None
        assert child.active_parse_attempt_id is None
        assert primary.parse_status == PARSE_PENDING
        assert child.parse_status == PARSE_IGNORED
        assert child.amount == 75
        assert child.notes == "manual child edit won"
        assert child.money_in == 75
        assert child.money_out == 0
        assert child.needs_review is True
        assert child.reviewed_by == "manual-reviewer"
        assert child.reviewed_at.replace(
            tzinfo=manual_reviewed_at.tzinfo
        ) == manual_reviewed_at
        assert transaction.is_deleted is True
        assert transaction.amount == 50
        assert transaction.source_content.endswith("Buy 50 cash")

    fixture.engine.dispose()


def test_claim_cas_rejects_same_status_manual_edit_to_pending_primary() -> None:
    fixture = GroupFixture()
    original_parse_claim_rows = worker_module._parse_claim_rows
    manual_reviewed_at = utcnow()

    def edit_primary_after_worker_preread(session, row):
        stale_rows = original_parse_claim_rows(session, row)
        with Session(fixture.engine) as edit_session:
            primary = edit_session.get(DiscordMessage, fixture.primary_id)
            primary.amount = 88
            primary.notes = "manual pending edit won"
            primary.money_in = 0
            primary.money_out = 88
            primary.reviewed_by = "manual-reviewer"
            primary.reviewed_at = manual_reviewed_at
            edit_session.add(primary)
            sync_transaction_from_message(edit_session, primary)
            edit_session.commit()
        return stale_rows

    with patch("app.discord.worker.build_stitch_group", side_effect=fixture.stitch_group), patch(
        "app.discord.worker._parse_claim_rows",
        side_effect=edit_primary_after_worker_preread,
    ):
        with Session(fixture.engine) as session:
            attempt = claim_message_for_parse(session, fixture.primary_id)
            session.commit()
            assert attempt is None

    with Session(fixture.engine) as session:
        primary = session.get(DiscordMessage, fixture.primary_id)
        child = session.get(DiscordMessage, fixture.child_id)
        transaction = session.get(Transaction, fixture.transaction_id)
        attempts = session.exec(select(ParseAttempt)).all()

        assert attempts == []
        assert primary.active_parse_attempt_id is None
        assert child.active_parse_attempt_id is None
        assert primary.parse_status == PARSE_PENDING
        assert primary.amount == 88
        assert primary.notes == "manual pending edit won"
        assert primary.money_in == 0
        assert primary.money_out == 88
        assert primary.reviewed_by == "manual-reviewer"
        assert primary.reviewed_at.replace(
            tzinfo=manual_reviewed_at.tzinfo
        ) == manual_reviewed_at
        assert transaction.is_deleted is True
        assert transaction.amount == 50
        assert transaction.source_content.endswith("Buy 50 cash")

    fixture.engine.dispose()


def test_stale_group_parse_after_child_edit_cannot_revive_or_overwrite_money() -> None:
    fixture = GroupFixture()

    async def edit_during_parse(**_kwargs):
        fixture.edit_child_and_revoke()
        return _parse_result(amount=50)

    with patch("app.discord.worker.build_stitch_group", side_effect=fixture.stitch_group):
        attempt_id = fixture.claim()
        with patch("app.discord.worker.managed_session", fixture.managed_session), patch(
            "app.discord.worker.parse_message",
            new=AsyncMock(side_effect=edit_during_parse),
        ), patch(
            "app.discord.worker.build_parser_attachment_inputs",
            new=AsyncMock(return_value=[]),
        ):
            asyncio.run(process_row(fixture.primary_id, attempt_id))

    with Session(fixture.engine) as session:
        transaction = session.get(Transaction, fixture.transaction_id)
        associations = session.exec(
            select(TransactionSourceRevision).where(
                TransactionSourceRevision.transaction_id == fixture.transaction_id
            )
        ).all()
        revisions = session.exec(
            select(DiscordMessageRevision).where(
                DiscordMessageRevision.message_id == fixture.child_id
            )
        ).all()
        assert transaction.is_deleted is True
        assert transaction.amount == 50
        assert len(associations) == 2
        assert len(revisions) == 2
    fixture.engine.dispose()


def test_projection_edit_during_parse_await_discards_stale_result() -> None:
    fixture = GroupFixture()
    manual_reviewed_at = utcnow()

    async def direct_edit_during_parse(**_kwargs):
        with Session(fixture.engine) as edit_session:
            child = edit_session.get(DiscordMessage, fixture.child_id)
            transaction = edit_session.get(Transaction, fixture.transaction_id)
            child.amount = 75
            child.notes = "manual edit during await won"
            child.money_in = 75
            child.money_out = 0
            child.needs_review = True
            child.reviewed_by = "manual-reviewer"
            child.reviewed_at = manual_reviewed_at
            transaction.amount = 75
            transaction.money_in = 75
            transaction.money_out = 0
            transaction.notes = "manual transaction edit won"
            edit_session.add(child)
            edit_session.add(transaction)
            edit_session.commit()
        return _parse_result(amount=50)

    with patch("app.discord.worker.build_stitch_group", side_effect=fixture.stitch_group):
        attempt_id = fixture.claim()
        parser_mock = AsyncMock(side_effect=direct_edit_during_parse)
        with patch("app.discord.worker.managed_session", fixture.managed_session), patch(
            "app.discord.worker.parse_message",
            new=parser_mock,
        ), patch(
            "app.discord.worker.build_parser_attachment_inputs",
            new=AsyncMock(return_value=[]),
        ):
            asyncio.run(process_row(fixture.primary_id, attempt_id))

    parser_mock.assert_awaited_once()
    with Session(fixture.engine) as session:
        attempt = session.get(ParseAttempt, attempt_id)
        primary = session.get(DiscordMessage, fixture.primary_id)
        child = session.get(DiscordMessage, fixture.child_id)
        transaction = session.get(Transaction, fixture.transaction_id)

        assert attempt.finished_at is not None
        assert attempt.success is False
        assert primary.active_parse_attempt_id is None
        assert child.active_parse_attempt_id is None
        assert primary.parse_status == PARSE_PENDING
        assert child.parse_status == PARSE_IGNORED
        assert child.amount == 75
        assert child.notes == "manual edit during await won"
        assert child.money_in == 75
        assert child.money_out == 0
        assert child.needs_review is True
        assert child.reviewed_by == "manual-reviewer"
        assert child.reviewed_at.replace(
            tzinfo=manual_reviewed_at.tzinfo
        ) == manual_reviewed_at
        assert transaction.amount == 75
        assert transaction.money_in == 75
        assert transaction.money_out == 0
        assert transaction.notes == "manual transaction edit won"

    fixture.engine.dispose()


def test_successful_stitched_worker_parse_keeps_primary_transaction_live() -> None:
    fixture = GroupFixture()
    with patch("app.discord.worker.build_stitch_group", side_effect=fixture.stitch_group):
        attempt_id = fixture.claim()
        with patch("app.discord.worker.managed_session", fixture.managed_session), patch(
            "app.discord.worker.parse_message",
            new=AsyncMock(return_value=_parse_result(amount=50)),
        ), patch(
            "app.discord.worker.build_parser_attachment_inputs",
            new=AsyncMock(return_value=[]),
        ):
            asyncio.run(process_row(fixture.primary_id, attempt_id))

    with Session(fixture.engine) as session:
        primary = session.get(DiscordMessage, fixture.primary_id)
        child = session.get(DiscordMessage, fixture.child_id)
        transaction = session.get(Transaction, fixture.transaction_id)
        associations = session.exec(
            select(TransactionSourceRevision).where(
                TransactionSourceRevision.transaction_id == fixture.transaction_id
            )
        ).all()
        assert primary.parse_status == PARSE_PARSED
        assert child.parse_status == PARSE_IGNORED
        assert transaction.is_deleted is False
        assert len(associations) == 2
    fixture.engine.dispose()


@pytest.mark.parametrize(
    ("field_name", "unsafe_value"),
    [
        ("parsed_amount", float("inf")),
        ("confidence", float("nan")),
    ],
)
def test_worker_rejects_unsafe_parser_financials_without_overwriting_safe_projection(
    field_name: str,
    unsafe_value: float,
) -> None:
    fixture = GroupFixture()
    result = _parse_result(amount=75)
    result[field_name] = unsafe_value

    with patch("app.discord.worker.build_stitch_group", side_effect=fixture.stitch_group):
        attempt_id = fixture.claim()
        with patch("app.discord.worker.managed_session", fixture.managed_session), patch(
            "app.discord.worker.parse_message",
            new=AsyncMock(return_value=result),
        ), patch(
            "app.discord.worker.build_parser_attachment_inputs",
            new=AsyncMock(return_value=[]),
        ):
            asyncio.run(process_row(fixture.primary_id, attempt_id))

    with Session(fixture.engine) as session:
        attempt = session.get(ParseAttempt, attempt_id)
        primary = session.get(DiscordMessage, fixture.primary_id)
        child = session.get(DiscordMessage, fixture.child_id)
        transaction = session.get(Transaction, fixture.transaction_id)

        assert attempt.finished_at is not None
        assert attempt.success is False
        assert "finite" in (attempt.error or "").lower() or "between" in (
            attempt.error or ""
        ).lower()
        assert primary.active_parse_attempt_id is None
        assert child.active_parse_attempt_id is None
        assert primary.parse_status == PARSE_FAILED
        assert primary.amount == 50
        assert primary.confidence is None
        assert transaction.amount == 50
        assert transaction.money_out == 50
        assert transaction.confidence is None
        assert transaction.is_deleted is False
        json.dumps(
            {
                "attempt_error": attempt.error,
                "row_error": primary.last_error,
                "row_amount": primary.amount,
                "row_confidence": primary.confidence,
                "transaction_amount": transaction.amount,
                "transaction_confidence": transaction.confidence,
            },
            allow_nan=False,
        )
    fixture.engine.dispose()


@pytest.mark.parametrize(
    ("result_key", "unsafe_value"),
    [
        (
            "_parse_disagreement",
            {
                "fields": ["parsed_type"],
                "rule": {"parsed_type": "sell", "parsed_amount": float("inf")},
                "ai": {"parsed_type": "buy", "parsed_amount": 75.0},
            },
        ),
        ("parsed_items", [{"name": "Card", "estimated_value": float("nan")}]),
        ("_openai_usage", {"input_tokens": 10, "estimated_cost_usd": float("inf")}),
    ],
)
def test_worker_rejects_non_json_parser_metadata_before_logging_or_projection_mutation(
    result_key: str,
    unsafe_value,
) -> None:
    fixture = GroupFixture()
    result = _parse_result(amount=75)
    result[result_key] = unsafe_value

    with patch("app.discord.worker.build_stitch_group", side_effect=fixture.stitch_group):
        attempt_id = fixture.claim()
        with patch("app.discord.worker.managed_session", fixture.managed_session), patch(
            "app.discord.worker.parse_message",
            new=AsyncMock(return_value=result),
        ), patch(
            "app.discord.worker.build_parser_attachment_inputs",
            new=AsyncMock(return_value=[]),
        ):
            asyncio.run(process_row(fixture.primary_id, attempt_id))

    with Session(fixture.engine) as session:
        attempt = session.get(ParseAttempt, attempt_id)
        primary = session.get(DiscordMessage, fixture.primary_id)
        transaction = session.get(Transaction, fixture.transaction_id)
        logs = session.exec(select(OperationsLog)).all()

        assert attempt.finished_at is not None
        assert attempt.success is False
        assert "strict json" in (attempt.error or "").lower()
        assert primary.parse_status == PARSE_FAILED
        assert primary.amount == 50
        assert primary.parse_disagreement_json is None
        assert transaction.amount == 50
        assert transaction.is_deleted is False
        assert not any(log.event_type == "queue.parse_disagreement" for log in logs)
        for log in logs:
            json.dumps(json.loads(log.details_json), allow_nan=False)

    fixture.engine.dispose()


def test_stale_ignore_cannot_delete_freshly_revived_group_transaction() -> None:
    fixture = GroupFixture()

    async def revive_then_return_stale_ignore(**_kwargs):
        fixture.edit_child_and_revoke(content="Buy 75 cash")
        with Session(fixture.engine) as session:
            primary = session.get(DiscordMessage, fixture.primary_id)
            child = session.get(DiscordMessage, fixture.child_id)
            primary.parse_status = PARSE_PARSED
            primary.amount = 75
            primary.money_out = 75
            child.parse_status = PARSE_IGNORED
            sync_transaction_from_message(
                session,
                primary,
                source_rows=[primary, child],
                source_content="Message 1: Photo\nMessage 2: Buy 75 cash",
            )
            session.commit()
        return _parse_result(ignore=True)

    with patch("app.discord.worker.build_stitch_group", side_effect=fixture.stitch_group):
        attempt_id = fixture.claim()
        with patch("app.discord.worker.managed_session", fixture.managed_session), patch(
            "app.discord.worker.parse_message",
            new=AsyncMock(side_effect=revive_then_return_stale_ignore),
        ), patch(
            "app.discord.worker.build_parser_attachment_inputs",
            new=AsyncMock(return_value=[]),
        ):
            asyncio.run(process_row(fixture.primary_id, attempt_id))

    with Session(fixture.engine) as session:
        transaction = session.get(Transaction, fixture.transaction_id)
        assert transaction.is_deleted is False
        assert transaction.amount == 75
        assert transaction.source_content.endswith("Buy 75 cash")
    fixture.engine.dispose()


def test_parser_exception_after_child_edit_preserves_newer_pending_state() -> None:
    fixture = GroupFixture()

    async def edit_then_raise(**_kwargs):
        fixture.edit_child_and_revoke()
        raise RuntimeError("parser failed after edit")

    with patch("app.discord.worker.build_stitch_group", side_effect=fixture.stitch_group):
        attempt_id = fixture.claim()
        with patch("app.discord.worker.managed_session", fixture.managed_session), patch(
            "app.discord.worker.parse_message",
            new=AsyncMock(side_effect=edit_then_raise),
        ), patch(
            "app.discord.worker.build_parser_attachment_inputs",
            new=AsyncMock(return_value=[]),
        ):
            asyncio.run(process_row(fixture.primary_id, attempt_id))

    with Session(fixture.engine) as session:
        primary = session.get(DiscordMessage, fixture.primary_id)
        child = session.get(DiscordMessage, fixture.child_id)
        assert primary.parse_status == PARSE_PENDING
        assert child.parse_status == PARSE_PENDING
        assert primary.last_error == "child edited during parse"
    fixture.engine.dispose()


def test_newer_attempt_created_during_attachment_prep_is_untouched() -> None:
    fixture = GroupFixture()
    newer_attempt_id: dict[str, int] = {}

    async def replace_claim_during_attachment_prep(*_args, **_kwargs):
        fixture.edit_child_and_revoke()
        newer_attempt_id["id"] = fixture.claim()
        return []

    with patch("app.discord.worker.build_stitch_group", side_effect=fixture.stitch_group):
        old_attempt_id = fixture.claim()
        with patch("app.discord.worker.managed_session", fixture.managed_session), patch(
            "app.discord.worker.build_parser_attachment_inputs",
            new=AsyncMock(side_effect=replace_claim_during_attachment_prep),
        ), patch(
            "app.discord.worker.parse_message",
            new=AsyncMock(return_value=_parse_result()),
        ):
            asyncio.run(process_row(fixture.primary_id, old_attempt_id))

    with Session(fixture.engine) as session:
        newer_attempt = session.get(ParseAttempt, newer_attempt_id["id"])
        primary = session.get(DiscordMessage, fixture.primary_id)
        child = session.get(DiscordMessage, fixture.child_id)
        assert newer_attempt.finished_at is None
        assert primary.active_parse_attempt_id == newer_attempt.id
        assert child.active_parse_attempt_id == newer_attempt.id
        assert primary.parse_status == PARSE_PROCESSING
    fixture.engine.dispose()


def test_replaced_group_claim_after_expansion_is_rejected_before_any_await() -> None:
    fixture = GroupFixture()
    replacement_attempt_id: dict[str, int] = {}
    original_validator = worker_module.reload_and_validate_parse_claim

    def replace_claim_before_validation(session, **kwargs):
        fixture.edit_child_and_revoke(content="Buy 75 cash")
        replacement_attempt_id["id"] = fixture.claim()
        return original_validator(session, **kwargs)

    attachment_mock = AsyncMock(return_value=[])
    parser_mock = AsyncMock(return_value=_parse_result(amount=50))

    with patch("app.discord.worker.build_stitch_group", side_effect=fixture.stitch_group):
        old_attempt_id = fixture.claim()
        with patch("app.discord.worker.managed_session", fixture.managed_session), patch(
            "app.discord.worker.reload_and_validate_parse_claim",
            side_effect=replace_claim_before_validation,
        ), patch(
            "app.discord.worker.build_parser_attachment_inputs",
            new=attachment_mock,
        ), patch(
            "app.discord.worker.parse_message",
            new=parser_mock,
        ):
            asyncio.run(process_row(fixture.primary_id, old_attempt_id))

    attachment_mock.assert_not_awaited()
    parser_mock.assert_not_awaited()

    with Session(fixture.engine) as session:
        old_attempt = session.get(ParseAttempt, old_attempt_id)
        replacement_attempt = session.get(
            ParseAttempt,
            replacement_attempt_id["id"],
        )
        primary = session.get(DiscordMessage, fixture.primary_id)
        child = session.get(DiscordMessage, fixture.child_id)
        transaction = session.get(Transaction, fixture.transaction_id)

        assert old_attempt.finished_at is not None
        assert old_attempt.success is False
        assert replacement_attempt.finished_at is None
        assert replacement_attempt.success is False
        assert primary.active_parse_attempt_id == replacement_attempt.id
        assert child.active_parse_attempt_id == replacement_attempt.id
        assert primary.parse_status == PARSE_PROCESSING
        assert child.content == "Buy 75 cash"
        assert transaction.is_deleted is True
        assert transaction.amount == 50
        assert transaction.source_content.endswith("Buy 50 cash")

    fixture.engine.dispose()


def test_quarantined_stitched_child_blocks_primary_claim_until_canonical_refresh() -> None:
    fixture = GroupFixture()
    with patch("app.discord.worker.build_stitch_group", side_effect=fixture.stitch_group):
        old_attempt_id = fixture.claim()

    with patch("app.discord.discord_ingest.managed_session", fixture.managed_session):
        assert block_message_after_raw_edit_fetch_failure(
            "group-child",
            error=RuntimeError("Discord unavailable"),
        )

    with Session(fixture.engine) as session:
        blocked_claim = claim_message_for_parse(session, fixture.primary_id)
        session.commit()
        assert blocked_claim is None
        transaction = session.get(Transaction, fixture.transaction_id)
        child = session.get(DiscordMessage, fixture.child_id)
        old_attempt = session.get(ParseAttempt, old_attempt_id)
        assert transaction.is_deleted is True
        assert child.parse_status == "failed"
        assert "canonical Discord refresh required" in (child.last_error or "")
        assert old_attempt.finished_at is not None

    with Session(fixture.engine) as session:
        child = session.get(DiscordMessage, fixture.child_id)
        canonical_message = SimpleNamespace(
            id=child.discord_message_id,
            guild=SimpleNamespace(id="guild"),
            channel=SimpleNamespace(id=child.channel_id, name=child.channel_name),
            author=SimpleNamespace(id="author", bot=False, __str__=lambda self: "seller"),
            content=child.content,
            attachments=[],
            created_at=child.created_at,
            edited_at=utcnow(),
        )

    with patch("app.discord.discord_ingest.managed_session", fixture.managed_session), patch(
        "app.discord.discord_ingest.sync_attachment_assets",
    ):
        assert insert_or_update_message(
            canonical_message,
            is_edit=True,
            watched_channel_ids=set(),
            canonical_source_refresh=True,
        ) == (True, "updated")

    with Session(fixture.engine) as session:
        child = session.get(DiscordMessage, fixture.child_id)
        assert child.source_refresh_required is False
        assert child.last_error is None
        assert child.parse_status == PARSE_PENDING

    with patch("app.discord.worker.build_stitch_group", side_effect=fixture.stitch_group):
        refreshed_attempt_id = fixture.claim()
        assert refreshed_attempt_id != old_attempt_id
    fixture.engine.dispose()


def test_raw_fetch_quarantine_persists_and_revokes_claim_when_invalidation_is_corrupt() -> None:
    fixture = GroupFixture()
    with patch("app.discord.worker.build_stitch_group", side_effect=fixture.stitch_group):
        attempt_id = fixture.claim()

    with Session(fixture.engine) as session:
        corrupt_primary = DiscordMessage(
            discord_message_id="corrupt-second-primary",
            channel_id="chan",
            channel_name="deals",
            author_name="seller",
            content="corrupt primary",
            attachment_urls_json="[]",
            created_at=utcnow() + timedelta(seconds=2),
            parse_status=PARSE_PARSED,
            stitched_group_id="group",
            stitched_primary=True,
            stitched_message_ids_json=f"[{fixture.primary_id}, {fixture.child_id}]",
        )
        session.add(corrupt_primary)
        session.commit()

    with patch("app.discord.discord_ingest.managed_session", fixture.managed_session), patch(
        "app.discord.discord_ingest.ingest_log",
        side_effect=RuntimeError("operations log unavailable"),
    ):
        assert block_message_after_raw_edit_fetch_failure(
            "group-child",
            error=RuntimeError("Discord unavailable"),
        )

    with Session(fixture.engine) as session:
        primary = session.get(DiscordMessage, fixture.primary_id)
        child = session.get(DiscordMessage, fixture.child_id)
        attempt = session.get(ParseAttempt, attempt_id)
        transaction = session.get(Transaction, fixture.transaction_id)
        assert child.source_refresh_required is True
        assert child.parse_status == PARSE_FAILED
        assert child.active_parse_attempt_id is None
        assert primary.active_parse_attempt_id is None
        assert attempt.finished_at is not None
        assert attempt.success is False
        assert transaction.is_deleted is False

        # The reportability boundary must independently exclude the stale money
        # even though corrupt provenance prevented transactional invalidation.
        primary.parse_status = PARSE_PARSED
        transaction.parse_status = PARSE_PARSED
        session.add(primary)
        session.add(transaction)
        session.commit()
        assert fixture.transaction_id not in {
            row.id for row in session.exec(transaction_base_query()).all()
        }
    fixture.engine.dispose()


def test_retry_route_cannot_clear_raw_fetch_quarantine_or_revive_transaction() -> None:
    from app.routers.messages import retry_message

    fixture = GroupFixture()
    with patch("app.discord.discord_ingest.managed_session", fixture.managed_session):
        assert block_message_after_raw_edit_fetch_failure(
            "group-primary",
            error=RuntimeError("Discord unavailable"),
        )

    with Session(fixture.engine) as session, patch(
        "app.routers.messages.require_role_response",
        return_value=None,
    ):
        with pytest.raises(HTTPException) as exc_info:
            retry_message(request=object(), message_id=fixture.primary_id, session=session)
        assert exc_info.value.status_code == 409
        assert "canonical Discord source refresh" in str(exc_info.value.detail)
        session.rollback()

    with Session(fixture.engine) as session:
        primary = session.get(DiscordMessage, fixture.primary_id)
        transaction = session.get(Transaction, fixture.transaction_id)
        assert primary.parse_status == "failed"
        assert primary.source_refresh_required is True
        assert "canonical Discord refresh required" in (primary.last_error or "")
        assert transaction.is_deleted is True
        assert len(
            session.exec(
                select(TransactionSourceRevision).where(
                    TransactionSourceRevision.transaction_id == fixture.transaction_id
                )
            ).all()
        ) == 2
    fixture.engine.dispose()


def test_approve_rejects_quarantine_committed_after_capture_before_source_lock() -> None:
    from app.discord import transactions as transaction_module
    from app.routers.messages import approve_message

    fixture = GroupFixture()
    original_lock = transaction_module.lock_source_group_mutation_guards

    def quarantine_then_lock(session: Session, guards):
        primary = session.get(DiscordMessage, fixture.primary_id)
        transaction = session.get(Transaction, fixture.transaction_id)
        primary.source_refresh_required = True
        primary.last_error = "canonical Discord refresh required after concurrent fetch failure"
        transaction.is_deleted = True
        transaction.needs_review = True
        session.add_all([primary, transaction])
        session.commit()
        return original_lock(session, guards)

    with Session(fixture.engine) as session, patch(
        "app.routers.messages.require_role_response",
        return_value=None,
    ), patch(
        "app.routers.messages.lock_source_group_mutation_guards",
        side_effect=quarantine_then_lock,
    ):
        with pytest.raises(HTTPException) as exc_info:
            approve_message(
                request=object(),
                message_id=fixture.primary_id,
                session=session,
            )
        assert exc_info.value.status_code == 409
        assert "changed during manual mutation" in str(exc_info.value.detail)
        assert not session.dirty

    with Session(fixture.engine) as session:
        primary = session.get(DiscordMessage, fixture.primary_id)
        transaction = session.get(Transaction, fixture.transaction_id)
        assert primary.source_refresh_required is True
        assert "concurrent fetch failure" in (primary.last_error or "")
        assert primary.parse_status == PARSE_PENDING
        assert transaction.is_deleted is True
        assert transaction.needs_review is True
        assert session.exec(select(ReviewCorrection)).all() == []
        assert session.exec(select(AuditLog)).all() == []
    fixture.engine.dispose()


def test_approve_rejects_source_with_active_parse_claim_before_preflight() -> None:
    from app.routers.messages import approve_message

    fixture = GroupFixture()
    attempt_id = fixture.claim()

    with Session(fixture.engine) as session, patch(
        "app.routers.messages.require_role_response",
        return_value=None,
    ):
        with pytest.raises(HTTPException) as exc_info:
            approve_message(
                request=object(),
                message_id=fixture.primary_id,
                session=session,
            )
        assert exc_info.value.status_code == 409
        assert "active parse attempt" in str(exc_info.value.detail)
        assert not session.dirty

    with Session(fixture.engine) as session:
        primary = session.get(DiscordMessage, fixture.primary_id)
        child = session.get(DiscordMessage, fixture.child_id)
        attempt = session.get(ParseAttempt, attempt_id)
        transaction = session.get(Transaction, fixture.transaction_id)
        assert primary.active_parse_attempt_id == attempt_id
        assert child.active_parse_attempt_id == attempt_id
        assert primary.parse_status == PARSE_PROCESSING
        assert attempt.finished_at is None
        assert transaction.is_deleted is False
        assert session.exec(select(ReviewCorrection)).all() == []
        assert session.exec(select(AuditLog)).all() == []
    fixture.engine.dispose()


def test_reparse_rejects_source_with_active_parse_claim_and_preserves_attempt() -> None:
    from app.routers.messages import reparse_message_form

    fixture = GroupFixture()
    attempt_id = fixture.claim()

    with Session(fixture.engine) as session, patch(
        "app.routers.messages.require_role_response",
        return_value=None,
    ):
        with pytest.raises(HTTPException) as exc_info:
            reparse_message_form(
                request=object(),
                message_id=fixture.primary_id,
                session=session,
            )
        assert exc_info.value.status_code == 409
        assert "active parse attempt" in str(exc_info.value.detail)
        assert not session.dirty

    with Session(fixture.engine) as session:
        primary = session.get(DiscordMessage, fixture.primary_id)
        child = session.get(DiscordMessage, fixture.child_id)
        attempt = session.get(ParseAttempt, attempt_id)
        transaction = session.get(Transaction, fixture.transaction_id)
        assert primary.active_parse_attempt_id == attempt_id
        assert child.active_parse_attempt_id == attempt_id
        assert primary.parse_status == PARSE_PROCESSING
        assert attempt.finished_at is None
        assert transaction.is_deleted is False
    fixture.engine.dispose()


def test_channels_edit_rejects_bound_child_quarantine_before_source_lock() -> None:
    from app.discord import transactions as transaction_module
    from app.routers.channels_api import edit_message_form

    fixture = GroupFixture()
    original_lock = transaction_module.lock_source_group_mutation_guards
    with Session(fixture.engine) as session:
        primary = session.get(DiscordMessage, fixture.primary_id)
        before = (
            primary.content,
            primary.deal_type,
            primary.amount,
            primary.payment_method,
            primary.entry_kind,
            primary.expense_category,
            primary.notes,
            primary.parse_status,
            primary.needs_review,
        )

    def quarantine_child_then_lock(session: Session, guards):
        child = session.get(DiscordMessage, fixture.child_id)
        transaction = session.get(Transaction, fixture.transaction_id)
        child.source_refresh_required = True
        child.last_error = "canonical Discord refresh required after concurrent child failure"
        transaction.is_deleted = True
        transaction.needs_review = True
        session.add_all([child, transaction])
        session.commit()
        return original_lock(session, guards)

    request = SimpleNamespace(
        state=SimpleNamespace(
            current_user=SimpleNamespace(id=42, username="reviewer", display_name="Reviewer")
        )
    )
    with Session(fixture.engine) as session, patch(
        "app.routers.channels_api.require_role_response",
        return_value=None,
    ), patch(
        "app.discord.transactions.lock_source_group_mutation_guards",
        side_effect=quarantine_child_then_lock,
    ):
        with pytest.raises(HTTPException) as exc_info:
            edit_message_form(
                request=request,
                message_id=fixture.primary_id,
                return_path="/table",
                status=None,
                channel_id=None,
                filter_expense_category=None,
                after=None,
                before=None,
                sort_by="time",
                sort_dir="desc",
                page=1,
                limit=100,
                parse_status=PARSE_PARSED,
                needs_review=None,
                deal_type="sell",
                amount="999",
                payment_method="zelle",
                cash_direction=None,
                category="sales",
                entry_kind="sale",
                expense_category="sales",
                confidence="0.99",
                notes="must not persist",
                trade_summary=None,
                item_names_text=None,
                items_in_text=None,
                items_out_text=None,
                approve_after_save="true",
                stay_on_detail=None,
                review_action=None,
                next_message_id=None,
                session=session,
            )
        assert exc_info.value.status_code == 409
        assert "changed during manual mutation" in str(exc_info.value.detail)
        assert not session.dirty

    with Session(fixture.engine) as session:
        primary = session.get(DiscordMessage, fixture.primary_id)
        child = session.get(DiscordMessage, fixture.child_id)
        transaction = session.get(Transaction, fixture.transaction_id)
        assert (
            primary.content,
            primary.deal_type,
            primary.amount,
            primary.payment_method,
            primary.entry_kind,
            primary.expense_category,
            primary.notes,
            primary.parse_status,
            primary.needs_review,
        ) == before
        assert child.source_refresh_required is True
        assert "concurrent child failure" in (child.last_error or "")
        assert transaction.is_deleted is True
        assert transaction.needs_review is True
        assert session.exec(select(ReviewCorrection)).all() == []
        assert session.exec(select(AuditLog)).all() == []
    fixture.engine.dispose()


def test_channels_edit_rejects_worker_claim_created_after_capture_before_lock() -> None:
    from app.discord import transactions as transaction_module
    from app.routers.channels_api import edit_message_form

    fixture = GroupFixture()
    original_lock = transaction_module.lock_source_group_mutation_guards
    claimed_attempt_id = None

    def claim_then_lock(session: Session, guards):
        nonlocal claimed_attempt_id
        attempt = claim_message_for_parse(session, fixture.primary_id)
        assert attempt is not None
        claimed_attempt_id = attempt.id
        session.commit()
        return original_lock(session, guards)

    request = SimpleNamespace(
        state=SimpleNamespace(
            current_user=SimpleNamespace(id=42, username="reviewer", display_name="Reviewer")
        )
    )
    with Session(fixture.engine) as session, patch(
        "app.routers.channels_api.require_role_response",
        return_value=None,
    ), patch(
        "app.discord.transactions.lock_source_group_mutation_guards",
        side_effect=claim_then_lock,
    ):
        with pytest.raises(HTTPException) as exc_info:
            edit_message_form(
                request=request,
                message_id=fixture.primary_id,
                return_path="/table",
                status=None,
                channel_id=None,
                filter_expense_category=None,
                after=None,
                before=None,
                sort_by="time",
                sort_dir="desc",
                page=1,
                limit=100,
                parse_status=PARSE_PARSED,
                needs_review=None,
                deal_type="sell",
                amount="999",
                payment_method="zelle",
                cash_direction=None,
                category="sales",
                entry_kind="sale",
                expense_category="sales",
                confidence="0.99",
                notes="must not persist",
                trade_summary=None,
                item_names_text=None,
                items_in_text=None,
                items_out_text=None,
                approve_after_save="true",
                stay_on_detail=None,
                review_action=None,
                next_message_id=None,
                session=session,
            )
        assert exc_info.value.status_code == 409
        assert "changed during manual mutation" in str(exc_info.value.detail)
        assert not session.dirty

    with Session(fixture.engine) as session:
        primary = session.get(DiscordMessage, fixture.primary_id)
        child = session.get(DiscordMessage, fixture.child_id)
        attempt = session.get(ParseAttempt, claimed_attempt_id)
        transaction = session.get(Transaction, fixture.transaction_id)
        assert primary.active_parse_attempt_id == claimed_attempt_id
        assert child.active_parse_attempt_id == claimed_attempt_id
        assert primary.parse_status == PARSE_PROCESSING
        assert primary.amount == 50
        assert primary.entry_kind == "buy"
        assert attempt.finished_at is None
        assert transaction.amount == 50
        assert transaction.is_deleted is False
        assert session.exec(select(ReviewCorrection)).all() == []
        assert session.exec(select(AuditLog)).all() == []
    fixture.engine.dispose()


def test_retry_primary_rejects_quarantined_bound_child_before_mutation() -> None:
    from app.routers.messages import retry_message

    fixture = GroupFixture()
    with patch("app.discord.discord_ingest.managed_session", fixture.managed_session):
        assert block_message_after_raw_edit_fetch_failure(
            "group-child",
            error=RuntimeError("Discord unavailable"),
        )

    with Session(fixture.engine) as session:
        primary = session.get(DiscordMessage, fixture.primary_id)
        before = (
            primary.parse_status,
            primary.parse_attempts,
            primary.last_error,
            primary.active_reparse_run_id,
        )

    with Session(fixture.engine) as session, patch(
        "app.routers.messages.require_role_response",
        return_value=None,
    ):
        with pytest.raises(HTTPException) as exc_info:
            retry_message(request=object(), message_id=fixture.primary_id, session=session)
        assert exc_info.value.status_code == 409
        assert "canonical Discord source refresh" in str(exc_info.value.detail)
        assert not session.dirty

    with Session(fixture.engine) as session:
        primary = session.get(DiscordMessage, fixture.primary_id)
        child = session.get(DiscordMessage, fixture.child_id)
        transaction = session.get(Transaction, fixture.transaction_id)
        assert (
            primary.parse_status,
            primary.parse_attempts,
            primary.last_error,
            primary.active_reparse_run_id,
        ) == before
        assert child.source_refresh_required is True
        assert transaction.is_deleted is True
        assert len(
            session.exec(
                select(TransactionSourceRevision).where(
                    TransactionSourceRevision.transaction_id == fixture.transaction_id
                )
            ).all()
        ) == 2
    fixture.engine.dispose()


def test_retry_primary_rejects_corrupt_bound_source_positions_before_mutation() -> None:
    from app.routers.messages import retry_message

    fixture = GroupFixture()
    with Session(fixture.engine) as session:
        child_association = session.exec(
            select(TransactionSourceRevision).where(
                TransactionSourceRevision.transaction_id == fixture.transaction_id,
                TransactionSourceRevision.message_id == fixture.child_id,
            )
        ).one()
        child_association.source_position = 2
        session.add(child_association)
        session.commit()
        primary = session.get(DiscordMessage, fixture.primary_id)
        before = (
            primary.parse_status,
            primary.parse_attempts,
            primary.last_error,
            primary.active_reparse_run_id,
        )

    with Session(fixture.engine) as session, patch(
        "app.routers.messages.require_role_response",
        return_value=None,
    ):
        with pytest.raises(HTTPException) as exc_info:
            retry_message(request=object(), message_id=fixture.primary_id, session=session)
        assert exc_info.value.status_code == 409
        assert "positions" in str(exc_info.value.detail)
        assert not session.dirty

    with Session(fixture.engine) as session:
        primary = session.get(DiscordMessage, fixture.primary_id)
        transaction = session.get(Transaction, fixture.transaction_id)
        associations = session.exec(
            select(TransactionSourceRevision)
            .where(TransactionSourceRevision.transaction_id == fixture.transaction_id)
            .order_by(TransactionSourceRevision.source_position)
        ).all()
        assert (
            primary.parse_status,
            primary.parse_attempts,
            primary.last_error,
            primary.active_reparse_run_id,
        ) == before
        assert transaction.is_deleted is False
        assert [association.source_position for association in associations] == [0, 2]
    fixture.engine.dispose()


def test_retry_allows_current_standalone_source_with_preserved_historical_group_binding() -> None:
    from app.routers.messages import retry_message

    fixture = GroupFixture()
    with Session(fixture.engine) as session:
        primary = session.get(DiscordMessage, fixture.primary_id)
        child = session.get(DiscordMessage, fixture.child_id)
        transaction = session.get(Transaction, fixture.transaction_id)
        primary.stitched_group_id = None
        primary.stitched_primary = False
        primary.stitched_message_ids_json = "[]"
        child.stitched_group_id = None
        child.stitched_primary = False
        child.stitched_message_ids_json = "[]"
        transaction.is_deleted = True
        transaction.needs_review = True
        transaction.parse_status = PARSE_PENDING
        session.add_all([primary, child, transaction])
        session.commit()
        association_snapshot = [
            (
                association.id,
                association.message_id,
                association.revision_id,
                association.source_position,
            )
            for association in session.exec(
                select(TransactionSourceRevision)
                .where(TransactionSourceRevision.transaction_id == fixture.transaction_id)
                .order_by(TransactionSourceRevision.source_position)
            ).all()
        ]

    with Session(fixture.engine) as session, patch(
        "app.routers.messages.require_role_response",
        return_value=None,
    ):
        response = retry_message(
            request=object(),
            message_id=fixture.primary_id,
            session=session,
        )
        assert response["ok"] is True

    with Session(fixture.engine) as session:
        primary = session.get(DiscordMessage, fixture.primary_id)
        transaction = session.get(Transaction, fixture.transaction_id)
        associations = session.exec(
            select(TransactionSourceRevision)
            .where(TransactionSourceRevision.transaction_id == fixture.transaction_id)
            .order_by(TransactionSourceRevision.source_position)
        ).all()
        assert primary.parse_status == PARSE_PENDING
        assert transaction.is_deleted is True
        assert [
            (
                association.id,
                association.message_id,
                association.revision_id,
                association.source_position,
            )
            for association in associations
        ] == association_snapshot
    fixture.engine.dispose()


def test_retry_form_cannot_clear_raw_fetch_quarantine_or_revive_transaction() -> None:
    from app.routers.messages import reparse_message_form

    fixture = GroupFixture()
    with patch("app.discord.discord_ingest.managed_session", fixture.managed_session):
        assert block_message_after_raw_edit_fetch_failure(
            "group-primary",
            error=RuntimeError("Discord unavailable"),
        )

    with Session(fixture.engine) as session, patch(
        "app.routers.messages.require_role_response",
        return_value=None,
    ):
        with pytest.raises(HTTPException) as exc_info:
            reparse_message_form(
                request=object(),
                message_id=fixture.primary_id,
                session=session,
            )
        assert exc_info.value.status_code == 409
        assert "canonical Discord source refresh" in str(exc_info.value.detail)
        session.rollback()

    with Session(fixture.engine) as session:
        primary = session.get(DiscordMessage, fixture.primary_id)
        transaction = session.get(Transaction, fixture.transaction_id)
        assert primary.parse_status == "failed"
        assert primary.source_refresh_required is True
        assert transaction.is_deleted is True
        assert len(
            session.exec(
                select(TransactionSourceRevision).where(
                    TransactionSourceRevision.transaction_id == fixture.transaction_id
                )
            ).all()
        ) == 2
    fixture.engine.dispose()


def test_noncanonical_message_update_cannot_release_raw_fetch_quarantine() -> None:
    fixture = GroupFixture()
    with patch("app.discord.discord_ingest.managed_session", fixture.managed_session):
        assert block_message_after_raw_edit_fetch_failure(
            "group-primary",
            error=RuntimeError("Discord unavailable"),
        )

    class Author:
        id = "author"
        bot = False

        def __str__(self):
            return "seller"

    with Session(fixture.engine) as session:
        primary = session.get(DiscordMessage, fixture.primary_id)
        incoming = SimpleNamespace(
            id=primary.discord_message_id,
            guild=SimpleNamespace(id="guild"),
            channel=SimpleNamespace(id=primary.channel_id, name=primary.channel_name),
            author=Author(),
            content=primary.content,
            attachments=[],
            created_at=primary.created_at,
            edited_at=utcnow(),
        )

    with patch("app.discord.discord_ingest.managed_session", fixture.managed_session):
        assert insert_or_update_message(
            incoming,
            is_edit=True,
            watched_channel_ids=set(),
        ) == (False, "refresh_required")

    with Session(fixture.engine) as session:
        primary = session.get(DiscordMessage, fixture.primary_id)
        transaction = session.get(Transaction, fixture.transaction_id)
        assert primary.source_refresh_required is True
        assert primary.parse_status == "failed"
        assert "canonical Discord refresh required" in (primary.last_error or "")
        assert transaction.is_deleted is True
    fixture.engine.dispose()


def test_reparse_service_skips_raw_fetch_quarantine_without_queueing_it() -> None:
    from app.discord.reparse import reparse_message_rows

    fixture = GroupFixture()
    with patch("app.discord.discord_ingest.managed_session", fixture.managed_session):
        assert block_message_after_raw_edit_fetch_failure(
            "group-primary",
            error=RuntimeError("Discord unavailable"),
        )

    with Session(fixture.engine) as session:
        primary = session.get(DiscordMessage, fixture.primary_id)
        assert reparse_message_rows(
            session,
            [primary],
            reason="manual service reparse",
            reset_attempts=True,
        ) == 0

    with Session(fixture.engine) as session:
        primary = session.get(DiscordMessage, fixture.primary_id)
        transaction = session.get(Transaction, fixture.transaction_id)
        assert primary.parse_status == "failed"
        assert primary.source_refresh_required is True
        assert "canonical Discord refresh required" in (primary.last_error or "")
        assert transaction.is_deleted is True
    fixture.engine.dispose()
