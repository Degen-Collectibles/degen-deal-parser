import asyncio
import json
from contextlib import contextmanager
from datetime import timedelta
from unittest.mock import AsyncMock, patch

import pytest
from app import db
from app.discord import worker as worker_module
from app.discord.transactions import sync_transaction_from_message
from app.models import (
    DiscordMessage,
    GmailReceipt,
    OperationsLog,
    PARSE_IGNORED,
    PARSE_FAILED,
    PARSE_PARSED,
    PARSE_PENDING,
    PARSE_PROCESSING,
    PARSE_REVIEW_REQUIRED,
    ParseAttempt,
    Transaction,
    utcnow,
)
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine, select


def make_message(
    *,
    message_id: int = 1,
    content: str = "sell card 25 cash",
    attachments: list[str] | None = None,
) -> DiscordMessage:
    return DiscordMessage(
        id=message_id,
        discord_message_id=f"token-efficiency-{message_id}",
        channel_id="deals",
        channel_name="deal-log",
        author_id="42",
        author_name="tester",
        content=content,
        attachment_urls_json=json.dumps(attachments or []),
        created_at=utcnow() + timedelta(seconds=message_id),
    )


def test_discord_message_exposes_successful_parse_identity_metadata():
    columns = DiscordMessage.__table__.columns

    assert "last_parse_input_fingerprint" in columns
    assert "last_successful_parse_status" in columns


def test_additive_migrations_include_successful_parse_identity_metadata():
    for migrations in (db.SQLITE_ADDITIVE_MIGRATIONS, db.POSTGRES_ADDITIVE_MIGRATIONS):
        assert migrations["discordmessage"]["last_parse_input_fingerprint"] == "TEXT"
        assert migrations["discordmessage"]["last_successful_parse_status"] == "TEXT"


def test_parse_input_fingerprint_is_deterministic_and_content_sensitive():
    row = make_message()

    first = worker_module.build_parse_input_fingerprint(
        [row],
        provider="nvidia",
        model="us/azure/openai/eccn-gpt-5.5",
    )
    second = worker_module.build_parse_input_fingerprint(
        [row],
        provider="nvidia",
        model="us/azure/openai/eccn-gpt-5.5",
    )
    row.content = "sell changed card 30 cash"
    changed = worker_module.build_parse_input_fingerprint(
        [row],
        provider="nvidia",
        model="us/azure/openai/eccn-gpt-5.5",
    )

    assert first == second
    assert changed != first


def test_parse_input_fingerprint_changes_with_attachments_group_or_model():
    first_row = make_message(attachments=["https://cdn.example/one.jpg"])
    second_row = make_message(message_id=2, content="zelle 25")

    baseline = worker_module.build_parse_input_fingerprint(
        [first_row],
        provider="nvidia",
        model="model-a",
    )
    changed_attachments = make_message(
        attachments=["https://cdn.example/two.jpg", "https://cdn.example/one.jpg"]
    )

    assert worker_module.build_parse_input_fingerprint(
        [changed_attachments], provider="nvidia", model="model-a"
    ) != baseline
    assert worker_module.build_parse_input_fingerprint(
        [first_row, second_row], provider="nvidia", model="model-a"
    ) != baseline
    assert worker_module.build_parse_input_fingerprint(
        [first_row], provider="nvidia", model="model-b"
    ) != baseline


def test_parse_input_fingerprint_tolerates_malformed_attachment_json():
    row = make_message()
    row.attachment_urls_json = "not-json"

    fingerprint = worker_module.build_parse_input_fingerprint(
        [row], provider="nvidia", model="model-a"
    )

    assert len(fingerprint) == 64


def make_engine():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


def parsed_result() -> dict:
    return {
        "parsed_type": "sell",
        "parsed_amount": 25.0,
        "parsed_payment_method": "cash",
        "parsed_cash_direction": "to_store",
        "parsed_category": "singles",
        "parsed_items": ["Charizard"],
        "parsed_items_in": [],
        "parsed_items_out": ["Charizard"],
        "parsed_trade_summary": None,
        "parsed_notes": "parsed",
        "image_summary": None,
        "confidence": 0.95,
        "needs_review": False,
        "ignore_message": False,
    }


def test_discord_sync_preserves_gmail_owned_transaction():
    engine = make_engine()
    with Session(engine) as session:
        row = make_message()
        row.id = None
        row.parse_status = PARSE_IGNORED
        session.add(row)
        session.commit()
        session.refresh(row)

        transaction = Transaction(
            source_message_id=row.id,
            source_kind="gmail_sortswift",
            source_external_id="gmail-message-1",
            occurred_at=row.created_at,
            parse_status=PARSE_REVIEW_REQUIRED,
            needs_review=True,
        )
        session.add(transaction)
        session.commit()
        session.refresh(transaction)
        receipt = GmailReceipt(
            gmail_message_id="gmail-message-1",
            transaction_id=transaction.id,
        )
        session.add(receipt)
        session.commit()

        result = sync_transaction_from_message(session, row)
        session.commit()

        assert result is not None
        assert session.get(Transaction, transaction.id) is not None
        assert session.get(GmailReceipt, receipt.id).transaction_id == transaction.id


def test_process_row_skips_external_source_before_attachment_or_ai_work():
    engine = make_engine()
    with Session(engine) as session:
        row = make_message(attachments=["https://cdn.example/card.jpg"])
        row.id = None
        row.parse_status = PARSE_PROCESSING
        row.parse_attempts = 1
        session.add(row)
        session.commit()
        session.refresh(row)
        transaction = Transaction(
            source_message_id=row.id,
            source_kind="gmail_sortswift",
            source_external_id="gmail-message-2",
            occurred_at=row.created_at,
            parse_status=PARSE_REVIEW_REQUIRED,
            needs_review=True,
        )
        session.add(transaction)
        session.add(ParseAttempt(message_id=row.id, attempt_number=1))
        session.commit()

        @contextmanager
        def fake_managed_session():
            yield session

        with (
            patch("app.discord.worker.managed_session", new=fake_managed_session),
            patch(
                "app.discord.worker.build_parser_attachment_inputs",
                new_callable=AsyncMock,
                return_value=[],
            ) as attachment_mock,
            patch(
                "app.discord.worker.parse_message",
                new_callable=AsyncMock,
                return_value=parsed_result(),
            ) as parse_mock,
        ):
            asyncio.run(worker_module.process_row(row.id))

        session.refresh(row)
        attempt = session.exec(
            select(ParseAttempt).where(ParseAttempt.message_id == row.id)
        ).one()

        attachment_mock.assert_not_awaited()
        parse_mock.assert_not_awaited()
        assert row.parse_status == PARSE_REVIEW_REQUIRED
        assert row.needs_review is True
        assert attempt.success is True
        assert attempt.finished_at is not None
        assert attempt.input_tokens == 0
        assert attempt.output_tokens == 0
        assert attempt.total_tokens == 0


def test_process_row_skips_unchanged_automatic_input_and_restores_review_status():
    engine = make_engine()
    with Session(engine) as session:
        row = make_message(attachments=["https://cdn.example/card.jpg"])
        row.id = None
        row.parse_status = PARSE_PROCESSING
        row.parse_attempts = 2
        row.last_error = "auto reprocess"
        session.add(row)
        session.commit()
        session.refresh(row)
        row.last_parse_input_fingerprint = worker_module.build_parse_input_fingerprint([row])
        row.last_successful_parse_status = PARSE_REVIEW_REQUIRED
        session.add(row)
        session.add(ParseAttempt(message_id=row.id, attempt_number=2))
        session.commit()

        @contextmanager
        def fake_managed_session():
            yield session

        with (
            patch("app.discord.worker.managed_session", new=fake_managed_session),
            patch(
                "app.discord.worker.build_parser_attachment_inputs",
                new_callable=AsyncMock,
                return_value=[],
            ) as attachment_mock,
            patch(
                "app.discord.worker.parse_message",
                new_callable=AsyncMock,
                return_value=parsed_result(),
            ) as parse_mock,
        ):
            asyncio.run(worker_module.process_row(row.id))

        session.refresh(row)
        attempt = session.exec(
            select(ParseAttempt).where(ParseAttempt.message_id == row.id)
        ).one()
        logs = session.exec(
            select(OperationsLog).where(OperationsLog.event_type == "queue.parse_skipped_unchanged")
        ).all()

        attachment_mock.assert_not_awaited()
        parse_mock.assert_not_awaited()
        assert row.parse_status == PARSE_REVIEW_REQUIRED
        assert row.needs_review is True
        assert row.parse_attempts == 2
        assert attempt.success is True
        assert attempt.total_tokens == 0
        assert len(logs) == 1


def test_manual_reparse_bypasses_unchanged_input_guard():
    engine = make_engine()
    with Session(engine) as session:
        row = make_message()
        row.id = None
        row.parse_status = PARSE_PROCESSING
        row.parse_attempts = 1
        row.last_error = "manual row reparse"
        session.add(row)
        session.commit()
        session.refresh(row)
        row.last_parse_input_fingerprint = worker_module.build_parse_input_fingerprint([row])
        row.last_successful_parse_status = PARSE_PARSED
        session.add(row)
        session.add(ParseAttempt(message_id=row.id, attempt_number=1))
        session.commit()

        @contextmanager
        def fake_managed_session():
            yield session

        with (
            patch("app.discord.worker.managed_session", new=fake_managed_session),
            patch(
                "app.discord.worker.build_parser_attachment_inputs",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "app.discord.worker.parse_message",
                new_callable=AsyncMock,
                return_value=parsed_result(),
            ) as parse_mock,
        ):
            asyncio.run(worker_module.process_row(row.id))

        parse_mock.assert_awaited_once()


def test_successful_group_parse_stores_fingerprint_and_terminal_status_on_every_row():
    engine = make_engine()
    with Session(engine) as session:
        primary = make_message(message_id=1, content="sell card")
        child = make_message(message_id=2, content="cash 25")
        primary.id = None
        child.id = None
        primary.parse_status = PARSE_PROCESSING
        primary.parse_attempts = 1
        child.parse_status = PARSE_PARSED
        session.add(primary)
        session.add(child)
        session.commit()
        session.refresh(primary)
        session.refresh(child)
        session.add(ParseAttempt(message_id=primary.id, attempt_number=1))
        session.commit()

        @contextmanager
        def fake_managed_session():
            yield session

        with (
            patch("app.discord.worker.managed_session", new=fake_managed_session),
            patch(
                "app.discord.worker.build_stitch_group",
                return_value=[primary, child],
            ),
            patch(
                "app.discord.worker.build_parser_attachment_inputs",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "app.discord.worker.parse_message",
                new_callable=AsyncMock,
                return_value=parsed_result(),
            ),
        ):
            asyncio.run(worker_module.process_row(primary.id))

        session.refresh(primary)
        session.refresh(child)

        assert primary.last_parse_input_fingerprint
        assert child.last_parse_input_fingerprint == primary.last_parse_input_fingerprint
        assert primary.last_successful_parse_status == PARSE_PARSED
        assert child.last_successful_parse_status == PARSE_IGNORED


def test_process_row_rolls_back_failed_session_before_recording_row_failure():
    engine = make_engine()
    with Session(engine) as session:
        row = make_message()
        row.id = None
        row.parse_status = PARSE_PROCESSING
        row.parse_attempts = 1
        session.add(row)
        session.commit()
        session.refresh(row)
        session.add(ParseAttempt(message_id=row.id, attempt_number=1))
        session.commit()

        @contextmanager
        def fake_managed_session():
            yield session

        def violate_unique_message_id(active_session: Session, active_row: DiscordMessage):
            duplicate = make_message(message_id=99)
            duplicate.id = None
            duplicate.discord_message_id = active_row.discord_message_id
            active_session.add(duplicate)
            active_session.flush()

        with (
            patch("app.discord.worker.managed_session", new=fake_managed_session),
            patch(
                "app.discord.worker.build_parser_attachment_inputs",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch(
                "app.discord.worker.parse_message",
                new_callable=AsyncMock,
                return_value=parsed_result(),
            ),
            patch(
                "app.discord.worker.sync_transaction_from_message",
                side_effect=violate_unique_message_id,
            ),
        ):
            try:
                asyncio.run(worker_module.process_row(row.id))
            except Exception as exc:
                session.rollback()
                pytest.fail(f"process_row escaped instead of recording the row failure: {exc}")

        session.expire_all()
        failed_row = session.get(DiscordMessage, row.id)
        attempt = session.exec(
            select(ParseAttempt).where(ParseAttempt.message_id == row.id)
        ).one()

        assert failed_row.parse_status == PARSE_FAILED
        assert "UNIQUE constraint failed" in failed_row.last_error
        assert attempt.success is False
        assert attempt.finished_at is not None
        assert "UNIQUE constraint failed" in attempt.error


def test_process_once_continues_after_unhandled_row_exception():
    engine = make_engine()
    with Session(engine) as session:
        first = make_message(message_id=1)
        second = make_message(message_id=2)
        first.id = None
        second.id = None
        first.parse_status = PARSE_PENDING
        second.parse_status = PARSE_PENDING
        session.add(first)
        session.add(second)
        session.commit()

        @contextmanager
        def fake_managed_session():
            yield session

        with (
            patch("app.discord.worker.managed_session", new=fake_managed_session),
            patch("app.discord.worker.settings.parser_batch_size", 2),
            patch(
                "app.discord.worker.process_row",
                new_callable=AsyncMock,
                side_effect=[RuntimeError("first row exploded"), None],
            ) as process_mock,
        ):
            try:
                asyncio.run(worker_module.process_once())
            except Exception as exc:
                pytest.fail(f"process_once stopped before the second row: {exc}")

        assert [call.args[0] for call in process_mock.await_args_list] == [first.id, second.id]
