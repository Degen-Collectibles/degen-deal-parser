import unittest
import asyncio
import json
import types
from datetime import timedelta
from contextlib import contextmanager
from pathlib import Path
import shutil
import uuid
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException
from sqlalchemy import update
from sqlmodel import Session, SQLModel, create_engine, select
from starlette.requests import Request

from app.routers.admin_actions import (
    admin_parser_learned_rule_log_page,
    admin_parser_reparse_range_form,
    admin_parser_reprocess_form,
    admin_parser_reparse_range,
)
from app.routers.channels_api import admin_queue_state_counts
from app.routers.messages import bulk_reparse_filtered_messages_form, reparse_message_form
from app.shared import build_debug_snapshot
from app.models import (
    AttachmentAsset,
    DISCORD_SOURCE_REFRESH_REQUIRED_ERROR,
    DiscordMessage,
    OperationsLog,
    PARSE_FAILED,
    PARSE_IGNORED,
    PARSE_PARSED,
    PARSE_PENDING,
    PARSE_PROCESSING,
    PARSE_REVIEW_REQUIRED,
    ParseAttempt,
    ReparseRun,
    Transaction,
    TransactionItem,
    TransactionSourceRevision,
    utcnow,
)
from app.discord.message_revisions import ensure_message_revision
from app.discord.parser import choose_image_urls
from app.discord.reparse_runs import (
    create_reparse_run_record,
    finalize_reparse_run_queue_record,
    list_recent_reparse_runs,
    record_reparse_run_outcome,
)
from app.reporting import get_financial_rows
from app.discord.transactions import (
    SourceMutationSnapshot,
    StaleSourceRevisionError,
    get_transactions,
    sync_transaction_from_message,
    transaction_base_query,
)
from app.discord.worker import (
    MAX_ATTEMPTS_ERROR,
    RangeReparseSelectionLimitError,
    close_or_recover_unfinished_attempts,
    queue_reparse_range,
)
from app.discord.worker import claim_message_for_parse, process_once, process_row


def make_request(path: str) -> Request:
    return Request({"type": "http", "method": "POST", "path": path, "headers": []})


class QueueReparseValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path.cwd() / "tests" / ".tmp_validation" / str(uuid.uuid4())
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = self.temp_dir / "validation.db"
        self.engine = create_engine(f"sqlite:///{db_path.as_posix()}", connect_args={"check_same_thread": False})
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def session(self) -> Session:
        return Session(self.engine)

    def make_message(self, **overrides) -> DiscordMessage:
        now = utcnow()
        defaults = {
            "discord_message_id": f"msg-{now.timestamp()}",
            "channel_id": "chan-1",
            "channel_name": "chan-1",
            "author_name": "tester",
            "content": "sold card $20 zelle",
            "created_at": now,
            "parse_status": PARSE_PARSED,
            "parse_attempts": 0,
            "needs_review": False,
        }
        defaults.update(overrides)
        return DiscordMessage(**defaults)

    def test_close_or_recover_marks_exhausted_pending_row_failed(self) -> None:
        with self.session() as session:
            row = self.make_message(
                discord_message_id="exhausted-row",
                parse_status=PARSE_PENDING,
                parse_attempts=3,
                last_error=None,
            )
            session.add(row)
            session.commit()
            session.refresh(row)

            close_or_recover_unfinished_attempts(session)
            session.refresh(row)

            self.assertEqual(row.parse_status, PARSE_FAILED)
            self.assertEqual(row.last_error, MAX_ATTEMPTS_ERROR)

    def test_process_once_exhausts_failed_row_at_retry_cap(self) -> None:
        with self.session() as session:
            row = self.make_message(
                discord_message_id="failed-at-cap",
                parse_status=PARSE_FAILED,
                parse_attempts=3,
                last_error=None,
            )
            session.add(row)
            session.commit()
            session.refresh(row)

            @contextmanager
            def fake_managed_session():
                yield session

            with patch("app.discord.worker.managed_session", new=fake_managed_session):
                asyncio.run(process_once())

            session.refresh(row)
            self.assertEqual(row.parse_status, PARSE_FAILED)
            self.assertEqual(row.parse_attempts, 3)
            self.assertEqual(row.last_error, MAX_ATTEMPTS_ERROR)

    def test_process_once_exhausts_failed_row_only_once_when_already_at_retry_cap(self) -> None:
        with self.session() as session:
            row = self.make_message(
                discord_message_id="failed-at-cap-repeat",
                parse_status=PARSE_FAILED,
                parse_attempts=3,
                last_error="re-queued after stitch group changed",
            )
            session.add(row)
            session.commit()
            session.refresh(row)

            @contextmanager
            def fake_managed_session():
                yield session

            with patch("app.discord.worker.managed_session", new=fake_managed_session):
                asyncio.run(process_once())
                asyncio.run(process_once())

            session.refresh(row)
            self.assertEqual(row.parse_status, PARSE_FAILED)
            self.assertEqual(row.parse_attempts, 3)
            self.assertTrue(row.last_error.startswith(MAX_ATTEMPTS_ERROR))
            self.assertIn("Previous error: re-queued after stitch group changed", row.last_error)

            logs = session.exec(
                select(OperationsLog).where(OperationsLog.event_type == "queue.max_attempts_reached")
            ).all()
            self.assertEqual(len(logs), 1)

    def test_process_once_prioritizes_recent_pending_rows_before_old_backlog(self) -> None:
        now = utcnow()
        with self.session() as session:
            old_backlog = self.make_message(
                discord_message_id="old-backlog",
                content="old backlog buy 10 cash",
                created_at=now - timedelta(days=90),
                parse_status=PARSE_PENDING,
            )
            recent_first = self.make_message(
                discord_message_id="recent-first",
                content="recent first sell 20 cash",
                created_at=now - timedelta(minutes=40),
                parse_status=PARSE_PENDING,
            )
            recent_second = self.make_message(
                discord_message_id="recent-second",
                content="recent second sell 30 cash",
                created_at=now - timedelta(minutes=5),
                parse_status=PARSE_PENDING,
            )
            session.add(old_backlog)
            session.add(recent_first)
            session.add(recent_second)
            session.commit()

            @contextmanager
            def fake_managed_session():
                yield session

            with patch("app.discord.worker.managed_session", new=fake_managed_session), patch(
                "app.discord.worker.process_row", new_callable=AsyncMock
            ) as process_row_mock, patch("app.discord.worker.settings.parser_batch_size", 2):
                asyncio.run(process_once())

            processed_ids = [call.args[0] for call in process_row_mock.await_args_list]
            self.assertEqual(processed_ids, [recent_first.id, recent_second.id])
            self.assertTrue(all(len(call.args) == 2 for call in process_row_mock.await_args_list))

            session.refresh(old_backlog)
            session.refresh(recent_first)
            session.refresh(recent_second)
            self.assertEqual(old_backlog.parse_status, PARSE_PENDING)
            self.assertEqual(recent_first.parse_status, PARSE_PROCESSING)
            self.assertEqual(recent_second.parse_status, PARSE_PROCESSING)
            self.assertEqual(
                [call.args[1] for call in process_row_mock.await_args_list],
                [recent_first.active_parse_attempt_id, recent_second.active_parse_attempt_id],
            )

    def test_atomic_competing_claims_allow_only_one_active_attempt(self) -> None:
        with self.session() as session:
            row = self.make_message(
                discord_message_id="atomic-claim",
                parse_status=PARSE_PENDING,
            )
            session.add(row)
            session.commit()
            row_id = row.id

        with self.session() as first_session:
            first_attempt = claim_message_for_parse(first_session, row_id)
            self.assertIsNotNone(first_attempt)
            first_attempt_id = first_attempt.id
            first_session.commit()

        with self.session() as competing_session:
            competing_attempt = claim_message_for_parse(competing_session, row_id)
            competing_session.commit()
            self.assertIsNone(competing_attempt)

        with self.session() as session:
            row = session.get(DiscordMessage, row_id)
            attempts = session.exec(
                select(ParseAttempt).where(ParseAttempt.message_id == row_id)
            ).all()
            self.assertEqual(row.active_parse_attempt_id, first_attempt_id)
            self.assertEqual(row.parse_status, PARSE_PROCESSING)
            self.assertEqual(row.parse_attempts, 1)
            self.assertEqual([attempt.id for attempt in attempts], [first_attempt_id])

    def test_stale_recovery_cannot_overwrite_finalizer_that_already_cleared_claim(self) -> None:
        import app.discord.worker as worker_module

        with self.session() as session:
            row = self.make_message(
                discord_message_id="recovery-finalizer-race",
                parse_status=PARSE_PROCESSING,
                parse_attempts=1,
            )
            session.add(row)
            session.flush()
            attempt = ParseAttempt(
                message_id=row.id,
                attempt_number=1,
                started_at=utcnow() - timedelta(hours=1),
            )
            session.add(attempt)
            session.flush()
            row.active_parse_attempt_id = attempt.id
            session.add(row)
            session.commit()
            row_id = row.id
            attempt_id = attempt.id

        original_release = worker_module.release_attempt_claims
        finalized_at = utcnow()

        def finalizer_wins_before_recovery_release(session, old_attempt_id, message_ids):
            session.exec(
                update(DiscordMessage)
                .where(DiscordMessage.id == row_id)
                .values(
                    active_parse_attempt_id=None,
                    parse_status=PARSE_PARSED,
                    amount=99,
                )
                .execution_options(synchronize_session=False)
            )
            session.exec(
                update(ParseAttempt)
                .where(ParseAttempt.id == attempt_id)
                .values(success=True, error=None, finished_at=finalized_at)
                .execution_options(synchronize_session=False)
            )
            session.commit()
            return original_release(session, old_attempt_id, message_ids)

        with self.session() as session, patch(
            "app.discord.worker.release_attempt_claims",
            side_effect=finalizer_wins_before_recovery_release,
        ):
            close_or_recover_unfinished_attempts(session)

        with self.session() as session:
            row = session.get(DiscordMessage, row_id)
            attempt = session.get(ParseAttempt, attempt_id)
            self.assertEqual(row.parse_status, PARSE_PARSED)
            self.assertEqual(row.amount, 99)
            self.assertIsNone(row.active_parse_attempt_id)
            self.assertTrue(attempt.success)
            self.assertEqual(
                attempt.finished_at.replace(tzinfo=finalized_at.tzinfo),
                finalized_at,
            )
            self.assertIsNone(attempt.error)

    def test_choose_image_urls_accepts_data_image_urls(self) -> None:
        urls = [
            "data:image/png;base64,ZmFrZS1pbWFnZS1ieXRlcw==",
            "https://example.com/not-an-image.txt",
        ]

        self.assertEqual(
            choose_image_urls(urls, use_first_image_only=True),
            ["data:image/png;base64,ZmFrZS1pbWFnZS1ieXRlcw=="],
        )

    def test_process_row_prefers_cached_image_assets_over_raw_discord_urls(self) -> None:
        raw_url = (
            "https://cdn.discordapp.com/attachments/chan/msg/expired.jpg"
            "?ex=69cad376&is=69c981f6&hm=deadbeef"
        )
        expected_data_url = "data:image/jpeg;base64,Y2FjaGVkLWltYWdlLWJ5dGVz"

        with self.session() as session, patch("app.discord.worker.parse_message") as parse_message_mock, patch(
            "app.discord.worker.recover_attachment_assets_for_message",
            new_callable=AsyncMock,
        ) as recover_mock:
            row = self.make_message(
                discord_message_id="cached-image-row",
                parse_status=PARSE_PENDING,
                parse_attempts=0,
                attachment_urls_json=f'["{raw_url}"]',
            )
            session.add(row)
            session.commit()
            session.refresh(row)

            session.add(
                AttachmentAsset(
                    message_id=row.id,
                    source_url=raw_url,
                    filename="expired.jpg",
                    content_type="image/jpeg",
                    is_image=True,
                    data=b"cached-image-bytes",
                )
            )
            session.commit()

            parse_message_mock.return_value = {
                "parsed_type": "sell",
                "parsed_amount": 25.0,
                "parsed_payment_method": "cash",
                "parsed_cash_direction": "to_store",
                "parsed_category": "singles",
                "parsed_items": ["Charizard"],
                "parsed_items_in": [],
                "parsed_items_out": ["Charizard"],
                "parsed_trade_summary": None,
                "parsed_notes": "cached image parse",
                "image_summary": "cached image used",
                "confidence": 0.95,
                "needs_review": False,
                "ignore_message": False,
            }

            @contextmanager
            def fake_managed_session():
                yield session

            with patch("app.discord.worker.managed_session", new=fake_managed_session):
                asyncio.run(process_row(row.id))

            recover_mock.assert_not_awaited()
            parse_message_mock.assert_awaited_once()
            attachment_urls = parse_message_mock.await_args.kwargs["attachment_urls"]
            self.assertEqual(attachment_urls, [expected_data_url])

    def test_process_row_discards_stale_parse_after_source_edit_then_fresh_parse_revives(self) -> None:
        from app.discord.message_revisions import ensure_message_revision
        from app.discord.transactions import cleanup_transaction_dependents
        from app.models import DiscordMessageRevision

        def parse_result(*, amount: float, item: str) -> dict:
            return {
                "parsed_type": "buy",
                "parsed_amount": amount,
                "parsed_payment_method": "cash",
                "parsed_cash_direction": None,
                "parsed_category": "inventory",
                "parsed_items": [item],
                "parsed_items_in": [item],
                "parsed_items_out": [],
                "parsed_trade_summary": None,
                "parsed_notes": f"parsed {item}",
                "image_summary": None,
                "confidence": 0.95,
                "needs_review": False,
                "ignore_message": False,
            }

        with self.session() as session:
            row = self.make_message(
                discord_message_id="edit-during-parse",
                content="Buy original for 50",
                attachment_urls_json='["https://cdn.discord.com/original.png"]',
                parse_status=PARSE_PARSED,
                deal_type="buy",
                entry_kind="buy",
                amount=50.0,
                money_out=50.0,
            )
            session.add(row)
            session.flush()
            transaction = sync_transaction_from_message(session, row)
            session.commit()
            row_id = row.id
            transaction_id = transaction.id
            original_revision_id = transaction.source_revision_id

            row.parse_status = PARSE_PENDING
            row.parse_attempts = 0
            session.add(row)
            session.commit()
            original_attempt = ParseAttempt(
                message_id=row_id,
                attempt_number=1,
                started_at=utcnow(),
            )
            session.add(original_attempt)
            session.commit()
            original_attempt_id = original_attempt.id

        newer_attempt_id: dict[str, int] = {}

        async def edit_while_old_parse_is_in_flight(**_kwargs):
            with self.session() as edit_session:
                edited_row = edit_session.get(DiscordMessage, row_id)
                edited_transaction = edit_session.get(Transaction, transaction_id)
                edited_row.content = "Buy edited for 75"
                edited_row.attachment_urls_json = (
                    '["https://cdn.discord.com/edited.png"]'
                )
                edited_row.edited_at = utcnow()
                edited_row.parse_status = PARSE_PENDING
                edited_row.parse_attempts = 0
                edited_row.last_error = None
                edited_revision = ensure_message_revision(edit_session, edited_row)
                cleanup_transaction_dependents(
                    edit_session,
                    edited_transaction,
                    bank_unmatch_reason="Edited while worker parse was in flight.",
                )
                edited_transaction.is_deleted = True
                edited_transaction.needs_review = True
                edited_transaction.parse_status = PARSE_PENDING
                newer_attempt = ParseAttempt(
                    message_id=row_id,
                    attempt_number=2,
                    started_at=utcnow(),
                )
                edit_session.add(edited_row)
                edit_session.add(edited_transaction)
                edit_session.add(newer_attempt)
                edit_session.commit()
                self.assertEqual(edited_revision.revision_number, 2)
                newer_attempt_id["id"] = newer_attempt.id
            return parse_result(amount=50.0, item="Original Card")

        with self.session() as worker_session:
            @contextmanager
            def stale_worker_managed_session():
                yield worker_session

            with patch(
                "app.discord.worker.managed_session",
                new=stale_worker_managed_session,
            ), patch(
                "app.discord.worker.parse_message",
                new=AsyncMock(side_effect=edit_while_old_parse_is_in_flight),
            ):
                asyncio.run(process_row(row_id))

        with self.session() as session:
            row_after_discard = session.get(DiscordMessage, row_id)
            transaction_after_discard = session.get(Transaction, transaction_id)
            revisions_after_discard = list(
                session.exec(
                    select(DiscordMessageRevision).order_by(
                        DiscordMessageRevision.revision_number
                    )
                ).all()
            )
            discard_log = session.exec(
                select(OperationsLog).where(
                    OperationsLog.event_type == "queue.parse_discarded_source_changed"
                )
            ).first()
            original_attempt = session.get(ParseAttempt, original_attempt_id)
            newer_attempt = session.get(ParseAttempt, newer_attempt_id["id"])

            self.assertEqual(row_after_discard.content, "Buy edited for 75")
            self.assertEqual(row_after_discard.parse_status, PARSE_PENDING)
            self.assertEqual(
                row_after_discard.current_revision_id,
                revisions_after_discard[1].id,
            )
            self.assertEqual(
                [revision.revision_number for revision in revisions_after_discard],
                [1, 2],
            )
            self.assertTrue(transaction_after_discard.is_deleted)
            self.assertEqual(
                transaction_after_discard.source_revision_id,
                original_revision_id,
            )
            self.assertEqual(
                transaction_after_discard.source_content,
                "Buy original for 50",
            )
            self.assertIsNotNone(discard_log)
            self.assertFalse(original_attempt.success)
            self.assertIsNotNone(original_attempt.finished_at)
            self.assertIn("changed while parsing", original_attempt.error or "")
            self.assertFalse(newer_attempt.success)
            self.assertIsNone(newer_attempt.finished_at)
            self.assertIsNone(newer_attempt.error)

        with self.session() as fresh_worker_session:
            @contextmanager
            def fresh_worker_managed_session():
                yield fresh_worker_session

            with patch(
                "app.discord.worker.managed_session",
                new=fresh_worker_managed_session,
            ), patch(
                "app.discord.worker.parse_message",
                new=AsyncMock(
                    return_value=parse_result(amount=75.0, item="Edited Card")
                ),
            ):
                asyncio.run(process_row(row_id))

        with self.session() as session:
            final_row = session.get(DiscordMessage, row_id)
            final_transaction = session.get(Transaction, transaction_id)
            final_revisions = list(
                session.exec(
                    select(DiscordMessageRevision).order_by(
                        DiscordMessageRevision.revision_number
                    )
                ).all()
            )
            reportable_ids = {transaction.id for transaction in get_transactions(session)}

        self.assertEqual([revision.revision_number for revision in final_revisions], [1, 2])
        self.assertEqual(final_row.current_revision_id, final_revisions[1].id)
        self.assertEqual(final_row.parse_status, PARSE_PARSED)
        self.assertEqual(final_transaction.id, transaction_id)
        self.assertFalse(final_transaction.is_deleted)
        self.assertEqual(final_transaction.source_revision_id, final_revisions[1].id)
        self.assertEqual(final_transaction.source_content, "Buy edited for 75")
        self.assertIn(transaction_id, reportable_ids)

    def test_queue_reparse_range_includes_legacy_needs_review_alias(self) -> None:
        with self.session() as session:
            row = self.make_message(
                discord_message_id="legacy-needs-review",
                parse_status="needs_review",
                needs_review=False,
                parse_attempts=2,
            )
            session.add(row)
            session.commit()

            result = queue_reparse_range(
                session,
                start=utcnow() - timedelta(days=1),
                end=utcnow() + timedelta(days=1),
                include_statuses=[PARSE_REVIEW_REQUIRED],
                include_reviewed=False,
                reason="validation reparse",
            )

            self.assertEqual(result["matched"], 1)
            self.assertEqual(result["queued"], 1)
            session.refresh(row)
            self.assertEqual(row.parse_status, PARSE_PENDING)

    def test_queue_reparse_range_includes_legacy_deleted_alias(self) -> None:
        with self.session() as session:
            row = self.make_message(
                discord_message_id="legacy-deleted",
                parse_status="deleted",
                needs_review=False,
                is_deleted=False,
                parse_attempts=1,
            )
            session.add(row)
            session.commit()

            result = queue_reparse_range(
                session,
                start=utcnow() - timedelta(days=1),
                end=utcnow() + timedelta(days=1),
                include_statuses=[PARSE_IGNORED],
                include_reviewed=False,
                reason="validation reparse",
            )

            self.assertEqual(result["matched"], 1)
            self.assertEqual(result["queued"], 1)
            session.refresh(row)
            self.assertEqual(row.parse_status, PARSE_PENDING)

    def test_queue_reparse_range_includes_legacy_queued_alias(self) -> None:
        with self.session() as session:
            row = self.make_message(
                discord_message_id="legacy-queued",
                parse_status="queued",
                needs_review=False,
                parse_attempts=1,
            )
            session.add(row)
            session.commit()

            result = queue_reparse_range(
                session,
                start=utcnow() - timedelta(days=1),
                end=utcnow() + timedelta(days=1),
                include_statuses=[PARSE_PENDING],
                include_reviewed=False,
                reason="validation reparse",
            )

            self.assertEqual(result["matched"], 1)
            self.assertEqual(result["already_queued"], 1)
            session.refresh(row)
            self.assertEqual(row.parse_status, PARSE_PENDING)

    def test_queue_reparse_range_queues_all_rows_across_mutating_batches(self) -> None:
        with self.session() as session:
            base_time = utcnow()
            rows = [
                self.make_message(
                    discord_message_id=f"stable-range-{index}",
                    created_at=base_time + timedelta(microseconds=index),
                    parse_status=PARSE_PARSED,
                )
                for index in range(600)
            ]
            session.add_all(rows)
            session.commit()

            result = queue_reparse_range(
                session,
                include_statuses=[PARSE_PARSED],
                include_reviewed=False,
                reason="stable range regression",
            )

            queued_rows = session.exec(
                select(DiscordMessage).where(DiscordMessage.parse_status == PARSE_PENDING)
            ).all()
            self.assertEqual(result["matched"], 600)
            self.assertEqual(result["queued"], 600)
            self.assertEqual(len(queued_rows), 600)

    def test_queue_reparse_range_excludes_rows_inserted_after_snapshot(self) -> None:
        with self.session() as session:
            base_time = utcnow()
            rows = [
                self.make_message(
                    discord_message_id=f"snapshot-range-{index}",
                    created_at=base_time + timedelta(microseconds=index),
                    parse_status=PARSE_PARSED,
                )
                for index in range(501)
            ]
            session.add_all(rows)
            session.commit()

            original_commit = session.commit
            inserted_id: list[int] = []

            def commit_then_insert_new_match() -> None:
                original_commit()
                if inserted_id:
                    return
                with self.session() as concurrent_session:
                    inserted = self.make_message(
                        discord_message_id="snapshot-range-late-arrival",
                        # Backdated into the already-selected range so only the
                        # insertion snapshot (not the time upper bound) excludes it.
                        created_at=base_time + timedelta(microseconds=499),
                        parse_status=PARSE_PARSED,
                    )
                    concurrent_session.add(inserted)
                    concurrent_session.commit()
                    inserted_id.append(inserted.id)

            with patch.object(session, "commit", side_effect=commit_then_insert_new_match):
                result = queue_reparse_range(
                    session,
                    include_statuses=[PARSE_PARSED],
                    reason="snapshot range regression",
                )

            late_arrival = session.get(DiscordMessage, inserted_id[0])
            self.assertEqual(result["matched"], 501)
            self.assertEqual(result["queued"], 501)
            self.assertEqual(late_arrival.parse_status, PARSE_PARSED)

    def test_queue_reparse_range_excludes_backfill_between_snapshot_boundary_reads(self) -> None:
        with self.session() as session:
            base_time = utcnow()
            rows = [
                self.make_message(
                    discord_message_id=f"boundary-range-{index}",
                    created_at=base_time + timedelta(microseconds=index),
                    parse_status=PARSE_PARSED,
                )
                for index in range(10)
            ]
            session.add_all(rows)
            session.commit()

            original_exec = session.exec
            exec_count = 0
            inserted_id: list[int] = []

            def exec_then_insert_historical_match(statement, *args, **kwargs):
                nonlocal exec_count
                result = original_exec(statement, *args, **kwargs)
                exec_count += 1
                if exec_count == 1:
                    inserted = self.make_message(
                        discord_message_id="boundary-range-late-backfill",
                        created_at=base_time + timedelta(microseconds=5),
                        parse_status=PARSE_PARSED,
                    )
                    session.add(inserted)
                    session.flush()
                    inserted_id.append(inserted.id)
                return result

            with patch.object(session, "exec", side_effect=exec_then_insert_historical_match):
                result = queue_reparse_range(
                    session,
                    include_statuses=[PARSE_PARSED],
                    include_reviewed=True,
                    reason="snapshot boundary regression",
                )

            late_backfill = session.get(DiscordMessage, inserted_id[0])
            self.assertEqual(result["matched"], 10)
            self.assertEqual(result["queued"], 10)
            self.assertEqual(late_backfill.parse_status, PARSE_PARSED)

    def test_queue_reparse_range_freezes_membership_before_late_row_becomes_eligible(self) -> None:
        with self.session() as session:
            base_time = utcnow()
            late_eligible = self.make_message(
                discord_message_id="range-late-eligible",
                created_at=base_time - timedelta(seconds=1),
                parse_status=PARSE_PENDING,
            )
            initial_rows = [
                self.make_message(
                    discord_message_id=f"range-initial-member-{index}",
                    created_at=base_time + timedelta(microseconds=index),
                    parse_status=PARSE_PARSED,
                )
                for index in range(10)
            ]
            session.add_all([late_eligible, *initial_rows])
            session.commit()
            late_eligible_id = late_eligible.id

            original_exec = session.exec
            exec_count = 0

            def exec_then_make_older_row_eligible(statement, *args, **kwargs):
                nonlocal exec_count
                result = original_exec(statement, *args, **kwargs)
                exec_count += 1
                if exec_count == 1:
                    original_exec(
                        update(DiscordMessage)
                        .where(DiscordMessage.id == late_eligible_id)
                        .values(parse_status=PARSE_PARSED)
                        .execution_options(synchronize_session=False)
                    )
                return result

            with patch.object(session, "exec", side_effect=exec_then_make_older_row_eligible):
                result = queue_reparse_range(
                    session,
                    include_statuses=[PARSE_PARSED],
                    include_reviewed=True,
                    reason="frozen membership regression",
                )

            session.expire_all()
            late_eligible_after = session.get(DiscordMessage, late_eligible_id)
            self.assertEqual(result["matched"], 10)
            self.assertEqual(result["queued"], 10)
            self.assertEqual(late_eligible_after.parse_status, PARSE_PARSED)
            self.assertEqual(late_eligible_after.parse_attempts, 0)
            self.assertIsNone(late_eligible_after.last_error)

    def test_queue_reparse_range_does_not_overwrite_selected_row_that_changes_status(self) -> None:
        with self.session() as session:
            base_time = utcnow()
            rows = [
                self.make_message(
                    discord_message_id=f"range-status-race-{index}",
                    created_at=base_time + timedelta(microseconds=index),
                    parse_status=PARSE_PARSED,
                )
                for index in range(10)
            ]
            session.add_all(rows)
            session.commit()
            changed_id = rows[0].id

            original_exec = session.exec
            exec_count = 0

            def exec_then_change_selected_status(statement, *args, **kwargs):
                nonlocal exec_count
                result = original_exec(statement, *args, **kwargs)
                exec_count += 1
                if exec_count == 1:
                    original_exec(
                        update(DiscordMessage)
                        .where(DiscordMessage.id == changed_id)
                        .values(
                            parse_status=PARSE_PENDING,
                            last_error="concurrent queue owner",
                        )
                        .execution_options(synchronize_session=False)
                    )
                return result

            with patch.object(session, "exec", side_effect=exec_then_change_selected_status):
                result = queue_reparse_range(
                    session,
                    include_statuses=[PARSE_PARSED],
                    include_reviewed=True,
                    reason="do not overwrite concurrent status",
                )

            session.expire_all()
            changed_after = session.get(DiscordMessage, changed_id)
            self.assertEqual(result["matched"], 10)
            self.assertEqual(result["queued"], 9)
            self.assertEqual(result["skipped_changed"], 1)
            self.assertEqual(changed_after.parse_status, PARSE_PENDING)
            self.assertEqual(changed_after.last_error, "concurrent queue owner")

    def test_queue_reparse_range_cas_rejects_status_change_after_fresh_read(self) -> None:
        from app.discord import worker as worker_module

        with self.session() as session:
            row = self.make_message(
                discord_message_id="range-cas-status-race",
                parse_status=PARSE_PARSED,
            )
            session.add(row)
            session.commit()
            row_id = row.id
            original_cas = worker_module.cas_range_reparse_selection
            injected = False

            def change_status_then_cas(cas_session, snapshot):
                nonlocal injected
                if not injected:
                    injected = True
                    cas_session.exec(
                        update(DiscordMessage)
                        .where(DiscordMessage.id == row_id)
                        .values(
                            parse_status=PARSE_PENDING,
                            last_error="concurrent status after fresh read",
                        )
                        .execution_options(synchronize_session=False)
                    )
                    cas_session.commit()
                return original_cas(cas_session, snapshot)

            with patch(
                "app.discord.worker.cas_range_reparse_selection",
                side_effect=change_status_then_cas,
            ):
                result = queue_reparse_range(
                    session,
                    include_statuses=[PARSE_PARSED],
                    include_reviewed=True,
                    reason="post-read status CAS regression",
                )

            session.expire_all()
            row_after = session.get(DiscordMessage, row_id)
            self.assertEqual(result["matched"], 1)
            self.assertEqual(result["queued"], 0)
            self.assertEqual(result["skipped_changed"], 1)
            self.assertEqual(row_after.parse_status, PARSE_PENDING)
            self.assertEqual(row_after.last_error, "concurrent status after fresh read")

    def test_queue_reparse_range_cas_rejects_review_change_after_fresh_read(self) -> None:
        from app.discord import worker as worker_module

        with self.session() as session:
            row = self.make_message(
                discord_message_id="range-cas-review-race",
                parse_status=PARSE_PARSED,
            )
            session.add(row)
            session.commit()
            row_id = row.id
            reviewed_at = utcnow()
            original_cas = worker_module.cas_range_reparse_selection
            injected = False

            def change_review_then_cas(cas_session, snapshot):
                nonlocal injected
                if not injected:
                    injected = True
                    cas_session.exec(
                        update(DiscordMessage)
                        .where(DiscordMessage.id == row_id)
                        .values(
                            reviewed_by="concurrent-reviewer",
                            reviewed_at=reviewed_at,
                        )
                        .execution_options(synchronize_session=False)
                    )
                    cas_session.commit()
                return original_cas(cas_session, snapshot)

            with patch(
                "app.discord.worker.cas_range_reparse_selection",
                side_effect=change_review_then_cas,
            ):
                result = queue_reparse_range(
                    session,
                    include_statuses=[PARSE_PARSED],
                    include_reviewed=True,
                    reason="post-read review CAS regression",
                )

            session.expire_all()
            row_after = session.get(DiscordMessage, row_id)
            self.assertEqual(result["matched"], 1)
            self.assertEqual(result["queued"], 0)
            self.assertEqual(result["skipped_changed"], 1)
            self.assertEqual(row_after.parse_status, PARSE_PARSED)
            self.assertEqual(row_after.reviewed_by, "concurrent-reviewer")
            self.assertEqual(
                row_after.reviewed_at.replace(tzinfo=reviewed_at.tzinfo),
                reviewed_at,
            )

    def test_queue_reparse_range_fails_before_mutation_when_selection_exceeds_cap(self) -> None:
        with self.session() as session:
            rows = [
                self.make_message(
                    discord_message_id=f"range-selection-cap-{index}",
                    parse_status=PARSE_PARSED,
                )
                for index in range(4)
            ]
            session.add_all(rows)
            session.commit()
            row_ids = [row.id for row in rows]

            with patch("app.discord.worker.MAX_RANGE_REPARSE_SELECTION", 3):
                with self.assertRaises(RangeReparseSelectionLimitError) as exc:
                    queue_reparse_range(
                        session,
                        include_statuses=[PARSE_PARSED],
                        include_reviewed=True,
                        reason="selection cap regression",
                    )

            self.assertIn("more than 3 messages", str(exc.exception))
            self.assertIn("No messages were changed", str(exc.exception))
            session.expire_all()
            rows_after = session.exec(
                select(DiscordMessage)
                .where(DiscordMessage.id.in_(row_ids))
                .order_by(DiscordMessage.id)
            ).all()
            self.assertTrue(all(row.parse_status == PARSE_PARSED for row in rows_after))
            self.assertTrue(all(row.last_error is None for row in rows_after))

    def test_admin_range_reparse_returns_clear_client_error_when_selection_exceeds_cap(self) -> None:
        with self.session() as session:
            session.add_all(
                [
                    self.make_message(
                        discord_message_id=f"admin-range-selection-cap-{index}",
                        parse_status=PARSE_PARSED,
                    )
                    for index in range(4)
                ]
            )
            session.commit()

            with patch("app.routers.admin_actions.require_role_response", return_value=None), patch(
                "app.routers.admin_actions.safe_create_reparse_run",
                return_value="test-run",
            ), patch(
                "app.routers.admin_actions.safe_finalize_reparse_run_queue",
            ) as finalize_mock, patch("app.discord.worker.MAX_RANGE_REPARSE_SELECTION", 3):
                with self.assertRaises(HTTPException) as exc:
                    admin_parser_reparse_range(
                        make_request("/admin/parser/reparse-range"),
                        after="2020-01-01",
                        before="2030-01-01",
                        channel_id=None,
                        include_failed=None,
                        include_ignored=None,
                        include_reviewed=None,
                        force_reviewed=None,
                        session=session,
                    )

            self.assertEqual(exc.exception.status_code, 400)
            self.assertIn("narrow the date/channel/status filters", str(exc.exception.detail))
            self.assertIn("No messages were changed", str(exc.exception.detail))
            finalize_mock.assert_called_once_with(
                run_id="test-run",
                selected_count=0,
                queued_count=0,
                already_queued_count=0,
                skipped_reviewed_count=0,
                first_message_id=None,
                last_message_id=None,
                first_message_created_at=None,
                last_message_created_at=None,
            )

    def test_queue_reparse_range_tombstones_all_transactions_for_ignored_child(self) -> None:
        with self.session() as session:
            base_time = utcnow()
            primary = self.make_message(
                discord_message_id="range-primary",
                content="Photo",
                created_at=base_time,
                parse_status=PARSE_PARSED,
            )
            child = self.make_message(
                discord_message_id="range-child",
                content="Buy 50 cash",
                created_at=base_time + timedelta(seconds=1),
                parse_status=PARSE_PARSED,
            )
            session.add_all([primary, child])
            session.flush()
            child_transaction = sync_transaction_from_message(session, child)

            membership = json.dumps([primary.id, child.id])
            primary.stitched_group_id = "range-group"
            primary.stitched_primary = True
            primary.stitched_message_ids_json = membership
            child.stitched_group_id = "range-group"
            child.stitched_primary = False
            child.stitched_message_ids_json = membership
            child.parse_status = PARSE_IGNORED
            ensure_message_revision(session, primary)
            ensure_message_revision(session, child)
            primary_transaction = sync_transaction_from_message(
                session,
                primary,
                source_rows=[primary, child],
                source_content="Message 1: Photo\n\nMessage 2: Buy 50 cash",
            )
            session.add_all(
                [
                    TransactionItem(
                        transaction_id=child_transaction.id,
                        direction="named",
                        item_name="Child evidence",
                    ),
                    TransactionItem(
                        transaction_id=primary_transaction.id,
                        direction="named",
                        item_name="Primary evidence",
                    ),
                ]
            )
            session.commit()

            transaction_ids = [child_transaction.id, primary_transaction.id]
            associations_before = session.exec(
                select(TransactionSourceRevision)
                .where(TransactionSourceRevision.transaction_id.in_(transaction_ids))
                .order_by(TransactionSourceRevision.id)
            ).all()
            association_snapshot = [
                (
                    association.id,
                    association.transaction_id,
                    association.message_id,
                    association.revision_id,
                    association.source_position,
                )
                for association in associations_before
            ]
            source_content_before = {
                transaction.id: transaction.source_content
                for transaction in session.exec(
                    select(Transaction).where(Transaction.id.in_(transaction_ids))
                ).all()
            }

            result = queue_reparse_range(
                session,
                include_statuses=[PARSE_IGNORED],
                include_reviewed=False,
                reason="ignored child range regression",
            )

            session.expire_all()
            child_after = session.get(DiscordMessage, child.id)
            transactions_after = session.exec(
                select(Transaction).where(Transaction.id.in_(transaction_ids))
            ).all()
            associations_after = session.exec(
                select(TransactionSourceRevision)
                .where(TransactionSourceRevision.transaction_id.in_(transaction_ids))
                .order_by(TransactionSourceRevision.id)
            ).all()
            items_after = session.exec(
                select(TransactionItem).where(TransactionItem.transaction_id.in_(transaction_ids))
            ).all()

            self.assertEqual(result["queued"], 1)
            self.assertEqual(child_after.parse_status, PARSE_PENDING)
            self.assertEqual({transaction.id for transaction in transactions_after}, set(transaction_ids))
            self.assertTrue(all(transaction.is_deleted for transaction in transactions_after))
            self.assertEqual(
                {transaction.id: transaction.source_content for transaction in transactions_after},
                source_content_before,
            )
            self.assertEqual(
                [
                    (
                        association.id,
                        association.transaction_id,
                        association.message_id,
                        association.revision_id,
                        association.source_position,
                    )
                    for association in associations_after
                ],
                association_snapshot,
            )
            self.assertEqual(items_after, [])

    def test_queue_reparse_range_tombstones_live_transaction_for_already_pending_row(self) -> None:
        with self.session() as session:
            row = self.make_message(
                discord_message_id="already-pending-with-live-transaction",
                parse_status=PARSE_PENDING,
                amount=19.0,
                entry_kind="sale",
                money_in=19.0,
            )
            session.add(row)
            session.flush()
            transaction = Transaction(
                source_message_id=row.id,
                occurred_at=row.created_at,
                parse_status=PARSE_PARSED,
                entry_kind="sale",
                payment_method="cash",
                expense_category="inventory",
                amount=19.0,
                money_in=19.0,
                money_out=0.0,
                source_content=row.content,
                is_deleted=False,
            )
            session.add(transaction)
            session.commit()

            result = queue_reparse_range(
                session,
                include_statuses=[PARSE_PENDING],
                reason="pending cleanup regression",
            )

            session.refresh(row)
            session.refresh(transaction)
            self.assertEqual(result["matched"], 1)
            self.assertEqual(result["already_queued"], 1)
            self.assertEqual(row.parse_status, PARSE_PENDING)
            self.assertTrue(transaction.is_deleted)

    def test_queue_reparse_range_never_resets_quarantined_row(self) -> None:
        with self.session() as session:
            row = self.make_message(
                discord_message_id="quarantined-range-row",
                parse_status=PARSE_FAILED,
                parse_attempts=4,
                last_error="canonical Discord refresh required after raw edit fetch failure: unavailable",
                source_refresh_required=True,
            )
            session.add(row)
            session.commit()
            before = (row.parse_status, row.parse_attempts, row.last_error)

            result = queue_reparse_range(session, reason="must not clear quarantine")

            session.refresh(row)
            self.assertEqual(result["matched"], 1)
            self.assertEqual(result["queued"], 0)
            self.assertEqual((row.parse_status, row.parse_attempts, row.last_error), before)
            self.assertTrue(row.source_refresh_required)

    def test_queue_reparse_range_quarantines_integrity_conflict_and_hides_all_bound_transactions(self) -> None:
        with self.session() as session:
            base_time = utcnow()
            conflict = self.make_message(
                discord_message_id="range-oversized-conflict",
                created_at=base_time,
                parse_status=PARSE_IGNORED,
                content="shared ignored evidence",
            )
            primary_rows = [
                self.make_message(
                    discord_message_id=f"range-conflict-primary-{index}",
                    created_at=base_time + timedelta(seconds=index + 1),
                    parse_status=PARSE_PARSED,
                    amount=30.0,
                    entry_kind="sale",
                    money_in=30.0,
                )
                for index in range(33)
            ]
            session.add_all([conflict, *primary_rows])
            session.flush()
            conflict_revision = ensure_message_revision(session, conflict)
            conflict_transactions = []
            for primary in primary_rows:
                transaction = sync_transaction_from_message(session, primary)
                session.add(
                    TransactionSourceRevision(
                        transaction_id=transaction.id,
                        message_id=conflict.id,
                        revision_id=conflict_revision.id,
                        source_position=1,
                    )
                )
                conflict_transactions.append(transaction)
            session.commit()
            conflict_id = conflict.id
            conflict_transaction_ids = [transaction.id for transaction in conflict_transactions]
            transaction_snapshot = [
                (
                    transaction.id,
                    transaction.is_deleted,
                    transaction.parse_status,
                    transaction.source_content,
                    transaction.source_revision_id,
                )
                for transaction in session.exec(
                    select(Transaction)
                    .where(Transaction.id.in_(conflict_transaction_ids))
                    .order_by(Transaction.id)
                ).all()
            ]
            association_snapshot = [
                (
                    association.id,
                    association.transaction_id,
                    association.message_id,
                    association.revision_id,
                    association.source_position,
                )
                for association in session.exec(
                    select(TransactionSourceRevision)
                    .where(TransactionSourceRevision.transaction_id.in_(conflict_transaction_ids))
                    .order_by(TransactionSourceRevision.id)
                ).all()
            ]
            self.assertEqual(len(session.exec(transaction_base_query()).all()), 33)

            result = queue_reparse_range(
                session,
                include_statuses=[PARSE_IGNORED],
                reason="integrity conflict regression",
            )

            session.expire_all()
            conflict_after = session.get(DiscordMessage, conflict_id)
            conflict_transactions_after = session.exec(
                select(Transaction)
                .where(Transaction.id.in_(conflict_transaction_ids))
                .order_by(Transaction.id)
            ).all()
            associations_after = session.exec(
                select(TransactionSourceRevision)
                .where(TransactionSourceRevision.transaction_id.in_(conflict_transaction_ids))
                .order_by(TransactionSourceRevision.id)
            ).all()
            self.assertEqual(result["matched"], 1)
            self.assertEqual(result["queued"], 0)
            self.assertEqual(result["skipped_integrity"], 1)
            self.assertEqual(conflict_after.parse_status, PARSE_FAILED)
            self.assertTrue(conflict_after.needs_review)
            self.assertTrue(conflict_after.source_refresh_required)
            self.assertIsNone(conflict_after.active_reparse_run_id)
            self.assertEqual(
                conflict_after.last_error,
                f"{DISCORD_SOURCE_REFRESH_REQUIRED_ERROR}: range reparse integrity blocked: "
                "transaction association fanout exceeds the safe invalidation bound",
            )
            self.assertEqual(len(conflict_transactions_after), 33)
            self.assertEqual(
                [
                    (
                        transaction.id,
                        transaction.is_deleted,
                        transaction.parse_status,
                        transaction.source_content,
                        transaction.source_revision_id,
                    )
                    for transaction in conflict_transactions_after
                ],
                transaction_snapshot,
            )
            self.assertEqual(
                [
                    (
                        association.id,
                        association.transaction_id,
                        association.message_id,
                        association.revision_id,
                        association.source_position,
                    )
                    for association in associations_after
                ],
                association_snapshot,
            )
            self.assertEqual(session.exec(transaction_base_query()).all(), [])

            with patch("app.routers.messages.require_role_response", return_value=None):
                with self.assertRaises(HTTPException) as exc:
                    reparse_message_form(
                        make_request(f"/messages/{conflict_id}/retry-form"),
                        message_id=conflict_id,
                        session=session,
                    )
            self.assertEqual(exc.exception.status_code, 409)

        @contextmanager
        def managed_test_session():
            with self.session() as managed:
                yield managed

        canonical_message = types.SimpleNamespace(
            id="range-oversized-conflict",
            content="shared ignored evidence",
            attachments=[],
            guild=None,
            channel=types.SimpleNamespace(id=1, name="chan-1"),
            author=types.SimpleNamespace(id=123),
            edited_at=None,
        )
        with patch("app.discord.discord_ingest.managed_session", managed_test_session), patch(
            "app.discord.discord_ingest.transaction_service.invalidate_transactions_for_message",
            side_effect=StaleSourceRevisionError("invalidation still blocked"),
        ):
            from app.discord.discord_ingest import insert_or_update_message

            refresh_result = insert_or_update_message(
                canonical_message,
                canonical_source_refresh=True,
            )
            self.assertEqual(refresh_result, (False, "refresh_required"))

        with self.session() as session:
            conflict_after_failed_refresh = session.get(DiscordMessage, conflict_id)
            transactions_after_failed_refresh = session.exec(
                select(Transaction)
                .where(Transaction.id.in_(conflict_transaction_ids))
                .order_by(Transaction.id)
            ).all()
            associations_after_failed_refresh = session.exec(
                select(TransactionSourceRevision)
                .where(
                    TransactionSourceRevision.transaction_id.in_(
                        conflict_transaction_ids
                    )
                )
                .order_by(TransactionSourceRevision.id)
            ).all()
            self.assertEqual(conflict_after_failed_refresh.parse_status, PARSE_FAILED)
            self.assertTrue(conflict_after_failed_refresh.needs_review)
            self.assertTrue(conflict_after_failed_refresh.source_refresh_required)
            self.assertIn(DISCORD_SOURCE_REFRESH_REQUIRED_ERROR, conflict_after_failed_refresh.last_error or "")
            self.assertEqual(
                [
                    (
                        transaction.id,
                        transaction.is_deleted,
                        transaction.parse_status,
                        transaction.source_content,
                        transaction.source_revision_id,
                    )
                    for transaction in transactions_after_failed_refresh
                ],
                transaction_snapshot,
            )
            self.assertEqual(
                [
                    (
                        association.id,
                        association.transaction_id,
                        association.message_id,
                        association.revision_id,
                        association.source_position,
                    )
                    for association in associations_after_failed_refresh
                ],
                association_snapshot,
            )
            self.assertEqual(session.exec(transaction_base_query()).all(), [])

    def test_admin_range_reparse_blocks_reviewed_rows_without_force_confirmation(self) -> None:
        with self.session() as session, patch("app.routers.admin_actions.require_role_response", return_value=None):
            with self.assertRaises(HTTPException) as exc:
                admin_parser_reparse_range(
                    make_request("/admin/parser/reparse-range"),
                    after="2026-03-01",
                    before="2026-03-31",
                    include_reviewed="true",
                    force_reviewed=None,
                    session=session,
                )

        self.assertEqual(exc.exception.status_code, 400)
        self.assertIn("force_reviewed", exc.exception.detail)

    def test_admin_range_false_strings_do_not_include_reviewed_or_optional_statuses(self) -> None:
        with self.session() as session:
            reviewed_at = utcnow()
            reviewed = self.make_message(
                discord_message_id="api-false-flags-reviewed",
                parse_status=PARSE_PARSED,
                reviewed_by="reviewer",
                reviewed_at=reviewed_at,
            )
            session.add(reviewed)
            session.commit()
            reviewed_id = reviewed.id

            with patch("app.routers.admin_actions.require_role_response", return_value=None), patch(
                "app.routers.admin_actions.safe_create_reparse_run",
                return_value="false-flags-run",
            ), patch("app.routers.admin_actions.safe_finalize_reparse_run_queue"):
                response = admin_parser_reparse_range(
                    make_request("/admin/parser/reparse-range"),
                    after="2020-01-01",
                    before="2030-01-01",
                    channel_id=None,
                    include_failed="false",
                    include_ignored="0",
                    include_reviewed="off",
                    force_reviewed="no",
                    session=session,
                )

            session.expire_all()
            reviewed_after = session.get(DiscordMessage, reviewed_id)
            self.assertEqual(response["matched"], 0)
            self.assertEqual(response["queued"], 0)
            self.assertEqual(response["skipped_reviewed"], 1)
            self.assertEqual(response["skipped_quarantined"], 0)
            self.assertEqual(response["skipped_integrity"], 0)
            self.assertEqual(response["skipped_changed"], 0)
            self.assertFalse(response["include_reviewed"])
            self.assertNotIn(PARSE_FAILED, response["included_statuses"])
            self.assertNotIn(PARSE_IGNORED, response["included_statuses"])
            self.assertEqual(reviewed_after.parse_status, PARSE_PARSED)
            self.assertEqual(reviewed_after.reviewed_by, "reviewer")
            self.assertIsNotNone(reviewed_after.reviewed_at)

    def test_admin_range_true_include_requires_explicit_true_force(self) -> None:
        with self.session() as session:
            reviewed = self.make_message(
                discord_message_id="api-false-force-reviewed",
                parse_status=PARSE_PARSED,
                reviewed_by="reviewer",
                reviewed_at=utcnow(),
            )
            session.add(reviewed)
            session.commit()
            reviewed_id = reviewed.id

            with patch("app.routers.admin_actions.require_role_response", return_value=None), patch(
                "app.routers.admin_actions.safe_create_reparse_run",
            ) as create_mock:
                with self.assertRaises(HTTPException) as exc:
                    admin_parser_reparse_range(
                        make_request("/admin/parser/reparse-range"),
                        after="2020-01-01",
                        before="2030-01-01",
                        channel_id=None,
                        include_failed="",
                        include_ignored="false",
                        include_reviewed="true",
                        force_reviewed="false",
                        session=session,
                    )

            self.assertEqual(exc.exception.status_code, 400)
            self.assertIn("force_reviewed", str(exc.exception.detail))
            create_mock.assert_not_called()
            session.expire_all()
            self.assertEqual(session.get(DiscordMessage, reviewed_id).parse_status, PARSE_PARSED)

    def test_admin_range_form_false_strings_leave_reviewed_row_unchanged(self) -> None:
        with self.session() as session:
            reviewed = self.make_message(
                discord_message_id="form-false-flags-reviewed",
                parse_status=PARSE_PARSED,
                reviewed_by="reviewer",
                reviewed_at=utcnow(),
            )
            session.add(reviewed)
            session.commit()
            reviewed_id = reviewed.id

            with patch("app.routers.admin_actions.require_role_response", return_value=None), patch(
                "app.routers.admin_actions.safe_create_reparse_run",
                return_value="form-false-flags-run",
            ), patch("app.routers.admin_actions.safe_finalize_reparse_run_queue"):
                response = admin_parser_reparse_range_form(
                    make_request("/admin/parser/reparse-range-form"),
                    return_path="/table",
                    after="2020-01-01",
                    before="2030-01-01",
                    channel_id=None,
                    include_failed="no",
                    include_ignored="false",
                    include_reviewed="0",
                    force_reviewed="off",
                    session=session,
                )

            self.assertEqual(response.status_code, 303)
            self.assertIn("skipped+reviewed%3A+1", response.headers["location"])
            session.expire_all()
            reviewed_after = session.get(DiscordMessage, reviewed_id)
            self.assertEqual(reviewed_after.parse_status, PARSE_PARSED)
            self.assertEqual(reviewed_after.reviewed_by, "reviewer")

    def test_admin_range_form_true_include_rejects_false_force(self) -> None:
        with self.session() as session:
            reviewed = self.make_message(
                discord_message_id="form-false-force-reviewed",
                parse_status=PARSE_PARSED,
                reviewed_by="reviewer",
                reviewed_at=utcnow(),
            )
            session.add(reviewed)
            session.commit()
            reviewed_id = reviewed.id

            with patch("app.routers.admin_actions.require_role_response", return_value=None), patch(
                "app.routers.admin_actions.safe_create_reparse_run",
            ) as create_mock:
                response = admin_parser_reparse_range_form(
                    make_request("/admin/parser/reparse-range-form"),
                    return_path="/table",
                    after="2020-01-01",
                    before="2030-01-01",
                    channel_id=None,
                    include_failed="false",
                    include_ignored="false",
                    include_reviewed="yes",
                    force_reviewed="0",
                    session=session,
                )

            self.assertEqual(response.status_code, 303)
            self.assertIn("error=Reviewed+rows+require+force_reviewed", response.headers["location"])
            create_mock.assert_not_called()
            session.expire_all()
            self.assertEqual(session.get(DiscordMessage, reviewed_id).parse_status, PARSE_PARSED)

    def test_manual_reprocess_false_force_string_does_not_enable_full_reprocess(self) -> None:
        with self.session() as session, patch(
            "app.routers.admin_actions.require_role_response",
            return_value=None,
        ), patch(
            "app.routers.admin_actions.queue_auto_reprocess_candidates",
            return_value=2,
        ) as queue_mock:
            response = admin_parser_reprocess_form(
                make_request("/admin/parser/reprocess-form"),
                return_path="/table",
                force="false",
                session=session,
            )

        queue_mock.assert_called_once_with(session, force=False)
        self.assertIn("manual+parser+reprocess", response.headers["location"])
        self.assertNotIn("manual+full", response.headers["location"])

    def test_reparse_run_records_queue_summary_and_outcomes(self) -> None:
        with self.session() as session:
            queued_row = self.make_message(
                discord_message_id="reparse-run-queued",
                parse_status=PARSE_PARSED,
            )
            reviewed_row = self.make_message(
                discord_message_id="reparse-run-reviewed",
                parse_status=PARSE_PARSED,
                reviewed_at=utcnow(),
            )
            session.add(queued_row)
            session.add(reviewed_row)
            session.commit()
            session.refresh(queued_row)

            run = create_reparse_run_record(
                session,
                source="test",
                reason="validation reparse",
                range_after=utcnow() - timedelta(days=1),
                range_before=utcnow() + timedelta(days=1),
                channel_id="chan-1",
                include_reviewed=False,
                force_reviewed=False,
                requested_statuses=[PARSE_PARSED],
            )

            result = queue_reparse_range(
                session,
                start=utcnow() - timedelta(days=1),
                end=utcnow() + timedelta(days=1),
                include_statuses=[PARSE_PARSED],
                include_reviewed=False,
                reason="validation reparse",
                reparse_run_id=run.run_id,
            )
            finalize_reparse_run_queue_record(
                session,
                run_id=run.run_id,
                selected_count=result["matched"],
                queued_count=result["queued"],
                already_queued_count=result["already_queued"],
                skipped_reviewed_count=result["skipped_reviewed"],
                first_message_id=result["first_message_id"],
                last_message_id=result["last_message_id"],
                first_message_created_at=result["first_message_created_at"],
                last_message_created_at=result["last_message_created_at"],
            )
            session.refresh(queued_row)
            self.assertEqual(queued_row.active_reparse_run_id, run.run_id)

            record_reparse_run_outcome(session, run_id=run.run_id, success=True)
            refreshed_run = session.exec(select(ReparseRun).where(ReparseRun.run_id == run.run_id)).first()

            self.assertIsNotNone(refreshed_run)
            self.assertEqual(refreshed_run.selected_count, 1)
            self.assertEqual(refreshed_run.skipped_reviewed_count, 1)
            self.assertEqual(refreshed_run.succeeded_count, 1)
            self.assertEqual(refreshed_run.failed_count, 0)
            self.assertEqual(refreshed_run.status, "completed")
            self.assertIsNotNone(refreshed_run.finished_at)
            self.assertEqual(list_recent_reparse_runs(session, limit=5)[0].run_id, run.run_id)

    def test_admin_learned_rule_log_page_shows_recent_events(self) -> None:
        with self.session() as session, patch("app.routers.admin_actions.require_role_response", return_value=None):
            row = self.make_message(
                discord_message_id="learned-rule-log-row",
                content="sold charizard 45 cash",
            )
            session.add(row)
            session.commit()
            session.refresh(row)

            session.add(
                OperationsLog(
                    event_type="queue.learned_rule_applied",
                    level="info",
                    source="worker",
                    message="learned_rule_applied",
                    details_json=(
                        '{"message_id": %d, "pattern_type": "payment_only_sell", '
                        '"status": "applied", "reason": "matched payment-only sell phrase", '
                        '"correction_source": "learned_rule"}'
                    ) % row.id,
                )
            )
            session.commit()

            response = admin_parser_learned_rule_log_page(
                make_request("/admin/parser/learned-rule-log"),
                limit=50,
                session=session,
            )

            events = response.context["events"]
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0]["outcome"], "applied")
            self.assertEqual(events[0]["rule_matched"], "payment only sell (learned_rule)")
            self.assertEqual(events[0]["message_snippet"], "sold charizard 45 cash")

    def test_admin_queue_state_counts_includes_legacy_alias_rows_and_filters(self) -> None:
        with self.session() as session:
            session.add(
                self.make_message(
                    discord_message_id="legacy-queued-count",
                    parse_status="queued",
                )
            )
            session.add(
                self.make_message(
                    discord_message_id="legacy-needs-review-count",
                    parse_status="needs_review",
                )
            )
            session.add(
                self.make_message(
                    discord_message_id="legacy-deleted-count",
                    parse_status="deleted",
                    is_deleted=False,
                )
            )
            session.commit()

            with patch("app.routers.channels_api.require_role_response", return_value=None):
                result = admin_queue_state_counts(
                    make_request("/admin/queue-state-counts"),
                    status=None,
                    channel_id=None,
                    entry_kind=None,
                    after=None,
                    before=None,
                    session=session,
                )
                ignored_result = admin_queue_state_counts(
                    make_request("/admin/queue-state-counts"),
                    status="ignored",
                    channel_id=None,
                    entry_kind=None,
                    after=None,
                    before=None,
                    session=session,
                )

            self.assertEqual(result["counts"]["queued"], 1)
            self.assertEqual(result["counts"]["needs_review"], 1)
            self.assertEqual(ignored_result["counts"]["ignored"], 1)

    def test_grouped_child_ignored_row_tombstones_standalone_transaction(self) -> None:
        with self.session() as session:
            row = self.make_message(
                discord_message_id="grouped-child",
                parse_status=PARSE_PARSED,
                amount=15.0,
                entry_kind="sale",
                payment_method="cash",
                cash_direction="to_store",
                money_in=15.0,
            )
            session.add(row)
            session.commit()
            session.refresh(row)

            sync_transaction_from_message(session, row)
            session.commit()

            primary = self.make_message(
                discord_message_id="grouped-primary",
                content="Photo",
                created_at=row.created_at - timedelta(seconds=1),
                parse_status=PARSE_PARSED,
                amount=15.0,
                entry_kind="sale",
                payment_method="cash",
                cash_direction="to_store",
                money_in=15.0,
            )
            session.add(primary)
            session.flush()
            membership = json.dumps([primary.id, row.id])
            primary.stitched_group_id = "group-1"
            primary.stitched_primary = True
            primary.stitched_message_ids_json = membership
            row.parse_status = PARSE_IGNORED
            row.stitched_group_id = "group-1"
            row.stitched_primary = False
            row.stitched_message_ids_json = membership
            primary_transaction = sync_transaction_from_message(
                session,
                primary,
                source_rows=[primary, row],
                source_content="Message 1: Photo\nMessage 2: sold card $20 zelle",
            )
            session.commit()

            sync_transaction_from_message(session, row)
            session.commit()

            transaction = session.exec(
                select(Transaction).where(Transaction.source_message_id == row.id)
            ).first()
            self.assertIsNotNone(transaction)
            self.assertTrue(transaction.is_deleted)
            self.assertEqual(transaction.parse_status, PARSE_IGNORED)
            self.assertFalse(session.get(Transaction, primary_transaction.id).is_deleted)

    def test_ignored_primary_row_clears_stale_parsed_and_financial_fields(self) -> None:
        with self.session() as session, patch("app.discord.worker.parse_message") as parse_message_mock:
            row = self.make_message(
                discord_message_id="ignored-primary",
                parse_status=PARSE_PENDING,
                parse_attempts=0,
            )
            session.add(row)
            session.commit()
            session.refresh(row)

            parse_message_mock.return_value = {
                "parsed_type": "buy",
                "parsed_amount": 25.0,
                "parsed_payment_method": "zelle",
                "parsed_cash_direction": None,
                "parsed_category": "inventory",
                "parsed_items": ["Charizard"],
                "parsed_items_in": ["Charizard"],
                "parsed_items_out": [],
                "parsed_trade_summary": "ignored",
                "parsed_notes": "ignored by parser",
                "image_summary": "image used",
                "confidence": 0.77,
                "needs_review": False,
                "ignore_message": True,
            }

            @contextmanager
            def fake_managed_session():
                yield session

            with patch("app.discord.worker.managed_session", new=fake_managed_session):
                asyncio.run(process_row(row.id))

            session.refresh(row)
            self.assertEqual(row.parse_status, PARSE_IGNORED)
            self.assertTrue(
                all(
                    value in (None, "[]", "")
                    for value in (
                        row.deal_type,
                        row.amount,
                        row.payment_method,
                        row.cash_direction,
                        row.category,
                        row.trade_summary,
                        row.notes,
                        row.confidence,
                        row.image_summary,
                        row.entry_kind,
                        row.money_in,
                        row.money_out,
                        row.expense_category,
                    )
                )
            )
            self.assertEqual(row.item_names_json, "[]")
            self.assertEqual(row.items_in_json, "[]")
            self.assertEqual(row.items_out_json, "[]")

    def test_get_transactions_misses_legacy_transaction_needs_review_status(self) -> None:
        with self.session() as session:
            row = self.make_message(
                discord_message_id="legacy-transaction-needs-review",
                parse_status="needs_review",
                needs_review=True,
                amount=11.0,
                entry_kind="sale",
                payment_method="zelle",
                cash_direction="to_store",
                money_in=11.0,
            )
            session.add(row)
            session.commit()
            session.refresh(row)

            sync_transaction_from_message(session, row)
            session.commit()

            transactions = get_transactions(session)

            self.assertEqual(len(transactions), 1)
            self.assertEqual(transactions[0].source_message_id, row.id)

    def test_get_financial_rows_misses_legacy_message_needs_review_status(self) -> None:
        with self.session() as session:
            row = self.make_message(
                discord_message_id="legacy-reporting-needs-review",
                parse_status="needs_review",
                needs_review=True,
                amount=17.0,
                entry_kind="sale",
                payment_method="cash",
                cash_direction="to_store",
                money_in=17.0,
            )
            session.add(row)
            session.commit()

            rows = get_financial_rows(session)

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].discord_message_id, "legacy-reporting-needs-review")

    def test_build_debug_snapshot_reports_stuck_processing_and_recent_worker_failure(self) -> None:
        with self.session() as session:
            stale_started_at = utcnow() - timedelta(minutes=25)
            row = self.make_message(
                discord_message_id="stuck-processing",
                parse_status=PARSE_PROCESSING,
                parse_attempts=2,
                created_at=utcnow() - timedelta(minutes=30),
                ingested_at=utcnow() - timedelta(minutes=30),
            )
            session.add(row)
            session.commit()
            session.refresh(row)

            session.add(
                ParseAttempt(
                    message_id=row.id,
                    attempt_number=2,
                    started_at=stale_started_at,
                    finished_at=None,
                    success=False,
                    error="still running",
                )
            )
            session.add(
                OperationsLog(
                    event_type="queue.parse_failed",
                    level="warning",
                    source="worker",
                    message="parse_failed",
                    details_json='{"error":"still running"}',
                )
            )
            session.commit()

            snapshot = build_debug_snapshot(session)

            self.assertEqual(snapshot["queue_counts"]["processing"], 1)
            self.assertEqual(len(snapshot["stuck_processing"]), 1)
            self.assertEqual(snapshot["stuck_processing"][0]["message_id"], row.id)
            self.assertGreaterEqual(len(snapshot["recent_worker_failures"]), 1)

    def test_bulk_requeue_filtered_messages_form_resets_matching_review_rows(self) -> None:
        with self.session() as session, patch("app.routers.messages.require_role_response", return_value=None):
            matching = self.make_message(
                discord_message_id="filtered-review-match",
                channel_id="chan-review",
                channel_name="chan-review",
                parse_status=PARSE_REVIEW_REQUIRED,
                needs_review=True,
                parse_attempts=3,
                last_error="needs human review",
                reviewed_by="reviewer",
                reviewed_at=utcnow(),
                amount=25.0,
                entry_kind="sale",
                money_in=25.0,
                expense_category="inventory",
            )
            other_channel = self.make_message(
                discord_message_id="filtered-review-other-channel",
                channel_id="chan-other",
                channel_name="chan-other",
                parse_status=PARSE_REVIEW_REQUIRED,
                needs_review=True,
                parse_attempts=2,
                amount=30.0,
                entry_kind="sale",
                money_in=30.0,
                expense_category="inventory",
            )
            other_category = self.make_message(
                discord_message_id="filtered-review-other-category",
                channel_id="chan-review",
                channel_name="chan-review",
                parse_status=PARSE_FAILED,
                needs_review=False,
                parse_attempts=4,
                last_error="parse failed",
                amount=40.0,
                entry_kind="sale",
                money_in=40.0,
                expense_category="travel",
            )
            session.add(matching)
            session.add(other_channel)
            session.add(other_category)
            session.commit()
            session.refresh(matching)
            session.refresh(other_channel)
            session.refresh(other_category)

            sync_transaction_from_message(session, matching)
            sync_transaction_from_message(session, other_channel)
            sync_transaction_from_message(session, other_category)
            session.commit()

            response = bulk_reparse_filtered_messages_form(
                make_request("/messages/bulk/requeue-filtered-form"),
                return_path="/review",
                status="review_queue",
                channel_id="chan-review",
                expense_category="inventory",
                after=None,
                before=None,
                sort_by="time",
                sort_dir="desc",
                page=1,
                limit=25,
                session=session,
            )

            self.assertEqual(response.status_code, 303)
            session.refresh(matching)
            session.refresh(other_channel)
            session.refresh(other_category)

            self.assertEqual(matching.parse_status, PARSE_PENDING)
            self.assertEqual(matching.parse_attempts, 0)
            self.assertEqual(matching.last_error, "manual filtered reparse")
            self.assertFalse(matching.needs_review)
            self.assertIsNone(matching.reviewed_by)
            self.assertIsNone(matching.reviewed_at)
            self.assertIsNone(matching.active_reparse_run_id)

            self.assertEqual(other_channel.parse_status, PARSE_REVIEW_REQUIRED)
            self.assertTrue(other_channel.needs_review)
            self.assertEqual(other_category.parse_status, PARSE_FAILED)
            self.assertEqual(other_category.last_error, "parse failed")

            matching_tx = session.exec(select(Transaction).where(Transaction.source_message_id == matching.id)).all()
            other_channel_tx = session.exec(select(Transaction).where(Transaction.source_message_id == other_channel.id)).all()
            other_category_tx = session.exec(select(Transaction).where(Transaction.source_message_id == other_category.id)).all()
            self.assertEqual(len(matching_tx), 1)
            self.assertTrue(matching_tx[0].is_deleted)
            self.assertEqual(len(other_channel_tx), 1)
            self.assertEqual(len(other_category_tx), 0)

    def test_bulk_requeue_filtered_preflights_all_rows_before_first_chunk_commit(self) -> None:
        base_time = utcnow()
        with self.session() as session:
            safe_rows = [
                self.make_message(
                    discord_message_id=f"filtered-safe-{index}",
                    channel_id="chan-review",
                    channel_name="chan-review",
                    created_at=base_time + timedelta(microseconds=index),
                    parse_status=PARSE_REVIEW_REQUIRED,
                    needs_review=True,
                    parse_attempts=3,
                    last_error="needs human review",
                    amount=25.0,
                    entry_kind="sale",
                    money_in=25.0,
                    expense_category="inventory",
                )
                for index in range(25)
            ]
            primary = self.make_message(
                discord_message_id="filtered-conflict-primary",
                channel_id="chan-review",
                channel_name="chan-review",
                created_at=base_time + timedelta(microseconds=25),
                parse_status=PARSE_REVIEW_REQUIRED,
                needs_review=True,
                parse_attempts=3,
                last_error="needs human review",
                amount=50.0,
                entry_kind="sale",
                money_in=50.0,
                expense_category="inventory",
            )
            child = self.make_message(
                discord_message_id="filtered-conflict-child",
                channel_id="chan-review",
                channel_name="chan-review",
                created_at=base_time + timedelta(seconds=1),
                parse_status=PARSE_IGNORED,
                needs_review=False,
                amount=None,
                entry_kind=None,
                money_in=0.0,
                expense_category="inventory",
            )
            session.add_all([*safe_rows, primary, child])
            session.flush()
            membership = json.dumps([primary.id, child.id])
            primary.stitched_group_id = "filtered-conflict-group"
            primary.stitched_primary = True
            primary.stitched_message_ids_json = membership
            child.stitched_group_id = "filtered-conflict-group"
            child.stitched_primary = False
            child.stitched_message_ids_json = membership
            ensure_message_revision(session, primary)
            ensure_message_revision(session, child)
            transaction = sync_transaction_from_message(
                session,
                primary,
                source_rows=[primary, child],
                source_content="Message 1: sold card 50 cash\n\nMessage 2: receipt",
            )
            child.source_refresh_required = True
            child.last_error = "canonical Discord refresh required after raw edit fetch failure: unavailable"
            session.add(child)
            session.commit()
            safe_ids = [row.id for row in safe_rows]
            primary_id = primary.id
            child_id = child.id
            transaction_id = transaction.id

        with self.session() as session, patch(
            "app.routers.messages.require_role_response",
            return_value=None,
        ):
            with self.assertRaises(HTTPException) as exc:
                bulk_reparse_filtered_messages_form(
                    make_request("/messages/bulk/requeue-filtered-form"),
                    return_path="/review",
                    status="review_queue",
                    channel_id="chan-review",
                    expense_category="inventory",
                    after=None,
                    before=None,
                    sort_by="time",
                    sort_dir="desc",
                    page=1,
                    limit=25,
                    session=session,
                )
            self.assertEqual(exc.exception.status_code, 409)
            self.assertFalse(session.dirty)

        with self.session() as session:
            safe_after = session.exec(
                select(DiscordMessage)
                .where(DiscordMessage.id.in_(safe_ids))
                .order_by(DiscordMessage.id)
            ).all()
            primary_after = session.get(DiscordMessage, primary_id)
            child_after = session.get(DiscordMessage, child_id)
            transaction_after = session.get(Transaction, transaction_id)
            self.assertEqual(len(safe_after), 25)
            self.assertTrue(all(row.parse_status == PARSE_REVIEW_REQUIRED for row in safe_after))
            self.assertTrue(all(row.parse_attempts == 3 for row in safe_after))
            self.assertTrue(all(row.last_error == "needs human review" for row in safe_after))
            self.assertEqual(primary_after.parse_status, PARSE_REVIEW_REQUIRED)
            self.assertEqual(primary_after.parse_attempts, 3)
            self.assertEqual(primary_after.last_error, "needs human review")
            self.assertTrue(child_after.source_refresh_required)
            self.assertFalse(transaction_after.is_deleted)

    def test_bulk_requeue_filtered_rejects_more_than_500_rows_without_writes(self) -> None:
        with self.session() as session:
            rows = [
                self.make_message(
                    discord_message_id=f"filtered-cap-{index}",
                    channel_id="chan-cap",
                    parse_status=PARSE_REVIEW_REQUIRED,
                    needs_review=True,
                    parse_attempts=3,
                    last_error="unchanged",
                )
                for index in range(501)
            ]
            session.add_all(rows)
            session.commit()
            row_ids = [row.id for row in rows]

        with self.session() as session, patch(
            "app.routers.messages.require_role_response",
            return_value=None,
        ):
            with self.assertRaises(HTTPException) as exc:
                bulk_reparse_filtered_messages_form(
                    make_request("/messages/bulk/requeue-filtered-form"),
                    return_path="/review",
                    status="review_queue",
                    channel_id="chan-cap",
                    expense_category=None,
                    after=None,
                    before=None,
                    sort_by="time",
                    sort_dir="desc",
                    page=1,
                    limit=100,
                    session=session,
                )
            self.assertEqual(exc.exception.status_code, 409)
            self.assertEqual(
                exc.exception.detail,
                "Filtered reparse matches more than 500 rows; narrow the filters and retry.",
            )
            self.assertFalse(session.dirty)

        with self.session() as session:
            rows_after = session.exec(
                select(DiscordMessage)
                .where(DiscordMessage.id.in_(row_ids))
                .order_by(DiscordMessage.id)
            ).all()
            self.assertEqual(len(rows_after), 501)
            self.assertTrue(all(row.parse_status == PARSE_REVIEW_REQUIRED for row in rows_after))
            self.assertTrue(all(row.parse_attempts == 3 for row in rows_after))
            self.assertTrue(all(row.last_error == "unchanged" for row in rows_after))

    def test_bulk_requeue_filtered_rejects_aggregate_source_guard_overflow_before_lock(self) -> None:
        with self.session() as session:
            row = self.make_message(
                discord_message_id="filtered-guard-overflow",
                channel_id="chan-guard-overflow",
                parse_status=PARSE_REVIEW_REQUIRED,
                needs_review=True,
                parse_attempts=3,
                last_error="unchanged",
            )
            session.add(row)
            session.commit()
            row_id = row.id

        guards = [
            SourceMutationSnapshot(
                id=source_id,
                content="source",
                attachment_urls_json="[]",
                current_revision_id=None,
                is_deleted=False,
                source_refresh_required=False,
                last_error=None,
                stitched_group_id=None,
                stitched_primary=False,
                stitched_message_ids_json="[]",
                active_parse_attempt_id=None,
            )
            for source_id in range(1, 4098)
        ]

        with self.session() as session, patch(
            "app.routers.messages.require_role_response",
            return_value=None,
        ), patch(
            "app.routers.messages.capture_source_group_mutation_guards",
            return_value=guards,
        ), patch(
            "app.routers.messages.lock_source_group_mutation_guards"
        ) as lock_guards, patch(
            "app.routers.messages.reparse_message_rows"
        ) as reparse_rows:
            with self.assertRaises(HTTPException) as exc:
                bulk_reparse_filtered_messages_form(
                    make_request("/messages/bulk/requeue-filtered-form"),
                    return_path="/review",
                    status="review_queue",
                    channel_id="chan-guard-overflow",
                    expense_category=None,
                    after=None,
                    before=None,
                    sort_by="time",
                    sort_dir="desc",
                    page=1,
                    limit=100,
                    session=session,
                )
            self.assertEqual(exc.exception.status_code, 409)
            self.assertEqual(
                exc.exception.detail,
                "Filtered reparse source set exceeds 4096 rows; narrow the filters and retry.",
            )
            lock_guards.assert_not_called()
            reparse_rows.assert_not_called()

        with self.session() as session:
            row = session.get(DiscordMessage, row_id)
            self.assertEqual(row.parse_status, PARSE_REVIEW_REQUIRED)
            self.assertEqual(row.parse_attempts, 3)
            self.assertEqual(row.last_error, "unchanged")

    def test_bulk_requeue_filtered_rejects_active_parse_claim_without_mutation(self) -> None:
        with self.session() as session:
            row = self.make_message(
                discord_message_id="filtered-active-claim",
                channel_id="chan-active-claim",
                parse_status=PARSE_REVIEW_REQUIRED,
                needs_review=True,
                parse_attempts=3,
                last_error="unchanged",
            )
            session.add(row)
            session.flush()
            attempt = ParseAttempt(message_id=row.id, attempt_number=4)
            session.add(attempt)
            session.flush()
            row.active_parse_attempt_id = attempt.id
            session.add(row)
            session.commit()
            row_id = row.id
            attempt_id = attempt.id

        with self.session() as session, patch(
            "app.routers.messages.require_role_response",
            return_value=None,
        ):
            with self.assertRaises(HTTPException) as exc:
                bulk_reparse_filtered_messages_form(
                    make_request("/messages/bulk/requeue-filtered-form"),
                    return_path="/review",
                    status="review_queue",
                    channel_id="chan-active-claim",
                    expense_category=None,
                    after=None,
                    before=None,
                    sort_by="time",
                    sort_dir="desc",
                    page=1,
                    limit=100,
                    session=session,
                )
            self.assertEqual(exc.exception.status_code, 409)
            self.assertIn("active parse attempt", str(exc.exception.detail))
            self.assertFalse(session.dirty)

        with self.session() as session:
            row = session.get(DiscordMessage, row_id)
            attempt = session.get(ParseAttempt, attempt_id)
            self.assertEqual(row.parse_status, PARSE_REVIEW_REQUIRED)
            self.assertEqual(row.parse_attempts, 3)
            self.assertEqual(row.last_error, "unchanged")
            self.assertEqual(row.active_parse_attempt_id, attempt_id)
            self.assertIsNone(attempt.finished_at)

    def test_bulk_requeue_filtered_ignores_newly_eligible_low_id_after_freeze(self) -> None:
        with self.session() as session:
            newly_eligible = self.make_message(
                discord_message_id="filtered-newly-eligible",
                channel_id="chan-freeze",
                parse_status=PARSE_PARSED,
                needs_review=False,
                parse_attempts=7,
                last_error="must remain unchanged",
            )
            frozen = self.make_message(
                discord_message_id="filtered-frozen",
                channel_id="chan-freeze",
                parse_status=PARSE_REVIEW_REQUIRED,
                needs_review=True,
                parse_attempts=3,
                last_error="review me",
            )
            session.add_all([newly_eligible, frozen])
            session.commit()
            newly_eligible_id = newly_eligible.id
            frozen_id = frozen.id

        captured_once = False

        def capture_then_change_filter(session: Session, row: DiscordMessage):
            nonlocal captured_once
            if not captured_once:
                captured_once = True
                concurrent = session.get(DiscordMessage, newly_eligible_id)
                concurrent.parse_status = PARSE_REVIEW_REQUIRED
                concurrent.needs_review = True
                session.add(concurrent)
                session.commit()
            from app.discord.transactions import capture_source_group_mutation_guards

            return capture_source_group_mutation_guards(session, row)

        with self.session() as session, patch(
            "app.routers.messages.require_role_response",
            return_value=None,
        ), patch(
            "app.routers.messages.capture_source_group_mutation_guards",
            side_effect=capture_then_change_filter,
        ):
            response = bulk_reparse_filtered_messages_form(
                make_request("/messages/bulk/requeue-filtered-form"),
                return_path="/review",
                status="review_queue",
                channel_id="chan-freeze",
                expense_category=None,
                after=None,
                before=None,
                sort_by="time",
                sort_dir="desc",
                page=1,
                limit=100,
                session=session,
            )
            self.assertEqual(response.status_code, 303)

        with self.session() as session:
            newly_eligible_after = session.get(DiscordMessage, newly_eligible_id)
            frozen_after = session.get(DiscordMessage, frozen_id)
            self.assertEqual(newly_eligible_after.parse_status, PARSE_REVIEW_REQUIRED)
            self.assertEqual(newly_eligible_after.parse_attempts, 7)
            self.assertEqual(newly_eligible_after.last_error, "must remain unchanged")
            self.assertEqual(frozen_after.parse_status, PARSE_PENDING)

    def test_bulk_requeue_filtered_rolls_back_when_frozen_row_is_quarantined_before_lock(self) -> None:
        from app.discord import transactions as transaction_module

        with self.session() as session:
            rows = [
                self.make_message(
                    discord_message_id=f"filtered-late-quarantine-{index}",
                    channel_id="chan-late-quarantine",
                    parse_status=PARSE_REVIEW_REQUIRED,
                    needs_review=True,
                    parse_attempts=3,
                    last_error=f"original-{index}",
                )
                for index in range(2)
            ]
            session.add_all(rows)
            session.commit()
            row_ids = [row.id for row in rows]

        original_lock = transaction_module.lock_source_group_mutation_guards

        def quarantine_then_lock(session: Session, guards):
            quarantined = session.get(DiscordMessage, row_ids[1])
            quarantined.source_refresh_required = True
            quarantined.last_error = (
                "canonical Discord refresh required after concurrent filtered failure"
            )
            session.add(quarantined)
            session.commit()
            return original_lock(session, guards)

        with self.session() as session, patch(
            "app.routers.messages.require_role_response",
            return_value=None,
        ), patch(
            "app.routers.messages.lock_source_group_mutation_guards",
            side_effect=quarantine_then_lock,
        ):
            with self.assertRaises(HTTPException) as exc:
                bulk_reparse_filtered_messages_form(
                    make_request("/messages/bulk/requeue-filtered-form"),
                    return_path="/review",
                    status="review_queue",
                    channel_id="chan-late-quarantine",
                    expense_category=None,
                    after=None,
                    before=None,
                    sort_by="time",
                    sort_dir="desc",
                    page=1,
                    limit=100,
                    session=session,
                )
            self.assertEqual(exc.exception.status_code, 409)
            self.assertIn("changed during manual mutation", str(exc.exception.detail))
            self.assertFalse(session.dirty)

        with self.session() as session:
            first = session.get(DiscordMessage, row_ids[0])
            quarantined = session.get(DiscordMessage, row_ids[1])
            self.assertEqual(first.parse_status, PARSE_REVIEW_REQUIRED)
            self.assertEqual(first.parse_attempts, 3)
            self.assertEqual(first.last_error, "original-0")
            self.assertEqual(quarantined.parse_status, PARSE_REVIEW_REQUIRED)
            self.assertEqual(quarantined.parse_attempts, 3)
            self.assertTrue(quarantined.source_refresh_required)
            self.assertIn("concurrent filtered failure", quarantined.last_error)

    def test_bulk_requeue_filtered_accepts_exactly_500_rows(self) -> None:
        with self.session() as session:
            rows = [
                self.make_message(
                    discord_message_id=f"filtered-boundary-{index}",
                    channel_id="chan-boundary",
                    parse_status=PARSE_REVIEW_REQUIRED,
                    needs_review=True,
                    parse_attempts=3,
                    last_error="review me",
                )
                for index in range(500)
            ]
            session.add_all(rows)
            session.commit()
            row_ids = [row.id for row in rows]

        with self.session() as session, patch(
            "app.routers.messages.require_role_response",
            return_value=None,
        ):
            response = bulk_reparse_filtered_messages_form(
                make_request("/messages/bulk/requeue-filtered-form"),
                return_path="/review",
                status="review_queue",
                channel_id="chan-boundary",
                expense_category=None,
                after=None,
                before=None,
                sort_by="time",
                sort_dir="desc",
                page=1,
                limit=100,
                session=session,
            )
            self.assertEqual(response.status_code, 303)

        with self.session() as session:
            rows_after = session.exec(
                select(DiscordMessage)
                .where(DiscordMessage.id.in_(row_ids))
                .order_by(DiscordMessage.id)
            ).all()
            self.assertEqual(len(rows_after), 500)
            self.assertTrue(all(row.parse_status == PARSE_PENDING for row in rows_after))
            self.assertTrue(all(row.parse_attempts == 0 for row in rows_after))

    def test_retry_message_form_resets_attempts_and_removes_transaction_until_reparsed(self) -> None:
        with self.session() as session, patch("app.routers.messages.require_role_response", return_value=None):
            reviewed_at = utcnow()
            row = self.make_message(
                discord_message_id="retry-row",
                parse_status=PARSE_PARSED,
                parse_attempts=3,
                reviewed_by="reviewer",
                reviewed_at=reviewed_at,
                amount=20.0,
                entry_kind="sale",
                payment_method="zelle",
                cash_direction="to_store",
                money_in=20.0,
            )
            session.add(row)
            session.commit()
            session.refresh(row)
            sync_transaction_from_message(session, row)
            session.commit()

            response = reparse_message_form(
                make_request(f"/messages/{row.id}/retry-form"),
                message_id=row.id,
                page=1,
                limit=100,
                session=session,
            )

            session.refresh(row)
            transaction = session.exec(
                select(Transaction).where(Transaction.source_message_id == row.id)
            ).first()

            self.assertEqual(response.status_code, 303)
            self.assertEqual(row.parse_status, PARSE_PENDING)
            self.assertEqual(row.parse_attempts, 0)
            self.assertIsNone(row.reviewed_by)
            self.assertIsNone(row.reviewed_at)
            self.assertIsNotNone(transaction)
            self.assertTrue(transaction.is_deleted)
            self.assertEqual(transaction.parse_status, PARSE_PENDING)

    def test_retry_message_form_translates_late_integrity_conflict_and_rolls_back(self) -> None:
        with self.session() as session:
            row = self.make_message(
                discord_message_id="retry-late-integrity-conflict",
                parse_status=PARSE_PARSED,
                parse_attempts=3,
                last_error="original error",
                amount=20.0,
                entry_kind="sale",
                money_in=20.0,
            )
            session.add(row)
            session.commit()
            row_id = row.id
            before = (row.parse_status, row.parse_attempts, row.last_error)

        def stage_then_conflict(session: Session, rows, **_kwargs):
            rows[0].parse_status = PARSE_PENDING
            rows[0].parse_attempts = 0
            rows[0].last_error = "staged mutation"
            session.add(rows[0])
            raise StaleSourceRevisionError("late transaction source conflict")

        with self.session() as session, patch(
            "app.routers.messages.require_role_response",
            return_value=None,
        ), patch(
            "app.routers.messages.reparse_message_rows",
            side_effect=stage_then_conflict,
        ):
            with self.assertRaises(HTTPException) as exc:
                reparse_message_form(
                    make_request(f"/messages/{row_id}/retry-form"),
                    message_id=row_id,
                    session=session,
                )
            self.assertEqual(exc.exception.status_code, 409)
            self.assertIn("late transaction source conflict", str(exc.exception.detail))
            self.assertFalse(session.dirty)

        with self.session() as session:
            row_after = session.get(DiscordMessage, row_id)
            self.assertEqual(
                (row_after.parse_status, row_after.parse_attempts, row_after.last_error),
                before,
            )


if __name__ == "__main__":
    unittest.main()
