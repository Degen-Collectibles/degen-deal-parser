import asyncio
import json
import shutil
import types
import unittest
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

import discord
from sqlmodel import SQLModel, Session, create_engine, select

import app.discord.discord_ingest as discord_ingest_module
from app.discord.discord_ingest import (
    get_attachment_payloads,
    DealIngestBot,
    invalidate_available_channels_cache,
    insert_or_update_message,
    list_available_discord_channels,
    mark_message_deleted_row,
    persist_available_discord_channels,
)
from app.discord.channels import get_available_channel_choices
from app.discord.message_revisions import ensure_message_revision
from app.discord.transactions import MAX_TRANSACTION_SOURCE_ROWS, transaction_base_query
from app.discord.worker import claim_message_for_parse, process_once
from app.ledger import _load_unbanked_cash_transactions
from app.models import (
    AvailableDiscordChannel,
    BankStatementImport,
    BankTransaction,
    BookkeepingEntry,
    BookkeepingImport,
    DISCORD_SOURCE_REFRESH_REQUIRED_ERROR,
    DiscordMessage,
    DiscordMessageRevision,
    PARSE_FAILED,
    PARSE_IGNORED,
    PARSE_PARSED,
    PARSE_PENDING,
    ParseAttempt,
    Transaction,
    TransactionItem,
    TransactionSourceRevision,
    WatchedChannel,
)


def _utcnow():
    return datetime.now(timezone.utc)


def _fake_author(author_id, author_name):
    class FakeAuthor:
        def __init__(self):
            self.id = int(author_id)
            self.bot = False
        def __str__(self):
            return author_name
    return FakeAuthor()


def _fake_message(
    msg_id="111",
    content="$50 buy",
    channel_id="999",
    channel_name="deals",
    guild_id="888",
    author_id="777",
    author_name="Trader#0001",
    attachments=None,
    edited_at=None,
):
    attachment_list = attachments or []
    return types.SimpleNamespace(
        id=int(msg_id),
        content=content,
        channel=types.SimpleNamespace(id=int(channel_id), name=channel_name),
        guild=types.SimpleNamespace(id=int(guild_id)),
        author=_fake_author(author_id, author_name),
        attachments=attachment_list,
        created_at=_utcnow(),
        edited_at=edited_at,
    )


def _fake_attachment(url="https://cdn.discord.com/a.png", filename="a.png", content_type="image/png"):
    return types.SimpleNamespace(url=url, filename=filename, content_type=content_type)


class InsertOrUpdateMessageTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path.cwd() / "tests" / ".tmp_discord_ingest" / str(uuid.uuid4())
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = self.temp_dir / "ingest.db"
        self.engine = create_engine(
            f"sqlite:///{db_path.as_posix()}",
            connect_args={"check_same_thread": False},
        )
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self):
        self.engine.dispose()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @contextmanager
    def _managed_session(self):
        with Session(self.engine) as session:
            yield session

    def _patch(self):
        return [
            patch("app.discord.discord_ingest.managed_session", self._managed_session),
            patch("app.discord.discord_ingest.sync_attachment_assets"),
            patch("app.discord.discord_ingest.ingest_log"),
        ]

    _WATCHED_CHANNEL_IDS = {999}

    def test_new_message_stored_as_pending(self):
        msg = _fake_message()
        patches = self._patch()
        for p in patches:
            p.start()
        try:
            tracked, action = insert_or_update_message(msg, is_edit=False, watched_channel_ids=self._WATCHED_CHANNEL_IDS)
        finally:
            for p in patches:
                p.stop()

        self.assertTrue(tracked)
        self.assertEqual(action, "inserted")
        with Session(self.engine) as session:
            row = session.exec(select(DiscordMessage)).first()
            revision = session.exec(select(DiscordMessageRevision)).first()
        self.assertIsNotNone(row)
        self.assertIsNotNone(revision)
        self.assertEqual(row.parse_status, PARSE_PENDING)
        self.assertEqual(row.content, "$50 buy")
        self.assertEqual(revision.revision_number, 1)
        self.assertEqual(revision.content, "$50 buy")
        self.assertEqual(revision.attachment_urls_json, "[]")
        self.assertEqual(row.current_revision_id, revision.id)

    def test_new_message_stores_channel_and_author(self):
        msg = _fake_message(channel_name="trades", author_name="Jeff#1234")
        patches = self._patch()
        for p in patches:
            p.start()
        try:
            insert_or_update_message(msg, watched_channel_ids=self._WATCHED_CHANNEL_IDS)
        finally:
            for p in patches:
                p.stop()

        with Session(self.engine) as session:
            row = session.exec(select(DiscordMessage)).first()
        self.assertEqual(row.channel_name, "trades")
        self.assertEqual(row.author_name, "Jeff#1234")

    def test_edit_resets_parsed_row_to_pending(self):
        with Session(self.engine) as session:
            row = DiscordMessage(
                discord_message_id="111",
                channel_id="999",
                channel_name="deals",
                author_id="777",
                author_name="Trader#0001",
                content="$50 buy",
                attachment_urls_json="[]",
                parse_status=PARSE_PARSED,
                parse_attempts=2,
                created_at=_utcnow(),
            )
            session.add(row)
            session.commit()

        msg = _fake_message(msg_id="111", content="$55 buy edited")
        patches = self._patch()
        for p in patches:
            p.start()
        try:
            tracked, action = insert_or_update_message(
                msg,
                is_edit=True,
                watched_channel_ids=self._WATCHED_CHANNEL_IDS,
            )
        finally:
            for p in patches:
                p.stop()

        self.assertEqual(action, "updated")
        with Session(self.engine) as session:
            row = session.exec(select(DiscordMessage)).first()
        self.assertEqual(row.parse_status, PARSE_PENDING)
        self.assertEqual(row.parse_attempts, 0)
        self.assertIsNone(row.last_error)

    def test_non_edit_update_preserves_parse_status(self):
        with Session(self.engine) as session:
            row = DiscordMessage(
                discord_message_id="111",
                channel_id="999",
                channel_name="deals",
                author_id="777",
                author_name="Trader#0001",
                content="$50 buy",
                attachment_urls_json="[]",
                parse_status=PARSE_PARSED,
                parse_attempts=1,
                created_at=_utcnow(),
            )
            session.add(row)
            session.commit()

        msg = _fake_message(msg_id="111")
        patches = self._patch()
        for p in patches:
            p.start()
        try:
            insert_or_update_message(msg, is_edit=False, watched_channel_ids=self._WATCHED_CHANNEL_IDS)
        finally:
            for p in patches:
                p.stop()

        with Session(self.engine) as session:
            row = session.exec(select(DiscordMessage)).first()
        self.assertEqual(row.parse_status, PARSE_PARSED)

    def test_existing_message_edit_to_empty_is_captured_after_channel_disabled(self):
        original = _fake_message(msg_id="301", content="Buy 50 cash")
        empty_edit = _fake_message(msg_id="301", content="", edited_at=_utcnow())
        patches = self._patch()
        for item in patches:
            item.start()
        try:
            self.assertEqual(
                insert_or_update_message(original, watched_channel_ids=self._WATCHED_CHANNEL_IDS),
                (True, "inserted"),
            )
            self.assertEqual(
                insert_or_update_message(
                    empty_edit,
                    is_edit=True,
                    watched_channel_ids=set(),
                ),
                (True, "updated"),
            )
        finally:
            for item in patches:
                item.stop()

        with Session(self.engine) as session:
            row = session.exec(
                select(DiscordMessage).where(DiscordMessage.discord_message_id == "301")
            ).one()
            revisions = session.exec(
                select(DiscordMessageRevision)
                .where(DiscordMessageRevision.message_id == row.id)
                .order_by(DiscordMessageRevision.revision_number)
            ).all()
            self.assertEqual(row.content, "")
            self.assertEqual(row.parse_status, PARSE_PENDING)
            self.assertEqual([revision.content for revision in revisions], ["Buy 50 cash", ""])

    def test_duplicate_edit_delivery_is_idempotent_and_does_not_tombstone_money(self):
        message = _fake_message(msg_id="302", content="Sell 50 cash", edited_at=_utcnow())
        patches = self._patch()
        for item in patches:
            item.start()
        try:
            insert_or_update_message(message, watched_channel_ids=self._WATCHED_CHANNEL_IDS)
            with Session(self.engine) as session:
                row = session.exec(
                    select(DiscordMessage).where(DiscordMessage.discord_message_id == "302")
                ).one()
                row.parse_status = PARSE_PARSED
                row.deal_type = "sell"
                row.entry_kind = "sale"
                row.amount = 50
                row.money_in = 50
                sync_transaction_from_message = discord_ingest_module.sync_transaction_from_message
                transaction = sync_transaction_from_message(session, row)
                session.commit()
                transaction_id = transaction.id

            self.assertEqual(
                insert_or_update_message(
                    message,
                    is_edit=True,
                    watched_channel_ids=set(),
                ),
                (True, "updated"),
            )
        finally:
            for item in patches:
                item.stop()

        with Session(self.engine) as session:
            transaction = session.get(Transaction, transaction_id)
            revisions = session.exec(select(DiscordMessageRevision)).all()
            self.assertFalse(transaction.is_deleted)
            self.assertEqual(len(revisions), 1)

    def test_uncached_raw_edit_fetches_and_updates_existing_message(self):
        original = _fake_message(msg_id="303", content="Buy 20 cash")
        edited = _fake_message(msg_id="303", content="Buy 25 cash", edited_at=_utcnow())
        patches = self._patch()
        for item in patches:
            item.start()
        try:
            insert_or_update_message(original, watched_channel_ids=self._WATCHED_CHANNEL_IDS)
            channel = types.SimpleNamespace(fetch_message=AsyncMock(return_value=edited))
            bot = types.SimpleNamespace(
                get_channel=lambda _channel_id: channel,
                fetch_channel=AsyncMock(return_value=channel),
            )
            payload = types.SimpleNamespace(
                message_id=303,
                channel_id=999,
                cached_message=None,
                data={"content": "Buy 25 cash"},
            )
            asyncio.run(DealIngestBot.on_raw_message_edit(bot, payload))
            asyncio.run(DealIngestBot.on_raw_message_edit(bot, payload))
        finally:
            for item in patches:
                item.stop()

        with Session(self.engine) as session:
            row = session.exec(
                select(DiscordMessage).where(DiscordMessage.discord_message_id == "303")
            ).one()
            self.assertEqual(row.content, "Buy 25 cash")
            self.assertEqual(row.parse_status, PARSE_PENDING)
            self.assertEqual(
                len(
                    session.exec(
                        select(DiscordMessageRevision).where(
                            DiscordMessageRevision.message_id == row.id
                        )
                    ).all()
                ),
                2,
            )

    def test_raw_embed_only_update_does_not_fetch_or_mutate_source(self):
        original = _fake_message(msg_id="305", content="Buy 20 cash")
        patches = self._patch()
        for item in patches:
            item.start()
        try:
            insert_or_update_message(original, watched_channel_ids=self._WATCHED_CHANNEL_IDS)
            channel = types.SimpleNamespace(fetch_message=AsyncMock())
            bot = types.SimpleNamespace(
                get_channel=lambda _channel_id: channel,
                fetch_channel=AsyncMock(return_value=channel),
            )
            payload = types.SimpleNamespace(
                message_id=305,
                channel_id=999,
                cached_message=original,
                data={"embeds": [{"title": "preview changed"}]},
            )
            asyncio.run(DealIngestBot.on_raw_message_edit(bot, payload))
            channel.fetch_message.assert_not_awaited()
        finally:
            for item in patches:
                item.stop()

        with Session(self.engine) as session:
            row = session.exec(
                select(DiscordMessage).where(DiscordMessage.discord_message_id == "305")
            ).one()
            self.assertEqual(row.content, "Buy 20 cash")

    def test_raw_unknown_unwatched_edit_does_not_fetch(self):
        channel = types.SimpleNamespace(fetch_message=AsyncMock())
        bot = types.SimpleNamespace(
            get_channel=lambda _channel_id: channel,
            fetch_channel=AsyncMock(return_value=channel),
        )
        payload = types.SimpleNamespace(
            message_id=99999,
            channel_id=444,
            cached_message=None,
            data={"content": "unknown"},
        )
        patches = self._patch()
        for item in patches:
            item.start()
        try:
            with patch("app.discord.discord_ingest.get_enabled_channel_ids", return_value=set()):
                asyncio.run(DealIngestBot.on_raw_message_edit(bot, payload))
            channel.fetch_message.assert_not_awaited()
        finally:
            for item in patches:
                item.stop()

    def test_raw_edit_not_found_soft_deletes_stored_message(self):
        original = _fake_message(msg_id="306", content="Sell 20 cash")
        response = types.SimpleNamespace(status=404, reason="Not Found", headers={})
        not_found = discord.NotFound(response, "missing")
        patches = self._patch()
        for item in patches:
            item.start()
        try:
            insert_or_update_message(original, watched_channel_ids=self._WATCHED_CHANNEL_IDS)
            channel = types.SimpleNamespace(fetch_message=AsyncMock(side_effect=not_found))
            bot = types.SimpleNamespace(
                get_channel=lambda _channel_id: channel,
                fetch_channel=AsyncMock(return_value=channel),
            )
            payload = types.SimpleNamespace(
                message_id=306,
                channel_id=999,
                cached_message=None,
                data={"content": "Sell 25 cash"},
            )
            asyncio.run(DealIngestBot.on_raw_message_edit(bot, payload))
        finally:
            for item in patches:
                item.stop()

        with Session(self.engine) as session:
            row = session.exec(
                select(DiscordMessage).where(DiscordMessage.discord_message_id == "306")
            ).one()
            self.assertTrue(row.is_deleted)
            self.assertEqual(row.parse_status, PARSE_IGNORED)

    def test_raw_edit_fetch_failure_blocks_old_projection_and_tombstones_money(self):
        original = _fake_message(msg_id="307", content="Sell 20 cash")
        patches = self._patch()
        for item in patches:
            item.start()
        try:
            insert_or_update_message(original, watched_channel_ids=self._WATCHED_CHANNEL_IDS)
            with Session(self.engine) as session:
                row = session.exec(
                    select(DiscordMessage).where(DiscordMessage.discord_message_id == "307")
                ).one()
                row.parse_status = PARSE_PARSED
                row.deal_type = "sell"
                row.entry_kind = "sale"
                row.amount = 20
                row.money_in = 20
                transaction = discord_ingest_module.sync_transaction_from_message(session, row)
                session.commit()
                transaction_id = transaction.id

            channel = types.SimpleNamespace(
                fetch_message=AsyncMock(side_effect=RuntimeError("temporary Discord outage"))
            )
            bot = types.SimpleNamespace(
                get_channel=lambda _channel_id: channel,
                fetch_channel=AsyncMock(return_value=channel),
            )
            payload = types.SimpleNamespace(
                message_id=307,
                channel_id=999,
                cached_message=None,
                data={"content": "Sell 25 cash"},
            )
            asyncio.run(DealIngestBot.on_raw_message_edit(bot, payload))
        finally:
            for item in patches:
                item.stop()

        with Session(self.engine) as session:
            row = session.exec(
                select(DiscordMessage).where(DiscordMessage.discord_message_id == "307")
            ).one()
            transaction = session.get(Transaction, transaction_id)
            self.assertEqual(row.parse_status, PARSE_FAILED)
            self.assertEqual(row.parse_attempts, discord_ingest_module.settings.parser_max_attempts)
            self.assertIn("canonical Discord refresh required", row.last_error or "")
            self.assertTrue(transaction.is_deleted)
            self.assertIsNone(claim_message_for_parse(session, row.id))

        with patch("app.discord.worker.managed_session", self._managed_session), patch(
            "app.discord.worker.process_row",
            new_callable=AsyncMock,
        ) as process_row_mock:
            asyncio.run(process_once())
            process_row_mock.assert_not_awaited()

        # A later canonical fetch of the unchanged projection must clear the
        # explicit refresh block even if retry-limit handling prefixed its error.
        patches = self._patch()
        for item in patches:
            item.start()
        try:
            self.assertEqual(
                insert_or_update_message(
                    original,
                    is_edit=True,
                    watched_channel_ids=self._WATCHED_CHANNEL_IDS,
                    canonical_source_refresh=True,
                ),
                (True, "updated"),
            )
        finally:
            for item in patches:
                item.stop()
        with Session(self.engine) as session:
            row = session.exec(
                select(DiscordMessage).where(DiscordMessage.discord_message_id == "307")
            ).one()
            self.assertEqual(row.parse_status, PARSE_PENDING)
            self.assertEqual(row.parse_attempts, 0)

    def test_uncached_raw_delete_invalidates_persisted_message(self):
        original = _fake_message(msg_id="304", content="Sell 40 cash")
        patches = self._patch()
        for item in patches:
            item.start()
        try:
            insert_or_update_message(original, watched_channel_ids=self._WATCHED_CHANNEL_IDS)
            bot = types.SimpleNamespace()
            payload = types.SimpleNamespace(
                message_id=304,
                channel_id=999,
                cached_message=None,
            )
            asyncio.run(DealIngestBot.on_raw_message_delete(bot, payload))
        finally:
            for item in patches:
                item.stop()

        with Session(self.engine) as session:
            row = session.exec(
                select(DiscordMessage).where(DiscordMessage.discord_message_id == "304")
            ).one()
            self.assertTrue(row.is_deleted)
            self.assertEqual(row.parse_status, PARSE_IGNORED)

    def test_edit_lazily_captures_revisions_and_atomically_tombstones_stale_money(self):
        original_attachments = '["https://cdn.discord.com/original.png"]'
        edited_attachment_url = "https://cdn.discord.com/edited.png"
        edited_attachments = json.dumps([edited_attachment_url])

        with Session(self.engine) as session:
            row = DiscordMessage(
                discord_message_id="111",
                channel_id="999",
                channel_name="deals",
                author_id="777",
                author_name="Trader#0001",
                content="$50 buy",
                attachment_urls_json=original_attachments,
                parse_status=PARSE_PARSED,
                parse_attempts=2,
                deal_type="buy",
                entry_kind="buy",
                amount=50.0,
                money_out=50.0,
                item_names_json='["Original Card"]',
                created_at=_utcnow(),
            )
            session.add(row)
            session.flush()

            transaction = Transaction(
                source_message_id=row.id,
                source_revision_id=None,
                occurred_at=row.created_at,
                parse_status=PARSE_PARSED,
                source_content=row.content,
                amount=50.0,
                money_out=50.0,
            )
            session.add(transaction)
            session.flush()
            transaction_id = transaction.id
            session.add(
                TransactionItem(
                    transaction_id=transaction.id,
                    direction="named",
                    item_name="Original Card",
                )
            )

            bookkeeping_import = BookkeepingImport(show_label="audit")
            session.add(bookkeeping_import)
            session.flush()
            bookkeeping_entry = BookkeepingEntry(
                import_id=bookkeeping_import.id,
                row_index=1,
                matched_transaction_id=transaction.id,
                match_status="matched_strong",
            )
            session.add(bookkeeping_entry)

            bank_import = BankStatementImport(
                label="audit",
                account_label="Checking",
                account_type="checking",
            )
            session.add(bank_import)
            session.flush()
            bank_transaction = BankTransaction(
                import_id=bank_import.id,
                row_index=1,
                account_label="Checking",
                classification="logged_in_discord_strong",
                confidence="high",
                matched_transaction_id=transaction.id,
                matched_source_message_id=row.id,
                matched_platform="discord",
                match_reason="Original match",
            )
            session.add(bank_transaction)
            session.commit()
            bookkeeping_entry_id = bookkeeping_entry.id
            bank_transaction_id = bank_transaction.id

        msg = _fake_message(
            msg_id="111",
            content="$55 buy edited",
            attachments=[_fake_attachment(url=edited_attachment_url)],
            edited_at=_utcnow(),
        )
        patches = self._patch()
        for p in patches:
            p.start()
        try:
            tracked, action = insert_or_update_message(
                msg,
                is_edit=True,
                watched_channel_ids=self._WATCHED_CHANNEL_IDS,
            )
        finally:
            for p in patches:
                p.stop()

        self.assertTrue(tracked)
        self.assertEqual(action, "updated")
        with Session(self.engine) as session:
            row = session.exec(select(DiscordMessage)).one()
            revisions = list(
                session.exec(
                    select(DiscordMessageRevision).order_by(
                        DiscordMessageRevision.revision_number
                    )
                ).all()
            )
            transaction = session.get(Transaction, transaction_id)
            items = list(
                session.exec(
                    select(TransactionItem).where(
                        TransactionItem.transaction_id == transaction_id
                    )
                ).all()
            )
            bookkeeping_entry = session.get(BookkeepingEntry, bookkeeping_entry_id)
            bank_transaction = session.get(BankTransaction, bank_transaction_id)

        self.assertEqual(
            [(revision.content, revision.attachment_urls_json) for revision in revisions],
            [
                ("$50 buy", original_attachments),
                ("$55 buy edited", edited_attachments),
            ],
        )
        self.assertEqual([revision.revision_number for revision in revisions], [1, 2])
        self.assertEqual(row.current_revision_id, revisions[1].id)
        self.assertEqual(transaction.id, transaction_id)
        self.assertEqual(transaction.source_revision_id, revisions[0].id)
        self.assertEqual(transaction.source_content, "$50 buy")
        self.assertTrue(transaction.is_deleted)
        self.assertTrue(transaction.needs_review)
        self.assertEqual(transaction.parse_status, PARSE_PENDING)
        self.assertEqual(items, [])
        self.assertIsNone(bookkeeping_entry.matched_transaction_id)
        self.assertEqual(bookkeeping_entry.match_status, "unmatched")
        self.assertIsNone(bank_transaction.matched_transaction_id)
        self.assertIsNone(bank_transaction.matched_source_message_id)
        self.assertIsNone(bank_transaction.matched_platform)
        self.assertEqual(bank_transaction.classification, "needs_review")
        self.assertEqual(bank_transaction.confidence, "low")

    def test_identical_edit_dedupes_revision_snapshot_while_resetting_parse_state(self):
        attachment_url = "https://cdn.discord.com/same.png"
        msg = _fake_message(
            content="$50 buy",
            attachments=[_fake_attachment(url=attachment_url)],
        )
        patches = self._patch()
        for p in patches:
            p.start()
        try:
            insert_or_update_message(
                msg,
                is_edit=False,
                watched_channel_ids=self._WATCHED_CHANNEL_IDS,
            )
            identical_edit = _fake_message(
                content="$50 buy",
                attachments=[_fake_attachment(url=attachment_url)],
                edited_at=_utcnow(),
            )
            insert_or_update_message(
                identical_edit,
                is_edit=True,
                watched_channel_ids=self._WATCHED_CHANNEL_IDS,
            )
        finally:
            for p in patches:
                p.stop()

        with Session(self.engine) as session:
            row = session.exec(select(DiscordMessage)).one()
            revisions = list(session.exec(select(DiscordMessageRevision)).all())

        self.assertEqual(len(revisions), 1)
        self.assertEqual(row.current_revision_id, revisions[0].id)
        self.assertEqual(revisions[0].attachment_urls_json, json.dumps([attachment_url]))
        self.assertEqual(row.parse_status, PARSE_PENDING)

    def test_failed_reparse_leaves_old_transaction_tombstoned_on_old_revision(self):
        with Session(self.engine) as session:
            row = DiscordMessage(
                discord_message_id="111",
                channel_id="999",
                channel_name="deals",
                author_id="777",
                author_name="Trader#0001",
                content="$50 buy",
                attachment_urls_json="[]",
                parse_status=PARSE_PARSED,
                deal_type="buy",
                entry_kind="buy",
                amount=50.0,
                money_out=50.0,
                created_at=_utcnow(),
            )
            session.add(row)
            session.flush()
            transaction = Transaction(
                source_message_id=row.id,
                occurred_at=row.created_at,
                parse_status=PARSE_PARSED,
                source_content=row.content,
                amount=50.0,
                money_out=50.0,
            )
            session.add(transaction)
            session.commit()
            transaction_id = transaction.id

        patches = self._patch()
        for p in patches:
            p.start()
        try:
            insert_or_update_message(
                _fake_message(content="$55 edited"),
                is_edit=True,
                watched_channel_ids=self._WATCHED_CHANNEL_IDS,
            )
        finally:
            for p in patches:
                p.stop()

        with Session(self.engine) as session:
            row = session.exec(select(DiscordMessage)).one()
            revisions = list(
                session.exec(
                    select(DiscordMessageRevision).order_by(
                        DiscordMessageRevision.revision_number
                    )
                ).all()
            )
            row.parse_status = PARSE_FAILED
            row.last_error = "simulated parser failure"
            session.add(row)
            session.commit()
            original_revision_id = revisions[0].id
            edited_revision_id = revisions[1].id

        with Session(self.engine) as session:
            row = session.exec(select(DiscordMessage)).one()
            transaction = session.get(Transaction, transaction_id)
            current_revision_id = row.current_revision_id
            transaction_is_deleted = transaction.is_deleted
            transaction_source_revision_id = transaction.source_revision_id
            transaction_source_content = transaction.source_content

        self.assertEqual(row.parse_status, PARSE_FAILED)
        self.assertEqual(current_revision_id, edited_revision_id)
        self.assertTrue(transaction_is_deleted)
        self.assertEqual(transaction_source_revision_id, original_revision_id)
        self.assertEqual(transaction_source_content, "$50 buy")

    def test_edit_rolls_back_projection_and_revisions_when_new_revision_capture_raises(self):
        with Session(self.engine) as session:
            row = DiscordMessage(
                discord_message_id="111",
                channel_id="999",
                content="$50 buy",
                attachment_urls_json="[]",
                parse_status=PARSE_PARSED,
                created_at=_utcnow(),
            )
            session.add(row)
            session.commit()

        call_count = 0

        def fail_second_revision_capture(session, row):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise RuntimeError("simulated revision failure")
            return ensure_message_revision(session, row)

        with patch("app.discord.discord_ingest.managed_session", self._managed_session), \
             patch("app.discord.discord_ingest.sync_attachment_assets"), \
             patch("app.discord.discord_ingest.ingest_log") as ingest_log, \
             patch(
                 "app.discord.message_revisions.ensure_message_revision",
                 side_effect=fail_second_revision_capture,
             ):
            with self.assertRaisesRegex(RuntimeError, "simulated revision failure"):
                insert_or_update_message(
                    _fake_message(content="$55 edited"),
                    is_edit=True,
                    watched_channel_ids=self._WATCHED_CHANNEL_IDS,
                )

        with Session(self.engine) as session:
            row = session.exec(select(DiscordMessage)).one()
            revisions = list(session.exec(select(DiscordMessageRevision)).all())

        self.assertEqual(row.content, "$50 buy")
        self.assertEqual(row.parse_status, PARSE_PARSED)
        self.assertIsNone(row.current_revision_id)
        self.assertEqual(revisions, [])
        self.assertTrue(
            any(
                call.kwargs.get("action") == "message_update_failed"
                and call.kwargs.get("success") is False
                for call in ingest_log.mock_calls
            )
        )

    def test_edit_rolls_back_when_transaction_cleanup_raises(self):
        with Session(self.engine) as session:
            row = DiscordMessage(
                discord_message_id="111",
                channel_id="999",
                content="$50 buy",
                attachment_urls_json="[]",
                parse_status=PARSE_PARSED,
                created_at=_utcnow(),
            )
            session.add(row)
            session.flush()
            transaction = Transaction(
                source_message_id=row.id,
                occurred_at=row.created_at,
                parse_status=PARSE_PARSED,
                source_content=row.content,
            )
            session.add(transaction)
            session.commit()
            transaction_id = transaction.id

        with patch("app.discord.discord_ingest.managed_session", self._managed_session), \
             patch("app.discord.discord_ingest.sync_attachment_assets"), \
             patch("app.discord.discord_ingest.ingest_log") as ingest_log, \
             patch(
                 "app.discord.transactions.cleanup_transaction_dependents",
                 side_effect=RuntimeError("simulated cleanup failure"),
             ):
            with self.assertRaisesRegex(RuntimeError, "simulated cleanup failure"):
                insert_or_update_message(
                    _fake_message(content="$55 edited"),
                    is_edit=True,
                    watched_channel_ids=self._WATCHED_CHANNEL_IDS,
                )

        with Session(self.engine) as session:
            row = session.exec(select(DiscordMessage)).one()
            revisions = list(session.exec(select(DiscordMessageRevision)).all())
            transaction = session.get(Transaction, transaction_id)

        self.assertEqual(row.content, "$50 buy")
        self.assertEqual(row.parse_status, PARSE_PARSED)
        self.assertIsNone(row.current_revision_id)
        self.assertEqual(revisions, [])
        self.assertFalse(transaction.is_deleted)
        self.assertIsNone(transaction.source_revision_id)
        self.assertTrue(
            any(
                call.kwargs.get("action") == "message_update_failed"
                and call.kwargs.get("success") is False
                for call in ingest_log.mock_calls
            )
        )


class MarkMessageDeletedTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self):
        self.engine.dispose()

    def _make_row(self, parse_status=PARSE_PARSED):
        with Session(self.engine) as session:
            row = DiscordMessage(
                discord_message_id="222",
                channel_id="999",
                channel_name="deals",
                author_id="777",
                author_name="Trader#0001",
                content="$50 buy",
                attachment_urls_json="[]",
                parse_status=parse_status,
                created_at=_utcnow(),
            )
            session.add(row)
            session.commit()
            session.refresh(row)
            return row.id

    def test_sets_is_deleted_and_ignored(self):
        row_id = self._make_row()
        with Session(self.engine) as session:
            row = session.get(DiscordMessage, row_id)
            with patch("app.discord.discord_ingest.sync_transaction_from_message"), \
                 patch("app.discord.discord_ingest.ingest_log"):
                result = mark_message_deleted_row(session, row)

        self.assertTrue(result)
        with Session(self.engine) as session:
            row = session.get(DiscordMessage, row_id)
        self.assertTrue(row.is_deleted)
        self.assertEqual(row.parse_status, PARSE_IGNORED)

    def test_double_delete_is_noop(self):
        row_id = self._make_row()
        with Session(self.engine) as session:
            row = session.get(DiscordMessage, row_id)
            with patch("app.discord.discord_ingest.sync_transaction_from_message"), \
                 patch("app.discord.discord_ingest.ingest_log"):
                first = mark_message_deleted_row(session, row)
                second = mark_message_deleted_row(session, row)

        self.assertTrue(first)
        self.assertFalse(second)


class SourceMutationFanoutFailClosedTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path.cwd() / "tests" / ".tmp_discord_fanout" / str(uuid.uuid4())
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = self.temp_dir / "fanout.db"
        self.engine = create_engine(
            f"sqlite:///{db_path.as_posix()}",
            connect_args={"check_same_thread": False},
        )
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self):
        self.engine.dispose()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @contextmanager
    def _managed_session(self):
        with Session(self.engine) as session:
            yield session

    def _seed_association_fanout(self, *, child_discord_id: str = "400") -> dict:
        base_time = datetime(2026, 6, 28, 12, tzinfo=timezone.utc)
        with Session(self.engine) as session:
            child = DiscordMessage(
                discord_message_id=child_discord_id,
                channel_id="999",
                channel_name="deals",
                author_id="777",
                author_name="Trader#0001",
                content="Buy 50 cash",
                attachment_urls_json="[]",
                parse_status=PARSE_IGNORED,
                created_at=base_time + timedelta(minutes=5),
            )
            session.add(child)
            session.flush()
            child_revision = ensure_message_revision(session, child)
            attempt = ParseAttempt(
                message_id=child.id,
                attempt_number=1,
                started_at=base_time,
            )
            session.add(attempt)
            session.flush()
            child.active_parse_attempt_id = attempt.id
            session.add(child)

            transaction_ids: list[int] = []
            for index in range(MAX_TRANSACTION_SOURCE_ROWS + 1):
                primary = DiscordMessage(
                    discord_message_id=f"{child_discord_id}-primary-{index}",
                    channel_id="999",
                    channel_name="deals",
                    author_id="777",
                    author_name="Trader#0001",
                    content=f"Buy card {index} 50 cash",
                    attachment_urls_json="[]",
                    parse_status=PARSE_PARSED,
                    deal_type="buy",
                    entry_kind="buy",
                    payment_method="cash",
                    amount=50.0,
                    money_out=50.0,
                    created_at=base_time + timedelta(seconds=index),
                )
                session.add(primary)
                session.flush()
                primary_revision = ensure_message_revision(session, primary)
                transaction = Transaction(
                    source_message_id=primary.id,
                    source_revision_id=primary_revision.id,
                    discord_message_id=primary.discord_message_id,
                    channel_id=primary.channel_id,
                    channel_name=primary.channel_name,
                    author_name=primary.author_name,
                    occurred_at=primary.created_at,
                    parse_status=PARSE_PARSED,
                    deal_type="buy",
                    entry_kind="buy",
                    payment_method="cash",
                    amount=50.0,
                    money_out=50.0,
                    source_content=primary.content,
                )
                session.add(transaction)
                session.flush()
                transaction_ids.append(transaction.id)
                session.add_all(
                    [
                        TransactionSourceRevision(
                            transaction_id=transaction.id,
                            message_id=primary.id,
                            revision_id=primary_revision.id,
                            source_position=0,
                        ),
                        TransactionSourceRevision(
                            transaction_id=transaction.id,
                            message_id=child.id,
                            revision_id=child_revision.id,
                            source_position=1,
                        ),
                        TransactionItem(
                            transaction_id=transaction.id,
                            direction="in",
                            item_name=f"Card {index}",
                        ),
                    ]
                )
            session.commit()
            return {
                "child_id": child.id,
                "child_revision_id": child_revision.id,
                "attempt_id": attempt.id,
                "transaction_ids": transaction_ids,
            }

    def _assert_fanout_is_quarantined_without_partial_cleanup(self, seeded: dict) -> None:
        with Session(self.engine) as session:
            child = session.get(DiscordMessage, seeded["child_id"])
            attempt = session.get(ParseAttempt, seeded["attempt_id"])
            transactions = session.exec(
                select(Transaction)
                .where(Transaction.id.in_(seeded["transaction_ids"]))
                .order_by(Transaction.id)
            ).all()
            associations = session.exec(
                select(TransactionSourceRevision)
                .where(TransactionSourceRevision.transaction_id.in_(seeded["transaction_ids"]))
                .order_by(
                    TransactionSourceRevision.transaction_id,
                    TransactionSourceRevision.source_position,
                )
            ).all()
            items = session.exec(
                select(TransactionItem).where(
                    TransactionItem.transaction_id.in_(seeded["transaction_ids"])
                )
            ).all()

            self.assertTrue(child.source_refresh_required)
            self.assertTrue(child.needs_review)
            self.assertIn(DISCORD_SOURCE_REFRESH_REQUIRED_ERROR, child.last_error or "")
            self.assertIn("deferred", (child.last_error or "").lower())
            self.assertIsNone(child.active_parse_attempt_id)
            self.assertIsNotNone(attempt.finished_at)
            self.assertFalse(attempt.success)
            self.assertEqual(child.current_revision_id, seeded["child_revision_id"])
            self.assertEqual(child.content, "Buy 50 cash")
            self.assertEqual(len(transactions), MAX_TRANSACTION_SOURCE_ROWS + 1)
            self.assertTrue(all(transaction.is_deleted is False for transaction in transactions))
            self.assertEqual(
                [transaction.source_content for transaction in transactions],
                [f"Buy card {index} 50 cash" for index in range(MAX_TRANSACTION_SOURCE_ROWS + 1)],
            )
            self.assertEqual(len(associations), 2 * (MAX_TRANSACTION_SOURCE_ROWS + 1))
            self.assertEqual(len(items), MAX_TRANSACTION_SOURCE_ROWS + 1)
            self.assertEqual(session.exec(transaction_base_query()).all(), [])
            self.assertEqual(_load_unbanked_cash_transactions(session), [])

    def test_raw_delete_persists_deleted_quarantine_when_fanout_exceeds_bound(self):
        seeded = self._seed_association_fanout(child_discord_id="400")
        with Session(self.engine) as session:
            self.assertEqual(len(session.exec(transaction_base_query()).all()), 33)
            self.assertEqual(len(_load_unbanked_cash_transactions(session)), 33)

        payload = types.SimpleNamespace(message_id=400, channel_id=999, cached_message=None)
        with patch("app.discord.discord_ingest.managed_session", self._managed_session), patch(
            "app.discord.discord_ingest.ingest_log"
        ):
            asyncio.run(DealIngestBot.on_raw_message_delete(types.SimpleNamespace(), payload))

        with Session(self.engine) as session:
            child = session.get(DiscordMessage, seeded["child_id"])
            self.assertTrue(child.is_deleted)
            self.assertIsNotNone(child.deleted_at)
            self.assertEqual(child.parse_status, PARSE_IGNORED)
        self._assert_fanout_is_quarantined_without_partial_cleanup(seeded)

    def test_cached_delete_survives_logging_failure_and_persists_quarantine(self):
        seeded = self._seed_association_fanout(child_discord_id="401")
        cached_message = _fake_message(msg_id="401", content="Buy 50 cash")
        with patch("app.discord.discord_ingest.managed_session", self._managed_session), patch(
            "app.discord.discord_ingest.ingest_log",
            side_effect=RuntimeError("operations log unavailable"),
        ):
            asyncio.run(DealIngestBot.on_message_delete(types.SimpleNamespace(), cached_message))

        with Session(self.engine) as session:
            child = session.get(DiscordMessage, seeded["child_id"])
            self.assertTrue(child.is_deleted)
            self.assertEqual(child.parse_status, PARSE_IGNORED)
        self._assert_fanout_is_quarantined_without_partial_cleanup(seeded)

    def test_bulk_delete_continues_after_corrupt_fanout_and_deletes_safe_row(self):
        seeded = self._seed_association_fanout(child_discord_id="402")
        with Session(self.engine) as session:
            safe = DiscordMessage(
                discord_message_id="403",
                channel_id="999",
                channel_name="deals",
                author_id="777",
                author_name="Trader#0001",
                content="Sell 25 cash",
                attachment_urls_json="[]",
                parse_status=PARSE_PARSED,
                deal_type="sell",
                entry_kind="sale",
                payment_method="cash",
                amount=25.0,
                money_in=25.0,
                created_at=datetime(2026, 6, 28, 13, tzinfo=timezone.utc),
            )
            session.add(safe)
            session.flush()
            safe_transaction = discord_ingest_module.sync_transaction_from_message(session, safe)
            session.commit()
            safe_id = safe.id
            safe_transaction_id = safe_transaction.id

        payload = types.SimpleNamespace(message_ids={402, 403})
        with patch("app.discord.discord_ingest.managed_session", self._managed_session), patch(
            "app.discord.discord_ingest.ingest_log"
        ):
            asyncio.run(DealIngestBot.on_raw_bulk_message_delete(types.SimpleNamespace(), payload))

        with Session(self.engine) as session:
            corrupt = session.get(DiscordMessage, seeded["child_id"])
            safe = session.get(DiscordMessage, safe_id)
            safe_transaction = session.get(Transaction, safe_transaction_id)
            self.assertTrue(corrupt.is_deleted)
            self.assertTrue(corrupt.source_refresh_required)
            self.assertTrue(safe.is_deleted)
            self.assertFalse(safe.source_refresh_required)
            self.assertTrue(safe_transaction.is_deleted)
        self._assert_fanout_is_quarantined_without_partial_cleanup(seeded)

    def test_canonical_edit_persists_new_projection_but_quarantines_failed_cleanup(self):
        seeded = self._seed_association_fanout(child_discord_id="404")
        edited = _fake_message(
            msg_id="404",
            content="Buy 75 cash",
            attachments=[_fake_attachment(url="https://cdn.discord.com/edited.png")],
            edited_at=datetime(2026, 6, 28, 14, tzinfo=timezone.utc),
        )
        with patch("app.discord.discord_ingest.managed_session", self._managed_session), patch(
            "app.discord.discord_ingest.sync_attachment_assets"
        ), patch("app.discord.discord_ingest.ingest_log"):
            self.assertEqual(
                insert_or_update_message(
                    edited,
                    is_edit=True,
                    watched_channel_ids=set(),
                    canonical_source_refresh=True,
                ),
                (False, "refresh_required"),
            )

            # A repeated canonical refresh may retry cleanup, but it cannot
            # release the integrity quarantine while the fanout is still corrupt.
            self.assertEqual(
                insert_or_update_message(
                    edited,
                    is_edit=True,
                    watched_channel_ids=set(),
                    canonical_source_refresh=True,
                ),
                (False, "refresh_required"),
            )

        with Session(self.engine) as session:
            child = session.get(DiscordMessage, seeded["child_id"])
            revisions = session.exec(
                select(DiscordMessageRevision)
                .where(DiscordMessageRevision.message_id == child.id)
                .order_by(DiscordMessageRevision.revision_number)
            ).all()
            self.assertFalse(child.is_deleted)
            self.assertTrue(child.source_refresh_required)
            self.assertEqual(child.content, "Buy 75 cash")
            self.assertEqual(
                child.attachment_urls_json,
                json.dumps(["https://cdn.discord.com/edited.png"]),
            )
            self.assertEqual([revision.revision_number for revision in revisions], [1, 2])
            self.assertEqual(child.current_revision_id, revisions[-1].id)
            self.assertEqual(len(session.exec(transaction_base_query()).all()), 0)

        # The helper expects the original child projection; the transaction and
        # association invariants are checked explicitly here instead.
        with Session(self.engine) as session:
            transactions = session.exec(
                select(Transaction).where(Transaction.id.in_(seeded["transaction_ids"]))
            ).all()
            associations = session.exec(
                select(TransactionSourceRevision).where(
                    TransactionSourceRevision.transaction_id.in_(seeded["transaction_ids"])
                )
            ).all()
            self.assertTrue(all(transaction.is_deleted is False for transaction in transactions))
            self.assertEqual(len(associations), 2 * (MAX_TRANSACTION_SOURCE_ROWS + 1))

    def test_canonical_refresh_cannot_resurrect_unresolved_fanout_delete(self):
        seeded = self._seed_association_fanout(child_discord_id="405")
        payload = types.SimpleNamespace(message_id=405, channel_id=999, cached_message=None)
        refreshed = _fake_message(
            msg_id="405",
            content="Buy 80 cash",
            edited_at=datetime(2026, 6, 28, 15, tzinfo=timezone.utc),
        )
        with patch("app.discord.discord_ingest.managed_session", self._managed_session), patch(
            "app.discord.discord_ingest.sync_attachment_assets"
        ), patch("app.discord.discord_ingest.ingest_log"):
            asyncio.run(DealIngestBot.on_raw_message_delete(types.SimpleNamespace(), payload))
            with Session(self.engine) as session:
                deleted_at = session.get(DiscordMessage, seeded["child_id"]).deleted_at

            self.assertEqual(
                insert_or_update_message(
                    refreshed,
                    is_edit=True,
                    watched_channel_ids=set(),
                    canonical_source_refresh=True,
                ),
                (False, "refresh_required"),
            )

        with Session(self.engine) as session:
            child = session.get(DiscordMessage, seeded["child_id"])
            self.assertTrue(child.is_deleted)
            self.assertEqual(child.deleted_at, deleted_at)
            self.assertEqual(child.parse_status, PARSE_IGNORED)
            self.assertTrue(child.source_refresh_required)
            self.assertEqual(session.exec(transaction_base_query()).all(), [])


class GetAttachmentPayloadsTests(unittest.TestCase):
    def test_returns_empty_for_no_attachments(self):
        msg = _fake_message(attachments=[])
        self.assertEqual(get_attachment_payloads(msg), [])

    def test_extracts_image_by_content_type(self):
        att = _fake_attachment(url="https://cdn/img.png", filename="img.png", content_type="image/png")
        msg = _fake_message(attachments=[att])
        payloads = get_attachment_payloads(msg)
        self.assertEqual(len(payloads), 1)
        self.assertTrue(payloads[0]["is_image"])
        self.assertEqual(payloads[0]["url"], "https://cdn/img.png")

    def test_extracts_image_by_filename_extension(self):
        att = _fake_attachment(url="https://cdn/photo.jpg", filename="photo.jpg", content_type=None)
        msg = _fake_message(attachments=[att])
        payloads = get_attachment_payloads(msg)
        self.assertTrue(payloads[0]["is_image"])

    def test_non_image_attachment_is_false(self):
        att = _fake_attachment(url="https://cdn/doc.pdf", filename="doc.pdf", content_type="application/pdf")
        msg = _fake_message(attachments=[att])
        payloads = get_attachment_payloads(msg)
        self.assertFalse(payloads[0]["is_image"])

    def test_multiple_attachments(self):
        atts = [
            _fake_attachment(url="https://cdn/a.png", filename="a.png", content_type="image/png"),
            _fake_attachment(url="https://cdn/b.pdf", filename="b.pdf", content_type="application/pdf"),
        ]
        msg = _fake_message(attachments=atts)
        payloads = get_attachment_payloads(msg)
        self.assertEqual(len(payloads), 2)
        self.assertTrue(payloads[0]["is_image"])
        self.assertFalse(payloads[1]["is_image"])


class AvailableDiscordChannelInventoryTests(unittest.TestCase):
    def setUp(self):
        invalidate_available_channels_cache()

    def tearDown(self):
        invalidate_available_channels_cache()

    def _fake_client(self, guild=None):
        class FakeLoop:
            def is_closed(self):
                return False

        return types.SimpleNamespace(
            guilds=[guild or types.SimpleNamespace(id=1, name="Degen", text_channels=[])],
            loop=FakeLoop(),
            is_closed=lambda: False,
            is_ready=lambda: True,
        )

    def _fake_rest_channel(self, channel_id="222", name="2026-may-9-10-eastbaycardshow"):
        guild = types.SimpleNamespace(id=111, name="Degen Guild", text_channels=[])
        channel = types.SimpleNamespace(
            id=int(channel_id),
            name=name,
            created_at=datetime(2026, 5, 9, tzinfo=timezone.utc),
            last_message_id=None,
        )
        return guild, channel

    def _run_coroutine_threadsafe_immediately(self, coro, _loop):
        class FakeFuture:
            def __init__(self, value):
                self.value = value
                self.cancelled = False

            def result(self, timeout=None):
                return self.value

            def cancel(self):
                self.cancelled = True

        return FakeFuture(asyncio.run(coro))

    def test_normal_cache_miss_uses_rest_channel_inventory(self):
        guild, channel = self._fake_rest_channel()

        async def fake_fetch(_client):
            return [(guild, channel, "Show Deals")], True

        with patch("app.discord.discord_ingest.get_discord_client", return_value=self._fake_client(guild)), patch(
            "app.discord.discord_ingest._fetch_live_guild_channels_rest", side_effect=fake_fetch
        ) as fetch_mock, patch(
            "app.discord.discord_ingest.asyncio.run_coroutine_threadsafe",
            side_effect=self._run_coroutine_threadsafe_immediately,
        ), patch(
            "app.discord.discord_ingest.persist_available_discord_channels"
        ) as persist_mock, patch(
            "app.discord.discord_ingest.get_cached_available_discord_channels", return_value=[]
        ):
            channels = list_available_discord_channels()

        self.assertEqual(fetch_mock.call_count, 1)
        self.assertEqual([row["channel_id"] for row in channels], ["222"])
        self.assertEqual(channels[0]["label"], "Show Deals / #2026-may-9-10-eastbaycardshow")
        persist_mock.assert_called_once()
        self.assertTrue(persist_mock.call_args.kwargs["remove_missing"])

    def test_in_memory_cache_prevents_repeated_rest_fetch_until_forced(self):
        guild, channel = self._fake_rest_channel()

        async def fake_fetch(_client):
            return [(guild, channel, "Show Deals")], True

        with patch("app.discord.discord_ingest.get_discord_client", return_value=self._fake_client(guild)), patch(
            "app.discord.discord_ingest._fetch_live_guild_channels_rest", side_effect=fake_fetch
        ) as fetch_mock, patch(
            "app.discord.discord_ingest.asyncio.run_coroutine_threadsafe",
            side_effect=self._run_coroutine_threadsafe_immediately,
        ), patch(
            "app.discord.discord_ingest.persist_available_discord_channels"
        ), patch(
            "app.discord.discord_ingest.get_cached_available_discord_channels", return_value=[]
        ):
            first = list_available_discord_channels()
            second = list_available_discord_channels()
            forced = list_available_discord_channels(force_refresh=True)

        self.assertEqual(fetch_mock.call_count, 2)
        self.assertEqual(first, second)
        self.assertEqual(forced, first)

    def test_non_authoritative_fallback_keeps_persisted_inventory(self):
        guild, channel = self._fake_rest_channel(channel_id="333", name="offline-deals")
        cached_private_channel = {
            "guild_id": "111",
            "guild_name": "Degen Guild",
            "channel_id": "444",
            "channel_name": "2026-may-9-10-eastbaycardshow",
            "category_name": "Show Deals",
            "label": "Show Deals / #2026-may-9-10-eastbaycardshow",
            "created_at": None,
            "last_message_at": None,
        }

        async def fake_fetch(_client):
            return [(guild, channel, "Offline Deals")], False

        with patch("app.discord.discord_ingest.get_discord_client", return_value=self._fake_client(guild)), patch(
            "app.discord.discord_ingest._fetch_live_guild_channels_rest", side_effect=fake_fetch
        ), patch(
            "app.discord.discord_ingest.asyncio.run_coroutine_threadsafe",
            side_effect=self._run_coroutine_threadsafe_immediately,
        ), patch(
            "app.discord.discord_ingest.persist_available_discord_channels"
        ) as persist_mock, patch(
            "app.discord.discord_ingest.get_cached_available_discord_channels", return_value=[cached_private_channel]
        ):
            channels = list_available_discord_channels()

        self.assertEqual({row["channel_id"] for row in channels}, {"333", "444"})
        self.assertFalse(persist_mock.call_args.kwargs["remove_missing"])

    def test_rest_fetch_resolves_category_name_from_fetched_categories(self):
        class FakeCategory:
            def __init__(self):
                self.id = 10
                self.name = "Show Deals"

        class FakeTextChannel:
            def __init__(self):
                self.id = 555
                self.name = "2026-may-9-10-eastbaycardshow"
                self.category = None
                self.category_id = 10

        category = FakeCategory()
        text_channel = FakeTextChannel()

        class FakeGuild:
            id = 111
            text_channels = []

            async def fetch_channels(self):
                return [category, text_channel]

        guild = FakeGuild()
        with patch.object(discord_ingest_module.discord, "CategoryChannel", FakeCategory), patch.object(
            discord_ingest_module.discord, "TextChannel", FakeTextChannel
        ):
            pairs, authoritative = asyncio.run(
                discord_ingest_module._fetch_live_guild_channels_rest(
                    types.SimpleNamespace(guilds=[guild])
                )
            )

        self.assertTrue(authoritative)
        self.assertEqual(pairs, [(guild, text_channel, "Show Deals")])

    def test_financials_channels_are_discoverable_for_ledger_ingest(self):
        guild = types.SimpleNamespace(id=111, name="Degen Guild")
        financials = types.SimpleNamespace(
            id=7001,
            name="financials",
            created_at=datetime(2026, 5, 20, tzinfo=timezone.utc),
            last_message_id=None,
        )
        loans = types.SimpleNamespace(
            id=7002,
            name="loans",
            created_at=datetime(2026, 5, 20, tzinfo=timezone.utc),
            last_message_id=None,
        )

        rows = discord_ingest_module._build_available_discord_channel_rows(
            [
                (guild, financials, "Financials"),
                (guild, loans, "Financials"),
            ]
        )

        self.assertEqual({row["channel_id"] for row in rows}, {"7001", "7002"})
        self.assertEqual(
            {row["label"] for row in rows},
            {"Financials / #financials", "Financials / #loans"},
        )

    def test_purchase_channels_are_discoverable_by_name_hint(self):
        guild = types.SimpleNamespace(id=111, name="Degen Guild")
        purchase_channel = types.SimpleNamespace(
            id=7101,
            name="alex-purchases",
            created_at=datetime(2026, 5, 20, tzinfo=timezone.utc),
            last_message_id=None,
        )

        rows = discord_ingest_module._build_available_discord_channel_rows(
            [(guild, purchase_channel, "Employees")]
        )

        self.assertEqual({row["channel_id"] for row in rows}, {"7101"})
        self.assertEqual(rows[0]["label"], "Employees / #alex-purchases")

    def test_year_past_shows_channels_are_discoverable_for_ledger_ingest(self):
        guild = types.SimpleNamespace(id=111, name="Degen Guild")
        show_channel = types.SimpleNamespace(
            id=8001,
            name="2026-may-9-eastbaycardshow",
            created_at=datetime(2026, 5, 9, tzinfo=timezone.utc),
            last_message_id=None,
        )

        rows = discord_ingest_module._build_available_discord_channel_rows(
            [(guild, show_channel, "2026 Past Shows")]
        )

        self.assertEqual({row["channel_id"] for row in rows}, {"8001"})
        self.assertEqual(rows[0]["category_name"], "2026 Past Shows")
        self.assertEqual(rows[0]["label"], "2026 Past Shows / #2026-may-9-eastbaycardshow")


class AvailableDiscordChannelPersistenceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path.cwd() / "tests" / ".tmp_channel_inventory" / str(uuid.uuid4())
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = self.temp_dir / "channels.db"
        self.engine = create_engine(
            f"sqlite:///{db_path.as_posix()}",
            connect_args={"check_same_thread": False},
        )
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self):
        self.engine.dispose()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @contextmanager
    def _managed_session(self):
        with Session(self.engine) as session:
            yield session

    def _channel(self, channel_id: str, *, category_name: str, channel_name: str) -> dict:
        return {
            "guild_id": "1",
            "guild_name": "Degen Guild",
            "channel_id": channel_id,
            "channel_name": channel_name,
            "category_name": category_name,
            "label": f"{category_name} / #{channel_name}",
            "created_at": None,
            "last_message_at": None,
        }

    def _persist(self, channels: list[dict]) -> None:
        with patch("app.discord.discord_ingest.managed_session", self._managed_session):
            persist_available_discord_channels(channels)

    def test_auto_adds_new_show_deals_channel_as_backfill_ready(self):
        self._persist([
            self._channel("1001", category_name="Show Deals", channel_name="2026-may-eastbaycardshow")
        ])

        with Session(self.engine) as session:
            available = session.exec(select(AvailableDiscordChannel)).one()
            watched = session.exec(select(WatchedChannel)).one()

        self.assertEqual(available.channel_id, "1001")
        self.assertEqual(watched.channel_id, "1001")
        self.assertEqual(watched.channel_name, "Show Deals / #2026-may-eastbaycardshow")
        self.assertTrue(watched.is_enabled)
        self.assertTrue(watched.backfill_enabled)
        self.assertIsNone(watched.backfill_after)
        self.assertIsNone(watched.backfill_before)

    def test_auto_adds_year_past_shows_channels_as_backfill_ready(self):
        self._persist([
            self._channel("2001", category_name="2026 Past Shows", channel_name="2026-may-9-eastbaycardshow")
        ])

        with Session(self.engine) as session:
            available = session.exec(select(AvailableDiscordChannel)).one()
            watched = session.exec(select(WatchedChannel)).one()

        self.assertEqual(available.channel_id, "2001")
        self.assertEqual(watched.channel_id, "2001")
        self.assertEqual(watched.channel_name, "2026 Past Shows / #2026-may-9-eastbaycardshow")
        self.assertTrue(watched.is_enabled)
        self.assertTrue(watched.backfill_enabled)
        self.assertIsNone(watched.backfill_after)
        self.assertIsNone(watched.backfill_before)

    def test_does_not_auto_add_other_deal_categories(self):
        self._persist([
            self._channel("2002", category_name="Offline Deals", channel_name="offline-deals"),
            self._channel("2003", category_name="Employees", channel_name="employee-deals"),
        ])

        with Session(self.engine) as session:
            watched_rows = session.exec(select(WatchedChannel)).all()
            available_rows = session.exec(select(AvailableDiscordChannel)).all()

        self.assertEqual(watched_rows, [])
        self.assertEqual({row.channel_id for row in available_rows}, {"2002", "2003"})

    def test_auto_adds_offline_purchase_channels_as_backfill_ready(self):
        self._persist([
            self._channel("7101", category_name="Offline Deals", channel_name="jeff-purchases")
        ])

        with Session(self.engine) as session:
            watched = session.exec(select(WatchedChannel)).one()
            available = session.exec(select(AvailableDiscordChannel)).one()

        self.assertEqual(available.channel_id, "7101")
        self.assertEqual(watched.channel_id, "7101")
        self.assertEqual(watched.channel_name, "Offline Deals / #jeff-purchases")
        self.assertTrue(watched.is_enabled)
        self.assertTrue(watched.backfill_enabled)

    def test_preserves_existing_channel_flags_and_backfill_windows(self):
        after = datetime(2026, 4, 1, tzinfo=timezone.utc)
        before = datetime(2026, 4, 30, tzinfo=timezone.utc)
        with Session(self.engine) as session:
            session.add(
                WatchedChannel(
                    channel_id="3001",
                    channel_name="Old Label",
                    is_enabled=False,
                    backfill_enabled=False,
                    backfill_after=after,
                    backfill_before=before,
                )
            )
            session.commit()

        self._persist([
            self._channel("3001", category_name="Show Deals", channel_name="renamed-cardshow-deals")
        ])

        with Session(self.engine) as session:
            watched = session.exec(select(WatchedChannel)).one()

        self.assertEqual(watched.channel_name, "Show Deals / #renamed-cardshow-deals")
        self.assertFalse(watched.is_enabled)
        self.assertFalse(watched.backfill_enabled)
        self.assertEqual(watched.backfill_after, after.replace(tzinfo=None))
        self.assertEqual(watched.backfill_before, before.replace(tzinfo=None))

    def test_show_deals_auto_add_is_idempotent(self):
        channel = self._channel("4001", category_name="Show Deals", channel_name="2026-show-deals")

        self._persist([channel])
        self._persist([channel])

        with Session(self.engine) as session:
            watched_rows = session.exec(select(WatchedChannel)).all()
            available_rows = session.exec(select(AvailableDiscordChannel)).all()

        self.assertEqual(len(watched_rows), 1)
        self.assertEqual(watched_rows[0].channel_id, "4001")
        self.assertEqual(len(available_rows), 1)

    def test_auto_adds_financials_channels_as_backfill_ready(self):
        self._persist(
            [
                self._channel("7001", category_name="Financials", channel_name="financials"),
                self._channel("7002", category_name="Financials", channel_name="loans"),
            ]
        )

        with Session(self.engine) as session:
            watched_rows = session.exec(select(WatchedChannel)).all()
            available_rows = session.exec(select(AvailableDiscordChannel)).all()

        watched_by_id = {row.channel_id: row for row in watched_rows}
        self.assertEqual(set(watched_by_id), {"7001", "7002"})
        self.assertEqual({row.channel_id for row in available_rows}, {"7001", "7002"})
        self.assertEqual(watched_by_id["7001"].channel_name, "Financials / #financials")
        self.assertEqual(watched_by_id["7002"].channel_name, "Financials / #loans")
        self.assertTrue(watched_by_id["7001"].is_enabled)
        self.assertTrue(watched_by_id["7002"].is_enabled)
        self.assertTrue(watched_by_id["7001"].backfill_enabled)
        self.assertTrue(watched_by_id["7002"].backfill_enabled)


class ShowDealsAutoWatchMessageTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path.cwd() / "tests" / ".tmp_channel_inventory" / str(uuid.uuid4())
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = self.temp_dir / "message_auto_watch.db"
        self.engine = create_engine(
            f"sqlite:///{db_path.as_posix()}",
            connect_args={"check_same_thread": False},
        )
        SQLModel.metadata.create_all(self.engine)
        invalidate_available_channels_cache()

    def tearDown(self):
        invalidate_available_channels_cache()
        self.engine.dispose()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @contextmanager
    def _managed_session(self):
        with Session(self.engine) as session:
            yield session

    def test_new_show_deals_message_auto_watches_channel_before_ingest(self):
        guild = types.SimpleNamespace(id=111, name="Degen Guild")
        channel = types.SimpleNamespace(
            id=5001,
            name="2026-may-16-westgate",
            guild=guild,
            category=types.SimpleNamespace(name="Show Deals"),
            category_id=10,
            created_at=datetime(2026, 5, 16, tzinfo=timezone.utc),
            last_message_id=9001,
        )
        message = _fake_message(
            msg_id="9001",
            content="Buy $190",
            channel_id="5001",
            channel_name="2026-may-16-westgate",
            attachments=[_fake_attachment()],
        )
        message.channel = channel

        async def noop_auto_import(_message):
            return None

        with patch("app.discord.discord_ingest.managed_session", self._managed_session), patch(
            "app.discord.discord_ingest.maybe_auto_import_bookkeeping_message",
            side_effect=noop_auto_import,
        ), patch("app.discord.discord_ingest.sync_attachment_assets"):
            bot = discord_ingest_module.DealIngestBot(
                intents=discord_ingest_module.discord.Intents.none()
            )
            asyncio.run(bot.on_message(message))

        with Session(self.engine) as session:
            watched = session.exec(select(WatchedChannel)).one_or_none()
            available = session.exec(select(AvailableDiscordChannel)).one_or_none()
            stored_message = session.exec(select(DiscordMessage)).one_or_none()

        self.assertIsNotNone(watched)
        self.assertEqual(watched.channel_id, "5001")
        self.assertTrue(watched.is_enabled)
        self.assertTrue(watched.backfill_enabled)
        self.assertEqual(watched.channel_name, "Show Deals / #2026-may-16-westgate")
        self.assertIsNotNone(available)
        self.assertEqual(available.channel_name, "2026-may-16-westgate")
        self.assertIsNotNone(stored_message)
        self.assertEqual(stored_message.channel_id, "5001")

    def test_backfill_auto_imports_bookkeeping_sheet_messages(self):
        guild = types.SimpleNamespace(id=111, name="Degen Guild")

        class FakeChannel:
            def __init__(self):
                self.id = 5002
                self.name = "2026-may-31-card-party"
                self.guild = guild

            def history(self, **_kwargs):
                async def _messages():
                    message = _fake_message(
                        msg_id="9002",
                        content="May 31 sales log https://docs.google.com/spreadsheets/d/sheet-123/edit#gid=0",
                        channel_id="5002",
                        channel_name=self.name,
                    )
                    message.channel = self
                    message.guild = self.guild
                    yield message

                return _messages()

        imported_messages = []

        async def fake_auto_import(message):
            imported_messages.append(str(message.id))

        with patch("app.discord.discord_ingest.managed_session", self._managed_session), patch(
            "app.discord.discord_ingest.get_enabled_channel_ids", return_value={5002}
        ), patch(
            "app.discord.discord_ingest.maybe_auto_import_bookkeeping_message",
            side_effect=fake_auto_import,
        ), patch("app.discord.discord_ingest.sync_attachment_assets"):
            bot = discord_ingest_module.DealIngestBot(
                intents=discord_ingest_module.discord.Intents.none()
            )
            with patch.object(bot, "get_channel", return_value=FakeChannel()):
                result = asyncio.run(bot.backfill_channel(5002, limit=1))

        self.assertTrue(result["ok"])
        self.assertEqual(result["inserted"], 1)
        self.assertEqual(imported_messages, ["9002"])


class AvailableChannelChoiceTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self):
        self.engine.dispose()

    def test_admin_choices_merge_live_and_cached_available_inventory(self):
        live_channel = {
            "guild_id": "1",
            "guild_name": "Degen Guild",
            "channel_id": "111",
            "channel_name": "offline-deals",
            "category_name": "Offline Deals",
            "label": "Offline Deals / #offline-deals",
            "created_at": None,
            "last_message_at": None,
        }
        cached_channel = {
            "guild_id": "1",
            "guild_name": "Degen Guild",
            "channel_id": "222",
            "channel_name": "2026-may-9-10-eastbaycardshow",
            "category_name": "Show Deals",
            "label": "Show Deals / #2026-may-9-10-eastbaycardshow",
            "created_at": None,
            "last_message_at": None,
        }

        with Session(self.engine) as session, patch(
            "app.discord.channels.list_available_discord_channels", return_value=[live_channel]
        ), patch("app.discord.channels.get_cached_available_discord_channels", return_value=[cached_channel]):
            choices, has_live = get_available_channel_choices(session)

        self.assertTrue(has_live)
        self.assertEqual({row["channel_id"] for row in choices}, {"111", "222"})

    def test_admin_choices_use_cached_available_inventory_before_generic_fallback(self):
        cached_channel = {
            "guild_id": "1",
            "guild_name": "Degen Guild",
            "channel_id": "222",
            "channel_name": "2026-may-9-10-eastbaycardshow",
            "category_name": "Show Deals",
            "label": "Show Deals / #2026-may-9-10-eastbaycardshow",
            "created_at": None,
            "last_message_at": None,
        }

        with Session(self.engine) as session, patch(
            "app.discord.channels.list_available_discord_channels", return_value=[]
        ), patch("app.discord.channels.get_cached_available_discord_channels", return_value=[cached_channel]):
            choices, has_live = get_available_channel_choices(session)

        self.assertFalse(has_live)
        self.assertEqual(choices, [cached_channel])


if __name__ == "__main__":
    unittest.main()
