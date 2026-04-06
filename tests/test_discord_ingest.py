import json
import shutil
import types
import unittest
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from sqlmodel import SQLModel, Session, create_engine, select

from app.discord_ingest import (
    get_attachment_payloads,
    insert_or_update_message,
    mark_message_deleted_row,
)
from app.models import DiscordMessage, PARSE_IGNORED, PARSE_PARSED, PARSE_PENDING


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
            patch("app.discord_ingest.managed_session", self._managed_session),
            patch("app.discord_ingest.sync_attachment_assets"),
            patch("app.discord_ingest.ingest_log"),
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
        self.assertIsNotNone(row)
        self.assertEqual(row.parse_status, PARSE_PENDING)
        self.assertEqual(row.content, "$50 buy")

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
            tracked, action = insert_or_update_message(msg, is_edit=True, watched_channel_ids=self._WATCHED_CHANNEL_IDS)
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
            with patch("app.discord_ingest.sync_transaction_from_message"), \
                 patch("app.discord_ingest.ingest_log"):
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
            with patch("app.discord_ingest.sync_transaction_from_message"), \
                 patch("app.discord_ingest.ingest_log"):
                first = mark_message_deleted_row(session, row)
                second = mark_message_deleted_row(session, row)

        self.assertTrue(first)
        self.assertFalse(second)


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


if __name__ == "__main__":
    unittest.main()
