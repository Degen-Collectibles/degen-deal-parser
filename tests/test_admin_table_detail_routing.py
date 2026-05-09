import shutil
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import HTTPException
from sqlmodel import Session, SQLModel, create_engine
from starlette.requests import Request

from app.models import (
    DiscordMessage,
    PARSE_FAILED,
    PARSE_PARSED,
    PARSE_PENDING,
    PARSE_REVIEW_REQUIRED,
    WatchedChannel,
    utcnow,
)
from app.routers.deals import deal_detail_page
from app.routers.messages import mark_incorrect_message_form


def make_request(path: str, *, role: str = "admin") -> Request:
    request = Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "headers": [],
            "scheme": "http",
            "server": ("testserver", 80),
            "client": ("testclient", 50000),
            "root_path": "",
        }
    )
    request.state.current_user = SimpleNamespace(
        username="tester",
        display_name="Test Operator",
        role=role,
    )
    return request


class AdminTableDetailRoutingTests(unittest.TestCase):
    maxDiff = None

    def setUp(self) -> None:
        self.temp_dir = Path.cwd() / "tests" / ".tmp_admin_table_detail_routing" / str(uuid.uuid4())
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = self.temp_dir / "routing.db"
        self.engine = create_engine(
            f"sqlite:///{db_path.as_posix()}",
            connect_args={"check_same_thread": False},
        )
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _message(
        self,
        session: Session,
        *,
        parse_status: str,
        channel_id: str = "chan-stored-only",
        needs_review: bool = False,
    ) -> DiscordMessage:
        row = DiscordMessage(
            discord_message_id=f"disc-{uuid.uuid4()}",
            channel_id=channel_id,
            channel_name="stored only",
            author_name="tester",
            content=f"{parse_status} message",
            created_at=utcnow(),
            parse_status=parse_status,
            needs_review=needs_review,
        )
        session.add(row)
        session.commit()
        session.refresh(row)
        return row

    def _detail(
        self,
        session: Session,
        row: DiscordMessage,
        *,
        return_path: str,
    ):
        return deal_detail_page(
            message_id=row.id,
            request=make_request(f"/deals/{row.id}"),
            return_path=return_path,
            status=None,
            channel_id=None,
            entry_kind=None,
            expense_category=None,
            after=None,
            before=None,
            sort_by="time",
            sort_dir="desc",
            page=1,
            limit=100,
            success=None,
            error=None,
            session=session,
        )

    def test_admin_table_detail_opens_visible_non_parsed_and_stored_only_rows(self) -> None:
        with Session(self.engine) as session, patch(
            "app.routers.deals.require_role_response",
            return_value=None,
        ) as require_role, patch(
            "app.routers.deals.get_watched_channels",
            return_value=[],
        ):
            cases = [
                (PARSE_PENDING, False),
                (PARSE_FAILED, False),
                (PARSE_REVIEW_REQUIRED, True),
                (PARSE_PARSED, False),
            ]
            for parse_status, needs_review in cases:
                row = self._message(session, parse_status=parse_status, needs_review=needs_review)
                response = self._detail(session, row, return_path="/table")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.context["deal"]["id"], row.id)

        self.assertTrue(any(call.args[1] == "admin" for call in require_role.call_args_list))

    def test_public_deal_detail_stays_parsed_only_for_enabled_watched_channels(self) -> None:
        watched = [WatchedChannel(channel_id="chan-public", channel_name="deals", is_enabled=True)]
        with Session(self.engine) as session, patch(
            "app.routers.deals.require_role_response",
            return_value=None,
        ) as require_role, patch(
            "app.routers.deals.get_watched_channels",
            return_value=watched,
        ):
            parsed = self._message(session, parse_status=PARSE_PARSED, channel_id="chan-public")
            response = self._detail(session, parsed, return_path="/deals")
            self.assertEqual(response.status_code, 200)

            pending = self._message(session, parse_status=PARSE_PENDING, channel_id="chan-public")
            with self.assertRaises(HTTPException) as exc:
                self._detail(session, pending, return_path="/deals")
            self.assertEqual(exc.exception.status_code, 404)

        self.assertTrue(any(call.args[1] == "viewer" for call in require_role.call_args_list))

    def test_mark_incorrect_redirect_keeps_admin_detail_return_path(self) -> None:
        with Session(self.engine) as session, patch(
            "app.routers.messages.require_role_response",
            return_value=None,
        ), patch("app.routers.messages.sync_transaction_from_message"):
            row = self._message(session, parse_status=PARSE_PARSED)
            response = mark_incorrect_message_form(
                request=make_request(f"/messages/{row.id}/mark-incorrect-form"),
                message_id=row.id,
                return_path="/table",
                status="parsed",
                channel_id="",
                expense_category="",
                filter_expense_category=None,
                after=None,
                before=None,
                sort_by="time",
                sort_dir="desc",
                page=1,
                limit=100,
                session=session,
            )
            session.refresh(row)

        self.assertEqual(response.status_code, 303)
        self.assertIn(f"/deals/{row.id}?", response.headers["location"])
        self.assertIn("return_path=%2Ftable", response.headers["location"])
        self.assertEqual(row.parse_status, PARSE_REVIEW_REQUIRED)


if __name__ == "__main__":
    unittest.main()
