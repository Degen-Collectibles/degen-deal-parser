import shutil
import unittest
import uuid
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from sqlmodel import Session, SQLModel, create_engine
from starlette.requests import Request

from app.models import DiscordMessage, PARSE_PARSED, PARSE_REVIEW_REQUIRED, utcnow
from app.routers.channels_api import edit_message_form
from app.routers.messages import reviewer_focus_page, reviewer_queue_page


def make_request(path: str, *, role: str = "reviewer") -> Request:
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
        username="reviewer",
        display_name="Reviewer",
        role=role,
    )
    return request


def read_template(name: str) -> str:
    return Path("app/templates", name).read_text(encoding="utf-8")


class MobileReviewQueueTests(unittest.TestCase):
    maxDiff = None

    def setUp(self) -> None:
        self.temp_dir = Path.cwd() / "tests" / ".tmp_mobile_review_queue" / str(uuid.uuid4())
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = self.temp_dir / "mobile_review.db"
        self.engine = create_engine(
            f"sqlite:///{db_path.as_posix()}",
            connect_args={"check_same_thread": False},
        )
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _review_row(self, session: Session, *, content: str, created_offset: int = 0) -> DiscordMessage:
        row = DiscordMessage(
            discord_message_id=f"mobile-review-{uuid.uuid4()}",
            channel_id="chan-review",
            channel_name="review-log",
            author_name="tester",
            content=content,
            created_at=utcnow() + timedelta(seconds=created_offset),
            parse_status=PARSE_REVIEW_REQUIRED,
            needs_review=True,
            amount=25,
            payment_method="cash",
            entry_kind="sale",
            expense_category="inventory",
        )
        session.add(row)
        session.commit()
        session.refresh(row)
        return row

    def test_primary_review_template_has_mobile_filter_sheet_and_status_states(self) -> None:
        review_source = read_template("review_queue.html")
        focus_source = read_template("review_focus.html")
        table_source = read_template("messages_table.html")

        self.assertIn("rq-mobile-filter-panel", review_source)
        self.assertIn("Failed parse", review_source)
        self.assertIn("Transaction synced", review_source)
        self.assertIn("Open Sheet", review_source)
        self.assertIn("Original Discord Message", focus_source)
        self.assertIn("Save &amp; Next", focus_source)
        self.assertIn("Save, Approve &amp; Next", focus_source)
        self.assertIn("mobile-filter-card", table_source)
        self.assertIn("/review/focus/{{ row['id'] }}", table_source)

    def test_review_queue_renders_mobile_status_context(self) -> None:
        with Session(self.engine) as session, patch("app.routers.messages.require_role_response", return_value=None):
            self._review_row(session, content="sell slab 25 cash")
            response = reviewer_queue_page(
                make_request("/review?after="),
                channel_id=None,
                expense_category=None,
                after="",
                before=None,
                sort_by="time",
                sort_dir="desc",
                page=1,
                limit=25,
                success=None,
                error=None,
                session=session,
            )

        body = response.body.decode("utf-8")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Needs review", body)
        self.assertIn("Not imported yet", body)
        self.assertIn("Bookkeeping", body)

    def test_focus_page_preserves_queue_navigation_for_next_correction(self) -> None:
        with Session(self.engine) as session, patch("app.routers.messages.require_role_response", return_value=None):
            first = self._review_row(session, content="first sell 25 cash", created_offset=1)
            second = self._review_row(session, content="second sell 30 cash", created_offset=2)

            response = reviewer_focus_page(
                first.id,
                make_request(f"/review/focus/{first.id}"),
                channel_id="chan-review",
                expense_category="inventory",
                after=None,
                before=None,
                sort_by="time",
                sort_dir="asc",
                page=1,
                limit=25,
                success=None,
                error=None,
                session=session,
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["next_message_id"], second.id)
        self.assertIn(f"/review/focus/{second.id}", response.context["next_url"])
        self.assertIn("channel_id=chan-review", response.context["next_url"])
        self.assertIn("expense_category=inventory", response.context["next_url"])

    def test_correction_form_can_save_approve_and_redirect_to_next_focus_row(self) -> None:
        with Session(self.engine) as session, patch(
            "app.routers.channels_api.require_role_response",
            return_value=None,
        ), patch(
            "app.routers.channels_api.compute_manual_financials",
            return_value=("sale", 25.0, 0.0, "inventory"),
        ), patch("app.routers.channels_api.save_review_correction"), patch(
            "app.routers.channels_api.sync_transaction_from_message"
        ):
            first = self._review_row(session, content="first sell 25 cash")
            second = self._review_row(session, content="second sell 30 cash", created_offset=1)
            response = edit_message_form(
                make_request(f"/messages/{first.id}/edit-form"),
                message_id=first.id,
                return_path=f"/review/focus/{first.id}",
                status="review_queue",
                channel_id="chan-review",
                filter_expense_category="inventory",
                after="",
                before="",
                sort_by="time",
                sort_dir="asc",
                page=1,
                limit=25,
                parse_status=PARSE_REVIEW_REQUIRED,
                needs_review="true",
                deal_type="sell",
                amount="25",
                payment_method="cash",
                cash_direction=None,
                category="singles",
                entry_kind="sale",
                expense_category="inventory",
                confidence=None,
                notes="corrected on phone",
                trade_summary=None,
                item_names_text=None,
                items_in_text=None,
                items_out_text=None,
                approve_after_save=None,
                stay_on_detail=None,
                review_action="approve_next",
                next_message_id=second.id,
                session=session,
            )
            session.refresh(first)
            second_id = second.id

        self.assertEqual(response.status_code, 303)
        self.assertIn(f"/review/focus/{second_id}", response.headers["location"])
        self.assertIn("expense_category=inventory", response.headers["location"])
        self.assertIn("approved", response.headers["location"])
        self.assertEqual(first.parse_status, PARSE_PARSED)
        self.assertFalse(first.needs_review)


if __name__ == "__main__":
    unittest.main()
