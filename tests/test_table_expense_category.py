import shutil
import unittest
import uuid
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import patch

from sqlmodel import Session, SQLModel, create_engine
from starlette.requests import Request

from app.main import edit_message_form, messages_table, review_table, reviewer_queue_page
from app.models import DiscordMessage, PARSE_PARSED, utcnow


def make_request(path: str) -> Request:
    return Request({"type": "http", "method": "GET", "path": path, "headers": []})


class TableExpenseCategoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path.cwd() / "tests" / ".tmp_table_expense_category" / str(uuid.uuid4())
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        db_path = self.temp_dir / "table_expense_category.db"
        self.engine = create_engine(
            f"sqlite:///{db_path.as_posix()}",
            connect_args={"check_same_thread": False},
        )
        SQLModel.metadata.create_all(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_messages_table_serializes_expense_category_in_row_data(self) -> None:
        with Session(self.engine) as session, patch("app.main.require_role_response", return_value=None), patch(
            "app.main.get_available_channel_choices", return_value=([], False)
        ), patch("app.main.get_watched_channels", return_value=[]):
            row = DiscordMessage(
                discord_message_id="row-1",
                channel_id="chan-1",
                channel_name="deals",
                author_name="tester",
                content="sold cards",
                created_at=utcnow(),
                parse_status=PARSE_PARSED,
                expense_category="inventory",
            )
            session.add(row)
            session.commit()

            response = messages_table(
                make_request("/table"),
                status=None,
                channel_id=None,
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

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.context["rows"]), 1)
        self.assertEqual(response.context["rows"][0]["expense_category"], "inventory")

    def test_messages_table_filters_by_expense_category(self) -> None:
        with Session(self.engine) as session, patch("app.main.require_role_response", return_value=None), patch(
            "app.main.get_available_channel_choices", return_value=([], False)
        ), patch("app.main.get_watched_channels", return_value=[]):
            inventory_row = DiscordMessage(
                discord_message_id="row-1",
                channel_id="chan-1",
                channel_name="deals",
                author_name="tester",
                content="sold cards",
                created_at=utcnow(),
                parse_status=PARSE_PARSED,
                expense_category="inventory",
            )
            shipping_row = DiscordMessage(
                discord_message_id="row-2",
                channel_id="chan-1",
                channel_name="deals",
                author_name="tester",
                content="paid shipping",
                created_at=utcnow(),
                parse_status=PARSE_PARSED,
                expense_category="shipping",
            )
            session.add(inventory_row)
            session.add(shipping_row)
            session.commit()

            response = messages_table(
                make_request("/table?expense_category=inventory"),
                status=None,
                channel_id=None,
                expense_category="inventory",
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

        rows = response.context["rows"]
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["selected_expense_category"], "inventory")
        self.assertEqual(response.context["summary"]["total"], 1)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["expense_category"], "inventory")
        self.assertIsNotNone(rows[0]["id"])

    def test_review_table_filters_by_expense_category(self) -> None:
        with Session(self.engine) as session, patch("app.main.require_role_response", return_value=None), patch(
            "app.main.get_available_channel_choices", return_value=([], False)
        ), patch("app.main.get_watched_channels", return_value=[]):
            inventory_row = DiscordMessage(
                discord_message_id="row-10",
                channel_id="chan-1",
                channel_name="deals",
                author_name="tester",
                content="parking",
                created_at=utcnow(),
                parse_status="needs_review",
                needs_review=True,
                expense_category="travel",
            )
            shipping_row = DiscordMessage(
                discord_message_id="row-11",
                channel_id="chan-1",
                channel_name="deals",
                author_name="tester",
                content="rent",
                created_at=utcnow(),
                parse_status="needs_review",
                needs_review=True,
                expense_category="rent",
            )
            session.add(inventory_row)
            session.add(shipping_row)
            session.commit()

            response = review_table(
                make_request("/review-table?expense_category=travel"),
                channel_id=None,
                expense_category="travel",
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

        rows = response.context["rows"]
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["selected_expense_category"], "travel")
        self.assertEqual(response.context["summary"]["total"], 1)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["expense_category"], "travel")

    def test_review_queue_filters_by_expense_category(self) -> None:
        with Session(self.engine) as session, patch("app.main.require_role_response", return_value=None):
            travel_row = DiscordMessage(
                discord_message_id="row-30",
                channel_id="chan-1",
                channel_name="deals",
                author_name="tester",
                content="parking",
                created_at=utcnow(),
                parse_status="needs_review",
                needs_review=True,
                expense_category="travel",
            )
            rent_row = DiscordMessage(
                discord_message_id="row-31",
                channel_id="chan-1",
                channel_name="deals",
                author_name="tester",
                content="rent",
                created_at=utcnow(),
                parse_status="needs_review",
                needs_review=True,
                expense_category="rent",
            )
            session.add(travel_row)
            session.add(rent_row)
            session.commit()

            response = reviewer_queue_page(
                make_request("/review?expense_category=travel"),
                channel_id=None,
                expense_category="travel",
                after=None,
                before=None,
                sort_by="time",
                sort_dir="desc",
                page=1,
                limit=25,
                success=None,
                error=None,
                session=session,
            )

        rows = response.context["rows"]
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["selected_expense_category"], "travel")
        self.assertEqual(response.context["summary"]["total"], 1)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["expense_category"], "travel")

    def test_review_table_edit_persists_expense_category_and_keeps_filter(self) -> None:
        with Session(self.engine) as session, patch("app.main.require_role_response", return_value=None), patch(
            "app.main.compute_manual_financials", return_value=("expense", 12.5, 0.0, "inventory")
        ) as compute_manual_financials, patch("app.main.save_review_correction") as save_review_correction, patch(
            "app.main.sync_transaction_from_message"
        ) as sync_transaction_from_message:
            row = DiscordMessage(
                discord_message_id="row-20",
                channel_id="chan-1",
                channel_name="deals",
                author_name="tester",
                content="shipping reimbursement",
                created_at=utcnow(),
                parse_status=PARSE_PARSED,
                expense_category="shipping",
            )
            session.add(row)
            session.commit()

            request = make_request("/messages/1/edit-form")
            request.state.current_user = SimpleNamespace(username="reviewer", display_name="Reviewer")

            response = edit_message_form(
                request,
                message_id=row.id,
                return_path="/review-table",
                status="review_queue",
                channel_id="chan-1",
                filter_expense_category="travel",
                after=None,
                before=None,
                sort_by="time",
                sort_dir="desc",
                page=1,
                limit=100,
                parse_status=PARSE_PARSED,
                needs_review=None,
                deal_type=None,
                amount="12.50",
                payment_method=None,
                cash_direction=None,
                category=None,
                entry_kind="expense",
                expense_category="inventory",
                confidence=None,
                notes=None,
                trade_summary=None,
                item_names_text=None,
                items_in_text=None,
                items_out_text=None,
                approve_after_save=None,
                stay_on_detail=None,
                session=session,
            )

            session.refresh(row)

        self.assertEqual(response.status_code, 303)
        self.assertIn("/review-table?", response.headers["location"])
        self.assertIn("expense_category=travel", response.headers["location"])
        self.assertEqual(row.expense_category, "inventory")
        self.assertTrue(compute_manual_financials.called)
        self.assertTrue(save_review_correction.called)
        self.assertTrue(sync_transaction_from_message.called)


if __name__ == "__main__":
    unittest.main()
