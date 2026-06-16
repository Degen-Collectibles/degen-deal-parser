import csv
from datetime import datetime, timezone
from io import StringIO
from types import SimpleNamespace
from unittest.mock import patch

from sqlmodel import Session, SQLModel, create_engine, select

import app.routers.reports as reports_module
from app.discord.transactions import get_transactions
from app.models import DiscordMessage, PARSE_PARSED, Transaction
from app.shared import csv_response


def make_engine():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    SQLModel.metadata.create_all(engine)
    return engine


def make_request(role: str = "viewer"):
    return SimpleNamespace(state=SimpleNamespace(current_user=SimpleNamespace(role=role, username="tester")))


def test_csv_response_escapes_formula_like_text_cells():
    response = csv_response(
        "report.csv",
        [
            {
                "notes": '=HYPERLINK("http://evil.test","x")',
                "trade_summary": "+SUM(1,2)",
                "source_content": "-cmd",
                "message": "@risk",
            }
        ],
    )

    exported_row = next(csv.DictReader(StringIO(response.body.decode("utf-8"))))
    assert exported_row["notes"].startswith("'=")
    assert exported_row["trade_summary"].startswith("'+")
    assert exported_row["source_content"].startswith("'-")
    assert exported_row["message"].startswith("'@")


def add_message_and_transaction(
    session: Session,
    *,
    message_id: int,
    discord_id: str,
    created_at: datetime,
    occurred_at: datetime,
    amount: float,
    stitched_group_id: str | None = None,
    stitched_primary: bool = True,
) -> Transaction:
    message = DiscordMessage(
        id=message_id,
        discord_message_id=discord_id,
        channel_id="sales",
        channel_name="store-sales-and-trades",
        author_name="tester",
        content=f"sold card {amount} zelle",
        created_at=created_at,
        parse_status=PARSE_PARSED,
        stitched_group_id=stitched_group_id,
        stitched_primary=stitched_primary,
        deal_type="sell",
        amount=amount,
        payment_method="zelle",
        entry_kind="sale",
        money_in=amount,
        money_out=0.0,
        expense_category="inventory",
    )
    session.add(message)
    session.flush()
    tx = Transaction(
        source_message_id=message.id,
        discord_message_id=discord_id,
        channel_id="sales",
        channel_name="store-sales-and-trades",
        author_name="tester",
        occurred_at=occurred_at,
        parse_status=PARSE_PARSED,
        deal_type="sell",
        entry_kind="sale",
        payment_method="zelle",
        category="singles",
        expense_category="inventory",
        amount=amount,
        money_in=amount,
        money_out=0.0,
        source_content=message.content,
    )
    session.add(tx)
    session.flush()
    return tx


def test_reports_summary_and_export_use_transaction_occurred_at_for_same_range():
    engine = make_engine()
    with Session(engine) as session:
        add_message_and_transaction(
            session,
            message_id=1,
            discord_id="event-in-range",
            created_at=datetime(2026, 5, 21, 7, 30, tzinfo=timezone.utc),
            occurred_at=datetime(2026, 5, 20, 19, 30, tzinfo=timezone.utc),
            amount=100.0,
        )
        add_message_and_transaction(
            session,
            message_id=2,
            discord_id="event-out-range",
            created_at=datetime(2026, 5, 20, 20, 0, tzinfo=timezone.utc),
            occurred_at=datetime(2026, 5, 21, 20, 0, tzinfo=timezone.utc),
            amount=50.0,
        )
        session.commit()

        with patch.object(reports_module, "require_role_response", return_value=None):
            summary = reports_module.report_summary(
                make_request(),
                start="2026-05-20",
                end="2026-05-20",
                channel_id=None,
                session=session,
            )
            response = reports_module.report_transactions_csv(
                make_request(),
                start="2026-05-20",
                end="2026-05-20",
                channel_id=None,
                entry_kind=None,
                session=session,
            )

    rows = list(csv.DictReader(StringIO(response.body.decode("utf-8"))))
    assert summary["totals"]["money_in"] == 100.0
    assert summary["rows"] == 1
    assert [row["source_message_id"] for row in rows] == ["1"]


def test_transaction_reports_defensively_exclude_stale_stitched_children():
    engine = make_engine()
    occurred = datetime(2026, 5, 20, 12, tzinfo=timezone.utc)
    with Session(engine) as session:
        add_message_and_transaction(
            session,
            message_id=10,
            discord_id="stitched-parent",
            created_at=occurred,
            occurred_at=occurred,
            amount=150.0,
            stitched_group_id="group-1",
            stitched_primary=True,
        )
        add_message_and_transaction(
            session,
            message_id=11,
            discord_id="stale-child",
            created_at=occurred,
            occurred_at=occurred,
            amount=150.0,
            stitched_group_id="group-1",
            stitched_primary=False,
        )
        session.commit()

        rows = get_transactions(session)

    assert [row.discord_message_id for row in rows] == ["stitched-parent"]


def test_reports_page_template_labels_date_filters_as_pacific_time():
    from pathlib import Path

    source = Path("app/templates/reports.html").read_text(encoding="utf-8")

    assert "Pacific Time" in source
