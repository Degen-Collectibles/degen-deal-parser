from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional

from sqlmodel import Session, select

from .models import DiscordMessage


def parse_report_datetime(value: Optional[str], *, end_of_day: bool = False) -> Optional[datetime]:
    if not value:
        return None

    value = value.strip()
    if not value:
        return None

    dt = datetime.fromisoformat(value)
    if len(value) == 10:
        if end_of_day:
            dt = dt.replace(hour=23, minute=59, second=59, microsecond=999999)
        else:
            dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def financial_base_query(
    *,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    channel_id: Optional[str] = None,
):
    stmt = select(DiscordMessage).where(DiscordMessage.is_deleted == False)
    stmt = stmt.where(DiscordMessage.parse_status.in_(["parsed", "needs_review"]))
    stmt = stmt.where(
        (DiscordMessage.stitched_group_id == None) | (DiscordMessage.stitched_primary == True)
    )

    if start:
        stmt = stmt.where(DiscordMessage.created_at >= start)
    if end:
        stmt = stmt.where(DiscordMessage.created_at <= end)
    if channel_id:
        stmt = stmt.where(DiscordMessage.channel_id == channel_id)

    return stmt.order_by(DiscordMessage.created_at)


def get_financial_rows(
    session: Session,
    *,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    channel_id: Optional[str] = None,
) -> list[DiscordMessage]:
    stmt = financial_base_query(start=start, end=end, channel_id=channel_id)
    return session.exec(stmt).all()


def build_financial_summary(rows: list[DiscordMessage]) -> dict:
    totals = defaultdict(float)
    count_by_kind = defaultdict(int)
    expense_categories = defaultdict(float)
    category_breakdown = defaultdict(float)

    for row in rows:
        money_in = float(row.money_in or 0.0)
        money_out = float(row.money_out or 0.0)
        entry_kind = row.entry_kind or "unknown"

        totals["money_in"] += money_in
        totals["money_out"] += money_out
        totals["net"] += money_in - money_out
        count_by_kind[entry_kind] += 1

        if entry_kind == "sale":
            totals["sales"] += money_in
        elif entry_kind == "buy":
            totals["buys"] += money_out
        elif entry_kind == "expense":
            totals["expenses"] += money_out
        elif entry_kind == "trade":
            totals["trade_cash_in"] += money_in
            totals["trade_cash_out"] += money_out

        if row.expense_category:
            expense_categories[row.expense_category] += money_out

        if row.category:
            category_breakdown[row.category] += money_in or money_out

    return {
        "totals": {key: round(value, 2) for key, value in totals.items()},
        "counts": dict(count_by_kind),
        "expense_categories": {
            key: round(value, 2)
            for key, value in sorted(expense_categories.items(), key=lambda item: (-item[1], item[0]))
        },
        "deal_categories": {
            key: round(value, 2)
            for key, value in sorted(category_breakdown.items(), key=lambda item: (-item[1], item[0]))
        },
        "rows": len(rows),
    }
