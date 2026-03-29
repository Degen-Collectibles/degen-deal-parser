import asyncio
import json
import uuid
import re
from datetime import datetime, timezone, timedelta

from sqlmodel import Session, select

from .config import get_settings
from .db import engine
from .financials import compute_financials
from .models import DiscordMessage, ParseAttempt
from .parser import parse_message, TimedOutRowError

settings = get_settings()


def utcnow():
    return datetime.now(timezone.utc)


async def parser_loop(stop_event: asyncio.Event):
    while not stop_event.is_set():
        try:
            await process_once()
        except Exception as e:
            print(f"[worker] loop error: {e}")
        await asyncio.sleep(settings.parser_poll_seconds)


async def process_once():
    row_ids: list[int] = []

    with Session(engine) as session:
        rows = session.exec(
            select(DiscordMessage)
            .where(DiscordMessage.parse_status.in_(["queued", "failed"]))
            .where(DiscordMessage.parse_attempts < settings.parser_max_attempts)
            .order_by(DiscordMessage.created_at)
            .limit(settings.parser_batch_size)
        ).all()

        for row in rows:
            row.parse_status = "processing"
            row.parse_attempts += 1

            session.add(
                ParseAttempt(
                    message_id=row.id,
                    attempt_number=row.parse_attempts,
                    model_used="gpt-5-nano",
                )
            )

            row_ids.append(row.id)

        session.commit()

    for row_id in row_ids:
        await process_row(row_id)


async def process_row(row_id: int):
    with Session(engine) as session:
        row = session.get(DiscordMessage, row_id)
        if not row:
            return

        if row.is_deleted:
            return

        if row.parse_status not in ["processing", "queued", "failed"]:
            return

        group_rows = [row]
        if settings.stitch_enabled:
            group_rows = build_stitch_group(
                session=session,
                row=row,
                window_seconds=settings.stitch_window_seconds,
                max_messages=settings.stitch_max_messages,
            )

        group_rows = sorted(group_rows, key=lambda r: r.created_at)
        primary_row = group_rows[0]

        combined_text, combined_attachments, grouped_row_ids = combine_group_payload(group_rows)

        group_id = str(uuid.uuid4()) if len(group_rows) > 1 else None

        attempt = session.exec(
            select(ParseAttempt)
            .where(ParseAttempt.message_id == row.id)
            .order_by(ParseAttempt.id.desc())
        ).first()

        try:
            result = await parse_message(
                content=combined_text,
                attachment_urls=combined_attachments,
                author_name=row.author_name or "",
            )
            financials = compute_financials(
                parsed_type=result.get("parsed_type"),
                amount=result.get("parsed_amount"),
                cash_direction=result.get("parsed_cash_direction"),
                message_text=combined_text,
            )

            for grouped_row in group_rows:
                grouped_row.stitched_group_id = group_id
                grouped_row.stitched_primary = (grouped_row.id == primary_row.id)
                grouped_row.stitched_message_ids_json = json.dumps(grouped_row_ids)

            primary_row.deal_type = result.get("parsed_type")
            primary_row.amount = result.get("parsed_amount")
            primary_row.payment_method = result.get("parsed_payment_method")
            primary_row.cash_direction = result.get("parsed_cash_direction")
            primary_row.category = result.get("parsed_category")
            primary_row.item_names_json = json.dumps(result.get("parsed_items", []))
            primary_row.items_in_json = json.dumps(result.get("parsed_items_in", []))
            primary_row.items_out_json = json.dumps(result.get("parsed_items_out", []))
            primary_row.trade_summary = result.get("parsed_trade_summary")
            primary_row.notes = result.get("parsed_notes")
            primary_row.confidence = result.get("confidence")
            primary_row.needs_review = bool(result.get("needs_review", False))
            primary_row.image_summary = result.get("image_summary")
            primary_row.entry_kind = financials.entry_kind
            primary_row.money_in = financials.money_in
            primary_row.money_out = financials.money_out
            primary_row.expense_category = financials.expense_category
            primary_row.parse_status = "needs_review" if primary_row.needs_review else "parsed"
            primary_row.last_error = None

            for grouped_row in group_rows:
                if grouped_row.id != primary_row.id:
                    grouped_row.entry_kind = None
                    grouped_row.money_in = None
                    grouped_row.money_out = None
                    grouped_row.expense_category = None
                    grouped_row.parse_status = "parsed"
                    grouped_row.last_error = None

            if attempt:
                attempt.success = True
                attempt.error = None
                attempt.finished_at = utcnow()
                session.add(attempt)

            for grouped_row in group_rows:
                session.add(grouped_row)

            session.commit()

        except TimedOutRowError as e:
            row.parse_status = "failed"
            row.last_error = f"timeout: {e}"

            if attempt:
                attempt.success = False
                attempt.error = f"timeout: {e}"
                attempt.finished_at = utcnow()
                session.add(attempt)

            session.add(row)
            session.commit()

        except Exception as e:
            row.parse_status = "failed"
            row.last_error = str(e)

            if attempt:
                attempt.success = False
                attempt.error = str(e)
                attempt.finished_at = utcnow()
                session.add(attempt)

            session.add(row)
            session.commit()
def looks_like_fragment(row: DiscordMessage) -> bool:
    text = (row.content or "").strip().lower()
    has_images = bool(json.loads(row.attachment_urls_json or "[]"))

    if has_images and len(text) <= 30:
        return True

    fragment_patterns = [
        r"^(zelle|venmo|paypal|cash|tap|card)\s*\$?\d+",
        r"^\+?\s*\$?\d+\s*(zelle|venmo|paypal|cash|tap|card)$",
        r"^(top|bottom|left|right).*\b(in|out)\b",
        r"^\+?\s*\d+\s*(zelle|venmo|paypal|cash|tap|card)$",
    ]

    return any(re.search(p, text, re.I) for p in fragment_patterns)
def build_stitch_group(
    session: Session,
    row: DiscordMessage,
    window_seconds: int,
    max_messages: int,
) -> list[DiscordMessage]:
    if row.is_deleted:
        return [row]

    start_time = row.created_at - timedelta(seconds=window_seconds)
    end_time = row.created_at + timedelta(seconds=window_seconds)

    candidates = session.exec(
        select(DiscordMessage)
        .where(DiscordMessage.channel_id == row.channel_id)
        .where(DiscordMessage.author_id == row.author_id)
        .where(DiscordMessage.is_deleted == False)
        .where(DiscordMessage.created_at >= start_time)
        .where(DiscordMessage.created_at <= end_time)
        .order_by(DiscordMessage.created_at)
    ).all()

    if not candidates:
        return [row]

    candidates = [c for c in candidates if c.parse_status != "deleted"][:max_messages]

    # only keep rows very near the base row
    group_rows = [c for c in candidates if abs((c.created_at - row.created_at).total_seconds()) <= window_seconds]

    if row not in group_rows:
        group_rows.append(row)

    group_rows = sorted(group_rows, key=lambda r: r.created_at)

    if not should_stitch_rows(row, group_rows):
        return [row]

    return group_rows

def combine_group_payload(rows: list[DiscordMessage]) -> tuple[str, list[str], list[int]]:
    combined_parts = []
    combined_attachments = []
    row_ids = []

    for i, r in enumerate(rows, start=1):
        text = (r.content or "").strip()
        if text:
            combined_parts.append(f"Message {i}: {text}")
        else:
            combined_parts.append(f"Message {i}: [no text]")

        combined_attachments.extend(json.loads(r.attachment_urls_json or "[]"))
        row_ids.append(r.id)

    combined_text = "\n\n".join(combined_parts)
    return combined_text, combined_attachments, row_ids
def normalize_text(text: str) -> str:
    return (text or "").strip().lower()


def has_images(row: DiscordMessage) -> bool:
    return bool(json.loads(row.attachment_urls_json or "[]"))


def is_payment_only_text(text: str) -> bool:
    text = normalize_text(text)
    patterns = [
        r"^(zelle|venmo|paypal|cash|tap|card)\s*\$?\d+(?:\.\d{1,2})?$",
        r"^\$?\d+(?:\.\d{1,2})?\s*(zelle|venmo|paypal|cash|tap|card)$",
        r"^\+\s*\$?\d+(?:\.\d{1,2})?\s*(zelle|venmo|paypal|cash|tap|card)?$",
        r"^(plus|\+)\s*\$?\d+(?:\.\d{1,2})?\s*(zelle|venmo|paypal|cash|tap|card)?$",
    ]
    return any(re.fullmatch(p, text, re.I) for p in patterns)


def is_trade_fragment_text(text: str) -> bool:
    text = normalize_text(text)
    patterns = [
        r".*\b(in|out)\b.*",
        r"^(top|bottom|left|right).*$",
        r"^.*\bplus\b.*$",
    ]
    return any(re.fullmatch(p, text, re.I) for p in patterns)


def is_short_fragment(row: DiscordMessage) -> bool:
    text = normalize_text(row.content)
    if has_images(row) and len(text) <= 20:
        return True
    if is_payment_only_text(text):
        return True
    if is_trade_fragment_text(text) and len(text) <= 50:
        return True
    return False


def looks_like_complete_deal(row: DiscordMessage) -> bool:
    text = normalize_text(row.content)

    sell_buy_patterns = [
        r".*\b(sold|sell|bought|buy|paid)\b.*\d+.*",
    ]
    complete = any(re.fullmatch(p, text, re.I) for p in sell_buy_patterns)

    # image + substantial text can also be a complete standalone log
    if has_images(row) and len(text) >= 25:
        return True

    return complete


def should_stitch_rows(base_row: DiscordMessage, candidate_rows: list[DiscordMessage]) -> bool:
    if len(candidate_rows) <= 1:
        return False

    payment_fragments = 0
    short_fragments = 0
    complete_deals = 0

    for r in candidate_rows:
        text = normalize_text(r.content)
        if is_payment_only_text(text):
            payment_fragments += 1
        if is_short_fragment(r):
            short_fragments += 1
        if looks_like_complete_deal(r):
            complete_deals += 1

    # Too many full standalone deals close together -> do not stitch
    if complete_deals >= 2:
        return False

    # More than one payment fragment usually means multiple separate deals
    if payment_fragments >= 2:
        return False

    # Stitch only if there is at least one short/incomplete fragment
    if short_fragments == 0:
        return False

    # Good common case:
    # one image/incomplete row + one payment/direction fragment
    return True
