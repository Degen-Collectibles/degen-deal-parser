import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlmodel import Session, select

from .channels import get_channel_filter_choices, get_watched_channels, upsert_watched_channel
from .config import get_settings
from .db import get_session, init_db, engine
from .discord_ingest import (
    get_discord_client,
    list_available_discord_channels,
    parse_iso_datetime,
    run_discord_bot,
)
from .models import DiscordMessage, ParseAttempt, WatchedChannel, utcnow
from .reporting import build_financial_summary, get_financial_rows, parse_report_datetime
from .schemas import HealthOut
from .worker import parser_loop


settings = get_settings()

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

def message_list_item(row: DiscordMessage) -> dict:
    attachment_urls = json.loads(row.attachment_urls_json or "[]")
    item_names = json.loads(row.item_names_json or "[]")
    items_in = json.loads(row.items_in_json or "[]")
    items_out = json.loads(row.items_out_json or "[]")
    stitched_ids = json.loads(row.stitched_message_ids_json or "[]")

    image_urls = [
        url for url in attachment_urls
        if any(ext in url.lower() for ext in [".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp"])
    ]

    return {
        "id": row.id,
        "time": row.created_at.isoformat(sep=" ", timespec="seconds") if row.created_at else None,
        "edited_at": row.edited_at.isoformat(sep=" ", timespec="seconds") if row.edited_at else None,
        "is_deleted": row.is_deleted,
        "channel": row.channel_name,
        "channel_id": row.channel_id,
        "author": row.author_name,
        "message": row.content,
        "status": row.parse_status,
        "type": row.deal_type,
        "amount": row.amount,
        "payment": row.payment_method,
        "cash_direction": row.cash_direction,
        "category": row.category,
        "items": item_names,
        "items_in": items_in,
        "items_out": items_out,
        "trade_summary": row.trade_summary,
        "confidence": row.confidence,
        "needs_review": row.needs_review,
        "notes": row.notes,
        "entry_kind": row.entry_kind,
        "money_in": row.money_in,
        "money_out": row.money_out,
        "expense_category": row.expense_category,
        "has_images": len(image_urls) > 0,
        "image_urls": image_urls,
        "first_image_url": image_urls[0] if image_urls else None,
        "parse_attempts": row.parse_attempts,
        "stitched_group_id": row.stitched_group_id,
        "stitched_primary": row.stitched_primary,
        "stitched_message_ids": stitched_ids,
        "stitched_count": len(stitched_ids),
    }


def message_detail_item(row: DiscordMessage) -> dict:
    return {
        "id": row.id,
        "discord_message_id": row.discord_message_id,
        "guild_id": row.guild_id,
        "channel_id": row.channel_id,
        "channel_name": row.channel_name,
        "author_id": row.author_id,
        "author_name": row.author_name,
        "content": row.content,
        "attachment_urls": json.loads(row.attachment_urls_json or "[]"),
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "ingested_at": row.ingested_at.isoformat() if row.ingested_at else None,
        "parse_status": row.parse_status,
        "parse_attempts": row.parse_attempts,
        "last_error": row.last_error,
        "deal_type": row.deal_type,
        "amount": row.amount,
        "payment_method": row.payment_method,
        "cash_direction": row.cash_direction,
        "category": row.category,
        "item_names": json.loads(row.item_names_json or "[]"),
        "items_in": json.loads(row.items_in_json or "[]"),
        "items_out": json.loads(row.items_out_json or "[]"),
        "trade_summary": row.trade_summary,
        "notes": row.notes,
        "confidence": row.confidence,
        "needs_review": row.needs_review,
        "image_summary": row.image_summary,
        "entry_kind": row.entry_kind,
        "money_in": row.money_in,
        "money_out": row.money_out,
        "expense_category": row.expense_category,
    }


def get_message_rows(
    session: Session,
    status: Optional[str] = None,
    channel_id: Optional[str] = None,
    limit: int = 100,
):
    stmt = select(DiscordMessage).order_by(DiscordMessage.created_at.desc())

    if status:
        stmt = stmt.where(DiscordMessage.parse_status == status)

    if channel_id:
        stmt = stmt.where(DiscordMessage.channel_id == channel_id)

    stmt = stmt.limit(limit)
    return session.exec(stmt).all()

def get_summary(session: Session, status: Optional[str] = None, channel_id: Optional[str] = None) -> dict:
    stmt = select(DiscordMessage)

    if status:
        stmt = stmt.where(DiscordMessage.parse_status == status)

    if channel_id:
        stmt = stmt.where(DiscordMessage.channel_id == channel_id)

    rows = session.exec(stmt).all()

    total = len(rows)
    parsed = sum(1 for r in rows if r.parse_status == "parsed")
    processing = sum(1 for r in rows if r.parse_status == "processing")
    queued = sum(1 for r in rows if r.parse_status == "queued")
    failed = sum(1 for r in rows if r.parse_status == "failed")
    needs_review = sum(1 for r in rows if r.parse_status == "needs_review")
    with_images = sum(1 for r in rows if json.loads(r.attachment_urls_json or "[]"))
    deleted = sum(1 for r in rows if r.is_deleted)

    return {
        "total": total,
        "parsed": parsed,
        "processing": processing,
        "queued": queued,
        "failed": failed,
        "needs_review": needs_review,
        "with_images": with_images,
        "deleted": deleted,
    }


def build_watched_channel_groups(
    watched_channels: list[WatchedChannel],
    available_discord_channels: list[dict],
) -> list[dict]:
    metadata_by_channel_id = {
        channel["channel_id"]: channel
        for channel in available_discord_channels
    }
    grouped: dict[str, list[WatchedChannel]] = {}

    for watched_channel in watched_channels:
        metadata = metadata_by_channel_id.get(watched_channel.channel_id, {})
        category_name = metadata.get("category_name") or "Other"
        grouped.setdefault(category_name, []).append(watched_channel)

    return [
        {
            "category_name": category_name,
            "channels": sorted(
                channels,
                key=lambda row: (row.channel_name or row.channel_id).lower(),
            ),
        }
        for category_name, channels in sorted(grouped.items(), key=lambda item: item[0].lower())
    ]


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()

    stop_event = asyncio.Event()
    app.state.stop_event = stop_event

    discord_task = asyncio.create_task(run_discord_bot(stop_event))
    worker_task = asyncio.create_task(parser_loop(stop_event))

    app.state.discord_task = discord_task
    app.state.worker_task = worker_task

    yield

    stop_event.set()

    for task in [discord_task, worker_task]:
        task.cancel()

    await asyncio.gather(discord_task, worker_task, return_exceptions=True)


app = FastAPI(title=settings.app_name, lifespan=lifespan)


@app.get("/health", response_model=HealthOut)
def health():
    return HealthOut(ok=True)


@app.get("/channels")
def list_channels(session: Session = Depends(get_session)):
    return get_channel_filter_choices(session)


@app.get("/messages")
def list_messages(
    status: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
    entry_kind: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    session: Session = Depends(get_session),
):
    rows = get_message_rows(session, status=status, channel_id=channel_id, limit=limit)
    if entry_kind:
        rows = [row for row in rows if row.entry_kind == entry_kind]
    return [message_list_item(row) for row in rows]


@app.get("/review")
def review_queue(
    limit: int = Query(default=100, ge=1, le=500),
    session: Session = Depends(get_session),
):
    rows = session.exec(
        select(DiscordMessage)
        .where(DiscordMessage.parse_status.in_(["needs_review", "failed"]))
        .order_by(DiscordMessage.created_at.desc())
        .limit(limit)
    ).all()

    return [message_list_item(row) for row in rows]


@app.get("/messages/{message_id}")
def get_message(message_id: int, session: Session = Depends(get_session)):
    row = session.get(DiscordMessage, message_id)
    if not row:
        raise HTTPException(status_code=404, detail="Message not found")

    return message_detail_item(row)


@app.get("/reports/summary")
def report_summary(
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    start_dt = parse_report_datetime(start)
    end_dt = parse_report_datetime(end, end_of_day=True)
    rows = get_financial_rows(session, start=start_dt, end=end_dt, channel_id=channel_id)
    summary = build_financial_summary(rows)
    summary["filters"] = {
        "start": start_dt.isoformat() if start_dt else None,
        "end": end_dt.isoformat() if end_dt else None,
        "channel_id": channel_id,
    }
    return summary


@app.get("/reports/messages")
def report_messages(
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
    entry_kind: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    start_dt = parse_report_datetime(start)
    end_dt = parse_report_datetime(end, end_of_day=True)
    rows = get_financial_rows(session, start=start_dt, end=end_dt, channel_id=channel_id)
    if entry_kind:
        rows = [row for row in rows if row.entry_kind == entry_kind]
    return [message_list_item(row) for row in rows]


@app.post("/messages/{message_id}/retry")
def retry_message(message_id: int, session: Session = Depends(get_session)):
    row = session.get(DiscordMessage, message_id)
    if not row:
        raise HTTPException(status_code=404, detail="Message not found")

    row.parse_status = "queued"
    row.last_error = None
    session.add(row)
    session.commit()

    return {"ok": True, "message": f"Message {message_id} re-queued for parsing."}


@app.post("/messages/{message_id}/retry-form")
def retry_message_form(
    message_id: int,
    status: Optional[str] = Form(default=None),
    channel_id: Optional[str] = Form(default=None),
    limit: int = Form(default=100),
    session: Session = Depends(get_session),
):
    row = session.get(DiscordMessage, message_id)
    if row:
        row.parse_status = "queued"
        row.last_error = None
        session.add(row)
        session.commit()

    redirect_url = f"/table?success=Re-queued+message+{message_id}&limit={limit}"
    if status:
        redirect_url += f"&status={status}"
    if channel_id:
        redirect_url += f"&channel_id={channel_id}"

    return RedirectResponse(url=redirect_url, status_code=303)


@app.post("/admin/clear")
def clear_all_messages():
    with Session(engine) as session:
        attempts = session.exec(select(ParseAttempt)).all()
        rows = session.exec(select(DiscordMessage)).all()
        count = len(rows)
        for attempt in attempts:
            session.delete(attempt)
        for row in rows:
            session.delete(row)
        session.commit()

    return {"ok": True, "deleted": count}
@app.post("/admin/clear/form")
def clear_all_messages_form():
    with Session(engine) as session:
        attempts = session.exec(select(ParseAttempt)).all()
        rows = session.exec(select(DiscordMessage)).all()
        count = len(rows)
        for attempt in attempts:
            session.delete(attempt)
        for row in rows:
            session.delete(row)
        session.commit()

    return RedirectResponse(
        url=f"/table?success=Cleared+{count}+messages",
        status_code=303,
    )

@app.post("/admin/clear/channel/{channel_id}")
def clear_channel_messages(channel_id: str):
    with Session(engine) as session:
        rows = session.exec(
            select(DiscordMessage).where(DiscordMessage.channel_id == channel_id)
        ).all()

        count = len(rows)

        for row in rows:
            attempts = session.exec(
                select(ParseAttempt).where(ParseAttempt.message_id == row.id)
            ).all()
            for attempt in attempts:
                session.delete(attempt)

        for row in rows:
            session.delete(row)

        session.commit()

    return {
        "ok": True,
        "channel_id": channel_id,
        "deleted": count,
    }
@app.post("/admin/clear/channel")
def clear_channel_messages_form(
    channel_id: str = Form(...),
):
    with Session(engine) as session:
        rows = session.exec(
            select(DiscordMessage).where(DiscordMessage.channel_id == channel_id)
        ).all()

        count = len(rows)
        channel_name = rows[0].channel_name if rows else channel_id

        for row in rows:
            attempts = session.exec(
                select(ParseAttempt).where(ParseAttempt.message_id == row.id)
            ).all()
            for attempt in attempts:
                session.delete(attempt)

            session.delete(row)

        session.commit()

    return RedirectResponse(
        url=f"/table?success=Cleared+{count}+messages+from+{channel_name}",
        status_code=303,
    )

@app.post("/admin/backfill")
async def admin_backfill(
    channel_id: Optional[str] = Form(default=None),
    after: Optional[str] = Form(default=None),
    before: Optional[str] = Form(default=None),
    limit: Optional[int] = Form(default=None),
    oldest_first: bool = Form(default=True),
):
    client = get_discord_client()
    if client is None:
        raise HTTPException(status_code=503, detail="Discord client is not ready yet")

    after_dt = parse_iso_datetime(after, end_of_day=False)
    before_dt = parse_iso_datetime(before, end_of_day=True)

    if channel_id:
        return await client.backfill_channel(
            channel_id=int(channel_id),
            limit=limit,
            oldest_first=oldest_first,
            after=after_dt,
            before=before_dt,
        )

    return await client.backfill_enabled_channels(
        limit_per_channel=limit,
        oldest_first=oldest_first,
        after=after_dt,
        before=before_dt,
    )


@app.post("/admin/backfill/form")
async def admin_backfill_form(
    channel_id: Optional[str] = Form(default=None),
    after: Optional[str] = Form(default=None),
    before: Optional[str] = Form(default=None),
    limit: Optional[int] = Form(default=None),
    oldest_first: bool = Form(default=True),
):
    client = get_discord_client()
    if client is None:
        return RedirectResponse(
            url="/table?error=Discord+client+is+not+ready+yet",
            status_code=303,
        )

    after_dt = parse_iso_datetime(after)
    before_dt = parse_iso_datetime(before)

    try:
        if channel_id:
            result = await client.backfill_channel(
                channel_id=int(channel_id),
                limit=limit,
                oldest_first=oldest_first,
                after=after_dt,
                before=before_dt,
            )
            if result.get("ok"):
                channel_name = result.get("channel_name") or result.get("channel_id")
                msg = f"Backfill complete for {channel_name}: inserted={result.get('inserted', 0)}, skipped={result.get('skipped', 0)}"
                return RedirectResponse(url=f"/table?success={msg}", status_code=303)

            return RedirectResponse(url=f"/table?error={result.get('error', 'Backfill failed')}", status_code=303)

        result = await client.backfill_enabled_channels(
            limit_per_channel=limit,
            oldest_first=oldest_first,
            after=after_dt,
            before=before_dt,
        )
        msg = f"Backfill complete: inserted={result.get('total_inserted', 0)}, skipped={result.get('total_skipped', 0)}"
        return RedirectResponse(url=f"/table?success={msg}", status_code=303)

    except Exception as e:
        return RedirectResponse(url=f"/table?error={str(e)}", status_code=303)


@app.get("/table", response_class=HTMLResponse)
def messages_table(
    request: Request,
    status: Optional[str] = Query(default=None),
    channel_id: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    success: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    rows = get_message_rows(session, status=status, channel_id=channel_id, limit=limit)
    items = [message_list_item(row) for row in rows]
    channels = get_channel_filter_choices(session)
    summary = get_summary(session, status=status, channel_id=channel_id)
    watched_channels = get_watched_channels(session)
    available_discord_channels = list_available_discord_channels()
    watched_channel_groups = build_watched_channel_groups(watched_channels, available_discord_channels)
    financial_rows = get_financial_rows(session, channel_id=channel_id)
    financial_summary = build_financial_summary(financial_rows)

    return templates.TemplateResponse(
        request,
        "messages_table.html",
        {
            "request": request,
            "title": "Messages Table",
            "rows": items,
            "channels": channels,
            "selected_channel_id": channel_id or "",
            "selected_status": status or "",
            "selected_limit": limit,
            "summary": summary,
            "financial_summary": financial_summary,
            "success": success,
            "error": error,
            "watched_channels": watched_channels,
            "watched_channel_groups": watched_channel_groups,
            "available_discord_channels": available_discord_channels,
        },
    )


@app.get("/review-table", response_class=HTMLResponse)
def review_table(
    request: Request,
    limit: int = Query(default=100, ge=1, le=500),
    success: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
    session: Session = Depends(get_session),
):
    rows = session.exec(
        select(DiscordMessage)
        .where(DiscordMessage.parse_status.in_(["needs_review", "failed"]))
        .order_by(DiscordMessage.created_at.desc())
        .limit(limit)
    ).all()

    items = [message_list_item(row) for row in rows]
    channels = get_channel_filter_choices(session)
    summary = get_summary(session)
    financial_summary = build_financial_summary(get_financial_rows(session))
    watched_channels = get_watched_channels(session)
    available_discord_channels = list_available_discord_channels()
    watched_channel_groups = build_watched_channel_groups(watched_channels, available_discord_channels)

    return templates.TemplateResponse(
        request,
        "messages_table.html",
        {
            "request": request,
            "title": "Review Queue",
            "rows": items,
            "channels": channels,
            "selected_channel_id": "",
            "selected_status": "",
            "selected_limit": limit,
            "summary": summary,
            "financial_summary": financial_summary,
            "success": success,
            "error": error,
            "watched_channels": watched_channels,
            "watched_channel_groups": watched_channel_groups,
            "available_discord_channels": available_discord_channels,
        },
    )
@app.get("/admin/channels")
def admin_list_channels(session: Session = Depends(get_session)):
    rows = get_watched_channels(session)
    return [
        {
            "id": row.id,
            "channel_id": row.channel_id,
            "channel_name": row.channel_name,
            "is_enabled": row.is_enabled,
            "backfill_enabled": row.backfill_enabled,
        }
        for row in rows
    ]


@app.post("/admin/channels/add")
def admin_add_channel(
    channel_id: str = Form(...),
    channel_name: Optional[str] = Form(default=None),
    backfill_enabled: Optional[str] = Form(default=None),
    session: Session = Depends(get_session),
):
    channel = upsert_watched_channel(
        session,
        channel_id=channel_id,
        channel_name=channel_name,
        is_enabled=True,
        backfill_enabled=bool(backfill_enabled),
    )

    return RedirectResponse(
        url=f"/table?success=Saved+channel+{channel.channel_id}",
        status_code=303,
    )

@app.post("/admin/channels/toggle")
def admin_toggle_channel(
    channel_id: str = Form(...),
    session: Session = Depends(get_session),
):
    row = session.exec(
        select(WatchedChannel).where(WatchedChannel.channel_id == channel_id)
    ).first()

    if not row:
        return RedirectResponse(
            url=f"/table?error=Channel+{channel_id}+not+found",
            status_code=303,
        )

    row.is_enabled = not row.is_enabled
    row.updated_at = utcnow()
    session.add(row)
    session.commit()

    state = "enabled" if row.is_enabled else "disabled"
    return RedirectResponse(
        url=f"/table?success=Channel+{channel_id}+{state}",
        status_code=303,
    )


@app.post("/admin/channels/toggle-backfill")
def admin_toggle_channel_backfill(
    channel_id: str = Form(...),
    session: Session = Depends(get_session),
):
    row = session.exec(
        select(WatchedChannel).where(WatchedChannel.channel_id == channel_id)
    ).first()

    if not row:
        return RedirectResponse(
            url=f"/table?error=Channel+{channel_id}+not+found",
            status_code=303,
        )

    row.backfill_enabled = not row.backfill_enabled
    row.updated_at = utcnow()
    session.add(row)
    session.commit()

    state = "enabled" if row.backfill_enabled else "disabled"
    return RedirectResponse(
        url=f"/table?success=Backfill+for+channel+{channel_id}+{state}",
        status_code=303,
    )


@app.post("/admin/channels/remove")
def admin_remove_channel(
    channel_id: str = Form(...),
    session: Session = Depends(get_session),
):
    row = session.exec(
        select(WatchedChannel).where(WatchedChannel.channel_id == channel_id)
    ).first()

    if not row:
        return RedirectResponse(
            url=f"/table?error=Channel+{channel_id}+not+found",
            status_code=303,
        )

    session.delete(row)
    session.commit()

    return RedirectResponse(
        url=f"/table?success=Removed+channel+{channel_id}",
        status_code=303,
    )
@app.get("/admin/discord/channels")
def admin_list_discord_channels():
    channels = list_available_discord_channels()
    return channels
