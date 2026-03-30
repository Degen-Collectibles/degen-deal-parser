import asyncio
import json
from datetime import datetime, timezone
from typing import Optional

from sqlmodel import Session, select

from .db import managed_session
from .models import BackfillRequest

BACKFILL_POLL_SECONDS = 5.0


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def enqueue_backfill_request(
    session: Session,
    *,
    channel_id: Optional[str],
    after: Optional[datetime],
    before: Optional[datetime],
    limit_per_channel: Optional[int],
    oldest_first: bool,
    requested_by: Optional[str],
) -> BackfillRequest:
    request = BackfillRequest(
        channel_id=channel_id,
        after=after,
        before=before,
        limit_per_channel=limit_per_channel,
        oldest_first=oldest_first,
        requested_by=requested_by,
        status="queued",
        created_at=utcnow(),
    )
    session.add(request)
    session.commit()
    session.refresh(request)
    return request


def claim_next_backfill_request(session: Session) -> Optional[BackfillRequest]:
    request = session.exec(
        select(BackfillRequest)
        .where(BackfillRequest.status == "queued")
        .order_by(BackfillRequest.created_at, BackfillRequest.id)
    ).first()
    if not request:
        return None

    request.status = "processing"
    request.started_at = utcnow()
    request.finished_at = None
    request.error_message = None
    session.add(request)
    session.commit()
    session.refresh(request)
    return request


def mark_backfill_request_complete(
    session: Session,
    request_id: int,
    *,
    ok: bool,
    result: dict,
) -> None:
    request = session.get(BackfillRequest, request_id)
    if not request:
        return

    request.status = "completed" if ok else "failed"
    request.finished_at = utcnow()
    request.result_json = json.dumps(result)
    request.error_message = result.get("error")
    request.inserted_count = int(
        result.get("inserted", result.get("total_inserted", 0)) or 0
    )
    request.skipped_count = int(
        result.get("skipped", result.get("total_skipped", 0)) or 0
    )
    session.add(request)
    session.commit()


async def process_backfill_request_once(client) -> bool:
    if client is None or client.is_closed() or not client.is_ready():
        return False

    with managed_session() as session:
        request = claim_next_backfill_request(session)

    if not request or request.id is None:
        return False

    try:
        if request.channel_id:
            result = await client.backfill_channel(
                channel_id=int(request.channel_id),
                after=request.after,
                before=request.before,
                limit=request.limit_per_channel,
                oldest_first=request.oldest_first,
            )
        else:
            result = await client.backfill_enabled_channels(
                after=request.after,
                before=request.before,
                limit_per_channel=request.limit_per_channel,
                oldest_first=request.oldest_first,
            )
    except Exception as exc:
        result = {"ok": False, "error": str(exc)}

    with managed_session() as session:
        mark_backfill_request_complete(
            session,
            request.id,
            ok=bool(result.get("ok")),
            result=result,
        )

    return True


async def backfill_request_loop(stop_event: asyncio.Event, get_client) -> None:
    while not stop_event.is_set():
        try:
            processed = await process_backfill_request_once(get_client())
        except Exception as exc:
            print(f"[backfill] queue loop error: {exc}")
            processed = False

        if processed:
            continue

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=BACKFILL_POLL_SECONDS)
        except asyncio.TimeoutError:
            pass
