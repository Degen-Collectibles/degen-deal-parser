import asyncio
import base64
import mimetypes
import json
import logging
import uuid
import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from sqlalchemy import and_, case, func, or_, update
from sqlalchemy.exc import OperationalError
from sqlmodel import Session, select

from ..config import get_settings
from ..financial_values import (
    validate_optional_confidence,
    validate_optional_money,
    validate_strict_json_value,
)
from .corrections import auto_promote_eligible_patterns
from ..db import dispose_engine, is_sqlite_lock_error, managed_session
from .discord_ingest import get_discord_client, recover_attachment_assets_for_message, sync_attachment_assets
from ..display_media import (
    encode_bytes_as_vision_data_url,
    extract_image_urls,
    parse_attachment_urls_json,
)
from .financials import compute_financials
from ..models import (
    AttachmentAsset,
    DISCORD_SOURCE_REFRESH_REQUIRED_ERROR,
    DiscordMessage,
    discord_source_refresh_blocked,
    OperationsLog,
    ParseAttempt,
    Transaction,
    WatchedChannel,
    PARSE_FAILED,
    PARSE_IGNORED,
    PARSE_PARSED,
    PARSE_PENDING,
    PARSE_PROCESSING,
    PARSE_REVIEW_REQUIRED,
    expand_parse_status_filter_values,
    normalize_parse_status,
)
from ..ai_client import get_model, get_provider
from .parser import parse_message, TimedOutRowError
from .reparse_runs import safe_record_reparse_run_outcome
from ..runtime_logging import structured_log_line
from .transactions import (
    SourceRefreshRequiredError,
    StaleSourceRevisionError,
    invalidate_transactions_for_message,
    sync_transaction_from_message,
)

settings = get_settings()
STALE_PROCESSING_AFTER = timedelta(minutes=10)
STALE_RECOVERY_ERROR = "Recovered from stale processing state after worker interruption."
MAX_ATTEMPTS_ERROR = "Maximum parse attempts reached; requeue with attempt reset to retry."
OFFLINE_EDIT_REPARSE_ERROR = "Recovered refreshed message after offline audit."
logger = logging.getLogger(__name__)
MAX_RANGE_REPARSE_SELECTION = 10_000


class ParseClaimConflict(RuntimeError):
    """Raised when a parse attempt cannot exclusively own its full source group."""


class RangeReparseSelectionLimitError(ValueError):
    """Raised before mutation when a range reparse selection is too large."""


def _null_safe_value_condition(column, value):
    return column.is_(None) if value is None else column == value


@dataclass(frozen=True)
class RangeReparseSelectionSnapshot:
    message_id: int
    created_at: datetime
    content: str
    attachment_urls_json: str
    current_revision_id: int | None
    is_deleted: bool
    parse_status: str
    parse_attempts: int
    needs_review: bool
    reviewed_by: str | None
    reviewed_at: datetime | None
    source_refresh_required: bool
    active_parse_attempt_id: int | None
    stitched_group_id: str | None
    stitched_primary: bool
    stitched_message_ids_json: str
    active_reparse_run_id: str | None
    last_error: str | None


def snapshot_range_reparse_selection(
    row: DiscordMessage,
) -> RangeReparseSelectionSnapshot:
    if row.id is None:
        raise RangeReparseSelectionLimitError(
            "Range reparse selection contained an unpersisted Discord message."
        )
    return RangeReparseSelectionSnapshot(
        message_id=row.id,
        created_at=row.created_at,
        content=row.content,
        attachment_urls_json=row.attachment_urls_json,
        current_revision_id=row.current_revision_id,
        is_deleted=bool(row.is_deleted),
        parse_status=row.parse_status,
        parse_attempts=row.parse_attempts,
        needs_review=bool(row.needs_review),
        reviewed_by=row.reviewed_by,
        reviewed_at=row.reviewed_at,
        source_refresh_required=bool(row.source_refresh_required),
        active_parse_attempt_id=row.active_parse_attempt_id,
        stitched_group_id=row.stitched_group_id,
        stitched_primary=bool(row.stitched_primary),
        stitched_message_ids_json=row.stitched_message_ids_json,
        active_reparse_run_id=row.active_reparse_run_id,
        last_error=row.last_error,
    )


def freeze_range_reparse_selection(
    session: Session,
    stmt,
) -> list[RangeReparseSelectionSnapshot]:
    rows = session.exec(stmt.limit(MAX_RANGE_REPARSE_SELECTION + 1)).all()
    if len(rows) > MAX_RANGE_REPARSE_SELECTION:
        raise RangeReparseSelectionLimitError(
            "Range reparse matched more than "
            f"{MAX_RANGE_REPARSE_SELECTION:,} messages; narrow the date/channel/status "
            "filters and retry. No messages were changed."
        )
    return [snapshot_range_reparse_selection(row) for row in rows]


def range_reparse_selection_unchanged(
    row: DiscordMessage,
    snapshot: RangeReparseSelectionSnapshot,
) -> bool:
    return (
        row.id == snapshot.message_id
        and row.created_at == snapshot.created_at
        and row.content == snapshot.content
        and row.attachment_urls_json == snapshot.attachment_urls_json
        and row.current_revision_id == snapshot.current_revision_id
        and bool(row.is_deleted) == snapshot.is_deleted
        and row.parse_status == snapshot.parse_status
        and row.parse_attempts == snapshot.parse_attempts
        and bool(row.needs_review) == snapshot.needs_review
        and row.reviewed_by == snapshot.reviewed_by
        and row.reviewed_at == snapshot.reviewed_at
        and bool(row.source_refresh_required) == snapshot.source_refresh_required
        and row.active_parse_attempt_id == snapshot.active_parse_attempt_id
        and row.stitched_group_id == snapshot.stitched_group_id
        and bool(row.stitched_primary) == snapshot.stitched_primary
        and row.stitched_message_ids_json == snapshot.stitched_message_ids_json
        and row.active_reparse_run_id == snapshot.active_reparse_run_id
        and row.last_error == snapshot.last_error
    )


def cas_range_reparse_selection(
    session: Session,
    snapshot: RangeReparseSelectionSnapshot,
) -> bool:
    """Write-lock one unchanged range member, including on SQLite.

    PostgreSQL's preceding ``FOR UPDATE`` read already locks the row, while
    SQLite ignores that clause. This snapshot-conditioned no-op update closes
    the SQLite read/check/write gap and also provides a final CAS on every
    selected field before the reparse reset begins.
    """

    result = session.exec(
        update(DiscordMessage)
        .where(DiscordMessage.id == snapshot.message_id)
        .where(DiscordMessage.created_at == snapshot.created_at)
        .where(DiscordMessage.content == snapshot.content)
        .where(DiscordMessage.attachment_urls_json == snapshot.attachment_urls_json)
        .where(
            _null_safe_value_condition(
                DiscordMessage.current_revision_id,
                snapshot.current_revision_id,
            )
        )
        .where(DiscordMessage.is_deleted == snapshot.is_deleted)
        .where(DiscordMessage.parse_status == snapshot.parse_status)
        .where(DiscordMessage.parse_attempts == snapshot.parse_attempts)
        .where(DiscordMessage.needs_review == snapshot.needs_review)
        .where(
            _null_safe_value_condition(
                DiscordMessage.reviewed_by,
                snapshot.reviewed_by,
            )
        )
        .where(
            _null_safe_value_condition(
                DiscordMessage.reviewed_at,
                snapshot.reviewed_at,
            )
        )
        .where(
            DiscordMessage.source_refresh_required
            == snapshot.source_refresh_required
        )
        .where(
            _null_safe_value_condition(
                DiscordMessage.active_parse_attempt_id,
                snapshot.active_parse_attempt_id,
            )
        )
        .where(
            _null_safe_value_condition(
                DiscordMessage.stitched_group_id,
                snapshot.stitched_group_id,
            )
        )
        .where(DiscordMessage.stitched_primary == snapshot.stitched_primary)
        .where(
            DiscordMessage.stitched_message_ids_json
            == snapshot.stitched_message_ids_json
        )
        .where(
            _null_safe_value_condition(
                DiscordMessage.active_reparse_run_id,
                snapshot.active_reparse_run_id,
            )
        )
        .where(
            _null_safe_value_condition(
                DiscordMessage.last_error,
                snapshot.last_error,
            )
        )
        .values(last_seen_at=DiscordMessage.last_seen_at)
        .execution_options(synchronize_session=False)
    )
    return result.rowcount == 1


def quarantine_range_reparse_integrity_conflict(
    session: Session,
    *,
    message_id: int,
    error: Exception,
) -> None:
    quarantine_error = (
        f"{DISCORD_SOURCE_REFRESH_REQUIRED_ERROR}: "
        f"range reparse integrity blocked: {error}"
    )
    result = session.exec(
        update(DiscordMessage)
        .where(DiscordMessage.id == message_id)
        .values(
            parse_status=PARSE_FAILED,
            needs_review=True,
            source_refresh_required=True,
            last_error=quarantine_error,
            active_reparse_run_id=None,
        )
        .execution_options(synchronize_session=False)
    )
    if result.rowcount != 1:
        raise StaleSourceRevisionError(
            f"Could not quarantine range reparse source message_id={message_id}"
        )


@dataclass(frozen=True)
class MessageProjectionSnapshot:
    message_id: int
    content: str
    attachment_urls_json: str
    current_revision_id: int | None
    is_deleted: bool
    parse_status: str
    parse_attempts: int
    source_refresh_required: bool
    active_parse_attempt_id: int | None
    stitched_group_id: str | None
    stitched_primary: bool
    stitched_message_ids_json: str
    last_stitched_at: datetime | None
    active_reparse_run_id: str | None
    last_error: str | None
    needs_review: bool
    reviewed_by: str | None
    reviewed_at: datetime | None
    deal_type: str | None
    amount: float | None
    payment_method: str | None
    cash_direction: str | None
    category: str | None
    item_names_json: str
    items_in_json: str
    items_out_json: str
    trade_summary: str | None
    notes: str | None
    confidence: float | None
    image_summary: str | None
    entry_kind: str | None
    money_in: float | None
    money_out: float | None
    expense_category: str | None
    parse_disagreement_json: str | None


_MESSAGE_PROJECTION_CAS_FIELDS = (
    "content",
    "attachment_urls_json",
    "current_revision_id",
    "is_deleted",
    "parse_status",
    "parse_attempts",
    "source_refresh_required",
    "active_parse_attempt_id",
    "stitched_group_id",
    "stitched_primary",
    "stitched_message_ids_json",
    "last_stitched_at",
    "active_reparse_run_id",
    "last_error",
    "needs_review",
    "reviewed_by",
    "reviewed_at",
    "deal_type",
    "amount",
    "payment_method",
    "cash_direction",
    "category",
    "item_names_json",
    "items_in_json",
    "items_out_json",
    "trade_summary",
    "notes",
    "confidence",
    "image_summary",
    "entry_kind",
    "money_in",
    "money_out",
    "expense_category",
    "parse_disagreement_json",
)


def _message_projection_conditions(statement, projection):
    for field_name in _MESSAGE_PROJECTION_CAS_FIELDS:
        statement = statement.where(
            _null_safe_value_condition(
                getattr(DiscordMessage, field_name),
                getattr(projection, field_name),
            )
        )
    return statement


def snapshot_message_projection(row: DiscordMessage) -> MessageProjectionSnapshot:
    if row.id is None:
        raise ParseClaimConflict("cannot snapshot an unpersisted Discord message")
    return MessageProjectionSnapshot(
        message_id=row.id,
        content=row.content,
        attachment_urls_json=row.attachment_urls_json,
        current_revision_id=row.current_revision_id,
        is_deleted=bool(row.is_deleted),
        parse_status=row.parse_status,
        parse_attempts=row.parse_attempts,
        source_refresh_required=bool(row.source_refresh_required),
        active_parse_attempt_id=row.active_parse_attempt_id,
        stitched_group_id=row.stitched_group_id,
        stitched_primary=bool(row.stitched_primary),
        stitched_message_ids_json=row.stitched_message_ids_json,
        last_stitched_at=row.last_stitched_at,
        active_reparse_run_id=row.active_reparse_run_id,
        last_error=row.last_error,
        needs_review=bool(row.needs_review),
        reviewed_by=row.reviewed_by,
        reviewed_at=row.reviewed_at,
        deal_type=row.deal_type,
        amount=row.amount,
        payment_method=row.payment_method,
        cash_direction=row.cash_direction,
        category=row.category,
        item_names_json=row.item_names_json,
        items_in_json=row.items_in_json,
        items_out_json=row.items_out_json,
        trade_summary=row.trade_summary,
        notes=row.notes,
        confidence=row.confidence,
        image_summary=row.image_summary,
        entry_kind=row.entry_kind,
        money_in=row.money_in,
        money_out=row.money_out,
        expense_category=row.expense_category,
        parse_disagreement_json=row.parse_disagreement_json,
    )


def lock_message_projection_snapshots(
    session: Session,
    snapshots: list[MessageProjectionSnapshot],
) -> None:
    """CAS and write-lock every claimed projection before publishing parse output."""

    with session.no_autoflush:
        for snapshot in sorted(snapshots, key=lambda item: item.message_id):
            statement = _message_projection_conditions(
                update(DiscordMessage).where(
                    DiscordMessage.id == snapshot.message_id
                ),
                snapshot,
            )
            result = session.exec(
                statement
                .values(last_seen_at=DiscordMessage.last_seen_at)
                .execution_options(synchronize_session=False)
            )
            if result.rowcount != 1:
                raise StaleSourceRevisionError(
                    "Discord message source or parse ownership changed while parsing; "
                    f"discarding stale result for message_id={snapshot.message_id}"
                )


def release_attempt_claims(
    session: Session,
    attempt_id: int | None,
    message_ids: Iterable[int],
) -> set[int]:
    if attempt_id is None:
        return set()
    released_ids: set[int] = set()
    for message_id in sorted(set(message_ids)):
        result = session.exec(
            update(DiscordMessage)
            .where(DiscordMessage.id == message_id)
            .where(DiscordMessage.active_parse_attempt_id == attempt_id)
            .values(active_parse_attempt_id=None)
            .execution_options(synchronize_session=False)
        )
        if result.rowcount == 1:
            released_ids.add(message_id)
    return released_ids


def finish_attempt_if_unfinished(
    session: Session,
    attempt_id: int | None,
    *,
    success: bool,
    error: str | None,
) -> bool:
    if attempt_id is None:
        return False
    result = session.exec(
        update(ParseAttempt)
        .where(ParseAttempt.id == attempt_id)
        .where(ParseAttempt.finished_at == None)  # noqa: E711
        .values(
            success=success,
            error=error,
            finished_at=utcnow(),
        )
        .execution_options(synchronize_session=False)
    )
    return result.rowcount == 1


def discard_stale_parse(
    session: Session,
    *,
    row_id: int,
    attempt_id: int | None,
    affected_ids: Iterable[int],
    error: Exception,
) -> None:
    session.rollback()
    current_row = session.get(DiscordMessage, row_id)
    base_was_owned = (
        current_row is not None
        and attempt_id is not None
        and current_row.active_parse_attempt_id == attempt_id
    )
    owned_ids = list(
        session.exec(
            select(DiscordMessage.id).where(
                DiscordMessage.active_parse_attempt_id == attempt_id
            )
        ).all()
    ) if attempt_id is not None else []
    released_ids = release_attempt_claims(
        session,
        attempt_id,
        [*affected_ids, *owned_ids],
    )
    if (
        base_was_owned
        and row_id in released_ids
        and current_row is not None
        and canonical_status(current_row) == PARSE_PROCESSING
    ):
        set_row_status(current_row, PARSE_PENDING, error=str(error))
        current_row.parse_attempts = max((current_row.parse_attempts or 0) - 1, 0)
        session.add(current_row)
    finish_attempt_if_unfinished(
        session,
        attempt_id,
        success=False,
        error=str(error),
    )
    worker_log(
        action="parse_discarded_source_changed",
        row=current_row,
        level="warning",
        success=False,
        error=str(error),
        session=session,
    )
    session.commit()


def utcnow():
    return datetime.now(timezone.utc)


def canonical_status(row: DiscordMessage) -> str:
    return normalize_parse_status(
        row.parse_status,
        is_deleted=bool(row.is_deleted),
        needs_review=bool(row.needs_review),
    )


def set_row_status(
    row: DiscordMessage,
    status: str,
    *,
    error: str | None = None,
    clear_error: bool = False,
) -> None:
    row.parse_status = normalize_parse_status(status)
    row.needs_review = row.parse_status == PARSE_REVIEW_REQUIRED
    if clear_error:
        row.last_error = None
    elif error is not None:
        row.last_error = error


def worker_log(
    *,
    action: str,
    row: DiscordMessage | None = None,
    level: str = "info",
    success: bool | None = None,
    error: str | None = None,
    session: Session | None = None,
    **details,
) -> None:
    payload = {
        "message_id": getattr(row, "id", None),
        "discord_message_id": getattr(row, "discord_message_id", None),
        "channel": getattr(row, "channel_name", None),
        "channel_id": getattr(row, "channel_id", None),
        "current_state": canonical_status(row) if row is not None else None,
    }
    payload.update(details)
    message = structured_log_line(
        runtime="worker",
        action=action,
        success=success,
        error=error,
        **payload,
    )
    getattr(logger, level if level in {"debug", "info", "warning", "error"} else "info")(message)

    if session is None:
        return
    session.add(
        OperationsLog(
            event_type=f"queue.{action}",
            level="error" if level == "error" else level,
            source="worker",
            message=action,
            details_json=json.dumps(payload, default=str),
        )
    )


def reset_for_reprocess(
    row: DiscordMessage,
    *,
    reason: str,
    reset_attempts: bool = False,
) -> bool:
    if discord_source_refresh_blocked(
        row.last_error,
        row.source_refresh_required,
    ):
        # Only a successful canonical Discord refresh may release this
        # quarantine.  Reparse/retry helpers must not clear the legacy marker
        # by overwriting ``last_error`` or move the projection back to pending.
        row.source_refresh_required = True
        return False
    set_row_status(row, PARSE_PENDING, error=reason)
    row.reviewed_by = None
    row.reviewed_at = None
    if reset_attempts:
        row.parse_attempts = 0
    return True


def exhausted_retry_error(existing_error: str | None, *, reason: str) -> str:
    existing = (existing_error or "").strip()
    if not existing:
        return reason
    if existing.startswith(reason):
        return existing
    return f"{reason} Previous error: {existing}"


def row_retry_limit_already_exhausted(row: DiscordMessage, *, reason: str) -> bool:
    return (
        canonical_status(row) == PARSE_FAILED
        and (row.parse_attempts or 0) >= settings.parser_max_attempts
        and ((row.last_error or "").strip().startswith(reason))
    )


def exhaust_retry_limit(session: Session, row: DiscordMessage, *, reason: str) -> None:
    if row_retry_limit_already_exhausted(row, reason=reason):
        session.add(row)
        return

    exhausted_error = exhausted_retry_error(row.last_error, reason=reason)
    set_row_status(row, PARSE_FAILED, error=exhausted_error)
    session.add(row)
    worker_log(
        action="max_attempts_reached",
        row=row,
        level="warning",
        success=False,
        error=exhausted_error,
        session=session,
        parse_attempts=row.parse_attempts,
        max_attempts=settings.parser_max_attempts,
    )


def schedule_next_reprocess_run() -> datetime:
    return utcnow() + timedelta(hours=max(settings.parser_reprocess_interval_hours, 0.25))


def schedule_next_offline_audit_run() -> datetime:
    return utcnow() + timedelta(minutes=max(settings.periodic_offline_audit_interval_minutes, 1.0))


def schedule_next_auto_promote_run() -> datetime:
    return utcnow() + timedelta(minutes=max(settings.auto_promote_interval_minutes, 1.0))


def normalize_legacy_queue_states(session: Session) -> int:
    rows = session.exec(
        select(DiscordMessage).where(
            DiscordMessage.parse_status.in_(["queued", "needs_review", "deleted"])
        )
    ).all()
    changed = 0
    for row in rows:
        normalized_status = canonical_status(row)
        if row.parse_status == normalized_status:
            continue
        row.parse_status = normalized_status
        if normalized_status == PARSE_IGNORED and not row.last_error and row.is_deleted:
            row.last_error = "message deleted"
        session.add(row)
        changed += 1
    if changed:
        session.commit()
    return changed


def _attempt_timestamp(attempt: ParseAttempt) -> datetime | None:
    timestamp = attempt.finished_at or attempt.started_at
    if timestamp is not None and timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp


def latest_attempt_timestamp(session: Session, row_id: int) -> datetime | None:
    latest_attempt = session.exec(
        select(ParseAttempt)
        .where(ParseAttempt.message_id == row_id)
        .order_by(ParseAttempt.started_at.desc(), ParseAttempt.id.desc())
    ).first()
    if not latest_attempt:
        return None
    return _attempt_timestamp(latest_attempt)


def row_has_nearby_siblings(session: Session, row: DiscordMessage) -> bool:
    if row.id is None or row.is_deleted:
        return False
    if not row.channel_id:
        return False
    if not row.author_name:
        return False
    if row.reviewed_at is not None:
        return False
    if canonical_status(row) not in {PARSE_PARSED, PARSE_REVIEW_REQUIRED, PARSE_FAILED}:
        return False
    return len(
        build_stitch_group(
            session=session,
            row=row,
            window_seconds=settings.stitch_window_seconds,
            max_messages=settings.stitch_max_messages,
        )
    ) > 1


def _requeue_refreshed_message(row: DiscordMessage) -> None:
    reset_for_reprocess(row, reason=OFFLINE_EDIT_REPARSE_ERROR, reset_attempts=True)
    row.active_reparse_run_id = None


def reconcile_deleted_message(session: Session, row: DiscordMessage) -> None:
    invalidate_transactions_for_message(
        session,
        row,
        reason="Unmatched because a source Discord message was deleted.",
    )
    set_row_status(row, PARSE_IGNORED, error="message deleted")
    row.active_reparse_run_id = None
    session.add(row)


def reconcile_offline_audit_rows(session: Session, *, batch_size: int | None = None) -> int:
    effective_batch_size = batch_size or settings.parser_batch_size
    lookback_cutoff = utcnow() - timedelta(hours=max(settings.periodic_offline_audit_lookback_hours, 1.0))
    watched_channel_ids = [
        channel_id
        for channel_id in session.exec(
            select(WatchedChannel.channel_id).where(WatchedChannel.is_enabled == True)  # noqa: E712
        ).all()
        if channel_id
    ]

    changed = 0
    recently_touched = or_(
        DiscordMessage.created_at >= lookback_cutoff,
        DiscordMessage.edited_at >= lookback_cutoff,
        DiscordMessage.deleted_at >= lookback_cutoff,
        DiscordMessage.last_seen_at >= lookback_cutoff,
    )

    deleted_stmt = (
        select(DiscordMessage)
        .outerjoin(Transaction, Transaction.source_message_id == DiscordMessage.id)
        .where(recently_touched)
        .where(
            or_(
                DiscordMessage.is_deleted == True,  # noqa: E712
                DiscordMessage.deleted_at != None,  # noqa: E711
            )
        )
        .where(
            (DiscordMessage.parse_status != PARSE_IGNORED)
            | (DiscordMessage.last_error != "message deleted")
            | (Transaction.id != None)  # noqa: E711
        )
        .order_by(DiscordMessage.deleted_at.desc(), DiscordMessage.edited_at.desc(), DiscordMessage.created_at.desc())
        .limit(effective_batch_size)
    )
    if watched_channel_ids:
        deleted_stmt = deleted_stmt.where(DiscordMessage.channel_id.in_(watched_channel_ids))

    deleted_rows = session.exec(deleted_stmt).all()
    for row in deleted_rows:
        if not row.is_deleted:
            row.is_deleted = True
            row.deleted_at = row.deleted_at or utcnow()
        reconcile_deleted_message(session, row)
        worker_log(
            action="deleted_row_reconciled",
            row=row,
            success=True,
            session=session,
        )
        changed += 1

    edited_stmt = (
        select(DiscordMessage)
        .where(recently_touched)
        .where(DiscordMessage.is_deleted == False)  # noqa: E712
        .where(DiscordMessage.edited_at != None)  # noqa: E711
        .where(
            DiscordMessage.parse_status.in_(
                sorted(
                    expand_parse_status_filter_values(
                        [PARSE_PARSED, PARSE_REVIEW_REQUIRED, PARSE_FAILED, PARSE_IGNORED]
                    )
                )
            )
        )
        .order_by(DiscordMessage.edited_at.desc(), DiscordMessage.deleted_at.desc(), DiscordMessage.created_at.desc())
        .limit(effective_batch_size)
    )
    if watched_channel_ids:
        edited_stmt = edited_stmt.where(DiscordMessage.channel_id.in_(watched_channel_ids))

    edited_rows = session.exec(edited_stmt).all()
    for row in edited_rows:
        latest_attempt = session.exec(
            select(ParseAttempt)
            .where(ParseAttempt.message_id == row.id)
            .order_by(ParseAttempt.started_at.desc(), ParseAttempt.id.desc())
        ).first()
        last_attempt_timestamp = _attempt_timestamp(latest_attempt) if latest_attempt else None
        edited_at = row.edited_at
        if edited_at is not None and edited_at.tzinfo is None:
            edited_at = edited_at.replace(tzinfo=timezone.utc)

        if last_attempt_timestamp is not None and edited_at is not None and last_attempt_timestamp >= edited_at:
            continue

        _requeue_refreshed_message(row)
        session.add(row)
        worker_log(
            action="offline_edit_requeued",
            row=row,
            success=True,
            session=session,
            edited_at=edited_at,
            last_attempt_at=last_attempt_timestamp,
        )
        changed += 1

    if changed:
        session.commit()

    return changed

def queue_recent_stitch_audit_candidates(session: Session, *, batch_size: int | None = None) -> int:
    effective_batch_size = batch_size or settings.periodic_stitch_audit_limit
    audit_cutoff = utcnow() - timedelta(hours=max(settings.periodic_stitch_audit_lookback_hours, 0.25))
    min_age_cutoff = utcnow() - timedelta(minutes=max(settings.periodic_stitch_audit_min_age_minutes, 1))
    review_cutoff = utcnow() - timedelta(hours=max(settings.parser_reprocess_interval_hours, 0.25))
    watched_channel_ids = [
        channel_id
        for channel_id in session.exec(
            select(WatchedChannel.channel_id).where(WatchedChannel.is_enabled == True)  # noqa: E712
        ).all()
        if channel_id
    ]
    if not watched_channel_ids:
        return 0

    candidate_rows = session.exec(
        select(DiscordMessage)
        .where(DiscordMessage.channel_id.in_(watched_channel_ids))
        .where(DiscordMessage.is_deleted == False)  # noqa: E712
        .where(
            DiscordMessage.parse_status.in_(
                sorted(expand_parse_status_filter_values([PARSE_PARSED, PARSE_REVIEW_REQUIRED, PARSE_FAILED]))
            )
        )
        .where(DiscordMessage.reviewed_at == None)  # noqa: E711
        .where(
            or_(
                DiscordMessage.created_at >= audit_cutoff,
                DiscordMessage.edited_at >= audit_cutoff,
                DiscordMessage.deleted_at >= audit_cutoff,
                DiscordMessage.last_seen_at >= audit_cutoff,
            )
        )
        .where(DiscordMessage.created_at <= min_age_cutoff)
        .order_by(
            DiscordMessage.needs_review.desc(),
            DiscordMessage.edited_at.desc(),
            DiscordMessage.created_at.asc(),
            DiscordMessage.id.asc(),
        )
        .limit(max(effective_batch_size * 5, effective_batch_size))
    ).all()

    queued_count = 0
    for row in candidate_rows:
        if queued_count >= effective_batch_size:
            break
        if row.id is None:
            continue
        if canonical_status(row) not in {PARSE_PARSED, PARSE_REVIEW_REQUIRED, PARSE_FAILED}:
            continue
        if row.parse_attempts >= settings.parser_max_attempts:
            continue

        last_attempt_timestamp = latest_attempt_timestamp(session, row.id)
        edited_at = row.edited_at
        if edited_at is not None and edited_at.tzinfo is None:
            edited_at = edited_at.replace(tzinfo=timezone.utc)

        requeue_reason: str | None = None
        if edited_at is not None and (last_attempt_timestamp is None or edited_at > last_attempt_timestamp):
            requeue_reason = "recent stitch audit: edited after last parse"
        elif row_recently_attempted(session, row.id, review_cutoff):
            continue
        elif row_may_benefit_from_auto_reprocess(row):
            requeue_reason = "recent stitch audit: fragment-like"
        elif row_has_nearby_siblings(session, row):
            requeue_reason = "recent stitch audit: nearby siblings"

        if not requeue_reason:
            continue

        if not reset_for_reprocess(row, reason=requeue_reason, reset_attempts=False):
            continue
        row.parse_attempts = max((row.parse_attempts or 0) - 1, 0)
        session.add(row)
        worker_log(
            action="recent_stitch_audit_requeued",
            row=row,
            success=True,
            session=session,
            reason=requeue_reason,
            edited_at=edited_at,
            last_attempt_at=last_attempt_timestamp,
            parse_attempts=row.parse_attempts,
        )
        queued_count += 1

    if queued_count:
        session.commit()

    return queued_count


def close_or_recover_unfinished_attempts(session: Session) -> None:
    recovery_now = utcnow()
    cutoff = recovery_now - STALE_PROCESSING_AFTER
    attempts = session.exec(
        select(ParseAttempt)
        .where(ParseAttempt.finished_at == None)  # noqa: E711
        .order_by(ParseAttempt.started_at)
        .limit(5000)
    ).all()
    unfinished_attempt_message_ids = {attempt.message_id for attempt in attempts}

    changed = False
    for attempt in attempts:
        row = session.get(DiscordMessage, attempt.message_id)
        if not row:
            missing_error = attempt.error or "message missing during recovery"
            attempt_finished = finish_attempt_if_unfinished(
                session,
                attempt.id,
                success=False,
                error=missing_error,
            )
            worker_log(
                action="attempt_recovered_missing_row",
                level="warning",
                success=False,
                error=missing_error,
                session=session,
                parse_attempt_id=attempt.id,
                message_id=attempt.message_id,
            )
            changed = attempt_finished or changed
            continue

        owned_row_ids = list(
            session.exec(
                select(DiscordMessage.id).where(
                    DiscordMessage.active_parse_attempt_id == attempt.id
                )
            ).all()
        )

        attempt_started_at = attempt.started_at
        if attempt_started_at is not None and attempt_started_at.tzinfo is None:
            attempt_started_at = attempt_started_at.replace(tzinfo=timezone.utc)

        # A persisted claim is the authority. Never infer completion from a
        # constituent's terminal status while a fresh owner is still working.
        if owned_row_ids:
            if not attempt_started_at or attempt_started_at >= cutoff:
                continue
            released_ids = release_attempt_claims(session, attempt.id, owned_row_ids)
            if row.id in released_ids and canonical_status(row) == PARSE_PROCESSING:
                set_row_status(row, PARSE_PENDING, error=STALE_RECOVERY_ERROR)
                row.parse_attempts = max((row.parse_attempts or 0) - 1, 0)
                session.add(row)
            attempt_finished = finish_attempt_if_unfinished(
                session,
                attempt.id,
                success=False,
                error="recovered stale processing attempt",
            )
            changed = bool(released_ids or attempt_finished) or changed
            continue

        normalized_status = canonical_status(row)
        if row.parse_status != normalized_status:
            normalized = session.exec(
                update(DiscordMessage)
                .where(DiscordMessage.id == row.id)
                .where(DiscordMessage.active_parse_attempt_id == None)  # noqa: E711
                .where(DiscordMessage.parse_status == row.parse_status)
                .values(parse_status=normalized_status)
                .execution_options(synchronize_session=False)
            )
            if normalized.rowcount != 1:
                continue
            row.parse_status = normalized_status
            changed = True

        if normalized_status in {PARSE_PARSED, PARSE_REVIEW_REQUIRED, PARSE_IGNORED}:
            attempt_finished = finish_attempt_if_unfinished(
                session,
                attempt.id,
                success=True,
                error=None,
            )
            worker_log(
                action="attempt_closed_after_terminal_state",
                row=row,
                success=True,
                session=session,
                parse_attempt_id=attempt.id,
            )
            changed = attempt_finished or changed
            continue

        if normalized_status == PARSE_PROCESSING and attempt_started_at and attempt_started_at < cutoff:
            recovered_attempts = max((row.parse_attempts or 0) - 1, 0)
            recovered = session.exec(
                update(DiscordMessage)
                .where(DiscordMessage.id == row.id)
                .where(DiscordMessage.active_parse_attempt_id == None)  # noqa: E711
                .where(DiscordMessage.parse_status == PARSE_PROCESSING)
                .where(DiscordMessage.parse_attempts == row.parse_attempts)
                .values(
                    parse_status=PARSE_PENDING,
                    needs_review=False,
                    last_error=STALE_RECOVERY_ERROR,
                    parse_attempts=recovered_attempts,
                )
                .execution_options(synchronize_session=False)
            )
            if recovered.rowcount != 1:
                continue
            finish_attempt_if_unfinished(
                session,
                attempt.id,
                success=False,
                error="recovered stale processing attempt",
            )
            worker_log(
                action="stale_processing_recovered",
                row=row,
                level="warning",
                success=False,
                error="recovered stale processing attempt",
                session=session,
                parse_attempt_id=attempt.id,
                parse_attempts=recovered_attempts,
            )
            changed = True

    recovered_rows = session.exec(
        select(DiscordMessage)
        .where(DiscordMessage.parse_status.in_(sorted(expand_parse_status_filter_values([PARSE_PENDING]))))
        .where(DiscordMessage.parse_attempts >= settings.parser_max_attempts)
        .where(DiscordMessage.last_error == STALE_RECOVERY_ERROR)
        .where(DiscordMessage.active_parse_attempt_id == None)  # noqa: E711
    ).all()
    for row in recovered_rows:
        row.parse_attempts = max(settings.parser_max_attempts - 1, 0)
        session.add(row)
        changed = True

    exhausted_rows = session.exec(
        select(DiscordMessage)
        .where(
            DiscordMessage.parse_status.in_(
                sorted(expand_parse_status_filter_values([PARSE_PENDING, PARSE_PROCESSING]))
            )
        )
        .where(DiscordMessage.parse_attempts >= settings.parser_max_attempts)
        .where(DiscordMessage.active_parse_attempt_id == None)  # noqa: E711
    ).all()
    for row in exhausted_rows:
        exhaust_retry_limit(session, row, reason=MAX_ATTEMPTS_ERROR)
        changed = True

    orphaned_processing_rows = session.exec(
        select(DiscordMessage)
        .where(DiscordMessage.parse_status == PARSE_PROCESSING)
        .where(DiscordMessage.active_parse_attempt_id == None)  # noqa: E711
    ).all()
    for row in orphaned_processing_rows:
        if row.id in unfinished_attempt_message_ids:
            continue
        set_row_status(row, PARSE_PENDING, error="Recovered processing row without an active parse attempt.")
        row.parse_attempts = max((row.parse_attempts or 0) - 1, 0)
        session.add(row)
        worker_log(
            action="orphaned_processing_recovered",
            row=row,
            level="warning",
            success=False,
            error=row.last_error,
            session=session,
            parse_attempts=row.parse_attempts,
        )
        changed = True

    if changed:
        session.commit()


def clear_stitch_fields(row: DiscordMessage) -> None:
    row.stitched_group_id = None
    row.stitched_primary = False
    row.stitched_message_ids_json = "[]"


def clear_parsed_fields(row: DiscordMessage) -> None:
    row.deal_type = None
    row.amount = None
    row.payment_method = None
    row.cash_direction = None
    row.category = None
    row.item_names_json = "[]"
    row.items_in_json = "[]"
    row.items_out_json = "[]"
    row.trade_summary = None
    row.notes = None
    row.confidence = None
    row.needs_review = False
    row.image_summary = None
    row.entry_kind = None
    row.money_in = None
    row.money_out = None
    row.expense_category = None


def mark_grouped_child_ignored(row: DiscordMessage) -> None:
    clear_parsed_fields(row)
    set_row_status(row, PARSE_IGNORED, clear_error=True)


def find_stale_group_members(
    session: Session,
    group_rows: list[DiscordMessage],
) -> list[DiscordMessage]:
    row_ids = [grouped_row.id for grouped_row in group_rows if grouped_row.id is not None]
    stale_rows: list[DiscordMessage] = []

    for grouped_row in group_rows:
        prior_group_id = grouped_row.stitched_group_id
        if not prior_group_id:
            continue

        existing_group_rows = session.exec(
            select(DiscordMessage).where(DiscordMessage.stitched_group_id == prior_group_id)
        ).all()

        for existing_row in existing_group_rows:
            if existing_row.id in row_ids:
                continue
            if existing_row not in stale_rows:
                stale_rows.append(existing_row)

    return stale_rows


def apply_stale_group_member_changes(
    group_rows: list[DiscordMessage],
    primary_row: DiscordMessage,
    stale_rows: list[DiscordMessage],
) -> None:
    for stale_row in stale_rows:
        if discord_source_refresh_blocked(
            stale_row.last_error,
            stale_row.source_refresh_required,
        ):
            stale_row.source_refresh_required = True
            continue
        clear_stitch_fields(stale_row)
        clear_parsed_fields(stale_row)
        if not stale_row.is_deleted:
            reset_for_reprocess(stale_row, reason="re-queued after stitch group changed")
    if len(group_rows) == 1:
        clear_stitch_fields(primary_row)


def clear_stale_group_members(
    session: Session,
    group_rows: list[DiscordMessage],
    primary_row: DiscordMessage,
) -> list[DiscordMessage]:
    """Compatibility wrapper for synchronous callers; workers use two phases."""

    stale_rows = [
        stale
        for stale in find_stale_group_members(session, group_rows)
        if not stale.is_deleted
    ]
    apply_stale_group_member_changes(group_rows, primary_row, stale_rows)
    return stale_rows


def _attachment_asset_content_type(asset: AttachmentAsset) -> str:
    if asset.content_type:
        return asset.content_type.split(";", 1)[0].strip() or "application/octet-stream"
    guessed_type, _ = mimetypes.guess_type(asset.filename or "")
    return guessed_type or "application/octet-stream"


def _attachment_asset_data_url(asset: AttachmentAsset) -> str | None:
    """Return a vision-API-ready data URL for a cached attachment.

    Delegates to the shared helper in ``display_media`` which shrinks
    oversized images before base64-encoding. Bedrock-hosted Claude
    rejects any single image source over 5 MiB, so a raw large JPEG
    would fail the whole chat.completions call; the shrinker re-encodes
    at lower quality / resolution until it fits.
    """
    if not asset.data:
        return None
    content_type = _attachment_asset_content_type(asset)
    return encode_bytes_as_vision_data_url(asset.data, content_type)


def _cached_parser_image_inputs(session: Session, group_rows: list[DiscordMessage]) -> list[str]:
    row_ids = [row.id for row in group_rows if row.id is not None]
    if not row_ids:
        return []

    image_assets = session.exec(
        select(AttachmentAsset)
        .where(AttachmentAsset.message_id.in_(row_ids))
        .where(AttachmentAsset.is_image == True)  # noqa: E712
        .order_by(AttachmentAsset.message_id.asc(), AttachmentAsset.id.asc())
    ).all()
    if not image_assets:
        return []

    assets_by_message_id: dict[int, list[AttachmentAsset]] = {}
    for asset in image_assets:
        assets_by_message_id.setdefault(asset.message_id, []).append(asset)

    parser_inputs: list[str] = []
    for row in group_rows:
        if row.id is None:
            continue
        for asset in assets_by_message_id.get(row.id, []):
            data_url = _attachment_asset_data_url(asset)
            if data_url:
                parser_inputs.append(data_url)
    return parser_inputs


async def build_parser_attachment_inputs(
    session: Session,
    group_rows: list[DiscordMessage],
    fallback_attachment_urls: list[str],
) -> list[str]:
    parser_inputs = _cached_parser_image_inputs(session, group_rows)
    recovery_candidates = [
        (grouped_row.channel_id, grouped_row.discord_message_id, grouped_row.id)
        for grouped_row in group_rows
        if grouped_row.id is not None
        and extract_image_urls(parse_attachment_urls_json(grouped_row.attachment_urls_json))
        and grouped_row.channel_id
        and grouped_row.discord_message_id
    ]
    session.commit()
    if parser_inputs:
        return parser_inputs

    recovered_any = False
    for channel_id, discord_message_id, message_id in recovery_candidates:
        try:
            recovered = await recover_attachment_assets_for_message(
                channel_id=channel_id,
                discord_message_id=discord_message_id,
                message_row_id=message_id,
            )
            recovered_any = recovered_any or recovered
        except OperationalError as exc:
            if is_sqlite_lock_error(exc):
                worker_log(
                    action="attachment_recovery_sqlite_busy",
                    level="warning",
                    success=False,
                    error="SQLite busy during attachment recovery; falling back to URLs",
                    message_id=message_id,
                )
            else:
                raise

    if recovered_any:
        session.expire_all()
        parser_inputs = _cached_parser_image_inputs(session, group_rows)
        session.commit()
        if parser_inputs:
            return parser_inputs

    return fallback_attachment_urls


def auto_promote_once() -> None:
    with managed_session() as session:
        promoted = auto_promote_eligible_patterns(
            session,
            min_count=settings.auto_promote_min_count,
            min_confidence=settings.auto_promote_min_confidence,
        )
        for normalized_text in promoted:
            worker_log(
                action="auto_promoted_correction_pattern",
                normalized_text=normalized_text,
                session=session,
            )
        if promoted:
            session.commit()


async def parser_loop(stop_event: asyncio.Event):
    next_reprocess_at = schedule_next_reprocess_run()
    next_offline_audit_at = utcnow()
    next_auto_promote_at = utcnow() + timedelta(minutes=10)
    while not stop_event.is_set():
        try:
            await process_once()
            if settings.periodic_offline_audit_enabled and utcnow() >= next_offline_audit_at:
                await offline_audit_once()
                next_offline_audit_at = schedule_next_offline_audit_run()
            if settings.parser_reprocess_enabled and utcnow() >= next_reprocess_at:
                await auto_reprocess_once()
                next_reprocess_at = schedule_next_reprocess_run()
            if settings.auto_promote_enabled and utcnow() >= next_auto_promote_at:
                await asyncio.to_thread(auto_promote_once)
                next_auto_promote_at = schedule_next_auto_promote_run()
        except OperationalError as e:
            worker_log(action="loop_database_error", level="error", success=False, error=str(e))
            dispose_engine()
        except Exception as e:
            worker_log(action="loop_error", level="error", success=False, error=str(e))
        await asyncio.sleep(settings.parser_poll_seconds)


def _claim_projection_conditions(statement, row: DiscordMessage):
    return _message_projection_conditions(
        statement.where(DiscordMessage.id == row.id),
        row,
    )


def _sorted_unique_message_rows(
    rows: Iterable[DiscordMessage],
    *,
    exclude_ids: set[int] | None = None,
) -> list[DiscordMessage]:
    excluded = exclude_ids or set()
    unique_rows = {
        row.id: row
        for row in rows
        if row.id is not None and row.id not in excluded
    }
    return sorted(
        unique_rows.values(),
        key=lambda row: (row.created_at, row.id or 0),
    )


def _claim_rows_for_attempt(
    session: Session,
    *,
    attempt_id: int,
    rows: list[DiscordMessage],
    base_row_id: int | None = None,
) -> None:
    unique_rows = {row.id: row for row in rows if row.id is not None}
    for row_id in sorted(unique_rows):
        source_row = unique_rows[row_id]
        if discord_source_refresh_blocked(
            source_row.last_error,
            source_row.source_refresh_required,
        ):
            raise ParseClaimConflict(
                f"message_id={row_id} requires canonical Discord source refresh"
            )
        if source_row.active_parse_attempt_id not in {None, attempt_id}:
            raise ParseClaimConflict(
                f"message_id={row_id} is owned by another parse attempt"
            )
        if (
            row_id != base_row_id
            and source_row.active_parse_attempt_id is None
            and (source_row.is_deleted or canonical_status(source_row) == PARSE_PROCESSING)
        ):
            raise ParseClaimConflict(
                f"message_id={row_id} is not eligible for constituent claim"
            )
        statement = _claim_projection_conditions(update(DiscordMessage), source_row)
        values = {"active_parse_attempt_id": attempt_id}
        if row_id == base_row_id:
            if canonical_status(source_row) not in {PARSE_PENDING, PARSE_FAILED}:
                raise ParseClaimConflict(
                    f"message_id={row_id} is no longer eligible for parse claim"
                )
            values.update(
                parse_status=PARSE_PROCESSING,
                parse_attempts=(source_row.parse_attempts or 0) + 1,
            )
        result = session.exec(
            statement.values(**values).execution_options(synchronize_session=False)
        )
        if result.rowcount != 1:
            raise ParseClaimConflict(
                f"message_id={row_id} changed or is owned by another parse attempt"
            )


def _parse_claim_rows(session: Session, row: DiscordMessage) -> list[DiscordMessage]:
    group_rows = [row]
    if settings.stitch_enabled:
        group_rows = build_stitch_group(
            session=session,
            row=row,
            window_seconds=settings.stitch_window_seconds,
            max_messages=settings.stitch_max_messages,
        )
    group_rows = _sorted_unique_message_rows(group_rows)
    group_ids = {grouped.id for grouped in group_rows if grouped.id is not None}
    stale_rows = _sorted_unique_message_rows(
        find_stale_group_members(session, group_rows),
        exclude_ids=group_ids,
    )
    return group_rows + stale_rows


def reload_and_validate_parse_claim(
    session: Session,
    *,
    row_id: int,
    attempt_id: int,
    group_row_ids: Iterable[int],
    stale_row_ids: Iterable[int],
) -> tuple[ParseAttempt, DiscordMessage, list[DiscordMessage], list[DiscordMessage]]:
    """Reload and validate an expanded group claim before any external await."""

    group_ids = list(group_row_ids)
    stale_ids = list(stale_row_ids)
    if len(group_ids) != len(set(group_ids)) or row_id not in group_ids:
        raise ParseClaimConflict("parse claim group membership is invalid")
    if len(stale_ids) != len(set(stale_ids)) or set(group_ids) & set(stale_ids):
        raise ParseClaimConflict("parse claim stale membership is invalid")

    expected_ids = [*group_ids, *stale_ids]
    session.expire_all()
    attempt = session.get(ParseAttempt, attempt_id)
    if (
        attempt is None
        or attempt.message_id != row_id
        or attempt.finished_at is not None
    ):
        raise ParseClaimConflict(
            "parse attempt changed or finished after group claim expansion"
        )

    reloaded_rows = session.exec(
        select(DiscordMessage)
        .where(DiscordMessage.id.in_(expected_ids))
        .order_by(DiscordMessage.created_at, DiscordMessage.id)
    ).all()
    rows_by_id = {
        source_row.id: source_row
        for source_row in reloaded_rows
        if source_row.id is not None
    }
    if len(reloaded_rows) != len(expected_ids) or set(rows_by_id) != set(expected_ids):
        raise ParseClaimConflict(
            "parse claim source membership changed after group claim expansion"
        )

    for source_row in reloaded_rows:
        if source_row.active_parse_attempt_id != attempt_id:
            raise ParseClaimConflict(
                f"message_id={source_row.id} is no longer owned by parse attempt"
            )
        if source_row.is_deleted:
            raise ParseClaimConflict(
                f"message_id={source_row.id} was deleted after group claim expansion"
            )
        if discord_source_refresh_blocked(
            source_row.last_error,
            source_row.source_refresh_required,
        ):
            raise ParseClaimConflict(
                f"message_id={source_row.id} requires canonical Discord source refresh"
            )

    base_row = rows_by_id[row_id]
    if canonical_status(base_row) != PARSE_PROCESSING:
        raise ParseClaimConflict(
            "parse attempt base message is no longer canonically processing"
        )

    group_rows = sorted(
        (rows_by_id[group_id] for group_id in group_ids),
        key=lambda source_row: (source_row.created_at, source_row.id or 0),
    )
    stale_rows = sorted(
        (rows_by_id[stale_id] for stale_id in stale_ids),
        key=lambda source_row: (source_row.created_at, source_row.id or 0),
    )
    return attempt, base_row, group_rows, stale_rows


def claim_message_for_parse(session: Session, row_id: int) -> ParseAttempt | None:
    """Atomically claim a base row and every source row its parse may publish."""

    row = session.get(DiscordMessage, row_id)
    if row is None or row.is_deleted:
        return None
    if canonical_status(row) not in {PARSE_PENDING, PARSE_FAILED}:
        return None
    if (row.parse_attempts or 0) >= settings.parser_max_attempts:
        return None
    if row.active_parse_attempt_id is not None:
        return None

    claim_rows = _parse_claim_rows(session, row)
    attempt_id: int | None = None
    try:
        with session.begin_nested():
            attempt = ParseAttempt(
                message_id=row.id,
                attempt_number=(row.parse_attempts or 0) + 1,
                model_used=get_model(),
                provider_used=get_provider(),
            )
            session.add(attempt)
            session.flush()
            attempt_id = attempt.id
            _claim_rows_for_attempt(
                session,
                attempt_id=attempt.id,
                rows=claim_rows,
                base_row_id=row.id,
            )
    except ParseClaimConflict:
        session.expire_all()
        return None

    session.expire_all()
    return session.get(ParseAttempt, attempt_id) if attempt_id is not None else None


async def process_once():
    with managed_session() as session:
        normalize_legacy_queue_states(session)
        close_or_recover_unfinished_attempts(session)
        live_priority_cutoff = utcnow() - timedelta(
            hours=max(settings.parser_live_priority_lookback_hours, 0.0)
        )
        live_priority_bucket = case(
            (DiscordMessage.created_at >= live_priority_cutoff, 0),
            else_=1,
        )

        rows = session.exec(
            select(DiscordMessage)
            .where(
                DiscordMessage.parse_status.in_(
                    sorted(expand_parse_status_filter_values([PARSE_PENDING, PARSE_FAILED]))
                )
            )
            .where(DiscordMessage.parse_attempts < settings.parser_max_attempts)
            .where(DiscordMessage.active_parse_attempt_id == None)  # noqa: E711
            .order_by(live_priority_bucket, DiscordMessage.created_at, DiscordMessage.id)
            .limit(settings.parser_batch_size)
        ).all()

        skipped_rows = session.exec(
            select(DiscordMessage)
            .where(
                DiscordMessage.parse_status.in_(
                    sorted(expand_parse_status_filter_values([PARSE_PENDING, PARSE_FAILED]))
                )
            )
            .where(DiscordMessage.parse_attempts >= settings.parser_max_attempts)
            .where(DiscordMessage.active_parse_attempt_id == None)  # noqa: E711
            .order_by(live_priority_bucket, DiscordMessage.created_at, DiscordMessage.id)
            .limit(settings.parser_batch_size)
        ).all()
        for row in skipped_rows:
            exhaust_retry_limit(session, row, reason=MAX_ATTEMPTS_ERROR)

        candidate_ids = [row.id for row in rows if row.id is not None]
        session.commit()

    # Claim and immediately process one group at a time. This avoids parking a
    # later batch claim behind an unrelated AI call where recovery could age it
    # out before work begins.
    for row_id in candidate_ids:
        claimed: tuple[int, int] | None = None
        with managed_session() as session:
            attempt = claim_message_for_parse(session, row_id)
            if attempt is None or attempt.id is None:
                continue
            claimed_row = session.get(DiscordMessage, row_id)
            claimed = (row_id, attempt.id)
            worker_log(
                action="processing_started",
                row=claimed_row,
                success=True,
                session=session,
                parse_attempts=claimed_row.parse_attempts,
                parse_attempt_id=attempt.id,
            )
            session.commit()
        await process_row(*claimed)


def row_may_benefit_from_auto_reprocess(row: DiscordMessage) -> bool:
    if row.is_deleted or row.reviewed_at is not None:
        return False

    text = normalize_text(row.content)

    if canonical_status(row) == PARSE_REVIEW_REQUIRED:
        return True
    if row.confidence is None or float(row.confidence) < 0.9:
        return True
    if not row.stitched_group_id and (looks_like_fragment(row) or is_payment_method_only_text(text)):
        return True
    if not row.stitched_group_id and has_images(row) and len(text) <= 30:
        return True

    return False


def row_recently_attempted(session: Session, row_id: int, cutoff: datetime) -> bool:
    latest_attempt = session.exec(
        select(ParseAttempt)
        .where(ParseAttempt.message_id == row_id)
        .order_by(ParseAttempt.started_at.desc())
    ).first()
    if not latest_attempt or latest_attempt.started_at is None:
        return False

    started_at = latest_attempt.started_at
    if started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=timezone.utc)
    return started_at >= cutoff


def queue_auto_reprocess_candidates(
    session: Session,
    *,
    batch_size: int | None = None,
    force: bool = False,
) -> int:
    queued_count = 0
    effective_batch_size = batch_size or settings.parser_reprocess_batch_size
    review_cutoff = utcnow() - timedelta(hours=max(settings.parser_reprocess_interval_hours, 0.25))
    min_age_cutoff = utcnow() - timedelta(minutes=max(settings.parser_reprocess_min_age_minutes, 1))
    lookback_cutoff = utcnow() - timedelta(days=max(settings.parser_reprocess_lookback_days, 1))

    candidate_rows = session.exec(
        select(DiscordMessage)
        .where(DiscordMessage.is_deleted == False)
        .where(
            DiscordMessage.parse_status.in_(
                sorted(expand_parse_status_filter_values([PARSE_PARSED, PARSE_REVIEW_REQUIRED]))
            )
        )
        .where(DiscordMessage.reviewed_at == None)  # noqa: E711
        .where(DiscordMessage.created_at >= lookback_cutoff)
        .where(DiscordMessage.created_at <= min_age_cutoff)
        .order_by(
            DiscordMessage.needs_review.desc(),
            DiscordMessage.created_at.asc(),
            DiscordMessage.id.asc(),
        )
        .limit(max(effective_batch_size * 5, effective_batch_size))
    ).all()

    for row in candidate_rows:
        if queued_count >= effective_batch_size:
            break
        if row.id is None:
            continue
        if not force and not row_may_benefit_from_auto_reprocess(row):
            continue
        if not force and row_recently_attempted(session, row.id, review_cutoff):
            continue

        if not reset_for_reprocess(
            row,
            reason="manual reprocess" if force else "auto reprocess",
            reset_attempts=force,
        ):
            continue
        if not force:
            row.parse_attempts = max((row.parse_attempts or 0) - 1, 0)
        session.add(row)
        worker_log(
            action="reprocess_queued",
            row=row,
            success=True,
            session=session,
            force=force,
            parse_attempts=row.parse_attempts,
        )
        queued_count += 1

    if queued_count:
        session.commit()

    return queued_count


def queue_reparse_range(
    session: Session,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    channel_id: str | None = None,
    include_statuses: Iterable[str] | None = None,
    include_reviewed: bool = False,
    reset_attempts: bool = True,
    reason: str = "manual range reparse",
    reparse_run_id: str | None = None,
) -> dict[str, int]:
    requested_statuses = {
        normalize_parse_status(status)
        for status in (include_statuses or (PARSE_PARSED, PARSE_REVIEW_REQUIRED, PARSE_FAILED, PARSE_IGNORED))
        if status and normalize_parse_status(status) != PARSE_PROCESSING
    }
    if not requested_statuses:
        requested_statuses = {PARSE_PARSED, PARSE_REVIEW_REQUIRED, PARSE_FAILED, PARSE_IGNORED}
    raw_filter_statuses = expand_parse_status_filter_values(requested_statuses)

    stmt = (
        select(DiscordMessage)
        .where(DiscordMessage.is_deleted == False)
        .where(DiscordMessage.parse_status.in_(sorted(raw_filter_statuses)))
        .order_by(DiscordMessage.created_at, DiscordMessage.id)
    )

    if start is not None:
        stmt = stmt.where(DiscordMessage.created_at >= start)
    if end is not None:
        stmt = stmt.where(DiscordMessage.created_at <= end)
    if channel_id:
        stmt = stmt.where(DiscordMessage.channel_id == channel_id)
    skipped_reviewed_count = 0
    if not include_reviewed:
        review_scope = stmt.subquery()
        skipped_reviewed_count = int(
            session.exec(
                select(func.count())
                .select_from(review_scope)
                .where(review_scope.c.reviewed_at != None)  # noqa: E711
            ).one()
        )
        stmt = stmt.where(DiscordMessage.reviewed_at == None)  # noqa: E711

    chunk_size = 500
    result = {
        "matched": 0,
        "queued": 0,
        "already_queued": 0,
        "skipped_quarantined": 0,
        "skipped_integrity": 0,
        "skipped_changed": 0,
        "skipped_reviewed": skipped_reviewed_count,
        "first_message_id": None,
        "last_message_id": None,
        "first_message_created_at": None,
        "last_message_created_at": None,
    }

    selection = freeze_range_reparse_selection(session, stmt)
    if not selection:
        return result
    result["matched"] = len(selection)
    result["first_message_id"] = selection[0].message_id
    result["first_message_created_at"] = selection[0].created_at
    result["last_message_id"] = selection[-1].message_id
    result["last_message_created_at"] = selection[-1].created_at

    for chunk_start in range(0, len(selection), chunk_size):
        batch_snapshots = selection[chunk_start : chunk_start + chunk_size]
        batch_ids = [snapshot.message_id for snapshot in batch_snapshots]
        # The initial selection objects may still be in the identity map. Force
        # a fresh locked read so a concurrent status/review/source update is
        # observed before this routine mutates the row.
        session.expire_all()
        batch_rows = session.exec(
            select(DiscordMessage)
            .where(DiscordMessage.id.in_(batch_ids))
            .order_by(DiscordMessage.id)
            .with_for_update()
            .execution_options(populate_existing=True)
        ).all()
        rows_by_id = {row.id: row for row in batch_rows if row.id is not None}

        chunk_touched = False
        for snapshot in batch_snapshots:
            row = rows_by_id.get(snapshot.message_id)
            if row is None or not range_reparse_selection_unchanged(row, snapshot):
                result["skipped_changed"] += 1
                continue
            if not cas_range_reparse_selection(session, snapshot):
                session.expire(row)
                result["skipped_changed"] += 1
                continue
            if discord_source_refresh_blocked(
                row.last_error,
                row.source_refresh_required,
            ):
                row.source_refresh_required = True
                session.add(row)
                result["skipped_quarantined"] += 1
                chunk_touched = True
                continue
            if canonical_status(row) == PARSE_PENDING:
                try:
                    with session.begin_nested():
                        set_row_status(row, PARSE_PENDING, error=reason)
                        row.active_reparse_run_id = reparse_run_id
                        session.add(row)
                        sync_transaction_from_message(session, row)
                except (SourceRefreshRequiredError, StaleSourceRevisionError) as exc:
                    quarantine_range_reparse_integrity_conflict(
                        session,
                        message_id=snapshot.message_id,
                        error=exc,
                    )
                    result["skipped_integrity"] += 1
                    chunk_touched = True
                    continue
                result["already_queued"] += 1
                chunk_touched = True
                continue

            try:
                with session.begin_nested():
                    if not reset_for_reprocess(
                        row,
                        reason=reason,
                        reset_attempts=reset_attempts,
                    ):
                        session.add(row)
                        result["skipped_quarantined"] += 1
                        chunk_touched = True
                        continue
                    row.active_reparse_run_id = reparse_run_id
                    session.add(row)
                    sync_transaction_from_message(session, row)
            except (SourceRefreshRequiredError, StaleSourceRevisionError) as exc:
                quarantine_range_reparse_integrity_conflict(
                    session,
                    message_id=snapshot.message_id,
                    error=exc,
                )
                result["skipped_integrity"] += 1
                chunk_touched = True
                continue
            result["queued"] += 1
            chunk_touched = True

        if chunk_touched:
            session.commit()

    return result


async def auto_reprocess_once():
    with managed_session() as session:
        queue_auto_reprocess_candidates(session)


async def offline_audit_once():
    with managed_session() as session:
        worker_log(
            action="offline_audit_started",
            success=True,
            session=session,
            periodic_limit=settings.periodic_offline_audit_limit_per_channel,
            lookback_hours=settings.periodic_offline_audit_lookback_hours,
        )
        deleted_or_edited = reconcile_offline_audit_rows(
            session,
            batch_size=settings.periodic_offline_audit_limit_per_channel,
        )
        worker_log(
            action="offline_audit_completed",
            success=True,
            session=session,
            deleted_or_edited=deleted_or_edited,
        )


async def run_periodic_stitch_audit_once() -> dict | None:
    if not settings.periodic_stitch_audit_enabled:
        return None

    lookback_hours = max(settings.periodic_stitch_audit_lookback_hours, 0.25)
    min_age_minutes = max(settings.periodic_stitch_audit_min_age_minutes, 1)
    batch_limit = max(settings.periodic_stitch_audit_limit, 1)

    with managed_session() as session:
        queued = queue_recent_stitch_audit_candidates(session, batch_size=batch_limit)
        worker_log(
            action="recent_stitch_audit_completed",
            success=True,
            session=session,
            lookback_hours=lookback_hours,
            min_age_minutes=min_age_minutes,
            batch_limit=batch_limit,
            queued=queued,
        )

    return {
        "ok": True,
        "lookback_hours": lookback_hours,
        "min_age_minutes": min_age_minutes,
        "batch_limit": batch_limit,
        "queued": queued,
    }


async def periodic_stitch_audit_loop(stop_event: asyncio.Event) -> None:
    if not settings.periodic_stitch_audit_enabled or not settings.parser_worker_enabled:
        return

    interval_minutes = max(settings.periodic_stitch_audit_interval_minutes, 5.0)
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_minutes * 60)
            break
        except asyncio.TimeoutError:
            pass

        try:
            await run_periodic_stitch_audit_once()
        except OperationalError as exc:
            worker_log(
                action="recent_stitch_audit_database_error",
                level="error",
                success=False,
                error=str(exc),
            )
            dispose_engine()
        except Exception as exc:
            worker_log(
                action="recent_stitch_audit_failed",
                level="error",
                success=False,
                error=str(exc),
            )


async def process_row(row_id: int, attempt_id: int | None = None):
    with managed_session() as session:
        attempt_id_was_supplied = attempt_id is not None
        row = session.get(DiscordMessage, row_id)
        if not row:
            worker_log(
                action="processing_skipped_missing_row",
                level="warning",
                success=False,
                error="message missing",
                session=session,
                message_id=row_id,
            )
            session.commit()
            return

        # Production dispatch always supplies the exact claimed attempt. Direct
        # tests may omit it, but fallback resolution happens once, here, before
        # attachment preparation or parser awaits.
        if attempt_id is not None:
            attempt = session.get(ParseAttempt, attempt_id)
        elif row.active_parse_attempt_id is not None:
            attempt = session.get(ParseAttempt, row.active_parse_attempt_id)
            attempt_id = row.active_parse_attempt_id
        else:
            attempt = session.exec(
                select(ParseAttempt)
                .where(ParseAttempt.message_id == row.id)
                .where(ParseAttempt.finished_at == None)  # noqa: E711
                .order_by(ParseAttempt.id.desc())
            ).first()
            attempt_id = attempt.id if attempt is not None else None

        if attempt is not None and (
            attempt.message_id != row.id or attempt.finished_at is not None
        ):
            worker_log(
                action="processing_skipped_invalid_claim",
                row=row,
                level="warning",
                success=False,
                error="parse attempt does not own this unfinished message",
                session=session,
                parse_attempt_id=attempt_id,
            )
            session.commit()
            return
        if attempt_id is not None and attempt is None:
            worker_log(
                action="processing_skipped_missing_attempt",
                row=row,
                level="warning",
                success=False,
                error="claimed parse attempt is missing",
                session=session,
                parse_attempt_id=attempt_id,
            )
            session.commit()
            return
        if (
            attempt_id_was_supplied
            and attempt_id is not None
            and row.active_parse_attempt_id != attempt_id
        ):
            worker_log(
                action="processing_skipped_lost_claim",
                row=row,
                level="warning",
                success=False,
                error="parse attempt no longer owns its base message",
                session=session,
                parse_attempt_id=attempt_id,
            )
            session.commit()
            return

        current_status = canonical_status(row)
        active_reparse_run_id = row.active_reparse_run_id

        if row.is_deleted:
            reconcile_deleted_message(session, row)
            worker_log(
                action="processing_skipped_deleted",
                row=row,
                level="warning",
                success=False,
                error=row.last_error,
                session=session,
            )
            finish_attempt_if_unfinished(
                session,
                attempt_id,
                success=False,
                error=row.last_error,
            )
            if attempt_id is not None:
                owned_ids = session.exec(
                    select(DiscordMessage.id).where(
                        DiscordMessage.active_parse_attempt_id == attempt_id
                    )
                ).all()
                release_attempt_claims(session, attempt_id, owned_ids)
            session.commit()
            safe_record_reparse_run_outcome(
                run_id=active_reparse_run_id,
                success=False,
                error_message=row.last_error,
            )
            return

        if current_status not in [PARSE_PROCESSING, PARSE_PENDING, PARSE_FAILED]:
            worker_log(
                action="processing_skipped_state",
                row=row,
                level="warning",
                success=False,
                error=f"skip because state is {current_status}",
                session=session,
            )
            finish_attempt_if_unfinished(
                session,
                attempt_id,
                success=False,
                error=f"skip because state is {current_status}",
            )
            if attempt_id is not None:
                owned_ids = session.exec(
                    select(DiscordMessage.id).where(
                        DiscordMessage.active_parse_attempt_id == attempt_id
                    )
                ).all()
                release_attempt_claims(session, attempt_id, owned_ids)
            row.active_reparse_run_id = None
            session.add(row)
            session.commit()
            safe_record_reparse_run_outcome(
                run_id=active_reparse_run_id,
                success=False,
                error_message=f"skip because state is {current_status}",
            )
            return

        group_rows = [row]
        if settings.stitch_enabled:
            group_rows = build_stitch_group(
                session=session,
                row=row,
                window_seconds=settings.stitch_window_seconds,
                max_messages=settings.stitch_max_messages,
            )

        group_rows = _sorted_unique_message_rows(group_rows)
        primary_row = group_rows[0]
        group_ids = {grouped.id for grouped in group_rows if grouped.id is not None}
        stale_rows = _sorted_unique_message_rows(
            find_stale_group_members(session, group_rows),
            exclude_ids=group_ids,
        )
        if attempt_id is not None:
            previously_claimed_rows = session.exec(
                select(DiscordMessage).where(
                    DiscordMessage.active_parse_attempt_id == attempt_id
                ).order_by(DiscordMessage.created_at, DiscordMessage.id)
            ).all()
            stale_rows = _sorted_unique_message_rows(
                [*stale_rows, *previously_claimed_rows],
                exclude_ids=group_ids,
            )
        affected_rows = group_rows + stale_rows
        affected_ids = [affected.id for affected in affected_rows if affected.id is not None]

        if attempt_id is not None:
            try:
                with session.begin_nested():
                    _claim_rows_for_attempt(
                        session,
                        attempt_id=attempt_id,
                        rows=affected_rows,
                        base_row_id=(
                            row_id
                            if canonical_status(row) in {PARSE_PENDING, PARSE_FAILED}
                            else None
                        ),
                    )
                session.commit()
                attempt, row, group_rows, stale_rows = reload_and_validate_parse_claim(
                    session,
                    row_id=row_id,
                    attempt_id=attempt_id,
                    group_row_ids=[
                        grouped.id for grouped in group_rows if grouped.id is not None
                    ],
                    stale_row_ids=[
                        stale.id for stale in stale_rows if stale.id is not None
                    ],
                )
            except ParseClaimConflict as exc:
                discard_stale_parse(
                    session,
                    row_id=row_id,
                    attempt_id=attempt_id,
                    affected_ids=affected_ids,
                    error=exc,
                )
                return

            primary_row = group_rows[0]
            affected_rows = group_rows + stale_rows
            affected_ids = [
                affected.id for affected in affected_rows if affected.id is not None
            ]

        source_snapshots = [snapshot_message_projection(affected) for affected in affected_rows]
        combined_text, combined_attachments, grouped_row_ids = combine_group_payload(group_rows)
        parser_author_name = row.author_name or ""
        parser_channel_name = row.channel_name or ""
        session.commit()

        group_id = str(uuid.uuid4()) if len(group_rows) > 1 else None
        stitched_at = utcnow() if group_id else None

        try:
            parser_attachment_inputs = await build_parser_attachment_inputs(
                session,
                group_rows,
                combined_attachments,
            )
            result = await parse_message(
                content=combined_text,
                attachment_urls=parser_attachment_inputs,
                author_name=parser_author_name,
                channel_name=parser_channel_name,
            )
            parsed_amount = validate_optional_money(
                result.get("parsed_amount"),
                field_name="parsed amount",
            )
            parsed_confidence = validate_optional_confidence(
                result.get("confidence"),
                field_name="confidence",
            )
            validate_strict_json_value(result, field_name="parser result")
            lock_message_projection_snapshots(session, source_snapshots)
            apply_stale_group_member_changes(group_rows, primary_row, stale_rows)
            worker_log(
                action="parse_started",
                row=row,
                success=True,
                session=session,
                grouped_message_ids=grouped_row_ids,
                attachment_count=len(combined_attachments),
            )
            normalized_cash_direction = result.get("parsed_cash_direction") if result.get("parsed_type") == "trade" else None
            learned_rule_event = result.pop("_learned_rule_event", None)
            usage = result.pop("_openai_usage", None) or {}
            model_used = result.pop("_openai_model", None)
            provider_used = result.pop("_ai_provider", None)
            parse_disagreement = result.pop("_parse_disagreement", None)
            parse_agreement = result.pop("_parse_agreement", False)
            if parse_disagreement:
                worker_log(
                    action="parse_disagreement",
                    row=primary_row,
                    level="info",
                    success=True,
                    session=session,
                    grouped_message_ids=grouped_row_ids,
                    disagreement_fields=parse_disagreement.get("fields"),
                    rule_parse=parse_disagreement.get("rule"),
                    ai_parse=parse_disagreement.get("ai"),
                )
            elif parse_agreement:
                worker_log(
                    action="parse_agreement",
                    row=primary_row,
                    level="info",
                    success=True,
                    session=session,
                    grouped_message_ids=grouped_row_ids,
                )
            if learned_rule_event:
                learned_rule_status = learned_rule_event.get("status") or "unknown"
                learned_rule_reason = learned_rule_event.get("reason")
                worker_log(
                    action=f"learned_rule_{learned_rule_status}",
                    row=row,
                    success=learned_rule_status == "applied",
                    level="warning" if learned_rule_status == "rejected" else "info",
                    error=None if learned_rule_status == "applied" else learned_rule_reason,
                    session=session,
                    grouped_message_ids=grouped_row_ids,
                    **learned_rule_event,
                )
            financials = compute_financials(
                parsed_type=result.get("parsed_type"),
                parsed_category=result.get("parsed_category"),
                amount=parsed_amount,
                cash_direction=normalized_cash_direction,
                message_text=combined_text,
            )
            validated_money_in = validate_optional_money(
                financials.money_in,
                field_name="money in",
            )
            validated_money_out = validate_optional_money(
                financials.money_out,
                field_name="money out",
            )

            for grouped_row in group_rows:
                grouped_row.stitched_group_id = group_id
                grouped_row.stitched_primary = bool(
                    group_id and grouped_row.id == primary_row.id
                )
                grouped_row.stitched_message_ids_json = json.dumps(grouped_row_ids)
                grouped_row.last_stitched_at = stitched_at

            primary_row.deal_type = result.get("parsed_type")
            primary_row.amount = parsed_amount
            primary_row.payment_method = result.get("parsed_payment_method")
            primary_row.cash_direction = normalized_cash_direction
            primary_row.category = result.get("parsed_category")
            primary_row.item_names_json = json.dumps(result.get("parsed_items", []))
            primary_row.items_in_json = json.dumps(result.get("parsed_items_in", []))
            primary_row.items_out_json = json.dumps(result.get("parsed_items_out", []))
            primary_row.trade_summary = result.get("parsed_trade_summary")
            primary_row.notes = result.get("parsed_notes")
            primary_row.confidence = parsed_confidence
            primary_row.needs_review = bool(result.get("needs_review", False))
            primary_row.image_summary = result.get("image_summary")
            primary_row.entry_kind = financials.entry_kind
            primary_row.money_in = validated_money_in
            primary_row.money_out = validated_money_out
            primary_row.expense_category = financials.expense_category
            primary_row.parse_disagreement_json = (
                json.dumps(parse_disagreement, sort_keys=True) if parse_disagreement else None
            )
            if result.get("ignore_message"):
                clear_parsed_fields(primary_row)
                set_row_status(primary_row, PARSE_IGNORED, clear_error=True)
            else:
                set_row_status(
                    primary_row,
                    PARSE_REVIEW_REQUIRED if primary_row.needs_review else PARSE_PARSED,
                    clear_error=True,
                )

            for grouped_row in group_rows:
                if grouped_row.id != primary_row.id:
                    mark_grouped_child_ignored(grouped_row)

            if attempt:
                attempt.success = True
                attempt.error = None
                attempt.finished_at = utcnow()
                attempt.model_used = model_used or attempt.model_used
                attempt.provider_used = provider_used or attempt.provider_used
                attempt.input_tokens = usage.get("input_tokens")
                attempt.cached_input_tokens = usage.get("cached_input_tokens")
                attempt.output_tokens = usage.get("output_tokens")
                attempt.total_tokens = usage.get("total_tokens")
                attempt.estimated_cost_usd = usage.get("estimated_cost_usd")
                session.add(attempt)

            for grouped_row in group_rows:
                session.add(grouped_row)
            for stale_row in stale_rows:
                session.add(stale_row)

            for grouped_row in group_rows:
                worker_log(
                    action="transaction_sync_started",
                    row=grouped_row,
                    success=True,
                    session=session,
                )
                try:
                    if grouped_row.id == primary_row.id:
                        sync_transaction_from_message(
                            session,
                            grouped_row,
                            source_rows=group_rows,
                            source_content=(
                                combined_text if len(group_rows) > 1 else (grouped_row.content or "")
                            ),
                        )
                    else:
                        sync_transaction_from_message(session, grouped_row)
                except Exception as exc:
                    worker_log(
                        action="transaction_sync_failed",
                        row=grouped_row,
                        level="error",
                        success=False,
                        error=str(exc),
                        session=session,
                    )
                    raise
                worker_log(
                    action="transaction_sync_succeeded",
                    row=grouped_row,
                    success=True,
                    session=session,
                )
            for stale_row in stale_rows:
                worker_log(
                    action="transaction_sync_started",
                    row=stale_row,
                    success=True,
                    session=session,
                )
                try:
                    sync_transaction_from_message(session, stale_row)
                except Exception as exc:
                    worker_log(
                        action="transaction_sync_failed",
                        row=stale_row,
                        level="error",
                        success=False,
                        error=str(exc),
                        session=session,
                    )
                    raise
                worker_log(
                    action="transaction_sync_succeeded",
                    row=stale_row,
                    success=True,
                    session=session,
                )
            worker_log(
                action="parse_succeeded",
                row=primary_row,
                success=True,
                session=session,
                grouped_message_ids=grouped_row_ids,
                final_state=canonical_status(primary_row),
                needs_review=primary_row.needs_review,
            )
            row.active_reparse_run_id = None
            session.add(row)
            release_attempt_claims(session, attempt_id, affected_ids)
            session.commit()
            safe_record_reparse_run_outcome(run_id=active_reparse_run_id, success=True)

        except StaleSourceRevisionError as e:
            discard_stale_parse(
                session,
                row_id=row_id,
                attempt_id=attempt_id,
                affected_ids=affected_ids,
                error=e,
            )
            safe_record_reparse_run_outcome(
                run_id=active_reparse_run_id,
                success=False,
                error_message=str(e),
            )

        except TimedOutRowError as e:
            try:
                lock_message_projection_snapshots(session, source_snapshots)
            except StaleSourceRevisionError as stale_error:
                discard_stale_parse(
                    session,
                    row_id=row_id,
                    attempt_id=attempt_id,
                    affected_ids=affected_ids,
                    error=stale_error,
                )
                safe_record_reparse_run_outcome(
                    run_id=active_reparse_run_id,
                    success=False,
                    error_message=str(stale_error),
                )
                return
            set_row_status(row, PARSE_FAILED, error=f"timeout: {e}")
            row.active_reparse_run_id = None

            if attempt:
                attempt.success = False
                attempt.error = f"timeout: {e}"
                attempt.finished_at = utcnow()
                session.add(attempt)

            session.add(row)
            release_attempt_claims(session, attempt_id, affected_ids)
            worker_log(
                action="parse_failed",
                row=row,
                level="error",
                success=False,
                  error=row.last_error,
                  session=session,
              )
            session.commit()
            safe_record_reparse_run_outcome(
                run_id=active_reparse_run_id,
                success=False,
                error_message=row.last_error,
            )

        except Exception as e:
            try:
                lock_message_projection_snapshots(session, source_snapshots)
            except StaleSourceRevisionError as stale_error:
                discard_stale_parse(
                    session,
                    row_id=row_id,
                    attempt_id=attempt_id,
                    affected_ids=affected_ids,
                    error=stale_error,
                )
                safe_record_reparse_run_outcome(
                    run_id=active_reparse_run_id,
                    success=False,
                    error_message=str(stale_error),
                )
                return
            set_row_status(row, PARSE_FAILED, error=str(e))
            row.active_reparse_run_id = None

            if attempt:
                attempt.success = False
                attempt.error = str(e)
                attempt.finished_at = utcnow()
                session.add(attempt)

            session.add(row)
            release_attempt_claims(session, attempt_id, affected_ids)
            worker_log(
                action="parse_failed",
                row=row,
                level="error",
                success=False,
                  error=row.last_error,
                  session=session,
              )
            session.commit()
            safe_record_reparse_run_outcome(
                run_id=active_reparse_run_id,
                success=False,
                error_message=row.last_error,
            )
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
    if row.is_deleted or discord_source_refresh_blocked(
        row.last_error,
        row.source_refresh_required,
    ):
        return [row]

    start_time = row.created_at - timedelta(seconds=window_seconds)
    end_time = row.created_at + timedelta(seconds=window_seconds)

    candidate_cap = max(200, max_messages * 30)
    candidates = session.exec(
        select(DiscordMessage)
        .where(DiscordMessage.channel_id == row.channel_id)
        .where(DiscordMessage.is_deleted == False)
        .where(DiscordMessage.created_at >= start_time)
        .where(DiscordMessage.created_at <= end_time)
        .order_by(DiscordMessage.created_at, DiscordMessage.id)
        .limit(candidate_cap)
    ).all()

    if not candidates:
        return [row]

    candidates = [
        candidate
        for candidate in candidates
        if not candidate.is_deleted
        and not discord_source_refresh_blocked(
            candidate.last_error,
            candidate.source_refresh_required,
        )
    ]
    candidates = [candidate for candidate in candidates if same_author(candidate, row)]
    candidates = [
        candidate for candidate in candidates
        if abs((candidate.created_at - row.created_at).total_seconds()) <= window_seconds
    ]

    if row not in candidates:
        candidates.append(row)

    group_rows = [row]
    nearby_candidates = sorted(
        [candidate for candidate in candidates if candidate.id != row.id],
        key=lambda candidate: (
            abs((candidate.created_at - row.created_at).total_seconds()),
            candidate.created_at,
            candidate.id or 0,
        ),
    )

    for candidate in nearby_candidates:
        if len(group_rows) >= max_messages:
            break
        if not stitch_group_needs_more_context(group_rows):
            break
        if not candidate_improves_group(group_rows, candidate):
            continue

        tentative_group = sorted(
            group_rows + [candidate],
            key=lambda grouped_row: (grouped_row.created_at, grouped_row.id or 0),
        )
        if should_stitch_rows(row, tentative_group):
            group_rows = tentative_group

    if len(group_rows) <= 1 or not should_stitch_rows(row, group_rows):
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


def same_author(left: DiscordMessage, right: DiscordMessage) -> bool:
    left_author_id = (left.author_id or "").strip()
    right_author_id = (right.author_id or "").strip()
    if left_author_id and right_author_id:
        return left_author_id == right_author_id

    left_author_name = (left.author_name or "").strip().lower()
    right_author_name = (right.author_name or "").strip().lower()
    return bool(left_author_name and right_author_name and left_author_name == right_author_name)


def is_payment_only_text(text: str) -> bool:
    text = normalize_text(text)
    patterns = [
        r"^(zelle|venmo|paypal|cash|tap|card|cc|dc)\s*\$?\d+(?:\.\d{1,2})?$",
        r"^\$?\d+(?:\.\d{1,2})?\s*(zelle|venmo|paypal|cash|tap|card|cc|dc)$",
        r"^\+\s*\$?\d+(?:\.\d{1,2})?\s*(zelle|venmo|paypal|cash|tap|card|cc|dc)?$",
        r"^(plus|\+)\s*\$?\d+(?:\.\d{1,2})?\s*(zelle|venmo|paypal|cash|tap|card|cc|dc)?$",
    ]
    return any(re.fullmatch(p, text, re.I) for p in patterns)


def is_payment_method_only_text(text: str) -> bool:
    text = normalize_text(text)
    return bool(re.fullmatch(r"(zelle|venmo|paypal|cash|tap|card|cc|dc)", text, re.I))


def is_trade_fragment_text(text: str) -> bool:
    text = normalize_text(text)
    patterns = [
        r".*\b(in|out)\b.*",
        r"^(top|bottom|left|right).*$",
        r"^.*\bplus\b.*$",
    ]
    return any(re.fullmatch(p, text, re.I) for p in patterns)


def is_explicit_buy_sell_text(text: str) -> bool:
    text = normalize_text(text)
    has_explicit_verb = bool(re.search(r"\b(sold|sell|bought|buy|paid)\b", text, re.I))
    has_payment_amount = bool(
        re.search(r"\b(zelle|venmo|paypal|cash|tap|card|cc|dc)\s*\$?\d+(?:\.\d{1,2})?\b", text, re.I)
        or re.search(r"\$?\d+(?:\.\d{1,2})?\s*(zelle|venmo|paypal|cash|tap|card|cc|dc)\b", text, re.I)
    )
    has_non_quantity_number = bool(
        re.search(r"\b(sold|sell|bought|buy|paid)\b.*\b\d+(?:\.\d{1,2})?\b(?!\s*(box|boxes|pack|packs|slab|slabs|case|cases|card|cards|binder|binders|lot|lots)\b)", text, re.I)
    )
    return has_payment_amount or (has_explicit_verb and has_non_quantity_number)


def is_short_fragment(row: DiscordMessage) -> bool:
    text = normalize_text(row.content)
    if is_explicit_buy_sell_text(text):
        return False
    if has_images(row) and len(text) <= 20:
        return True
    if is_payment_only_text(text):
        return True
    if is_payment_method_only_text(text):
        return True
    if is_trade_fragment_text(text) and len(text) <= 50:
        return True
    return False


def looks_like_complete_deal(row: DiscordMessage) -> bool:
    text = normalize_text(row.content)
    complete = is_explicit_buy_sell_text(text)

    # image + substantial text can also be a complete standalone log
    if has_images(row) and len(text) >= 25:
        return True

    return complete


def contains_amount(text: str) -> bool:
    return bool(re.search(r"\$?\d+(?:\.\d{1,2})?", normalize_text(text)))


def has_descriptive_text(row: DiscordMessage) -> bool:
    text = normalize_text(row.content)
    if not text or is_payment_only_text(text):
        return False
    if is_explicit_buy_sell_text(text):
        return True
    return len(text) >= 8


def stitch_profile(rows: list[DiscordMessage]) -> dict[str, int]:
    profile = {
        "images": 0,
        "payment_fragments": 0,
        "descriptions": 0,
        "trade_fragments": 0,
    }

    for row in rows:
        text = normalize_text(row.content)
        if has_images(row):
            profile["images"] += 1
        if is_payment_only_text(text) or is_payment_method_only_text(text):
            profile["payment_fragments"] += 1
        if has_descriptive_text(row):
            profile["descriptions"] += 1
        if is_trade_fragment_text(text):
            profile["trade_fragments"] += 1

    return profile


def stitch_group_needs_more_context(rows: list[DiscordMessage]) -> bool:
    profile = stitch_profile(rows)

    if profile["descriptions"] >= 1 and profile["payment_fragments"] >= 1:
        return False
    if (
        profile["trade_fragments"] >= 1
        and profile["payment_fragments"] >= 1
        and (profile["images"] >= 1 or profile["descriptions"] >= 1)
    ):
        return False
    if (
        profile["images"] >= 1
        and profile["descriptions"] >= 1
        and profile["payment_fragments"] >= 1
    ):
        return False

    return True


def candidate_improves_group(group_rows: list[DiscordMessage], candidate: DiscordMessage) -> bool:
    before = stitch_profile(group_rows)
    after = stitch_profile(group_rows + [candidate])

    if after == before:
        return False

    return (
        (before["images"] == 0 and after["images"] > before["images"])
        or (before["payment_fragments"] == 0 and after["payment_fragments"] > before["payment_fragments"])
        or (before["descriptions"] == 0 and after["descriptions"] > before["descriptions"])
        or (
            before["trade_fragments"] == 0
            and after["trade_fragments"] > before["trade_fragments"]
            and stitch_group_needs_more_context(group_rows)
        )
    )


def should_force_stitch(base_row: DiscordMessage, candidate_rows: list[DiscordMessage]) -> bool:
    if len(candidate_rows) != 2:
        return False

    sorted_rows = sorted(
        candidate_rows,
        key=lambda candidate: (candidate.created_at, candidate.id or 0),
    )
    first_row, second_row = sorted_rows
    first_text = normalize_text(first_row.content)
    second_text = normalize_text(second_row.content)

    if has_images(first_row) and has_images(second_row):
        return False

    def _has_deal_text(text: str) -> bool:
        return is_explicit_buy_sell_text(text) or is_trade_fragment_text(text) or is_payment_only_text(text)

    image_then_text = has_images(first_row) and len(first_text) <= 20 and _has_deal_text(second_text)
    text_then_image = has_images(second_row) and len(second_text) <= 20 and _has_deal_text(first_text)

    if image_then_text or text_then_image:
        if not has_large_gap(sorted_rows, max_gap_seconds=45):
            return True

    if has_large_gap(sorted_rows, max_gap_seconds=8):
        return False

    return False


def has_large_gap(candidate_rows: list[DiscordMessage], max_gap_seconds: int = 12) -> bool:
    if len(candidate_rows) <= 1:
        return False

    sorted_rows = sorted(
        candidate_rows,
        key=lambda row: (row.created_at, row.id or 0),
    )
    for previous, current in zip(sorted_rows, sorted_rows[1:]):
        gap = abs((current.created_at - previous.created_at).total_seconds())
        if gap > max_gap_seconds:
            return True
    return False


def should_stitch_rows(base_row: DiscordMessage, candidate_rows: list[DiscordMessage]) -> bool:
    if len(candidate_rows) <= 1:
        return False

    if should_force_stitch(base_row, candidate_rows):
        return True

    payment_fragments = 0
    short_fragments = 0
    complete_deals = 0
    rows_with_images = 0
    amount_mentions = 0

    for r in candidate_rows:
        text = normalize_text(r.content)
        if is_payment_only_text(text):
            payment_fragments += 1
        if is_short_fragment(r):
            short_fragments += 1
        if looks_like_complete_deal(r):
            complete_deals += 1
        if has_images(r):
            rows_with_images += 1
        if contains_amount(text):
            amount_mentions += 1

    if has_large_gap(candidate_rows):
        return False

    # Too many full standalone deals close together -> do not stitch
    if complete_deals >= 2:
        return False

    # Two image posts close together are often separate showroom deals
    if rows_with_images >= 2 and short_fragments < len(candidate_rows):
        return False

    # Multiple amount mentions usually mean multiple separate deals unless one row is clearly just a payment fragment.
    if amount_mentions >= 2 and payment_fragments == 0:
        return False

    # More than one payment fragment usually means multiple separate deals
    if payment_fragments >= 2:
        return False

    # If the base row itself already looks complete, only stitch when the neighbor is clearly a short fragment.
    if looks_like_complete_deal(base_row) and short_fragments <= 1:
        return False

    # Stitch only if there is at least one short/incomplete fragment
    if short_fragments == 0:
        return False

    # Good common case:
    # one image/incomplete row + one payment/direction fragment
    return True


# ---------------------------------------------------------------------------
# Inventory pricing refresh loop
# ---------------------------------------------------------------------------

async def _refresh_inventory_prices_once() -> None:
    """
    Fetch updated market prices for inventory items that are stale (in_stock or
    listed, and last_priced_at is NULL or older than the daily refresh window).
    Rate-limited to 1 request per second to avoid hammering pricing APIs.
    """
    from datetime import timedelta
    import httpx
    from ..models import InventoryItem, INVENTORY_IN_STOCK, INVENTORY_LISTED
    from ..inventory.pricing import fetch_price_for_item
    from ..inventory.price_updates import record_inventory_price_result

    stale_hours = max(min(settings.inventory_price_stale_hours, 23.0), 1.0)
    stale_cutoff = utcnow() - timedelta(hours=stale_hours)

    with managed_session() as session:
        items = session.exec(
            select(InventoryItem)
            .where(InventoryItem.status.in_([INVENTORY_IN_STOCK, INVENTORY_LISTED]))
            .where(
                or_(
                    InventoryItem.last_priced_at == None,  # noqa: E711
                    InventoryItem.last_priced_at < stale_cutoff,
                )
            )
            .order_by(InventoryItem.last_priced_at.asc().nullsfirst())
        ).all()
        item_ids = [i.id for i in items]

    if not item_ids:
        return

    worker_log(
        action="inventory.price_refresh.start",
        success=True,
        count=len(item_ids),
    )

    refreshed = 0
    async with httpx.AsyncClient(timeout=15.0) as client:
        for item_id in item_ids:
            with managed_session() as session:
                item = session.get(InventoryItem, item_id)
                if not item:
                    continue
                try:
                    result = await fetch_price_for_item(
                        item,
                        client,
                        api_key=settings.scrydex_api_key,
                        base_url=settings.scrydex_base_url,
                    )
                    if result:
                        _history, alert_event = record_inventory_price_result(
                            session,
                            item,
                            result,
                        )
                        session.commit()
                        refreshed += 1
                        if alert_event in {"created", "updated"}:
                            worker_log(
                                action="inventory.resticker_alert.created",
                                success=True,
                                inventory_item_id=item_id,
                                suggested_price=result.get("market_price"),
                            )
                except Exception as exc:
                    worker_log(
                        action="inventory.price_refresh.item_failed",
                        level="warning",
                        success=False,
                        error=str(exc),
                        inventory_item_id=item_id,
                    )

            # Rate limit: 1 req/sec
            await asyncio.sleep(1.0)

    worker_log(
        action="inventory.price_refresh.done",
        success=True,
        refreshed=refreshed,
        checked=len(item_ids),
    )


def _seconds_until_next_inventory_price_refresh(now: datetime | None = None) -> float:
    """Return seconds until the next daily inventory market-price refresh."""
    pacific = ZoneInfo("America/Los_Angeles")
    now_pt = (now or utcnow()).astimezone(pacific)
    refresh_hour = min(max(int(settings.inventory_price_refresh_hour_pacific or 8), 0), 23)
    next_run = now_pt.replace(hour=refresh_hour, minute=0, second=0, microsecond=0)
    if now_pt >= next_run:
        next_run += timedelta(days=1)
    return max((next_run - now_pt).total_seconds(), 60.0)


async def periodic_inventory_price_loop(stop_event: asyncio.Event) -> None:
    """Background loop that refreshes stale inventory market prices once a day."""
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=_seconds_until_next_inventory_price_refresh(),
            )
            break
        except asyncio.TimeoutError:
            pass

        try:
            await _refresh_inventory_prices_once()
        except OperationalError as exc:
            worker_log(
                action="inventory.price_refresh.db_error",
                level="error",
                success=False,
                error=str(exc),
            )
            dispose_engine()
        except Exception as exc:
            worker_log(
                action="inventory.price_refresh.loop_error",
                level="error",
                success=False,
                error=str(exc),
            )
