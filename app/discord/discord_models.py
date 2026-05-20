from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import Column, LargeBinary
from sqlmodel import Field, SQLModel

PARSE_PENDING = "pending"
PARSE_PROCESSING = "processing"
PARSE_PARSED = "parsed"
PARSE_FAILED = "failed"
PARSE_REVIEW_REQUIRED = "review_required"
PARSE_IGNORED = "ignored"

LEGACY_PARSE_STATUS_ALIASES = {
    "queued": PARSE_PENDING,
    "needs_review": PARSE_REVIEW_REQUIRED,
    "deleted": PARSE_IGNORED,
}

ACTIVE_PARSE_STATUSES = {PARSE_PENDING, PARSE_PROCESSING}
TERMINAL_PARSE_STATUSES = {PARSE_PARSED, PARSE_FAILED, PARSE_REVIEW_REQUIRED, PARSE_IGNORED}
ALL_PARSE_STATUSES = ACTIVE_PARSE_STATUSES | TERMINAL_PARSE_STATUSES

BACKFILL_QUEUED = "queued"
BACKFILL_PROCESSING = "processing"
BACKFILL_COMPLETED = "completed"
BACKFILL_CANCELLED = "cancelled"
BACKFILL_FAILED = "failed"
BACKFILL_TERMINAL_STATUSES = {BACKFILL_COMPLETED, BACKFILL_CANCELLED, BACKFILL_FAILED}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class WatchedChannel(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    channel_id: str = Field(index=True, unique=True)
    channel_name: Optional[str] = Field(default=None, index=True)

    is_enabled: bool = Field(default=True, index=True)
    backfill_enabled: bool = Field(default=True, index=True)
    backfill_after: Optional[datetime] = Field(default=None, index=True)
    backfill_before: Optional[datetime] = Field(default=None, index=True)

    created_at: datetime = Field(default_factory=utcnow, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)


class AvailableDiscordChannel(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    channel_id: str = Field(index=True, unique=True)
    channel_name: str = Field(index=True)
    guild_id: Optional[str] = Field(default=None, index=True)
    guild_name: Optional[str] = Field(default=None, index=True)
    category_name: Optional[str] = Field(default=None, index=True)
    label: str = Field(index=True)
    created_at_discord: Optional[datetime] = Field(default=None, index=True)
    last_message_at: Optional[datetime] = Field(default=None, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)


class DiscordMessage(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    discord_message_id: str = Field(index=True, unique=True)
    guild_id: Optional[str] = Field(default=None, index=True)
    channel_id: str = Field(index=True)
    channel_name: Optional[str] = Field(default=None, index=True)

    author_id: Optional[str] = Field(default=None)
    author_name: Optional[str] = Field(default=None)

    content: str = ""
    attachment_urls_json: str = "[]"

    created_at: datetime = Field(index=True)
    ingested_at: datetime = Field(default_factory=utcnow, index=True)
    last_seen_at: Optional[datetime] = Field(default=None, index=True)
    edited_at: Optional[datetime] = None
    deleted_at: Optional[datetime] = Field(default=None, index=True)
    is_deleted: bool = Field(default=False, index=True)

    stitched_group_id: Optional[str] = Field(default=None, index=True)
    stitched_primary: bool = Field(default=False, index=True)
    stitched_message_ids_json: str = "[]"
    last_stitched_at: Optional[datetime] = Field(default=None, index=True)

    parse_status: str = Field(default=PARSE_PENDING, index=True)
    parse_attempts: int = Field(default=0)
    last_error: Optional[str] = None
    active_reparse_run_id: Optional[str] = Field(default=None, index=True)

    deal_type: Optional[str] = None
    amount: Optional[float] = None
    payment_method: Optional[str] = None
    cash_direction: Optional[str] = None
    category: Optional[str] = None

    item_names_json: str = "[]"
    items_in_json: str = "[]"
    items_out_json: str = "[]"

    trade_summary: Optional[str] = None
    notes: Optional[str] = None
    confidence: Optional[float] = None
    needs_review: bool = Field(default=False)
    image_summary: Optional[str] = None
    reviewed_by: Optional[str] = Field(default=None, index=True)
    reviewed_at: Optional[datetime] = Field(default=None, index=True)

    entry_kind: Optional[str] = Field(default=None, index=True)
    money_in: Optional[float] = None
    money_out: Optional[float] = None
    expense_category: Optional[str] = Field(default=None, index=True)

    # Populated when the rules-based parse and the AI parse disagree on a
    # key field (deal_type, amount, payment_method, cash_direction). Stores
    # JSON of the form {"rule": {...}, "ai": {...}, "fields": [...]}.
    parse_disagreement_json: Optional[str] = Field(default=None)

    # Populated by the AI review resolver agent. Stores JSON of the form
    # {"resolution": "auto_resolved" | "needs_human",
    #  "confidence": 0..1,
    #  "reasoning": "...", "proposed_parse": {...}, "resolved_at": "..."}.
    ai_resolver_reasoning_json: Optional[str] = Field(default=None)


class AttachmentAsset(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    message_id: int = Field(index=True, foreign_key="discordmessage.id")
    source_url: str = Field(index=True)
    filename: Optional[str] = None
    content_type: Optional[str] = None
    is_image: bool = Field(default=False, index=True)
    data: bytes = Field(sa_column=Column(LargeBinary, nullable=False))
    created_at: datetime = Field(default_factory=utcnow, index=True)


class ParseAttempt(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    message_id: int = Field(index=True, foreign_key="discordmessage.id")
    attempt_number: int
    started_at: datetime = Field(default_factory=utcnow)
    finished_at: Optional[datetime] = None
    success: bool = False
    error: Optional[str] = None
    model_used: Optional[str] = None
    provider_used: Optional[str] = None
    input_tokens: Optional[int] = None
    cached_input_tokens: Optional[int] = None
    output_tokens: Optional[int] = None
    total_tokens: Optional[int] = None
    estimated_cost_usd: Optional[float] = None


class ReparseRun(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    run_id: str = Field(index=True, unique=True)
    source: str = Field(default="unknown", index=True)
    reason: Optional[str] = Field(default=None)

    requested_at: datetime = Field(default_factory=utcnow, index=True)
    finished_at: Optional[datetime] = Field(default=None, index=True)
    duration_ms: Optional[int] = None

    range_after: Optional[datetime] = Field(default=None, index=True)
    range_before: Optional[datetime] = Field(default=None, index=True)
    channel_id: Optional[str] = Field(default=None, index=True)
    requested_statuses_json: str = "[]"

    include_reviewed: bool = Field(default=False)
    force_reviewed: bool = Field(default=False)

    selected_count: int = Field(default=0)
    queued_count: int = Field(default=0)
    already_queued_count: int = Field(default=0)
    skipped_reviewed_count: int = Field(default=0)
    succeeded_count: int = Field(default=0)
    failed_count: int = Field(default=0)

    first_message_id: Optional[int] = None
    last_message_id: Optional[int] = None
    first_message_created_at: Optional[datetime] = Field(default=None, index=True)
    last_message_created_at: Optional[datetime] = Field(default=None, index=True)

    status: str = Field(default="queued", index=True)
    error_message: Optional[str] = None


class BackfillRequest(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    channel_id: Optional[str] = Field(default=None, index=True)
    after: Optional[datetime] = Field(default=None, index=True)
    before: Optional[datetime] = Field(default=None, index=True)
    limit_per_channel: Optional[int] = None
    oldest_first: bool = Field(default=True, index=True)
    status: str = Field(default=BACKFILL_QUEUED, index=True)
    requested_by: Optional[str] = Field(default=None, index=True)
    result_json: str = "{}"
    error_message: Optional[str] = None
    inserted_count: int = Field(default=0)
    skipped_count: int = Field(default=0)
    created_at: datetime = Field(default_factory=utcnow, index=True)
    started_at: Optional[datetime] = Field(default=None, index=True)
    finished_at: Optional[datetime] = Field(default=None, index=True)
