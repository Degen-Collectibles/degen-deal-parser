from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional
from sqlmodel import SQLModel, Field


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class WatchedChannel(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)

    channel_id: str = Field(index=True, unique=True)
    channel_name: Optional[str] = Field(default=None, index=True)

    is_enabled: bool = Field(default=True, index=True)
    backfill_enabled: bool = Field(default=True, index=True)

    created_at: datetime = Field(default_factory=utcnow, index=True)
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
    edited_at: Optional[datetime] = None
    is_deleted: bool = Field(default=False, index=True)

    stitched_group_id: Optional[str] = Field(default=None, index=True)
    stitched_primary: bool = Field(default=False, index=True)
    stitched_message_ids_json: str = "[]"

    parse_status: str = Field(default="queued", index=True)
    parse_attempts: int = Field(default=0)
    last_error: Optional[str] = None

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

    entry_kind: Optional[str] = Field(default=None, index=True)
    money_in: Optional[float] = None
    money_out: Optional[float] = None
    expense_category: Optional[str] = Field(default=None, index=True)


class ParseAttempt(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    message_id: int = Field(index=True, foreign_key="discordmessage.id")
    attempt_number: int
    started_at: datetime = Field(default_factory=utcnow)
    finished_at: Optional[datetime] = None
    success: bool = False
    error: Optional[str] = None
    model_used: Optional[str] = None
