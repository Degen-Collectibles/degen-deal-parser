from __future__ import annotations

from typing import Optional

from sqlmodel import Session, select

from .discord_ingest import list_available_discord_channels
from .models import WatchedChannel, utcnow


def get_watched_channels(session: Session) -> list[WatchedChannel]:
    return session.exec(
        select(WatchedChannel).order_by(WatchedChannel.channel_name, WatchedChannel.channel_id)
    ).all()


def resolve_channel_label(channel_id: str, preferred_name: Optional[str] = None) -> str:
    if preferred_name and preferred_name.strip():
        return preferred_name.strip()

    available = list_available_discord_channels()
    matched = next((channel for channel in available if channel["channel_id"] == channel_id), None)
    if matched:
        return matched["label"]

    return channel_id


def upsert_watched_channel(
    session: Session,
    *,
    channel_id: str,
    channel_name: Optional[str] = None,
    is_enabled: bool = True,
    backfill_enabled: bool = True,
) -> WatchedChannel:
    channel_id = channel_id.strip()
    existing = session.exec(
        select(WatchedChannel).where(WatchedChannel.channel_id == channel_id)
    ).first()

    resolved_name = resolve_channel_label(channel_id, channel_name)
    now = utcnow()

    if existing:
        existing.channel_name = resolved_name
        existing.is_enabled = is_enabled
        existing.backfill_enabled = backfill_enabled
        existing.updated_at = now
        session.add(existing)
        session.commit()
        session.refresh(existing)
        return existing

    row = WatchedChannel(
        channel_id=channel_id,
        channel_name=resolved_name,
        is_enabled=is_enabled,
        backfill_enabled=backfill_enabled,
        created_at=now,
        updated_at=now,
    )
    session.add(row)
    session.commit()
    session.refresh(row)
    return row


def get_channel_filter_choices(session: Session) -> list[dict]:
    watched = get_watched_channels(session)
    choices: dict[str, dict] = {
        channel.channel_id: {
            "channel_id": channel.channel_id,
            "channel_name": channel.channel_name or channel.channel_id,
        }
        for channel in watched
    }

    for channel in list_available_discord_channels():
        choices.setdefault(
            channel["channel_id"],
            {
                "channel_id": channel["channel_id"],
                "channel_name": channel["label"],
            },
        )

    return sorted(choices.values(), key=lambda row: row["channel_name"].lower())
