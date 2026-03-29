import asyncio
import json
from datetime import datetime, timezone
from typing import Optional

import discord
from sqlmodel import Session, select

from .config import get_settings
from .db import engine
from .models import DiscordMessage, WatchedChannel

settings = get_settings()

discord_client_instance = None
ALLOWED_CHANNEL_CATEGORIES = {
    "Employees",
    "Show Deals",
    "Past Shows",
    "Offline Deals",
}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso_datetime(value: Optional[str], *, end_of_day: bool = False) -> Optional[datetime]:
    if not value:
        return None

    value = value.strip()
    if not value:
        return None

    if len(value) == 10:
        dt = datetime.fromisoformat(value)
        if end_of_day:
            dt = dt.replace(hour=23, minute=59, second=59, microsecond=999999)
        else:
            dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        return dt.replace(tzinfo=timezone.utc)

    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def get_enabled_channel_ids() -> set[int]:
    with Session(engine) as session:
        rows = session.exec(
            select(WatchedChannel).where(WatchedChannel.is_enabled == True)
        ).all()
        return {int(r.channel_id) for r in rows}


def get_backfill_channel_ids() -> set[int]:
    with Session(engine) as session:
        rows = session.exec(
            select(WatchedChannel).where(WatchedChannel.backfill_enabled == True)
        ).all()
        return {int(r.channel_id) for r in rows}


def seed_channels_from_env() -> None:
    with Session(engine) as session:
        existing = session.exec(select(WatchedChannel)).all()
        existing_ids = {row.channel_id for row in existing}

        for channel_id in settings.channel_ids:
            cid = str(channel_id)
            if cid in existing_ids:
                continue

            session.add(
                WatchedChannel(
                    channel_id=cid,
                    channel_name=cid,
                    is_enabled=True,
                    backfill_enabled=True,
                )
            )

        session.commit()


def get_attachment_urls(message: discord.Message) -> list[str]:
    return [a.url for a in message.attachments]


def get_message_row(session: Session, discord_message_id: str) -> Optional[DiscordMessage]:
    return session.exec(
        select(DiscordMessage).where(
            DiscordMessage.discord_message_id == discord_message_id
        )
    ).first()


def is_watched_channel(channel_id: int) -> bool:
    return channel_id in get_enabled_channel_ids()


def should_track_message(message: discord.Message) -> bool:
    if message.author.bot:
        return False

    if not is_watched_channel(message.channel.id):
        return False

    if not message.content and not message.attachments:
        return False

    return True


def insert_or_update_message(message: discord.Message, *, is_edit: bool = False) -> tuple[bool, str]:
    if not should_track_message(message):
        return False, "ignored"

    attachment_urls = get_attachment_urls(message)

    with Session(engine) as session:
        existing = get_message_row(session, str(message.id))

        if existing:
            existing.guild_id = str(message.guild.id) if message.guild else None
            existing.channel_id = str(message.channel.id)
            existing.channel_name = getattr(message.channel, "name", None)
            existing.author_id = str(message.author.id)
            existing.author_name = str(message.author)
            existing.content = message.content or ""
            existing.attachment_urls_json = json.dumps(attachment_urls)
            existing.is_deleted = False

            if is_edit:
                existing.edited_at = utcnow()
                existing.parse_status = "queued"
                existing.last_error = None

            session.add(existing)
            session.commit()
            return True, "updated"

        row = DiscordMessage(
            discord_message_id=str(message.id),
            guild_id=str(message.guild.id) if message.guild else None,
            channel_id=str(message.channel.id),
            channel_name=getattr(message.channel, "name", None),
            author_id=str(message.author.id),
            author_name=str(message.author),
            content=message.content or "",
            attachment_urls_json=json.dumps(attachment_urls),
            created_at=message.created_at,
            parse_status="queued",
            is_deleted=False,
        )
        session.add(row)
        session.commit()
        return True, "inserted"


def mark_message_deleted(message: discord.Message) -> bool:
    with Session(engine) as session:
        existing = get_message_row(session, str(message.id))
        if not existing:
            return False

        existing.is_deleted = True
        existing.edited_at = utcnow()
        existing.parse_status = "deleted"
        session.add(existing)
        session.commit()
        return True


class DealIngestBot(discord.Client):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.ready_event = asyncio.Event()
        self.startup_backfill_done = False

    async def on_ready(self):
        print(f"[discord] logged in as {self.user}")

        seed_channels_from_env()

        if not self.startup_backfill_done and settings.startup_backfill_enabled:
            self.startup_backfill_done = True
            await self.backfill_enabled_channels(
                limit_per_channel=settings.startup_backfill_limit_per_channel,
                oldest_first=settings.startup_backfill_oldest_first,
            )

        self.ready_event.set()

    async def on_message(self, message: discord.Message):
        ok, action = insert_or_update_message(message, is_edit=False)
        if ok and action == "inserted":
            print(f"[discord] live ingested message {message.id}")
        elif ok and action == "updated":
            print(f"[discord] refreshed existing message {message.id}")

    async def on_message_edit(self, before: discord.Message, after: discord.Message):
        ok, action = insert_or_update_message(after, is_edit=True)
        if ok:
            print(f"[discord] edited message {after.id} -> {action}")

    async def on_message_delete(self, message: discord.Message):
        ok = mark_message_deleted(message)
        if ok:
            print(f"[discord] deleted message {message.id}")

    async def backfill_channel(
        self,
        channel_id: int,
        limit: Optional[int] = None,
        oldest_first: bool = True,
        after: Optional[datetime] = None,
        before: Optional[datetime] = None,
    ) -> dict:
        channel = self.get_channel(channel_id)
        if channel is None:
            try:
                channel = await self.fetch_channel(channel_id)
            except Exception as e:
                return {
                    "ok": False,
                    "channel_id": channel_id,
                    "error": f"Unable to fetch channel: {e}",
                }

        inserted_count = 0
        updated_count = 0
        skipped_count = 0

        async for message in channel.history(
            limit=limit,
            oldest_first=oldest_first,
            after=after,
            before=before,
        ):
            if not should_track_message(message):
                skipped_count += 1
                continue

            ok, action = insert_or_update_message(message, is_edit=False)
            if not ok:
                skipped_count += 1
            elif action == "inserted":
                inserted_count += 1
            else:
                updated_count += 1

        return {
            "ok": True,
            "channel_id": channel_id,
            "channel_name": getattr(channel, "name", None),
            "inserted": inserted_count,
            "updated": updated_count,
            "skipped": skipped_count,
            "limit": limit,
            "after": after.isoformat() if after else None,
            "before": before.isoformat() if before else None,
        }

    async def backfill_enabled_channels(
        self,
        limit_per_channel: Optional[int] = None,
        oldest_first: bool = True,
        after: Optional[datetime] = None,
        before: Optional[datetime] = None,
    ) -> dict:
        total_inserted = 0
        total_updated = 0
        total_skipped = 0
        results = []

        for channel_id in sorted(get_backfill_channel_ids()):
            result = await self.backfill_channel(
                channel_id=channel_id,
                limit=limit_per_channel,
                oldest_first=oldest_first,
                after=after,
                before=before,
            )
            results.append(result)

            if result.get("ok"):
                total_inserted += result.get("inserted", 0)
                total_updated += result.get("updated", 0)
                total_skipped += result.get("skipped", 0)

        return {
            "ok": True,
            "results": results,
            "total_inserted": total_inserted,
            "total_updated": total_updated,
            "total_skipped": total_skipped,
            "after": after.isoformat() if after else None,
            "before": before.isoformat() if before else None,
        }


async def run_discord_bot(stop_event: asyncio.Event):
    global discord_client_instance

    intents = discord.Intents.default()
    intents.message_content = True
    intents.guilds = True
    intents.messages = True

    client = DealIngestBot(intents=intents)
    discord_client_instance = client

    async with client:
        bot_task = asyncio.create_task(client.start(settings.discord_bot_token))
        try:
            await stop_event.wait()
        finally:
            await client.close()
            await bot_task


def get_discord_client() -> Optional[DealIngestBot]:
    return discord_client_instance
    
def list_available_discord_channels() -> list[dict]:
    client = get_discord_client()
    if client is None:
        return []

    channels: list[dict] = []

    for guild in client.guilds:
        for channel in guild.text_channels:
            category = getattr(channel, "category", None)
            category_name = category.name if category else None

            if category_name not in ALLOWED_CHANNEL_CATEGORIES:
                continue

            channels.append(
                {
                    "guild_id": str(guild.id),
                    "guild_name": guild.name,
                    "channel_id": str(channel.id),
                    "channel_name": channel.name,
                    "category_name": category_name,
                    "label": f"{guild.name} / {category_name} / #{channel.name}",
                }
            )

    channels.sort(
        key=lambda x: (
            x["guild_name"].lower(),
            (x["category_name"] or "").lower(),
            x["channel_name"].lower(),
        )
    )
    return channels
