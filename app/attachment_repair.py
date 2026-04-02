from __future__ import annotations

import mimetypes
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import httpx
from sqlalchemy import or_, select
from sqlmodel import Session

from .attachment_storage import attachment_cache_path, write_attachment_cache_file
from .db import managed_session
from .models import AttachmentAsset, DiscordMessage


@dataclass(frozen=True)
class AttachmentAssetSnapshot:
    asset_id: int
    source_url: str
    filename: Optional[str]
    content_type: Optional[str]
    is_image: bool


@dataclass(frozen=True)
class AttachmentRepairCandidate:
    message_id: int
    channel_id: str
    discord_message_id: str
    created_at: datetime
    attachment_urls: list[str]
    existing_assets: list[AttachmentAssetSnapshot]
    missing_cache_asset_ids: list[int]

    @property
    def existing_urls(self) -> set[str]:
        return {asset.source_url for asset in self.existing_assets}

    @property
    def missing_attachment_urls(self) -> list[str]:
        return [url for url in self.attachment_urls if url not in self.existing_urls]

    @property
    def missing_attachment_count(self) -> int:
        return len(self.missing_attachment_urls)

    @property
    def missing_cache_count(self) -> int:
        return len(self.missing_cache_asset_ids)

    @property
    def is_incomplete(self) -> bool:
        return self.missing_attachment_count > 0 or self.missing_cache_count > 0


def parse_attachment_urls_json(raw_value: str | None) -> list[str]:
    try:
        parsed = json.loads(raw_value or "[]")
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item).strip() for item in parsed if isinstance(item, str) and item.strip()]


def row_reference_time(row: DiscordMessage) -> datetime:
    return max(
        value
        for value in [
            row.created_at,
            row.last_seen_at or row.created_at,
            row.edited_at or row.created_at,
        ]
        if value is not None
    )


def attachment_cache_file_exists(asset_id: int, *, filename: Optional[str], content_type: Optional[str]) -> bool:
    path = attachment_cache_path(
        asset_id,
        filename=filename,
        content_type=content_type,
    )
    return path.exists()


def attachment_repair_candidate_query(
    session: Session,
    *,
    since: Optional[datetime] = None,
    before: Optional[datetime] = None,
    before_message_id: Optional[int] = None,
    limit: Optional[int] = None,
) -> list[AttachmentRepairCandidate]:
    stmt = (
        select(DiscordMessage)
        .where(DiscordMessage.attachment_urls_json != "[]")
        .where(DiscordMessage.channel_id != "")
        .where(DiscordMessage.discord_message_id != "")
    )
    if before_message_id is not None:
        stmt = stmt.where(DiscordMessage.id < before_message_id)
    if since is not None:
        stmt = stmt.where(
            or_(
                DiscordMessage.created_at >= since,
                DiscordMessage.last_seen_at >= since,
                DiscordMessage.edited_at >= since,
            )
        )
    if before is not None:
        stmt = stmt.where(
            or_(
                DiscordMessage.created_at <= before,
                DiscordMessage.last_seen_at <= before,
                DiscordMessage.edited_at <= before,
            )
        )

    stmt = stmt.order_by(DiscordMessage.id.desc())
    if limit is not None:
        stmt = stmt.limit(limit)

    rows = session.exec(stmt).all()
    if not rows:
        return []

    row_ids = [row.id for row in rows if row.id is not None]
    asset_rows = session.exec(
        select(
            AttachmentAsset.id,
            AttachmentAsset.message_id,
            AttachmentAsset.source_url,
            AttachmentAsset.filename,
            AttachmentAsset.content_type,
            AttachmentAsset.is_image,
        ).where(AttachmentAsset.message_id.in_(row_ids))
    ).all()

    assets_by_message_id: dict[int, list[AttachmentAssetSnapshot]] = {}
    for asset_id, message_id, source_url, filename, content_type, is_image in asset_rows:
        if asset_id is None or message_id is None:
            continue
        assets_by_message_id.setdefault(int(message_id), []).append(
            AttachmentAssetSnapshot(
                asset_id=int(asset_id),
                source_url=str(source_url or ""),
                filename=filename,
                content_type=content_type,
                is_image=bool(is_image),
            )
        )

    candidates: list[AttachmentRepairCandidate] = []
    for row in rows:
        if row.id is None:
            continue
        attachment_urls = parse_attachment_urls_json(row.attachment_urls_json)
        if not attachment_urls:
            continue

        asset_snapshots = assets_by_message_id.get(row.id, [])
        missing_cache_asset_ids = [
            asset.asset_id
            for asset in asset_snapshots
            if not attachment_cache_file_exists(
                asset.asset_id,
                filename=asset.filename,
                content_type=asset.content_type,
            )
        ]

        candidate = AttachmentRepairCandidate(
            message_id=row.id,
            channel_id=row.channel_id,
            discord_message_id=row.discord_message_id,
            created_at=row_reference_time(row),
            attachment_urls=attachment_urls,
            existing_assets=asset_snapshots,
            missing_cache_asset_ids=missing_cache_asset_ids,
        )
        if candidate.is_incomplete:
            candidates.append(candidate)

    return candidates


def count_missing_attachment_urls(candidate: AttachmentRepairCandidate) -> int:
    return candidate.missing_attachment_count


def filename_from_url(url: str, index: int) -> str:
    parsed = urlparse(url)
    name = Path(parsed.path).name.strip()
    if not name:
        return f"attachment-{index + 1}.bin"
    return name


def is_image_attachment(filename: str, content_type: Optional[str]) -> bool:
    ext = Path(filename).suffix.lower()
    if content_type and content_type.startswith("image/"):
        return True
    return ext in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp"}


def download_attachment(url: str) -> tuple[bytes, Optional[str]]:
    with httpx.Client(follow_redirects=True, timeout=30.0) as client:
        response = client.get(url)
        response.raise_for_status()
        return response.content, response.headers.get("content-type")


def row_status_snapshot(message_id: int) -> tuple[int, int]:
    with managed_session() as session:
        assets = session.exec(
            select(
                AttachmentAsset.id,
                AttachmentAsset.filename,
                AttachmentAsset.content_type,
            ).where(AttachmentAsset.message_id == message_id)
        ).all()

    cached_count = 0
    for asset_id, filename, content_type in assets:
        if asset_id is None:
            continue
        if attachment_cache_file_exists(
            int(asset_id),
            filename=filename,
            content_type=content_type,
        ):
            cached_count += 1
    return len(assets), cached_count


def restore_missing_assets_from_urls(message_id: int, attachment_urls: list[str]) -> tuple[int, int]:
    restored = 0
    failed = 0
    if not attachment_urls:
        return restored, failed

    new_assets: list[AttachmentAsset] = []
    with managed_session() as session:
        existing_urls = {
            str(asset.source_url)
            for asset in session.exec(
                select(AttachmentAsset).where(AttachmentAsset.message_id == message_id)
            ).all()
            if asset.source_url
        }

        for index, url in enumerate(attachment_urls):
            if url in existing_urls:
                continue

            try:
                data, content_type = download_attachment(url)
            except Exception:
                failed += 1
                continue

            filename = filename_from_url(url, index)
            asset = AttachmentAsset(
                message_id=message_id,
                source_url=url,
                filename=filename,
                content_type=content_type or mimetypes.guess_type(filename)[0],
                is_image=is_image_attachment(filename, content_type),
                data=data,
            )
            session.add(asset)
            new_assets.append(asset)
            restored += 1

        if new_assets:
            session.commit()
            for asset in new_assets:
                session.refresh(asset)
                if asset.id is not None:
                    write_attachment_cache_file(
                        asset.id,
                        filename=asset.filename,
                        content_type=asset.content_type,
                        data=asset.data,
                    )

    return restored, failed
