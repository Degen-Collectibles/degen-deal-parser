from __future__ import annotations

import json
from typing import Iterable

from sqlalchemy import select
from sqlmodel import Session

from .models import AttachmentAsset, DiscordMessage


IMAGE_EXTENSIONS = [".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp"]


def parse_attachment_urls_json(value: str | None) -> list[str]:
    try:
        loaded = json.loads(value or "[]")
    except json.JSONDecodeError:
        return []
    if not isinstance(loaded, list):
        return []
    cleaned: list[str] = []
    for entry in loaded:
        if isinstance(entry, str) and entry.strip():
            cleaned.append(entry.strip())
    return cleaned


def extract_image_urls(attachment_urls: list[str]) -> list[str]:
    return [
        url for url in attachment_urls
        if any(ext in url.lower() for ext in IMAGE_EXTENSIONS)
    ]


def get_cached_attachment_map(session: Session, message_ids: list[int]) -> dict[int, dict[str, list[str]]]:
    valid_ids = [message_id for message_id in message_ids if message_id is not None]
    if not valid_ids:
        return {}

    assets = session.exec(
        select(AttachmentAsset.id, AttachmentAsset.message_id, AttachmentAsset.is_image)
        .where(AttachmentAsset.message_id.in_(valid_ids))
        .order_by(AttachmentAsset.message_id.asc(), AttachmentAsset.id.asc())
    ).all()

    results: dict[int, dict[str, list[str]]] = {}
    for asset_id, message_id, is_image in assets:
        if asset_id is None:
            continue
        bucket = results.setdefault(
            message_id,
            {"all_urls": [], "image_urls": []},
        )
        asset_url = f"/attachments/{asset_id}"
        bucket["all_urls"].append(asset_url)
        if is_image:
            bucket["image_urls"].append(asset_url)

    return results


def normalize_attachment_urls_for_row(
    row: DiscordMessage,
    cached_assets: dict[str, list[str]] | None = None,
) -> tuple[list[str], list[str]]:
    if cached_assets:
        return list(cached_assets["all_urls"]), list(cached_assets["image_urls"])

    attachment_urls = parse_attachment_urls_json(row.attachment_urls_json)
    if row.id is None:
        return attachment_urls, extract_image_urls(attachment_urls)

    proxy_urls = [
        f"/messages/{row.id}/attachments/{index}"
        for index, _url in enumerate(attachment_urls)
    ]
    image_proxy_urls = [
        proxy_urls[index]
        for index, url in enumerate(attachment_urls)
        if any(ext in url.lower() for ext in IMAGE_EXTENSIONS)
    ]
    return proxy_urls, image_proxy_urls


def row_has_images(
    row: DiscordMessage,
    *,
    cached_assets: dict[str, list[str]] | None = None,
) -> bool:
    _, image_urls = normalize_attachment_urls_for_row(row, cached_assets)
    return bool(image_urls)


def merge_display_attachment_urls(
    *attachment_groups: Iterable[str] | None,
    image_groups: Iterable[Iterable[str] | None] | None = None,
) -> tuple[list[str], list[str]]:
    merged_urls: list[str] = []
    seen: set[str] = set()

    for attachment_group in attachment_groups:
        if not attachment_group:
            continue
        for url in attachment_group:
            if not url or url in seen:
                continue
            seen.add(url)
            merged_urls.append(url)

    if image_groups is None:
        return merged_urls, extract_image_urls(merged_urls)

    merged_image_urls: list[str] = []
    seen_images: set[str] = set()
    for image_group in image_groups:
        if not image_group:
            continue
        for url in image_group:
            if not url or url in seen_images:
                continue
            seen_images.add(url)
            merged_image_urls.append(url)

    return merged_urls, merged_image_urls
