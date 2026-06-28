from __future__ import annotations

import hashlib
import struct

from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, select

from ..models import DiscordMessage, DiscordMessageRevision


_HASH_DOMAIN = b"discord-message-revision-v1\x00"
_MAX_INSERT_ATTEMPTS = 3


def compute_message_snapshot_hash(content: str, attachment_urls_json: str) -> str:
    """Hash the exact source strings with length-prefixed, unambiguous framing."""

    content_bytes = content.encode("utf-8")
    attachment_bytes = attachment_urls_json.encode("utf-8")
    framed_snapshot = b"".join(
        (
            _HASH_DOMAIN,
            struct.pack(">Q", len(content_bytes)),
            content_bytes,
            struct.pack(">Q", len(attachment_bytes)),
            attachment_bytes,
        )
    )
    return hashlib.sha256(framed_snapshot).hexdigest()


def get_latest_message_revision(
    session: Session,
    message_id: int,
) -> DiscordMessageRevision | None:
    return session.exec(
        select(DiscordMessageRevision)
        .where(DiscordMessageRevision.message_id == message_id)
        .order_by(
            DiscordMessageRevision.revision_number.desc(),
            DiscordMessageRevision.id.desc(),
        )
        .limit(1)
    ).first()


def _matches_snapshot(
    revision: DiscordMessageRevision,
    *,
    content: str,
    attachment_urls_json: str,
    snapshot_hash: str,
) -> bool:
    return (
        revision.snapshot_hash == snapshot_hash
        and revision.content == content
        and revision.attachment_urls_json == attachment_urls_json
    )


def ensure_message_revision(
    session: Session,
    message: DiscordMessage,
) -> DiscordMessageRevision:
    """Return or append the exact current projection revision without committing."""

    session.add(message)
    if message.id is None:
        session.flush()
    if message.id is None:  # pragma: no cover - SQLAlchemy assigns the primary key on flush
        raise RuntimeError("Discord message must have an ID before revision capture")

    content = message.content
    attachment_urls_json = message.attachment_urls_json
    snapshot_hash = compute_message_snapshot_hash(content, attachment_urls_json)

    last_integrity_error: IntegrityError | None = None
    for _attempt in range(_MAX_INSERT_ATTEMPTS):
        with session.no_autoflush:
            latest = get_latest_message_revision(session, message.id)
        if latest is not None and _matches_snapshot(
            latest,
            content=content,
            attachment_urls_json=attachment_urls_json,
            snapshot_hash=snapshot_hash,
        ):
            message.current_revision_id = latest.id
            session.add(message)
            return latest

        revision = DiscordMessageRevision(
            message_id=message.id,
            revision_number=(latest.revision_number + 1) if latest is not None else 1,
            content=content,
            attachment_urls_json=attachment_urls_json,
            source_edited_at=message.edited_at,
            snapshot_hash=snapshot_hash,
        )
        try:
            with session.begin_nested():
                session.add(revision)
                session.flush()
        except IntegrityError as exc:
            # Another writer may have claimed the same next revision number.
            # The savepoint keeps the caller's transaction usable so the next
            # pass can dedupe that row or append after it.
            last_integrity_error = exc
            continue

        message.current_revision_id = revision.id
        session.add(message)
        session.flush()
        return revision

    error = RuntimeError(
        f"Could not append a revision for Discord message {message.id} after "
        f"{_MAX_INSERT_ATTEMPTS} attempts"
    )
    if last_integrity_error is not None:
        raise error from last_integrity_error
    raise error  # pragma: no cover - every failed insert raises IntegrityError
