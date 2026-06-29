"""Quota-backed pending upload lifecycle for Live Hit images."""

from __future__ import annotations

import os
import secrets
import shutil
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

from sqlmodel import Session, select
from sqlalchemy import update

from .models import LiveHitImageUpload


MAX_HIT_IMAGE_BYTES = 10 * 1024 * 1024
MAX_HIT_UPLOAD_REQUEST_BYTES = MAX_HIT_IMAGE_BYTES + 512 * 1024
PENDING_UPLOAD_TTL = timedelta(minutes=30)
MAX_PENDING_UPLOADS_PER_USER = 5
MAX_PENDING_UPLOADS_GLOBAL = 50
MAX_PENDING_BYTES_PER_USER = 5 * MAX_HIT_IMAGE_BYTES
MAX_PENDING_BYTES_GLOBAL = 50 * MAX_HIT_IMAGE_BYTES
MAX_DURABLE_BYTES_PER_USER = 2 * 1024 * 1024 * 1024
MAX_DURABLE_BYTES_GLOBAL = 10 * 1024 * 1024 * 1024
MIN_FREE_DISK_BYTES = 2 * 1024 * 1024 * 1024
UPLOAD_MARKER_PREFIX = "upload:"

_CONTENT_TYPE_EXTENSIONS = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
}


class HitImageUploadError(ValueError):
    def __init__(self, message: str, *, status_code: int, code: str) -> None:
        super().__init__(message)
        self.public_message = message
        self.status_code = status_code
        self.code = code


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def upload_marker(token: str) -> str:
    return f"{UPLOAD_MARKER_PREFIX}{token}"


def parse_upload_marker(value: str | None) -> str | None:
    raw = str(value or "").strip()
    if not raw.startswith(UPLOAD_MARKER_PREFIX):
        return None
    token = raw[len(UPLOAD_MARKER_PREFIX) :]
    if len(token) < 32 or len(token) > 128 or not all(ch.isalnum() or ch in "-_" for ch in token):
        return None
    return token


@contextmanager
def _quota_lock(images_dir: Path, *, timeout_seconds: float = 2.0) -> Iterator[None]:
    """Cross-process lock for same-host quota reservation and cleanup."""
    images_dir.mkdir(parents=True, exist_ok=True)
    lock_path = images_dir / ".quota.lock"
    handle = lock_path.open("a+b")
    try:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        deadline = time.monotonic() + max(timeout_seconds, 0.0)
        while True:
            try:
                if os.name == "nt":
                    import msvcrt

                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except (BlockingIOError, OSError):
                if time.monotonic() >= deadline:
                    raise HitImageUploadError(
                        "Image upload capacity is busy. Try again shortly.",
                        status_code=503,
                        code="upload_quota_lock_unavailable",
                    ) from None
                time.sleep(0.02)
        try:
            yield
        finally:
            if os.name == "nt":
                import msvcrt

                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        handle.close()


def _safe_unlink(path: Path) -> bool:
    try:
        path.unlink(missing_ok=True)
        return True
    except OSError:
        return False


def prune_expired_pending_uploads(
    session: Session,
    *,
    images_dir: Path,
    now: datetime | None = None,
) -> int:
    """Delete only expired, never-bound pending uploads."""
    current = _aware(now or utcnow())
    candidates = session.exec(
        select(LiveHitImageUpload).where(
            LiveHitImageUpload.bound_hit_id == None,  # noqa: E711
            LiveHitImageUpload.state.in_(["pending", "pruning"]),
        )
    ).all()
    removed = 0
    for candidate in candidates:
        if candidate.state == "pending" and _aware(candidate.expires_at) > current:
            continue
        if candidate.state == "pending":
            claim = session.exec(
                update(LiveHitImageUpload)
                .where(
                    LiveHitImageUpload.id == candidate.id,
                    LiveHitImageUpload.state == "pending",
                    LiveHitImageUpload.bound_hit_id == None,  # noqa: E711
                    LiveHitImageUpload.expires_at <= current,
                )
                .values(state="pruning")
                .execution_options(synchronize_session=False)
            )
            if claim.rowcount != 1:
                session.rollback()
                continue
            session.commit()
        row = session.get(LiveHitImageUpload, candidate.id)
        if row is None or row.state != "pruning" or row.bound_hit_id is not None:
            session.rollback()
            continue
        file_removed = _safe_unlink(images_dir / row.filename)
        temp_removed = _safe_unlink(images_dir / f".{row.filename}.{row.token}.part")
        if not file_removed or not temp_removed:
            session.rollback()
            continue
        session.delete(row)
        session.commit()
        removed += 1
    return removed


def _untracked_disk_bytes(images_dir: Path, tracked_names: set[str]) -> int:
    total = 0
    if not images_dir.exists():
        return total
    for path in images_dir.iterdir():
        if not path.is_file() or path.name == ".quota.lock" or path.name in tracked_names:
            continue
        try:
            total += path.stat().st_size
        except OSError:
            continue
    return total


def reserve_pending_upload(
    session: Session,
    *,
    images_dir: Path,
    owner_user_id: int,
    content_type: str,
    now: datetime | None = None,
) -> LiveHitImageUpload:
    normalized_type = str(content_type or "").strip().lower()
    extension = _CONTENT_TYPE_EXTENSIONS.get(normalized_type)
    if extension is None:
        raise HitImageUploadError(
            "Unsupported image type. Use JPEG, PNG, or WebP.",
            status_code=415,
            code="unsupported_image_type",
        )
    current = _aware(now or utcnow())
    with _quota_lock(images_dir):
        prune_expired_pending_uploads(session, images_dir=images_dir, now=current)
        rows = session.exec(select(LiveHitImageUpload)).all()
        pending = [
            row for row in rows if row.bound_hit_id is None and row.state == "pending"
        ]
        user_rows = [row for row in rows if row.owner_user_id == owner_user_id]
        user_pending = [row for row in pending if row.owner_user_id == owner_user_id]
        tracked_names = {row.filename for row in rows}
        global_bytes = sum(max(int(row.size_bytes or 0), 0) for row in rows)
        global_bytes += _untracked_disk_bytes(images_dir, tracked_names)
        user_bytes = sum(max(int(row.size_bytes or 0), 0) for row in user_rows)
        pending_global_bytes = sum(max(int(row.size_bytes or 0), 0) for row in pending)
        pending_user_bytes = sum(max(int(row.size_bytes or 0), 0) for row in user_pending)

        quota_checks = (
            (len(user_pending) >= MAX_PENDING_UPLOADS_PER_USER, "Too many pending image uploads."),
            (len(pending) >= MAX_PENDING_UPLOADS_GLOBAL, "Image upload capacity is full."),
            (pending_user_bytes + MAX_HIT_IMAGE_BYTES > MAX_PENDING_BYTES_PER_USER, "Pending image quota exceeded."),
            (pending_global_bytes + MAX_HIT_IMAGE_BYTES > MAX_PENDING_BYTES_GLOBAL, "Global pending image quota exceeded."),
            (user_bytes + MAX_HIT_IMAGE_BYTES > MAX_DURABLE_BYTES_PER_USER, "Image storage quota exceeded."),
            (global_bytes + MAX_HIT_IMAGE_BYTES > MAX_DURABLE_BYTES_GLOBAL, "Global image storage quota exceeded."),
        )
        for exceeded, message in quota_checks:
            if exceeded:
                raise HitImageUploadError(message, status_code=429, code="image_upload_quota_exceeded")
        try:
            free_bytes = shutil.disk_usage(images_dir).free
        except OSError:
            free_bytes = None
        if free_bytes is not None and free_bytes - MAX_HIT_IMAGE_BYTES < MIN_FREE_DISK_BYTES:
            raise HitImageUploadError(
                "Image storage is temporarily unavailable.",
                status_code=507,
                code="image_storage_low_space",
            )

        row = LiveHitImageUpload(
            token=secrets.token_urlsafe(32),
            filename=f"{uuid.uuid4().hex}{extension}",
            owner_user_id=int(owner_user_id),
            size_bytes=MAX_HIT_IMAGE_BYTES,
            content_type=normalized_type,
            state="pending",
            created_at=current,
            expires_at=current + PENDING_UPLOAD_TTL,
        )
        session.add(row)
        session.commit()
        session.refresh(row)
        return row


def finalize_pending_upload(
    session: Session,
    *,
    images_dir: Path,
    token: str,
    owner_user_id: int,
    actual_size: int,
    content_type: str,
    now: datetime | None = None,
) -> LiveHitImageUpload:
    current = _aware(now or utcnow())
    with _quota_lock(images_dir):
        row = session.exec(
            select(LiveHitImageUpload).where(LiveHitImageUpload.token == token)
        ).first()
        if (
            row is None
            or row.owner_user_id != int(owner_user_id)
            or row.bound_hit_id is not None
            or row.state != "pending"
            or _aware(row.expires_at) <= current
            or not (images_dir / row.filename).is_file()
        ):
            raise HitImageUploadError(
                "Image upload is no longer valid.",
                status_code=409,
                code="invalid_pending_upload",
            )
        row.size_bytes = int(actual_size)
        row.content_type = str(content_type)
        session.add(row)
        session.commit()
        session.refresh(row)
        return row


def abandon_pending_upload(
    session: Session,
    *,
    images_dir: Path,
    token: str,
) -> None:
    with _quota_lock(images_dir):
        row = session.exec(
            select(LiveHitImageUpload).where(LiveHitImageUpload.token == token)
        ).first()
        if row is None or row.bound_hit_id is not None or row.state != "pending":
            session.rollback()
            return
        _safe_unlink(images_dir / row.filename)
        _safe_unlink(images_dir / f".{row.filename}.{row.token}.part")
        session.delete(row)
        session.commit()


def bind_pending_upload(
    session: Session,
    *,
    images_dir: Path,
    marker: str,
    owner_user_id: int,
    hit_id: int,
    now: datetime | None = None,
) -> str:
    token = parse_upload_marker(marker)
    current = _aware(now or utcnow())
    if token is None:
        raise HitImageUploadError(
            "Image upload is no longer valid.",
            status_code=409,
            code="invalid_pending_upload",
        )
    row = session.exec(
        select(LiveHitImageUpload).where(LiveHitImageUpload.token == token)
    ).first()
    if (
        row is None
        or row.owner_user_id != int(owner_user_id)
        or row.bound_hit_id is not None
        or row.state != "pending"
        or _aware(row.expires_at) <= current
        or not (images_dir / row.filename).is_file()
    ):
        raise HitImageUploadError(
            "Image upload is no longer valid.",
            status_code=409,
            code="invalid_pending_upload",
        )
    result = session.exec(
        update(LiveHitImageUpload)
        .where(
            LiveHitImageUpload.token == token,
            LiveHitImageUpload.owner_user_id == int(owner_user_id),
            LiveHitImageUpload.state == "pending",
            LiveHitImageUpload.bound_hit_id == None,  # noqa: E711
            LiveHitImageUpload.expires_at > current,
        )
        .values(state="bound", bound_hit_id=int(hit_id), bound_at=current)
        .execution_options(synchronize_session=False)
    )
    if result.rowcount != 1:
        raise HitImageUploadError(
            "Image upload is no longer valid.",
            status_code=409,
            code="invalid_pending_upload",
        )
    session.flush()
    return row.filename
