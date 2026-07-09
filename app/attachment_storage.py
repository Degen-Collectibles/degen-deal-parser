import io
import logging
import mimetypes
import os
import re
import tempfile
from pathlib import Path
from typing import Optional

from PIL import Image

from .config import get_settings
from .image_security import (
    DISCORD_ATTACHMENT_PROFILE,
    ImageSecurityError,
    image_decode_slot,
    validate_image_file,
)

logger = logging.getLogger(__name__)

SAFE_FILENAME_RE = re.compile(r"[^A-Za-z0-9._-]+")
THUMB_MAX_SIZE = (240, 240)
THUMB_CACHE_MAX_BYTES = 512 * 1024


def _attachment_cache_dir() -> Path:
    return get_settings().media_path("attachments")


def _thumbnail_cache_dir() -> Path:
    return get_settings().media_path("attachments", "thumbs")


def ensure_attachment_cache_dir() -> Path:
    path = _attachment_cache_dir()
    path.mkdir(parents=True, exist_ok=True)
    return path


def guess_attachment_suffix(filename: Optional[str], content_type: Optional[str]) -> str:
    if filename:
        suffix = Path(filename).suffix.strip()
        if suffix:
            return suffix
    if content_type:
        guessed = mimetypes.guess_extension(content_type.split(";")[0].strip())
        if guessed:
            return guessed
    return ".bin"


def attachment_cache_path(
    asset_id: int,
    *,
    filename: Optional[str],
    content_type: Optional[str],
) -> Path:
    suffix = guess_attachment_suffix(filename, content_type)
    safe_stem = SAFE_FILENAME_RE.sub("-", Path(filename or "attachment").stem).strip("-") or "attachment"
    return ensure_attachment_cache_dir() / f"{asset_id}-{safe_stem}{suffix}"


def write_attachment_cache_file(
    asset_id: int,
    *,
    filename: Optional[str],
    content_type: Optional[str],
    data: bytes,
) -> Path:
    path = attachment_cache_path(asset_id, filename=filename, content_type=content_type)
    if not path.exists():
        path.write_bytes(data)
    return path


def delete_attachment_cache_file(
    asset_id: int,
    *,
    filename: Optional[str],
    content_type: Optional[str],
) -> None:
    path = attachment_cache_path(asset_id, filename=filename, content_type=content_type)
    if path.exists():
        path.unlink()
    thumb = thumbnail_cache_path(asset_id)
    if thumb.exists():
        thumb.unlink()


def ensure_thumbnail_cache_dir() -> Path:
    path = _thumbnail_cache_dir()
    path.mkdir(parents=True, exist_ok=True)
    return path


def thumbnail_cache_path(asset_id: int) -> Path:
    return ensure_thumbnail_cache_dir() / f"{asset_id}.jpg"


def warm_attachment_cache(session, *, throttle_seconds: float = 0.1) -> tuple[int, int]:
    """Extract attachment blobs from DB to disk cache. Returns (extracted, already_cached)."""
    import time
    from sqlalchemy import select as sa_select
    from .models import AttachmentAsset

    already_cached = 0
    extracted = 0
    offset = 0
    batch_size = 50

    while True:
        rows = session.exec(
            sa_select(
                AttachmentAsset.id,
                AttachmentAsset.filename,
                AttachmentAsset.content_type,
                AttachmentAsset.is_image,
            )
            .order_by(AttachmentAsset.id.asc())
            .offset(offset)
            .limit(batch_size)
        ).all()
        if not rows:
            break

        needs_extract: list[tuple[int, str | None, str | None, bool]] = []
        for asset_id, filename, content_type, is_image in rows:
            if asset_id is None:
                continue
            path = attachment_cache_path(asset_id, filename=filename, content_type=content_type)
            if path.exists():
                already_cached += 1
                if is_image:
                    generate_thumbnail(path, asset_id)
            else:
                needs_extract.append((asset_id, filename, content_type, is_image))

        for asset_id, filename, content_type, is_image in needs_extract:
            try:
                asset = session.get(AttachmentAsset, asset_id)
                if asset and asset.data:
                    file_path = write_attachment_cache_file(
                        asset_id, filename=asset.filename,
                        content_type=asset.content_type, data=asset.data,
                    )
                    extracted += 1
                    if is_image:
                        generate_thumbnail(file_path, asset_id)
            except Exception:
                logger.debug("cache warm: failed to extract asset %s", asset_id, exc_info=True)
            if throttle_seconds > 0:
                time.sleep(throttle_seconds)

        offset += batch_size

    return extracted, already_cached


def generate_thumbnail(source_path: Path, asset_id: int) -> Optional[Path]:
    thumb_path = thumbnail_cache_path(asset_id)
    temp_path: Path | None = None

    try:
        with image_decode_slot():
            if thumb_path.exists():
                try:
                    cached = validate_image_file(
                        thumb_path,
                        allowed_formats=frozenset({"JPEG"}),
                        max_bytes=THUMB_CACHE_MAX_BYTES,
                        max_dimension=max(THUMB_MAX_SIZE),
                        max_pixels=THUMB_MAX_SIZE[0] * THUMB_MAX_SIZE[1],
                        max_frames=1,
                    )
                    with Image.open(io.BytesIO(cached.decoded_bytes)) as cached_image:
                        cached_image.load()
                    return thumb_path
                except ImageSecurityError:
                    pass
                except (OSError, SyntaxError, ValueError, TypeError):
                    pass

            validated = validate_image_file(
                source_path,
                max_bytes=DISCORD_ATTACHMENT_PROFILE.max_decoded_bytes,
                max_dimension=DISCORD_ATTACHMENT_PROFILE.max_dimension,
                max_pixels=DISCORD_ATTACHMENT_PROFILE.max_pixels,
                max_frames=1,
            )
            with Image.open(io.BytesIO(validated.decoded_bytes)) as source_image:
                source_image.load()
                converted = source_image.convert("RGB") if source_image.mode != "RGB" else None
                image = converted or source_image
                try:
                    image.thumbnail(THUMB_MAX_SIZE, Image.LANCZOS)
                    buf = io.BytesIO()
                    image.save(buf, format="JPEG", quality=80, optimize=True)
                finally:
                    if converted is not None:
                        converted.close()
            with tempfile.NamedTemporaryFile(
                mode="wb",
                dir=thumb_path.parent,
                prefix=f".{thumb_path.name}.",
                suffix=".tmp",
                delete=False,
            ) as temp_file:
                temp_path = Path(temp_file.name)
                temp_file.write(buf.getvalue())
                temp_file.flush()
            os.replace(temp_path, thumb_path)
            temp_path = None
        return thumb_path
    except Exception:
        logger.debug("thumbnail generation failed for asset %s", asset_id)
        return None
    finally:
        if temp_path is not None:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                logger.debug("thumbnail temporary-file cleanup failed for asset %s", asset_id)
