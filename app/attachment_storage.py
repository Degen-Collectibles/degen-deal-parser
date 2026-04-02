import mimetypes
import re
from pathlib import Path
from typing import Optional

from .config import BASE_DIR

ATTACHMENT_CACHE_DIR = BASE_DIR / "data" / "attachments"
SAFE_FILENAME_RE = re.compile(r"[^A-Za-z0-9._-]+")


def ensure_attachment_cache_dir() -> Path:
    ATTACHMENT_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return ATTACHMENT_CACHE_DIR


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
