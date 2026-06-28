"""Degen Eye v2 employee scan capture storage.

This module keeps a durable, local dataset of real employee scan photos plus
the scanner prediction and the later batch-review confirmation label. Capture
admission is fail-closed for ownership, rate, quota, free-space, and path
integrity failures so storage exhaustion cannot remain silent.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import shutil
import sqlite3
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Iterable, Iterator, Optional

from ..config import get_settings
from ..image_security import FULL_SCAN_PROFILE, ImageSecurityError, validate_image_base64

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parent.parent
_WRITE_LOCK = RLock()
_CAPTURE_ID_SEP = "_"
_CAPTURE_ID_RE = re.compile(r"^[0-9]{8}_[0-9a-f]{32}$", re.ASCII)
_MAX_CANDIDATE_SUMMARIES = 5
_DEFAULT_INDEX_PATH = _ROOT / "data" / "phash_index.sqlite"
MAX_CAPTURES_PER_USER = 2_000
MAX_CAPTURES_GLOBAL = 20_000
MAX_CAPTURE_BYTES_PER_USER = 2 * 1024 * 1024 * 1024
MAX_CAPTURE_BYTES_GLOBAL = 20 * 1024 * 1024 * 1024
MAX_CAPTURE_METADATA_BYTES = 64 * 1024
MIN_CAPTURE_FREE_BYTES = 2 * 1024 * 1024 * 1024
MAX_IN_FLIGHT_CAPTURE_BYTES = FULL_SCAN_PROFILE.max_decoded_bytes
CAPTURE_REQUESTS_PER_USER = 30
CAPTURE_RATE_WINDOW_SECONDS = 60.0
CAPTURE_RETENTION_SECONDS = 30 * 24 * 60 * 60
MAX_UNPROTECTED_CAPTURES = 10_000
_CAPTURE_LOCK_NAME = ".capture-quota.lock"
_CAPTURE_RATE_NAME = ".capture-rate.json"
_CAPTURE_STAGING_NAME = ".staging"
_CAPTURE_TRASH_NAME = ".trash"
_CAPTURE_IMAGE_SUFFIXES = (".jpg", ".png", ".webp")

_PHASH_SCHEMA = """
CREATE TABLE IF NOT EXISTS phash_index (
    card_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    number TEXT NOT NULL,
    set_id TEXT NOT NULL,
    set_name TEXT NOT NULL,
    phash BLOB NOT NULL,
    image_url TEXT,
    tcgplayer_url TEXT,
    source TEXT NOT NULL,
    indexed_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_phash_set ON phash_index(set_id);
CREATE INDEX IF NOT EXISTS idx_phash_name ON phash_index(name);
CREATE TABLE IF NOT EXISTS phash_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


class CaptureStorageError(ValueError):
    def __init__(self, message: str, *, status_code: int, code: str) -> None:
        super().__init__(message)
        self.public_message = message
        self.status_code = status_code
        self.code = code


@dataclass(frozen=True)
class _CaptureRecord:
    capture_id: str
    metadata_path: Path
    payload: dict[str, Any]
    image_path: Path
    image_bytes: int
    metadata_bytes: int
    owner_user_id: int
    atomic_dir: Optional[Path]


@contextmanager
def _capture_storage_lock(
    root: Optional[Path] = None,
    *,
    timeout_seconds: float = 2.0,
) -> Iterator[Path]:
    """Serialize capture accounting and writes across this host.

    The advisory file lock is intentionally a same-host boundary. Multiple
    application hosts need a shared transactional quota service; local file
    locking cannot provide a deployment-global guarantee.
    """
    configured_root = root or capture_root()
    if _is_link_like(configured_root):
        raise CaptureStorageError(
            "Scan capture storage path requires administrator review.",
            status_code=503,
            code="capture_storage_unsafe_path",
        )
    resolved_root = configured_root.resolve()
    resolved_root.mkdir(parents=True, exist_ok=True)
    lock_path = resolved_root / _CAPTURE_LOCK_NAME
    if _is_link_like(lock_path):
        raise CaptureStorageError(
            "Scan capture storage lock requires administrator review.",
            status_code=503,
            code="capture_storage_unsafe_path",
        )
    with _WRITE_LOCK:
        handle = lock_path.open("a+b")
        try:
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"0")
                handle.flush()
            deadline = time.monotonic() + max(float(timeout_seconds), 0.0)
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
                        raise CaptureStorageError(
                            "Scan capture storage is busy. Try again shortly.",
                            status_code=503,
                            code="capture_lock_unavailable",
                        ) from None
                    time.sleep(0.02)
            try:
                yield resolved_root
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


def _available_capture_disk_bytes(root: Path) -> int:
    return int(shutil.disk_usage(root).free)


def _is_link_like(path: Path) -> bool:
    is_junction = getattr(path, "is_junction", None)
    return path.is_symlink() or bool(is_junction and is_junction())


def _safe_remove_tree(path: Path, root: Path) -> None:
    try:
        if _is_link_like(path):
            path.unlink(missing_ok=True)
            return
        path.resolve().relative_to(root.resolve())
        shutil.rmtree(path, ignore_errors=False)
    except FileNotFoundError:
        return


def _cleanup_internal_artifacts_unlocked(root: Path) -> None:
    for name in (_CAPTURE_STAGING_NAME, _CAPTURE_TRASH_NAME):
        container = root / name
        if _is_link_like(container):
            raise CaptureStorageError(
                "Scan capture storage requires administrator review.",
                status_code=503,
                code="capture_storage_unsafe_path",
            )
        if not container.exists():
            continue
        for child in list(container.iterdir()):
            try:
                if child.is_dir() and not _is_link_like(child):
                    _safe_remove_tree(child, root)
                else:
                    child.unlink(missing_ok=True)
            except OSError as exc:
                raise CaptureStorageError(
                    "Scan capture storage cleanup failed.",
                    status_code=503,
                    code="capture_storage_cleanup_failed",
                ) from exc


def _record_capture_request_unlocked(root: Path, owner_user_id: int, now: float) -> None:
    state_path = root / _CAPTURE_RATE_NAME
    if _is_link_like(state_path):
        raise CaptureStorageError(
            "Scan capture rate state requires administrator review.",
            status_code=503,
            code="capture_storage_unsafe_path",
        )
    state: dict[str, Any] = {"version": 1, "users": {}}
    if state_path.exists():
        try:
            loaded = json.loads(state_path.read_text(encoding="utf-8"))
            if type(loaded) is not dict or type(loaded.get("users")) is not dict:
                raise ValueError("invalid rate state")
            state = loaded
        except Exception as exc:
            raise CaptureStorageError(
                "Scan capture rate state requires administrator review.",
                status_code=503,
                code="capture_rate_state_invalid",
            ) from exc

    cutoff = now - max(float(CAPTURE_RATE_WINDOW_SECONDS), 0.0)
    users = state["users"]
    key = str(owner_user_id)
    raw_values = users.get(key, [])
    if type(raw_values) is not list:
        raise CaptureStorageError(
            "Scan capture rate state requires administrator review.",
            status_code=503,
            code="capture_rate_state_invalid",
        )
    recent = [float(value) for value in raw_values if float(value) > cutoff]
    if len(recent) >= max(int(CAPTURE_REQUESTS_PER_USER), 0):
        raise CaptureStorageError(
            "Too many scan captures. Try again shortly.",
            status_code=429,
            code="capture_rate_limited",
        )
    recent.append(now)
    users[key] = recent
    # Bound stale user entries while retaining all active windows.
    state["users"] = {
        user_key: values
        for user_key, values in users.items()
        if user_key == key or (type(values) is list and any(float(v) > cutoff for v in values))
    }
    _write_json_atomic(state_path, state)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_iso() -> str:
    return _utc_now().isoformat(timespec="seconds")


def _capture_enabled() -> bool:
    return bool(getattr(get_settings(), "degen_eye_v2_capture_enabled", True))


def _resolve_under_data_root(raw: str, fallback: str) -> Path:
    """Resolve a v2 path: absolute paths used as-is; relative paths join
    DATA_ROOT, with a legacy leading `data/` segment stripped so existing
    `.env` values keep working when DATA_ROOT is set to /opt/degen/data."""
    value = (raw or "").strip() or fallback
    path = Path(value)
    if path.is_absolute():
        return path
    parts = path.parts
    if parts and parts[0] == "data":
        path = Path(*parts[1:]) if len(parts) > 1 else Path()
    return get_settings().data_root_path / path


def capture_root() -> Path:
    raw = str(getattr(get_settings(), "degen_eye_v2_capture_dir", "v2_training_scans") or "")
    return _resolve_under_data_root(raw, "v2_training_scans")


def default_index_path() -> Path:
    raw = str(getattr(get_settings(), "degen_eye_v2_index_path", "phash_index.sqlite") or "")
    return _resolve_under_data_root(raw, "phash_index.sqlite")


def _make_capture_id(now: datetime) -> str:
    return f"{now.strftime('%Y%m%d')}{_CAPTURE_ID_SEP}{uuid.uuid4().hex}"


def _date_dir_from_id(capture_id: object) -> Optional[str]:
    if type(capture_id) is not str or _CAPTURE_ID_RE.fullmatch(capture_id) is None:
        return None
    prefix = capture_id[:8]
    try:
        parsed = datetime.strptime(prefix, "%Y%m%d")
    except ValueError:
        return None
    return parsed.strftime("%Y-%m-%d")


def is_canonical_capture_id(capture_id: object) -> bool:
    """Return whether ``capture_id`` is an exact server-generated identifier."""
    return _date_dir_from_id(capture_id) is not None


def _image_kind(raw: bytes) -> tuple[str, str]:
    if raw.startswith(b"\xff\xd8\xff"):
        return ("jpg", "image/jpeg")
    if raw.startswith(b"\x89PNG\r\n\x1a\n"):
        return ("png", "image/png")
    if raw.startswith(b"RIFF") and raw[8:12] == b"WEBP":
        return ("webp", "image/webp")
    return ("img", "application/octet-stream")


def _display_path(path: Path) -> str:
    try:
        return path.relative_to(_ROOT).as_posix()
    except ValueError:
        return str(path)


def _candidate_summary(candidate: Any) -> dict[str, Any]:
    if not isinstance(candidate, dict):
        return {}
    return {
        "id": candidate.get("id"),
        "name": candidate.get("name"),
        "number": candidate.get("number"),
        "set_id": candidate.get("set_id"),
        "set_name": candidate.get("set_name"),
        "source": candidate.get("source"),
        "confidence": candidate.get("confidence"),
        "score": candidate.get("score"),
        "market_price": candidate.get("market_price"),
        "image_url": candidate.get("image_url"),
        "tcgplayer_url": candidate.get("tcgplayer_url"),
    }


def _prediction_summary(result: dict[str, Any]) -> dict[str, Any]:
    debug = result.get("debug") or {}
    v2 = debug.get("v2") or {}
    phash = v2.get("phash") or {}
    return {
        "captured_at": _utc_iso(),
        "status": result.get("status"),
        "processing_time_ms": result.get("processing_time_ms"),
        "game": result.get("game"),
        "best_match": _candidate_summary(result.get("best_match") or {}),
        "candidates": [
            _candidate_summary(candidate)
            for candidate in (result.get("candidates") or [])[:_MAX_CANDIDATE_SUMMARIES]
            if isinstance(candidate, dict)
        ],
        "debug": {
            "mode": debug.get("mode"),
            "engines_used": debug.get("engines_used"),
            "pipeline_tier": debug.get("pipeline_tier"),
            "extraction_method": debug.get("extraction_method"),
            "phash": {
                "source": phash.get("source"),
                "selected": phash.get("selected"),
                "top": (phash.get("top") or [])[:_MAX_CANDIDATE_SUMMARIES],
                "exactness": phash.get("exactness"),
            },
        },
        "error": result.get("error"),
    }


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        tmp.write_text(json.dumps(payload, ensure_ascii=True, indent=2, default=str), encoding="utf-8")
        tmp.replace(path)
    except OSError:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def _serialized_json_size(payload: dict[str, Any]) -> int:
    return len(
        json.dumps(
            payload,
            ensure_ascii=True,
            indent=2,
            default=str,
        ).encode("utf-8")
    )


def _metadata_path(capture_id: object) -> Optional[Path]:
    """Resolve one exact canonical capture path beneath the configured root.

    ``capture_id`` is deliberately not stripped, case-folded, coerced, or
    otherwise normalized.  It originated in an employee-controlled request;
    only the exact server-generated representation is accepted.
    """
    date_dir = _date_dir_from_id(capture_id)
    if date_dir is None:
        return None
    root = capture_root().resolve()
    candidates = (
        root / date_dir / capture_id / "metadata.json",
        root / date_dir / f"{capture_id}.json",
    )
    for candidate in candidates:
        safe = _safe_existing_file(candidate, root)
        if safe is not None:
            return safe
    return None


def _safe_existing_file(candidate: Path, root: Path) -> Optional[Path]:
    try:
        if _is_link_like(candidate):
            return None
        current = candidate.parent
        while current != root:
            if _is_link_like(current):
                return None
            current.relative_to(root)
            current = current.parent
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root)
        if not resolved.is_file():
            return None
        return resolved
    except (OSError, RuntimeError, ValueError):
        return None


def _load_metadata_unlocked(capture_id: object) -> tuple[Optional[Path], Optional[dict[str, Any]]]:
    path = _metadata_path(capture_id)
    if path is None:
        return (None, None)
    try:
        if path.stat().st_size > MAX_CAPTURE_METADATA_BYTES:
            return (path, None)
        payload = json.loads(path.read_text(encoding="utf-8"))
        if type(payload) is not dict or payload.get("capture_id") != capture_id:
            return (path, None)
        return (path, payload)
    except Exception:
        logger.warning("[degen_eye_v2_training] failed to read capture metadata %s", capture_id, exc_info=True)
        return (path, None)


def _load_metadata(capture_id: object) -> tuple[Optional[Path], Optional[dict[str, Any]]]:
    with _capture_storage_lock():
        return _load_metadata_unlocked(capture_id)


def _update_metadata(capture_id: object, updater) -> bool:
    try:
        with _capture_storage_lock() as root:
            path, payload = _load_metadata_unlocked(capture_id)
            if path is None or payload is None:
                return False
            updated = updater(dict(payload))
            if type(updated) is not dict or updated.get("capture_id") != capture_id:
                return False
            updated["updated_at"] = _utc_iso()
            new_metadata_bytes = _serialized_json_size(updated)
            if new_metadata_bytes > MAX_CAPTURE_METADATA_BYTES:
                return False
            records, _count, global_bytes, _unknown = _scan_capture_records_unlocked(root)
            record = next(
                (candidate for candidate in records if candidate.capture_id == capture_id),
                None,
            )
            if record is not None:
                delta = max(0, new_metadata_bytes - record.metadata_bytes)
                owner_bytes = sum(
                    candidate.image_bytes + candidate.metadata_bytes
                    for candidate in records
                    if candidate.owner_user_id == record.owner_user_id
                )
                if owner_bytes + delta > MAX_CAPTURE_BYTES_PER_USER:
                    return False
                if global_bytes + delta > MAX_CAPTURE_BYTES_GLOBAL:
                    return False
            if _available_capture_disk_bytes(root) < (
                max(int(MIN_CAPTURE_FREE_BYTES), 0) + new_metadata_bytes
            ):
                return False
            _write_json_atomic(path, updated)
        return True
    except Exception:
        logger.warning("[degen_eye_v2_training] failed to update capture %s", capture_id, exc_info=True)
        return False


def _resolve_record_image(
    root: Path,
    metadata_path: Path,
    payload: dict[str, Any],
    capture_id: str,
) -> Optional[Path]:
    image = payload.get("image")
    if type(image) is not dict:
        return None
    raw_path = image.get("path")
    if type(raw_path) is not str or not raw_path:
        return None
    candidate = Path(raw_path)
    if not candidate.is_absolute():
        candidate = _ROOT / candidate
    safe = _safe_existing_file(candidate, root)
    if safe is None:
        return None

    date_dir = _date_dir_from_id(capture_id)
    if date_dir is None:
        return None
    atomic_metadata = (root / date_dir / capture_id / "metadata.json").resolve()
    legacy_metadata = (root / date_dir / f"{capture_id}.json").resolve()
    resolved_metadata = metadata_path.resolve()
    if resolved_metadata == atomic_metadata:
        expected_images = {
            (root / date_dir / capture_id / f"image{suffix}").resolve()
            for suffix in _CAPTURE_IMAGE_SUFFIXES
        }
    elif resolved_metadata == legacy_metadata:
        expected_images = {
            (root / date_dir / f"{capture_id}{suffix}").resolve()
            for suffix in _CAPTURE_IMAGE_SUFFIXES
        }
    else:
        return None
    return safe if safe in expected_images else None


def _scan_capture_records_unlocked(
    root: Path,
) -> tuple[list[_CaptureRecord], int, int, bool]:
    records: list[_CaptureRecord] = []
    metadata_files: set[Path] = set()
    referenced_images: set[Path] = set()
    all_files: list[Path] = []
    unsafe_or_unknown = False

    for current_raw, dirnames, filenames in os.walk(root, followlinks=False):
        current = Path(current_raw)
        kept_dirs: list[str] = []
        for dirname in dirnames:
            child = current / dirname
            if current == root and dirname in {
                _CAPTURE_STAGING_NAME,
                _CAPTURE_TRASH_NAME,
            }:
                continue
            if _is_link_like(child):
                unsafe_or_unknown = True
                continue
            kept_dirs.append(dirname)
        dirnames[:] = kept_dirs
        for filename in filenames:
            path = current / filename
            if _is_link_like(path):
                unsafe_or_unknown = True
                continue
            if path.parent == root and path.name in {
                _CAPTURE_LOCK_NAME,
                _CAPTURE_RATE_NAME,
            }:
                continue
            all_files.append(path)

    json_files = [path for path in all_files if path.suffix.lower() == ".json"]
    global_count = 0
    global_bytes = 0
    for path in json_files:
        metadata_files.add(path.resolve())
        global_count += 1
        try:
            metadata_bytes = path.stat().st_size
        except OSError:
            metadata_bytes = 0
            unsafe_or_unknown = True
        global_bytes += max(metadata_bytes, 0)
        if metadata_bytes > MAX_CAPTURE_METADATA_BYTES:
            unsafe_or_unknown = True
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            unsafe_or_unknown = True
            continue
        if type(payload) is not dict or not is_canonical_capture_id(payload.get("capture_id")):
            unsafe_or_unknown = True
            continue
        capture_id = str(payload["capture_id"])
        date_dir = _date_dir_from_id(capture_id)
        resolved_metadata = path.resolve()
        expected_paths = {
            (root / str(date_dir) / capture_id / "metadata.json").resolve(),
            (root / str(date_dir) / f"{capture_id}.json").resolve(),
        }
        if resolved_metadata not in expected_paths:
            unsafe_or_unknown = True
            continue
        employee = payload.get("employee")
        owner_user_id = employee.get("id") if type(employee) is dict else None
        if type(owner_user_id) is not int or owner_user_id <= 0:
            unsafe_or_unknown = True
            continue
        image_path = _resolve_record_image(root, path, payload, capture_id)
        if image_path is None:
            unsafe_or_unknown = True
            continue
        try:
            image_bytes = image_path.stat().st_size
        except OSError:
            unsafe_or_unknown = True
            continue
        if image_bytes > MAX_IN_FLIGHT_CAPTURE_BYTES:
            unsafe_or_unknown = True
            continue
        global_bytes += image_bytes
        referenced_images.add(image_path)
        atomic_dir = (
            path.parent
            if path.name == "metadata.json" and path.parent.name == capture_id
            else None
        )
        records.append(
            _CaptureRecord(
                capture_id=capture_id,
                metadata_path=resolved_metadata,
                payload=payload,
                image_path=image_path,
                image_bytes=image_bytes,
                metadata_bytes=metadata_bytes,
                owner_user_id=owner_user_id,
                atomic_dir=atomic_dir,
            )
        )

    for path in all_files:
        try:
            resolved = path.resolve()
        except OSError:
            unsafe_or_unknown = True
            continue
        if resolved in metadata_files or resolved in referenced_images:
            continue
        unsafe_or_unknown = True
        global_count += 1
        try:
            global_bytes += max(path.stat().st_size, 0)
        except OSError:
            pass
    return records, global_count, global_bytes, unsafe_or_unknown


def _find_duplicate_capture_unlocked(
    records: Iterable[_CaptureRecord],
    owner_user_id: int,
    image_sha256: str,
) -> Optional[str]:
    for record in records:
        if record.owner_user_id != owner_user_id:
            continue
        image = record.payload.get("image")
        existing_hash = image.get("sha256") if type(image) is dict else None
        if not existing_hash:
            try:
                existing_hash = hashlib.sha256(record.image_path.read_bytes()).hexdigest()
            except OSError:
                continue
        if existing_hash == image_sha256:
            return record.capture_id
    return None


def _capture_is_retention_protected(payload: dict[str, Any]) -> bool:
    if payload.get("confirmed_label") or payload.get("inventory_item_id"):
        return True
    if payload.get("confirmed_at") or payload.get("confirmed_by"):
        return True
    training = payload.get("training")
    if type(training) is dict and (
        training.get("eligible")
        or training.get("indexed_at")
        or training.get("index_path")
        or training.get("phash_source")
    ):
        return True
    for key in (
        "protected",
        "curated",
        "evidence_hold",
        "legal_hold",
        "retain",
    ):
        if payload.get(key):
            return True
    retention = payload.get("retention")
    return type(retention) is dict and any(bool(value) for value in retention.values())


def _capture_created_at(payload: dict[str, Any]) -> Optional[datetime]:
    value = payload.get("created_at")
    if type(value) is not str or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _delete_capture_record_unlocked(
    root: Path,
    record: _CaptureRecord,
    *,
    shared_image: bool,
) -> None:
    trash_root = root / _CAPTURE_TRASH_NAME
    trash_root.mkdir(parents=True, exist_ok=True)
    trash_path = trash_root / f"{record.capture_id}.{uuid.uuid4().hex}"

    if record.atomic_dir is not None:
        atomic_dir = record.atomic_dir
        try:
            if _is_link_like(atomic_dir):
                raise OSError("capture directory is a symlink")
            atomic_dir.resolve().relative_to(root)
            atomic_dir.replace(trash_path)
            _safe_remove_tree(trash_path, root)
            return
        except Exception as exc:
            raise CaptureStorageError(
                "Scan capture retention cleanup failed.",
                status_code=503,
                code="capture_retention_failed",
            ) from exc

    trash_path.mkdir()
    staged_image = trash_path / record.image_path.name
    staged_metadata = trash_path / record.metadata_path.name
    moved_image = False
    moved_metadata = False
    try:
        if not shared_image:
            record.image_path.replace(staged_image)
            moved_image = True
        record.metadata_path.replace(staged_metadata)
        moved_metadata = True
        _safe_remove_tree(trash_path, root)
    except Exception as exc:
        try:
            if moved_metadata and staged_metadata.exists():
                staged_metadata.replace(record.metadata_path)
            if moved_image and staged_image.exists():
                staged_image.replace(record.image_path)
            if trash_path.exists():
                _safe_remove_tree(trash_path, root)
        except Exception:
            logger.error(
                "[degen_eye_v2_training] retention rollback failed for %s",
                record.capture_id,
                exc_info=True,
            )
        raise CaptureStorageError(
            "Scan capture retention cleanup failed.",
            status_code=503,
            code="capture_retention_failed",
        ) from exc


def _prune_capture_retention_unlocked(
    root: Path,
    records: list[_CaptureRecord],
    *,
    current: datetime,
    max_age_seconds: int,
    max_unprotected: int,
) -> int:
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    current = current.astimezone(timezone.utc)

    eligible: list[tuple[datetime, _CaptureRecord]] = []
    for record in records:
        if _capture_is_retention_protected(record.payload):
            continue
        created_at = _capture_created_at(record.payload)
        if created_at is None:
            continue
        eligible.append((created_at, record))
    eligible.sort(key=lambda item: (item[0], item[1].capture_id))

    delete_ids = {
        record.capture_id
        for created_at, record in eligible
        if (current - created_at).total_seconds() >= max(int(max_age_seconds), 0)
    }
    overflow = max(0, len(eligible) - max(int(max_unprotected), 0))
    delete_ids.update(record.capture_id for _created, record in eligible[:overflow])

    image_ref_counts: dict[Path, int] = {}
    for record in records:
        image_ref_counts[record.image_path] = image_ref_counts.get(record.image_path, 0) + 1

    deleted = 0
    for _created, record in eligible:
        if record.capture_id not in delete_ids:
            continue
        _delete_capture_record_unlocked(
            root,
            record,
            shared_image=image_ref_counts.get(record.image_path, 0) > 1,
        )
        deleted += 1
    return deleted


def prune_capture_retention(
    *,
    now: Optional[datetime] = None,
    max_age_seconds: int = CAPTURE_RETENTION_SECONDS,
    max_unprotected: int = MAX_UNPROTECTED_CAPTURES,
) -> int:
    """Prune only old/overflow unprotected captures beneath the data root."""
    with _capture_storage_lock() as root:
        _cleanup_internal_artifacts_unlocked(root)
        records, _global_count, _global_bytes, unknown = _scan_capture_records_unlocked(root)
        if unknown:
            return 0
        return _prune_capture_retention_unlocked(
            root,
            records,
            current=now or _utc_now(),
            max_age_seconds=max_age_seconds,
            max_unprotected=max_unprotected,
        )


def create_scan_capture(
    image_b64: str,
    *,
    source: str,
    category_id: str = "3",
    employee: Optional[dict[str, Any]] = None,
    owner_user_id: Optional[int] = None,
    scan_id: Optional[str] = None,
    request_meta: Optional[dict[str, Any]] = None,
) -> Optional[str]:
    """Persist one full-resolution employee scan photo and initial metadata.

    Quota accounting and the directory commit are serialized by a same-host
    advisory lock. The image and metadata become visible together through one
    atomic directory rename.
    """
    if not _capture_enabled():
        return None
    if type(owner_user_id) is not int or owner_user_id <= 0:
        raise CaptureStorageError(
            "An authenticated capture owner is required.",
            status_code=403,
            code="capture_owner_required",
        )
    try:
        try:
            validated = validate_image_base64(image_b64, profile=FULL_SCAN_PROFILE)
        except ImageSecurityError as exc:
            raise CaptureStorageError(
                exc.public_message,
                status_code=exc.status_code,
                code=exc.code,
            ) from exc
        raw = validated.decoded_bytes
        image_sha256 = hashlib.sha256(raw).hexdigest()
        ext, content_type = _image_kind(raw)
        now = _utc_now()
        capture_id = _make_capture_id(now)
        employee_payload = dict(employee or {})
        employee_payload["id"] = owner_user_id

        with _capture_storage_lock() as root:
            _cleanup_internal_artifacts_unlocked(root)
            required_free = (
                max(int(MIN_CAPTURE_FREE_BYTES), 0)
                + max(int(MAX_IN_FLIGHT_CAPTURE_BYTES), 0)
                + max(int(MAX_CAPTURE_METADATA_BYTES), 0)
            )
            if _available_capture_disk_bytes(root) < required_free:
                raise CaptureStorageError(
                    "Scan capture storage is low on free space.",
                    status_code=507,
                    code="capture_free_space_floor",
                )
            _record_capture_request_unlocked(root, owner_user_id, time.time())
            records, global_count, global_bytes, unsafe_or_unknown = (
                _scan_capture_records_unlocked(root)
            )
            if unsafe_or_unknown:
                raise CaptureStorageError(
                    "Legacy scan capture ownership requires administrator review.",
                    status_code=503,
                    code="capture_legacy_owner_unknown",
                )

            duplicate = _find_duplicate_capture_unlocked(
                records,
                owner_user_id,
                image_sha256,
            )
            if duplicate is not None:
                return duplicate

            deleted = _prune_capture_retention_unlocked(
                root,
                records,
                current=now,
                max_age_seconds=CAPTURE_RETENTION_SECONDS,
                max_unprotected=max(int(MAX_UNPROTECTED_CAPTURES) - 1, 0),
            )
            if deleted:
                records, global_count, global_bytes, unsafe_or_unknown = (
                    _scan_capture_records_unlocked(root)
                )
                if unsafe_or_unknown:
                    raise CaptureStorageError(
                        "Legacy scan capture ownership requires administrator review.",
                        status_code=503,
                        code="capture_legacy_owner_unknown",
                    )

            day_dir = root / now.strftime("%Y-%m-%d")
            final_dir = day_dir / capture_id
            final_image = final_dir / f"image.{ext}"
            payload = {
                "capture_id": capture_id,
                "created_at": now.isoformat(timespec="seconds"),
                "updated_at": now.isoformat(timespec="seconds"),
                "source": source,
                "category_id": str(category_id or "3"),
                "scan_id": scan_id,
                "employee": employee_payload,
                "request": request_meta or {},
                "image": {
                    "path": _display_path(final_image),
                    "bytes": len(raw),
                    "content_type": content_type,
                    "sha256": image_sha256,
                },
                "prediction": None,
                "confirmed_label": None,
                "confirmed_at": None,
                "confirmed_by": None,
                "inventory_item_id": None,
                "training": {
                    "eligible": False,
                    "indexed_at": None,
                    "index_path": None,
                    "phash_source": None,
                },
            }
            metadata_bytes = _serialized_json_size(payload)
            if metadata_bytes > MAX_CAPTURE_METADATA_BYTES:
                raise CaptureStorageError(
                    "Scan capture metadata is too large.",
                    status_code=413,
                    code="capture_metadata_too_large",
                )
            candidate_bytes = len(raw) + metadata_bytes

            owner_records = [
                record for record in records if record.owner_user_id == owner_user_id
            ]
            owner_count = len(owner_records)
            owner_bytes = sum(
                record.image_bytes + record.metadata_bytes for record in owner_records
            )
            if owner_count >= MAX_CAPTURES_PER_USER:
                raise CaptureStorageError(
                    "Your saved scan capture limit has been reached.",
                    status_code=507,
                    code="capture_user_count_quota",
                )
            if global_count >= MAX_CAPTURES_GLOBAL:
                raise CaptureStorageError(
                    "Saved scan capture capacity has been reached.",
                    status_code=507,
                    code="capture_global_count_quota",
                )
            if owner_bytes + candidate_bytes > MAX_CAPTURE_BYTES_PER_USER:
                raise CaptureStorageError(
                    "Your saved scan capture storage limit has been reached.",
                    status_code=507,
                    code="capture_user_bytes_quota",
                )
            if global_bytes + candidate_bytes > MAX_CAPTURE_BYTES_GLOBAL:
                raise CaptureStorageError(
                    "Saved scan capture storage capacity has been reached.",
                    status_code=507,
                    code="capture_global_bytes_quota",
                )

            staging_root = root / _CAPTURE_STAGING_NAME
            stage_dir = staging_root / f"{capture_id}.{uuid.uuid4().hex}.tmp"
            stage_image = stage_dir / f"image.{ext}"
            stage_metadata = stage_dir / "metadata.json"
            try:
                day_dir.mkdir(parents=True, exist_ok=True)
                staging_root.mkdir(parents=True, exist_ok=True)
                stage_dir.mkdir()
                stage_image.write_bytes(raw)
                _write_json_atomic(stage_metadata, payload)
                if final_dir.exists():
                    raise OSError("capture id collision")
                stage_dir.replace(final_dir)
            except Exception as exc:
                try:
                    if stage_dir.exists() or _is_link_like(stage_dir):
                        _safe_remove_tree(stage_dir, root)
                except OSError:
                    logger.warning(
                        "[degen_eye_v2_training] failed to clean capture staging %s",
                        stage_dir,
                        exc_info=True,
                    )
                raise CaptureStorageError(
                    "Scan capture could not be saved.",
                    status_code=503,
                    code="capture_storage_failed",
                ) from exc
        return capture_id
    except CaptureStorageError:
        raise
    except Exception as exc:
        logger.warning("[degen_eye_v2_training] failed to create scan capture", exc_info=True)
        raise CaptureStorageError(
            "Scan capture could not be saved.",
            status_code=503,
            code="capture_storage_failed",
        ) from exc


def attach_prediction(capture_id: object, result: dict[str, Any]) -> bool:
    if not capture_id:
        return False

    summary = _prediction_summary(result or {})

    def _apply(payload: dict[str, Any]) -> dict[str, Any]:
        payload["prediction"] = summary
        return payload

    return _update_metadata(capture_id, _apply)


def attach_confirmed_label(
    capture_id: Optional[str],
    label: dict[str, Any],
    *,
    expected_employee_id: int,
    inventory_item_id: Optional[int] = None,
    confirmed_by: Optional[dict[str, Any]] = None,
) -> bool:
    if not capture_id:
        return False
    clean_label = {
        "card_name": (label.get("card_name") or "").strip(),
        "game": (label.get("game") or "").strip(),
        "set_id": (label.get("set_id") or "").strip(),
        "set_name": (label.get("set_name") or "").strip(),
        "card_number": (label.get("card_number") or "").strip(),
        "condition": (label.get("condition") or "").strip(),
        "variant": (label.get("variant") or "").strip(),
        "is_foil": bool(label.get("is_foil")),
        "image_url": (label.get("image_url") or "").strip(),
        "auto_price": label.get("auto_price"),
        "notes": (label.get("notes") or "").strip(),
    }

    eligible = bool(clean_label["card_name"] and (clean_label["card_number"] or clean_label["set_name"]))

    def _apply(payload: dict[str, Any]) -> Optional[dict[str, Any]]:
        if not _payload_belongs_to_employee(payload, expected_employee_id):
            return None
        payload["confirmed_label"] = clean_label
        payload["confirmed_at"] = _utc_iso()
        payload["confirmed_by"] = confirmed_by or {}
        payload["inventory_item_id"] = inventory_item_id
        training = dict(payload.get("training") or {})
        training["eligible"] = eligible
        payload["training"] = training
        return payload

    return _update_metadata(capture_id, _apply)


def _payload_belongs_to_employee(payload: dict[str, Any], expected_employee_id: object) -> bool:
    if type(expected_employee_id) is not int or expected_employee_id <= 0:
        return False
    employee = payload.get("employee")
    if type(employee) is not dict:
        return False
    owner_id = employee.get("id")
    return type(owner_id) is int and owner_id > 0 and owner_id == expected_employee_id


def capture_belongs_to_employee(capture_id: object, expected_employee_id: object) -> bool:
    """Return whether an exact canonical capture belongs to one numeric user ID."""
    if type(expected_employee_id) is not int or expected_employee_id <= 0:
        return False
    with _capture_storage_lock():
        _path, payload = _load_metadata_unlocked(capture_id)
        return payload is not None and _payload_belongs_to_employee(payload, expected_employee_id)


def mark_training_indexed(
    capture_id: str,
    *,
    index_path: str,
    phash_source: str,
) -> bool:
    def _apply(payload: dict[str, Any]) -> dict[str, Any]:
        training = dict(payload.get("training") or {})
        training.update({
            "eligible": True,
            "indexed_at": _utc_iso(),
            "index_path": index_path,
            "phash_source": phash_source,
        })
        payload["training"] = training
        return payload

    return _update_metadata(capture_id, _apply)


def capture_stats() -> dict[str, Any]:
    root = capture_root()
    labeled = 0
    indexed = 0
    status_counts: dict[str, int] = {}
    total = 0
    bytes_total = 0
    if root.exists():
        with _capture_storage_lock(root) as locked_root:
            records, total, bytes_total, _unknown = _scan_capture_records_unlocked(
                locked_root
            )
        for record in records:
            payload = record.payload
            if payload.get("confirmed_label"):
                labeled += 1
            training = payload.get("training") or {}
            if training.get("indexed_at"):
                indexed += 1
            prediction = payload.get("prediction") or {}
            status = str(prediction.get("status") or "UNSCANNED")
            status_counts[status] = status_counts.get(status, 0) + 1
    return {
        "enabled": _capture_enabled(),
        "root": str(root),
        "captures": total,
        "labeled": labeled,
        "indexed": indexed,
        "unlabeled": max(0, total - labeled),
        "bytes": bytes_total,
        "status_counts": status_counts,
    }


def iter_confirmed_captures(*, include_indexed: bool = False) -> list[dict[str, Any]]:
    """Return confirmed capture metadata rows for offline training scripts."""
    rows: list[dict[str, Any]] = []
    root = capture_root()
    if not root.exists():
        return rows
    with _capture_storage_lock(root) as locked_root:
        records, _total, _bytes, _unknown = _scan_capture_records_unlocked(locked_root)
    for record in records:
        payload = dict(record.payload)
        label = payload.get("confirmed_label") or {}
        if not label.get("card_name"):
            continue
        training = payload.get("training") or {}
        if training.get("indexed_at") and not include_indexed:
            continue
        payload["_metadata_path"] = str(record.metadata_path)
        payload["_image_path"] = str(record.image_path)
        rows.append(payload)
    rows.sort(key=lambda row: row.get("created_at") or "")
    return rows


def _phash_to_blob(value: int) -> bytes:
    return int(value).to_bytes(8, byteorder="big", signed=False)


def _norm_training_value(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().replace("'", "").replace("\u2019", "").split())


def _number_core(value: Any) -> str:
    raw = str(value or "").split("/", 1)[0].strip().lower()
    return raw.lstrip("0") or raw


def _open_training_db(index_path: Path) -> sqlite3.Connection:
    index_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(index_path)
    conn.executescript(_PHASH_SCHEMA)
    conn.commit()
    return conn


def _set_training_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO phash_meta (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )


def _resolve_canonical_index_row(conn: sqlite3.Connection, label: dict[str, Any]) -> dict[str, str]:
    name = _norm_training_value(label.get("card_name"))
    if not name:
        return {}
    number = _number_core(label.get("card_number"))
    set_id = _norm_training_value(label.get("set_id"))
    set_name = _norm_training_value(label.get("set_name"))
    rows = conn.execute(
        """
        SELECT card_id, number, set_id, set_name, image_url, tcgplayer_url, source
        FROM phash_index
        WHERE lower(name) = ?
        """,
        (name,),
    ).fetchall()
    if not rows:
        return {}
    if number:
        rows = [r for r in rows if _number_core(r[1]) == number]
    if not rows:
        return {}

    non_capture = [r for r in rows if str(r[6] or "") != "employee_capture"] or rows
    if set_id:
        exact = [r for r in non_capture if _norm_training_value(r[2]) == set_id]
        if exact:
            non_capture = exact
    elif set_name:
        exact = [r for r in non_capture if _norm_training_value(r[3]) == set_name]
        if exact:
            non_capture = exact

    row = non_capture[0]
    return {
        "set_id": row[2] or "",
        "set_name": row[3] or "",
        "image_url": row[4] or "",
        "tcgplayer_url": row[5] or "",
    }


def _hash_capture_image(image_path: Path) -> tuple[Optional[int], str]:
    from .card_detect import detect_and_crop
    from .phash_scanner import compute_phash

    raw = image_path.read_bytes()
    crop_bytes, debug = detect_and_crop(raw)
    source = "crop" if crop_bytes else "raw"
    phash = compute_phash(crop_bytes or raw)
    reason = str((debug or {}).get("reason") or source)
    return phash, f"{source}:{reason}"


def _upsert_capture_exemplar(
    conn: sqlite3.Connection,
    *,
    capture_id: str,
    label: dict[str, Any],
    canonical: dict[str, str],
    phash_int: int,
) -> None:
    name = (label.get("card_name") or "").strip()
    number = (label.get("card_number") or "").strip()
    set_id = (label.get("set_id") or canonical.get("set_id") or "employee_capture").strip()
    set_name = (label.get("set_name") or canonical.get("set_name") or set_id).strip()
    image_url = (canonical.get("image_url") or label.get("image_url") or "").strip()
    tcgplayer_url = (canonical.get("tcgplayer_url") or "").strip()
    conn.execute(
        """
        INSERT INTO phash_index (card_id, name, number, set_id, set_name,
                                 phash, image_url, tcgplayer_url, source, indexed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'employee_capture', datetime('now'))
        ON CONFLICT(card_id) DO UPDATE SET
            name=excluded.name,
            number=excluded.number,
            set_id=excluded.set_id,
            set_name=excluded.set_name,
            phash=excluded.phash,
            image_url=excluded.image_url,
            tcgplayer_url=excluded.tcgplayer_url,
            source='employee_capture',
            indexed_at=datetime('now')
        """,
        (
            f"employee_capture:{capture_id}",
            name,
            number,
            set_id,
            set_name,
            _phash_to_blob(phash_int),
            image_url,
            tcgplayer_url,
        ),
    )


def train_confirmed_captures(
    *,
    index_path: Optional[Path | str] = None,
    limit: int = 200,
    include_indexed: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Promote confirmed employee captures into the local pHash index."""
    resolved_index = Path(index_path).resolve() if index_path else default_index_path().resolve()
    rows = iter_confirmed_captures(include_indexed=include_indexed)
    if limit:
        rows = rows[: max(0, int(limit))]

    conn = _open_training_db(resolved_index)
    considered = len(rows)
    indexed = 0
    skipped = 0
    errors: list[dict[str, str]] = []
    t0 = time.monotonic()
    try:
        for row in rows:
            capture_id = str(row.get("capture_id") or "").strip()
            label = row.get("confirmed_label") or {}
            image_path = Path(str(row.get("_image_path") or ""))
            if not capture_id or not label.get("card_name") or not image_path.exists():
                skipped += 1
                continue
            try:
                phash_int, phash_source = _hash_capture_image(image_path)
                if phash_int is None:
                    skipped += 1
                    continue
                canonical = _resolve_canonical_index_row(conn, label)
                if not dry_run:
                    _upsert_capture_exemplar(
                        conn,
                        capture_id=capture_id,
                        label=label,
                        canonical=canonical,
                        phash_int=phash_int,
                    )
                    mark_training_indexed(
                        capture_id,
                        index_path=str(resolved_index),
                        phash_source=phash_source,
                    )
                indexed += 1
                if indexed % 50 == 0:
                    conn.commit()
            except Exception as exc:
                skipped += 1
                if len(errors) < 10:
                    errors.append({"capture_id": capture_id, "error": str(exc)[:300]})

        if not dry_run:
            _set_training_meta(conn, "last_employee_training_at", str(int(time.time())))
            cur = conn.execute("SELECT COUNT(*) FROM phash_index WHERE source = 'employee_capture'")
            _set_training_meta(conn, "employee_training_count", str(cur.fetchone()[0]))
            cur = conn.execute("SELECT COUNT(*) FROM phash_index")
            _set_training_meta(conn, "card_count", str(cur.fetchone()[0]))
            conn.commit()
    finally:
        conn.close()

    return {
        "index_path": str(resolved_index),
        "captures_considered": considered,
        "indexed": indexed,
        "skipped": skipped,
        "dry_run": dry_run,
        "include_indexed": include_indexed,
        "limit": limit,
        "elapsed_ms": round((time.monotonic() - t0) * 1000, 1),
        "errors": errors,
    }
