"""Bounded validation and concurrency controls for untrusted card images.

Validation deliberately inspects only the container header with Pillow. Pixel
materialization remains the responsibility of scanner helpers, which must hold
``image_decode_slot`` while calling Pillow ``load``/``convert`` or OpenCV
``imdecode``.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import contextvars
import io
import os
import re
import threading
import warnings
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator, TypeVar

from PIL import Image, UnidentifiedImageError


@dataclass(frozen=True)
class ImageValidationProfile:
    max_encoded_chars: int
    max_decoded_bytes: int
    max_dimension: int
    max_pixels: int


FULL_SCAN_PROFILE = ImageValidationProfile(
    max_encoded_chars=12 * 1024 * 1024,
    max_decoded_bytes=9 * 1024 * 1024,
    max_dimension=8192,
    max_pixels=24_000_000,
)

DETECT_ONLY_PROFILE = ImageValidationProfile(
    max_encoded_chars=2 * 1024 * 1024,
    # The encoded cap makes this unreachable for canonical base64, but an
    # explicit byte budget keeps validate_image_bytes safe on its own.
    max_decoded_bytes=1536 * 1024,
    max_dimension=2048,
    max_pixels=4_000_000,
)

# Discord attachments arrive outside the scanner JSON endpoints and can be
# larger than a camera capture. Keep their byte budget separate while sharing
# the scanner's proven dimension and aggregate-pixel ceilings.
DISCORD_ATTACHMENT_PROFILE = ImageValidationProfile(
    max_encoded_chars=20 * 1024 * 1024,
    max_decoded_bytes=15 * 1024 * 1024,
    max_dimension=8192,
    max_pixels=24_000_000,
)

# JSON endpoints carry one base64 image plus only small scalar metadata. Keep
# the transport budget close to the validator budget so Starlette never has
# to buffer an attacker-controlled body before image validation runs.
IMAGE_JSON_REQUEST_OVERHEAD_BYTES = 16 * 1024
FULL_SCAN_REQUEST_MAX_BYTES = (
    FULL_SCAN_PROFILE.max_encoded_chars + IMAGE_JSON_REQUEST_OVERHEAD_BYTES
)
DETECT_ONLY_REQUEST_MAX_BYTES = (
    DETECT_ONLY_PROFILE.max_encoded_chars + IMAGE_JSON_REQUEST_OVERHEAD_BYTES
)

# A data-URL prefix is at most 23 characters for the supported MIME types.
# Allow a little surrounding whitespace for compatibility, but bound it
# before ``strip`` so whitespace cannot turn into an unbounded allocation.
IMAGE_VALUE_OVERHEAD_CHARS = 256


@dataclass(frozen=True)
class ValidatedImage:
    encoded_b64: str
    decoded_bytes: bytes
    image_format: str
    mime_type: str
    width: int
    height: int


class ImageSecurityError(ValueError):
    def __init__(self, message: str, *, status_code: int, code: str) -> None:
        super().__init__(message)
        self.public_message = message
        self.status_code = status_code
        self.code = code


class ImageDecodeBusy(ImageSecurityError):
    def __init__(self) -> None:
        super().__init__(
            "Image processing is busy. Try again shortly.",
            status_code=429,
            code="image_decode_busy",
        )


_FORMAT_TO_MIME = {
    "JPEG": "image/jpeg",
    "PNG": "image/png",
    "WEBP": "image/webp",
}
SUPPORTED_IMAGE_FORMATS = frozenset(_FORMAT_TO_MIME)
_DATA_URL_RE = re.compile(
    r"\Adata:(image/(?:jpeg|png|webp));base64,(.*)\Z",
    re.IGNORECASE,
)


def _split_image_value(value: Any) -> tuple[str, str | None]:
    if not isinstance(value, str):
        raise ImageSecurityError(
            "Invalid image encoding.",
            status_code=400,
            code="invalid_image_encoding",
        )
    text = value.strip()
    if not text:
        raise ImageSecurityError(
            "Missing image data.",
            status_code=400,
            code="missing_image",
        )
    if text[:5].lower() == "data:":
        match = _DATA_URL_RE.fullmatch(text)
        if match is None:
            raise ImageSecurityError(
                "Unsupported image data URL.",
                status_code=415,
                code="unsupported_image_data_url",
            )
        return match.group(2), match.group(1).lower()
    return text, None


def _inspect_image_header(
    raw: bytes,
    *,
    profile: ImageValidationProfile,
    declared_mime: str | None,
    allowed_formats: frozenset[str],
    max_frames: int | None,
) -> tuple[str, str, int, int]:
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(io.BytesIO(raw)) as image:
                image_format = str(image.format or "").upper()
                width, height = image.size
                frame_count = (
                    int(getattr(image, "n_frames", 1) or 1)
                    if max_frames is not None
                    else None
                )
    except (Image.DecompressionBombWarning, Image.DecompressionBombError):
        raise ImageSecurityError(
            "Image dimensions are too large.",
            status_code=413,
            code="image_dimensions_exceeded",
        ) from None
    except UnidentifiedImageError:
        raise ImageSecurityError(
            "Unsupported image type.",
            status_code=415,
            code="unsupported_image_type",
        ) from None
    except (OSError, SyntaxError, ValueError, TypeError):
        raise ImageSecurityError(
            "Image could not be read.",
            status_code=422,
            code="invalid_image_data",
        ) from None

    mime_type = _FORMAT_TO_MIME.get(image_format)
    if mime_type is None or image_format not in allowed_formats:
        raise ImageSecurityError(
            "Unsupported image type.",
            status_code=415,
            code="unsupported_image_type",
        )
    if declared_mime is not None and declared_mime != mime_type:
        raise ImageSecurityError(
            "Image type does not match its data.",
            status_code=415,
            code="image_mime_mismatch",
        )
    if width <= 0 or height <= 0:
        raise ImageSecurityError(
            "Image could not be read.",
            status_code=422,
            code="invalid_image_dimensions",
        )
    if width > profile.max_dimension or height > profile.max_dimension:
        raise ImageSecurityError(
            "Image dimensions are too large.",
            status_code=413,
            code="image_dimensions_exceeded",
        )
    if width * height > profile.max_pixels:
        raise ImageSecurityError(
            "Image has too many pixels.",
            status_code=413,
            code="image_pixels_exceeded",
        )
    if (
        max_frames is not None
        and frame_count is not None
        and frame_count > max(int(max_frames), 0)
    ):
        raise ImageSecurityError(
            "Animated images are not supported.",
            status_code=415,
            code="image_frames_exceeded",
        )
    return image_format, mime_type, width, height


def validate_image_bytes(
    raw: bytes,
    *,
    profile: ImageValidationProfile = FULL_SCAN_PROFILE,
    declared_mime: str | None = None,
    allowed_formats: frozenset[str] = SUPPORTED_IMAGE_FORMATS,
    max_frames: int | None = None,
) -> ValidatedImage:
    if not isinstance(raw, bytes):
        raise ImageSecurityError(
            "Invalid image data.",
            status_code=400,
            code="invalid_image_data",
        )
    if not raw:
        raise ImageSecurityError(
            "Missing image data.",
            status_code=400,
            code="missing_image",
        )
    if len(raw) > profile.max_decoded_bytes:
        raise ImageSecurityError(
            "Image is too large.",
            status_code=413,
            code="image_bytes_exceeded",
        )
    image_format, mime_type, width, height = _inspect_image_header(
        raw,
        profile=profile,
        declared_mime=declared_mime,
        allowed_formats=frozenset(str(value).upper() for value in allowed_formats),
        max_frames=max_frames,
    )
    return ValidatedImage(
        encoded_b64=base64.b64encode(raw).decode("ascii"),
        decoded_bytes=raw,
        image_format=image_format,
        mime_type=mime_type,
        width=width,
        height=height,
    )


def validate_image_base64(
    value: Any,
    *,
    profile: ImageValidationProfile = FULL_SCAN_PROFILE,
    allowed_formats: frozenset[str] = SUPPORTED_IMAGE_FORMATS,
    max_frames: int | None = None,
) -> ValidatedImage:
    if isinstance(value, str) and len(value) > (
        profile.max_encoded_chars + IMAGE_VALUE_OVERHEAD_CHARS
    ):
        raise ImageSecurityError(
            "Image is too large.",
            status_code=413,
            code="image_encoding_exceeded",
        )
    payload, declared_mime = _split_image_value(value)
    if len(payload) > profile.max_encoded_chars:
        raise ImageSecurityError(
            "Image is too large.",
            status_code=413,
            code="image_encoding_exceeded",
        )
    try:
        raw = base64.b64decode(payload, validate=True)
    except (binascii.Error, ValueError):
        raise ImageSecurityError(
            "Invalid image encoding.",
            status_code=400,
            code="invalid_image_encoding",
        ) from None
    return validate_image_bytes(
        raw,
        profile=profile,
        declared_mime=declared_mime,
        allowed_formats=allowed_formats,
        max_frames=max_frames,
    )


def validate_image_file(
    source: bytes | str | os.PathLike[str],
    *,
    allowed_formats: frozenset[str] = SUPPORTED_IMAGE_FORMATS,
    max_bytes: int = FULL_SCAN_PROFILE.max_decoded_bytes,
    max_dimension: int = FULL_SCAN_PROFILE.max_dimension,
    max_pixels: int = FULL_SCAN_PROFILE.max_pixels,
    max_frames: int | None = None,
) -> ValidatedImage:
    """Validate uploaded image bytes or a local file without pixel decoding.

    File paths are size-checked with ``stat`` before any read and then read at
    most ``max_bytes + 1`` bytes. Callers can choose a narrower format set and
    resource budget while sharing the same stable errors as base64 scanners.
    """
    profile = ImageValidationProfile(
        max_encoded_chars=0,
        max_decoded_bytes=max(int(max_bytes), 0),
        max_dimension=max(int(max_dimension), 0),
        max_pixels=max(int(max_pixels), 0),
    )
    if isinstance(source, bytes):
        raw = source
    else:
        try:
            path = Path(source)
            if path.stat().st_size > profile.max_decoded_bytes:
                raise ImageSecurityError(
                    "Image is too large.",
                    status_code=413,
                    code="image_bytes_exceeded",
                )
            with path.open("rb") as handle:
                raw = handle.read(profile.max_decoded_bytes + 1)
        except ImageSecurityError:
            raise
        except (OSError, TypeError, ValueError):
            raise ImageSecurityError(
                "Image file could not be read.",
                status_code=422,
                code="image_file_unreadable",
            ) from None
    return validate_image_bytes(
        raw,
        profile=profile,
        allowed_formats=allowed_formats,
        max_frames=max_frames,
    )


_DECODE_LIMIT = 2
_DECODE_ACQUIRE_TIMEOUT = 0.05
_DECODE_SEMAPHORE = threading.BoundedSemaphore(_DECODE_LIMIT)
_T = TypeVar("_T")


def _current_async_task() -> asyncio.Task[Any] | None:
    try:
        return asyncio.current_task()
    except RuntimeError:
        return None


class _DecodePermit:
    def __init__(self) -> None:
        self.owner_task = _current_async_task()
        self.owner_thread_id = threading.get_ident()
        self.active = True

    def owned_by_current_task(self) -> bool:
        task = _current_async_task()
        if self.owner_task is not None:
            return task is self.owner_task
        return task is None and threading.get_ident() == self.owner_thread_id

    def allows_reentry(self) -> bool:
        if not self.active:
            return False
        handoff = _DECODE_THREAD_HANDOFF.get()
        if (
            handoff is not None
            and handoff[0] is self
            and handoff[1] == threading.get_ident()
        ):
            return True
        return self.owned_by_current_task()


_DECODE_PERMIT: contextvars.ContextVar[_DecodePermit | None] = contextvars.ContextVar(
    "image_decode_permit",
    default=None,
)
_DECODE_THREAD_HANDOFF: contextvars.ContextVar[tuple[_DecodePermit, int] | None] = contextvars.ContextVar(
    "image_decode_thread_handoff",
    default=None,
)


@contextmanager
def image_decode_slot(*, acquire_timeout: float = _DECODE_ACQUIRE_TIMEOUT) -> Iterator[None]:
    inherited_permit = _DECODE_PERMIT.get()
    if inherited_permit is not None and inherited_permit.allows_reentry():
        yield
        return

    acquired = _DECODE_SEMAPHORE.acquire(timeout=max(float(acquire_timeout), 0.0))
    if not acquired:
        raise ImageDecodeBusy()
    permit = _DecodePermit()
    token = _DECODE_PERMIT.set(permit)
    try:
        yield
    finally:
        permit.active = False
        _DECODE_PERMIT.reset(token)
        _DECODE_SEMAPHORE.release()


async def run_in_image_decode_thread(
    func: Callable[..., _T],
    /,
    *args: Any,
    **kwargs: Any,
) -> _T:
    """Run a decode helper in a worker thread with an explicit permit handoff.

    Context variables are copied by both ``asyncio.to_thread`` and
    ``asyncio.create_task``. Only the task that owns the active permit may
    authorize this thread handoff; detached child tasks must acquire a new
    semaphore slot.
    """
    permit = _DECODE_PERMIT.get()
    handoff = permit if permit is not None and permit.active and permit.owned_by_current_task() else None

    def invoke() -> _T:
        worker_handoff = (handoff, threading.get_ident()) if handoff is not None else None
        token = _DECODE_THREAD_HANDOFF.set(worker_handoff)
        try:
            return func(*args, **kwargs)
        finally:
            _DECODE_THREAD_HANDOFF.reset(token)

    # Cancelling an ``asyncio.to_thread`` await does not stop the native
    # worker. Shield it and defer cancellation propagation until the worker
    # exits so a surrounding ImageDecodeLease cannot be released while its
    # OpenCV/Pillow operation is still consuming native capacity.
    worker = asyncio.create_task(asyncio.to_thread(invoke))
    cancellation: asyncio.CancelledError | None = None
    while True:
        try:
            result = await asyncio.shield(worker)
            break
        except asyncio.CancelledError as exc:
            if cancellation is None:
                cancellation = exc
            if worker.done():
                break

    if cancellation is not None:
        # Retrieve a worker exception so asyncio does not report it as
        # unhandled; cancellation remains the caller-visible outcome.
        if not worker.cancelled():
            try:
                worker.result()
            except BaseException:
                pass
        raise cancellation
    return result


class ImageDecodeLease:
    """A reserved slot that can be activated later by a streaming iterator."""

    def __init__(self) -> None:
        self._released = False
        self._release_lock = threading.Lock()
        self._permit: _DecodePermit | None = None

    @contextmanager
    def activate(self) -> Iterator[None]:
        if self._released:
            raise RuntimeError("Image decode lease already released")
        if self._permit is not None:
            raise RuntimeError("Image decode lease is already active")
        permit = _DecodePermit()
        self._permit = permit
        token = _DECODE_PERMIT.set(permit)
        try:
            yield
        finally:
            permit.active = False
            self._permit = None
            _DECODE_PERMIT.reset(token)

    def release(self) -> None:
        with self._release_lock:
            if self._released:
                return
            self._released = True
            if self._permit is not None:
                self._permit.active = False
            _DECODE_SEMAPHORE.release()


def acquire_image_decode_lease(
    *,
    acquire_timeout: float = _DECODE_ACQUIRE_TIMEOUT,
) -> ImageDecodeLease:
    acquired = _DECODE_SEMAPHORE.acquire(timeout=max(float(acquire_timeout), 0.0))
    if not acquired:
        raise ImageDecodeBusy()
    return ImageDecodeLease()
