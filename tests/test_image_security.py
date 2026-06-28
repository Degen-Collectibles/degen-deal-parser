from __future__ import annotations

import asyncio
import base64
import io
import json
import threading
import warnings
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from PIL import Image

from app import image_security
from app.inventory import card_detect, phash_scanner, pokemon_scanner, pricing
from app.inventory import routes as inventory_routes


class FakeJsonRequest:
    def __init__(self, payload: object):
        self._payload = payload
        self.headers: dict[str, str] = {}
        self.client = None

    async def json(self) -> object:
        return self._payload


def _image_b64(
    image_format: str = "JPEG",
    *,
    size: tuple[int, int] = (24, 32),
    data_url_mime: str | None = None,
) -> str:
    mode = "RGB" if image_format.upper() in {"JPEG", "WEBP"} else "RGBA"
    image = Image.new(mode, size, (40, 80, 120) if mode == "RGB" else (40, 80, 120, 255))
    buffer = io.BytesIO()
    image.save(buffer, format=image_format)
    encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
    if data_url_mime:
        return f"data:{data_url_mime};base64,{encoded}"
    return encoded


def _compressed_png_b64(size: tuple[int, int]) -> str:
    image = Image.new("1", size, 0)
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def _response_json(response) -> dict:
    return json.loads(response.body)


@pytest.mark.parametrize(
    ("image_format", "mime_type"),
    [
        ("JPEG", "image/jpeg"),
        ("PNG", "image/png"),
        ("WEBP", "image/webp"),
    ],
)
def test_full_validator_accepts_supported_raw_and_data_url_images(
    image_format: str,
    mime_type: str,
) -> None:
    raw_result = image_security.validate_image_base64(_image_b64(image_format))
    data_url_result = image_security.validate_image_base64(
        _image_b64(image_format, data_url_mime=mime_type)
    )

    assert raw_result.image_format == image_format
    assert raw_result.mime_type == mime_type
    assert raw_result.width == 24
    assert raw_result.height == 32
    assert data_url_result.image_format == image_format
    assert data_url_result.encoded_b64 == raw_result.encoded_b64


@pytest.mark.parametrize(
    ("value", "status_code"),
    [
        ("%%%not-base64%%%", 400),
        ("data:image/svg+xml;base64,PHN2Zz48L3N2Zz4=", 415),
        ("data:text/html;base64,PGgxPm5vPC9oMT4=", 415),
        ("data:image/jpeg;charset=utf-8;base64,AA==", 415),
    ],
)
def test_validator_rejects_invalid_base64_or_unsafe_data_url(
    value: str,
    status_code: int,
) -> None:
    with pytest.raises(image_security.ImageSecurityError) as exc_info:
        image_security.validate_image_base64(value)

    assert exc_info.value.status_code == status_code
    assert "PIL" not in str(exc_info.value)
    assert "decoder" not in str(exc_info.value).lower()


def test_encoded_oversize_rejects_before_pillow_header_inspection(monkeypatch: pytest.MonkeyPatch) -> None:
    opened = MagicMock(side_effect=AssertionError("Pillow must not be called"))
    monkeypatch.setattr(image_security.Image, "open", opened)

    with pytest.raises(image_security.ImageSecurityError) as exc_info:
        image_security.validate_image_base64(
            "A" * (image_security.FULL_SCAN_PROFILE.max_encoded_chars + 1)
        )

    assert exc_info.value.status_code == 413
    opened.assert_not_called()


def test_decoded_oversize_rejects_before_pillow_header_inspection(monkeypatch: pytest.MonkeyPatch) -> None:
    opened = MagicMock(side_effect=AssertionError("Pillow must not be called"))
    monkeypatch.setattr(image_security.Image, "open", opened)

    with pytest.raises(image_security.ImageSecurityError) as exc_info:
        image_security.validate_image_bytes(
            b"x" * (image_security.FULL_SCAN_PROFILE.max_decoded_bytes + 1)
        )

    assert exc_info.value.status_code == 413
    opened.assert_not_called()


def test_surrounding_whitespace_cannot_bypass_raw_image_value_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = image_security.ImageValidationProfile(
        max_encoded_chars=16,
        max_decoded_bytes=16,
        max_dimension=16,
        max_pixels=256,
    )
    opened = MagicMock(side_effect=AssertionError("Pillow must not be called"))
    monkeypatch.setattr(image_security.Image, "open", opened)
    oversized_raw_value = (
        " " * (image_security.IMAGE_VALUE_OVERHEAD_CHARS + 1)
        + "A" * profile.max_encoded_chars
    )

    with pytest.raises(image_security.ImageSecurityError) as exc_info:
        image_security.validate_image_base64(oversized_raw_value, profile=profile)

    assert exc_info.value.status_code == 413
    assert exc_info.value.code == "image_encoding_exceeded"
    opened.assert_not_called()


def test_file_validator_accepts_bytes_or_path_and_honors_custom_limits(tmp_path: Path) -> None:
    raw = base64.b64decode(_image_b64("PNG"), validate=True)
    path = tmp_path / "upload.bin"
    path.write_bytes(raw)

    from_bytes = image_security.validate_image_file(
        raw,
        allowed_formats=frozenset({"PNG"}),
        max_bytes=len(raw),
        max_dimension=64,
        max_pixels=2048,
    )
    from_path = image_security.validate_image_file(
        path,
        allowed_formats=frozenset({"PNG"}),
        max_bytes=len(raw),
        max_dimension=64,
        max_pixels=2048,
    )

    assert (from_bytes.image_format, from_bytes.width, from_bytes.height) == ("PNG", 24, 32)
    assert from_path == from_bytes


def test_file_validator_rejects_by_stat_before_read_or_pillow(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "oversize.bin"
    path.write_bytes(b"x" * 32)
    opened = MagicMock(side_effect=AssertionError("Pillow must not be called"))
    monkeypatch.setattr(image_security.Image, "open", opened)

    with pytest.raises(image_security.ImageSecurityError) as exc_info:
        image_security.validate_image_file(path, max_bytes=31)

    assert exc_info.value.status_code == 413
    opened.assert_not_called()


def test_file_validator_rejects_detected_format_outside_caller_allowlist() -> None:
    raw = base64.b64decode(_image_b64("JPEG"), validate=True)

    with pytest.raises(image_security.ImageSecurityError) as exc_info:
        image_security.validate_image_file(raw, allowed_formats=frozenset({"PNG"}))

    assert exc_info.value.status_code == 415
    assert exc_info.value.code == "unsupported_image_type"


def test_mime_must_match_detected_image_format() -> None:
    jpeg_as_png = _image_b64("JPEG", data_url_mime="image/png")

    with pytest.raises(image_security.ImageSecurityError) as exc_info:
        image_security.validate_image_base64(jpeg_as_png)

    assert exc_info.value.status_code == 415
    assert exc_info.value.code == "image_mime_mismatch"


def test_unsupported_gif_is_rejected() -> None:
    with pytest.raises(image_security.ImageSecurityError) as exc_info:
        image_security.validate_image_base64(_image_b64("GIF"))

    assert exc_info.value.status_code == 415


def test_compressed_huge_pixel_image_is_rejected_before_native_decode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Mode 1 keeps the test fixture small while declaring >24M pixels.
    huge_png = _compressed_png_b64((5000, 4801))
    with patch("app.inventory.card_detect.cv2.imdecode") as cv2_decode:
        with pytest.raises(image_security.ImageSecurityError) as exc_info:
            image_security.validate_image_base64(huge_png)

    assert len(huge_png) < 100_000
    assert exc_info.value.status_code == 413
    assert exc_info.value.code == "image_pixels_exceeded"
    cv2_decode.assert_not_called()


def test_detect_profile_enforces_smaller_dimension_and_pixel_budgets() -> None:
    over_dimension = _compressed_png_b64((2049, 1))
    over_pixels = _compressed_png_b64((2001, 2000))

    with pytest.raises(image_security.ImageSecurityError) as dimension_error:
        image_security.validate_image_base64(
            over_dimension,
            profile=image_security.DETECT_ONLY_PROFILE,
        )
    with pytest.raises(image_security.ImageSecurityError) as pixel_error:
        image_security.validate_image_base64(
            over_pixels,
            profile=image_security.DETECT_ONLY_PROFILE,
        )

    assert dimension_error.value.status_code == 413
    assert dimension_error.value.code == "image_dimensions_exceeded"
    assert pixel_error.value.status_code == 413
    assert pixel_error.value.code == "image_pixels_exceeded"


def test_pillow_decompression_bomb_warning_is_a_stable_rejection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class WarningImage:
        format = "PNG"
        size = (10, 10)

        def __enter__(self):
            warnings.warn("bomb details must not escape", Image.DecompressionBombWarning)
            return self

        def __exit__(self, *args):
            return False

    monkeypatch.setattr(image_security.Image, "open", lambda *_args, **_kwargs: WarningImage())

    with pytest.raises(image_security.ImageSecurityError) as exc_info:
        image_security.validate_image_base64(_image_b64("PNG"))

    assert exc_info.value.status_code == 413
    assert str(exc_info.value) == "Image dimensions are too large."
    assert "bomb details" not in str(exc_info.value)


def test_decode_limiter_rejects_third_concurrent_native_decode() -> None:
    entered = threading.Barrier(3)
    release = threading.Event()
    failures: list[BaseException] = []

    def holder() -> None:
        try:
            with image_security.image_decode_slot():
                entered.wait(timeout=2)
                release.wait(timeout=2)
        except BaseException as exc:  # pragma: no cover - assertion aid
            failures.append(exc)

    threads = [threading.Thread(target=holder), threading.Thread(target=holder)]
    for thread in threads:
        thread.start()
    entered.wait(timeout=2)
    try:
        with pytest.raises(image_security.ImageDecodeBusy) as exc_info:
            with image_security.image_decode_slot(acquire_timeout=0.01):
                pass
        assert exc_info.value.status_code == 429
    finally:
        release.set()
        for thread in threads:
            thread.join(timeout=2)
    assert failures == []


def test_detached_child_does_not_inherit_reentrant_decode_permit() -> None:
    async def drive() -> None:
        gate = asyncio.Event()
        parent_lease = image_security.acquire_image_decode_lease()

        async def detached_child() -> None:
            await gate.wait()
            with image_security.image_decode_slot(acquire_timeout=0.01):
                pass

        with parent_lease.activate():
            child_task = asyncio.create_task(detached_child())
        parent_lease.release()

        lease_one = image_security.acquire_image_decode_lease()
        lease_two = image_security.acquire_image_decode_lease()
        try:
            gate.set()
            with pytest.raises(image_security.ImageDecodeBusy):
                await child_task
        finally:
            lease_one.release()
            lease_two.release()

    asyncio.run(drive())


def test_lease_owner_can_explicitly_handoff_decode_permit_to_worker_thread() -> None:
    def native_helper() -> str:
        with image_security.image_decode_slot(acquire_timeout=0.01):
            return "ok"

    async def drive() -> str:
        lease_one = image_security.acquire_image_decode_lease()
        lease_two = image_security.acquire_image_decode_lease()
        try:
            with lease_one.activate():
                return await image_security.run_in_image_decode_thread(native_helper)
        finally:
            lease_one.release()
            lease_two.release()

    assert asyncio.run(drive()) == "ok"


def test_cancelled_handoff_keeps_lease_until_native_worker_finishes() -> None:
    worker_started = threading.Event()
    release_worker = threading.Event()

    def blocking_native_helper() -> None:
        with image_security.image_decode_slot(acquire_timeout=0.01):
            worker_started.set()
            release_worker.wait(timeout=2)

    async def owner() -> None:
        lease = image_security.acquire_image_decode_lease()
        try:
            with lease.activate():
                await image_security.run_in_image_decode_thread(blocking_native_helper)
        finally:
            lease.release()

    async def drive() -> None:
        owner_task = asyncio.create_task(owner())
        assert await asyncio.to_thread(worker_started.wait, 2)
        owner_task.cancel()
        await asyncio.sleep(0)

        # Cancellation must remain pending while the native worker holds the
        # original lease. Only the one genuinely free slot is available.
        assert not owner_task.done()
        other_lease = image_security.acquire_image_decode_lease()
        try:
            with pytest.raises(image_security.ImageDecodeBusy):
                image_security.acquire_image_decode_lease(acquire_timeout=0.01)
        finally:
            other_lease.release()

        release_worker.set()
        with pytest.raises(asyncio.CancelledError):
            await owner_task

        # Once the worker exits and cancellation propagates, both slots are
        # available again.
        lease_one = image_security.acquire_image_decode_lease()
        lease_two = image_security.acquire_image_decode_lease()
        lease_one.release()
        lease_two.release()

    try:
        asyncio.run(drive())
    finally:
        release_worker.set()


def test_decode_limiter_covers_opencv_processing_after_imdecode() -> None:
    raw = base64.b64decode(_image_b64("JPEG", size=(64, 96)), validate=True)
    entered = threading.Barrier(3)
    release = threading.Event()
    failures: list[BaseException] = []
    real_canny = card_detect.cv2.Canny

    def blocking_canny(*args, **kwargs):
        entered.wait(timeout=2)
        release.wait(timeout=2)
        return real_canny(*args, **kwargs)

    def holder() -> None:
        try:
            card_detect.detect_box(raw)
        except BaseException as exc:  # pragma: no cover - assertion aid
            failures.append(exc)

    with patch.object(card_detect.cv2, "Canny", side_effect=blocking_canny):
        threads = [threading.Thread(target=holder), threading.Thread(target=holder)]
        for thread in threads:
            thread.start()
        entered.wait(timeout=2)
        try:
            with pytest.raises(image_security.ImageDecodeBusy):
                card_detect.detect_box(raw)
        finally:
            release.set()
            for thread in threads:
                thread.join(timeout=2)

    assert failures == []


@pytest.mark.parametrize("image_format", ["JPEG", "PNG", "WEBP"])
def test_v1_route_accepts_valid_supported_images_and_passes_normalized_base64(
    image_format: str,
) -> None:
    pipeline = AsyncMock(return_value={"status": "MATCHED", "debug": {}})
    request = FakeJsonRequest({"image": _image_b64(image_format), "category_id": "3"})

    with patch.object(inventory_routes, "_require_employee_permission", return_value=None), patch.object(
        inventory_routes,
        "run_pokemon_pipeline",
        pipeline,
    ):
        response = asyncio.run(inventory_routes.inventory_scan_pokemon_identify(request, MagicMock()))

    assert response.status_code == 200
    normalized = pipeline.await_args.args[0]
    assert "," not in normalized
    assert base64.b64decode(normalized, validate=True)


@pytest.mark.parametrize(
    ("image_format", "mime_type"),
    [("JPEG", "image/jpeg"), ("PNG", "image/png"), ("WEBP", "image/webp")],
)
def test_generic_ai_route_preserves_valid_image_mime(
    image_format: str,
    mime_type: str,
) -> None:
    identify = AsyncMock(
        return_value={"card_name": "Pikachu", "game": "Pokemon", "confidence": 0.95}
    )
    lookup = AsyncMock(return_value={})
    request = FakeJsonRequest({"image": _image_b64(image_format)})

    with patch.object(inventory_routes, "_require_employee_permission", return_value=None), patch.object(
        inventory_routes,
        "identify_card_from_image",
        identify,
    ), patch.object(
        inventory_routes,
        "lookup_card_image_and_price",
        lookup,
    ):
        response = asyncio.run(inventory_routes.inventory_scan_identify(request, MagicMock()))

    assert response.status_code == 200
    assert identify.await_args.kwargs["mime_type"] == mime_type


@pytest.mark.parametrize(
    ("route_name", "payload", "expected_status"),
    [
        ("inventory_scan_pokemon_identify", {"image": "%%%"}, 400),
        ("degen_eye_v2_scan", {"image": "%%%"}, 400),
        ("degen_eye_v2_scan_init", {"image": "%%%"}, 400),
        ("degen_eye_v2_detect_only", {"image": "%%%"}, 400),
        ("inventory_scan_identify", {"image": "%%%"}, 400),
    ],
)
def test_routes_reject_invalid_image_before_pipeline(
    route_name: str,
    payload: dict[str, str],
    expected_status: int,
) -> None:
    request = FakeJsonRequest(payload)
    route = getattr(inventory_routes, route_name)
    with ExitStack() as stack:
        stack.enter_context(patch.object(inventory_routes, "_require_employee_permission", return_value=None))
        pipeline = stack.enter_context(
            patch.object(inventory_routes, "run_pokemon_pipeline", new=AsyncMock())
        )
        v2_pipeline = stack.enter_context(
            patch.object(inventory_routes, "run_v2_pipeline", new=AsyncMock())
        )
        generic = stack.enter_context(
            patch.object(inventory_routes, "identify_card_from_image", new=AsyncMock())
        )
        capture = stack.enter_context(
            patch.object(inventory_routes, "create_scan_capture")
        )
        response = asyncio.run(route(request, MagicMock()))

    assert response.status_code == expected_status
    assert _response_json(response)["error"] == "Invalid image encoding."
    pipeline.assert_not_awaited()
    v2_pipeline.assert_not_awaited()
    generic.assert_not_awaited()
    capture.assert_not_called()


@pytest.mark.parametrize(
    ("path", "request_limit"),
    [
        ("/degen_eye/identify", image_security.FULL_SCAN_REQUEST_MAX_BYTES),
        ("/degen_eye/v2/scan", image_security.FULL_SCAN_REQUEST_MAX_BYTES),
        ("/degen_eye/v2/scan-init", image_security.FULL_SCAN_REQUEST_MAX_BYTES),
        ("/degen_eye/v2/detect-only", image_security.DETECT_ONLY_REQUEST_MAX_BYTES),
        ("/inventory/scan/identify", image_security.FULL_SCAN_REQUEST_MAX_BYTES),
        ("/inventory/scan/slab-ximilar", image_security.FULL_SCAN_REQUEST_MAX_BYTES),
    ],
)
def test_main_app_rejects_oversized_scan_json_before_reading_body(
    path: str,
    request_limit: int,
) -> None:
    from app import main as app_main

    sent: list[dict] = []

    async def receive() -> dict:
        raise AssertionError("oversized request body must not be read")

    async def send(message: dict) -> None:
        sent.append(message)

    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("ascii"),
        "query_string": b"",
        "root_path": "",
        "headers": [(b"content-length", str(request_limit + 1).encode("ascii"))],
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
    }

    asyncio.run(app_main.app(scope, receive, send))

    assert sent[0]["type"] == "http.response.start"
    assert sent[0]["status"] == 413
    assert sent[1]["body"] == b'{"detail":"request_body_too_large"}'


def test_detect_route_rejects_huge_pixels_before_opencv() -> None:
    request = FakeJsonRequest({"image": _compressed_png_b64((2001, 2000))})
    with patch.object(inventory_routes, "_require_employee_permission", return_value=None), patch(
        "app.inventory.card_detect.cv2.imdecode"
    ) as cv2_decode:
        response = asyncio.run(inventory_routes.degen_eye_v2_detect_only(request, MagicMock()))

    assert response.status_code == 413
    assert _response_json(response)["code"] == "image_pixels_exceeded"
    cv2_decode.assert_not_called()


def test_full_scan_route_rejects_compressed_huge_pixels_before_pipeline() -> None:
    request = FakeJsonRequest({"image": _compressed_png_b64((5000, 4801))})
    pipeline = AsyncMock()
    with patch.object(inventory_routes, "_require_employee_permission", return_value=None), patch.object(
        inventory_routes,
        "run_pokemon_pipeline",
        pipeline,
    ):
        response = asyncio.run(inventory_routes.inventory_scan_pokemon_identify(request, MagicMock()))

    assert response.status_code == 413
    assert _response_json(response)["code"] == "image_pixels_exceeded"
    pipeline.assert_not_awaited()


def test_card_detect_helper_rejects_huge_header_before_opencv() -> None:
    raw = base64.b64decode(_compressed_png_b64((5000, 4801)), validate=True)
    with patch("app.inventory.card_detect.cv2.imdecode") as cv2_decode:
        with pytest.raises(image_security.ImageSecurityError) as exc_info:
            card_detect.detect_and_crop(raw)

    assert exc_info.value.status_code == 413
    cv2_decode.assert_not_called()


def test_third_detect_request_gets_429_while_two_native_decodes_are_running() -> None:
    encoded = _image_b64("JPEG")
    entered = threading.Event()
    release = threading.Event()
    count_lock = threading.Lock()
    entered_count = 0

    def blocking_imdecode(*_args, **_kwargs):
        nonlocal entered_count
        with count_lock:
            entered_count += 1
            if entered_count == 2:
                entered.set()
        release.wait(timeout=3)
        return None

    async def drive():
        first = asyncio.create_task(
            inventory_routes.degen_eye_v2_detect_only(FakeJsonRequest({"image": encoded}), MagicMock())
        )
        second = asyncio.create_task(
            inventory_routes.degen_eye_v2_detect_only(FakeJsonRequest({"image": encoded}), MagicMock())
        )
        assert await asyncio.to_thread(entered.wait, 2)
        third = await inventory_routes.degen_eye_v2_detect_only(
            FakeJsonRequest({"image": encoded}),
            MagicMock(),
        )
        release.set()
        await asyncio.gather(first, second)
        return third

    with patch.object(inventory_routes, "_require_employee_permission", return_value=None), patch(
        "app.inventory.card_detect.cv2.imdecode",
        side_effect=blocking_imdecode,
    ):
        try:
            response = asyncio.run(drive())
        finally:
            release.set()

    assert response.status_code == 429
    assert _response_json(response) == {
        "error": "Image processing is busy. Try again shortly.",
        "code": "image_decode_busy",
    }


def test_v2_stream_rejects_before_response_when_decode_slots_are_full() -> None:
    encoded = _image_b64("JPEG")
    lease_one = image_security.acquire_image_decode_lease()
    lease_two = image_security.acquire_image_decode_lease()
    try:
        with patch.object(inventory_routes, "_require_employee_permission", return_value=None), patch.object(
            inventory_routes,
            "_claim_v2_pending_scan",
            return_value=(encoded, "3", None),
        ):
            response = asyncio.run(
                inventory_routes.degen_eye_v2_scan_stream(
                    FakeJsonRequest({}),
                    "a" * 32,
                    MagicMock(),
                )
            )
    finally:
        lease_one.release()
        lease_two.release()

    assert response.status_code == 429
    assert _response_json(response)["code"] == "image_decode_busy"


def test_every_scanner_native_decode_path_uses_shared_capacity_limiter() -> None:
    encoded = _image_b64("JPEG")
    raw = base64.b64decode(encoded, validate=True)
    lease_one = image_security.acquire_image_decode_lease()
    lease_two = image_security.acquire_image_decode_lease()
    try:
        with pytest.raises(image_security.ImageDecodeBusy):
            card_detect.detect_and_crop(raw)
        with pytest.raises(image_security.ImageDecodeBusy):
            card_detect.detect_box(raw)
        with pytest.raises(image_security.ImageDecodeBusy):
            phash_scanner.compute_phash(raw)
        with pytest.raises(image_security.ImageDecodeBusy):
            pricing._prepare_ximilar_image_base64(encoded)
        with pytest.raises(image_security.ImageDecodeBusy):
            asyncio.run(pokemon_scanner._run_ximilar_pipeline(encoded, "test-token"))
        with patch.object(pokemon_scanner, "has_ai_key", return_value=True):
            with pytest.raises(image_security.ImageDecodeBusy):
                asyncio.run(pokemon_scanner._run_vision_pipeline(encoded))
        with patch.object(pokemon_scanner, "has_tiebreaker_key", return_value=True):
            with pytest.raises(image_security.ImageDecodeBusy):
                asyncio.run(pokemon_scanner._run_tiebreaker(encoded, None, None))
    finally:
        lease_one.release()
        lease_two.release()


@pytest.mark.parametrize("image_format", ["JPEG", "PNG", "WEBP"])
def test_slab_preprocessor_keeps_supported_formats_bounded(image_format: str) -> None:
    prepared = pricing._prepare_ximilar_image_base64(_image_b64(image_format))
    decoded = base64.b64decode(prepared, validate=True)
    with Image.open(io.BytesIO(decoded)) as image:
        assert image.format == "JPEG"
        assert max(image.size) <= 960


def test_slab_route_rejects_mime_mismatch_before_preprocess_or_network() -> None:
    request = FakeJsonRequest(
        {"image": _image_b64("JPEG", data_url_mime="image/png"), "game": "Pokemon"}
    )
    with patch.object(inventory_routes, "_require_employee_permission", return_value=None), patch.object(
        inventory_routes.settings,
        "ximilar_api_token",
        "test-token",
    ), patch.object(
        inventory_routes,
        "fetch_ximilar_slab_price_from_image",
        new=AsyncMock(),
    ) as slab_fetch:
        response = asyncio.run(inventory_routes.inventory_scan_slab_ximilar(request, MagicMock()))

    assert response.status_code == 415
    assert _response_json(response)["code"] == "image_mime_mismatch"
    slab_fetch.assert_not_awaited()
