from __future__ import annotations

import asyncio
import io
import json
import os
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from PIL import Image

from app import attachment_storage, display_media, image_security
from app.discord import worker


def _image_bytes(
    image_format: str = "PNG",
    *,
    size: tuple[int, int] = (64, 48),
    mode: str = "RGB",
) -> bytes:
    image = Image.new(mode, size, 96)
    buffer = io.BytesIO()
    image.save(buffer, format=image_format)
    image.close()
    return buffer.getvalue()


def _compressed_png(size: tuple[int, int]) -> bytes:
    image = Image.new("1", size, 0)
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    image.close()
    return buffer.getvalue()


def _animated_png() -> bytes:
    first = Image.new("RGBA", (24, 24), (255, 0, 0, 255))
    second = Image.new("RGBA", (24, 24), (0, 0, 255, 255))
    buffer = io.BytesIO()
    first.save(
        buffer,
        format="PNG",
        save_all=True,
        append_images=[second],
        duration=100,
        loop=0,
    )
    first.close()
    second.close()
    return buffer.getvalue()


def _noisy_png() -> bytes:
    image = Image.effect_noise((512, 512), 100).convert("RGB")
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    image.close()
    return buffer.getvalue()


def test_attachment_profile_has_dedicated_discord_resource_budget() -> None:
    profile = image_security.DISCORD_ATTACHMENT_PROFILE

    assert profile.max_decoded_bytes == 15 * 1024 * 1024
    assert profile.max_dimension == 8192
    assert profile.max_pixels == 24_000_000


def test_thumbnail_rejects_compressed_pixel_bomb_before_materialization(
    tmp_path: Path,
) -> None:
    source = tmp_path / "bomb.png"
    source.write_bytes(_compressed_png((5000, 4801)))
    output = tmp_path / "thumb.jpg"

    with patch.object(attachment_storage, "thumbnail_cache_path", return_value=output), patch.object(
        Image.Image,
        "thumbnail",
        side_effect=AssertionError("pixel materialization must not run"),
    ) as thumbnail:
        result = attachment_storage.generate_thumbnail(source, 101)

    assert result is None
    assert not output.exists()
    thumbnail.assert_not_called()


def test_thumbnail_rejects_over_15_mib_before_pillow_open(tmp_path: Path) -> None:
    source = tmp_path / "oversized.png"
    source.write_bytes(b"x" * (15 * 1024 * 1024 + 1))
    output = tmp_path / "thumb.jpg"

    with patch.object(attachment_storage, "thumbnail_cache_path", return_value=output), patch.object(
        image_security.Image,
        "open",
        side_effect=AssertionError("Pillow must not inspect over-budget bytes"),
    ) as opened:
        result = attachment_storage.generate_thumbnail(source, 102)

    assert result is None
    assert not output.exists()
    opened.assert_not_called()


def test_thumbnail_rejects_animated_input_without_cache_write(tmp_path: Path) -> None:
    source = tmp_path / "animated.png"
    source.write_bytes(_animated_png())
    output = tmp_path / "thumb.jpg"

    with patch.object(attachment_storage, "thumbnail_cache_path", return_value=output):
        result = attachment_storage.generate_thumbnail(source, 103)

    assert result is None
    assert not output.exists()


def test_thumbnail_busy_slot_returns_none_without_cache_write(tmp_path: Path) -> None:
    source = tmp_path / "safe.png"
    source.write_bytes(_image_bytes())
    output = tmp_path / "thumb.jpg"
    lease_one = image_security.acquire_image_decode_lease()
    lease_two = image_security.acquire_image_decode_lease()
    try:
        with patch.object(attachment_storage, "thumbnail_cache_path", return_value=output):
            result = attachment_storage.generate_thumbnail(source, 104)
    finally:
        lease_one.release()
        lease_two.release()

    assert result is None
    assert not output.exists()


def test_thumbnail_success_decodes_validated_memory_and_writes_jpeg(tmp_path: Path) -> None:
    source = tmp_path / "safe.png"
    source.write_bytes(_image_bytes(mode="L"))
    output = tmp_path / "thumb.jpg"

    with patch.object(attachment_storage, "thumbnail_cache_path", return_value=output):
        result = attachment_storage.generate_thumbnail(source, 105)

    assert result == output
    assert output.exists()
    with Image.open(output) as thumbnail:
        thumbnail.load()
        assert thumbnail.format == "JPEG"
        assert thumbnail.mode == "RGB"
        assert max(thumbnail.size) <= 240


def test_valid_bounded_legacy_thumbnail_is_validated_under_slot_without_source_read(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source-no-longer-needed.png"
    output = tmp_path / "thumb.jpg"
    output.write_bytes(_image_bytes("JPEG"))

    with patch.object(attachment_storage, "thumbnail_cache_path", return_value=output), patch.object(
        attachment_storage,
        "validate_image_file",
        wraps=attachment_storage.validate_image_file,
    ) as validate, patch.object(
        attachment_storage,
        "image_decode_slot",
        wraps=image_security.image_decode_slot,
    ) as decode_slot:
        result = attachment_storage.generate_thumbnail(source, 106)

    assert result == output
    assert validate.call_args.args[0] == output
    assert validate.call_args.kwargs["allowed_formats"] == frozenset({"JPEG"})
    decode_slot.assert_called_once_with()


def test_partial_cached_thumbnail_is_ignored_and_atomically_regenerated(tmp_path: Path) -> None:
    source = tmp_path / "safe.png"
    source.write_bytes(_image_bytes("PNG"))
    output = tmp_path / "thumb.jpg"
    output.write_bytes(b"\xff\xd8\xff\xe0partial-jpeg")

    with patch.object(attachment_storage, "thumbnail_cache_path", return_value=output):
        result = attachment_storage.generate_thumbnail(source, 107)

    assert result == output
    with Image.open(output) as regenerated:
        regenerated.load()
        assert regenerated.format == "JPEG"
        assert max(regenerated.size) <= 240


def test_thumbnail_publish_is_atomic_during_concurrent_observation(tmp_path: Path) -> None:
    source = tmp_path / "safe.png"
    source.write_bytes(_image_bytes("PNG"))
    output = tmp_path / "thumb.jpg"
    replace_entered = threading.Event()
    release_replace = threading.Event()
    failures: list[BaseException] = []
    results: list[Path | None] = []
    real_replace = os.replace

    def blocking_replace(source_name, destination_name) -> None:
        assert Path(source_name).parent == output.parent
        assert Path(source_name).exists()
        assert Path(destination_name) == output
        assert not output.exists()
        replace_entered.set()
        release_replace.wait(timeout=2)
        real_replace(source_name, destination_name)

    def generate() -> None:
        try:
            results.append(attachment_storage.generate_thumbnail(source, 108))
        except BaseException as exc:  # pragma: no cover - assertion aid
            failures.append(exc)

    with patch.object(attachment_storage, "thumbnail_cache_path", return_value=output), patch.object(
        os,
        "replace",
        side_effect=blocking_replace,
    ):
        thread = threading.Thread(target=generate)
        thread.start()
        assert replace_entered.wait(timeout=2)
        assert not output.exists()
        release_replace.set()
        thread.join(timeout=2)

    assert not thread.is_alive()
    assert failures == []
    assert results == [output]
    assert output.exists()
    assert list(tmp_path.glob(f".{output.name}.*.tmp")) == []


def test_thumbnail_atomic_publish_cleans_temp_when_replace_fails(tmp_path: Path) -> None:
    source = tmp_path / "safe.png"
    source.write_bytes(_image_bytes("PNG"))
    output = tmp_path / "thumb.jpg"

    with patch.object(attachment_storage, "thumbnail_cache_path", return_value=output), patch.object(
        os,
        "replace",
        side_effect=OSError("simulated replace failure"),
    ):
        result = attachment_storage.generate_thumbnail(source, 109)

    assert result is None
    assert not output.exists()
    assert list(tmp_path.glob(f".{output.name}.*.tmp")) == []


def test_valid_cached_thumbnail_returns_none_when_decode_slots_are_busy(tmp_path: Path) -> None:
    source = tmp_path / "unused.png"
    output = tmp_path / "thumb.jpg"
    output.write_bytes(_image_bytes("JPEG"))
    lease_one = image_security.acquire_image_decode_lease()
    lease_two = image_security.acquire_image_decode_lease()
    try:
        with patch.object(attachment_storage, "thumbnail_cache_path", return_value=output):
            result = attachment_storage.generate_thumbnail(source, 110)
    finally:
        lease_one.release()
        lease_two.release()

    assert result is None


def test_small_vision_image_is_validated_and_uses_actual_mime() -> None:
    safe_png = _image_bytes("PNG")

    result = display_media.shrink_image_to_limit(
        safe_png,
        "image/jpeg",
        max_bytes=len(safe_png),
    )

    assert result == (safe_png, "image/png")


def test_small_vision_image_returns_none_when_header_decode_slot_is_busy() -> None:
    safe_png = _image_bytes("PNG")
    lease_one = image_security.acquire_image_decode_lease()
    lease_two = image_security.acquire_image_decode_lease()
    try:
        result = display_media.shrink_image_to_limit(
            safe_png,
            "image/png",
            max_bytes=len(safe_png),
        )
    finally:
        lease_one.release()
        lease_two.release()

    assert result is None


def test_unsafe_small_image_never_reaches_vision_base64() -> None:
    unsafe_gif = _image_bytes("GIF", mode="P")

    with patch.object(display_media.base64, "b64encode") as encode:
        result = display_media.encode_bytes_as_vision_data_url(
            unsafe_gif,
            "image/gif",
            max_bytes=len(unsafe_gif),
        )

    assert result is None
    encode.assert_not_called()


def test_truncated_small_image_never_reaches_vision_base64() -> None:
    truncated_jpeg = _image_bytes("JPEG")[:-1]
    with Image.open(io.BytesIO(truncated_jpeg)) as image:
        assert image.format == "JPEG"
        with pytest.raises(OSError):
            image.load()

    vision_base64 = MagicMock(wraps=display_media.base64)
    with patch.object(display_media, "base64", vision_base64):
        result = display_media.encode_bytes_as_vision_data_url(
            truncated_jpeg,
            "image/jpeg",
            max_bytes=len(truncated_jpeg),
        )

    assert result is None
    vision_base64.b64encode.assert_not_called()


def test_compressed_pixel_bomb_never_reaches_local_decode_or_vision_base64() -> None:
    bomb = _compressed_png((5000, 4801))

    with patch.object(Image.Image, "load") as load, patch.object(
        display_media.base64,
        "b64encode",
    ) as encode:
        result = display_media.encode_bytes_as_vision_data_url(
            bomb,
            "image/png",
            max_bytes=len(bomb),
        )

    assert result is None
    load.assert_not_called()
    encode.assert_not_called()


def test_oversized_safe_vision_image_shrinks_while_holding_decode_slot() -> None:
    noisy_png = _noisy_png()
    max_bytes = 100_000
    assert len(noisy_png) > max_bytes

    with patch.object(
        display_media,
        "image_decode_slot",
        wraps=image_security.image_decode_slot,
    ) as decode_slot:
        result = display_media.shrink_image_to_limit(
            noisy_png,
            "image/png",
            max_bytes=max_bytes,
        )

    assert result is not None
    image_bytes, mime_type = result
    assert len(image_bytes) <= max_bytes
    assert mime_type == "image/jpeg"
    decode_slot.assert_called_once_with()


def test_oversized_vision_image_returns_none_when_decode_slots_are_busy() -> None:
    noisy_png = _noisy_png()
    max_bytes = 100_000
    lease_one = image_security.acquire_image_decode_lease()
    lease_two = image_security.acquire_image_decode_lease()
    try:
        result = display_media.shrink_image_to_limit(
            noisy_png,
            "image/png",
            max_bytes=max_bytes,
        )
    finally:
        lease_one.release()
        lease_two.release()

    assert result is None


def test_wide_small_image_is_rejected_before_vision_base64() -> None:
    too_wide = _compressed_png((8193, 1))

    with patch.object(display_media.base64, "b64encode") as encode:
        result = display_media.encode_bytes_as_vision_data_url(
            too_wide,
            "image/png",
            max_bytes=len(too_wide),
        )

    assert result is None
    encode.assert_not_called()


def _worker_row(
    *,
    recoverable: bool = True,
    attachment_url: str = "https://cdn.example.invalid/unvalidated.png",
) -> SimpleNamespace:
    return SimpleNamespace(
        id=1,
        channel_id="channel-1" if recoverable else None,
        discord_message_id="message-1" if recoverable else None,
        attachment_urls_json=json.dumps([attachment_url]),
    )


def _worker_asset(data: bytes, *, content_type: str = "image/png") -> SimpleNamespace:
    return SimpleNamespace(
        message_id=1,
        data=data,
        content_type=content_type,
        filename="cached.png",
    )


def _query_result(assets: list[SimpleNamespace]) -> SimpleNamespace:
    return SimpleNamespace(all=lambda: assets)


def test_parser_inputs_drop_remote_fallback_when_cached_bytes_are_unsafe() -> None:
    session = MagicMock()
    session.exec.return_value = _query_result([_worker_asset(_image_bytes("GIF", mode="P"))])

    with pytest.raises(RuntimeError, match="Validated attachment image is unavailable"):
        asyncio.run(
            worker.build_parser_attachment_inputs(
                session,
                [_worker_row(recoverable=False)],
                ["https://cdn.example.invalid/unvalidated.png"],
            )
        )


def test_parser_inputs_raise_for_extensionless_unsafe_cached_image_asset() -> None:
    extensionless_url = "https://cdn.example.invalid/attachments/channel/message/12345"
    session = MagicMock()
    session.exec.return_value = _query_result([_worker_asset(_image_bytes("GIF", mode="P"))])

    with pytest.raises(RuntimeError, match="Validated attachment image is unavailable"):
        asyncio.run(
            worker.build_parser_attachment_inputs(
                session,
                [_worker_row(recoverable=False, attachment_url=extensionless_url)],
                [extensionless_url],
            )
        )


def test_parser_inputs_drop_remote_fallback_when_recovered_bytes_are_unsafe() -> None:
    session = MagicMock()
    session.exec.side_effect = [
        _query_result([]),
        _query_result([_worker_asset(_image_bytes("GIF", mode="P"))]),
    ]

    with patch.object(
        worker,
        "recover_attachment_assets_for_message",
        new=AsyncMock(return_value=True),
    ):
        with pytest.raises(RuntimeError, match="Validated attachment image is unavailable"):
            asyncio.run(
                worker.build_parser_attachment_inputs(
                    session,
                    [_worker_row()],
                    ["https://cdn.example.invalid/unvalidated.png"],
                )
            )


def test_parser_inputs_drop_remote_fallback_when_recovery_is_unavailable() -> None:
    session = MagicMock()
    session.exec.return_value = _query_result([])

    with patch.object(
        worker,
        "recover_attachment_assets_for_message",
        new=AsyncMock(return_value=False),
    ):
        with pytest.raises(RuntimeError, match="Validated attachment image is unavailable"):
            asyncio.run(
                worker.build_parser_attachment_inputs(
                    session,
                    [_worker_row()],
                    ["https://cdn.example.invalid/unvalidated.png"],
                )
            )


def test_parser_inputs_allow_empty_result_when_original_message_has_no_image() -> None:
    session = MagicMock()
    session.exec.return_value = _query_result([])

    result = asyncio.run(worker.build_parser_attachment_inputs(session, [_worker_row()], []))

    assert result == []


def test_parser_inputs_preserve_successfully_recovered_valid_image() -> None:
    safe_png = _image_bytes("PNG")
    session = MagicMock()
    session.exec.side_effect = [
        _query_result([]),
        _query_result([_worker_asset(safe_png)]),
    ]

    with patch.object(
        worker,
        "recover_attachment_assets_for_message",
        new=AsyncMock(return_value=True),
    ):
        result = asyncio.run(
            worker.build_parser_attachment_inputs(
                session,
                [_worker_row()],
                ["https://cdn.example.invalid/unvalidated.png"],
            )
        )

    assert len(result) == 1
    assert result[0].startswith("data:image/png;base64,")
    assert "cdn.example.invalid" not in result[0]
