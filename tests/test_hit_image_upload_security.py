from __future__ import annotations

import asyncio
import io
import json
import threading
from datetime import timedelta
from types import SimpleNamespace

import pytest
import httpx
from fastapi import FastAPI, File, UploadFile
from PIL import Image
from sqlalchemy.pool import StaticPool
from sqlalchemy.sql.dml import Update
from sqlmodel import Session, SQLModel, create_engine, select

from app.hit_image_uploads import (
    MAX_HIT_IMAGE_BYTES,
    HitImageUploadError,
    bind_pending_upload,
    finalize_pending_upload,
    prune_expired_pending_uploads,
    reserve_pending_upload,
    upload_marker,
    utcnow,
)
from app.models import LiveHit, LiveHitImageUpload, User
from app.request_body_limits import ExactPathBodyLimitMiddleware


def _engine():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


def _image_bytes(image_format: str) -> bytes:
    output = io.BytesIO()
    Image.new("RGB", (16, 12), color=(10, 20, 30)).save(output, format=image_format)
    return output.getvalue()


def _animated_webp_bytes(
    *,
    frame_count: int = 3,
    size: tuple[int, int] = (24, 18),
    corrupt_second_frame: bool = False,
) -> bytes:
    assert frame_count >= 3 if corrupt_second_frame else frame_count >= 1
    source_frames = [
        Image.new("RGBA", size, color=color)
        for color in ((255, 0, 0, 255), (0, 255, 0, 255), (0, 0, 255, 255))
    ]
    frames = [source_frames[index % len(source_frames)] for index in range(frame_count)]
    output = io.BytesIO()
    frames[0].save(
        output,
        format="WEBP",
        save_all=True,
        append_images=frames[1:],
        duration=100,
        loop=0,
        lossless=True,
    )
    raw = bytearray(output.getvalue())
    if not corrupt_second_frame:
        return bytes(raw)

    anmf_offsets: list[int] = []
    offset = 12
    while offset + 8 <= len(raw):
        chunk_size = int.from_bytes(raw[offset + 4 : offset + 8], "little")
        if raw[offset : offset + 4] == b"ANMF":
            anmf_offsets.append(offset)
        offset += 8 + chunk_size + (chunk_size & 1)
    assert len(anmf_offsets) == frame_count

    inner_chunk_offset = anmf_offsets[1] + 8 + 16
    assert raw[inner_chunk_offset : inner_chunk_offset + 4] == b"VP8L"
    inner_size = int.from_bytes(
        raw[inner_chunk_offset + 4 : inner_chunk_offset + 8],
        "little",
    )
    inner_payload_offset = inner_chunk_offset + 8
    assert inner_size > 5
    raw[inner_payload_offset + 5 : inner_payload_offset + inner_size] = b"\x00" * (
        inner_size - 5
    )
    return bytes(raw)


def _seed_user(session: Session, user_id: int) -> None:
    session.add(
        User(
            id=user_id,
            username=f"user-{user_id}",
            password_hash="x",
            password_salt="x",
            role="employee",
            is_active=True,
        )
    )
    session.commit()


def _serve_referenced_hit_image(
    session: Session,
    *,
    tmp_path,
    monkeypatch,
    filename: str,
    data: bytes,
):
    from app.routers import hits

    (tmp_path / filename).write_bytes(data)
    session.add(
        LiveHit(
            streamer_name="legacy",
            hit_note="stored image",
            image_filename=filename,
        )
    )
    session.commit()
    monkeypatch.setattr(hits, "_require_live_hits", lambda request, session=None: None)
    monkeypatch.setattr(hits, "_hit_images_dir", lambda: tmp_path)
    request = SimpleNamespace(state=SimpleNamespace(current_user=SimpleNamespace(id=1)))
    return hits.serve_hit_image(filename, request, session)


def _upload_hit_image(session: Session, *, tmp_path, monkeypatch, data: bytes):
    from app.routers import hits

    _seed_user(session, 71)
    upload = _FakeUpload(data, "image/webp")
    request = SimpleNamespace(state=SimpleNamespace(current_user=SimpleNamespace(id=71)))
    monkeypatch.setattr(hits, "_require_live_hits", lambda request, session=None: None)
    monkeypatch.setattr(hits, "_hit_images_dir", lambda: tmp_path)
    return asyncio.run(hits.hits_upload_image(request, upload, session))


def _track_webp_loads(monkeypatch) -> list[None]:
    from PIL import WebPImagePlugin

    load_calls: list[None] = []
    original_load = WebPImagePlugin.WebPImageFile.load

    def tracked_load(self, *args, **kwargs):
        load_calls.append(None)
        return original_load(self, *args, **kwargs)

    monkeypatch.setattr(WebPImagePlugin.WebPImageFile, "load", tracked_load)
    return load_calls


def test_hit_upload_state_migration_is_declared_for_both_databases():
    from app import db

    assert db.SQLITE_ADDITIVE_MIGRATIONS["live_hit_image_uploads"]["state"] == (
        "TEXT DEFAULT 'pending'"
    )
    assert db.POSTGRES_ADDITIVE_MIGRATIONS["live_hit_image_uploads"]["state"] == (
        "TEXT DEFAULT 'pending'"
    )
    expected_index = (
        "CREATE INDEX IF NOT EXISTS ix_live_hit_image_uploads_state "
        "ON live_hit_image_uploads (state)"
    )
    assert expected_index in db.SQLITE_INDEX_MIGRATIONS
    assert expected_index in db.POSTGRES_INDEX_MIGRATIONS


def test_exact_path_body_limit_rejects_content_length_before_downstream():
    called = False
    sent: list[dict] = []

    async def downstream(scope, receive, send):
        nonlocal called
        called = True

    middleware = ExactPathBodyLimitMiddleware(
        downstream,
        limits={("POST", "/api/hits/upload-image"): 5},
    )
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/api/hits/upload-image",
        "headers": [(b"content-length", b"6")],
    }
    async def send(message):
        sent.append(message)

    asyncio.run(middleware(scope, _empty_receive, send))

    assert called is False
    assert sent[0]["status"] == 413


def test_exact_path_body_limit_rejects_chunked_body_during_receive():
    sent: list[dict] = []
    chunks = iter(
        [
            {"type": "http.request", "body": b"123", "more_body": True},
            {"type": "http.request", "body": b"456", "more_body": False},
        ]
    )

    async def receive():
        return next(chunks)

    async def downstream(scope, bounded_receive, send):
        while True:
            message = await bounded_receive()
            if not message.get("more_body"):
                break

    middleware = ExactPathBodyLimitMiddleware(
        downstream,
        limits={("POST", "/api/hits/upload-image"): 5},
    )
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/api/hits/upload-image",
        "headers": [],
    }
    async def send(message):
        sent.append(message)

    asyncio.run(middleware(scope, receive, send))

    assert sent[0]["status"] == 413


def test_exact_path_body_limit_returns_413_for_real_chunked_fastapi_multipart():
    endpoint_called = False
    app = FastAPI()

    @app.post("/api/hits/upload-image")
    async def upload(file: UploadFile = File(...)):
        nonlocal endpoint_called
        endpoint_called = True
        return {"ok": True}

    app.add_middleware(
        ExactPathBodyLimitMiddleware,
        limits={("POST", "/api/hits/upload-image"): 1024},
    )
    boundary = "codex-security-boundary"
    body = (
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="file"; filename="large.png"\r\n'
        "Content-Type: image/png\r\n\r\n"
    ).encode("ascii") + (b"x" * 2048) + f"\r\n--{boundary}--\r\n".encode("ascii")

    async def run_request():
        async def chunks():
            for offset in range(0, len(body), 137):
                yield body[offset : offset + 137]

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.post(
                "/api/hits/upload-image",
                headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
                content=chunks(),
            )

    response = asyncio.run(run_request())

    assert response.status_code == 413
    assert response.json() == {"detail": "request_body_too_large"}
    assert endpoint_called is False


async def _empty_receive():
    return {"type": "http.request", "body": b"", "more_body": False}


@pytest.mark.parametrize(
    ("image_format", "content_type"),
    [("JPEG", "image/jpeg"), ("PNG", "image/png"), ("WEBP", "image/webp")],
)
def test_upload_route_streams_bounded_chunks_and_returns_server_token(
    tmp_path,
    monkeypatch,
    image_format,
    content_type,
):
    from app.routers import hits

    engine = _engine()
    with Session(engine) as session:
        _seed_user(session, 11)
        upload = _FakeUpload(_image_bytes(image_format), content_type)
        request = SimpleNamespace(state=SimpleNamespace(current_user=SimpleNamespace(id=11)))
        monkeypatch.setattr(hits, "_require_live_hits", lambda request, session=None: None)
        monkeypatch.setattr(hits, "_hit_images_dir", lambda: tmp_path)

        response = asyncio.run(hits.hits_upload_image(request, upload, session))
        payload = json.loads(response.body)

        assert response.status_code == 200
        assert payload["filename"].startswith("upload:")
        assert payload["upload_token"] not in {row.name for row in tmp_path.iterdir()}
        assert upload.read_sizes and set(upload.read_sizes) == {64 * 1024}
        row = session.exec(select(LiveHitImageUpload)).one()
        assert row.owner_user_id == 11
        assert row.bound_hit_id is None
        assert (tmp_path / row.filename).read_bytes() == _image_bytes(image_format)


def test_upload_route_rejects_magic_mismatch_and_removes_reservation(tmp_path, monkeypatch):
    from app.routers import hits

    engine = _engine()
    with Session(engine) as session:
        _seed_user(session, 12)
        upload = _FakeUpload(_image_bytes("PNG"), "image/jpeg")
        request = SimpleNamespace(state=SimpleNamespace(current_user=SimpleNamespace(id=12)))
        monkeypatch.setattr(hits, "_require_live_hits", lambda request, session=None: None)
        monkeypatch.setattr(hits, "_hit_images_dir", lambda: tmp_path)

        response = asyncio.run(hits.hits_upload_image(request, upload, session))

        assert response.status_code == 415
        assert session.exec(select(LiveHitImageUpload)).all() == []
        assert [path for path in tmp_path.iterdir() if path.name != ".quota.lock"] == []


def test_upload_route_rejects_truncated_jpeg_that_header_verify_accepts(tmp_path, monkeypatch):
    from app.routers import hits

    engine = _engine()
    with Session(engine) as session:
        _seed_user(session, 14)
        upload = _FakeUpload(_image_bytes("JPEG")[:-1], "image/jpeg")
        request = SimpleNamespace(state=SimpleNamespace(current_user=SimpleNamespace(id=14)))
        monkeypatch.setattr(hits, "_require_live_hits", lambda request, session=None: None)
        monkeypatch.setattr(hits, "_hit_images_dir", lambda: tmp_path)

        response = asyncio.run(hits.hits_upload_image(request, upload, session))

        assert response.status_code == 422
        assert session.exec(select(LiveHitImageUpload)).all() == []
        assert [path for path in tmp_path.iterdir() if path.name != ".quota.lock"] == []


def test_upload_route_stops_after_file_cap_without_unbounded_read(tmp_path, monkeypatch):
    from app.routers import hits

    engine = _engine()
    with Session(engine) as session:
        _seed_user(session, 13)
        upload = _OversizeUpload("image/png")
        request = SimpleNamespace(state=SimpleNamespace(current_user=SimpleNamespace(id=13)))
        monkeypatch.setattr(hits, "_require_live_hits", lambda request, session=None: None)
        monkeypatch.setattr(hits, "_hit_images_dir", lambda: tmp_path)

        response = asyncio.run(hits.hits_upload_image(request, upload, session))

        assert response.status_code == 413
        assert upload.total_returned <= MAX_HIT_IMAGE_BYTES + 64 * 1024
        assert set(upload.read_sizes) == {64 * 1024}
        assert session.exec(select(LiveHitImageUpload)).all() == []


def test_serve_hit_image_rejects_referenced_legacy_active_bytes(tmp_path, monkeypatch):
    engine = _engine()
    with Session(engine) as session:
        response = _serve_referenced_hit_image(
            session,
            tmp_path=tmp_path,
            monkeypatch=monkeypatch,
            filename="legacy-active.png",
            data=b"<!doctype html><script>alert(document.domain)</script>",
        )

    assert response.status_code == 415


@pytest.mark.parametrize(
    ("image_format", "expected_media_type"),
    [("JPEG", "image/jpeg"), ("PNG", "image/png"), ("WEBP", "image/webp")],
)
def test_serve_hit_image_validates_legacy_bytes_and_sets_browser_hardening_headers(
    tmp_path,
    monkeypatch,
    image_format,
    expected_media_type,
):
    engine = _engine()
    image_bytes = _image_bytes(image_format)
    with Session(engine) as session:
        response = _serve_referenced_hit_image(
            session,
            tmp_path=tmp_path,
            monkeypatch=monkeypatch,
            filename=f"legacy-{image_format.lower()}.bin",
            data=image_bytes,
        )

    assert response.status_code == 200
    assert response.media_type == expected_media_type
    assert response.body == image_bytes
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["content-security-policy"] == "sandbox; default-src 'none'"
    assert response.headers["content-disposition"].startswith("inline")
    assert "\r" not in response.headers["content-disposition"]
    assert "\n" not in response.headers["content-disposition"]
    assert response.headers["cache-control"] == "private, max-age=300"


def test_serve_hit_image_revalidates_bytes_after_bound_upload_is_replaced(
    tmp_path,
    monkeypatch,
):
    from app.routers import hits

    engine = _engine()
    with Session(engine) as session:
        _seed_user(session, 61)
        upload = reserve_pending_upload(
            session,
            images_dir=tmp_path,
            owner_user_id=61,
            content_type="image/png",
        )
        image_path = tmp_path / upload.filename
        image_path.write_bytes(_image_bytes("PNG"))
        finalize_pending_upload(
            session,
            images_dir=tmp_path,
            token=upload.token,
            owner_user_id=61,
            actual_size=image_path.stat().st_size,
            content_type="image/png",
        )
        hit = LiveHit(streamer_name="replacement", hit_note="replacement")
        session.add(hit)
        session.flush()
        hit.image_filename = bind_pending_upload(
            session,
            images_dir=tmp_path,
            marker=upload_marker(upload.token),
            owner_user_id=61,
            hit_id=hit.id,
        )
        session.add(hit)
        session.commit()

        image_path.write_bytes(b"<svg><script>alert(document.domain)</script></svg>")
        monkeypatch.setattr(hits, "_require_live_hits", lambda request, session=None: None)
        monkeypatch.setattr(hits, "_hit_images_dir", lambda: tmp_path)
        request = SimpleNamespace(state=SimpleNamespace(current_user=SimpleNamespace(id=61)))
        response = hits.serve_hit_image(upload.filename, request, session)

    assert response.status_code == 415


def test_serve_hit_image_rejects_raster_with_active_trailing_payload(tmp_path, monkeypatch):
    engine = _engine()
    with Session(engine) as session:
        response = _serve_referenced_hit_image(
            session,
            tmp_path=tmp_path,
            monkeypatch=monkeypatch,
            filename="legacy-polyglot.png",
            data=_image_bytes("PNG") + b"<script>alert(document.domain)</script>",
        )

    assert response.status_code == 415


def test_serve_hit_image_rejects_png_polyglot_with_second_end_marker(tmp_path, monkeypatch):
    png_end_marker = b"\x00\x00\x00\x00IEND\xaeB\x60\x82"
    engine = _engine()
    with Session(engine) as session:
        response = _serve_referenced_hit_image(
            session,
            tmp_path=tmp_path,
            monkeypatch=monkeypatch,
            filename="legacy-double-iend.png",
            data=(
                _image_bytes("PNG")
                + b"<script>alert(document.domain)</script>"
                + png_end_marker
            ),
        )

    assert response.status_code == 415


def test_serve_hit_image_rejects_jpeg_polyglot_with_second_end_marker(tmp_path, monkeypatch):
    engine = _engine()
    with Session(engine) as session:
        response = _serve_referenced_hit_image(
            session,
            tmp_path=tmp_path,
            monkeypatch=monkeypatch,
            filename="legacy-double-eoi.jpg",
            data=_image_bytes("JPEG") + b"<script>alert(document.domain)</script>\xff\xd9",
        )

    assert response.status_code == 415


def test_serve_hit_image_preserves_valid_animated_webp(tmp_path, monkeypatch):
    image_bytes = _animated_webp_bytes()
    with Image.open(io.BytesIO(image_bytes)) as decoded:
        assert decoded.n_frames == 3

    engine = _engine()
    with Session(engine) as session:
        response = _serve_referenced_hit_image(
            session,
            tmp_path=tmp_path,
            monkeypatch=monkeypatch,
            filename="legacy-animated.bin",
            data=image_bytes,
        )

    assert response.status_code == 200
    assert response.media_type == "image/webp"
    assert response.body == image_bytes


def test_serve_hit_image_rejects_animated_webp_with_corrupt_later_frame(
    tmp_path,
    monkeypatch,
):
    image_bytes = _animated_webp_bytes(corrupt_second_frame=True)
    with Image.open(io.BytesIO(image_bytes)) as decoded:
        assert decoded.n_frames == 3
        decoded.load()
        decoded.seek(1)
        with pytest.raises(OSError):
            decoded.load()

    engine = _engine()
    with Session(engine) as session:
        response = _serve_referenced_hit_image(
            session,
            tmp_path=tmp_path,
            monkeypatch=monkeypatch,
            filename="legacy-corrupt-animation.webp",
            data=image_bytes,
        )

    assert response.status_code == 422


def test_upload_hit_image_preserves_modest_animated_webp(tmp_path, monkeypatch):
    image_bytes = _animated_webp_bytes()
    engine = _engine()
    with Session(engine) as session:
        response = _upload_hit_image(
            session,
            tmp_path=tmp_path,
            monkeypatch=monkeypatch,
            data=image_bytes,
        )

        assert response.status_code == 200
        upload = session.exec(select(LiveHitImageUpload)).one()
        assert (tmp_path / upload.filename).read_bytes() == image_bytes


def test_upload_hit_image_rejects_excessive_animation_frames_before_decode(
    tmp_path,
    monkeypatch,
):
    image_bytes = _animated_webp_bytes(frame_count=121, size=(8, 8))
    with Image.open(io.BytesIO(image_bytes)) as decoded:
        assert decoded.n_frames == 121
    load_calls = _track_webp_loads(monkeypatch)

    engine = _engine()
    with Session(engine) as session:
        response = _upload_hit_image(
            session,
            tmp_path=tmp_path,
            monkeypatch=monkeypatch,
            data=image_bytes,
        )

        assert response.status_code == 413
        assert json.loads(response.body)["code"] == "image_frame_count_exceeded"
        assert load_calls == []
        assert session.exec(select(LiveHitImageUpload)).all() == []


def test_serve_hit_image_rejects_excessive_animation_frames_before_decode(
    tmp_path,
    monkeypatch,
):
    image_bytes = _animated_webp_bytes(frame_count=121, size=(8, 8))
    with Image.open(io.BytesIO(image_bytes)) as decoded:
        assert decoded.n_frames == 121
    load_calls = _track_webp_loads(monkeypatch)

    engine = _engine()
    with Session(engine) as session:
        response = _serve_referenced_hit_image(
            session,
            tmp_path=tmp_path,
            monkeypatch=monkeypatch,
            filename="legacy-too-many-frames.webp",
            data=image_bytes,
        )

    assert response.status_code == 413
    assert json.loads(response.body)["code"] == "image_frame_count_exceeded"
    assert load_calls == []


def test_upload_hit_image_rejects_excessive_animation_pixel_work_before_decode(
    tmp_path,
    monkeypatch,
):
    image_bytes = _animated_webp_bytes(frame_count=25, size=(1024, 1024))
    with Image.open(io.BytesIO(image_bytes)) as decoded:
        assert decoded.n_frames == 25
        assert decoded.size == (1024, 1024)
    load_calls = _track_webp_loads(monkeypatch)

    engine = _engine()
    with Session(engine) as session:
        response = _upload_hit_image(
            session,
            tmp_path=tmp_path,
            monkeypatch=monkeypatch,
            data=image_bytes,
        )

        assert response.status_code == 413
        assert json.loads(response.body)["code"] == "image_animation_pixels_exceeded"
        assert load_calls == []
        assert session.exec(select(LiveHitImageUpload)).all() == []


def test_serve_hit_image_rejects_excessive_animation_pixel_work_before_decode(
    tmp_path,
    monkeypatch,
):
    image_bytes = _animated_webp_bytes(frame_count=25, size=(1024, 1024))
    with Image.open(io.BytesIO(image_bytes)) as decoded:
        assert decoded.n_frames == 25
        assert decoded.size == (1024, 1024)
    load_calls = _track_webp_loads(monkeypatch)

    engine = _engine()
    with Session(engine) as session:
        response = _serve_referenced_hit_image(
            session,
            tmp_path=tmp_path,
            monkeypatch=monkeypatch,
            filename="legacy-too-much-animation-work.webp",
            data=image_bytes,
        )

    assert response.status_code == 413
    assert json.loads(response.body)["code"] == "image_animation_pixels_exceeded"
    assert load_calls == []


def test_serve_hit_image_rejects_oversized_referenced_file(tmp_path, monkeypatch):
    from app.routers import hits

    image_bytes = _image_bytes("PNG")
    monkeypatch.setattr(hits, "MAX_IMAGE_SIZE", len(image_bytes) - 1)
    engine = _engine()
    with Session(engine) as session:
        response = _serve_referenced_hit_image(
            session,
            tmp_path=tmp_path,
            monkeypatch=monkeypatch,
            filename="legacy-oversized.png",
            data=image_bytes,
        )

    assert response.status_code == 413


def test_serve_hit_image_rejects_truncated_raster(tmp_path, monkeypatch):
    engine = _engine()
    with Session(engine) as session:
        response = _serve_referenced_hit_image(
            session,
            tmp_path=tmp_path,
            monkeypatch=monkeypatch,
            filename="legacy-truncated.jpg",
            data=_image_bytes("JPEG")[:-1],
        )

    assert response.status_code == 422


def test_binding_is_same_user_one_time_and_raw_filename_is_not_a_capability(tmp_path):
    engine = _engine()
    with Session(engine) as session:
        _seed_user(session, 21)
        _seed_user(session, 22)
        row = reserve_pending_upload(
            session,
            images_dir=tmp_path,
            owner_user_id=21,
            content_type="image/png",
        )
        (tmp_path / row.filename).write_bytes(_image_bytes("PNG"))
        row = finalize_pending_upload(
            session,
            images_dir=tmp_path,
            token=row.token,
            owner_user_id=21,
            actual_size=(tmp_path / row.filename).stat().st_size,
            content_type="image/png",
        )
        hit = LiveHit(streamer_name="A", hit_note="B")
        session.add(hit)
        session.flush()

        with pytest.raises(HitImageUploadError):
            bind_pending_upload(
                session,
                images_dir=tmp_path,
                marker=row.filename,
                owner_user_id=21,
                hit_id=hit.id,
            )
        with pytest.raises(HitImageUploadError):
            bind_pending_upload(
                session,
                images_dir=tmp_path,
                marker=upload_marker(row.token),
                owner_user_id=22,
                hit_id=hit.id,
            )

        filename = bind_pending_upload(
            session,
            images_dir=tmp_path,
            marker=upload_marker(row.token),
            owner_user_id=21,
            hit_id=hit.id,
        )
        hit.image_filename = filename
        session.add(hit)
        session.commit()

        with pytest.raises(HitImageUploadError):
            bind_pending_upload(
                session,
                images_dir=tmp_path,
                marker=upload_marker(row.token),
                owner_user_id=21,
                hit_id=hit.id,
            )


def test_binding_and_hit_insert_roll_back_together(tmp_path):
    engine = _engine()
    with Session(engine) as session:
        _seed_user(session, 31)
        row = reserve_pending_upload(
            session,
            images_dir=tmp_path,
            owner_user_id=31,
            content_type="image/png",
        )
        (tmp_path / row.filename).write_bytes(_image_bytes("PNG"))
        finalize_pending_upload(
            session,
            images_dir=tmp_path,
            token=row.token,
            owner_user_id=31,
            actual_size=(tmp_path / row.filename).stat().st_size,
            content_type="image/png",
        )
        hit = LiveHit(streamer_name="rollback", hit_note="rollback")
        session.add(hit)
        session.flush()
        bind_pending_upload(
            session,
            images_dir=tmp_path,
            marker=upload_marker(row.token),
            owner_user_id=31,
            hit_id=hit.id,
        )
        session.rollback()

        assert session.exec(select(LiveHit).where(LiveHit.streamer_name == "rollback")).first() is None
        persisted = session.exec(
            select(LiveHitImageUpload).where(LiveHitImageUpload.token == row.token)
        ).one()
        assert persisted.bound_hit_id is None


def test_binding_rejects_expired_or_missing_pending_file(tmp_path):
    engine = _engine()
    now = utcnow()
    with Session(engine) as session:
        _seed_user(session, 32)
        expired = LiveHitImageUpload(
            token="e" * 43,
            filename="expired.png",
            owner_user_id=32,
            size_bytes=10,
            content_type="image/png",
            created_at=now - timedelta(hours=2),
            expires_at=now - timedelta(hours=1),
        )
        missing = LiveHitImageUpload(
            token="m" * 43,
            filename="missing.png",
            owner_user_id=32,
            size_bytes=10,
            content_type="image/png",
            created_at=now,
            expires_at=now + timedelta(hours=1),
        )
        session.add(expired)
        session.add(missing)
        hit = LiveHit(streamer_name="missing", hit_note="missing")
        session.add(hit)
        session.commit()

        for row in (expired, missing):
            with pytest.raises(HitImageUploadError):
                bind_pending_upload(
                    session,
                    images_dir=tmp_path,
                    marker=upload_marker(row.token),
                    owner_user_id=32,
                    hit_id=hit.id,
                    now=now,
                )


def test_conditional_claim_rejects_reuse_from_stale_session(tmp_path):
    engine = _engine()
    with Session(engine) as setup:
        _seed_user(setup, 33)
        row = reserve_pending_upload(
            setup,
            images_dir=tmp_path,
            owner_user_id=33,
            content_type="image/png",
        )
        (tmp_path / row.filename).write_bytes(_image_bytes("PNG"))
        finalize_pending_upload(
            setup,
            images_dir=tmp_path,
            token=row.token,
            owner_user_id=33,
            actual_size=(tmp_path / row.filename).stat().st_size,
            content_type="image/png",
        )
        first_hit = LiveHit(streamer_name="first", hit_note="first")
        second_hit = LiveHit(streamer_name="second", hit_note="second")
        setup.add(first_hit)
        setup.add(second_hit)
        setup.commit()
        token = row.token
        first_hit_id = first_hit.id
        second_hit_id = second_hit.id

    with Session(engine) as stale, Session(engine) as winner:
        stale.exec(select(LiveHitImageUpload).where(LiveHitImageUpload.token == token)).one()
        bind_pending_upload(
            winner,
            images_dir=tmp_path,
            marker=upload_marker(token),
            owner_user_id=33,
            hit_id=first_hit_id,
        )
        winner.commit()

        with pytest.raises(HitImageUploadError):
            bind_pending_upload(
                stale,
                images_dir=tmp_path,
                marker=upload_marker(token),
                owner_user_id=33,
                hit_id=second_hit_id,
            )


def test_cleanup_stale_candidate_cannot_delete_newly_bound_evidence(tmp_path):
    database_path = tmp_path / "bind-prune-race.db"
    engine = create_engine(
        f"sqlite:///{database_path}",
        connect_args={"check_same_thread": False, "timeout": 5},
    )
    SQLModel.metadata.create_all(engine)
    cleanup_now = utcnow()
    with Session(engine) as setup:
        _seed_user(setup, 34)
        hit = LiveHit(streamer_name="race", hit_note="race")
        setup.add(hit)
        row = LiveHitImageUpload(
            token="r" * 43,
            filename="race.png",
            owner_user_id=34,
            size_bytes=10,
            content_type="image/png",
            state="pending",
            created_at=cleanup_now - timedelta(hours=1),
            expires_at=cleanup_now - timedelta(milliseconds=500),
        )
        setup.add(row)
        setup.commit()
        setup.refresh(hit)
        hit_id = hit.id
        token = row.token
        (tmp_path / row.filename).write_bytes(_image_bytes("PNG"))

    cleanup_selected = threading.Event()
    bind_finished = threading.Event()
    cleanup_error: list[BaseException] = []

    def run_cleanup():
        try:
            with Session(engine) as cleanup_session:
                original_exec = cleanup_session.exec

                def paused_exec(statement, *args, **kwargs):
                    if isinstance(statement, Update):
                        cleanup_session.rollback()
                        cleanup_selected.set()
                        assert bind_finished.wait(timeout=5)
                    return original_exec(statement, *args, **kwargs)

                cleanup_session.exec = paused_exec  # type: ignore[method-assign]
                prune_expired_pending_uploads(
                    cleanup_session,
                    images_dir=tmp_path,
                    now=cleanup_now,
                )
        except BaseException as exc:  # pragma: no cover - surfaced below
            cleanup_error.append(exc)

    cleanup_thread = threading.Thread(target=run_cleanup)
    cleanup_thread.start()
    assert cleanup_selected.wait(timeout=5)

    with Session(engine) as bind_session:
        hit = bind_session.get(LiveHit, hit_id)
        filename = bind_pending_upload(
            bind_session,
            images_dir=tmp_path,
            marker=upload_marker(token),
            owner_user_id=34,
            hit_id=hit_id,
            now=cleanup_now - timedelta(seconds=1),
        )
        hit.image_filename = filename
        bind_session.add(hit)
        bind_session.commit()
    bind_finished.set()
    cleanup_thread.join(timeout=5)

    assert not cleanup_thread.is_alive()
    assert cleanup_error == []
    with Session(engine) as verify:
        upload = verify.exec(
            select(LiveHitImageUpload).where(LiveHitImageUpload.token == token)
        ).one()
        hit = verify.get(LiveHit, hit_id)
        assert upload.state == "bound"
        assert upload.bound_hit_id == hit_id
        assert hit.image_filename == "race.png"
        assert (tmp_path / "race.png").is_file()


def test_expired_cleanup_deletes_only_unbound_pending_files(tmp_path):
    engine = _engine()
    now = utcnow()
    with Session(engine) as session:
        _seed_user(session, 41)
        hit = LiveHit(streamer_name="evidence", hit_note="keep", is_deleted=True)
        session.add(hit)
        session.flush()
        pending = LiveHitImageUpload(
            token="p" * 43,
            filename="pending.png",
            owner_user_id=41,
            size_bytes=10,
            content_type="image/png",
            created_at=now - timedelta(hours=2),
            expires_at=now - timedelta(hours=1),
        )
        bound = LiveHitImageUpload(
            token="b" * 43,
            filename="bound.png",
            owner_user_id=41,
            size_bytes=10,
            content_type="image/png",
            created_at=now - timedelta(hours=2),
            expires_at=now - timedelta(hours=1),
            bound_hit_id=hit.id,
            bound_at=now - timedelta(hours=1),
        )
        session.add(pending)
        session.add(bound)
        session.commit()
        (tmp_path / pending.filename).write_bytes(b"pending")
        (tmp_path / bound.filename).write_bytes(b"bound")

        assert prune_expired_pending_uploads(session, images_dir=tmp_path, now=now) == 1
        session.commit()

        assert not (tmp_path / pending.filename).exists()
        assert (tmp_path / bound.filename).read_bytes() == b"bound"
        assert session.exec(select(LiveHitImageUpload)).one().token == bound.token


def test_reservation_enforces_pending_and_untracked_global_quotas(tmp_path, monkeypatch):
    import app.hit_image_uploads as uploads

    engine = _engine()
    with Session(engine) as session:
        _seed_user(session, 51)
        monkeypatch.setattr(uploads, "MIN_FREE_DISK_BYTES", 0)
        monkeypatch.setattr(uploads, "MAX_PENDING_UPLOADS_PER_USER", 1)
        reserve_pending_upload(
            session,
            images_dir=tmp_path,
            owner_user_id=51,
            content_type="image/png",
        )
        with pytest.raises(HitImageUploadError) as pending_error:
            reserve_pending_upload(
                session,
                images_dir=tmp_path,
                owner_user_id=51,
                content_type="image/png",
            )
        assert pending_error.value.code == "image_upload_quota_exceeded"

    engine = _engine()
    with Session(engine) as session:
        _seed_user(session, 52)
        (tmp_path / "legacy-untracked.bin").write_bytes(b"1234")
        monkeypatch.setattr(uploads, "MAX_DURABLE_BYTES_GLOBAL", MAX_HIT_IMAGE_BYTES + 3)
        with pytest.raises(HitImageUploadError) as global_error:
            reserve_pending_upload(
                session,
                images_dir=tmp_path,
                owner_user_id=52,
                content_type="image/png",
            )
        assert global_error.value.code == "image_upload_quota_exceeded"


class _FakeUpload:
    def __init__(self, data: bytes, content_type: str) -> None:
        self._data = data
        self._offset = 0
        self.content_type = content_type
        self.read_sizes: list[int] = []

    async def read(self, size: int = -1) -> bytes:
        self.read_sizes.append(size)
        if size < 0:
            raise AssertionError("unbounded upload read")
        chunk = self._data[self._offset : self._offset + size]
        self._offset += len(chunk)
        return chunk

    async def close(self) -> None:
        return None


class _OversizeUpload:
    def __init__(self, content_type: str) -> None:
        self.content_type = content_type
        self.total_returned = 0
        self.read_sizes: list[int] = []

    async def read(self, size: int = -1) -> bytes:
        self.read_sizes.append(size)
        if size < 0:
            raise AssertionError("unbounded upload read")
        if self.total_returned > MAX_HIT_IMAGE_BYTES:
            return b""
        self.total_returned += size
        return b"x" * size

    async def close(self) -> None:
        return None
