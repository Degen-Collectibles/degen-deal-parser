from __future__ import annotations

import asyncio
import base64
import io
import json
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from PIL import Image

from app.inventory import degen_eye_v2_training as training
from app.inventory import routes as inventory_routes


class _JsonRequest:
    def __init__(self, payload: dict) -> None:
        self._payload = payload
        self.headers: dict[str, str] = {}
        self.client = None

    async def json(self) -> dict:
        return self._payload


def _image_b64(color: tuple[int, int, int] = (20, 40, 60)) -> str:
    output = io.BytesIO()
    Image.new("RGB", (24, 32), color=color).save(output, format="JPEG")
    return base64.b64encode(output.getvalue()).decode("ascii")


def _create_capture(
    *,
    owner_user_id: int = 7,
    color: tuple[int, int, int] = (20, 40, 60),
) -> str:
    capture_id = training.create_scan_capture(
        _image_b64(color),
        source="test",
        employee={"id": owner_user_id, "username": f"user-{owner_user_id}"},
        owner_user_id=owner_user_id,
    )
    assert capture_id is not None
    return capture_id


def _rewrite_capture(capture_id: str, update) -> Path:
    path, payload = training._load_metadata(capture_id)
    assert path is not None and payload is not None
    update(payload)
    training._write_json_atomic(path, payload)
    return path


def _prune_retention(**kwargs) -> int:
    prune = getattr(training, "prune_capture_retention", None)
    return 0 if prune is None else prune(**kwargs)


@pytest.fixture
def capture_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "captures"
    monkeypatch.setattr(training, "capture_root", lambda: root)
    monkeypatch.setattr(training, "_capture_enabled", lambda: True)
    return root


def test_capture_rejects_missing_numeric_owner(capture_root: Path) -> None:
    with pytest.raises(ValueError):
        training.create_scan_capture(
            _image_b64(),
            source="test",
            employee={},
        )

    assert list(capture_root.rglob("*.json")) == []


@pytest.mark.parametrize("owner_user_id", [True, 0, -1, "7"])
def test_capture_rejects_noncanonical_numeric_owner(
    capture_root: Path,
    owner_user_id: object,
) -> None:
    with pytest.raises(training.CaptureStorageError) as exc_info:
        training.create_scan_capture(
            _image_b64(),
            source="test",
            employee={"id": owner_user_id},
            owner_user_id=owner_user_id,
        )

    assert exc_info.value.code == "capture_owner_required"


def test_identical_capture_for_same_owner_reuses_one_durable_copy(capture_root: Path) -> None:
    first = _create_capture(owner_user_id=7)
    second = _create_capture(owner_user_id=7)

    assert second == first
    image_files = [
        path
        for path in capture_root.rglob("*")
        if path.is_file() and path.suffix.lower() in {".jpg", ".png", ".webp", ".img"}
    ]
    assert len(image_files) == 1


def test_identical_image_never_reuses_capture_across_owners(capture_root: Path) -> None:
    first = _create_capture(owner_user_id=7)
    second = _create_capture(owner_user_id=8)

    assert second != first
    _path, first_payload = training._load_metadata(first)
    _path, second_payload = training._load_metadata(second)
    assert first_payload is not None and first_payload["employee"]["id"] == 7
    assert second_payload is not None and second_payload["employee"]["id"] == 8


def test_per_user_capture_count_quota_blocks_next_distinct_image(
    capture_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(training, "MAX_CAPTURES_PER_USER", 1, raising=False)
    _create_capture(owner_user_id=7, color=(10, 20, 30))

    with pytest.raises(training.CaptureStorageError) as exc_info:
        _create_capture(owner_user_id=7, color=(30, 20, 10))

    assert exc_info.value.status_code == 507
    assert exc_info.value.code == "capture_user_count_quota"


def test_global_capture_count_quota_includes_other_users(
    capture_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(training, "MAX_CAPTURES_GLOBAL", 1, raising=False)
    _create_capture(owner_user_id=7, color=(1, 2, 3))

    with pytest.raises(training.CaptureStorageError) as exc_info:
        _create_capture(owner_user_id=8, color=(4, 5, 6))

    assert exc_info.value.code == "capture_global_count_quota"


def test_per_user_capture_byte_quota_counts_existing_images(
    capture_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _create_capture(owner_user_id=7, color=(11, 12, 13))
    durable_bytes = sum(
        path.stat().st_size
        for path in capture_root.rglob("*")
        if path.is_file() and path.name not in {".capture-quota.lock", ".capture-rate.json"}
    )
    monkeypatch.setattr(training, "MAX_CAPTURE_BYTES_PER_USER", durable_bytes, raising=False)

    with pytest.raises(training.CaptureStorageError) as exc_info:
        _create_capture(owner_user_id=7, color=(14, 15, 16))

    assert exc_info.value.code == "capture_user_bytes_quota"


def test_global_capture_byte_quota_counts_all_users(
    capture_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _create_capture(owner_user_id=7, color=(21, 22, 23))
    durable_bytes = sum(
        path.stat().st_size
        for path in capture_root.rglob("*")
        if path.is_file() and path.name not in {".capture-quota.lock", ".capture-rate.json"}
    )
    monkeypatch.setattr(training, "MAX_CAPTURE_BYTES_GLOBAL", durable_bytes, raising=False)

    with pytest.raises(training.CaptureStorageError) as exc_info:
        _create_capture(owner_user_id=8, color=(24, 25, 26))

    assert exc_info.value.code == "capture_global_bytes_quota"


def test_capture_metadata_is_bounded_before_durable_write(capture_root: Path) -> None:
    with pytest.raises(training.CaptureStorageError) as exc_info:
        training.create_scan_capture(
            _image_b64(),
            source="test",
            category_id="x" * (128 * 1024),
            employee={"id": 7},
            owner_user_id=7,
        )

    assert exc_info.value.status_code == 413
    assert exc_info.value.code == "capture_metadata_too_large"
    assert list(capture_root.rglob("metadata.json")) == []


def test_capture_stats_excludes_lock_and_rate_control_files(capture_root: Path) -> None:
    _create_capture(owner_user_id=7)

    stats = training.capture_stats()

    assert stats["captures"] == 1


def test_free_space_floor_reserves_worst_case_decoded_capture(
    capture_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(training, "MIN_CAPTURE_FREE_BYTES", 1_000, raising=False)
    monkeypatch.setattr(training, "MAX_IN_FLIGHT_CAPTURE_BYTES", 500, raising=False)
    monkeypatch.setattr(training, "_available_capture_disk_bytes", lambda _root: 1_499, raising=False)

    with pytest.raises(training.CaptureStorageError) as exc_info:
        _create_capture(owner_user_id=7)

    assert exc_info.value.code == "capture_free_space_floor"
    assert list(capture_root.rglob("*.jpg")) == []


def test_per_user_capture_request_rate_limit_is_enforced(
    capture_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(training, "CAPTURE_REQUESTS_PER_USER", 2, raising=False)
    monkeypatch.setattr(training, "CAPTURE_RATE_WINDOW_SECONDS", 60.0, raising=False)
    _create_capture(owner_user_id=7, color=(31, 32, 33))
    _create_capture(owner_user_id=7, color=(34, 35, 36))

    with pytest.raises(training.CaptureStorageError) as exc_info:
        _create_capture(owner_user_id=7, color=(37, 38, 39))

    assert exc_info.value.status_code == 429
    assert exc_info.value.code == "capture_rate_limited"


def test_unknown_legacy_owner_blocks_new_capture_conservatively(capture_root: Path) -> None:
    day = capture_root / "2026-06-28"
    day.mkdir(parents=True)
    raw = base64.b64decode(_image_b64(), validate=True)
    image_path = day / "legacy.jpg"
    image_path.write_bytes(raw)
    (day / "20260628_0123456789abcdef0123456789abcdef.json").write_text(
        json.dumps(
            {
                "capture_id": "20260628_0123456789abcdef0123456789abcdef",
                "image": {"path": str(image_path), "bytes": len(raw)},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(training.CaptureStorageError) as exc_info:
        _create_capture(owner_user_id=7, color=(90, 80, 70))

    assert exc_info.value.code == "capture_legacy_owner_unknown"


def test_mismatched_legacy_metadata_path_fails_closed(capture_root: Path) -> None:
    day = capture_root / "2026-06-28"
    day.mkdir(parents=True)
    raw = base64.b64decode(_image_b64(), validate=True)
    image_path = day / "legacy.jpg"
    image_path.write_bytes(raw)
    (day / "wrong-name.json").write_text(
        json.dumps(
            {
                "capture_id": "20260628_0123456789abcdef0123456789abcdef",
                "created_at": "2026-06-28T00:00:00+00:00",
                "employee": {"id": 7},
                "image": {"path": str(image_path), "bytes": len(raw)},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(training.CaptureStorageError) as exc_info:
        _create_capture(owner_user_id=7, color=(70, 80, 90))

    assert exc_info.value.code == "capture_legacy_owner_unknown"


def test_oversized_legacy_metadata_is_rejected_before_json_parse(
    capture_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    day = capture_root / "2026-06-28"
    day.mkdir(parents=True)
    metadata = day / "20260628_0123456789abcdef0123456789abcdef.json"
    metadata.write_bytes(b"{" + b" " * training.MAX_CAPTURE_METADATA_BYTES + b"}")
    loads = MagicMock(side_effect=AssertionError("oversized metadata must not be parsed"))
    monkeypatch.setattr(training.json, "loads", loads)

    with pytest.raises(training.CaptureStorageError) as exc_info:
        _create_capture(owner_user_id=7, color=(70, 71, 72))

    assert exc_info.value.code == "capture_legacy_owner_unknown"
    loads.assert_not_called()


def test_metadata_failure_cleans_all_partial_capture_files(
    capture_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_write = training._write_json_atomic

    def fail_capture_metadata(path: Path, payload: dict) -> None:
        if payload.get("capture_id"):
            raise OSError("simulated metadata failure")
        original_write(path, payload)

    monkeypatch.setattr(training, "_write_json_atomic", fail_capture_metadata)

    with pytest.raises(training.CaptureStorageError) as exc_info:
        training.create_scan_capture(
            _image_b64(),
            source="test",
            employee={"id": 7},
            owner_user_id=7,
        )

    assert exc_info.value.code == "capture_storage_failed"
    assert list(capture_root.rglob("*.jpg")) == []
    assert not any(path.name.endswith(".tmp") for path in capture_root.rglob("*"))


def test_retention_prunes_only_unprotected_old_capture(capture_root: Path) -> None:
    now = datetime(2026, 6, 28, tzinfo=timezone.utc)
    old = (now - timedelta(days=60)).isoformat()
    confirmed = _create_capture(owner_user_id=7, color=(41, 1, 1))
    eligible = _create_capture(owner_user_id=7, color=(42, 1, 1))
    indexed = _create_capture(owner_user_id=7, color=(43, 1, 1))
    curated = _create_capture(owner_user_id=7, color=(44, 1, 1))
    disposable = _create_capture(owner_user_id=7, color=(45, 1, 1))

    paths = {
        confirmed: _rewrite_capture(
            confirmed,
            lambda payload: payload.update(
                {"created_at": old, "confirmed_label": {"card_name": "Pikachu"}}
            ),
        ),
        eligible: _rewrite_capture(
            eligible,
            lambda payload: payload.update(
                {"created_at": old, "training": {"eligible": True, "indexed_at": None}}
            ),
        ),
        indexed: _rewrite_capture(
            indexed,
            lambda payload: payload.update(
                {"created_at": old, "training": {"eligible": False, "indexed_at": old}}
            ),
        ),
        curated: _rewrite_capture(
            curated,
            lambda payload: payload.update({"created_at": old, "curated": True}),
        ),
        disposable: _rewrite_capture(
            disposable,
            lambda payload: payload.update({"created_at": old}),
        ),
    }

    deleted = _prune_retention(
        now=now,
        max_age_seconds=1,
        max_unprotected=100,
    )

    assert deleted == 1
    assert not paths[disposable].exists()
    assert all(paths[capture_id].exists() for capture_id in (confirmed, eligible, indexed, curated))


def test_retention_count_bound_deletes_oldest_unprotected_captures(
    capture_root: Path,
) -> None:
    now = datetime(2026, 6, 28, tzinfo=timezone.utc)
    capture_ids = [
        _create_capture(owner_user_id=7, color=(value, 2, 2))
        for value in (51, 52, 53)
    ]
    paths: list[Path] = []
    for index, capture_id in enumerate(capture_ids):
        paths.append(
            _rewrite_capture(
                capture_id,
                lambda payload, index=index: payload.update(
                    {"created_at": (now - timedelta(minutes=3 - index)).isoformat()}
                ),
            )
        )

    deleted = _prune_retention(
        now=now,
        max_age_seconds=10_000,
        max_unprotected=1,
    )

    assert deleted == 2
    assert not paths[0].exists()
    assert not paths[1].exists()
    assert paths[2].exists()


def test_capture_write_applies_unprotected_retention_bound(
    capture_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(training, "MAX_UNPROTECTED_CAPTURES", 1)
    first = _create_capture(owner_user_id=7, color=(61, 2, 2))
    first_path, _payload = training._load_metadata(first)
    assert first_path is not None

    second = _create_capture(owner_user_id=7, color=(62, 2, 2))
    second_path, _payload = training._load_metadata(second)

    assert not first_path.exists()
    assert second_path is not None and second_path.exists()


def test_retention_never_traverses_symlink_outside_capture_root(
    capture_root: Path,
    tmp_path: Path,
) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    sentinel = outside / "sentinel.txt"
    sentinel.write_text("keep", encoding="utf-8")
    link = capture_root / "2020-01-01"
    capture_root.mkdir(parents=True, exist_ok=True)
    try:
        link.symlink_to(outside, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"symlink creation is unavailable: {exc}")

    assert _prune_retention(
        now=datetime(2026, 6, 28, tzinfo=timezone.utc),
        max_age_seconds=0,
        max_unprotected=0,
    ) == 0
    assert sentinel.read_text(encoding="utf-8") == "keep"


def test_retention_never_deletes_image_path_outside_capture_root(
    capture_root: Path,
    tmp_path: Path,
) -> None:
    outside = tmp_path / "outside.jpg"
    outside.write_bytes(base64.b64decode(_image_b64(), validate=True))
    metadata = capture_root / "2026-06-28" / f"{training._make_capture_id(datetime(2026, 6, 28, tzinfo=timezone.utc))}.json"
    metadata.parent.mkdir(parents=True, exist_ok=True)
    capture_id = metadata.stem
    metadata.write_text(
        json.dumps(
            {
                "capture_id": capture_id,
                "created_at": "2020-01-01T00:00:00+00:00",
                "employee": {"id": 7},
                "image": {"path": str(outside), "bytes": outside.stat().st_size},
                "training": {"eligible": False, "indexed_at": None},
            }
        ),
        encoding="utf-8",
    )

    assert _prune_retention(
        now=datetime(2026, 6, 28, tzinfo=timezone.utc),
        max_age_seconds=0,
        max_unprotected=0,
    ) == 0
    assert outside.exists()


def test_retention_fails_closed_when_unknown_legacy_artifact_exists(
    capture_root: Path,
) -> None:
    capture_id = _create_capture(owner_user_id=7)
    capture_path = _rewrite_capture(
        capture_id,
        lambda payload: payload.update({"created_at": "2020-01-01T00:00:00+00:00"}),
    )
    unknown = capture_root / "2020-01-01" / "unknown.json"
    unknown.parent.mkdir(parents=True, exist_ok=True)
    unknown.write_text("{}", encoding="utf-8")

    assert _prune_retention(
        now=datetime(2026, 6, 28, tzinfo=timezone.utc),
        max_age_seconds=0,
        max_unprotected=0,
    ) == 0
    assert capture_path.exists()


def test_retention_never_treats_internal_control_file_as_legacy_capture_image(
    capture_root: Path,
) -> None:
    capture_id = "20260628_0123456789abcdef0123456789abcdef"
    day = capture_root / "2026-06-28"
    day.mkdir(parents=True)
    rate_state = capture_root / ".capture-rate.json"
    rate_state.write_text('{"version":1,"users":{}}', encoding="utf-8")
    metadata = day / f"{capture_id}.json"
    metadata.write_text(
        json.dumps(
            {
                "capture_id": capture_id,
                "created_at": "2020-01-01T00:00:00+00:00",
                "employee": {"id": 7},
                "image": {
                    "path": str(rate_state),
                    "bytes": rate_state.stat().st_size,
                },
                "training": {"eligible": False, "indexed_at": None},
            }
        ),
        encoding="utf-8",
    )

    with training._capture_storage_lock(capture_root):
        records, _count, _bytes, unknown = training._scan_capture_records_unlocked(
            capture_root.resolve()
        )
    deleted = _prune_retention(
        now=datetime(2026, 6, 28, tzinfo=timezone.utc),
        max_age_seconds=0,
        max_unprotected=0,
    )

    assert records == []
    assert unknown is True
    assert deleted == 0
    assert metadata.exists()
    assert rate_state.exists()


def test_canonical_flat_legacy_capture_remains_eligible_for_accounting(
    capture_root: Path,
) -> None:
    capture_id = "20260628_fedcba9876543210fedcba9876543210"
    day = capture_root / "2026-06-28"
    day.mkdir(parents=True)
    image = day / f"{capture_id}.jpg"
    image.write_bytes(base64.b64decode(_image_b64(), validate=True))
    metadata = day / f"{capture_id}.json"
    metadata.write_text(
        json.dumps(
            {
                "capture_id": capture_id,
                "created_at": "2026-06-28T00:00:00+00:00",
                "employee": {"id": 7},
                "image": {"path": str(image), "bytes": image.stat().st_size},
                "training": {"eligible": False, "indexed_at": None},
            }
        ),
        encoding="utf-8",
    )

    with training._capture_storage_lock(capture_root):
        records, count, total_bytes, unknown = training._scan_capture_records_unlocked(
            capture_root.resolve()
        )

    assert [record.capture_id for record in records] == [capture_id]
    assert count == 1
    assert total_bytes == image.stat().st_size + metadata.stat().st_size
    assert unknown is False


def test_training_iterator_excludes_confirmed_image_outside_capture_root(
    capture_root: Path,
    tmp_path: Path,
) -> None:
    outside = tmp_path / "outside-training.jpg"
    outside.write_bytes(base64.b64decode(_image_b64(), validate=True))
    capture_id = "20260628_0123456789abcdef0123456789abcdef"
    metadata = capture_root / "2026-06-28" / f"{capture_id}.json"
    metadata.parent.mkdir(parents=True, exist_ok=True)
    metadata.write_text(
        json.dumps(
            {
                "capture_id": capture_id,
                "created_at": "2026-06-28T00:00:00+00:00",
                "employee": {"id": 7},
                "confirmed_label": {"card_name": "Pikachu"},
                "image": {"path": str(outside), "bytes": outside.stat().st_size},
                "training": {"eligible": True, "indexed_at": None},
            }
        ),
        encoding="utf-8",
    )

    assert training.iter_confirmed_captures() == []


@pytest.mark.parametrize("route_name", ["degen_eye_v2_scan", "degen_eye_v2_scan_init"])
def test_both_v2_capture_routes_pass_authenticated_numeric_owner(route_name: str) -> None:
    route = getattr(inventory_routes, route_name)
    create = MagicMock(return_value=None)
    user = SimpleNamespace(id=77, username="employee", display_name="Employee", role="employee")

    with patch.object(inventory_routes, "_require_employee_permission", return_value=None), patch.object(
        inventory_routes,
        "_current_user",
        return_value=user,
    ), patch.object(
        inventory_routes,
        "create_scan_capture",
        create,
    ), patch.object(
        inventory_routes,
        "run_v2_pipeline",
        new=AsyncMock(return_value={"status": "MATCHED", "debug": {}}),
    ), patch.object(
        inventory_routes,
        "_write_v2_pending_scan",
    ):
        response = asyncio.run(
            route(_JsonRequest({"image": _image_b64(), "category_id": "3"}), MagicMock())
        )

    assert response.status_code == 200
    assert create.call_args.kwargs["owner_user_id"] == 77


@pytest.mark.parametrize("route_name", ["degen_eye_v2_scan", "degen_eye_v2_scan_init"])
def test_both_v2_routes_surface_capture_rate_limit(route_name: str) -> None:
    route = getattr(inventory_routes, route_name)
    user = SimpleNamespace(id=77, username="employee", display_name="Employee", role="employee")
    pipeline = AsyncMock(return_value={"status": "MATCHED", "debug": {}})
    write_pending = MagicMock()
    error = training.CaptureStorageError(
        "Too many scan captures. Try again shortly.",
        status_code=429,
        code="capture_rate_limited",
    )

    with patch.object(inventory_routes, "_require_employee_permission", return_value=None), patch.object(
        inventory_routes,
        "_current_user",
        return_value=user,
    ), patch.object(
        inventory_routes,
        "create_scan_capture",
        side_effect=error,
    ), patch.object(
        inventory_routes,
        "run_v2_pipeline",
        new=pipeline,
    ), patch.object(
        inventory_routes,
        "_write_v2_pending_scan",
        write_pending,
    ):
        try:
            response = asyncio.run(
                route(_JsonRequest({"image": _image_b64(), "category_id": "3"}), MagicMock())
            )
        except training.CaptureStorageError:
            pytest.fail("capture storage errors must be converted to a stable HTTP response")

    assert response.status_code == 429
    assert json.loads(response.body) == {
        "error": "Too many scan captures. Try again shortly.",
        "code": "capture_rate_limited",
    }
    pipeline.assert_not_awaited()
    write_pending.assert_not_called()


def test_metadata_updates_use_same_cross_process_storage_lock(
    capture_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capture_id = _create_capture(owner_user_id=7)
    entered = False
    real_lock = training._capture_storage_lock

    @contextmanager
    def observed_lock(*args, **kwargs):
        nonlocal entered
        entered = True
        with real_lock(*args, **kwargs) as root:
            yield root

    monkeypatch.setattr(training, "_capture_storage_lock", observed_lock)

    assert training.attach_prediction(capture_id, {"status": "MATCHED"}) is True
    assert entered is True


def test_metadata_updates_cannot_expand_capture_past_metadata_cap(
    capture_root: Path,
) -> None:
    capture_id = _create_capture(owner_user_id=7)
    path, _payload = training._load_metadata(capture_id)
    assert path is not None
    before = path.read_bytes()

    updated = training.attach_confirmed_label(
        capture_id,
        {
            "card_name": "Pikachu",
            "set_name": "Base Set",
            "notes": "x" * (training.MAX_CAPTURE_METADATA_BYTES + 1),
        },
        expected_employee_id=7,
    )

    assert updated is False
    assert path.read_bytes() == before


def test_metadata_updates_respect_existing_owner_byte_quota(
    capture_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capture_id = _create_capture(owner_user_id=7)
    path, payload = training._load_metadata(capture_id)
    assert path is not None and payload is not None
    image_path = Path(payload["image"]["path"])
    before = path.read_bytes()
    monkeypatch.setattr(
        training,
        "MAX_CAPTURE_BYTES_PER_USER",
        path.stat().st_size + image_path.stat().st_size,
    )

    assert training.attach_prediction(capture_id, {"status": "MATCHED"}) is False
    assert path.read_bytes() == before


def test_cross_process_lock_closes_global_quota_race(
    capture_root: Path,
    tmp_path: Path,
) -> None:
    ready = tmp_path / "child-ready"
    encoded = _image_b64((91, 92, 93))
    script = r"""
import sys
from pathlib import Path
from app.inventory import degen_eye_v2_training as training

root = Path(sys.argv[1])
ready = Path(sys.argv[2])
encoded = sys.argv[3]
training.capture_root = lambda: root
training._capture_enabled = lambda: True
training.MAX_CAPTURES_GLOBAL = 1
training.MIN_CAPTURE_FREE_BYTES = 0
training.CAPTURE_REQUESTS_PER_USER = 100
ready.write_text("ready", encoding="utf-8")
try:
    capture_id = training.create_scan_capture(
        encoded,
        source="child",
        employee={"id": 8},
        owner_user_id=8,
    )
    print("created:" + str(capture_id), flush=True)
except training.CaptureStorageError as exc:
    print("error:" + exc.code, flush=True)
"""

    with training._capture_storage_lock(capture_root):
        child = subprocess.Popen(
            [sys.executable, "-c", script, str(capture_root), str(ready), encoded],
            cwd=str(Path.cwd()),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        deadline = time.monotonic() + 5
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert ready.exists()
        time.sleep(0.1)
        assert child.poll() is None

        day = capture_root / "2026-06-28"
        day.mkdir(parents=True, exist_ok=True)
        raw = base64.b64decode(_image_b64((81, 82, 83)), validate=True)
        existing_capture_id = "20260628_0123456789abcdef0123456789abcdef"
        image_path = day / f"{existing_capture_id}.jpg"
        image_path.write_bytes(raw)
        (day / f"{existing_capture_id}.json").write_text(
            json.dumps(
                {
                    "capture_id": existing_capture_id,
                    "created_at": "2026-06-28T00:00:00+00:00",
                    "employee": {"id": 7},
                    "image": {"path": str(image_path), "bytes": len(raw)},
                    "training": {"eligible": False, "indexed_at": None},
                }
            ),
            encoding="utf-8",
        )

    stdout, stderr = child.communicate(timeout=10)
    assert child.returncode == 0, stderr
    assert "error:capture_global_count_quota" in stdout
