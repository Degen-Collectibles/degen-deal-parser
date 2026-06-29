"""Security regression coverage for Degen Eye v2 capture metadata.

Capture identifiers cross an employee-controlled HTTP boundary.  These tests
therefore exercise the storage helpers directly with hostile identifiers as
well as canonical server-generated identifiers.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.inventory import degen_eye_v2_training as training
from app.inventory import routes as inventory_routes


CAPTURE_ID = "20260628_0123456789abcdef0123456789abcdef"
OTHER_CAPTURE_ID = "20260628_fedcba9876543210fedcba9876543210"


def _capture_path(root: Path, capture_id: str = CAPTURE_ID) -> Path:
    path = root / "2026-06-28" / f"{capture_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _write_capture(
    root: Path,
    *,
    capture_id: str = CAPTURE_ID,
    payload_capture_id: object = CAPTURE_ID,
    employee_id: object = 7,
) -> Path:
    path = _capture_path(root, capture_id)
    payload = {
        "capture_id": payload_capture_id,
        "employee": {"id": employee_id},
        "prediction": None,
        "confirmed_label": None,
        "training": {"eligible": False, "indexed_at": None},
    }
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    return path


@pytest.fixture
def capture_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "captures"
    root.mkdir()
    monkeypatch.setattr(training, "capture_root", lambda: root)
    return root


@pytest.mark.parametrize(
    "capture_id",
    [
        "*",
        "../outside",
        "20260628/0123456789abcdef0123456789abcdef",
        r"20260628\0123456789abcdef0123456789abcdef",
        "/tmp/capture",
        r"C:\capture",
        r"\\server\share\capture",
        f" {CAPTURE_ID}",
        f"{CAPTURE_ID} ",
        "20260628_0123456789ABCDEF0123456789ABCDEF",
        "20260628_01234567",
        "20260230_0123456789abcdef0123456789abcdef",
        None,
        123,
        True,
        {"capture_id": CAPTURE_ID},
    ],
)
def test_hostile_or_noncanonical_ids_cannot_update_metadata(
    capture_root: Path,
    capture_id: object,
) -> None:
    canonical = _write_capture(capture_root)
    before = canonical.read_bytes()

    assert training.attach_prediction(capture_id, {"status": "ready"}) is False
    assert canonical.read_bytes() == before


def test_wildcard_cannot_select_an_existing_capture(capture_root: Path) -> None:
    canonical = _write_capture(capture_root)
    before = canonical.read_bytes()

    assert training.attach_prediction("*", {"status": "attacker"}) is False
    assert canonical.read_bytes() == before


def test_parent_traversal_cannot_select_a_capture(capture_root: Path) -> None:
    # The old recursive glob could interpret ``..`` after a matched directory.
    (capture_root / "search-anchor").mkdir()
    outside_name = capture_root / "outside.json"
    outside_name.write_text(
        json.dumps({"capture_id": "../outside", "prediction": None}),
        encoding="utf-8",
    )
    before = outside_name.read_bytes()

    assert training.attach_prediction("../outside", {"status": "attacker"}) is False
    assert outside_name.read_bytes() == before


def test_symlink_escape_is_rejected(capture_root: Path, tmp_path: Path) -> None:
    outside = tmp_path / "outside.json"
    outside.write_text(
        json.dumps({"capture_id": CAPTURE_ID, "employee": {"id": 7}}),
        encoding="utf-8",
    )
    link = _capture_path(capture_root)
    try:
        link.symlink_to(outside)
    except OSError as exc:
        pytest.skip(f"symlink creation is unavailable: {exc}")
    before = outside.read_bytes()

    assert training.attach_prediction(CAPTURE_ID, {"status": "attacker"}) is False
    assert outside.read_bytes() == before


def test_json_root_must_be_an_object(capture_root: Path) -> None:
    path = _capture_path(capture_root)
    path.write_text(json.dumps([{"capture_id": CAPTURE_ID}]), encoding="utf-8")
    before = path.read_bytes()

    assert training.attach_prediction(CAPTURE_ID, {"status": "ready"}) is False
    assert path.read_bytes() == before


def test_payload_capture_id_must_match_filename(capture_root: Path) -> None:
    path = _write_capture(capture_root, payload_capture_id=OTHER_CAPTURE_ID)
    before = path.read_bytes()

    assert training.attach_prediction(CAPTURE_ID, {"status": "ready"}) is False
    assert path.read_bytes() == before


def test_missing_canonical_capture_is_rejected(capture_root: Path) -> None:
    assert training.attach_prediction(CAPTURE_ID, {"status": "ready"}) is False
    assert training.capture_belongs_to_employee(CAPTURE_ID, 7) is False


@pytest.mark.parametrize(
    "value",
    [
        " ",
        "*",
        "../outside",
        "20260628/0123456789abcdef0123456789abcdef",
        r"20260628\0123456789abcdef0123456789abcdef",
        "/tmp/capture",
        r"C:\capture",
        r"\\server\share\capture",
        "20260628_0123456789ABCDEF0123456789ABCDEF",
        "20260628_01234567",
        "20260230_0123456789abcdef0123456789abcdef",
        123,
        True,
        [],
    ],
)
def test_route_extractor_rejects_noncanonical_nonempty_values(value: object) -> None:
    assert inventory_routes._extract_batch_capture_id({"capture_id": value}) == (False, None)


def test_route_extractor_preserves_exact_no_capture_compatibility() -> None:
    assert inventory_routes._extract_batch_capture_id({}) == (True, None)
    assert inventory_routes._extract_batch_capture_id({"capture_id": None}) == (True, None)
    assert inventory_routes._extract_batch_capture_id({"_v2_capture_id": ""}) == (True, None)


def test_route_extractor_rejects_conflicting_capture_ids() -> None:
    assert inventory_routes._extract_batch_capture_id({
        "_v2_capture_id": CAPTURE_ID,
        "capture_id": OTHER_CAPTURE_ID,
    }) == (False, None)


def test_canonical_capture_operations_succeed(capture_root: Path) -> None:
    path = _write_capture(capture_root)

    assert training.attach_prediction(CAPTURE_ID, {"status": "ready"}) is True
    assert training.attach_confirmed_label(
        CAPTURE_ID,
        {"card_name": "Pikachu", "set_name": "Base Set"},
        expected_employee_id=7,
        inventory_item_id=42,
        confirmed_by={"id": 7, "username": "employee"},
    ) is True
    assert training.mark_training_indexed(
        CAPTURE_ID,
        index_path="phash.sqlite",
        phash_source="employee_capture",
    ) is True

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["capture_id"] == CAPTURE_ID
    assert payload["prediction"]["status"] == "ready"
    assert payload["confirmed_label"]["card_name"] == "Pikachu"
    assert payload["inventory_item_id"] == 42
    assert payload["training"]["indexed_at"]


@pytest.mark.parametrize("owner", [True, "7", None, 8])
def test_confirmation_rejects_non_numeric_missing_or_wrong_owner(
    capture_root: Path,
    owner: object,
) -> None:
    path = _write_capture(capture_root, employee_id=owner)
    before = path.read_bytes()

    assert training.capture_belongs_to_employee(CAPTURE_ID, 7) is False
    assert training.attach_confirmed_label(
        CAPTURE_ID,
        {"card_name": "Pikachu", "set_name": "Base Set"},
        expected_employee_id=7,
        inventory_item_id=42,
    ) is False
    assert path.read_bytes() == before


@pytest.mark.parametrize("expected_employee_id", [True, "7", None])
def test_confirmation_rejects_non_integer_expected_owner(
    capture_root: Path,
    expected_employee_id: object,
) -> None:
    path = _write_capture(capture_root, employee_id=7)
    before = path.read_bytes()

    assert training.capture_belongs_to_employee(CAPTURE_ID, expected_employee_id) is False
    assert training.attach_confirmed_label(
        CAPTURE_ID,
        {"card_name": "Pikachu", "set_name": "Base Set"},
        expected_employee_id=expected_employee_id,
        inventory_item_id=42,
    ) is False
    assert path.read_bytes() == before
