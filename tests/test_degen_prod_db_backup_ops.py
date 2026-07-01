from __future__ import annotations

import ast
import copy
import dataclasses
import importlib.util
import inspect
import json
import os
import stat
import subprocess
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
OPS_HELPER = ROOT / "deploy" / "linux" / "degen-prod-db-backup-ops.py"
HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64
TARGETS = (
    "/usr/local/sbin/degen-prod-db-backup",
    "/usr/local/sbin/degen-prod-db-retention",
    "/usr/local/sbin/degen-prod-db-backup-env",
    "/usr/local/sbin/degen-prod-db-backup-ops",
    "/etc/systemd/system/degen-prod-db-backup.service",
    "/etc/systemd/system/degen-prod-db-backup.timer",
    "/etc/degen/prod-db-backup.env",
)
SOURCE_ASSETS = (
    "deploy/linux/degen-prod-db-backup.sh",
    "deploy/linux/degen-prod-db-retention.py",
    "deploy/linux/degen-prod-db-backup-env.py",
    "deploy/linux/degen-prod-db-backup-ops.py",
    "deploy/systemd/degen-prod-db-backup.service",
    "deploy/systemd/degen-prod-db-backup.timer",
    "deploy/systemd/degen-prod-db-backup.env.example",
)


def load_ops_helper():
    spec = importlib.util.spec_from_file_location("degen_prod_db_backup_ops", OPS_HELPER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def source_verified_state(operation_dir: Path) -> dict[str, object]:
    return {
        "schema_version": 1,
        "operation_id": operation_dir.name,
        "operation_dir": str(operation_dir),
        "phase": "source_verified",
        "phase_history": [
            {"phase": "source_verified", "epoch": 1_750_000_000, "evidence_sha256": HASH_A}
        ],
        "reviewed_source": {
            "commit": "1" * 40,
            "archive_sha256": HASH_B,
            "manifest_sha256": HASH_C,
            "asset_hashes": {asset: HASH_A for asset in SOURCE_ASSETS},
        },
        "effective_config": None,
        "host_stage": None,
        "snapshot": None,
        "prior_runtime": None,
        "install": None,
        "rclone_evidence_groups": [],
        "probe": None,
        "dry_run": None,
        "policy": None,
        "observation": None,
        "active_transaction": None,
        "failure": None,
        "secondary_errors": [],
        "recovery": None,
    }


def private_operation_dir(tmp_path: Path) -> tuple[Path, int]:
    operation_dir = tmp_path / "20260630T235959Z"
    operation_dir.mkdir(mode=0o700)
    operation_dir.chmod(0o700)
    return operation_dir, operation_dir.stat().st_uid


def write_state_file(operation_dir: Path, state: dict[str, object]) -> Path:
    state_file = operation_dir / "operation-state.json"
    state_file.write_text(json.dumps(state), encoding="utf-8")
    state_file.chmod(0o600)
    return state_file


def append_phase(state: dict[str, object], phase: str, epoch: int) -> None:
    state["phase"] = phase
    history = state["phase_history"]
    assert isinstance(history, list)
    history.append({"phase": phase, "epoch": epoch, "evidence_sha256": HASH_B})


def observed_state(operation_dir: Path) -> dict[str, object]:
    state = source_verified_state(operation_dir)
    append_phase(state, "staging_prepared", 1_750_000_010)
    state["effective_config"] = {"BACKUP_DIR": "/opt/degen/backups/db"}
    state["host_stage"] = {
        "manifest_sha256": HASH_A,
        "asset_hashes": {asset: HASH_A for asset in SOURCE_ASSETS},
        "environment_sha256": HASH_C,
    }
    append_phase(state, "snapshotted", 1_750_000_020)
    state["snapshot"] = {
        "manifest_sha256": HASH_B,
        "targets": {
            target: {
                "present": True,
                "sha256": HASH_A,
                "mode": 0o600,
                "uid": 0,
                "gid": 0,
            }
            for target in TARGETS
        },
        "rclone_audit": {
            "path": "/etc/degen/rclone.conf",
            "sha256": HASH_A,
            "inode": 10,
            "uid": 0,
            "gid": 0,
            "mode": 0o600,
            "size": 100,
            "mtime_ns": 1_750_000_000_000_000_000,
        },
    }
    state["prior_runtime"] = {
        "timer_enabled": True,
        "timer_active": True,
        "pids": {"web": 100, "postgres": 200},
        "preinstall_trigger_epoch": None,
    }
    append_phase(state, "installing", 1_750_000_030)
    state["install"] = {
        "next_target_index": 7,
        "current_target": None,
        "previous_sha256": None,
        "intended_sha256": None,
        "installed_hashes": {},
        "started_epoch": 1_750_000_030,
        "completed_epoch": None,
    }
    append_phase(state, "installed", 1_750_000_040)
    state["install"]["installed_hashes"] = {target: HASH_A for target in TARGETS}
    state["install"]["installed_hashes"]["/etc/degen/prod-db-backup.env"] = HASH_C
    state["install"]["completed_epoch"] = 1_750_000_040
    append_phase(state, "probing", 1_750_000_050)
    append_phase(state, "probed", 1_750_000_060)
    state["probe"] = {
        "prefix": "probe-20260630T235959Z/",
        "owned_names": ["probe.dump", "probe.dump.sha256"],
        "cleanup_proven": True,
        "evidence_sha256": HASH_A,
    }
    append_phase(state, "dry_run_recording", 1_750_000_070)
    append_phase(state, "dry_run_recorded", 1_750_000_080)
    state["dry_run"] = {
        "inventory_names": ["a.dump", "a.dump.sha256"],
        "casefold_names": ["a.dump", "a.dump.sha256"],
        "keep_names": ["a.dump", "a.dump.sha256"],
        "protected_names": [],
        "delete_names": [],
        "candidate_sha256": HASH_A,
        "evidence_sha256": HASH_B,
    }
    append_phase(state, "policy_enabling", 1_750_000_090)
    append_phase(state, "policy_enabled", 1_750_000_100)
    state["policy"] = {"environment_sha256": HASH_B, "enabled_epoch": 1_750_000_100}
    append_phase(state, "observing", 1_750_000_110)
    append_phase(state, "observed", 1_750_000_120)
    state["observation"] = {
        "run_epoch": 1_750_000_120,
        "journal_sha256": HASH_A,
        "local_sha256": HASH_B,
        "remote_sha256": HASH_C,
        "evidence_sha256": HASH_A,
    }
    return state


def schema_rich_state(operation_dir: Path) -> dict[str, object]:
    state = observed_state(operation_dir)
    state["rclone_evidence_groups"] = [
        {
            "group_id": "install",
            "purpose": "credential-refresh-audit",
            "before": {
                "sha256": HASH_A,
                "inode": 10,
                "uid": 0,
                "gid": 0,
                "mode": 0o600,
                "size": 100,
                "mtime_ns": 1_750_000_000_000_000_000,
            },
            "after": {
                "sha256": HASH_B,
                "inode": 10,
                "uid": 0,
                "gid": 0,
                "mode": 0o600,
                "size": 101,
                "mtime_ns": 1_750_000_001_000_000_000,
            },
            "evidence_sha256": HASH_C,
        }
    ]
    state["active_transaction"] = {
        "kind": "probe",
        "prior_stable_phase": "installed",
        "prior_timer_enabled": True,
        "prior_timer_active": True,
        "guard": {
            "timer_stopped": True,
            "service_inactive_verified": True,
            "legacy_lock_acquired": True,
            "runtime_lock_acquired": True,
            "locks_released": False,
            "timer_restored": False,
        },
        "started_epoch": 1_750_000_050,
        "policy_environment_sha256": None,
        "probe": {
            "prefix": "probe-20260630T235959Z/",
            "objects": [
                {
                    "name": "probe.dump",
                    "expected_sha256": HASH_A,
                    "expected_size": 64,
                    "created": True,
                    "verified": True,
                    "cleaned": False,
                }
            ],
        },
    }
    state["failure"] = {
        "phase": "probing",
        "primary_error": "remote probe failed",
        "epoch": 1_750_000_051,
        "evidence_sha256": HASH_A,
    }
    state["secondary_errors"] = [
        {
            "stage": "cleanup",
            "error": "cleanup failed",
            "epoch": 1_750_000_052,
            "evidence_sha256": HASH_B,
        }
    ]
    state["recovery"] = {
        "kind": "probe",
        "next_target_index": 0,
        "current_target": None,
        "previous_sha256": None,
        "intended_sha256": None,
        "started_epoch": 1_750_000_053,
        "completed_epoch": None,
        "evidence_sha256": HASH_C,
    }
    return state


NORMAL_PHASES = (
    "source_verified",
    "staging_prepared",
    "snapshotted",
    "installing",
    "installed",
    "probing",
    "probed",
    "dry_run_recording",
    "dry_run_recorded",
    "policy_enabling",
    "policy_enabled",
    "observing",
    "observed",
)


def active_transaction(kind: str, prior_phase: str, started_epoch: int) -> dict[str, object]:
    probe = None
    if kind == "probe":
        probe = {
            "prefix": "probe-20260630T235959Z/",
            "objects": [
                {
                    "name": "probe.dump",
                    "expected_sha256": HASH_A,
                    "expected_size": 64,
                    "created": False,
                    "verified": False,
                    "cleaned": False,
                }
            ],
        }
    return {
        "kind": kind,
        "prior_stable_phase": prior_phase,
        "prior_timer_enabled": True,
        "prior_timer_active": True,
        "guard": {
            "timer_stopped": False,
            "service_inactive_verified": False,
            "legacy_lock_acquired": False,
            "runtime_lock_acquired": False,
            "locks_released": False,
            "timer_restored": False,
        },
        "started_epoch": started_epoch,
        "policy_environment_sha256": HASH_B if kind == "policy" else None,
        "probe": probe,
    }


def state_at_phase(operation_dir: Path, phase: str) -> dict[str, object]:
    if phase not in NORMAL_PHASES:
        raise ValueError(phase)
    complete = observed_state(operation_dir)
    cutoff = NORMAL_PHASES.index(phase)
    state = copy.deepcopy(complete)
    state["phase"] = phase
    state["phase_history"] = copy.deepcopy(complete["phase_history"][: cutoff + 1])
    state["rclone_evidence_groups"] = []
    state["failure"] = None
    state["secondary_errors"] = []
    state["recovery"] = None
    state["active_transaction"] = None

    if cutoff < NORMAL_PHASES.index("staging_prepared"):
        state["effective_config"] = None
        state["host_stage"] = None
    if cutoff < NORMAL_PHASES.index("snapshotted"):
        state["snapshot"] = None
        state["prior_runtime"] = None
    if cutoff < NORMAL_PHASES.index("installing"):
        state["install"] = None
    elif cutoff < NORMAL_PHASES.index("installed"):
        state["install"] = {
            "next_target_index": 0,
            "current_target": TARGETS[0],
            "previous_sha256": HASH_A,
            "intended_sha256": HASH_A,
            "installed_hashes": {},
            "started_epoch": 1_750_000_030,
            "completed_epoch": None,
        }
    if cutoff < NORMAL_PHASES.index("probed"):
        state["probe"] = None
    if cutoff < NORMAL_PHASES.index("dry_run_recorded"):
        state["dry_run"] = None
    if cutoff < NORMAL_PHASES.index("policy_enabled"):
        state["policy"] = None
    if cutoff < NORMAL_PHASES.index("observed"):
        state["observation"] = None

    transactions = {
        "probing": ("probe", "installed", 1_750_000_050),
        "dry_run_recording": ("dry_run", "probed", 1_750_000_070),
        "policy_enabling": ("policy", "dry_run_recorded", 1_750_000_090),
        "observing": ("observe", "policy_enabled", 1_750_000_110),
    }
    if phase in transactions:
        state["active_transaction"] = active_transaction(*transactions[phase])
    return state


def recovery_receipt(
    kind: str,
    *,
    index: int = 0,
    current_target: str | None = None,
    previous_sha256: str | None = None,
    intended_sha256: str | None = None,
    completed_epoch: int | None = None,
    started_epoch: int = 1_750_000_041,
) -> dict[str, object]:
    return {
        "kind": kind,
        "next_target_index": index,
        "current_target": current_target,
        "previous_sha256": previous_sha256,
        "intended_sha256": intended_sha256,
        "started_epoch": started_epoch,
        "completed_epoch": completed_epoch,
        "evidence_sha256": HASH_C,
    }


def install_recovery_state(operation_dir: Path, phase: str) -> dict[str, object]:
    state = state_at_phase(operation_dir, "installing")
    append_phase(state, "recovering", 1_750_000_041)
    state["recovery"] = recovery_receipt(
        "install",
        current_target=TARGETS[0],
        previous_sha256=HASH_A,
        intended_sha256=HASH_A,
    )
    state["failure"] = {
        "phase": "installing",
        "primary_error": "install failed",
        "epoch": 1_750_000_040,
        "evidence_sha256": HASH_B,
    }
    if phase == "recovering":
        return state
    if phase == "recovery_required":
        append_phase(state, "recovery_required", 1_750_000_042)
        return state
    if phase == "rolled_back":
        state["recovery"].update(
            {
                "next_target_index": 7,
                "current_target": None,
                "previous_sha256": None,
                "intended_sha256": None,
                "completed_epoch": 1_750_000_050,
            }
        )
        append_phase(state, "rolled_back", 1_750_000_050)
        return state
    raise ValueError(phase)


def manual_rollback_state(operation_dir: Path, phase: str) -> dict[str, object]:
    state = state_at_phase(operation_dir, "installed")
    append_phase(state, "manual_rollback", 1_750_000_050)
    state["recovery"] = recovery_receipt(
        "manual_rollback",
        current_target=TARGETS[0],
        previous_sha256=HASH_A,
        intended_sha256=HASH_A,
        started_epoch=1_750_000_050,
    )
    if phase == "manual_rollback":
        return state
    if phase == "recovery_required":
        append_phase(state, "recovery_required", 1_750_000_051)
        return state
    if phase == "rolled_back":
        state["recovery"].update(
            {
                "next_target_index": 7,
                "current_target": None,
                "previous_sha256": None,
                "intended_sha256": None,
                "completed_epoch": 1_750_000_060,
            }
        )
        append_phase(state, "rolled_back", 1_750_000_060)
        return state
    raise ValueError(phase)


def failed_transaction_state(operation_dir: Path, kind: str, phase: str) -> dict[str, object]:
    starts = {
        "probe": ("probing", "recovering_probe"),
        "guard_dry_run": ("dry_run_recording", "recovering_guard"),
        "guard_observe": ("observing", "recovering_guard"),
        "policy": ("policy_enabling", "recovering_policy"),
    }
    start_phase, recovery_phase = starts[kind]
    state = state_at_phase(operation_dir, start_phase)
    recovery_kind = "guard" if kind.startswith("guard_") else kind
    if recovery_kind == "policy":
        state["recovery"] = recovery_receipt(
            "policy",
            current_target="/etc/degen/prod-db-backup.env",
            previous_sha256=HASH_C,
            intended_sha256=HASH_C,
            started_epoch=1_750_000_121,
        )
    else:
        state["recovery"] = recovery_receipt(
            recovery_kind,
            started_epoch=1_750_000_121,
        )
    if recovery_kind == "policy":
        append_phase(state, "recovering_policy", 1_750_000_121)
        if phase == "recovering_policy":
            return state
        append_phase(state, "recovery_required", 1_750_000_122)
        return state
    append_phase(state, "recovery_required", 1_750_000_121)
    if phase == "recovery_required":
        return state
    append_phase(state, recovery_phase, 1_750_000_122)
    return state


def value_at_path(value: object, path: tuple[object, ...]) -> object:
    current = value
    for part in path:
        current = current[part]
    return current


def test_operations_context_interface_exists() -> None:
    module = load_ops_helper()

    assert module.OperationsContext is not None


def test_sources_parse_with_python_310_grammar() -> None:
    for path in (OPS_HELPER, Path(__file__)):
        ast.parse(path.read_text(encoding="utf-8"), filename=str(path), feature_version=(3, 10))


def test_operation_paths_interface_is_frozen_and_exact() -> None:
    module = load_ops_helper()

    assert [field.name for field in dataclasses.fields(module.OperationPaths)] == [
        "operation_dir",
        "source_archive",
        "source_dir",
        "snapshot_dir",
        "staged_dir",
        "state_file",
    ]
    paths = module.build_operation_paths(Path("/tmp/operation"))
    assert paths == module.OperationPaths(
        operation_dir=Path("/tmp/operation"),
        source_archive=Path("/tmp/operation/source.tar"),
        source_dir=Path("/tmp/operation/source"),
        snapshot_dir=Path("/tmp/operation/snapshot"),
        staged_dir=Path("/tmp/operation/staged"),
        state_file=Path("/tmp/operation/operation-state.json"),
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        paths.operation_dir = Path("/tmp/rebound")


def test_operations_context_interface_is_frozen_and_exact() -> None:
    module = load_ops_helper()
    fields = [field.name for field in dataclasses.fields(module.OperationsContext)]

    assert fields == [
        "operation_id",
        "paths",
        "effective_uid",
        "command_runner",
        "clock",
        "expected_commit",
        "expected_archive_sha256",
        "expected_manifest_sha256",
        "host_root",
    ]


def test_public_interface_names_exist() -> None:
    module = load_ops_helper()

    for name in (
        "validate_operation_dir",
        "load_operation_state",
        "atomic_write_operation_state",
        "build_operation_paths",
        "validate_operation_state",
        "validate_operation_state_for_context",
        "sanitize_error_text",
    ):
        assert callable(getattr(module, name))


def test_cli_exposes_only_show_state_subcommand() -> None:
    help_result = subprocess.run(
        [sys.executable, str(OPS_HELPER), "--help"],
        text=True,
        capture_output=True,
        check=False,
    )
    unknown_result = subprocess.run(
        [sys.executable, str(OPS_HELPER), "verify-source", "--operation-dir", "/tmp/nope"],
        text=True,
        capture_output=True,
        check=False,
    )

    assert help_result.returncode == 0
    assert "show-state" in help_result.stdout
    assert "verify-source" not in help_result.stdout
    assert unknown_result.returncode == 2
    assert unknown_result.stdout == ""


def test_cli_rejects_non_root_without_path_access(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    module = load_ops_helper()
    monkeypatch.setattr(module, "_effective_uid", lambda: 1000)

    result = module.main(["show-state", "--operation-dir", "/opt/degen/backups/config/20260630T235959Z"])

    captured = capsys.readouterr()
    assert result == 1
    assert captured.out == ""
    assert captured.err == "error: root privileges are required\n"


@pytest.mark.parametrize(
    "path",
    [
        "/opt/degen/backups/config/not-a-stamp",
        "/opt/degen/backups/config/20261340T256199Z",
        "/opt/degen/backups/config/20260630T235959Z/extra",
        "/tmp/20260630T235959Z",
        "opt/degen/backups/config/20260630T235959Z",
    ],
)
def test_cli_rejects_wrong_lexical_operation_path_before_open(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    path: str,
) -> None:
    module = load_ops_helper()
    monkeypatch.setattr(module, "_effective_uid", lambda: 0)

    result = module.main(["show-state", "--operation-dir", path])

    captured = capsys.readouterr()
    assert result == 1
    assert captured.out == ""
    assert captured.err == "error: invalid production operation directory\n"


def test_validate_operation_dir_accepts_private_owned_0700_directory(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)

    module.validate_operation_dir(operation_dir, effective_uid=uid)


@pytest.mark.parametrize("mode", [0o600, 0o750, 0o777])
@pytest.mark.skipif(os.name != "posix", reason="Windows does not preserve POSIX directory modes")
def test_validate_operation_dir_rejects_wrong_mode(tmp_path: Path, mode: int) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    operation_dir.chmod(mode)

    with pytest.raises(module.OperationStateError, match="mode 0700"):
        module.validate_operation_dir(operation_dir, effective_uid=uid)


def test_validate_operation_dir_rejects_wrong_owner(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)

    with pytest.raises(module.OperationStateError, match="effective UID"):
        module.validate_operation_dir(operation_dir, effective_uid=uid + 1)


def test_validate_operation_dir_rejects_relative_path(tmp_path: Path) -> None:
    module = load_ops_helper()

    with pytest.raises(module.OperationStateError, match="absolute"):
        module.validate_operation_dir(Path("relative/path"), effective_uid=0)


def test_validate_operation_dir_rejects_non_directory(tmp_path: Path) -> None:
    module = load_ops_helper()
    file_path = tmp_path / "not-a-directory"
    file_path.write_text("x", encoding="utf-8")

    with pytest.raises(module.OperationStateError, match="directory"):
        module.validate_operation_dir(file_path, effective_uid=file_path.stat().st_uid)


def test_validate_operation_dir_rejects_symlink_component(tmp_path: Path) -> None:
    module = load_ops_helper()
    real_parent = tmp_path / "real"
    real_parent.mkdir()
    operation_dir = real_parent / "20260630T235959Z"
    operation_dir.mkdir(mode=0o700)
    operation_dir.chmod(0o700)
    linked_parent = tmp_path / "linked"
    try:
        linked_parent.symlink_to(real_parent, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"directory symlinks unavailable: {exc}")

    with pytest.raises(module.OperationStateError, match="symlink"):
        module.validate_operation_dir(
            linked_parent / operation_dir.name,
            effective_uid=operation_dir.stat().st_uid,
        )


def test_load_operation_state_reads_owned_regular_0600_file(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    expected = source_verified_state(operation_dir)
    state_file = write_state_file(operation_dir, expected)

    assert module.load_operation_state(state_file, effective_uid=uid) == expected


@pytest.mark.parametrize("mode", [0o400, 0o640, 0o666])
@pytest.mark.skipif(os.name != "posix", reason="Windows does not preserve exact POSIX file modes")
def test_load_operation_state_rejects_wrong_mode(tmp_path: Path, mode: int) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    state_file = write_state_file(operation_dir, source_verified_state(operation_dir))
    state_file.chmod(mode)

    with pytest.raises(module.OperationStateError, match="mode 0600"):
        module.load_operation_state(state_file, effective_uid=uid)


def test_load_operation_state_rejects_wrong_owner(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    state_file = write_state_file(operation_dir, source_verified_state(operation_dir))

    with pytest.raises(module.OperationStateError, match="effective UID"):
        module.load_operation_state(state_file, effective_uid=uid + 1)


def test_load_operation_state_rejects_hard_link(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    state_file = write_state_file(operation_dir, source_verified_state(operation_dir))
    os.link(state_file, tmp_path / "alias.json")

    with pytest.raises(module.OperationStateError, match="single link"):
        module.load_operation_state(state_file, effective_uid=uid)


def test_load_operation_state_rejects_symlink(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    target = tmp_path / "target.json"
    target.write_text("{}", encoding="utf-8")
    target.chmod(0o600)
    state_file = operation_dir / "operation-state.json"
    try:
        state_file.symlink_to(target)
    except OSError as exc:
        pytest.skip(f"file symlinks unavailable: {exc}")

    with pytest.raises(module.OperationStateError, match="symlink"):
        module.load_operation_state(state_file, effective_uid=uid)


def test_load_operation_state_requires_fixed_basename(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    wrong = operation_dir / "other.json"
    wrong.write_text("{}", encoding="utf-8")
    wrong.chmod(0o600)

    with pytest.raises(module.OperationStateError, match="operation-state.json"):
        module.load_operation_state(wrong, effective_uid=uid)


def test_validate_operation_state_accepts_minimal_and_complete_valid_schemas(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)

    module.validate_operation_state(source_verified_state(operation_dir), operation_dir)
    module.validate_operation_state(observed_state(operation_dir), operation_dir)


SCHEMA_OBJECT_PATHS = (
    (),
    ("phase_history", 0),
    ("reviewed_source",),
    ("host_stage",),
    ("snapshot",),
    ("snapshot", "targets", TARGETS[0]),
    ("snapshot", "rclone_audit"),
    ("prior_runtime",),
    ("install",),
    ("rclone_evidence_groups", 0),
    ("rclone_evidence_groups", 0, "before"),
    ("rclone_evidence_groups", 0, "after"),
    ("probe",),
    ("dry_run",),
    ("policy",),
    ("observation",),
    ("active_transaction",),
    ("active_transaction", "guard"),
    ("active_transaction", "probe"),
    ("active_transaction", "probe", "objects", 0),
    ("failure",),
    ("secondary_errors", 0),
    ("recovery",),
)


@pytest.mark.parametrize("path", SCHEMA_OBJECT_PATHS)
@pytest.mark.parametrize("mutation", ["missing", "extra"])
def test_recursive_schema_rejects_missing_or_extra_keys(
    tmp_path: Path,
    path: tuple[object, ...],
    mutation: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    state = schema_rich_state(operation_dir)
    target = value_at_path(state, path)
    assert isinstance(target, dict)
    if mutation == "missing":
        del target[next(iter(target))]
    else:
        target["unexpected"] = "value"

    with pytest.raises(module.OperationStateError, match="keys"):
        module._validate_state_schema(state)


@pytest.mark.parametrize(
    ("path", "bad_value", "message"),
    [
        (("schema_version",), True, "schema_version"),
        (("phase_history", 0, "epoch"), True, "epoch"),
        (("phase_history", 0, "epoch"), 1.0, "epoch"),
        (("phase_history", 0, "evidence_sha256"), "A" * 64, "SHA-256"),
        (("snapshot", "rclone_audit", "inode"), False, "inode"),
        (("snapshot", "rclone_audit", "mtime_ns"), 1.5, "mtime_ns"),
        (("install", "next_target_index"), True, "next_target_index"),
        (("active_transaction", "prior_timer_enabled"), 1, "prior_timer_enabled"),
        (("active_transaction", "probe", "objects", 0, "expected_size"), 2.5, "expected_size"),
        (("recovery", "completed_epoch"), False, "completed_epoch"),
    ],
)
def test_schema_rejects_wrong_primitive_types(
    tmp_path: Path,
    path: tuple[object, ...],
    bad_value: object,
    message: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    state = schema_rich_state(operation_dir)
    parent = value_at_path(state, path[:-1])
    parent[path[-1]] = bad_value

    with pytest.raises(module.OperationStateError, match=message):
        module._validate_state_schema(state)


@pytest.mark.parametrize("operation_id", ["", 1, None])
def test_schema_rejects_invalid_operation_id(tmp_path: Path, operation_id: object) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    state = source_verified_state(operation_dir)
    state["operation_id"] = operation_id

    with pytest.raises(module.OperationStateError, match="operation_id"):
        module.validate_operation_state(state, operation_dir)


def test_validate_operation_state_rejects_operation_path_rebinding(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    state = source_verified_state(operation_dir)
    state["operation_dir"] = str(tmp_path / "other")

    with pytest.raises(module.OperationStateError, match="operation_dir"):
        module.validate_operation_state(state, operation_dir)


def test_validate_operation_state_rejects_lexical_dotdot_binding(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    rebound = operation_dir / ".." / operation_dir.name
    state = source_verified_state(rebound)

    with pytest.raises(module.OperationStateError, match="operation_dir"):
        module.validate_operation_state(state, rebound)


@pytest.mark.parametrize(
    "mutator",
    [
        lambda raw: raw.replace('"schema_version": 1,', '"schema_version": 1, "schema_version": 1,', 1),
        lambda raw: raw.replace('"commit": "', '"commit": "first", "commit": "', 1),
        lambda raw: raw.replace('"phase": "source_verified", "epoch"', '"phase": "source_verified", "phase": "source_verified", "epoch"', 1),
    ],
)
def test_load_operation_state_rejects_duplicate_keys_at_multiple_depths(
    tmp_path: Path,
    mutator,
) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    raw = json.dumps(source_verified_state(operation_dir))
    state_file = operation_dir / "operation-state.json"
    state_file.write_text(mutator(raw), encoding="utf-8")
    state_file.chmod(0o600)

    with pytest.raises(module.OperationStateError, match="duplicate"):
        module.load_operation_state(state_file, effective_uid=uid)


@pytest.mark.parametrize(
    "raw",
    [
        "{",
        "{} trailing",
        '{"schema_version":NaN}',
        '{"schema_version":Infinity}',
        b'{"schema_version":1,"operation_id":"\xff"}',
    ],
)
def test_load_operation_state_rejects_corrupt_trailing_or_nonfinite_json(
    tmp_path: Path,
    raw: str | bytes,
) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    state_file = operation_dir / "operation-state.json"
    state_file.write_bytes(raw.encode("utf-8") if isinstance(raw, str) else raw)
    state_file.chmod(0o600)

    with pytest.raises(module.OperationStateError):
        module.load_operation_state(state_file, effective_uid=uid)


@pytest.mark.parametrize("phase", NORMAL_PHASES)
def test_phase_receipt_matrix_accepts_every_normal_phase(tmp_path: Path, phase: str) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)

    module.validate_operation_state(state_at_phase(operation_dir, phase), operation_dir)


@pytest.mark.parametrize(
    ("phase", "required_field"),
    [
        ("staging_prepared", "effective_config"),
        ("staging_prepared", "host_stage"),
        ("snapshotted", "snapshot"),
        ("snapshotted", "prior_runtime"),
        ("installing", "install"),
        ("installed", "install"),
        ("probed", "probe"),
        ("dry_run_recorded", "dry_run"),
        ("policy_enabled", "policy"),
        ("observed", "observation"),
    ],
)
def test_phase_receipt_matrix_rejects_missing_required_receipt(
    tmp_path: Path,
    phase: str,
    required_field: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, phase)
    state[required_field] = None

    with pytest.raises(module.OperationStateError, match=required_field):
        module.validate_operation_state(state, operation_dir)


@pytest.mark.parametrize(
    ("phase", "early_field"),
    [
        ("source_verified", "effective_config"),
        ("staging_prepared", "snapshot"),
        ("snapshotted", "install"),
        ("installing", "probe"),
        ("probing", "probe"),
        ("probed", "dry_run"),
        ("dry_run_recording", "dry_run"),
        ("dry_run_recorded", "policy"),
        ("policy_enabling", "policy"),
        ("policy_enabled", "observation"),
        ("observing", "observation"),
    ],
)
def test_phase_receipt_matrix_rejects_early_receipt(
    tmp_path: Path,
    phase: str,
    early_field: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, phase)
    complete = observed_state(operation_dir)
    state[early_field] = copy.deepcopy(complete[early_field])

    with pytest.raises(module.OperationStateError, match=early_field):
        module.validate_operation_state(state, operation_dir)


@pytest.mark.parametrize(
    ("phase", "kind", "prior"),
    [
        ("probing", "probe", "installed"),
        ("dry_run_recording", "dry_run", "probed"),
        ("policy_enabling", "policy", "dry_run_recorded"),
        ("observing", "observe", "policy_enabled"),
    ],
)
def test_transient_phase_requires_matching_active_transaction(
    tmp_path: Path,
    phase: str,
    kind: str,
    prior: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, phase)
    state["active_transaction"] = None

    with pytest.raises(module.OperationStateError, match="active_transaction"):
        module.validate_operation_state(state, operation_dir)

    state = state_at_phase(operation_dir, phase)
    state["active_transaction"]["kind"] = "observe" if kind != "observe" else "probe"
    with pytest.raises(module.OperationStateError, match="active_transaction"):
        module.validate_operation_state(state, operation_dir)

    state = state_at_phase(operation_dir, phase)
    state["active_transaction"]["prior_stable_phase"] = "installed" if prior != "installed" else "probed"
    with pytest.raises(module.OperationStateError, match="active_transaction"):
        module.validate_operation_state(state, operation_dir)


def test_stable_phase_rejects_active_transaction(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "installed")
    state["active_transaction"] = active_transaction("probe", "installed", 1_750_000_050)

    with pytest.raises(module.OperationStateError, match="active_transaction"):
        module.validate_operation_state(state, operation_dir)


def test_snapshot_requires_exact_seven_targets_and_presence_coherence(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "snapshotted")
    del state["snapshot"]["targets"][TARGETS[0]]
    with pytest.raises(module.OperationStateError, match="snapshot.targets"):
        module.validate_operation_state(state, operation_dir)

    state = state_at_phase(operation_dir, "snapshotted")
    state["snapshot"]["targets"][TARGETS[0]]["present"] = False
    with pytest.raises(module.OperationStateError, match="snapshot.targets"):
        module.validate_operation_state(state, operation_dir)


def test_install_hashes_require_exact_provenance_bound_seven_targets(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "installed")
    module.validate_operation_state(state, operation_dir)

    del state["install"]["installed_hashes"][TARGETS[0]]
    with pytest.raises(module.OperationStateError, match="installed_hashes"):
        module.validate_operation_state(state, operation_dir)

    state = state_at_phase(operation_dir, "installed")
    state["install"]["installed_hashes"][TARGETS[0]] = HASH_B
    with pytest.raises(module.OperationStateError, match="provenance"):
        module.validate_operation_state(state, operation_dir)

    state = state_at_phase(operation_dir, "installed")
    state["install"]["installed_hashes"]["/etc/degen/prod-db-backup.env"] = HASH_A
    with pytest.raises(module.OperationStateError, match="environment"):
        module.validate_operation_state(state, operation_dir)


@pytest.mark.parametrize("field", ["installed_hashes", "completed_epoch"])
def test_install_receipt_is_incomplete_until_installed_history(
    tmp_path: Path,
    field: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "installing")
    if field == "installed_hashes":
        state["install"][field] = {target: HASH_A for target in TARGETS}
    else:
        state["install"][field] = 1_750_000_040

    with pytest.raises(module.OperationStateError, match=field):
        module.validate_operation_state(state, operation_dir)


def test_phase_and_receipt_epochs_obey_lifecycle_ordering(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "observed")
    state["phase_history"][5]["epoch"] = state["phase_history"][4]["epoch"] - 1
    with pytest.raises(module.OperationStateError, match="nondecreasing"):
        module.validate_operation_state(state, operation_dir)

    state = state_at_phase(operation_dir, "observed")
    state["install"]["completed_epoch"] = state["install"]["started_epoch"] - 1
    with pytest.raises(module.OperationStateError, match="install"):
        module.validate_operation_state(state, operation_dir)

    state = state_at_phase(operation_dir, "observed")
    state["observation"]["run_epoch"] = state["policy"]["enabled_epoch"]
    with pytest.raises(module.OperationStateError, match="observation"):
        module.validate_operation_state(state, operation_dir)


@pytest.mark.parametrize(
    "state_factory,phase",
    [
        (install_recovery_state, "recovering"),
        (install_recovery_state, "recovery_required"),
        (install_recovery_state, "rolled_back"),
        (manual_rollback_state, "manual_rollback"),
        (manual_rollback_state, "recovery_required"),
        (manual_rollback_state, "rolled_back"),
    ],
)
def test_install_and_manual_recovery_states_accept_both_terminal_history_shapes(
    tmp_path: Path,
    state_factory,
    phase: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)

    module.validate_operation_state(state_factory(operation_dir, phase), operation_dir)


@pytest.mark.parametrize(
    ("kind", "phase"),
    [
        ("probe", "recovery_required"),
        ("probe", "recovering_probe"),
        ("guard_dry_run", "recovery_required"),
        ("guard_dry_run", "recovering_guard"),
        ("guard_observe", "recovering_guard"),
        ("policy", "recovery_required"),
        ("policy", "recovering_policy"),
    ],
)
def test_transaction_recovery_states_require_matching_kind_and_active_transaction(
    tmp_path: Path,
    kind: str,
    phase: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    state = failed_transaction_state(operation_dir, kind, phase)
    module.validate_operation_state(state, operation_dir)

    state["recovery"]["kind"] = "install"
    with pytest.raises(module.OperationStateError, match="recovery"):
        module.validate_operation_state(state, operation_dir)


@pytest.mark.parametrize(
    ("old_phase", "new_phase"),
    [
        ("source_verified", "snapshotted"),
        ("installed", "dry_run_recording"),
        ("observed", "source_verified"),
        ("rolled_back", "source_verified"),
    ],
)
def test_validate_previous_state_rejects_forbidden_phase_transitions(
    tmp_path: Path,
    old_phase: str,
    new_phase: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    old = (
        install_recovery_state(operation_dir, "rolled_back")
        if old_phase == "rolled_back"
        else state_at_phase(operation_dir, old_phase)
    )
    new = state_at_phase(operation_dir, new_phase)

    with pytest.raises(module.OperationStateError, match="transition|history"):
        module.validate_operation_state(new, operation_dir, old)


def test_phase_change_requires_exactly_one_append_only_history_entry(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    old = state_at_phase(operation_dir, "source_verified")
    new = state_at_phase(operation_dir, "staging_prepared")
    module.validate_operation_state(new, operation_dir, old)

    altered = copy.deepcopy(new)
    altered["phase_history"][0]["evidence_sha256"] = HASH_C
    with pytest.raises(module.OperationStateError, match="history"):
        module.validate_operation_state(altered, operation_dir, old)

    truncated = copy.deepcopy(new)
    truncated["phase_history"] = truncated["phase_history"][-1:]
    with pytest.raises(module.OperationStateError, match="history"):
        module.validate_operation_state(truncated, operation_dir, old)


def test_exact_noop_allowed_but_stable_same_phase_mutation_rejected(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "installed")
    module.validate_operation_state(copy.deepcopy(state), operation_dir, state)

    changed = copy.deepcopy(state)
    changed["effective_config"]["BACKUP_DIR"] = "/rebound"
    with pytest.raises(module.OperationStateError, match="same-phase"):
        module.validate_operation_state(changed, operation_dir, state)


def test_install_cursor_advances_one_exact_target_and_never_rebinds(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    old = state_at_phase(operation_dir, "installing")
    new = copy.deepcopy(old)
    new["install"].update(
        {
            "next_target_index": 1,
            "current_target": TARGETS[1],
            "previous_sha256": HASH_A,
            "intended_sha256": HASH_A,
        }
    )
    module.validate_operation_state(new, operation_dir, old)

    for bad_index, bad_target in ((2, TARGETS[2]), (1, TARGETS[2])):
        bad = copy.deepcopy(old)
        bad["install"].update(
            {
                "next_target_index": bad_index,
                "current_target": bad_target,
                "previous_sha256": HASH_A,
                "intended_sha256": HASH_A,
            }
        )
        with pytest.raises(module.OperationStateError, match="cursor"):
            module.validate_operation_state(bad, operation_dir, old)

    rebound = copy.deepcopy(old)
    rebound["install"]["previous_sha256"] = HASH_B
    with pytest.raises(module.OperationStateError, match="cursor"):
        module.validate_operation_state(rebound, operation_dir, old)

    rebound = copy.deepcopy(old)
    rebound["install"]["started_epoch"] += 1
    with pytest.raises(module.OperationStateError, match="install.*start"):
        module.validate_operation_state(rebound, operation_dir, old)


@pytest.mark.parametrize("kind", ["install", "manual_rollback"])
def test_recovery_cursor_advances_independently_and_install_receipt_is_frozen(
    tmp_path: Path,
    kind: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    old = (
        install_recovery_state(operation_dir, "recovering")
        if kind == "install"
        else manual_rollback_state(operation_dir, "manual_rollback")
    )
    new = copy.deepcopy(old)
    new["recovery"].update(
        {
            "next_target_index": 1,
            "current_target": TARGETS[1],
            "previous_sha256": HASH_A,
            "intended_sha256": HASH_A,
        }
    )
    module.validate_operation_state(new, operation_dir, old)

    rebound = copy.deepcopy(new)
    rebound["install"]["next_target_index"] = 2
    with pytest.raises(module.OperationStateError, match="install"):
        module.validate_operation_state(rebound, operation_dir, old)

    skipped = copy.deepcopy(old)
    skipped["recovery"].update(
        {
            "next_target_index": 2,
            "current_target": TARGETS[2],
            "previous_sha256": HASH_A,
            "intended_sha256": HASH_A,
        }
    )
    with pytest.raises(module.OperationStateError, match="recovery.*cursor"):
        module.validate_operation_state(skipped, operation_dir, old)


def test_probe_and_guard_recovery_require_null_file_cursor(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    for kind in ("probe", "guard_dry_run"):
        state = failed_transaction_state(operation_dir, kind, "recovery_required")
        state["recovery"]["current_target"] = TARGETS[0]
        state["recovery"]["previous_sha256"] = HASH_A
        state["recovery"]["intended_sha256"] = HASH_A
        with pytest.raises(module.OperationStateError, match="null.*cursor|cursor.*null"):
            module.validate_operation_state(state, operation_dir)


def test_policy_recovery_cursor_targets_only_fixed_environment_path(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    state = failed_transaction_state(operation_dir, "policy", "recovering_policy")
    state["recovery"]["current_target"] = TARGETS[0]

    with pytest.raises(module.OperationStateError, match="environment"):
        module.validate_operation_state(state, operation_dir)


@pytest.mark.parametrize("field", ["next_target_index", "started_epoch", "evidence_sha256"])
def test_recovery_required_resume_preserves_cursor_start_and_evidence(
    tmp_path: Path,
    field: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    old = install_recovery_state(operation_dir, "recovery_required")
    resumed = copy.deepcopy(old)
    append_phase(resumed, "recovering", 1_750_000_043)
    if field == "next_target_index":
        resumed["recovery"][field] = 1
        resumed["recovery"]["current_target"] = TARGETS[1]
    elif field == "started_epoch":
        resumed["recovery"][field] += 1
    else:
        resumed["recovery"][field] = HASH_A

    with pytest.raises(module.OperationStateError, match="resume|recovery"):
        module.validate_operation_state(resumed, operation_dir, old)


def test_append_only_evidence_errors_and_primary_failure_are_immutable(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    old = install_recovery_state(operation_dir, "recovering")
    old["secondary_errors"] = [
        {"stage": "one", "error": "first", "epoch": 1_750_000_041, "evidence_sha256": HASH_A}
    ]
    new = copy.deepcopy(old)
    new["secondary_errors"].append(
        {"stage": "two", "error": "second", "epoch": 1_750_000_042, "evidence_sha256": HASH_B}
    )
    module.validate_operation_state(new, operation_dir, old)

    reordered = copy.deepcopy(new)
    reordered["secondary_errors"].reverse()
    with pytest.raises(module.OperationStateError, match="secondary_errors"):
        module.validate_operation_state(reordered, operation_dir, old)

    changed_failure = copy.deepcopy(new)
    changed_failure["failure"]["primary_error"] = "replacement"
    with pytest.raises(module.OperationStateError, match="primary"):
        module.validate_operation_state(changed_failure, operation_dir, old)


def test_context_validator_binds_operation_and_reviewed_digests(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "source_verified")
    context = module.OperationsContext(
        operation_id=operation_dir.name,
        paths=module.build_operation_paths(operation_dir),
        effective_uid=uid,
        command_runner=lambda argv, pass_fds: None,
        clock=lambda: datetime.now(timezone.utc),
        expected_commit="1" * 40,
        expected_archive_sha256=HASH_B,
        expected_manifest_sha256=HASH_C,
        host_root=tmp_path,
    )
    module.validate_operation_state_for_context(state, context)

    for field, bad_value in (
        ("operation_id", "other"),
        ("operation_dir", str(tmp_path / "other")),
    ):
        bad = copy.deepcopy(state)
        bad[field] = bad_value
        with pytest.raises(module.OperationStateError, match=field):
            module.validate_operation_state_for_context(bad, context)

    for field in ("commit", "archive_sha256", "manifest_sha256"):
        bad = copy.deepcopy(state)
        bad["reviewed_source"][field] = HASH_A if field != "commit" else "2" * 40
        with pytest.raises(module.OperationStateError, match=field):
            module.validate_operation_state_for_context(bad, context)


def successful_policy_reset(
    operation_dir: Path,
) -> tuple[dict[str, object], dict[str, object]]:
    previous = failed_transaction_state(operation_dir, "policy", "recovering_policy")
    previous["failure"] = {
        "phase": "policy_enabling",
        "primary_error": "policy write failed",
        "epoch": 1_750_000_120,
        "evidence_sha256": HASH_A,
    }
    previous["secondary_errors"] = [
        {"stage": "restore", "error": "retry needed", "epoch": 1_750_000_121, "evidence_sha256": HASH_B}
    ]
    previous["rclone_evidence_groups"] = copy.deepcopy(
        schema_rich_state(operation_dir)["rclone_evidence_groups"]
    )
    mark_transaction_complete(previous)
    current = copy.deepcopy(previous)
    current["probe"] = None
    current["dry_run"] = None
    current["policy"] = None
    current["observation"] = None
    current["active_transaction"] = None
    current["recovery"].update(
        {
            "next_target_index": 1,
            "current_target": None,
            "previous_sha256": None,
            "intended_sha256": None,
            "completed_epoch": 1_750_000_130,
        }
    )
    append_phase(current, "installed", 1_750_000_130)
    return previous, current


def test_recovering_policy_to_installed_allows_only_complete_authorized_reset(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    previous, current = successful_policy_reset(operation_dir)

    module.validate_operation_state(current, operation_dir, previous)
    assert current["install"] == previous["install"]
    assert current["recovery"]["completed_epoch"] != current["install"]["completed_epoch"]


@pytest.mark.parametrize("field", ["probe", "dry_run"])
def test_policy_reset_rejects_partial_later_receipt_clear(tmp_path: Path, field: str) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    previous, current = successful_policy_reset(operation_dir)
    current[field] = copy.deepcopy(previous[field])

    with pytest.raises(module.OperationStateError, match=field):
        module.validate_operation_state(current, operation_dir, previous)


@pytest.mark.parametrize(
    "field",
    [
        "reviewed_source",
        "effective_config",
        "host_stage",
        "snapshot",
        "prior_runtime",
        "install",
        "rclone_evidence_groups",
        "failure",
        "secondary_errors",
    ],
)
def test_policy_reset_preserves_all_frozen_receipts_and_evidence(
    tmp_path: Path,
    field: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    previous, current = successful_policy_reset(operation_dir)
    if isinstance(current[field], list):
        current[field].append(copy.deepcopy(current[field][-1]))
    elif isinstance(current[field], dict):
        current[field][next(iter(current[field]))] = "changed"
    else:
        raise AssertionError(field)

    with pytest.raises(module.OperationStateError):
        module.validate_operation_state(current, operation_dir, previous)


def test_policy_reset_requires_cleared_active_transaction_and_completed_recovery(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    previous, current = successful_policy_reset(operation_dir)
    current["active_transaction"] = copy.deepcopy(previous["active_transaction"])
    with pytest.raises(module.OperationStateError, match="active_transaction"):
        module.validate_operation_state(current, operation_dir, previous)

    previous, current = successful_policy_reset(operation_dir)
    current["recovery"]["completed_epoch"] = None
    with pytest.raises(module.OperationStateError, match="completion"):
        module.validate_operation_state(current, operation_dir, previous)


def test_same_receipt_reset_is_forbidden_on_any_other_transition(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    previous = state_at_phase(operation_dir, "policy_enabled")
    current = copy.deepcopy(previous)
    current["probe"] = None
    current["dry_run"] = None
    current["policy"] = None
    append_phase(current, "installed", 1_750_000_130)

    with pytest.raises(module.OperationStateError, match="transition|history"):
        module.validate_operation_state(current, operation_dir, previous)


@pytest.mark.parametrize(
    "raw",
    [
        "DATABASE_URL=postgresql://user:pass@db.internal/degen",
        "password=hunter2",
        "PGPASSWORD=hunter2",
        "token: abcdefghijklmnopqrstuvwxyz",
        "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.payload.signature",
        "github token ghp_abcdefghijklmnopqrstuvwxyz123456",
        "-----BEGIN PRIVATE KEY----- secret",
        "[onedrive]\ntype = onedrive\ntoken = {secret-json}",
    ],
)
def test_sanitize_error_text_redacts_secret_material(raw: str) -> None:
    module = load_ops_helper()

    sanitized = module.sanitize_error_text(RuntimeError(raw))

    assert sanitized
    assert sanitized != raw
    assert len(sanitized) <= 512
    assert "user:pass" not in sanitized
    assert "hunter2" not in sanitized
    assert "abcdefghijklmnopqrstuvwxyz" not in sanitized
    assert "secret-json" not in sanitized
    assert "PRIVATE KEY" not in sanitized
    assert "\n" not in sanitized
    module._reject_residual_secrets({"error": sanitized})


def test_sanitize_error_text_handles_broken_stringification_and_controls() -> None:
    module = load_ops_helper()

    class Broken:
        def __str__(self) -> str:
            raise RuntimeError("must not escape")

    assert module.sanitize_error_text(Broken()) == "Broken"
    sanitized = module.sanitize_error_text("line one\x00\r\nline two " + "x" * 1000)
    assert "\x00" not in sanitized
    assert "\n" not in sanitized
    assert len(sanitized) == 512


@pytest.mark.parametrize(
    ("location", "secret"),
    [
        ("config_key", "DATABASE_URL"),
        ("config_key", "api_token"),
        ("config_key", "credentials"),
        ("config_key", "PGPASSWORD"),
        ("config_value", "postgresql://user:pass@host/db"),
        ("config_value", "password=hunter2"),
        ("primary", "request failed token=abcdef"),
        ("secondary", "Authorization: Bearer abc.def.ghi"),
        ("probe_prefix", "https://user:pass@example.invalid/path"),
    ],
)
def test_state_validator_rejects_residual_secrets_anywhere(
    tmp_path: Path,
    location: str,
    secret: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "probing")
    if location == "config_key":
        state["effective_config"][secret] = "value"
    elif location == "config_value":
        state["effective_config"]["SAFE"] = secret
    elif location == "primary":
        state["failure"] = {
            "phase": "probing",
            "primary_error": secret,
            "epoch": 1_750_000_051,
            "evidence_sha256": HASH_A,
        }
    elif location == "secondary":
        state["secondary_errors"] = [
            {"stage": "cleanup", "error": secret, "epoch": 1_750_000_052, "evidence_sha256": HASH_B}
        ]
    else:
        state["active_transaction"]["probe"]["prefix"] = secret

    with pytest.raises(module.OperationStateError, match="secret"):
        module.validate_operation_state(state, operation_dir)


def test_non_secret_operational_error_text_is_accepted(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "probing")
    state["failure"] = {
        "phase": "probing",
        "primary_error": "rclone token refresh failed before remote inventory",
        "epoch": 1_750_000_051,
        "evidence_sha256": HASH_A,
    }

    module.validate_operation_state(state, operation_dir)


def canonical_state_bytes(state: dict[str, object]) -> bytes:
    return (
        json.dumps(state, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
        + "\n"
    ).encode("utf-8")


def test_atomic_write_creates_canonical_0600_initial_state(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    state = source_verified_state(operation_dir)
    state_file = operation_dir / "operation-state.json"

    module.atomic_write_operation_state(state_file, state, effective_uid=uid)

    assert state_file.read_bytes() == canonical_state_bytes(state)
    expected_mode = 0o600 if os.name == "posix" else 0o666
    assert stat.S_IMODE(state_file.stat().st_mode) == expected_mode
    assert module.load_operation_state(state_file, effective_uid=uid) == state


def test_atomic_write_absent_state_accepts_only_one_entry_source_verified(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    state_file = operation_dir / "operation-state.json"
    invalid = state_at_phase(operation_dir, "staging_prepared")

    with pytest.raises(module.OperationStateError, match="absent|initial"):
        module.atomic_write_operation_state(state_file, invalid, effective_uid=uid)

    assert not state_file.exists()
    assert list(operation_dir.iterdir()) == []


def test_atomic_write_exact_noop_does_not_create_temp_or_replace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    state = source_verified_state(operation_dir)
    state_file = write_state_file(operation_dir, state)
    before = state_file.stat()
    events: list[str] = []
    monkeypatch.setattr(module, "_atomic_event_hook", lambda event, **kwargs: events.append(event))

    module.atomic_write_operation_state(state_file, copy.deepcopy(state), effective_uid=uid)

    after = state_file.stat()
    assert events == []
    assert (after.st_dev, after.st_ino, after.st_mtime_ns) == (
        before.st_dev,
        before.st_ino,
        before.st_mtime_ns,
    )


def test_atomic_write_validates_replacement_before_opening_temp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    invalid = source_verified_state(operation_dir)
    invalid["operation_id"] = ""
    events: list[str] = []
    monkeypatch.setattr(module, "_atomic_event_hook", lambda event, **kwargs: events.append(event))

    with pytest.raises(module.OperationStateError, match="operation_id"):
        module.atomic_write_operation_state(
            operation_dir / "operation-state.json",
            invalid,
            effective_uid=uid,
        )

    assert events == []
    assert list(operation_dir.iterdir()) == []


def test_atomic_write_performs_legal_compare_and_swap_transition(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    old = state_at_phase(operation_dir, "source_verified")
    state_file = write_state_file(operation_dir, old)
    new = state_at_phase(operation_dir, "staging_prepared")

    module.atomic_write_operation_state(state_file, new, effective_uid=uid)

    assert state_file.read_bytes() == canonical_state_bytes(new)


def test_atomic_write_cas_rejects_byte_change_before_replace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    old = state_at_phase(operation_dir, "source_verified")
    state_file = write_state_file(operation_dir, old)
    new = state_at_phase(operation_dir, "staging_prepared")
    raced_bytes = json.dumps(old, indent=2).encode("utf-8")

    def race(event: str, **kwargs: object) -> None:
        if event == "before_cas":
            state_file.write_bytes(raced_bytes)
            state_file.chmod(0o600)

    monkeypatch.setattr(module, "_atomic_event_hook", race)

    with pytest.raises(module.OperationStateError, match="compare-and-swap"):
        module.atomic_write_operation_state(state_file, new, effective_uid=uid)

    assert state_file.read_bytes() == raced_bytes
    assert not any(path.name.startswith(".operation-state.json.") for path in operation_dir.iterdir())


def test_atomic_write_cas_rejects_destination_inode_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    old = state_at_phase(operation_dir, "source_verified")
    state_file = write_state_file(operation_dir, old)
    old_bytes = state_file.read_bytes()
    new = state_at_phase(operation_dir, "staging_prepared")

    def race(event: str, **kwargs: object) -> None:
        if event == "before_cas":
            replacement = operation_dir / "replacement.json"
            replacement.write_bytes(old_bytes)
            replacement.chmod(0o600)
            os.replace(replacement, state_file)

    monkeypatch.setattr(module, "_atomic_event_hook", race)

    with pytest.raises(module.OperationStateError, match="compare-and-swap"):
        module.atomic_write_operation_state(state_file, new, effective_uid=uid)

    assert state_file.read_bytes() == old_bytes


def test_atomic_write_never_unlinks_raced_temp_name(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    old = state_at_phase(operation_dir, "source_verified")
    state_file = write_state_file(operation_dir, old)
    old_bytes = state_file.read_bytes()
    new = state_at_phase(operation_dir, "staging_prepared")
    attacker_bytes = b"attacker-owned"
    attacker_path: Path | None = None

    def race(event: str, **kwargs: object) -> None:
        nonlocal attacker_path
        if event == "before_replace":
            temp_path = Path(kwargs["temp_path"])
            temp_path.replace(operation_dir / "stolen-temp")
            temp_path.write_bytes(attacker_bytes)
            temp_path.chmod(0o600)
            attacker_path = temp_path

    monkeypatch.setattr(module, "_atomic_event_hook", race)

    with pytest.raises(module.OperationStateError, match="temporary|binding"):
        module.atomic_write_operation_state(state_file, new, effective_uid=uid)

    assert state_file.read_bytes() == old_bytes
    assert attacker_path is not None and attacker_path.read_bytes() == attacker_bytes


@pytest.mark.parametrize("event_name", ["after_temp_open", "after_temp_fsync", "before_cas", "before_replace"])
def test_atomic_write_failure_before_replace_preserves_old_bytes_and_cleans_owned_temp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    event_name: str,
) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    old = state_at_phase(operation_dir, "source_verified")
    state_file = write_state_file(operation_dir, old)
    old_bytes = state_file.read_bytes()
    new = state_at_phase(operation_dir, "staging_prepared")

    def fail(event: str, **kwargs: object) -> None:
        if event == event_name:
            raise RuntimeError("injected failure")

    monkeypatch.setattr(module, "_atomic_event_hook", fail)

    with pytest.raises(RuntimeError, match="injected"):
        module.atomic_write_operation_state(state_file, new, effective_uid=uid)

    assert state_file.read_bytes() == old_bytes
    assert not any(path.name.startswith(".operation-state.json.") for path in operation_dir.iterdir())


def test_atomic_writer_source_requires_exclusive_nofollow_cloexec_and_durability_calls() -> None:
    module = load_ops_helper()
    source = inspect.getsource(module)

    for required in (
        "os.O_EXCL",
        "os.O_NOFOLLOW",
        "os.O_CLOEXEC",
        "os.fsync",
        "os.replace",
    ):
        assert required in source


def test_show_state_emits_exact_canonical_json_plus_one_lf(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = load_ops_helper()
    raw_path = "/opt/degen/backups/config/20260630T235959Z"
    state = source_verified_state(Path(raw_path))
    state["operation_dir"] = raw_path
    captured_path: list[tuple[Path, int]] = []
    monkeypatch.setattr(module, "_effective_uid", lambda: 0)
    monkeypatch.setattr(module, "_require_posix_descriptor_primitives", lambda: None)
    monkeypatch.setattr(
        module,
        "load_operation_state",
        lambda path, *, effective_uid: captured_path.append((path, effective_uid)) or state,
    )
    monkeypatch.setattr(module, "validate_operation_state", lambda *args, **kwargs: None)

    result = module.main(["show-state", "--operation-dir", raw_path])

    captured = capsys.readouterr()
    assert result == 0
    assert captured.out.encode("utf-8") == canonical_state_bytes(state)
    assert captured.err == ""
    assert captured_path == [(Path(raw_path) / "operation-state.json", 0)]


def test_show_state_sanitizes_failures_without_stdout_or_traceback(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = load_ops_helper()
    raw_path = "/opt/degen/backups/config/20260630T235959Z"
    monkeypatch.setattr(module, "_effective_uid", lambda: 0)
    monkeypatch.setattr(module, "_require_posix_descriptor_primitives", lambda: None)

    def fail(*args: object, **kwargs: object) -> dict[str, object]:
        raise RuntimeError("DATABASE_URL=postgresql://user:pass@host/db")

    monkeypatch.setattr(module, "load_operation_state", fail)

    result = module.main(["show-state", "--operation-dir", raw_path])

    captured = capsys.readouterr()
    assert result == 1
    assert captured.out == ""
    assert captured.err == "error: [REDACTED]\n"
    assert "Traceback" not in captured.err
    assert "user:pass" not in captured.err


def test_show_state_fails_closed_on_missing_posix_primitives_before_storage(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = load_ops_helper()
    raw_path = "/opt/degen/backups/config/20260630T235959Z"
    storage_called = False
    monkeypatch.setattr(module, "_effective_uid", lambda: 0)

    def unavailable() -> None:
        raise module.OperationStateError("required POSIX descriptor primitives are unavailable")

    def load(*args: object, **kwargs: object) -> dict[str, object]:
        nonlocal storage_called
        storage_called = True
        return {}

    monkeypatch.setattr(module, "_require_posix_descriptor_primitives", unavailable)
    monkeypatch.setattr(module, "load_operation_state", load)

    result = module.main(["show-state", "--operation-dir", raw_path])

    captured = capsys.readouterr()
    assert result == 1
    assert not storage_called
    assert captured.out == ""
    assert captured.err == "error: required POSIX descriptor primitives are unavailable\n"


def test_cli_has_no_host_root_or_test_override() -> None:
    help_result = subprocess.run(
        [sys.executable, str(OPS_HELPER), "show-state", "--help"],
        text=True,
        capture_output=True,
        check=False,
    )

    assert help_result.returncode == 0
    assert "host-root" not in help_result.stdout
    assert "test" not in help_result.stdout.lower()


@pytest.mark.parametrize(
    ("old_phase", "new_phase"),
    list(zip(NORMAL_PHASES, NORMAL_PHASES[1:])),
)
def test_every_normal_forward_transition_accepts_exact_receipts(
    tmp_path: Path,
    old_phase: str,
    new_phase: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)

    previous = state_at_phase(operation_dir, old_phase)
    if previous["active_transaction"] is not None:
        mark_transaction_complete(previous)
    if old_phase == "installing":
        previous["install"].update(
            {
                "next_target_index": len(TARGETS),
                "current_target": None,
                "previous_sha256": None,
                "intended_sha256": None,
            }
        )
    module.validate_operation_state(
        state_at_phase(operation_dir, new_phase),
        operation_dir,
        previous,
    )


def test_forward_transition_cannot_mutate_an_earlier_receipt(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    previous = state_at_phase(operation_dir, "probed")
    current = state_at_phase(operation_dir, "dry_run_recording")
    current["probe"]["evidence_sha256"] = HASH_C

    with pytest.raises(module.OperationStateError, match="probe"):
        module.validate_operation_state(current, operation_dir, previous)


@pytest.mark.parametrize("field", ["previous_sha256", "intended_sha256"])
def test_install_cursor_hash_tuple_is_bound_to_snapshot_and_staged_target(
    tmp_path: Path,
    field: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "installing")
    state["install"][field] = HASH_B

    with pytest.raises(module.OperationStateError, match="install.*cursor"):
        module.validate_operation_state(state, operation_dir)


def test_active_guard_and_probe_object_progress_is_monotonic_and_ordered(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    previous = state_at_phase(operation_dir, "probing")
    current = copy.deepcopy(previous)
    current["active_transaction"]["guard"]["timer_stopped"] = True
    module.validate_operation_state(current, operation_dir, previous)

    regressed = copy.deepcopy(current)
    regressed["active_transaction"]["guard"]["timer_stopped"] = False
    with pytest.raises(module.OperationStateError, match="guard"):
        module.validate_operation_state(regressed, operation_dir, current)

    impossible = state_at_phase(operation_dir, "probing")
    impossible["active_transaction"]["guard"]["service_inactive_verified"] = True
    with pytest.raises(module.OperationStateError, match="guard"):
        module.validate_operation_state(impossible, operation_dir)

    impossible = state_at_phase(operation_dir, "probing")
    impossible["active_transaction"]["probe"]["objects"][0]["verified"] = True
    with pytest.raises(module.OperationStateError, match="probe"):
        module.validate_operation_state(impossible, operation_dir)


def test_completed_recovery_can_be_replaced_only_by_later_manual_rollback_attempt(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    required = failed_transaction_state(operation_dir, "policy", "recovery_required")
    resumed = copy.deepcopy(required)
    append_phase(resumed, "recovering_policy", 1_750_000_123)
    module.validate_operation_state(resumed, operation_dir, required)
    mark_transaction_complete(resumed)

    installed = copy.deepcopy(resumed)
    for field in ("probe", "dry_run", "policy", "observation", "active_transaction"):
        installed[field] = None
    installed["recovery"].update(
        {
            "next_target_index": 1,
            "current_target": None,
            "previous_sha256": None,
            "intended_sha256": None,
            "completed_epoch": 1_750_000_130,
        }
    )
    append_phase(installed, "installed", 1_750_000_130)
    module.validate_operation_state(installed, operation_dir, resumed)

    manual = copy.deepcopy(installed)
    manual["recovery"] = recovery_receipt(
        "manual_rollback",
        current_target=TARGETS[0],
        previous_sha256=HASH_A,
        intended_sha256=HASH_A,
    )
    manual["recovery"]["started_epoch"] = 1_750_000_140
    append_phase(manual, "manual_rollback", 1_750_000_140)
    module.validate_operation_state(manual, operation_dir, installed)

    ordinary = copy.deepcopy(installed)
    ordinary["recovery"] = copy.deepcopy(manual["recovery"])
    append_phase(ordinary, "probing", 1_750_000_140)
    ordinary["active_transaction"] = active_transaction("probe", "installed", 1_750_000_140)
    with pytest.raises(module.OperationStateError, match="recovery"):
        module.validate_operation_state(ordinary, operation_dir, installed)


def test_posix_atomic_primitive_gate_uses_rename_capability_for_replace() -> None:
    module = load_ops_helper()
    linux_style_support = {module.os.open, module.os.stat, module.os.unlink, module.os.rename}

    assert module._write_descriptor_primitives_available(linux_style_support)
    assert not module._write_descriptor_primitives_available(linux_style_support - {module.os.unlink})


def mark_transaction_complete(state: dict[str, object]) -> None:
    transaction = state["active_transaction"]
    assert isinstance(transaction, dict)
    guard = transaction["guard"]
    assert isinstance(guard, dict)
    for field in guard:
        guard[field] = True
    probe = transaction["probe"]
    if isinstance(probe, dict):
        objects = probe["objects"]
        assert isinstance(objects, list)
        for item in objects:
            item["created"] = True
            item["verified"] = True
            item["cleaned"] = True


@pytest.mark.parametrize(
    ("transient", "stable"),
    [
        ("probing", "probed"),
        ("dry_run_recording", "dry_run_recorded"),
        ("policy_enabling", "policy_enabled"),
        ("observing", "observed"),
    ],
)
def test_success_transition_rejects_incomplete_guard_before_clearing_transaction(
    tmp_path: Path,
    transient: str,
    stable: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)

    with pytest.raises(module.OperationStateError, match="guard|transaction"):
        module.validate_operation_state(
            state_at_phase(operation_dir, stable),
            operation_dir,
            state_at_phase(operation_dir, transient),
        )


def test_probe_cleanup_cannot_precede_verification(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "probing")
    item = state["active_transaction"]["probe"]["objects"][0]
    item["created"] = True
    item["cleaned"] = True

    with pytest.raises(module.OperationStateError, match="probe"):
        module.validate_operation_state(state, operation_dir)


@pytest.mark.parametrize("kind", ["probe", "guard_dry_run"])
def test_recovery_return_rejects_incomplete_guard_before_stable_phase(
    tmp_path: Path,
    kind: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    recovery_phase = "recovering_probe" if kind == "probe" else "recovering_guard"
    stable_phase = "installed" if kind == "probe" else "probed"
    previous = failed_transaction_state(operation_dir, kind, recovery_phase)
    current = copy.deepcopy(previous)
    current["active_transaction"] = None
    current["recovery"]["completed_epoch"] = 1_750_000_130
    append_phase(current, stable_phase, 1_750_000_130)

    with pytest.raises(module.OperationStateError, match="guard|transaction"):
        module.validate_operation_state(current, operation_dir, previous)


def test_installed_requires_terminal_cleared_install_cursor(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "installed")
    state["install"].update(
        {
            "next_target_index": 0,
            "current_target": TARGETS[0],
            "previous_sha256": HASH_A,
            "intended_sha256": HASH_A,
        }
    )

    with pytest.raises(module.OperationStateError, match="install.*cursor"):
        module.validate_operation_state(state, operation_dir)


def test_entering_installing_requires_first_write_ahead_cursor(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    previous = state_at_phase(operation_dir, "snapshotted")
    current = state_at_phase(operation_dir, "installing")
    current["install"].update(
        {
            "next_target_index": 1,
            "current_target": TARGETS[1],
            "previous_sha256": HASH_A,
            "intended_sha256": HASH_A,
        }
    )

    with pytest.raises(module.OperationStateError, match="index zero|first target|entry"):
        module.validate_operation_state(current, operation_dir, previous)


@pytest.mark.parametrize("kind", ["install", "manual_rollback", "policy"])
def test_new_recovery_attempt_requires_index_zero(
    tmp_path: Path,
    kind: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    if kind == "install":
        previous = state_at_phase(operation_dir, "installing")
        current = install_recovery_state(operation_dir, "recovering")
        current["recovery"].update(
            {
                "next_target_index": 4,
                "current_target": TARGETS[4],
                "previous_sha256": HASH_A,
                "intended_sha256": HASH_A,
            }
        )
    elif kind == "manual_rollback":
        previous = state_at_phase(operation_dir, "installed")
        current = manual_rollback_state(operation_dir, "manual_rollback")
        current["recovery"].update(
            {
                "next_target_index": 4,
                "current_target": TARGETS[4],
                "previous_sha256": HASH_A,
                "intended_sha256": HASH_A,
            }
        )
    else:
        previous = state_at_phase(operation_dir, "policy_enabling")
        current = failed_transaction_state(operation_dir, "policy", "recovering_policy")
        current["recovery"].update(
            {
                "next_target_index": 1,
                "current_target": None,
                "previous_sha256": None,
                "intended_sha256": None,
            }
        )

    with pytest.raises(module.OperationStateError, match="index zero|new recovery|entry"):
        module.validate_operation_state(current, operation_dir, previous)


@pytest.mark.parametrize("kind", ["install", "manual_rollback", "policy"])
def test_recovery_cursor_hashes_are_bound_to_restore_provenance(
    tmp_path: Path,
    kind: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    if kind == "install":
        state = install_recovery_state(operation_dir, "recovering")
        state["recovery"]["previous_sha256"] = HASH_B
        state["recovery"]["intended_sha256"] = HASH_B
    elif kind == "manual_rollback":
        state = manual_rollback_state(operation_dir, "manual_rollback")
        state["recovery"]["previous_sha256"] = HASH_B
        state["recovery"]["intended_sha256"] = HASH_B
    else:
        state = failed_transaction_state(operation_dir, "policy", "recovering_policy")
        state["recovery"]["previous_sha256"] = None
        state["recovery"]["intended_sha256"] = None

    with pytest.raises(module.OperationStateError, match="recovery.*hash|provenance"):
        module.validate_operation_state(state, operation_dir)


def test_recovery_receipt_is_null_before_first_recovery_history(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "source_verified")
    state["recovery"] = recovery_receipt(
        "install",
        current_target=TARGETS[0],
        previous_sha256=HASH_A,
        intended_sha256=HASH_A,
    )

    with pytest.raises(module.OperationStateError, match="recovery.*null|recovery.*history"):
        module.validate_operation_state(state, operation_dir)


def test_stable_return_preserves_completed_latest_recovery_receipt(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    _, state = successful_policy_reset(operation_dir)
    state["recovery"] = None

    with pytest.raises(module.OperationStateError, match="recovery"):
        module.validate_operation_state(state, operation_dir)


def test_phase_change_preserves_active_transaction_identity_and_progress(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    previous = state_at_phase(operation_dir, "probing")
    current = failed_transaction_state(operation_dir, "probe", "recovery_required")
    current["active_transaction"]["probe"]["prefix"] = "rebound-prefix/"

    with pytest.raises(module.OperationStateError, match="active_transaction|probe.*identity"):
        module.validate_operation_state(current, operation_dir, previous)

    previous = state_at_phase(operation_dir, "probing")
    previous["active_transaction"]["guard"]["timer_stopped"] = True
    current = failed_transaction_state(operation_dir, "probe", "recovery_required")
    with pytest.raises(module.OperationStateError, match="guard|progress"):
        module.validate_operation_state(current, operation_dir, previous)


def test_recovering_guard_stable_return_requires_completed_recovery(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    previous = failed_transaction_state(operation_dir, "guard_dry_run", "recovering_guard")
    mark_transaction_complete(previous)
    current = copy.deepcopy(previous)
    current["active_transaction"] = None
    append_phase(current, "probed", 1_750_000_130)

    with pytest.raises(module.OperationStateError, match="recovery.*completion"):
        module.validate_operation_state(current, operation_dir, previous)


def test_context_validator_rejects_rebound_derived_operation_paths(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "source_verified")
    paths = module.build_operation_paths(operation_dir)
    for field in ("source_archive", "source_dir", "snapshot_dir", "staged_dir", "state_file"):
        rebound = dataclasses.replace(paths, **{field: tmp_path / f"rebound-{field}"})
        context = module.OperationsContext(
            operation_id=operation_dir.name,
            paths=rebound,
            effective_uid=uid,
            command_runner=lambda argv, pass_fds: None,
            clock=lambda: datetime.now(timezone.utc),
            expected_commit="1" * 40,
            expected_archive_sha256=HASH_B,
            expected_manifest_sha256=HASH_C,
            host_root=tmp_path,
        )
        with pytest.raises(module.OperationStateError, match="paths"):
            module.validate_operation_state_for_context(state, context)


@pytest.mark.parametrize(
    ("raw", "forbidden_tail"),
    [
        ("password='hello world'", "world"),
        ("token: abc def ghi", "def ghi"),
        ('authorization="Bearer abc def"', "abc def"),
    ],
)
def test_sanitizer_redacts_complete_assignment_value_without_tail(
    raw: str,
    forbidden_tail: str,
) -> None:
    module = load_ops_helper()

    sanitized = module.sanitize_error_text(RuntimeError(raw))

    assert forbidden_tail not in sanitized
    module._reject_residual_secrets({"error": sanitized})


def test_atomic_writer_rechecks_destination_after_before_replace_hook(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    old = state_at_phase(operation_dir, "source_verified")
    state_file = write_state_file(operation_dir, old)
    new = state_at_phase(operation_dir, "staging_prepared")
    raced_bytes = json.dumps(old, indent=2).encode("utf-8")

    def race(event: str, **kwargs: object) -> None:
        if event == "before_replace":
            state_file.write_bytes(raced_bytes)
            state_file.chmod(0o600)

    monkeypatch.setattr(module, "_atomic_event_hook", race)

    with pytest.raises(module.OperationStateError, match="compare-and-swap"):
        module.atomic_write_operation_state(state_file, new, effective_uid=uid)

    assert state_file.read_bytes() == raced_bytes


def test_posix_atomic_writer_uses_exclusive_flock_and_final_cas() -> None:
    module = load_ops_helper()
    source = inspect.getsource(module._atomic_write_posix)

    assert "flock" in inspect.getsource(module)
    assert source.index('"before_replace"') < source.rindex("_cas_matches_posix")


def completed_probe_recovery_then_new_probe(
    operation_dir: Path,
) -> tuple[dict[str, object], dict[str, object]]:
    recovering = failed_transaction_state(operation_dir, "probe", "recovering_probe")
    mark_transaction_complete(recovering)
    installed = copy.deepcopy(recovering)
    installed["active_transaction"] = None
    installed["recovery"]["completed_epoch"] = 1_750_000_130
    append_phase(installed, "installed", 1_750_000_130)
    probing = copy.deepcopy(installed)
    probing["active_transaction"] = active_transaction("probe", "installed", 1_750_000_140)
    append_phase(probing, "probing", 1_750_000_140)
    return installed, probing


def test_later_same_kind_recovery_is_new_attempt_with_nondecreasing_epoch(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    installed, probing = completed_probe_recovery_then_new_probe(operation_dir)
    module.validate_operation_state(probing, operation_dir, installed)

    required = copy.deepcopy(probing)
    append_phase(required, "recovery_required", 1_750_000_150)
    required["recovery"] = recovery_receipt(
        "probe",
        started_epoch=1_750_000_150,
    )
    required["recovery"]["evidence_sha256"] = HASH_B
    module.validate_operation_state(required, operation_dir, probing)

    regressed = copy.deepcopy(required)
    regressed["recovery"]["started_epoch"] = 1_750_000_120
    with pytest.raises(module.OperationStateError, match="epoch|start"):
        module.validate_operation_state(regressed, operation_dir, probing)

    rebound = copy.deepcopy(probing)
    rebound["recovery"]["completed_epoch"] += 1
    with pytest.raises(module.OperationStateError, match="completed recovery"):
        module.validate_operation_state(rebound, operation_dir, installed)


@pytest.mark.skipif(os.name != "posix", reason="production operation lock uses POSIX flock")
def test_posix_flock_serializes_cooperating_atomic_writers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    state_file = write_state_file(operation_dir, state_at_phase(operation_dir, "source_verified"))
    first_paused = threading.Event()
    release_first = threading.Event()
    second_done = threading.Event()
    failures: list[BaseException] = []

    def hook(event: str, **kwargs: object) -> None:
        if threading.current_thread().name == "first-writer" and event == "before_replace":
            first_paused.set()
            assert release_first.wait(timeout=5)

    monkeypatch.setattr(module, "_atomic_event_hook", hook)

    def write(state: dict[str, object], done: threading.Event | None = None) -> None:
        try:
            module.atomic_write_operation_state(state_file, state, effective_uid=uid)
        except BaseException as exc:
            failures.append(exc)
        finally:
            if done is not None:
                done.set()

    first = threading.Thread(
        target=write,
        args=(state_at_phase(operation_dir, "staging_prepared"),),
        name="first-writer",
    )
    second = threading.Thread(
        target=write,
        args=(state_at_phase(operation_dir, "snapshotted"), second_done),
        name="second-writer",
    )
    first.start()
    assert first_paused.wait(timeout=5)
    second.start()
    assert not second_done.wait(timeout=0.2)
    release_first.set()
    first.join(timeout=5)
    second.join(timeout=5)

    assert not first.is_alive() and not second.is_alive()
    assert failures == []
    assert module.load_operation_state(state_file, effective_uid=uid)["phase"] == "snapshotted"


def test_manual_rollback_previous_hash_is_exact_installed_live_hash(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    state = manual_rollback_state(operation_dir, "manual_rollback")
    state["snapshot"]["targets"][TARGETS[0]]["sha256"] = HASH_B
    state["recovery"]["previous_sha256"] = HASH_B
    state["recovery"]["intended_sha256"] = HASH_B

    with pytest.raises(module.OperationStateError, match="installed.*provenance|previous hash"):
        module.validate_operation_state(state, operation_dir)


def test_context_free_state_rejects_recovery_start_before_latest_attempt_entry(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    _, probing = completed_probe_recovery_then_new_probe(operation_dir)
    state = copy.deepcopy(probing)
    append_phase(state, "recovery_required", 1_750_000_150)
    state["recovery"] = recovery_receipt("probe", started_epoch=1_750_000_120)
    state["recovery"]["evidence_sha256"] = HASH_B

    with pytest.raises(module.OperationStateError, match="recovery.*start|attempt.*epoch"):
        module.validate_operation_state(state, operation_dir)


def test_policy_transaction_requires_precommitted_enabled_environment_hash(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "policy_enabling")
    state["active_transaction"]["policy_environment_sha256"] = HASH_B

    module.validate_operation_state(state, operation_dir)

    for bad_value in (None, "B" * 64):
        invalid = copy.deepcopy(state)
        invalid["active_transaction"]["policy_environment_sha256"] = bad_value
        with pytest.raises(module.OperationStateError, match="policy_environment_sha256"):
            module.validate_operation_state(invalid, operation_dir)


def test_non_policy_transaction_requires_null_policy_environment_hash(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "probing")
    state["active_transaction"]["policy_environment_sha256"] = HASH_B

    with pytest.raises(module.OperationStateError, match="policy_environment_sha256"):
        module.validate_operation_state(state, operation_dir)


def test_policy_success_receipt_must_copy_precommitted_environment_hash(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    previous = state_at_phase(operation_dir, "policy_enabling")
    previous["active_transaction"]["policy_environment_sha256"] = HASH_B
    mark_transaction_complete(previous)
    current = state_at_phase(operation_dir, "policy_enabled")
    module.validate_operation_state(current, operation_dir, previous)

    current["policy"]["environment_sha256"] = HASH_C
    with pytest.raises(module.OperationStateError, match="policy.*environment"):
        module.validate_operation_state(current, operation_dir, previous)


def test_policy_recovery_accepts_only_precommitted_enabled_or_disabled_live_hash(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    state = failed_transaction_state(operation_dir, "policy", "recovering_policy")
    state["active_transaction"]["policy_environment_sha256"] = HASH_B
    state["recovery"]["previous_sha256"] = HASH_B
    state["recovery"]["intended_sha256"] = HASH_C
    module.validate_operation_state(state, operation_dir)

    invalid = copy.deepcopy(state)
    invalid["recovery"]["previous_sha256"] = HASH_A
    with pytest.raises(module.OperationStateError, match="policy.*provenance"):
        module.validate_operation_state(invalid, operation_dir)


def test_policy_environment_hash_is_immutable_through_recovery(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    previous = state_at_phase(operation_dir, "policy_enabling")
    previous["active_transaction"]["policy_environment_sha256"] = HASH_B
    current = failed_transaction_state(operation_dir, "policy", "recovering_policy")
    current["active_transaction"]["policy_environment_sha256"] = HASH_A

    with pytest.raises(module.OperationStateError, match="active_transaction|policy_environment"):
        module.validate_operation_state(current, operation_dir, previous)


def test_context_free_recovery_start_must_equal_latest_attempt_epoch(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    _, probing = completed_probe_recovery_then_new_probe(operation_dir)
    state = copy.deepcopy(probing)
    append_phase(state, "recovery_required", 1_750_000_150)
    state["recovery"] = recovery_receipt("probe", started_epoch=1_750_000_151)
    state["recovery"]["evidence_sha256"] = HASH_B

    with pytest.raises(module.OperationStateError, match="recovery.*start|attempt.*epoch"):
        module.validate_operation_state(state, operation_dir)


def test_atomic_writer_rejects_oversized_state_before_any_temp_or_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    old = state_at_phase(operation_dir, "source_verified")
    state_file = write_state_file(operation_dir, old)
    old_bytes = state_file.read_bytes()
    oversized = state_at_phase(operation_dir, "staging_prepared")
    oversized["effective_config"]["SAFE_PADDING"] = "x" * (module._MAX_STATE_BYTES + 1)
    events: list[str] = []
    monkeypatch.setattr(module, "_atomic_event_hook", lambda event, **kwargs: events.append(event))

    with pytest.raises(module.OperationStateError, match="size limit"):
        module.atomic_write_operation_state(state_file, oversized, effective_uid=uid)

    assert state_file.read_bytes() == old_bytes
    assert events == []
    assert not any(path.name.startswith(".operation-state.json.") for path in operation_dir.iterdir())


def test_atomic_writer_rejects_post_replace_destination_inode_substitution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    state_file = write_state_file(operation_dir, state_at_phase(operation_dir, "source_verified"))
    new = state_at_phase(operation_dir, "staging_prepared")

    def race(event: str, **kwargs: object) -> None:
        if event == "after_replace":
            replacement = operation_dir / "replacement.json"
            replacement.write_bytes(canonical_state_bytes(new))
            replacement.chmod(0o600)
            os.replace(replacement, state_file)

    monkeypatch.setattr(module, "_atomic_event_hook", race)

    with pytest.raises(module.OperationStateError, match="destination.*inode|temporary inode"):
        module.atomic_write_operation_state(state_file, new, effective_uid=uid)


def test_atomic_writer_revalidates_lexical_operation_directory_after_replace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    state_file = write_state_file(operation_dir, state_at_phase(operation_dir, "source_verified"))
    new = state_at_phase(operation_dir, "staging_prepared")
    moved = tmp_path / "moved-operation"

    def race(event: str, **kwargs: object) -> None:
        if event == "after_replace":
            operation_dir.rename(moved)
            operation_dir.mkdir(mode=0o700)
            operation_dir.chmod(0o700)
            rebound = operation_dir / "operation-state.json"
            rebound.write_bytes(canonical_state_bytes(new))
            rebound.chmod(0o600)

    monkeypatch.setattr(module, "_atomic_event_hook", race)

    with pytest.raises(module.OperationStateError, match="operation directory.*binding"):
        module.atomic_write_operation_state(state_file, new, effective_uid=uid)


@pytest.mark.parametrize(
    "raw",
    [
        "refresh_token=supersecretvalue",
        "access_token: supersecretvalue",
        "client_secret=supersecretvalue",
        "oauth_token=supersecretvalue",
        '"refresh_token": "supersecretvalue"',
    ],
)
def test_compound_oauth_credentials_are_redacted_and_rejected(raw: str, tmp_path: Path) -> None:
    module = load_ops_helper()
    sanitized = module.sanitize_error_text(RuntimeError(raw))
    assert "supersecretvalue" not in sanitized
    module._reject_residual_secrets({"error": sanitized})

    operation_dir, _ = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "probing")
    state["failure"] = {
        "phase": "probing",
        "primary_error": raw,
        "epoch": 1_750_000_051,
        "evidence_sha256": HASH_A,
    }
    with pytest.raises(module.OperationStateError, match="secret"):
        module.validate_operation_state(state, operation_dir)


def test_cli_redacts_compound_oauth_credential_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = load_ops_helper()
    raw_path = "/opt/degen/backups/config/20260630T235959Z"
    monkeypatch.setattr(module, "_effective_uid", lambda: 0)
    monkeypatch.setattr(module, "_require_posix_descriptor_primitives", lambda: None)

    def fail(*args: object, **kwargs: object) -> dict[str, object]:
        raise RuntimeError("client_secret=supersecretvalue")

    monkeypatch.setattr(module, "load_operation_state", fail)
    result = module.main(["show-state", "--operation-dir", raw_path])
    captured = capsys.readouterr()
    assert result == 1
    assert "supersecretvalue" not in captured.err


def test_installing_to_installed_requires_prior_terminal_cursor(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    previous = state_at_phase(operation_dir, "installing")
    current = state_at_phase(operation_dir, "installed")

    with pytest.raises(module.OperationStateError, match="prior.*cursor|install.*terminal"):
        module.validate_operation_state(current, operation_dir, previous)


def test_guarded_transaction_entry_requires_zero_progress(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    previous = state_at_phase(operation_dir, "installed")
    current = state_at_phase(operation_dir, "probing")
    mark_transaction_complete(current)

    with pytest.raises(module.OperationStateError, match="entry.*zero|initial.*progress"):
        module.validate_operation_state(current, operation_dir, previous)


def test_same_phase_guard_progress_advances_at_most_one_step(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    previous = state_at_phase(operation_dir, "probing")
    current = copy.deepcopy(previous)
    current["active_transaction"]["guard"]["timer_stopped"] = True
    current["active_transaction"]["guard"]["service_inactive_verified"] = True

    with pytest.raises(module.OperationStateError, match="one.*guard|progress.*step"):
        module.validate_operation_state(current, operation_dir, previous)


def test_same_phase_probe_object_progress_advances_at_most_one_flag(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    previous = state_at_phase(operation_dir, "probing")
    current = copy.deepcopy(previous)
    item = current["active_transaction"]["probe"]["objects"][0]
    item["created"] = True
    item["verified"] = True

    with pytest.raises(module.OperationStateError, match="one.*probe|progress.*flag"):
        module.validate_operation_state(current, operation_dir, previous)


def test_policy_enabled_digest_must_differ_from_disabled_staged_digest(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "policy_enabling")
    state["active_transaction"]["policy_environment_sha256"] = HASH_C

    with pytest.raises(module.OperationStateError, match="enabled.*disabled|policy_environment"):
        module.validate_operation_state(state, operation_dir)


def test_context_free_guard_recovery_returns_only_to_recorded_prior_stable_phase(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    state = failed_transaction_state(operation_dir, "guard_observe", "recovering_guard")
    mark_transaction_complete(state)
    state["active_transaction"] = None
    state["recovery"]["completed_epoch"] = 1_750_000_130
    state["dry_run"] = None
    state["policy"] = None
    state["observation"] = None
    append_phase(state, "probed", 1_750_000_130)

    with pytest.raises(module.OperationStateError, match="guard.*prior|history.*recovery"):
        module.validate_operation_state(state, operation_dir)


def test_stable_policy_receipt_digest_must_differ_from_disabled_staged_digest(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "policy_enabled")
    state["policy"]["environment_sha256"] = HASH_C

    with pytest.raises(module.OperationStateError, match="enabled.*disabled|policy.*digest"):
        module.validate_operation_state(state, operation_dir)


def test_install_completion_preserves_original_started_epoch(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    previous = state_at_phase(operation_dir, "installing")
    previous["install"].update(
        {
            "next_target_index": len(TARGETS),
            "current_target": None,
            "previous_sha256": None,
            "intended_sha256": None,
        }
    )
    current = state_at_phase(operation_dir, "installed")
    current["install"]["started_epoch"] += 1

    with pytest.raises(module.OperationStateError, match="install.*start|started_epoch|immutable"):
        module.validate_operation_state(current, operation_dir, previous)
