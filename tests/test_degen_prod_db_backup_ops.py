from __future__ import annotations

import ast
import copy
import contextlib
import dataclasses
import hashlib
import importlib.util
import inspect
import io
import json
import os
import shutil
import stat
import subprocess
import sys
import tarfile
import threading
from datetime import datetime, timezone
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
OPS_HELPER = ROOT / "deploy" / "linux" / "degen-prod-db-backup-ops.py"
ENV_HELPER = ROOT / "deploy" / "linux" / "degen-prod-db-backup-env.py"
ENV_EXAMPLE = ROOT / "deploy" / "systemd" / "degen-prod-db-backup.env.example"
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
SOURCE_MANIFEST = "deploy/linux/degen-prod-db-backup-assets.sha256"
SOURCE_COMMIT = "1" * 40
EFFECTIVE_CONFIG = {
    "APP_ENV_FILE": "/opt/degen/web.env",
    "BACKUP_DIR": "/opt/degen/backups/db",
    "LOG_DIR": "/var/log/degen",
    "RCLONE_CONFIG": "/etc/degen/rclone.conf",
    "RCLONE_REMOTE_PATH": "onedrive:backups/degen-db",
    "KEEP_LOCAL_COUNT": "2",
    "KEEP_REMOTE_DAILY": "7",
    "KEEP_REMOTE_WEEKLY": "4",
    "KEEP_REMOTE_MONTHLY": "3",
    "REMOTE_PRUNE_ENABLED": "0",
    "MIN_FREE_AFTER_BYTES": "10737418240",
    "RETENTION_PLANNER": "/usr/local/sbin/degen-prod-db-retention",
    "LOCK_FILE": "/run/degen-prod-db-backup/backup.lock",
    "BACKUP_PREFIX": "degen_green_",
}


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


def source_asset_bytes() -> dict[str, bytes]:
    return {
        asset: f"reviewed fixture bytes for {asset}\n".encode("ascii")
        for asset in SOURCE_ASSETS
    }


def source_manifest_bytes(asset_bytes: dict[str, bytes]) -> bytes:
    return b"".join(
        hashlib.sha256(asset_bytes[asset]).hexdigest().encode("ascii")
        + b"  "
        + asset.encode("ascii")
        + b"\n"
        for asset in sorted(asset_bytes)
    )


def required_source_directories() -> tuple[str, ...]:
    directories: set[str] = set()
    for name in (*SOURCE_ASSETS, SOURCE_MANIFEST):
        parent = Path(name).parent
        while str(parent) not in ("", "."):
            directories.add(parent.as_posix())
            parent = parent.parent
    return tuple(sorted(directories, key=lambda value: (value.count("/"), value)))


def valid_archive_entries(
    asset_bytes: dict[str, bytes],
    manifest_bytes: bytes,
) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = [
        {"name": name, "type": tarfile.DIRTYPE, "data": b"", "linkname": ""}
        for name in required_source_directories()
    ]
    entries.extend(
        {"name": name, "type": tarfile.REGTYPE, "data": data, "linkname": ""}
        for name, data in sorted({**asset_bytes, SOURCE_MANIFEST: manifest_bytes}.items())
    )
    return entries


def write_source_archive(
    path: Path,
    entries: list[dict[str, object]],
    *,
    pax_headers: dict[str, str] | None = None,
) -> None:
    headers = {"comment": SOURCE_COMMIT} if pax_headers is None else pax_headers
    with tarfile.open(path, "w", format=tarfile.PAX_FORMAT, pax_headers=headers) as archive:
        for entry in entries:
            name = str(entry["name"])
            member_type = entry["type"]
            assert isinstance(member_type, bytes)
            data = entry.get("data", b"")
            assert isinstance(data, bytes)
            info = tarfile.TarInfo(name)
            info.type = member_type
            info.mode = (
                0o775
                if member_type == tarfile.DIRTYPE
                or name in SOURCE_ASSETS[:4]
                else 0o664
            )
            info.uid = 0
            info.gid = 0
            info.uname = "root"
            info.gname = "root"
            info.mtime = 1_700_000_000
            info.linkname = str(entry.get("linkname", ""))
            if member_type == tarfile.REGTYPE:
                info.size = len(data)
                archive.addfile(info, io.BytesIO(data))
            else:
                info.size = 0
                if member_type in (tarfile.CHRTYPE, tarfile.BLKTYPE):
                    info.devmajor = 1
                    info.devminor = 3
                archive.addfile(info)
    raw = bytearray(path.read_bytes())
    assert raw[156:157] == tarfile.XGLTYPE
    raw[0:100] = b"pax_global_header".ljust(100, b"\0")
    raw[100:108] = b"0000666\0"
    raw[108:116] = b"0000000\0"
    raw[116:124] = b"0000000\0"
    raw[136:148] = f"{1_700_000_000:011o}\0".encode("ascii")
    raw[265:297] = b"root".ljust(32, b"\0")
    raw[297:329] = b"root".ljust(32, b"\0")
    raw[329:337] = b"0000000\0"
    raw[337:345] = b"0000000\0"
    offset = 0
    while offset < len(raw):
        header = raw[offset : offset + 512]
        if not any(header):
            break
        size_field = header[124:136].rstrip(b"\0 ").lstrip(b" ") or b"0"
        size = int(size_field, 8)
        raw[offset + 329 : offset + 337] = b"0000000\0"
        raw[offset + 337 : offset + 345] = b"0000000\0"
        raw[offset + 148 : offset + 156] = b"        "
        checksum = sum(raw[offset : offset + 512])
        raw[offset + 148 : offset + 156] = f"{checksum:07o}\0".encode("ascii")
        offset += 512 + ((size + 511) // 512) * 512
    path.write_bytes(raw)
    path.chmod(0o600)


def mutate_source_tar_header(
    path: Path,
    member_name: str,
    start: int,
    replacement: bytes,
) -> None:
    raw = bytearray(path.read_bytes())
    offset = 0
    while offset < len(raw):
        header = raw[offset : offset + 512]
        if not any(header):
            break
        name = header[:100].split(b"\0", 1)[0].decode("ascii")
        size_field = header[124:136].rstrip(b"\0 ").lstrip(b" ") or b"0"
        size = int(size_field, 8)
        if name == member_name:
            raw[offset + start : offset + start + len(replacement)] = replacement
            raw[offset + 148 : offset + 156] = b"        "
            checksum = sum(raw[offset : offset + 512])
            raw[offset + 148 : offset + 156] = f"{checksum:07o}\0".encode("ascii")
            path.write_bytes(raw)
            path.chmod(0o600)
            return
        offset += 512 + ((size + 511) // 512) * 512
    raise AssertionError(f"missing tar member {member_name}")


def write_extracted_source(
    source_dir: Path,
    asset_bytes: dict[str, bytes],
    manifest_bytes: bytes,
) -> None:
    source_dir.mkdir(mode=0o700)
    source_dir.chmod(0o700)
    for directory in required_source_directories():
        path = source_dir / directory
        path.mkdir(exist_ok=True)
        if os.name == "posix":
            path.chmod(0o755)
    for name, data in {**asset_bytes, SOURCE_MANIFEST: manifest_bytes}.items():
        path = source_dir / name
        path.write_bytes(data)
        if os.name == "posix":
            path.chmod(0o644)


def source_verification_fixture(
    module: object,
    tmp_path: Path,
    *,
    asset_bytes: dict[str, bytes] | None = None,
    manifest_bytes: bytes | None = None,
    mutate_entries: object | None = None,
    archive_suffix: bytes = b"",
    pax_headers: dict[str, str] | None = None,
    mutate_archive: object | None = None,
) -> tuple[object, dict[str, bytes], bytes, list[tuple[tuple[str, ...], tuple[int, ...]]]]:
    operation_dir, uid = private_operation_dir(tmp_path)
    paths = module.build_operation_paths(operation_dir)
    assets = source_asset_bytes() if asset_bytes is None else dict(asset_bytes)
    manifest = source_manifest_bytes(assets) if manifest_bytes is None else manifest_bytes
    write_extracted_source(paths.source_dir, assets, manifest)
    entries = valid_archive_entries(assets, manifest)
    if mutate_entries is not None:
        mutate_entries(entries)
    write_source_archive(paths.source_archive, entries, pax_headers=pax_headers)
    if archive_suffix:
        with paths.source_archive.open("ab") as stream:
            stream.write(archive_suffix)
    if mutate_archive is not None:
        mutate_archive(paths.source_archive)
    archive_bytes = paths.source_archive.read_bytes()
    calls: list[tuple[tuple[str, ...], tuple[int, ...]]] = []

    def command_runner(
        argv: object,
        pass_fds: tuple[int, ...],
    ) -> subprocess.CompletedProcess[str]:
        argv_tuple = tuple(str(value) for value in argv)
        calls.append((argv_tuple, pass_fds))
        assert len(pass_fds) == 1
        assert "get-tar-commit-id" in argv_tuple
        assert str(paths.source_archive) not in argv_tuple
        assert SOURCE_COMMIT not in argv_tuple
        assert hashlib.sha256(archive_bytes).hexdigest() not in argv_tuple
        position = os.lseek(pass_fds[0], 0, os.SEEK_CUR)
        os.lseek(pass_fds[0], 0, os.SEEK_SET)
        observed = bytearray()
        while True:
            chunk = os.read(pass_fds[0], 64 * 1024)
            if not chunk:
                break
            observed.extend(chunk)
        os.lseek(pass_fds[0], position, os.SEEK_SET)
        assert bytes(observed) == archive_bytes
        return subprocess.CompletedProcess(argv_tuple, 0, SOURCE_COMMIT + "\n", "")

    context = module.OperationsContext(
        operation_id=operation_dir.name,
        paths=paths,
        effective_uid=uid,
        command_runner=command_runner,
        clock=lambda: datetime(2026, 7, 1, 12, 34, 56, tzinfo=timezone.utc),
        expected_commit=SOURCE_COMMIT,
        expected_archive_sha256=hashlib.sha256(archive_bytes).hexdigest(),
        expected_manifest_sha256=hashlib.sha256(manifest).hexdigest(),
        host_root=tmp_path,
    )
    return context, assets, manifest, calls


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
    state["effective_config"] = dict(EFFECTIVE_CONFIG)
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
        "verify_source_archive",
        "sanitize_error_text",
    ):
        assert callable(getattr(module, name))


def test_cli_exposes_exact_show_state_verify_source_and_prepare_staging_subcommands() -> None:
    help_result = subprocess.run(
        [sys.executable, str(OPS_HELPER), "--help"],
        text=True,
        capture_output=True,
        check=False,
    )
    unknown_result = subprocess.run(
        [sys.executable, str(OPS_HELPER), "unknown-command", "--operation-dir", "/tmp/nope"],
        text=True,
        capture_output=True,
        check=False,
    )

    assert help_result.returncode == 0
    assert "show-state" in help_result.stdout
    assert "verify-source" in help_result.stdout
    assert "prepare-staging" in help_result.stdout
    assert unknown_result.returncode == 2
    assert unknown_result.stdout == ""


def test_verify_source_cli_has_exact_required_approval_shape_and_no_override() -> None:
    help_result = subprocess.run(
        [sys.executable, str(OPS_HELPER), "verify-source", "--help"],
        text=True,
        capture_output=True,
        check=False,
    )

    assert help_result.returncode == 0
    for option in (
        "--operation-dir",
        "--archive",
        "--expected-commit",
        "--expected-archive-sha256",
        "--expected-manifest-sha256",
    ):
        assert option in help_result.stdout
    assert "host-root" not in help_result.stdout
    assert "test" not in help_result.stdout.lower()


def test_verify_source_cli_rejects_duplicate_approval_flags() -> None:
    module = load_ops_helper()
    operation_dir = "/opt/degen/backups/config/20260701T123456Z"

    with pytest.raises(SystemExit) as raised:
        module._build_parser().parse_args(
            [
                "verify-source",
                "--operation-dir",
                operation_dir,
                "--archive",
                operation_dir + "/source.tar",
                "--expected-commit",
                SOURCE_COMMIT,
                "--expected-commit",
                "2" * 40,
                "--expected-archive-sha256",
                HASH_A,
                "--expected-manifest-sha256",
                HASH_B,
            ]
        )

    assert raised.value.code == 2


def test_verify_source_cli_binds_all_approvals_to_one_context(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = load_ops_helper()
    operation_dir = "/opt/degen/backups/config/20260701T123456Z"
    archive = operation_dir + "/source.tar"
    observed: list[tuple[object, Path]] = []
    monkeypatch.setattr(module, "_effective_uid", lambda: 0)
    monkeypatch.setattr(module, "_require_posix_descriptor_primitives", lambda: None)
    monkeypatch.setattr(
        module,
        "verify_source_archive",
        lambda context, *, source_dir: observed.append((context, source_dir)) or {},
    )

    result = module.main(
        [
            "verify-source",
            "--operation-dir",
            operation_dir,
            "--archive",
            archive,
            "--expected-commit",
            SOURCE_COMMIT,
            "--expected-archive-sha256",
            HASH_A,
            "--expected-manifest-sha256",
            HASH_B,
        ]
    )

    captured = capsys.readouterr()
    assert result == 0
    assert captured.out == ""
    assert captured.err == ""
    assert len(observed) == 1
    context, source_dir = observed[0]
    assert context.paths == module.build_operation_paths(Path(operation_dir))
    assert context.expected_commit == SOURCE_COMMIT
    assert context.expected_archive_sha256 == HASH_A
    assert context.expected_manifest_sha256 == HASH_B
    assert context.host_root == Path("/")
    assert source_dir == context.paths.source_dir


def test_verify_source_cli_rejects_archive_path_rebinding_before_verification(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = load_ops_helper()
    operation_dir = "/opt/degen/backups/config/20260701T123456Z"
    called = False
    monkeypatch.setattr(module, "_effective_uid", lambda: 0)
    monkeypatch.setattr(module, "_require_posix_descriptor_primitives", lambda: None)

    def verify(*args: object, **kwargs: object) -> dict[str, str]:
        nonlocal called
        called = True
        return {}

    monkeypatch.setattr(module, "verify_source_archive", verify)

    result = module.main(
        [
            "verify-source",
            "--operation-dir",
            operation_dir,
            "--archive",
            operation_dir + "/other.tar",
            "--expected-commit",
            SOURCE_COMMIT,
            "--expected-archive-sha256",
            HASH_A,
            "--expected-manifest-sha256",
            HASH_B,
        ]
    )

    captured = capsys.readouterr()
    assert result == 1
    assert not called
    assert captured.out == ""
    assert "archive" in captured.err.lower()


def test_archive_digest_failure_happens_before_git_or_extracted_tree_access(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    context, _, _, calls = source_verification_fixture(module, tmp_path)
    shutil.rmtree(context.paths.source_dir)
    context = dataclasses.replace(context, expected_archive_sha256="0" * 64)

    with pytest.raises(module.OperationStateError, match="archive.*SHA-256|archive.*digest"):
        module.verify_source_archive(context, source_dir=context.paths.source_dir)

    assert calls == []
    assert not context.paths.state_file.exists()


def test_source_directory_argument_cannot_rebind_the_context_destination(tmp_path: Path) -> None:
    module = load_ops_helper()
    context, _, _, calls = source_verification_fixture(module, tmp_path)

    with pytest.raises(module.OperationStateError, match="source.*directory|source_dir"):
        module.verify_source_archive(context, source_dir=tmp_path / "other-source")

    assert calls == []
    assert not context.paths.state_file.exists()


def test_source_verification_binds_commit_archive_and_approved_manifest_to_state(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    context, assets, _, calls = source_verification_fixture(module, tmp_path)
    expected_hashes = {
        name: hashlib.sha256(contents).hexdigest() for name, contents in assets.items()
    }

    result = module.verify_source_archive(context, source_dir=context.paths.source_dir)

    assert result == expected_hashes
    assert len(calls) == 1
    state = module.load_operation_state(context.paths.state_file, effective_uid=context.effective_uid)
    reviewed_source = state["reviewed_source"]
    assert reviewed_source == {
        "commit": context.expected_commit,
        "archive_sha256": context.expected_archive_sha256,
        "manifest_sha256": context.expected_manifest_sha256,
        "asset_hashes": expected_hashes,
    }
    evidence = (
        b"degen-source-verification-v1\n"
        + json.dumps(
            reviewed_source,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
        + b"\n"
    )
    assert state["phase_history"] == [
        {
            "phase": "source_verified",
            "epoch": 1_782_909_296,
            "evidence_sha256": hashlib.sha256(evidence).hexdigest(),
        }
    ]
    module.validate_operation_state_for_context(state, context)


def test_source_verification_repeat_revalidates_then_returns_without_clock_or_rewrite(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    context, assets, _, calls = source_verification_fixture(module, tmp_path)
    expected = {
        name: hashlib.sha256(contents).hexdigest() for name, contents in assets.items()
    }
    module.verify_source_archive(context, source_dir=context.paths.source_dir)
    before = context.paths.state_file.read_bytes()

    def clock_must_not_run() -> datetime:
        raise AssertionError("repeat source verification must preserve the existing receipt epoch")

    repeated_context = dataclasses.replace(context, clock=clock_must_not_run)
    result = module.verify_source_archive(
        repeated_context,
        source_dir=repeated_context.paths.source_dir,
    )

    assert result == expected
    assert len(calls) == 2
    assert repeated_context.paths.state_file.read_bytes() == before


def test_source_verification_repeat_rejects_forged_evidence_without_rewrite(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    context, _, _, _ = source_verification_fixture(module, tmp_path)
    module.verify_source_archive(context, source_dir=context.paths.source_dir)
    state = module.load_operation_state(context.paths.state_file, effective_uid=context.effective_uid)
    history = state["phase_history"]
    assert isinstance(history, list) and isinstance(history[0], dict)
    assert history[0]["evidence_sha256"] != HASH_A
    history[0]["evidence_sha256"] = HASH_A
    write_state_file(context.paths.operation_dir, state)
    before = context.paths.state_file.read_bytes()

    with pytest.raises(module.OperationStateError, match="evidence"):
        module.verify_source_archive(context, source_dir=context.paths.source_dir)

    assert context.paths.state_file.read_bytes() == before


@pytest.mark.parametrize(
    ("field", "bad_value"),
    [
        ("expected_commit", "2" * 40),
        ("expected_archive_sha256", "0" * 64),
        ("expected_manifest_sha256", "f" * 64),
    ],
)
def test_source_verification_rejects_any_context_approval_mismatch_without_state(
    tmp_path: Path,
    field: str,
    bad_value: str,
) -> None:
    module = load_ops_helper()
    context, _, _, _ = source_verification_fixture(module, tmp_path)
    context = dataclasses.replace(context, **{field: bad_value})

    with pytest.raises(module.OperationStateError):
        module.verify_source_archive(context, source_dir=context.paths.source_dir)

    assert not context.paths.state_file.exists()


@pytest.mark.parametrize(
    "case",
    [
        "duplicate",
        "extra",
        "missing",
        "traversal",
        "absolute",
        "backslash",
    ],
)
def test_archive_rejects_duplicate_extra_missing_and_unsafe_member_names_without_state(
    tmp_path: Path,
    case: str,
) -> None:
    module = load_ops_helper()

    def mutate(entries: list[dict[str, object]]) -> None:
        if case == "duplicate":
            duplicate = next(entry for entry in entries if entry["name"] == SOURCE_ASSETS[0])
            entries.append(copy.deepcopy(duplicate))
        elif case == "extra":
            entries.append(
                {"name": "deploy/linux/extra.py", "type": tarfile.REGTYPE, "data": b"x", "linkname": ""}
            )
        elif case == "missing":
            entries[:] = [entry for entry in entries if entry["name"] != SOURCE_ASSETS[0]]
        else:
            unsafe = {
                "traversal": "../escape",
                "absolute": "/etc/shadow",
                "backslash": r"deploy\linux\escape.py",
            }[case]
            entries.append(
                {"name": unsafe, "type": tarfile.REGTYPE, "data": b"x", "linkname": ""}
            )

    context, _, _, _ = source_verification_fixture(module, tmp_path, mutate_entries=mutate)

    with pytest.raises(module.OperationStateError, match="archive"):
        module.verify_source_archive(context, source_dir=context.paths.source_dir)

    assert not context.paths.state_file.exists()


@pytest.mark.parametrize(
    ("label", "member_type"),
    [
        ("symlink", tarfile.SYMTYPE),
        ("hard-link", tarfile.LNKTYPE),
        ("sparse", tarfile.GNUTYPE_SPARSE),
        ("character-device", tarfile.CHRTYPE),
        ("block-device", tarfile.BLKTYPE),
        ("fifo", tarfile.FIFOTYPE),
        ("unknown", b"Z"),
    ],
)
def test_archive_rejects_every_nonregular_reviewed_asset_type_without_state(
    tmp_path: Path,
    label: str,
    member_type: bytes,
) -> None:
    module = load_ops_helper()

    def mutate(entries: list[dict[str, object]]) -> None:
        target = next(entry for entry in entries if entry["name"] == SOURCE_ASSETS[0])
        target["type"] = member_type
        target["data"] = b""
        if member_type in (tarfile.SYMTYPE, tarfile.LNKTYPE):
            target["linkname"] = SOURCE_ASSETS[1]

    context, _, _, _ = source_verification_fixture(module, tmp_path, mutate_entries=mutate)

    with pytest.raises(module.OperationStateError, match="archive"):
        module.verify_source_archive(context, source_dir=context.paths.source_dir)

    assert label
    assert not context.paths.state_file.exists()


@pytest.mark.parametrize("case", ["missing", "regular", "link"])
def test_archive_accepts_required_parent_entries_only_as_exact_real_directories(
    tmp_path: Path,
    case: str,
) -> None:
    module = load_ops_helper()

    def mutate(entries: list[dict[str, object]]) -> None:
        if case == "missing":
            entries[:] = [entry for entry in entries if entry["name"] != "deploy/linux"]
            return
        target = next(entry for entry in entries if entry["name"] == "deploy/linux")
        target["type"] = tarfile.REGTYPE if case == "regular" else tarfile.SYMTYPE
        target["linkname"] = "deploy/systemd" if case == "link" else ""

    context, _, _, _ = source_verification_fixture(module, tmp_path, mutate_entries=mutate)

    with pytest.raises(module.OperationStateError, match="archive"):
        module.verify_source_archive(context, source_dir=context.paths.source_dir)

    assert not context.paths.state_file.exists()


def test_archive_rejects_asset_bytes_that_do_not_match_the_strict_manifest(tmp_path: Path) -> None:
    module = load_ops_helper()

    def mutate(entries: list[dict[str, object]]) -> None:
        target = next(entry for entry in entries if entry["name"] == SOURCE_ASSETS[0])
        target["data"] = b"unreviewed bytes\n"

    context, _, _, _ = source_verification_fixture(module, tmp_path, mutate_entries=mutate)

    with pytest.raises(module.OperationStateError, match="archive.*hash|manifest"):
        module.verify_source_archive(context, source_dir=context.paths.source_dir)

    assert not context.paths.state_file.exists()


def test_archive_rejects_trailing_or_concatenated_payload_without_state(tmp_path: Path) -> None:
    module = load_ops_helper()
    context, _, _, _ = source_verification_fixture(
        module,
        tmp_path,
        archive_suffix=b"nonzero concatenated archive payload",
    )

    with pytest.raises(module.OperationStateError, match="archive"):
        module.verify_source_archive(context, source_dir=context.paths.source_dir)

    assert not context.paths.state_file.exists()


@pytest.mark.parametrize("suffix_size", [512, 10_240])
def test_archive_rejects_extra_zero_padding_beyond_canonical_git_record(
    tmp_path: Path,
    suffix_size: int,
) -> None:
    module = load_ops_helper()
    context, _, _, _ = source_verification_fixture(
        module,
        tmp_path,
        archive_suffix=b"\0" * suffix_size,
    )

    with pytest.raises(module.OperationStateError, match="archive.*padding|archive.*record"):
        module.verify_source_archive(context, source_dir=context.paths.source_dir)

    assert not context.paths.state_file.exists()


@pytest.mark.parametrize(
    ("label", "start", "replacement"),
    [
        ("mode", 100, b"0000755\0"),
        ("uid value", 108, b"0000001\0"),
        ("uid grammar", 108, b" 000000\0"),
        ("gid", 116, b"0000001\0"),
        ("mtime binding", 136, b"00000000001\0"),
        ("user", 265, b"admin".ljust(32, b"\0")),
        ("group", 297, b"admin".ljust(32, b"\0")),
        ("device major", 329, b"0000001\0"),
        ("device minor", 337, b"0000001\0"),
    ],
)
def test_archive_rejects_rechecksummed_noncanonical_git_header_metadata(
    tmp_path: Path,
    label: str,
    start: int,
    replacement: bytes,
) -> None:
    module = load_ops_helper()

    def mutate(path: Path) -> None:
        mutate_source_tar_header(path, SOURCE_ASSETS[0], start, replacement)

    context, _, _, _ = source_verification_fixture(
        module,
        tmp_path,
        mutate_archive=mutate,
    )

    with pytest.raises(module.OperationStateError, match="archive.*metadata|archive.*canonical"):
        module.verify_source_archive(context, source_dir=context.paths.source_dir)

    assert label
    assert not context.paths.state_file.exists()


@pytest.mark.parametrize(
    "pax_headers",
    [
        {"comment": "2" * 40},
        {"comment": SOURCE_COMMIT, "unexpected": "metadata"},
    ],
)
def test_archive_rejects_noncanonical_global_pax_metadata_without_state(
    tmp_path: Path,
    pax_headers: dict[str, str],
) -> None:
    module = load_ops_helper()
    context, _, _, _ = source_verification_fixture(module, tmp_path, pax_headers=pax_headers)

    with pytest.raises(module.OperationStateError, match="archive|commit"):
        module.verify_source_archive(context, source_dir=context.paths.source_dir)

    assert not context.paths.state_file.exists()


def _invalid_source_manifests() -> list[tuple[str, bytes]]:
    assets = source_asset_bytes()
    valid = source_manifest_bytes(assets)
    first, remainder = valid.split(b"\n", 1)
    digest, path = first.split(b"  ", 1)
    return [
        ("uppercase digest", digest.upper() + b"  " + path + b"\n" + remainder),
        ("one space", digest + b" " + path + b"\n" + remainder),
        ("three spaces", digest + b"   " + path + b"\n" + remainder),
        ("CRLF", valid.replace(b"\n", b"\r\n")),
        ("duplicate", first + b"\n" + first + b"\n" + remainder),
        ("traversal", digest + b"  ../escape\n" + remainder),
        ("absolute", digest + b"  /etc/shadow\n" + remainder),
        ("backslash", digest + b"  deploy\\linux\\file\n" + remainder),
        ("self entry", digest + b"  " + SOURCE_MANIFEST.encode("ascii") + b"\n" + remainder),
        ("blank record", b"\n" + valid),
        ("missing final LF", valid[:-1]),
    ]


@pytest.mark.parametrize(("label", "manifest_bytes"), _invalid_source_manifests())
def test_manifest_parser_rejects_noncanonical_or_unsafe_records_without_state(
    tmp_path: Path,
    label: str,
    manifest_bytes: bytes,
) -> None:
    module = load_ops_helper()
    context, _, _, _ = source_verification_fixture(
        module,
        tmp_path,
        manifest_bytes=manifest_bytes,
    )

    with pytest.raises(module.OperationStateError, match="manifest"):
        module.verify_source_archive(context, source_dir=context.paths.source_dir)

    assert label
    assert not context.paths.state_file.exists()


@pytest.mark.parametrize("case", ["extra_file", "extra_directory", "missing", "changed"])
def test_source_tree_must_contain_exact_reviewed_regular_file_bytes_without_state(
    tmp_path: Path,
    case: str,
) -> None:
    module = load_ops_helper()
    context, _, _, _ = source_verification_fixture(module, tmp_path)
    source_dir = context.paths.source_dir
    if case == "extra_file":
        (source_dir / "extra").write_bytes(b"extra")
    elif case == "extra_directory":
        (source_dir / "extra-dir").mkdir()
    elif case == "missing":
        (source_dir / SOURCE_ASSETS[0]).unlink()
    else:
        (source_dir / SOURCE_ASSETS[0]).write_bytes(b"changed bytes\n")

    with pytest.raises(module.OperationStateError, match="source"):
        module.verify_source_archive(context, source_dir=source_dir)

    assert not context.paths.state_file.exists()


def test_source_tree_rejects_symlinked_reviewed_file_without_state(tmp_path: Path) -> None:
    module = load_ops_helper()
    context, _, _, _ = source_verification_fixture(module, tmp_path)
    target = context.paths.source_dir / SOURCE_ASSETS[0]
    target.unlink()
    try:
        target.symlink_to(context.paths.source_dir / SOURCE_ASSETS[1])
    except OSError as exc:
        pytest.skip(f"symlink creation is unavailable: {exc}")

    with pytest.raises(module.OperationStateError, match="source.*link|source.*regular"):
        module.verify_source_archive(context, source_dir=context.paths.source_dir)

    assert not context.paths.state_file.exists()


def test_source_tree_rejects_hard_linked_reviewed_files_without_state(tmp_path: Path) -> None:
    module = load_ops_helper()
    context, _, _, _ = source_verification_fixture(module, tmp_path)
    first = context.paths.source_dir / SOURCE_ASSETS[0]
    second = context.paths.source_dir / SOURCE_ASSETS[1]
    first.unlink()
    os.link(second, first)

    with pytest.raises(module.OperationStateError, match="source.*link"):
        module.verify_source_archive(context, source_dir=context.paths.source_dir)

    assert not context.paths.state_file.exists()


def test_source_tree_rejects_unapproved_manifest_bytes_without_state(tmp_path: Path) -> None:
    module = load_ops_helper()
    context, _, _, _ = source_verification_fixture(module, tmp_path)
    manifest_path = context.paths.source_dir / SOURCE_MANIFEST
    manifest_path.write_bytes(manifest_path.read_bytes() + b"\n")

    with pytest.raises(module.OperationStateError, match="manifest|source"):
        module.verify_source_archive(context, source_dir=context.paths.source_dir)

    assert not context.paths.state_file.exists()


def test_source_tree_final_proof_rejects_earlier_file_mutation_after_initial_hash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, _, _, _ = source_verification_fixture(module, tmp_path)
    reviewed_paths = {
        Path(name).name: context.paths.source_dir / name
        for name in (*SOURCE_ASSETS, SOURCE_MANIFEST)
    }
    original_open = module.os.open
    first_reviewed_path: Path | None = None
    mutated = False

    def racing_open(path: object, flags: int, *args: object, **kwargs: object) -> int:
        nonlocal first_reviewed_path, mutated
        name = Path(os.fspath(path)).name
        reviewed_path = reviewed_paths.get(name)
        if reviewed_path is not None and first_reviewed_path is None:
            first_reviewed_path = reviewed_path
        elif reviewed_path is not None and not mutated and reviewed_path != first_reviewed_path:
            assert first_reviewed_path is not None
            changed = bytearray(first_reviewed_path.read_bytes())
            changed[0] = ord("0") if changed[0] != ord("0") else ord("1")
            first_reviewed_path.write_bytes(changed)
            mutated = True
        return original_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(module.os, "open", racing_open)

    with pytest.raises(module.OperationStateError, match="source.*changed|source.*proof"):
        module.verify_source_archive(context, source_dir=context.paths.source_dir)

    assert mutated
    assert not context.paths.state_file.exists()


def test_source_verification_revalidates_held_proof_at_atomic_before_replace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, _, _, _ = source_verification_fixture(module, tmp_path)
    manifest_path = context.paths.source_dir / SOURCE_MANIFEST
    original = manifest_path.read_bytes()
    mutated = False

    def race(event: str, **details: object) -> None:
        nonlocal mutated
        if event == "before_replace":
            changed = bytearray(original)
            changed[0] = ord("0") if changed[0] != ord("0") else ord("1")
            manifest_path.write_bytes(changed)
            mutated = True

    monkeypatch.setattr(module, "_atomic_event_hook", race)

    with pytest.raises(module.OperationStateError, match="source proof.*changed"):
        module.verify_source_archive(context, source_dir=context.paths.source_dir)

    assert mutated
    assert not context.paths.state_file.exists()
    assert not any(
        path.name.startswith(".operation-state.json.")
        for path in context.paths.operation_dir.iterdir()
    )


def test_source_state_private_pre_replace_validator_failure_preserves_absence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    state = source_verified_state(operation_dir)
    state_file = operation_dir / "operation-state.json"
    events: list[str] = []
    monkeypatch.setattr(
        module,
        "_atomic_event_hook",
        lambda event, **details: events.append(event),
    )

    def reject() -> None:
        events.append("pre_replace_validator")
        assert not state_file.exists()
        raise module.OperationStateError("trusted pre-replace validation failed")

    with pytest.raises(module.OperationStateError, match="pre-replace validation"):
        module._atomic_write_operation_state_internal(
            state_file,
            state,
            effective_uid=uid,
            pre_replace_validator=reject,
        )

    assert events[-2:] == ["before_replace", "pre_replace_validator"]
    assert "after_replace" not in events
    assert not state_file.exists()
    assert list(operation_dir.iterdir()) == []


@pytest.mark.parametrize(
    ("function_name", "temp_binding_name", "temp_contents_name", "cas_name"),
    [
        (
            "_atomic_write_posix",
            "_temp_identity_matches_posix",
            "_temp_contents_match_posix",
            "_cas_matches_posix",
        ),
        (
            "_atomic_write_fallback",
            "_temp_identity_matches_fallback",
            "_temp_contents_match_fallback",
            "_cas_matches_fallback",
        ),
    ],
)
def test_source_state_pre_replace_validator_is_followed_by_fresh_binding_and_cas_checks(
    function_name: str,
    temp_binding_name: str,
    temp_contents_name: str,
    cas_name: str,
) -> None:
    module = load_ops_helper()
    source = inspect.getsource(getattr(module, function_name))
    validator_index = source.index("pre_replace_validator()")
    replace_index = source.index("os.replace", validator_index)
    final_window = source[validator_index:replace_index]

    assert source.index(cas_name) < validator_index
    assert temp_binding_name in final_window
    assert temp_contents_name in final_window
    assert cas_name in final_window
    assert "_revalidate_operation_dir_binding" in final_window
    assert (
        final_window.index("_revalidate_operation_dir_binding")
        < final_window.index(temp_binding_name)
        < final_window.index(temp_contents_name)
        < final_window.index(cas_name)
    )


def test_source_state_callback_destination_creation_rechecks_cas_before_replace(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    state_file = operation_dir / "operation-state.json"
    desired = source_verified_state(operation_dir)
    attacker = copy.deepcopy(desired)
    attacker["reviewed_source"]["commit"] = "2" * 40
    attacker_raw = canonical_state_bytes(attacker)

    def create_destination() -> None:
        state_file.write_bytes(attacker_raw)
        state_file.chmod(0o600)

    with pytest.raises(module.OperationStateError, match="compare-and-swap"):
        module._atomic_write_operation_state_internal(
            state_file,
            desired,
            effective_uid=uid,
            pre_replace_validator=create_destination,
        )

    assert state_file.read_bytes() == attacker_raw
    assert not any(
        path.name.startswith(".operation-state.json.")
        for path in operation_dir.iterdir()
    )


def test_source_state_callback_destination_replacement_rechecks_cas_before_replace(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    old = source_verified_state(operation_dir)
    state_file = write_state_file(operation_dir, old)
    desired = state_at_phase(operation_dir, "staging_prepared")
    attacker_raw = canonical_state_bytes(old)
    attacker_inode: int | None = None

    def replace_destination() -> None:
        nonlocal attacker_inode
        attacker = operation_dir / "attacker-state.json"
        attacker.write_bytes(attacker_raw)
        attacker.chmod(0o600)
        os.replace(attacker, state_file)
        attacker_inode = state_file.stat().st_ino

    with pytest.raises(module.OperationStateError, match="compare-and-swap"):
        module._atomic_write_operation_state_internal(
            state_file,
            desired,
            effective_uid=uid,
            pre_replace_validator=replace_destination,
        )

    assert attacker_inode is not None
    assert state_file.stat().st_ino == attacker_inode
    assert state_file.read_bytes() == attacker_raw
    assert not any(
        path.name.startswith(".operation-state.json.")
        for path in operation_dir.iterdir()
    )


def test_source_state_callback_temp_name_swap_rechecks_binding_before_replace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    state_file = operation_dir / "operation-state.json"
    desired = source_verified_state(operation_dir)
    attacker_raw = b'{"attacker":true}\n'
    temp_path: Path | None = None

    def capture_temp(event: str, **details: object) -> None:
        nonlocal temp_path
        if event == "before_replace":
            temp_path = Path(str(details["temp_path"]))

    def swap_temp_name() -> None:
        assert temp_path is not None
        temp_path.unlink()
        temp_path.write_bytes(attacker_raw)
        temp_path.chmod(0o600)

    monkeypatch.setattr(module, "_atomic_event_hook", capture_temp)

    with pytest.raises(module.OperationStateError):
        module._atomic_write_operation_state_internal(
            state_file,
            desired,
            effective_uid=uid,
            pre_replace_validator=swap_temp_name,
        )

    assert not state_file.exists()
    assert temp_path is not None
    assert temp_path.read_bytes() == attacker_raw


def test_source_state_callback_same_inode_temp_overwrite_rechecks_canonical_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    state_file = operation_dir / "operation-state.json"
    desired = source_verified_state(operation_dir)
    attacker = copy.deepcopy(desired)
    attacker["reviewed_source"]["commit"] = "2" * 40
    attacker_raw = canonical_state_bytes(attacker)
    assert len(attacker_raw) == len(canonical_state_bytes(desired))
    temp_path: Path | None = None

    def capture_temp(event: str, **details: object) -> None:
        nonlocal temp_path
        if event == "before_replace":
            temp_path = Path(str(details["temp_path"]))

    def overwrite_temp_contents() -> None:
        assert temp_path is not None
        original = temp_path.stat()
        temp_path.write_bytes(attacker_raw)
        temp_path.chmod(0o600)
        os.utime(
            temp_path,
            ns=(original.st_atime_ns, original.st_mtime_ns),
        )

    monkeypatch.setattr(module, "_atomic_event_hook", capture_temp)

    with pytest.raises(module.OperationStateError, match="temporary.*bytes|temporary.*binding"):
        module._atomic_write_operation_state_internal(
            state_file,
            desired,
            effective_uid=uid,
            pre_replace_validator=overwrite_temp_contents,
        )

    assert not state_file.exists()
    assert not any(
        path.name.startswith(".operation-state.json.")
        for path in operation_dir.iterdir()
    )


@pytest.mark.parametrize("race", ["mutate", "replace", "unlink_recreate"])
def test_source_verification_revalidates_archive_at_atomic_before_replace(
    race: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, _, _, _ = source_verification_fixture(module, tmp_path)
    archive = context.paths.source_archive
    original = archive.read_bytes()
    raced = False

    def race_archive(event: str, **details: object) -> None:
        nonlocal raced
        if event != "before_replace":
            return
        raced = True
        try:
            if race == "mutate":
                changed = bytearray(original)
                changed[-1] = 1
                archive.write_bytes(changed)
                archive.chmod(0o600)
            elif race == "replace":
                replacement = archive.with_name("replacement-source.tar")
                replacement.write_bytes(original)
                replacement.chmod(0o600)
                os.replace(replacement, archive)
            else:
                archive.unlink()
                archive.write_bytes(original)
                archive.chmod(0o600)
        except PermissionError as exc:
            raise module.OperationStateError(
                "source archive race was blocked while the proof descriptor was held"
            ) from exc

    monkeypatch.setattr(module, "_atomic_event_hook", race_archive)

    with pytest.raises(module.OperationStateError, match="source archive"):
        module.verify_source_archive(context, source_dir=context.paths.source_dir)

    assert raced
    assert not context.paths.state_file.exists()
    assert not any(
        path.name.startswith(".operation-state.json.")
        for path in context.paths.operation_dir.iterdir()
    )


def test_source_verification_private_writer_reuses_held_operation_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, _, _, _ = source_verification_fixture(module, tmp_path)
    original = module._open_validated_operation_dir
    opens = 0

    @contextlib.contextmanager
    def count_opens(path: Path, effective_uid: int):
        nonlocal opens
        opens += 1
        with original(path, effective_uid) as directory_fd:
            yield directory_fd

    monkeypatch.setattr(module, "_open_validated_operation_dir", count_opens)

    module.verify_source_archive(context, source_dir=context.paths.source_dir)

    assert opens == 1


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX directory descriptors")
def test_source_verification_held_operation_directory_rejects_path_inode_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, _, _, _ = source_verification_fixture(module, tmp_path)
    operation_dir = context.paths.operation_dir
    detached = tmp_path / "detached-operation"
    raced = False

    def replace_operation_directory(event: str, **details: object) -> None:
        nonlocal raced
        if event == "before_replace":
            raced = True
            operation_dir.rename(detached)
            operation_dir.mkdir(mode=0o700)
            operation_dir.chmod(0o700)

    monkeypatch.setattr(module, "_atomic_event_hook", replace_operation_directory)

    with pytest.raises(module.OperationStateError, match="operation directory.*binding"):
        module.verify_source_archive(context, source_dir=context.paths.source_dir)

    assert raced
    assert not (detached / "operation-state.json").exists()
    assert not (operation_dir / "operation-state.json").exists()


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX directory descriptors")
def test_source_verification_rejects_operation_directory_rebind_before_private_writer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, _, _, _ = source_verification_fixture(module, tmp_path)
    operation_dir = context.paths.operation_dir
    detached = tmp_path / "detached-before-writer"
    original_writer = module._atomic_write_operation_state_internal
    raced = False

    def rebind_then_write(*args: object, **kwargs: object) -> None:
        nonlocal raced
        raced = True
        operation_dir.rename(detached)
        operation_dir.mkdir(mode=0o700)
        operation_dir.chmod(0o700)
        original_writer(*args, **kwargs)

    monkeypatch.setattr(module, "_atomic_write_operation_state_internal", rebind_then_write)

    with pytest.raises(module.OperationStateError, match="operation directory.*binding"):
        module.verify_source_archive(context, source_dir=context.paths.source_dir)

    assert raced
    assert not (detached / "operation-state.json").exists()
    assert not (operation_dir / "operation-state.json").exists()


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX directory descriptors")
def test_private_writer_does_not_close_borrowed_operation_directory_descriptor(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    state_file = operation_dir / "operation-state.json"
    state = source_verified_state(operation_dir)

    with module._open_validated_operation_dir(operation_dir, uid) as directory_fd:
        assert directory_fd is not None
        metadata = os.fstat(directory_fd)
        module._atomic_write_operation_state_internal(
            state_file,
            state,
            effective_uid=uid,
            pre_replace_validator=None,
            operation_directory_binding=module._OperationDirectoryBinding(
                operation_dir,
                directory_fd,
                metadata,
            ),
        )
        assert module._same_identity(os.fstat(directory_fd), metadata)

    assert state_file.read_bytes() == canonical_state_bytes(state)


@pytest.mark.skipif(os.name != "posix", reason="requires observable POSIX descriptors")
@pytest.mark.parametrize("target", ["source", "deploy"])
def test_source_directory_descriptor_closes_when_initial_fstat_fails(
    target: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, _, _, _ = source_verification_fixture(module, tmp_path)
    real_open = module.os.open
    real_fstat = module.os.fstat
    target_fd: int | None = None

    def tracking_open(path: object, flags: int, *args: object, **kwargs: object) -> int:
        nonlocal target_fd
        descriptor = real_open(path, flags, *args, **kwargs)
        if os.fspath(path) == target and target_fd is None:
            target_fd = descriptor
        return descriptor

    def failing_fstat(descriptor: int) -> os.stat_result:
        if target_fd is not None and descriptor == target_fd:
            raise OSError("injected source-directory fstat failure")
        return real_fstat(descriptor)

    monkeypatch.setattr(module.os, "open", tracking_open)
    monkeypatch.setattr(module.os, "fstat", failing_fstat)
    monkeypatch.setattr(module, "_descriptor_primitives_available", lambda: True)

    with pytest.raises(
        module.OperationStateError,
        match="operation directory descriptor validation|source directory enumeration",
    ):
        module.verify_source_archive(context, source_dir=context.paths.source_dir)

    assert target_fd is not None
    with pytest.raises(OSError):
        real_fstat(target_fd)


@pytest.mark.skipif(os.name != "posix", reason="requires observable POSIX descriptors")
@pytest.mark.parametrize(
    ("target", "label", "failure_call"),
    [("source", "source directory", 1), ("deploy", "source directory deploy", 2)],
)
def test_source_directory_descriptor_closes_when_metadata_validation_fails(
    target: str,
    label: str,
    failure_call: int,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, _, _, _ = source_verification_fixture(module, tmp_path)
    real_open = module.os.open
    real_fstat = module.os.fstat
    real_validate = module._validate_source_directory_metadata
    target_fd: int | None = None
    label_calls = 0

    def tracking_open(path: object, flags: int, *args: object, **kwargs: object) -> int:
        nonlocal target_fd
        descriptor = real_open(path, flags, *args, **kwargs)
        if os.fspath(path) == target and target_fd is None:
            target_fd = descriptor
        return descriptor

    def failing_validation(metadata: os.stat_result, effective_uid: int, observed: str) -> None:
        nonlocal label_calls
        if observed == label:
            label_calls += 1
            if label_calls == failure_call:
                assert target_fd is not None
                raise module.OperationStateError(
                    "injected source-directory metadata validation failure"
                )
        real_validate(metadata, effective_uid, observed)

    monkeypatch.setattr(module.os, "open", tracking_open)
    monkeypatch.setattr(module, "_validate_source_directory_metadata", failing_validation)
    monkeypatch.setattr(module, "_descriptor_primitives_available", lambda: True)

    with pytest.raises(
        module.OperationStateError,
        match="injected source-directory metadata validation failure",
    ):
        module.verify_source_archive(context, source_dir=context.paths.source_dir)

    assert target_fd is not None
    with pytest.raises(OSError):
        real_fstat(target_fd)


@pytest.mark.skipif(
    os.name != "posix" or not Path("/usr/bin/git").is_file(),
    reason="requires POSIX and fixed /usr/bin/git",
)
def test_source_verification_real_git_archive_uses_default_inherited_fd_transport(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    repository = tmp_path / "reviewed-repository"
    repository.mkdir()
    assets = source_asset_bytes()
    manifest = source_manifest_bytes(assets)
    for name, contents in {**assets, SOURCE_MANIFEST: manifest}.items():
        path = repository / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(contents)

    git = "/usr/bin/git"
    subprocess.run([git, "init", "--quiet"], cwd=repository, check=True)
    subprocess.run([git, "config", "user.name", "Degen Test"], cwd=repository, check=True)
    subprocess.run(
        [git, "config", "user.email", "degen-test@example.invalid"],
        cwd=repository,
        check=True,
    )
    subprocess.run([git, "config", "core.autocrlf", "false"], cwd=repository, check=True)
    subprocess.run([git, "config", "tar.umask", "0002"], cwd=repository, check=True)
    subprocess.run([git, "add", "--", *SOURCE_ASSETS, SOURCE_MANIFEST], cwd=repository, check=True)
    subprocess.run(
        [git, "update-index", "--chmod=+x", "--", *SOURCE_ASSETS[:4]],
        cwd=repository,
        check=True,
    )
    commit_environment = {
        **os.environ,
        "GIT_AUTHOR_DATE": "2026-07-01T12:00:00+00:00",
        "GIT_COMMITTER_DATE": "2026-07-01T12:00:00+00:00",
    }
    subprocess.run(
        [git, "commit", "--quiet", "-m", "reviewed backup assets"],
        cwd=repository,
        env=commit_environment,
        check=True,
    )
    commit = subprocess.run(
        [git, "rev-parse", "HEAD"],
        cwd=repository,
        check=True,
        text=True,
        capture_output=True,
    ).stdout.strip()

    operation_dir, uid = private_operation_dir(tmp_path)
    paths = module.build_operation_paths(operation_dir)
    write_extracted_source(paths.source_dir, assets, manifest)
    subprocess.run(
        [
            git,
            "-c",
            "core.autocrlf=false",
            "-c",
            "tar.umask=0002",
            "archive",
            "--format=tar",
            f"--output={paths.source_archive}",
            commit,
            "--",
            *SOURCE_ASSETS,
            SOURCE_MANIFEST,
        ],
        cwd=repository,
        check=True,
    )
    paths.source_archive.chmod(0o600)
    archive_sha256 = hashlib.sha256(paths.source_archive.read_bytes()).hexdigest()
    context = module.OperationsContext(
        operation_id=operation_dir.name,
        paths=paths,
        effective_uid=uid,
        command_runner=module._default_command_runner,
        clock=lambda: datetime(2026, 7, 1, 12, 34, 56, tzinfo=timezone.utc),
        expected_commit=commit,
        expected_archive_sha256=archive_sha256,
        expected_manifest_sha256=hashlib.sha256(manifest).hexdigest(),
        host_root=tmp_path,
    )

    observed = module.verify_source_archive(context, source_dir=paths.source_dir)

    assert observed == {name: hashlib.sha256(contents).hexdigest() for name, contents in assets.items()}
    assert paths.state_file.exists()


def test_source_tree_enumeration_stops_at_the_fixed_child_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, _, _, _ = source_verification_fixture(module, tmp_path)
    real_scandir = module.os.scandir
    with real_scandir(context.paths.source_dir) as iterator:
        deploy_entry = next(iterator)
    requests = 0
    injected = False

    class ExtraEntry:
        name = "unreviewed-extra"

    class BombingEntries:
        def __init__(self) -> None:
            self._entries = iter((deploy_entry, ExtraEntry()))

        def __iter__(self) -> object:
            return self

        def __next__(self) -> object:
            nonlocal requests
            requests += 1
            if requests > 2:
                raise AssertionError("source enumeration consumed beyond the fixed root child bound")
            return next(self._entries)

        def __enter__(self) -> object:
            return self

        def __exit__(self, *args: object) -> None:
            return None

    def bounded_scandir(path: object) -> object:
        nonlocal injected
        is_source_root = (
            isinstance(path, int) and not injected
        ) or (
            not isinstance(path, int)
            and os.fspath(path) == os.fspath(context.paths.source_dir)
        )
        if is_source_root:
            injected = True
            return BombingEntries()
        return real_scandir(path)

    monkeypatch.setattr(module.os, "scandir", bounded_scandir)

    with pytest.raises(module.OperationStateError, match="source.*entries|source.*extra"):
        module.verify_source_archive(context, source_dir=context.paths.source_dir)

    assert requests == 2
    assert not context.paths.state_file.exists()


def test_source_verifier_never_uses_general_tar_extraction_apis() -> None:
    source = OPS_HELPER.read_text(encoding="utf-8")

    for forbidden in (".extractfile(", ".extract(", ".extractall("):
        assert forbidden not in source


@pytest.mark.skipif(os.name != "posix", reason="POSIX mode semantics are required")
def test_source_tree_rejects_group_writable_reviewed_file_without_state(tmp_path: Path) -> None:
    module = load_ops_helper()
    context, _, _, _ = source_verification_fixture(module, tmp_path)
    target = context.paths.source_dir / SOURCE_ASSETS[0]
    target.chmod(0o664)

    with pytest.raises(module.OperationStateError, match="source.*mode"):
        module.verify_source_archive(context, source_dir=context.paths.source_dir)

    assert not context.paths.state_file.exists()


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
    oversized["effective_config"]["LOG_DIR"] = "x" * (module._MAX_STATE_BYTES + 1)
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


def staging_source_asset_bytes() -> dict[str, bytes]:
    assets = source_asset_bytes()
    assets["deploy/linux/degen-prod-db-backup-env.py"] = ENV_HELPER.read_bytes()
    assets["deploy/systemd/degen-prod-db-backup.env.example"] = ENV_EXAMPLE.read_bytes()
    return assets


def host_root_path(host_root: Path, absolute_path: str) -> Path:
    assert absolute_path.startswith("/")
    return host_root.joinpath(*absolute_path.split("/")[1:])


def write_private_host_file(path: Path, contents: bytes, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(contents)
    path.chmod(mode)


def host_staging_fixture(
    module: object,
    tmp_path: Path,
    *,
    include_pair: bool = True,
    pair_prefix: str = "degen_green_",
    configured_prefix: str | None = None,
    database_name: str = "degen",
    hostname: str = "green",
    database_url: str = "postgresql+psycopg://degen:DB_URL_SENTINEL@db.internal/degen?sslmode=require",
) -> tuple[object, dict[str, object]]:
    context, assets, _manifest, _source_calls = source_verification_fixture(
        module,
        tmp_path,
        asset_bytes=staging_source_asset_bytes(),
    )
    module.verify_source_archive(context, source_dir=context.paths.source_dir)

    managed = ENV_EXAMPLE.read_bytes() + b"UNMANAGED_SAFE=keep-me\n"
    if configured_prefix is not None:
        managed += f"BACKUP_PREFIX={configured_prefix}\n".encode("ascii")
    managed_path = host_root_path(context.host_root, "/etc/degen/prod-db-backup.env")
    write_private_host_file(managed_path, managed)

    app_env = (
        b"APP_SETTING=ENV_CONTENT_SENTINEL\n"
        + f"DATABASE_URL='{database_url}'\n".encode("ascii")
    )
    app_env_path = host_root_path(context.host_root, "/opt/degen/web.env")
    write_private_host_file(app_env_path, app_env)

    rclone_path = host_root_path(context.host_root, "/etc/degen/rclone.conf")
    write_private_host_file(
        rclone_path,
        b"[onedrive]\ntype=onedrive\ntoken=RCLONE_CONTENT_SENTINEL\n",
    )

    backup_dir = host_root_path(context.host_root, "/opt/degen/backups/db")
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_dir.chmod(0o700)
    dump_name = f"{pair_prefix}20260630T235959Z.dump"
    dump_path = backup_dir / dump_name
    sidecar_path = backup_dir / f"{dump_name}.sha256"
    dump_bytes = b"controlled PostgreSQL custom-format dump fixture\n"
    if include_pair:
        write_private_host_file(dump_path, dump_bytes)
        sidecar_path.write_bytes(
            hashlib.sha256(dump_bytes).hexdigest().encode("ascii")
            + b"  "
            + dump_name.encode("ascii")
            + b"\n"
        )
        sidecar_path.chmod(0o600)

    calls: list[dict[str, object]] = []
    events: list[str] = []
    inherited_fds: list[int] = []
    completed_processes: list[subprocess.CompletedProcess[str]] = []
    source_runner = context.command_runner

    def command_runner(
        argv: object,
        pass_fds: tuple[int, ...],
    ) -> subprocess.CompletedProcess[str]:
        argv_tuple = tuple(str(value) for value in argv)
        if "get-tar-commit-id" in argv_tuple:
            return source_runner(argv_tuple, pass_fds)
        calls.append({"argv": argv_tuple, "pass_fds": pass_fds})
        if argv_tuple and argv_tuple[0] == "/usr/bin/pg_restore":
            events.append("pg_restore")
            assert argv_tuple == ("/usr/bin/pg_restore", "--list", str(dump_path))
            assert pass_fds == ()
            assert dump_path.read_bytes() == dump_bytes
            completed = subprocess.CompletedProcess(argv_tuple, 0, "archive listing\n", "")
        elif len(argv_tuple) > 4 and argv_tuple[3] == "pgdatabase":
            events.append("psql")
            assert len(pass_fds) == 1
            inherited_fds.append(pass_fds[0])
            payload = bytearray()
            while True:
                chunk = os.read(pass_fds[0], 4096)
                if not chunk:
                    break
                payload.extend(chunk)
            assert bytes(payload).decode("utf-8") == database_url.replace(
                "postgresql+psycopg://", "postgresql://", 1
            )
            completed = subprocess.CompletedProcess(argv_tuple, 0, database_name + "\n", "")
        elif argv_tuple == ("/bin/hostname", "-s"):
            events.append("hostname")
            assert pass_fds == ()
            completed = subprocess.CompletedProcess(argv_tuple, 0, hostname + "\n", "")
        else:
            raise AssertionError(f"unexpected staging command: {argv_tuple!r}")
        completed_processes.append(completed)
        return completed

    context = dataclasses.replace(context, command_runner=command_runner)
    return context, {
        "app_env": app_env,
        "assets": assets,
        "backup_dir": backup_dir,
        "calls": calls,
        "completed_processes": completed_processes,
        "database_url": database_url,
        "dump_bytes": dump_bytes,
        "dump_name": dump_name,
        "dump_path": dump_path,
        "events": events,
        "inherited_fds": inherited_fds,
        "managed_path": managed_path,
        "rclone_path": rclone_path,
        "sidecar_path": sidecar_path,
    }


def expected_host_stage_manifest(
    context: object,
    assets: dict[str, bytes],
    environment_sha256: str,
    dump_name: str,
    dump_sha256: str,
) -> dict[str, object]:
    target_by_source = dict(zip(SOURCE_ASSETS[:6], TARGETS[:6]))
    return {
        "schema_version": 1,
        "operation": {
            "archive_sha256": context.expected_archive_sha256,
            "commit": context.expected_commit,
            "manifest_sha256": context.expected_manifest_sha256,
            "operation_dir": str(context.paths.operation_dir),
            "operation_id": context.operation_id,
        },
        "selected_pair": {
            "dump_basename": dump_name,
            "dump_sha256": dump_sha256,
        },
        "reviewed_assets": [
            {
                "mode": 0o755 if source.startswith("deploy/linux/") else 0o644,
                "sha256": hashlib.sha256(assets[source]).hexdigest(),
                "source": source,
                "staged_path": f"reviewed/{source}",
                "target": target_by_source.get(source),
            }
            for source in sorted(SOURCE_ASSETS)
        ],
        "host_environment": {
            "mode": 0o600,
            "sha256": environment_sha256,
            "staged_path": "host/etc/degen/prod-db-backup.env",
            "target": "/etc/degen/prod-db-backup.env",
        },
    }


def test_prepare_host_staging_builds_exact_assets_manifest_and_state(tmp_path: Path) -> None:
    module = load_ops_helper()
    context, fixture = host_staging_fixture(module, tmp_path)

    result = module.prepare_host_staging(context)

    assert set(result) == {"effective_config", "host_stage"}
    effective = result["effective_config"]
    assert isinstance(effective, dict)
    assert effective["BACKUP_PREFIX"] == "degen_green_"
    assert effective["REMOTE_PRUNE_ENABLED"] == "0"
    assert set(effective) == {
        "APP_ENV_FILE",
        "BACKUP_DIR",
        "LOG_DIR",
        "RCLONE_CONFIG",
        "RCLONE_REMOTE_PATH",
        "KEEP_LOCAL_COUNT",
        "KEEP_REMOTE_DAILY",
        "KEEP_REMOTE_WEEKLY",
        "KEEP_REMOTE_MONTHLY",
        "REMOTE_PRUNE_ENABLED",
        "MIN_FREE_AFTER_BYTES",
        "RETENTION_PLANNER",
        "LOCK_FILE",
        "BACKUP_PREFIX",
    }
    staged_environment = context.paths.staged_dir / "host/etc/degen/prod-db-backup.env"
    environment_bytes = staged_environment.read_bytes()
    assert b"BACKUP_PREFIX=degen_green_\n" in environment_bytes
    assert b"REMOTE_PRUNE_ENABLED=0\n" in environment_bytes
    assert b"UNMANAGED_SAFE=keep-me\n" in environment_bytes
    assert fixture["database_url"].encode("ascii") not in environment_bytes
    assert b"ENV_CONTENT_SENTINEL" not in environment_bytes
    assert b"RCLONE_CONTENT_SENTINEL" not in environment_bytes

    for source, expected_bytes in fixture["assets"].items():
        staged = context.paths.staged_dir / "reviewed" / source
        assert staged.read_bytes() == expected_bytes
        if os.name == "posix":
            expected_mode = 0o755 if source.startswith("deploy/linux/") else 0o644
            assert stat.S_IMODE(staged.stat().st_mode) == expected_mode
    if os.name == "posix":
        assert stat.S_IMODE(context.paths.staged_dir.stat().st_mode) == 0o700
        assert stat.S_IMODE(staged_environment.stat().st_mode) == 0o600

    environment_sha256 = hashlib.sha256(environment_bytes).hexdigest()
    expected_manifest = expected_host_stage_manifest(
        context,
        fixture["assets"],
        environment_sha256,
        fixture["dump_name"],
        hashlib.sha256(fixture["dump_bytes"]).hexdigest(),
    )
    expected_manifest_bytes = (
        json.dumps(expected_manifest, sort_keys=True, separators=(",", ":")).encode("ascii")
        + b"\n"
    )
    manifest_path = context.paths.staged_dir / "host-stage-manifest.json"
    assert manifest_path.read_bytes() == expected_manifest_bytes
    expected_host_stage = {
        "manifest_sha256": hashlib.sha256(expected_manifest_bytes).hexdigest(),
        "asset_hashes": {
            source: hashlib.sha256(contents).hexdigest()
            for source, contents in fixture["assets"].items()
        },
        "environment_sha256": environment_sha256,
    }
    assert result["host_stage"] == expected_host_stage

    state = module.load_operation_state(
        context.paths.state_file, effective_uid=context.effective_uid
    )
    module.validate_operation_state_for_context(state, context)
    assert state["phase"] == "staging_prepared"
    assert state["effective_config"] == effective
    assert state["host_stage"] == expected_host_stage
    assert [entry["phase"] for entry in state["phase_history"]] == [
        "source_verified",
        "staging_prepared",
    ]
    for field in (
        "snapshot",
        "prior_runtime",
        "install",
        "probe",
        "dry_run",
        "policy",
        "observation",
        "active_transaction",
        "failure",
        "recovery",
    ):
        assert state[field] is None


def test_prepare_host_staging_no_existing_pair_fails_before_staging_or_state(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    context, fixture = host_staging_fixture(module, tmp_path, include_pair=False)
    before = context.paths.state_file.read_bytes()

    with pytest.raises(module.OperationStateError, match="verified local backup pair"):
        module.prepare_host_staging(context)

    assert fixture["events"] == []
    assert context.paths.state_file.read_bytes() == before
    assert not context.paths.staged_dir.exists()


def test_host_stage_refuses_to_reverse_live_enabled_prune_policy(tmp_path: Path) -> None:
    module = load_ops_helper()
    context, fixture = host_staging_fixture(module, tmp_path)
    managed_path = fixture["managed_path"]
    managed_path.write_bytes(
        managed_path.read_bytes().replace(
            b"REMOTE_PRUNE_ENABLED=0\n", b"REMOTE_PRUNE_ENABLED=1\n"
        )
    )
    managed_path.chmod(0o600)
    before = context.paths.state_file.read_bytes()

    with pytest.raises(module.OperationStateError, match="prune|enabled"):
        module.prepare_host_staging(context)

    assert context.paths.state_file.read_bytes() == before
    assert not context.paths.staged_dir.exists()


@pytest.mark.parametrize(
    "secret_key",
    ["AWS_SECRET_ACCESS_KEY", "API_KEY", "PASSWORD", "PGPASSWORD"],
)
def test_host_stage_rejects_unmanaged_secret_assignment_before_persistent_stage(
    tmp_path: Path, secret_key: str
) -> None:
    module = load_ops_helper()
    context, fixture = host_staging_fixture(module, tmp_path)
    sentinel = "UNMANAGED_SECRET_ASSIGNMENT_SENTINEL"
    managed_path = fixture["managed_path"]
    managed_path.write_bytes(
        managed_path.read_bytes() + f"{secret_key}={sentinel}\n".encode("ascii")
    )
    managed_path.chmod(0o600)
    before = context.paths.state_file.read_bytes()

    with pytest.raises(module.OperationStateError) as raised:
        module.prepare_host_staging(context)

    assert sentinel not in str(raised.value)
    assert secret_key not in str(raised.value)
    assert context.paths.state_file.read_bytes() == before
    assert not context.paths.staged_dir.exists()


def test_host_stage_resumes_only_an_exact_fully_verified_preexisting_stage(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    context, _fixture = host_staging_fixture(module, tmp_path)
    source_state_bytes = context.paths.state_file.read_bytes()
    first = module.prepare_host_staging(context)
    identities = {
        path.relative_to(context.paths.staged_dir).as_posix(): (path.stat().st_ino, path.stat().st_mtime_ns)
        for path in context.paths.staged_dir.rglob("*")
        if path.is_file()
    }
    context.paths.state_file.write_bytes(source_state_bytes)
    context.paths.state_file.chmod(0o600)

    second = module.prepare_host_staging(context)

    assert second == first
    assert {
        path.relative_to(context.paths.staged_dir).as_posix(): (path.stat().st_ino, path.stat().st_mtime_ns)
        for path in context.paths.staged_dir.rglob("*")
        if path.is_file()
    } == identities


def test_host_stage_exact_resume_does_not_create_or_unlink_render_scratch(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    context, _fixture = host_staging_fixture(module, tmp_path)
    source_state_bytes = context.paths.state_file.read_bytes()
    first = module.prepare_host_staging(context)
    context.paths.state_file.write_bytes(source_state_bytes)
    context.paths.state_file.chmod(0o600)
    assert not hasattr(module, "_remove_render_scratch")

    assert module.prepare_host_staging(context) == first


def test_host_stage_exact_resume_fsyncs_held_files_and_directories_before_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = load_ops_helper()
    context, _fixture = host_staging_fixture(module, tmp_path)
    source_state_bytes = context.paths.state_file.read_bytes()
    module.prepare_host_staging(context)
    context.paths.state_file.write_bytes(source_state_bytes)
    context.paths.state_file.chmod(0o600)
    events: list[str] = []
    original_fsync = module._fsync_stage_directories
    original_atomic = module._atomic_write_operation_state_internal

    def record_fsync(*args: object, **kwargs: object) -> None:
        events.append("stage-fsync")
        original_fsync(*args, **kwargs)

    def record_atomic(*args: object, **kwargs: object) -> None:
        events.append("state-atomic")
        original_atomic(*args, **kwargs)

    monkeypatch.setattr(module, "_fsync_stage_directories", record_fsync)
    monkeypatch.setattr(module, "_atomic_write_operation_state_internal", record_atomic)

    module.prepare_host_staging(context)

    assert events == ["stage-fsync", "state-atomic"]


def test_host_stage_new_write_rejects_parent_directory_inode_swap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = load_ops_helper()
    context, _fixture = host_staging_fixture(module, tmp_path)
    original = module._write_exclusive_staged_file
    swapped = False

    def swapping_writer(path: Path, *args: object, **kwargs: object):
        nonlocal swapped
        if not swapped and path.parent.name == "linux":
            swapped = True
            moved = path.parent.with_name("linux-original-inode")
            path.parent.rename(moved)
            path.parent.mkdir(mode=0o700)
        return original(path, *args, **kwargs)

    monkeypatch.setattr(module, "_write_exclusive_staged_file", swapping_writer)
    before = context.paths.state_file.read_bytes()

    with pytest.raises(module.OperationStateError, match="staged|stage|directory|binding"):
        module.prepare_host_staging(context)

    assert swapped
    assert context.paths.state_file.read_bytes() == before
    replacement_parent = context.paths.staged_dir / "reviewed/deploy/linux"
    assert list(replacement_parent.iterdir()) == []


def test_host_stage_closes_held_stage_directories_when_state_construction_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = load_ops_helper()
    context, _fixture = host_staging_fixture(module, tmp_path)
    closed: list[object] = []
    original_close = module._close_stage_directories

    def record_close(proof: object) -> None:
        closed.append(proof)
        original_close(proof)

    monkeypatch.setattr(module, "_close_stage_directories", record_close)
    monkeypatch.setattr(
        module,
        "_staging_prepared_state",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            module.OperationStateError("forced state construction failure")
        ),
    )

    with pytest.raises(module.OperationStateError, match="forced state"):
        module.prepare_host_staging(context)

    assert len(closed) == 1


@pytest.mark.parametrize("residue_kind", ["partial", "different"])
def test_host_stage_rejects_partial_or_different_preexisting_residue(
    tmp_path: Path, residue_kind: str
) -> None:
    module = load_ops_helper()
    context, _fixture = host_staging_fixture(module, tmp_path)
    context.paths.staged_dir.mkdir(mode=0o700)
    residue = context.paths.staged_dir / "host-stage-manifest.json"
    residue.write_bytes(b"{}\n" if residue_kind == "partial" else b"different\n")
    residue.chmod(0o600)
    before = context.paths.state_file.read_bytes()

    with pytest.raises(module.OperationStateError, match="staged|stage|residue|manifest"):
        module.prepare_host_staging(context)

    assert context.paths.state_file.read_bytes() == before
    assert residue.exists()


@pytest.mark.skipif(not hasattr(os, "symlink"), reason="symlinks unavailable")
def test_host_stage_rejects_symlinked_preexisting_staged_directory(tmp_path: Path) -> None:
    module = load_ops_helper()
    context, _fixture = host_staging_fixture(module, tmp_path)
    target = context.paths.operation_dir / "not-staged"
    target.mkdir()
    try:
        context.paths.staged_dir.symlink_to(target, target_is_directory=True)
    except OSError:
        pytest.skip("symlink creation unavailable")

    with pytest.raises(module.OperationStateError, match="staged|stage|symlink"):
        module.prepare_host_staging(context)

    assert context.paths.staged_dir.is_symlink()


def test_host_stage_rejects_host_root_with_symlinked_intermediate(tmp_path: Path) -> None:
    module = load_ops_helper()
    context, _fixture = host_staging_fixture(module, tmp_path)
    real_parent = tmp_path / "real-host-parent"
    real_parent.mkdir()
    (real_parent / "root").mkdir()
    linked_parent = tmp_path / "linked-host-parent"
    try:
        linked_parent.symlink_to(real_parent, target_is_directory=True)
    except OSError:
        if os.name != "nt":
            pytest.skip("symlink creation unavailable")
        junction = subprocess.run(
            ["cmd.exe", "/d", "/c", "mklink", "/J", str(linked_parent), str(real_parent)],
            text=True,
            capture_output=True,
            check=False,
        )
        if junction.returncode != 0:
            pytest.skip("junction creation unavailable")
    rebound = dataclasses.replace(context, host_root=linked_parent / "root")

    with pytest.raises(module.OperationStateError, match="host_root|symlink|canonical"):
        module._host_path(rebound, "/etc/degen/prod-db-backup.env")


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX directory rebinding")
def test_host_stage_atomic_callback_rejects_host_root_intermediate_rebind(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = load_ops_helper()
    context, fixture = host_staging_fixture(module, tmp_path)
    anchor = tmp_path / "host-anchor"
    host_root = anchor / "root"
    host_root.mkdir(parents=True)
    shutil.copytree(tmp_path / "etc", host_root / "etc")
    shutil.copytree(tmp_path / "opt", host_root / "opt")
    new_dump = host_root_path(host_root, "/opt/degen/backups/db") / fixture["dump_name"]
    original_runner = context.command_runner

    def rooted_runner(argv: object, pass_fds: tuple[int, ...]):
        argv_tuple = tuple(str(value) for value in argv)
        if argv_tuple and argv_tuple[0] == "/usr/bin/pg_restore":
            assert argv_tuple == ("/usr/bin/pg_restore", "--list", str(new_dump))
            assert pass_fds == ()
            return subprocess.CompletedProcess(argv_tuple, 0, "archive listing\n", "")
        return original_runner(argv_tuple, pass_fds)

    context = dataclasses.replace(
        context,
        host_root=host_root,
        command_runner=rooted_runner,
    )
    before = context.paths.state_file.read_bytes()
    swapped = False

    def rebind(event: str, **_details: object) -> None:
        nonlocal swapped
        if event == "before_replace":
            moved = anchor.with_name("host-anchor-original")
            anchor.rename(moved)
            anchor.symlink_to(moved, target_is_directory=True)
            swapped = True

    monkeypatch.setattr(module, "_atomic_event_hook", rebind)

    with pytest.raises(module.OperationStateError, match="host_root|ancestor|binding"):
        module.prepare_host_staging(context)

    assert swapped
    assert context.paths.state_file.read_bytes() == before


@pytest.mark.parametrize(
    "sidecar_bytes",
    [
        b"A" * 64 + b"  degen_green_20260630T235959Z.dump\n",
        b"0" * 64 + b"  degen_green_20260630T235959Z.dump\n",
        HASH_A.encode("ascii") + b" degen_green_20260630T235959Z.dump\n",
        HASH_A.encode("ascii") + b"  degen_green_20260630T235959Z.dump\n\n",
    ],
    ids=("uppercase", "hash-mismatch", "one-space", "extra-record"),
)
def test_existing_pair_requires_canonical_sidecar_then_hash_before_pg_restore(
    tmp_path: Path, sidecar_bytes: bytes
) -> None:
    module = load_ops_helper()
    context, fixture = host_staging_fixture(module, tmp_path)
    fixture["sidecar_path"].write_bytes(sidecar_bytes)
    fixture["sidecar_path"].chmod(0o600)
    before = context.paths.state_file.read_bytes()

    with pytest.raises(module.OperationStateError):
        module.prepare_host_staging(context)

    assert "pg_restore" not in fixture["events"]
    assert context.paths.state_file.read_bytes() == before
    assert not context.paths.staged_dir.exists()


def test_existing_pair_newest_complete_timestamp_wins(tmp_path: Path) -> None:
    module = load_ops_helper()
    context, fixture = host_staging_fixture(module, tmp_path)
    newest_name = "degen_green_20260701T000001Z.dump"
    newest = fixture["backup_dir"] / newest_name
    newest_bytes = fixture["dump_bytes"] + b"newest"
    write_private_host_file(newest, newest_bytes)
    newest_sidecar = fixture["backup_dir"] / f"{newest_name}.sha256"
    write_private_host_file(
        newest_sidecar,
        hashlib.sha256(newest_bytes).hexdigest().encode("ascii")
        + b"  "
        + newest_name.encode("ascii")
        + b"\n",
    )

    original_runner = context.command_runner

    def newest_runner(argv: object, pass_fds: tuple[int, ...]):
        argv_tuple = tuple(str(value) for value in argv)
        if argv_tuple and argv_tuple[0] == "/usr/bin/pg_restore":
            assert argv_tuple == ("/usr/bin/pg_restore", "--list", str(newest))
            assert pass_fds == ()
            fixture["events"].append("pg_restore")
            return subprocess.CompletedProcess(argv_tuple, 0, "archive listing\n", "")
        return original_runner(argv_tuple, pass_fds)

    result = module.prepare_host_staging(dataclasses.replace(context, command_runner=newest_runner))

    assert result["effective_config"]["BACKUP_PREFIX"] == "degen_green_"


def test_existing_pair_corrupt_newest_blocks_without_falling_back_to_older(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    context, fixture = host_staging_fixture(module, tmp_path)
    newest_name = "degen_green_20260701T000001Z.dump"
    newest = fixture["backup_dir"] / newest_name
    write_private_host_file(newest, b"corrupt newest dump\n")
    write_private_host_file(
        fixture["backup_dir"] / f"{newest_name}.sha256",
        b"0" * 64 + b"  " + newest_name.encode("ascii") + b"\n",
    )

    with pytest.raises(module.OperationStateError, match="SHA-256|sidecar|archive"):
        module.prepare_host_staging(context)

    assert "pg_restore" not in fixture["events"]
    assert not context.paths.staged_dir.exists()


def test_existing_pair_equal_timestamp_with_different_prefix_is_ambiguous(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    context, fixture = host_staging_fixture(module, tmp_path)
    other_name = "other_green_20260630T235959Z.dump"
    other = fixture["backup_dir"] / other_name
    write_private_host_file(other, fixture["dump_bytes"])
    write_private_host_file(
        fixture["backup_dir"] / f"{other_name}.sha256",
        hashlib.sha256(fixture["dump_bytes"]).hexdigest().encode("ascii")
        + b"  "
        + other_name.encode("ascii")
        + b"\n",
    )

    with pytest.raises(module.OperationStateError, match="ambiguous"):
        module.prepare_host_staging(context)

    assert "pg_restore" not in fixture["events"]


@pytest.mark.parametrize("unsafe_kind", ["incomplete", "unexpected", "directory"])
def test_existing_pair_rejects_incomplete_or_unsafe_directory_entries(
    tmp_path: Path, unsafe_kind: str
) -> None:
    module = load_ops_helper()
    context, fixture = host_staging_fixture(module, tmp_path)
    if unsafe_kind == "incomplete":
        fixture["sidecar_path"].unlink()
    elif unsafe_kind == "unexpected":
        write_private_host_file(fixture["backup_dir"] / "notes.txt", b"unexpected\n")
    else:
        (fixture["backup_dir"] / "nested").mkdir()

    with pytest.raises(module.OperationStateError):
        module.prepare_host_staging(context)

    assert "pg_restore" not in fixture["events"]
    assert not context.paths.staged_dir.exists()


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX link semantics")
@pytest.mark.parametrize("link_kind", ["symlink", "hardlink"])
def test_existing_pair_rejects_links_without_pg_restore(
    tmp_path: Path, link_kind: str
) -> None:
    module = load_ops_helper()
    context, fixture = host_staging_fixture(module, tmp_path)
    dump_path = fixture["dump_path"]
    original = dump_path.with_suffix(".original")
    dump_path.rename(original)
    if link_kind == "symlink":
        dump_path.symlink_to(original.name)
    else:
        os.link(original, dump_path)

    with pytest.raises(module.OperationStateError):
        module.prepare_host_staging(context)

    assert "pg_restore" not in fixture["events"]
    assert not context.paths.staged_dir.exists()


def test_existing_pair_replacement_during_pg_restore_fails_before_staged_state(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    context, fixture = host_staging_fixture(module, tmp_path)
    original_runner = context.command_runner

    def replacing_runner(argv: object, pass_fds: tuple[int, ...]):
        argv_tuple = tuple(str(value) for value in argv)
        completed = original_runner(argv_tuple, pass_fds)
        if argv_tuple and argv_tuple[0] == "/usr/bin/pg_restore":
            replacement = fixture["dump_path"].with_suffix(".replacement")
            write_private_host_file(replacement, fixture["dump_bytes"])
            os.replace(replacement, fixture["dump_path"])
        return completed

    with pytest.raises(module.OperationStateError):
        module.prepare_host_staging(dataclasses.replace(context, command_runner=replacing_runner))

    assert not context.paths.staged_dir.exists()
    state = module.load_operation_state(
        context.paths.state_file, effective_uid=context.effective_uid
    )
    assert state["phase"] == "source_verified"


def test_existing_pair_pre_replace_reinventory_rejects_newer_complete_pair(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = load_ops_helper()
    context, fixture = host_staging_fixture(module, tmp_path)
    before = context.paths.state_file.read_bytes()
    monkeypatch.setattr(module, "_revalidate_host_directory_proof", lambda _proof: None)

    def inject_newer(event: str, **_details: object) -> None:
        if event != "before_replace":
            return
        name = "degen_green_20260701T000001Z.dump"
        contents = fixture["dump_bytes"] + b"newer-before-cas"
        write_private_host_file(fixture["backup_dir"] / name, contents)
        write_private_host_file(
            fixture["backup_dir"] / f"{name}.sha256",
            hashlib.sha256(contents).hexdigest().encode("ascii")
            + b"  "
            + name.encode("ascii")
            + b"\n",
        )

    monkeypatch.setattr(module, "_atomic_event_hook", inject_newer)

    with pytest.raises(module.OperationStateError, match="newest|selected|inventory|pair"):
        module.prepare_host_staging(context)

    assert context.paths.state_file.read_bytes() == before


def test_existing_pair_default_pg_restore_discards_unbounded_command_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    observed: list[dict[str, object]] = []

    def fake_run(argv: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        observed.append(kwargs)
        return subprocess.CompletedProcess(argv, 0, None, None)

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    completed = module._default_command_runner(
        ("/usr/bin/pg_restore", "--list", "/trusted/backup.dump"),
        (),
    )

    assert len(observed) == 1
    assert observed[0]["stdout"] is subprocess.DEVNULL
    assert observed[0]["stderr"] is subprocess.DEVNULL
    assert "capture_output" not in observed[0]
    assert completed.stdout == ""
    assert completed.stderr == ""


@pytest.mark.parametrize(
    ("pair_prefix", "configured_prefix"),
    [("wrong_green_", None), ("degen_green_", "wrong_green_")],
)
def test_host_stage_requires_filename_and_config_prefix_to_match_live_identity(
    tmp_path: Path,
    pair_prefix: str,
    configured_prefix: str | None,
) -> None:
    module = load_ops_helper()
    context, fixture = host_staging_fixture(
        module,
        tmp_path,
        pair_prefix=pair_prefix,
        configured_prefix=configured_prefix,
    )

    with pytest.raises(module.OperationStateError, match="prefix"):
        module.prepare_host_staging(context)

    assert fixture["events"] == ["pg_restore", "psql", "hostname"]
    assert not context.paths.staged_dir.exists()


def test_pgdatabase_uses_fresh_bounded_inherited_fd_for_each_fixed_psql_query(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    context, fixture = host_staging_fixture(module, tmp_path)

    module.prepare_host_staging(context)

    psql_calls = [
        call
        for call in fixture["calls"]
        if len(call["argv"]) > 4 and call["argv"][3] == "pgdatabase"
    ]
    assert len(psql_calls) == 2
    for call in psql_calls:
        argv = call["argv"]
        assert argv[:4] == (
            sys.executable,
            "-c",
            module._INHERITED_FD_EXEC_SHIM,
            "pgdatabase",
        )
        assert argv[5:] == (
            "/usr/bin/psql",
            "psql",
            "--no-psqlrc",
            "--tuples-only",
            "--no-align",
            "--command",
            "SELECT current_database();",
        )
        assert fixture["database_url"] not in "\n".join(argv)
        assert len(call["pass_fds"]) == 1
    for descriptor in fixture["inherited_fds"]:
        with pytest.raises(OSError):
            os.fstat(descriptor)


def test_pgdatabase_default_runner_scrubs_ambient_secret_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    observed: list[dict[str, object]] = []
    monkeypatch.setenv("DATABASE_URL", "postgresql://ambient:SECRET@db/degen")
    monkeypatch.setenv("PGPASSWORD", "AMBIENT_PGPASSWORD_SENTINEL")
    monkeypatch.setenv("PGDATABASE", "postgresql://ambient:OTHER@db/degen")

    def fake_run(argv: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        observed.append(kwargs)
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    module._default_command_runner(("/bin/hostname", "-s"), ())

    assert len(observed) == 1
    child_env = observed[0]["env"]
    assert child_env == {
        "LANG": "C",
        "LC_ALL": "C",
        "PATH": "/usr/sbin:/usr/bin:/sbin:/bin",
    }


def test_pgdatabase_writer_start_failure_is_generic_and_closes_both_pipe_fds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    opened: list[int] = []
    runner_called = False

    def pipe(_payload: bytes) -> tuple[int, int]:
        pair = os.pipe()
        opened.extend(pair)
        return pair

    class FailingThread:
        def __init__(self, *args: object, **kwargs: object) -> None:
            pass

        def start(self) -> None:
            raise RuntimeError("THREAD_START_SECRET_SENTINEL")

        def join(self, timeout: int) -> None:
            raise AssertionError("an unstarted writer must never be joined")

        def is_alive(self) -> bool:
            return False

    def runner(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        nonlocal runner_called
        runner_called = True
        raise AssertionError("runner must not be called")

    context = type("Context", (), {"command_runner": runner})()
    monkeypatch.setattr(module, "_write_secret_pipe", pipe)
    monkeypatch.setattr(module.threading, "Thread", FailingThread)

    with pytest.raises(module.OperationStateError) as raised:
        module._query_current_database(context, "postgresql://user:secret@db/degen")

    assert "THREAD_START_SECRET_SENTINEL" not in str(raised.value)
    assert not runner_called
    assert len(opened) == 2
    for descriptor in opened:
        with pytest.raises(OSError):
            os.fstat(descriptor)


@pytest.mark.parametrize("changed_identity", ["database", "hostname"])
def test_host_stage_pre_replace_identity_requery_rejects_change(
    tmp_path: Path, changed_identity: str
) -> None:
    module = load_ops_helper()
    context, fixture = host_staging_fixture(module, tmp_path)
    original_runner = context.command_runner
    counts = {"database": 0, "hostname": 0}

    def changing_runner(argv: object, pass_fds: tuple[int, ...]):
        argv_tuple = tuple(str(value) for value in argv)
        completed = original_runner(argv_tuple, pass_fds)
        if len(argv_tuple) > 4 and argv_tuple[3] == "pgdatabase":
            counts["database"] += 1
            if changed_identity == "database" and counts["database"] == 2:
                return subprocess.CompletedProcess(argv_tuple, 0, "changeddb\n", "")
        if argv_tuple == ("/bin/hostname", "-s"):
            counts["hostname"] += 1
            if changed_identity == "hostname" and counts["hostname"] == 2:
                return subprocess.CompletedProcess(argv_tuple, 0, "changedhost\n", "")
        return completed

    before = context.paths.state_file.read_bytes()
    with pytest.raises(module.OperationStateError, match="identity|prefix|changed"):
        module.prepare_host_staging(
            dataclasses.replace(context, command_runner=changing_runner)
        )

    assert counts[changed_identity] == 2
    assert context.paths.state_file.read_bytes() == before


def test_host_stage_secret_hygiene_excludes_environment_rclone_and_completed_process(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    context, fixture = host_staging_fixture(module, tmp_path)

    result = module.prepare_host_staging(context)

    state_bytes = context.paths.state_file.read_bytes()
    staged_bytes = b"".join(
        path.read_bytes()
        for path in context.paths.staged_dir.rglob("*")
        if path.is_file()
    )
    observable = json.dumps(result, sort_keys=True).encode("utf-8") + state_bytes + staged_bytes
    for forbidden in (
        fixture["database_url"].encode("ascii"),
        b"DB_URL_SENTINEL",
        b"ENV_CONTENT_SENTINEL",
        b"RCLONE_CONTENT_SENTINEL",
    ):
        assert forbidden not in observable
    for completed in fixture["completed_processes"]:
        serialized = f"{completed.args!r}\n{completed.stdout}\n{completed.stderr}"
        assert fixture["database_url"] not in serialized
        assert "DB_URL_SENTINEL" not in serialized


def test_pgdatabase_failure_scrubs_completed_process_and_exception_text(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    context, fixture = host_staging_fixture(module, tmp_path)
    original_runner = context.command_runner
    captured: list[subprocess.CompletedProcess[str]] = []

    def leaking_runner(argv: object, pass_fds: tuple[int, ...]):
        argv_tuple = tuple(str(value) for value in argv)
        if len(argv_tuple) > 4 and argv_tuple[3] == "pgdatabase":
            completed = subprocess.CompletedProcess(
                (fixture["database_url"],),
                1,
                fixture["database_url"],
                "DATABASE_URL=" + fixture["database_url"],
            )
            captured.append(completed)
            return completed
        return original_runner(argv_tuple, pass_fds)

    with pytest.raises(module.OperationStateError) as raised:
        module.prepare_host_staging(dataclasses.replace(context, command_runner=leaking_runner))

    assert fixture["database_url"] not in str(raised.value)
    assert "DB_URL_SENTINEL" not in str(raised.value)
    assert len(captured) == 1
    state_observable = context.paths.state_file.read_text(encoding="utf-8")
    assert fixture["database_url"] not in state_observable
    assert "DB_URL_SENTINEL" not in state_observable
    assert not context.paths.staged_dir.exists()


def test_host_stage_atomic_pre_replace_revalidates_source_and_staged_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = load_ops_helper()
    context, _fixture = host_staging_fixture(module, tmp_path)
    before = context.paths.state_file.read_bytes()

    def tamper(event: str, **_details: object) -> None:
        if event == "before_replace":
            path = context.paths.staged_dir / "host/etc/degen/prod-db-backup.env"
            path.write_bytes(path.read_bytes() + b"TAMPERED=1\n")

    monkeypatch.setattr(module, "_atomic_event_hook", tamper)

    with pytest.raises(module.OperationStateError):
        module.prepare_host_staging(context)

    assert context.paths.state_file.read_bytes() == before


def test_host_stage_atomic_pre_replace_rejects_same_inode_identical_rewrite(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = load_ops_helper()
    context, _fixture = host_staging_fixture(module, tmp_path)
    before = context.paths.state_file.read_bytes()

    def rewrite(event: str, **_details: object) -> None:
        if event == "before_replace":
            path = context.paths.staged_dir / "host-stage-manifest.json"
            raw = path.read_bytes()
            path.write_bytes(raw)
            path.chmod(0o600)

    monkeypatch.setattr(module, "_atomic_event_hook", rewrite)

    with pytest.raises(module.OperationStateError, match="stage|staged|changed"):
        module.prepare_host_staging(context)

    assert context.paths.state_file.read_bytes() == before


@pytest.mark.parametrize(
    "swap_target",
    [
        "archive",
        "source-helper",
        "pair",
        "live-env",
        "staged-manifest",
        "staged-asset",
    ],
)
def test_host_stage_atomic_pre_replace_rejects_same_bytes_inode_swaps(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    swap_target: str,
) -> None:
    module = load_ops_helper()
    context, fixture = host_staging_fixture(module, tmp_path)
    before = context.paths.state_file.read_bytes()

    def replace_same_bytes(path: Path) -> None:
        replacement = path.with_name(path.name + ".same-bytes-replacement")
        replacement.write_bytes(path.read_bytes())
        replacement.chmod(stat.S_IMODE(path.stat().st_mode))
        os.replace(replacement, path)

    def swap(event: str, **_details: object) -> None:
        if event != "before_replace":
            return
        targets = {
            "archive": context.paths.source_archive,
            "source-helper": context.paths.source_dir
            / "deploy/linux/degen-prod-db-backup-env.py",
            "pair": fixture["dump_path"],
            "live-env": fixture["managed_path"],
            "staged-manifest": context.paths.staged_dir / "host-stage-manifest.json",
            "staged-asset": context.paths.staged_dir
            / "reviewed/deploy/linux/degen-prod-db-backup.sh",
        }
        replace_same_bytes(targets[swap_target])

    monkeypatch.setattr(module, "_atomic_event_hook", swap)

    with pytest.raises(module.OperationStateError):
        module.prepare_host_staging(context)

    assert context.paths.state_file.read_bytes() == before


def test_prepare_staging_cli_has_only_operation_dir_and_reconstructs_sealed_context(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = load_ops_helper()
    operation_dir_raw = "/opt/degen/backups/config/20260701T123456Z"
    operation_dir = Path(operation_dir_raw)
    state = source_verified_state(operation_dir)
    observed: list[object] = []
    monkeypatch.setattr(module, "_effective_uid", lambda: 0)
    monkeypatch.setattr(module, "_require_posix_descriptor_primitives", lambda: None)
    monkeypatch.setattr(module, "load_operation_state", lambda path, *, effective_uid: state)
    if os.name != "posix":
        # The production path is POSIX-absolute but pathlib intentionally treats
        # it as drive-relative on Windows; real context validation is covered by
        # the direct API tests and by the Linux follow-up.
        monkeypatch.setattr(module, "validate_operation_state_for_context", lambda *_: None)
    monkeypatch.setattr(
        module,
        "prepare_host_staging",
        lambda context: observed.append(context)
        or {"effective_config": {}, "host_stage": {}},
    )

    help_result = subprocess.run(
        [sys.executable, str(OPS_HELPER), "prepare-staging", "--help"],
        text=True,
        capture_output=True,
        check=False,
    )
    result = module.main(["prepare-staging", "--operation-dir", operation_dir_raw])

    captured = capsys.readouterr()
    assert help_result.returncode == 0
    assert "--operation-dir" in help_result.stdout
    for forbidden in ("archive", "digest", "commit", "manifest", "host-root", "runner"):
        assert forbidden not in help_result.stdout.lower()
    assert result == 0
    assert captured.out == ""
    assert captured.err == ""
    assert len(observed) == 1
    context = observed[0]
    assert context.paths == module.build_operation_paths(operation_dir)
    assert context.operation_id == operation_dir.name
    assert context.expected_commit == state["reviewed_source"]["commit"]
    assert context.expected_archive_sha256 == state["reviewed_source"]["archive_sha256"]
    assert context.expected_manifest_sha256 == state["reviewed_source"]["manifest_sha256"]
    assert context.host_root == Path("/")


@pytest.mark.parametrize("invalid_kind", ["missing", "extra", "enabled-prune", "bad-prefix"])
def test_host_stage_state_requires_exact_disabled_managed_configuration(
    tmp_path: Path, invalid_kind: str
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "staging_prepared")
    if invalid_kind == "missing":
        state["effective_config"].pop("LOCK_FILE")
    elif invalid_kind == "extra":
        state["effective_config"]["UNREVIEWED"] = "value"
    elif invalid_kind == "enabled-prune":
        state["effective_config"]["REMOTE_PRUNE_ENABLED"] = "1"
    else:
        state["effective_config"]["BACKUP_PREFIX"] = "unsafe prefix"

    with pytest.raises(module.OperationStateError, match="effective_config|managed|prefix|prune"):
        module.validate_operation_state(state, operation_dir)
