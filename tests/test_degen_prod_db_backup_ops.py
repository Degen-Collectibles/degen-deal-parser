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
import tempfile
import threading
import time
import types
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

import pytest


ROOT = Path(__file__).resolve().parents[1]
OPS_HELPER = ROOT / "deploy" / "linux" / "degen-prod-db-backup-ops.py"
ENV_HELPER = ROOT / "deploy" / "linux" / "degen-prod-db-backup-env.py"
ENV_EXAMPLE = ROOT / "deploy" / "systemd" / "degen-prod-db-backup.env.example"
HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64
HASH_D = "d" * 64
SAFE_PROBE_PREFIX = (
    "onedrive:backups/degen-db-probe/"
    "20260630T235959Z-0123456789abcdef0123456789abcdef/"
)
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


def task8_enabled_environment_bytes(disabled: bytes) -> bytes:
    disabled_assignment = b"REMOTE_PRUNE_ENABLED=0\n"
    enabled_assignment = b"REMOTE_PRUNE_ENABLED=1\n"
    assert disabled.count(disabled_assignment) == 1
    assert enabled_assignment not in disabled
    return disabled.replace(disabled_assignment, enabled_assignment, 1)


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


def task8_evidence_sha256(label: str, payload: object) -> str:
    canonical = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(
        b"degen-task8-" + label.encode("ascii") + b"-v1\n" + canonical + b"\n"
    ).hexdigest()


def task7_evidence_sha256(label: str, payload: object) -> str:
    canonical = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(
        b"degen-task7-" + label.encode("ascii") + b"-v1\n" + canonical + b"\n"
    ).hexdigest()


def task8_probe_objects(
    *,
    prefix: str = SAFE_PROBE_PREFIX,
    created: bool = False,
    verified: bool = False,
    cleaned: bool = False,
) -> list[dict[str, object]]:
    identity = prefix.rstrip("/").rsplit("/", 1)[-1]
    operation_id, separator, token = identity.rpartition("-")
    assert separator == "-" and operation_id and len(token) == 32
    assert token == token.lower() and all(character in "0123456789abcdef" for character in token)
    dump_bytes = (
        "degen-db-remote-probe-v1\n"
        f"operation_id={operation_id}\n"
        f"token={token}\n"
    ).encode("ascii")
    dump_sha256 = hashlib.sha256(dump_bytes).hexdigest()
    sidecar_bytes = f"{dump_sha256}  probe.dump\n".encode("ascii")
    return [
        {
            "name": "probe.dump",
            "expected_sha256": dump_sha256,
            "expected_size": len(dump_bytes),
            "created": created,
            "verified": verified,
            "cleaned": cleaned,
        },
        {
            "name": "probe.dump.sha256",
            "expected_sha256": hashlib.sha256(sidecar_bytes).hexdigest(),
            "expected_size": len(sidecar_bytes),
            "created": created,
            "verified": verified,
            "cleaned": cleaned,
        },
    ]


def task8_probe_entry_evidence(
    operation_dir: Path,
    epoch: int,
    objects: list[dict[str, object]],
    *,
    prefix: str = SAFE_PROBE_PREFIX,
) -> str:
    core = {
        "operation_id": operation_dir.name,
        "operation_dir": str(operation_dir),
        "epoch": epoch,
        "prior_stable_phase": "installed",
        "prefix": prefix,
        "objects": [
            {
                "name": item["name"],
                "expected_sha256": item["expected_sha256"],
                "expected_size": item["expected_size"],
            }
            for item in objects
        ],
    }
    return task8_evidence_sha256("probe-entry", core)


def task8_file_audit(
    sha256: str,
    *,
    inode: int,
    size: int,
    mtime_ns: int,
) -> dict[str, object]:
    return {
        "sha256": sha256,
        "inode": inode,
        "uid": 0,
        "gid": 0,
        "mode": 0o600,
        "size": size,
        "mtime_ns": mtime_ns,
    }


def task8_rclone_group(
    kind: str,
    started_epoch: int,
    attempt_ordinal: int,
    group_ordinal: int,
    *,
    pending: bool = False,
    purpose: str = "remote-list",
    outcome: str = "success",
    result_sha256: str | None = HASH_A,
) -> dict[str, object]:
    group_id = (
        f"task8:{kind}:{started_epoch}:{attempt_ordinal}:{group_ordinal}"
    )
    before = task8_file_audit(
        HASH_A,
        inode=10 + group_ordinal,
        size=100 + group_ordinal,
        mtime_ns=1_750_000_000_000_000_000 + group_ordinal,
    )
    after = None if pending else task8_file_audit(
        HASH_B,
        inode=10 + group_ordinal,
        size=101 + group_ordinal,
        mtime_ns=1_750_000_001_000_000_000 + group_ordinal,
    )
    payload = {
        "group_id": group_id,
        "purpose": purpose,
        "before": before,
        "after": after,
        "outcome": None if pending else outcome,
    }
    if kind in {"dry_run", "policy", "observe"}:
        payload["result_sha256"] = (
            None if pending or outcome == "indeterminate" else result_sha256
        )
    return {
        **payload,
        "evidence_sha256": (
            None
            if pending
            else task7_evidence_sha256(
                "rclone-audit",
                payload,
            )
        ),
    }


def task8_successful_probe_groups(
    started_epoch: int = 1_750_000_050,
    attempt_ordinal: int = 0,
) -> list[dict[str, object]]:
    purposes = [
        "probe-precreate-root",
        "probe-precreate-root-absence",
        *(
            f"probe-create:{name}:strict-no-existing"
            for name in ("probe.dump", "probe.dump.sha256")
        ),
        "probe-owned-inventory",
        *(f"probe-verify:{name}" for name in ("probe.dump", "probe.dump.sha256")),
        *(f"probe-cleanup:{name}" for name in ("probe.dump", "probe.dump.sha256")),
        "probe-prefix-empty",
    ]
    return [
        task8_rclone_group(
            "probe",
            started_epoch,
            attempt_ordinal,
            ordinal,
            purpose=purpose,
        )
        for ordinal, purpose in enumerate(purposes)
    ]


def task8_pending_copy(group: dict[str, object]) -> dict[str, object]:
    pending = copy.deepcopy(group)
    pending["after"] = None
    if str(pending["group_id"]).startswith("task8:"):
        pending["outcome"] = None
    if str(pending["group_id"]).startswith(("task8:dry_run:", "task8:policy:")):
        pending["result_sha256"] = None
    pending["evidence_sha256"] = None
    return pending


def task8_probe_completion_evidence(
    operation_dir: Path,
    *,
    entry: dict[str, object],
    completed_epoch: int,
    prefix: str,
    owned_names: list[str],
    cleanup_proven: bool,
    groups: list[dict[str, object]],
) -> str:
    return task8_evidence_sha256(
        "probe-complete",
        {
            "operation_id": operation_dir.name,
            "operation_dir": str(operation_dir),
            "entry": {
                "epoch": entry["epoch"],
                "evidence_sha256": entry["evidence_sha256"],
            },
            "completed_epoch": completed_epoch,
            "prefix": prefix,
            "owned_names": owned_names,
            "cleanup_proven": cleanup_proven,
            "rclone_groups": groups,
        },
    )


def task8_reseal_probe_completion(
    state: dict[str, object],
    operation_dir: Path,
) -> str:
    history = state["phase_history"]
    probe = state["probe"]
    groups = state["rclone_evidence_groups"]
    assert isinstance(history, list)
    assert isinstance(probe, dict)
    assert isinstance(groups, list)
    entry = next(item for item in reversed(history) if item["phase"] == "probing")
    completed = next(item for item in reversed(history) if item["phase"] == "probed")
    probe_entries = [item for item in history if item["phase"] == "probing"]
    attempt_ordinal = next(
        index for index, candidate in enumerate(probe_entries) if candidate is entry
    )
    group_prefix = f"task8:probe:{entry['epoch']}:{attempt_ordinal}:"
    matching_groups = [
        group
        for group in groups
        if isinstance(group, dict)
        and isinstance(group.get("group_id"), str)
        and str(group["group_id"]).startswith(group_prefix)
        and group.get("after") is not None
    ]
    matching_groups.sort(key=lambda group: int(str(group["group_id"]).rsplit(":", 1)[1]))
    evidence = task8_probe_completion_evidence(
        operation_dir,
        entry=entry,
        completed_epoch=int(completed["epoch"]),
        prefix=str(probe["prefix"]),
        owned_names=list(probe["owned_names"]),
        cleanup_proven=bool(probe["cleanup_proven"]),
        groups=matching_groups,
    )
    probe["evidence_sha256"] = evidence
    completed["evidence_sha256"] = evidence
    return evidence


def task8_reseal_rclone_group(
    group: dict[str, object],
    *,
    dry_run_receipt: dict[str, object] | None = None,
) -> None:
    if dry_run_receipt is not None:
        purpose = str(group["purpose"])
        group["result_sha256"] = (
            task8_expected_dry_run_result_sha256(purpose, dry_run_receipt)
            if group["outcome"] == "success"
            else None
        )
    payload = {
        "group_id": group["group_id"],
        "purpose": group["purpose"],
        "before": group["before"],
        "after": group["after"],
    }
    if str(group["group_id"]).startswith("task8:"):
        payload["outcome"] = group["outcome"]
    if str(group["group_id"]).startswith(("task8:dry_run:", "task8:policy:")):
        payload["result_sha256"] = group["result_sha256"]
    group["evidence_sha256"] = task7_evidence_sha256(
        "rclone-audit",
        payload,
    )


def task8_policy_inventory_result_sha256(dry_run: dict[str, object]) -> str:
    return task8_evidence_sha256(
        "policy-inventory-result",
        {
            field: dry_run[field]
            for field in (
                "inventory_names",
                "casefold_names",
                "keep_names",
                "protected_names",
                "delete_names",
                "candidate_sha256",
            )
        },
    )


def task8_successful_policy_group(
    dry_run: dict[str, object],
    *,
    started_epoch: int = 1_750_000_090,
    attempt_ordinal: int = 0,
) -> dict[str, object]:
    return task8_rclone_group(
        "policy",
        started_epoch,
        attempt_ordinal,
        0,
        purpose="enable-prune-inventory",
        result_sha256=task8_policy_inventory_result_sha256(dry_run),
    )


def task8_policy_runtime_baseline_sha256(
    runtime_baseline: dict[str, object],
) -> str:
    return task8_evidence_sha256(
        "policy-runtime-baseline",
        runtime_baseline,
    )


def task8_policy_entry_evidence(
    state: dict[str, object],
    entry: dict[str, object],
    *,
    runtime_baseline: dict[str, object],
) -> str:
    return task8_evidence_sha256(
        "policy-entry",
        {
            "operation_id": state["operation_id"],
            "operation_dir": state["operation_dir"],
            "epoch": entry["epoch"],
            "prior_stable_phase": "dry_run_recorded",
            "effective_config": state["effective_config"],
            "installed_hashes": state["install"]["installed_hashes"],
            "dry_run_evidence_sha256": state["dry_run"]["evidence_sha256"],
            "enabled_environment_sha256": state["host_stage"][
                "enabled_environment_sha256"
            ],
            "runtime_baseline_sha256": task8_policy_runtime_baseline_sha256(
                runtime_baseline
            ),
        },
    )


def task8_policy_applied_evidence(
    state: dict[str, object],
    entry: dict[str, object],
    policy: dict[str, object],
    group: dict[str, object],
) -> str:
    return task8_evidence_sha256(
        "policy-applied",
        {
            "entry": {
                "epoch": entry["epoch"],
                "evidence_sha256": entry["evidence_sha256"],
            },
            "enabled_epoch": policy["enabled_epoch"],
            "environment_sha256": policy["environment_sha256"],
            "runtime_baseline_sha256": policy["runtime_baseline_sha256"],
            "applied_target": policy["applied_target"],
            "dry_run_evidence_sha256": state["dry_run"]["evidence_sha256"],
            "inventory_group": group,
        },
    )


def task8_policy_receipt(
    state: dict[str, object],
    entry: dict[str, object],
    group: dict[str, object],
    *,
    runtime_baseline: dict[str, object],
    environment_sha256: str = HASH_B,
    enabled_epoch: int | None = None,
    uid: int = 0,
    gid: int = 0,
) -> dict[str, object]:
    if enabled_epoch is None:
        enabled_epoch = int(entry["epoch"]) + 1
    receipt: dict[str, object] = {
        "environment_sha256": environment_sha256,
        "enabled_epoch": enabled_epoch,
        "runtime_baseline_sha256": task8_policy_runtime_baseline_sha256(
            runtime_baseline
        ),
        "applied_target": {
            "present": True,
            "sha256": environment_sha256,
            "mode": 0o600,
            "uid": uid,
            "gid": gid,
        },
        "applied_evidence_sha256": HASH_A,
    }
    receipt["applied_evidence_sha256"] = task8_policy_applied_evidence(
        state,
        entry,
        receipt,
        group,
    )
    return receipt


def task8_policy_completion_evidence(
    *,
    entry: dict[str, object],
    completed_epoch: int,
    policy: dict[str, object],
    dry_run: dict[str, object],
    group: dict[str, object],
    recovery: dict[str, object] | None = None,
) -> str:
    return task8_evidence_sha256(
        "policy-complete",
        {
            "entry": {
                "epoch": entry["epoch"],
                "evidence_sha256": entry["evidence_sha256"],
            },
            "completed_epoch": completed_epoch,
            "policy": policy,
            "dry_run_evidence_sha256": dry_run["evidence_sha256"],
            "inventory_group": group,
            "policy_recovery": (
                None
                if recovery is None
                else {
                    "kind": recovery["kind"],
                    "started_epoch": recovery["started_epoch"],
                    "completed_epoch": recovery["completed_epoch"],
                    "evidence_sha256": recovery["evidence_sha256"],
                    "next_target_index": recovery["next_target_index"],
                }
            ),
        },
    )


def task7_install_rclone_group() -> dict[str, object]:
    before = task8_file_audit(
        HASH_A,
        inode=9,
        size=99,
        mtime_ns=1_749_999_999_000_000_000,
    )
    after = task8_file_audit(
        HASH_B,
        inode=9,
        size=100,
        mtime_ns=1_750_000_000_000_000_000,
    )
    group = {
        "group_id": "install",
        "purpose": "credential-refresh-audit",
        "before": before,
        "after": after,
        "evidence_sha256": None,
    }
    task8_reseal_rclone_group(group)
    return group


TASK8_OBSERVE_PURPOSES = (
    "observe-inventory-before",
    "observe-current-stat-before",
    "observe-current-hash-before",
    "observe-current-sidecar-before",
    "observe-inventory-after",
    "observe-current-hash-after",
    "observe-current-sidecar-after",
    "observe-current-stat-after",
)


def task8_observation_entry_evidence_fixture(
    state: dict[str, object],
    entry: dict[str, object],
    run_epoch: int,
) -> str:
    runtime_baseline = copy.deepcopy(state["prior_runtime"])
    runtime_baseline["preinstall_trigger_epoch"] = run_epoch
    return task8_evidence_sha256(
        "observation-entry",
        {
            "operation_id": state["operation_id"],
            "operation_dir": state["operation_dir"],
            "epoch": entry["epoch"],
            "prior_stable_phase": "policy_enabled",
            "effective_config": state["effective_config"],
            "installed_hashes": state["install"]["installed_hashes"],
            "policy": state["policy"],
            "runtime_baseline": runtime_baseline,
        },
    )


def task8_observation_groups_fixture(
    started_epoch: int,
) -> list[dict[str, object]]:
    groups = [
        task8_rclone_group(
            "observe",
            started_epoch,
            0,
            ordinal,
            purpose=purpose,
            result_sha256=task8_evidence_sha256(
                "observation-rclone-result",
                {"purpose": purpose, "result_domain": {"ordinal": ordinal}},
            ),
        )
        for ordinal, purpose in enumerate(TASK8_OBSERVE_PURPOSES)
    ]
    for ordinal, group in enumerate(groups):
        if ordinal:
            group["before"] = copy.deepcopy(groups[ordinal - 1]["after"])
        group["evidence_sha256"] = task7_evidence_sha256(
            "rclone-audit",
            {
                "group_id": group["group_id"],
                "purpose": group["purpose"],
                "before": group["before"],
                "after": group["after"],
                "outcome": group["outcome"],
                "result_sha256": group["result_sha256"],
            },
        )
    return groups


def task8_observation_completion_evidence_fixture(
    state: dict[str, object],
    entry: dict[str, object],
    completed: dict[str, object],
    groups: list[dict[str, object]],
) -> str:
    observation = state["observation"]
    return task8_evidence_sha256(
        "observation-complete",
        {
            "operation_id": state["operation_id"],
            "operation_dir": state["operation_dir"],
            "entry": {
                "epoch": entry["epoch"],
                "evidence_sha256": entry["evidence_sha256"],
            },
            "completed_epoch": completed["epoch"],
            "run_epoch": observation["run_epoch"],
            "journal_sha256": observation["journal_sha256"],
            "local_sha256": observation["local_sha256"],
            "remote_sha256": observation["remote_sha256"],
            "install_completed_epoch": state["install"]["completed_epoch"],
            "policy_enabled_epoch": state["policy"]["enabled_epoch"],
            "policy_applied_evidence_sha256": state["policy"][
                "applied_evidence_sha256"
            ],
            "preinstall_trigger_epoch": state["prior_runtime"][
                "preinstall_trigger_epoch"
            ],
            "rclone_groups": groups,
        },
    )


def observed_state(operation_dir: Path) -> dict[str, object]:
    state = source_verified_state(operation_dir)
    append_phase(state, "staging_prepared", 1_750_000_010)
    state["effective_config"] = dict(EFFECTIVE_CONFIG)
    state["host_stage"] = {
        "manifest_sha256": HASH_A,
        "asset_hashes": {asset: HASH_A for asset in SOURCE_ASSETS},
        "environment_sha256": HASH_C,
        "enabled_environment_sha256": HASH_B,
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
        "pids": {
            "postgresql:postgresql@15-main.service": 200,
            "system:degen-web.service": 100,
            "system:degen-worker.service": 101,
            "user:1001:degen:degen-ops-discord-bot.service": 102,
        },
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
        "runtime_directory_created": False,
        "validated_epoch": 1_750_000_039,
        "validation_evidence_sha256": HASH_B,
    }
    append_phase(state, "installed", 1_750_000_040)
    state["install"]["installed_hashes"] = {target: HASH_A for target in TARGETS}
    state["install"]["installed_hashes"]["/etc/degen/prod-db-backup.env"] = HASH_C
    state["install"]["completed_epoch"] = 1_750_000_040
    append_phase(state, "probing", 1_750_000_050)
    history = state["phase_history"]
    assert isinstance(history, list)
    probe_entry = history[-1]
    assert isinstance(probe_entry, dict)
    probe_entry["evidence_sha256"] = task8_probe_entry_evidence(
        operation_dir,
        1_750_000_050,
        task8_probe_objects(),
    )
    probe_groups = task8_successful_probe_groups()
    state["rclone_evidence_groups"] = copy.deepcopy(probe_groups)
    append_phase(state, "probed", 1_750_000_060)
    owned_names = ["probe.dump", "probe.dump.sha256"]
    completion_evidence = task8_probe_completion_evidence(
        operation_dir,
        entry=probe_entry,
        completed_epoch=1_750_000_060,
        prefix=SAFE_PROBE_PREFIX,
        owned_names=owned_names,
        cleanup_proven=True,
        groups=probe_groups,
    )
    history[-1]["evidence_sha256"] = completion_evidence
    state["probe"] = {
        "prefix": SAFE_PROBE_PREFIX,
        "owned_names": owned_names,
        "cleanup_proven": True,
        "evidence_sha256": completion_evidence,
    }
    append_phase(state, "dry_run_recording", 1_750_000_070)
    dry_run_entry = history[-1]
    assert isinstance(dry_run_entry, dict)
    dry_run_entry["evidence_sha256"] = task8_dry_run_entry_evidence(
        state,
        dry_run_entry,
    )
    dry_run_groups = task8_successful_dry_run_groups()
    state["rclone_evidence_groups"].extend(copy.deepcopy(dry_run_groups))
    append_phase(state, "dry_run_recorded", 1_750_000_080)
    state["dry_run"] = task8_valid_dry_run_receipt()
    task8_reseal_dry_run_state(state)
    append_phase(state, "policy_enabling", 1_750_000_090)
    policy_entry = history[-1]
    assert isinstance(policy_entry, dict)
    policy_entry["evidence_sha256"] = task8_policy_entry_evidence(
        state,
        policy_entry,
        runtime_baseline=recovery_runtime_baseline(),
    )
    policy_group = task8_successful_policy_group(state["dry_run"])
    state["rclone_evidence_groups"].append(policy_group)
    state["policy"] = task8_policy_receipt(
        state,
        policy_entry,
        policy_group,
        runtime_baseline=recovery_runtime_baseline(),
    )
    append_phase(state, "policy_enabled", 1_750_000_100)
    history[-1]["evidence_sha256"] = task8_policy_completion_evidence(
        entry=policy_entry,
        completed_epoch=1_750_000_100,
        policy=state["policy"],
        dry_run=state["dry_run"],
        group=policy_group,
    )
    append_phase(state, "observing", 1_750_000_110)
    observation_entry = history[-1]
    observation_run_epoch = 1_750_000_105
    observation_entry["evidence_sha256"] = (
        task8_observation_entry_evidence_fixture(
            state,
            observation_entry,
            observation_run_epoch,
        )
    )
    observation_groups = task8_observation_groups_fixture(1_750_000_110)
    state["rclone_evidence_groups"].extend(copy.deepcopy(observation_groups))
    append_phase(state, "observed", 1_750_000_120)
    state["observation"] = {
        "run_epoch": observation_run_epoch,
        "journal_sha256": HASH_A,
        "local_sha256": HASH_B,
        "remote_sha256": HASH_C,
        "evidence_sha256": HASH_A,
    }
    observation_completion = task8_observation_completion_evidence_fixture(
        state,
        observation_entry,
        history[-1],
        observation_groups,
    )
    state["observation"]["evidence_sha256"] = observation_completion
    history[-1]["evidence_sha256"] = observation_completion
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
        "runtime_baseline": recovery_runtime_baseline(),
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
            "prefix": SAFE_PROBE_PREFIX,
            "objects": task8_probe_objects(created=True, verified=True),
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
        "runtime_directory_created": False,
        "runtime_baseline": recovery_runtime_baseline(),
        "restored_epoch": None,
        "restore_evidence_sha256": None,
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
            "prefix": SAFE_PROBE_PREFIX,
            "objects": task8_probe_objects(),
        }
    return {
        "kind": kind,
        "prior_stable_phase": prior_phase,
        "prior_timer_enabled": True,
        "prior_timer_active": True,
        "runtime_baseline": recovery_runtime_baseline(),
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


def recovery_runtime_baseline() -> dict[str, object]:
    return {
        "timer_enabled": True,
        "timer_active": True,
        "pids": {
            "postgresql:postgresql@15-main.service": 200,
            "system:degen-web.service": 100,
            "system:degen-worker.service": 101,
            "user:1001:degen:degen-ops-discord-bot.service": 102,
        },
        "preinstall_trigger_epoch": None,
    }


def state_at_phase(operation_dir: Path, phase: str) -> dict[str, object]:
    if phase not in NORMAL_PHASES:
        raise ValueError(phase)
    complete = observed_state(operation_dir)
    cutoff = NORMAL_PHASES.index(phase)
    state = copy.deepcopy(complete)
    state["phase"] = phase
    state["phase_history"] = copy.deepcopy(complete["phase_history"][: cutoff + 1])
    if cutoff < NORMAL_PHASES.index("probed"):
        state["rclone_evidence_groups"] = []
    else:
        groups = copy.deepcopy(complete["rclone_evidence_groups"])
        if cutoff < NORMAL_PHASES.index("dry_run_recorded"):
            groups = [
                group
                for group in groups
                if not str(group["group_id"]).startswith("task8:dry_run:")
            ]
        if cutoff < NORMAL_PHASES.index("policy_enabled"):
            groups = [
                group
                for group in groups
                if not str(group["group_id"]).startswith("task8:policy:")
            ]
        if cutoff < NORMAL_PHASES.index("observed"):
            groups = [
                group
                for group in groups
                if not str(group["group_id"]).startswith("task8:observe:")
            ]
        state["rclone_evidence_groups"] = groups
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
            "runtime_directory_created": False,
            "validated_epoch": None,
            "validation_evidence_sha256": None,
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
        if phase == "observing":
            state["active_transaction"]["runtime_baseline"][
                "preinstall_trigger_epoch"
            ] = 1_750_000_105
            state["phase_history"][-1]["evidence_sha256"] = (
                task8_observation_entry_evidence_fixture(
                    state,
                    state["phase_history"][-1],
                    1_750_000_105,
                )
            )
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
    runtime_directory_created: bool = False,
    restored_epoch: int | None = None,
    restore_evidence_sha256: str | None = None,
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
        "runtime_directory_created": runtime_directory_created,
        "runtime_baseline": recovery_runtime_baseline(),
        "restored_epoch": restored_epoch,
        "restore_evidence_sha256": restore_evidence_sha256,
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
                "restored_epoch": 1_750_000_049,
                "restore_evidence_sha256": HASH_B,
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
                "restored_epoch": 1_750_000_059,
                "restore_evidence_sha256": HASH_B,
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
    if isinstance(state.get("active_transaction"), dict):
        state["recovery"]["runtime_baseline"] = copy.deepcopy(
            state["active_transaction"]["runtime_baseline"]
        )
    if recovery_kind == "policy":
        append_phase(state, "recovering_policy", 1_750_000_121)
        state["recovery"]["evidence_sha256"] = state["phase_history"][-1][
            "evidence_sha256"
        ]
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


def test_task9_tracked_asset_manifest_is_exact_and_current() -> None:
    module = load_ops_helper()
    manifest_path = ROOT / SOURCE_MANIFEST
    lf_pinned_paths = {*SOURCE_ASSETS, SOURCE_MANIFEST}
    for path in sorted(lf_pinned_paths):
        attributes = subprocess.run(
            ["git", "check-attr", "text", "eol", "--", path],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.splitlines()
        assert attributes == [f"{path}: text: set", f"{path}: eol: lf"]
        raw = (ROOT / path).read_bytes()
        assert b"\r" not in raw
        unfiltered = subprocess.run(
            ["git", "hash-object", "--no-filters", "--stdin"],
            cwd=ROOT,
            check=True,
            input=raw,
            capture_output=True,
        ).stdout.strip()
        filtered = subprocess.run(
            ["git", "hash-object", f"--path={path}", "--stdin"],
            cwd=ROOT,
            check=True,
            input=raw,
            capture_output=True,
        ).stdout.strip()
        assert filtered == unfiltered
    expected_records = {
        asset: hashlib.sha256((ROOT / asset).read_bytes()).hexdigest()
        for asset in sorted(SOURCE_ASSETS)
    }
    expected = b"".join(
        f"{digest}  {asset}\n".encode("ascii")
        for asset, digest in expected_records.items()
    )

    raw = manifest_path.read_bytes()

    assert raw == expected
    assert module._parse_source_manifest(raw) == expected_records


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
    ("active_transaction", "runtime_baseline"),
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
        (("host_stage", "enabled_environment_sha256"), "B" * 64, "SHA-256"),
        (("install", "next_target_index"), True, "next_target_index"),
        (("active_transaction", "prior_timer_enabled"), 1, "prior_timer_enabled"),
        (("active_transaction", "runtime_baseline", "timer_active"), 1, "timer_active"),
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


def test_task8_host_stage_schema_requires_precomputed_enabled_environment_digest(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = schema_rich_state(operation_dir)
    del state["host_stage"]["enabled_environment_sha256"]

    with pytest.raises(module.OperationStateError, match="enabled_environment_sha256"):
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
    previous["rclone_evidence_groups"].insert(0, task7_install_rclone_group())
    mark_transaction_complete(previous)
    previous["recovery"].update(
        {
            "next_target_index": 1,
            "current_target": None,
            "previous_sha256": None,
            "intended_sha256": None,
        }
    )
    current = copy.deepcopy(previous)
    current["probe"] = None
    current["dry_run"] = None
    current["policy"] = None
    current["observation"] = None
    current["active_transaction"] = None
    current["recovery"].update(
        {
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


def test_policy_recovery_terminal_transition_requires_prior_cursor_write(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    previous = failed_transaction_state(
        operation_dir,
        "policy",
        "recovering_policy",
    )
    mark_transaction_complete(previous)
    current = copy.deepcopy(previous)
    for field in ("probe", "dry_run", "policy", "observation", "active_transaction"):
        current[field] = None
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

    with pytest.raises(
        module.OperationStateError,
        match="prior cursor|terminal|uncommitted policy.*guard",
    ):
        module.validate_operation_state(current, operation_dir, previous)


def task8_committed_policy_recovery_completion(
    operation_dir: Path,
) -> tuple[dict[str, object], dict[str, object]]:
    previous = task8_provisional_policy_state(operation_dir)
    append_phase(previous, "recovering_policy", 1_750_000_121)
    previous["recovery"] = recovery_receipt(
        "policy",
        index=1,
        current_target=None,
        previous_sha256=None,
        intended_sha256=None,
        started_epoch=1_750_000_121,
    )
    previous["recovery"]["evidence_sha256"] = previous["phase_history"][-1][
        "evidence_sha256"
    ]
    mark_transaction_complete(previous)
    current = copy.deepcopy(previous)
    current["active_transaction"] = None
    current["recovery"]["completed_epoch"] = 1_750_000_130
    append_phase(current, "policy_enabled", 1_750_000_130)
    entry = next(
        item
        for item in reversed(current["phase_history"])
        if item["phase"] == "policy_enabling"
    )
    policy = current["policy"]
    dry_run = current["dry_run"]
    group = current["rclone_evidence_groups"][-1]
    assert isinstance(policy, dict) and isinstance(dry_run, dict)
    current["phase_history"][-1]["evidence_sha256"] = (
        task8_policy_completion_evidence(
            entry=entry,
            completed_epoch=1_750_000_130,
            policy=policy,
            dry_run=dry_run,
            group=group,
            recovery=current["recovery"],
        )
    )
    return previous, current


def test_committed_policy_recovery_forward_completes_and_preserves_receipt(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    previous, current = task8_committed_policy_recovery_completion(operation_dir)

    module.validate_operation_state(previous, operation_dir)
    module.validate_operation_state(current, operation_dir, previous)

    assert current["phase"] == "policy_enabled"
    assert current["policy"] == previous["policy"]
    assert current["dry_run"] == previous["dry_run"]
    assert current["recovery"]["completed_epoch"] == 1_750_000_130


def test_policy_completion_evidence_uses_immutable_recovery_history(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    _previous, state = task8_committed_policy_recovery_completion(operation_dir)
    entry = next(
        item
        for item in reversed(state["phase_history"])
        if item["phase"] == "policy_enabling"
    )
    completed = state["phase_history"][-1]
    group = state["rclone_evidence_groups"][-1]
    original = module._task8_policy_completion_evidence_sha256(
        state,
        entry,
        completed,
        state["policy"],
        group,
    )
    state["recovery"] = recovery_receipt(
        "guard",
        index=0,
        current_target=None,
        previous_sha256=None,
        intended_sha256=None,
        started_epoch=1_750_000_140,
        completed_epoch=1_750_000_150,
    )

    assert module._task8_policy_completion_evidence_sha256(
        state,
        entry,
        completed,
        state["policy"],
        group,
    ) == original


def test_policy_recovery_evidence_is_bound_to_attempt_start_history(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    _previous, state = task8_committed_policy_recovery_completion(operation_dir)
    state["recovery"]["evidence_sha256"] = HASH_D
    entry = next(
        item
        for item in reversed(state["phase_history"])
        if item["phase"] == "policy_enabling"
    )
    completed = state["phase_history"][-1]
    completed["evidence_sha256"] = module._task8_policy_completion_evidence_sha256(
        state,
        entry,
        completed,
        state["policy"],
        state["rclone_evidence_groups"][-1],
    )

    with pytest.raises(module.OperationStateError, match="recovery.*evidence.*history"):
        module.validate_operation_state(state, operation_dir)


def test_committed_policy_recovery_completion_epoch_is_bound_to_terminal_phase(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    _previous, current = task8_committed_policy_recovery_completion(operation_dir)
    current["recovery"]["completed_epoch"] = 9_999_999_999
    entry = next(
        item
        for item in reversed(current["phase_history"])
        if item["phase"] == "policy_enabling"
    )
    completed = current["phase_history"][-1]
    current["phase_history"][-1]["evidence_sha256"] = (
        module._task8_policy_completion_evidence_sha256(
            current,
            entry,
            completed,
            current["policy"],
            current["rclone_evidence_groups"][-1],
        )
    )

    with pytest.raises(module.OperationStateError, match="terminal phase epoch"):
        module.validate_operation_state(current, operation_dir)


def test_committed_policy_recovery_cannot_erase_commit_by_rolling_back(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)
    previous, _forward = task8_committed_policy_recovery_completion(operation_dir)
    current = copy.deepcopy(previous)
    for field in ("probe", "dry_run", "policy", "observation", "active_transaction"):
        current[field] = None
    current["recovery"]["completed_epoch"] = 1_750_000_130
    append_phase(current, "installed", 1_750_000_130)

    with pytest.raises(module.OperationStateError, match="committed.*forward|policy"):
        module.validate_operation_state(current, operation_dir, previous)


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
    [
        edge
        for edge in zip(NORMAL_PHASES, NORMAL_PHASES[1:])
        if edge != ("probing", "probed")
    ],
)
def test_every_normal_forward_transition_accepts_exact_receipts(
    tmp_path: Path,
    old_phase: str,
    new_phase: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _ = private_operation_dir(tmp_path)

    previous = (
        task8_provisional_policy_state(operation_dir)
        if (old_phase, new_phase) == ("policy_enabling", "policy_enabled")
        else state_at_phase(operation_dir, old_phase)
    )
    if previous["active_transaction"] is not None:
        mark_transaction_complete(previous)
    if old_phase == "installing":
        previous["install"].update(
            {
                "next_target_index": len(TARGETS),
                "current_target": None,
                "previous_sha256": None,
                "intended_sha256": None,
                "validated_epoch": 1_750_000_039,
                "validation_evidence_sha256": HASH_B,
            }
        )
    current = state_at_phase(operation_dir, new_phase)
    if (old_phase, new_phase) == (
        "dry_run_recording",
        "dry_run_recorded",
    ):
        # A real dry-run reaches its stable receipt only after all three
        # audited command groups have already been persisted one at a time.
        previous["rclone_evidence_groups"] = copy.deepcopy(
            current["rclone_evidence_groups"]
        )
    if (old_phase, new_phase) == ("observing", "observed"):
        previous["rclone_evidence_groups"] = copy.deepcopy(
            current["rclone_evidence_groups"]
        )
    module.validate_operation_state(
        current,
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
    resumed["recovery"].update(
        {
            "next_target_index": 1,
            "current_target": None,
            "previous_sha256": None,
            "intended_sha256": None,
        }
    )

    installed = copy.deepcopy(resumed)
    for field in ("probe", "dry_run", "policy", "observation", "active_transaction"):
        installed[field] = None
    installed["recovery"].update(
        {
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
        previous = (
            task8_provisional_policy_state(operation_dir)
            if transient == "policy_enabling"
            else state_at_phase(operation_dir, transient)
        )
        module.validate_operation_state(
            state_at_phase(operation_dir, stable),
            operation_dir,
            previous,
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
    transaction = probing["active_transaction"]
    assert isinstance(transaction, dict)
    probe = transaction["probe"]
    assert isinstance(probe, dict)
    objects = probe["objects"]
    assert isinstance(objects, list)
    history = probing["phase_history"]
    assert isinstance(history, list)
    history[-1]["evidence_sha256"] = task8_probe_entry_evidence(
        operation_dir,
        1_750_000_140,
        objects,
        prefix=str(probe["prefix"]),
    )
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
    with pytest.raises(
        module.OperationStateError,
        match="completed recovery|terminal phase epoch",
    ):
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
    assert state["active_transaction"]["policy_environment_sha256"] == state["host_stage"][
        "enabled_environment_sha256"
    ]

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
    previous = task8_provisional_policy_state(operation_dir)
    previous["active_transaction"]["policy_environment_sha256"] = HASH_B
    mark_transaction_complete(previous)
    current = state_at_phase(operation_dir, "policy_enabled")
    assert current["policy"]["environment_sha256"] == current["host_stage"][
        "enabled_environment_sha256"
    ]
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
    enabled_environment_sha256: str,
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
            "enabled_sha256": enabled_environment_sha256,
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
    enabled_environment_bytes = task8_enabled_environment_bytes(environment_bytes)
    enabled_environment_sha256 = hashlib.sha256(enabled_environment_bytes).hexdigest()
    expected_manifest = expected_host_stage_manifest(
        context,
        fixture["assets"],
        environment_sha256,
        enabled_environment_sha256,
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
        "enabled_environment_sha256": enabled_environment_sha256,
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
    monkeypatch.setattr(module, "_PG_RESTORE_EXECUTABLE", sys.executable)

    def reject_unbounded_run(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("pg_restore must use the bounded Popen runner")

    monkeypatch.setattr(module.subprocess, "run", reject_unbounded_run)
    child = "import os;os.write(1,b'x'*1048576);os.write(2,b'y'*1048576)"

    completed = module._default_command_runner(
        (sys.executable, "-c", child),
        (),
    )

    assert completed.stdout == ""
    assert completed.stderr == ""


def test_existing_pair_default_pg_restore_honors_bounded_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    monkeypatch.setattr(module, "_PG_RESTORE_EXECUTABLE", sys.executable)
    started = time.monotonic()

    with pytest.raises(module.OperationStateError, match="timeout"):
        module._default_command_runner(
            (sys.executable, "-c", "import time;time.sleep(1)"),
            (),
            timeout_seconds=0.1,
        )

    assert time.monotonic() - started < 0.8


def test_checked_command_keeps_default_limit_and_allows_larger_bounded_output() -> None:
    module = load_ops_helper()
    argv = (sys.executable, "-c", "print('bounded')")
    payload = "x" * (module._MAX_COMMAND_OUTPUT_BYTES + 1)

    def runner(
        command: object,
        _pass_fds: tuple[int, ...],
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(tuple(command), 0, payload, "")

    context = types.SimpleNamespace(command_runner=runner)
    with pytest.raises(module.OperationStateError, match="size limit"):
        module._checked_command(context, argv, (), "default bounded command")

    completed = module._checked_command(
        context,
        argv,
        (),
        "explicit bounded command",
        max_output_bytes=len(payload.encode("utf-8")),
    )

    assert completed.stdout == payload
    assert completed.stderr == ""


def test_checked_command_rejects_output_limit_above_hard_state_bound() -> None:
    module = load_ops_helper()
    called = False

    def runner(
        command: object,
        _pass_fds: tuple[int, ...],
    ) -> subprocess.CompletedProcess[str]:
        nonlocal called
        called = True
        return subprocess.CompletedProcess(tuple(command), 0, "", "")

    context = types.SimpleNamespace(command_runner=runner)
    with pytest.raises(module.OperationStateError, match="output.*limit|limit.*output"):
        module._checked_command(
            context,
            ("/bin/true",),
            (),
            "invalid output bound",
            max_output_bytes=module._MAX_STATE_BYTES + 1,
        )

    assert not called


def test_default_command_runner_uses_bounded_pipe_capture_not_subprocess_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    argv = (
        sys.executable,
        "-c",
        "import sys;sys.stdout.write('ok');sys.stderr.write('warning')",
    )

    def reject_unbounded_run(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("generic command capture must not use subprocess.run")

    monkeypatch.setattr(module.subprocess, "run", reject_unbounded_run)

    completed = module._default_command_runner(
        argv,
        (),
        max_output_bytes=1024,
    )

    assert tuple(completed.args) == argv
    assert completed.returncode == 0
    assert completed.stdout == "ok"
    assert completed.stderr == "warning"


def test_default_command_runner_drains_both_streams_and_fails_closed_over_cap() -> None:
    child = (
        "import os\n"
        "chunk_out=b'OVER_CAP_STDOUT_SENTINEL'*256\n"
        "chunk_err=b'OVER_CAP_STDERR_SENTINEL'*256\n"
        "for _ in range(256): os.write(1,chunk_out)\n"
        "for _ in range(256): os.write(2,chunk_err)\n"
    )
    driver = f"""
import importlib.util
import sys
spec = importlib.util.spec_from_file_location("bounded_runner_probe", {str(OPS_HELPER)!r})
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
try:
    module._default_command_runner(
        (sys.executable, "-c", {child!r}),
        (),
        max_output_bytes=8192,
    )
except module.OperationStateError as exc:
    message = str(exc).lower()
    if "output" not in message or not any(word in message for word in ("size", "limit", "cap")):
        raise SystemExit("wrong bounded-output error: " + message)
    print("bounded-output-rejected")
else:
    raise SystemExit("oversized command output was accepted")
"""

    completed = subprocess.run(
        (sys.executable, "-c", driver),
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout == "bounded-output-rejected\n"
    assert completed.stderr == ""
    assert "OVER_CAP_STDOUT_SENTINEL" not in completed.stdout
    assert "OVER_CAP_STDERR_SENTINEL" not in completed.stderr


@pytest.mark.skipif(os.name != "posix", reason="process-group cleanup requires POSIX")
@pytest.mark.parametrize(
    ("redirect_descendant_output", "discard_parent_output"),
    ((False, False), (True, False), (True, True)),
)
def test_default_command_runner_timeout_kills_descendants_across_output_modes(
    tmp_path: Path,
    redirect_descendant_output: bool,
    discard_parent_output: bool,
) -> None:
    pid_file = tmp_path / "descendant.pid"
    descendant = (
        "import signal,time\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "time.sleep(60)\n"
    )
    descendant_stdio = (
        ",stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL"
        if redirect_descendant_output
        else ""
    )
    child = (
        "import pathlib,subprocess,sys,time\n"
        f"proc=subprocess.Popen([sys.executable,'-c',{descendant!r}]{descendant_stdio})\n"
        f"pathlib.Path({str(pid_file)!r}).write_text(str(proc.pid), encoding='ascii')\n"
        "time.sleep(60)\n"
    )
    driver = f"""
import importlib.util
import os
import pathlib
import signal
import sys
import time
spec = importlib.util.spec_from_file_location("bounded_runner_tree_probe", {str(OPS_HELPER)!r})
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
if {discard_parent_output!r}:
    module._PG_RESTORE_EXECUTABLE = sys.executable
try:
    module._default_command_runner(
        (sys.executable, "-c", {child!r}),
        (),
        max_output_bytes=1024,
        timeout_seconds=0.5,
    )
except module.OperationStateError as exc:
    if "timeout" not in str(exc).lower():
        raise SystemExit("wrong timeout error: " + str(exc))
else:
    raise SystemExit("timed command unexpectedly completed")
pid_path = pathlib.Path({str(pid_file)!r})
if not pid_path.is_file():
    raise SystemExit("descendant pid was not recorded")
pid = int(pid_path.read_text(encoding="ascii"))
deadline = time.monotonic() + 5
while time.monotonic() < deadline:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        print("process-group-clean")
        break
    stat_path = pathlib.Path(f"/proc/{{pid}}/stat")
    try:
        state = (
            stat_path.read_text(encoding="ascii").split()[2]
            if stat_path.is_file()
            else None
        )
    except (FileNotFoundError, ProcessLookupError):
        print("process-group-clean")
        break
    if state == "Z":
        print("process-group-clean")
        break
    time.sleep(0.05)
else:
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    raise SystemExit("descendant survived command timeout")
"""

    completed = subprocess.run(
        (sys.executable, "-c", driver),
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert completed.stdout == "process-group-clean\n"
    assert completed.stderr == ""


@pytest.mark.skipif(os.name != "posix", reason="pass_fds requires POSIX")
def test_inherited_fd_exec_shim_stdin_maps_input_and_preserves_runtime_lock(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    payload = b"canonical-inventory-payload\n"
    input_fd, writer_fd = os.pipe()
    runtime_lock_fd = os.open(tmp_path / "runtime.lock", os.O_RDWR | os.O_CREAT, 0o600)
    try:
        os.write(writer_fd, payload)
        os.close(writer_fd)
        writer_fd = -1
        child = (
            "import hashlib,os,sys;"
            "data=sys.stdin.buffer.read();"
            "os.fstat(int(sys.argv[1]));"
            "print(hashlib.sha256(data).hexdigest())"
        )
        argv = (
            sys.executable,
            "-c",
            module._INHERITED_FD_EXEC_SHIM,
            "stdin",
            str(input_fd),
            sys.executable,
            sys.executable,
            "-c",
            child,
            str(runtime_lock_fd),
        )

        completed = subprocess.run(
            argv,
            check=False,
            capture_output=True,
            text=True,
            close_fds=True,
            pass_fds=(input_fd, runtime_lock_fd),
        )

        assert completed.returncode == 0, completed.stderr
        assert completed.stdout == hashlib.sha256(payload).hexdigest() + "\n"
        assert completed.stderr == ""
    finally:
        for descriptor in (input_fd, writer_fd, runtime_lock_fd):
            if descriptor >= 0:
                try:
                    os.close(descriptor)
                except OSError:
                    pass


def _task8_remote_planner_fixture(
    operation_dir: Path,
    *,
    kind: str = "dry_run",
) -> tuple[dict[str, object], list[str], datetime, str, dict[str, list[str]]]:
    phase = "dry_run_recording" if kind == "dry_run" else "policy_enabling"
    state = state_at_phase(operation_dir, phase)
    history = state["phase_history"]
    effective = state["effective_config"]
    assert isinstance(history, list) and isinstance(effective, dict)
    entry = history[-1]
    assert entry["phase"] == phase
    now = datetime.fromtimestamp(int(entry["epoch"]), tz=timezone.utc)
    inventory = list(task8_valid_dry_run_receipt()["inventory_names"])
    planner = task8_load_retention_planner()
    plan = planner.plan_inventory(
        inventory,
        mode="remote",
        prefix=str(effective["BACKUP_PREFIX"]),
        now=now,
        daily=int(str(effective["KEEP_REMOTE_DAILY"])),
        weekly=int(str(effective["KEEP_REMOTE_WEEKLY"])),
        monthly=int(str(effective["KEEP_REMOTE_MONTHLY"])),
    )
    raw = json.dumps(plan, sort_keys=True, separators=(",", ":")) + "\n"
    return state, inventory, now, raw, task8_flatten_retention_plan(plan)


def test_task8_remote_planner_transports_bounded_canonical_inventory_only_via_stdin_fd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state, inventory, _now, planner_output, expected = _task8_remote_planner_fixture(
        operation_dir
    )
    payload = ("\n".join(inventory) + "\n").encode("ascii")
    runtime_lock_path = tmp_path / "runtime.lock"
    runtime_lock_fd = os.open(runtime_lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    before_files = {
        path.relative_to(tmp_path): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }
    observed_payloads: list[bytes] = []

    def reject_tempfile(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("inventory transport must not create a temporary file")

    for name in (
        "NamedTemporaryFile",
        "TemporaryFile",
        "SpooledTemporaryFile",
        "mkstemp",
    ):
        monkeypatch.setattr(tempfile, name, reject_tempfile)

    def runner(
        command: object,
        pass_fds: tuple[int, ...],
    ) -> subprocess.CompletedProcess[str]:
        argv = tuple(str(value) for value in command)
        assert len(pass_fds) == 2
        assert int(argv[4]) == pass_fds[0]
        assert pass_fds[1] == runtime_lock_fd
        os.fstat(runtime_lock_fd)
        chunks: list[bytes] = []
        while True:
            chunk = os.read(pass_fds[0], 64 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        observed = b"".join(chunks)
        observed_payloads.append(observed)
        argv_text = "\x00".join(argv)
        assert payload.decode("ascii") not in argv_text
        assert all(name not in argv_text for name in inventory)
        assert all(
            payload.decode("ascii") not in value and all(name not in value for name in inventory)
            for value in os.environ.values()
        )
        return subprocess.CompletedProcess(argv, 0, planner_output, "")

    try:
        context = types.SimpleNamespace(command_runner=runner)
        result = module._task8_run_remote_retention_planner(
            context,
            state,
            inventory,
            runtime_lock_fd=runtime_lock_fd,
        )
    finally:
        os.close(runtime_lock_fd)

    after_files = {
        path.relative_to(tmp_path): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }
    assert observed_payloads == [payload]
    assert len(payload) <= module._MAX_STATE_BYTES
    assert result == expected
    assert after_files == before_files


def test_task8_remote_planner_uses_exact_installed_options_and_entry_epoch(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state, inventory, now, planner_output, _expected = _task8_remote_planner_fixture(
        operation_dir
    )
    runtime_lock_fd = os.open(tmp_path / "runtime.lock", os.O_RDWR | os.O_CREAT, 0o600)
    observed: list[tuple[tuple[str, ...], tuple[int, ...]]] = []

    def runner(
        command: object,
        pass_fds: tuple[int, ...],
    ) -> subprocess.CompletedProcess[str]:
        argv = tuple(str(value) for value in command)
        observed.append((argv, pass_fds))
        while os.read(pass_fds[0], 64 * 1024):
            pass
        return subprocess.CompletedProcess(argv, 0, planner_output, "")

    try:
        context = types.SimpleNamespace(command_runner=runner)
        module._task8_run_remote_retention_planner(
            context,
            state,
            inventory,
            runtime_lock_fd=runtime_lock_fd,
        )
    finally:
        os.close(runtime_lock_fd)

    assert len(observed) == 1
    argv, pass_fds = observed[0]
    assert argv == (
        sys.executable,
        "-c",
        module._INHERITED_FD_EXEC_SHIM,
        "stdin",
        str(pass_fds[0]),
        "/usr/local/sbin/degen-prod-db-retention",
        "degen-prod-db-retention",
        "--mode",
        "remote",
        "--prefix",
        "degen_green_",
        "--now",
        now.strftime("%Y%m%dT%H%M%SZ"),
        "--daily",
        "7",
        "--weekly",
        "4",
        "--monthly",
        "3",
        "--format",
        "json",
    )
    assert len(pass_fds) == 2
    assert pass_fds[1] == runtime_lock_fd


def test_task8_remote_planner_uses_policy_entry_epoch_for_enablement_review(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state, inventory, now, planner_output, expected = _task8_remote_planner_fixture(
        operation_dir,
        kind="policy",
    )
    runtime_lock_fd = os.open(tmp_path / "runtime.lock", os.O_RDWR | os.O_CREAT, 0o600)
    observed: list[tuple[str, ...]] = []

    def runner(
        command: object,
        pass_fds: tuple[int, ...],
    ) -> subprocess.CompletedProcess[str]:
        argv = tuple(str(value) for value in command)
        observed.append(argv)
        while os.read(pass_fds[0], 64 * 1024):
            pass
        return subprocess.CompletedProcess(argv, 0, planner_output, "")

    try:
        result = module._task8_run_remote_retention_planner(
            types.SimpleNamespace(command_runner=runner),
            state,
            inventory,
            runtime_lock_fd=runtime_lock_fd,
        )
    finally:
        os.close(runtime_lock_fd)

    assert result == expected
    assert len(observed) == 1
    argv = observed[0]
    assert argv[argv.index("--now") + 1] == now.strftime("%Y%m%dT%H%M%SZ")


def test_task8_remote_planner_rejects_inventory_beyond_entry_bound_before_command(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "dry_run_recording")
    inventory = [f"unknown-{index:04d}.txt" for index in range(4097)]
    runtime_lock_fd = os.open(tmp_path / "runtime.lock", os.O_RDWR | os.O_CREAT, 0o600)
    called = False

    def runner(
        command: object,
        pass_fds: tuple[int, ...],
    ) -> subprocess.CompletedProcess[str]:
        nonlocal called
        called = True
        return subprocess.CompletedProcess(tuple(command), 0, "", "")

    try:
        context = types.SimpleNamespace(command_runner=runner)
        with pytest.raises(module.OperationStateError, match="entry bound|entry limit"):
            module._task8_run_remote_retention_planner(
                context,
                state,
                inventory,
                runtime_lock_fd=runtime_lock_fd,
            )
    finally:
        os.close(runtime_lock_fd)

    assert not called


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
    monkeypatch.setenv("DATABASE_URL", "postgresql://ambient:SECRET@db/degen")
    monkeypatch.setenv("PGPASSWORD", "AMBIENT_PGPASSWORD_SENTINEL")
    monkeypatch.setenv("PGDATABASE", "postgresql://ambient:OTHER@db/degen")
    script = (
        "import json,os;"
        "print(json.dumps({key:os.environ.get(key) for key in "
        "('LANG','LC_ALL','PATH','DATABASE_URL','PGPASSWORD','PGDATABASE')},"
        "sort_keys=True,separators=(',',':')))"
    )

    completed = module._default_command_runner(
        (sys.executable, "-c", script),
        (),
    )

    child_env = json.loads(completed.stdout)
    assert child_env == {
        "LANG": "C",
        "LC_ALL": "C",
        "PATH": "/usr/sbin:/usr/bin:/sbin:/bin",
        "DATABASE_URL": None,
        "PGPASSWORD": None,
        "PGDATABASE": None,
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


def host_snapshot_fixture(
    module: object,
    tmp_path: Path,
    *,
    absent_targets: tuple[str, ...] = (),
) -> tuple[object, dict[str, object]]:
    context, staging = host_staging_fixture(module, tmp_path)
    module.prepare_host_staging(context)
    source_runner = context.command_runner
    target_bytes: dict[str, bytes] = {}
    target_modes = {
        TARGETS[0]: 0o755,
        TARGETS[1]: 0o750,
        TARGETS[2]: 0o755,
        TARGETS[3]: 0o755,
        TARGETS[4]: 0o644,
        TARGETS[5]: 0o640,
        TARGETS[6]: 0o600,
    }
    for index, target in enumerate(TARGETS):
        path = host_root_path(context.host_root, target)
        if path.exists():
            path.unlink()
        if target in absent_targets:
            path.parent.mkdir(parents=True, exist_ok=True)
            continue
        contents = f"prior host bytes {index} for {target}\n".encode("ascii")
        write_private_host_file(path, contents, target_modes[target])
        target_bytes[target] = contents

    runtime = {
        "timer_enabled": "enabled",
        "timer_active": "active",
        "timer_substate": "waiting",
        "timer_trigger": "Wed 2026-07-01 00:00:00 UTC",
        "timer_trigger_epoch": 1_782_883_200,
        "postgres_units": ["postgresql@15-main.service"],
        "service_pids": {
            "postgresql@15-main.service": 210,
            "degen-web.service": 310,
            "degen-worker.service": 320,
        },
        "bot_users": [("1001", "degen")],
        "bot_pids": {"degen": 410},
        "service_states": {},
    }
    calls: list[tuple[str, ...]] = []

    def completed(
        argv: tuple[str, ...],
        stdout: str,
        *,
        returncode: int = 0,
        stderr: str = "",
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(argv, returncode, stdout, stderr)

    def command_runner(
        argv: object,
        pass_fds: tuple[int, ...],
    ) -> subprocess.CompletedProcess[str]:
        argv_tuple = tuple(str(value) for value in argv)
        if "get-tar-commit-id" in argv_tuple:
            return source_runner(argv_tuple, pass_fds)
        calls.append(argv_tuple)
        assert pass_fds == ()
        assert not any(
            token in argv_tuple
            for token in (
                "start",
                "stop",
                "restart",
                "enable",
                "disable",
                "daemon-reload",
            )
        )
        assert not any("rclone" in token.lower() for token in argv_tuple)
        executable = Path(argv_tuple[0]).name if argv_tuple else ""
        if executable == "systemctl" and "list-units" in argv_tuple:
            stdout = "".join(
                f"{unit} loaded active running PostgreSQL service\n"
                for unit in runtime["postgres_units"]
            )
            return completed(argv_tuple, stdout)
        if executable == "systemctl" and "show" in argv_tuple:
            unit = argv_tuple[argv_tuple.index("show") + 1]
            if unit == "degen-prod-db-backup.timer":
                return completed(
                    argv_tuple,
                    "UnitFileState="
                    + str(runtime["timer_enabled"])
                    + "\nActiveState="
                    + str(runtime["timer_active"])
                    + "\nSubState="
                    + str(runtime["timer_substate"])
                    + "\nLastTriggerUSec="
                    + str(runtime["timer_trigger"])
                    + "\n",
                )
            if unit == "degen-ops-discord-bot.service":
                machine = next(
                    token.split("=", 1)[1]
                    for token in argv_tuple
                    if token.startswith("--machine=")
                )
                username = machine.split("@", 1)[0]
                pid = runtime["bot_pids"].get(username)
                if pid is None:
                    return completed(
                        argv_tuple,
                        "LoadState=not-found\nActiveState=inactive\n"
                        "SubState=dead\nMainPID=0\n",
                    )
                return completed(
                    argv_tuple,
                    "LoadState=loaded\nActiveState=active\n"
                    f"SubState=running\nMainPID={pid}\n",
                )
            override = runtime["service_states"].get(unit)
            if override is not None:
                load_state, active, substate, pid = override
                return completed(
                    argv_tuple,
                    f"LoadState={load_state}\nActiveState={active}\n"
                    f"SubState={substate}\nMainPID={pid}\n",
                )
            pid = runtime["service_pids"].get(unit, 0)
            active = "active" if pid else "inactive"
            substate = "running" if pid else "dead"
            return completed(
                argv_tuple,
                f"LoadState=loaded\nActiveState={active}\n"
                f"SubState={substate}\nMainPID={pid}\n",
            )
        if executable == "loginctl" and "list-users" in argv_tuple:
            return completed(
                argv_tuple,
                "".join(
                    f"{uid} {username}\n"
                    for uid, username in runtime["bot_users"]
                ),
            )
        if executable == "date":
            assert str(runtime["timer_trigger"]) in argv_tuple
            return completed(argv_tuple, f"{runtime['timer_trigger_epoch']}\n")
        raise AssertionError(f"unexpected snapshot command: {argv_tuple!r}")

    return dataclasses.replace(context, command_runner=command_runner), {
        "calls": calls,
        "rclone_bytes": staging["rclone_path"].read_bytes(),
        "rclone_path": staging["rclone_path"],
        "runtime": runtime,
        "target_bytes": target_bytes,
        "target_modes": target_modes,
    }


def snapshot_artifact_name(target: str) -> str:
    return PurePosixPath(target).name


def expected_prior_runtime() -> dict[str, object]:
    return {
        "timer_enabled": True,
        "timer_active": True,
        "preinstall_trigger_epoch": 1_782_883_200,
        "pids": {
            "postgresql:postgresql@15-main.service": 210,
            "system:degen-web.service": 310,
            "system:degen-worker.service": 320,
            "user:1001:degen:degen-ops-discord-bot.service": 410,
        },
    }


def test_snapshot_host_state_saves_exact_targets_manifest_and_receipt(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    context, fixture = host_snapshot_fixture(module, tmp_path)

    result = module.snapshot_host_state(context)

    assert set(result) == {"snapshot", "prior_runtime"}
    assert result["prior_runtime"] == expected_prior_runtime()
    snapshot = result["snapshot"]
    assert set(snapshot) == {"manifest_sha256", "targets", "rclone_audit"}
    assert tuple(snapshot["targets"]) == TARGETS
    assert snapshot["rclone_audit"]["path"] == "/etc/degen/rclone.conf"
    assert "/etc/degen/rclone.conf" not in snapshot["targets"]

    expected_artifacts: dict[str, bytes] = {}
    for target in TARGETS:
        name = snapshot_artifact_name(target)
        expected_artifacts[name] = fixture["target_bytes"][target]
        source = host_root_path(context.host_root, target)
        metadata = source.stat()
        assert snapshot["targets"][target] == {
            "present": True,
            "sha256": hashlib.sha256(fixture["target_bytes"][target]).hexdigest(),
            "mode": stat.S_IMODE(metadata.st_mode) if os.name == "posix" else 0o600,
            "uid": metadata.st_uid,
            "gid": metadata.st_gid,
        }
    expected_artifacts["rclone.conf.audit"] = fixture["rclone_bytes"]
    for name, contents in expected_artifacts.items():
        path = context.paths.snapshot_dir / name
        assert path.read_bytes() == contents
        if os.name == "posix":
            assert stat.S_IMODE(path.stat().st_mode) == 0o600

    manifest = context.paths.snapshot_dir / "SHA256SUMS"
    expected_manifest = b"".join(
        hashlib.sha256(expected_artifacts[name]).hexdigest().encode("ascii")
        + b"  "
        + name.encode("ascii")
        + b"\n"
        for name in sorted(expected_artifacts)
    )
    assert manifest.read_bytes() == expected_manifest
    assert snapshot["manifest_sha256"] == hashlib.sha256(expected_manifest).hexdigest()
    if os.name == "posix":
        assert stat.S_IMODE(context.paths.snapshot_dir.stat().st_mode) == 0o700
        assert stat.S_IMODE(manifest.stat().st_mode) == 0o600

    state = module.load_operation_state(
        context.paths.state_file, effective_uid=context.effective_uid
    )
    module.validate_operation_state_for_context(state, context)
    assert state["phase"] == "snapshotted"
    assert state["snapshot"] == snapshot
    assert state["prior_runtime"] == expected_prior_runtime()
    assert [entry["phase"] for entry in state["phase_history"]] == [
        "source_verified",
        "staging_prepared",
        "snapshotted",
    ]


def test_absence_marker_is_mutually_exclusive_and_manifested(tmp_path: Path) -> None:
    module = load_ops_helper()
    absent = (TARGETS[1], TARGETS[3])
    context, _fixture = host_snapshot_fixture(
        module,
        tmp_path,
        absent_targets=absent,
    )

    result = module.snapshot_host_state(context)

    snapshot = result["snapshot"]
    manifest_names = {
        line.split(b"  ", 1)[1].decode("ascii")
        for line in (context.paths.snapshot_dir / "SHA256SUMS").read_bytes().splitlines()
    }
    for target in absent:
        name = snapshot_artifact_name(target)
        marker = context.paths.snapshot_dir / f"{name}.absent"
        assert not (context.paths.snapshot_dir / name).exists()
        assert marker.read_bytes() == f"ABSENT {target}\n".encode("ascii")
        assert f"{name}.absent" in manifest_names
        assert snapshot["targets"][target] == {
            "present": False,
            "sha256": None,
            "mode": None,
            "uid": None,
            "gid": None,
        }


@pytest.mark.parametrize("invalid_kind", ["symlink", "directory", "hardlink"])
def test_snapshot_rejects_link_nonregular_or_hardlinked_target(
    tmp_path: Path,
    invalid_kind: str,
) -> None:
    module = load_ops_helper()
    context, _fixture = host_snapshot_fixture(module, tmp_path)
    target = host_root_path(context.host_root, TARGETS[0])
    prior = target.with_name(target.name + ".prior")
    target.rename(prior)
    if invalid_kind == "symlink":
        try:
            target.symlink_to(prior.name)
        except OSError:
            pytest.skip("symlink creation is unavailable")
    elif invalid_kind == "directory":
        target.mkdir()
    else:
        os.link(prior, target)
    before = context.paths.state_file.read_bytes()

    with pytest.raises(module.OperationStateError):
        module.snapshot_host_state(context)

    assert context.paths.state_file.read_bytes() == before
    assert not context.paths.snapshot_dir.exists()


def test_snapshot_unstable_source_fails_and_closes_held_descriptors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, _fixture = host_snapshot_fixture(module, tmp_path)
    target = host_root_path(context.host_root, TARGETS[0])
    opened: list[int] = []

    def mutate(event: str, **details: object) -> None:
        if event == "snapshot_target_opened" and details.get("logical_path") == TARGETS[0]:
            opened.append(int(details["descriptor"]))
            target.write_bytes(target.read_bytes() + b"changed while held\n")

    monkeypatch.setattr(module, "_atomic_event_hook", mutate)

    with pytest.raises(module.OperationStateError):
        module.snapshot_host_state(context)

    assert opened
    for descriptor in opened:
        with pytest.raises(OSError):
            os.fstat(descriptor)
    assert not context.paths.snapshot_dir.exists()


@pytest.mark.parametrize("changed", ["present", "absent"])
def test_snapshot_pre_replace_revalidates_present_and_absent_sources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    changed: str,
) -> None:
    module = load_ops_helper()
    absent_targets = (TARGETS[1],) if changed == "absent" else ()
    context, _fixture = host_snapshot_fixture(
        module,
        tmp_path,
        absent_targets=absent_targets,
    )
    before = context.paths.state_file.read_bytes()

    def replace(event: str, **_details: object) -> None:
        if event != "before_replace":
            return
        target_name = TARGETS[1] if changed == "absent" else TARGETS[0]
        target = host_root_path(context.host_root, target_name)
        if changed == "absent":
            write_private_host_file(target, b"appeared after absence proof\n", 0o600)
        else:
            replacement = target.with_name(target.name + ".replacement")
            write_private_host_file(
                replacement,
                target.read_bytes(),
                stat.S_IMODE(target.stat().st_mode),
            )
            os.replace(replacement, target)

    monkeypatch.setattr(module, "_atomic_event_hook", replace)

    with pytest.raises(module.OperationStateError):
        module.snapshot_host_state(context)

    assert context.paths.state_file.read_bytes() == before


@pytest.mark.parametrize("artifact", ["saved", "absence", "audit", "manifest"])
def test_snapshot_manifest_is_reverified_through_state_cas(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    artifact: str,
) -> None:
    module = load_ops_helper()
    context, _fixture = host_snapshot_fixture(
        module,
        tmp_path,
        absent_targets=(TARGETS[1],),
    )
    before = context.paths.state_file.read_bytes()

    def tamper(event: str, **_details: object) -> None:
        if event != "before_replace":
            return
        paths = {
            "saved": context.paths.snapshot_dir / snapshot_artifact_name(TARGETS[0]),
            "absence": context.paths.snapshot_dir
            / f"{snapshot_artifact_name(TARGETS[1])}.absent",
            "audit": context.paths.snapshot_dir / "rclone.conf.audit",
            "manifest": context.paths.snapshot_dir / "SHA256SUMS",
        }
        path = paths[artifact]
        path.write_bytes(path.read_bytes() + b"tampered\n")
        path.chmod(0o600)

    monkeypatch.setattr(module, "_atomic_event_hook", tamper)

    with pytest.raises(module.OperationStateError):
        module.snapshot_host_state(context)

    assert context.paths.state_file.read_bytes() == before


def test_snapshot_revalidates_source_stage_and_state_before_host_capture(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    context, fixture = host_snapshot_fixture(module, tmp_path)
    manifest = context.paths.staged_dir / "host-stage-manifest.json"
    manifest.write_bytes(manifest.read_bytes() + b"tampered\n")
    before = context.paths.state_file.read_bytes()

    with pytest.raises(module.OperationStateError):
        module.snapshot_host_state(context)

    assert fixture["calls"] == []
    assert context.paths.state_file.read_bytes() == before
    assert not context.paths.snapshot_dir.exists()


def test_rclone_audit_is_root_only_secret_free_json_and_never_a_rollback_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, fixture = host_snapshot_fixture(module, tmp_path)
    audit_seen_at_cas = False
    write_order: list[str] = []

    def inspect(event: str, **_details: object) -> None:
        nonlocal audit_seen_at_cas
        if event == "snapshot_artifact_written":
            write_order.append(str(_details["name"]))
        if event == "before_replace":
            audit = context.paths.snapshot_dir / "rclone.conf.audit"
            assert audit.read_bytes() == fixture["rclone_bytes"]
            audit_seen_at_cas = True

    monkeypatch.setattr(module, "_atomic_event_hook", inspect)

    result = module.snapshot_host_state(context)

    assert audit_seen_at_cas
    assert write_order[0] == "rclone.conf.audit"
    assert write_order[-1] == "SHA256SUMS"
    observable_json = json.dumps(result, sort_keys=True).encode("utf-8")
    assert b"RCLONE_CONTENT_SENTINEL" not in observable_json
    assert fixture["rclone_bytes"] not in observable_json
    assert "/etc/degen/rclone.conf" not in result["snapshot"]["targets"]
    assert all(not any("rclone" in token.lower() for token in call) for call in fixture["calls"])
    audit = context.paths.snapshot_dir / "rclone.conf.audit"
    if os.name == "posix":
        assert stat.S_IMODE(audit.stat().st_mode) == 0o600


def test_prior_runtime_is_exact_and_recaptured_during_state_cas(tmp_path: Path) -> None:
    module = load_ops_helper()
    context, fixture = host_snapshot_fixture(module, tmp_path)

    result = module.snapshot_host_state(context)

    assert result["prior_runtime"] == expected_prior_runtime()
    calls = fixture["calls"]
    assert sum("list-units" in call for call in calls) >= 2
    assert sum(
        "show" in call and "degen-prod-db-backup.timer" in call for call in calls
    ) >= 2
    assert all(call and call[0].startswith("/") for call in calls)


@pytest.mark.parametrize(
    "ambiguous",
    ["postgres", "bot-owner", "web-pid", "timer-enabled", "timer-active"],
)
def test_prior_runtime_fails_closed_on_ambiguous_unit_owner_or_state(
    tmp_path: Path,
    ambiguous: str,
) -> None:
    module = load_ops_helper()
    context, fixture = host_snapshot_fixture(module, tmp_path)
    runtime = fixture["runtime"]
    if ambiguous == "postgres":
        runtime["postgres_units"].append("postgresql@16-main.service")
    elif ambiguous == "bot-owner":
        runtime["bot_users"].append(("1002", "other"))
        runtime["bot_pids"]["other"] = 411
    elif ambiguous == "web-pid":
        runtime["service_pids"]["degen-web.service"] = 0
    elif ambiguous == "timer-enabled":
        runtime["timer_enabled"] = "indirect"
    else:
        runtime["timer_active"] = "activating"
    before = context.paths.state_file.read_bytes()

    with pytest.raises(module.OperationStateError):
        module.snapshot_host_state(context)

    assert context.paths.state_file.read_bytes() == before


def test_prior_runtime_requires_backup_service_inactive_dead_with_zero_pid(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    context, fixture = host_snapshot_fixture(module, tmp_path)
    fixture["runtime"]["service_pids"]["degen-prod-db-backup.service"] = 999
    before = context.paths.state_file.read_bytes()

    with pytest.raises(module.OperationStateError, match="backup|service|inactive"):
        module.snapshot_host_state(context)

    assert context.paths.state_file.read_bytes() == before


@pytest.mark.parametrize(
    "bad_keys",
    [
        {"postgresql:postgresql@15-main.service": 210},
        {
            **expected_prior_runtime()["pids"],
            "system:unexpected.service": 999,
        },
        {
            **expected_prior_runtime()["pids"],
            "postgresql:postgresql@16-main.service": 211,
        },
        {
            key.replace("user:1001:degen:", "user:bad:degen:"): value
            for key, value in expected_prior_runtime()["pids"].items()
        },
    ],
)
def test_prior_runtime_pid_identity_keys_are_exact_and_unambiguous(
    tmp_path: Path,
    bad_keys: dict[str, int],
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "snapshotted")
    state["prior_runtime"]["pids"] = bad_keys

    with pytest.raises(module.OperationStateError, match="prior_runtime|pids|identity"):
        module.validate_operation_state(state, operation_dir)


def test_prior_runtime_rejects_duplicate_pid_values_and_wrong_audit_path(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "snapshotted")
    state["prior_runtime"]["pids"] = expected_prior_runtime()["pids"]
    state["prior_runtime"]["pids"]["system:degen-worker.service"] = 310
    with pytest.raises(module.OperationStateError, match="prior_runtime|pids|unique"):
        module.validate_operation_state(state, operation_dir)

    state = state_at_phase(operation_dir, "snapshotted")
    state["prior_runtime"]["pids"] = expected_prior_runtime()["pids"]
    state["snapshot"]["rclone_audit"]["path"] = "/tmp/wrong-rclone.conf"
    with pytest.raises(module.OperationStateError, match="rclone|path|effective_config"):
        module.validate_operation_state(state, operation_dir)


@pytest.mark.parametrize("changed", ["pid", "timer", "postgres-unit"])
def test_prior_runtime_change_during_state_cas_is_rejected(
    tmp_path: Path,
    changed: str,
) -> None:
    module = load_ops_helper()
    context, fixture = host_snapshot_fixture(module, tmp_path)
    original_runner = context.command_runner
    list_calls = 0
    timer_calls = 0
    web_calls = 0

    def changing_runner(argv: object, pass_fds: tuple[int, ...]):
        nonlocal list_calls, timer_calls, web_calls
        argv_tuple = tuple(str(value) for value in argv)
        completed = original_runner(argv_tuple, pass_fds)
        if "list-units" in argv_tuple:
            list_calls += 1
            if changed == "postgres-unit" and list_calls == 2:
                completed.stdout = (
                    "postgresql@16-main.service loaded active running PostgreSQL service\n"
                )
        if "show" in argv_tuple and "degen-prod-db-backup.timer" in argv_tuple:
            timer_calls += 1
            if changed == "timer" and timer_calls == 2:
                completed.stdout = completed.stdout.replace(
                    str(fixture["runtime"]["timer_trigger"]),
                    "Wed 2026-07-01 00:00:01 UTC",
                )
        if "show" in argv_tuple and "degen-web.service" in argv_tuple:
            web_calls += 1
            if changed == "pid" and web_calls == 2:
                completed.stdout = completed.stdout.replace("MainPID=310", "MainPID=311")
        return completed

    before = context.paths.state_file.read_bytes()
    with pytest.raises(
        module.OperationStateError,
        match="runtime|changed|identity|timer|trigger|PostgreSQL|service",
    ):
        module.snapshot_host_state(dataclasses.replace(context, command_runner=changing_runner))

    assert context.paths.state_file.read_bytes() == before


def test_rclone_audit_source_replacement_during_state_cas_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, fixture = host_snapshot_fixture(module, tmp_path)
    before = context.paths.state_file.read_bytes()

    def replace(event: str, **_details: object) -> None:
        if event != "before_replace":
            return
        source = fixture["rclone_path"]
        replacement = source.with_name(source.name + ".replacement")
        write_private_host_file(replacement, fixture["rclone_bytes"], 0o600)
        os.replace(replacement, source)

    monkeypatch.setattr(module, "_atomic_event_hook", replace)

    with pytest.raises(module.OperationStateError):
        module.snapshot_host_state(context)

    assert context.paths.state_file.read_bytes() == before


def test_snapshot_failure_after_artifact_creation_preserves_state_and_residue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, _fixture = host_snapshot_fixture(module, tmp_path)
    before = context.paths.state_file.read_bytes()

    def interrupt(event: str, **_details: object) -> None:
        if event == "before_replace":
            raise module.OperationStateError("injected snapshot CAS interruption")

    monkeypatch.setattr(module, "_atomic_event_hook", interrupt)

    with pytest.raises(module.OperationStateError, match="interruption"):
        module.snapshot_host_state(context)

    assert context.paths.state_file.read_bytes() == before
    residue = {
        path.name: path.read_bytes()
        for path in context.paths.snapshot_dir.iterdir()
        if path.is_file()
    }
    assert "rclone.conf.audit" in residue
    assert "SHA256SUMS" in residue
    monkeypatch.setattr(module, "_atomic_event_hook", lambda *_args, **_kwargs: None)
    with pytest.raises(module.OperationStateError, match="snapshot"):
        module.snapshot_host_state(context)
    assert {
        path.name: path.read_bytes()
        for path in context.paths.snapshot_dir.iterdir()
        if path.is_file()
    } == residue


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX mode semantics")
@pytest.mark.parametrize("unsafe_mode", [0o777, 0o6755, 0o1755])
def test_snapshot_rejects_unsafe_present_target_mode(
    tmp_path: Path,
    unsafe_mode: int,
) -> None:
    module = load_ops_helper()
    context, _fixture = host_snapshot_fixture(module, tmp_path)
    target = host_root_path(context.host_root, TARGETS[0])
    target.chmod(unsafe_mode)
    before = context.paths.state_file.read_bytes()

    with pytest.raises(module.OperationStateError, match="mode|target|unsafe"):
        module.snapshot_host_state(context)

    assert context.paths.state_file.read_bytes() == before
    assert not context.paths.snapshot_dir.exists()


def test_snapshot_already_snapshotted_is_verified_read_only_noop(tmp_path: Path) -> None:
    module = load_ops_helper()
    context, fixture = host_snapshot_fixture(module, tmp_path)
    first = module.snapshot_host_state(context)
    state_before = context.paths.state_file.read_bytes()
    artifacts_before = {
        path.name: (path.read_bytes(), path.stat().st_ino, path.stat().st_mtime_ns)
        for path in context.paths.snapshot_dir.iterdir()
        if path.is_file()
    }
    calls_before = len(fixture["calls"])
    directory_before = context.paths.snapshot_dir.stat()

    second = module.snapshot_host_state(context)

    assert second == first
    assert context.paths.state_file.read_bytes() == state_before
    assert {
        path.name: (path.read_bytes(), path.stat().st_ino, path.stat().st_mtime_ns)
        for path in context.paths.snapshot_dir.iterdir()
        if path.is_file()
    } == artifacts_before
    assert len(fixture["calls"]) > calls_before
    directory_after = context.paths.snapshot_dir.stat()
    assert (
        directory_after.st_ino,
        directory_after.st_mode,
        directory_after.st_mtime_ns,
        directory_after.st_ctime_ns,
    ) == (
        directory_before.st_ino,
        directory_before.st_mode,
        directory_before.st_mtime_ns,
        directory_before.st_ctime_ns,
    )


def test_snapshot_already_snapshotted_opens_artifacts_read_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, _fixture = host_snapshot_fixture(module, tmp_path)
    module.snapshot_host_state(context)
    artifact_names = {
        path.name for path in context.paths.snapshot_dir.iterdir() if path.is_file()
    }
    observed_flags: list[int] = []
    original_open = module.os.open

    def tracked_open(path: object, flags: int, *args: object, **kwargs: object) -> int:
        opened_path = Path(path)
        if opened_path.name in artifact_names and (
            not opened_path.is_absolute()
            or opened_path.parent == context.paths.snapshot_dir
        ):
            observed_flags.append(flags)
        return original_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(module.os, "open", tracked_open)

    module.snapshot_host_state(context)

    assert len(observed_flags) >= len(artifact_names)
    assert all(
        flags & (os.O_WRONLY | os.O_RDWR) == 0 for flags in observed_flags
    ), observed_flags


@pytest.mark.parametrize(
    ("receipt", "field"),
    [("target", "uid"), ("target", "gid"), ("audit", "size"), ("runtime", "trigger")],
)
def test_snapshot_state_rejects_negative_metadata(
    tmp_path: Path,
    receipt: str,
    field: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "snapshotted")
    if receipt == "target":
        state["snapshot"]["targets"][TARGETS[0]][field] = -1
    elif receipt == "audit":
        state["snapshot"]["rclone_audit"][field] = -1
    else:
        state["prior_runtime"]["preinstall_trigger_epoch"] = -1

    with pytest.raises(module.OperationStateError):
        module.validate_operation_state(state, operation_dir)


def test_prior_runtime_requires_exact_timer_substate(tmp_path: Path) -> None:
    module = load_ops_helper()
    context, fixture = host_snapshot_fixture(module, tmp_path)
    result = module.snapshot_host_state(context)
    assert result["prior_runtime"]["timer_active"] is True

    invalid_root = tmp_path / "invalid"
    invalid_root.mkdir()
    context, fixture = host_snapshot_fixture(module, invalid_root)
    fixture["runtime"]["timer_substate"] = "running"

    with pytest.raises(module.OperationStateError, match="timer|state|substate"):
        module.snapshot_host_state(context)


def test_prior_runtime_maps_empty_last_trigger_to_null(tmp_path: Path) -> None:
    module = load_ops_helper()
    context, fixture = host_snapshot_fixture(module, tmp_path)
    fixture["runtime"]["timer_trigger"] = ""

    result = module.snapshot_host_state(context)

    assert result["prior_runtime"]["preinstall_trigger_epoch"] is None
    assert not any(Path(call[0]).name == "date" for call in fixture["calls"])


def test_prior_runtime_allows_canonical_root_bot_owner(tmp_path: Path) -> None:
    module = load_ops_helper()
    context, fixture = host_snapshot_fixture(module, tmp_path)
    fixture["runtime"]["bot_users"] = [("0", "root")]
    fixture["runtime"]["bot_pids"] = {"root": 410}

    result = module.snapshot_host_state(context)

    assert result["prior_runtime"]["pids"] == {
        "postgresql:postgresql@15-main.service": 210,
        "system:degen-web.service": 310,
        "system:degen-worker.service": 320,
        "user:0:root:degen-ops-discord-bot.service": 410,
    }


def test_snapshot_rejects_preexisting_snapshot_path(tmp_path: Path) -> None:
    module = load_ops_helper()
    context, _fixture = host_snapshot_fixture(module, tmp_path)
    context.paths.snapshot_dir.mkdir(mode=0o700)

    with pytest.raises(module.OperationStateError, match="snapshot"):
        module.snapshot_host_state(context)


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX symlink semantics")
def test_snapshot_private_host_root_cannot_escape_through_symlink(tmp_path: Path) -> None:
    module = load_ops_helper()
    context, _fixture = host_snapshot_fixture(module, tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    escaped = tmp_path / "escaped-host"
    escaped.symlink_to(outside, target_is_directory=True)
    escaped_context = dataclasses.replace(context, host_root=escaped)

    with pytest.raises(module.OperationStateError, match="host_root|symlink|binding"):
        module.snapshot_host_state(escaped_context)


def test_snapshot_cli_has_only_operation_dir_and_reconstructs_sealed_context(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = load_ops_helper()
    operation_dir_raw = "/opt/degen/backups/config/20260701T123456Z"
    operation_dir = Path(operation_dir_raw)
    state = state_at_phase(operation_dir, "staging_prepared")
    observed: list[object] = []
    monkeypatch.setattr(module, "_effective_uid", lambda: 0)
    monkeypatch.setattr(module, "_require_posix_descriptor_primitives", lambda: None)
    monkeypatch.setattr(module, "load_operation_state", lambda path, *, effective_uid: state)
    if os.name != "posix":
        monkeypatch.setattr(module, "validate_operation_state_for_context", lambda *_: None)
    monkeypatch.setattr(
        module,
        "snapshot_host_state",
        lambda context: observed.append(context)
        or {"snapshot": state["snapshot"], "prior_runtime": state["prior_runtime"]},
        raising=False,
    )

    help_result = subprocess.run(
        [sys.executable, str(OPS_HELPER), "snapshot", "--help"],
        text=True,
        capture_output=True,
        check=False,
    )
    result = module.main(["snapshot", "--operation-dir", operation_dir_raw])

    captured = capsys.readouterr()
    assert help_result.returncode == 0
    assert "--operation-dir" in help_result.stdout
    for forbidden in ("host-root", "archive", "digest", "commit", "runner"):
        assert forbidden not in help_result.stdout.lower()
    assert result == 0
    assert captured.out == ""
    assert captured.err == ""
    assert len(observed) == 1
    context = observed[0]
    assert context.host_root == Path("/")
    assert context.paths == module.build_operation_paths(operation_dir)


class SimulatedTask7Crash(BaseException):
    pass


def configure_task7_test_lock_seam(module: object, context: object) -> None:
    if os.name == "posix" or not hasattr(module, "MigrationLocks"):
        return
    lock_files = (
        host_root_path(context.host_root, "/run/lock/degen-prod-db-backup.lock"),
        host_root_path(context.host_root, "/run/degen-prod-db-backup/backup.lock"),
    )

    def acquire_test_migration_locks(
        _context: object,
        *,
        before_action=None,
        after_action=None,
    ) -> object:
        lock_files[1].parent.mkdir(parents=True, exist_ok=True)
        descriptors: list[int] = []
        try:
            for kind, path in zip(("legacy", "runtime"), lock_files):
                action = f"{kind}_lock_acquire"
                if before_action is not None:
                    before_action(action)
                module._atomic_event_hook(f"task7_before_{kind}_lock")
                descriptor = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
                descriptors.append(descriptor)
                module._atomic_event_hook(
                    "migration_lock_acquired",
                    kind=kind,
                    fd=descriptor,
                )
                module._atomic_event_hook(f"task7_after_{kind}_lock")
                if after_action is not None:
                    after_action(action, descriptor)
            if before_action is not None:
                before_action("post_lock_service_recheck")
            return module.MigrationLocks(
                legacy_fd=descriptors[0],
                runtime_fd=descriptors[1],
            )
        except BaseException:
            for descriptor in reversed(descriptors):
                os.close(descriptor)
            raise

    module.acquire_migration_locks = acquire_test_migration_locks


def task7_transaction_fixture(
    module: object,
    tmp_path: Path,
    *,
    absent_targets: tuple[str, ...] = (),
    timer_enabled: bool = True,
    timer_active: bool = True,
) -> tuple[object, dict[str, object]]:
    context, snapshot_fixture = host_snapshot_fixture(
        module,
        tmp_path,
        absent_targets=absent_targets,
    )
    runtime = snapshot_fixture["runtime"]
    runtime["timer_enabled"] = "enabled" if timer_enabled else "disabled"
    runtime["timer_active"] = "active" if timer_active else "inactive"
    runtime["timer_substate"] = "waiting" if timer_active else "dead"
    module.snapshot_host_state(context)
    readonly_runner = context.command_runner
    snapshot_fixture["calls"].clear()

    run_lock = host_root_path(context.host_root, "/run/lock")
    run_lock.mkdir(parents=True, exist_ok=True)
    if os.name == "posix":
        run_lock.parent.chmod(0o755)
        run_lock.chmod(0o1777)

    expected_bytes: dict[str, bytes] = {}
    for source, target in zip(SOURCE_ASSETS[:6], TARGETS[:6]):
        expected_bytes[target] = (
            context.paths.staged_dir / "reviewed" / source
        ).read_bytes()
    expected_bytes[TARGETS[6]] = (
        context.paths.staged_dir / "host/etc/degen/prod-db-backup.env"
    ).read_bytes()
    expected_modes = {
        **{target: 0o755 for target in TARGETS[:4]},
        TARGETS[4]: 0o644,
        TARGETS[5]: 0o644,
        TARGETS[6]: 0o600,
    }
    prior: dict[str, dict[str, object]] = {}
    for target in TARGETS:
        path = host_root_path(context.host_root, target)
        if path.exists():
            metadata = path.stat()
            prior[target] = {
                "present": True,
                "bytes": path.read_bytes(),
                "mode": stat.S_IMODE(metadata.st_mode),
                "uid": metadata.st_uid,
                "gid": metadata.st_gid,
            }
        else:
            prior[target] = {
                "present": False,
                "bytes": None,
                "mode": None,
                "uid": None,
                "gid": None,
            }

    calls: list[dict[str, object]] = []
    events: list[str] = []
    controls: dict[str, object] = {
        "fail_command": None,
        "fail_command_remaining": 1,
        "noop_command": None,
        "rclone_after_preflight": None,
    }

    def command_runner(
        argv: object,
        pass_fds: tuple[int, ...],
    ) -> subprocess.CompletedProcess[str]:
        argv_tuple = tuple(str(value) for value in argv)
        executable = Path(argv_tuple[0]).name if argv_tuple else ""
        if (
            "get-tar-commit-id" in argv_tuple
            or executable in {"loginctl", "date"}
            or (
                executable == "systemctl"
                and ("show" in argv_tuple or "list-units" in argv_tuple)
            )
        ):
            return readonly_runner(argv_tuple, pass_fds)
        action: str
        if executable == "systemctl" and "daemon-reload" in argv_tuple:
            action = "daemon-reload"
        elif executable == "systemctl" and len(argv_tuple) >= 3:
            verb = argv_tuple[1]
            unit = argv_tuple[2]
            if verb not in {"stop", "start", "enable", "disable"}:
                raise AssertionError(f"unexpected Task 7 command: {argv_tuple!r}")
            assert unit == "degen-prod-db-backup.timer"
            action = f"{verb}-timer"
        elif executable == "degen-prod-db-backup" and "preflight" in argv_tuple:
            action = "preflight"
            assert "--lock-fd" in argv_tuple
            inherited = int(argv_tuple[argv_tuple.index("--lock-fd") + 1])
            assert pass_fds == (inherited,)
        else:
            raise AssertionError(f"unexpected Task 7 command: {argv_tuple!r}")
        calls.append({"action": action, "argv": argv_tuple, "pass_fds": pass_fds})
        events.append(action)
        if action == "preflight" and controls["rclone_after_preflight"] is not None:
            rotated = controls["rclone_after_preflight"]
            assert isinstance(rotated, bytes)
            write_private_host_file(
                snapshot_fixture["rclone_path"],
                rotated,
                0o600,
            )
        if controls["fail_command"] == action and int(controls["fail_command_remaining"]) > 0:
            controls["fail_command_remaining"] = int(controls["fail_command_remaining"]) - 1
            if controls["fail_command_remaining"] == 0:
                controls["fail_command"] = None
            return subprocess.CompletedProcess(argv_tuple, 1, "", "controlled failure")
        if controls["noop_command"] == action:
            return subprocess.CompletedProcess(argv_tuple, 0, "", "")
        if action == "stop-timer":
            runtime["timer_active"] = "inactive"
            runtime["timer_substate"] = "dead"
        elif action == "start-timer":
            runtime["timer_active"] = "active"
            runtime["timer_substate"] = "waiting"
        elif action == "enable-timer":
            runtime["timer_enabled"] = "enabled"
        elif action == "disable-timer":
            runtime["timer_enabled"] = "disabled"
        return subprocess.CompletedProcess(argv_tuple, 0, "", "")

    configure_task7_test_lock_seam(module, context)

    return dataclasses.replace(context, command_runner=command_runner), {
        "calls": calls,
        "controls": controls,
        "events": events,
        "expected_bytes": expected_bytes,
        "expected_modes": expected_modes,
        "prior": prior,
        "rclone_path": snapshot_fixture["rclone_path"],
        "runtime": runtime,
        "timer_active": timer_active,
        "timer_enabled": timer_enabled,
    }


def assert_task7_prior_targets_restored(
    context: object,
    fixture: dict[str, object],
) -> None:
    prior = fixture["prior"]
    for target in TARGETS:
        expected = prior[target]
        path = host_root_path(context.host_root, target)
        if expected["present"]:
            assert path.read_bytes() == expected["bytes"]
            if os.name == "posix":
                metadata = path.stat()
                assert stat.S_IMODE(metadata.st_mode) == expected["mode"]
                assert metadata.st_uid == expected["uid"]
                assert metadata.st_gid == expected["gid"]
        else:
            assert not path.exists()


def task7_file_audit(path: Path) -> dict[str, object]:
    metadata = path.stat()
    return {
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "inode": metadata.st_ino,
        "uid": metadata.st_uid,
        "gid": metadata.st_gid,
        "mode": stat.S_IMODE(metadata.st_mode) if os.name == "posix" else 0o600,
        "size": metadata.st_size,
        "mtime_ns": metadata.st_mtime_ns,
    }


def task7_error_evidence_sha256(kind: str, receipt: dict[str, object]) -> str:
    if kind == "primary":
        payload = {
            "phase": receipt["phase"],
            "primary_error": receipt["primary_error"],
            "epoch": receipt["epoch"],
        }
    elif kind == "secondary":
        payload = {
            "stage": receipt["stage"],
            "error": receipt["error"],
            "epoch": receipt["epoch"],
        }
    else:
        raise ValueError(kind)
    canonical = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(
        f"degen-task7-{kind}-error-v1\n".encode("ascii") + canonical + b"\n"
    ).hexdigest()


def task7_write_incomplete_install_state(
    module: object,
    context: object,
    fixture: dict[str, object],
    *,
    index: int = 0,
) -> dict[str, object]:
    state = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    history = state["phase_history"]
    started = int(history[-1]["epoch"]) + 1
    append_phase(state, "installing", started)
    target = TARGETS[index] if index < len(TARGETS) else None
    snapshot_targets = state["snapshot"]["targets"]
    previous = (
        snapshot_targets[target]["sha256"]
        if target is not None and snapshot_targets[target]["present"]
        else None
    )
    intended = (
        hashlib.sha256(fixture["expected_bytes"][target]).hexdigest()
        if target is not None
        else None
    )
    state["install"] = {
        "next_target_index": index,
        "current_target": target,
        "previous_sha256": previous,
        "intended_sha256": intended,
        "installed_hashes": {},
        "started_epoch": started,
        "completed_epoch": None,
        "runtime_directory_created": False,
        "validated_epoch": None,
        "validation_evidence_sha256": None,
    }
    write_state_file(context.paths.operation_dir, state)
    return state


def test_migration_locks_public_contract_is_exact() -> None:
    module = load_ops_helper()

    assert [field.name for field in dataclasses.fields(module.MigrationLocks)] == [
        "legacy_fd",
        "runtime_fd",
    ]
    assert module.MigrationLocks.__dataclass_params__.frozen is True
    assert callable(module.acquire_migration_locks)


@pytest.mark.parametrize(
    "api_name",
    [
        "install_host_configuration",
        "recover_host_configuration",
        "rollback_host_configuration",
        "ensure_runtime_directory",
        "verify_running_source_helper",
    ],
)
def test_task7_public_api_surface_exists(api_name: str) -> None:
    module = load_ops_helper()

    assert callable(getattr(module, api_name))


@pytest.mark.parametrize("receipt", ["install", "recovery"])
@pytest.mark.parametrize("created", [False, True])
def test_task7_schema_records_runtime_directory_creation(
    tmp_path: Path,
    receipt: str,
    created: bool,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = (
        state_at_phase(operation_dir, "installing")
        if receipt == "install"
        else install_recovery_state(operation_dir, "recovering")
    )
    state["install"].update(
        {
            "runtime_directory_created": created,
            "validated_epoch": None,
            "validation_evidence_sha256": None,
        }
    )
    if receipt == "recovery":
        state["recovery"].update(
            {
                "runtime_directory_created": created,
                "restored_epoch": None,
                "restore_evidence_sha256": None,
            }
        )

    module.validate_operation_state(state, operation_dir)


@pytest.mark.parametrize(
    ("receipt", "field"),
    [
        ("install", "runtime_directory_created"),
        ("install", "validated_epoch"),
        ("install", "validation_evidence_sha256"),
        ("recovery", "runtime_directory_created"),
        ("recovery", "runtime_baseline"),
        ("recovery", "restored_epoch"),
        ("recovery", "restore_evidence_sha256"),
    ],
)
def test_task7_schema_requires_runtime_and_provisional_evidence_fields(
    tmp_path: Path,
    receipt: str,
    field: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = (
        state_at_phase(operation_dir, "installing")
        if receipt == "install"
        else install_recovery_state(operation_dir, "recovering")
    )
    state["install"].update(
        {
            "runtime_directory_created": False,
            "validated_epoch": None,
            "validation_evidence_sha256": None,
        }
    )
    if receipt == "recovery":
        state["recovery"].update(
            {
                "runtime_directory_created": False,
                "restored_epoch": None,
                "restore_evidence_sha256": None,
            }
        )
    state[receipt].pop(field)

    with pytest.raises(module.OperationStateError, match=field):
        module.validate_operation_state(state, operation_dir)


@pytest.mark.parametrize("receipt", ["install", "recovery"])
def test_task7_provisional_evidence_requires_terminal_cursor_and_complete_pair(
    tmp_path: Path,
    receipt: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = (
        state_at_phase(operation_dir, "installing")
        if receipt == "install"
        else install_recovery_state(operation_dir, "recovering")
    )
    state["install"].update(
        {
            "runtime_directory_created": False,
            "validated_epoch": None,
            "validation_evidence_sha256": None,
        }
    )
    if receipt == "install":
        target = state["install"]
        epoch_field = "validated_epoch"
        digest_field = "validation_evidence_sha256"
    else:
        state["recovery"].update(
            {
                "runtime_directory_created": False,
                "restored_epoch": None,
                "restore_evidence_sha256": None,
            }
        )
        target = state["recovery"]
        epoch_field = "restored_epoch"
        digest_field = "restore_evidence_sha256"
    target.update(
        {
            "next_target_index": len(TARGETS),
            "current_target": None,
            "previous_sha256": None,
            "intended_sha256": None,
            epoch_field: 1_750_000_042,
            digest_field: HASH_B,
        }
    )
    module.validate_operation_state(state, operation_dir)

    partial = copy.deepcopy(state)
    partial[receipt][digest_field] = None
    with pytest.raises(module.OperationStateError, match=receipt):
        module.validate_operation_state(partial, operation_dir)

    premature = copy.deepcopy(state)
    premature[receipt].update(
        {
            "next_target_index": 0,
            "current_target": TARGETS[0],
            "previous_sha256": HASH_A,
            "intended_sha256": HASH_A,
        }
    )
    with pytest.raises(module.OperationStateError, match=receipt):
        module.validate_operation_state(premature, operation_dir)


def test_install_completion_cannot_create_provisional_validation_in_same_write(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    previous = state_at_phase(operation_dir, "installing")
    previous["install"].update(
        {
            "next_target_index": len(TARGETS),
            "current_target": None,
            "previous_sha256": None,
            "intended_sha256": None,
        }
    )
    completed = state_at_phase(operation_dir, "installed")

    with pytest.raises(
        module.OperationStateError,
        match="provisional|immutable|validation",
    ):
        module.validate_operation_state(completed, operation_dir, previous)


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX flock semantics")
def test_acquire_migration_locks_creates_safe_files_in_stable_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, _fixture = task7_transaction_fixture(module, tmp_path)
    lock_parent = host_root_path(context.host_root, "/run/lock")
    events: list[str] = []

    def record(event: str, **details: object) -> None:
        if event == "migration_lock_acquired":
            events.append(str(details["kind"]))

    monkeypatch.setattr(module, "_atomic_event_hook", record)
    locks = module.acquire_migration_locks(context)
    try:
        assert events == ["legacy", "runtime"]
        assert stat.S_IMODE(lock_parent.stat().st_mode) == 0o1777
        assert os.fstat(locks.legacy_fd).st_nlink == 1
        assert os.fstat(locks.runtime_fd).st_nlink == 1
        runtime_dir = host_root_path(context.host_root, "/run/degen-prod-db-backup")
        assert stat.S_IMODE(runtime_dir.stat().st_mode) == 0o700
        for path in (
            lock_parent / "degen-prod-db-backup.lock",
            runtime_dir / "backup.lock",
        ):
            assert stat.S_IMODE(path.stat().st_mode) == 0o600
    finally:
        os.close(locks.runtime_fd)
        os.close(locks.legacy_fd)


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX link semantics")
@pytest.mark.parametrize("unsafe", ["symlink", "hardlink", "directory", "mode"])
def test_acquire_migration_locks_rejects_unsafe_legacy_lock(
    tmp_path: Path,
    unsafe: str,
) -> None:
    module = load_ops_helper()
    context, _fixture = task7_transaction_fixture(module, tmp_path)
    lock_parent = host_root_path(context.host_root, "/run/lock")
    lock_path = lock_parent / "degen-prod-db-backup.lock"
    other = lock_parent / "other"
    if unsafe == "symlink":
        write_private_host_file(other, b"other\n")
        lock_path.symlink_to(other.name)
    elif unsafe == "hardlink":
        write_private_host_file(other, b"other\n")
        os.link(other, lock_path)
    elif unsafe == "directory":
        lock_path.mkdir()
    else:
        write_private_host_file(lock_path, b"lock\n", 0o666)

    with pytest.raises(module.OperationStateError):
        module.acquire_migration_locks(context)


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX directory metadata")
@pytest.mark.parametrize("unsafe", ["mode", "symlink", "owner"])
def test_acquire_migration_locks_requires_safe_run_lock_parent(
    tmp_path: Path,
    unsafe: str,
) -> None:
    module = load_ops_helper()
    context, _fixture = task7_transaction_fixture(module, tmp_path)
    lock_parent = host_root_path(context.host_root, "/run/lock")
    if unsafe == "mode":
        lock_parent.chmod(0o755)
    elif unsafe == "symlink":
        lock_parent.rmdir()
        outside = tmp_path / "outside-run-lock"
        outside.mkdir(mode=0o777)
        outside.chmod(0o1777)
        lock_parent.symlink_to(outside, target_is_directory=True)
    else:
        if os.geteuid() != 0:
            pytest.skip("changing owner requires root")
        os.chown(lock_parent, 1, 1)

    with pytest.raises(module.OperationStateError, match="lock|parent|mode|owner|symlink"):
        module.acquire_migration_locks(context)


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX directory metadata")
@pytest.mark.parametrize("unsafe", ["symlink", "file", "mode", "owner"])
def test_ensure_runtime_directory_rejects_unsafe_existing_path(
    tmp_path: Path,
    unsafe: str,
) -> None:
    module = load_ops_helper()
    context, _fixture = task7_transaction_fixture(module, tmp_path)
    runtime_dir = host_root_path(context.host_root, "/run/degen-prod-db-backup")
    outside = tmp_path / "outside-runtime"
    if unsafe == "symlink":
        outside.mkdir(mode=0o700)
        runtime_dir.symlink_to(outside, target_is_directory=True)
    elif unsafe == "file":
        write_private_host_file(runtime_dir, b"not a directory\n", 0o600)
    else:
        runtime_dir.mkdir(mode=0o700)
        if unsafe == "mode":
            runtime_dir.chmod(0o755)
        else:
            if os.geteuid() != 0:
                pytest.skip("changing owner requires root")
            os.chown(runtime_dir, 1, 1)

    with pytest.raises(module.OperationStateError, match="runtime|directory|mode|owner|symlink"):
        module.acquire_migration_locks(context)


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX link semantics")
@pytest.mark.parametrize("unsafe", ["symlink", "hardlink", "directory", "mode"])
def test_acquire_migration_locks_rejects_unsafe_runtime_lock(
    tmp_path: Path,
    unsafe: str,
) -> None:
    module = load_ops_helper()
    context, _fixture = task7_transaction_fixture(module, tmp_path)
    runtime_dir = host_root_path(context.host_root, "/run/degen-prod-db-backup")
    runtime_dir.mkdir(mode=0o700)
    lock_path = runtime_dir / "backup.lock"
    other = runtime_dir / "other"
    if unsafe == "symlink":
        write_private_host_file(other, b"other\n")
        lock_path.symlink_to(other.name)
    elif unsafe == "hardlink":
        write_private_host_file(other, b"other\n")
        os.link(other, lock_path)
    elif unsafe == "directory":
        lock_path.mkdir()
    else:
        write_private_host_file(lock_path, b"lock\n", 0o666)
    before = len(os.listdir("/proc/self/fd"))

    with pytest.raises(module.OperationStateError):
        module.acquire_migration_locks(context)

    assert len(os.listdir("/proc/self/fd")) == before


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX path/FD races")
@pytest.mark.parametrize("race_target", ["runtime_directory", "runtime_lock"])
def test_migration_guard_rejects_runtime_path_descriptor_rebind(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    race_target: str,
) -> None:
    module = load_ops_helper()
    context, _fixture = task7_transaction_fixture(module, tmp_path)
    runtime_dir = host_root_path(context.host_root, "/run/degen-prod-db-backup")
    runtime_dir.mkdir(mode=0o700)
    lock_path = runtime_dir / "backup.lock"
    if race_target == "runtime_lock":
        write_private_host_file(lock_path, b"lock\n", 0o600)
    original_open = module.os.open
    raced = False

    def racing_open(path: object, flags: int, *args: object, **kwargs: object) -> int:
        nonlocal raced
        descriptor = original_open(path, flags, *args, **kwargs)
        if raced or not isinstance(path, (str, bytes, os.PathLike)):
            return descriptor
        name = Path(path).name
        expected = "degen-prod-db-backup" if race_target == "runtime_directory" else "backup.lock"
        if name != expected:
            return descriptor
        raced = True
        if race_target == "runtime_directory":
            moved = runtime_dir.with_name("degen-prod-db-backup-opened")
            runtime_dir.rename(moved)
            runtime_dir.mkdir(mode=0o700)
        else:
            moved = lock_path.with_name("backup.lock.opened")
            lock_path.rename(moved)
            write_private_host_file(lock_path, b"replacement\n", 0o600)
        return descriptor

    monkeypatch.setattr(module.os, "open", racing_open)
    monkeypatch.setattr(module, "_require_posix_descriptor_primitives", lambda: None)
    monkeypatch.setattr(module, "_descriptor_primitives_available", lambda: True)

    with pytest.raises(module.OperationStateError, match="binding|changed|runtime|lock"):
        module.acquire_migration_locks(context)

    assert raced is True


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX exclusive mkdir")
def test_runtime_directory_first_attempt_rejects_unexpected_eexist_race(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, _fixture = task7_transaction_fixture(module, tmp_path)
    runtime_dir = host_root_path(context.host_root, "/run/degen-prod-db-backup")
    original_mkdir = module.os.mkdir
    raced = False

    def racing_mkdir(path: object, mode: int = 0o777, *args: object, **kwargs: object) -> None:
        nonlocal raced
        if (
            not raced
            and isinstance(path, (str, os.PathLike))
            and Path(path).name == "degen-prod-db-backup"
        ):
            raced = True
            original_mkdir(path, 0o700, *args, **kwargs)
        original_mkdir(path, mode, *args, **kwargs)

    monkeypatch.setattr(module.os, "mkdir", racing_mkdir)

    with pytest.raises(module.OperationStateError, match="runtime|race|exist"):
        module.acquire_migration_locks(context)

    assert raced is True
    assert runtime_dir.is_dir()


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX flock semantics")
def test_migration_lock_contender_fails_and_does_not_leak_fds(tmp_path: Path) -> None:
    module = load_ops_helper()
    context, _fixture = task7_transaction_fixture(module, tmp_path)
    lock_parent = host_root_path(context.host_root, "/run/lock")
    first = module.acquire_migration_locks(context)
    before = len(os.listdir("/proc/self/fd"))
    try:
        with pytest.raises(module.OperationStateError, match="lock|contender|busy"):
            module.acquire_migration_locks(context)
        assert len(os.listdir("/proc/self/fd")) == before
    finally:
        os.close(first.runtime_fd)
        os.close(first.legacy_fd)


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX flock primitives")
def test_release_migration_locks_reports_actual_unlock_and_close_errors_in_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import fcntl

    module = load_ops_helper()
    calls: list[tuple[str, int]] = []

    def fail_flock(descriptor: int, operation: int) -> None:
        assert operation == fcntl.LOCK_UN
        calls.append(("flock", descriptor))
        raise OSError(f"unlock {descriptor}")

    def fail_close(descriptor: int) -> None:
        calls.append(("close", descriptor))
        raise OSError(f"close {descriptor}")

    monkeypatch.setattr(fcntl, "flock", fail_flock)
    monkeypatch.setattr(
        module,
        "os",
        types.SimpleNamespace(close=fail_close, name=os.name),
    )
    issues = module._release_migration_locks(
        module.MigrationLocks(legacy_fd=101, runtime_fd=202)
    )

    assert calls == [
        ("flock", 202),
        ("close", 202),
        ("flock", 101),
        ("close", 101),
    ]
    assert [stage for stage, _error in issues] == [
        "release_runtime_lock",
        "release_runtime_lock",
        "release_legacy_lock",
        "release_legacy_lock",
    ]
    assert [issue.release_uncertain for issue in issues] == [True, True, True, True]


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX flock semantics")
def test_both_migration_locks_remain_held_through_replace_reload_and_preflight(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import fcntl

    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    legacy_path = host_root_path(
        context.host_root,
        "/run/lock/degen-prod-db-backup.lock",
    )
    runtime_path = host_root_path(
        context.host_root,
        "/run/degen-prod-db-backup/backup.lock",
    )
    checked: list[str] = []

    def assert_separately_contended(path: Path) -> None:
        descriptor = os.open(path, os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            with pytest.raises(BlockingIOError):
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(descriptor)

    def inspect_locks(event: str, **_details: object) -> None:
        if event not in {
            "task7_after_target_replace",
            "task7_after_daemon_reload",
            "task7_after_validation",
        }:
            return
        assert_separately_contended(legacy_path)
        assert_separately_contended(runtime_path)
        checked.append(event)

    monkeypatch.setattr(module, "_atomic_event_hook", inspect_locks)
    original_runner = context.command_runner

    def inspect_inherited_fd(
        argv: object,
        pass_fds: tuple[int, ...],
    ) -> subprocess.CompletedProcess[str]:
        argv_tuple = tuple(str(value) for value in argv)
        if Path(argv_tuple[0]).name == "degen-prod-db-backup" and "preflight" in argv_tuple:
            assert len(pass_fds) == 1
            fcntl.flock(pass_fds[0], fcntl.LOCK_EX | fcntl.LOCK_NB)
            assert_separately_contended(runtime_path)
            assert_separately_contended(legacy_path)
            checked.append("inherited-same-ofd-preflight")
        return original_runner(argv_tuple, pass_fds)

    context = dataclasses.replace(context, command_runner=inspect_inherited_fd)
    module.install_host_configuration(context)

    assert "task7_after_target_replace" in checked
    assert "task7_after_daemon_reload" in checked
    assert "task7_after_validation" in checked
    assert "inherited-same-ofd-preflight" in checked
    assert fixture["runtime"]["timer_active"] == "active"


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX flock semantics")
def test_external_legacy_owner_blocks_install_before_timer_mutation(
    tmp_path: Path,
) -> None:
    import fcntl

    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    path = host_root_path(context.host_root, "/run/lock/degen-prod-db-backup.lock")
    descriptor = os.open(
        path,
        os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
        0o600,
    )
    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        with pytest.raises(module.OperationStateError, match="lock|busy|contender"):
            module.install_host_configuration(context)
    finally:
        os.close(descriptor)

    assert not any(call["action"] in {"disable-timer", "stop-timer"} for call in fixture["calls"])
    assert_task7_prior_targets_restored(context, fixture)


def test_install_happy_path_is_transactional_and_provenance_bound(tmp_path: Path) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)

    module.install_host_configuration(context)

    state = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    module.validate_operation_state_for_context(state, context)
    assert state["phase"] == "installed"
    assert state["install"]["completed_epoch"] is not None
    assert state["install"]["runtime_directory_created"] is True
    assert state["install"]["installed_hashes"] == {
        target: hashlib.sha256(fixture["expected_bytes"][target]).hexdigest()
        for target in TARGETS
    }
    for target in TARGETS:
        path = host_root_path(context.host_root, target)
        assert path.read_bytes() == fixture["expected_bytes"][target]
        if os.name == "posix":
            assert stat.S_IMODE(path.stat().st_mode) == fixture["expected_modes"][target]
    environment_target = host_root_path(context.host_root, TARGETS[6])
    reviewed_environment_example = (
        context.paths.staged_dir / "reviewed" / SOURCE_ASSETS[6]
    )
    assert environment_target.read_bytes() != reviewed_environment_example.read_bytes()
    assert state["install"]["installed_hashes"][TARGETS[6]] == state["host_stage"][
        "environment_sha256"
    ]
    actions = [call["action"] for call in fixture["calls"]]
    assert actions.count("daemon-reload") == 1
    assert actions.count("preflight") == 1
    assert not any(
        "degen-prod-db-backup.service" in call["argv"] and "start" in call["argv"]
        for call in fixture["calls"]
    )
    assert fixture["runtime"]["timer_active"] == "active"
    assert fixture["runtime"]["timer_enabled"] == "enabled"


def test_install_persists_provisional_validation_before_reverse_unlock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, _fixture = task7_transaction_fixture(module, tmp_path)
    observed: dict[str, dict[str, object]] = {}

    def capture(event: str, **_details: object) -> None:
        if event not in {
            "task7_after_daemon_reload",
            "task7_after_validation",
            "task7_before_lock_release",
            "task7_after_lock_release",
            "task7_after_timer_restore",
        }:
            return
        state = module.load_operation_state(
            context.paths.state_file,
            effective_uid=context.effective_uid,
        )
        observed[event] = copy.deepcopy(state)

    monkeypatch.setattr(module, "_atomic_event_hook", capture)
    module.install_host_configuration(context)

    for event in (
        "task7_after_daemon_reload",
        "task7_after_validation",
        "task7_before_lock_release",
        "task7_after_lock_release",
        "task7_after_timer_restore",
    ):
        state = observed[event]
        assert state["phase"] == "installing"
        assert state["install"]["installed_hashes"] == {}
        assert state["install"]["completed_epoch"] is None
    before_release = observed["task7_before_lock_release"]["install"]
    assert before_release["next_target_index"] == len(TARGETS)
    assert before_release["current_target"] is None
    assert before_release["validated_epoch"] is not None
    assert len(before_release["validation_evidence_sha256"]) == 64
    assert len(observed["task7_before_lock_release"]["rclone_evidence_groups"]) == 1
    final = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert final["phase"] == "installed"
    assert final["install"]["completed_epoch"] is not None


@pytest.mark.parametrize("oauth_refresh", [False, True])
def test_install_preflight_records_exact_secret_free_rclone_evidence_group(
    tmp_path: Path,
    oauth_refresh: bool,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    before = task7_file_audit(fixture["rclone_path"])
    secret = b"[onedrive]\ntype=onedrive\ntoken=TASK7_ROTATED_SECRET_SENTINEL\n"
    if oauth_refresh:
        fixture["controls"]["rclone_after_preflight"] = secret

    module.install_host_configuration(context)

    after = task7_file_audit(fixture["rclone_path"])
    state = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert len(state["rclone_evidence_groups"]) == 1
    group = state["rclone_evidence_groups"][0]
    assert group["group_id"] == "install"
    assert group["purpose"] == "credential-refresh-audit"
    assert group["before"] == before
    assert group["after"] == after
    assert len(group["evidence_sha256"]) == 64
    int(group["evidence_sha256"], 16)
    assert (before != after) is oauth_refresh
    serialized = json.dumps(state, sort_keys=True)
    assert "TASK7_ROTATED_SECRET_SENTINEL" not in serialized
    assert "token=" not in serialized


def test_failed_preflight_still_records_after_rclone_audit_before_recovery(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    rotated = b"[onedrive]\ntype=onedrive\ntoken=FAILED_PREFLIGHT_REFRESH\n"
    fixture["controls"]["rclone_after_preflight"] = rotated
    fixture["controls"]["fail_command"] = "preflight"

    with pytest.raises(module.OperationStateError):
        module.install_host_configuration(context)

    state = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert len(state["rclone_evidence_groups"]) == 1
    group = state["rclone_evidence_groups"][0]
    assert group["before"] != group["after"]
    assert group["after"] == task7_file_audit(fixture["rclone_path"])
    assert "FAILED_PREFLIGHT_REFRESH" not in json.dumps(state, sort_keys=True)
    assert fixture["rclone_path"].read_bytes() == rotated


def test_rclone_audit_group_write_is_retried_before_recovery_can_finish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    rotated = b"[onedrive]\ntype=onedrive\ntoken=RETRIED_AUDIT_SECRET\n"
    fixture["controls"]["rclone_after_preflight"] = rotated
    original_write = module._task7_write_state
    audit_write_failed = False

    def fail_first_audit_write(
        write_context: object,
        binding: object,
        state: dict[str, object],
        **kwargs: object,
    ) -> None:
        nonlocal audit_write_failed
        groups = state["rclone_evidence_groups"]
        if groups and groups[-1]["after"] is not None and not audit_write_failed:
            audit_write_failed = True
            raise module.OperationStateError("controlled audit receipt write")
        original_write(write_context, binding, state, **kwargs)

    monkeypatch.setattr(module, "_task7_write_state", fail_first_audit_write)
    module.install_host_configuration(context)

    state = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert audit_write_failed is True
    assert state["phase"] == "installed"
    assert len(state["rclone_evidence_groups"]) == 1
    assert state["rclone_evidence_groups"][0]["after"] == task7_file_audit(
        fixture["rclone_path"]
    )
    assert "RETRIED_AUDIT_SECRET" not in json.dumps(state, sort_keys=True)


def test_rclone_audit_survives_two_write_failures_before_terminal_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    rotated = b"[onedrive]\ntype=onedrive\ntoken=CARRIED_AUDIT_SECRET\n"
    fixture["controls"]["rclone_after_preflight"] = rotated
    original_write = module._task7_write_state
    blocked = 0

    def block_two_audit_writes(
        write_context: object,
        binding: object,
        state: dict[str, object],
        **kwargs: object,
    ) -> None:
        nonlocal blocked
        groups = state["rclone_evidence_groups"]
        if groups and groups[-1]["after"] is not None and blocked < 2:
            blocked += 1
            raise module.OperationStateError("controlled persistent audit receipt write")
        original_write(write_context, binding, state, **kwargs)

    monkeypatch.setattr(module, "_task7_write_state", block_two_audit_writes)

    with pytest.raises(module.OperationStateError, match="audit"):
        module.install_host_configuration(context)

    state = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert blocked == 2
    assert state["phase"] == "rolled_back"
    assert len(state["rclone_evidence_groups"]) == 1
    assert state["rclone_evidence_groups"][0]["after"] == task7_file_audit(
        fixture["rclone_path"]
    )
    assert fixture["rclone_path"].read_bytes() == rotated
    assert "CARRIED_AUDIT_SECRET" not in json.dumps(state, sort_keys=True)


def test_preflight_failure_remains_primary_when_rclone_audit_write_also_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    rotated = b"[onedrive]\ntype=onedrive\ntoken=COMBINED_FAILURE_SECRET\n"
    fixture["controls"]["rclone_after_preflight"] = rotated
    fixture["controls"]["fail_command"] = "preflight"
    original_write = module._task7_write_state
    blocked = 0

    def block_two_audit_writes(
        write_context: object,
        binding: object,
        state: dict[str, object],
        **kwargs: object,
    ) -> None:
        nonlocal blocked
        groups = state["rclone_evidence_groups"]
        if groups and groups[-1]["after"] is not None and blocked < 2:
            blocked += 1
            raise module.OperationStateError("controlled combined audit write")
        original_write(write_context, binding, state, **kwargs)

    monkeypatch.setattr(module, "_task7_write_state", block_two_audit_writes)

    with pytest.raises(module.OperationStateError, match="preflight"):
        module.install_host_configuration(context)

    state = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert state["phase"] == "rolled_back"
    assert "preflight" in state["failure"]["primary_error"]
    assert any(
        item["stage"] == "rclone_audit_persistence"
        and "audit" in item["error"]
        for item in state["secondary_errors"]
    )
    assert len(state["rclone_evidence_groups"]) == 1
    assert fixture["rclone_path"].read_bytes() == rotated
    assert "COMBINED_FAILURE_SECRET" not in json.dumps(state, sort_keys=True)


def test_persistent_rclone_audit_storage_failure_blocks_terminal_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    rotated = b"[onedrive]\ntype=onedrive\ntoken=BLOCKED_AUDIT_SECRET\n"
    fixture["controls"]["rclone_after_preflight"] = rotated
    original_write = module._task7_write_state

    def block_all_audit_writes(
        write_context: object,
        binding: object,
        state: dict[str, object],
        **kwargs: object,
    ) -> None:
        groups = state["rclone_evidence_groups"]
        if groups and groups[-1]["after"] is not None:
            raise module.OperationStateError("persistent audit storage outage")
        original_write(write_context, binding, state, **kwargs)

    monkeypatch.setattr(module, "_task7_write_state", block_all_audit_writes)

    with pytest.raises(module.OperationStateError, match="audit"):
        module.install_host_configuration(context)

    state = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert state["phase"] not in {"installed", "rolled_back"}
    assert len(state["rclone_evidence_groups"]) == 1
    assert state["rclone_evidence_groups"][0]["after"] is None
    assert fixture["rclone_path"].read_bytes() == rotated

    resumed = load_ops_helper()
    configure_task7_test_lock_seam(resumed, context)
    resumed.recover_host_configuration(context)
    final = resumed.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert final["phase"] == "rolled_back"
    assert len(final["rclone_evidence_groups"]) == 1
    assert final["rclone_evidence_groups"][0]["after"] == task7_file_audit(
        fixture["rclone_path"]
    )


def test_preflight_crash_survives_compound_rclone_audit_write_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    rotated = b"[onedrive]\ntype=onedrive\ntoken=CRASH_AUDIT_SECRET\n"
    fixture["controls"]["rclone_after_preflight"] = rotated
    original_runner = context.command_runner

    def crash_after_preflight(
        argv: object,
        pass_fds: tuple[int, ...],
    ) -> subprocess.CompletedProcess[str]:
        result = original_runner(argv, pass_fds)
        argv_tuple = tuple(str(value) for value in argv)
        if Path(argv_tuple[0]).name == "degen-prod-db-backup" and "preflight" in argv_tuple:
            raise SimulatedTask7Crash("preflight process crash")
        return result

    context = dataclasses.replace(context, command_runner=crash_after_preflight)
    original_write = module._task7_write_state
    blocked = 0

    def block_two_audit_writes(
        write_context: object,
        binding: object,
        state: dict[str, object],
        **kwargs: object,
    ) -> None:
        nonlocal blocked
        groups = state["rclone_evidence_groups"]
        if groups and groups[-1]["after"] is not None and blocked < 2:
            blocked += 1
            raise module.OperationStateError("controlled crash audit write")
        original_write(write_context, binding, state, **kwargs)

    monkeypatch.setattr(module, "_task7_write_state", block_two_audit_writes)

    with pytest.raises(SimulatedTask7Crash, match="preflight"):
        module.install_host_configuration(context)

    crashed = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert crashed["phase"] == "installing"
    assert crashed["failure"] is None
    assert len(crashed["rclone_evidence_groups"]) == 1
    assert fixture["runtime"]["timer_active"] == "inactive"

    resumed = load_ops_helper()
    configure_task7_test_lock_seam(resumed, context)
    resumed.recover_host_configuration(context)
    final = resumed.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert final["phase"] == "rolled_back"
    assert len(final["rclone_evidence_groups"]) == 1
    assert fixture["rclone_path"].read_bytes() == rotated
    assert "CRASH_AUDIT_SECRET" not in json.dumps(final, sort_keys=True)


def test_preflight_crash_is_not_masked_by_after_audit_capture_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    original_runner = context.command_runner

    def crash_after_preflight(
        argv: object,
        pass_fds: tuple[int, ...],
    ) -> subprocess.CompletedProcess[str]:
        result = original_runner(argv, pass_fds)
        argv_tuple = tuple(str(value) for value in argv)
        if Path(argv_tuple[0]).name == "degen-prod-db-backup" and "preflight" in argv_tuple:
            raise SimulatedTask7Crash("original preflight crash")
        return result

    context = dataclasses.replace(context, command_runner=crash_after_preflight)
    original_audit = module._task7_capture_file_audit
    audit_calls = 0

    def fail_after_audit(*args: object, **kwargs: object) -> dict[str, object]:
        nonlocal audit_calls
        audit_calls += 1
        if audit_calls == 2:
            raise module.OperationStateError("controlled after-audit capture failure")
        return original_audit(*args, **kwargs)

    monkeypatch.setattr(module, "_task7_capture_file_audit", fail_after_audit)

    with pytest.raises(SimulatedTask7Crash, match="original preflight"):
        module.install_host_configuration(context)

    state = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert audit_calls == 2
    assert state["phase"] == "installing"
    assert state["failure"] is None
    assert len(state["rclone_evidence_groups"]) == 1
    assert state["rclone_evidence_groups"][0]["after"] is None
    assert fixture["runtime"]["timer_active"] == "inactive"


def test_ordinary_preflight_failure_remains_primary_when_after_audit_is_unknown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    fixture["controls"]["fail_command"] = "preflight"
    fixture["controls"]["rclone_after_preflight"] = (
        b"[onedrive]\ntype=onedrive\ntoken=UNKNOWN_AFTER_AUDIT_SECRET\n"
    )
    original_audit = module._task7_capture_file_audit
    audit_calls = 0

    def fail_after_audit(*args: object, **kwargs: object) -> dict[str, object]:
        nonlocal audit_calls
        audit_calls += 1
        if audit_calls == 2:
            raise module.OperationStateError("controlled after-audit capture failure")
        return original_audit(*args, **kwargs)

    monkeypatch.setattr(module, "_task7_capture_file_audit", fail_after_audit)

    with pytest.raises(module.OperationStateError, match="preflight"):
        module.install_host_configuration(context)

    state = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert state["phase"] not in {"installed", "rolled_back"}
    assert "preflight" in state["failure"]["primary_error"]
    assert any(
        item["stage"] == "rclone_audit_capture"
        and "after-audit" in item["error"]
        for item in state["secondary_errors"]
    )
    assert len(state["rclone_evidence_groups"]) == 1
    assert state["rclone_evidence_groups"][0]["after"] is None
    assert "UNKNOWN_AFTER_AUDIT_SECRET" not in json.dumps(state, sort_keys=True)


def test_after_audit_crash_escapes_even_after_ordinary_preflight_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    fixture["controls"]["fail_command"] = "preflight"
    original_audit = module._task7_capture_file_audit
    audit_calls = 0

    def crash_after_audit(*args: object, **kwargs: object) -> dict[str, object]:
        nonlocal audit_calls
        audit_calls += 1
        if audit_calls == 2:
            raise SimulatedTask7Crash("after-audit crash")
        return original_audit(*args, **kwargs)

    monkeypatch.setattr(module, "_task7_capture_file_audit", crash_after_audit)

    with pytest.raises(SimulatedTask7Crash, match="after-audit"):
        module.install_host_configuration(context)

    state = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert state["phase"] == "installing"
    assert state["failure"] is None
    assert len(state["rclone_evidence_groups"]) == 1
    assert state["rclone_evidence_groups"][0]["after"] is None
    assert fixture["runtime"]["timer_active"] == "inactive"


def test_failed_install_recovery_never_restores_rclone_audit_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    rotated = b"[onedrive]\ntype=onedrive\ntoken=ROTATED_DURING_PREFLIGHT\n"
    fixture["controls"]["rclone_after_preflight"] = rotated

    def fail_after_validation(event: str, **_details: object) -> None:
        if event == "task7_after_validation":
            raise module.OperationStateError("controlled post-preflight failure")

    monkeypatch.setattr(module, "_atomic_event_hook", fail_after_validation)
    with pytest.raises(module.OperationStateError, match="controlled"):
        module.install_host_configuration(context)

    assert fixture["rclone_path"].read_bytes() == rotated
    assert_task7_prior_targets_restored(context, fixture)


def test_runtime_directory_creation_is_recorded_per_install_and_recovery_attempt(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    context, _fixture = task7_transaction_fixture(module, tmp_path)
    runtime_dir = host_root_path(context.host_root, "/run/degen-prod-db-backup")
    runtime_dir.mkdir(mode=0o700)
    if os.name == "posix":
        runtime_dir.chmod(0o700)

    module.install_host_configuration(context)
    installed = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert installed["install"]["runtime_directory_created"] is False

    lock_path = runtime_dir / "backup.lock"
    if lock_path.exists():
        lock_path.unlink()
    runtime_dir.rmdir()
    module.rollback_host_configuration(context)
    rolled_back = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert rolled_back["install"]["runtime_directory_created"] is False
    assert rolled_back["recovery"]["runtime_directory_created"] is True


def test_install_rejects_runtime_directory_created_after_absence_proof(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    runtime_dir = host_root_path(context.host_root, "/run/degen-prod-db-backup")
    raced = False

    def create_after_marker(event: str, **_details: object) -> None:
        nonlocal raced
        if event == "task7_after_installing_state" and not raced:
            raced = True
            runtime_dir.mkdir(mode=0o700)
            if os.name == "posix":
                runtime_dir.chmod(0o700)

    monkeypatch.setattr(module, "_atomic_event_hook", create_after_marker)
    with pytest.raises(module.OperationStateError, match="runtime|race|exist"):
        module.install_host_configuration(context)

    state = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert state["install"]["runtime_directory_created"] is True
    assert state["phase"] != "installed"
    assert_task7_prior_targets_restored(context, fixture)


@pytest.mark.parametrize(
    ("timer_enabled", "timer_active"),
    [(False, False), (False, True), (True, False), (True, True)],
)
def test_install_restores_exact_prior_timer_state(
    tmp_path: Path,
    timer_enabled: bool,
    timer_active: bool,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(
        module,
        tmp_path,
        timer_enabled=timer_enabled,
        timer_active=timer_active,
    )

    module.install_host_configuration(context)

    assert (fixture["runtime"]["timer_enabled"] == "enabled") is timer_enabled
    assert (fixture["runtime"]["timer_active"] == "active") is timer_active
    actions = [call["action"] for call in fixture["calls"]]
    assert ("disable-timer" in actions) is timer_enabled
    assert ("enable-timer" in actions) is timer_enabled
    assert "stop-timer" in actions
    assert ("start-timer" in actions) is timer_active
    if timer_enabled:
        assert actions.index("disable-timer") < actions.index("stop-timer")
    if timer_enabled and timer_active:
        assert actions.index("enable-timer") < actions.index("start-timer")


@pytest.mark.parametrize(
    ("noop_action", "timer_enabled", "timer_active"),
    [
        ("disable-timer", True, True),
        ("stop-timer", True, True),
        ("enable-timer", True, False),
        ("start-timer", False, True),
    ],
)
def test_install_rejects_successful_but_stubborn_timer_transition(
    tmp_path: Path,
    noop_action: str,
    timer_enabled: bool,
    timer_active: bool,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(
        module,
        tmp_path,
        timer_enabled=timer_enabled,
        timer_active=timer_active,
    )
    fixture["controls"]["noop_command"] = noop_action

    with pytest.raises(module.OperationStateError, match="timer|runtime|state"):
        module.install_host_configuration(context)

    state = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert state["phase"] != "installed"


@pytest.mark.parametrize(
    "primitive",
    ["write", "fsync", "fchmod", "fchown"],
)
def test_ordinary_install_temp_failure_cleans_only_owned_temporary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    primitive: str,
) -> None:
    if primitive in {"fchmod", "fchown"} and os.name != "posix":
        pytest.skip("requires POSIX target metadata primitives")
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    target = host_root_path(context.host_root, TARGETS[0])
    temporary = target.with_name(
        f".{target.name}.{context.operation_id}.install.tmp"
    )
    unrelated = target.with_name(".degen-prod-db-backup.operator.tmp")
    write_private_host_file(unrelated, b"operator-owned\n", 0o600)
    armed = False
    failed = False

    def arm(event: str, **_details: object) -> None:
        nonlocal armed
        if event == "task7_after_installing_state":
            armed = True

    monkeypatch.setattr(module, "_atomic_event_hook", arm)
    if primitive == "write":
        original = module._write_all

        def fail_write(descriptor: int, data: bytes) -> None:
            nonlocal failed
            if armed and not failed:
                failed = True
                os.write(descriptor, data[: min(32, len(data))])
                raise OSError("controlled partial target write failure")
            original(descriptor, data)

        monkeypatch.setattr(module, "_write_all", fail_write)
    elif primitive == "fsync":
        original = module.os.fsync

        def fail_fsync(descriptor: int) -> None:
            nonlocal failed
            metadata = os.fstat(descriptor)
            if (
                armed
                and not failed
                and stat.S_ISREG(metadata.st_mode)
                and metadata.st_size == len(fixture["expected_bytes"][TARGETS[0]])
            ):
                failed = True
                raise OSError("controlled target fsync failure")
            original(descriptor)

        monkeypatch.setattr(module.os, "fsync", fail_fsync)
    elif primitive == "fchmod":
        original = module.os.fchmod

        def fail_fchmod(descriptor: int, mode: int) -> None:
            nonlocal failed
            if armed and not failed and mode == 0o755:
                failed = True
                raise OSError("controlled target fchmod failure")
            original(descriptor, mode)

        monkeypatch.setattr(module.os, "fchmod", fail_fchmod)
    else:
        original = module.os.fchown

        def fail_fchown(descriptor: int, uid: int, gid: int) -> None:
            nonlocal failed
            if armed and not failed:
                failed = True
                raise OSError("controlled target fchown failure")
            original(descriptor, uid, gid)

        monkeypatch.setattr(module.os, "fchown", fail_fchown)

    with pytest.raises((module.OperationStateError, OSError), match="controlled"):
        module.install_host_configuration(context)

    assert failed is True
    assert not temporary.exists()
    assert unrelated.read_bytes() == b"operator-owned\n"


def test_install_temp_open_race_preserves_unowned_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, _fixture = task7_transaction_fixture(module, tmp_path)
    target = host_root_path(context.host_root, TARGETS[0])
    temporary = target.with_name(
        f".{target.name}.{context.operation_id}.install.tmp"
    )
    attacker = b"attacker-created race file\n"
    original_open = module.os.open
    armed = False
    raced = False

    def arm(event: str, **_details: object) -> None:
        nonlocal armed
        if event == "task7_after_installing_state":
            armed = True

    def racing_open(
        path: object,
        flags: int,
        *args: object,
        **kwargs: object,
    ) -> int:
        nonlocal raced
        if (
            armed
            and not raced
            and Path(path).name == temporary.name
            and flags & os.O_EXCL
        ):
            raced = True
            descriptor = original_open(path, flags, *args, **kwargs)
            try:
                os.write(descriptor, attacker)
            finally:
                os.close(descriptor)
        return original_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(module, "_atomic_event_hook", arm)
    monkeypatch.setattr(module.os, "open", racing_open)
    if os.name == "posix":
        monkeypatch.setattr(module, "_descriptor_primitives_available", lambda: True)
        monkeypatch.setattr(
            module,
            "_write_descriptor_primitives_available",
            lambda: True,
        )
        monkeypatch.setattr(module, "_require_posix_descriptor_primitives", lambda: None)

    with pytest.raises(module.OperationStateError, match="exist|race|temporary"):
        module.install_host_configuration(context)

    assert raced is True
    assert temporary.read_bytes() == attacker


def test_primary_failure_receipt_write_is_retried_before_terminal_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, _fixture = task7_transaction_fixture(module, tmp_path)
    original_write = module._task7_write_state
    receipt_write_failed = False
    target_failed = False

    def fail_primary(event: str, **_details: object) -> None:
        nonlocal target_failed
        if event == "task7_after_target_replace" and not target_failed:
            target_failed = True
            raise module.OperationStateError("durable original install failure")

    def fail_first_receipt_write(
        write_context: object,
        binding: object,
        state: dict[str, object],
        **kwargs: object,
    ) -> None:
        nonlocal receipt_write_failed
        if state["failure"] is not None and not receipt_write_failed:
            receipt_write_failed = True
            raise module.OperationStateError("controlled failure receipt write")
        original_write(write_context, binding, state, **kwargs)

    monkeypatch.setattr(module, "_atomic_event_hook", fail_primary)
    monkeypatch.setattr(module, "_task7_write_state", fail_first_receipt_write)

    with pytest.raises(module.OperationStateError, match="original"):
        module.install_host_configuration(context)

    state = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert receipt_write_failed is True
    assert state["phase"] == "rolled_back"
    assert state["failure"]["primary_error"] == "durable original install failure"


@pytest.mark.parametrize(
    "tamper",
    [
        "source",
        "source_manifest",
        "stage",
        "stage_manifest",
        "snapshot",
        "snapshot_manifest",
        "state",
    ],
)
def test_install_revalidates_every_proof_before_first_mutation(
    tmp_path: Path,
    tamper: str,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    if tamper == "source":
        path = context.paths.source_dir / SOURCE_ASSETS[0]
    elif tamper == "source_manifest":
        path = context.paths.source_dir / SOURCE_MANIFEST
    elif tamper == "stage":
        path = context.paths.staged_dir / "reviewed" / SOURCE_ASSETS[0]
    elif tamper == "stage_manifest":
        path = context.paths.staged_dir / "host-stage-manifest.json"
    elif tamper == "snapshot":
        path = context.paths.snapshot_dir / snapshot_artifact_name(TARGETS[0])
    elif tamper == "snapshot_manifest":
        path = context.paths.snapshot_dir / "SHA256SUMS"
    else:
        path = context.paths.state_file
    path.write_bytes(path.read_bytes() + b"tampered before install\n")
    if os.name == "posix":
        path.chmod(0o600 if tamper in {"snapshot", "state"} else path.stat().st_mode)

    with pytest.raises(module.OperationStateError):
        module.install_host_configuration(context)

    assert fixture["calls"] == []
    assert_task7_prior_targets_restored(context, fixture)
    assert not host_root_path(
        context.host_root,
        "/run/degen-prod-db-backup",
    ).exists()


@pytest.mark.parametrize("tamper", ["content", "symlink", "hardlink", "mode"])
def test_install_refuses_live_target_drift_before_guard_or_timer_mutation(
    tmp_path: Path,
    tamper: str,
) -> None:
    if tamper in {"symlink", "hardlink", "mode"} and os.name != "posix":
        pytest.skip("requires POSIX target metadata")
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    target = host_root_path(context.host_root, TARGETS[0])
    if tamper == "content":
        target.write_bytes(target.read_bytes() + b"drift\n")
    elif tamper == "mode":
        target.chmod(0o777)
    else:
        outside = tmp_path / f"outside-{tamper}"
        write_private_host_file(outside, b"outside\n", 0o600)
        target.unlink()
        if tamper == "symlink":
            target.symlink_to(outside)
        else:
            os.link(outside, target)

    with pytest.raises(module.OperationStateError, match="target|snapshot|changed|binding|mode"):
        module.install_host_configuration(context)

    assert not any(
        call["action"]
        in {
            "disable-timer",
            "stop-timer",
            "daemon-reload",
            "preflight",
        }
        for call in fixture["calls"]
    )
    assert not host_root_path(
        context.host_root,
        "/run/degen-prod-db-backup",
    ).exists()


@pytest.mark.parametrize(
    "service_state",
    [
        ("loaded", "active", "running", 0),
        ("loaded", "inactive", "running", 0),
        ("loaded", "inactive", "dead", 999),
        ("loaded", "failed", "failed", 0),
    ],
)
def test_install_requires_service_inactive_dead_and_zero_pid_before_locking(
    tmp_path: Path,
    service_state: tuple[str, str, str, int],
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    fixture["runtime"]["service_states"][
        "degen-prod-db-backup.service"
    ] = service_state

    with pytest.raises(module.OperationStateError, match="service|inactive|dead|MainPID"):
        module.install_host_configuration(context)

    assert_task7_prior_targets_restored(context, fixture)
    assert fixture["runtime"]["timer_enabled"] == "enabled"
    assert fixture["runtime"]["timer_active"] == "active"


@pytest.mark.parametrize("drift", ["enabled", "active", "trigger", "protected_pid"])
def test_install_refuses_prior_runtime_drift_before_first_mutation(
    tmp_path: Path,
    drift: str,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    runtime = fixture["runtime"]
    if drift == "enabled":
        runtime["timer_enabled"] = "disabled"
    elif drift == "active":
        runtime["timer_active"] = "inactive"
        runtime["timer_substate"] = "dead"
    elif drift == "trigger":
        runtime["timer_trigger_epoch"] += 60
    else:
        runtime["service_pids"]["degen-web.service"] += 1

    with pytest.raises(module.OperationStateError, match="runtime|timer|trigger|PID|pid"):
        module.install_host_configuration(context)

    assert fixture["calls"] == []
    assert not host_root_path(
        context.host_root,
        "/run/degen-prod-db-backup",
    ).exists()


@pytest.mark.parametrize("proof", ["source", "stage", "snapshot", "state"])
def test_held_later_phase_proof_rejects_change_after_initial_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    proof: str,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    paths = {
        "source": context.paths.source_dir / SOURCE_ASSETS[0],
        "stage": context.paths.staged_dir / "reviewed" / SOURCE_ASSETS[0],
        "snapshot": context.paths.snapshot_dir / snapshot_artifact_name(TARGETS[0]),
        "state": context.paths.state_file,
    }
    raced = False

    def mutate(event: str, **_details: object) -> None:
        nonlocal raced
        if event == "task7_after_installing_state" and not raced:
            raced = True
            path = paths[proof]
            path.write_bytes(path.read_bytes() + b"post-proof mutation\n")
            if os.name == "posix":
                path.chmod(0o600 if proof in {"snapshot", "state"} else path.stat().st_mode)

    monkeypatch.setattr(module, "_atomic_event_hook", mutate)
    with pytest.raises(module.OperationStateError, match="changed|proof|binding|state"):
        module.install_host_configuration(context)

    assert_task7_prior_targets_restored(context, fixture)


def test_installing_write_ahead_is_durable_before_timer_disable_and_stop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    sequence = fixture["events"]
    original = module._atomic_write_operation_state_internal

    def record_state(*args: object, **kwargs: object) -> None:
        state = args[1]
        original(*args, **kwargs)
        if state["phase"] == "installing":
            sequence.append("installing-state-durable")

    monkeypatch.setattr(module, "_atomic_write_operation_state_internal", record_state)

    module.install_host_configuration(context)

    assert sequence.index("installing-state-durable") < sequence.index("disable-timer")
    assert sequence.index("installing-state-durable") < sequence.index("stop-timer")


def test_each_target_cursor_is_durable_and_exact_before_rename(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(
        module,
        tmp_path,
        absent_targets=(TARGETS[2], TARGETS[3]),
    )
    observed: list[str] = []

    def inspect(event: str, **details: object) -> None:
        if event != "task7_before_target_replace":
            return
        index = int(details["index"])
        target = str(details["target"])
        state = module.load_operation_state(
            context.paths.state_file,
            effective_uid=context.effective_uid,
        )
        cursor = state["install"]
        snapshot_target = state["snapshot"]["targets"][target]
        assert index == len(observed)
        assert target == TARGETS[index]
        assert cursor["next_target_index"] == index
        assert cursor["current_target"] == target
        assert cursor["previous_sha256"] == (
            snapshot_target["sha256"] if snapshot_target["present"] else None
        )
        assert cursor["intended_sha256"] == hashlib.sha256(
            fixture["expected_bytes"][target]
        ).hexdigest()
        observed.append(target)

    monkeypatch.setattr(module, "_atomic_event_hook", inspect)
    module.install_host_configuration(context)

    assert tuple(observed) == TARGETS


@pytest.mark.parametrize(
    "fault_event",
    [
        "task7_after_target_replace",
        "task7_after_target_parent_fsync",
        "task7_after_daemon_reload",
        "task7_after_validation",
        "task7_after_runtime_lock_release",
        "task7_after_legacy_lock_release",
    ],
)
def test_install_failure_restores_every_prior_target_and_timer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault_event: str,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(
        module,
        tmp_path,
        absent_targets=(TARGETS[2], TARGETS[3]),
    )

    def fail(event: str, **_details: object) -> None:
        if event == fault_event:
            raise module.OperationStateError(f"controlled {fault_event}")

    monkeypatch.setattr(module, "_atomic_event_hook", fail)

    with pytest.raises(module.OperationStateError, match="controlled"):
        module.install_host_configuration(context)

    assert_task7_prior_targets_restored(context, fixture)
    assert fixture["runtime"]["timer_active"] == "active"
    assert fixture["runtime"]["timer_enabled"] == "enabled"
    state = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert state["phase"] == "rolled_back"
    assert state["install"]["installed_hashes"] == {}
    assert state["install"]["completed_epoch"] is None


@pytest.mark.parametrize(
    "fail_action",
    ["disable-timer", "stop-timer", "daemon-reload", "preflight"],
)
def test_each_guard_command_failure_recovers_targets_and_exact_timer_state(
    tmp_path: Path,
    fail_action: str,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(
        module,
        tmp_path,
        absent_targets=(TARGETS[2], TARGETS[3]),
    )
    fixture["controls"]["fail_command"] = fail_action

    with pytest.raises(module.OperationStateError):
        module.install_host_configuration(context)

    assert_task7_prior_targets_restored(context, fixture)
    assert fixture["runtime"]["timer_active"] == "active"
    assert fixture["runtime"]["timer_enabled"] == "enabled"
    state = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert state["phase"] == "rolled_back"


@pytest.mark.parametrize(
    "crash_event",
    [
        "task7_after_installing_state",
        "task7_after_runtime_directory_create",
        "task7_after_timer_disable",
        "task7_after_timer_stop",
        "task7_after_daemon_reload",
        "task7_after_validation",
        "task7_after_runtime_lock_release",
        "task7_after_legacy_lock_release",
        "task7_after_timer_enable",
        "task7_after_timer_start",
        "task7_after_timer_restore",
    ],
)
def test_crash_restart_recover_restores_from_each_guard_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crash_event: str,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(
        module,
        tmp_path,
        absent_targets=(TARGETS[2], TARGETS[3]),
    )

    def crash(event: str, **_details: object) -> None:
        if event == crash_event:
            raise SimulatedTask7Crash(crash_event)

    monkeypatch.setattr(module, "_atomic_event_hook", crash)
    with pytest.raises(SimulatedTask7Crash):
        module.install_host_configuration(context)

    resumed = load_ops_helper()
    configure_task7_test_lock_seam(resumed, context)
    resumed.recover_host_configuration(context)

    assert_task7_prior_targets_restored(context, fixture)
    state = resumed.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert state["phase"] == "rolled_back"
    assert state["install"]["installed_hashes"] == {}
    assert state["install"]["completed_epoch"] is None
    if crash_event == "task7_after_runtime_directory_create":
        assert state["install"]["runtime_directory_created"] is True
        assert state["recovery"]["runtime_directory_created"] is False
    if crash_event in {
        "task7_after_validation",
        "task7_after_runtime_lock_release",
        "task7_after_legacy_lock_release",
        "task7_after_timer_enable",
        "task7_after_timer_start",
        "task7_after_timer_restore",
    }:
        assert state["install"]["validated_epoch"] is not None
        assert len(state["install"]["validation_evidence_sha256"]) == 64


@pytest.mark.parametrize(
    "crash_event",
    [
        "task7_after_staged_file_fsync",
        "task7_after_target_replace",
        "task7_after_target_parent_fsync",
        "task7_after_cursor_state",
    ],
)
@pytest.mark.parametrize("target_index", range(len(TARGETS)))
def test_crash_restart_recover_restores_at_each_target_durable_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crash_event: str,
    target_index: int,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(
        module,
        tmp_path,
        absent_targets=(TARGETS[2], TARGETS[3]),
    )
    expected_index = target_index + 1 if crash_event == "task7_after_cursor_state" else target_index

    def crash(event: str, **details: object) -> None:
        if event == crash_event and details.get("index") == expected_index:
            raise SimulatedTask7Crash(f"{crash_event}:{target_index}")

    monkeypatch.setattr(module, "_atomic_event_hook", crash)
    with pytest.raises(SimulatedTask7Crash):
        module.install_host_configuration(context)

    resumed = load_ops_helper()
    configure_task7_test_lock_seam(resumed, context)
    resumed.recover_host_configuration(context)

    assert_task7_prior_targets_restored(context, fixture)
    state = resumed.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert state["phase"] == "rolled_back"
    assert state["install"]["installed_hashes"] == {}
    assert state["install"]["completed_epoch"] is None


def test_restart_recovery_removes_only_operation_owned_target_temporary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    target_parent = host_root_path(context.host_root, TARGETS[0]).parent
    unrelated = target_parent / ".degen-prod-db-backup.unrelated.tmp"
    write_private_host_file(unrelated, b"unrelated operator file\n", 0o600)
    captured: list[Path] = []

    def crash(event: str, **details: object) -> None:
        if event == "task7_after_staged_file_fsync" and details.get("index") == 0:
            captured.append(Path(str(details["temp_path"])))
            raise SimulatedTask7Crash("owned target temp fsynced")

    monkeypatch.setattr(module, "_atomic_event_hook", crash)
    with pytest.raises(SimulatedTask7Crash):
        module.install_host_configuration(context)

    assert len(captured) == 1
    assert captured[0].name == (
        f".{PurePosixPath(TARGETS[0]).name}.{context.operation_id}.install.tmp"
    )
    assert captured[0].exists()
    assert unrelated.read_bytes() == b"unrelated operator file\n"
    resumed = load_ops_helper()
    configure_task7_test_lock_seam(resumed, context)
    resumed.recover_host_configuration(context)

    assert not captured[0].exists()
    assert unrelated.read_bytes() == b"unrelated operator file\n"
    assert_task7_prior_targets_restored(context, fixture)


def test_install_refuses_preexisting_deterministic_target_temporary(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    target = host_root_path(context.host_root, TARGETS[0])
    temporary = target.with_name(
        f".{target.name}.{context.operation_id}.install.tmp"
    )
    write_private_host_file(temporary, b"unrelated preexisting bytes\n", 0o600)

    with pytest.raises(module.OperationStateError, match="temporary|exist|owned"):
        module.install_host_configuration(context)

    assert temporary.read_bytes() == b"unrelated preexisting bytes\n"
    assert_task7_prior_targets_restored(context, fixture)


def test_manual_rollback_restart_resumes_after_absence_unlink_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(
        module,
        tmp_path,
        absent_targets=(TARGETS[2],),
    )
    module.install_host_configuration(context)
    installed = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    frozen_install = copy.deepcopy(installed["install"])

    def crash(event: str, **details: object) -> None:
        if event == "task7_after_target_unlink" and details.get("target") == TARGETS[2]:
            raise SimulatedTask7Crash("absence unlink")

    monkeypatch.setattr(module, "_atomic_event_hook", crash)
    with pytest.raises(SimulatedTask7Crash):
        module.rollback_host_configuration(context)

    resumed = load_ops_helper()
    configure_task7_test_lock_seam(resumed, context)
    resumed.recover_host_configuration(context)
    state = resumed.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert state["phase"] == "rolled_back"
    assert state["install"] == frozen_install
    assert not host_root_path(context.host_root, TARGETS[2]).exists()
    assert_task7_prior_targets_restored(context, fixture)


@pytest.mark.parametrize("primitive", ["write", "fsync", "fchmod", "fchown"])
def test_ordinary_recovery_temp_failure_cleans_owned_temporary_and_resumes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    primitive: str,
) -> None:
    if primitive in {"fchmod", "fchown"} and os.name != "posix":
        pytest.skip("requires POSIX recovery metadata primitives")
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    module.install_host_configuration(context)
    target = host_root_path(context.host_root, TARGETS[0])
    temporary = target.with_name(
        f".{target.name}.{context.operation_id}.recovery.tmp"
    )
    armed = False
    failed = False

    def arm(event: str, **_details: object) -> None:
        nonlocal armed
        if event == "task7_after_recovery_state":
            armed = True

    monkeypatch.setattr(module, "_atomic_event_hook", arm)
    if primitive == "write":
        original = module._write_all

        def fail_write(descriptor: int, data: bytes) -> None:
            nonlocal failed
            if armed and not failed:
                failed = True
                os.write(descriptor, data[: min(32, len(data))])
                raise OSError("controlled partial recovery write failure")
            original(descriptor, data)

        monkeypatch.setattr(module, "_write_all", fail_write)
    elif primitive == "fsync":
        original = module.os.fsync
        prior_size = len(fixture["prior"][TARGETS[0]]["bytes"])

        def fail_fsync(descriptor: int) -> None:
            nonlocal failed
            metadata = os.fstat(descriptor)
            if (
                armed
                and not failed
                and stat.S_ISREG(metadata.st_mode)
                and metadata.st_size == prior_size
            ):
                failed = True
                raise OSError("controlled recovery fsync failure")
            original(descriptor)

        monkeypatch.setattr(module.os, "fsync", fail_fsync)
    elif primitive == "fchmod":
        original = module.os.fchmod
        prior_mode = int(fixture["prior"][TARGETS[0]]["mode"])

        def fail_fchmod(descriptor: int, mode: int) -> None:
            nonlocal failed
            if armed and not failed and mode == prior_mode:
                failed = True
                raise OSError("controlled recovery fchmod failure")
            original(descriptor, mode)

        monkeypatch.setattr(module.os, "fchmod", fail_fchmod)
    else:
        original = module.os.fchown

        def fail_fchown(descriptor: int, uid: int, gid: int) -> None:
            nonlocal failed
            if armed and not failed:
                failed = True
                raise OSError("controlled recovery fchown failure")
            original(descriptor, uid, gid)

        monkeypatch.setattr(module.os, "fchown", fail_fchown)

    with pytest.raises((module.OperationStateError, OSError), match="controlled"):
        module.rollback_host_configuration(context)

    assert failed is True
    assert not temporary.exists()
    module.recover_host_configuration(context)
    final = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert final["phase"] == "rolled_back"
    assert_task7_prior_targets_restored(context, fixture)


def test_recovery_temp_open_race_preserves_unowned_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    module.install_host_configuration(context)
    target = host_root_path(context.host_root, TARGETS[0])
    temporary = target.with_name(
        f".{target.name}.{context.operation_id}.recovery.tmp"
    )
    attacker = b"attacker recovery race file\n"
    original_open = module.os.open
    armed = False
    raced = False

    def arm(event: str, **_details: object) -> None:
        nonlocal armed
        if event == "task7_after_recovery_state":
            armed = True

    def racing_open(
        path: object,
        flags: int,
        *args: object,
        **kwargs: object,
    ) -> int:
        nonlocal raced
        if (
            armed
            and not raced
            and Path(path).name == temporary.name
            and flags & os.O_EXCL
        ):
            raced = True
            descriptor = original_open(path, flags, *args, **kwargs)
            try:
                os.write(descriptor, attacker)
            finally:
                os.close(descriptor)
        return original_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(module, "_atomic_event_hook", arm)
    monkeypatch.setattr(module.os, "open", racing_open)
    if os.name == "posix":
        monkeypatch.setattr(module, "_descriptor_primitives_available", lambda: True)
        monkeypatch.setattr(
            module,
            "_write_descriptor_primitives_available",
            lambda: True,
        )
        monkeypatch.setattr(module, "_require_posix_descriptor_primitives", lambda: None)

    with pytest.raises(module.OperationStateError, match="exist|race|temporary"):
        module.rollback_host_configuration(context)

    assert raced is True
    assert temporary.read_bytes() == attacker
    temporary.unlink()
    module.recover_host_configuration(context)
    final = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert final["phase"] == "rolled_back"
    assert_task7_prior_targets_restored(context, fixture)


def test_manual_rollback_binds_current_timer_and_pid_baseline(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    module.install_host_configuration(context)
    runtime = fixture["runtime"]
    runtime["timer_enabled"] = "disabled"
    runtime["timer_active"] = "inactive"
    runtime["timer_substate"] = "dead"
    runtime["service_pids"]["postgresql@15-main.service"] = 1200
    runtime["service_pids"]["degen-web.service"] = 1100
    runtime["service_pids"]["degen-worker.service"] = 1101
    runtime["bot_pids"]["degen"] = 1102
    baseline = module._capture_prior_runtime(context)

    module.rollback_host_configuration(context)

    state = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert state["phase"] == "rolled_back"
    assert state["recovery"]["runtime_baseline"] == baseline
    assert fixture["runtime"]["timer_enabled"] == "disabled"
    assert fixture["runtime"]["timer_active"] == "inactive"


def test_manual_rollback_runtime_baseline_is_durable_across_crash_resume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    module.install_host_configuration(context)
    runtime = fixture["runtime"]
    runtime["timer_enabled"] = "disabled"
    runtime["timer_active"] = "active"
    runtime["timer_substate"] = "waiting"
    runtime["service_pids"]["postgresql@15-main.service"] = 2200
    runtime["service_pids"]["degen-web.service"] = 2100
    runtime["service_pids"]["degen-worker.service"] = 2101
    runtime["bot_pids"]["degen"] = 2102
    baseline = module._capture_prior_runtime(context)

    def crash(event: str, **_details: object) -> None:
        if event == "task7_after_timer_stop":
            raise SimulatedTask7Crash("manual baseline crash")

    monkeypatch.setattr(module, "_atomic_event_hook", crash)
    with pytest.raises(SimulatedTask7Crash, match="baseline"):
        module.rollback_host_configuration(context)

    interrupted = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert interrupted["phase"] == "manual_rollback"
    assert interrupted["recovery"]["runtime_baseline"] == baseline

    resumed = load_ops_helper()
    configure_task7_test_lock_seam(resumed, context)
    resumed.recover_host_configuration(context)
    final = resumed.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert final["phase"] == "rolled_back"
    assert final["recovery"]["runtime_baseline"] == baseline
    assert fixture["runtime"]["timer_enabled"] == "disabled"
    assert fixture["runtime"]["timer_active"] == "active"


def test_timer_restore_primary_is_durable_before_requiesce_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    module.install_host_configuration(context)
    fixture["controls"]["fail_command"] = "start-timer"

    def crash(event: str, **_details: object) -> None:
        if event == "task7_after_timer_restore_failure_state":
            raise SimulatedTask7Crash("post-primary requiesce crash")

    monkeypatch.setattr(module, "_atomic_event_hook", crash)
    with pytest.raises(SimulatedTask7Crash, match="post-primary"):
        module.rollback_host_configuration(context)

    interrupted = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert interrupted["phase"] == "manual_rollback"
    assert interrupted["failure"] is not None
    assert "start" in interrupted["failure"]["primary_error"]
    assert interrupted["secondary_errors"] == []

    resumed = load_ops_helper()
    configure_task7_test_lock_seam(resumed, context)
    resumed.recover_host_configuration(context)
    final = resumed.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert final["phase"] == "rolled_back"
    assert final["failure"] == interrupted["failure"]


@pytest.mark.parametrize(
    "crash_event",
    [
        "task7_after_recovery_file_fsync",
        "task7_after_recovery_target_replace",
        "task7_after_recovery_target_parent_fsync",
        "task7_after_cursor_state",
    ],
)
@pytest.mark.parametrize("target_index", range(len(TARGETS)))
def test_manual_recovery_restart_at_each_present_target_durable_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crash_event: str,
    target_index: int,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    module.install_host_configuration(context)
    expected_index = target_index + 1 if crash_event == "task7_after_cursor_state" else target_index

    def crash(event: str, **details: object) -> None:
        if event == crash_event and details.get("index") == expected_index:
            raise SimulatedTask7Crash(f"{crash_event}:{target_index}")

    monkeypatch.setattr(module, "_atomic_event_hook", crash)
    with pytest.raises(SimulatedTask7Crash):
        module.rollback_host_configuration(context)

    resumed = load_ops_helper()
    configure_task7_test_lock_seam(resumed, context)
    resumed.recover_host_configuration(context)
    final = resumed.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert final["phase"] == "rolled_back"
    assert_task7_prior_targets_restored(context, fixture)


@pytest.mark.parametrize("target_index", range(len(TARGETS)))
@pytest.mark.parametrize(
    "crash_event",
    ["task7_after_target_unlink", "task7_after_recovery_target_parent_fsync"],
)
def test_manual_recovery_restart_after_each_absence_unlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target_index: int,
    crash_event: str,
) -> None:
    target = TARGETS[target_index]
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(
        module,
        tmp_path,
        absent_targets=(target,),
    )
    module.install_host_configuration(context)

    def crash(event: str, **details: object) -> None:
        if event == crash_event and details.get("target") == target:
            raise SimulatedTask7Crash(f"{crash_event}:{target_index}")

    monkeypatch.setattr(module, "_atomic_event_hook", crash)
    with pytest.raises(SimulatedTask7Crash):
        module.rollback_host_configuration(context)

    resumed = load_ops_helper()
    configure_task7_test_lock_seam(resumed, context)
    resumed.recover_host_configuration(context)
    final = resumed.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert final["phase"] == "rolled_back"
    assert_task7_prior_targets_restored(context, fixture)


def test_fresh_install_refuses_incomplete_state_with_exact_recover_command(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    task7_write_incomplete_install_state(module, context, fixture)
    expected = (
        str(
            context.paths.source_dir
            / "deploy/linux/degen-prod-db-backup-ops.py"
        )
        + " recover --operation-dir "
        + str(context.paths.operation_dir)
    )

    with pytest.raises(module.OperationStateError) as raised:
        module.install_host_configuration(context)

    assert expected in str(raised.value)
    assert fixture["calls"] == []


def test_fresh_install_refuses_recovery_required_state_with_source_helper_command(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    fixture["controls"]["fail_command"] = "start-timer"
    fixture["controls"]["fail_command_remaining"] = 2
    with pytest.raises(module.OperationStateError):
        module.install_host_configuration(context)
    fixture["controls"]["fail_command"] = None
    expected = (
        str(context.paths.source_dir / "deploy/linux/degen-prod-db-backup-ops.py")
        + " recover --operation-dir "
        + str(context.paths.operation_dir)
    )

    with pytest.raises(module.OperationStateError) as raised:
        module.install_host_configuration(context)

    assert expected in str(raised.value)


@pytest.mark.parametrize(
    ("fail_action", "timer_enabled", "timer_active"),
    [
        ("enable-timer", True, False),
        ("start-timer", False, True),
        ("start-timer", True, True),
    ],
)
def test_timer_restore_failure_enters_verified_durable_recovery_required(
    tmp_path: Path,
    fail_action: str,
    timer_enabled: bool,
    timer_active: bool,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(
        module,
        tmp_path,
        timer_enabled=timer_enabled,
        timer_active=timer_active,
    )
    fixture["controls"]["fail_command"] = fail_action
    fixture["controls"]["fail_command_remaining"] = 2

    with pytest.raises(module.OperationStateError):
        module.install_host_configuration(context)

    state = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert state["phase"] == "recovery_required"
    assert state["failure"] is not None
    assert state["recovery"]["kind"] == "install"
    assert state["recovery"]["completed_epoch"] is None
    assert "recovering" in [entry["phase"] for entry in state["phase_history"]]
    assert state["install"]["installed_hashes"] == {}
    assert state["install"]["completed_epoch"] is None
    assert fixture["runtime"]["timer_active"] == "inactive"


def test_recover_resume_preserves_install_receipt_and_recovery_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)

    def crash_install(event: str, **_details: object) -> None:
        if event == "task7_after_target_replace":
            raise SimulatedTask7Crash("install replacement")

    monkeypatch.setattr(module, "_atomic_event_hook", crash_install)
    with pytest.raises(SimulatedTask7Crash):
        module.install_host_configuration(context)

    recovering = load_ops_helper()
    configure_task7_test_lock_seam(recovering, context)

    def crash_recovery(event: str, **details: object) -> None:
        if event == "task7_after_cursor_state" and details.get("index") == 2:
            raise SimulatedTask7Crash("recovery cursor two")

    monkeypatch.setattr(recovering, "_atomic_event_hook", crash_recovery)
    with pytest.raises(SimulatedTask7Crash):
        recovering.recover_host_configuration(context)
    before = recovering.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    frozen_install = copy.deepcopy(before["install"])
    recovery_identity = copy.deepcopy(before["recovery"])

    resumed = load_ops_helper()
    configure_task7_test_lock_seam(resumed, context)
    resumed.recover_host_configuration(context)
    after = resumed.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )

    assert after["install"] == frozen_install
    assert after["recovery"]["kind"] == recovery_identity["kind"]
    assert after["recovery"]["started_epoch"] == recovery_identity["started_epoch"]
    assert after["recovery"]["evidence_sha256"] == recovery_identity["evidence_sha256"]


def test_recover_resume_preserves_provisional_restore_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    task7_write_incomplete_install_state(module, context, fixture)

    def crash(event: str, **_details: object) -> None:
        if event == "task7_after_recovery_validation_state":
            raise SimulatedTask7Crash("recovery validated")

    monkeypatch.setattr(module, "_atomic_event_hook", crash)
    with pytest.raises(SimulatedTask7Crash):
        module.recover_host_configuration(context)
    before = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    provisional = {
        "restored_epoch": before["recovery"]["restored_epoch"],
        "restore_evidence_sha256": before["recovery"]["restore_evidence_sha256"],
    }
    assert provisional["restored_epoch"] is not None
    assert len(provisional["restore_evidence_sha256"]) == 64

    resumed = load_ops_helper()
    configure_task7_test_lock_seam(resumed, context)
    resumed.recover_host_configuration(context)
    after = resumed.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert after["phase"] == "rolled_back"
    assert {
        "restored_epoch": after["recovery"]["restored_epoch"],
        "restore_evidence_sha256": after["recovery"]["restore_evidence_sha256"],
    } == provisional


@pytest.mark.parametrize("helper_target", [TARGETS[2], TARGETS[3]])
@pytest.mark.parametrize("prior_present", [False, True])
@pytest.mark.parametrize("flow", ["initial_recovery", "manual_rollback"])
def test_recovery_restores_new_helper_target_bytes_or_absence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    helper_target: str,
    prior_present: bool,
    flow: str,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(
        module,
        tmp_path,
        absent_targets=() if prior_present else (helper_target,),
    )

    if flow == "manual_rollback":
        module.install_host_configuration(context)
        assert host_root_path(context.host_root, helper_target).exists()
        module.rollback_host_configuration(context)
    else:
        failed = False

        def fail_after_validation(event: str, **_details: object) -> None:
            nonlocal failed
            if event == "task7_after_validation" and not failed:
                failed = True
                raise module.OperationStateError("controlled initial recovery")

        monkeypatch.setattr(module, "_atomic_event_hook", fail_after_validation)
        with pytest.raises(module.OperationStateError, match="controlled"):
            module.install_host_configuration(context)

    if prior_present:
        assert host_root_path(context.host_root, helper_target).read_bytes() == fixture[
            "prior"
        ][helper_target]["bytes"]
    else:
        assert not host_root_path(context.host_root, helper_target).exists()
    assert_task7_prior_targets_restored(context, fixture)


def test_manual_rollback_preserves_historical_install_receipt_and_skips_rclone(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    module.install_host_configuration(context)
    installed = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    frozen_install = copy.deepcopy(installed["install"])
    rclone_path = fixture["rclone_path"]
    changed_rclone = b"[onedrive]\ntype=onedrive\ntoken=ROTATED_AFTER_SNAPSHOT\n"
    write_private_host_file(rclone_path, changed_rclone, 0o600)

    module.rollback_host_configuration(context)

    rolled_back = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert rolled_back["phase"] == "rolled_back"
    assert rolled_back["install"] == frozen_install
    assert rolled_back["recovery"]["kind"] == "manual_rollback"
    assert rolled_back["recovery"]["restored_epoch"] is not None
    assert len(rolled_back["recovery"]["restore_evidence_sha256"]) == 64
    assert rolled_back["recovery"]["completed_epoch"] is not None
    assert rclone_path.read_bytes() == changed_rclone
    assert_task7_prior_targets_restored(context, fixture)


@pytest.mark.parametrize("identity", ["postgres", "web", "worker", "bot"])
def test_manual_rollback_pid_parity_failure_stays_quiesced_and_recoverable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    identity: str,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    module.install_host_configuration(context)
    installed = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    frozen_install = copy.deepcopy(installed["install"])

    def change_pid(event: str, **_details: object) -> None:
        if event != "task7_before_recovery_pid_validation":
            return
        if identity == "postgres":
            fixture["runtime"]["service_pids"]["postgresql@15-main.service"] = 211
        elif identity == "web":
            fixture["runtime"]["service_pids"]["degen-web.service"] = 311
        elif identity == "worker":
            fixture["runtime"]["service_pids"]["degen-worker.service"] = 321
        else:
            fixture["runtime"]["bot_pids"]["degen"] = 411

    monkeypatch.setattr(module, "_atomic_event_hook", change_pid)
    with pytest.raises(module.OperationStateError, match="PID|pid|process|identity"):
        module.rollback_host_configuration(context)

    state = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert state["phase"] == "recovery_required"
    assert state["install"] == frozen_install
    assert state["recovery"]["restored_epoch"] is None
    assert state["recovery"]["restore_evidence_sha256"] is None
    assert fixture["runtime"]["timer_enabled"] == "disabled"
    assert fixture["runtime"]["timer_active"] == "inactive"
    assert_task7_prior_targets_restored(context, fixture)


def test_primary_failure_is_immutable_and_lock_errors_append_in_exact_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    primary_raised = False
    secondary_raised: set[str] = set()

    def fail_in_order(event: str, **_details: object) -> None:
        nonlocal primary_raised
        if event == "task7_after_validation" and not primary_raised:
            primary_raised = True
            raise module.OperationStateError("controlled primary install failure")
        failures = {
            "task7_before_runtime_lock_release": (
                "runtime",
                "controlled runtime lock release failure",
            ),
            "task7_before_legacy_lock_release": (
                "legacy",
                "controlled legacy lock release failure",
            ),
        }
        if event in failures:
            kind, message = failures[event]
            if kind not in secondary_raised:
                secondary_raised.add(kind)
                raise module.OperationStateError(message)

    monkeypatch.setattr(module, "_atomic_event_hook", fail_in_order)
    with pytest.raises(module.OperationStateError, match="primary"):
        module.install_host_configuration(context)
    state = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )

    assert state["phase"] == "rolled_back"
    assert state["failure"]["phase"] == "installing"
    assert state["failure"]["primary_error"] == "controlled primary install failure"
    assert state["failure"]["evidence_sha256"] == task7_error_evidence_sha256(
        "primary",
        state["failure"],
    )
    assert [error["stage"] for error in state["secondary_errors"][:2]] == [
        "release_runtime_lock",
        "release_legacy_lock",
    ]
    assert [error["error"] for error in state["secondary_errors"][:2]] == [
        "controlled runtime lock release failure",
        "controlled legacy lock release failure",
    ]
    epochs = [error["epoch"] for error in state["secondary_errors"]]
    assert epochs == sorted(epochs)
    for error in state["secondary_errors"]:
        assert error["evidence_sha256"] == task7_error_evidence_sha256(
            "secondary",
            error,
        )
    assert fixture["runtime"]["timer_enabled"] == "enabled"
    assert fixture["runtime"]["timer_active"] == "active"


@pytest.mark.parametrize(
    ("primitive", "stage"),
    [
        ("flock", "release_runtime_lock"),
        ("close", "release_runtime_lock"),
    ],
)
def test_existing_primary_plus_os_lock_release_uncertainty_blocks_recovery_terminal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    primitive: str,
    stage: str,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    original_release = module._release_migration_locks
    original_close = module.os.close
    release_cycles = 0
    failed = False
    leaked_descriptors: list[int] = []

    def inject_second_release_uncertainty(locks: object) -> list[object]:
        nonlocal release_cycles
        release_cycles += 1
        if release_cycles != 2:
            return list(original_release(locks))
        message = f"controlled {primitive} primitive release uncertainty"
        if os.name != "posix":
            issues = list(original_release(locks))
            issues.append((stage, OSError(message)))
            return issues
        if primitive == "flock":
            import fcntl

            original_flock = fcntl.flock
            injected = False

            def fail_runtime_unlock(descriptor: int, operation: int) -> object:
                nonlocal injected
                if (
                    not injected
                    and descriptor == locks.runtime_fd
                    and operation == fcntl.LOCK_UN
                ):
                    injected = True
                    raise OSError(message)
                return original_flock(descriptor, operation)

            with monkeypatch.context() as release_patch:
                release_patch.setattr(fcntl, "flock", fail_runtime_unlock)
                return list(original_release(locks))
        injected = False

        def fail_runtime_close(descriptor: int) -> None:
            nonlocal injected
            if not injected and descriptor == locks.runtime_fd:
                injected = True
                leaked_descriptors.append(descriptor)
                raise OSError(message)
            original_close(descriptor)

        with monkeypatch.context() as release_patch:
            release_patch.setattr(module.os, "close", fail_runtime_close)
            return list(original_release(locks))

    def fail_after_validation(event: str, **_details: object) -> None:
        nonlocal failed
        if event == "task7_after_validation" and not failed:
            failed = True
            raise module.OperationStateError("controlled immutable primary")

    monkeypatch.setattr(
        module,
        "_release_migration_locks",
        inject_second_release_uncertainty,
    )
    monkeypatch.setattr(module, "_atomic_event_hook", fail_after_validation)

    try:
        with pytest.raises(module.OperationStateError, match="immutable primary"):
            module.install_host_configuration(context)
    finally:
        for descriptor in leaked_descriptors:
            original_close(descriptor)

    state = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert release_cycles == 2
    assert state["failure"]["primary_error"] == "controlled immutable primary"
    assert state["phase"] == "recovery_required"
    assert state["phase_history"][-1]["phase"] == "recovery_required"
    assert state["recovery"]["completed_epoch"] is None
    assert state["secondary_errors"][-1]["stage"] == stage
    assert primitive in state["secondary_errors"][-1]["error"]
    assert fixture["runtime"]["timer_enabled"] == "disabled"
    assert fixture["runtime"]["timer_active"] == "inactive"


def test_timer_restore_error_appends_without_replacing_existing_primary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    fixture["controls"]["fail_command"] = "start-timer"
    failed = False

    def fail_install(event: str, **_details: object) -> None:
        nonlocal failed
        if event == "task7_after_target_replace" and not failed:
            failed = True
            raise module.OperationStateError("original target replacement failure")

    monkeypatch.setattr(module, "_atomic_event_hook", fail_install)
    with pytest.raises(module.OperationStateError, match="original"):
        module.install_host_configuration(context)
    state = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )

    assert state["phase"] == "recovery_required"
    assert state["failure"]["primary_error"] == "original target replacement failure"
    assert any(
        error["stage"] == "timer_restore"
        and "controlled failure" in error["error"]
        for error in state["secondary_errors"]
    )


def test_failure_receipts_preserve_first_error_and_redact_secondary_secrets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    failures = 0
    primary_raised = False

    def fail_recovery(event: str, **_details: object) -> None:
        nonlocal failures, primary_raised
        if event == "task7_after_target_replace" and not primary_raised:
            primary_raised = True
            raise module.OperationStateError(
                "TOKEN=PRIMARY_SECRET_SENTINEL controlled install failure"
            )
        if event == "task7_before_recovery_target":
            failures += 1
            raise module.OperationStateError(
                "PASSWORD=SECONDARY_SECRET_SENTINEL recovery failed"
            )

    monkeypatch.setattr(module, "_atomic_event_hook", fail_recovery)
    with pytest.raises(module.OperationStateError):
        module.install_host_configuration(context)
    state = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )

    assert failures == 1
    assert state["failure"] is not None
    primary = state["failure"]["primary_error"]
    assert "PRIMARY_SECRET_SENTINEL" not in primary
    assert "TOKEN" not in primary
    assert "SECONDARY_SECRET_SENTINEL" not in primary
    assert len(state["secondary_errors"]) >= 1
    observable = json.dumps(state["secondary_errors"], sort_keys=True)
    assert "SECONDARY_SECRET_SENTINEL" not in observable
    assert "PASSWORD" not in observable


@pytest.mark.parametrize(
    "boundary",
    [
        "task7_after_legacy_lock",
        "task7_after_runtime_lock",
        "task7_after_target_replace",
        "task7_before_lock_release",
        "task7_after_runtime_lock_release",
    ],
)
@pytest.mark.skipif(os.name != "posix", reason="requires POSIX flock semantics")
def test_real_old_runtime_contender_is_excluded_at_every_guard_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    import fcntl

    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    legacy_path = host_root_path(
        context.host_root,
        "/run/lock/degen-prod-db-backup.lock",
    )
    runtime_path = host_root_path(
        context.host_root,
        "/run/degen-prod-db-backup/backup.lock",
    )
    observed = False

    def expect_lock(path: Path, *, contended: bool) -> None:
        descriptor = os.open(path, os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            if contended:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            else:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)

    def contend(event: str, **_details: object) -> None:
        nonlocal observed
        if event != boundary or observed:
            return
        observed = True
        expect_lock(legacy_path, contended=True)
        if boundary == "task7_after_runtime_lock_release":
            expect_lock(runtime_path, contended=False)
        elif boundary != "task7_after_legacy_lock":
            expect_lock(runtime_path, contended=True)

    monkeypatch.setattr(module, "_atomic_event_hook", contend)
    module.install_host_configuration(context)

    assert observed is True
    state = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert state["phase"] == "installed"
    assert fixture["runtime"]["timer_active"] == "active"


@pytest.mark.skipif(os.name != "posix", reason="requires real dual flock guard")
def test_backup_service_inactivity_is_rechecked_while_both_locks_are_held(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import fcntl

    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)

    def activate(event: str, **details: object) -> None:
        if event == "migration_lock_acquired" and details.get("kind") == "runtime":
            for path in (
                host_root_path(
                    context.host_root,
                    "/run/lock/degen-prod-db-backup.lock",
                ),
                host_root_path(
                    context.host_root,
                    "/run/degen-prod-db-backup/backup.lock",
                ),
            ):
                descriptor = os.open(path, os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC)
                try:
                    with pytest.raises(BlockingIOError):
                        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                finally:
                    os.close(descriptor)
            fixture["runtime"]["service_pids"]["degen-prod-db-backup.service"] = 999

    monkeypatch.setattr(module, "_atomic_event_hook", activate)

    with pytest.raises(module.OperationStateError, match="service|inactive|pid"):
        module.install_host_configuration(context)

    assert_task7_prior_targets_restored(context, fixture)


@pytest.mark.parametrize("flow", ["install", "recover", "manual_rollback"])
def test_dual_guard_reacquires_and_reverse_releases_before_timer_restore(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    flow: str,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    if flow == "manual_rollback":
        module.install_host_configuration(context)
        fixture["events"].clear()
        fixture["calls"].clear()
    elif flow == "recover":
        task7_write_incomplete_install_state(module, context, fixture)
    acquired: list[str] = []
    released: list[str] = []

    def record(event: str, **details: object) -> None:
        if event == "migration_lock_acquired":
            kind = str(details["kind"])
            acquired.append(kind)
            fixture["events"].append(f"acquire-{kind}")
        elif event == "migration_lock_released":
            kind = str(details["kind"])
            released.append(kind)
            fixture["events"].append(f"release-{kind}")

    monkeypatch.setattr(module, "_atomic_event_hook", record)
    if flow == "install":
        module.install_host_configuration(context)
    elif flow == "recover":
        module.recover_host_configuration(context)
    else:
        module.rollback_host_configuration(context)

    assert acquired == ["legacy", "runtime"]
    assert released == ["runtime", "legacy"]
    persistent_restore = [
        action
        for action in ("enable-timer", "start-timer")
        if action in fixture["events"]
    ]
    for action in persistent_restore:
        assert fixture["events"].index("release-legacy") < fixture["events"].index(action)


def test_recovery_state_does_not_start_until_both_migration_locks_are_owned(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    task7_write_incomplete_install_state(module, context, fixture)
    observations: list[tuple[str, str]] = []

    def inspect(event: str, **details: object) -> None:
        if event == "migration_lock_acquired":
            state = module.load_operation_state(
                context.paths.state_file,
                effective_uid=context.effective_uid,
            )
            observations.append((str(details["kind"]), str(state["phase"])))
        elif event == "task7_after_recovery_state":
            state = module.load_operation_state(
                context.paths.state_file,
                effective_uid=context.effective_uid,
            )
            observations.append(("recovery-state", str(state["phase"])))

    monkeypatch.setattr(module, "_atomic_event_hook", inspect)
    module.recover_host_configuration(context)

    assert observations[:3] == [
        ("legacy", "installing"),
        ("runtime", "installing"),
        ("recovery-state", "recovering"),
    ]


def test_operation_transaction_lock_serializes_final_timer_and_state_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, _fixture = task7_transaction_fixture(module, tmp_path)
    paused = threading.Event()
    release = threading.Event()
    install_errors: list[BaseException] = []
    recover_errors: list[BaseException] = []

    def pause_after_unlock(event: str, **_details: object) -> None:
        if (
            event == "task7_after_legacy_lock_release"
            and threading.current_thread().name == "task7-install"
        ):
            if os.name == "posix":
                import fcntl

                descriptor = os.open(
                    context.paths.operation_dir,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC,
                )
                try:
                    with pytest.raises(BlockingIOError):
                        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                finally:
                    os.close(descriptor)
            paused.set()
            assert release.wait(timeout=5)

    monkeypatch.setattr(module, "_atomic_event_hook", pause_after_unlock)

    def install() -> None:
        try:
            module.install_host_configuration(context)
        except BaseException as exc:
            install_errors.append(exc)

    def recover() -> None:
        try:
            module.recover_host_configuration(context)
        except BaseException as exc:
            recover_errors.append(exc)

    install_thread = threading.Thread(target=install, name="task7-install")
    recover_thread = threading.Thread(target=recover, name="task7-recover")
    install_thread.start()
    assert paused.wait(timeout=5)
    recover_thread.start()
    try:
        recover_thread.join(timeout=0.25)
        mid_state = module.load_operation_state(
            context.paths.state_file,
            effective_uid=context.effective_uid,
        )
        assert mid_state["phase"] == "installing"
        assert "recovering" not in [
            entry["phase"] for entry in mid_state["phase_history"]
        ]
    finally:
        release.set()
    install_thread.join(timeout=5)
    recover_thread.join(timeout=5)

    assert not install_thread.is_alive()
    assert not recover_thread.is_alive()
    assert install_errors == []
    assert len(recover_errors) == 1
    final = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    assert final["phase"] == "installed"


@pytest.mark.parametrize(
    ("command", "phase", "api_name"),
    [
        ("install", "snapshotted", "install_host_configuration"),
        ("recover", "installing", "recover_host_configuration"),
        ("rollback", "installed", "rollback_host_configuration"),
    ],
)
def test_task7_cli_has_only_operation_dir_and_uses_verified_source_context(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    command: str,
    phase: str,
    api_name: str,
) -> None:
    module = load_ops_helper()
    operation_dir_raw = "/opt/degen/backups/config/20260701T123456Z"
    operation_dir = Path(operation_dir_raw)
    state = state_at_phase(operation_dir, phase)
    state["install"] = copy.deepcopy(state["install"])
    if state["install"] is not None:
        state["install"].update(
            {
                "runtime_directory_created": False,
                "validated_epoch": (
                    1_750_000_039 if phase == "installed" else None
                ),
                "validation_evidence_sha256": HASH_B if phase == "installed" else None,
            }
        )
    observed: list[object] = []
    source_checks: list[object] = []
    monkeypatch.setattr(module, "_effective_uid", lambda: 0)
    monkeypatch.setattr(module, "_require_posix_descriptor_primitives", lambda: None)
    monkeypatch.setattr(module, "load_operation_state", lambda path, *, effective_uid: state)
    if os.name != "posix":
        monkeypatch.setattr(module, "validate_operation_state_for_context", lambda *_: None)
    monkeypatch.setattr(
        module,
        "verify_running_source_helper",
        lambda context: source_checks.append(context),
        raising=False,
    )
    monkeypatch.setattr(
        module,
        api_name,
        lambda context: observed.append(context) or {},
        raising=False,
    )

    help_result = subprocess.run(
        [sys.executable, str(OPS_HELPER), command, "--help"],
        text=True,
        capture_output=True,
        check=False,
    )
    result = module.main([command, "--operation-dir", operation_dir_raw])

    captured = capsys.readouterr()
    assert help_result.returncode == 0
    assert "--operation-dir" in help_result.stdout
    for forbidden in ("host-root", "archive", "digest", "commit", "runner"):
        assert forbidden not in help_result.stdout.lower()
    assert result == 0
    assert captured.out == ""
    assert captured.err == ""
    assert len(observed) == 1
    assert source_checks == observed
    assert observed[0].host_root == Path("/")
    assert observed[0].paths == module.build_operation_paths(operation_dir)


def test_recover_refuses_running_helper_outside_verified_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, _fixture = task7_transaction_fixture(module, tmp_path)
    verified_helper = (
        context.paths.source_dir / "deploy/linux/degen-prod-db-backup-ops.py"
    )
    installed_helper = host_root_path(
        context.host_root,
        "/usr/local/sbin/degen-prod-db-backup-ops",
    )

    monkeypatch.setattr(module, "__file__", str(verified_helper))
    module.verify_running_source_helper(context)
    monkeypatch.setattr(module, "__file__", str(installed_helper))
    with pytest.raises(module.OperationStateError, match="verified|source|helper"):
        module.verify_running_source_helper(context)


@pytest.mark.parametrize("tamper", ["hash", "inode"])
def test_verified_source_helper_proof_rejects_hash_or_inode_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    tamper: str,
) -> None:
    if tamper == "inode" and os.name != "posix":
        pytest.skip("requires POSIX descriptor/path identity")
    module = load_ops_helper()
    context, _fixture = task7_transaction_fixture(module, tmp_path)
    helper = context.paths.source_dir / "deploy/linux/degen-prod-db-backup-ops.py"
    monkeypatch.setattr(module, "__file__", str(helper))
    if tamper == "hash":
        helper.write_bytes(helper.read_bytes() + b"tampered helper\n")
    else:
        original_open = module.os.open
        raced = False

        def racing_open(path: object, flags: int, *args: object, **kwargs: object) -> int:
            nonlocal raced
            descriptor = original_open(path, flags, *args, **kwargs)
            if not raced and Path(path) == helper:
                raced = True
                replacement = helper.with_name("helper-replacement")
                replacement.write_bytes(helper.read_bytes())
                replacement.chmod(helper.stat().st_mode)
                os.replace(replacement, helper)
            return descriptor

        monkeypatch.setattr(module.os, "open", racing_open)

    with pytest.raises(module.OperationStateError, match="verified|source|helper|changed"):
        module.verify_running_source_helper(context)


def task8_pending_rclone_group(
    active_kind: str,
    started_epoch: int,
    group_ordinal: int,
    *,
    attempt_ordinal: int = 0,
    purpose: str = "remote-list",
) -> dict[str, object]:
    if active_kind == "observe" and purpose == "remote-list":
        purpose = TASK8_OBSERVE_PURPOSES[group_ordinal]
    return task8_rclone_group(
        active_kind,
        started_epoch,
        attempt_ordinal,
        group_ordinal,
        pending=True,
        purpose=purpose,
    )


def task8_policy_origin_manual_rollback_state(
    operation_dir: Path,
    origin_phase: str,
) -> dict[str, object]:
    state = state_at_phase(operation_dir, origin_phase)
    history = state["phase_history"]
    assert isinstance(history, list)
    started_epoch = int(history[-1]["epoch"]) + 10
    append_phase(state, "manual_rollback", started_epoch)
    policy = state["policy"]
    snapshot = state["snapshot"]
    assert isinstance(policy, dict) and isinstance(snapshot, dict)
    target = TARGETS[-1]
    state["recovery"] = recovery_receipt(
        "manual_rollback",
        index=len(TARGETS) - 1,
        current_target=target,
        previous_sha256=str(policy["environment_sha256"]),
        intended_sha256=str(snapshot["targets"][target]["sha256"]),
        started_epoch=started_epoch,
    )
    return state


def task8_policy_enabled_runtime_state(
    module: object,
    context: object,
) -> tuple[dict[str, object], Path, bytes, bytes]:
    state = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    history = state["phase_history"]
    assert isinstance(history, list)
    epoch = int(history[-1]["epoch"])
    append_phase(state, "probing", epoch + 1)
    append_phase(state, "probed", epoch + 2)
    state["probe"] = {
        "prefix": SAFE_PROBE_PREFIX,
        "owned_names": ["probe.dump", "probe.dump.sha256"],
        "cleanup_proven": True,
        "evidence_sha256": HASH_A,
    }
    append_phase(state, "dry_run_recording", epoch + 3)
    append_phase(state, "dry_run_recorded", epoch + 4)
    state["dry_run"] = {
        "inventory_names": ["a.dump", "a.dump.sha256"],
        "casefold_names": ["a.dump", "a.dump.sha256"],
        "keep_names": ["a.dump", "a.dump.sha256"],
        "protected_names": [],
        "delete_names": [],
        "candidate_sha256": HASH_A,
        "evidence_sha256": HASH_B,
    }
    environment = host_root_path(context.host_root, TARGETS[-1])
    disabled_bytes = environment.read_bytes()
    enabled_bytes = task8_enabled_environment_bytes(disabled_bytes)
    enabled_sha256 = hashlib.sha256(enabled_bytes).hexdigest()
    assert state["host_stage"]["enabled_environment_sha256"] == enabled_sha256
    append_phase(state, "policy_enabling", epoch + 5)
    policy_entry = history[-1]
    assert isinstance(policy_entry, dict)
    policy_entry["evidence_sha256"] = task8_policy_entry_evidence(
        state,
        policy_entry,
        runtime_baseline=recovery_runtime_baseline(),
    )
    policy_group = task8_successful_policy_group(
        state["dry_run"],
        started_epoch=epoch + 5,
    )
    state["rclone_evidence_groups"].append(policy_group)
    state["policy"] = task8_policy_receipt(
        state,
        policy_entry,
        policy_group,
        runtime_baseline=recovery_runtime_baseline(),
        environment_sha256=enabled_sha256,
        uid=environment.stat().st_uid,
        gid=environment.stat().st_gid,
    )
    append_phase(state, "policy_enabled", epoch + 6)
    history[-1]["evidence_sha256"] = task8_policy_completion_evidence(
        entry=policy_entry,
        completed_epoch=epoch + 6,
        policy=state["policy"],
        dry_run=state["dry_run"],
        group=policy_group,
    )
    return state, environment, disabled_bytes, enabled_bytes


@pytest.fixture
def task8_installed_helper_fixture(tmp_path: Path):
    module = load_ops_helper()
    context, _fixture = task7_transaction_fixture(module, tmp_path)
    module.install_host_configuration(context)
    installed_helper = host_root_path(
        context.host_root,
        "/usr/local/sbin/degen-prod-db-backup-ops",
    )
    return module, context, installed_helper


def test_task8_rclone_force_checkpoint_replaces_and_fsyncs_identical_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    state = source_verified_state(operation_dir)
    state_file = write_state_file(operation_dir, state)
    events: list[str] = []
    monkeypatch.setattr(
        module,
        "_atomic_event_hook",
        lambda event, **_details: events.append(event),
    )

    module._atomic_write_operation_state_internal(
        state_file,
        copy.deepcopy(state),
        effective_uid=uid,
        pre_replace_validator=None,
        force_checkpoint=True,
    )

    assert events.count("before_replace") == 1
    assert events.count("after_temp_fsync") == 1
    assert events.count("after_replace") == 1
    assert events.count("after_parent_fsync") == 1
    assert module.load_operation_state(state_file, effective_uid=uid) == state


def test_task8_rclone_task7_writer_forwards_force_checkpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    state = source_verified_state(operation_dir)
    state_file = write_state_file(operation_dir, state)
    context = types.SimpleNamespace(
        paths=types.SimpleNamespace(state_file=state_file),
        effective_uid=uid,
    )
    binding = object()
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    monkeypatch.setattr(
        module,
        "_atomic_write_operation_state_internal",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )

    module._task7_write_state(
        context,
        binding,
        state,
        force_checkpoint=True,
    )

    assert len(calls) == 1
    args, kwargs = calls[0]
    assert args == (state_file, state)
    assert kwargs["effective_uid"] == uid
    assert kwargs["operation_directory_binding"] is binding
    assert kwargs["operation_lock_held"] is True
    assert kwargs["force_checkpoint"] is True


def test_task8_probe_remote_installed_helper_accepts_exact_installed_provenance(
    task8_installed_helper_fixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, context, installed_helper = task8_installed_helper_fixture
    verifier = getattr(module, "verify_running_installed_helper", None)
    assert callable(verifier), "Task 8 installed-helper proof is missing"
    monkeypatch.setattr(module, "__file__", str(installed_helper))

    verifier(context)


def test_task8_probe_remote_installed_helper_rejects_verified_source_path(
    task8_installed_helper_fixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, context, _installed_helper = task8_installed_helper_fixture
    verifier = getattr(module, "verify_running_installed_helper", None)
    assert callable(verifier), "Task 8 installed-helper proof is missing"
    source_helper = context.paths.source_dir / "deploy/linux/degen-prod-db-backup-ops.py"
    monkeypatch.setattr(module, "__file__", str(source_helper))

    with pytest.raises(module.OperationStateError, match="installed|helper|path"):
        verifier(context)


def test_task8_probe_remote_installed_helper_rejects_hash_tamper(
    task8_installed_helper_fixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, context, installed_helper = task8_installed_helper_fixture
    verifier = getattr(module, "verify_running_installed_helper", None)
    assert callable(verifier), "Task 8 installed-helper proof is missing"
    installed_helper.write_bytes(installed_helper.read_bytes() + b"tampered\n")
    monkeypatch.setattr(module, "__file__", str(installed_helper))

    with pytest.raises(module.OperationStateError, match="installed|helper|hash|changed"):
        verifier(context)


@pytest.mark.parametrize("metadata_tamper", ["mode", "hardlink", "symlink"])
def test_task8_probe_remote_installed_helper_rejects_unsafe_metadata(
    task8_installed_helper_fixture,
    monkeypatch: pytest.MonkeyPatch,
    metadata_tamper: str,
) -> None:
    module, context, installed_helper = task8_installed_helper_fixture
    if metadata_tamper == "mode":
        if os.name != "posix":
            pytest.skip("exact executable mode requires POSIX metadata")
        installed_helper.chmod(0o700)
    elif metadata_tamper == "hardlink":
        sibling = installed_helper.with_name(installed_helper.name + ".hardlink")
        try:
            os.link(installed_helper, sibling)
        except OSError as exc:
            pytest.skip(f"hard links unavailable: {exc}")
    else:
        original = installed_helper.with_name(installed_helper.name + ".original")
        installed_helper.rename(original)
        try:
            installed_helper.symlink_to(original.name)
        except OSError as exc:
            original.rename(installed_helper)
            pytest.skip(f"file symlinks unavailable: {exc}")
    verifier = getattr(module, "verify_running_installed_helper", None)
    assert callable(verifier), "Task 8 installed-helper proof is missing"
    monkeypatch.setattr(module, "__file__", str(installed_helper))

    with pytest.raises(
        module.OperationStateError,
        match="installed|helper|mode|link|regular|metadata|unsafe",
    ):
        verifier(context)


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX descriptor/path identity")
def test_task8_probe_remote_installed_helper_rejects_descriptor_path_inode_swap(
    task8_installed_helper_fixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, context, installed_helper = task8_installed_helper_fixture
    verifier = getattr(module, "verify_running_installed_helper", None)
    assert callable(verifier), "Task 8 installed-helper proof is missing"
    payload = installed_helper.read_bytes()
    original_open = module.os.open
    raced = False

    def racing_open(path: object, flags: int, *args: object, **kwargs: object) -> int:
        nonlocal raced
        descriptor = original_open(path, flags, *args, **kwargs)
        candidate = Path(os.fspath(path))
        if not raced and (
            candidate == installed_helper or candidate.name == installed_helper.name
        ):
            raced = True
            replacement = installed_helper.with_name(installed_helper.name + ".replacement")
            replacement.write_bytes(payload)
            replacement.chmod(0o755)
            os.replace(replacement, installed_helper)
        return descriptor

    monkeypatch.setattr(module, "__file__", str(installed_helper))
    monkeypatch.setattr(module.os, "open", racing_open)

    with pytest.raises(module.OperationStateError, match="installed|helper|binding|changed"):
        verifier(context)
    assert raced


@pytest.mark.skipif(
    os.name != "posix" or not hasattr(os, "geteuid") or os.geteuid() != 0,
    reason="requires root POSIX ownership mutation",
)
def test_task8_probe_remote_installed_helper_requires_root_owner(
    task8_installed_helper_fixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, context, installed_helper = task8_installed_helper_fixture
    verifier = getattr(module, "verify_running_installed_helper", None)
    assert callable(verifier), "Task 8 installed-helper proof is missing"
    os.chown(installed_helper, 1, installed_helper.stat().st_gid)
    monkeypatch.setattr(module, "__file__", str(installed_helper))

    with pytest.raises(module.OperationStateError, match="installed|helper|owner|root"):
        verifier(context)


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX parent symlink semantics")
def test_task8_probe_remote_installed_helper_rejects_unsafe_parent_symlink(
    task8_installed_helper_fixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, context, installed_helper = task8_installed_helper_fixture
    verifier = getattr(module, "verify_running_installed_helper", None)
    assert callable(verifier), "Task 8 installed-helper proof is missing"
    parent = installed_helper.parent
    real_parent = parent.with_name(parent.name + "-real")
    parent.rename(real_parent)
    parent.symlink_to(real_parent.name, target_is_directory=True)
    monkeypatch.setattr(module, "__file__", str(installed_helper))

    with pytest.raises(
        module.OperationStateError,
        match="installed|helper|parent|symlink|unsafe|directory",
    ):
        verifier(context)


@pytest.mark.parametrize(
    ("command", "phase", "handler_name"),
    [
        ("probe-remote", "installed", "probe_remote_storage"),
        ("record-dry-run", "probed", "record_remote_dry_run"),
        ("enable-prune", "dry_run_recorded", "enable_remote_prune"),
        ("observe", "policy_enabled", "observe_scheduled_backup"),
    ],
)
def test_task8_probe_dry_run_enable_observe_cli_routes_only_through_installed_helper(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    command: str,
    phase: str,
    handler_name: str,
) -> None:
    module = load_ops_helper()
    operation_dir_raw = "/opt/degen/backups/config/20260630T235959Z"
    operation_dir = Path(operation_dir_raw)
    state = state_at_phase(operation_dir, phase)
    events: list[tuple[str, object]] = []
    monkeypatch.setattr(module, "_effective_uid", lambda: 0)
    monkeypatch.setattr(module, "_require_posix_descriptor_primitives", lambda: None)
    monkeypatch.setattr(
        module,
        "load_operation_state",
        lambda path, *, effective_uid: state,
    )
    if os.name != "posix":
        monkeypatch.setattr(module, "validate_operation_state_for_context", lambda *_: None)
    monkeypatch.setattr(
        module,
        "verify_running_source_helper",
        lambda _context: (_ for _ in ()).throw(
            AssertionError("Task 8 command used the verified source helper")
        ),
    )
    monkeypatch.setattr(
        module,
        "verify_running_installed_helper",
        lambda context: events.append(("verify", context)),
        raising=False,
    )
    monkeypatch.setattr(
        module,
        handler_name,
        lambda context: events.append(("handler", context)) or {},
        raising=False,
    )
    parser = module._build_parser()
    command_action = next(
        action for action in parser._actions if action.dest == "command"
    )
    assert command_action.choices is not None
    assert command in command_action.choices, f"Task 8 CLI command {command!r} is missing"

    result = module.main([command, "--operation-dir", operation_dir_raw])

    captured = capsys.readouterr()
    assert result == 0
    assert captured.out == ""
    assert captured.err == ""
    assert [label for label, _context in events] == ["verify", "handler"]
    assert events[0][1] is events[1][1]
    assert events[1][1].paths == module.build_operation_paths(operation_dir)
    assert events[1][1].host_root == Path("/")


@pytest.mark.parametrize(
    ("command", "phase", "handler_name"),
    [
        ("probe-remote", "installed", "probe_remote_storage"),
        ("record-dry-run", "probed", "record_remote_dry_run"),
        ("enable-prune", "dry_run_recorded", "enable_remote_prune"),
        ("observe", "policy_enabled", "observe_scheduled_backup"),
    ],
)
def test_task8_probe_dry_run_enable_observe_cli_verifier_failure_blocks_handler(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    command: str,
    phase: str,
    handler_name: str,
) -> None:
    module = load_ops_helper()
    operation_dir_raw = "/opt/degen/backups/config/20260630T235959Z"
    operation_dir = Path(operation_dir_raw)
    state = state_at_phase(operation_dir, phase)
    events: list[str] = []
    monkeypatch.setattr(module, "_effective_uid", lambda: 0)
    monkeypatch.setattr(module, "_require_posix_descriptor_primitives", lambda: None)
    monkeypatch.setattr(
        module,
        "load_operation_state",
        lambda path, *, effective_uid: state,
    )
    if os.name != "posix":
        monkeypatch.setattr(module, "validate_operation_state_for_context", lambda *_: None)

    def refuse_installed_helper(_context: object) -> None:
        events.append("verify")
        raise module.OperationStateError("installed helper verification refused")

    monkeypatch.setattr(
        module,
        "verify_running_installed_helper",
        refuse_installed_helper,
        raising=False,
    )
    monkeypatch.setattr(
        module,
        handler_name,
        lambda _context: events.append("handler") or {},
        raising=False,
    )
    parser = module._build_parser()
    command_action = next(
        action for action in parser._actions if action.dest == "command"
    )
    assert command_action.choices is not None
    assert command in command_action.choices, f"Task 8 CLI command {command!r} is missing"

    result = module.main([command, "--operation-dir", operation_dir_raw])

    captured = capsys.readouterr()
    assert result == 1
    assert captured.out == ""
    assert "installed helper verification refused" in captured.err
    assert events == ["verify"]


def test_task8_probe_cli_recover_remains_verified_source_only(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = load_ops_helper()
    operation_dir_raw = "/opt/degen/backups/config/20260701T123456Z"
    operation_dir = Path(operation_dir_raw)
    state = state_at_phase(operation_dir, "installing")
    source_checks: list[object] = []
    installed_checks: list[object] = []
    recovery_calls: list[object] = []
    monkeypatch.setattr(module, "_effective_uid", lambda: 0)
    monkeypatch.setattr(module, "_require_posix_descriptor_primitives", lambda: None)
    monkeypatch.setattr(
        module,
        "load_operation_state",
        lambda path, *, effective_uid: state,
    )
    if os.name != "posix":
        monkeypatch.setattr(module, "validate_operation_state_for_context", lambda *_: None)
    monkeypatch.setattr(
        module,
        "verify_running_source_helper",
        lambda context: source_checks.append(context),
    )
    monkeypatch.setattr(
        module,
        "verify_running_installed_helper",
        lambda context: installed_checks.append(context),
        raising=False,
    )
    monkeypatch.setattr(
        module,
        "recover_host_configuration",
        lambda context: recovery_calls.append(context) or {},
    )

    result = module.main(["recover", "--operation-dir", operation_dir_raw])

    captured = capsys.readouterr()
    assert result == 0
    assert captured.out == ""
    assert captured.err == ""
    assert source_checks == recovery_calls
    assert len(recovery_calls) == 1
    assert installed_checks == []


def test_task8_observation_accepts_scheduled_run_after_cutoff_before_observe_invocation(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "observed")
    state["prior_runtime"]["preinstall_trigger_epoch"] = 1_750_000_105
    state["observation"]["run_epoch"] = 1_750_000_106
    task8_reseal_observation_state(module, state)

    module.validate_operation_state(state, operation_dir)


def test_task8_observation_requires_run_strictly_after_all_cutoffs(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "observed")
    state["prior_runtime"]["preinstall_trigger_epoch"] = 1_750_000_105
    state["observation"]["run_epoch"] = 1_750_000_105
    task8_reseal_observation_state(module, state)

    with pytest.raises(module.OperationStateError, match="observation|newer|cutoff|trigger"):
        module.validate_operation_state(state, operation_dir)


@pytest.mark.parametrize("origin_phase", ["policy_enabled", "observed"])
def test_task8_enable_policy_origin_manual_rollback_accepts_exact_live_policy_digest(
    tmp_path: Path,
    origin_phase: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = task8_policy_origin_manual_rollback_state(operation_dir, origin_phase)

    module.validate_operation_state(state, operation_dir)


@pytest.mark.parametrize("origin_phase", ["policy_enabled", "observed"])
def test_task8_enable_policy_origin_manual_rollback_rejects_stale_disabled_digest(
    tmp_path: Path,
    origin_phase: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = task8_policy_origin_manual_rollback_state(operation_dir, origin_phase)
    state["recovery"]["previous_sha256"] = state["install"]["installed_hashes"][TARGETS[-1]]

    with pytest.raises(module.OperationStateError, match="policy|live|previous|environment"):
        module.validate_operation_state(state, operation_dir)


def test_task8_enable_policy_origin_live_target_proof_accepts_exact_enabled_environment(
    task8_installed_helper_fixture,
) -> None:
    module, context, _installed_helper = task8_installed_helper_fixture
    state, environment, _disabled_bytes, enabled_bytes = task8_policy_enabled_runtime_state(
        module,
        context,
    )
    environment.write_bytes(enabled_bytes)

    receipts = module._task7_require_recoverable_live_targets(context, state)

    assert receipts[TARGETS[-1]]["sha256"] == state["policy"]["environment_sha256"]


def test_task8_enable_policy_origin_live_target_proof_rejects_stale_disabled_environment(
    task8_installed_helper_fixture,
) -> None:
    module, context, _installed_helper = task8_installed_helper_fixture
    state, _environment, _disabled_bytes, _enabled_bytes = task8_policy_enabled_runtime_state(
        module,
        context,
    )

    with pytest.raises(module.OperationStateError, match="policy|live|provenance|environment"):
        module._task7_require_recoverable_live_targets(context, state)


@pytest.mark.parametrize(
    "unsafe_prefix",
    [
        "onedrive:backups/degen-db/20260630T235959Z-owned/",
        "onedrive:backups/degen-db-probe/OTHER-0123456789abcdef0123456789abcdef/",
        "onedrive:backups/degen-db-probe/20260630T235959Z-ABCDEF0123456789abcdef0123456789/",
        "onedrive:backups/degen-db-probe/20260630T235959Z-../../",
        "onedrive:backups/degen-db-probe/20260630T235959Z-0123456789abcdef0123456789abcdef",
        "other:backups/degen-db-probe/20260630T235959Z-0123456789abcdef0123456789abcdef/",
    ],
)
def test_task8_probe_refuses_unsafe_or_production_remote_prefix(
    tmp_path: Path,
    unsafe_prefix: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "probing")
    state["active_transaction"]["probe"]["prefix"] = unsafe_prefix

    with pytest.raises(module.OperationStateError, match="probe|prefix|remote"):
        module.validate_operation_state(state, operation_dir)


@pytest.mark.parametrize(
    "unsafe_name",
    [
        "../probe.dump",
        "nested/probe.dump",
        "nested\\probe.dump",
        ".",
        "probe.dump\n",
        "PROBE.DUMP",
    ],
)
def test_task8_probe_refuses_unsafe_or_casefold_colliding_object_names(
    tmp_path: Path,
    unsafe_name: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "probing")
    state["active_transaction"]["probe"]["objects"][1]["name"] = unsafe_name

    with pytest.raises(module.OperationStateError, match="probe|object|name|case"):
        module.validate_operation_state(state, operation_dir)


@pytest.mark.parametrize("receipt_field", ["prefix", "owned_names"])
def test_task8_probe_completion_receipt_binds_exact_case_preserving_transaction_identity(
    tmp_path: Path,
    receipt_field: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    previous = state_at_phase(operation_dir, "probing")
    mark_transaction_complete(previous)
    current = state_at_phase(operation_dir, "probed")
    current_groups = current["rclone_evidence_groups"]
    assert isinstance(current_groups, list) and current_groups
    previous["rclone_evidence_groups"] = copy.deepcopy(current_groups)
    module.validate_operation_state(current, operation_dir, previous)
    if receipt_field == "prefix":
        current["probe"]["prefix"] = SAFE_PROBE_PREFIX.replace(
            "0123456789abcdef0123456789abcdef",
            "fedcba9876543210fedcba9876543210",
        )
    else:
        current["probe"]["owned_names"] = ["PROBE.DUMP", "probe.dump.sha256"]

    with pytest.raises(module.OperationStateError, match="probe|receipt|identity|name"):
        module.validate_operation_state(current, operation_dir, previous)


@pytest.mark.parametrize(
    (
        "active_kind",
        "started_epoch",
        "direct_phase",
        "recovery_kind",
        "recovery_phase",
    ),
    [
        ("probe", 1_750_000_050, "probing", None, None),
        ("probe", 1_750_000_050, None, "probe", "recovering_probe"),
        ("dry_run", 1_750_000_070, "dry_run_recording", None, None),
        ("dry_run", 1_750_000_070, None, "guard_dry_run", "recovering_guard"),
        ("policy", 1_750_000_090, "policy_enabling", None, None),
        ("policy", 1_750_000_090, None, "policy", "recovering_policy"),
        ("observe", 1_750_000_110, "observing", None, None),
        ("observe", 1_750_000_110, None, "guard_observe", "recovering_guard"),
    ],
)
def test_task8_rclone_pending_group_is_bound_to_command_kind_and_active_phase(
    tmp_path: Path,
    active_kind: str,
    started_epoch: int,
    direct_phase: str | None,
    recovery_kind: str | None,
    recovery_phase: str | None,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    if direct_phase is not None:
        state = state_at_phase(operation_dir, direct_phase)
    else:
        assert recovery_kind is not None and recovery_phase is not None
        state = failed_transaction_state(operation_dir, recovery_kind, recovery_phase)
    if active_kind == "policy":
        state["rclone_evidence_groups"].append(
            task8_pending_rclone_group(
                active_kind,
                started_epoch,
                0,
                purpose="enable-prune-inventory",
            )
        )
        module.validate_operation_state(state, operation_dir)
        return
    if active_kind == "dry_run":
        completed_purpose = TASK8_DRY_RUN_PURPOSES[0]
        pending_purpose = TASK8_DRY_RUN_PURPOSES[1]
    elif active_kind == "observe":
        completed_purpose = TASK8_OBSERVE_PURPOSES[0]
        pending_purpose = TASK8_OBSERVE_PURPOSES[1]
    else:
        completed_purpose = "remote-inventory"
        pending_purpose = "remote-verify"
    completed = task8_rclone_group(
        active_kind,
        started_epoch,
        0,
        0,
        purpose=completed_purpose,
    )
    state["rclone_evidence_groups"].extend(
        [
            completed,
            task8_pending_rclone_group(
                active_kind,
                started_epoch,
                1,
                purpose=pending_purpose,
            ),
        ]
    )

    module.validate_operation_state(state, operation_dir)


def test_task8_rclone_pending_group_rejects_command_id_mismatched_to_transaction(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "probing")
    state["rclone_evidence_groups"].append(
        task8_pending_rclone_group("observe", 1_750_000_050, 0)
    )

    with pytest.raises(module.OperationStateError, match="rclone|audit|identity|transaction"):
        module.validate_operation_state(state, operation_dir)


@pytest.mark.parametrize(
    "invalid_group_id",
    [
        "probe-remote",
        "task8:probe:1750000050:-1:0",
        "task8:probe:1750000050:0:-1",
        "task8:probe:1750000050:01:0",
        "task8:probe:1750000050:0:01",
        "task8:probe:1750000050:not-an-integer:0",
        "task8:probe:1750000051:0:0",
    ],
)
def test_task8_rclone_pending_group_requires_exact_kind_epoch_ordinal_identity(
    tmp_path: Path,
    invalid_group_id: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "probing")
    group = task8_pending_rclone_group("probe", 1_750_000_050, 0)
    group["group_id"] = invalid_group_id
    state["rclone_evidence_groups"].append(group)

    with pytest.raises(module.OperationStateError, match="rclone|audit|identity|transaction"):
        module.validate_operation_state(state, operation_dir)


def task8_same_epoch_retry_probe_state(operation_dir: Path) -> dict[str, object]:
    _installed, second_attempt = completed_probe_recovery_then_new_probe(operation_dir)
    retry_epoch = int(second_attempt["active_transaction"]["started_epoch"])
    required = copy.deepcopy(second_attempt)
    append_phase(required, "recovery_required", retry_epoch)
    required["recovery"] = recovery_receipt("probe", started_epoch=retry_epoch)
    recovering = copy.deepcopy(required)
    append_phase(recovering, "recovering_probe", retry_epoch)
    mark_transaction_complete(recovering)
    installed = copy.deepcopy(recovering)
    installed["active_transaction"] = None
    installed["recovery"]["completed_epoch"] = retry_epoch
    append_phase(installed, "installed", retry_epoch)
    third_attempt = copy.deepcopy(installed)
    third_attempt["active_transaction"] = active_transaction(
        "probe",
        "installed",
        retry_epoch,
    )
    append_phase(third_attempt, "probing", retry_epoch)
    history = third_attempt["phase_history"]
    assert isinstance(history, list)
    history[-1]["evidence_sha256"] = task8_probe_entry_evidence(
        operation_dir,
        retry_epoch,
        task8_probe_objects(),
    )
    return third_attempt


def task8_cursor_state(
    operation_dir: Path,
    phase_kind: str,
    cursor: int,
) -> dict[str, object]:
    if phase_kind == "installing":
        state = state_at_phase(operation_dir, "installing")
        cursor_receipt = state["install"]
    elif phase_kind == "active_recovery":
        state = manual_rollback_state(operation_dir, "manual_rollback")
        cursor_receipt = state["recovery"]
    elif phase_kind == "stable_manual":
        state = state_at_phase(operation_dir, "installed")
        cursor_receipt = None
    elif phase_kind == "policy_recovery":
        state = task8_policy_origin_manual_rollback_state(operation_dir, "policy_enabled")
        cursor_receipt = state["recovery"]
    else:
        raise ValueError(phase_kind)
    snapshot = state["snapshot"]
    assert isinstance(snapshot, dict)
    targets = snapshot["targets"]
    assert isinstance(targets, dict)
    for target in TARGETS:
        targets[target]["sha256"] = HASH_D
    if isinstance(cursor_receipt, dict):
        if cursor == len(TARGETS):
            cursor_receipt.update(
                {
                    "next_target_index": cursor,
                    "current_target": None,
                    "previous_sha256": None,
                    "intended_sha256": None,
                }
            )
        else:
            target = TARGETS[cursor]
            cursor_receipt.update(
                {
                    "next_target_index": cursor,
                    "current_target": target,
                    "previous_sha256": HASH_A,
                    "intended_sha256": HASH_D,
                }
            )
    return state


def task8_live_receipt(
    module: object,
    state: dict[str, object],
    target: str,
    provenance: str,
) -> dict[str, object]:
    if provenance == "snapshot":
        return copy.deepcopy(state["snapshot"]["targets"][target])
    if provenance not in {"installed", "disabled"}:
        raise ValueError(provenance)
    expected_hash = module._target_staged_hash(state, target)
    if (
        provenance == "installed"
        and target == TARGETS[-1]
        and state["policy"] is not None
    ):
        expected_hash = state["policy"]["environment_sha256"]
    return {
        "present": True,
        "sha256": expected_hash,
        "mode": module._task7_install_mode(target),
        "uid": 0,
        "gid": 0,
    }


def task8_check_live_provenance(
    module: object,
    monkeypatch: pytest.MonkeyPatch,
    state: dict[str, object],
    provenance_by_target: dict[str, str],
) -> dict[str, dict[str, object]]:
    receipts = {
        target: task8_live_receipt(module, state, target, provenance)
        for target, provenance in provenance_by_target.items()
    }
    monkeypatch.setattr(
        module,
        "_task7_capture_target_receipt",
        lambda _context, target: copy.deepcopy(receipts[target]),
    )
    return module._task7_require_recoverable_live_targets(object(), state)


def test_task8_rclone_completed_groups_validate_without_a_pending_tail(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "probing")
    state["rclone_evidence_groups"] = [
        task8_rclone_group("probe", 1_750_000_050, 0, 0),
        task8_rclone_group("probe", 1_750_000_050, 0, 1),
    ]

    module.validate_operation_state(state, operation_dir)


@pytest.mark.parametrize(
    "rebound_group_id",
    [
        "task8:observe:1750000050:0:0",
        "task8:probe:1750000051:0:0",
        "task8:probe:1750000050:1:0",
        "task8:probe:1750000050:0:2",
    ],
)
def test_task8_rclone_completed_group_identity_is_bound_to_history_and_ordinals(
    tmp_path: Path,
    rebound_group_id: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "probing")
    group = task8_rclone_group("probe", 1_750_000_050, 0, 0)
    group["group_id"] = rebound_group_id
    task8_reseal_rclone_group(group)
    state["rclone_evidence_groups"] = [group]

    with pytest.raises(module.OperationStateError, match="rclone|group|history|ordinal|identity"):
        module.validate_operation_state(state, operation_dir)


def test_task8_rclone_completed_group_rejects_arbitrary_evidence_digest(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "probing")
    group = task8_rclone_group("probe", 1_750_000_050, 0, 0)
    group["evidence_sha256"] = HASH_D
    state["rclone_evidence_groups"] = [group]

    with pytest.raises(module.OperationStateError, match="rclone|group|evidence"):
        module.validate_operation_state(state, operation_dir)


def test_task8_rclone_completed_groups_reject_duplicate_group_ordinal(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "probing")
    state["rclone_evidence_groups"] = [
        task8_rclone_group("probe", 1_750_000_050, 0, 0, purpose="remote-list"),
        task8_rclone_group("probe", 1_750_000_050, 0, 0, purpose="remote-check"),
    ]

    with pytest.raises(module.OperationStateError, match="rclone|group|ordinal|duplicate"):
        module.validate_operation_state(state, operation_dir)


def test_task8_rclone_same_second_retry_uses_history_attempt_ordinal(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = task8_same_epoch_retry_probe_state(operation_dir)
    state["rclone_evidence_groups"] = [
        task8_rclone_group("probe", 1_750_000_140, 2, 0)
    ]

    module.validate_operation_state(state, operation_dir)


@pytest.mark.parametrize("append_shape", ["completed", "two-completed", "completed-pending"])
def test_task8_rclone_group_append_requires_one_pending_write(
    tmp_path: Path,
    append_shape: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    previous = state_at_phase(operation_dir, "probing")
    current = copy.deepcopy(previous)
    first = task8_rclone_group("probe", 1_750_000_050, 0, 0)
    groups = [first]
    if append_shape == "two-completed":
        groups.append(task8_rclone_group("probe", 1_750_000_050, 0, 1))
    elif append_shape == "completed-pending":
        groups.append(task8_pending_rclone_group("probe", 1_750_000_050, 1))
    current["rclone_evidence_groups"] = groups

    with pytest.raises(module.OperationStateError, match="pending|one|rclone|append|transition"):
        module.validate_operation_state(current, operation_dir, previous)


def test_task8_rclone_pending_group_completion_is_a_separate_same_phase_write(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    previous = state_at_phase(operation_dir, "probing")
    completed = task8_rclone_group("probe", 1_750_000_050, 0, 0)
    previous["rclone_evidence_groups"] = [task8_pending_copy(completed)]
    current = copy.deepcopy(previous)
    current["rclone_evidence_groups"] = [completed]

    module.validate_operation_state(current, operation_dir, previous)


def test_task8_rclone_pending_completion_cannot_rebind_group_identity(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    previous = state_at_phase(operation_dir, "probing")
    previous["rclone_evidence_groups"] = [
        task8_pending_rclone_group("probe", 1_750_000_050, 0)
    ]
    current = copy.deepcopy(previous)
    current["rclone_evidence_groups"] = [
        task8_rclone_group("probe", 1_750_000_050, 0, 1)
    ]

    with pytest.raises(module.OperationStateError, match="same|rclone|identity|pending"):
        module.validate_operation_state(current, operation_dir, previous)


def test_task8_probe_standalone_receipt_accepts_exact_hash_chain(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)

    module.validate_operation_state(
        state_at_phase(operation_dir, "probed"),
        operation_dir,
    )


def test_task8_probe_objects_use_exact_deterministic_dump_and_sidecar_bytes() -> None:
    identity = SAFE_PROBE_PREFIX.rstrip("/").rsplit("/", 1)[-1]
    operation_id, _separator, token = identity.rpartition("-")
    dump_bytes = (
        b"degen-db-remote-probe-v1\n"
        + f"operation_id={operation_id}\n".encode("ascii")
        + f"token={token}\n".encode("ascii")
    )
    dump_sha256 = hashlib.sha256(dump_bytes).hexdigest()
    sidecar_bytes = f"{dump_sha256}  probe.dump\n".encode("ascii")
    objects = task8_probe_objects()

    assert objects == [
        {
            "name": "probe.dump",
            "expected_sha256": dump_sha256,
            "expected_size": len(dump_bytes),
            "created": False,
            "verified": False,
            "cleaned": False,
        },
        {
            "name": "probe.dump.sha256",
            "expected_sha256": hashlib.sha256(sidecar_bytes).hexdigest(),
            "expected_size": len(sidecar_bytes),
            "created": False,
            "verified": False,
            "cleaned": False,
        },
    ]


@pytest.mark.parametrize(
    ("object_index", "field"),
    [(0, "expected_sha256"), (0, "expected_size"), (1, "expected_sha256"), (1, "expected_size")],
)
def test_task8_probe_entry_rejects_resealed_nondeterministic_object_identity(
    tmp_path: Path,
    object_index: int,
    field: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "probing")
    objects = state["active_transaction"]["probe"]["objects"]
    if field == "expected_sha256":
        objects[object_index][field] = HASH_D
    else:
        objects[object_index][field] = int(objects[object_index][field]) + 1
    state["phase_history"][-1]["evidence_sha256"] = task8_probe_entry_evidence(
        operation_dir,
        1_750_000_050,
        objects,
    )

    with pytest.raises(module.OperationStateError, match="probe|object|identity|deterministic"):
        module.validate_operation_state(state, operation_dir)


def test_task8_probe_completion_excludes_unrelated_install_rclone_history(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "probed")
    expected_probe_evidence = state["probe"]["evidence_sha256"]
    state["rclone_evidence_groups"].insert(0, task7_install_rclone_group())

    assert task8_reseal_probe_completion(state, operation_dir) == expected_probe_evidence
    module.validate_operation_state(state, operation_dir)


@pytest.mark.parametrize(
    "mutation",
    [
        "cleanup-false",
        "empty-names",
        "rebound-prefix",
        "casefold-names",
        "no-groups",
        "receipt-digest",
        "receipt-and-history-digest",
        "group-digest",
        "entry-start-epoch",
        "entry-digest",
    ],
)
def test_task8_probe_hash_chain_rejects_rebound_state(
    tmp_path: Path,
    mutation: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    entry_mutation = mutation.startswith("entry-")
    state = state_at_phase(operation_dir, "probing" if entry_mutation else "probed")
    if mutation == "cleanup-false":
        state["probe"]["cleanup_proven"] = False
        task8_reseal_probe_completion(state, operation_dir)
    elif mutation == "empty-names":
        state["probe"]["owned_names"] = []
        task8_reseal_probe_completion(state, operation_dir)
    elif mutation == "rebound-prefix":
        state["probe"]["prefix"] = SAFE_PROBE_PREFIX.replace(
            "0123456789abcdef0123456789abcdef",
            "fedcba9876543210fedcba9876543210",
        )
        task8_reseal_probe_completion(state, operation_dir)
    elif mutation == "casefold-names":
        state["probe"]["owned_names"] = ["PROBE.DUMP", "probe.dump.sha256"]
        task8_reseal_probe_completion(state, operation_dir)
    elif mutation == "no-groups":
        state["rclone_evidence_groups"] = []
        task8_reseal_probe_completion(state, operation_dir)
    elif mutation == "receipt-digest":
        state["probe"]["evidence_sha256"] = HASH_D
    elif mutation == "receipt-and-history-digest":
        state["probe"]["evidence_sha256"] = HASH_D
        state["phase_history"][-1]["evidence_sha256"] = HASH_D
    elif mutation == "group-digest":
        state["rclone_evidence_groups"][0]["evidence_sha256"] = HASH_D
        task8_reseal_probe_completion(state, operation_dir)
    elif mutation == "entry-start-epoch":
        state["active_transaction"]["started_epoch"] = 1_750_000_051
    else:
        state["phase_history"][-1]["evidence_sha256"] = HASH_D

    with pytest.raises(
        module.OperationStateError,
        match="probe|cleanup|entry|identity|name|prefix|rclone|evidence|digest|epoch",
    ):
        module.validate_operation_state(state, operation_dir)


def test_task8_probe_success_requires_group_completion_before_stable_transition(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    completed_groups = task8_successful_probe_groups()
    pending = state_at_phase(operation_dir, "probing")
    pending["rclone_evidence_groups"] = [
        *completed_groups[:-1],
        task8_pending_copy(completed_groups[-1]),
    ]
    completed = copy.deepcopy(pending)
    completed["rclone_evidence_groups"] = completed_groups
    module.validate_operation_state(completed, operation_dir, pending)

    ready = copy.deepcopy(completed)
    mark_transaction_complete(ready)
    stable = state_at_phase(operation_dir, "probed")
    module.validate_operation_state(stable, operation_dir, ready)


def test_task8_probe_success_transition_rejects_missing_rclone_groups(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    previous = state_at_phase(operation_dir, "probing")
    mark_transaction_complete(previous)
    current = state_at_phase(operation_dir, "probed")
    current["rclone_evidence_groups"] = []
    task8_reseal_probe_completion(current, operation_dir)

    with pytest.raises(module.OperationStateError, match="probe|rclone|group|evidence"):
        module.validate_operation_state(current, operation_dir, previous)


def test_task8_probe_success_transition_rejects_nonterminal_ownership_chain(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    nonterminal = task8_successful_probe_groups()[:-1]
    previous = state_at_phase(operation_dir, "probing")
    mark_transaction_complete(previous)
    previous["rclone_evidence_groups"] = copy.deepcopy(nonterminal)
    current = state_at_phase(operation_dir, "probed")
    current["rclone_evidence_groups"] = copy.deepcopy(nonterminal)
    task8_reseal_probe_completion(current, operation_dir)

    for prior in (None, previous):
        with pytest.raises(
            module.OperationStateError,
            match="probe|cleanup|prefix-empty|terminal",
        ):
            module.validate_operation_state(
                current,
                operation_dir,
                prior,
            )


def test_task8_context_free_probed_receipt_rejects_indeterminate_terminal_outcome(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "probed")
    terminal = state["rclone_evidence_groups"][-1]
    assert isinstance(terminal, dict)
    terminal["outcome"] = "indeterminate"
    task8_reseal_rclone_group(terminal)
    task8_reseal_probe_completion(state, operation_dir)

    with pytest.raises(
        module.OperationStateError,
        match="probe|indeterminate|prefix-empty|terminal|success",
    ):
        module.validate_operation_state(state, operation_dir)


def test_task8_observation_accepts_run_at_observing_entry_epoch(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "observed")
    state["observation"]["run_epoch"] = 1_750_000_110
    task8_reseal_observation_state(module, state)

    module.validate_operation_state(state, operation_dir)


def test_task8_observation_rejects_run_after_observing_entry_epoch(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "observed")
    state["observation"]["run_epoch"] = 1_750_000_111
    task8_reseal_observation_state(module, state)

    with pytest.raises(module.OperationStateError, match="observation|observing|entry|future"):
        module.validate_operation_state(state, operation_dir)


def task8_provenance_matrix(
    phase_kind: str,
    cursor: int,
    current: str,
) -> dict[str, str]:
    if phase_kind == "installing":
        before, after = "installed", "snapshot"
    elif phase_kind in {"active_recovery", "policy_recovery"}:
        before, after = "snapshot", "installed"
    elif phase_kind == "stable_manual":
        return {target: "installed" for target in TARGETS}
    else:
        raise ValueError(phase_kind)
    if cursor == len(TARGETS):
        return {target: before for target in TARGETS}
    return {
        target: before if index < cursor else current if index == cursor else after
        for index, target in enumerate(TARGETS)
    }


@pytest.mark.parametrize(
    ("phase_kind", "current"),
    [
        ("active_recovery", "snapshot"),
        ("active_recovery", "installed"),
        ("installing", "snapshot"),
        ("installing", "installed"),
    ],
)
def test_task8_cursor_accepts_exact_position_matrix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase_kind: str,
    current: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = task8_cursor_state(operation_dir, phase_kind, 3)
    provenance = task8_provenance_matrix(phase_kind, 3, current)

    assert tuple(
        task8_check_live_provenance(module, monkeypatch, state, provenance)
    ) == TARGETS


@pytest.mark.parametrize(
    ("phase_kind", "cursor", "target_index", "wrong_provenance"),
    [
        ("active_recovery", 3, 0, "installed"),
        ("active_recovery", 3, 4, "snapshot"),
        ("active_recovery", len(TARGETS), 6, "installed"),
        ("installing", 3, 0, "snapshot"),
        ("installing", 3, 4, "installed"),
        ("stable_manual", 0, 0, "snapshot"),
    ],
)
def test_task8_cursor_rejects_provenance_outside_position_matrix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase_kind: str,
    cursor: int,
    target_index: int,
    wrong_provenance: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = task8_cursor_state(operation_dir, phase_kind, cursor)
    provenance = task8_provenance_matrix(phase_kind, cursor, "snapshot")
    provenance[TARGETS[target_index]] = wrong_provenance

    with pytest.raises(module.OperationStateError, match="live|cursor|provenance|target"):
        task8_check_live_provenance(module, monkeypatch, state, provenance)


def test_task8_cursor_policy_recovery_uses_enabled_environment_as_installed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    cursor = 3
    state = task8_cursor_state(operation_dir, "policy_recovery", cursor)
    provenance = task8_provenance_matrix("policy_recovery", cursor, "snapshot")
    task8_check_live_provenance(module, monkeypatch, state, provenance)
    provenance[TARGETS[-1]] = "disabled"

    with pytest.raises(module.OperationStateError, match="live|policy|environment|target"):
        task8_check_live_provenance(module, monkeypatch, state, provenance)


@pytest.mark.parametrize("phase", ["recovering_policy", "recovery_required"])
@pytest.mark.parametrize("mutation", ["cleanup", "resealed-completion"])
def test_task8_policy_recovery_revalidates_retained_probe_hash_chain(
    tmp_path: Path,
    phase: str,
    mutation: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = failed_transaction_state(operation_dir, "policy", phase)
    probe = state["probe"]
    history = state["phase_history"]
    assert isinstance(probe, dict) and isinstance(history, list)
    if mutation == "cleanup":
        probe["cleanup_proven"] = False
        task8_reseal_probe_completion(state, operation_dir)
    else:
        probe["evidence_sha256"] = HASH_D
        completed = next(entry for entry in reversed(history) if entry["phase"] == "probed")
        completed["evidence_sha256"] = HASH_D

    with pytest.raises(
        module.OperationStateError,
        match="probe|cleanup|completion|evidence|digest|hash",
    ):
        module.validate_operation_state(state, operation_dir)


@pytest.mark.parametrize("reversed_pair", [(0, 1), (1, 2), (2, 3)])
def test_task8_completed_rclone_groups_follow_global_attempt_occurrence_order(
    tmp_path: Path,
    reversed_pair: tuple[int, int],
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "observed")
    attempt_blocks = [
        copy.deepcopy(state["rclone_evidence_groups"]),
        [task8_successful_dry_run_groups()[0]],
        [task8_rclone_group("policy", 1_750_000_090, 0, 0)],
        [task8_rclone_group("observe", 1_750_000_110, 0, 0)],
    ]
    left, right = reversed_pair
    attempt_blocks[left], attempt_blocks[right] = (
        attempt_blocks[right],
        attempt_blocks[left],
    )
    state["rclone_evidence_groups"] = [
        group for block in attempt_blocks for group in block
    ]

    with pytest.raises(
        module.OperationStateError,
        match="rclone|group|global|history|occurrence|order",
    ):
        module.validate_operation_state(state, operation_dir)


def test_task8_legacy_install_rclone_completion_retains_canonical_pending_first_flow(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    completed_group = task7_install_rclone_group()
    initial = state_at_phase(operation_dir, "installing")
    pending = copy.deepcopy(initial)
    pending["rclone_evidence_groups"] = [task8_pending_copy(completed_group)]
    module.validate_operation_state(pending, operation_dir, initial)

    completed = copy.deepcopy(pending)
    completed["rclone_evidence_groups"] = [completed_group]
    module.validate_operation_state(completed, operation_dir, pending)
    module.validate_operation_state(completed, operation_dir)


def test_task8_legacy_install_rclone_rejects_direct_completed_append(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    previous = state_at_phase(operation_dir, "installing")
    current = copy.deepcopy(previous)
    current["rclone_evidence_groups"] = [task7_install_rclone_group()]

    with pytest.raises(
        module.OperationStateError,
        match="install|rclone|pending|append|before|after",
    ):
        module.validate_operation_state(current, operation_dir, previous)


def test_task8_install_recovery_rejects_staged_previous_hash_beyond_frozen_install_cursor(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = install_recovery_state(operation_dir, "recovering")
    install_cursor = 2
    recovery_cursor = 4
    state["install"].update(
        {
            "next_target_index": install_cursor,
            "current_target": TARGETS[install_cursor],
            "previous_sha256": HASH_A,
            "intended_sha256": HASH_A,
        }
    )
    source = SOURCE_ASSETS[recovery_cursor]
    state["reviewed_source"]["asset_hashes"][source] = HASH_B
    state["host_stage"]["asset_hashes"][source] = HASH_B
    state["recovery"].update(
        {
            "next_target_index": recovery_cursor,
            "current_target": TARGETS[recovery_cursor],
            "previous_sha256": HASH_B,
            "intended_sha256": HASH_A,
        }
    )

    with pytest.raises(
        module.OperationStateError,
        match="install|recovery|previous|baseline|cursor|snapshot|provenance",
    ):
        module.validate_operation_state(state, operation_dir)


@pytest.mark.parametrize(
    ("state_kind", "phase"),
    [
        ("direct", "probing"),
        ("direct", "dry_run_recording"),
        ("direct", "policy_enabling"),
        ("direct", "observing"),
        ("probe", "recovering_probe"),
        ("guard_dry_run", "recovering_guard"),
        ("policy", "recovering_policy"),
        ("guard_observe", "recovering_guard"),
    ],
)
def test_task8_active_transaction_start_equals_current_entry_phase_epoch(
    tmp_path: Path,
    state_kind: str,
    phase: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = (
        state_at_phase(operation_dir, phase)
        if state_kind == "direct"
        else failed_transaction_state(operation_dir, state_kind, phase)
    )
    state["active_transaction"]["started_epoch"] += 1

    with pytest.raises(
        module.OperationStateError,
        match="active_transaction|transaction|start|entry|epoch|history",
    ):
        module.validate_operation_state(state, operation_dir)


@pytest.mark.parametrize(
    "phase",
    ["dry_run_recording", "recovery_required", "recovering_guard"],
)
def test_task8_valid_dry_run_entry_provenance_accepts_active_and_guard_recovery_states(
    tmp_path: Path,
    phase: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = task8_guard_recovery_state(
        operation_dir,
        "guard_dry_run",
        "dry_run",
        "dry_run_recording",
        phase,
    )

    module.validate_operation_state(state, operation_dir)


@pytest.mark.parametrize(
    "phase",
    ["dry_run_recording", "recovery_required", "recovering_guard"],
)
def test_task8_dry_run_entry_provenance_rejects_tampering_in_active_and_guard_recovery_states(
    tmp_path: Path,
    phase: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = task8_guard_recovery_state(
        operation_dir,
        "guard_dry_run",
        "dry_run",
        "dry_run_recording",
        phase,
    )
    entry = next(
        item
        for item in reversed(state["phase_history"])
        if item["phase"] == "dry_run_recording"
    )
    assert entry["evidence_sha256"] != HASH_D
    entry["evidence_sha256"] = HASH_D

    with pytest.raises(
        module.OperationStateError,
        match=r"dry[_ -]?run.*entry.*evidence|entry.*evidence.*dry[_ -]?run|provenance",
    ):
        module.validate_operation_state(state, operation_dir)


@pytest.mark.parametrize("validation_mode", ["transition", "standalone"])
def test_task8_policy_digest_rejects_arbitrary_value_instead_of_precomputed_enabled_hash(
    tmp_path: Path,
    validation_mode: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    if validation_mode == "transition":
        previous = state_at_phase(operation_dir, "dry_run_recorded")
        state = state_at_phase(operation_dir, "policy_enabling")
        state["active_transaction"]["policy_environment_sha256"] = HASH_D
    else:
        previous = None
        state = state_at_phase(operation_dir, "policy_enabled")
        state["policy"]["environment_sha256"] = HASH_D
    assert state["host_stage"]["enabled_environment_sha256"] == HASH_B

    with pytest.raises(
        module.OperationStateError,
        match="policy.*(enabled|environment|precomputed|digest)|(enabled|environment).*policy",
    ):
        module.validate_operation_state(state, operation_dir, previous)


TASK8_GUARD_CASES = (
    ("probe", "installed", "probing"),
    ("dry_run", "probed", "dry_run_recording"),
    ("policy", "dry_run_recorded", "policy_enabling"),
    ("observe", "policy_enabled", "observing"),
)


class Task8CallbackFailure(Exception):
    pass


def task8_competing_process_can_lock(path: Path) -> bool:
    assert os.name == "posix"
    probe = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import fcntl, os, sys\n"
                "fd = os.open(sys.argv[1], os.O_RDWR)\n"
                "try:\n"
                "    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)\n"
                "except BlockingIOError:\n"
                "    raise SystemExit(73)\n"
                "fcntl.flock(fd, fcntl.LOCK_UN)\n"
                "os.close(fd)\n"
            ),
            str(path),
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    assert probe.returncode in {0, 73}, (probe.returncode, probe.stdout, probe.stderr)
    return probe.returncode == 0


def task8_close_if_open(descriptor: int) -> None:
    try:
        os.fstat(descriptor)
    except OSError:
        return
    os.close(descriptor)


def task8_guard_test_context(operation_dir: Path, epoch: int) -> object:
    return types.SimpleNamespace(
        operation_id=operation_dir.name,
        paths=types.SimpleNamespace(
            operation_dir=operation_dir,
            state_file=operation_dir / "operation-state.json",
        ),
        effective_uid=os.geteuid() if hasattr(os, "geteuid") else 0,
        clock=lambda: datetime.fromtimestamp(epoch, tz=timezone.utc),
    )


def task8_mark_guard_through(
    state: dict[str, object],
    final_field: str,
) -> None:
    ordered = (
        "timer_stopped",
        "service_inactive_verified",
        "legacy_lock_acquired",
        "runtime_lock_acquired",
        "locks_released",
        "timer_restored",
    )
    guard = state["active_transaction"]["guard"]
    assert isinstance(guard, dict)
    for field in ordered:
        guard[field] = True
        if field == final_field:
            return
    raise AssertionError(final_field)


@pytest.mark.parametrize(
    "api_name",
    [
        "_task8_force_checkpoint",
        "_task8_advance_guard",
        "_task8_enter_guarded_transaction",
        "_task8_acquire_guard",
        "_task8_release_guard_locks",
        "_task8_restore_guard_timer",
    ],
)
def test_task8_guard_primitive_api_surface_exists(api_name: str) -> None:
    module = load_ops_helper()

    assert callable(getattr(module, api_name, None)), f"missing Task 8 guard API: {api_name}"


@pytest.mark.parametrize(("kind", "_prior_phase", "entry_phase"), TASK8_GUARD_CASES)
def test_task8_active_transaction_carries_exact_full_runtime_baseline(
    tmp_path: Path,
    kind: str,
    _prior_phase: str,
    entry_phase: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, entry_phase)
    transaction = state["active_transaction"]
    baseline = recovery_runtime_baseline()
    if kind == "observe":
        baseline["preinstall_trigger_epoch"] = 1_750_000_105

    assert transaction["kind"] == kind
    assert transaction["runtime_baseline"] == baseline
    assert transaction["prior_timer_enabled"] is baseline["timer_enabled"]
    assert transaction["prior_timer_active"] is baseline["timer_active"]
    assert set(transaction["runtime_baseline"]) == {
        "timer_enabled",
        "timer_active",
        "pids",
        "preinstall_trigger_epoch",
    }
    module.validate_operation_state(state, operation_dir)


@pytest.mark.parametrize(
    "mutation",
    [
        "missing",
        "extra",
        "timer-enabled-mismatch",
        "timer-active-mismatch",
        "invalid-pids",
        "invalid-trigger",
    ],
)
def test_task8_active_transaction_runtime_baseline_schema_fails_closed(
    tmp_path: Path,
    mutation: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "probing")
    transaction = state["active_transaction"]
    baseline = transaction["runtime_baseline"]
    assert isinstance(baseline, dict)
    if mutation == "missing":
        transaction.pop("runtime_baseline")
    elif mutation == "extra":
        baseline["unexpected"] = 1
    elif mutation == "timer-enabled-mismatch":
        transaction["prior_timer_enabled"] = not baseline["timer_enabled"]
    elif mutation == "timer-active-mismatch":
        transaction["prior_timer_active"] = not baseline["timer_active"]
    elif mutation == "invalid-pids":
        baseline["pids"] = {"system:degen-web.service": 0}
    else:
        baseline["preinstall_trigger_epoch"] = -1

    with pytest.raises(
        module.OperationStateError,
        match="active_transaction|runtime_baseline|prior_timer|pids|trigger",
    ):
        module.validate_operation_state(state, operation_dir)


@pytest.mark.parametrize("field", ["timer_enabled", "timer_active", "pids", "preinstall_trigger_epoch"])
def test_task8_active_transaction_runtime_baseline_is_immutable(
    tmp_path: Path,
    field: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    previous = state_at_phase(operation_dir, "probing")
    current = copy.deepcopy(previous)
    baseline = current["active_transaction"]["runtime_baseline"]
    if field in {"timer_enabled", "timer_active"}:
        baseline[field] = not baseline[field]
        current["active_transaction"][
            "prior_timer_enabled" if field == "timer_enabled" else "prior_timer_active"
        ] = baseline[field]
    elif field == "pids":
        baseline[field]["system:degen-web.service"] += 1000
    else:
        baseline[field] = 1_749_999_999

    with pytest.raises(
        module.OperationStateError,
        match="active_transaction|runtime_baseline|identity|immutable",
    ):
        module.validate_operation_state(current, operation_dir, previous)


@pytest.mark.parametrize(
    ("state_kind", "recovery_phase"),
    [
        ("probe", "recovering_probe"),
        ("probe", "recovery_required"),
        ("guard_dry_run", "recovering_guard"),
        ("guard_dry_run", "recovery_required"),
        ("policy", "recovering_policy"),
        ("policy", "recovery_required"),
        ("guard_observe", "recovering_guard"),
        ("guard_observe", "recovery_required"),
    ],
)
def test_task8_every_recovery_receipt_copies_transaction_runtime_baseline(
    tmp_path: Path,
    state_kind: str,
    recovery_phase: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = failed_transaction_state(operation_dir, state_kind, recovery_phase)
    transaction = state["active_transaction"]
    recovery = state["recovery"]

    assert recovery["runtime_baseline"] == transaction["runtime_baseline"]
    module.validate_operation_state(state, operation_dir)

    rebound = copy.deepcopy(state)
    rebound["recovery"]["runtime_baseline"]["preinstall_trigger_epoch"] = 7
    with pytest.raises(
        module.OperationStateError,
        match="recovery|runtime_baseline|active_transaction|immutable",
    ):
        module.validate_operation_state(rebound, operation_dir)


@pytest.mark.parametrize(("kind", "prior_phase", "entry_phase"), TASK8_GUARD_CASES)
def test_task8_guard_entry_is_durable_before_any_external_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    prior_phase: str,
    entry_phase: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, prior_phase)
    entry_epoch = int(state["phase_history"][-1]["epoch"]) + 1
    context = task8_guard_test_context(operation_dir, entry_epoch)
    writes: list[dict[str, object]] = []
    monkeypatch.setattr(
        module,
        "_task7_write_state",
        lambda _context, _binding, candidate, **_kwargs: writes.append(copy.deepcopy(candidate)),
    )
    enter = getattr(module, "_task8_enter_guarded_transaction", None)
    assert callable(enter), "missing Task 8 guarded transaction entry"
    baseline = recovery_runtime_baseline()
    if kind == "observe":
        baseline["preinstall_trigger_epoch"] = 1_750_000_095
    probe = (
        {"prefix": SAFE_PROBE_PREFIX, "objects": task8_probe_objects()}
        if kind == "probe"
        else None
    )

    enter(
        context,
        object(),
        state,
        kind=kind,
        runtime_baseline=baseline,
        policy_environment_sha256=HASH_B if kind == "policy" else None,
        probe=probe,
    )

    assert len(writes) == 1
    durable = writes[0]
    assert durable["phase"] == entry_phase
    assert durable["phase_history"][-1]["phase"] == entry_phase
    transaction = durable["active_transaction"]
    assert transaction["kind"] == kind
    assert transaction["prior_stable_phase"] == prior_phase
    assert transaction["started_epoch"] == durable["phase_history"][-1]["epoch"]
    assert transaction["runtime_baseline"] == baseline
    assert transaction["prior_timer_enabled"] is baseline["timer_enabled"]
    assert transaction["prior_timer_active"] is baseline["timer_active"]
    assert not any(transaction["guard"].values())
    module.validate_operation_state(durable, operation_dir, state_at_phase(operation_dir, prior_phase))


def test_task8_force_checkpoint_always_replaces_and_fsyncs_same_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "probing")
    context = task8_guard_test_context(operation_dir, 1_750_000_051)
    calls: list[tuple[dict[str, object], dict[str, object]]] = []
    monkeypatch.setattr(
        module,
        "_task7_write_state",
        lambda _context, _binding, candidate, **kwargs: calls.append(
            (copy.deepcopy(candidate), dict(kwargs))
        ),
    )
    checkpoint = getattr(module, "_task8_force_checkpoint", None)
    assert callable(checkpoint), "missing Task 8 forced checkpoint primitive"
    before = copy.deepcopy(state)

    checkpoint(context, object(), state, "timer_stop")

    assert state == before
    assert calls == [(before, {"force_checkpoint": True})]


def test_task8_guard_milestones_advance_one_per_durable_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "probing")
    context = task8_guard_test_context(operation_dir, 1_750_000_051)
    writes: list[dict[str, object]] = []
    monkeypatch.setattr(
        module,
        "_task7_write_state",
        lambda _context, _binding, candidate, **_kwargs: writes.append(copy.deepcopy(candidate)),
    )
    advance = getattr(module, "_task8_advance_guard", None)
    assert callable(advance), "missing Task 8 guard progress primitive"
    fields = tuple(state["active_transaction"]["guard"])

    for field in fields:
        advance(context, object(), state, field)

    assert len(writes) == len(fields)
    previous_true = 0
    for index, durable in enumerate(writes, start=1):
        guard = durable["active_transaction"]["guard"]
        assert sum(value is True for value in guard.values()) == index
        assert sum(value is True for value in guard.values()) - previous_true == 1
        previous_true = index


@pytest.mark.parametrize(("kind", "_prior_phase", "entry_phase"), TASK8_GUARD_CASES)
def test_task8_guard_actions_checkpoint_immediately_and_preserve_lock_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    _prior_phase: str,
    entry_phase: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, entry_phase)
    context = task8_guard_test_context(operation_dir, 1_750_000_051)
    binding = object()
    events: list[str] = []

    def checkpoint(_context: object, _binding: object, _state: object, action: str) -> None:
        events.append(f"checkpoint:{action}")

    def advance(_context: object, _binding: object, candidate: dict[str, object], field: str) -> None:
        candidate["active_transaction"]["guard"][field] = True
        events.append(f"advance:{field}")

    def quiesce(_context: object, _baseline: object, *, before_action=None) -> None:
        for action in (
            "timer_disable",
            "timer_stop",
            "quiesce_service_check",
            "quiesce_runtime_readback",
        ):
            before_action(action)
            events.append(f"action:{action}")

    def acquire(_context: object, *, before_action=None, after_action=None) -> object:
        for action, descriptor in (
            ("legacy_lock_acquire", 101),
            ("runtime_lock_acquire", 202),
        ):
            before_action(action)
            events.append(f"action:{action}")
            after_action(action, descriptor)
        before_action("post_lock_service_recheck")
        events.append("action:post_lock_service_recheck")
        return module.MigrationLocks(legacy_fd=101, runtime_fd=202)

    def release(_locks: object, *, before_action=None) -> list[object]:
        for action in ("runtime_lock_release", "legacy_lock_release"):
            before_action(action)
            events.append(f"action:{action}")
        return []

    def restore(_context: object, baseline: dict[str, object], *, before_action=None) -> None:
        if baseline["timer_enabled"]:
            before_action("timer_enable")
            events.append("action:timer_enable")
        if baseline["timer_active"]:
            before_action("timer_start")
            events.append("action:timer_start")
        before_action("restore_runtime_readback")
        events.append("action:restore_runtime_readback")

    monkeypatch.setattr(module, "_task8_force_checkpoint", checkpoint, raising=False)
    monkeypatch.setattr(module, "_task8_advance_guard", advance, raising=False)
    monkeypatch.setattr(module, "_quiesce_backup_timer", quiesce)
    monkeypatch.setattr(module, "acquire_migration_locks", acquire)
    monkeypatch.setattr(module, "_release_migration_locks", release)
    monkeypatch.setattr(module, "_require_backup_service_inactive", lambda _context: events.append("action:pre_restore_service_check"))
    monkeypatch.setattr(module, "_restore_backup_timer", restore)

    locks = module._task8_acquire_guard(context, binding, state)
    module._task8_release_guard_locks(context, binding, state, locks)
    module._task8_restore_guard_timer(context, binding, state)

    expected_actions = (
        "timer_disable",
        "timer_stop",
        "quiesce_service_check",
        "quiesce_runtime_readback",
        "legacy_lock_acquire",
        "runtime_lock_acquire",
        "post_lock_service_recheck",
        "runtime_lock_release",
        "legacy_lock_release",
        "pre_restore_service_check",
        "timer_enable",
        "timer_start",
        "restore_runtime_readback",
    )
    for action in expected_actions:
        action_index = events.index(f"action:{action}")
        assert events[action_index - 1] == f"checkpoint:{action}", events
    assert events.index("action:legacy_lock_acquire") < events.index("action:runtime_lock_acquire")
    assert events.index("action:runtime_lock_release") < events.index("action:legacy_lock_release")
    assert events.index("advance:timer_stopped") < events.index("advance:service_inactive_verified")
    assert events.index("advance:service_inactive_verified") < events.index("checkpoint:legacy_lock_acquire")
    assert events.index("advance:legacy_lock_acquired") < events.index("checkpoint:runtime_lock_acquire")
    assert events.index("advance:locks_released") < events.index("checkpoint:pre_restore_service_check")
    assert all(state["active_transaction"]["guard"].values())


@pytest.mark.parametrize(
    ("timer_enabled", "timer_active", "expected_actions"),
    [
        (False, False, ("pre_restore_service_check", "restore_runtime_readback")),
        (False, True, ("pre_restore_service_check", "timer_start", "restore_runtime_readback")),
        (True, False, ("pre_restore_service_check", "timer_enable", "restore_runtime_readback")),
        (
            True,
            True,
            (
                "pre_restore_service_check",
                "timer_enable",
                "timer_start",
                "restore_runtime_readback",
            ),
        ),
    ],
)
def test_task8_guard_restores_all_exact_timer_matrices(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    timer_enabled: bool,
    timer_active: bool,
    expected_actions: tuple[str, ...],
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "probing")
    transaction = state["active_transaction"]
    transaction["runtime_baseline"]["timer_enabled"] = timer_enabled
    transaction["runtime_baseline"]["timer_active"] = timer_active
    transaction["prior_timer_enabled"] = timer_enabled
    transaction["prior_timer_active"] = timer_active
    task8_mark_guard_through(state, "locks_released")
    context = task8_guard_test_context(operation_dir, 1_750_000_051)
    events: list[str] = []
    monkeypatch.setattr(
        module,
        "_task8_force_checkpoint",
        lambda _context, _binding, _state, action: events.append(action),
        raising=False,
    )
    monkeypatch.setattr(
        module,
        "_task8_advance_guard",
        lambda _context, _binding, candidate, field: candidate["active_transaction"]["guard"].__setitem__(field, True),
        raising=False,
    )
    monkeypatch.setattr(module, "_require_backup_service_inactive", lambda _context: None)

    def restore(_context: object, baseline: dict[str, object], *, before_action=None) -> None:
        if baseline["timer_enabled"]:
            before_action("timer_enable")
        if baseline["timer_active"]:
            before_action("timer_start")
        before_action("restore_runtime_readback")

    monkeypatch.setattr(module, "_restore_backup_timer", restore)

    module._task8_restore_guard_timer(context, object(), state)

    assert tuple(events) == expected_actions
    assert transaction["guard"]["timer_restored"] is True


def test_task8_release_uncertainty_blocks_timer_restore(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "probing")
    task8_mark_guard_through(state, "runtime_lock_acquired")
    context = task8_guard_test_context(operation_dir, 1_750_000_051)
    restored: list[bool] = []
    issue = module._Task7LockReleaseIssue(
        "release_runtime_lock",
        OSError("controlled unlock uncertainty"),
        True,
    )
    monkeypatch.setattr(module, "_task8_force_checkpoint", lambda *_args: None, raising=False)
    monkeypatch.setattr(module, "_release_migration_locks", lambda *_args, **_kwargs: [issue])
    monkeypatch.setattr(module, "_restore_backup_timer", lambda *_args, **_kwargs: restored.append(True))

    with pytest.raises(
        module.OperationStateError,
        match="release|lock|uncertain|runtime",
    ):
        module._task8_release_guard_locks(
            context,
            object(),
            state,
            module.MigrationLocks(legacy_fd=101, runtime_fd=202),
        )
    assert state["active_transaction"]["guard"]["locks_released"] is False

    with pytest.raises(
        module.OperationStateError,
        match="lock|release|guard|restore",
    ):
        module._task8_restore_guard_timer(context, object(), state)
    assert restored == []


@pytest.mark.parametrize("noop_action", ["disable-timer", "stop-timer"])
def test_task8_guard_rejects_stubborn_quiesce_success_without_readback_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    noop_action: str,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(module, tmp_path)
    state = state_at_phase(context.paths.operation_dir, "probing")
    state["active_transaction"]["runtime_baseline"] = copy.deepcopy(state["prior_runtime"])
    state["active_transaction"]["prior_timer_enabled"] = state["prior_runtime"]["timer_enabled"]
    state["active_transaction"]["prior_timer_active"] = state["prior_runtime"]["timer_active"]
    fixture["controls"]["noop_command"] = noop_action
    advanced: list[str] = []
    monkeypatch.setattr(module, "_task8_force_checkpoint", lambda *_args: None, raising=False)
    monkeypatch.setattr(module, "_task8_advance_guard", lambda *_args: advanced.append(str(_args[-1])), raising=False)

    with pytest.raises(module.OperationStateError, match="timer|runtime|state"):
        module._task8_acquire_guard(context, object(), state)

    assert advanced == []


def test_task8_task7_primitives_keep_optional_callback_compatibility() -> None:
    module = load_ops_helper()
    expectations = {
        "_quiesce_backup_timer": ("before_action",),
        "acquire_migration_locks": ("before_action", "after_action"),
        "_release_migration_locks": ("before_action",),
        "_restore_backup_timer": ("before_action",),
    }

    for name, callback_names in expectations.items():
        parameters = inspect.signature(getattr(module, name)).parameters
        for callback_name in callback_names:
            assert callback_name in parameters, f"{name} lacks {callback_name} callback"
            assert parameters[callback_name].default is None


def test_task8_task7_timer_callbacks_run_immediately_before_each_action(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    baseline = recovery_runtime_baseline()
    context = object()
    events: list[str] = []
    quiesced = copy.deepcopy(baseline)
    quiesced["timer_enabled"] = False
    quiesced["timer_active"] = False

    monkeypatch.setattr(
        module,
        "_task7_systemctl",
        lambda _context, action: events.append(f"action:timer_{action}"),
    )
    monkeypatch.setattr(
        module,
        "_require_backup_service_inactive",
        lambda _context: events.append("action:quiesce_service_check"),
    )
    monkeypatch.setattr(
        module,
        "_capture_prior_runtime",
        lambda _context: events.append("action:quiesce_runtime_readback") or quiesced,
    )
    callback = lambda action: events.append(f"checkpoint:{action}")

    module._quiesce_backup_timer(context, baseline, before_action=callback)

    assert events == [
        "checkpoint:timer_disable",
        "action:timer_disable",
        "checkpoint:timer_stop",
        "action:timer_stop",
        "checkpoint:quiesce_service_check",
        "action:quiesce_service_check",
        "checkpoint:quiesce_runtime_readback",
        "action:quiesce_runtime_readback",
    ]

    events.clear()
    monkeypatch.setattr(
        module,
        "_require_backup_service_inactive",
        lambda _context: events.append("action:pre_restore_service_check"),
    )
    monkeypatch.setattr(
        module,
        "_capture_prior_runtime",
        lambda _context: events.append("action:restore_runtime_readback") or baseline,
    )

    module._restore_backup_timer(context, baseline, before_action=callback)

    assert events == [
        "checkpoint:timer_enable",
        "action:timer_enable",
        "checkpoint:timer_start",
        "action:timer_start",
        "checkpoint:restore_runtime_readback",
        "action:restore_runtime_readback",
    ]


def test_task8_task7_timer_primitives_omit_callbacks_without_behavior_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    baseline = recovery_runtime_baseline()
    quiesced = copy.deepcopy(baseline)
    quiesced["timer_enabled"] = False
    quiesced["timer_active"] = False
    events: list[str] = []
    monkeypatch.setattr(
        module,
        "_task7_systemctl",
        lambda _context, action: events.append(action),
    )
    monkeypatch.setattr(module, "_require_backup_service_inactive", lambda _context: None)
    monkeypatch.setattr(module, "_capture_prior_runtime", lambda _context: quiesced)

    module._quiesce_backup_timer(object(), baseline)

    assert events == ["disable", "stop"]
    events.clear()
    monkeypatch.setattr(module, "_capture_prior_runtime", lambda _context: baseline)

    module._restore_backup_timer(object(), baseline)

    assert events == ["enable", "start"]


@pytest.mark.skipif(os.name == "posix", reason="exercises the Windows fake lock seam")
def test_task8_windows_fake_lock_seam_reports_callbacks_with_live_descriptors(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    context, _fixture = task7_transaction_fixture(module, tmp_path)
    events: list[str] = []
    descriptors: list[int] = []

    def after(action: str, descriptor: int) -> None:
        os.fstat(descriptor)
        events.append(f"after:{action}")
        descriptors.append(descriptor)

    locks = module.acquire_migration_locks(
        context,
        before_action=lambda action: events.append(f"before:{action}"),
        after_action=after,
    )
    try:
        assert events == [
            "before:legacy_lock_acquire",
            "after:legacy_lock_acquire",
            "before:runtime_lock_acquire",
            "after:runtime_lock_acquire",
            "before:post_lock_service_recheck",
        ]
        assert descriptors == [locks.legacy_fd, locks.runtime_fd]
        os.fstat(locks.legacy_fd)
        os.fstat(locks.runtime_fd)
    finally:
        os.close(locks.runtime_fd)
        os.close(locks.legacy_fd)


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX flock descriptors")
def test_task8_posix_lock_callbacks_observe_live_fds_and_reverse_release(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    context, _fixture = task7_transaction_fixture(module, tmp_path)
    legacy_path = host_root_path(
        context.host_root,
        "/run/lock/degen-prod-db-backup.lock",
    )
    runtime_path = host_root_path(
        context.host_root,
        "/run/degen-prod-db-backup/backup.lock",
    )
    events: list[str] = []
    acquired: list[int] = []

    def after_acquire(action: str, descriptor: int) -> None:
        os.fstat(descriptor)
        assert task8_competing_process_can_lock(legacy_path) is False
        if action == "runtime_lock_acquire":
            assert task8_competing_process_can_lock(runtime_path) is False
        acquired.append(descriptor)
        events.append(f"after:{action}")

    locks = module.acquire_migration_locks(
        context,
        before_action=lambda action: events.append(f"before:{action}"),
        after_action=after_acquire,
    )

    def before_release(action: str) -> None:
        if action == "runtime_lock_release":
            os.fstat(locks.runtime_fd)
            os.fstat(locks.legacy_fd)
            assert task8_competing_process_can_lock(runtime_path) is False
            assert task8_competing_process_can_lock(legacy_path) is False
        else:
            with pytest.raises(OSError):
                os.fstat(locks.runtime_fd)
            os.fstat(locks.legacy_fd)
            assert task8_competing_process_can_lock(runtime_path) is True
            assert task8_competing_process_can_lock(legacy_path) is False
        events.append(f"before:{action}")

    issues = module._release_migration_locks(locks, before_action=before_release)

    assert issues == []
    assert acquired == [locks.legacy_fd, locks.runtime_fd]
    assert events == [
        "before:legacy_lock_acquire",
        "after:legacy_lock_acquire",
        "before:runtime_lock_acquire",
        "after:runtime_lock_acquire",
        "before:post_lock_service_recheck",
        "before:runtime_lock_release",
        "before:legacy_lock_release",
    ]
    for descriptor in acquired:
        with pytest.raises(OSError):
            os.fstat(descriptor)
    assert task8_competing_process_can_lock(runtime_path) is True
    assert task8_competing_process_can_lock(legacy_path) is True


@pytest.mark.parametrize(
    "failure_point",
    [
        "before:legacy_lock_acquire",
        "after:legacy_lock_acquire",
        "before:runtime_lock_acquire",
        "after:runtime_lock_acquire",
        "before:post_lock_service_recheck",
    ],
)
def test_task8_lock_acquire_callback_failure_unwinds_every_open_descriptor(
    tmp_path: Path,
    failure_point: str,
) -> None:
    module = load_ops_helper()
    context, _fixture = task7_transaction_fixture(module, tmp_path)
    legacy_path = host_root_path(
        context.host_root,
        "/run/lock/degen-prod-db-backup.lock",
    )
    runtime_path = host_root_path(
        context.host_root,
        "/run/degen-prod-db-backup/backup.lock",
    )
    opened: list[int] = []
    returned_locks = None

    def before(action: str) -> None:
        if failure_point == f"before:{action}":
            raise Task8CallbackFailure(failure_point)

    def after(action: str, descriptor: int) -> None:
        opened.append(descriptor)
        if failure_point == f"after:{action}":
            raise Task8CallbackFailure(failure_point)

    try:
        with pytest.raises(Task8CallbackFailure, match=failure_point):
            returned_locks = module.acquire_migration_locks(
                context,
                before_action=before,
                after_action=after,
            )
        assert returned_locks is None
        for descriptor in opened:
            with pytest.raises(OSError):
                os.fstat(descriptor)
        if os.name == "posix":
            for path in (legacy_path, runtime_path):
                if path.exists():
                    assert task8_competing_process_can_lock(path) is True
    finally:
        for descriptor in opened:
            task8_close_if_open(descriptor)
        if returned_locks is not None:
            task8_close_if_open(returned_locks.runtime_fd)
            task8_close_if_open(returned_locks.legacy_fd)


@pytest.mark.parametrize(
    "failure_action",
    ["runtime_lock_release", "legacy_lock_release"],
)
def test_task8_release_checkpoint_failure_unwinds_both_locks_and_blocks_restore(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_action: str,
) -> None:
    module = load_ops_helper()
    context, _fixture = task7_transaction_fixture(module, tmp_path)
    state = state_at_phase(context.paths.operation_dir, "probing")
    task8_mark_guard_through(state, "runtime_lock_acquired")
    legacy_path = host_root_path(
        context.host_root,
        "/run/lock/degen-prod-db-backup.lock",
    )
    runtime_path = host_root_path(
        context.host_root,
        "/run/degen-prod-db-backup/backup.lock",
    )
    locks = module.acquire_migration_locks(context)
    advanced: list[str] = []
    restore_calls: list[bool] = []

    def fail_checkpoint(
        _context: object,
        _binding: object,
        _state: object,
        action: str,
    ) -> None:
        if action == failure_action:
            raise Task8CallbackFailure(action)

    monkeypatch.setattr(module, "_task8_force_checkpoint", fail_checkpoint, raising=False)
    monkeypatch.setattr(
        module,
        "_task8_advance_guard",
        lambda _context, _binding, _state, field: advanced.append(field),
        raising=False,
    )
    monkeypatch.setattr(
        module,
        "_restore_backup_timer",
        lambda *_args, **_kwargs: restore_calls.append(True),
    )
    try:
        with pytest.raises(Task8CallbackFailure, match=failure_action):
            module._task8_release_guard_locks(context, object(), state, locks)
        for descriptor in (locks.runtime_fd, locks.legacy_fd):
            with pytest.raises(OSError):
                os.fstat(descriptor)
        if os.name == "posix":
            assert task8_competing_process_can_lock(runtime_path) is True
            assert task8_competing_process_can_lock(legacy_path) is True
        assert advanced == []
        assert state["active_transaction"]["guard"]["locks_released"] is False
        with pytest.raises(
            module.OperationStateError,
            match="lock|release|guard|restore",
        ):
            module._task8_restore_guard_timer(context, object(), state)
        assert restore_calls == []
    finally:
        task8_close_if_open(locks.runtime_fd)
        task8_close_if_open(locks.legacy_fd)


def test_task8_internal_release_hook_issue_is_distinct_and_blocks_progress(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context, _fixture = task7_transaction_fixture(module, tmp_path)
    state = state_at_phase(context.paths.operation_dir, "probing")
    task8_mark_guard_through(state, "runtime_lock_acquired")
    locks = module.acquire_migration_locks(context)
    restore_calls: list[bool] = []
    monkeypatch.setattr(module, "_task8_force_checkpoint", lambda *_args: None, raising=False)
    monkeypatch.setattr(
        module,
        "_task8_advance_guard",
        lambda *_args: pytest.fail("hook-only release issue advanced guard progress"),
        raising=False,
    )

    def fail_internal_hook(event: str, **_details: object) -> None:
        if event == "task7_before_runtime_lock_release":
            raise module.OperationStateError("controlled internal release hook")

    monkeypatch.setattr(module, "_atomic_event_hook", fail_internal_hook)
    monkeypatch.setattr(
        module,
        "_restore_backup_timer",
        lambda *_args, **_kwargs: restore_calls.append(True),
    )
    try:
        with pytest.raises(module.OperationStateError) as caught:
            module._task8_release_guard_locks(context, object(), state, locks)
        message = str(caught.value).lower()
        assert "controlled internal release hook" in message
        assert "uncertain" not in message
        for descriptor in (locks.runtime_fd, locks.legacy_fd):
            with pytest.raises(OSError):
                os.fstat(descriptor)
        assert state["active_transaction"]["guard"]["locks_released"] is False
        with pytest.raises(
            module.OperationStateError,
            match="lock|release|guard|restore",
        ):
            module._task8_restore_guard_timer(context, object(), state)
        assert restore_calls == []
    finally:
        task8_close_if_open(locks.runtime_fd)
        task8_close_if_open(locks.legacy_fd)


@pytest.mark.parametrize("operation", ["acquire", "restore"])
@pytest.mark.parametrize("mismatch", ["protected-pid", "trigger"])
def test_task8_guard_rejects_full_runtime_baseline_mismatch_without_milestone(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    mismatch: str,
) -> None:
    module = load_ops_helper()
    context, _fixture = task7_transaction_fixture(module, tmp_path)
    state = state_at_phase(context.paths.operation_dir, "probing")
    baseline = copy.deepcopy(state["prior_runtime"])
    transaction = state["active_transaction"]
    transaction["runtime_baseline"] = copy.deepcopy(baseline)
    transaction["prior_timer_enabled"] = baseline["timer_enabled"]
    transaction["prior_timer_active"] = baseline["timer_active"]
    if operation == "restore":
        task8_mark_guard_through(state, "locks_released")
        observed = copy.deepcopy(baseline)
    else:
        observed = copy.deepcopy(baseline)
        observed["timer_enabled"] = False
        observed["timer_active"] = False
    if mismatch == "protected-pid":
        observed["pids"]["system:degen-web.service"] += 1
    else:
        trigger = observed["preinstall_trigger_epoch"]
        observed["preinstall_trigger_epoch"] = 1 if trigger is None else trigger + 1
    advanced: list[str] = []
    monkeypatch.setattr(module, "_task8_force_checkpoint", lambda *_args: None, raising=False)
    monkeypatch.setattr(
        module,
        "_task8_advance_guard",
        lambda _context, _binding, _state, field: advanced.append(field),
        raising=False,
    )
    monkeypatch.setattr(module, "_capture_prior_runtime", lambda _context: observed)

    with pytest.raises(module.OperationStateError, match="runtime|timer|baseline|state"):
        if operation == "acquire":
            module._task8_acquire_guard(context, object(), state)
        else:
            module._task8_restore_guard_timer(context, object(), state)

    assert advanced == []
    guard = transaction["guard"]
    if operation == "acquire":
        assert guard["timer_stopped"] is False
        assert guard["service_inactive_verified"] is False
    else:
        assert guard["timer_restored"] is False


@pytest.mark.parametrize(
    ("failure", "timer_enabled", "timer_active"),
    [
        ("enable-timer", True, False),
        ("start-timer", False, True),
        ("final-readback", False, False),
    ],
)
def test_task8_restore_wrapper_propagates_stubborn_timer_or_readback_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
    timer_enabled: bool,
    timer_active: bool,
) -> None:
    module = load_ops_helper()
    context, fixture = task7_transaction_fixture(
        module,
        tmp_path,
        timer_enabled=timer_enabled,
        timer_active=timer_active,
    )
    state = state_at_phase(context.paths.operation_dir, "probing")
    persisted = module.load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    baseline = copy.deepcopy(persisted["prior_runtime"])
    transaction = state["active_transaction"]
    transaction["runtime_baseline"] = copy.deepcopy(baseline)
    transaction["prior_timer_enabled"] = baseline["timer_enabled"]
    transaction["prior_timer_active"] = baseline["timer_active"]
    task8_mark_guard_through(state, "locks_released")
    module._quiesce_backup_timer(context, baseline)
    if failure in {"enable-timer", "start-timer"}:
        fixture["controls"]["noop_command"] = failure
    else:
        observed = copy.deepcopy(baseline)
        observed["pids"]["system:degen-worker.service"] += 1
        monkeypatch.setattr(module, "_capture_prior_runtime", lambda _context: observed)
    advanced: list[str] = []
    monkeypatch.setattr(module, "_task8_force_checkpoint", lambda *_args: None, raising=False)
    monkeypatch.setattr(
        module,
        "_task8_advance_guard",
        lambda _context, _binding, _state, field: advanced.append(field),
        raising=False,
    )

    with pytest.raises(module.OperationStateError, match="timer|runtime|state"):
        module._task8_restore_guard_timer(context, object(), state)

    assert advanced == []
    assert transaction["guard"]["timer_restored"] is False


TASK8_GUARD_RECOVERY_CASES = (
    (
        "dry_run",
        "guard_dry_run",
        "dry_run_recording",
        "probed",
        "dry_run",
    ),
    (
        "observe",
        "guard_observe",
        "observing",
        "policy_enabled",
        "observation",
    ),
)

TASK8_GUARD_RECOVERY_MILESTONES = (
    None,
    "timer_stopped",
    "service_inactive_verified",
    "legacy_lock_acquired",
    "runtime_lock_acquired",
    "locks_released",
    "timer_restored",
)


class Task8GuardProcessDeath(BaseException):
    pass


def task8_guard_recovery_primary_error(transaction_kind: str) -> str:
    return (
        f"interrupted Task 8 {transaction_kind} transaction requires guard recovery"
    )


def task8_guard_recovery_state(
    operation_dir: Path,
    state_kind: str,
    transaction_kind: str,
    raw_phase: str,
    phase: str,
    milestone: str | None = None,
) -> dict[str, object]:
    if phase == raw_phase:
        state = state_at_phase(operation_dir, raw_phase)
    else:
        state = failed_transaction_state(operation_dir, state_kind, phase)
        failure_epoch = int(state["recovery"]["started_epoch"])
        failure = {
            "phase": raw_phase,
            "primary_error": task8_guard_recovery_primary_error(transaction_kind),
            "epoch": failure_epoch,
        }
        failure["evidence_sha256"] = task7_error_evidence_sha256(
            "primary",
            failure,
        )
        state["failure"] = failure
    if milestone is not None:
        task8_mark_guard_through(state, milestone)
    return state


def task8_install_guard_recovery_harness(
    module: object,
    monkeypatch: pytest.MonkeyPatch,
    initial: dict[str, object],
    operation_dir: Path,
) -> tuple[object, dict[str, object]]:
    holder: dict[str, object] = {"state": copy.deepcopy(initial)}
    writes: list[dict[str, object]] = []
    proof_entries: list[tuple[str, frozenset[str]]] = []
    proof_routes: list[str] = []
    proof_states: list[dict[str, object]] = []
    timeline: list[str] = []
    external_actions: list[str] = []
    release_actions: list[str] = []
    command_calls: list[tuple[str, ...]] = []
    controls: dict[str, object] = {
        "crash_after": None,
        "error_after": None,
        "release_issues": [],
        "write_error_after": None,
        "checkpoint_error_action": None,
        "substitute_proof_phase": None,
        "write_crash_after_phase": None,
        "write_crash_after_guard_field": None,
        "write_crash_after_probe_progress": None,
    }
    transaction = initial["active_transaction"]
    if isinstance(transaction, dict):
        baseline = copy.deepcopy(transaction["runtime_baseline"])
        guard = transaction["guard"]
        assert isinstance(guard, dict)
    else:
        baseline = copy.deepcopy(initial["prior_runtime"])
        assert isinstance(baseline, dict)
        guard = {
            "timer_stopped": False,
            "timer_restored": False,
        }
    live_runtime = copy.deepcopy(baseline)
    if guard["timer_stopped"] and not guard["timer_restored"]:
        live_runtime["timer_enabled"] = False
        live_runtime["timer_active"] = False
    epoch = max(
        int(entry["epoch"])
        for entry in initial["phase_history"]
    )
    epoch_counter = iter(range(epoch + 1, epoch + 500))

    def clock() -> datetime:
        return datetime.fromtimestamp(next(epoch_counter), tz=timezone.utc)

    def command_runner(
        argv: object,
        _pass_fds: tuple[int, ...],
    ) -> subprocess.CompletedProcess[str]:
        argv_tuple = tuple(str(value) for value in argv)
        command_calls.append(argv_tuple)
        raise AssertionError(
            f"guard recovery must not invoke rclone/delete/purge: {argv_tuple!r}"
        )

    context = types.SimpleNamespace(
        operation_id=operation_dir.name,
        paths=types.SimpleNamespace(
            operation_dir=operation_dir,
            state_file=operation_dir / "operation-state.json",
            source_dir=operation_dir / "source",
        ),
        effective_uid=os.geteuid() if hasattr(os, "geteuid") else 0,
        clock=clock,
        command_runner=command_runner,
    )
    binding = object()

    @contextlib.contextmanager
    def open_operation_transaction(_context: object):
        yield binding

    @contextlib.contextmanager
    def open_verified_material(
        _context: object,
        allowed_phases: frozenset[str],
        *,
        helper_route: str = "source",
    ):
        current = copy.deepcopy(holder["state"])
        phase = str(current["phase"])
        assert phase in allowed_phases
        proof_entries.append((phase, allowed_phases))
        proof_routes.append(helper_route)
        proof_states.append(copy.deepcopy(current))
        timeline.append(f"proof:{phase}")
        if controls["substitute_proof_phase"] == phase:
            current["operation_id"] = str(current["operation_id"]) + "-substituted"
        yield types.SimpleNamespace(
            source=types.SimpleNamespace(state=current),
        )

    def load_state(_context: object) -> dict[str, object]:
        return copy.deepcopy(holder["state"])

    def write_state(
        _context: object,
        _binding: object,
        candidate: dict[str, object],
        **_kwargs: object,
    ) -> None:
        previous = copy.deepcopy(holder["state"])
        module.validate_operation_state(candidate, operation_dir, previous)
        durable = copy.deepcopy(candidate)
        holder["state"] = durable
        writes.append(durable)
        timeline.append(f"write:{durable['phase']}")
        crash_field = controls["write_crash_after_guard_field"]
        durable_transaction = durable.get("active_transaction")
        if crash_field is not None and isinstance(durable_transaction, dict):
            durable_guard = durable_transaction.get("guard")
            previous_transaction = previous.get("active_transaction")
            previous_guard = (
                previous_transaction.get("guard")
                if isinstance(previous_transaction, dict)
                else None
            )
            if (
                isinstance(durable_guard, dict)
                and isinstance(previous_guard, dict)
                and durable_guard.get(crash_field) is True
                and previous_guard.get(crash_field) is False
            ):
                controls["write_crash_after_guard_field"] = None
                raise Task8GuardProcessDeath(
                    f"durable write after {crash_field}"
                )
        crash_progress = controls["write_crash_after_probe_progress"]
        if crash_progress is not None and isinstance(durable_transaction, dict):
            crash_name, crash_progress_field = crash_progress
            durable_probe = durable_transaction.get("probe")
            previous_transaction = previous.get("active_transaction")
            previous_probe = (
                previous_transaction.get("probe")
                if isinstance(previous_transaction, dict)
                else None
            )
            durable_objects = (
                durable_probe.get("objects") if isinstance(durable_probe, dict) else None
            )
            previous_objects = (
                previous_probe.get("objects") if isinstance(previous_probe, dict) else None
            )
            if isinstance(durable_objects, list) and isinstance(previous_objects, list):
                durable_item = next(
                    (item for item in durable_objects if item.get("name") == crash_name),
                    None,
                )
                previous_item = next(
                    (item for item in previous_objects if item.get("name") == crash_name),
                    None,
                )
                if (
                    isinstance(durable_item, dict)
                    and isinstance(previous_item, dict)
                    and previous_item.get(crash_progress_field) is False
                    and durable_item.get(crash_progress_field) is True
                ):
                    controls["write_crash_after_probe_progress"] = None
                    raise Task8ProbeProcessDeath(
                        f"durable write after {crash_name} {crash_progress_field}"
                    )
        crash_phase = controls["write_crash_after_phase"]
        if crash_phase is not None and durable["phase"] == crash_phase:
            controls["write_crash_after_phase"] = None
            raise Task8GuardProcessDeath(
                f"durable write after {crash_phase}"
            )
        failure_phase = controls["write_error_after"]
        if failure_phase is not None and durable["phase"] == failure_phase:
            controls["write_error_after"] = None
            raise module.OperationStateError(
                f"controlled checkpoint failure after {failure_phase}"
            )

    def maybe_fail(action: str) -> None:
        external_actions.append(action)
        timeline.append(f"action:{action}")
        if controls["crash_after"] == action:
            raise Task8GuardProcessDeath(action)
        if controls["error_after"] == action:
            controls["error_after"] = None
            raise module.OperationStateError(f"controlled guard error after {action}")

    def quiesce(
        _context: object,
        _baseline: dict[str, object],
        *,
        before_action=None,
    ) -> None:
        for action in (
            "timer_disable",
            "timer_stop",
            "quiesce_service_check",
            "quiesce_runtime_readback",
        ):
            if before_action is not None:
                before_action(action)
            if action == "timer_disable":
                live_runtime["timer_enabled"] = False
            elif action == "timer_stop":
                live_runtime["timer_active"] = False
            maybe_fail(action)
        expected = copy.deepcopy(_baseline)
        expected["timer_enabled"] = False
        expected["timer_active"] = False
        if live_runtime != expected:
            raise module.OperationStateError("controlled quiesce runtime mismatch")

    fresh_acquisitions: list[tuple[int, int]] = []

    def acquire(
        _context: object,
        *,
        before_action=None,
        after_action=None,
    ) -> object:
        pair = (1001 + len(fresh_acquisitions) * 2, 1002 + len(fresh_acquisitions) * 2)
        fresh_acquisitions.append(pair)
        for action, descriptor in (
            ("legacy_lock_acquire", pair[0]),
            ("runtime_lock_acquire", pair[1]),
        ):
            if before_action is not None:
                before_action(action)
            maybe_fail(action)
            if after_action is not None:
                after_action(action, descriptor)
        if before_action is not None:
            before_action("post_lock_service_recheck")
        maybe_fail("post_lock_service_recheck")
        return module.MigrationLocks(legacy_fd=pair[0], runtime_fd=pair[1])

    def release(_locks: object, *, before_action=None) -> list[object]:
        for action in ("runtime_lock_release", "legacy_lock_release"):
            if before_action is not None:
                before_action(action)
            release_actions.append(action)
            maybe_fail(action)
        return list(controls["release_issues"])

    def require_service_inactive(_context: object) -> None:
        maybe_fail("pre_restore_service_check")

    def restore(
        _context: object,
        expected: dict[str, object],
        *,
        before_action=None,
    ) -> None:
        if expected["timer_enabled"]:
            if before_action is not None:
                before_action("timer_enable")
            live_runtime["timer_enabled"] = True
            maybe_fail("timer_enable")
        if expected["timer_active"]:
            if before_action is not None:
                before_action("timer_start")
            live_runtime["timer_active"] = True
            maybe_fail("timer_start")
        if before_action is not None:
            before_action("restore_runtime_readback")
        maybe_fail("restore_runtime_readback")
        if live_runtime != expected:
            raise module.OperationStateError("controlled exact runtime restore mismatch")

    monkeypatch.setattr(module, "_open_operation_transaction", open_operation_transaction)
    monkeypatch.setattr(module, "_open_verified_transaction_material", open_verified_material)
    monkeypatch.setattr(module, "_revalidate_transaction_material", lambda _material: None)
    monkeypatch.setattr(module, "_task7_load_state", load_state)
    monkeypatch.setattr(module, "_task7_write_state", write_state)
    monkeypatch.setattr(module, "_capture_prior_runtime", lambda _context: copy.deepcopy(live_runtime))
    monkeypatch.setattr(module, "_quiesce_backup_timer", quiesce)
    monkeypatch.setattr(module, "_require_backup_service_inactive", require_service_inactive)
    monkeypatch.setattr(module, "acquire_migration_locks", acquire)
    monkeypatch.setattr(module, "_release_migration_locks", release)
    monkeypatch.setattr(module, "_restore_backup_timer", restore)
    monkeypatch.setattr(module, "_task7_emergency_close_locks", lambda _locks: None)
    original_force_checkpoint = module._task8_force_checkpoint

    def force_checkpoint(
        _context: object,
        _binding: object,
        state: dict[str, object],
        action: str,
    ) -> None:
        if controls["checkpoint_error_action"] == action:
            controls["checkpoint_error_action"] = None
            raise module.OperationStateError(
                f"controlled checkpoint uncertainty before {action}"
            )
        original_force_checkpoint(_context, _binding, state, action)

    monkeypatch.setattr(module, "_task8_force_checkpoint", force_checkpoint)

    return context, {
        "holder": holder,
        "writes": writes,
        "proof_entries": proof_entries,
        "proof_routes": proof_routes,
        "proof_states": proof_states,
        "timeline": timeline,
        "external_actions": external_actions,
        "release_actions": release_actions,
        "command_calls": command_calls,
        "controls": controls,
        "baseline": baseline,
        "live_runtime": live_runtime,
        "fresh_acquisitions": fresh_acquisitions,
    }


def test_task8_guard_recovery_refuses_tampered_dry_run_entry_before_blessing_probed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    initial = task8_guard_recovery_state(
        operation_dir,
        "guard_dry_run",
        "dry_run",
        "dry_run_recording",
        "recovering_guard",
    )
    entry = next(
        item
        for item in reversed(initial["phase_history"])
        if item["phase"] == "dry_run_recording"
    )
    assert entry["evidence_sha256"] != HASH_D
    entry["evidence_sha256"] = HASH_D
    context, harness = task8_install_guard_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
    )
    harness_open_verified = module._open_verified_transaction_material

    @contextlib.contextmanager
    def validating_open_verified(
        open_context: object,
        allowed_phases: frozenset[str],
        *,
        helper_route: str = "source",
    ):
        current = copy.deepcopy(harness["holder"]["state"])
        module.validate_operation_state(current, operation_dir)
        with harness_open_verified(
            open_context,
            allowed_phases,
            helper_route=helper_route,
        ) as material:
            yield material

    monkeypatch.setattr(
        module,
        "_open_verified_transaction_material",
        validating_open_verified,
    )

    with pytest.raises(
        module.OperationStateError,
        match=r"dry[_ -]?run.*entry.*evidence|entry.*evidence.*dry[_ -]?run|provenance",
    ):
        module.recover_host_configuration(context)

    durable = harness["holder"]["state"]
    assert durable == initial
    assert durable["phase"] == "recovering_guard"
    assert durable["active_transaction"] is not None
    assert durable["recovery"]["completed_epoch"] is None
    assert harness["writes"] == []
    assert harness["external_actions"] == []
    assert harness["command_calls"] == []


@pytest.mark.parametrize(
    ("transaction_kind", "state_kind", "raw_phase", "prior_phase", "later_receipt"),
    TASK8_GUARD_RECOVERY_CASES,
)
def test_task8_raw_guard_recovery_records_failure_then_separate_recovery_entry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    transaction_kind: str,
    state_kind: str,
    raw_phase: str,
    prior_phase: str,
    later_receipt: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    initial = task8_guard_recovery_state(
        operation_dir,
        state_kind,
        transaction_kind,
        raw_phase,
        raw_phase,
    )
    context, harness = task8_install_guard_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
    )

    result = module.recover_host_configuration(context)

    writes = harness["writes"]
    required_index = next(
        index for index, state in enumerate(writes) if state["phase"] == "recovery_required"
    )
    recovering_index = next(
        index for index, state in enumerate(writes) if state["phase"] == "recovering_guard"
    )
    assert required_index < recovering_index
    required = writes[required_index]
    expected_primary = task8_guard_recovery_primary_error(transaction_kind)
    assert required["failure"]["phase"] == raw_phase
    assert required["failure"]["primary_error"] == expected_primary
    assert required["failure"]["evidence_sha256"] == task7_error_evidence_sha256(
        "primary",
        required["failure"],
    )
    recovery = required["recovery"]
    assert recovery["kind"] == "guard"
    assert recovery["runtime_baseline"] == initial["active_transaction"]["runtime_baseline"]
    assert recovery["started_epoch"] == required["phase_history"][-1]["epoch"]
    assert writes[recovering_index]["recovery"] == recovery
    assert harness["proof_entries"]
    final = harness["holder"]["state"]
    assert result == final
    assert final["phase"] == prior_phase
    assert final["active_transaction"] is None
    assert final[later_receipt] is None
    assert final["failure"]["primary_error"] == expected_primary
    assert final["recovery"]["kind"] == "guard"
    assert final["recovery"]["completed_epoch"] is not None
    assert harness["command_calls"] == []


@pytest.mark.parametrize(
    ("transaction_kind", "state_kind", "raw_phase", "prior_phase", "later_receipt"),
    TASK8_GUARD_RECOVERY_CASES,
)
@pytest.mark.parametrize("milestone", TASK8_GUARD_RECOVERY_MILESTONES)
def test_task8_guard_recovery_resumes_every_historical_milestone_with_fresh_locks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    transaction_kind: str,
    state_kind: str,
    raw_phase: str,
    prior_phase: str,
    later_receipt: str,
    milestone: str | None,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    initial = task8_guard_recovery_state(
        operation_dir,
        state_kind,
        transaction_kind,
        raw_phase,
        "recovering_guard",
        milestone,
    )
    original_failure = copy.deepcopy(initial["failure"])
    original_recovery = copy.deepcopy(initial["recovery"])
    context, harness = task8_install_guard_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
    )

    result = module.recover_host_configuration(context)

    final = harness["holder"]["state"]
    assert result == final
    assert final["phase"] == prior_phase
    assert final["active_transaction"] is None
    assert final[later_receipt] is None
    assert final["failure"] == original_failure
    assert final["recovery"]["kind"] == "guard"
    assert final["recovery"]["runtime_baseline"] == original_recovery["runtime_baseline"]
    assert final["recovery"]["started_epoch"] == original_recovery["started_epoch"]
    assert final["recovery"]["evidence_sha256"] == original_recovery["evidence_sha256"]
    assert final["recovery"]["completed_epoch"] is not None
    assert harness["live_runtime"] == harness["baseline"]
    assert harness["command_calls"] == []
    if milestone == "timer_restored":
        assert harness["fresh_acquisitions"] == []
        assert harness["release_actions"] == []
    else:
        assert len(harness["fresh_acquisitions"]) == 1
        assert harness["release_actions"] == [
            "runtime_lock_release",
            "legacy_lock_release",
        ]


@pytest.mark.parametrize(
    ("transaction_kind", "state_kind", "raw_phase", "prior_phase", "later_receipt"),
    TASK8_GUARD_RECOVERY_CASES,
)
def test_task8_recovery_required_guard_dispatches_through_verified_material(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    transaction_kind: str,
    state_kind: str,
    raw_phase: str,
    prior_phase: str,
    later_receipt: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    initial = task8_guard_recovery_state(
        operation_dir,
        state_kind,
        transaction_kind,
        raw_phase,
        "recovery_required",
    )
    context, harness = task8_install_guard_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
    )

    module.recover_host_configuration(context)

    phases = [state["phase"] for state in harness["writes"]]
    assert "recovering_guard" in phases
    assert phases[-1] == prior_phase
    assert harness["proof_entries"][0][0] == "recovery_required"
    assert "recovery_required" in harness["proof_entries"][0][1]
    final = harness["holder"]["state"]
    assert final[later_receipt] is None
    assert final["active_transaction"] is None
    assert harness["command_calls"] == []


@pytest.mark.parametrize(
    ("state_factory", "phase"),
    [
        (install_recovery_state, "recovering"),
        (install_recovery_state, "recovery_required"),
        (manual_rollback_state, "manual_rollback"),
        (manual_rollback_state, "recovery_required"),
    ],
)
def test_task8_guard_dispatcher_preserves_existing_task7_recovery_routing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    state_factory,
    phase: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_factory(operation_dir, phase)
    context = types.SimpleNamespace()
    sentinel = {"task7": phase}
    calls: list[bool] = []
    monkeypatch.setattr(module, "_task7_load_state", lambda _context: state)
    monkeypatch.setattr(
        module,
        "_task7_run_recovery",
        lambda _context, *, manual_request, **_kwargs: calls.append(manual_request)
        or sentinel,
    )

    assert module.recover_host_configuration(context) == sentinel
    assert calls == [False]


def test_task8_guard_dispatcher_preserves_manual_rollback_routing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    sentinel = {"task7": "manual"}
    calls: list[bool] = []
    monkeypatch.setattr(
        module,
        "_task7_run_recovery",
        lambda _context, *, manual_request, **_kwargs: calls.append(manual_request)
        or sentinel,
    )

    assert module.rollback_host_configuration(object()) == sentinel
    assert calls == [True]


@pytest.mark.parametrize(
    ("transaction_kind", "state_kind", "raw_phase", "prior_phase", "later_receipt"),
    TASK8_GUARD_RECOVERY_CASES,
)
def test_task8_guard_recovery_preserves_primary_and_orders_secondary_errors_on_resume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    transaction_kind: str,
    state_kind: str,
    raw_phase: str,
    prior_phase: str,
    later_receipt: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    initial = task8_guard_recovery_state(
        operation_dir,
        state_kind,
        transaction_kind,
        raw_phase,
        "recovering_guard",
    )
    earlier = {
        "stage": "earlier_guard_failure",
        "error": "earlier sanitized failure",
        "epoch": int(initial["recovery"]["started_epoch"]),
    }
    earlier["evidence_sha256"] = task7_error_evidence_sha256(
        "secondary",
        earlier,
    )
    initial["secondary_errors"] = [earlier]
    original_failure = copy.deepcopy(initial["failure"])
    original_recovery = copy.deepcopy(initial["recovery"])
    context, harness = task8_install_guard_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
    )
    harness["controls"]["error_after"] = "pre_restore_service_check"

    with pytest.raises(module.OperationStateError, match="controlled guard error"):
        module.recover_host_configuration(context)

    interrupted = harness["holder"]["state"]
    assert interrupted["phase"] == "recovery_required"
    assert interrupted["failure"] == original_failure
    assert interrupted["recovery"]["completed_epoch"] is None
    assert interrupted["recovery"]["started_epoch"] == original_recovery["started_epoch"]
    assert interrupted["recovery"]["evidence_sha256"] == original_recovery["evidence_sha256"]
    assert interrupted["secondary_errors"][0] == earlier
    assert len(interrupted["secondary_errors"]) >= 2
    assert [item["epoch"] for item in interrupted["secondary_errors"]] == sorted(
        item["epoch"] for item in interrupted["secondary_errors"]
    )
    assert "controlled guard error" in interrupted["secondary_errors"][-1]["error"]
    assert harness["live_runtime"]["timer_enabled"] is False
    assert harness["live_runtime"]["timer_active"] is False

    final = module.recover_host_configuration(context)

    assert final["phase"] == prior_phase
    assert final[later_receipt] is None
    assert final["failure"] == original_failure
    assert final["secondary_errors"][:1] == [earlier]
    assert final["recovery"]["started_epoch"] == original_recovery["started_epoch"]
    assert final["recovery"]["evidence_sha256"] == original_recovery["evidence_sha256"]
    assert final["recovery"]["completed_epoch"] is not None


TASK8_GUARD_RECOVERY_CRASH_BOUNDARIES = (
    ("timer_disable", None),
    ("timer_stop", None),
    ("quiesce_service_check", None),
    ("quiesce_runtime_readback", None),
    ("legacy_lock_acquire", "service_inactive_verified"),
    ("runtime_lock_acquire", "legacy_lock_acquired"),
    ("post_lock_service_recheck", "runtime_lock_acquired"),
    ("runtime_lock_release", "runtime_lock_acquired"),
    ("legacy_lock_release", "runtime_lock_acquired"),
    ("pre_restore_service_check", "locks_released"),
    ("timer_enable", "locks_released"),
    ("timer_start", "locks_released"),
    ("restore_runtime_readback", "locks_released"),
)


@pytest.mark.parametrize(
    ("transaction_kind", "state_kind", "raw_phase", "_prior_phase", "_later_receipt"),
    TASK8_GUARD_RECOVERY_CASES,
)
@pytest.mark.parametrize(
    ("crash_action", "expected_milestone"),
    TASK8_GUARD_RECOVERY_CRASH_BOUNDARIES,
)
def test_task8_guard_recovery_process_death_keeps_last_durable_milestone(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    transaction_kind: str,
    state_kind: str,
    raw_phase: str,
    _prior_phase: str,
    _later_receipt: str,
    crash_action: str,
    expected_milestone: str | None,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    initial = task8_guard_recovery_state(
        operation_dir,
        state_kind,
        transaction_kind,
        raw_phase,
        "recovering_guard",
    )
    context, harness = task8_install_guard_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
    )
    harness["controls"]["crash_after"] = crash_action

    with pytest.raises(Task8GuardProcessDeath, match=crash_action):
        module.recover_host_configuration(context)

    interrupted = harness["holder"]["state"]
    assert interrupted["phase"] == "recovering_guard"
    assert interrupted["active_transaction"] is not None
    assert interrupted["recovery"]["completed_epoch"] is None
    expected = copy.deepcopy(initial)
    if expected_milestone is not None:
        task8_mark_guard_through(expected, expected_milestone)
    assert interrupted["active_transaction"]["guard"] == expected["active_transaction"]["guard"]
    assert interrupted["phase_history"][-1]["phase"] == "recovering_guard"
    assert harness["command_calls"] == []


@pytest.mark.parametrize("uncertainty", ["release", "checkpoint"])
def test_task8_guard_recovery_release_or_checkpoint_uncertainty_stays_quiesced(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    uncertainty: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    initial = task8_guard_recovery_state(
        operation_dir,
        "guard_dry_run",
        "dry_run",
        "dry_run_recording",
        "recovering_guard",
    )
    context, harness = task8_install_guard_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
    )
    if uncertainty == "release":
        harness["controls"]["release_issues"] = [
            module._Task7LockReleaseIssue(
                "release_runtime_lock",
                OSError("controlled release uncertainty"),
                True,
            )
        ]
        expected = "release uncertainty"
    else:
        harness["controls"]["checkpoint_error_action"] = "runtime_lock_release"
        expected = "checkpoint uncertainty"

    with pytest.raises(Exception, match=expected):
        module.recover_host_configuration(context)

    interrupted = harness["holder"]["state"]
    assert interrupted["phase"] == "recovery_required"
    assert interrupted["active_transaction"]["guard"]["locks_released"] is False
    assert interrupted["active_transaction"]["guard"]["timer_restored"] is False
    assert interrupted["recovery"]["completed_epoch"] is None
    assert harness["live_runtime"]["timer_enabled"] is False
    assert harness["live_runtime"]["timer_active"] is False
    assert harness["command_calls"] == []


@pytest.mark.parametrize(
    ("transaction_kind", "state_kind", "raw_phase", "_prior_phase", "_later_receipt"),
    TASK8_GUARD_RECOVERY_CASES,
)
def test_task8_timer_restored_milestone_requires_exact_live_readback_before_finalization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    transaction_kind: str,
    state_kind: str,
    raw_phase: str,
    _prior_phase: str,
    _later_receipt: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    initial = task8_guard_recovery_state(
        operation_dir,
        state_kind,
        transaction_kind,
        raw_phase,
        "recovering_guard",
        "timer_restored",
    )
    context, harness = task8_install_guard_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
    )
    harness["live_runtime"]["timer_active"] = False
    harness["controls"]["crash_after"] = "timer_disable"

    with pytest.raises(Task8GuardProcessDeath, match="timer_disable"):
        module.recover_host_configuration(context)

    interrupted = harness["holder"]["state"]
    assert interrupted["phase"] == "recovering_guard"
    assert interrupted["active_transaction"] is not None
    assert interrupted["recovery"]["completed_epoch"] is None
    assert harness["external_actions"][0] == "timer_disable"
    assert harness["command_calls"] == []


@pytest.mark.parametrize(
    ("transaction_kind", "state_kind", "raw_phase", "prior_phase", "later_receipt"),
    TASK8_GUARD_RECOVERY_CASES,
)
def test_task8_guard_recovery_review_gap_finalizes_pending_rclone_audit_locally(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    transaction_kind: str,
    state_kind: str,
    raw_phase: str,
    prior_phase: str,
    later_receipt: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    initial = task8_guard_recovery_state(
        operation_dir,
        state_kind,
        transaction_kind,
        raw_phase,
        raw_phase,
    )
    started_epoch = int(initial["active_transaction"]["started_epoch"])
    completed_group = task8_rclone_group(
        transaction_kind,
        started_epoch,
        0,
        0,
        purpose=(
            TASK8_DRY_RUN_PURPOSES[0]
            if transaction_kind == "dry_run"
            else (
                TASK8_OBSERVE_PURPOSES[0]
                if transaction_kind == "observe"
                else "remote-list"
            )
        ),
        outcome="indeterminate",
    )
    initial["rclone_evidence_groups"].append(task8_pending_copy(completed_group))
    module.validate_operation_state(initial, operation_dir)
    context, harness = task8_install_guard_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
    )
    local_audits: list[str] = []
    monkeypatch.setattr(
        module,
        "_task7_capture_file_audit",
        lambda _context, path: local_audits.append(path)
        or copy.deepcopy(completed_group["after"]),
    )

    final = module.recover_host_configuration(context)

    assert final["phase"] == prior_phase
    assert final[later_receipt] is None
    assert final["rclone_evidence_groups"][-1] == completed_group
    assert local_audits == ["/etc/degen/rclone.conf"]
    assert harness["command_calls"] == []
    assert not any(
        token in action.casefold()
        for action in harness["external_actions"]
        for token in ("rclone", "delete", "purge")
    )


def test_task8_guard_recovery_review_gap_records_both_release_issues_in_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    initial = task8_guard_recovery_state(
        operation_dir,
        "guard_dry_run",
        "dry_run",
        "dry_run_recording",
        "recovering_guard",
    )
    context, harness = task8_install_guard_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
    )
    runtime_error = OSError("controlled runtime unlock uncertainty")
    legacy_error = OSError("controlled legacy close uncertainty")
    harness["controls"]["release_issues"] = [
        module._Task7LockReleaseIssue(
            "release_runtime_lock",
            runtime_error,
            True,
        ),
        module._Task7LockReleaseIssue(
            "release_legacy_lock",
            legacy_error,
            True,
        ),
    ]

    with pytest.raises(
        Exception,
        match="runtime unlock uncertainty.*legacy close uncertainty",
    ):
        module.recover_host_configuration(context)

    interrupted = harness["holder"]["state"]
    assert interrupted["phase"] == "recovery_required"
    assert interrupted["recovery"]["completed_epoch"] is None
    assert interrupted["active_transaction"]["guard"]["locks_released"] is False
    assert interrupted["active_transaction"]["guard"]["timer_restored"] is False
    assert [
        item["stage"] for item in interrupted["secondary_errors"][-2:]
    ] == ["release_runtime_lock", "release_legacy_lock"]
    assert "runtime unlock uncertainty" in interrupted["secondary_errors"][-2]["error"]
    assert "legacy close uncertainty" in interrupted["secondary_errors"][-1]["error"]
    assert "pre_restore_service_check" not in harness["external_actions"]
    assert "timer_enable" not in harness["external_actions"]
    assert "timer_start" not in harness["external_actions"]
    assert harness["live_runtime"]["timer_enabled"] is False
    assert harness["live_runtime"]["timer_active"] is False


@pytest.mark.parametrize(
    ("transaction_kind", "state_kind", "raw_phase", "_prior_phase", "_later_receipt"),
    TASK8_GUARD_RECOVERY_CASES,
)
def test_task8_guard_recovery_review_gap_rebinds_exact_proof_after_phase_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    transaction_kind: str,
    state_kind: str,
    raw_phase: str,
    _prior_phase: str,
    _later_receipt: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    initial = task8_guard_recovery_state(
        operation_dir,
        state_kind,
        transaction_kind,
        raw_phase,
        raw_phase,
    )
    context, harness = task8_install_guard_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
    )

    module.recover_host_configuration(context)

    timeline = harness["timeline"]
    first_raw_proof = timeline.index(f"proof:{raw_phase}")
    raw_write = timeline.index(f"write:{raw_phase}")
    rebound_raw_proof = timeline.index(f"proof:{raw_phase}", raw_write + 1)
    required_write = timeline.index("write:recovery_required")
    required_proof = timeline.index("proof:recovery_required", required_write + 1)
    recovering_write = timeline.index("write:recovering_guard")
    recovering_proof = timeline.index("proof:recovering_guard", recovering_write + 1)
    first_action = next(
        index for index, event in enumerate(timeline) if event.startswith("action:")
    )
    assert (
        first_raw_proof
        < raw_write
        < rebound_raw_proof
        < required_write
        < required_proof
        < recovering_write
        < recovering_proof
        < first_action
    )
    assert [state["phase"] for state in harness["proof_states"][:4]] == [
        raw_phase,
        raw_phase,
        "recovery_required",
        "recovering_guard",
    ]
    for proof_state in harness["proof_states"]:
        module.validate_operation_state(proof_state, operation_dir)


@pytest.mark.parametrize(
    ("transaction_kind", "state_kind", "raw_phase", "proof_phase"),
    [
        ("dry_run", "guard_dry_run", "dry_run_recording", "dry_run_recording"),
        ("dry_run", "guard_dry_run", "dry_run_recording", "recovery_required"),
        ("dry_run", "guard_dry_run", "dry_run_recording", "recovering_guard"),
        ("observe", "guard_observe", "observing", "observing"),
        ("observe", "guard_observe", "observing", "recovery_required"),
        ("observe", "guard_observe", "observing", "recovering_guard"),
    ],
)
def test_task8_guard_recovery_review_gap_rejects_proof_state_substitution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    transaction_kind: str,
    state_kind: str,
    raw_phase: str,
    proof_phase: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    initial_phase = raw_phase if proof_phase == raw_phase else proof_phase
    initial = task8_guard_recovery_state(
        operation_dir,
        state_kind,
        transaction_kind,
        raw_phase,
        initial_phase,
    )
    context, harness = task8_install_guard_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
    )
    harness["controls"]["substitute_proof_phase"] = proof_phase

    with pytest.raises(
        module.OperationStateError,
        match="proof|material|state|changed|substitut",
    ):
        module.recover_host_configuration(context)

    durable = harness["holder"]["state"]
    if durable != initial:
        assert proof_phase == "recovering_guard"
        assert durable["phase"] == "recovery_required"
        assert durable["active_transaction"] == initial["active_transaction"]
        assert durable["recovery"]["completed_epoch"] is None
        assert durable["failure"] == initial["failure"]
    assert harness["external_actions"] == []
    assert harness["command_calls"] == []


@pytest.mark.parametrize(
    ("transaction_kind", "state_kind", "raw_phase", "prior_phase", "_later_receipt"),
    TASK8_GUARD_RECOVERY_CASES,
)
@pytest.mark.parametrize("boundary", ["timer_restored", "completion"])
def test_task8_guard_recovery_review_gap_durable_write_then_process_death(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    transaction_kind: str,
    state_kind: str,
    raw_phase: str,
    prior_phase: str,
    _later_receipt: str,
    boundary: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    initial_milestone = "locks_released" if boundary == "timer_restored" else "timer_restored"
    initial = task8_guard_recovery_state(
        operation_dir,
        state_kind,
        transaction_kind,
        raw_phase,
        "recovering_guard",
        initial_milestone,
    )
    context, harness = task8_install_guard_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
    )
    if boundary == "timer_restored":
        harness["controls"]["write_crash_after_guard_field"] = "timer_restored"
    else:
        harness["controls"]["write_crash_after_phase"] = prior_phase

    with pytest.raises(Task8GuardProcessDeath, match="durable write"):
        module.recover_host_configuration(context)

    durable = harness["holder"]["state"]
    if boundary == "timer_restored":
        assert durable["phase"] == "recovering_guard"
        assert durable["active_transaction"]["guard"]["timer_restored"] is True
        assert durable["recovery"]["completed_epoch"] is None
        action_count = len(harness["external_actions"])
        final = module.recover_host_configuration(context)
        assert final["phase"] == prior_phase
        assert final["recovery"]["completed_epoch"] is not None
        assert harness["external_actions"][action_count:] == [
            "pre_restore_service_check"
        ]
    else:
        assert durable["phase"] == prior_phase
        assert durable["active_transaction"] is None
        assert durable["recovery"]["completed_epoch"] is not None
    module.validate_operation_state(durable, operation_dir)


class Task8ProbeProcessDeath(BaseException):
    pass


def task8_probe_payloads(
    prefix: str = SAFE_PROBE_PREFIX,
) -> dict[str, bytes]:
    identity = prefix.rstrip("/").rsplit("/", 1)[-1]
    operation_id, separator, token = identity.rpartition("-")
    assert separator == "-" and operation_id and len(token) == 32
    dump = (
        "degen-db-remote-probe-v1\n"
        f"operation_id={operation_id}\n"
        f"token={token}\n"
    ).encode("ascii")
    dump_sha256 = hashlib.sha256(dump).hexdigest()
    return {
        "probe.dump": dump,
        "probe.dump.sha256": f"{dump_sha256}  probe.dump\n".encode("ascii"),
    }


def task8_probe_inventory_json(
    remote_objects: dict[str, bytes],
    *,
    remote_directories: set[str] | None = None,
) -> str:
    rows = [
        {
            "Path": name,
            "Name": name,
            "Size": len(contents),
            "MimeType": "application/octet-stream",
            "ModTime": "2026-07-01T00:00:00Z",
            "IsDir": False,
            "ID": f"test-object-{index}",
            "OrigID": "",
            "Tier": "",
        }
        for index, (name, contents) in enumerate(sorted(remote_objects.items()))
    ]
    rows.extend(
        {
            "Path": name,
            "Name": name.rsplit("/", 1)[-1],
            "Size": -1,
            "MimeType": "inode/directory",
            "ModTime": "2026-07-01T00:00:00Z",
            "IsDir": True,
            "ID": f"test-directory-{index}",
            "OrigID": "",
            "Tier": "",
        }
        for index, name in enumerate(sorted(remote_directories or set()))
    )
    return json.dumps(rows, separators=(",", ":"))


def task8_probe_recovery_primary_error() -> str:
    return "interrupted Task 8 probe transaction requires probe recovery"


def task8_probe_recovery_state(
    operation_dir: Path,
    phase: str,
    *,
    progress: dict[str, tuple[bool, bool, bool]] | None = None,
    milestone: str | None = None,
    include_ownership_evidence: bool = True,
    include_empty_proof: bool = False,
    pending_purpose: str | None = None,
) -> dict[str, object]:
    if phase == "probing":
        state = state_at_phase(operation_dir, phase)
    elif phase in {"recovery_required", "recovering_probe"}:
        state = failed_transaction_state(operation_dir, "probe", phase)
        failure_epoch = int(state["recovery"]["started_epoch"])
        failure = {
            "phase": "probing",
            "primary_error": task8_probe_recovery_primary_error(),
            "epoch": failure_epoch,
        }
        failure["evidence_sha256"] = task7_error_evidence_sha256(
            "primary",
            failure,
        )
        state["failure"] = failure
    else:
        raise ValueError(phase)

    transaction = state["active_transaction"]
    assert isinstance(transaction, dict)
    probe = transaction["probe"]
    assert isinstance(probe, dict)
    objects = probe["objects"]
    assert isinstance(objects, list)
    progress = progress or {}
    for item in objects:
        flags = progress.get(str(item["name"]), (False, False, False))
        item["created"], item["verified"], item["cleaned"] = flags

    groups: list[dict[str, object]] = []
    started_epoch = int(transaction["started_epoch"])

    def completed_group(purpose: str) -> None:
        groups.append(
            task8_rclone_group(
                "probe",
                started_epoch,
                0,
                len(groups),
                purpose=purpose,
            )
        )

    completed_group("probe-precreate-absence")
    for item in objects:
        name = str(item["name"])
        if item["created"] and include_ownership_evidence:
            completed_group(f"probe-create:{name}:strict-no-existing")
        if item["verified"]:
            completed_group(f"probe-verify:{name}")
        if item["cleaned"]:
            completed_group(f"probe-cleanup:{name}")
    if include_empty_proof:
        completed_group("probe-prefix-empty")
    if pending_purpose is not None:
        groups.append(
            task8_pending_rclone_group(
                "probe",
                started_epoch,
                len(groups),
                purpose=pending_purpose,
            )
        )
    state["rclone_evidence_groups"] = groups
    if milestone is not None:
        task8_mark_guard_through(state, milestone)
    return state


def task8_probe_list_argv(prefix: str = SAFE_PROBE_PREFIX) -> tuple[str, ...]:
    return (
        "/usr/bin/rclone",
        "--config",
        "/etc/degen/rclone.conf",
        "lsjson",
        prefix,
        "--recursive",
    )


def task8_probe_hashsum_argv(
    name: str,
    prefix: str = SAFE_PROBE_PREFIX,
) -> tuple[str, ...]:
    return (
        "/usr/bin/rclone",
        "--config",
        "/etc/degen/rclone.conf",
        "hashsum",
        "SHA-256",
        f"{prefix}{name}",
        "--download",
    )


def task8_probe_delete_argv(
    name: str,
    prefix: str = SAFE_PROBE_PREFIX,
) -> tuple[str, ...]:
    return (
        "/usr/bin/rclone",
        "--config",
        "/etc/degen/rclone.conf",
        "deletefile",
        f"{prefix}{name}",
    )


def task8_install_probe_recovery_harness(
    module: object,
    monkeypatch: pytest.MonkeyPatch,
    initial: dict[str, object],
    operation_dir: Path,
    *,
    remote_objects: dict[str, bytes] | None = None,
    remote_directories: set[str] | None = None,
) -> tuple[object, dict[str, object]]:
    context, harness = task8_install_guard_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
    )
    remote_store = {
        name: bytes(contents)
        for name, contents in (remote_objects or {}).items()
    }
    remote_dirs = set(remote_directories or set())
    command_calls = harness["command_calls"]
    command_states: list[dict[str, object]] = []
    command_pass_fds: list[tuple[int, ...]] = []
    command_purposes: list[str] = []
    create_sources: list[dict[str, object]] = []
    controls = harness["controls"]
    controls.update(
        {
            "command_crash_after": None,
            "command_crash_after_purpose": None,
            "command_error_after": None,
            "command_error_after_purpose": None,
            "command_stdout_after": {},
            "listing_responses": [],
            "namespace_listing_responses": [],
            "audit_error_on_call": None,
        }
    )
    historical_probe_purposes = {
        str(group.get("purpose"))
        for group in initial.get("rclone_evidence_groups", [])
        if isinstance(group, dict) and group.get("group_id") != "install"
    }
    prefix_exists_from_history = any(
        purpose == "probe-precreate-absence"
        or purpose in {
            "probe-owned-inventory",
            "probe-recovery-inventory",
            "probe-prefix-empty",
        }
        or purpose.startswith(
            (
                "probe-create:",
                "probe-adopt:",
                "probe-verify:",
                "probe-cleanup:",
            )
        )
        for purpose in historical_probe_purposes
    )
    controls["probe_prefix_exists"] = bool(
        remote_store or remote_dirs or prefix_exists_from_history
    )
    controls["probe_parent_exists"] = bool(controls["probe_prefix_exists"])
    local_audits: list[dict[str, object]] = []

    def capture_audit(_context: object, logical_path: str) -> dict[str, object]:
        assert logical_path == "/etc/degen/rclone.conf"
        ordinal = len(local_audits)
        if controls["audit_error_on_call"] == ordinal:
            controls["audit_error_on_call"] = None
            raise module.OperationStateError(
                "controlled after-audit capture failure"
            )
        audit = task8_file_audit(
            hashlib.sha256(f"audit-{ordinal}".encode("ascii")).hexdigest(),
            inode=400 + ordinal,
            size=600 + ordinal,
            mtime_ns=1_760_000_000_000_000_000 + ordinal,
        )
        local_audits.append(audit)
        harness["timeline"].append("audit:rclone-config")
        return copy.deepcopy(audit)

    def listing() -> str:
        rows = []
        for index, (name, contents) in enumerate(sorted(remote_store.items())):
            rows.append(
                {
                    "Path": name,
                    "Name": name,
                    "Size": len(contents),
                    "MimeType": "application/octet-stream",
                    "ModTime": "2026-07-01T00:00:00Z",
                    "IsDir": False,
                    "ID": f"test-object-{index}",
                    "OrigID": "",
                    "Tier": "",
                }
            )
        for index, name in enumerate(sorted(remote_dirs)):
            rows.append(
                {
                    "Path": name,
                    "Name": name.rsplit("/", 1)[-1],
                    "Size": -1,
                    "MimeType": "inode/directory",
                    "ModTime": "2026-07-01T00:00:00Z",
                    "IsDir": True,
                    "ID": f"test-directory-{index}",
                    "OrigID": "",
                    "Tier": "",
                }
            )
        return json.dumps(rows, separators=(",", ":"))

    def namespace_listing(argv_tuple: tuple[str, ...]) -> str:
        target = argv_tuple[-1]
        identity = SAFE_PROBE_PREFIX.rstrip("/").rsplit("/", 1)[-1]
        if target == "onedrive:backups/":
            rows = [
                {
                    "Path": "degen-db",
                    "Name": "degen-db",
                    "Size": -1,
                    "MimeType": "inode/directory",
                    "ModTime": "2026-07-01T00:00:00Z",
                    "IsDir": True,
                    "ID": "unrelated-backup-directory",
                    "OrigID": "",
                    "Tier": "",
                }
            ]
            if controls["probe_parent_exists"]:
                rows.append(
                    {
                        "Path": "degen-db-probe",
                        "Name": "degen-db-probe",
                        "Size": -1,
                        "MimeType": "inode/directory",
                        "ModTime": "2026-07-01T00:00:00Z",
                        "IsDir": True,
                        "ID": "probe-parent-directory",
                        "OrigID": "",
                        "Tier": "",
                    }
                )
        elif target == "onedrive:backups/degen-db-probe/":
            rows = []
            if controls["probe_prefix_exists"]:
                rows.append(
                    {
                        "Path": identity,
                        "Name": identity,
                        "Size": -1,
                        "MimeType": "inode/directory",
                        "ModTime": "2026-07-01T00:00:00Z",
                        "IsDir": True,
                        "ID": "colliding-probe-directory",
                        "OrigID": "",
                        "Tier": "",
                    }
                )
        else:
            raise AssertionError(f"unexpected namespace inventory target: {target!r}")
        return json.dumps(rows, separators=(",", ":"))

    def command_runner(
        argv: object,
        pass_fds: tuple[int, ...],
    ) -> subprocess.CompletedProcess[str]:
        argv_tuple = tuple(str(value) for value in argv)
        fresh_acquisitions = harness["fresh_acquisitions"]
        assert fresh_acquisitions
        runtime_lock_fd = fresh_acquisitions[-1][1]
        assert pass_fds == (runtime_lock_fd,)
        current = harness["holder"]["state"]
        groups = current["rclone_evidence_groups"]
        assert isinstance(groups, list) and groups
        pending = groups[-1]
        assert isinstance(pending, dict) and pending["after"] is None
        purpose = str(pending["purpose"])
        command_calls.append(argv_tuple)
        command_pass_fds.append(pass_fds)
        command_purposes.append(purpose)
        command_states.append(copy.deepcopy(current))
        harness["timeline"].append("command:" + " ".join(argv_tuple[3:]))
        stdout = ""
        side_effect = None
        if argv_tuple in {
            task8_probe_precreate_root_argv(),
            task8_probe_precreate_parent_argv(),
        }:
            queued_responses = controls["namespace_listing_responses"]
            assert isinstance(queued_responses, list)
            stdout = (
                queued_responses.pop(0)
                if queued_responses
                else namespace_listing(argv_tuple)
            )
        elif argv_tuple == task8_probe_list_argv():
            if not controls["probe_prefix_exists"]:
                return subprocess.CompletedProcess(
                    argv_tuple,
                    3,
                    "",
                    "directory not found",
                )
            queued_responses = controls["listing_responses"]
            assert isinstance(queued_responses, list)
            stdout = queued_responses.pop(0) if queued_responses else listing()
        elif argv_tuple[:6] == (
            "/usr/bin/rclone",
            "--config",
            "/etc/degen/rclone.conf",
            "--ignore-existing",
            "--error-on-no-transfer",
            "copyto",
        ) and len(argv_tuple) == 8:
            source = argv_tuple[6]
            target = argv_tuple[7]
            if not target.startswith(SAFE_PROBE_PREFIX):
                raise AssertionError(f"unsafe probe create target: {target!r}")
            name = target[len(SAFE_PROBE_PREFIX) :]
            if name not in task8_probe_payloads():
                raise AssertionError(f"untracked probe create target: {target!r}")
            source_path = Path(source)
            assert source_path.is_file()
            assert source_path.resolve().is_relative_to(operation_dir.resolve())
            contents = source_path.read_bytes()
            assert contents == task8_probe_payloads()[name]
            create_sources.append(
                {
                    "name": name,
                    "source": source,
                    "contents": contents,
                    "pass_fds": pass_fds,
                }
            )
            if name in remote_store:
                return subprocess.CompletedProcess(
                    argv_tuple,
                    9,
                    "",
                    "strict no-transfer refusal",
                )

            def create() -> None:
                controls["probe_parent_exists"] = True
                controls["probe_prefix_exists"] = True
                remote_store[name] = contents

            side_effect = create
        elif argv_tuple[:5] == (
            "/usr/bin/rclone",
            "--config",
            "/etc/degen/rclone.conf",
            "hashsum",
            "SHA-256",
        ) and len(argv_tuple) == 7 and argv_tuple[-1] == "--download":
            target = argv_tuple[-2]
            if not target.startswith(SAFE_PROBE_PREFIX):
                raise AssertionError(f"unsafe probe hash target: {target!r}")
            name = target[len(SAFE_PROBE_PREFIX) :]
            if name not in remote_store:
                return subprocess.CompletedProcess(argv_tuple, 3, "", "not found")
            stdout = (
                f"{hashlib.sha256(remote_store[name]).hexdigest()}  {name}\n"
            )
        elif argv_tuple[:4] == (
            "/usr/bin/rclone",
            "--config",
            "/etc/degen/rclone.conf",
            "deletefile",
        ) and len(argv_tuple) == 5:
            target = argv_tuple[-1]
            if not target.startswith(SAFE_PROBE_PREFIX):
                raise AssertionError(f"unsafe probe delete target: {target!r}")
            name = target[len(SAFE_PROBE_PREFIX) :]

            def delete() -> None:
                remote_store.pop(name, None)

            side_effect = delete
        else:
            raise AssertionError(f"unexpected probe recovery command: {argv_tuple!r}")

        if side_effect is not None:
            side_effect()
        stdout_overrides = controls["command_stdout_after"]
        assert isinstance(stdout_overrides, dict)
        if argv_tuple in stdout_overrides:
            stdout = str(stdout_overrides.pop(argv_tuple))
        if controls["command_crash_after"] == argv_tuple:
            controls["command_crash_after"] = None
            raise Task8ProbeProcessDeath("process death after remote command")
        if controls["command_crash_after_purpose"] == purpose:
            controls["command_crash_after_purpose"] = None
            raise Task8ProbeProcessDeath("process death after remote command")
        if controls["command_error_after"] == argv_tuple:
            controls["command_error_after"] = None
            return subprocess.CompletedProcess(
                argv_tuple,
                1,
                stdout,
                "controlled remote command failure",
            )
        if controls["command_error_after_purpose"] == purpose:
            controls["command_error_after_purpose"] = None
            return subprocess.CompletedProcess(
                argv_tuple,
                1,
                stdout,
                "controlled remote command failure",
            )
        return subprocess.CompletedProcess(argv_tuple, 0, stdout, "")

    monkeypatch.setattr(module, "_task7_capture_file_audit", capture_audit)
    context.command_runner = command_runner
    harness.update(
        {
            "remote_store": remote_store,
            "remote_directories": remote_dirs,
            "command_states": command_states,
            "command_pass_fds": command_pass_fds,
            "command_purposes": command_purposes,
            "create_sources": create_sources,
            "local_audits": local_audits,
        }
    )
    return context, harness


def test_task8_probe_recovery_dispatches_probe_without_regressing_existing_routes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    probe_state = {"phase": "probing", "recovery": None}
    guard_state = {
        "phase": "recovery_required",
        "recovery": {"kind": "guard"},
    }
    task7_state = {
        "phase": "recovery_required",
        "recovery": {"kind": "install"},
    }
    states = {
        "probe": probe_state,
        "guard": guard_state,
        "task7": task7_state,
    }
    calls: list[tuple[str, str]] = []
    monkeypatch.setattr(
        module,
        "_task7_load_state",
        lambda context: states[context.route],
    )
    monkeypatch.setattr(
        module,
        "_task8_run_probe_recovery",
        lambda context: calls.append(("probe", context.route)) or {"route": "probe"},
        raising=False,
    )
    monkeypatch.setattr(
        module,
        "_task8_run_guard_recovery",
        lambda context: calls.append(("guard", context.route)) or {"route": "guard"},
    )
    monkeypatch.setattr(
        module,
        "_task7_run_recovery",
        lambda context, *, manual_request: calls.append(("task7", context.route))
        or {"route": "task7", "manual": manual_request},
    )

    assert module.recover_host_configuration(types.SimpleNamespace(route="probe")) == {
        "route": "probe"
    }
    assert module.recover_host_configuration(types.SimpleNamespace(route="guard")) == {
        "route": "guard"
    }
    assert module.recover_host_configuration(types.SimpleNamespace(route="task7")) == {
        "route": "task7",
        "manual": False,
    }
    assert calls == [
        ("probe", "probe"),
        ("guard", "guard"),
        ("task7", "task7"),
    ]


def test_task8_probe_recovery_raw_state_records_separate_receipts_and_cleans_exact_objects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    progress = {
        "probe.dump": (True, True, False),
        "probe.dump.sha256": (True, True, False),
    }
    initial = task8_probe_recovery_state(
        operation_dir,
        "probing",
        progress=progress,
        milestone="runtime_lock_acquired",
    )
    module.validate_operation_state(initial, operation_dir)
    context, harness = task8_install_probe_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
        remote_objects=task8_probe_payloads(),
    )
    transaction = initial["active_transaction"]
    assert isinstance(transaction, dict)
    probe_identity = transaction["probe"]
    assert isinstance(probe_identity, dict)
    payloads = task8_probe_payloads()
    assert [item["name"] for item in probe_identity["objects"]] == [
        "probe.dump",
        "probe.dump.sha256",
    ]
    for item in probe_identity["objects"]:
        payload = payloads[str(item["name"])]
        assert item["expected_size"] == len(payload)
        assert item["expected_sha256"] == hashlib.sha256(payload).hexdigest()

    result = module.recover_host_configuration(context)

    writes = harness["writes"]
    required_index = next(
        index for index, state in enumerate(writes) if state["phase"] == "recovery_required"
    )
    recovering_index = next(
        index for index, state in enumerate(writes) if state["phase"] == "recovering_probe"
    )
    assert required_index < recovering_index
    required = writes[required_index]
    assert required["failure"]["phase"] == "probing"
    assert required["failure"]["primary_error"] == task8_probe_recovery_primary_error()
    assert required["failure"]["evidence_sha256"] == task7_error_evidence_sha256(
        "primary",
        required["failure"],
    )
    recovery = required["recovery"]
    assert recovery["kind"] == "probe"
    assert recovery["next_target_index"] == 0
    assert all(
        recovery[field] is None
        for field in ("current_target", "previous_sha256", "intended_sha256")
    )
    assert recovery["runtime_baseline"] == initial["active_transaction"]["runtime_baseline"]
    assert writes[recovering_index]["recovery"] == recovery

    expected_commands = [
        task8_probe_precreate_root_argv(),
        task8_probe_precreate_parent_argv(),
        task8_probe_list_argv(),
        task8_probe_hashsum_argv("probe.dump"),
        task8_probe_hashsum_argv("probe.dump.sha256"),
        task8_probe_delete_argv("probe.dump"),
        task8_probe_delete_argv("probe.dump.sha256"),
        task8_probe_list_argv(),
    ]
    assert harness["command_calls"] == expected_commands
    assert harness["remote_store"] == {}
    assert result["phase"] == "installed"
    assert result["probe"] is None
    assert result["active_transaction"] is None
    assert result["recovery"]["completed_epoch"] is not None
    assert result["failure"] == required["failure"]
    assert all(
        group["after"] is not None and group["evidence_sha256"] is not None
        for group in result["rclone_evidence_groups"]
    )
    for command_state in harness["command_states"]:
        groups = command_state["rclone_evidence_groups"]
        assert groups[-1]["after"] is None
        assert groups[-1]["evidence_sha256"] is None
    for argv, command_state in zip(
        harness["command_calls"],
        harness["command_states"],
    ):
        if "deletefile" not in argv:
            continue
        name = str(argv[-1])[len(SAFE_PROBE_PREFIX) :]
        transaction = command_state["active_transaction"]
        assert isinstance(transaction, dict)
        probe = transaction["probe"]
        assert isinstance(probe, dict)
        item = next(candidate for candidate in probe["objects"] if candidate["name"] == name)
        assert item["created"] is True
        assert item["verified"] is True
        assert item["cleaned"] is False

    timeline = harness["timeline"]
    first_raw_proof = timeline.index("proof:probing")
    raw_write = timeline.index("write:probing")
    rebound_raw_proof = timeline.index("proof:probing", raw_write + 1)
    required_write = timeline.index("write:recovery_required")
    required_proof = timeline.index("proof:recovery_required", required_write + 1)
    recovering_write = timeline.index("write:recovering_probe")
    recovering_proof = timeline.index("proof:recovering_probe", recovering_write + 1)
    first_command = next(
        index for index, event in enumerate(timeline) if event.startswith("command:")
    )
    assert (
        first_raw_proof
        < raw_write
        < rebound_raw_proof
        < required_write
        < required_proof
        < recovering_write
        < recovering_proof
        < first_command
    )
    command_indices = [
        index
        for index, event in enumerate(timeline)
        if event.startswith("command:")
    ]
    for command_index in command_indices:
        prior_write = max(
            index
            for index, event in enumerate(timeline[:command_index])
            if event.startswith("write:")
        )
        prior_proof = max(
            index
            for index, event in enumerate(timeline[:command_index])
            if event == "proof:recovering_probe"
        )
        assert prior_write < prior_proof < command_index
    module.validate_operation_state(result, operation_dir)


@pytest.mark.parametrize(
    "milestone",
    (
        None,
        "timer_stopped",
        "service_inactive_verified",
        "legacy_lock_acquired",
        "runtime_lock_acquired",
        "locks_released",
    ),
)
def test_task8_probe_recovery_reacquires_fresh_locks_for_every_historical_milestone(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    milestone: str | None,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    initial = task8_probe_recovery_state(
        operation_dir,
        "recovering_probe",
        milestone=milestone,
    )
    module.validate_operation_state(initial, operation_dir)
    context, harness = task8_install_probe_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
    )

    final = module.recover_host_configuration(context)

    assert final["phase"] == "installed"
    assert len(harness["fresh_acquisitions"]) == 1
    assert harness["release_actions"] == [
        "runtime_lock_release",
        "legacy_lock_release",
    ]
    assert harness["command_purposes"] == [
        "probe-recovery-root",
        "probe-recovery-parent",
        "probe-recovery-inventory",
        "probe-prefix-empty",
    ]
    assert harness["command_calls"] == [
        task8_probe_precreate_root_argv(),
        task8_probe_precreate_parent_argv(),
        task8_probe_list_argv(),
        task8_probe_list_argv(),
    ]
    assert harness["live_runtime"] == harness["baseline"]


def test_task8_probe_recovery_timer_restored_still_freshly_proves_remote_prefix_empty(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    completed_progress = {
        "probe.dump": (True, True, True),
        "probe.dump.sha256": (True, True, True),
    }
    initial = task8_probe_recovery_state(
        operation_dir,
        "recovering_probe",
        progress=completed_progress,
        milestone="timer_restored",
        include_empty_proof=True,
    )
    module.validate_operation_state(initial, operation_dir)
    context, harness = task8_install_probe_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
    )

    final = module.recover_host_configuration(context)

    assert final["phase"] == "installed"
    assert final["recovery"]["completed_epoch"] is not None
    assert len(harness["fresh_acquisitions"]) == 1
    assert harness["release_actions"] == [
        "runtime_lock_release",
        "legacy_lock_release",
    ]
    assert harness["command_purposes"] == [
        "probe-recovery-root",
        "probe-recovery-parent",
        "probe-recovery-inventory",
        "probe-prefix-empty",
    ]
    assert harness["command_calls"] == [
        task8_probe_precreate_root_argv(),
        task8_probe_precreate_parent_argv(),
        task8_probe_list_argv(),
        task8_probe_list_argv(),
    ]
    assert not any("deletefile" in argv for argv in harness["command_calls"])
    assert harness["live_runtime"] == harness["baseline"]


@pytest.mark.parametrize(
    ("invalid_listing", "error_pattern"),
    (
        ("[", "JSON|listing"),
        ((
            '[{"Path":"probe.dump","Path":"probe.dump",'
            '"Name":"probe.dump","Size":96,"MimeType":"application/octet-stream",'
            '"ModTime":"2026-07-01T00:00:00Z","IsDir":false,'
            '"ID":"duplicate-key","OrigID":"","Tier":""}]'
        ), "duplicate"),
    ),
    ids=("malformed-json", "duplicate-key"),
)
def test_task8_probe_recovery_invalid_empty_proof_retries_with_a_new_remote_list(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    invalid_listing: str,
    error_pattern: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    completed_progress = {
        "probe.dump": (True, True, True),
        "probe.dump.sha256": (True, True, True),
    }
    initial = task8_probe_recovery_state(
        operation_dir,
        "recovering_probe",
        progress=completed_progress,
        milestone="timer_restored",
        include_empty_proof=True,
    )
    module.validate_operation_state(initial, operation_dir)
    original_failure = copy.deepcopy(initial["failure"])
    initial_group_count = len(initial["rclone_evidence_groups"])
    context, harness = task8_install_probe_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
    )
    harness["controls"]["listing_responses"] = [invalid_listing, "[]"]

    with pytest.raises(module.OperationStateError, match=error_pattern):
        module.recover_host_configuration(context)

    refused = harness["holder"]["state"]
    assert refused["phase"] == "recovery_required"
    assert refused["failure"] == original_failure
    assert refused["recovery"]["completed_epoch"] is None
    assert len(refused["rclone_evidence_groups"]) == initial_group_count + 3
    recovery_attempt = refused["rclone_evidence_groups"][-3:]
    assert [group["purpose"] for group in recovery_attempt] == [
        "probe-recovery-root",
        "probe-recovery-parent",
        "probe-recovery-inventory",
    ]
    assert [group["outcome"] for group in recovery_attempt] == [
        "success",
        "success",
        "indeterminate",
    ]
    completed_invalid_list = refused["rclone_evidence_groups"][-1]
    assert completed_invalid_list["outcome"] == "indeterminate"
    assert completed_invalid_list["after"] is not None
    assert completed_invalid_list["evidence_sha256"] is not None
    assert harness["command_calls"] == [
        task8_probe_precreate_root_argv(),
        task8_probe_precreate_parent_argv(),
        task8_probe_list_argv(),
    ]

    final = module.recover_host_configuration(context)

    assert final["phase"] == "installed"
    assert final["recovery"]["completed_epoch"] is not None
    assert harness["command_calls"] == [
        task8_probe_precreate_root_argv(),
        task8_probe_precreate_parent_argv(),
        task8_probe_list_argv(),
        task8_probe_precreate_root_argv(),
        task8_probe_precreate_parent_argv(),
        task8_probe_list_argv(),
        task8_probe_list_argv(),
    ]
    assert not any(
        command in argv
        for argv in harness["command_calls"]
        for command in ("hashsum", "deletefile")
    )


def test_task8_probe_recovery_completes_pending_local_audit_without_replaying_remote_create(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    initial = task8_probe_recovery_state(
        operation_dir,
        "recovering_probe",
        milestone="runtime_lock_acquired",
        pending_purpose="probe-create:probe.dump.sha256:strict-no-existing",
    )
    pending = copy.deepcopy(initial["rclone_evidence_groups"][-1])
    module.validate_operation_state(initial, operation_dir)
    context, harness = task8_install_probe_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
    )

    final = module.recover_host_configuration(context)

    completed = next(
        group
        for group in final["rclone_evidence_groups"]
        if group["group_id"] == pending["group_id"]
    )
    assert completed["purpose"] == pending["purpose"]
    assert completed["outcome"] == "indeterminate"
    assert completed["before"] == pending["before"]
    assert completed["after"] is not None
    assert completed["evidence_sha256"] is not None
    assert harness["local_audits"]
    assert not any(
        command in argv
        for argv in harness["command_calls"]
        for command in ("copyto", "moveto", "hashsum", "deletefile", "purge")
    )
    assert final["phase"] == "installed"
    assert harness["command_purposes"] == [
        "probe-recovery-root",
        "probe-recovery-parent",
        "probe-recovery-inventory",
        "probe-prefix-empty",
    ]
    assert harness["command_calls"] == [
        task8_probe_precreate_root_argv(),
        task8_probe_precreate_parent_argv(),
        task8_probe_list_argv(),
        task8_probe_list_argv(),
    ]
    assert harness["remote_store"] == {}


@pytest.mark.parametrize(
    "unsafe_case",
    (
        "untracked",
        "casefold",
        "nested",
        "directory",
        "size",
        "digest",
        "ownership",
    ),
)
def test_task8_probe_recovery_refuses_unsafe_live_prefix_without_any_delete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    unsafe_case: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    progress = {
        "probe.dump": (True, True, False),
        "probe.dump.sha256": (True, True, False),
    }
    include_ownership = True
    remote = task8_probe_payloads()
    remote_directories: set[str] = set()
    if unsafe_case == "untracked":
        remote["intruder.dump"] = b"untracked\n"
    elif unsafe_case == "casefold":
        remote["PROBE.DUMP"] = remote["probe.dump"]
    elif unsafe_case == "nested":
        remote["nested/stray.dump"] = b"nested residue\n"
    elif unsafe_case == "directory":
        remote_directories.add(".hidden-probe-residue")
    elif unsafe_case == "size":
        remote["probe.dump.sha256"] += b"x"
    elif unsafe_case == "digest":
        payload = remote["probe.dump.sha256"]
        replacement = b"0" if payload[:1] != b"0" else b"1"
        remote["probe.dump.sha256"] = replacement + payload[1:]
    else:
        progress["probe.dump.sha256"] = (False, False, False)
        include_ownership = False
    initial = task8_probe_recovery_state(
        operation_dir,
        "recovering_probe",
        progress=progress,
        milestone="runtime_lock_acquired",
        include_ownership_evidence=include_ownership,
    )
    module.validate_operation_state(initial, operation_dir)
    original_failure = copy.deepcopy(initial["failure"])
    context, harness = task8_install_probe_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
        remote_objects=remote,
        remote_directories=remote_directories,
    )

    with pytest.raises(module.OperationStateError, match="probe|remote|prefix|object|ownership"):
        module.recover_host_configuration(context)

    interrupted = harness["holder"]["state"]
    assert interrupted["phase"] == "recovery_required"
    assert interrupted["failure"] == original_failure
    assert interrupted["recovery"]["completed_epoch"] is None
    assert not any("deletefile" in argv for argv in harness["command_calls"])
    assert harness["remote_store"] == remote
    assert harness["remote_directories"] == remote_directories
    assert harness["live_runtime"]["timer_enabled"] is False
    assert harness["live_runtime"]["timer_active"] is False
    if unsafe_case == "digest":
        failed_verify = interrupted["rclone_evidence_groups"][-1]
        assert failed_verify["purpose"] == "probe-verify:probe.dump.sha256"
        assert failed_verify["outcome"] == "indeterminate"


def test_task8_probe_recovery_preserves_primary_orders_secondary_and_resumes_after_refusal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    progress = {
        "probe.dump": (True, True, False),
        "probe.dump.sha256": (True, True, False),
    }
    initial = task8_probe_recovery_state(
        operation_dir,
        "recovering_probe",
        progress=progress,
        milestone="runtime_lock_acquired",
    )
    earlier = {
        "stage": "earlier_probe_failure",
        "error": "earlier sanitized probe failure",
        "epoch": int(initial["recovery"]["started_epoch"]),
    }
    earlier["evidence_sha256"] = task7_error_evidence_sha256("secondary", earlier)
    initial["secondary_errors"] = [earlier]
    module.validate_operation_state(initial, operation_dir)
    remote = task8_probe_payloads()
    remote["untracked.dump"] = b"untracked\n"
    original_failure = copy.deepcopy(initial["failure"])
    original_recovery = copy.deepcopy(initial["recovery"])
    context, harness = task8_install_probe_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
        remote_objects=remote,
    )

    with pytest.raises(module.OperationStateError):
        module.recover_host_configuration(context)

    refused = harness["holder"]["state"]
    assert refused["failure"] == original_failure
    assert refused["secondary_errors"][0] == earlier
    assert len(refused["secondary_errors"]) >= 2
    assert [item["epoch"] for item in refused["secondary_errors"]] == sorted(
        item["epoch"] for item in refused["secondary_errors"]
    )
    assert refused["recovery"]["started_epoch"] == original_recovery["started_epoch"]
    assert refused["recovery"]["evidence_sha256"] == original_recovery["evidence_sha256"]
    del harness["remote_store"]["untracked.dump"]

    final = module.recover_host_configuration(context)

    assert final["phase"] == "installed"
    assert final["failure"] == original_failure
    assert final["secondary_errors"][0] == earlier
    assert final["recovery"]["started_epoch"] == original_recovery["started_epoch"]
    assert final["recovery"]["evidence_sha256"] == original_recovery["evidence_sha256"]
    assert final["recovery"]["completed_epoch"] is not None


def test_task8_probe_recovery_process_death_after_delete_is_resumable_without_fabricated_receipts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    progress = {
        "probe.dump": (True, True, False),
        "probe.dump.sha256": (True, True, False),
    }
    initial = task8_probe_recovery_state(
        operation_dir,
        "recovering_probe",
        progress=progress,
        milestone="runtime_lock_acquired",
    )
    module.validate_operation_state(initial, operation_dir)
    context, harness = task8_install_probe_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
        remote_objects=task8_probe_payloads(),
    )
    original_failure = copy.deepcopy(initial["failure"])
    original_secondary = copy.deepcopy(initial["secondary_errors"])
    harness["controls"]["command_crash_after"] = task8_probe_delete_argv("probe.dump")

    with pytest.raises(Task8ProbeProcessDeath, match="remote command"):
        module.recover_host_configuration(context)

    interrupted = harness["holder"]["state"]
    assert interrupted["phase"] == "recovering_probe"
    assert interrupted["failure"] == original_failure
    assert interrupted["secondary_errors"] == original_secondary
    assert interrupted["recovery"]["completed_epoch"] is None
    objects = interrupted["active_transaction"]["probe"]["objects"]
    dump = next(item for item in objects if item["name"] == "probe.dump")
    assert dump["cleaned"] is False
    assert "probe.dump" not in harness["remote_store"]
    pending_delete = interrupted["rclone_evidence_groups"][-1]
    pending_delete_id = str(pending_delete["group_id"])
    assert pending_delete["purpose"] == "probe-cleanup:probe.dump"
    assert pending_delete["after"] is None
    assert pending_delete["outcome"] is None
    assert pending_delete["evidence_sha256"] is None

    final = module.recover_host_configuration(context)

    assert final["phase"] == "installed"
    assert final["failure"] == original_failure
    assert final["secondary_errors"] == original_secondary
    assert harness["remote_store"] == {}
    assert harness["command_calls"].count(task8_probe_delete_argv("probe.dump")) == 1
    finalized_delete = next(
        group
        for group in final["rclone_evidence_groups"]
        if group["group_id"] == pending_delete_id
    )
    assert finalized_delete["purpose"] == "probe-cleanup:probe.dump"
    assert finalized_delete["outcome"] == "indeterminate"
    assert finalized_delete["after"] is not None
    assert finalized_delete["evidence_sha256"] is not None


@pytest.mark.parametrize(
    ("purpose", "argv"),
    (
        (
            "probe-create:probe.dump:strict-no-existing",
            task8_probe_delete_argv("probe.dump"),
        ),
        (
            "probe-verify:probe.dump",
            task8_probe_hashsum_argv("probe.dump.sha256"),
        ),
        (
            "probe-cleanup:probe.dump",
            task8_probe_list_argv(),
        ),
    ),
    ids=("create-purpose-delete", "verify-wrong-object", "cleanup-purpose-list"),
)
def test_task8_probe_recovery_audited_command_rejects_purpose_argv_mismatch_before_intent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    purpose: str,
    argv: tuple[str, ...],
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    initial = task8_probe_recovery_state(
        operation_dir,
        "recovering_probe",
        milestone="runtime_lock_acquired",
    )
    module.validate_operation_state(initial, operation_dir)
    context, harness = task8_install_probe_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
        remote_objects=task8_probe_payloads(),
    )

    with pytest.raises(
        module.OperationStateError,
        match="purpose|argv|command|target|binding",
    ):
        module._task8_run_audited_rclone_command(
            context,
            object(),
            harness["holder"]["state"],
            purpose=purpose,
            argv=argv,
            label="controlled purpose binding",
            runtime_lock_fd=17,
        )

    assert harness["writes"] == []
    assert harness["local_audits"] == []
    assert harness["command_calls"] == []
    assert harness["holder"]["state"] == initial


@pytest.mark.parametrize(
    "prior_purposes",
    (
        ("probe-precreate-absence",),
        (
            "probe-precreate-absence",
            "probe-recovery-root",
        ),
    ),
    ids=("historical-precreate-only", "fresh-root-without-parent"),
)
def test_task8_probe_recovery_inventory_binding_rejects_skipped_fresh_parent_proof(
    tmp_path: Path,
    prior_purposes: tuple[str, ...],
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = task8_probe_recovery_state(
        operation_dir,
        "recovering_probe",
        milestone="runtime_lock_acquired",
    )
    task8_replace_probe_purpose_sequence(state, prior_purposes)
    module.validate_operation_state(state, operation_dir)
    before = copy.deepcopy(state)

    with pytest.raises(
        module.OperationStateError,
        match="recovery|inventory|parent|proof|order|purpose",
    ):
        module._task8_validate_probe_recovery_purpose_argv(
            state,
            "probe-recovery-inventory",
            task8_probe_list_argv(),
        )

    assert state == before


def test_task8_probe_recovery_inventory_binding_accepts_fresh_root_parent_proof(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = task8_probe_recovery_state(
        operation_dir,
        "recovering_probe",
        milestone="runtime_lock_acquired",
    )
    task8_replace_probe_purpose_sequence(
        state,
        (
            "probe-precreate-absence",
            "probe-recovery-root",
            "probe-recovery-parent",
        ),
    )
    module.validate_operation_state(state, operation_dir)
    before = copy.deepcopy(state)

    module._task8_validate_probe_recovery_purpose_argv(
        state,
        "probe-recovery-inventory",
        task8_probe_list_argv(),
    )

    assert state == before


def task8_replace_probe_purpose_sequence(
    state: dict[str, object],
    purposes: tuple[str, ...],
) -> None:
    task8_replace_probe_event_sequence(
        state,
        tuple((purpose, "success") for purpose in purposes),
    )


def task8_replace_probe_event_sequence(
    state: dict[str, object],
    events: tuple[tuple[str, str], ...],
) -> None:
    transaction = state["active_transaction"]
    assert isinstance(transaction, dict)
    started_epoch = int(transaction["started_epoch"])
    state["rclone_evidence_groups"] = [
        task8_rclone_group(
            "probe",
            started_epoch,
            0,
            ordinal,
            purpose=purpose,
            outcome=outcome,
        )
        for ordinal, (purpose, outcome) in enumerate(events)
    ]


@pytest.mark.parametrize(
    ("purpose_order", "already_cleaned"),
    (
        (
            (
                "probe-create:probe.dump:strict-no-existing",
                "probe-precreate-absence",
                "probe-verify:probe.dump",
            ),
            False,
        ),
        (
            (
                "probe-precreate-absence",
                "probe-verify:probe.dump",
                "probe-create:probe.dump:strict-no-existing",
            ),
            False,
        ),
        (
            (
                "probe-precreate-absence",
                "probe-create:probe.dump:strict-no-existing",
                "probe-cleanup:probe.dump",
                "probe-verify:probe.dump",
            ),
            False,
        ),
        (
            (
                "probe-precreate-absence",
                "probe-create:probe.dump:strict-no-existing",
                "probe-verify:probe.dump",
                "probe-prefix-empty",
                "probe-cleanup:probe.dump",
            ),
            True,
        ),
    ),
    ids=(
        "create-before-precreate",
        "verify-before-create",
        "cleanup-before-verify",
        "final-empty-before-cleanup",
    ),
)
def test_task8_probe_recovery_invalid_ownership_purpose_order_cannot_authorize_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    purpose_order: tuple[str, ...],
    already_cleaned: bool,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    progress = {
        "probe.dump": (True, True, already_cleaned),
        "probe.dump.sha256": (False, False, False),
    }
    milestone = "timer_restored" if already_cleaned else "runtime_lock_acquired"
    initial = task8_probe_recovery_state(
        operation_dir,
        "recovering_probe",
        progress=progress,
        milestone=milestone,
    )
    task8_replace_probe_purpose_sequence(initial, purpose_order)
    module.validate_operation_state(initial, operation_dir)
    remote = {} if already_cleaned else {"probe.dump": task8_probe_payloads()["probe.dump"]}
    context, harness = task8_install_probe_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
        remote_objects=remote,
    )

    with pytest.raises(
        module.OperationStateError,
        match="ownership|purpose|order|pre-create|verify|cleanup|empty",
    ):
        module.recover_host_configuration(context)

    interrupted = harness["holder"]["state"]
    assert interrupted["phase"] == "recovery_required"
    assert interrupted["active_transaction"] is not None
    assert interrupted["recovery"]["completed_epoch"] is None
    assert not any("deletefile" in argv for argv in harness["command_calls"])


def test_task8_probe_recovery_records_command_then_after_audit_failure_as_distinct_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    initial = task8_probe_recovery_state(
        operation_dir,
        "recovering_probe",
        milestone="runtime_lock_acquired",
    )
    module.validate_operation_state(initial, operation_dir)
    original_failure = copy.deepcopy(initial["failure"])
    context, harness = task8_install_probe_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
    )
    harness["controls"]["command_error_after"] = task8_probe_list_argv()
    # Root and parent proofs consume two complete before/after audit pairs;
    # fail the after-audit capture for the exact-prefix inventory command.
    harness["controls"]["audit_error_on_call"] = 5

    with pytest.raises(
        module.OperationStateError,
        match="remote probe prefix inventory failed",
    ):
        module.recover_host_configuration(context)

    interrupted = harness["holder"]["state"]
    assert interrupted["phase"] == "recovery_required"
    assert interrupted["failure"] == original_failure
    assert interrupted["recovery"]["completed_epoch"] is None
    assert [
        item["stage"] for item in interrupted["secondary_errors"][-2:]
    ] == ["probe_remote_cleanup", "rclone_audit_capture"]
    assert "remote probe prefix inventory failed" in interrupted["secondary_errors"][-2]["error"]
    assert "after-audit capture failure" in interrupted["secondary_errors"][-1]["error"]
    assert [item["epoch"] for item in interrupted["secondary_errors"]] == sorted(
        item["epoch"] for item in interrupted["secondary_errors"]
    )


def test_task8_probe_recovery_same_epoch_group_id_binds_last_history_occurrence(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = task8_same_epoch_retry_probe_state(operation_dir)
    transaction = state["active_transaction"]
    assert isinstance(transaction, dict)
    started_epoch = int(transaction["started_epoch"])
    probe_entries = [
        entry for entry in state["phase_history"] if entry["phase"] == "probing"
    ]
    active_attempt = len(probe_entries) - 1
    assert sum(entry["epoch"] == started_epoch for entry in probe_entries) >= 2
    prior_purposes = (
        "probe-precreate-absence",
        "probe-create:probe.dump:strict-no-existing",
        "probe-create:probe.dump.sha256:strict-no-existing",
        "probe-verify:probe.dump",
        "probe-verify:probe.dump.sha256",
        "probe-cleanup:probe.dump",
        "probe-cleanup:probe.dump.sha256",
        "probe-prefix-empty",
    )
    state["rclone_evidence_groups"] = [
        task8_rclone_group(
            "probe",
            started_epoch,
            active_attempt - 1,
            ordinal,
            purpose=purpose,
        )
        for ordinal, purpose in enumerate(prior_purposes)
    ]
    module.validate_operation_state(state, operation_dir)

    assert module._task8_next_rclone_group_id(state) == (
        f"task8:probe:{started_epoch}:{active_attempt}:0"
    )


def test_task8_probe_recovery_same_epoch_purposes_do_not_inherit_prior_attempt(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = task8_same_epoch_retry_probe_state(operation_dir)
    transaction = state["active_transaction"]
    assert isinstance(transaction, dict)
    started_epoch = int(transaction["started_epoch"])
    probe_entries = [
        entry for entry in state["phase_history"] if entry["phase"] == "probing"
    ]
    active_attempt = len(probe_entries) - 1
    prior_purposes = (
        "probe-precreate-absence",
        "probe-create:probe.dump:strict-no-existing",
        "probe-create:probe.dump.sha256:strict-no-existing",
        "probe-verify:probe.dump",
        "probe-verify:probe.dump.sha256",
        "probe-cleanup:probe.dump",
        "probe-cleanup:probe.dump.sha256",
        "probe-prefix-empty",
    )
    state["rclone_evidence_groups"] = [
        task8_rclone_group(
            "probe",
            started_epoch,
            active_attempt - 1,
            ordinal,
            purpose=purpose,
        )
        for ordinal, purpose in enumerate(prior_purposes)
    ] + [
        task8_rclone_group(
            "probe",
            started_epoch,
            active_attempt,
            0,
            purpose="probe-precreate-absence",
        )
    ]
    module.validate_operation_state(state, operation_dir)

    assert module._task8_probe_completed_purposes(state) == {
        "probe-precreate-absence"
    }


@pytest.mark.parametrize(
    ("purpose_order", "dump_progress"),
    (
        (
            ("probe-precreate-absence", "probe-unknown:event"),
            (False, False, False),
        ),
        (
            (
                "probe-precreate-absence",
                "probe-create:untracked.dump:strict-no-existing",
            ),
            (False, False, False),
        ),
        (
            ("probe-precreate-absence", "probe-precreate-absence"),
            (False, False, False),
        ),
        (
            (
                "probe-precreate-absence",
                "probe-create:probe.dump:strict-no-existing",
                "probe-create:probe.dump:strict-no-existing",
            ),
            (True, False, False),
        ),
        (
            (
                "probe-create:probe.dump:strict-no-existing",
                "probe-precreate-absence",
                "probe-create:probe.dump:strict-no-existing",
            ),
            (True, False, False),
        ),
        (
            (
                "probe-precreate-absence",
                "probe-verify:probe.dump",
                "probe-create:probe.dump:strict-no-existing",
                "probe-verify:probe.dump",
            ),
            (True, True, False),
        ),
        (
            (
                "probe-precreate-absence",
                "probe-cleanup:probe.dump",
                "probe-create:probe.dump:strict-no-existing",
                "probe-verify:probe.dump",
                "probe-cleanup:probe.dump",
                "probe-prefix-empty",
            ),
            (True, True, True),
        ),
        (
            (
                "probe-precreate-absence",
                "probe-cleanup:probe.dump",
            ),
            (False, False, False),
        ),
        (
            (
                "probe-precreate-absence",
                "probe-create:probe.dump:strict-no-existing",
                "probe-cleanup:probe.dump",
            ),
            (True, False, False),
        ),
        (
            (
                "probe-precreate-absence",
                "probe-create:probe.dump:strict-no-existing",
                "probe-cleanup:probe.dump",
            ),
            (True, True, False),
        ),
    ),
    ids=(
        "unknown-purpose",
        "unknown-object-name",
        "duplicate-precreate",
        "duplicate-create",
        "early-create-not-cured-by-later-create",
        "early-verify-not-cured-by-later-verify",
        "early-cleanup-not-cured-by-later-cleanup",
        "cleanup-purpose-without-created-verified-flags",
        "cleanup-purpose-without-verified-flag",
        "cleanup-purpose-without-preceding-verify",
    ),
)
def test_task8_probe_recovery_purpose_chain_rejects_unknown_duplicate_or_sticky_invalid_event(
    tmp_path: Path,
    purpose_order: tuple[str, ...],
    dump_progress: tuple[bool, bool, bool],
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    initial = task8_probe_recovery_state(
        operation_dir,
        "recovering_probe",
        progress={
            "probe.dump": dump_progress,
            "probe.dump.sha256": (False, False, False),
        },
        milestone="runtime_lock_acquired",
    )
    task8_replace_probe_purpose_sequence(initial, purpose_order)
    module.validate_operation_state(initial, operation_dir)

    with pytest.raises(
        module.OperationStateError,
        match="purpose|unknown|duplicate|order|pre-create|creation|verification|cleanup|flag",
    ):
        module._task8_require_probe_purpose_chain(initial)


def test_task8_probe_recovery_prefix_empty_must_be_single_terminal_purpose_before_external_action(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    initial = task8_probe_recovery_state(
        operation_dir,
        "recovering_probe",
        progress={
            "probe.dump": (True, True, True),
            "probe.dump.sha256": (False, False, False),
        },
        milestone="timer_restored",
    )
    task8_replace_probe_purpose_sequence(
        initial,
        (
            "probe-precreate-absence",
            "probe-create:probe.dump:strict-no-existing",
            "probe-verify:probe.dump",
            "probe-cleanup:probe.dump",
            "probe-prefix-empty",
            "probe-recovery-inventory",
            "probe-prefix-empty",
        ),
    )
    module.validate_operation_state(initial, operation_dir)
    context, harness = task8_install_probe_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
    )

    with pytest.raises(
        module.OperationStateError,
        match="purpose|prefix-empty|terminal|order",
    ):
        module.recover_host_configuration(context)

    interrupted = harness["holder"]["state"]
    assert interrupted["phase"] == "recovery_required"
    assert interrupted["recovery"]["completed_epoch"] is None
    assert harness["external_actions"] == []
    assert harness["fresh_acquisitions"] == []
    assert harness["command_calls"] == []
    assert harness["local_audits"] == []


def test_task8_probe_recovery_before_first_rclone_missing_prefix_uses_root_absence_proof(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    initial = task8_probe_recovery_state(
        operation_dir,
        "recovering_probe",
        milestone="runtime_lock_acquired",
    )
    initial["rclone_evidence_groups"] = []
    module.validate_operation_state(initial, operation_dir)
    context, harness = task8_install_probe_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
    )

    final = module.recover_host_configuration(context)

    assert final["phase"] == "installed"
    assert final["active_transaction"] is None
    assert final["recovery"]["completed_epoch"] is not None
    assert harness["command_purposes"] == [
        "probe-recovery-root",
        "probe-recovery-root-absence",
    ]
    assert harness["command_calls"] == [
        task8_probe_precreate_root_argv(),
        task8_probe_precreate_root_argv(),
    ]
    assert task8_probe_list_argv() not in harness["command_calls"]
    runtime_lock_fd = harness["fresh_acquisitions"][0][1]
    assert harness["command_pass_fds"] == [
        (runtime_lock_fd,),
        (runtime_lock_fd,),
    ]
    for purpose, command_state in zip(
        harness["command_purposes"],
        harness["command_states"],
        strict=True,
    ):
        pending = command_state["rclone_evidence_groups"][-1]
        assert pending["purpose"] == purpose
        assert pending["after"] is None
        assert pending["outcome"] is None
    assert len(harness["local_audits"]) == 4
    assert not any("deletefile" in argv for argv in harness["command_calls"])
    assert harness["remote_store"] == {}


def test_task8_probe_recovery_parent_exists_candidate_absent_uses_parent_proof(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    initial = task8_probe_recovery_state(
        operation_dir,
        "recovering_probe",
        milestone="runtime_lock_acquired",
    )
    initial["rclone_evidence_groups"] = []
    module.validate_operation_state(initial, operation_dir)
    context, harness = task8_install_probe_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
    )
    harness["controls"]["probe_parent_exists"] = True
    harness["controls"]["probe_prefix_exists"] = False

    final = module.recover_host_configuration(context)

    assert final["phase"] == "installed"
    assert final["active_transaction"] is None
    assert final["recovery"]["completed_epoch"] is not None
    assert harness["command_purposes"] == [
        "probe-recovery-root",
        "probe-recovery-parent",
        "probe-recovery-parent-absence",
    ]
    assert harness["command_calls"] == [
        task8_probe_precreate_root_argv(),
        task8_probe_precreate_parent_argv(),
        task8_probe_precreate_parent_argv(),
    ]
    assert task8_probe_list_argv() not in harness["command_calls"]
    runtime_lock_fd = harness["fresh_acquisitions"][0][1]
    assert harness["command_pass_fds"] == [
        (runtime_lock_fd,),
        (runtime_lock_fd,),
        (runtime_lock_fd,),
    ]
    assert len(harness["local_audits"]) == 6
    assert not any("deletefile" in argv for argv in harness["command_calls"])
    assert harness["remote_store"] == {}


def test_task8_probe_recovery_before_first_rclone_live_object_refuses_without_delete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    initial = task8_probe_recovery_state(
        operation_dir,
        "recovering_probe",
        milestone="runtime_lock_acquired",
    )
    initial["rclone_evidence_groups"] = []
    module.validate_operation_state(initial, operation_dir)
    remote = {"probe.dump": task8_probe_payloads()["probe.dump"]}
    context, harness = task8_install_probe_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
        remote_objects=remote,
    )

    with pytest.raises(
        module.OperationStateError,
        match="ownership|pre-create|created|remote probe",
    ):
        module.recover_host_configuration(context)

    interrupted = harness["holder"]["state"]
    assert interrupted["phase"] == "recovery_required"
    assert interrupted["recovery"]["completed_epoch"] is None
    assert harness["command_purposes"] == [
        "probe-recovery-root",
        "probe-recovery-parent",
    ]
    assert harness["command_calls"] == [
        task8_probe_precreate_root_argv(),
        task8_probe_precreate_parent_argv(),
    ]
    assert task8_probe_list_argv() not in harness["command_calls"]
    assert not any("deletefile" in argv for argv in harness["command_calls"])
    assert harness["remote_store"] == remote


@pytest.mark.parametrize(
    ("remote_shape", "remote", "prefix_exists"),
    (
        ("missing-prefix", {}, False),
        ("empty-prefix", {}, True),
        (
            "live-object",
            {"probe.dump": task8_probe_payloads()["probe.dump"]},
            True,
        ),
    ),
    ids=("missing-prefix", "empty-prefix", "live-object"),
)
def test_task8_probe_recovery_pending_create_with_false_flag_is_resolved_by_fresh_inventory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    remote_shape: str,
    remote: dict[str, bytes],
    prefix_exists: bool,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    initial = task8_probe_recovery_state(
        operation_dir,
        "recovering_probe",
        milestone="runtime_lock_acquired",
        pending_purpose="probe-create:probe.dump:strict-no-existing",
    )
    pending_id = str(initial["rclone_evidence_groups"][-1]["group_id"])
    module.validate_operation_state(initial, operation_dir)
    context, harness = task8_install_probe_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
        remote_objects=remote,
    )
    harness["controls"]["probe_parent_exists"] = True
    harness["controls"]["probe_prefix_exists"] = prefix_exists

    result = module.recover_host_configuration(context)
    assert result["phase"] == "installed"
    assert result["recovery"]["completed_epoch"] is not None

    finalized_pending = next(
        group
        for group in result["rclone_evidence_groups"]
        if group["group_id"] == pending_id
    )
    assert finalized_pending["purpose"] == (
        "probe-create:probe.dump:strict-no-existing"
    )
    assert finalized_pending["outcome"] == "indeterminate"
    assert finalized_pending["after"] is not None
    assert finalized_pending["evidence_sha256"] is not None
    expected_commands = [
        task8_probe_precreate_root_argv(),
        task8_probe_precreate_parent_argv(),
    ]
    expected_purposes = [
        "probe-recovery-root",
        "probe-recovery-parent",
    ]
    if remote_shape == "missing-prefix":
        expected_commands.append(task8_probe_precreate_parent_argv())
        expected_purposes.append("probe-recovery-parent-absence")
    else:
        expected_commands.append(task8_probe_list_argv())
        expected_purposes.append("probe-recovery-inventory")
    if remote_shape == "live-object":
        expected_commands.extend(
            [
                task8_probe_hashsum_argv("probe.dump"),
                task8_probe_delete_argv("probe.dump"),
            ]
        )
        expected_purposes.extend(
            [
                "probe-adopt:probe.dump",
                "probe-cleanup:probe.dump",
            ]
        )
    if remote_shape != "missing-prefix":
        expected_commands.append(task8_probe_list_argv())
        expected_purposes.append("probe-prefix-empty")
    assert harness["command_purposes"][-len(expected_purposes) :] == expected_purposes
    assert harness["command_calls"] == expected_commands
    if remote_shape == "missing-prefix":
        assert task8_probe_list_argv() not in harness["command_calls"]
    else:
        parent_index = harness["command_calls"].index(
            task8_probe_precreate_parent_argv()
        )
        exact_indexes = [
            index
            for index, argv in enumerate(harness["command_calls"])
            if argv == task8_probe_list_argv()
        ]
        assert exact_indexes and min(exact_indexes) > parent_index
    runtime_lock_fd = harness["fresh_acquisitions"][0][1]
    assert harness["command_pass_fds"] == [
        (runtime_lock_fd,) for _command in expected_commands
    ]
    assert harness["remote_store"] == {}


def task8_set_probe_progress_flag(
    state: dict[str, object],
    name: str,
    field: str,
) -> None:
    transaction = state["active_transaction"]
    assert isinstance(transaction, dict)
    probe = transaction["probe"]
    assert isinstance(probe, dict)
    objects = probe["objects"]
    assert isinstance(objects, list)
    item = next(candidate for candidate in objects if candidate["name"] == name)
    assert item[field] is False
    item[field] = True


@pytest.mark.parametrize("audit_change", ("append-pending", "complete-pending"))
@pytest.mark.parametrize("flag_kind", ("guard", "probe"))
def test_task8_probe_recovery_rclone_audit_change_cannot_simultaneously_advance_flag(
    tmp_path: Path,
    audit_change: str,
    flag_kind: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    previous = task8_probe_recovery_state(
        operation_dir,
        "recovering_probe",
    )
    transaction = previous["active_transaction"]
    assert isinstance(transaction, dict)
    started_epoch = int(transaction["started_epoch"])
    group_ordinal = len(previous["rclone_evidence_groups"])
    purpose = (
        "probe-create:probe.dump:strict-no-existing"
        if flag_kind == "probe"
        else "probe-recovery-inventory"
    )
    pending = task8_pending_rclone_group(
        "probe",
        started_epoch,
        group_ordinal,
        purpose=purpose,
    )
    completed = task8_rclone_group(
        "probe",
        started_epoch,
        0,
        group_ordinal,
        purpose=purpose,
    )
    if audit_change == "append-pending":
        current = copy.deepcopy(previous)
        current["rclone_evidence_groups"].append(pending)
    else:
        previous["rclone_evidence_groups"].append(pending)
        current = copy.deepcopy(previous)
        current["rclone_evidence_groups"][-1] = completed
    if flag_kind == "probe":
        task8_set_probe_progress_flag(current, "probe.dump", "created")
    else:
        current["active_transaction"]["guard"]["timer_stopped"] = True
    module.validate_operation_state(previous, operation_dir)

    with pytest.raises(
        module.OperationStateError,
        match="rclone|audit|simultaneous|flag|progress|causal",
    ):
        module.validate_operation_state(current, operation_dir, previous)


@pytest.mark.parametrize(
    ("field", "prior_flags", "purpose_order"),
    (
        (
            "created",
            (False, False, False),
            (
                "probe-precreate-absence",
                "probe-create:probe.dump:strict-no-existing",
            ),
        ),
        (
            "verified",
            (True, False, False),
            (
                "probe-precreate-absence",
                "probe-create:probe.dump:strict-no-existing",
                "probe-verify:probe.dump",
            ),
        ),
        (
            "cleaned",
            (True, True, False),
            (
                "probe-precreate-absence",
                "probe-create:probe.dump:strict-no-existing",
                "probe-verify:probe.dump",
                "probe-cleanup:probe.dump",
            ),
        ),
    ),
)
def test_task8_probe_recovery_exact_completed_purpose_allows_matching_flag_advance(
    tmp_path: Path,
    field: str,
    prior_flags: tuple[bool, bool, bool],
    purpose_order: tuple[str, ...],
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    previous = task8_probe_recovery_state(
        operation_dir,
        "recovering_probe",
        progress={
            "probe.dump": prior_flags,
            "probe.dump.sha256": (False, False, False),
        },
    )
    task8_replace_probe_purpose_sequence(previous, purpose_order)
    current = copy.deepcopy(previous)
    task8_set_probe_progress_flag(current, "probe.dump", field)

    module.validate_operation_state(current, operation_dir, previous)


@pytest.mark.parametrize(
    ("field", "prior_flags", "base_purposes"),
    (
        (
            "created",
            (False, False, False),
            ("probe-precreate-absence",),
        ),
        (
            "verified",
            (True, False, False),
            (
                "probe-precreate-absence",
                "probe-create:probe.dump:strict-no-existing",
            ),
        ),
        (
            "cleaned",
            (True, True, False),
            (
                "probe-precreate-absence",
                "probe-create:probe.dump:strict-no-existing",
                "probe-verify:probe.dump",
            ),
        ),
    ),
)
def test_task8_probe_recovery_flag_advance_rejects_wrong_final_purpose(
    tmp_path: Path,
    field: str,
    prior_flags: tuple[bool, bool, bool],
    base_purposes: tuple[str, ...],
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    previous = task8_probe_recovery_state(
        operation_dir,
        "recovering_probe",
        progress={
            "probe.dump": prior_flags,
            "probe.dump.sha256": (False, False, False),
        },
    )
    task8_replace_probe_purpose_sequence(
        previous,
        (*base_purposes, "probe-recovery-inventory"),
    )
    current = copy.deepcopy(previous)
    task8_set_probe_progress_flag(current, "probe.dump", field)
    module.validate_operation_state(previous, operation_dir)

    with pytest.raises(
        module.OperationStateError,
        match="purpose|object|flag|progress|causal|completed",
    ):
        module.validate_operation_state(current, operation_dir, previous)


@pytest.mark.parametrize(
    ("field", "dump_flags", "sidecar_flags", "purpose_order"),
    (
        (
            "created",
            (False, False, False),
            (False, False, False),
            (
                "probe-precreate-absence",
                "probe-create:probe.dump.sha256:strict-no-existing",
            ),
        ),
        (
            "verified",
            (True, False, False),
            (True, False, False),
            (
                "probe-precreate-absence",
                "probe-create:probe.dump:strict-no-existing",
                "probe-create:probe.dump.sha256:strict-no-existing",
                "probe-verify:probe.dump.sha256",
            ),
        ),
        (
            "cleaned",
            (True, True, False),
            (True, True, False),
            (
                "probe-precreate-absence",
                "probe-create:probe.dump:strict-no-existing",
                "probe-create:probe.dump.sha256:strict-no-existing",
                "probe-verify:probe.dump",
                "probe-verify:probe.dump.sha256",
                "probe-cleanup:probe.dump.sha256",
            ),
        ),
    ),
)
def test_task8_probe_recovery_flag_advance_rejects_completed_purpose_for_wrong_object(
    tmp_path: Path,
    field: str,
    dump_flags: tuple[bool, bool, bool],
    sidecar_flags: tuple[bool, bool, bool],
    purpose_order: tuple[str, ...],
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    previous = task8_probe_recovery_state(
        operation_dir,
        "recovering_probe",
        progress={
            "probe.dump": dump_flags,
            "probe.dump.sha256": sidecar_flags,
        },
    )
    task8_replace_probe_purpose_sequence(previous, purpose_order)
    current = copy.deepcopy(previous)
    task8_set_probe_progress_flag(current, "probe.dump", field)
    module.validate_operation_state(previous, operation_dir)

    with pytest.raises(
        module.OperationStateError,
        match="purpose|object|flag|progress|causal|completed",
    ):
        module.validate_operation_state(current, operation_dir, previous)


def task8_probe_recovery_stable_transition(
    previous: dict[str, object],
) -> dict[str, object]:
    current = copy.deepcopy(previous)
    history = current["phase_history"]
    assert isinstance(history, list)
    completed_epoch = max(int(entry["epoch"]) for entry in history) + 1
    current["active_transaction"] = None
    current["recovery"]["completed_epoch"] = completed_epoch
    append_phase(current, "installed", completed_epoch)
    return current


@pytest.mark.parametrize(
    "evidence_mutation",
    ("no-groups", "no-terminal-empty", "pending-tail"),
)
def test_task8_probe_recovery_stable_transition_requires_terminal_current_attempt_empty_proof(
    tmp_path: Path,
    evidence_mutation: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    completed = (True, True, True)
    previous = task8_probe_recovery_state(
        operation_dir,
        "recovering_probe",
        progress={
            "probe.dump": completed,
            "probe.dump.sha256": completed,
        },
        milestone="timer_restored",
        include_empty_proof=True,
    )
    transaction = previous["active_transaction"]
    assert isinstance(transaction, dict)
    if evidence_mutation == "no-groups":
        previous["rclone_evidence_groups"] = []
    elif evidence_mutation == "no-terminal-empty":
        previous["rclone_evidence_groups"] = previous["rclone_evidence_groups"][:-1]
    else:
        previous["rclone_evidence_groups"].append(
            task8_pending_rclone_group(
                "probe",
                int(transaction["started_epoch"]),
                len(previous["rclone_evidence_groups"]),
                purpose="probe-prefix-empty",
            )
        )
    current = task8_probe_recovery_stable_transition(previous)
    module.validate_operation_state(previous, operation_dir)

    with pytest.raises(
        module.OperationStateError,
        match="probe|cleanup|prefix-empty|terminal|rclone|pending|current-attempt",
    ):
        module.validate_operation_state(current, operation_dir, previous)


def test_task8_probe_recovery_stable_transition_accepts_exact_terminal_chain(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    completed = (True, True, True)
    previous = task8_probe_recovery_state(
        operation_dir,
        "recovering_probe",
        progress={
            "probe.dump": completed,
            "probe.dump.sha256": completed,
        },
        milestone="timer_restored",
        include_empty_proof=True,
    )
    current = task8_probe_recovery_stable_transition(previous)

    module.validate_operation_state(current, operation_dir, previous)


def test_task8_probe_pending_group_may_complete_only_as_success_or_indeterminate(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    previous = task8_probe_recovery_state(
        operation_dir,
        "recovering_probe",
    )
    transaction = previous["active_transaction"]
    assert isinstance(transaction, dict)
    pending = task8_pending_rclone_group(
        "probe",
        int(transaction["started_epoch"]),
        len(previous["rclone_evidence_groups"]),
        purpose="probe-recovery-inventory",
    )
    previous["rclone_evidence_groups"].append(pending)
    indeterminate = copy.deepcopy(pending)
    indeterminate["outcome"] = "indeterminate"
    indeterminate["after"] = task8_file_audit(
        HASH_B,
        inode=901,
        size=902,
        mtime_ns=1_760_000_000_000_000_001,
    )
    payload = {
        field: indeterminate[field]
        for field in ("group_id", "purpose", "before", "after", "outcome")
    }
    indeterminate["evidence_sha256"] = task7_evidence_sha256(
        "rclone-audit",
        payload,
    )
    current = copy.deepcopy(previous)
    current["rclone_evidence_groups"][-1] = indeterminate

    module.validate_operation_state(current, operation_dir, previous)

    rebound = copy.deepcopy(current)
    rebound["rclone_evidence_groups"][-1]["outcome"] = "arbitrary"
    rebound_payload = {
        field: rebound["rclone_evidence_groups"][-1][field]
        for field in ("group_id", "purpose", "before", "after", "outcome")
    }
    rebound["rclone_evidence_groups"][-1]["evidence_sha256"] = (
        task7_evidence_sha256("rclone-audit", rebound_payload)
    )
    with pytest.raises(
        module.OperationStateError,
        match="rclone|purpose|completion|indeterminate|rebind",
    ):
        module.validate_operation_state(rebound, operation_dir, previous)


@pytest.mark.parametrize(
    "mutation",
    ("missing-outcome", "pending-with-outcome", "completed-without-outcome", "unknown-outcome"),
)
def test_task8_rclone_outcome_schema_is_exact_and_coherent(
    tmp_path: Path,
    mutation: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "probing")
    group = task8_rclone_group("probe", 1_750_000_050, 0, 0)
    if mutation == "missing-outcome":
        del group["outcome"]
    elif mutation == "pending-with-outcome":
        group = task8_pending_copy(group)
        group["outcome"] = "success"
    elif mutation == "completed-without-outcome":
        group["outcome"] = None
        task8_reseal_rclone_group(group)
    else:
        group["outcome"] = "failed"
        task8_reseal_rclone_group(group)
    state["rclone_evidence_groups"] = [group]

    with pytest.raises(
        module.OperationStateError,
        match="rclone|outcome|exact keys|pending|success|indeterminate",
    ):
        module.validate_operation_state(state, operation_dir)


def test_task8_rclone_outcome_is_digest_bound_and_finalized_groups_are_immutable(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    previous = task8_probe_recovery_state(operation_dir, "recovering_probe")
    module.validate_operation_state(previous, operation_dir)
    success = previous["rclone_evidence_groups"][-1]
    assert isinstance(success, dict)
    indeterminate = copy.deepcopy(success)
    indeterminate["outcome"] = "indeterminate"
    task8_reseal_rclone_group(indeterminate)
    assert indeterminate["evidence_sha256"] != success["evidence_sha256"]

    digest_tamper = copy.deepcopy(previous)
    digest_tamper["rclone_evidence_groups"][-1]["outcome"] = "indeterminate"
    with pytest.raises(module.OperationStateError, match="rclone|evidence|digest"):
        module.validate_operation_state(digest_tamper, operation_dir)

    rebound = copy.deepcopy(previous)
    rebound["rclone_evidence_groups"][-1] = indeterminate
    with pytest.raises(module.OperationStateError, match="rclone|append-only"):
        module.validate_operation_state(rebound, operation_dir, previous)


@pytest.mark.parametrize(
    ("field", "prior_flags", "events"),
    (
        (
            "verified",
            (True, False, False),
            (
                ("probe-precreate-absence", "success"),
                ("probe-create:probe.dump:strict-no-existing", "success"),
                ("probe-verify:probe.dump", "indeterminate"),
            ),
        ),
        (
            "cleaned",
            (True, True, False),
            (
                ("probe-precreate-absence", "success"),
                ("probe-create:probe.dump:strict-no-existing", "success"),
                ("probe-verify:probe.dump", "success"),
                ("probe-cleanup:probe.dump", "indeterminate"),
            ),
        ),
    ),
)
def test_task8_probe_indeterminate_result_cannot_directly_advance_progress(
    tmp_path: Path,
    field: str,
    prior_flags: tuple[bool, bool, bool],
    events: tuple[tuple[str, str], ...],
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    previous = task8_probe_recovery_state(
        operation_dir,
        "recovering_probe",
        progress={
            "probe.dump": prior_flags,
            "probe.dump.sha256": (False, False, False),
        },
    )
    task8_replace_probe_event_sequence(previous, events)
    current = copy.deepcopy(previous)
    task8_set_probe_progress_flag(current, "probe.dump", field)

    module.validate_operation_state(previous, operation_dir)
    with pytest.raises(module.OperationStateError, match="probe|progress|completed|success"):
        module.validate_operation_state(current, operation_dir, previous)


def test_task8_probe_indeterminate_prefix_empty_cannot_terminalize_recovery(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    complete = (True, True, True)
    previous = task8_probe_recovery_state(
        operation_dir,
        "recovering_probe",
        progress={"probe.dump": complete, "probe.dump.sha256": complete},
        milestone="timer_restored",
        include_empty_proof=True,
    )
    terminal = previous["rclone_evidence_groups"][-1]
    assert isinstance(terminal, dict)
    terminal["outcome"] = "indeterminate"
    task8_reseal_rclone_group(terminal)
    current = task8_probe_recovery_stable_transition(previous)

    module.validate_operation_state(previous, operation_dir)
    with pytest.raises(
        module.OperationStateError,
        match="probe|cleanup|prefix-empty|terminal|success",
    ):
        module.validate_operation_state(current, operation_dir, previous)


def test_task8_probe_nonempty_delete_result_is_indeterminate_and_resumes_without_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    progress = {
        "probe.dump": (True, True, False),
        "probe.dump.sha256": (True, True, False),
    }
    initial = task8_probe_recovery_state(
        operation_dir,
        "recovering_probe",
        progress=progress,
        milestone="runtime_lock_acquired",
    )
    context, harness = task8_install_probe_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
        remote_objects=task8_probe_payloads(),
    )
    dump_delete = task8_probe_delete_argv("probe.dump")
    harness["controls"]["command_stdout_after"] = {
        dump_delete: "unexpected delete output\n"
    }

    with pytest.raises(module.OperationStateError, match="delete|output"):
        module.recover_host_configuration(context)

    interrupted = harness["holder"]["state"]
    last_group = interrupted["rclone_evidence_groups"][-1]
    assert last_group["purpose"] == "probe-cleanup:probe.dump"
    assert last_group["outcome"] == "indeterminate"
    dump = next(
        item
        for item in interrupted["active_transaction"]["probe"]["objects"]
        if item["name"] == "probe.dump"
    )
    assert dump["cleaned"] is False
    assert "probe.dump" not in harness["remote_store"]

    final = module.recover_host_configuration(context)

    assert final["phase"] == "installed"
    assert harness["command_calls"].count(dump_delete) == 1
    assert harness["remote_store"] == {}


@pytest.mark.parametrize(
    ("last_success", "prior_flags", "events", "expected_first_flags", "remote_live"),
    (
        (
            "create",
            (False, False, False),
            (
                ("probe-precreate-absence", "success"),
                ("probe-create:probe.dump:strict-no-existing", "success"),
            ),
            (False, False, False),
            True,
        ),
        (
            "create-absent",
            (False, False, False),
            (
                ("probe-precreate-absence", "success"),
                ("probe-create:probe.dump:strict-no-existing", "success"),
            ),
            (False, False, False),
            False,
        ),
        (
            "verify",
            (True, False, False),
            (
                ("probe-precreate-absence", "success"),
                ("probe-create:probe.dump:strict-no-existing", "success"),
                ("probe-verify:probe.dump", "success"),
            ),
            (True, True, False),
            True,
        ),
        (
            "cleanup",
            (True, True, False),
            (
                ("probe-precreate-absence", "success"),
                ("probe-create:probe.dump:strict-no-existing", "success"),
                ("probe-verify:probe.dump", "success"),
                ("probe-cleanup:probe.dump", "success"),
            ),
            (True, True, True),
            False,
        ),
        (
            "adopt",
            (False, False, False),
            (
                ("probe-precreate-absence", "success"),
                (
                    "probe-create:probe.dump:strict-no-existing",
                    "indeterminate",
                ),
                ("probe-recovery-root", "success"),
                ("probe-recovery-parent", "success"),
                ("probe-recovery-inventory", "success"),
                ("probe-adopt:probe.dump", "success"),
            ),
            (True, True, False),
            True,
        ),
    ),
)
def test_task8_probe_reconciles_last_semantic_success_before_fresh_remote_work(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    last_success: str,
    prior_flags: tuple[bool, bool, bool],
    events: tuple[tuple[str, str], ...],
    expected_first_flags: tuple[bool, bool, bool],
    remote_live: bool,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    initial = task8_probe_recovery_state(
        operation_dir,
        "recovering_probe",
        progress={
            "probe.dump": prior_flags,
            "probe.dump.sha256": (False, False, False),
        },
        milestone="runtime_lock_acquired",
    )
    task8_replace_probe_event_sequence(initial, events)
    remote = (
        {"probe.dump": task8_probe_payloads()["probe.dump"]}
        if remote_live
        else {}
    )
    context, harness = task8_install_probe_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
        remote_objects=remote,
    )

    final = module.recover_host_configuration(context)

    assert final["phase"] == "installed", last_success
    first_command_state = harness["command_states"][0]
    dump = next(
        item
        for item in first_command_state["active_transaction"]["probe"]["objects"]
        if item["name"] == "probe.dump"
    )
    assert (dump["created"], dump["verified"], dump["cleaned"]) == (
        expected_first_flags
    )


@pytest.mark.parametrize("unsafe_case", ("wrong-hash", "untracked"))
def test_task8_probe_ambiguous_create_adoption_refuses_unsafe_inventory_without_delete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    unsafe_case: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    initial = task8_probe_recovery_state(
        operation_dir,
        "recovering_probe",
        milestone="runtime_lock_acquired",
        pending_purpose="probe-create:probe.dump:strict-no-existing",
    )
    dump = task8_probe_payloads()["probe.dump"]
    remote = {"probe.dump": dump}
    if unsafe_case == "wrong-hash":
        remote["probe.dump"] = bytes([dump[0] ^ 1]) + dump[1:]
    else:
        remote["intruder.dump"] = b"untracked\n"
    context, harness = task8_install_probe_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
        remote_objects=remote,
    )

    with pytest.raises(module.OperationStateError, match="probe|remote|hash|untracked"):
        module.recover_host_configuration(context)

    interrupted = harness["holder"]["state"]
    tracked = next(
        item
        for item in interrupted["active_transaction"]["probe"]["objects"]
        if item["name"] == "probe.dump"
    )
    assert tracked["created"] is False
    assert tracked["verified"] is False
    assert tracked["cleaned"] is False
    assert not any("deletefile" in argv for argv in harness["command_calls"])
    assert harness["remote_store"] == remote
    assert interrupted["rclone_evidence_groups"][-1]["outcome"] == (
        "indeterminate"
    )


TASK8_NORMAL_PROBE_PURPOSES = (
    "probe-precreate-root",
    "probe-precreate-root-absence",
    "probe-create:probe.dump:strict-no-existing",
    "probe-create:probe.dump.sha256:strict-no-existing",
    "probe-owned-inventory",
    "probe-verify:probe.dump",
    "probe-verify:probe.dump.sha256",
    "probe-cleanup:probe.dump",
    "probe-cleanup:probe.dump.sha256",
    "probe-prefix-empty",
)


def task8_probe_precreate_root_argv() -> tuple[str, ...]:
    return (
        "/usr/bin/rclone",
        "--config",
        "/etc/degen/rclone.conf",
        "lsjson",
        "onedrive:backups/",
    )


def task8_probe_precreate_parent_argv() -> tuple[str, ...]:
    return (
        "/usr/bin/rclone",
        "--config",
        "/etc/degen/rclone.conf",
        "lsjson",
        "onedrive:backups/degen-db-probe/",
    )


def task8_probe_create_argv(source: str, name: str) -> tuple[str, ...]:
    return (
        "/usr/bin/rclone",
        "--config",
        "/etc/degen/rclone.conf",
        "--ignore-existing",
        "--error-on-no-transfer",
        "copyto",
        source,
        f"{SAFE_PROBE_PREFIX}{name}",
    )


def task8_probe_progress_snapshot(
    state: dict[str, object],
) -> tuple[tuple[bool, bool, bool], ...] | None:
    transaction = state.get("active_transaction")
    if not isinstance(transaction, dict) or transaction.get("kind") != "probe":
        return None
    probe = transaction.get("probe")
    assert isinstance(probe, dict)
    objects = probe.get("objects")
    assert isinstance(objects, list)
    return tuple(
        (bool(item["created"]), bool(item["verified"]), bool(item["cleaned"]))
        for item in objects
    )


def task8_install_normal_probe_harness(
    module: object,
    monkeypatch: pytest.MonkeyPatch,
    operation_dir: Path,
    *,
    remote_objects: dict[str, bytes] | None = None,
) -> tuple[object, dict[str, object]]:
    token = "0123456789abcdef0123456789abcdef"

    def token_hex(byte_count: int) -> str:
        assert byte_count == 16
        return token

    monkeypatch.setattr(module.secrets, "token_hex", token_hex)
    initial = state_at_phase(operation_dir, "installed")
    module.validate_operation_state(initial, operation_dir)
    return task8_install_probe_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
        remote_objects=remote_objects,
    )


def test_task8_normal_probe_success_persists_exact_chain_and_stable_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_probe_harness(
        module,
        monkeypatch,
        operation_dir,
    )

    result = module.probe_remote_storage(context)

    assert result["phase"] == "probed"
    assert result["active_transaction"] is None
    assert result["failure"] is None
    assert result["recovery"] is None
    assert harness["remote_store"] == {}
    assert harness["proof_routes"]
    assert set(harness["proof_routes"]) == {"installed"}
    module.validate_operation_state(result, operation_dir)

    writes = harness["writes"]
    entry_state = next(
        durable
        for durable in writes
        if durable["phase"] == "probing"
        and len(durable["rclone_evidence_groups"]) == 0
    )
    transaction = entry_state["active_transaction"]
    assert isinstance(transaction, dict)
    assert transaction["kind"] == "probe"
    assert transaction["prior_stable_phase"] == "installed"
    assert transaction["started_epoch"] == entry_state["phase_history"][-1]["epoch"]
    assert transaction["runtime_baseline"] == recovery_runtime_baseline()
    assert transaction["probe"] == {
        "prefix": SAFE_PROBE_PREFIX,
        "objects": task8_probe_objects(),
    }
    assert not any(transaction["guard"].values())
    assert entry_state["probe"] is None
    assert entry_state["phase_history"][-1] == {
        "phase": "probing",
        "epoch": transaction["started_epoch"],
        "evidence_sha256": task8_probe_entry_evidence(
            operation_dir,
            int(transaction["started_epoch"]),
            task8_probe_objects(),
        ),
    }

    assert harness["command_purposes"] == list(TASK8_NORMAL_PROBE_PURPOSES)
    create_sources = harness["create_sources"]
    assert [item["name"] for item in create_sources] == [
        "probe.dump",
        "probe.dump.sha256",
    ]
    assert [item["contents"] for item in create_sources] == list(
        task8_probe_payloads().values()
    )
    expected_calls = [
        task8_probe_precreate_root_argv(),
        task8_probe_precreate_root_argv(),
        task8_probe_create_argv(
            str(create_sources[0]["source"]),
            "probe.dump",
        ),
        task8_probe_create_argv(
            str(create_sources[1]["source"]),
            "probe.dump.sha256",
        ),
        task8_probe_list_argv(),
        task8_probe_hashsum_argv("probe.dump"),
        task8_probe_hashsum_argv("probe.dump.sha256"),
        task8_probe_delete_argv("probe.dump"),
        task8_probe_delete_argv("probe.dump.sha256"),
        task8_probe_list_argv(),
    ]
    assert harness["command_calls"] == expected_calls
    assert len(harness["fresh_acquisitions"]) == 1
    runtime_lock_fd = harness["fresh_acquisitions"][0][1]
    assert harness["command_pass_fds"] == [
        (runtime_lock_fd,) for _call in expected_calls
    ]

    expected_before_command = (
        ((False, False, False), (False, False, False)),
        ((False, False, False), (False, False, False)),
        ((False, False, False), (False, False, False)),
        ((True, False, False), (False, False, False)),
        ((True, False, False), (True, False, False)),
        ((True, False, False), (True, False, False)),
        ((True, True, False), (True, False, False)),
        ((True, True, False), (True, True, False)),
        ((True, True, True), (True, True, False)),
        ((True, True, True), (True, True, True)),
    )
    assert tuple(
        task8_probe_progress_snapshot(state)
        for state in harness["command_states"]
    ) == expected_before_command
    for purpose, state in zip(
        TASK8_NORMAL_PROBE_PURPOSES,
        harness["command_states"],
        strict=True,
    ):
        pending = state["rclone_evidence_groups"][-1]
        assert pending["purpose"] == purpose
        assert pending["after"] is None
        assert pending["outcome"] is None
        assert pending["evidence_sha256"] is None

    progress_writes: list[tuple[tuple[bool, bool, bool], ...]] = []
    for durable in writes:
        snapshot = task8_probe_progress_snapshot(durable)
        if snapshot is not None and (
            not progress_writes or snapshot != progress_writes[-1]
        ):
            progress_writes.append(snapshot)
    assert progress_writes == [
        ((False, False, False), (False, False, False)),
        ((True, False, False), (False, False, False)),
        ((True, False, False), (True, False, False)),
        ((True, True, False), (True, False, False)),
        ((True, True, False), (True, True, False)),
        ((True, True, True), (True, True, False)),
        ((True, True, True), (True, True, True)),
    ]

    probe_groups = [
        group
        for group in result["rclone_evidence_groups"]
        if str(group["group_id"]).startswith("task8:probe:")
    ]
    assert [group["purpose"] for group in probe_groups] == list(
        TASK8_NORMAL_PROBE_PURPOSES
    )
    assert len(harness["local_audits"]) == 2 * len(probe_groups)
    for ordinal, group in enumerate(probe_groups):
        assert group["outcome"] == "success"
        assert group["before"] == harness["local_audits"][2 * ordinal]
        assert group["after"] == harness["local_audits"][2 * ordinal + 1]
        expected_group = copy.deepcopy(group)
        task8_reseal_rclone_group(expected_group)
        assert group["evidence_sha256"] == expected_group["evidence_sha256"]

    history = result["phase_history"]
    probe_entry = next(entry for entry in reversed(history) if entry["phase"] == "probing")
    completed = history[-1]
    assert completed["phase"] == "probed"
    assert result["probe"] == {
        "prefix": SAFE_PROBE_PREFIX,
        "owned_names": ["probe.dump", "probe.dump.sha256"],
        "cleanup_proven": True,
        "evidence_sha256": task8_probe_completion_evidence(
            operation_dir,
            entry=probe_entry,
            completed_epoch=int(completed["epoch"]),
            prefix=SAFE_PROBE_PREFIX,
            owned_names=["probe.dump", "probe.dump.sha256"],
            cleanup_proven=True,
            groups=probe_groups,
        ),
    }
    assert completed["evidence_sha256"] == result["probe"]["evidence_sha256"]

    assert harness["external_actions"] == [
        "timer_disable",
        "timer_stop",
        "quiesce_service_check",
        "quiesce_runtime_readback",
        "legacy_lock_acquire",
        "runtime_lock_acquire",
        "post_lock_service_recheck",
        "runtime_lock_release",
        "legacy_lock_release",
        "pre_restore_service_check",
        "timer_enable",
        "timer_start",
        "restore_runtime_readback",
    ]
    assert harness["release_actions"] == [
        "runtime_lock_release",
        "legacy_lock_release",
    ]
    assert harness["live_runtime"] == harness["baseline"]
    for command_state in harness["command_states"]:
        guard = command_state["active_transaction"]["guard"]
        assert guard == {
            "timer_stopped": True,
            "service_inactive_verified": True,
            "legacy_lock_acquired": True,
            "runtime_lock_acquired": True,
            "locks_released": False,
            "timer_restored": False,
        }
    command_indexes = [
        index
        for index, event in enumerate(harness["timeline"])
        if event.startswith("command:")
    ]
    assert command_indexes
    assert harness["timeline"].index("action:post_lock_service_recheck") < min(
        command_indexes
    )
    assert max(command_indexes) < harness["timeline"].index(
        "action:runtime_lock_release"
    )
    assert harness["timeline"].index("action:restore_runtime_readback") < next(
        index
        for index, event in enumerate(harness["timeline"])
        if event == "write:probed"
    )


def test_task8_normal_probe_colliding_namespace_is_indeterminate_recovery_required(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    preexisting = {"probe.dump": task8_probe_payloads()["probe.dump"]}
    context, harness = task8_install_normal_probe_harness(
        module,
        monkeypatch,
        operation_dir,
        remote_objects=preexisting,
    )

    with pytest.raises(
        module.OperationStateError,
        match="namespace|pre-create absence|already exists",
    ):
        module.probe_remote_storage(context)

    interrupted = harness["holder"]["state"]
    assert interrupted["phase"] == "recovery_required"
    assert interrupted["probe"] is None
    assert interrupted["failure"]["phase"] == "probing"
    assert interrupted["recovery"]["kind"] == "probe"
    assert interrupted["active_transaction"]["probe"] == {
        "prefix": SAFE_PROBE_PREFIX,
        "objects": task8_probe_objects(),
    }
    assert harness["command_calls"] == [
        task8_probe_precreate_root_argv(),
        task8_probe_precreate_parent_argv(),
    ]
    assert harness["command_purposes"] == [
        "probe-precreate-root",
        "probe-precreate-parent-absence",
    ]
    first_group = interrupted["rclone_evidence_groups"][-2]
    assert first_group["purpose"] == "probe-precreate-root"
    assert first_group["outcome"] == "success"
    group = interrupted["rclone_evidence_groups"][-1]
    assert group["purpose"] == "probe-precreate-parent-absence"
    assert group["after"] is not None
    assert group["outcome"] == "indeterminate"
    assert group["evidence_sha256"] is not None
    assert harness["remote_store"] == preexisting
    assert harness["create_sources"] == []
    assert harness["live_runtime"]["timer_enabled"] is False
    assert harness["live_runtime"]["timer_active"] is False
    assert "timer_enable" not in harness["external_actions"]
    assert "timer_start" not in harness["external_actions"]
    module.validate_operation_state(interrupted, operation_dir)


def test_task8_normal_probe_process_death_resumes_without_fabricated_receipts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_probe_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    harness["controls"]["command_crash_after_purpose"] = (
        "probe-create:probe.dump:strict-no-existing"
    )

    with pytest.raises(Task8ProbeProcessDeath, match="process death"):
        module.probe_remote_storage(context)

    crashed = copy.deepcopy(harness["holder"]["state"])
    assert crashed["phase"] == "probing"
    assert crashed["probe"] is None
    assert crashed["failure"] is None
    assert crashed["recovery"] is None
    assert crashed["active_transaction"]["guard"] == {
        "timer_stopped": True,
        "service_inactive_verified": True,
        "legacy_lock_acquired": True,
        "runtime_lock_acquired": True,
        "locks_released": False,
        "timer_restored": False,
    }
    assert task8_probe_progress_snapshot(crashed) == (
        (False, False, False),
        (False, False, False),
    )
    assert [group["purpose"] for group in crashed["rclone_evidence_groups"]] == [
        "probe-precreate-root",
        "probe-precreate-root-absence",
        "probe-create:probe.dump:strict-no-existing",
    ]
    assert crashed["rclone_evidence_groups"][0]["outcome"] == "success"
    assert crashed["rclone_evidence_groups"][1]["outcome"] == "success"
    pending = crashed["rclone_evidence_groups"][-1]
    assert pending["after"] is None
    assert pending["outcome"] is None
    assert pending["evidence_sha256"] is None
    assert harness["remote_store"] == {
        "probe.dump": task8_probe_payloads()["probe.dump"]
    }
    assert harness["release_actions"] == []
    assert harness["live_runtime"]["timer_enabled"] is False
    assert harness["live_runtime"]["timer_active"] is False
    module.validate_operation_state(crashed, operation_dir)


    final = module.recover_host_configuration(context)

    assert final["phase"] == "installed"
    assert final["probe"] is None
    assert final["active_transaction"] is None
    assert final["recovery"]["kind"] == "probe"
    assert final["recovery"]["completed_epoch"] is not None
    assert final["failure"]["phase"] == "probing"
    assert not any(entry["phase"] == "probed" for entry in final["phase_history"])
    create_group = next(
        group
        for group in final["rclone_evidence_groups"]
        if group["purpose"] == "probe-create:probe.dump:strict-no-existing"
    )
    assert create_group["after"] is not None
    assert create_group["outcome"] == "indeterminate"
    assert create_group["evidence_sha256"] is not None
    assert harness["command_purposes"].count(
        "probe-create:probe.dump:strict-no-existing"
    ) == 1
    assert harness["remote_store"] == {}
    assert harness["live_runtime"] == harness["baseline"]
    module.validate_operation_state(final, operation_dir)


def test_task8_normal_probe_crash_before_first_create_recovers_via_root_absence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_probe_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    harness["controls"]["command_crash_after_purpose"] = (
        "probe-precreate-root-absence"
    )

    with pytest.raises(Task8ProbeProcessDeath, match="process death"):
        module.probe_remote_storage(context)

    crashed = copy.deepcopy(harness["holder"]["state"])
    assert crashed["phase"] == "probing"
    assert crashed["failure"] is None
    assert crashed["recovery"] is None
    assert task8_probe_progress_snapshot(crashed) == (
        (False, False, False),
        (False, False, False),
    )
    assert harness["controls"]["probe_parent_exists"] is False
    assert harness["controls"]["probe_prefix_exists"] is False
    assert harness["remote_store"] == {}
    initial_groups = crashed["rclone_evidence_groups"]
    assert [group["purpose"] for group in initial_groups] == [
        "probe-precreate-root",
        "probe-precreate-root-absence",
    ]
    assert initial_groups[0]["outcome"] == "success"
    assert initial_groups[1]["after"] is None
    assert initial_groups[1]["outcome"] is None

    final = module.recover_host_configuration(context)

    assert final["phase"] == "installed"
    assert final["active_transaction"] is None
    assert final["probe"] is None
    assert final["recovery"]["kind"] == "probe"
    assert final["recovery"]["completed_epoch"] is not None
    assert harness["command_purposes"] == [
        "probe-precreate-root",
        "probe-precreate-root-absence",
        "probe-recovery-root",
        "probe-recovery-root-absence",
    ]
    assert harness["command_calls"] == [
        task8_probe_precreate_root_argv(),
        task8_probe_precreate_root_argv(),
        task8_probe_precreate_root_argv(),
        task8_probe_precreate_root_argv(),
    ]
    assert task8_probe_list_argv() not in harness["command_calls"]
    assert not any("deletefile" in argv for argv in harness["command_calls"])
    first_runtime_fd = harness["fresh_acquisitions"][0][1]
    recovery_runtime_fd = harness["fresh_acquisitions"][1][1]
    assert harness["command_pass_fds"] == [
        (first_runtime_fd,),
        (first_runtime_fd,),
        (recovery_runtime_fd,),
        (recovery_runtime_fd,),
    ]
    for purpose, command_state in zip(
        harness["command_purposes"],
        harness["command_states"],
        strict=True,
    ):
        pending = command_state["rclone_evidence_groups"][-1]
        assert pending["purpose"] == purpose
        assert pending["after"] is None
        assert pending["outcome"] is None
    groups = [
        group
        for group in final["rclone_evidence_groups"]
        if str(group["group_id"]).startswith("task8:probe:")
    ]
    assert [(group["purpose"], group["outcome"]) for group in groups] == [
        ("probe-precreate-root", "success"),
        ("probe-precreate-root-absence", "indeterminate"),
        ("probe-recovery-root", "success"),
        ("probe-recovery-root-absence", "success"),
    ]
    assert len(harness["local_audits"]) == 8
    assert harness["remote_store"] == {}
    assert harness["live_runtime"] == harness["baseline"]
    module.validate_operation_state(final, operation_dir)


def task8_probe_namespace_json(
    *entries: tuple[str, bool],
) -> str:
    return json.dumps(
        [
            {
                "Path": name,
                "Name": name,
                "Size": -1 if is_dir else 0,
                "MimeType": (
                    "inode/directory" if is_dir else "application/octet-stream"
                ),
                "ModTime": "2026-07-01T00:00:00Z",
                "IsDir": is_dir,
                "ID": f"namespace-{index}",
                "OrigID": "",
                "Tier": "",
            }
            for index, (name, is_dir) in enumerate(entries)
        ],
        separators=(",", ":"),
    )


def task8_probe_namespace_identity(prefix: str = SAFE_PROBE_PREFIX) -> str:
    return prefix.rstrip("/").rsplit("/", 1)[-1]


def test_task8_probe_adversarial_parent_exists_candidate_absent_succeeds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_probe_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    harness["controls"]["namespace_listing_responses"] = [
        task8_probe_namespace_json(("degen-db-probe", True)),
        task8_probe_namespace_json(("another-operation", True)),
    ]

    result = module.probe_remote_storage(context)

    expected_purposes = [
        "probe-precreate-root",
        "probe-precreate-parent-absence",
        *TASK8_NORMAL_PROBE_PURPOSES[2:],
    ]
    assert result["phase"] == "probed"
    assert harness["command_purposes"] == expected_purposes
    assert harness["command_calls"][:2] == [
        task8_probe_precreate_root_argv(),
        task8_probe_precreate_parent_argv(),
    ]
    assert "probe-precreate-root-absence" not in harness["command_purposes"]
    probe_groups = [
        group
        for group in result["rclone_evidence_groups"]
        if str(group["group_id"]).startswith("task8:probe:")
    ]
    assert [group["purpose"] for group in probe_groups] == expected_purposes
    assert all(group["outcome"] == "success" for group in probe_groups)
    assert harness["remote_store"] == {}
    module.validate_operation_state(result, operation_dir)


@pytest.mark.parametrize(
    ("scope", "defect"),
    (
        ("root", "case-collision"),
        ("root", "wrong-type"),
        ("parent", "case-collision"),
        ("parent", "wrong-type"),
    ),
)
def test_task8_probe_adversarial_namespace_metadata_fails_indeterminate_without_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    scope: str,
    defect: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_probe_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    if scope == "root":
        name = (
            "DEGEN-DB-PROBE"
            if defect == "case-collision"
            else "degen-db-probe"
        )
        responses = [task8_probe_namespace_json((name, defect != "wrong-type"))]
        expected_purposes = ["probe-precreate-root"]
    else:
        identity = task8_probe_namespace_identity()
        name = identity.upper() if defect == "case-collision" else identity
        responses = [
            task8_probe_namespace_json(("degen-db-probe", True)),
            task8_probe_namespace_json((name, defect != "wrong-type")),
        ]
        expected_purposes = [
            "probe-precreate-root",
            "probe-precreate-parent-absence",
        ]
    harness["controls"]["namespace_listing_responses"] = responses

    with pytest.raises(
        module.OperationStateError,
        match="namespace|case|directory|exists|type",
    ):
        module.probe_remote_storage(context)

    interrupted = harness["holder"]["state"]
    assert interrupted["phase"] == "recovery_required"
    assert interrupted["probe"] is None
    assert interrupted["recovery"]["kind"] == "probe"
    assert harness["command_purposes"] == expected_purposes
    assert interrupted["rclone_evidence_groups"][-1]["outcome"] == (
        "indeterminate"
    )
    assert interrupted["rclone_evidence_groups"][-1]["after"] is not None
    assert harness["create_sources"] == []
    assert harness["remote_store"] == {}
    assert not any(
        command in argv
        for argv in harness["command_calls"]
        for command in ("copyto", "hashsum", "deletefile")
    )
    module.validate_operation_state(interrupted, operation_dir)


@pytest.mark.parametrize(
    "defect",
    ("missing", "extra", "directory", "size-drift"),
)
def test_task8_probe_adversarial_owned_inventory_fails_before_hash_or_delete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    defect: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_probe_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    payloads = task8_probe_payloads()
    if defect == "missing":
        listing = task8_probe_inventory_json(
            {"probe.dump": payloads["probe.dump"]}
        )
    elif defect == "extra":
        listing = task8_probe_inventory_json(
            {**payloads, "intruder.dump": b"untracked\n"}
        )
    elif defect == "directory":
        listing = task8_probe_inventory_json(
            payloads,
            remote_directories={"unexpected-directory"},
        )
    else:
        rows = json.loads(task8_probe_inventory_json(payloads))
        rows[0]["Size"] += 1
        listing = json.dumps(rows, separators=(",", ":"))
    harness["controls"]["listing_responses"] = [listing]

    with pytest.raises(
        module.OperationStateError,
        match="inventory|object|directory|size|untracked|incomplete",
    ):
        module.probe_remote_storage(context)

    interrupted = harness["holder"]["state"]
    assert interrupted["phase"] == "recovery_required"
    assert interrupted["probe"] is None
    assert interrupted["recovery"]["kind"] == "probe"
    assert harness["command_purposes"] == list(TASK8_NORMAL_PROBE_PURPOSES[:5])
    assert task8_probe_progress_snapshot(interrupted) == (
        (True, False, False),
        (True, False, False),
    )
    group = interrupted["rclone_evidence_groups"][-1]
    assert group["purpose"] == "probe-owned-inventory"
    assert group["outcome"] == "indeterminate"
    assert group["after"] is not None
    assert harness["remote_store"] == payloads
    assert not any(
        command in argv
        for argv in harness["command_calls"]
        for command in ("hashsum", "deletefile")
    )
    module.validate_operation_state(interrupted, operation_dir)


def test_task8_probe_adversarial_create_failure_is_indeterminate_without_flags(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_probe_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    purpose = "probe-create:probe.dump:strict-no-existing"
    harness["controls"]["command_error_after_purpose"] = purpose

    with pytest.raises(
        module.OperationStateError,
        match="create|command|controlled|rclone",
    ):
        module.probe_remote_storage(context)

    interrupted = harness["holder"]["state"]
    assert interrupted["phase"] == "recovery_required"
    assert interrupted["probe"] is None
    assert interrupted["recovery"]["kind"] == "probe"
    assert harness["command_purposes"] == [
        "probe-precreate-root",
        "probe-precreate-root-absence",
        purpose,
    ]
    assert task8_probe_progress_snapshot(interrupted) == (
        (False, False, False),
        (False, False, False),
    )
    group = interrupted["rclone_evidence_groups"][-1]
    assert group["purpose"] == purpose
    assert group["outcome"] == "indeterminate"
    assert group["after"] is not None
    assert interrupted["active_transaction"]["probe"] == {
        "prefix": SAFE_PROBE_PREFIX,
        "objects": task8_probe_objects(),
    }
    assert harness["remote_store"] == {
        "probe.dump": task8_probe_payloads()["probe.dump"]
    }
    assert len(harness["create_sources"]) == 1
    assert not any(
        command in argv
        for argv in harness["command_calls"]
        for command in ("hashsum", "deletefile")
    )
    module.validate_operation_state(interrupted, operation_dir)


def test_task8_probe_adversarial_stable_receipt_requires_owned_inventory_when_resealed(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "probed")
    groups = state["rclone_evidence_groups"]
    assert isinstance(groups, list)
    retained = [
        copy.deepcopy(group)
        for group in groups
        if group["purpose"] != "probe-owned-inventory"
    ]
    ordinal = 0
    for group in retained:
        if str(group["group_id"]).startswith("task8:probe:"):
            group["group_id"] = str(group["group_id"]).rsplit(":", 1)[0] + (
                f":{ordinal}"
            )
            ordinal += 1
            task8_reseal_rclone_group(group)
    state["rclone_evidence_groups"] = retained
    task8_reseal_probe_completion(state, operation_dir)

    assert not any(
        group["purpose"] == "probe-owned-inventory"
        for group in state["rclone_evidence_groups"]
    )
    with pytest.raises(
        module.OperationStateError,
        match="probe|owned|inventory|sequence|purpose",
    ):
        module.validate_operation_state(state, operation_dir)


@pytest.mark.parametrize(
    "defect",
    (
        "missing-flags",
        "reordered-flags",
        "rcat",
        "immutable",
        "wrong-source",
        "wrong-target",
    ),
)
def test_task8_probe_adversarial_create_binding_rejects_unsafe_argv(
    tmp_path: Path,
    defect: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "probing")
    state["rclone_evidence_groups"] = task8_successful_probe_groups()[:2]
    module.validate_operation_state(state, operation_dir)
    purpose = "probe-create:probe.dump:strict-no-existing"
    source = str(module._task8_probe_source_path(state, "probe.dump"))
    valid = task8_probe_create_argv(source, "probe.dump")
    module._task8_validate_probe_recovery_purpose_argv(
        state,
        purpose,
        valid,
    )
    if defect == "missing-flags":
        argv = (*valid[:3], "copyto", *valid[6:])
    elif defect == "reordered-flags":
        argv = (
            *valid[:3],
            "--error-on-no-transfer",
            "--ignore-existing",
            *valid[5:],
        )
    elif defect == "rcat":
        argv = (*valid[:3], "rcat", valid[-1])
    elif defect == "immutable":
        argv = (*valid[:3], "--immutable", *valid[4:])
    elif defect == "wrong-source":
        argv = (*valid[:6], str(operation_dir / "wrong-probe-source"), valid[-1])
    else:
        argv = (*valid[:-1], "onedrive:backups/degen-db/probe.dump")
    before = copy.deepcopy(state)

    with pytest.raises(
        module.OperationStateError,
        match="probe|purpose|binding|descriptor|argv|source",
    ):
        module._task8_validate_probe_recovery_purpose_argv(
            state,
            purpose,
            tuple(argv),
        )

    assert state == before


def test_task8_probe_adversarial_unexpected_runtime_fd_rejects_before_audit_or_command(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "probing")
    state["rclone_evidence_groups"] = task8_successful_probe_groups()[:2]
    purpose = "probe-create:probe.dump:strict-no-existing"
    source = str(module._task8_probe_source_path(state, "probe.dump"))
    argv = task8_probe_create_argv(source, "probe.dump")
    audits: list[str] = []
    commands: list[tuple[tuple[str, ...], tuple[int, ...]]] = []
    monkeypatch.setattr(
        module,
        "_task8_begin_rclone_audit",
        lambda *_args, **_kwargs: audits.append("audit"),
    )
    context = types.SimpleNamespace(
        command_runner=lambda command, pass_fds: commands.append(
            (tuple(command), pass_fds)
        )
    )
    before = copy.deepcopy(state)

    with pytest.raises(
        module.OperationStateError,
        match="runtime lock|descriptor|rclone command",
    ):
        module._task8_run_audited_rclone_command(
            context,
            object(),
            state,
            purpose=purpose,
            argv=argv,
            label="controlled unsafe runtime lock descriptor",
            runtime_lock_fd=-1,
        )

    assert audits == []
    assert commands == []
    assert state == before


def task8_interrupted_probe_create_source(
    module: object,
    operation_dir: Path,
) -> tuple[dict[str, object], Path, bytes]:
    initial = task8_probe_recovery_state(
        operation_dir,
        "probing",
        milestone="runtime_lock_acquired",
        pending_purpose="probe-create:probe.dump:strict-no-existing",
    )
    pending = initial["rclone_evidence_groups"][-1]
    assert pending["purpose"] == "probe-create:probe.dump:strict-no-existing"
    assert pending["after"] is None
    expected = task8_probe_payloads()["probe.dump"]
    source_path = module._task8_probe_source_path(initial, "probe.dump")
    assert source_path.parent == operation_dir
    return initial, source_path, expected


def test_task8_probe_source_residue_deterministic_prefix_is_removed_before_remote_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    initial, source_path, expected = task8_interrupted_probe_create_source(
        module,
        operation_dir,
    )
    deterministic_prefix = expected[: len(expected) // 2]
    source_path.write_bytes(deterministic_prefix)
    source_path.chmod(0o600)
    context, harness = task8_install_probe_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
    )
    source_exists_at_remote_command: list[bool] = []
    original_runner = context.command_runner

    def observing_runner(
        argv: object,
        pass_fds: tuple[int, ...],
    ) -> subprocess.CompletedProcess[str]:
        source_exists_at_remote_command.append(os.path.lexists(source_path))
        return original_runner(argv, pass_fds)

    context.command_runner = observing_runner

    result = module.recover_host_configuration(context)

    assert result["phase"] == "installed"
    assert source_exists_at_remote_command
    assert not any(source_exists_at_remote_command)
    assert not os.path.lexists(source_path)
    assert harness["command_purposes"] == [
        "probe-recovery-root",
        "probe-recovery-parent",
        "probe-recovery-inventory",
        "probe-prefix-empty",
    ]
    assert not any("copyto" in argv for argv in harness["command_calls"])
    assert harness["remote_store"] == {}
    module.validate_operation_state(result, operation_dir)


@pytest.mark.parametrize("defect", ("wrong-bytes", "symlink", "unsafe-mode"))
def test_task8_probe_source_residue_unsafe_file_fails_closed_and_remains(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    defect: str,
) -> None:
    if defect == "unsafe-mode" and os.name != "posix":
        pytest.skip("unsafe source mode enforcement requires POSIX metadata")
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    initial, source_path, expected = task8_interrupted_probe_create_source(
        module,
        operation_dir,
    )
    deterministic_prefix = expected[: len(expected) // 2]
    outside: Path | None = None
    if defect == "wrong-bytes":
        source_path.write_bytes(
            bytes([deterministic_prefix[0] ^ 1]) + deterministic_prefix[1:]
        )
        source_path.chmod(0o600)
    elif defect == "symlink":
        outside = tmp_path / "outside-probe-source"
        outside.write_bytes(deterministic_prefix)
        outside.chmod(0o600)
        try:
            source_path.symlink_to(outside)
        except OSError as exc:
            pytest.skip(f"symlink creation is unavailable: {exc}")
    else:
        source_path.write_bytes(deterministic_prefix)
        source_path.chmod(0o644)
    context, harness = task8_install_probe_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
    )

    with pytest.raises(
        module.OperationStateError,
        match="probe source|deterministic|regular file|ownership|mode|unsafe",
    ):
        module.recover_host_configuration(context)

    interrupted = harness["holder"]["state"]
    assert interrupted["phase"] == "recovery_required"
    assert os.path.lexists(source_path)
    if defect == "wrong-bytes":
        assert source_path.read_bytes() != deterministic_prefix
    elif defect == "symlink":
        assert source_path.is_symlink()
        assert outside is not None and outside.read_bytes() == deterministic_prefix
    else:
        assert stat.S_IMODE(source_path.stat().st_mode) == 0o644
    assert harness["command_calls"] == []
    assert harness["remote_store"] == {}
    assert not any("deletefile" in argv for argv in harness["command_calls"])
    module.validate_operation_state(interrupted, operation_dir)


def test_task8_normal_probe_review_preserves_two_release_issues_and_blocks_timer_restore(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_probe_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    harness["controls"]["release_issues"] = [
        module._Task7LockReleaseIssue(
            "release_runtime_lock",
            OSError("controlled runtime unlock uncertainty"),
            True,
        ),
        module._Task7LockReleaseIssue(
            "release_legacy_lock",
            OSError("controlled legacy close uncertainty"),
            True,
        ),
    ]

    with pytest.raises(
        module.OperationStateError,
        match="normal probe migration lock release failed",
    ):
        module.probe_remote_storage(context)

    interrupted = harness["holder"]["state"]
    assert interrupted["phase"] == "recovery_required"
    assert interrupted["failure"]["phase"] == "probing"
    assert "migration lock release failed" in interrupted["failure"]["primary_error"]
    assert [item["stage"] for item in interrupted["secondary_errors"]] == [
        "release_runtime_lock",
        "release_legacy_lock",
    ]
    assert "runtime unlock uncertainty" in interrupted["secondary_errors"][0]["error"]
    assert "legacy close uncertainty" in interrupted["secondary_errors"][1]["error"]
    guard = interrupted["active_transaction"]["guard"]
    assert guard["locks_released"] is False
    assert guard["timer_restored"] is False
    assert harness["release_actions"] == [
        "runtime_lock_release",
        "legacy_lock_release",
    ]
    assert "pre_restore_service_check" not in harness["external_actions"]
    assert "timer_enable" not in harness["external_actions"]
    assert "timer_start" not in harness["external_actions"]
    assert harness["live_runtime"]["timer_enabled"] is False
    assert harness["live_runtime"]["timer_active"] is False
    module.validate_operation_state(interrupted, operation_dir)


def task8_install_probe_source_cleanup_failure(
    module: object,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[OSError, object, list[str]]:
    cleanup_error = OSError("controlled operation-source cleanup failure")
    original_unlink = module.os.unlink
    attempted_names: list[str] = []

    def failing_unlink(path: object, *args: object, **kwargs: object) -> None:
        name = Path(os.fspath(path)).name
        if name.startswith(".task8-probe-source-"):
            attempted_names.append(name)
            raise cleanup_error
        original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(module.os, "unlink", failing_unlink)
    return cleanup_error, original_unlink, attempted_names


def test_task8_normal_probe_review_rclone_primary_precedes_source_cleanup_secondary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_probe_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    _cleanup_error, original_unlink, attempted_names = (
        task8_install_probe_source_cleanup_failure(
            module,
            monkeypatch,
        )
    )
    purpose = "probe-create:probe.dump:strict-no-existing"
    harness["controls"]["command_error_after_purpose"] = purpose

    with pytest.raises(
        module.OperationStateError,
        match="remote probe strict create|controlled remote command failure",
    ):
        module.probe_remote_storage(context)

    interrupted = harness["holder"]["state"]
    assert interrupted["phase"] == "recovery_required"
    assert interrupted["failure"]["primary_error"] == (
        "remote probe strict create: probe.dump failed"
    )
    assert [item["stage"] for item in interrupted["secondary_errors"]] == [
        "probe_source_cleanup"
    ]
    assert "operation-source cleanup failure" in interrupted["secondary_errors"][0][
        "error"
    ]
    assert task8_probe_progress_snapshot(interrupted) == (
        (False, False, False),
        (False, False, False),
    )
    assert interrupted["rclone_evidence_groups"][-1]["purpose"] == purpose
    assert interrupted["rclone_evidence_groups"][-1]["outcome"] == "indeterminate"
    source_path = module._task8_probe_source_path(interrupted, "probe.dump")
    assert attempted_names == [source_path.name]
    assert source_path.exists()
    assert harness["remote_store"] == {
        "probe.dump": task8_probe_payloads()["probe.dump"]
    }

    monkeypatch.setattr(module.os, "unlink", original_unlink)
    final = module.recover_host_configuration(context)

    assert final["phase"] == "installed"
    assert not source_path.exists()
    assert harness["remote_store"] == {}
    assert final["failure"] == interrupted["failure"]
    assert final["secondary_errors"][:1] == interrupted["secondary_errors"]
    module.validate_operation_state(final, operation_dir)


def test_task8_normal_probe_review_baseexception_with_source_residue_fabricates_no_receipts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_probe_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    _cleanup_error, original_unlink, attempted_names = (
        task8_install_probe_source_cleanup_failure(
            module,
            monkeypatch,
        )
    )
    purpose = "probe-create:probe.dump:strict-no-existing"
    harness["controls"]["command_crash_after_purpose"] = purpose

    with pytest.raises(Task8ProbeProcessDeath, match="process death"):
        module.probe_remote_storage(context)

    crashed = harness["holder"]["state"]
    assert crashed["phase"] == "probing"
    assert crashed["failure"] is None
    assert crashed["secondary_errors"] == []
    assert crashed["recovery"] is None
    assert crashed["probe"] is None
    assert crashed["rclone_evidence_groups"][-1]["purpose"] == purpose
    assert crashed["rclone_evidence_groups"][-1]["after"] is None
    source_path = module._task8_probe_source_path(crashed, "probe.dump")
    assert attempted_names == [source_path.name]
    assert source_path.exists()

    monkeypatch.setattr(module.os, "unlink", original_unlink)
    final = module.recover_host_configuration(context)

    assert final["phase"] == "installed"
    assert not source_path.exists()
    assert harness["remote_store"] == {}
    module.validate_operation_state(final, operation_dir)


@pytest.mark.parametrize(
    "boundary",
    (
        "verify-command",
        "verify-flag",
        "delete-command",
        "delete-flag",
        "runtime-release",
        "legacy-release",
        "timer-enable",
        "timer-start",
        "timer-readback",
        "timer-restored-write",
        "probed-write",
    ),
)
def test_task8_normal_probe_review_process_death_boundary_matrix_is_resumable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_probe_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    if boundary == "verify-command":
        harness["controls"]["command_crash_after_purpose"] = (
            "probe-verify:probe.dump"
        )
    elif boundary == "verify-flag":
        harness["controls"]["write_crash_after_probe_progress"] = (
            "probe.dump",
            "verified",
        )
    elif boundary == "delete-command":
        harness["controls"]["command_crash_after_purpose"] = (
            "probe-cleanup:probe.dump"
        )
    elif boundary == "delete-flag":
        harness["controls"]["write_crash_after_probe_progress"] = (
            "probe.dump",
            "cleaned",
        )
    elif boundary == "runtime-release":
        harness["controls"]["crash_after"] = "runtime_lock_release"
    elif boundary == "legacy-release":
        harness["controls"]["crash_after"] = "legacy_lock_release"
    elif boundary == "timer-enable":
        harness["controls"]["crash_after"] = "timer_enable"
    elif boundary == "timer-start":
        harness["controls"]["crash_after"] = "timer_start"
    elif boundary == "timer-readback":
        harness["controls"]["crash_after"] = "restore_runtime_readback"
    elif boundary == "timer-restored-write":
        harness["controls"]["write_crash_after_guard_field"] = "timer_restored"
    else:
        harness["controls"]["write_crash_after_phase"] = "probed"

    with pytest.raises((Task8ProbeProcessDeath, Task8GuardProcessDeath)):
        module.probe_remote_storage(context)

    durable = copy.deepcopy(harness["holder"]["state"])
    assert durable["failure"] is None
    assert durable["secondary_errors"] == []
    assert durable["recovery"] is None
    if boundary == "probed-write":
        assert durable["phase"] == "probed"
        assert durable["active_transaction"] is None
        assert durable["probe"] is not None
        assert harness["remote_store"] == {}
        assert harness["live_runtime"] == harness["baseline"]
        module.validate_operation_state(durable, operation_dir)
        return

    assert durable["phase"] == "probing"
    assert durable["probe"] is None
    progress = task8_probe_progress_snapshot(durable)
    assert progress is not None
    dump_progress = progress[0]
    guard = durable["active_transaction"]["guard"]
    if boundary == "verify-command":
        assert dump_progress == (True, False, False)
        assert durable["rclone_evidence_groups"][-1]["after"] is None
    elif boundary == "verify-flag":
        assert dump_progress == (True, True, False)
        assert durable["rclone_evidence_groups"][-1]["outcome"] == "success"
    elif boundary == "delete-command":
        assert dump_progress == (True, True, False)
        assert durable["rclone_evidence_groups"][-1]["after"] is None
        assert "probe.dump" not in harness["remote_store"]
    elif boundary == "delete-flag":
        assert dump_progress == (True, True, True)
        assert durable["rclone_evidence_groups"][-1]["outcome"] == "success"
    elif boundary in {"runtime-release", "legacy-release"}:
        assert guard["locks_released"] is False
        assert guard["timer_restored"] is False
    elif boundary in {"timer-enable", "timer-start", "timer-readback"}:
        assert guard["locks_released"] is True
        assert guard["timer_restored"] is False
    else:
        assert guard["timer_restored"] is True
    module.validate_operation_state(durable, operation_dir)

    harness["controls"]["crash_after"] = None
    final = module.recover_host_configuration(context)

    assert final["phase"] == "installed"
    assert final["active_transaction"] is None
    assert final["probe"] is None
    assert final["recovery"]["kind"] == "probe"
    assert final["recovery"]["completed_epoch"] is not None
    assert harness["remote_store"] == {}
    assert harness["live_runtime"] == harness["baseline"]
    module.validate_operation_state(final, operation_dir)


def test_task8_normal_transaction_material_accepts_exact_installed_helper_route(
    task8_installed_helper_fixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, context, installed_helper = task8_installed_helper_fixture
    monkeypatch.setattr(module, "__file__", str(installed_helper))

    with module._open_verified_transaction_material(
        context,
        frozenset({"installed"}),
        helper_route="installed",
    ) as material:
        assert material.source.state["phase"] == "installed"
        module._revalidate_transaction_material(material)


@pytest.mark.parametrize("wrong_provenance", ["source-route", "installed-hash"])
def test_task8_normal_transaction_material_rejects_wrong_installed_provenance(
    task8_installed_helper_fixture,
    monkeypatch: pytest.MonkeyPatch,
    wrong_provenance: str,
) -> None:
    module, context, installed_helper = task8_installed_helper_fixture
    if wrong_provenance == "source-route":
        running_helper = (
            context.paths.source_dir / "deploy/linux/degen-prod-db-backup-ops.py"
        )
    else:
        installed_helper.write_bytes(installed_helper.read_bytes() + b"tampered\n")
        running_helper = installed_helper
    monkeypatch.setattr(module, "__file__", str(running_helper))

    with pytest.raises(
        module.OperationStateError,
        match="installed|helper|route|provenance|hash|changed",
    ):
        with module._open_verified_transaction_material(
            context,
            frozenset({"installed"}),
            helper_route="installed",
        ):
            pass


@pytest.mark.parametrize("second_failure", ("process-death", "ordinary-error"))
def test_task8_second_probe_after_completed_recovery_replaces_receipt_and_recovers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    second_failure: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    previously_recovered, _new_probe_fixture = completed_probe_recovery_then_new_probe(
        operation_dir
    )
    module.validate_operation_state(previously_recovered, operation_dir)

    def token_hex(byte_count: int) -> str:
        assert byte_count == 16
        return "0123456789abcdef0123456789abcdef"

    monkeypatch.setattr(module.secrets, "token_hex", token_hex)
    context, harness = task8_install_probe_recovery_harness(
        module,
        monkeypatch,
        previously_recovered,
        operation_dir,
    )
    create_purpose = "probe-create:probe.dump:strict-no-existing"
    first_recovery = copy.deepcopy(previously_recovered["recovery"])
    assert isinstance(first_recovery, dict)
    assert first_recovery["kind"] == "probe"
    assert first_recovery["completed_epoch"] is not None

    if second_failure == "process-death":
        harness["controls"]["command_crash_after_purpose"] = create_purpose
        with pytest.raises(Task8ProbeProcessDeath, match="process death"):
            module.probe_remote_storage(context)
    else:
        harness["controls"]["command_error_after_purpose"] = create_purpose
        with pytest.raises(module.OperationStateError, match="strict create"):
            module.probe_remote_storage(context)

    final = module.recover_host_configuration(context)

    assert final["phase"] == "installed"
    assert final["active_transaction"] is None
    assert final["probe"] is None
    replacement = final["recovery"]
    assert isinstance(replacement, dict)
    assert replacement["kind"] == "probe"
    assert replacement["started_epoch"] > first_recovery["completed_epoch"]
    assert replacement["completed_epoch"] is not None
    assert replacement != first_recovery
    assert final["failure"] is not None
    assert final["failure"]["phase"] == "probing"
    assert harness["remote_store"] == {}
    assert harness["live_runtime"] == harness["baseline"]
    module.validate_operation_state(final, operation_dir)


def test_task8_probe_post_yield_integrity_error_precedes_source_cleanup_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_probe_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    _cleanup_error, _original_unlink, attempted_names = (
        task8_install_probe_source_cleanup_failure(module, monkeypatch)
    )
    original_read = module._task8_read_probe_source
    read_calls = 0

    def fail_post_yield_integrity(
        descriptor: int,
        expected: bytes,
        *,
        effective_uid: int,
    ) -> object:
        nonlocal read_calls
        read_calls += 1
        if read_calls == 2:
            raise module.OperationStateError(
                "controlled post-yield source integrity failure"
            )
        return original_read(
            descriptor,
            expected,
            effective_uid=effective_uid,
        )

    monkeypatch.setattr(module, "_task8_read_probe_source", fail_post_yield_integrity)

    with pytest.raises(module.OperationStateError) as captured:
        module.probe_remote_storage(context)

    assert "post-yield source integrity failure" in str(captured.value)
    interrupted = harness["holder"]["state"]
    assert interrupted["phase"] == "recovery_required"
    assert interrupted["failure"]["primary_error"] == (
        "controlled post-yield source integrity failure"
    )
    assert [item["stage"] for item in interrupted["secondary_errors"]] == [
        "probe_source_cleanup"
    ]
    assert "operation-source cleanup failure" in interrupted["secondary_errors"][0][
        "error"
    ]
    assert read_calls == 2
    source_path = module._task8_probe_source_path(interrupted, "probe.dump")
    assert attempted_names == [source_path.name]
    assert source_path.exists()
    assert harness["remote_store"] == {
        "probe.dump": task8_probe_payloads()["probe.dump"]
    }
    module.validate_operation_state(interrupted, operation_dir)


def test_task8_dry_run_guard_recovery_replaces_completed_prior_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    prior_recovering = failed_transaction_state(
        operation_dir,
        "guard_dry_run",
        "recovering_guard",
    )
    mark_transaction_complete(prior_recovering)
    previously_recovered = copy.deepcopy(prior_recovering)
    previously_recovered["active_transaction"] = None
    previously_recovered["recovery"]["completed_epoch"] = 1_750_000_130
    append_phase(previously_recovered, "probed", 1_750_000_130)
    module.validate_operation_state(previously_recovered, operation_dir)

    interrupted = copy.deepcopy(previously_recovered)
    interrupted["active_transaction"] = active_transaction(
        "dry_run",
        "probed",
        1_750_000_140,
    )
    append_phase(interrupted, "dry_run_recording", 1_750_000_140)
    history = interrupted["phase_history"]
    assert isinstance(history, list)
    history[-1]["evidence_sha256"] = task8_dry_run_entry_evidence(
        interrupted,
        history[-1],
    )
    module.validate_operation_state(
        interrupted,
        operation_dir,
        previously_recovered,
    )
    prior_recovery = copy.deepcopy(previously_recovered["recovery"])
    assert interrupted["active_transaction"]["runtime_baseline"] == prior_recovery[
        "runtime_baseline"
    ]

    context, harness = task8_install_guard_recovery_harness(
        module,
        monkeypatch,
        interrupted,
        operation_dir,
    )

    final = module.recover_host_configuration(context)

    assert final["phase"] == "probed"
    assert final["active_transaction"] is None
    replacement = final["recovery"]
    assert isinstance(replacement, dict)
    assert replacement["kind"] == "guard"
    assert replacement["started_epoch"] > prior_recovery["completed_epoch"]
    assert replacement["completed_epoch"] is not None
    assert replacement != prior_recovery
    assert replacement["runtime_baseline"] == prior_recovery["runtime_baseline"]
    assert harness["live_runtime"] == harness["baseline"]
    assert harness["command_calls"] == []
    module.validate_operation_state(final, operation_dir)


@pytest.mark.parametrize("absence_proof", ("root", "parent"))
def test_task8_probe_indeterminate_final_delete_uses_namespace_absence_for_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    absence_proof: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    initial = task8_probe_recovery_state(
        operation_dir,
        "recovering_probe",
        progress={
            "probe.dump": (True, True, True),
            "probe.dump.sha256": (True, True, False),
        },
        milestone="runtime_lock_acquired",
    )
    task8_replace_probe_event_sequence(
        initial,
        (
            ("probe-precreate-absence", "success"),
            ("probe-create:probe.dump:strict-no-existing", "success"),
            (
                "probe-create:probe.dump.sha256:strict-no-existing",
                "success",
            ),
            ("probe-verify:probe.dump", "success"),
            ("probe-verify:probe.dump.sha256", "success"),
            ("probe-cleanup:probe.dump", "success"),
            ("probe-cleanup:probe.dump.sha256", "indeterminate"),
        ),
    )
    module.validate_operation_state(initial, operation_dir)
    context, harness = task8_install_probe_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
        remote_objects={},
    )
    if absence_proof == "root":
        harness["controls"]["probe_parent_exists"] = False
        harness["controls"]["probe_prefix_exists"] = False
    else:
        harness["controls"]["probe_parent_exists"] = True
        harness["controls"]["probe_prefix_exists"] = False

    final = module.recover_host_configuration(context)

    assert final["phase"] == "installed"
    assert final["active_transaction"] is None
    assert final["recovery"]["completed_epoch"] is not None
    expected_purposes = ["probe-recovery-root"]
    if absence_proof == "root":
        expected_purposes.append("probe-recovery-root-absence")
    else:
        expected_purposes.extend(
            ["probe-recovery-parent", "probe-recovery-parent-absence"]
        )
    assert harness["command_purposes"] == expected_purposes
    assert not any("deletefile" in argv for argv in harness["command_calls"])
    assert harness["remote_store"] == {}
    assert harness["live_runtime"] == harness["baseline"]
    cleaned_writes = []
    for durable in harness["writes"]:
        transaction = durable.get("active_transaction")
        if not isinstance(transaction, dict) or transaction.get("kind") != "probe":
            continue
        probe = transaction.get("probe")
        if not isinstance(probe, dict):
            continue
        objects = probe.get("objects")
        if not isinstance(objects, list):
            continue
        sidecar = next(
            item for item in objects if item["name"] == "probe.dump.sha256"
        )
        if sidecar["cleaned"] is True:
            cleaned_writes.append(durable)
    assert cleaned_writes
    assert cleaned_writes[0]["phase"] == "recovering_probe"
    module.validate_operation_state(final, operation_dir)


# Task 8 dry-run parsing and durable-receipt contract. These tests deliberately
# exercise pure decoders and context-free state validation before the workflow
# harness is extended for normal dry-run execution.
TASK8_DRY_RUN_PREFIX = "degen_green_"
TASK8_DRY_RUN_KEEP_DUMP = "degen_green_20260630T010203Z.dump"
TASK8_DRY_RUN_DELETE_DUMP = "degen_green_20260501T040506Z.dump"
TASK8_DRY_RUN_PROTECTED = "manual-preserve.txt"
TASK8_DRY_RUN_INVALID_STAMP = "20261301T040506Z"
TASK8_DRY_RUN_INVALID_DUMP = (
    f"{TASK8_DRY_RUN_PREFIX}{TASK8_DRY_RUN_INVALID_STAMP}.dump"
)
TASK8_DRY_RUN_EXPECTED_NOW = datetime(
    2026,
    7,
    1,
    tzinfo=timezone.utc,
)
TASK8_RCLONE_CONFIG_PATH = "/etc/degen/rclone.conf"
TASK8_RCLONE_BEFORE_METADATA = (
    f"sha256={HASH_A} device=2049 inode=12345 uid=0 gid=0 "
    "mode=0600 links=1 size=321 mtime_ns=1750000000000000000"
)
TASK8_RCLONE_REFRESHED_METADATA = (
    f"sha256={HASH_B} device=2049 inode=12345 uid=0 gid=0 "
    "mode=0600 links=1 size=400 mtime_ns=1750000001000000000"
)


def task8_dry_run_inventory_names() -> list[str]:
    return sorted(
        [
            TASK8_DRY_RUN_KEEP_DUMP,
            f"{TASK8_DRY_RUN_KEEP_DUMP}.sha256",
            TASK8_DRY_RUN_DELETE_DUMP,
            f"{TASK8_DRY_RUN_DELETE_DUMP}.sha256",
            TASK8_DRY_RUN_PROTECTED,
        ]
    )


def task8_dry_run_plan_payload() -> dict[str, object]:
    return {
        "mode": "remote",
        "prefix": TASK8_DRY_RUN_PREFIX,
        "keep": [
            {
                "dump": TASK8_DRY_RUN_KEEP_DUMP,
                "checksum": f"{TASK8_DRY_RUN_KEEP_DUMP}.sha256",
                "timestamp": "20260630T010203Z",
                "reasons": ["daily", "monthly", "newest", "weekly"],
            }
        ],
        "delete": [
            {
                "dump": TASK8_DRY_RUN_DELETE_DUMP,
                "checksum": f"{TASK8_DRY_RUN_DELETE_DUMP}.sha256",
                "timestamp": "20260501T040506Z",
                "reasons": ["expired"],
            }
        ],
        "protected": [
            {"name": TASK8_DRY_RUN_PROTECTED, "reason": "unknown-name"}
        ],
    }


def task8_unparseable_timestamp_plan_payload(
    *,
    reason: str = "unparseable-timestamp",
) -> dict[str, object]:
    return {
        "mode": "remote",
        "prefix": TASK8_DRY_RUN_PREFIX,
        "keep": [],
        "delete": [],
        "protected": [
            {"name": TASK8_DRY_RUN_INVALID_DUMP, "reason": reason},
            {
                "name": f"{TASK8_DRY_RUN_INVALID_DUMP}.sha256",
                "reason": reason,
            },
        ],
    }


def task8_canonical_json_line(value: object) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ) + "\n"


def task8_expected_dry_run_candidate_sha256(delete_names: list[str]) -> str:
    return task8_evidence_sha256(
        "dry-run-candidates",
        {"delete_names": delete_names},
    )


def task8_expected_dry_run_result_sha256(
    purpose: str,
    receipt: dict[str, object],
) -> str:
    if purpose in {"dry-run-inventory-before", "dry-run-inventory-after"}:
        result_domain = {
            "inventory_names": receipt["inventory_names"],
            "casefold_names": receipt["casefold_names"],
        }
    elif purpose == "dry-run-runtime":
        result_domain = {
            "delete_names": receipt["delete_names"],
            "candidate_sha256": receipt["candidate_sha256"],
        }
    else:
        raise AssertionError(purpose)
    return task8_evidence_sha256(
        "dry-run-result",
        {
            "purpose": purpose,
            "result_domain": result_domain,
        },
    )


def task8_expected_dry_run_group_evidence(
    group: dict[str, object],
) -> str:
    return task7_evidence_sha256(
        "rclone-audit",
        {
            "group_id": group["group_id"],
            "purpose": group["purpose"],
            "before": group["before"],
            "after": group["after"],
            "outcome": group["outcome"],
            "result_sha256": group["result_sha256"],
        },
    )


TASK8_DRY_RUN_PURPOSES = (
    "dry-run-inventory-before",
    "dry-run-runtime",
    "dry-run-inventory-after",
)
TASK8_DRY_RUN_RECEIPT_KEEP_DUMPS = (
    "degen_green_20250615T010203Z.dump",
    "degen_green_20250614T010203Z.dump",
    "degen_green_20250613T010203Z.dump",
    "degen_green_20250612T010203Z.dump",
    "degen_green_20250611T010203Z.dump",
    "degen_green_20250610T010203Z.dump",
    "degen_green_20250609T010203Z.dump",
    "degen_green_20250602T010203Z.dump",
    "degen_green_20250526T010203Z.dump",
    "degen_green_20250519T010203Z.dump",
    "degen_green_20250415T010203Z.dump",
)
TASK8_DRY_RUN_RECEIPT_DELETE_DUMP = "degen_green_20250301T010203Z.dump"


def task8_flatten_backup_pairs(dumps: tuple[str, ...] | list[str]) -> list[str]:
    return [name for dump in dumps for name in (dump, f"{dump}.sha256")]


def task8_valid_dry_run_receipt() -> dict[str, object]:
    keep_names = task8_flatten_backup_pairs(TASK8_DRY_RUN_RECEIPT_KEEP_DUMPS)
    delete_names = task8_flatten_backup_pairs(
        [TASK8_DRY_RUN_RECEIPT_DELETE_DUMP]
    )
    inventory_names = sorted(
        [*keep_names, *delete_names, TASK8_DRY_RUN_PROTECTED]
    )
    return {
        "inventory_names": inventory_names,
        "casefold_names": [name.casefold() for name in inventory_names],
        "keep_names": keep_names,
        "protected_names": [TASK8_DRY_RUN_PROTECTED],
        "delete_names": delete_names,
        "candidate_sha256": task8_expected_dry_run_candidate_sha256(
            delete_names
        ),
        "evidence_sha256": HASH_A,
    }


def task8_dry_run_entry_evidence(
    state: dict[str, object],
    entry: dict[str, object],
) -> str:
    effective_config = state["effective_config"]
    install = state["install"]
    assert isinstance(effective_config, dict) and isinstance(install, dict)
    installed_hashes = install["installed_hashes"]
    assert isinstance(installed_hashes, dict)
    return task8_evidence_sha256(
        "dry-run-entry",
        {
            "operation_id": state["operation_id"],
            "operation_dir": state["operation_dir"],
            "epoch": entry["epoch"],
            "prior_stable_phase": "probed",
            "effective_config": effective_config,
            "installed_hashes": installed_hashes,
        },
    )


def task8_successful_dry_run_groups(
    started_epoch: int = 1_750_000_070,
    attempt_ordinal: int = 0,
) -> list[dict[str, object]]:
    receipt = task8_valid_dry_run_receipt()
    groups = [
        task8_rclone_group(
            "dry_run",
            started_epoch,
            attempt_ordinal,
            ordinal,
            purpose=purpose,
        )
        for ordinal, purpose in enumerate(TASK8_DRY_RUN_PURPOSES)
    ]
    for group, purpose in zip(groups, TASK8_DRY_RUN_PURPOSES, strict=True):
        group["result_sha256"] = task8_expected_dry_run_result_sha256(
            purpose,
            receipt,
        )
        group["evidence_sha256"] = task8_expected_dry_run_group_evidence(group)
    return groups


def task8_dry_run_completion_pair(
    state: dict[str, object],
) -> tuple[dict[str, object], dict[str, object]]:
    history = state["phase_history"]
    assert isinstance(history, list)
    indices = [
        index
        for index, item in enumerate(history)
        if (
            index > 0
            and item["phase"] == "dry_run_recorded"
            and history[index - 1]["phase"] == "dry_run_recording"
        )
    ]
    assert indices
    completed_index = indices[-1]
    entry = history[completed_index - 1]
    completed = history[completed_index]
    assert isinstance(entry, dict) and isinstance(completed, dict)
    return entry, completed


def task8_dry_run_attempt_groups(
    state: dict[str, object],
    entry: dict[str, object],
) -> list[dict[str, object]]:
    history = state["phase_history"]
    groups = state["rclone_evidence_groups"]
    assert isinstance(history, list) and isinstance(groups, list)
    attempts = [item for item in history if item["phase"] == "dry_run_recording"]
    attempt_ordinal = next(
        index for index, candidate in enumerate(attempts) if candidate is entry
    )
    group_prefix = (
        f"task8:dry_run:{entry['epoch']}:{attempt_ordinal}:"
    )
    matching = [
        group
        for group in groups
        if str(group["group_id"]).startswith(group_prefix)
    ]
    return sorted(
        matching,
        key=lambda group: int(str(group["group_id"]).rsplit(":", 1)[1]),
    )


def task8_dry_run_completion_evidence(
    state: dict[str, object],
    dry_run: dict[str, object],
) -> str:
    entry, completed = task8_dry_run_completion_pair(state)
    groups = task8_dry_run_attempt_groups(state, entry)
    return task8_evidence_sha256(
        "dry-run-complete",
        {
            "operation_id": state["operation_id"],
            "operation_dir": state["operation_dir"],
            "entry": {
                "epoch": entry["epoch"],
                "evidence_sha256": entry["evidence_sha256"],
            },
            "completed_epoch": completed["epoch"],
            "inventory_names": dry_run["inventory_names"],
            "casefold_names": dry_run["casefold_names"],
            "keep_names": dry_run["keep_names"],
            "protected_names": dry_run["protected_names"],
            "delete_names": dry_run["delete_names"],
            "candidate_sha256": dry_run["candidate_sha256"],
            "rclone_groups": groups,
        },
    )


def task8_reseal_dry_run_state(state: dict[str, object]) -> None:
    dry_run = state["dry_run"]
    assert isinstance(dry_run, dict)
    _entry, completed = task8_dry_run_completion_pair(state)
    evidence = task8_dry_run_completion_evidence(state, dry_run)
    dry_run["evidence_sha256"] = evidence
    completed["evidence_sha256"] = evidence


def task8_valid_dry_run_state(operation_dir: Path) -> dict[str, object]:
    state = state_at_phase(operation_dir, "dry_run_recorded")
    entry, _completed = task8_dry_run_completion_pair(state)
    entry["evidence_sha256"] = task8_dry_run_entry_evidence(state, entry)
    probe_groups = [
        group
        for group in state["rclone_evidence_groups"]
        if not str(group["group_id"]).startswith("task8:dry_run:")
    ]
    state["rclone_evidence_groups"] = [
        *probe_groups,
        *task8_successful_dry_run_groups(),
    ]
    state["dry_run"] = task8_valid_dry_run_receipt()
    task8_reseal_dry_run_state(state)
    return state


def task8_remote_dry_run_log(
    delete_names: list[str],
    *,
    prefix: str = TASK8_DRY_RUN_PREFIX,
    final_metadata: str = TASK8_RCLONE_BEFORE_METADATA,
    final_change: str = "unchanged",
) -> str:
    lines = [
        "RCLONE_CONFIG_RECEIPT phase=before status=ok "
        f"path={TASK8_RCLONE_CONFIG_PATH} {TASK8_RCLONE_BEFORE_METADATA} "
        "change=baseline",
        f"Preflight passed for mode=remote-retention-dry-run prefix={prefix}"
    ]
    if delete_names:
        lines.extend(f"Remote retention candidate: {name}" for name in delete_names)
        lines.extend(
            f"Remote retention dry run: would delete {name}"
            for name in delete_names
        )
    else:
        lines.append("Remote retention candidates: none")
    lines.append("Remote retention dry run completed; no dump or deletion was performed")
    lines.append(
        "RCLONE_CONFIG_RECEIPT phase=final status=ok "
        f"path={TASK8_RCLONE_CONFIG_PATH} {final_metadata} change={final_change}"
    )
    return "".join(
        f"[2026-07-01T12:34:{index:02d}Z] {line}\n"
        for index, line in enumerate(lines)
    )


def test_task8_remote_lsf_inventory_decoder_returns_sorted_parallel_names() -> None:
    module = load_ops_helper()
    expected = task8_dry_run_inventory_names()
    raw = "\n".join(reversed(expected)) + "\n"

    inventory_names, casefold_names = module._task8_decode_remote_lsf_inventory(raw)

    assert inventory_names == expected
    assert casefold_names == [name.casefold() for name in expected]
    assert module._task8_decode_remote_lsf_inventory("") == ([], [])


@pytest.mark.parametrize(
    "raw",
    [
        "a.dump",
        "a.dump\r\n",
        "a.dump\x00\n",
        "a.dump\n\n",
        "nested/a.dump\n",
        "nested\\a.dump\n",
        "a.dump\na.dump\n",
        "A.dump\na.dump\n",
        "".join(f"item-{index}.dump\n" for index in range(4097)),
    ],
    ids=(
        "missing-terminal-lf",
        "crlf",
        "nul",
        "empty-record",
        "forward-nesting",
        "backslash-nesting",
        "duplicate",
        "casefold-collision",
        "over-entry-limit",
    ),
)
def test_task8_remote_lsf_inventory_decoder_rejects_ambiguous_input(raw: str) -> None:
    module = load_ops_helper()
    with pytest.raises(module.OperationStateError):
        module._task8_decode_remote_lsf_inventory(raw)


@pytest.mark.parametrize("decoder", ("inventory", "plan", "log"))
def test_task8_dry_run_decoders_wrap_lone_surrogate_input(
    decoder: str,
) -> None:
    module = load_ops_helper()
    raw = "\ud800\n"

    with pytest.raises(module.OperationStateError):
        if decoder == "inventory":
            module._task8_decode_remote_lsf_inventory(raw)
        elif decoder == "plan":
            module._task8_decode_remote_retention_plan(
                raw,
                expected_prefix=TASK8_DRY_RUN_PREFIX,
                inventory_names=[],
                expected_now=TASK8_DRY_RUN_EXPECTED_NOW,
            )
        else:
            module._task8_decode_remote_dry_run_log(
                raw,
                expected_prefix=TASK8_DRY_RUN_PREFIX,
                expected_delete_names=[],
            )


def test_task8_remote_retention_plan_decoder_flattens_exact_partition() -> None:
    module = load_ops_helper()
    flattened = module._task8_decode_remote_retention_plan(
        task8_canonical_json_line(task8_dry_run_plan_payload()),
        expected_prefix=TASK8_DRY_RUN_PREFIX,
        inventory_names=task8_dry_run_inventory_names(),
        expected_now=TASK8_DRY_RUN_EXPECTED_NOW,
    )
    assert flattened == {
        "keep_names": [
            TASK8_DRY_RUN_KEEP_DUMP,
            f"{TASK8_DRY_RUN_KEEP_DUMP}.sha256",
        ],
        "protected_names": [TASK8_DRY_RUN_PROTECTED],
        "delete_names": [
            TASK8_DRY_RUN_DELETE_DUMP,
            f"{TASK8_DRY_RUN_DELETE_DUMP}.sha256",
        ],
    }


@pytest.mark.parametrize(
    "mutation",
    (
        "duplicate-json-key",
        "nonfinite",
        "wrong-mode",
        "wrong-prefix",
        "extra-schema-key",
        "broken-pair",
        "incomplete-partition",
    ),
)
def test_task8_remote_retention_plan_decoder_rejects_untrusted_output(
    mutation: str,
) -> None:
    module = load_ops_helper()
    payload = task8_dry_run_plan_payload()
    if mutation == "duplicate-json-key":
        raw = task8_canonical_json_line(payload).replace(
            '"mode":"remote"', '"mode":"remote","mode":"remote"', 1
        )
    elif mutation == "nonfinite":
        raw = task8_canonical_json_line(payload).replace(
            '"mode":"remote"', '"mode":NaN', 1
        )
    else:
        if mutation == "wrong-mode":
            payload["mode"] = "local"
        elif mutation == "wrong-prefix":
            payload["prefix"] = "other_"
        elif mutation == "extra-schema-key":
            payload["unexpected"] = []
        elif mutation == "broken-pair":
            payload["delete"][0]["checksum"] = "wrong.dump.sha256"
        elif mutation == "incomplete-partition":
            payload["protected"] = []
        raw = task8_canonical_json_line(payload)

    with pytest.raises(module.OperationStateError):
        module._task8_decode_remote_retention_plan(
            raw,
            expected_prefix=TASK8_DRY_RUN_PREFIX,
            inventory_names=task8_dry_run_inventory_names(),
            expected_now=TASK8_DRY_RUN_EXPECTED_NOW,
        )


def test_task8_remote_retention_plan_accepts_planner_unparseable_timestamp_protection() -> None:
    module = load_ops_helper()
    inventory_names = sorted(
        [TASK8_DRY_RUN_INVALID_DUMP, f"{TASK8_DRY_RUN_INVALID_DUMP}.sha256"]
    )

    flattened = module._task8_decode_remote_retention_plan(
        task8_canonical_json_line(task8_unparseable_timestamp_plan_payload()),
        expected_prefix=TASK8_DRY_RUN_PREFIX,
        inventory_names=inventory_names,
        expected_now=TASK8_DRY_RUN_EXPECTED_NOW,
    )

    assert flattened == {
        "keep_names": [],
        "protected_names": [
            TASK8_DRY_RUN_INVALID_DUMP,
            f"{TASK8_DRY_RUN_INVALID_DUMP}.sha256",
        ],
        "delete_names": [],
    }


@pytest.mark.parametrize("partition", ("keep", "delete"))
def test_task8_remote_retention_plan_rejects_invalid_calendar_pair_as_actionable(
    partition: str,
) -> None:
    module = load_ops_helper()
    record = {
        "dump": TASK8_DRY_RUN_INVALID_DUMP,
        "checksum": f"{TASK8_DRY_RUN_INVALID_DUMP}.sha256",
        "timestamp": TASK8_DRY_RUN_INVALID_STAMP,
        "reasons": ["newest"] if partition == "keep" else ["expired"],
    }
    payload = {
        "mode": "remote",
        "prefix": TASK8_DRY_RUN_PREFIX,
        "keep": [record] if partition == "keep" else [],
        "delete": [record] if partition == "delete" else [],
        "protected": [],
    }

    with pytest.raises(module.OperationStateError):
        module._task8_decode_remote_retention_plan(
            task8_canonical_json_line(payload),
            expected_prefix=TASK8_DRY_RUN_PREFIX,
            inventory_names=sorted(
                [
                    TASK8_DRY_RUN_INVALID_DUMP,
                    f"{TASK8_DRY_RUN_INVALID_DUMP}.sha256",
                ]
            ),
            expected_now=TASK8_DRY_RUN_EXPECTED_NOW,
        )


def test_task8_remote_retention_plan_rejects_unparseable_name_reason_mismatch() -> None:
    module = load_ops_helper()
    payload = task8_unparseable_timestamp_plan_payload(reason="unknown-name")

    with pytest.raises(module.OperationStateError):
        module._task8_decode_remote_retention_plan(
            task8_canonical_json_line(payload),
            expected_prefix=TASK8_DRY_RUN_PREFIX,
            inventory_names=sorted(
                [
                    TASK8_DRY_RUN_INVALID_DUMP,
                    f"{TASK8_DRY_RUN_INVALID_DUMP}.sha256",
                ]
            ),
            expected_now=TASK8_DRY_RUN_EXPECTED_NOW,
        )


def test_task8_remote_retention_plan_rejects_false_protected_pair_reasons() -> None:
    module = load_ops_helper()
    old_dump = "degen_green_20200101T000000Z.dump"
    payload = {
        "mode": "remote",
        "prefix": TASK8_DRY_RUN_PREFIX,
        "keep": [],
        "delete": [],
        "protected": [
            {"name": old_dump, "reason": "future-timestamp"},
            {"name": f"{old_dump}.sha256", "reason": "incomplete-pair"},
        ],
    }

    with pytest.raises(module.OperationStateError):
        module._task8_decode_remote_retention_plan(
            task8_canonical_json_line(payload),
            expected_prefix=TASK8_DRY_RUN_PREFIX,
            inventory_names=[old_dump, f"{old_dump}.sha256"],
            expected_now=TASK8_DRY_RUN_EXPECTED_NOW,
        )


@pytest.mark.parametrize(
    "delete_names",
    (
        [],
        [
            TASK8_DRY_RUN_DELETE_DUMP,
            f"{TASK8_DRY_RUN_DELETE_DUMP}.sha256",
        ],
    ),
    ids=("no-candidates", "complete-pair"),
)
def test_task8_remote_dry_run_log_decoder_binds_exact_candidates(
    delete_names: list[str],
) -> None:
    module = load_ops_helper()
    assert module._task8_decode_remote_dry_run_log(
        task8_remote_dry_run_log(delete_names),
        expected_prefix=TASK8_DRY_RUN_PREFIX,
        expected_delete_names=delete_names,
    ) == delete_names


def test_task8_remote_dry_run_log_accepts_consistent_oauth_refresh_receipt() -> None:
    module = load_ops_helper()
    delete_names = [
        TASK8_DRY_RUN_DELETE_DUMP,
        f"{TASK8_DRY_RUN_DELETE_DUMP}.sha256",
    ]

    assert module._task8_decode_remote_dry_run_log(
        task8_remote_dry_run_log(
            delete_names,
            final_metadata=TASK8_RCLONE_REFRESHED_METADATA,
            final_change="possible-oauth-refresh",
        ),
        expected_prefix=TASK8_DRY_RUN_PREFIX,
        expected_delete_names=delete_names,
    ) == delete_names


@pytest.mark.parametrize(
    "mutation",
    (
        "missing-before",
        "missing-final",
        "malformed-before",
        "reordered-before",
        "nonroot-uid",
        "same-marked-refresh",
        "changed-marked-unchanged",
    ),
)
def test_task8_remote_dry_run_log_rejects_invalid_rclone_receipt_envelope(
    mutation: str,
) -> None:
    module = load_ops_helper()
    delete_names = [
        TASK8_DRY_RUN_DELETE_DUMP,
        f"{TASK8_DRY_RUN_DELETE_DUMP}.sha256",
    ]
    if mutation == "changed-marked-unchanged":
        raw = task8_remote_dry_run_log(
            delete_names,
            final_metadata=TASK8_RCLONE_REFRESHED_METADATA,
            final_change="unchanged",
        )
    else:
        raw = task8_remote_dry_run_log(
            delete_names,
            final_change=(
                "possible-oauth-refresh"
                if mutation == "same-marked-refresh"
                else "unchanged"
            ),
        )
    lines = raw.splitlines()
    if mutation == "missing-before":
        lines = [line for line in lines if "phase=before" not in line]
    elif mutation == "missing-final":
        lines = [line for line in lines if "phase=final" not in line]
    elif mutation == "malformed-before":
        lines[0] = lines[0].replace(f"sha256={HASH_A}", "sha256=INVALID", 1)
    elif mutation == "reordered-before":
        lines[0], lines[1] = lines[1], lines[0]
    elif mutation == "nonroot-uid":
        lines = [line.replace(" uid=0 ", " uid=999 ") for line in lines]
    raw = "\n".join(lines) + "\n"

    with pytest.raises(module.OperationStateError):
        module._task8_decode_remote_dry_run_log(
            raw,
            expected_prefix=TASK8_DRY_RUN_PREFIX,
            expected_delete_names=delete_names,
        )


@pytest.mark.parametrize(
    "mutation",
    (
        "wrong-prefix",
        "candidate-would-delete-mismatch",
        "unexpected-none",
        "missing-completion",
        "completion-not-final",
    ),
)
def test_task8_remote_dry_run_log_decoder_rejects_incomplete_receipts(
    mutation: str,
) -> None:
    module = load_ops_helper()
    delete_names = [
        TASK8_DRY_RUN_DELETE_DUMP,
        f"{TASK8_DRY_RUN_DELETE_DUMP}.sha256",
    ]
    raw = task8_remote_dry_run_log(delete_names)
    if mutation == "wrong-prefix":
        raw = raw.replace(f"prefix={TASK8_DRY_RUN_PREFIX}", "prefix=other_", 1)
    elif mutation == "candidate-would-delete-mismatch":
        raw = raw.replace(
            f"would delete {delete_names[1]}",
            f"would delete {TASK8_DRY_RUN_PROTECTED}",
            1,
        )
    elif mutation == "unexpected-none":
        raw = task8_remote_dry_run_log([])
    elif mutation == "missing-completion":
        raw = raw.rsplit("\n", 2)[0] + "\n"
    elif mutation == "completion-not-final":
        raw += "Remote retention candidates: none\n"

    with pytest.raises(module.OperationStateError):
        module._task8_decode_remote_dry_run_log(
            raw,
            expected_prefix=TASK8_DRY_RUN_PREFIX,
            expected_delete_names=delete_names,
        )


def test_task8_remote_dry_run_candidate_sha256_is_domain_separated_and_ordered() -> None:
    module = load_ops_helper()
    delete_names = [
        TASK8_DRY_RUN_DELETE_DUMP,
        f"{TASK8_DRY_RUN_DELETE_DUMP}.sha256",
    ]
    digest = module._task8_remote_dry_run_candidate_sha256(delete_names)
    assert digest == task8_expected_dry_run_candidate_sha256(delete_names)
    assert digest != module._task8_remote_dry_run_candidate_sha256(
        list(reversed(delete_names))
    )


@pytest.mark.parametrize("completed_count", (1, 2, 3))
def test_task8_dry_run_recording_accepts_exact_successful_purpose_prefix(
    tmp_path: Path,
    completed_count: int,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "dry_run_recording")
    state["rclone_evidence_groups"].extend(
        task8_successful_dry_run_groups()[:completed_count]
    )

    module.validate_operation_state(state, operation_dir)


@pytest.mark.parametrize("pending_ordinal", (0, 1, 2))
def test_task8_dry_run_recording_accepts_null_result_for_pending_prefix_group(
    tmp_path: Path,
    pending_ordinal: int,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "dry_run_recording")
    groups = task8_successful_dry_run_groups()[:pending_ordinal]
    groups.append(
        task8_pending_rclone_group(
            "dry_run",
            1_750_000_070,
            pending_ordinal,
            purpose=TASK8_DRY_RUN_PURPOSES[pending_ordinal],
        )
    )
    assert groups[-1]["result_sha256"] is None
    state["rclone_evidence_groups"].extend(groups)

    module.validate_operation_state(state, operation_dir)


def test_task8_dry_run_recording_accepts_null_result_for_indeterminate_group(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "dry_run_recording")
    group = task8_rclone_group(
        "dry_run",
        1_750_000_070,
        0,
        0,
        purpose=TASK8_DRY_RUN_PURPOSES[0],
        outcome="indeterminate",
    )
    assert group["result_sha256"] is None
    state["rclone_evidence_groups"].append(group)

    module.validate_operation_state(state, operation_dir)


def test_task8_dry_run_pending_to_success_transition_records_result_atomically(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    previous = state_at_phase(operation_dir, "dry_run_recording")
    pending = task8_pending_rclone_group(
        "dry_run",
        1_750_000_070,
        0,
        purpose=TASK8_DRY_RUN_PURPOSES[0],
    )
    previous["rclone_evidence_groups"].append(pending)
    current = copy.deepcopy(previous)
    completed = task8_successful_dry_run_groups()[0]
    current["rclone_evidence_groups"][-1] = completed

    module.validate_operation_state(previous, operation_dir)
    module.validate_operation_state(current, operation_dir, previous)


@pytest.mark.parametrize(
    "mutation",
    ("missing", "extra", "null", "uppercase", "malformed"),
)
def test_task8_dry_run_recording_rejects_missing_or_bad_success_result(
    tmp_path: Path,
    mutation: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "dry_run_recording")
    group = task8_successful_dry_run_groups()[0]
    if mutation == "missing":
        del group["result_sha256"]
    elif mutation == "extra":
        group["unexpected"] = "value"
    elif mutation == "null":
        group["result_sha256"] = None
    elif mutation == "uppercase":
        group["result_sha256"] = "A" * 64
    elif mutation == "malformed":
        group["result_sha256"] = "not-a-sha256"
    else:
        raise AssertionError(mutation)
    state["rclone_evidence_groups"].append(group)

    with pytest.raises(module.OperationStateError):
        module.validate_operation_state(state, operation_dir)


@pytest.mark.parametrize("outcome", (None, "indeterminate"))
def test_task8_dry_run_recording_rejects_result_for_non_success_group(
    tmp_path: Path,
    outcome: str | None,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "dry_run_recording")
    if outcome is None:
        group = task8_pending_rclone_group(
            "dry_run",
            1_750_000_070,
            0,
            purpose=TASK8_DRY_RUN_PURPOSES[0],
        )
        group["result_sha256"] = HASH_D
    else:
        group = task8_rclone_group(
            "dry_run",
            1_750_000_070,
            0,
            0,
            purpose=TASK8_DRY_RUN_PURPOSES[0],
            outcome=outcome,
        )
        group["result_sha256"] = HASH_D
        task8_reseal_rclone_group(group)
    state["rclone_evidence_groups"].append(group)

    with pytest.raises(module.OperationStateError):
        module.validate_operation_state(state, operation_dir)


def test_task8_dry_run_recording_rejects_success_with_arbitrary_audit_evidence(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "dry_run_recording")
    group = task8_successful_dry_run_groups()[0]
    group["evidence_sha256"] = HASH_D
    state["rclone_evidence_groups"].append(group)

    with pytest.raises(module.OperationStateError):
        module.validate_operation_state(state, operation_dir)


def test_task8_dry_run_recording_rejects_unknown_purpose_even_when_resealed(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "dry_run_recording")
    group = task8_rclone_group(
        "dry_run",
        1_750_000_070,
        0,
        0,
        purpose="not-an-allowed-dry-run-purpose",
    )
    state["rclone_evidence_groups"].append(group)

    with pytest.raises(module.OperationStateError):
        module.validate_operation_state(state, operation_dir)


def test_task8_dry_run_recording_rejects_known_purpose_at_wrong_ordinal(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "dry_run_recording")
    group = task8_rclone_group(
        "dry_run",
        1_750_000_070,
        0,
        0,
        purpose=TASK8_DRY_RUN_PURPOSES[1],
    )
    state["rclone_evidence_groups"].append(group)

    with pytest.raises(module.OperationStateError):
        module.validate_operation_state(state, operation_dir)


def test_task8_dry_run_recording_rejects_group_ordinal_beyond_exact_purpose_prefix(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "dry_run_recording")
    groups = task8_successful_dry_run_groups()
    groups.append(
        task8_rclone_group(
            "dry_run",
            1_750_000_070,
            0,
            3,
            purpose=TASK8_DRY_RUN_PURPOSES[0],
        )
    )
    state["rclone_evidence_groups"].extend(groups)

    with pytest.raises(module.OperationStateError):
        module.validate_operation_state(state, operation_dir)


def test_task8_context_free_validation_accepts_bound_dry_run_receipt(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    module.validate_operation_state(
        task8_valid_dry_run_state(operation_dir), operation_dir
    )


def test_task8_terminal_dry_run_receipt_rejects_resealed_wrong_result_commitment(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = task8_valid_dry_run_state(operation_dir)
    entry, _completed = task8_dry_run_completion_pair(state)
    groups = task8_dry_run_attempt_groups(state, entry)
    runtime_group = groups[1]
    assert runtime_group["purpose"] == "dry-run-runtime"
    runtime_group["result_sha256"] = HASH_D
    task8_reseal_rclone_group(runtime_group)
    task8_reseal_dry_run_state(state)

    with pytest.raises(module.OperationStateError):
        module.validate_operation_state(state, operation_dir)


@pytest.mark.parametrize(
    "mutation",
    (
        "casefold",
        "overlapping-partition",
        "split-pair",
        "candidate-digest",
        "completion-evidence",
    ),
)
def test_task8_context_free_validation_rejects_resealed_dry_run_mutations(
    tmp_path: Path,
    mutation: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = task8_valid_dry_run_state(operation_dir)
    dry_run = state["dry_run"]
    history = state["phase_history"]
    assert isinstance(dry_run, dict) and isinstance(history, list)
    if mutation == "casefold":
        dry_run["casefold_names"][0] = "not-the-inventory-casefold"
    elif mutation == "overlapping-partition":
        dry_run["delete_names"].append(TASK8_DRY_RUN_KEEP_DUMP)
        dry_run["candidate_sha256"] = task8_expected_dry_run_candidate_sha256(
            dry_run["delete_names"]
        )
    elif mutation == "split-pair":
        sidecar = dry_run["delete_names"].pop()
        dry_run["protected_names"].append(sidecar)
        dry_run["candidate_sha256"] = task8_expected_dry_run_candidate_sha256(
            dry_run["delete_names"]
        )
    elif mutation == "candidate-digest":
        dry_run["candidate_sha256"] = HASH_D
    elif mutation == "completion-evidence":
        dry_run["evidence_sha256"] = HASH_D
        history[-1]["evidence_sha256"] = HASH_D
    if mutation != "completion-evidence":
        task8_reseal_dry_run_state(state)

    with pytest.raises(module.OperationStateError):
        module.validate_operation_state(state, operation_dir)


def task8_ready_dry_run_previous(
    operation_dir: Path,
    completed: dict[str, object],
) -> dict[str, object]:
    previous = state_at_phase(operation_dir, "dry_run_recording")
    completed_entry, _completion = task8_dry_run_completion_pair(completed)
    previous_history = previous["phase_history"]
    assert isinstance(previous_history, list)
    previous_history[-1] = copy.deepcopy(completed_entry)
    previous["effective_config"] = copy.deepcopy(completed["effective_config"])
    previous["rclone_evidence_groups"] = copy.deepcopy(
        completed["rclone_evidence_groups"]
    )
    mark_transaction_complete(previous)
    return previous


def task8_apply_resealed_dry_run_mutation(
    state: dict[str, object],
    mutation: str,
) -> None:
    dry_run = state["dry_run"]
    groups = state["rclone_evidence_groups"]
    assert isinstance(dry_run, dict) and isinstance(groups, list)
    entry, _completed = task8_dry_run_completion_pair(state)
    dry_run_groups = [
        group
        for group in groups
        if str(group["group_id"]).startswith("task8:dry_run:")
    ]
    if mutation == "keep-delete-complete-pair-swap":
        dry_run["keep_names"], dry_run["delete_names"] = (
            dry_run["delete_names"],
            dry_run["keep_names"],
        )
        dry_run["candidate_sha256"] = task8_expected_dry_run_candidate_sha256(
            dry_run["delete_names"]
        )
    elif mutation == "reversed-pair-order":
        dry_run["delete_names"] = list(reversed(dry_run["delete_names"]))
        dry_run["candidate_sha256"] = task8_expected_dry_run_candidate_sha256(
            dry_run["delete_names"]
        )
    elif mutation == "rewritten-entry-digest":
        entry["evidence_sha256"] = HASH_D
    elif mutation == "missing-groups":
        state["rclone_evidence_groups"] = [
            group
            for group in groups
            if not str(group["group_id"]).startswith("task8:dry_run:")
        ]
    elif mutation == "pending-group":
        assert len(dry_run_groups) == len(TASK8_DRY_RUN_PURPOSES)
        pending = task8_pending_copy(dry_run_groups[-1])
        groups[groups.index(dry_run_groups[-1])] = pending
    elif mutation == "indeterminate-group":
        assert len(dry_run_groups) == len(TASK8_DRY_RUN_PURPOSES)
        dry_run_groups[1]["outcome"] = "indeterminate"
        task8_reseal_rclone_group(
            dry_run_groups[1],
            dry_run_receipt=dry_run,
        )
    elif mutation == "wrong-purpose-order":
        assert len(dry_run_groups) == len(TASK8_DRY_RUN_PURPOSES)
        dry_run_groups[0]["purpose"], dry_run_groups[1]["purpose"] = (
            dry_run_groups[1]["purpose"],
            dry_run_groups[0]["purpose"],
        )
        task8_reseal_rclone_group(
            dry_run_groups[0],
            dry_run_receipt=dry_run,
        )
        task8_reseal_rclone_group(
            dry_run_groups[1],
            dry_run_receipt=dry_run,
        )
    elif mutation == "retention-policy-config":
        effective = state["effective_config"]
        assert isinstance(effective, dict)
        effective["KEEP_REMOTE_DAILY"] = "6"
    elif mutation == "coherent-inventory-replacement":
        replacement = "degen_green_20250608T010203Z.dump"
        old_pair = task8_flatten_backup_pairs(
            [TASK8_DRY_RUN_RECEIPT_DELETE_DUMP]
        )
        replacement_pair = task8_flatten_backup_pairs([replacement])
        inventory = [
            name
            for name in dry_run["inventory_names"]
            if name not in set(old_pair)
        ]
        inventory.extend(replacement_pair)
        dry_run["inventory_names"] = sorted(inventory)
        dry_run["casefold_names"] = [
            name.casefold() for name in dry_run["inventory_names"]
        ]
        dry_run["delete_names"] = replacement_pair
        dry_run["candidate_sha256"] = task8_expected_dry_run_candidate_sha256(
            replacement_pair
        )
    elif mutation == "policy-equivalent-inventory-replacement":
        replacement = "degen_green_20250302T010203Z.dump"
        old_pair = task8_flatten_backup_pairs(
            [TASK8_DRY_RUN_RECEIPT_DELETE_DUMP]
        )
        replacement_pair = task8_flatten_backup_pairs([replacement])
        inventory = [
            name
            for name in dry_run["inventory_names"]
            if name not in set(old_pair)
        ]
        inventory.extend(replacement_pair)
        dry_run["inventory_names"] = sorted(inventory)
        dry_run["casefold_names"] = [
            name.casefold() for name in dry_run["inventory_names"]
        ]
        dry_run["delete_names"] = replacement_pair
        dry_run["candidate_sha256"] = task8_expected_dry_run_candidate_sha256(
            replacement_pair
        )
    else:
        raise AssertionError(mutation)
    task8_reseal_dry_run_state(state)


@pytest.mark.parametrize("validation_mode", ("context-free", "transition"))
@pytest.mark.parametrize(
    "mutation",
    (
        "keep-delete-complete-pair-swap",
        "reversed-pair-order",
        "rewritten-entry-digest",
        "missing-groups",
        "pending-group",
        "indeterminate-group",
        "wrong-purpose-order",
        "retention-policy-config",
        "coherent-inventory-replacement",
        "policy-equivalent-inventory-replacement",
    ),
)
def test_task8_dry_run_receipt_rejects_resealed_policy_and_audit_mutations(
    tmp_path: Path,
    validation_mode: str,
    mutation: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    baseline = task8_valid_dry_run_state(operation_dir)
    baseline_previous = task8_ready_dry_run_previous(operation_dir, baseline)
    module.validate_operation_state(
        baseline,
        operation_dir,
        baseline_previous if validation_mode == "transition" else None,
    )

    task8_apply_resealed_dry_run_mutation(baseline, mutation)
    with pytest.raises(module.OperationStateError):
        module.validate_operation_state(
            baseline,
            operation_dir,
            baseline_previous if validation_mode == "transition" else None,
        )


def test_task8_dry_run_completion_digest_commits_exact_current_attempt_groups(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = task8_valid_dry_run_state(operation_dir)
    receipt = state["dry_run"]
    entry, completed = task8_dry_run_completion_pair(state)
    assert isinstance(receipt, dict)

    assert module._task8_dry_run_completion_evidence_sha256(
        state,
        entry,
        completed,
        receipt,
    ) == task8_dry_run_completion_evidence(state, receipt)


def test_task8_dry_run_success_groups_bind_exact_result_domains(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = task8_valid_dry_run_state(operation_dir)
    receipt = state["dry_run"]
    entry, _completed = task8_dry_run_completion_pair(state)
    groups = task8_dry_run_attempt_groups(state, entry)
    assert isinstance(receipt, dict)
    assert len(groups) == len(TASK8_DRY_RUN_PURPOSES)

    for group, purpose in zip(groups, TASK8_DRY_RUN_PURPOSES, strict=True):
        expected_result = task8_expected_dry_run_result_sha256(
            purpose,
            receipt,
        )
        assert group["result_sha256"] == expected_result
        assert module._task8_dry_run_result_sha256(
            purpose,
            receipt,
        ) == expected_result
        expected_evidence = task8_expected_dry_run_group_evidence(group)
        assert group["evidence_sha256"] == expected_evidence
        assert module._task8_group_evidence_sha256(group) == expected_evidence
        altered = copy.deepcopy(group)
        altered["result_sha256"] = HASH_D
        assert module._task8_group_evidence_sha256(altered) != expected_evidence


@pytest.mark.parametrize(
    ("purpose", "receipt_field"),
    (
        ("dry-run-inventory-before", "inventory_names"),
        ("dry-run-inventory-before", "casefold_names"),
        ("dry-run-inventory-after", "inventory_names"),
        ("dry-run-inventory-after", "casefold_names"),
        ("dry-run-runtime", "delete_names"),
        ("dry-run-runtime", "candidate_sha256"),
    ),
)
def test_task8_dry_run_result_commitment_changes_with_each_bound_result_field(
    tmp_path: Path,
    purpose: str,
    receipt_field: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = task8_valid_dry_run_state(operation_dir)
    receipt = state["dry_run"]
    entry, _completed = task8_dry_run_completion_pair(state)
    groups = task8_dry_run_attempt_groups(state, entry)
    assert isinstance(receipt, dict)
    assert groups[TASK8_DRY_RUN_PURPOSES.index(purpose)]["purpose"] == purpose
    baseline = module._task8_dry_run_result_sha256(
        purpose,
        receipt,
    )
    mutated = copy.deepcopy(receipt)
    if receipt_field == "candidate_sha256":
        mutated[receipt_field] = HASH_D
    else:
        mutated[receipt_field] = [*mutated[receipt_field], "evidence-mutation"]

    assert module._task8_dry_run_result_sha256(
        purpose,
        mutated,
    ) != baseline


def test_task8_dry_run_entry_digest_commits_exact_policy_and_installed_hashes(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = task8_valid_dry_run_state(operation_dir)
    entry, _completed = task8_dry_run_completion_pair(state)

    assert module._task8_dry_run_entry_evidence_sha256(
        state,
        entry,
    ) == task8_dry_run_entry_evidence(state, entry)


def task8_load_retention_planner() -> object:
    planner_path = ROOT / "deploy" / "linux" / "degen-prod-db-retention.py"
    spec = importlib.util.spec_from_file_location(
        "degen_prod_db_retention_parity",
        planner_path,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def task8_flatten_retention_plan(plan: dict[str, object]) -> dict[str, list[str]]:
    return {
        "keep_names": [
            name
            for record in plan["keep"]
            for name in (record["dump"], record["checksum"])
        ],
        "protected_names": [record["name"] for record in plan["protected"]],
        "delete_names": [
            name
            for record in plan["delete"]
            for name in (record["dump"], record["checksum"])
        ],
    }


@pytest.mark.parametrize("inventory_kind", ("empty", "representative"))
def test_task8_pure_remote_plan_matches_installed_planner_policy(
    inventory_kind: str,
) -> None:
    module = load_ops_helper()
    planner = task8_load_retention_planner()
    now = datetime.fromtimestamp(1_750_000_070, tz=timezone.utc)
    if inventory_kind == "empty":
        inventory_names: list[str] = []
    else:
        future_dump = "degen_green_20260101T010203Z.dump"
        incomplete_dump = "degen_green_20250201T010203Z.dump"
        inventory_names = sorted(
            [
                *task8_valid_dry_run_receipt()["inventory_names"],
                TASK8_DRY_RUN_INVALID_DUMP,
                f"{TASK8_DRY_RUN_INVALID_DUMP}.sha256",
                future_dump,
                f"{future_dump}.sha256",
                incomplete_dump,
            ]
        )
    planner_result = planner.plan_inventory(
        inventory_names,
        mode="remote",
        prefix=TASK8_DRY_RUN_PREFIX,
        now=now,
        daily=7,
        weekly=4,
        monthly=3,
    )

    assert module._task8_plan_remote_inventory(
        inventory_names,
        prefix=TASK8_DRY_RUN_PREFIX,
        now=now,
        daily=7,
        weekly=4,
        monthly=3,
    ) == task8_flatten_retention_plan(planner_result)


# Normal dry-run execution is intentionally exercised through its public
# operation entrypoint.  This harness is separate from the decoder and
# command-runner unit tests above so it can prove the complete guard, audit,
# child-FD, receipt, and recovery lifecycle together.
class Task8DryRunProcessDeath(BaseException):
    pass


def task8_normal_dry_run_inventory_argv() -> tuple[str, ...]:
    return (
        "/usr/bin/rclone",
        "--config",
        TASK8_RCLONE_CONFIG_PATH,
        "lsf",
        EFFECTIVE_CONFIG["RCLONE_REMOTE_PATH"],
        "--files-only",
        "--max-depth",
        "1",
    )


def task8_normal_dry_run_runtime_argv(
    operation_dir: Path,
    runtime_lock_fd: int,
    now: datetime,
) -> tuple[str, ...]:
    installed_backup = operation_dir.joinpath(
        *PurePosixPath(TARGETS[0]).parts[1:]
    )
    return (
        str(installed_backup),
        "remote-retention-dry-run",
        "--lock-fd",
        str(runtime_lock_fd),
        "--now",
        now.strftime("%Y%m%dT%H%M%SZ"),
    )


def task8_normal_dry_run_inventory_output(names: list[str]) -> str:
    return "" if not names else "\n".join(reversed(names)) + "\n"


def task8_install_normal_dry_run_harness(
    module: object,
    monkeypatch: pytest.MonkeyPatch,
    operation_dir: Path,
    *,
    initial_phase: str = "probed",
    remote_prune_enabled: str = "0",
) -> tuple[object, dict[str, object]]:
    initial = state_at_phase(operation_dir, initial_phase)
    module.validate_operation_state(initial, operation_dir)
    if remote_prune_enabled != "0":
        initial["effective_config"]["REMOTE_PRUNE_ENABLED"] = (
            remote_prune_enabled
        )
    context, harness = task8_install_guard_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
    )
    context.host_root = operation_dir
    controls = harness["controls"]
    receipt = task8_valid_dry_run_receipt()
    inventory_names = list(receipt["inventory_names"])
    inventory_output = task8_normal_dry_run_inventory_output(inventory_names)
    controls.update(
        {
            "dry_run_inventory_outputs": [inventory_output, inventory_output],
            "dry_run_runtime_stdout": task8_remote_dry_run_log(
                list(receipt["delete_names"]),
            ),
            "dry_run_runtime_stderr": "",
            "dry_run_runtime_returncode": 0,
            "dry_run_crash_after_purpose": None,
            "installed_drift_phase": None,
            "live_target_proof_close_error": None,
        }
    )

    expected_install_bytes = {
        target: f"verified installed fixture for {target}\n".encode("ascii")
        for target in TARGETS
    }
    install = initial["install"]
    assert isinstance(install, dict)
    expected_install_hashes = copy.deepcopy(install["installed_hashes"])
    assert isinstance(expected_install_hashes, dict)
    target_verifications: list[dict[str, object]] = []
    install_byte_reads: list[str] = []
    planner_calls: list[dict[str, object]] = []
    installed_planner_calls: list[dict[str, object]] = []
    local_audits: list[dict[str, object]] = []
    live_target_proof_events: list[str] = []
    command_calls = harness["command_calls"]
    command_states: list[dict[str, object]] = []
    command_pass_fds: list[tuple[int, ...]] = []
    command_purposes: list[str] = []

    base_open_verified = module._open_verified_transaction_material

    @contextlib.contextmanager
    def validating_open_verified(
        open_context: object,
        allowed_phases: frozenset[str],
        *,
        helper_route: str = "source",
    ):
        current = copy.deepcopy(harness["holder"]["state"])
        module.validate_operation_state(current, operation_dir)
        with base_open_verified(
            open_context,
            allowed_phases,
            helper_route=helper_route,
        ) as material:
            yield material

    monkeypatch.setattr(
        module,
        "_open_verified_transaction_material",
        validating_open_verified,
    )

    base_write_state = module._task7_write_state

    def write_state(
        write_context: object,
        binding: object,
        candidate: dict[str, object],
        *,
        pre_replace_validator=None,
        **kwargs: object,
    ) -> None:
        receipt_hook = controls.get("policy_receipt_pre_replace_hook")
        if callable(receipt_hook):
            if receipt_hook(candidate):
                controls["policy_receipt_pre_replace_hook"] = None
        if pre_replace_validator is not None:
            pre_replace_validator()
        base_write_state(
            write_context,
            binding,
            candidate,
            **kwargs,
        )

    monkeypatch.setattr(module, "_task7_write_state", write_state)

    def install_bytes(material: object) -> dict[str, bytes]:
        source = material.source
        phase = str(source.state["phase"])
        install_byte_reads.append(phase)
        return copy.deepcopy(expected_install_bytes)

    def verify_installed_targets(
        _context: object,
        expected_bytes: dict[str, bytes],
    ) -> dict[str, str]:
        assert expected_bytes == expected_install_bytes
        current = harness["holder"]["state"]
        phase = str(current["phase"])
        observed = copy.deepcopy(expected_install_hashes)
        if controls["installed_drift_phase"] == phase:
            controls["installed_drift_phase"] = None
            observed[TARGETS[0]] = HASH_D
        target_verifications.append(
            {
                "phase": phase,
                "targets": tuple(observed),
                "hashes": copy.deepcopy(observed),
            }
        )
        harness["timeline"].append(f"verify-installed:{phase}")
        return observed

    monkeypatch.setattr(module, "_task7_install_bytes", install_bytes)
    monkeypatch.setattr(
        module,
        "_task7_verify_installed_targets",
        verify_installed_targets,
    )

    @contextlib.contextmanager
    def open_live_target_proofs(_context: object):
        live_target_proof_events.append("open")
        proofs = {target: object() for target in TARGETS}
        try:
            yield proofs
        finally:
            live_target_proof_events.append("close")
            close_error = controls["live_target_proof_close_error"]
            if close_error is not None:
                controls["live_target_proof_close_error"] = None
                raise close_error

    def validate_live_target_proofs(
        proof_state: dict[str, object],
        expected_bytes: dict[str, bytes],
        proofs: dict[str, object],
    ) -> None:
        assert expected_bytes == expected_install_bytes
        assert tuple(proofs) == TARGETS
        assert proof_state["install"]["installed_hashes"] == expected_install_hashes
        live_target_proof_events.append(
            "validate:" + str(proof_state["phase"])
        )

    monkeypatch.setattr(
        module,
        "_open_task7_live_target_proofs",
        open_live_target_proofs,
    )
    monkeypatch.setattr(
        module,
        "_task8_validate_live_installed_target_proofs",
        validate_live_target_proofs,
    )

    real_plan_remote_inventory = module._task8_plan_remote_inventory

    def plan_remote_inventory(
        names: list[str],
        *,
        prefix: str,
        now: datetime,
        daily: int,
        weekly: int,
        monthly: int,
    ) -> dict[str, list[str]]:
        planned = real_plan_remote_inventory(
            names,
            prefix=prefix,
            now=now,
            daily=daily,
            weekly=weekly,
            monthly=monthly,
        )
        planner_calls.append(
            {
                "inventory_names": list(names),
                "prefix": prefix,
                "now": now,
                "daily": daily,
                "weekly": weekly,
                "monthly": monthly,
                "result": copy.deepcopy(planned),
            }
        )
        return planned

    monkeypatch.setattr(
        module,
        "_task8_plan_remote_inventory",
        plan_remote_inventory,
    )

    def run_installed_planner(
        _context: object,
        planner_state: dict[str, object],
        names: list[str],
        *,
        runtime_lock_fd: int,
    ) -> dict[str, list[str]]:
        transaction = planner_state["active_transaction"]
        effective = planner_state["effective_config"]
        assert isinstance(transaction, dict) and isinstance(effective, dict)
        now = datetime.fromtimestamp(
            int(transaction["started_epoch"]),
            tz=timezone.utc,
        )
        installed_planner_calls.append(
            {
                "inventory_names": list(names),
                "runtime_lock_fd": runtime_lock_fd,
                "now": now,
            }
        )
        return plan_remote_inventory(
            names,
            prefix=str(effective["BACKUP_PREFIX"]),
            now=now,
            daily=int(str(effective["KEEP_REMOTE_DAILY"])),
            weekly=int(str(effective["KEEP_REMOTE_WEEKLY"])),
            monthly=int(str(effective["KEEP_REMOTE_MONTHLY"])),
        )

    monkeypatch.setattr(
        module,
        "_task8_run_remote_retention_planner",
        run_installed_planner,
    )

    def capture_audit(
        _context: object,
        logical_path: str,
    ) -> dict[str, object]:
        assert logical_path == TASK8_RCLONE_CONFIG_PATH
        ordinal = len(local_audits)
        audit = task8_file_audit(
            hashlib.sha256(f"dry-run-audit-{ordinal}".encode("ascii")).hexdigest(),
            inode=800 + ordinal,
            size=900 + ordinal,
            mtime_ns=1_770_000_000_000_000_000 + ordinal,
        )
        local_audits.append(audit)
        harness["timeline"].append("audit:rclone-config")
        return copy.deepcopy(audit)

    monkeypatch.setattr(module, "_task7_capture_file_audit", capture_audit)

    def command_runner(
        argv: object,
        pass_fds: tuple[int, ...],
    ) -> subprocess.CompletedProcess[str]:
        argv_tuple = tuple(str(value) for value in argv)
        acquisitions = harness["fresh_acquisitions"]
        assert acquisitions
        runtime_lock_fd = acquisitions[-1][1]
        assert pass_fds == (runtime_lock_fd,)
        current = harness["holder"]["state"]
        groups = current["rclone_evidence_groups"]
        assert isinstance(groups, list) and groups
        pending = groups[-1]
        assert isinstance(pending, dict) and pending["after"] is None
        purpose = str(pending["purpose"])
        expected_argv = {
            "dry-run-inventory-before": task8_normal_dry_run_inventory_argv(),
            "dry-run-runtime": task8_normal_dry_run_runtime_argv(
                operation_dir,
                runtime_lock_fd,
                datetime.fromtimestamp(
                    int(current["active_transaction"]["started_epoch"]),
                    tz=timezone.utc,
                ),
            ),
            "dry-run-inventory-after": task8_normal_dry_run_inventory_argv(),
        }
        assert purpose in expected_argv
        assert argv_tuple == expected_argv[purpose]
        forbidden_mutations = {
            "copy",
            "copyto",
            "delete",
            "deletefile",
            "move",
            "moveto",
            "purge",
        }
        assert not forbidden_mutations.intersection(argv_tuple)
        command_calls.append(argv_tuple)
        command_states.append(copy.deepcopy(current))
        command_pass_fds.append(pass_fds)
        command_purposes.append(purpose)
        harness["timeline"].append("command:" + purpose)

        if purpose in {
            "dry-run-inventory-before",
            "dry-run-inventory-after",
        }:
            queued = controls["dry_run_inventory_outputs"]
            assert isinstance(queued, list) and queued
            stdout = str(queued.pop(0))
            stderr = ""
            returncode = 0
        else:
            stdout = str(controls["dry_run_runtime_stdout"])
            stderr = str(controls["dry_run_runtime_stderr"])
            returncode = int(controls["dry_run_runtime_returncode"])
        if controls["dry_run_crash_after_purpose"] == purpose:
            controls["dry_run_crash_after_purpose"] = None
            raise Task8DryRunProcessDeath(
                f"process death after {purpose}"
            )
        return subprocess.CompletedProcess(
            argv_tuple,
            returncode,
            stdout,
            stderr,
        )

    context.command_runner = command_runner
    harness.update(
        {
            "initial": copy.deepcopy(initial),
            "expected_install_bytes": expected_install_bytes,
            "expected_install_hashes": expected_install_hashes,
            "target_verifications": target_verifications,
            "install_byte_reads": install_byte_reads,
            "planner_calls": planner_calls,
            "installed_planner_calls": installed_planner_calls,
            "local_audits": local_audits,
            "live_target_proof_events": live_target_proof_events,
            "command_states": command_states,
            "command_pass_fds": command_pass_fds,
            "command_purposes": command_purposes,
            "inventory_names": inventory_names,
        }
    )
    return context, harness


def test_task8_normal_dry_run_success_records_exact_guarded_review_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_dry_run_harness(
        module,
        monkeypatch,
        operation_dir,
    )

    result = module.record_remote_dry_run(context)

    assert harness["initial"]["phase"] == "probed"
    assert result["phase"] == "dry_run_recorded"
    assert result["active_transaction"] is None
    assert result["failure"] is None
    assert result["recovery"] is None
    assert result["policy"] is None
    module.validate_operation_state(result, operation_dir)
    assert harness["proof_routes"]
    assert set(harness["proof_routes"]) == {"installed"}

    verifications = harness["target_verifications"]
    assert len(verifications) >= 2
    assert verifications[0]["phase"] == "probed"
    assert verifications[-1]["phase"] == "dry_run_recording"
    for verification in verifications:
        assert verification["targets"] == TARGETS
        assert verification["hashes"] == harness["expected_install_hashes"]
    assert harness["install_byte_reads"][0] == "probed"

    entry_state = next(
        durable
        for durable in harness["writes"]
        if durable["phase"] == "dry_run_recording"
        and not any(
            str(group["group_id"]).startswith("task8:dry_run:")
            for group in durable["rclone_evidence_groups"]
        )
    )
    transaction = entry_state["active_transaction"]
    assert transaction["kind"] == "dry_run"
    assert transaction["prior_stable_phase"] == "probed"
    assert transaction["probe"] is None
    assert not any(transaction["guard"].values())
    assert entry_state["dry_run"] is None
    entry = entry_state["phase_history"][-1]
    assert entry == {
        "phase": "dry_run_recording",
        "epoch": transaction["started_epoch"],
        "evidence_sha256": task8_dry_run_entry_evidence(entry_state, entry),
    }

    runtime_lock_fd = harness["fresh_acquisitions"][0][1]
    expected_calls = [
        task8_normal_dry_run_inventory_argv(),
        task8_normal_dry_run_runtime_argv(
            operation_dir,
            runtime_lock_fd,
            datetime.fromtimestamp(int(entry["epoch"]), tz=timezone.utc),
        ),
        task8_normal_dry_run_inventory_argv(),
    ]
    assert harness["command_purposes"] == list(TASK8_DRY_RUN_PURPOSES)
    assert harness["command_calls"] == expected_calls
    assert harness["command_pass_fds"] == [
        (runtime_lock_fd,) for _call in expected_calls
    ]
    assert not any(
        token in {"copy", "copyto", "delete", "deletefile", "move", "moveto", "purge"}
        for argv in harness["command_calls"]
        for token in argv
    )

    dry_run = result["dry_run"]
    expected_receipt = task8_valid_dry_run_receipt()
    for field in (
        "inventory_names",
        "casefold_names",
        "keep_names",
        "protected_names",
        "delete_names",
        "candidate_sha256",
    ):
        assert dry_run[field] == expected_receipt[field]
    assert dry_run["inventory_names"] == harness["inventory_names"]
    assert dry_run["casefold_names"] == [
        name.casefold() for name in harness["inventory_names"]
    ]
    assert harness["planner_calls"]
    assert harness["installed_planner_calls"] == [
        {
            "inventory_names": harness["inventory_names"],
            "runtime_lock_fd": runtime_lock_fd,
            "now": datetime.fromtimestamp(
                int(entry["epoch"]),
                tz=timezone.utc,
            ),
        }
    ]
    assert any(
        call["result"]
        == {
            "keep_names": dry_run["keep_names"],
            "protected_names": dry_run["protected_names"],
            "delete_names": dry_run["delete_names"],
        }
        for call in harness["planner_calls"]
    )

    groups = [
        group
        for group in result["rclone_evidence_groups"]
        if str(group["group_id"]).startswith("task8:dry_run:")
    ]
    assert [group["purpose"] for group in groups] == list(
        TASK8_DRY_RUN_PURPOSES
    )
    assert len(harness["local_audits"]) == 2 * len(groups)
    for ordinal, (group, purpose) in enumerate(
        zip(groups, TASK8_DRY_RUN_PURPOSES, strict=True)
    ):
        assert group["outcome"] == "success"
        assert group["before"] == harness["local_audits"][2 * ordinal]
        assert group["after"] == harness["local_audits"][2 * ordinal + 1]
        assert group["result_sha256"] == module._task8_dry_run_result_sha256(
            purpose,
            dry_run,
        )
        assert group["evidence_sha256"] == module._task8_group_evidence_sha256(
            group,
        )

    assert harness["external_actions"] == [
        "timer_disable",
        "timer_stop",
        "quiesce_service_check",
        "quiesce_runtime_readback",
        "legacy_lock_acquire",
        "runtime_lock_acquire",
        "post_lock_service_recheck",
        "runtime_lock_release",
        "legacy_lock_release",
        "pre_restore_service_check",
        "timer_enable",
        "timer_start",
        "restore_runtime_readback",
    ]
    assert harness["release_actions"] == [
        "runtime_lock_release",
        "legacy_lock_release",
    ]
    assert harness["live_runtime"] == harness["baseline"]
    assert harness["live_target_proof_events"][0] == "open"
    assert harness["live_target_proof_events"][-1] == "close"
    assert harness["live_target_proof_events"].count("open") == 1
    assert harness["live_target_proof_events"].count("close") == 1
    assert harness["live_target_proof_events"].count(
        "validate:dry_run_recording"
    ) >= len(TASK8_DRY_RUN_PURPOSES) + 2
    assert harness["timeline"].index("action:restore_runtime_readback") < next(
        index
        for index, event in enumerate(harness["timeline"])
        if event == "write:dry_run_recorded"
    )


def test_task8_normal_dry_run_requires_exact_probed_start(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_dry_run_harness(
        module,
        monkeypatch,
        operation_dir,
        initial_phase="installed",
    )

    with pytest.raises(
        module.OperationStateError,
        match="probed|stable phase|phase",
    ):
        module.record_remote_dry_run(context)

    assert harness["holder"]["state"]["phase"] == "installed"
    assert harness["writes"] == []
    assert harness["external_actions"] == []
    assert harness["command_calls"] == []


def test_task8_normal_dry_run_requires_remote_prune_disabled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_dry_run_harness(
        module,
        monkeypatch,
        operation_dir,
        remote_prune_enabled="1",
    )

    with pytest.raises(
        module.OperationStateError,
        match="prune|disabled|REMOTE_PRUNE_ENABLED",
    ):
        module.record_remote_dry_run(context)

    assert harness["holder"]["state"]["phase"] == "probed"
    assert harness["writes"] == []
    assert harness["external_actions"] == []
    assert harness["command_calls"] == []


@pytest.mark.parametrize(
    "failure_kind",
    ("inventory-drift", "candidate-mismatch", "installed-target-drift"),
)
def test_task8_normal_dry_run_divergence_routes_to_guard_recovery_without_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_kind: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_dry_run_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    controls = harness["controls"]
    if failure_kind == "inventory-drift":
        drifted = sorted([*harness["inventory_names"], "manual-drift.txt"])
        controls["dry_run_inventory_outputs"][1] = (
            task8_normal_dry_run_inventory_output(drifted)
        )
        expected = "inventory|drift|changed"
    elif failure_kind == "candidate-mismatch":
        controls["dry_run_runtime_stdout"] = task8_remote_dry_run_log([])
        expected = "candidate|plan|differ"
    else:
        controls["installed_drift_phase"] = "dry_run_recording"
        expected = "installed|target|hash|drift"

    with pytest.raises(module.OperationStateError, match=expected):
        module.record_remote_dry_run(context)

    interrupted = harness["holder"]["state"]
    assert interrupted["phase"] == "recovery_required"
    assert interrupted["active_transaction"]["kind"] == "dry_run"
    assert interrupted["failure"]["phase"] == "dry_run_recording"
    assert interrupted["recovery"]["kind"] == "guard"
    assert interrupted["dry_run"] is None
    assert not any(
        durable["phase"] == "dry_run_recorded"
        for durable in harness["writes"]
    )
    module.validate_operation_state(interrupted, operation_dir)

    recovered = module.recover_host_configuration(context)

    assert recovered["phase"] == "probed"
    assert recovered["dry_run"] is None
    assert recovered["active_transaction"] is None
    module.validate_operation_state(recovered, operation_dir)


def test_task8_normal_dry_run_proof_teardown_preserves_primary_and_records_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_dry_run_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    harness["controls"]["dry_run_runtime_stdout"] = task8_remote_dry_run_log([])
    harness["controls"]["live_target_proof_close_error"] = (
        module.OperationStateError("controlled live target proof teardown failure")
    )

    with pytest.raises(module.OperationStateError, match="candidate|plan|differ"):
        module.record_remote_dry_run(context)

    interrupted = harness["holder"]["state"]
    assert interrupted["phase"] == "recovery_required"
    assert interrupted["failure"]["phase"] == "dry_run_recording"
    assert "candidate" in interrupted["failure"]["primary_error"].lower()
    assert interrupted["secondary_errors"][-1]["stage"] == (
        "installed_target_proof_close"
    )
    assert "proof teardown" in interrupted["secondary_errors"][-1]["error"]
    assert harness["release_actions"] == [
        "runtime_lock_release",
        "legacy_lock_release",
    ]
    module.validate_operation_state(interrupted, operation_dir)


def test_task8_normal_dry_run_baseexception_is_not_masked_by_proof_teardown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_dry_run_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    harness["controls"]["dry_run_crash_after_purpose"] = (
        "dry-run-inventory-before"
    )
    harness["controls"]["live_target_proof_close_error"] = (
        Task8DryRunProcessDeath("proof teardown process death")
    )

    with pytest.raises(
        Task8DryRunProcessDeath,
        match="process death after dry-run-inventory-before",
    ):
        module.record_remote_dry_run(context)

    crashed = harness["holder"]["state"]
    assert crashed["phase"] == "dry_run_recording"
    assert crashed["failure"] is None
    assert crashed["recovery"] is None
    assert harness["live_target_proof_events"][-1] == "close"
    module.validate_operation_state(crashed, operation_dir)


def test_task8_normal_dry_run_proof_teardown_process_death_overrides_ordinary_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_dry_run_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    harness["controls"]["dry_run_runtime_stdout"] = task8_remote_dry_run_log([])
    harness["controls"]["live_target_proof_close_error"] = (
        Task8DryRunProcessDeath("proof teardown process death")
    )

    with pytest.raises(Task8DryRunProcessDeath, match="proof teardown process death"):
        module.record_remote_dry_run(context)

    crashed = harness["holder"]["state"]
    assert crashed["phase"] == "dry_run_recording"
    assert crashed["failure"] is None
    assert crashed["recovery"] is None
    assert crashed["dry_run"] is None
    assert harness["release_actions"] == [
        "runtime_lock_release",
        "legacy_lock_release",
    ]
    assert harness["live_target_proof_events"][-1] == "close"
    module.validate_operation_state(crashed, operation_dir)


def test_task8_normal_dry_run_accepts_valid_runtime_log_above_default_command_cap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_dry_run_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    inventory_names = sorted(
        name
        for second in range(10)
        for name in (
            f"{TASK8_DRY_RUN_PREFIX}20200101T0000{second:02d}Z.dump",
            f"{TASK8_DRY_RUN_PREFIX}20200101T0000{second:02d}Z.dump.sha256",
        )
    )
    effective = harness["initial"]["effective_config"]
    plan = module._task8_plan_remote_inventory(
        inventory_names,
        prefix=str(effective["BACKUP_PREFIX"]),
        now=datetime(2026, 7, 1, tzinfo=timezone.utc),
        daily=int(effective["KEEP_REMOTE_DAILY"]),
        weekly=int(effective["KEEP_REMOTE_WEEKLY"]),
        monthly=int(effective["KEEP_REMOTE_MONTHLY"]),
    )
    assert len(plan["delete_names"]) == 18
    runtime_log = task8_remote_dry_run_log(plan["delete_names"])
    assert module._MAX_COMMAND_OUTPUT_BYTES < len(runtime_log.encode("utf-8"))
    assert len(runtime_log.encode("utf-8")) <= module._MAX_STATE_BYTES
    inventory_output = task8_normal_dry_run_inventory_output(inventory_names)
    harness["controls"]["dry_run_inventory_outputs"] = [
        inventory_output,
        inventory_output,
    ]
    harness["controls"]["dry_run_runtime_stdout"] = runtime_log

    result = module.record_remote_dry_run(context)

    assert result["phase"] == "dry_run_recorded"
    assert result["dry_run"]["inventory_names"] == inventory_names
    assert result["dry_run"]["delete_names"] == plan["delete_names"]
    module.validate_operation_state(result, operation_dir)


@pytest.mark.parametrize("output_kind", ("stderr", "malformed", "oversized"))
def test_task8_normal_dry_run_runtime_output_failure_records_no_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    output_kind: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_dry_run_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    controls = harness["controls"]
    if output_kind == "stderr":
        controls["dry_run_runtime_stderr"] = "unexpected runtime stderr\n"
    elif output_kind == "malformed":
        controls["dry_run_runtime_stdout"] = "not a timestamped log\n"
    else:
        controls["dry_run_runtime_stdout"] = "x" * (
            module._MAX_STATE_BYTES + 1
        )

    with pytest.raises(
        module.OperationStateError,
        match="stderr|output|log|size|limit|encoding|timestamp",
    ):
        module.record_remote_dry_run(context)

    interrupted = harness["holder"]["state"]
    assert interrupted["phase"] == "recovery_required"
    assert interrupted["dry_run"] is None
    assert interrupted["recovery"]["kind"] == "guard"
    assert not any(
        durable["phase"] == "dry_run_recorded"
        for durable in harness["writes"]
    )
    module.validate_operation_state(interrupted, operation_dir)


@pytest.mark.parametrize("purpose", TASK8_DRY_RUN_PURPOSES)
def test_task8_normal_dry_run_process_death_fabricates_no_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    purpose: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_dry_run_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    harness["controls"]["dry_run_crash_after_purpose"] = purpose

    with pytest.raises(Task8DryRunProcessDeath, match=purpose):
        module.record_remote_dry_run(context)

    crashed = harness["holder"]["state"]
    assert crashed["phase"] == "dry_run_recording"
    assert crashed["dry_run"] is None
    assert crashed["failure"] is None
    assert crashed["recovery"] is None
    assert crashed["active_transaction"]["kind"] == "dry_run"
    pending = crashed["rclone_evidence_groups"][-1]
    assert pending["purpose"] == purpose
    assert pending["before"] is not None
    assert pending["after"] is None
    assert pending["outcome"] is None
    assert pending["result_sha256"] is None
    assert pending["evidence_sha256"] is None
    assert not any(
        durable["phase"] == "dry_run_recorded"
        for durable in harness["writes"]
    )
    assert harness["release_actions"] == []
    assert harness["live_runtime"]["timer_enabled"] is False
    assert harness["live_runtime"]["timer_active"] is False
    module.validate_operation_state(crashed, operation_dir)


@pytest.mark.parametrize("purpose", TASK8_DRY_RUN_PURPOSES)
def test_task8_normal_dry_run_process_death_after_success_audit_recovers_without_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    purpose: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_dry_run_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    original_complete = module._task8_complete_rclone_audit
    injected = False

    def complete_then_crash(*args: object, **kwargs: object) -> dict[str, object]:
        nonlocal injected
        result = original_complete(*args, **kwargs)
        group = result["rclone_evidence_groups"][-1]
        if not injected and group["purpose"] == purpose:
            injected = True
            raise Task8DryRunProcessDeath(f"process death after audit {purpose}")
        return result

    monkeypatch.setattr(module, "_task8_complete_rclone_audit", complete_then_crash)

    with pytest.raises(Task8DryRunProcessDeath, match=f"audit {purpose}"):
        module.record_remote_dry_run(context)

    crashed = harness["holder"]["state"]
    assert crashed["phase"] == "dry_run_recording"
    assert crashed["dry_run"] is None
    assert crashed["failure"] is None
    assert crashed["recovery"] is None
    group = crashed["rclone_evidence_groups"][-1]
    assert group["purpose"] == purpose
    assert group["outcome"] == "success"
    assert group["after"] is not None
    assert group["result_sha256"] is not None
    module.validate_operation_state(crashed, operation_dir)

    recovered = module.recover_host_configuration(context)

    assert recovered["phase"] == "probed"
    assert recovered["dry_run"] is None
    assert recovered["active_transaction"] is None
    module.validate_operation_state(recovered, operation_dir)


def test_task8_normal_dry_run_process_death_after_planner_recovers_without_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_dry_run_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    original_planner = module._task8_run_remote_retention_planner

    def planner_then_crash(*args: object, **kwargs: object) -> dict[str, list[str]]:
        original_planner(*args, **kwargs)
        raise Task8DryRunProcessDeath("process death after installed planner")

    monkeypatch.setattr(module, "_task8_run_remote_retention_planner", planner_then_crash)

    with pytest.raises(Task8DryRunProcessDeath, match="installed planner"):
        module.record_remote_dry_run(context)

    crashed = harness["holder"]["state"]
    assert crashed["phase"] == "dry_run_recording"
    assert crashed["dry_run"] is None
    assert [
        group["purpose"]
        for group in crashed["rclone_evidence_groups"]
        if str(group["group_id"]).startswith("task8:dry_run:")
    ] == ["dry-run-inventory-before"]
    module.validate_operation_state(crashed, operation_dir)

    recovered = module.recover_host_configuration(context)

    assert recovered["phase"] == "probed"
    assert recovered["dry_run"] is None
    module.validate_operation_state(recovered, operation_dir)


@pytest.mark.parametrize(
    "action",
    (
        "runtime_lock_release",
        "legacy_lock_release",
        "timer_enable",
        "timer_start",
        "restore_runtime_readback",
    ),
)
def test_task8_normal_dry_run_process_death_during_release_or_restore_recovers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    action: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_dry_run_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    harness["controls"]["crash_after"] = action

    with pytest.raises(Task8GuardProcessDeath, match=action):
        module.record_remote_dry_run(context)

    crashed = harness["holder"]["state"]
    assert crashed["phase"] == "dry_run_recording"
    assert crashed["dry_run"] is None
    assert crashed["failure"] is None
    assert crashed["recovery"] is None
    module.validate_operation_state(crashed, operation_dir)

    harness["controls"]["crash_after"] = None
    recovered = module.recover_host_configuration(context)

    assert recovered["phase"] == "probed"
    assert recovered["dry_run"] is None
    assert recovered["active_transaction"] is None
    module.validate_operation_state(recovered, operation_dir)


def test_task8_normal_dry_run_process_death_after_timer_restored_write_recovers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_dry_run_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    harness["controls"]["write_crash_after_guard_field"] = "timer_restored"

    with pytest.raises(Task8GuardProcessDeath, match="timer_restored"):
        module.record_remote_dry_run(context)

    crashed = harness["holder"]["state"]
    assert crashed["phase"] == "dry_run_recording"
    assert crashed["active_transaction"]["guard"]["timer_restored"] is True
    assert crashed["dry_run"] is None
    module.validate_operation_state(crashed, operation_dir)

    recovered = module.recover_host_configuration(context)

    assert recovered["phase"] == "probed"
    assert recovered["dry_run"] is None
    module.validate_operation_state(recovered, operation_dir)


def test_task8_normal_dry_run_process_death_after_terminal_write_keeps_exact_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_dry_run_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    harness["controls"]["write_crash_after_phase"] = "dry_run_recorded"

    with pytest.raises(Task8GuardProcessDeath, match="dry_run_recorded"):
        module.record_remote_dry_run(context)

    durable = harness["holder"]["state"]
    assert durable["phase"] == "dry_run_recorded"
    assert durable["dry_run"] is not None
    assert durable["active_transaction"] is None
    assert durable["failure"] is None
    assert durable["recovery"] is None
    module.validate_operation_state(durable, operation_dir)


def task8_provisional_policy_state(operation_dir: Path) -> dict[str, object]:
    state = state_at_phase(operation_dir, "policy_enabling")
    dry_run = state["dry_run"]
    assert isinstance(dry_run, dict)
    group = task8_successful_policy_group(dry_run)
    state["rclone_evidence_groups"].append(group)
    transaction = state["active_transaction"]
    assert isinstance(transaction, dict)
    transaction["guard"] = {
        "timer_stopped": True,
        "service_inactive_verified": True,
        "legacy_lock_acquired": True,
        "runtime_lock_acquired": True,
        "locks_released": False,
        "timer_restored": False,
    }
    entry = state["phase_history"][-1]
    assert isinstance(entry, dict)
    state["policy"] = task8_policy_receipt(
        state,
        entry,
        group,
        runtime_baseline=transaction["runtime_baseline"],
    )
    return state


def task8_completed_policy_state(
    operation_dir: Path,
) -> tuple[dict[str, object], dict[str, object]]:
    previous = task8_provisional_policy_state(operation_dir)
    transaction = previous["active_transaction"]
    assert isinstance(transaction, dict)
    guard = transaction["guard"]
    assert isinstance(guard, dict)
    for field in guard:
        guard[field] = True
    current = copy.deepcopy(previous)
    current["active_transaction"] = None
    append_phase(current, "policy_enabled", 1_750_000_100)
    entry = next(
        item
        for item in reversed(current["phase_history"])
        if item["phase"] == "policy_enabling"
    )
    completed = current["phase_history"][-1]
    policy = current["policy"]
    dry_run = current["dry_run"]
    group = current["rclone_evidence_groups"][-1]
    assert isinstance(policy, dict) and isinstance(dry_run, dict)
    completed["evidence_sha256"] = task8_policy_completion_evidence(
        entry=entry,
        completed_epoch=int(completed["epoch"]),
        policy=policy,
        dry_run=dry_run,
        group=group,
    )
    return previous, current


def test_task8_policy_enabling_accepts_exact_provisional_applied_receipt(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = task8_provisional_policy_state(operation_dir)

    module.validate_operation_state(state, operation_dir)


def test_task8_policy_provisional_receipt_transition_requires_exact_held_guard(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    current = task8_provisional_policy_state(operation_dir)
    previous = copy.deepcopy(current)
    previous["policy"] = None

    module.validate_operation_state(previous, operation_dir)
    module.validate_operation_state(current, operation_dir, previous)


@pytest.mark.parametrize(
    "true_fields",
    [
        (),
        ("timer_stopped",),
        ("timer_stopped", "service_inactive_verified"),
        (
            "timer_stopped",
            "service_inactive_verified",
            "legacy_lock_acquired",
        ),
    ],
)
def test_task8_persisted_applied_policy_requires_both_held_locks(
    tmp_path: Path,
    true_fields: tuple[str, ...],
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = task8_provisional_policy_state(operation_dir)
    transaction = state["active_transaction"]
    assert isinstance(transaction, dict)
    transaction["guard"] = {
        field: field in true_fields
        for field in transaction["guard"]
    }

    with pytest.raises(module.OperationStateError, match="policy.*guard|held locks"):
        module.validate_operation_state(state, operation_dir)


@pytest.mark.parametrize(
    "locks_released,timer_restored",
    [(True, False), (True, True)],
)
def test_task8_persisted_uncommitted_raw_policy_cannot_release_guard(
    tmp_path: Path,
    locks_released: bool,
    timer_restored: bool,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = task8_provisional_policy_state(operation_dir)
    state["policy"] = None
    guard = state["active_transaction"]["guard"]
    guard["locks_released"] = locks_released
    guard["timer_restored"] = timer_restored

    with pytest.raises(module.OperationStateError, match="uncommitted policy.*guard"):
        module.validate_operation_state(state, operation_dir)


@pytest.mark.parametrize(
    "locks_released,timer_restored",
    [(False, False), (True, False), (True, True)],
)
def test_task8_persisted_applied_policy_accepts_only_guard_completion_suffixes(
    tmp_path: Path,
    locks_released: bool,
    timer_restored: bool,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = task8_provisional_policy_state(operation_dir)
    guard = state["active_transaction"]["guard"]
    guard["locks_released"] = locks_released
    guard["timer_restored"] = timer_restored

    module.validate_operation_state(state, operation_dir)


@pytest.mark.parametrize(
    "true_fields",
    [
        (),
        ("timer_stopped",),
        ("timer_stopped", "service_inactive_verified"),
        (
            "timer_stopped",
            "service_inactive_verified",
            "legacy_lock_acquired",
        ),
    ],
)
def test_task8_policy_provisional_receipt_rejects_wrong_guard_milestone(
    tmp_path: Path,
    true_fields: tuple[str, ...],
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    current = task8_provisional_policy_state(operation_dir)
    previous = copy.deepcopy(current)
    previous["policy"] = None
    guard = {
        field: field in true_fields
        for field in previous["active_transaction"]["guard"]
    }
    previous["active_transaction"]["guard"] = copy.deepcopy(guard)
    current["active_transaction"]["guard"] = copy.deepcopy(guard)

    module.validate_operation_state(previous, operation_dir)
    with pytest.raises(module.OperationStateError, match="policy.*lock|stopped runtime"):
        module.validate_operation_state(current, operation_dir, previous)


def test_task8_policy_entry_evidence_is_independently_recomputed(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = task8_provisional_policy_state(operation_dir)
    entry = state["phase_history"][-1]
    policy = state["policy"]
    group = state["rclone_evidence_groups"][-1]
    assert isinstance(entry, dict) and isinstance(policy, dict)
    entry["evidence_sha256"] = HASH_D
    policy["applied_evidence_sha256"] = task8_policy_applied_evidence(
        state,
        entry,
        policy,
        group,
    )

    with pytest.raises(module.OperationStateError, match="policy entry evidence"):
        module.validate_operation_state(state, operation_dir)


def test_task8_policy_enabled_epoch_cannot_precede_transaction_entry(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = task8_provisional_policy_state(operation_dir)
    entry = state["phase_history"][-1]
    policy = state["policy"]
    group = state["rclone_evidence_groups"][-1]
    assert isinstance(entry, dict) and isinstance(policy, dict)
    policy["enabled_epoch"] = int(entry["epoch"]) - 1
    policy["applied_evidence_sha256"] = task8_policy_applied_evidence(
        state,
        entry,
        policy,
        group,
    )

    with pytest.raises(module.OperationStateError, match="enabled_epoch|policy_enabling"):
        module.validate_operation_state(state, operation_dir)


def test_task8_completed_policy_rejects_enabled_epoch_after_completion(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    _previous, state = task8_completed_policy_state(operation_dir)
    entry = next(
        item
        for item in reversed(state["phase_history"])
        if item["phase"] == "policy_enabling"
    )
    completed = state["phase_history"][-1]
    policy = state["policy"]
    group = state["rclone_evidence_groups"][-1]
    assert isinstance(policy, dict)
    policy["enabled_epoch"] = int(completed["epoch"]) + 1
    policy["applied_evidence_sha256"] = task8_policy_applied_evidence(
        state,
        entry,
        policy,
        group,
    )
    completed["evidence_sha256"] = task8_policy_completion_evidence(
        entry=entry,
        completed_epoch=int(completed["epoch"]),
        policy=policy,
        dry_run=state["dry_run"],
        group=group,
    )

    with pytest.raises(module.OperationStateError, match="enabled_epoch|completion"):
        module.validate_operation_state(state, operation_dir)


def test_task8_policy_applied_target_and_evidence_are_exact(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = task8_provisional_policy_state(operation_dir)
    entry = state["phase_history"][-1]
    policy = state["policy"]
    group = state["rclone_evidence_groups"][-1]
    assert isinstance(entry, dict) and isinstance(policy, dict)
    policy["applied_target"]["mode"] = 0o644
    policy["applied_evidence_sha256"] = task8_policy_applied_evidence(
        state,
        entry,
        policy,
        group,
    )

    with pytest.raises(module.OperationStateError, match="policy.*metadata|applied_target"):
        module.validate_operation_state(state, operation_dir)

    state = task8_provisional_policy_state(operation_dir)
    state["policy"]["applied_evidence_sha256"] = HASH_D
    with pytest.raises(module.OperationStateError, match="applied evidence"):
        module.validate_operation_state(state, operation_dir)


def test_task8_policy_applied_target_owner_is_bound_to_context(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    state = task8_provisional_policy_state(operation_dir)
    entry = state["phase_history"][-1]
    policy = state["policy"]
    group = state["rclone_evidence_groups"][-1]
    assert isinstance(policy, dict)
    policy["applied_target"]["uid"] = uid + 1
    policy["applied_evidence_sha256"] = task8_policy_applied_evidence(
        state,
        entry,
        policy,
        group,
    )
    module.validate_operation_state(state, operation_dir)
    context = types.SimpleNamespace(
        operation_id=operation_dir.name,
        paths=module.build_operation_paths(operation_dir),
        expected_commit=state["reviewed_source"]["commit"],
        expected_archive_sha256=state["reviewed_source"]["archive_sha256"],
        expected_manifest_sha256=state["reviewed_source"]["manifest_sha256"],
        effective_uid=uid,
        host_root=tmp_path,
    )

    with pytest.raises(module.OperationStateError, match="owner.*context"):
        module.validate_operation_state_for_context(state, context)


def test_task8_policy_provisional_receipt_requires_exact_inventory_commitment(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = task8_provisional_policy_state(operation_dir)
    state["rclone_evidence_groups"][-1]["result_sha256"] = HASH_D
    task8_reseal_rclone_group(state["rclone_evidence_groups"][-1])

    with pytest.raises(module.OperationStateError, match="policy|inventory|result|commit"):
        module.validate_operation_state(state, operation_dir)


def test_task8_policy_enabled_completion_preserves_provisional_receipt_and_evidence(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    previous, current = task8_completed_policy_state(operation_dir)

    module.validate_operation_state(previous, operation_dir)
    module.validate_operation_state(current, operation_dir, previous)

    assert current["policy"] == previous["policy"]
    assert current["phase"] == "policy_enabled"
    assert current["active_transaction"] is None


def test_task8_policy_enabled_rejects_missing_current_attempt_inventory_commitment(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    _previous, current = task8_completed_policy_state(operation_dir)
    current["rclone_evidence_groups"] = [
        group
        for group in current["rclone_evidence_groups"]
        if not str(group["group_id"]).startswith("task8:policy:")
    ]

    with pytest.raises(module.OperationStateError, match="policy|inventory|audit|commit"):
        module.validate_operation_state(current, operation_dir)


def task8_policy_atomic_context(module: object, tmp_path: Path) -> object:
    operation_dir, uid = private_operation_dir(tmp_path)
    return module.OperationsContext(
        operation_id=operation_dir.name,
        paths=module.build_operation_paths(operation_dir),
        effective_uid=uid,
        command_runner=lambda argv, pass_fds: subprocess.CompletedProcess(
            tuple(str(value) for value in argv),
            0,
            "",
            "",
        ),
        clock=lambda: datetime(2026, 7, 1, 12, 34, 56, tzinfo=timezone.utc),
        expected_commit=SOURCE_COMMIT,
        expected_archive_sha256=HASH_A,
        expected_manifest_sha256=HASH_B,
        host_root=tmp_path,
    )


def task8_write_policy_environment(
    context: object,
    raw: bytes,
) -> Path:
    path = host_root_path(context.host_root, TARGETS[-1])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    path.chmod(0o600)
    return path


def test_task8_policy_atomic_environment_replace_is_exact_and_durable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context = task8_policy_atomic_context(module, tmp_path)
    disabled = b"DATABASE_URL=postgresql://example\nREMOTE_PRUNE_ENABLED=0\n"
    enabled = module._task8_enabled_environment_bytes(disabled)
    path = task8_write_policy_environment(context, disabled)
    events: list[tuple[str, str]] = []

    def capture(event: str, **details: object) -> None:
        if event.startswith("task8_policy_"):
            events.append((event, str(details["action"])))

    monkeypatch.setattr(module, "_atomic_event_hook", capture)

    receipt = module._task8_atomic_replace_policy_environment(
        context,
        expected_previous=disabled,
        intended=enabled,
        action="enable",
    )

    assert path.read_bytes() == enabled
    if os.name == "posix":
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        assert path.stat().st_uid == context.effective_uid
    assert receipt["sha256"] == hashlib.sha256(enabled).hexdigest()
    assert receipt["mode"] == (0o600 if os.name == "posix" else receipt["mode"])
    assert events == [
        ("task8_policy_after_environment_temp_open", "enable"),
        ("task8_policy_after_environment_temp_fsync", "enable"),
        ("task8_policy_before_environment_replace", "enable"),
        ("task8_policy_after_environment_replace", "enable"),
        ("task8_policy_after_environment_parent_fsync", "enable"),
        ("task8_policy_after_environment_readback", "enable"),
    ]
    assert not host_root_path(
        context.host_root,
        "/etc/degen/.prod-db-backup.env.20260630T235959Z.policy-enable.tmp",
    ).exists()


def test_task8_policy_atomic_environment_replace_rechecks_target_after_hook(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context = task8_policy_atomic_context(module, tmp_path)
    disabled = b"REMOTE_PRUNE_ENABLED=0\n"
    enabled = module._task8_enabled_environment_bytes(disabled)
    path = task8_write_policy_environment(context, disabled)
    raced = b"REMOTE_PRUNE_ENABLED=0\nATTACKER=1\n"

    def race(event: str, **_details: object) -> None:
        if event == "task8_policy_before_environment_replace":
            path.write_bytes(raced)
            path.chmod(0o600)

    monkeypatch.setattr(module, "_atomic_event_hook", race)

    with pytest.raises(module.OperationStateError, match="environment.*changed|compare-and-swap"):
        module._task8_atomic_replace_policy_environment(
            context,
            expected_previous=disabled,
            intended=enabled,
            action="enable",
        )

    assert path.read_bytes() == raced
    assert not host_root_path(
        context.host_root,
        "/etc/degen/.prod-db-backup.env.20260630T235959Z.policy-enable.tmp",
    ).exists()


def test_task8_policy_atomic_environment_replace_never_installs_swapped_temp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context = task8_policy_atomic_context(module, tmp_path)
    disabled = b"REMOTE_PRUNE_ENABLED=0\n"
    enabled = module._task8_enabled_environment_bytes(disabled)
    path = task8_write_policy_environment(context, disabled)
    attacker = b"REMOTE_PRUNE_ENABLED=1\nATTACKER=1\n"
    temp = host_root_path(
        context.host_root,
        "/etc/degen/.prod-db-backup.env.20260630T235959Z.policy-enable.tmp",
    )

    def race(event: str, **_details: object) -> None:
        if event == "task8_policy_before_environment_replace":
            stolen = temp.with_name("stolen-policy-temp")
            temp.replace(stolen)
            temp.write_bytes(attacker)
            temp.chmod(0o600)

    monkeypatch.setattr(module, "_atomic_event_hook", race)

    with pytest.raises(
        module.OperationStateError,
        match="temporary.*binding|temporary.*bytes|temporary.*size",
    ):
        module._task8_atomic_replace_policy_environment(
            context,
            expected_previous=disabled,
            intended=enabled,
            action="enable",
        )

    assert path.read_bytes() == disabled
    assert temp.read_bytes() == attacker


def test_task8_policy_atomic_environment_rejects_same_byte_target_inode_swap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if os.name != "posix":
        pytest.skip("Windows denies replacement while the held proof is open")
    module = load_ops_helper()
    context = task8_policy_atomic_context(module, tmp_path)
    disabled = b"REMOTE_PRUNE_ENABLED=0\n"
    enabled = module._task8_enabled_environment_bytes(disabled)
    path = task8_write_policy_environment(context, disabled)
    original_inode = path.stat().st_ino

    def race(event: str, **_details: object) -> None:
        if event == "task8_policy_before_environment_replace":
            replacement = path.with_name("replacement-policy-env")
            replacement.write_bytes(disabled)
            replacement.chmod(0o600)
            os.replace(replacement, path)

    monkeypatch.setattr(module, "_atomic_event_hook", race)

    with pytest.raises(module.OperationStateError, match="compare-and-swap|binding|changed"):
        module._task8_atomic_replace_policy_environment(
            context,
            expected_previous=disabled,
            intended=enabled,
            action="enable",
        )

    assert path.read_bytes() == disabled
    assert path.stat().st_ino != original_inode


def test_task8_policy_atomic_environment_rejects_same_inode_temp_overwrite(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context = task8_policy_atomic_context(module, tmp_path)
    disabled = b"REMOTE_PRUNE_ENABLED=0\n"
    enabled = module._task8_enabled_environment_bytes(disabled)
    attacker = b"REMOTE_PRUNE_ENABLED=X\n"
    assert len(attacker) == len(enabled)
    path = task8_write_policy_environment(context, disabled)
    temp = host_root_path(
        context.host_root,
        "/etc/degen/.prod-db-backup.env.20260630T235959Z.policy-enable.tmp",
    )

    def race(event: str, **_details: object) -> None:
        if event == "task8_policy_before_environment_replace":
            metadata = temp.stat()
            temp.write_bytes(attacker)
            temp.chmod(0o600)
            os.utime(temp, ns=(metadata.st_atime_ns, metadata.st_mtime_ns))

    monkeypatch.setattr(module, "_atomic_event_hook", race)

    with pytest.raises(module.OperationStateError, match="temporary.*bytes|temporary.*binding"):
        module._task8_atomic_replace_policy_environment(
            context,
            expected_previous=disabled,
            intended=enabled,
            action="enable",
        )

    assert path.read_bytes() == disabled


class Task8PolicyAtomicProcessDeath(BaseException):
    pass


def test_task8_policy_atomic_ordinary_failure_cleans_only_owned_temp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context = task8_policy_atomic_context(module, tmp_path)
    disabled = b"REMOTE_PRUNE_ENABLED=0\n"
    enabled = module._task8_enabled_environment_bytes(disabled)
    path = task8_write_policy_environment(context, disabled)
    temp = host_root_path(
        context.host_root,
        "/etc/degen/.prod-db-backup.env.20260630T235959Z.policy-enable.tmp",
    )

    def fail(event: str, **_details: object) -> None:
        if event == "task8_policy_after_environment_temp_fsync":
            raise RuntimeError("controlled ordinary failure")

    monkeypatch.setattr(module, "_atomic_event_hook", fail)

    with pytest.raises(RuntimeError, match="controlled ordinary"):
        module._task8_atomic_replace_policy_environment(
            context,
            expected_previous=disabled,
            intended=enabled,
            action="enable",
        )

    assert path.read_bytes() == disabled
    assert not temp.exists()


def test_task8_policy_atomic_process_death_leaves_exact_temp_for_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context = task8_policy_atomic_context(module, tmp_path)
    disabled = b"REMOTE_PRUNE_ENABLED=0\n"
    enabled = module._task8_enabled_environment_bytes(disabled)
    path = task8_write_policy_environment(context, disabled)
    temp = host_root_path(
        context.host_root,
        "/etc/degen/.prod-db-backup.env.20260630T235959Z.policy-enable.tmp",
    )

    def die(event: str, **_details: object) -> None:
        if event == "task8_policy_after_environment_temp_fsync":
            raise Task8PolicyAtomicProcessDeath("controlled process death")

    monkeypatch.setattr(module, "_atomic_event_hook", die)

    with pytest.raises(Task8PolicyAtomicProcessDeath, match="controlled process death"):
        module._task8_atomic_replace_policy_environment(
            context,
            expected_previous=disabled,
            intended=enabled,
            action="enable",
        )

    assert path.read_bytes() == disabled
    assert temp.read_bytes() == enabled
    monkeypatch.setattr(module, "_atomic_event_hook", lambda *_args, **_kwargs: None)
    module._task8_cleanup_policy_environment_temp(
        context,
        expected=enabled,
        action="enable",
    )
    assert not temp.exists()


def test_task8_policy_atomic_process_death_after_temp_open_leaves_recoverable_temp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context = task8_policy_atomic_context(module, tmp_path)
    disabled = b"REMOTE_PRUNE_ENABLED=0\n"
    enabled = module._task8_enabled_environment_bytes(disabled)
    path = task8_write_policy_environment(context, disabled)
    temp = host_root_path(
        context.host_root,
        "/etc/degen/.prod-db-backup.env.20260630T235959Z.policy-enable.tmp",
    )

    def die(event: str, **_details: object) -> None:
        if event == "task8_policy_after_environment_temp_open":
            raise Task8PolicyAtomicProcessDeath("controlled process death after open")

    monkeypatch.setattr(module, "_atomic_event_hook", die)

    with pytest.raises(Task8PolicyAtomicProcessDeath, match="after open"):
        module._task8_atomic_replace_policy_environment(
            context,
            expected_previous=disabled,
            intended=enabled,
            action="enable",
        )

    assert path.read_bytes() == disabled
    assert temp.read_bytes() == b""
    monkeypatch.setattr(module, "_atomic_event_hook", lambda *_args, **_kwargs: None)
    module._task8_cleanup_policy_environment_temp(
        context,
        expected=enabled,
        action="enable",
    )
    assert not temp.exists()


def test_task8_policy_cleanup_rejects_oversized_temp_before_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    context = task8_policy_atomic_context(module, tmp_path)
    enabled = b"REMOTE_PRUNE_ENABLED=1\n"
    temp = host_root_path(
        context.host_root,
        "/etc/degen/.prod-db-backup.env.20260630T235959Z.policy-enable.tmp",
    )
    temp.parent.mkdir(parents=True, exist_ok=True)
    temp.write_bytes(enabled + b"x")
    temp.chmod(0o600)

    def forbidden_read(*_args: object, **_kwargs: object) -> bytes:
        raise AssertionError("oversized policy temp must be rejected before allocation")

    monkeypatch.setattr(module, "_read_exact_descriptor", forbidden_read)

    with pytest.raises(module.OperationStateError, match="temporary.*size"):
        module._task8_cleanup_policy_environment_temp(
            context,
            expected=enabled,
            action="enable",
        )

    assert temp.exists()


def test_task8_policy_cleanup_fsyncs_parent_when_temp_is_already_absent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if os.name != "posix":
        pytest.skip("directory fsync durability is a POSIX production invariant")
    module = load_ops_helper()
    context = task8_policy_atomic_context(module, tmp_path)
    task8_write_policy_environment(context, b"REMOTE_PRUNE_ENABLED=0\n")
    events: list[str] = []
    monkeypatch.setattr(
        module,
        "_atomic_event_hook",
        lambda event, **_details: events.append(event),
    )

    module._task8_cleanup_policy_environment_temp(
        context,
        expected=b"REMOTE_PRUNE_ENABLED=1\n",
        action="enable",
    )

    assert "task8_policy_after_absent_temp_parent_fsync" in events


def test_task8_policy_recovery_durably_adopts_restore_visible_after_rename_death(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if os.name != "posix":
        pytest.skip("directory fsync durability is a POSIX production invariant")
    module = load_ops_helper()
    context = task8_policy_atomic_context(module, tmp_path)
    disabled = b"REMOTE_PRUNE_ENABLED=0\n"
    enabled = module._task8_enabled_environment_bytes(disabled)
    path = task8_write_policy_environment(context, enabled)

    def die(event: str, **details: object) -> None:
        if (
            event == "task8_policy_after_environment_replace"
            and details.get("action") == "restore"
        ):
            raise Task8PolicyAtomicProcessDeath("after restore rename")

    monkeypatch.setattr(module, "_atomic_event_hook", die)
    with pytest.raises(Task8PolicyAtomicProcessDeath, match="restore rename"):
        module._task8_atomic_replace_policy_environment(
            context,
            expected_previous=enabled,
            intended=disabled,
            action="restore",
        )
    assert path.read_bytes() == disabled

    fresh = load_ops_helper()
    events: list[str] = []
    monkeypatch.setattr(
        fresh,
        "_atomic_event_hook",
        lambda event, **_details: (
            events.append(event)
            if event.startswith("task8_policy_")
            else None
        ),
    )
    fresh._task8_durably_adopt_policy_environment(
        context,
        expected=disabled,
        revalidate=lambda: None,
    )

    assert path.read_bytes() == disabled
    assert events == [
        "task8_policy_before_environment_adoption_parent_fsync",
        "task8_policy_after_environment_adoption_parent_fsync",
    ]


@pytest.mark.parametrize(
    "crash_event",
    [
        "task8_policy_after_environment_replace",
        "task8_policy_after_environment_parent_fsync",
    ],
)
def test_task8_policy_fresh_recovery_restores_after_enablement_fsync_boundaries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crash_event: str,
) -> None:
    if os.name != "posix":
        pytest.skip("real rename/directory-fsync recovery is a POSIX invariant")
    module = load_ops_helper()
    context = task8_policy_atomic_context(module, tmp_path)
    disabled = b"REMOTE_PRUNE_ENABLED=0\n"
    enabled = module._task8_enabled_environment_bytes(disabled)
    path = task8_write_policy_environment(context, disabled)

    def die(event: str, **details: object) -> None:
        if event == crash_event and details.get("action") == "enable":
            raise Task8PolicyAtomicProcessDeath(crash_event)

    monkeypatch.setattr(module, "_atomic_event_hook", die)
    with pytest.raises(Task8PolicyAtomicProcessDeath, match=crash_event):
        module._task8_atomic_replace_policy_environment(
            context,
            expected_previous=disabled,
            intended=enabled,
            action="enable",
        )
    assert path.read_bytes() == enabled

    fresh = load_ops_helper()
    monkeypatch.setattr(fresh, "_atomic_event_hook", lambda *_args, **_kwargs: None)
    fresh._task8_atomic_replace_policy_environment(
        context,
        expected_previous=enabled,
        intended=disabled,
        action="restore",
    )
    fresh._task8_durably_adopt_policy_environment(
        context,
        expected=disabled,
        revalidate=lambda: None,
    )

    assert path.read_bytes() == disabled


def test_task8_policy_atomic_restore_uses_distinct_temp_namespace(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    context = task8_policy_atomic_context(module, tmp_path)
    disabled = b"REMOTE_PRUNE_ENABLED=0\n"
    enabled = module._task8_enabled_environment_bytes(disabled)
    path = task8_write_policy_environment(context, enabled)

    receipt = module._task8_atomic_replace_policy_environment(
        context,
        expected_previous=enabled,
        intended=disabled,
        action="restore",
    )

    assert path.read_bytes() == disabled
    assert receipt["sha256"] == hashlib.sha256(disabled).hexdigest()
    assert not host_root_path(
        context.host_root,
        "/etc/degen/.prod-db-backup.env.20260630T235959Z.policy-restore.tmp",
    ).exists()


def test_task8_policy_atomic_rejects_and_preserves_preexisting_temp(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    context = task8_policy_atomic_context(module, tmp_path)
    disabled = b"REMOTE_PRUNE_ENABLED=0\n"
    enabled = module._task8_enabled_environment_bytes(disabled)
    path = task8_write_policy_environment(context, disabled)
    temp = host_root_path(
        context.host_root,
        "/etc/degen/.prod-db-backup.env.20260630T235959Z.policy-enable.tmp",
    )
    temp.write_bytes(enabled)
    temp.chmod(0o600)

    with pytest.raises(module.OperationStateError, match="temporary already exists"):
        module._task8_atomic_replace_policy_environment(
            context,
            expected_previous=disabled,
            intended=enabled,
            action="enable",
        )

    assert path.read_bytes() == disabled
    assert temp.read_bytes() == enabled


def test_task8_policy_cleanup_removes_only_exact_owned_temp(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    context = task8_policy_atomic_context(module, tmp_path)
    enabled = b"REMOTE_PRUNE_ENABLED=1\n"
    temp = host_root_path(
        context.host_root,
        "/etc/degen/.prod-db-backup.env.20260630T235959Z.policy-enable.tmp",
    )
    temp.parent.mkdir(parents=True, exist_ok=True)
    temp.write_bytes(enabled)
    temp.chmod(0o600)

    module._task8_cleanup_policy_environment_temp(
        context,
        expected=enabled,
        action="enable",
    )

    assert not temp.exists()

    temp.write_bytes(b"attacker-owned\n")
    temp.chmod(0o600)
    with pytest.raises(module.OperationStateError, match="temporary.*unexpected bytes"):
        module._task8_cleanup_policy_environment_temp(
            context,
            expected=enabled,
            action="enable",
        )
    assert temp.read_bytes() == b"attacker-owned\n"


def test_task8_policy_immutable_proofs_survive_only_environment_replace(
    tmp_path: Path,
) -> None:
    if os.name != "posix":
        pytest.skip("descriptor identity proof requires POSIX")
    module = load_ops_helper()
    context = task8_policy_atomic_context(module, tmp_path)
    expected: dict[str, bytes] = {}
    for target in TARGETS:
        raw = f"installed fixture for {target}\n".encode("ascii")
        expected[target] = raw
        path = host_root_path(context.host_root, target)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        path.chmod(0o600 if target == TARGETS[-1] else module._task7_install_mode(target))
    state = state_at_phase(context.paths.operation_dir, "dry_run_recorded")
    state["install"]["installed_hashes"] = {
        target: hashlib.sha256(raw).hexdigest()
        for target, raw in expected.items()
    }
    disabled = expected[TARGETS[-1]] + b"REMOTE_PRUNE_ENABLED=0\n"
    enabled = module._task8_enabled_environment_bytes(disabled)
    task8_write_policy_environment(context, disabled)
    expected[TARGETS[-1]] = disabled
    state["install"]["installed_hashes"][TARGETS[-1]] = hashlib.sha256(disabled).hexdigest()

    with module._open_task8_policy_immutable_target_proofs(context) as proofs:
        module._task8_validate_live_installed_immutable_target_proofs(
            state,
            expected,
            proofs,
        )
        module._task8_atomic_replace_policy_environment(
            context,
            expected_previous=disabled,
            intended=enabled,
            action="enable",
        )
        module._task8_validate_live_installed_immutable_target_proofs(
            state,
            expected,
            proofs,
        )


def task8_install_normal_policy_harness(
    module: object,
    monkeypatch: pytest.MonkeyPatch,
    operation_dir: Path,
) -> tuple[object, dict[str, object]]:
    context, harness = task8_install_normal_dry_run_harness(
        module,
        monkeypatch,
        operation_dir,
        initial_phase="dry_run_recorded",
    )
    state = harness["holder"]["state"]
    disabled = b"DATABASE_URL=postgresql://example\nREMOTE_PRUNE_ENABLED=0\n"
    enabled = module._task8_enabled_environment_bytes(disabled)
    disabled_sha256 = hashlib.sha256(disabled).hexdigest()
    enabled_sha256 = hashlib.sha256(enabled).hexdigest()
    state["host_stage"]["environment_sha256"] = disabled_sha256
    state["host_stage"]["enabled_environment_sha256"] = enabled_sha256
    state["install"]["installed_hashes"][TARGETS[-1]] = disabled_sha256
    harness["expected_install_bytes"][TARGETS[-1]] = disabled
    harness["expected_install_hashes"][TARGETS[-1]] = disabled_sha256
    dry_run_entry, _dry_run_completed = task8_dry_run_completion_pair(state)
    dry_run_entry["evidence_sha256"] = task8_dry_run_entry_evidence(
        state,
        dry_run_entry,
    )
    task8_reseal_dry_run_state(state)
    module.validate_operation_state(state, operation_dir)

    controls = harness["controls"]
    controls.update(
        {
            "policy_inventory_output": task8_normal_dry_run_inventory_output(
                list(state["dry_run"]["inventory_names"])
            ),
            "policy_environment": disabled,
            "policy_atomic_calls": [],
            "policy_durable_adoptions": [],
            "policy_immutable_events": [],
            "policy_atomic_process_death_after_apply": False,
            "policy_atomic_error_after_apply": False,
            "policy_command_process_death": False,
            "policy_receipt_pre_replace_hook": None,
        }
    )

    @contextlib.contextmanager
    def open_immutable(_context: object):
        controls["policy_immutable_events"].append("open")
        proofs = {target: object() for target in TARGETS[:-1]}
        try:
            yield proofs
        finally:
            controls["policy_immutable_events"].append("close")

    def validate_immutable(
        proof_state: dict[str, object],
        expected_bytes: dict[str, bytes],
        proofs: dict[str, object],
    ) -> None:
        assert tuple(expected_bytes) == TARGETS
        assert tuple(proofs) == TARGETS[:-1]
        assert proof_state["install"]["installed_hashes"] == harness[
            "expected_install_hashes"
        ]
        controls["policy_immutable_events"].append(
            "validate:" + str(proof_state["phase"])
        )

    def atomic_replace(
        _context: object,
        *,
        expected_previous: bytes,
        intended: bytes,
        action: str,
        revalidate=None,
    ) -> dict[str, object]:
        assert action in {"enable", "restore"}
        if action == "enable":
            assert expected_previous == disabled
            assert intended == enabled
            assert controls["policy_environment"] == disabled
        else:
            assert expected_previous == enabled
            assert intended == disabled
            assert controls["policy_environment"] == enabled
        current = harness["holder"]["state"]
        guard = current["active_transaction"]["guard"]
        if action == "enable":
            assert guard == {
                "timer_stopped": True,
                "service_inactive_verified": True,
                "legacy_lock_acquired": True,
                "runtime_lock_acquired": True,
                "locks_released": False,
                "timer_restored": False,
            }
            assert current["policy"] is None
        else:
            assert guard["timer_stopped"] is True
            assert guard["service_inactive_verified"] is True
            assert guard["runtime_lock_acquired"] is True
            assert guard["timer_restored"] is False
        if revalidate is not None:
            revalidate()
        controls["policy_environment"] = intended
        controls["policy_atomic_calls"].append(
            {"action": action, "state": copy.deepcopy(current)}
        )
        harness["timeline"].append("atomic:policy-" + action)
        if controls["policy_atomic_process_death_after_apply"]:
            controls["policy_atomic_process_death_after_apply"] = False
            raise Task8GuardProcessDeath("policy atomic process death")
        if controls["policy_atomic_error_after_apply"]:
            controls["policy_atomic_error_after_apply"] = False
            raise module.OperationStateError("controlled policy atomic error")
        if revalidate is not None:
            revalidate()
        return {
            "present": True,
            "sha256": hashlib.sha256(intended).hexdigest(),
            "mode": 0o600,
            "uid": 0,
            "gid": 0,
        }

    def require_environment(_context: object, expected: bytes) -> dict[str, object]:
        if controls["policy_environment"] != expected:
            raise module.OperationStateError(
                "controlled policy environment changed from exact provenance"
            )
        return {
            "present": True,
            "sha256": hashlib.sha256(expected).hexdigest(),
            "mode": 0o600,
            "uid": 0,
            "gid": 0,
        }

    monkeypatch.setattr(
        module,
        "_open_task8_policy_immutable_target_proofs",
        open_immutable,
    )
    monkeypatch.setattr(
        module,
        "_task8_validate_live_installed_immutable_target_proofs",
        validate_immutable,
    )
    monkeypatch.setattr(
        module,
        "_task8_atomic_replace_policy_environment",
        atomic_replace,
    )
    monkeypatch.setattr(
        module,
        "_task8_require_exact_policy_environment",
        require_environment,
    )
    monkeypatch.setattr(
        module,
        "_task8_cleanup_policy_environment_temp",
        lambda *_args, **_kwargs: None,
    )

    def durably_adopt(
        _context: object,
        *,
        expected: bytes,
        revalidate,
    ) -> None:
        revalidate()
        require_environment(_context, expected)
        controls["policy_durable_adoptions"].append(expected)
        revalidate()

    monkeypatch.setattr(
        module,
        "_task8_durably_adopt_policy_environment",
        durably_adopt,
        raising=False,
    )

    def capture_allowed(
        _context: object,
        allowed: tuple[bytes, ...],
    ) -> tuple[bytes, dict[str, object]]:
        live = controls["policy_environment"]
        if live not in allowed:
            raise module.OperationStateError(
                "live policy environment has an unauthorized digest"
            )
        return live, {
            "present": True,
            "sha256": hashlib.sha256(live).hexdigest(),
            "mode": 0o600,
            "uid": 0,
            "gid": 0,
        }

    monkeypatch.setattr(
        module,
        "_task8_capture_allowed_policy_environment",
        capture_allowed,
    )

    command_calls: list[tuple[tuple[str, ...], tuple[int, ...]]] = []

    def command_runner(
        argv: object,
        pass_fds: tuple[int, ...],
    ) -> subprocess.CompletedProcess[str]:
        argv_tuple = tuple(str(value) for value in argv)
        current = harness["holder"]["state"]
        pending = current["rclone_evidence_groups"][-1]
        assert pending["purpose"] == "enable-prune-inventory"
        assert pending["after"] is None
        assert argv_tuple == task8_normal_dry_run_inventory_argv()
        assert pass_fds == (harness["fresh_acquisitions"][-1][1],)
        command_calls.append((argv_tuple, pass_fds))
        harness["timeline"].append("command:enable-prune-inventory")
        if controls["policy_command_process_death"]:
            controls["policy_command_process_death"] = False
            raise Task8GuardProcessDeath("policy inventory process death")
        return subprocess.CompletedProcess(
            argv_tuple,
            0,
            str(controls["policy_inventory_output"]),
            "",
        )

    context.command_runner = command_runner
    harness.update(
        {
            "disabled_environment": disabled,
            "enabled_environment": enabled,
            "policy_command_calls": command_calls,
        }
    )
    return context, harness


def test_task8_enable_prune_commits_applied_receipt_before_runtime_restore(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_policy_harness(
        module,
        monkeypatch,
        operation_dir,
    )

    result = module.enable_remote_prune(context)

    assert result["phase"] == "policy_enabled"
    assert result["active_transaction"] is None
    assert result["policy"] is not None
    assert result["policy"]["environment_sha256"] == hashlib.sha256(
        harness["enabled_environment"]
    ).hexdigest()
    policy_entry = next(
        item
        for item in reversed(result["phase_history"])
        if item["phase"] == "policy_enabling"
    )
    assert result["policy"]["enabled_epoch"] > policy_entry["epoch"]
    assert result["policy"]["applied_target"]["mode"] == 0o600
    assert result["failure"] is None
    assert harness["controls"]["policy_environment"] == harness[
        "enabled_environment"
    ]
    assert [
        call["action"] for call in harness["controls"]["policy_atomic_calls"]
    ] == ["enable"]
    assert len(harness["policy_command_calls"]) == 1
    module.validate_operation_state(result, operation_dir)

    applied_write_index = next(
        index
        for index, item in enumerate(harness["timeline"])
        if item == "write:policy_enabling"
        and harness["writes"][
            sum(1 for prior in harness["timeline"][: index + 1] if prior.startswith("write:")) - 1
        ]["policy"]
        is not None
    )
    assert applied_write_index < harness["timeline"].index(
        "action:runtime_lock_release"
    )
    assert applied_write_index < harness["timeline"].index("action:timer_start")
    assert harness["external_actions"] == [
        "timer_disable",
        "timer_stop",
        "quiesce_service_check",
        "quiesce_runtime_readback",
        "legacy_lock_acquire",
        "runtime_lock_acquire",
        "post_lock_service_recheck",
        "runtime_lock_release",
        "legacy_lock_release",
        "pre_restore_service_check",
        "timer_enable",
        "timer_start",
        "restore_runtime_readback",
    ]


def test_task8_policy_completion_epoch_survives_backward_clock_step(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, _harness = task8_install_normal_policy_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    original_record = module._task8_record_applied_policy

    def record_then_rewind(*args: object, **kwargs: object) -> dict[str, object]:
        result = original_record(*args, **kwargs)
        context.clock = lambda: datetime.fromtimestamp(1, tz=timezone.utc)
        return result

    monkeypatch.setattr(module, "_task8_record_applied_policy", record_then_rewind)

    result = module.enable_remote_prune(context)

    completed = next(
        item
        for item in reversed(result["phase_history"])
        if item["phase"] == "policy_enabled"
    )
    assert completed["epoch"] >= result["policy"]["enabled_epoch"]
    module.validate_operation_state(result, operation_dir)


def test_task8_visible_applied_receipt_is_force_checkpointed_after_write_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    expected = task8_provisional_policy_state(operation_dir)
    holder: dict[str, object] = {"state": copy.deepcopy(expected)}
    calls: list[bool] = []
    validations: list[str] = []

    def write_state(
        _context: object,
        _binding: object,
        candidate: dict[str, object],
        *,
        pre_replace_validator=None,
        force_checkpoint: bool = False,
    ) -> None:
        calls.append(force_checkpoint)
        if pre_replace_validator is not None:
            pre_replace_validator()
        holder["state"] = copy.deepcopy(candidate)
        if len(calls) == 1:
            raise OSError("controlled parent fsync failure after visible rename")

    monkeypatch.setattr(module, "_task7_write_state", write_state)
    monkeypatch.setattr(
        module,
        "_task7_load_state",
        lambda _context: copy.deepcopy(holder["state"]),
    )

    module._task7_write_required_receipt_state(
        types.SimpleNamespace(),
        object(),
        expected,
        "applied policy",
        pre_replace_validator=lambda: validations.append("validated"),
    )

    assert calls == [False, True]
    assert validations == ["validated", "validated"]


def test_task8_applied_receipt_recheckpoints_after_visible_atomic_replace_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, uid = private_operation_dir(tmp_path)
    expected = task8_provisional_policy_state(operation_dir)
    policy = expected["policy"]
    entry = expected["phase_history"][-1]
    group = expected["rclone_evidence_groups"][-1]
    policy["applied_target"]["uid"] = uid
    policy["applied_target"]["gid"] = os.getegid() if os.name == "posix" else 0
    policy["applied_evidence_sha256"] = task8_policy_applied_evidence(
        expected,
        entry,
        policy,
        group,
    )
    previous = copy.deepcopy(expected)
    previous["policy"] = None
    module.validate_operation_state(previous, operation_dir)
    write_state_file(operation_dir, previous)
    reviewed = expected["reviewed_source"]
    context = module.OperationsContext(
        operation_id=operation_dir.name,
        paths=module.build_operation_paths(operation_dir),
        effective_uid=uid,
        command_runner=lambda argv, pass_fds: subprocess.CompletedProcess(
            tuple(str(value) for value in argv), 0, "", ""
        ),
        clock=lambda: datetime(2026, 7, 1, 12, 34, 56, tzinfo=timezone.utc),
        expected_commit=reviewed["commit"],
        expected_archive_sha256=reviewed["archive_sha256"],
        expected_manifest_sha256=reviewed["manifest_sha256"],
        host_root=tmp_path,
    )
    events: list[str] = []
    failed = False

    def fail_once(event: str, **_details: object) -> None:
        nonlocal failed
        events.append(event)
        if event == "after_replace" and not failed:
            failed = True
            raise OSError("controlled failure before parent fsync")

    monkeypatch.setattr(module, "_atomic_event_hook", fail_once)
    validations: list[str] = []
    with module._open_operation_transaction(context) as binding:
        module._task7_write_required_receipt_state(
            context,
            binding,
            expected,
            "applied policy",
            pre_replace_validator=lambda: validations.append("validated"),
        )

    assert module._task7_load_state(context) == expected
    assert events.count("after_replace") == 2
    assert events.count("after_parent_fsync") == 1
    assert validations == ["validated", "validated"]


def test_task8_applied_policy_receipt_revalidates_environment_at_state_replace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_policy_harness(
        module,
        monkeypatch,
        operation_dir,
    )

    def drift(candidate: dict[str, object]) -> bool:
        if candidate["phase"] != "policy_enabling" or candidate["policy"] is None:
            return False
        harness["controls"]["policy_environment"] = b"REMOTE_PRUNE_ENABLED=drift\n"
        return True

    harness["controls"]["policy_receipt_pre_replace_hook"] = drift

    with pytest.raises(module.OperationStateError):
        module.enable_remote_prune(context)

    durable = harness["holder"]["state"]
    assert durable["phase"] != "policy_enabled"
    assert durable["policy"] is None


def test_task8_terminal_policy_receipt_revalidates_environment_at_state_replace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_policy_harness(
        module,
        monkeypatch,
        operation_dir,
    )

    def drift(candidate: dict[str, object]) -> bool:
        if candidate["phase"] != "policy_enabled":
            return False
        harness["controls"]["policy_environment"] = b"REMOTE_PRUNE_ENABLED=drift\n"
        return True

    harness["controls"]["policy_receipt_pre_replace_hook"] = drift

    with pytest.raises(module.OperationStateError):
        module.enable_remote_prune(context)

    durable = harness["holder"]["state"]
    assert durable["phase"] != "policy_enabled"
    assert durable["policy"] is not None


def test_task8_policy_recovery_rolls_back_only_before_applied_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_policy_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    harness["controls"]["policy_atomic_process_death_after_apply"] = True

    with pytest.raises(Task8GuardProcessDeath, match="policy atomic"):
        module.enable_remote_prune(context)

    interrupted = harness["holder"]["state"]
    assert interrupted["phase"] == "policy_enabling"
    assert interrupted["policy"] is None
    assert harness["controls"]["policy_environment"] == harness[
        "enabled_environment"
    ]

    proof_count_before_recovery = len(harness["proof_routes"])
    recovered = module.recover_host_configuration(context)

    assert recovered["phase"] == "installed"
    assert recovered["policy"] is None
    assert recovered["dry_run"] is None
    assert recovered["probe"] is None
    assert recovered["active_transaction"] is None
    assert recovered["recovery"]["kind"] == "policy"
    assert recovered["recovery"]["next_target_index"] == 1
    assert recovered["recovery"]["completed_epoch"] is not None
    assert harness["controls"]["policy_environment"] == harness[
        "disabled_environment"
    ]
    assert [
        call["action"] for call in harness["controls"]["policy_atomic_calls"]
    ] == ["enable", "restore"]
    assert harness["controls"]["policy_durable_adoptions"] == [
        harness["disabled_environment"]
    ]
    assert len(harness["fresh_acquisitions"]) == 2
    assert harness["live_runtime"] == harness["baseline"]
    assert set(harness["proof_routes"][proof_count_before_recovery:]) == {"source"}
    module.validate_operation_state(recovered, operation_dir)


def test_task8_policy_recovery_accepts_exact_terminal_receipt_after_late_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_policy_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    harness["controls"]["policy_atomic_process_death_after_apply"] = True
    with pytest.raises(Task8GuardProcessDeath):
        module.enable_remote_prune(context)

    original = module._task8_complete_policy_rollback

    def complete_then_fail(*args: object, **kwargs: object) -> dict[str, object]:
        result = original(*args, **kwargs)
        raise module.OperationStateError("controlled error after terminal receipt")

    monkeypatch.setattr(module, "_task8_complete_policy_rollback", complete_then_fail)

    recovered = module.recover_host_configuration(context)

    assert recovered["phase"] == "installed"
    assert recovered["policy"] is None
    assert recovered["active_transaction"] is None
    assert recovered["recovery"]["completed_epoch"] is not None
    assert harness["controls"]["policy_environment"] == harness[
        "disabled_environment"
    ]
    module.validate_operation_state(recovered, operation_dir)


@pytest.mark.parametrize("race_receipt", ["cursor", "terminal"])
def test_task8_policy_recovery_receipts_revalidate_environment_at_state_replace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    race_receipt: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_policy_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    controls = harness["controls"]
    controls["policy_atomic_process_death_after_apply"] = True
    with pytest.raises(Task8GuardProcessDeath):
        module.enable_remote_prune(context)

    def drift(candidate: dict[str, object]) -> bool:
        recovery = candidate.get("recovery")
        if not isinstance(recovery, dict):
            return False
        is_cursor = (
            candidate["phase"] == "recovering_policy"
            and recovery["next_target_index"] == 1
        )
        is_terminal = candidate["phase"] == "installed"
        if (race_receipt == "cursor" and not is_cursor) or (
            race_receipt == "terminal" and not is_terminal
        ):
            return False
        controls["policy_environment"] = b"REMOTE_PRUNE_ENABLED=drift\n"
        return True

    controls["policy_receipt_pre_replace_hook"] = drift

    with pytest.raises(module.OperationStateError):
        module.recover_host_configuration(context)

    durable = harness["holder"]["state"]
    assert durable["phase"] != "installed"
    assert durable["recovery"]["completed_epoch"] is None
    if race_receipt == "cursor":
        assert durable["recovery"]["next_target_index"] == 0


def test_task8_enable_prune_ordinary_precommit_failure_recovers_automatically(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_policy_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    harness["controls"]["policy_atomic_error_after_apply"] = True

    with pytest.raises(module.OperationStateError, match="controlled policy atomic"):
        module.enable_remote_prune(context)

    recovered = harness["holder"]["state"]
    assert recovered["phase"] == "installed"
    assert recovered["policy"] is None
    assert recovered["active_transaction"] is None
    assert recovered["recovery"]["kind"] == "policy"
    assert recovered["recovery"]["completed_epoch"] is not None
    assert harness["controls"]["policy_environment"] == harness[
        "disabled_environment"
    ]
    assert harness["live_runtime"] == harness["baseline"]
    assert harness["proof_routes"]
    assert set(harness["proof_routes"]) == {"installed"}
    module.validate_operation_state(recovered, operation_dir)


def test_task8_enable_prune_ordinary_postcommit_failure_forward_recovers_automatically(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_policy_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    original = module._task8_record_applied_policy

    def record_then_fail(*args: object, **kwargs: object) -> dict[str, object]:
        original(*args, **kwargs)
        raise module.OperationStateError("controlled ordinary postcommit failure")

    monkeypatch.setattr(module, "_task8_record_applied_policy", record_then_fail)

    recovered = module.enable_remote_prune(context)

    assert recovered["phase"] == "policy_enabled"
    assert recovered["policy"] is not None
    assert recovered["recovery"]["kind"] == "policy"
    assert recovered["recovery"]["completed_epoch"] is not None
    assert harness["controls"]["policy_environment"] == harness[
        "enabled_environment"
    ]
    assert not any(
        call["action"] == "restore"
        for call in harness["controls"]["policy_atomic_calls"]
    )
    assert set(harness["proof_routes"]) == {"installed"}
    module.validate_operation_state(recovered, operation_dir)


def test_task8_policy_recovery_forward_completes_after_applied_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_policy_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    original_record = module._task8_record_applied_policy

    def record_then_die(*args: object, **kwargs: object) -> dict[str, object]:
        result = original_record(*args, **kwargs)
        raise Task8GuardProcessDeath("after applied policy receipt")

    monkeypatch.setattr(module, "_task8_record_applied_policy", record_then_die)

    with pytest.raises(Task8GuardProcessDeath, match="applied policy receipt"):
        module.enable_remote_prune(context)

    interrupted = harness["holder"]["state"]
    assert interrupted["phase"] == "policy_enabling"
    assert interrupted["policy"] is not None
    committed_receipt = copy.deepcopy(interrupted["policy"])
    assert harness["controls"]["policy_environment"] == harness[
        "enabled_environment"
    ]
    context.clock = lambda: datetime.fromtimestamp(1, tz=timezone.utc)

    recovered = module.recover_host_configuration(context)

    assert recovered["phase"] == "policy_enabled"
    assert recovered["policy"] == committed_receipt
    assert recovered["dry_run"] is not None
    assert recovered["active_transaction"] is None
    assert recovered["recovery"]["kind"] == "policy"
    assert recovered["recovery"]["completed_epoch"] is not None
    assert recovered["recovery"]["started_epoch"] >= committed_receipt[
        "enabled_epoch"
    ]
    assert harness["controls"]["policy_environment"] == harness[
        "enabled_environment"
    ]
    assert [
        call["action"] for call in harness["controls"]["policy_atomic_calls"]
    ] == ["enable"]
    assert harness["controls"]["policy_durable_adoptions"] == [
        harness["enabled_environment"]
    ]
    assert len(harness["fresh_acquisitions"]) == 2
    assert harness["live_runtime"] == harness["baseline"]
    module.validate_operation_state(recovered, operation_dir)

    tampered = copy.deepcopy(recovered)
    tampered["recovery"]["evidence_sha256"] = HASH_D
    with pytest.raises(
        module.OperationStateError,
        match="policy recovery evidence|policy completion evidence",
    ):
        module.validate_operation_state(tampered, operation_dir)


@pytest.mark.parametrize(
    "crash_point",
    ["transaction-write", "timer-disable", "inventory-command", "environment-apply"],
)
def test_task8_policy_precommit_process_death_recovers_disabled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crash_point: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_policy_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    controls = harness["controls"]
    if crash_point == "transaction-write":
        controls["write_crash_after_phase"] = "policy_enabling"
    elif crash_point == "timer-disable":
        controls["crash_after"] = "timer_disable"
    elif crash_point == "inventory-command":
        controls["policy_command_process_death"] = True
    else:
        controls["policy_atomic_process_death_after_apply"] = True

    with pytest.raises(Task8GuardProcessDeath):
        module.enable_remote_prune(context)

    interrupted = harness["holder"]["state"]
    assert interrupted["phase"] == "policy_enabling"
    assert interrupted["policy"] is None
    controls["crash_after"] = None
    recovered = module.recover_host_configuration(context)

    assert recovered["phase"] == "installed"
    assert recovered["policy"] is None
    assert controls["policy_environment"] == harness["disabled_environment"]
    assert harness["live_runtime"] == harness["baseline"]
    module.validate_operation_state(recovered, operation_dir)


@pytest.mark.parametrize(
    "crash_point",
    ["applied-receipt", "runtime-lock-release", "timer-start", "terminal-write"],
)
def test_task8_policy_postcommit_process_death_never_rolls_back(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crash_point: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_policy_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    controls = harness["controls"]
    if crash_point == "applied-receipt":
        original = module._task8_record_applied_policy

        def record_then_die(*args: object, **kwargs: object) -> dict[str, object]:
            result = original(*args, **kwargs)
            raise Task8GuardProcessDeath("after applied receipt")

        monkeypatch.setattr(module, "_task8_record_applied_policy", record_then_die)
    elif crash_point == "runtime-lock-release":
        controls["crash_after"] = "runtime_lock_release"
    elif crash_point == "timer-start":
        controls["crash_after"] = "timer_start"
    else:
        controls["write_crash_after_phase"] = "policy_enabled"

    with pytest.raises(Task8GuardProcessDeath):
        module.enable_remote_prune(context)

    interrupted = harness["holder"]["state"]
    assert interrupted["policy"] is not None
    committed = copy.deepcopy(interrupted["policy"])
    controls["crash_after"] = None
    if interrupted["phase"] == "policy_enabled":
        recovered = interrupted
    else:
        recovered = module.recover_host_configuration(context)

    assert recovered["phase"] == "policy_enabled"
    assert recovered["policy"] == committed
    assert controls["policy_environment"] == harness["enabled_environment"]
    assert not any(
        call["action"] == "restore"
        for call in controls["policy_atomic_calls"]
    )
    assert harness["live_runtime"] == harness["baseline"]
    module.validate_operation_state(recovered, operation_dir)


@pytest.mark.parametrize(
    "crash_point",
    ["restore-atomic", "cursor-write", "runtime-lock-release", "timer-start", "terminal-write"],
)
def test_task8_policy_recovery_process_death_is_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crash_point: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_policy_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    controls = harness["controls"]
    controls["policy_atomic_process_death_after_apply"] = True
    with pytest.raises(Task8GuardProcessDeath):
        module.enable_remote_prune(context)
    assert controls["policy_environment"] == harness["enabled_environment"]

    original_advance = module._task8_advance_policy_recovery_cursor
    if crash_point == "restore-atomic":
        controls["policy_atomic_process_death_after_apply"] = True
    elif crash_point == "cursor-write":

        def advance_then_die(*args: object, **kwargs: object) -> dict[str, object]:
            result = original_advance(*args, **kwargs)
            raise Task8GuardProcessDeath("after policy recovery cursor")

        monkeypatch.setattr(
            module,
            "_task8_advance_policy_recovery_cursor",
            advance_then_die,
        )
    elif crash_point == "runtime-lock-release":
        controls["crash_after"] = "runtime_lock_release"
    elif crash_point == "timer-start":
        controls["crash_after"] = "timer_start"
    else:
        controls["write_crash_after_phase"] = "installed"

    with pytest.raises(Task8GuardProcessDeath):
        module.recover_host_configuration(context)

    interrupted = harness["holder"]["state"]
    controls["crash_after"] = None
    monkeypatch.setattr(
        module,
        "_task8_advance_policy_recovery_cursor",
        original_advance,
    )
    if interrupted["phase"] == "installed":
        recovered = interrupted
    else:
        recovered = module.recover_host_configuration(context)

    assert recovered["phase"] == "installed"
    assert recovered["policy"] is None
    assert controls["policy_environment"] == harness["disabled_environment"]
    assert harness["live_runtime"] == harness["baseline"]
    module.validate_operation_state(recovered, operation_dir)


@pytest.mark.parametrize(
    "crash_point",
    ["cursor-write", "runtime-lock-release", "timer-start", "terminal-write"],
)
def test_task8_committed_policy_recovery_process_death_is_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crash_point: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_policy_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    controls = harness["controls"]
    original_record = module._task8_record_applied_policy

    def record_then_die(*args: object, **kwargs: object) -> dict[str, object]:
        original_record(*args, **kwargs)
        raise Task8GuardProcessDeath("after applied policy receipt")

    monkeypatch.setattr(module, "_task8_record_applied_policy", record_then_die)
    with pytest.raises(Task8GuardProcessDeath):
        module.enable_remote_prune(context)
    monkeypatch.setattr(module, "_task8_record_applied_policy", original_record)
    assert controls["policy_environment"] == harness["enabled_environment"]
    proof_count_before_recovery = len(harness["proof_routes"])

    original_advance = module._task8_advance_policy_recovery_cursor
    if crash_point == "cursor-write":

        def advance_then_die(*args: object, **kwargs: object) -> dict[str, object]:
            result = original_advance(*args, **kwargs)
            raise Task8GuardProcessDeath("after committed policy cursor")

        monkeypatch.setattr(
            module,
            "_task8_advance_policy_recovery_cursor",
            advance_then_die,
        )
    elif crash_point == "runtime-lock-release":
        controls["crash_after"] = "runtime_lock_release"
    elif crash_point == "timer-start":
        controls["crash_after"] = "timer_start"
    else:
        controls["write_crash_after_phase"] = "policy_enabled"

    with pytest.raises(Task8GuardProcessDeath):
        module.recover_host_configuration(context)

    interrupted = harness["holder"]["state"]
    controls["crash_after"] = None
    monkeypatch.setattr(
        module,
        "_task8_advance_policy_recovery_cursor",
        original_advance,
    )
    if interrupted["phase"] == "policy_enabled":
        recovered = interrupted
    else:
        recovered = module.recover_host_configuration(context)

    assert recovered["phase"] == "policy_enabled"
    assert recovered["policy"] is not None
    assert recovered["recovery"]["completed_epoch"] is not None
    assert controls["policy_environment"] == harness["enabled_environment"]
    assert not any(
        call["action"] == "restore"
        for call in controls["policy_atomic_calls"]
    )
    assert set(harness["proof_routes"][proof_count_before_recovery:]) == {"source"}
    assert harness["live_runtime"] == harness["baseline"]
    module.validate_operation_state(recovered, operation_dir)


def test_task8_enable_prune_inventory_drift_requires_a_new_dry_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_policy_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    harness["controls"]["policy_inventory_output"] += "unexpected.dump\n"

    with pytest.raises(module.OperationStateError, match="changed after dry-run approval"):
        module.enable_remote_prune(context)

    recovered = harness["holder"]["state"]
    assert recovered["phase"] == "installed"
    assert recovered["policy"] is None
    assert harness["controls"]["policy_environment"] == harness[
        "disabled_environment"
    ]
    assert harness["controls"]["policy_atomic_calls"] == []
    groups = [
        group
        for group in recovered["rclone_evidence_groups"]
        if str(group["group_id"]).startswith("task8:policy:")
    ]
    assert len(groups) == 1
    assert groups[0]["outcome"] == "indeterminate"
    assert groups[0]["result_sha256"] is None


def test_task8_policy_recovery_rejects_uncommitted_third_environment_digest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_policy_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    harness["controls"]["policy_atomic_process_death_after_apply"] = True
    with pytest.raises(Task8GuardProcessDeath):
        module.enable_remote_prune(context)
    harness["controls"]["policy_environment"] = b"REMOTE_PRUNE_ENABLED=third\n"

    with pytest.raises(
        module.OperationStateError,
        match="unauthorized digest|authorized provenance",
    ):
        module.recover_host_configuration(context)

    state = harness["holder"]["state"]
    assert state["phase"] == "policy_enabling"
    assert state["policy"] is None
    assert [
        call["action"] for call in harness["controls"]["policy_atomic_calls"]
    ] == ["enable"]


def test_task8_committed_policy_recovery_rejects_disabled_environment_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_policy_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    original_record = module._task8_record_applied_policy

    def record_then_die(*args: object, **kwargs: object) -> dict[str, object]:
        result = original_record(*args, **kwargs)
        raise Task8GuardProcessDeath("after applied policy receipt")

    monkeypatch.setattr(module, "_task8_record_applied_policy", record_then_die)
    with pytest.raises(Task8GuardProcessDeath):
        module.enable_remote_prune(context)
    harness["controls"]["policy_environment"] = harness["disabled_environment"]

    with pytest.raises(
        module.OperationStateError,
        match="unauthorized digest|authorized provenance",
    ):
        module.recover_host_configuration(context)

    state = harness["holder"]["state"]
    assert state["phase"] == "policy_enabling"
    assert state["policy"] is not None
    assert not any(
        call["action"] == "restore"
        for call in harness["controls"]["policy_atomic_calls"]
    )


def test_task8_observation_service_decoder_requires_one_successful_inactive_invocation() -> None:
    module = load_ops_helper()
    raw = (
        "LoadState=loaded\n"
        "ActiveState=inactive\n"
        "SubState=dead\n"
        "MainPID=0\n"
        "Result=success\n"
        "ExecMainCode=1\n"
        "ExecMainStatus=0\n"
        "ExecMainStartTimestamp=Wed 2026-07-01 10:15:05 UTC\n"
        "ExecMainExitTimestamp=Wed 2026-07-01 10:18:09 UTC\n"
        "ExecMainStartTimestampMonotonic=123456789000\n"
        "TriggeredBy=degen-prod-db-backup.timer\n"
        "RefuseManualStart=yes\n"
        "InvocationID=0123456789abcdef0123456789abcdef\n"
    )

    decoded = module._task8_decode_observation_service(raw)

    assert decoded == {
        "start_timestamp": "Wed 2026-07-01 10:15:05 UTC",
        "exit_timestamp": "Wed 2026-07-01 10:18:09 UTC",
        "start_monotonic_usec": 123456789000,
        "invocation_id": "0123456789abcdef0123456789abcdef",
    }


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("LoadState", "not-found"),
        ("ActiveState", "active"),
        ("SubState", "running"),
        ("MainPID", "9"),
        ("Result", "exit-code"),
        ("ExecMainCode", "2"),
        ("ExecMainStatus", "1"),
        ("ExecMainStartTimestampMonotonic", "0"),
        ("TriggeredBy", ""),
        ("RefuseManualStart", "no"),
        ("InvocationID", "ABCDEF"),
    ],
)
def test_task8_observation_service_decoder_fails_closed(
    field: str,
    value: str,
) -> None:
    module = load_ops_helper()
    values = {
        "LoadState": "loaded",
        "ActiveState": "inactive",
        "SubState": "dead",
        "MainPID": "0",
        "Result": "success",
        "ExecMainCode": "1",
        "ExecMainStatus": "0",
        "ExecMainStartTimestamp": "Wed 2026-07-01 10:15:05 UTC",
        "ExecMainExitTimestamp": "Wed 2026-07-01 10:18:09 UTC",
        "ExecMainStartTimestampMonotonic": "123456789000",
        "TriggeredBy": "degen-prod-db-backup.timer",
        "RefuseManualStart": "yes",
        "InvocationID": "0123456789abcdef0123456789abcdef",
    }
    values[field] = value
    raw = "".join(f"{key}={item}\n" for key, item in values.items())

    with pytest.raises(module.OperationStateError, match="service|invocation|success"):
        module._task8_decode_observation_service(raw)


def test_task8_observation_timer_decoder_binds_realtime_trigger() -> None:
    module = load_ops_helper()

    decoded = module._task8_decode_observation_timer(
        "LastTriggerUSec=Wed 2026-07-01 10:15:04 UTC\n"
    )

    assert decoded == {
        "trigger_timestamp": "Wed 2026-07-01 10:15:04 UTC",
    }


@pytest.mark.parametrize(
    "raw",
    (
        "LastTriggerUSec=\n",
        "LastTriggerUSec=n/a\n",
        "LastTriggerUSec=never\n",
        "LastTriggerUSec=Wed 2026-07-01 10:15:04 UTC\nLastTriggerUSec=duplicate\n",
        "LastTriggerUSec=Wed 2026-07-01 10:15:04 UTC\nUnexpected=value\n",
    ),
)
def test_task8_observation_timer_decoder_fails_closed(raw: str) -> None:
    module = load_ops_helper()

    with pytest.raises(module.OperationStateError, match="timer|trigger|monotonic"):
        module._task8_decode_observation_timer(raw)


def test_task8_observation_timer_monotonic_decoder_accepts_exact_busctl_uint64() -> None:
    module = load_ops_helper()

    assert (
        module._task8_decode_observation_timer_monotonic(
            "t 123455789000\n"
        )
        == 123455789000
    )


@pytest.mark.parametrize(
    "raw",
    (
        "",
        "t 0\n",
        "t 01\n",
        "t -1\n",
        "t 123455789000",
        " t 123455789000\n",
        "t  123455789000\n",
        "t 123455789000 \n",
        "s 123455789000\n",
        "t 123455789000\nt 123455789001\n",
    ),
)
def test_task8_observation_timer_monotonic_decoder_fails_closed(raw: str) -> None:
    module = load_ops_helper()

    with pytest.raises(module.OperationStateError, match="timer|trigger|monotonic"):
        module._task8_decode_observation_timer_monotonic(raw)


def test_task8_observation_journal_decoder_correlates_exact_run_and_receipts() -> None:
    module = load_ops_helper()
    invocation = "0123456789abcdef0123456789abcdef"
    dump = "degen_green_20260701T101506Z.dump"
    before = (
        "RCLONE_CONFIG_RECEIPT phase=before status=ok "
        f"path={TASK8_RCLONE_CONFIG_PATH} {TASK8_RCLONE_BEFORE_METADATA} "
        "change=baseline"
    )
    final = (
        "RCLONE_CONFIG_RECEIPT phase=final status=ok "
        f"path={TASK8_RCLONE_CONFIG_PATH} {TASK8_RCLONE_BEFORE_METADATA} "
        "change=unchanged"
    )
    messages = [
        f"[2026-07-01T10:15:05Z] {before}",
        f"[2026-07-01T10:15:06Z] Creating PostgreSQL custom-format backup: {dump}",
        f"[2026-07-01T10:17:55Z] Remote backup pair verified: {dump} and {dump}.sha256",
        "[2026-07-01T10:18:08Z] Backup completed successfully",
        f"[2026-07-01T10:18:09Z] {final}",
    ]
    raw = "".join(
        json.dumps(
            {
                "__REALTIME_TIMESTAMP": str(
                    int(
                        datetime.strptime(
                            message[1:21],
                            "%Y-%m-%dT%H:%M:%SZ",
                        ).replace(tzinfo=timezone.utc).timestamp()
                    )
                    * 1_000_000
                ),
                "MESSAGE": message,
                "_SYSTEMD_INVOCATION_ID": invocation,
                "_SYSTEMD_UNIT": "degen-prod-db-backup.service",
            },
            separators=(",", ":"),
        )
        + "\n"
        for index, message in enumerate(messages)
    )

    decoded = module._task8_decode_observation_journal(
        raw,
        expected_invocation_id=invocation,
        expected_prefix="degen_green_",
    )

    assert decoded["dump_name"] == dump
    assert decoded["sidecar_name"] == f"{dump}.sha256"
    assert decoded["success_epoch"] == 1_782_901_088
    assert decoded["final_epoch"] == 1_782_901_089
    assert decoded["before_receipt"]["change"] == "baseline"
    assert decoded["final_receipt"]["change"] == "unchanged"
    assert decoded["log_suffix"] == "".join(f"{message}\n" for message in messages)


@pytest.mark.parametrize("defect", ("duplicate-success", "wrong-invocation", "reordered-final"))
def test_task8_observation_journal_decoder_rejects_mixed_or_ambiguous_runs(
    defect: str,
) -> None:
    module = load_ops_helper()
    invocation = "0123456789abcdef0123456789abcdef"
    dump = "degen_green_20260701T101506Z.dump"
    before = (
        "RCLONE_CONFIG_RECEIPT phase=before status=ok "
        f"path={TASK8_RCLONE_CONFIG_PATH} {TASK8_RCLONE_BEFORE_METADATA} "
        "change=baseline"
    )
    final = (
        "RCLONE_CONFIG_RECEIPT phase=final status=ok "
        f"path={TASK8_RCLONE_CONFIG_PATH} {TASK8_RCLONE_BEFORE_METADATA} "
        "change=unchanged"
    )
    messages = [
        f"[2026-07-01T10:15:05Z] {before}",
        f"[2026-07-01T10:15:06Z] Creating PostgreSQL custom-format backup: {dump}",
        f"[2026-07-01T10:17:55Z] Remote backup pair verified: {dump} and {dump}.sha256",
        "[2026-07-01T10:18:08Z] Backup completed successfully",
        f"[2026-07-01T10:18:09Z] {final}",
    ]
    if defect == "duplicate-success":
        messages.insert(-1, "[2026-07-01T10:18:08Z] Backup completed successfully")
    elif defect == "reordered-final":
        messages[0], messages[-1] = messages[-1], messages[0]
    raw = "".join(
        json.dumps(
            {
                "__REALTIME_TIMESTAMP": str(
                    int(
                        datetime.strptime(
                            message[1:21],
                            "%Y-%m-%dT%H:%M:%SZ",
                        ).replace(tzinfo=timezone.utc).timestamp()
                    )
                    * 1_000_000
                ),
                "MESSAGE": message,
                "_SYSTEMD_INVOCATION_ID": (
                    "fedcba9876543210fedcba9876543210"
                    if defect == "wrong-invocation" and index == 2
                    else invocation
                ),
                "_SYSTEMD_UNIT": "degen-prod-db-backup.service",
            },
            separators=(",", ":"),
        )
        + "\n"
        for index, message in enumerate(messages)
    )

    with pytest.raises(
        module.OperationStateError,
        match="journal|invocation|success|receipt|order",
    ):
        module._task8_decode_observation_journal(
            raw,
            expected_invocation_id=invocation,
            expected_prefix="degen_green_",
        )


def test_task8_observation_remote_decoders_are_exact_and_result_bound() -> None:
    module = load_ops_helper()
    dump = "degen_green_20260701T101506Z.dump"
    digest = "a" * 64
    stat_raw = json.dumps(
        {
            "Path": dump,
            "Name": dump,
            "Size": 1234,
            "IsDir": False,
            "ModTime": "2026-06-30T23:51:30.659886554-07:00",
            "ID": "remote-object-id",
        },
        sort_keys=True,
        separators=(",", ":"),
    ) + "\n"
    hash_raw = f"{digest}  {dump}\n"
    sidecar_raw = f"{digest}  {dump}\n"

    assert module._task8_decode_observation_remote_stat(
        stat_raw,
        expected_name=dump,
        expected_size=1234,
    )["Size"] == 1234
    assert module._task8_decode_observation_remote_hash(
        hash_raw,
        expected_name=dump,
    ) == digest
    assert module._task8_decode_observation_remote_sidecar(
        sidecar_raw,
        expected_name=dump,
        expected_sha256=digest,
    ) == sidecar_raw
    result = module._task8_observation_result_sha256(
        "observe-current-hash-before",
        {"name": dump, "sha256": digest},
    )
    assert len(result) == 64
    int(result, 16)


@pytest.mark.parametrize(
    "mod_time",
    (
        "2026-07-01T10:15:06Z",
        "2026-07-01T10:15:06.1+00:00",
        "2026-06-30T23:51:30.659886554-07:00",
    ),
)
def test_task8_observation_remote_stat_accepts_strict_rfc3339nano(
    mod_time: str,
) -> None:
    module = load_ops_helper()
    dump = "degen_green_20260701T101506Z.dump"
    raw = json.dumps(
        {
            "Path": dump,
            "Name": dump,
            "Size": 13,
            "IsDir": False,
            "ModTime": mod_time,
        },
        separators=(",", ":"),
    ) + "\n"

    decoded = module._task8_decode_observation_remote_stat(
        raw,
        expected_name=dump,
        expected_size=13,
    )

    assert decoded["ModTime"] == mod_time


@pytest.mark.parametrize(
    "mod_time",
    (
        "2026-07-01T10:15:06",
        "2026-07-01 10:15:06Z",
        "2026-07-01T10:15:06.z",
        "2026-07-01T10:15:06.1234567890Z",
        "2026-07-01T10:15:06+24:00",
        "2026-07-01T10:15:06+00:60",
        "2026-07-01T10:15:06-00:60",
        "2026-07-01T10:15:06+23:60",
        "2026-02-30T10:15:06Z",
    ),
)
def test_task8_observation_remote_stat_rejects_noncanonical_modtime(
    mod_time: str,
) -> None:
    module = load_ops_helper()
    dump = "degen_green_20260701T101506Z.dump"
    raw = json.dumps(
        {
            "Path": dump,
            "Name": dump,
            "Size": 13,
            "IsDir": False,
            "ModTime": mod_time,
        },
        separators=(",", ":"),
    ) + "\n"

    with pytest.raises(module.OperationStateError, match="remote stat ModTime"):
        module._task8_decode_observation_remote_stat(
            raw,
            expected_name=dump,
            expected_size=13,
        )


def test_task8_observation_remote_inventory_surfaces_and_rejects_nesting() -> None:
    module = load_ops_helper()
    names = [
        "degen_green_20260701T101506Z.dump",
        "degen_green_20260701T101506Z.dump.sha256",
    ]
    raw = json.dumps(
        [
            {
                "Path": name,
                "Name": name,
                "Size": 10 + index,
                "IsDir": False,
            }
            for index, name in enumerate(reversed(names))
        ],
        separators=(",", ":"),
    ) + "\n"

    assert module._task8_decode_observation_remote_inventory(raw) == (
        names,
        [name.casefold() for name in names],
    )

    for unsafe in (
        [{"Path": "nested", "Name": "nested", "Size": 0, "IsDir": True}],
        [
            {
                "Path": f"nested/{names[0]}",
                "Name": names[0],
                "Size": 10,
                "IsDir": False,
            }
        ],
    ):
        with pytest.raises(module.OperationStateError, match="remote|nested|directory|inventory"):
            module._task8_decode_observation_remote_inventory(
                json.dumps(unsafe, separators=(",", ":")) + "\n"
            )


def test_task8_observation_rclone_groups_are_result_bearing(tmp_path: Path) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "observing")
    group = task8_pending_rclone_group("observe", 1_750_000_110, 0)
    group["purpose"] = "observe-inventory-before"
    state["rclone_evidence_groups"].append(group)

    with pytest.raises(module.OperationStateError, match="result|digest"):
        completed = copy.deepcopy(state)
        completed_group = completed["rclone_evidence_groups"][-1]
        completed_group["after"] = task8_file_audit(
            HASH_B,
            inode=901,
            size=902,
            mtime_ns=1_750_000_111_000_000_000,
        )
        completed_group["outcome"] = "success"
        completed_group["evidence_sha256"] = HASH_C
        module.validate_operation_state(completed, operation_dir, state)


def task8_reseal_observation_state(
    module: object,
    state: dict[str, object],
) -> None:
    history = state["phase_history"]
    assert isinstance(history, list)
    entry = next(
        item for item in reversed(history) if item["phase"] == "observing"
    )
    completed = next(
        item for item in reversed(history) if item["phase"] == "observed"
    )
    entry["evidence_sha256"] = module._task8_observation_entry_evidence_sha256(
        state,
        entry,
    )
    groups = [
        task8_rclone_group(
            "observe",
            int(entry["epoch"]),
            0,
            ordinal,
            purpose=purpose,
        )
        for ordinal, purpose in enumerate(module._TASK8_OBSERVE_PURPOSES)
    ]
    for ordinal, group in enumerate(groups):
        if ordinal:
            group["before"] = copy.deepcopy(groups[ordinal - 1]["after"])
        group["result_sha256"] = module._task8_observation_result_sha256(
            str(group["purpose"]),
            {"ordinal": ordinal},
        )
        group["evidence_sha256"] = module._task8_group_evidence_sha256(group)
    state["rclone_evidence_groups"] = [
        group
        for group in state["rclone_evidence_groups"]
        if not str(group["group_id"]).startswith("task8:observe:")
    ] + groups
    observation = state["observation"]
    assert isinstance(observation, dict)
    evidence = module._task8_observation_completion_evidence_sha256(
        state,
        entry,
        completed,
        observation,
        groups,
    )
    observation["evidence_sha256"] = evidence
    completed["evidence_sha256"] = evidence


def test_task8_observation_state_semantics_bind_entry_groups_and_terminal_receipt(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "observed")
    task8_reseal_observation_state(module, state)

    module.validate_operation_state(state, operation_dir)

    tampered = copy.deepcopy(state)
    tampered["observation"]["local_sha256"] = HASH_D
    with pytest.raises(module.OperationStateError, match="observation|evidence|digest"):
        module.validate_operation_state(tampered, operation_dir)

    incomplete = copy.deepcopy(state)
    incomplete["rclone_evidence_groups"] = incomplete["rclone_evidence_groups"][:-1]
    with pytest.raises(module.OperationStateError, match="observation|rclone|sequence"):
        module.validate_operation_state(incomplete, operation_dir)

    tampered_result = copy.deepcopy(state)
    observe_groups = [
        group
        for group in tampered_result["rclone_evidence_groups"]
        if str(group["group_id"]).startswith("task8:observe:")
    ]
    observe_groups[2]["result_sha256"] = HASH_D
    with pytest.raises(module.OperationStateError, match="rclone|evidence|digest"):
        module.validate_operation_state(tampered_result, operation_dir)

    indeterminate = copy.deepcopy(state)
    observe_groups = [
        group
        for group in indeterminate["rclone_evidence_groups"]
        if str(group["group_id"]).startswith("task8:observe:")
    ]
    terminal = observe_groups[-1]
    terminal["outcome"] = "indeterminate"
    terminal["result_sha256"] = None
    terminal["evidence_sha256"] = module._task8_group_evidence_sha256(terminal)
    with pytest.raises(module.OperationStateError, match="observation|rclone|sequence"):
        module.validate_operation_state(indeterminate, operation_dir)


def test_task8_observing_entry_digest_is_context_free_revalidated(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "observing")
    entry = state["phase_history"][-1]
    entry["evidence_sha256"] = module._task8_observation_entry_evidence_sha256(
        state,
        entry,
    )

    module.validate_operation_state(state, operation_dir)

    state["active_transaction"]["runtime_baseline"]["pids"][
        "system:degen-web.service"
    ] += 1_000
    with pytest.raises(module.OperationStateError, match="observation|entry|evidence"):
        module.validate_operation_state(state, operation_dir)


@pytest.mark.parametrize(
    ("available_bytes", "low_space"),
    (
        (20_000_000_000, False),
        (10_737_418_239, True),
    ),
)
def test_task8_observation_local_evidence_verifies_pairs_and_disk_reserve(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    available_bytes: int,
    low_space: bool,
) -> None:
    module = load_ops_helper()
    context = task8_policy_atomic_context(module, tmp_path)
    state = state_at_phase(context.paths.operation_dir, "policy_enabled")
    backup_dir = host_root_path(
        context.host_root,
        str(state["effective_config"]["BACKUP_DIR"]),
    )
    log_dir = host_root_path(
        context.host_root,
        str(state["effective_config"]["LOG_DIR"]),
    )
    backup_dir.mkdir(parents=True)
    log_dir.mkdir(parents=True)
    if os.name == "posix":
        for directory in (backup_dir, log_dir):
            directory.chmod(0o700)
    pairs = (
        ("degen_green_20260630T101506Z.dump", b"older archive"),
        ("degen_green_20260701T101506Z.dump", b"new archive"),
    )
    for index, (name, payload) in enumerate(pairs):
        dump = backup_dir / name
        sidecar = backup_dir / f"{name}.sha256"
        dump.write_bytes(payload)
        digest = hashlib.sha256(payload).hexdigest()
        sidecar.write_bytes(f"{digest}  {name}\n".encode("ascii"))
        dump.chmod(0o600)
        sidecar.chmod(0o600)
        epoch = 1_751_364_906 if index else 1_751_278_506
        os.utime(dump, (epoch, epoch))
        os.utime(sidecar, (epoch, epoch))
    log_suffix = "[2026-07-01T10:18:09Z] Backup completed successfully\n"
    log_path = log_dir / "prod-db-backup.log"
    with log_path.open("wb") as stream:
        stream.seek(module._MAX_STATE_BYTES + 1)
        stream.write(log_suffix.encode("utf-8"))
    log_path.chmod(0o600)
    calls: list[tuple[tuple[str, ...], tuple[int, ...]]] = []

    def runner(
        argv: object,
        pass_fds: tuple[int, ...],
    ) -> subprocess.CompletedProcess[str]:
        argv_tuple = tuple(str(value) for value in argv)
        calls.append((argv_tuple, pass_fds))
        assert argv_tuple[:2] == ("/usr/bin/pg_restore", "--list")
        return subprocess.CompletedProcess(argv_tuple, 0, "archive listing\n", "")

    context = dataclasses.replace(context, command_runner=runner)
    if hasattr(module.os, "fstatvfs"):
        monkeypatch.setattr(
            module.os,
            "fstatvfs",
            lambda _descriptor: types.SimpleNamespace(
                f_bavail=available_bytes,
                f_frsize=1,
            ),
        )
    monkeypatch.setattr(
        module.shutil,
        "disk_usage",
        lambda _path: types.SimpleNamespace(free=available_bytes),
    )
    lock_path = tmp_path / "runtime.lock"
    lock_path.write_bytes(b"")
    lock_fd = os.open(lock_path, os.O_RDONLY)
    try:
        evidence_context = module._task8_open_observation_local_evidence(
            context,
            state,
            runtime_lock_fd=lock_fd,
            expected_dump_name=pairs[-1][0],
            expected_log_suffix=log_suffix,
        )
        if low_space:
            with pytest.raises(module.OperationStateError, match="filesystem|reserve"):
                with evidence_context:
                    pass
        else:
            with evidence_context as evidence:
                assert evidence["newest_dump"] == pairs[-1][0]
                assert len(evidence["pairs"]) == 2
                assert evidence["pairs"][0]["dump_name"] == pairs[-1][0]
                assert evidence["pairs"][1]["dump_name"] == pairs[0][0]
                assert evidence["free_bytes"] == available_bytes
                assert evidence["free_bytes"] >= evidence["reserve_bytes"]
    finally:
        os.close(lock_fd)

    assert len(calls) == 2
    assert all(lock_fd in pass_fds for _argv, pass_fds in calls)
    if os.name == "posix":
        assert all("/proc/self/fd/" in argv[2] for argv, _pass_fds in calls)


@pytest.mark.parametrize("defect", ("one-pair", "bad-sidecar", "archive-failure"))
def test_task8_observation_local_evidence_fails_closed(
    tmp_path: Path,
    defect: str,
) -> None:
    module = load_ops_helper()
    context = task8_policy_atomic_context(module, tmp_path)
    state = state_at_phase(context.paths.operation_dir, "policy_enabled")
    backup_dir = host_root_path(
        context.host_root,
        str(state["effective_config"]["BACKUP_DIR"]),
    )
    log_dir = host_root_path(
        context.host_root,
        str(state["effective_config"]["LOG_DIR"]),
    )
    backup_dir.mkdir(parents=True)
    log_dir.mkdir(parents=True)
    if os.name == "posix":
        backup_dir.chmod(0o700)
        log_dir.chmod(0o700)
    names = [
        "degen_green_20260630T101506Z.dump",
        "degen_green_20260701T101506Z.dump",
    ]
    if defect == "one-pair":
        names = names[-1:]
    for name in names:
        payload = name.encode("ascii")
        digest = hashlib.sha256(payload).hexdigest()
        (backup_dir / name).write_bytes(payload)
        sidecar_text = f"{digest}  {name}\n"
        if defect == "bad-sidecar" and name == names[-1]:
            sidecar_text = sidecar_text.upper()
        (backup_dir / f"{name}.sha256").write_bytes(sidecar_text.encode("ascii"))
        (backup_dir / name).chmod(0o600)
        (backup_dir / f"{name}.sha256").chmod(0o600)
    suffix = "[2026-07-01T10:18:09Z] Backup completed successfully\n"
    (log_dir / "prod-db-backup.log").write_bytes(suffix.encode("utf-8"))
    (log_dir / "prod-db-backup.log").chmod(0o600)

    def runner(
        argv: object,
        pass_fds: tuple[int, ...],
    ) -> subprocess.CompletedProcess[str]:
        del pass_fds
        argv_tuple = tuple(str(value) for value in argv)
        return subprocess.CompletedProcess(
            argv_tuple,
            1 if defect == "archive-failure" else 0,
            "",
            "",
        )

    context = dataclasses.replace(context, command_runner=runner)
    lock_path = tmp_path / "runtime.lock"
    lock_path.write_bytes(b"")
    lock_fd = os.open(lock_path, os.O_RDONLY)
    try:
        with pytest.raises(
            module.OperationStateError,
            match="pair|sidecar|archive|pg_restore|local",
        ):
            with module._task8_open_observation_local_evidence(
                context,
                state,
                runtime_lock_fd=lock_fd,
                expected_dump_name="degen_green_20260701T101506Z.dump",
                expected_log_suffix=suffix,
            ):
                pass
    finally:
        os.close(lock_fd)


def task8_observation_rclone_receipt(phase: str) -> dict[str, str]:
    return {
        "phase": phase,
        "path": TASK8_RCLONE_CONFIG_PATH,
        "sha256": HASH_A,
        "device": "2049",
        "inode": "12345",
        "uid": "0",
        "gid": "0",
        "mode": "0600",
        "links": "1",
        "size": "321",
        "mtime_ns": "1750000000000000000",
        "change": "baseline" if phase == "before" else "unchanged",
    }


def task8_observation_correlation_fixture() -> tuple[
    dict[str, object],
    dict[str, object],
    dict[str, object],
]:
    service = {
        "invocation_id": "0123456789abcdef0123456789abcdef",
        "start_epoch": 1_750_000_106,
        "exit_epoch": 1_750_000_109,
        "start_monotonic_usec": 123_456_789_000,
        "timer_trigger_epoch": 1_750_000_105,
        "timer_trigger_monotonic_usec": 123_455_789_000,
    }
    journal = {
        "dump_name": "degen_green_20250615T150826Z.dump",
        "sidecar_name": "degen_green_20250615T150826Z.dump.sha256",
        "stamp": "20250615T150826Z",
        "success_epoch": 1_750_000_108,
        "final_epoch": 1_750_000_109,
        "before_receipt": task8_observation_rclone_receipt("before"),
        "final_receipt": task8_observation_rclone_receipt("final"),
        "messages": ["safe journal message"],
        "trusted_epochs": [1_750_000_106, 1_750_000_108, 1_750_000_109],
        "log_suffix": "safe journal message\n",
    }
    local = {
        "backup_dir": EFFECTIVE_CONFIG["BACKUP_DIR"],
        "prefix": "degen_green_",
        "keep_local_count": 2,
        "free_bytes": 20_000_000_000,
        "reserve_bytes": 10_737_418_240,
        "pairs": [
            {
                "dump_name": journal["dump_name"],
                "sidecar_name": journal["sidecar_name"],
                "stamp": journal["stamp"],
                "stamp_epoch": 1_750_000_106,
                "dump_size": 100,
                "dump_mtime_ns": 1_750_000_106_000_000_000,
                "sidecar_mtime_ns": 1_750_000_107_000_000_000,
                "dump_sha256": HASH_A,
                "sidecar_sha256": HASH_B,
                "sidecar_text": f"{HASH_A}  {journal['dump_name']}\n",
            },
            {
                "dump_name": "degen_green_20250614T000000Z.dump",
                "sidecar_name": "degen_green_20250614T000000Z.dump.sha256",
                "stamp": "20250614T000000Z",
                "stamp_epoch": 1_749_859_200,
                "dump_size": 90,
                "dump_mtime_ns": 1_749_859_200_000_000_000,
                "sidecar_mtime_ns": 1_749_859_201_000_000_000,
                "dump_sha256": HASH_C,
                "sidecar_sha256": HASH_D,
                "sidecar_text": (
                    f"{HASH_C}  degen_green_20250614T000000Z.dump\n"
                ),
            },
        ],
        "newest_dump": journal["dump_name"],
        "log_suffix_sha256": hashlib.sha256(
            journal["log_suffix"].encode("utf-8")
        ).hexdigest(),
    }
    return service, journal, local


def test_task8_observation_correlation_accepts_one_fresh_and_one_older_pair(
    tmp_path: Path,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "observing")
    service, journal, local = task8_observation_correlation_fixture()

    result = module._task8_validate_observation_correlation(
        state,
        service=service,
        journal=journal,
        local=local,
    )

    assert result["run_epoch"] == 1_750_000_105
    assert len(result["journal_sha256"]) == 64
    assert len(result["local_sha256"]) == 64


@pytest.mark.parametrize(
    "defect",
    (
        "trigger-cutoff",
        "start-cutoff",
        "success-cutoff",
        "filename-cutoff",
        "dump-mtime-cutoff",
        "sidecar-mtime-cutoff",
        "start-before-trigger",
        "manual-retry-monotonic",
        "exit-after-entry",
    ),
)
def test_task8_observation_correlation_rejects_each_stale_or_future_boundary(
    tmp_path: Path,
    defect: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "observing")
    service, journal, local = task8_observation_correlation_fixture()
    cutoff = int(state["policy"]["enabled_epoch"])
    if defect == "trigger-cutoff":
        state["active_transaction"]["runtime_baseline"][
            "preinstall_trigger_epoch"
        ] = cutoff
    elif defect == "start-cutoff":
        service["start_epoch"] = cutoff
    elif defect == "success-cutoff":
        journal["success_epoch"] = cutoff
    elif defect == "filename-cutoff":
        local["pairs"][0]["stamp_epoch"] = cutoff
    elif defect == "dump-mtime-cutoff":
        local["pairs"][0]["dump_mtime_ns"] = cutoff * 1_000_000_000
    elif defect == "sidecar-mtime-cutoff":
        local["pairs"][0]["sidecar_mtime_ns"] = cutoff * 1_000_000_000
    elif defect == "start-before-trigger":
        service["start_epoch"] = 1_750_000_104
    elif defect == "manual-retry-monotonic":
        service["start_monotonic_usec"] = (
            int(service["timer_trigger_monotonic_usec"]) + 30_000_001
        )
    else:
        service["exit_epoch"] = 1_750_000_111

    with pytest.raises(
        module.OperationStateError,
        match="observation|fresh|cutoff|trigger|order|future|mtime|timestamp",
    ):
        module._task8_validate_observation_correlation(
            state,
            service=service,
            journal=journal,
            local=local,
        )


def task8_install_normal_observation_harness(
    module: object,
    monkeypatch: pytest.MonkeyPatch,
    operation_dir: Path,
) -> tuple[object, dict[str, object]]:
    initial = state_at_phase(operation_dir, "policy_enabled")
    context, harness = task8_install_guard_recovery_harness(
        module,
        monkeypatch,
        initial,
        operation_dir,
    )
    trigger_epoch = 1_750_000_095
    harness["baseline"]["preinstall_trigger_epoch"] = trigger_epoch
    harness["live_runtime"]["preinstall_trigger_epoch"] = trigger_epoch
    context.host_root = operation_dir
    expected_bytes = {
        target: f"observation installed bytes for {target}\n".encode("ascii")
        for target in TARGETS
    }
    enabled_bytes = module._task8_enabled_environment_bytes(
        expected_bytes[TARGETS[-1]].replace(
            b"observation installed bytes for /etc/degen/prod-db-backup.env\n",
            b"REMOTE_PRUNE_ENABLED=0\n",
        )
    )
    expected_bytes[TARGETS[-1]] = b"REMOTE_PRUNE_ENABLED=0\n"
    enabled_sha256 = hashlib.sha256(enabled_bytes).hexdigest()

    def bind_enabled_policy(candidate: dict[str, object]) -> None:
        candidate["host_stage"]["enabled_environment_sha256"] = enabled_sha256
        policy = candidate["policy"]
        policy["environment_sha256"] = enabled_sha256
        policy["applied_target"]["sha256"] = enabled_sha256
        history = candidate["phase_history"]
        entry = next(
            item for item in reversed(history) if item["phase"] == "policy_enabling"
        )
        entry["evidence_sha256"] = task8_policy_entry_evidence(
            candidate,
            entry,
            runtime_baseline=recovery_runtime_baseline(),
        )
        group = next(
            item
            for item in candidate["rclone_evidence_groups"]
            if str(item["group_id"]).startswith("task8:policy:")
        )
        policy["applied_evidence_sha256"] = task8_policy_applied_evidence(
            candidate,
            entry,
            policy,
            group,
        )
        completed = next(
            item for item in reversed(history) if item["phase"] == "policy_enabled"
        )
        completed["evidence_sha256"] = task8_policy_completion_evidence(
            entry=entry,
            completed_epoch=int(completed["epoch"]),
            policy=policy,
            dry_run=candidate["dry_run"],
            group=group,
        )

    bind_enabled_policy(initial)
    bind_enabled_policy(harness["holder"]["state"])
    module.validate_operation_state(harness["holder"]["state"], operation_dir)
    immutable_events: list[str] = []
    environment_checks: list[str] = []
    immutable_open_count = 0

    monkeypatch.setattr(
        module,
        "_task7_install_bytes",
        lambda _material: copy.deepcopy(expected_bytes),
    )

    @contextlib.contextmanager
    def open_immutable(_context: object):
        nonlocal immutable_open_count
        immutable_open_count += 1
        open_ordinal = immutable_open_count
        immutable_events.append("open")
        try:
            yield {target: object() for target in TARGETS[:-1]}
        finally:
            immutable_events.append("close")
            if (
                controls["observation_crash_after_stage"] == "immutable_close"
                and open_ordinal == 2
            ):
                controls["observation_crash_after_stage"] = None
                raise Task8GuardProcessDeath(
                    "observation process death after immutable proof close"
                )

    def validate_immutable(
        state: dict[str, object],
        observed_bytes: dict[str, bytes],
        proofs: dict[str, object],
    ) -> None:
        assert observed_bytes == expected_bytes
        assert tuple(proofs) == TARGETS[:-1]
        immutable_events.append("validate:" + str(state["phase"]))

    def require_environment(_context: object, expected: bytes) -> dict[str, object]:
        assert expected == enabled_bytes
        environment_checks.append("check")
        return copy.deepcopy(initial["policy"]["applied_target"])

    monkeypatch.setattr(
        module,
        "_open_task8_policy_immutable_target_proofs",
        open_immutable,
    )
    monkeypatch.setattr(
        module,
        "_task8_validate_live_installed_immutable_target_proofs",
        validate_immutable,
    )
    monkeypatch.setattr(
        module,
        "_task8_require_exact_policy_environment",
        require_environment,
    )

    stamp = datetime.fromtimestamp(1_750_000_096, tz=timezone.utc).strftime(
        "%Y%m%dT%H%M%SZ"
    )
    dump = f"degen_green_{stamp}.dump"
    service = {
        "invocation_id": "0123456789abcdef0123456789abcdef",
        "start_epoch": 1_750_000_096,
        "exit_epoch": 1_750_000_099,
        "start_monotonic_usec": 123_456_000_000,
    }
    timer = {
        "trigger_epoch": trigger_epoch,
        "trigger_monotonic_usec": 123_455_000_000,
    }
    journal = {
        "dump_name": dump,
        "sidecar_name": f"{dump}.sha256",
        "stamp": stamp,
        "success_epoch": 1_750_000_098,
        "final_epoch": 1_750_000_099,
        "before_receipt": task8_observation_rclone_receipt("before"),
        "final_receipt": task8_observation_rclone_receipt("final"),
        "messages": ["safe observation journal"],
        "trusted_epochs": [1_750_000_096, 1_750_000_098, 1_750_000_099],
        "log_suffix": "safe observation journal\n",
    }
    local = {
        "backup_dir": EFFECTIVE_CONFIG["BACKUP_DIR"],
        "prefix": "degen_green_",
        "keep_local_count": 2,
        "free_bytes": 20_000_000_000,
        "reserve_bytes": 10_737_418_240,
        "pairs": [
            {
                "dump_name": dump,
                "sidecar_name": f"{dump}.sha256",
                "stamp": stamp,
                "stamp_epoch": 1_750_000_096,
                "dump_size": 100,
                "dump_mtime_ns": 1_750_000_096_000_000_000,
                "sidecar_mtime_ns": 1_750_000_097_000_000_000,
                "dump_sha256": HASH_A,
                "sidecar_sha256": HASH_B,
                "sidecar_text": f"{HASH_A}  {dump}\n",
            },
            {
                "dump_name": "degen_green_20250614T000000Z.dump",
                "sidecar_name": "degen_green_20250614T000000Z.dump.sha256",
                "stamp": "20250614T000000Z",
                "stamp_epoch": 1_749_859_200,
                "dump_size": 90,
                "dump_mtime_ns": 1_749_859_200_000_000_000,
                "sidecar_mtime_ns": 1_749_859_201_000_000_000,
                "dump_sha256": HASH_C,
                "sidecar_sha256": HASH_D,
                "sidecar_text": (
                    f"{HASH_C}  degen_green_20250614T000000Z.dump\n"
                ),
            },
        ],
        "newest_dump": dump,
        "log_suffix_sha256": hashlib.sha256(
            journal["log_suffix"].encode("utf-8")
        ).hexdigest(),
    }
    controls = harness["controls"]
    controls.update(
        {
            "observation_error": None,
            "observation_crash_after_ordinal": None,
            "observation_crash_after_stage": None,
            "observation_timer_drift": False,
            "observation_service_drift": False,
        }
    )
    service_calls: list[dict[str, object]] = []
    timer_calls: list[dict[str, object]] = []
    journal_calls: list[dict[str, object]] = []
    local_events: list[str] = []
    remote_calls: list[str] = []

    def capture_service(*_args: object, **_kwargs: object) -> dict[str, object]:
        observed = copy.deepcopy(service)
        if controls["observation_service_drift"] and service_calls:
            observed["invocation_id"] = "fedcba9876543210fedcba9876543210"
        service_calls.append(copy.deepcopy(observed))
        if (
            controls["observation_crash_after_stage"] == "service_capture"
            and len(service_calls) == 1
        ):
            controls["observation_crash_after_stage"] = None
            raise Task8GuardProcessDeath(
                "observation process death after service capture"
            )
        return observed

    def capture_timer(*_args: object, **_kwargs: object) -> dict[str, object]:
        observed = copy.deepcopy(timer)
        if controls["observation_timer_drift"] and timer_calls:
            observed["trigger_monotonic_usec"] = (
                int(observed["trigger_monotonic_usec"]) + 1
            )
        timer_calls.append(copy.deepcopy(observed))
        if (
            controls["observation_crash_after_stage"] == "timer_capture"
            and len(timer_calls) == 1
        ):
            controls["observation_crash_after_stage"] = None
            raise Task8GuardProcessDeath(
                "observation process death after timer capture"
            )
        return observed

    def capture_journal(*_args: object, **_kwargs: object) -> dict[str, object]:
        journal_calls.append(copy.deepcopy(journal))
        if controls["observation_crash_after_stage"] == "journal_capture":
            controls["observation_crash_after_stage"] = None
            raise Task8GuardProcessDeath(
                "observation process death after journal capture"
            )
        return copy.deepcopy(journal)

    @contextlib.contextmanager
    def open_local(*_args: object, **_kwargs: object):
        local_events.append("open")
        if controls["observation_error"] == "local":
            raise module.OperationStateError("controlled local observation failure")
        try:
            yield copy.deepcopy(local)
        finally:
            local_events.append("close")
            if controls["observation_crash_after_stage"] == "local_close":
                controls["observation_crash_after_stage"] = None
                raise Task8GuardProcessDeath(
                    "observation process death after local proof close"
                )

    audit = task8_file_audit(
        HASH_A,
        inode=12345,
        size=321,
        mtime_ns=1_750_000_000_000_000_000,
    )
    monkeypatch.setattr(
        module,
        "_task7_capture_file_audit",
        lambda *_args, **_kwargs: copy.deepcopy(audit),
    )

    def capture_remote(
        remote_context: object,
        binding: object,
        state: dict[str, object],
        **_kwargs: object,
    ) -> dict[str, object]:
        del remote_context
        remote_calls.append("capture")
        if controls["observation_error"] == "remote":
            raise module.OperationStateError("controlled remote observation failure")
        for ordinal, purpose in enumerate(module._TASK8_OBSERVE_PURPOSES):
            module._task8_begin_rclone_audit(
                context,
                binding,
                state,
                purpose,
            )
            result_sha256 = module._task8_observation_result_sha256(
                purpose,
                {"ordinal": ordinal},
            )
            module._task8_complete_rclone_audit(
                context,
                binding,
                state,
                outcome="success",
                result_sha256=result_sha256,
            )
            if controls["observation_crash_after_ordinal"] == ordinal:
                raise Task8GuardProcessDeath("observation remote process death")
        return {"remote_sha256": HASH_C, "summary": {}}

    monkeypatch.setattr(module, "_task8_capture_observation_service", capture_service)
    monkeypatch.setattr(module, "_task8_capture_observation_timer", capture_timer)
    monkeypatch.setattr(module, "_task8_capture_observation_journal", capture_journal)
    monkeypatch.setattr(
        module,
        "_task8_open_observation_local_evidence",
        open_local,
    )
    monkeypatch.setattr(
        module,
        "_task8_capture_observation_remote_evidence",
        capture_remote,
    )
    harness.update(
        {
            "initial": copy.deepcopy(initial),
            "expected_bytes": expected_bytes,
            "enabled_bytes": enabled_bytes,
            "immutable_events": immutable_events,
            "environment_checks": environment_checks,
            "service": service,
            "timer": timer,
            "journal": journal,
            "local": local,
            "service_calls": service_calls,
            "timer_calls": timer_calls,
            "journal_calls": journal_calls,
            "local_events": local_events,
            "remote_calls": remote_calls,
        }
    )
    return context, harness


def test_task8_normal_observation_success_is_read_only_and_records_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_observation_harness(
        module,
        monkeypatch,
        operation_dir,
    )

    result = module.observe_scheduled_backup(context)

    assert result["phase"] == "observed"
    assert result["active_transaction"] is None
    assert result["observation"]["run_epoch"] == 1_750_000_095
    assert result["observation"]["remote_sha256"] == HASH_C
    assert result["failure"] is None
    module.validate_operation_state(result, operation_dir)
    assert set(harness["proof_routes"]) == {"installed"}
    assert harness["command_calls"] == []
    assert harness["remote_calls"] == ["capture"]
    assert len(harness["service_calls"]) == 2
    assert len(harness["timer_calls"]) == 2
    assert harness["local_events"] == ["open", "close"]
    assert harness["immutable_events"][0] == "open"
    assert harness["immutable_events"][-1] == "close"
    assert harness["environment_checks"]
    assert harness["release_actions"] == [
        "runtime_lock_release",
        "legacy_lock_release",
    ]
    assert harness["timeline"].index("action:restore_runtime_readback") < next(
        index
        for index, event in enumerate(harness["timeline"])
        if event == "write:observed"
    )


def test_task8_normal_observation_failure_routes_to_read_only_guard_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_observation_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    harness["controls"]["observation_error"] = "local"

    with pytest.raises(module.OperationStateError, match="local observation"):
        module.observe_scheduled_backup(context)

    interrupted = harness["holder"]["state"]
    assert interrupted["phase"] == "recovery_required"
    assert interrupted["active_transaction"]["kind"] == "observe"
    assert interrupted["observation"] is None
    assert interrupted["recovery"]["kind"] == "guard"
    module.validate_operation_state(interrupted, operation_dir)

    recovered = module.recover_host_configuration(context)

    assert recovered["phase"] == "policy_enabled"
    assert recovered["observation"] is None
    assert recovered["active_transaction"] is None
    assert harness["command_calls"] == []
    module.validate_operation_state(recovered, operation_dir)


@pytest.mark.parametrize("drift", ("timer", "service"))
def test_task8_normal_observation_rejects_second_read_timer_or_invocation_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    drift: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_observation_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    harness["controls"][f"observation_{drift}_drift"] = True

    with pytest.raises(module.OperationStateError, match="service invocation changed"):
        module.observe_scheduled_backup(context)

    interrupted = harness["holder"]["state"]
    assert interrupted["phase"] == "recovery_required"
    assert interrupted["observation"] is None
    module.validate_operation_state(interrupted, operation_dir)

    recovered = module.recover_host_configuration(context)

    assert recovered["phase"] == "policy_enabled"
    assert recovered["observation"] is None
    assert recovered["active_transaction"] is None
    module.validate_operation_state(recovered, operation_dir)


@pytest.mark.parametrize("ordinal", range(len(TASK8_OBSERVE_PURPOSES)))
def test_task8_normal_observation_process_death_fabricates_no_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ordinal: int,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_observation_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    harness["controls"]["observation_crash_after_ordinal"] = ordinal

    with pytest.raises(Task8GuardProcessDeath, match="observation remote"):
        module.observe_scheduled_backup(context)

    crashed = harness["holder"]["state"]
    assert crashed["phase"] == "observing"
    assert crashed["observation"] is None
    assert crashed["failure"] is None
    assert crashed["recovery"] is None
    assert crashed["active_transaction"]["kind"] == "observe"
    module.validate_operation_state(crashed, operation_dir)

    harness["controls"]["observation_crash_after_ordinal"] = None
    recovered = module.recover_host_configuration(context)

    assert recovered["phase"] == "policy_enabled"
    assert recovered["observation"] is None
    assert recovered["active_transaction"] is None
    assert harness["remote_calls"] == ["capture"]
    assert harness["command_calls"] == []
    module.validate_operation_state(recovered, operation_dir)


@pytest.mark.parametrize(
    ("boundary", "control_kind", "control_value", "durable_phase"),
    (
        ("timer capture", "stage", "timer_capture", "observing"),
        ("service capture", "stage", "service_capture", "observing"),
        ("journal capture", "stage", "journal_capture", "observing"),
        ("local proof close", "stage", "local_close", "observing"),
        (
            "runtime lock release",
            "action",
            "runtime_lock_release",
            "observing",
        ),
        (
            "legacy lock release",
            "action",
            "legacy_lock_release",
            "observing",
        ),
        (
            "timer restoration",
            "action",
            "restore_runtime_readback",
            "observing",
        ),
        (
            "durable timer-restored checkpoint",
            "guard_write",
            "timer_restored",
            "observing",
        ),
        (
            "immutable proof close",
            "stage",
            "immutable_close",
            "observing",
        ),
        ("terminal observed write", "phase_write", "observed", "observed"),
    ),
)
def test_task8_normal_observation_process_death_at_top_level_boundaries_is_recoverable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
    control_kind: str,
    control_value: str,
    durable_phase: str,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_observation_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    controls = harness["controls"]
    if control_kind == "stage":
        controls["observation_crash_after_stage"] = control_value
    elif control_kind == "action":
        controls["crash_after"] = control_value
    elif control_kind == "guard_write":
        controls["write_crash_after_guard_field"] = control_value
    else:
        assert control_kind == "phase_write"
        controls["write_crash_after_phase"] = control_value

    with pytest.raises(Task8GuardProcessDeath, match="observation|release|restore|write"):
        module.observe_scheduled_backup(context)

    durable = harness["holder"]["state"]
    assert durable["phase"] == durable_phase, boundary
    assert durable["failure"] is None
    assert durable["recovery"] is None
    module.validate_operation_state(durable, operation_dir)

    if durable_phase == "observed":
        assert durable["active_transaction"] is None
        assert durable["observation"] is not None
        return

    assert durable["active_transaction"]["kind"] == "observe"
    assert durable["observation"] is None
    controls["crash_after"] = None
    controls["observation_crash_after_stage"] = None

    recovered = module.recover_host_configuration(context)

    assert recovered["phase"] == "policy_enabled"
    assert recovered["active_transaction"] is None
    assert recovered["observation"] is None
    assert harness["command_calls"] == []
    module.validate_operation_state(recovered, operation_dir)


@pytest.mark.parametrize("same_name_replacement", (False, True))
def test_task8_normal_observation_real_remote_sequence_is_exact_and_nonmutating(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    same_name_replacement: bool,
) -> None:
    module = load_ops_helper()
    real_capture_remote = module._task8_capture_observation_remote_evidence
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_observation_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    monkeypatch.setattr(
        module,
        "_task8_capture_observation_remote_evidence",
        real_capture_remote,
    )
    newest = harness["local"]["pairs"][0]
    names = sorted([newest["dump_name"], newest["sidecar_name"]])
    inventory_output = json.dumps(
        [
            {
                "Path": name,
                "Name": name,
                "Size": newest["dump_size"] if not name.endswith(".sha256") else len(newest["sidecar_text"]),
                "IsDir": False,
            }
            for name in reversed(names)
        ],
        separators=(",", ":"),
    ) + "\n"
    remote_path = EFFECTIVE_CONFIG["RCLONE_REMOTE_PATH"]
    commands: list[tuple[tuple[str, ...], tuple[int, ...], str]] = []
    dump_replaced = False

    def runner(
        argv: object,
        pass_fds: tuple[int, ...],
    ) -> subprocess.CompletedProcess[str]:
        nonlocal dump_replaced
        argv_tuple = tuple(str(value) for value in argv)
        pending = harness["holder"]["state"]["rclone_evidence_groups"][-1]
        purpose = str(pending["purpose"])
        commands.append((argv_tuple, pass_fds, purpose))
        assert pass_fds == (harness["fresh_acquisitions"][0][1],)
        assert not {
            "copy",
            "copyto",
            "delete",
            "deletefile",
            "move",
            "moveto",
            "purge",
        }.intersection(argv_tuple)
        if purpose in {"observe-inventory-before", "observe-inventory-after"}:
            stdout = inventory_output
        elif purpose.startswith("observe-current-stat-"):
            stdout = json.dumps(
                {
                    "Path": newest["dump_name"],
                    "Name": newest["dump_name"],
                    "Size": newest["dump_size"],
                    "IsDir": False,
                    "ModTime": (
                        "2026-07-01T10:15:07Z"
                        if dump_replaced and purpose.endswith("-after")
                        else "2026-07-01T10:15:06Z"
                    ),
                    "ID": (
                        "replaced-remote-object-id"
                        if dump_replaced and purpose.endswith("-after")
                        else "stable-remote-object-id"
                    ),
                },
                separators=(",", ":"),
            ) + "\n"
        elif purpose.startswith("observe-current-hash-"):
            stdout = f"{newest['dump_sha256']}  {newest['dump_name']}\n"
            if same_name_replacement and purpose == "observe-current-hash-after":
                dump_replaced = True
        else:
            stdout = newest["sidecar_text"]
        return subprocess.CompletedProcess(argv_tuple, 0, stdout, "")

    context.command_runner = runner

    def planner(
        _context: object,
        planner_state: dict[str, object],
        inventory_names: list[str],
        *,
        runtime_lock_fd: int,
        expected_now_override: datetime | None = None,
    ) -> dict[str, list[str]]:
        assert planner_state["active_transaction"]["kind"] == "observe"
        assert inventory_names == names
        assert runtime_lock_fd == harness["fresh_acquisitions"][0][1]
        assert expected_now_override == datetime.strptime(
            str(newest["stamp"]),
            "%Y%m%dT%H%M%SZ",
        ).replace(tzinfo=timezone.utc)
        return {
            "keep_names": names,
            "protected_names": [],
            "delete_names": [],
        }

    monkeypatch.setattr(module, "_task8_run_remote_retention_planner", planner)

    if same_name_replacement:
        with pytest.raises(
            module.OperationStateError,
            match="remote|hash|sidecar|identity|changed|stat",
        ):
            module.observe_scheduled_backup(context)
        interrupted = harness["holder"]["state"]
        assert interrupted["phase"] == "recovery_required"
        assert interrupted["observation"] is None
        module.validate_operation_state(interrupted, operation_dir)
        return

    result = module.observe_scheduled_backup(context)

    assert result["phase"] == "observed"
    assert [purpose for _argv, _fds, purpose in commands] == list(
        TASK8_OBSERVE_PURPOSES
    )
    expected_argv = [
        (
            "/usr/bin/rclone",
            "--config",
            TASK8_RCLONE_CONFIG_PATH,
            "lsjson",
            remote_path,
            "--recursive",
        ),
        (
            "/usr/bin/rclone",
            "--config",
            TASK8_RCLONE_CONFIG_PATH,
            "lsjson",
            f"{remote_path}/{newest['dump_name']}",
            "--stat",
        ),
        (
            "/usr/bin/rclone",
            "--config",
            TASK8_RCLONE_CONFIG_PATH,
            "hashsum",
            "SHA-256",
            f"{remote_path}/{newest['dump_name']}",
            "--download",
        ),
        (
            "/usr/bin/rclone",
            "--config",
            TASK8_RCLONE_CONFIG_PATH,
            "cat",
            f"{remote_path}/{newest['sidecar_name']}",
        ),
        (
            "/usr/bin/rclone",
            "--config",
            TASK8_RCLONE_CONFIG_PATH,
            "lsjson",
            remote_path,
            "--recursive",
        ),
        (
            "/usr/bin/rclone",
            "--config",
            TASK8_RCLONE_CONFIG_PATH,
            "hashsum",
            "SHA-256",
            f"{remote_path}/{newest['dump_name']}",
            "--download",
        ),
        (
            "/usr/bin/rclone",
            "--config",
            TASK8_RCLONE_CONFIG_PATH,
            "cat",
            f"{remote_path}/{newest['sidecar_name']}",
        ),
        (
            "/usr/bin/rclone",
            "--config",
            TASK8_RCLONE_CONFIG_PATH,
            "lsjson",
            f"{remote_path}/{newest['dump_name']}",
            "--stat",
        ),
    ]
    assert [argv for argv, _fds, _purpose in commands] == expected_argv
    groups = [
        group
        for group in result["rclone_evidence_groups"]
        if str(group["group_id"]).startswith("task8:observe:")
    ]
    assert len(groups) == len(TASK8_OBSERVE_PURPOSES)
    assert all(group["outcome"] == "success" for group in groups)
    assert all(group["result_sha256"] is not None for group in groups)
    module.validate_operation_state(result, operation_dir)


def test_task8_normal_observation_remote_command_failure_is_indeterminate_and_recoverable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    real_capture_remote = module._task8_capture_observation_remote_evidence
    operation_dir, _uid = private_operation_dir(tmp_path)
    context, harness = task8_install_normal_observation_harness(
        module,
        monkeypatch,
        operation_dir,
    )
    monkeypatch.setattr(
        module,
        "_task8_capture_observation_remote_evidence",
        real_capture_remote,
    )
    newest = harness["local"]["pairs"][0]
    names = sorted([newest["dump_name"], newest["sidecar_name"]])
    inventory_output = json.dumps(
        [
            {
                "Path": name,
                "Name": name,
                "Size": (
                    newest["dump_size"]
                    if not name.endswith(".sha256")
                    else len(newest["sidecar_text"])
                ),
                "IsDir": False,
            }
            for name in names
        ],
        separators=(",", ":"),
    ) + "\n"
    purposes: list[str] = []

    def runner(
        argv: object,
        pass_fds: tuple[int, ...],
    ) -> subprocess.CompletedProcess[str]:
        argv_tuple = tuple(str(value) for value in argv)
        pending = harness["holder"]["state"]["rclone_evidence_groups"][-1]
        purpose = str(pending["purpose"])
        purposes.append(purpose)
        assert pass_fds == (harness["fresh_acquisitions"][0][1],)
        if purpose == "observe-inventory-before":
            return subprocess.CompletedProcess(argv_tuple, 0, inventory_output, "")
        if purpose == "observe-current-stat-before":
            stat_output = json.dumps(
                {
                    "Path": newest["dump_name"],
                    "Name": newest["dump_name"],
                    "Size": newest["dump_size"],
                    "IsDir": False,
                    "ModTime": "2026-07-01T10:15:06Z",
                    "ID": "stable-remote-object-id",
                },
                separators=(",", ":"),
            ) + "\n"
            return subprocess.CompletedProcess(argv_tuple, 0, stat_output, "")
        assert purpose == "observe-current-hash-before"
        return subprocess.CompletedProcess(
            argv_tuple,
            9,
            "",
            "simulated remote read failure\n",
        )

    context.command_runner = runner

    def planner(
        _context: object,
        _state: dict[str, object],
        inventory_names: list[str],
        **_kwargs: object,
    ) -> dict[str, list[str]]:
        assert inventory_names == names
        return {
            "keep_names": names,
            "protected_names": [],
            "delete_names": [],
        }

    monkeypatch.setattr(module, "_task8_run_remote_retention_planner", planner)

    with pytest.raises(
        module.OperationStateError,
        match="observation|command|rclone|failed",
    ):
        module.observe_scheduled_backup(context)

    interrupted = harness["holder"]["state"]
    assert interrupted["phase"] == "recovery_required"
    assert interrupted["observation"] is None
    groups = [
        group
        for group in interrupted["rclone_evidence_groups"]
        if str(group["group_id"]).startswith("task8:observe:")
    ]
    assert purposes == list(TASK8_OBSERVE_PURPOSES[:3])
    assert [group["purpose"] for group in groups] == purposes
    assert [group["outcome"] for group in groups] == [
        "success",
        "success",
        "indeterminate",
    ]
    assert groups[-1]["result_sha256"] is None
    assert groups[-1]["after"] is not None
    module.validate_operation_state(interrupted, operation_dir)

    recovered = module.recover_host_configuration(context)

    assert recovered["phase"] == "policy_enabled"
    assert recovered["observation"] is None
    assert recovered["active_transaction"] is None
    module.validate_operation_state(recovered, operation_dir)


def test_task8_observation_service_and_journal_capture_use_exact_readonly_commands(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_ops_helper()
    operation_dir, _uid = private_operation_dir(tmp_path)
    state = state_at_phase(operation_dir, "observing")
    invocation = "0123456789abcdef0123456789abcdef"
    service_raw = (
        "LoadState=loaded\nActiveState=inactive\nSubState=dead\nMainPID=0\n"
        "Result=success\nExecMainCode=1\nExecMainStatus=0\n"
        "ExecMainStartTimestamp=Wed 2026-07-01 10:15:05 UTC\n"
        "ExecMainExitTimestamp=Wed 2026-07-01 10:18:09 UTC\n"
        "ExecMainStartTimestampMonotonic=123456789000\n"
        "TriggeredBy=degen-prod-db-backup.timer\n"
        "RefuseManualStart=yes\n"
        f"InvocationID={invocation}\n"
    )
    timer_raw = "LastTriggerUSec=Wed 2026-07-01 10:15:04 UTC\n"
    timer_monotonic_raw = "t 123455789000\n"
    dump = "degen_green_20260701T101506Z.dump"
    messages = [
        "RCLONE_CONFIG_RECEIPT phase=before status=ok "
        f"path={TASK8_RCLONE_CONFIG_PATH} {TASK8_RCLONE_BEFORE_METADATA} change=baseline",
        f"Creating PostgreSQL custom-format backup: {dump}",
        f"Remote backup pair verified: {dump} and {dump}.sha256",
        "Backup completed successfully",
        "RCLONE_CONFIG_RECEIPT phase=final status=ok "
        f"path={TASK8_RCLONE_CONFIG_PATH} {TASK8_RCLONE_BEFORE_METADATA} change=unchanged",
    ]
    journal_epochs = (
        1_782_900_905,
        1_782_900_906,
        1_782_901_075,
        1_782_901_088,
        1_782_901_089,
    )
    journal_raw = "".join(
        json.dumps(
            {
                "__REALTIME_TIMESTAMP": str(epoch * 1_000_000),
                "MESSAGE": (
                    f"[{datetime.fromtimestamp(epoch, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}] "
                    f"{message}"
                ),
                "_SYSTEMD_INVOCATION_ID": invocation,
                "_SYSTEMD_UNIT": "degen-prod-db-backup.service",
            },
            separators=(",", ":"),
        )
        + "\n"
        for epoch, message in zip(journal_epochs, messages, strict=True)
    )
    calls: list[tuple[str, ...]] = []

    def readonly(
        _context: object,
        _binding: object,
        _state: dict[str, object],
        *,
        argv: tuple[str, ...],
        **_kwargs: object,
    ) -> str:
        calls.append(argv)
        if argv[0] == "/usr/bin/systemctl":
            return timer_raw if argv[2].endswith(".timer") else service_raw
        if argv[0] == "/usr/bin/busctl":
            return timer_monotonic_raw
        if argv[0] == "/usr/bin/date":
            if "10:15:04" in argv[2]:
                return "1782900904\n"
            return "1782900905\n" if "10:15:05" in argv[2] else "1782901089\n"
        return journal_raw

    monkeypatch.setattr(module, "_task8_run_observation_readonly_command", readonly)
    timer = module._task8_capture_observation_timer(
        object(),
        object(),
        state,
        runtime_lock_fd=7,
    )
    service = module._task8_capture_observation_service(
        object(),
        object(),
        state,
        runtime_lock_fd=7,
    )
    journal = module._task8_capture_observation_journal(
        object(),
        object(),
        state,
        runtime_lock_fd=7,
        invocation_id=invocation,
    )

    assert timer == {
        "trigger_epoch": 1_782_900_904,
        "trigger_monotonic_usec": 123455789000,
    }
    assert service["invocation_id"] == invocation
    assert service["start_epoch"] == 1_782_900_905
    assert service["exit_epoch"] == 1_782_901_089
    assert journal["dump_name"] == dump
    assert journal["success_epoch"] == 1_782_901_088
    assert journal["final_epoch"] == 1_782_901_089
    assert calls[0] == (
        "/usr/bin/systemctl",
        "show",
        "degen-prod-db-backup.timer",
        "--property=LastTriggerUSec",
        "--no-pager",
    )
    assert calls[1] == (
        "/usr/bin/busctl",
        "get-property",
        "org.freedesktop.systemd1",
        "/org/freedesktop/systemd1/unit/degen_2dprod_2ddb_2dbackup_2etimer",
        "org.freedesktop.systemd1.Timer",
        "LastTriggerUSecMonotonic",
    )
    assert calls[3][:3] == (
        "/usr/bin/systemctl",
        "show",
        "degen-prod-db-backup.service",
    )
    assert calls[-1] == (
        "/usr/bin/journalctl",
        "--unit=degen-prod-db-backup.service",
        f"_SYSTEMD_INVOCATION_ID={invocation}",
        "--output=json",
        "--no-pager",
    )
