#!/usr/bin/env python3
"""Privileged operation state for the Green database-backup rollout."""

from __future__ import annotations

import argparse
import contextlib
import copy
import hashlib
import json
import os
import re
import secrets
import stat
import subprocess
import sys
import tarfile
import threading
import types
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath


CommandRunner = Callable[
    [Sequence[str], tuple[int, ...]],
    subprocess.CompletedProcess[str],
]
Clock = Callable[[], datetime]
PreReplaceValidator = Callable[[], None]


@dataclass(frozen=True)
class _OperationDirectoryBinding:
    path: Path
    descriptor: int | None
    metadata: os.stat_result

_PRODUCTION_OPERATION_RE = re.compile(
    r"\A/opt/degen/backups/config/[0-9]{8}T[0-9]{6}Z\Z"
)
_SECRET_KEY_RE = re.compile(
    r"(?:\A|[_-])(?:passwords?|passwd|pwd|tokens?|secrets?|api[_-]?key|access[_-]?key|"
    r"private[_-]?key|database[_-]?url|pgdatabase|credentials?|authorization|cookies?)(?:\Z|[_-])|"
    r"\A(?:pgpassword|pgpassfile)\Z",
    re.IGNORECASE,
)
_DATABASE_URL_RE = re.compile(
    r"\b(?:postgres(?:ql)?|mysql|mariadb|mongodb(?:\+srv)?|redis)://[^\s]+",
    re.IGNORECASE,
)
_URL_USERINFO_RE = re.compile(r"\bhttps?://[^/\s:@]+:[^@\s/]+@[^\s]+", re.IGNORECASE)
_SECRET_ASSIGNMENT_RE = re.compile(
    r"(?<![A-Za-z0-9_])[\"']?(?:[A-Za-z0-9]+[_-])*"
    r"(?:passwords?|passwd|pwd|tokens?|secrets?|api[_-]?key|access[_-]?key|pgpassword|pgpassfile|"
    r"private[_-]?key|database[_-]?url|pgdatabase|authorization)"
    r"(?:[_-][A-Za-z0-9]+)*[\"']?\s*[:=]\s*"
    r"(?!\[REDACTED\](?:\s|\Z))(?:'[^'\r\n]*'|\"[^\"\r\n]*\"|[^\r\n]*)",
    re.IGNORECASE | re.MULTILINE,
)
_BEARER_RE = re.compile(r"\b(?:bearer|basic)\s+[A-Za-z0-9._~+/=-]+", re.IGNORECASE)
_TOKEN_PREFIX_RE = re.compile(
    r"\b(?:sk-[A-Za-z0-9_-]{8,}|gh[pousr]_[A-Za-z0-9_]{8,}|xox[baprs]-[A-Za-z0-9-]{8,}|"
    r"eyJ[A-Za-z0-9_-]{8,}(?:\.[A-Za-z0-9_-]+){1,2})"
)
_PRIVATE_KEY_RE = re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*", re.IGNORECASE | re.DOTALL)
_ENV_CONTENT_RE = re.compile(r"(?m)^[A-Z][A-Z0-9_]{2,}\s*=\s*\S+")
_RCLONE_CONTENT_RE = re.compile(
    r"(?ms)^\s*\[[^\]\r\n]+\]\s*(?:\r?\n)+\s*(?:type|token|client_id|client_secret)\s*="
)
_MAX_STATE_BYTES = 8 * 1024 * 1024
_PRODUCTION_HOST_ROOT = Path("/")
_SHA256_RE = re.compile(r"\A[0-9a-f]{64}\Z")
_GIT_COMMIT_RE = re.compile(r"\A[0-9a-f]{40}\Z")
_SOURCE_MANIFEST = "deploy/linux/degen-prod-db-backup-assets.sha256"
_MAX_SOURCE_ARCHIVE_BYTES = 16 * 1024 * 1024
_MAX_SOURCE_FILE_BYTES = 8 * 1024 * 1024
_MAX_SOURCE_MANIFEST_BYTES = 64 * 1024
_TAR_BLOCK_BYTES = 512
_GIT_TAR_RECORD_BYTES = 20 * _TAR_BLOCK_BYTES
_GIT_EXECUTABLE = "/usr/bin/git"
_PSQL_EXECUTABLE = "/usr/bin/psql"
_PG_RESTORE_EXECUTABLE = "/usr/bin/pg_restore"
_HOSTNAME_EXECUTABLE = "/bin/hostname"
_SYSTEMCTL_EXECUTABLE = "/usr/bin/systemctl"
_LOGINCTL_EXECUTABLE = "/usr/bin/loginctl"
_DATE_EXECUTABLE = "/usr/bin/date"
_MAX_COMMAND_OUTPUT_BYTES = 4096
_MAX_DATABASE_URL_BYTES = 4096
_MAX_APP_ENV_BYTES = 256 * 1024
_MAX_BACKUP_ENTRIES = 4096
_MAX_BACKUP_DUMP_BYTES = 1 << 50
_MAX_STAGED_MANIFEST_BYTES = 64 * 1024
_MAX_SNAPSHOT_FILE_BYTES = 8 * 1024 * 1024
_MAX_SNAPSHOT_ARTIFACTS = 16
_BACKUP_NAME_RE = re.compile(
    r"\A(?P<prefix>[A-Za-z0-9._-]+_)(?P<stamp>[0-9]{8}T[0-9]{6}Z)"
    r"\.dump(?P<sidecar>\.sha256)?\Z",
    re.ASCII,
)
_SAFE_LABEL_RE = re.compile(r"\A[A-Za-z0-9._-]{1,128}\Z", re.ASCII)
_SYSTEMD_UNIT_RE = re.compile(r"\A[A-Za-z0-9_.@-]{1,192}\.service\Z", re.ASCII)
_LOGIN_USER_RE = re.compile(r"\A[A-Za-z_][A-Za-z0-9_-]{0,31}\Z", re.ASCII)
_BACKUP_PREFIX_RE = re.compile(r"\A[A-Za-z0-9._-]+_\Z", re.ASCII)
_EFFECTIVE_CONFIG_KEYS = frozenset(
    {
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
)
_APP_ENV_ASSIGNMENT_RE = re.compile(
    r"\A(?P<key>[A-Za-z_][A-Za-z0-9_]*)=(?P<value>[^\r\n]*)\Z",
    re.ASCII,
)
_INHERITED_FD_EXEC_SHIM = """import os
import sys
mode = sys.argv[1]
fd = int(sys.argv[2])
executable = sys.argv[3]
argv = sys.argv[4:]
if mode == "git-stdin":
    os.dup2(fd, 0)
    if fd != 0:
        os.close(fd)
    os.execve(executable, argv, {})
if mode == "pgdatabase":
    chunks = []
    total = 0
    while True:
        chunk = os.read(fd, 4096)
        if not chunk:
            break
        total += len(chunk)
        if total > 4096:
            raise SystemExit(125)
        chunks.append(chunk)
    os.close(fd)
    value = b"".join(chunks).decode("utf-8", errors="strict")
    if not value or "\\x00" in value or "\\r" in value or "\\n" in value:
        raise SystemExit(125)
    os.execve(executable, argv, {"PGDATABASE": value})
raise SystemExit(125)
"""
_TOP_LEVEL_KEYS = frozenset(
    {
        "schema_version",
        "operation_id",
        "operation_dir",
        "phase",
        "phase_history",
        "reviewed_source",
        "effective_config",
        "host_stage",
        "snapshot",
        "prior_runtime",
        "install",
        "rclone_evidence_groups",
        "probe",
        "dry_run",
        "policy",
        "observation",
        "active_transaction",
        "failure",
        "secondary_errors",
        "recovery",
    }
)
_PHASES = frozenset(
    {
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
        "recovering",
        "recovering_policy",
        "recovering_probe",
        "recovering_guard",
        "manual_rollback",
        "recovery_required",
        "rolled_back",
    }
)
_TRANSACTION_KINDS = frozenset({"probe", "dry_run", "observe", "policy"})
_PRIOR_STABLE_PHASES = frozenset(
    {"installed", "probed", "dry_run_recorded", "policy_enabled"}
)
_RECOVERY_KINDS = frozenset(
    {"install", "policy", "manual_rollback", "probe", "guard"}
)
_TARGET_ORDER = (
    "/usr/local/sbin/degen-prod-db-backup",
    "/usr/local/sbin/degen-prod-db-retention",
    "/usr/local/sbin/degen-prod-db-backup-env",
    "/usr/local/sbin/degen-prod-db-backup-ops",
    "/etc/systemd/system/degen-prod-db-backup.service",
    "/etc/systemd/system/degen-prod-db-backup.timer",
    "/etc/degen/prod-db-backup.env",
)
_SNAPSHOT_TARGET_NAMES = {
    "/usr/local/sbin/degen-prod-db-backup": "degen-prod-db-backup",
    "/usr/local/sbin/degen-prod-db-retention": "degen-prod-db-retention",
    "/usr/local/sbin/degen-prod-db-backup-env": "degen-prod-db-backup-env",
    "/usr/local/sbin/degen-prod-db-backup-ops": "degen-prod-db-backup-ops",
    "/etc/systemd/system/degen-prod-db-backup.service": "degen-prod-db-backup.service",
    "/etc/systemd/system/degen-prod-db-backup.timer": "degen-prod-db-backup.timer",
    "/etc/degen/prod-db-backup.env": "prod-db-backup.env",
}
_RCLONE_CONFIG_PATH = "/etc/degen/rclone.conf"
_RCLONE_AUDIT_NAME = "rclone.conf.audit"
_SNAPSHOT_MANIFEST_NAME = "SHA256SUMS"
_SOURCE_TO_TARGET = (
    ("deploy/linux/degen-prod-db-backup.sh", _TARGET_ORDER[0]),
    ("deploy/linux/degen-prod-db-retention.py", _TARGET_ORDER[1]),
    ("deploy/linux/degen-prod-db-backup-env.py", _TARGET_ORDER[2]),
    ("deploy/linux/degen-prod-db-backup-ops.py", _TARGET_ORDER[3]),
    ("deploy/systemd/degen-prod-db-backup.service", _TARGET_ORDER[4]),
    ("deploy/systemd/degen-prod-db-backup.timer", _TARGET_ORDER[5]),
)
_SOURCE_ASSETS = frozenset(
    {
        *(source for source, _ in _SOURCE_TO_TARGET),
        "deploy/systemd/degen-prod-db-backup.env.example",
    }
)
_SOURCE_FILES = frozenset({*_SOURCE_ASSETS, _SOURCE_MANIFEST})
_SOURCE_DIRECTORIES = frozenset({"deploy", "deploy/linux", "deploy/systemd"})
_EXECUTABLE_SOURCE_ASSETS = frozenset(
    source for source in _SOURCE_ASSETS if source.startswith("deploy/linux/")
)
_NORMAL_PHASE_ORDER = (
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


class OperationStateError(ValueError):
    """Raised when operation state or its storage is not trustworthy."""


@dataclass(frozen=True)
class OperationPaths:
    operation_dir: Path
    source_archive: Path
    source_dir: Path
    snapshot_dir: Path
    staged_dir: Path
    state_file: Path


@dataclass(frozen=True)
class OperationsContext:
    operation_id: str
    paths: OperationPaths
    effective_uid: int
    command_runner: CommandRunner
    clock: Clock
    expected_commit: str
    expected_archive_sha256: str
    expected_manifest_sha256: str
    host_root: Path


def build_operation_paths(operation_dir: Path) -> OperationPaths:
    return OperationPaths(
        operation_dir=operation_dir,
        source_archive=operation_dir / "source.tar",
        source_dir=operation_dir / "source",
        snapshot_dir=operation_dir / "snapshot",
        staged_dir=operation_dir / "staged",
        state_file=operation_dir / "operation-state.json",
    )


def _same_identity(first: os.stat_result, second: os.stat_result) -> bool:
    return (first.st_dev, first.st_ino) == (second.st_dev, second.st_ino)


def _validate_operation_dir_metadata(
    metadata: os.stat_result,
    effective_uid: int,
    *,
    direct_test_fallback: bool = False,
) -> None:
    if not stat.S_ISDIR(metadata.st_mode):
        raise OperationStateError("operation path is not a directory")
    if metadata.st_uid != effective_uid:
        raise OperationStateError("operation directory is not owned by the effective UID")
    expected_mode = 0o777 if direct_test_fallback and os.name != "posix" else 0o700
    if stat.S_IMODE(metadata.st_mode) != expected_mode:
        raise OperationStateError("operation directory must have mode 0700")


def _validate_state_file_metadata(
    metadata: os.stat_result,
    effective_uid: int,
    *,
    direct_test_fallback: bool = False,
) -> None:
    if not stat.S_ISREG(metadata.st_mode):
        raise OperationStateError("operation state is not a regular file")
    if metadata.st_uid != effective_uid:
        raise OperationStateError("operation state is not owned by the effective UID")
    if metadata.st_nlink != 1:
        raise OperationStateError("operation state must have a single link")
    expected_mode = 0o666 if direct_test_fallback and os.name != "posix" else 0o600
    if stat.S_IMODE(metadata.st_mode) != expected_mode:
        raise OperationStateError("operation state must have mode 0600")


def _descriptor_primitives_available() -> bool:
    required_flags = ("O_NOFOLLOW", "O_DIRECTORY", "O_CLOEXEC")
    return (
        os.name == "posix"
        and all(hasattr(os, name) for name in required_flags)
        and os.open in getattr(os, "supports_dir_fd", set())
        and os.stat in getattr(os, "supports_dir_fd", set())
        and os.stat in getattr(os, "supports_follow_symlinks", set())
    )


def _require_posix_descriptor_primitives() -> None:
    if not _descriptor_primitives_available():
        raise OperationStateError("required POSIX descriptor primitives are unavailable")


def _path_components(path: Path) -> tuple[str, ...]:
    if not path.is_absolute():
        raise OperationStateError("operation directory must be absolute")
    components = path.parts
    if any(component in (".", "..") for component in components):
        raise OperationStateError("operation directory contains an unsafe component")
    return components


@contextlib.contextmanager
def _open_operation_dir_posix(path: Path, effective_uid: int):
    _require_posix_descriptor_primitives()
    components = _path_components(path)
    root = components[0]
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW
    descriptors: list[int] = []
    try:
        root_fd = os.open(root, flags)
        descriptors.append(root_fd)
        current_fd = root_fd
        for component in components[1:]:
            named = os.stat(component, dir_fd=current_fd, follow_symlinks=False)
            if stat.S_ISLNK(named.st_mode):
                raise OperationStateError("operation path contains a symlink component")
            child_fd = os.open(component, flags, dir_fd=current_fd)
            descriptors.append(child_fd)
            opened = os.fstat(child_fd)
            if not _same_identity(named, opened):
                raise OperationStateError("operation directory component changed while opening")
            if not stat.S_ISDIR(opened.st_mode):
                raise OperationStateError("operation path component is not a directory")
            current_fd = child_fd
        final_metadata = os.fstat(current_fd)
        named_final = os.stat(
            components[-1],
            dir_fd=descriptors[-2] if len(descriptors) > 1 else None,
            follow_symlinks=False,
        ) if len(components) > 1 else final_metadata
        if not _same_identity(named_final, final_metadata):
            raise OperationStateError("operation directory binding changed while opening")
        _validate_operation_dir_metadata(final_metadata, effective_uid)
        yield current_fd
    except OSError as exc:
        raise OperationStateError("operation directory descriptor validation failed") from exc
    finally:
        for descriptor in reversed(descriptors):
            try:
                os.close(descriptor)
            except OSError:
                pass


def _walk_operation_dir_fallback(path: Path, effective_uid: int) -> os.stat_result:
    components = _path_components(path)
    current = Path(components[0])
    for component in components[1:]:
        current = current / component
        try:
            metadata = current.lstat()
        except OSError as exc:
            raise OperationStateError("operation directory validation failed") from exc
        if stat.S_ISLNK(metadata.st_mode):
            raise OperationStateError("operation path contains a symlink component")
        if not stat.S_ISDIR(metadata.st_mode):
            raise OperationStateError("operation path component is not a directory")
    final_metadata = path.lstat()
    _validate_operation_dir_metadata(
        final_metadata,
        effective_uid,
        direct_test_fallback=True,
    )
    return final_metadata


@contextlib.contextmanager
def _open_validated_operation_dir(path: Path, effective_uid: int):
    if _descriptor_primitives_available():
        with _open_operation_dir_posix(path, effective_uid) as descriptor:
            yield descriptor
    else:
        _walk_operation_dir_fallback(path, effective_uid)
        yield None


def _revalidate_operation_dir_binding(
    path: Path,
    directory_fd: int | None,
    original_metadata: os.stat_result,
    effective_uid: int,
) -> None:
    try:
        if directory_fd is None:
            fresh_metadata = _walk_operation_dir_fallback(path, effective_uid)
            if not _same_identity(fresh_metadata, original_metadata):
                raise OperationStateError("operation directory binding changed")
            return

        held_metadata = os.fstat(directory_fd)
        _validate_operation_dir_metadata(held_metadata, effective_uid)
        if not _same_identity(held_metadata, original_metadata):
            raise OperationStateError("operation directory binding changed")
        with _open_operation_dir_posix(path, effective_uid) as fresh_fd:
            fresh_metadata = os.fstat(fresh_fd)
            if not _same_identity(fresh_metadata, held_metadata):
                raise OperationStateError("operation directory binding changed")
    except (OSError, OperationStateError) as exc:
        raise OperationStateError("operation directory binding revalidation failed") from exc


def validate_operation_dir(path: Path, *, effective_uid: int) -> None:
    with _open_validated_operation_dir(path, effective_uid):
        return


def load_operation_state(path: Path, *, effective_uid: int) -> dict[str, object]:
    if path.name != "operation-state.json":
        raise OperationStateError("operation state path must end in operation-state.json")
    operation_dir = path.parent
    with _open_validated_operation_dir(operation_dir, effective_uid) as directory_fd:
        if directory_fd is None:
            raw = _read_state_file_fallback(path, effective_uid)
        else:
            raw = _read_state_file_posix(directory_fd, effective_uid)
    state = _decode_operation_state(raw)
    validate_operation_state(state, operation_dir)
    return state


def _reject_duplicate_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise OperationStateError("operation state contains a duplicate JSON key")
        result[key] = value
    return result


def _reject_nonfinite(value: str) -> object:
    raise OperationStateError("operation state contains a non-finite number")


def _decode_operation_state(raw: bytes) -> dict[str, object]:
    try:
        text = raw.decode("utf-8")
        state = json.loads(
            text,
            object_pairs_hook=_reject_duplicate_pairs,
            parse_constant=_reject_nonfinite,
        )
    except OperationStateError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OperationStateError("operation state is not valid UTF-8 JSON") from exc
    if type(state) is not dict:
        raise OperationStateError("operation state must be a JSON object")
    return state


def _stable_file_metadata(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_nlink,
    )


def _read_bounded(descriptor: int) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = os.read(descriptor, 64 * 1024)
        if not chunk:
            break
        total += len(chunk)
        if total > _MAX_STATE_BYTES:
            raise OperationStateError("operation state exceeds the size limit")
        chunks.append(chunk)
    return b"".join(chunks)


def _read_state_file_posix(directory_fd: int, effective_uid: int) -> bytes:
    name = "operation-state.json"
    flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
    try:
        named_before = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if stat.S_ISLNK(named_before.st_mode):
            raise OperationStateError("operation state must not be a symlink")
        descriptor = os.open(name, flags, dir_fd=directory_fd)
    except OSError as exc:
        raise OperationStateError("operation state descriptor validation failed") from exc
    try:
        opened_before = os.fstat(descriptor)
        if not _same_identity(named_before, opened_before):
            raise OperationStateError("operation state binding changed while opening")
        _validate_state_file_metadata(opened_before, effective_uid)
        raw = _read_bounded(descriptor)
        opened_after = os.fstat(descriptor)
        named_after = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if _stable_file_metadata(opened_before) != _stable_file_metadata(opened_after):
            raise OperationStateError("operation state changed while reading")
        if not _same_identity(opened_after, named_after):
            raise OperationStateError("operation state path changed while reading")
        return raw
    except OSError as exc:
        raise OperationStateError("operation state read failed") from exc
    finally:
        os.close(descriptor)


def _read_state_file_fallback(path: Path, effective_uid: int) -> bytes:
    try:
        named_before = path.lstat()
        if stat.S_ISLNK(named_before.st_mode):
            raise OperationStateError("operation state must not be a symlink")
        with path.open("rb") as stream:
            opened_before = os.fstat(stream.fileno())
            if not _same_identity(named_before, opened_before):
                raise OperationStateError("operation state binding changed while opening")
            _validate_state_file_metadata(
                opened_before,
                effective_uid,
                direct_test_fallback=True,
            )
            raw = stream.read(_MAX_STATE_BYTES + 1)
            if len(raw) > _MAX_STATE_BYTES:
                raise OperationStateError("operation state exceeds the size limit")
            opened_after = os.fstat(stream.fileno())
        named_after = path.lstat()
    except OSError as exc:
        raise OperationStateError("operation state read failed") from exc
    if _stable_file_metadata(opened_before) != _stable_file_metadata(opened_after):
        raise OperationStateError("operation state changed while reading")
    if not _same_identity(opened_after, named_after):
        raise OperationStateError("operation state path changed while reading")
    return raw


def atomic_write_operation_state(
    path: Path,
    state: dict[str, object],
    *,
    effective_uid: int,
) -> None:
    _atomic_write_operation_state_internal(
        path,
        state,
        effective_uid=effective_uid,
        pre_replace_validator=None,
    )


def _atomic_write_operation_state_internal(
    path: Path,
    state: dict[str, object],
    *,
    effective_uid: int,
    pre_replace_validator: PreReplaceValidator | None,
    operation_directory_binding: _OperationDirectoryBinding | None = None,
) -> None:
    if path.name != "operation-state.json":
        raise OperationStateError("operation state path must end in operation-state.json")
    operation_dir = path.parent
    validate_operation_state(state, operation_dir)
    canonical = _canonical_state_bytes(state)
    if len(canonical) > _MAX_STATE_BYTES:
        raise OperationStateError("operation state exceeds the size limit")
    if operation_directory_binding is not None:
        _atomic_write_with_operation_directory_binding(
            path,
            state,
            canonical,
            effective_uid,
            operation_directory_binding,
            pre_replace_validator,
        )
        return
    with _open_validated_operation_dir(operation_dir, effective_uid) as directory_fd:
        operation_metadata = (
            operation_dir.lstat() if directory_fd is None else os.fstat(directory_fd)
        )
        binding = _OperationDirectoryBinding(
            operation_dir,
            directory_fd,
            operation_metadata,
        )
        _atomic_write_with_operation_directory_binding(
            path,
            state,
            canonical,
            effective_uid,
            binding,
            pre_replace_validator,
        )


def _atomic_write_with_operation_directory_binding(
    path: Path,
    state: dict[str, object],
    canonical: bytes,
    effective_uid: int,
    binding: _OperationDirectoryBinding,
    pre_replace_validator: PreReplaceValidator | None,
) -> None:
    if binding.path != path.parent:
        raise OperationStateError("operation directory binding path does not match state path")
    _revalidate_operation_dir_binding(
        binding.path,
        binding.descriptor,
        binding.metadata,
        effective_uid,
    )
    with _exclusive_operation_state_lock(binding.descriptor):
        _revalidate_operation_dir_binding(
            binding.path,
            binding.descriptor,
            binding.metadata,
            effective_uid,
        )
        _atomic_write_under_lock(
            path,
            state,
            canonical,
            effective_uid,
            binding.descriptor,
            binding.metadata,
            pre_replace_validator,
        )


@contextlib.contextmanager
def _exclusive_operation_state_lock(directory_fd: int | None):
    if directory_fd is None:
        yield
        return
    try:
        import fcntl
    except ImportError as exc:
        raise OperationStateError("required POSIX flock primitive is unavailable") from exc
    try:
        fcntl.flock(directory_fd, fcntl.LOCK_EX)
        yield
    except OSError as exc:
        raise OperationStateError("operation state lock failed") from exc
    finally:
        try:
            fcntl.flock(directory_fd, fcntl.LOCK_UN)
        except OSError:
            pass


def _atomic_write_under_lock(
    path: Path,
    state: dict[str, object],
    canonical: bytes,
    effective_uid: int,
    directory_fd: int | None,
    operation_metadata: os.stat_result,
    pre_replace_validator: PreReplaceValidator | None,
) -> None:
    operation_dir = path.parent
    _revalidate_operation_dir_binding(
        operation_dir,
        directory_fd,
        operation_metadata,
        effective_uid,
    )
    if directory_fd is None:
        existing = _capture_existing_fallback(path, effective_uid)
    else:
        existing = _capture_existing_posix(directory_fd, operation_dir, effective_uid)
    if existing is None:
        history = state["phase_history"]
        assert isinstance(history, list)
        if state["phase"] != "source_verified" or len(history) != 1:
            raise OperationStateError("absent operation state accepts only the initial source_verified receipt")
    else:
        old_state, _, _ = existing
        validate_operation_state(state, operation_dir, old_state)
        if state == old_state:
            return
    if directory_fd is None:
        _atomic_write_fallback(
            path,
            canonical,
            existing,
            effective_uid,
            operation_metadata,
            pre_replace_validator,
        )
    else:
        _atomic_write_posix(
            directory_fd,
            operation_dir,
            canonical,
            existing,
            effective_uid,
            operation_metadata,
            pre_replace_validator,
        )


def _canonical_state_bytes(state: dict[str, object]) -> bytes:
    try:
        text = json.dumps(
            state,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise OperationStateError("operation state cannot be serialized canonically") from exc
    return (text + "\n").encode("utf-8")


def _atomic_event_hook(event: str, **details: object) -> None:
    """Internal fault/race seam used only by direct unit tests."""


def _capture_existing_posix(
    directory_fd: int,
    operation_dir: Path,
    effective_uid: int,
) -> tuple[dict[str, object], bytes, os.stat_result] | None:
    try:
        named = os.stat("operation-state.json", dir_fd=directory_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise OperationStateError("operation state identity check failed") from exc
    raw = _read_state_file_posix(directory_fd, effective_uid)
    named_after = os.stat("operation-state.json", dir_fd=directory_fd, follow_symlinks=False)
    if not _same_identity(named, named_after):
        raise OperationStateError("operation state changed while capturing compare-and-swap input")
    state = _decode_operation_state(raw)
    validate_operation_state(state, operation_dir)
    return state, raw, named_after


def _capture_existing_fallback(
    path: Path,
    effective_uid: int,
) -> tuple[dict[str, object], bytes, os.stat_result] | None:
    try:
        named = path.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise OperationStateError("operation state identity check failed") from exc
    raw = _read_state_file_fallback(path, effective_uid)
    named_after = path.lstat()
    if not _same_identity(named, named_after):
        raise OperationStateError("operation state changed while capturing compare-and-swap input")
    state = _decode_operation_state(raw)
    validate_operation_state(state, path.parent)
    return state, raw, named_after


def _write_all(descriptor: int, data: bytes) -> None:
    offset = 0
    while offset < len(data):
        written = os.write(descriptor, data[offset:])
        if written <= 0:
            raise OperationStateError("operation state temporary write made no progress")
        offset += written


def _new_temp_name() -> str:
    return f".operation-state.json.{secrets.token_hex(16)}.tmp"


def _temp_identity_matches_posix(directory_fd: int, name: str, metadata: os.stat_result) -> bool:
    try:
        current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    except OSError:
        return False
    return _same_identity(current, metadata) and _stable_file_metadata(
        current
    ) == _stable_file_metadata(metadata)


def _temp_contents_match_posix(
    directory_fd: int,
    name: str,
    metadata: os.stat_result,
    canonical: bytes,
    effective_uid: int,
) -> bool:
    descriptor: int | None = None
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC
    try:
        named_before = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        _validate_state_file_metadata(named_before, effective_uid)
        if not _same_identity(named_before, metadata):
            return False
        descriptor = os.open(name, flags, dir_fd=directory_fd)
        opened_before = os.fstat(descriptor)
        _validate_state_file_metadata(opened_before, effective_uid)
        if not _same_identity(metadata, opened_before):
            return False
        raw = _read_bounded(descriptor)
        opened_after = os.fstat(descriptor)
        named_after = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        _validate_state_file_metadata(opened_after, effective_uid)
        _validate_state_file_metadata(named_after, effective_uid)
        if (
            opened_after.st_size != len(canonical)
            or not _same_identity(metadata, opened_after)
            or not _same_identity(opened_after, named_after)
        ):
            return False
        return raw == canonical
    except (OSError, OperationStateError):
        return False
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass


def _cleanup_temp_posix(directory_fd: int, name: str, metadata: os.stat_result | None) -> None:
    if metadata is None:
        return
    try:
        current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    except OSError:
        return
    if not _same_identity(current, metadata):
        return
    try:
        os.unlink(name, dir_fd=directory_fd)
    except OSError:
        pass


def _cas_matches_posix(
    directory_fd: int,
    operation_dir: Path,
    effective_uid: int,
    existing: tuple[dict[str, object], bytes, os.stat_result] | None,
) -> bool:
    current = _capture_existing_posix(directory_fd, operation_dir, effective_uid)
    if existing is None or current is None:
        return existing is None and current is None
    return _same_identity(existing[2], current[2]) and existing[1] == current[1]


def _atomic_write_posix(
    directory_fd: int,
    operation_dir: Path,
    canonical: bytes,
    existing: tuple[dict[str, object], bytes, os.stat_result] | None,
    effective_uid: int,
    operation_metadata: os.stat_result,
    pre_replace_validator: PreReplaceValidator | None,
) -> None:
    if not _write_descriptor_primitives_available():
        raise OperationStateError("required POSIX atomic-write primitives are unavailable")
    _revalidate_operation_dir_binding(
        operation_dir,
        directory_fd,
        operation_metadata,
        effective_uid,
    )
    name = _new_temp_name()
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
    descriptor: int | None = None
    temp_metadata: os.stat_result | None = None
    replaced = False
    try:
        descriptor = os.open(name, flags, 0o600, dir_fd=directory_fd)
        os.fchmod(descriptor, 0o600)
        temp_metadata = os.fstat(descriptor)
        _validate_state_file_metadata(temp_metadata, effective_uid)
        named = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if not _same_identity(temp_metadata, named):
            raise OperationStateError("temporary operation state path/descriptor binding failed")
        _atomic_event_hook("after_temp_open", temp_path=str(operation_dir / name))
        _write_all(descriptor, canonical)
        os.fsync(descriptor)
        written = os.fstat(descriptor)
        if written.st_size != len(canonical) or not _same_identity(temp_metadata, written):
            raise OperationStateError("temporary operation state changed while writing")
        temp_metadata = written
        _atomic_event_hook("after_temp_fsync", temp_path=str(operation_dir / name))
        _atomic_event_hook("before_cas", temp_path=str(operation_dir / name))
        _atomic_event_hook("before_replace", temp_path=str(operation_dir / name))
        if not _temp_identity_matches_posix(directory_fd, name, temp_metadata):
            raise OperationStateError("temporary operation state binding changed before replacement")
        if not _temp_contents_match_posix(
            directory_fd,
            name,
            temp_metadata,
            canonical,
            effective_uid,
        ):
            raise OperationStateError("temporary operation state bytes changed before replacement")
        if not _cas_matches_posix(directory_fd, operation_dir, effective_uid, existing):
            raise OperationStateError("operation state compare-and-swap check failed")
        if pre_replace_validator is not None:
            pre_replace_validator()
        _revalidate_operation_dir_binding(
            operation_dir,
            directory_fd,
            operation_metadata,
            effective_uid,
        )
        if not _temp_identity_matches_posix(directory_fd, name, temp_metadata):
            raise OperationStateError("temporary operation state binding changed before replacement")
        if not _temp_contents_match_posix(
            directory_fd,
            name,
            temp_metadata,
            canonical,
            effective_uid,
        ):
            raise OperationStateError("temporary operation state bytes changed before replacement")
        if not _cas_matches_posix(directory_fd, operation_dir, effective_uid, existing):
            raise OperationStateError("operation state compare-and-swap check failed")
        os.replace(
            name,
            "operation-state.json",
            src_dir_fd=directory_fd,
            dst_dir_fd=directory_fd,
        )
        replaced = True
        _atomic_event_hook(
            "after_replace",
            path=str(operation_dir / "operation-state.json"),
        )
        _revalidate_operation_dir_binding(
            operation_dir,
            directory_fd,
            operation_metadata,
            effective_uid,
        )
        installed = os.stat(
            "operation-state.json",
            dir_fd=directory_fd,
            follow_symlinks=False,
        )
        if not _same_identity(installed, temp_metadata):
            raise OperationStateError("final destination inode does not match the temporary inode")
        os.fsync(directory_fd)
        _atomic_event_hook("after_parent_fsync", path=str(operation_dir))
        final_raw = _read_state_file_posix(directory_fd, effective_uid)
        if final_raw != canonical:
            raise OperationStateError("operation state post-replacement bytes are not canonical")
        final_state = _decode_operation_state(final_raw)
        validate_operation_state(final_state, operation_dir)
        final_metadata = os.stat(
            "operation-state.json",
            dir_fd=directory_fd,
            follow_symlinks=False,
        )
        if not _same_identity(final_metadata, temp_metadata):
            raise OperationStateError("final destination inode does not match the temporary inode")
        _revalidate_operation_dir_binding(
            operation_dir,
            directory_fd,
            operation_metadata,
            effective_uid,
        )
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass
        if not replaced:
            _cleanup_temp_posix(directory_fd, name, temp_metadata)


def _write_descriptor_primitives_available(
    supported: set[object] | None = None,
) -> bool:
    capabilities = getattr(os, "supports_dir_fd", set()) if supported is None else supported
    # CPython exposes os.replace(..., src_dir_fd=, dst_dir_fd=) through the
    # same renameat capability represented by os.rename in supports_dir_fd.
    return {os.open, os.stat, os.unlink, os.rename}.issubset(capabilities)


def _temp_identity_matches_fallback(path: Path, metadata: os.stat_result) -> bool:
    try:
        current = path.lstat()
    except OSError:
        return False
    return _same_identity(current, metadata) and _stable_file_metadata(
        current
    ) == _stable_file_metadata(metadata)


def _temp_contents_match_fallback(
    path: Path,
    metadata: os.stat_result,
    canonical: bytes,
    effective_uid: int,
) -> bool:
    descriptor: int | None = None
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        named_before = path.lstat()
        _validate_state_file_metadata(
            named_before,
            effective_uid,
            direct_test_fallback=True,
        )
        if not _same_identity(named_before, metadata):
            return False
        descriptor = os.open(path, flags)
        opened_before = os.fstat(descriptor)
        _validate_state_file_metadata(
            opened_before,
            effective_uid,
            direct_test_fallback=True,
        )
        if not _same_identity(metadata, opened_before):
            return False
        raw = _read_bounded(descriptor)
        opened_after = os.fstat(descriptor)
        named_after = path.lstat()
        _validate_state_file_metadata(
            opened_after,
            effective_uid,
            direct_test_fallback=True,
        )
        _validate_state_file_metadata(
            named_after,
            effective_uid,
            direct_test_fallback=True,
        )
        if (
            opened_after.st_size != len(canonical)
            or not _same_identity(metadata, opened_after)
            or not _same_identity(opened_after, named_after)
        ):
            return False
        return raw == canonical
    except (OSError, OperationStateError):
        return False
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass


def _cleanup_temp_fallback(path: Path, metadata: os.stat_result | None) -> None:
    if metadata is None:
        return
    try:
        current = path.lstat()
    except OSError:
        return
    if not _same_identity(current, metadata):
        return
    try:
        path.unlink()
    except OSError:
        pass


def _cas_matches_fallback(
    path: Path,
    effective_uid: int,
    existing: tuple[dict[str, object], bytes, os.stat_result] | None,
) -> bool:
    current = _capture_existing_fallback(path, effective_uid)
    if existing is None or current is None:
        return existing is None and current is None
    return _same_identity(existing[2], current[2]) and existing[1] == current[1]


def _atomic_write_fallback(
    path: Path,
    canonical: bytes,
    existing: tuple[dict[str, object], bytes, os.stat_result] | None,
    effective_uid: int,
    operation_metadata: os.stat_result,
    pre_replace_validator: PreReplaceValidator | None,
) -> None:
    _revalidate_operation_dir_binding(
        path.parent,
        None,
        operation_metadata,
        effective_uid,
    )
    name = _new_temp_name()
    temp_path = path.parent / name
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_BINARY", 0)
    )
    descriptor: int | None = None
    temp_metadata: os.stat_result | None = None
    replaced = False
    try:
        descriptor = os.open(temp_path, flags, 0o600)
        try:
            os.chmod(temp_path, 0o600)
        except OSError as exc:
            raise OperationStateError("temporary operation state mode setup failed") from exc
        temp_metadata = os.fstat(descriptor)
        _validate_state_file_metadata(
            temp_metadata,
            effective_uid,
            direct_test_fallback=True,
        )
        if not _temp_identity_matches_fallback(temp_path, temp_metadata):
            raise OperationStateError("temporary operation state path/descriptor binding failed")
        _atomic_event_hook("after_temp_open", temp_path=str(temp_path))
        _write_all(descriptor, canonical)
        os.fsync(descriptor)
        written = os.fstat(descriptor)
        if written.st_size != len(canonical) or not _same_identity(temp_metadata, written):
            raise OperationStateError("temporary operation state changed while writing")
        temp_metadata = written
        _atomic_event_hook("after_temp_fsync", temp_path=str(temp_path))
        os.close(descriptor)
        descriptor = None
        try:
            closed_metadata = temp_path.lstat()
        except OSError as exc:
            raise OperationStateError(
                "temporary operation state metadata read failed after close"
            ) from exc
        if (
            not _same_identity(temp_metadata, closed_metadata)
            or closed_metadata.st_size != len(canonical)
        ):
            raise OperationStateError(
                "temporary operation state binding changed while closing"
            )
        _validate_state_file_metadata(
            closed_metadata,
            effective_uid,
            direct_test_fallback=True,
        )
        temp_metadata = closed_metadata
        _atomic_event_hook("before_cas", temp_path=str(temp_path))
        _atomic_event_hook("before_replace", temp_path=str(temp_path))
        if not _temp_identity_matches_fallback(temp_path, temp_metadata):
            raise OperationStateError("temporary operation state binding changed before replacement")
        if not _temp_contents_match_fallback(
            temp_path,
            temp_metadata,
            canonical,
            effective_uid,
        ):
            raise OperationStateError("temporary operation state bytes changed before replacement")
        if not _cas_matches_fallback(path, effective_uid, existing):
            raise OperationStateError("operation state compare-and-swap check failed")
        if pre_replace_validator is not None:
            pre_replace_validator()
        _revalidate_operation_dir_binding(
            path.parent,
            None,
            operation_metadata,
            effective_uid,
        )
        if not _temp_identity_matches_fallback(temp_path, temp_metadata):
            raise OperationStateError("temporary operation state binding changed before replacement")
        if not _temp_contents_match_fallback(
            temp_path,
            temp_metadata,
            canonical,
            effective_uid,
        ):
            raise OperationStateError("temporary operation state bytes changed before replacement")
        if not _cas_matches_fallback(path, effective_uid, existing):
            raise OperationStateError("operation state compare-and-swap check failed")
        os.replace(temp_path, path)
        replaced = True
        _atomic_event_hook("after_replace", path=str(path))
        _revalidate_operation_dir_binding(
            path.parent,
            None,
            operation_metadata,
            effective_uid,
        )
        installed = path.lstat()
        if not _same_identity(installed, temp_metadata):
            raise OperationStateError("final destination inode does not match the temporary inode")
        _atomic_event_hook("after_parent_fsync", path=str(path.parent))
        final_raw = _read_state_file_fallback(path, effective_uid)
        if final_raw != canonical:
            raise OperationStateError("operation state post-replacement bytes are not canonical")
        final_state = _decode_operation_state(final_raw)
        validate_operation_state(final_state, path.parent)
        final_metadata = path.lstat()
        if not _same_identity(final_metadata, temp_metadata):
            raise OperationStateError("final destination inode does not match the temporary inode")
        _revalidate_operation_dir_binding(
            path.parent,
            None,
            operation_metadata,
            effective_uid,
        )
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass
        if not replaced:
            _cleanup_temp_fallback(temp_path, temp_metadata)


def validate_operation_state(
    state: object,
    operation_dir: Path,
    previous_state: dict[str, object] | None = None,
) -> None:
    _reject_residual_secrets(state)
    _validate_state_schema(state)
    assert isinstance(state, dict)
    if not operation_dir.is_absolute():
        raise OperationStateError("operation_dir validation root must be absolute")
    if any(component in (".", "..") for component in operation_dir.parts):
        raise OperationStateError("operation_dir contains an unsafe lexical component")
    if state["operation_dir"] != str(operation_dir):
        raise OperationStateError("operation_dir does not match the validated operation path")
    history = state["phase_history"]
    assert isinstance(history, list)
    if not history or history[-1]["phase"] != state["phase"]:
        raise OperationStateError("phase_history must end at the current phase")
    _validate_receipt_phase_rules(state)
    _validate_lifecycle_epochs(state)
    _validate_phase_history_graph(state)
    if previous_state is not None:
        validate_operation_state(previous_state, operation_dir)
        _validate_state_transition(previous_state, state)


def _require_object(value: object, keys: frozenset[str], label: str) -> dict[str, object]:
    if type(value) is not dict:
        raise OperationStateError(f"{label} must be an object with exact keys")
    actual = frozenset(value)
    if actual != keys:
        raise OperationStateError(f"{label} has invalid keys")
    return value


def _require_string(value: object, label: str, *, nonempty: bool = False) -> str:
    if type(value) is not str or (nonempty and not value):
        raise OperationStateError(f"{label} must be a{' non-empty' if nonempty else ''} string")
    return value


def _require_int(value: object, label: str, *, minimum: int | None = 0) -> int:
    if type(value) is not int or (minimum is not None and value < minimum):
        raise OperationStateError(f"{label} must be an integer")
    return value


def _require_optional_int(value: object, label: str) -> int | None:
    if value is None:
        return None
    return _require_int(value, label)


def _require_bool(value: object, label: str) -> bool:
    if type(value) is not bool:
        raise OperationStateError(f"{label} must be a boolean")
    return value


def _require_hash(value: object, label: str) -> str:
    text = _require_string(value, label)
    if _SHA256_RE.fullmatch(text) is None:
        raise OperationStateError(f"{label} must be a lowercase SHA-256 digest")
    return text


def _require_optional_hash(value: object, label: str) -> str | None:
    if value is None:
        return None
    return _require_hash(value, label)


def _require_string_list(value: object, label: str) -> list[str]:
    if type(value) is not list:
        raise OperationStateError(f"{label} must be a list")
    for index, item in enumerate(value):
        _require_string(item, f"{label}[{index}]")
    return value


def _require_string_map(
    value: object,
    label: str,
    *,
    hash_values: bool = False,
) -> dict[str, str]:
    if type(value) is not dict:
        raise OperationStateError(f"{label} must be a string map")
    for key, item in value.items():
        _require_string(key, f"{label} key")
        if hash_values:
            _require_hash(item, f"{label} value")
        else:
            _require_string(item, f"{label} value")
    return value


def _validate_effective_config_receipt(value: object) -> None:
    item = _require_string_map(value, "effective_config")
    if frozenset(item) != _EFFECTIVE_CONFIG_KEYS:
        raise OperationStateError("effective_config must contain the exact managed key set")
    if any(not configured for configured in item.values()):
        raise OperationStateError("effective_config values must be nonempty")
    if item["REMOTE_PRUNE_ENABLED"] != "0":
        raise OperationStateError("effective_config remote prune must remain disabled")
    if _BACKUP_PREFIX_RE.fullmatch(item["BACKUP_PREFIX"]) is None:
        raise OperationStateError("effective_config backup prefix is invalid")


def _validate_history_entry(value: object, label: str) -> None:
    item = _require_object(
        value,
        frozenset({"phase", "epoch", "evidence_sha256"}),
        label,
    )
    phase = _require_string(item["phase"], f"{label}.phase")
    if phase not in _PHASES:
        raise OperationStateError(f"{label}.phase is invalid")
    _require_int(item["epoch"], f"{label}.epoch")
    _require_hash(item["evidence_sha256"], f"{label}.evidence_sha256")


def _validate_reviewed_source(value: object) -> None:
    item = _require_object(
        value,
        frozenset({"commit", "archive_sha256", "manifest_sha256", "asset_hashes"}),
        "reviewed_source",
    )
    _require_string(item["commit"], "reviewed_source.commit", nonempty=True)
    _require_hash(item["archive_sha256"], "reviewed_source.archive_sha256")
    _require_hash(item["manifest_sha256"], "reviewed_source.manifest_sha256")
    _require_string_map(item["asset_hashes"], "reviewed_source.asset_hashes", hash_values=True)


def _validate_host_stage(value: object) -> None:
    item = _require_object(
        value,
        frozenset({"manifest_sha256", "asset_hashes", "environment_sha256"}),
        "host_stage",
    )
    _require_hash(item["manifest_sha256"], "host_stage.manifest_sha256")
    _require_string_map(item["asset_hashes"], "host_stage.asset_hashes", hash_values=True)
    _require_hash(item["environment_sha256"], "host_stage.environment_sha256")


_FILE_AUDIT_KEYS = frozenset({"sha256", "inode", "uid", "gid", "mode", "size", "mtime_ns"})


def _validate_file_audit(value: object, label: str, *, include_path: bool = False) -> None:
    keys = _FILE_AUDIT_KEYS | ({"path"} if include_path else set())
    item = _require_object(value, frozenset(keys), label)
    if include_path:
        _require_string(item["path"], f"{label}.path", nonempty=True)
    _require_hash(item["sha256"], f"{label}.sha256")
    _require_int(item["inode"], f"{label}.inode", minimum=1)
    for field in ("uid", "gid", "size", "mtime_ns"):
        _require_int(item[field], f"{label}.{field}")
    mode = _require_int(item["mode"], f"{label}.mode")
    if mode > 0o7777:
        raise OperationStateError(f"{label}.mode is invalid")


def _validate_snapshot_target(value: object, label: str) -> None:
    item = _require_object(
        value,
        frozenset({"present", "sha256", "mode", "uid", "gid"}),
        label,
    )
    _require_bool(item["present"], f"{label}.present")
    _require_optional_hash(item["sha256"], f"{label}.sha256")
    for field in ("mode", "uid", "gid"):
        _require_optional_int(item[field], f"{label}.{field}")


def _validate_snapshot(value: object) -> None:
    item = _require_object(
        value,
        frozenset({"manifest_sha256", "targets", "rclone_audit"}),
        "snapshot",
    )
    _require_hash(item["manifest_sha256"], "snapshot.manifest_sha256")
    targets = item["targets"]
    if type(targets) is not dict:
        raise OperationStateError("snapshot.targets must be an object")
    for key, target in targets.items():
        _require_string(key, "snapshot.targets key", nonempty=True)
        _validate_snapshot_target(target, "snapshot.targets entry")
    _validate_file_audit(item["rclone_audit"], "snapshot.rclone_audit", include_path=True)


def _validate_prior_runtime(value: object) -> None:
    item = _require_object(
        value,
        frozenset({"timer_enabled", "timer_active", "pids", "preinstall_trigger_epoch"}),
        "prior_runtime",
    )
    _require_bool(item["timer_enabled"], "prior_runtime.timer_enabled")
    _require_bool(item["timer_active"], "prior_runtime.timer_active")
    pids = item["pids"]
    if type(pids) is not dict:
        raise OperationStateError("prior_runtime.pids must be an integer map")
    for key, pid in pids.items():
        _require_string(key, "prior_runtime.pids key", nonempty=True)
        _require_int(pid, "prior_runtime.pids value", minimum=1)
    fixed = {
        "system:degen-web.service",
        "system:degen-worker.service",
    }
    keys = set(pids)
    postgres_keys = [key for key in keys if key.startswith("postgresql:")]
    user_keys = [key for key in keys if key.startswith("user:")]
    if len(keys) != 4 or not fixed.issubset(keys) or len(postgres_keys) != 1 or len(user_keys) != 1:
        raise OperationStateError("prior_runtime.pids identity keys are incomplete or ambiguous")
    postgres_unit = postgres_keys[0].split(":", 1)[1]
    if (
        _SYSTEMD_UNIT_RE.fullmatch(postgres_unit) is None
        or not postgres_unit.startswith("postgresql")
    ):
        raise OperationStateError("prior_runtime PostgreSQL unit identity is invalid")
    user_parts = user_keys[0].split(":")
    if (
        len(user_parts) != 4
        or not user_parts[1].isdigit()
        or str(int(user_parts[1])) != user_parts[1]
        or int(user_parts[1]) < 0
        or _LOGIN_USER_RE.fullmatch(user_parts[2]) is None
        or user_parts[3] != "degen-ops-discord-bot.service"
    ):
        raise OperationStateError("prior_runtime bot owner identity is invalid")
    if len(set(pids.values())) != len(pids):
        raise OperationStateError("prior_runtime.pids values must be unique")
    _require_optional_int(item["preinstall_trigger_epoch"], "prior_runtime.preinstall_trigger_epoch")


def _validate_install(value: object) -> None:
    item = _require_object(
        value,
        frozenset(
            {
                "next_target_index",
                "current_target",
                "previous_sha256",
                "intended_sha256",
                "installed_hashes",
                "started_epoch",
                "completed_epoch",
            }
        ),
        "install",
    )
    _require_int(item["next_target_index"], "install.next_target_index")
    if item["current_target"] is not None:
        _require_string(item["current_target"], "install.current_target", nonempty=True)
    _require_optional_hash(item["previous_sha256"], "install.previous_sha256")
    _require_optional_hash(item["intended_sha256"], "install.intended_sha256")
    _require_string_map(item["installed_hashes"], "install.installed_hashes", hash_values=True)
    _require_int(item["started_epoch"], "install.started_epoch")
    _require_optional_int(item["completed_epoch"], "install.completed_epoch")


def _validate_rclone_evidence_group(value: object, label: str) -> None:
    item = _require_object(
        value,
        frozenset({"group_id", "purpose", "before", "after", "evidence_sha256"}),
        label,
    )
    _require_string(item["group_id"], f"{label}.group_id")
    _require_string(item["purpose"], f"{label}.purpose")
    _validate_file_audit(item["before"], f"{label}.before")
    _validate_file_audit(item["after"], f"{label}.after")
    _require_hash(item["evidence_sha256"], f"{label}.evidence_sha256")


def _validate_probe_receipt(value: object) -> None:
    item = _require_object(
        value,
        frozenset({"prefix", "owned_names", "cleanup_proven", "evidence_sha256"}),
        "probe",
    )
    _require_string(item["prefix"], "probe.prefix")
    _require_string_list(item["owned_names"], "probe.owned_names")
    _require_bool(item["cleanup_proven"], "probe.cleanup_proven")
    _require_hash(item["evidence_sha256"], "probe.evidence_sha256")


def _validate_dry_run(value: object) -> None:
    item = _require_object(
        value,
        frozenset(
            {
                "inventory_names",
                "casefold_names",
                "keep_names",
                "protected_names",
                "delete_names",
                "candidate_sha256",
                "evidence_sha256",
            }
        ),
        "dry_run",
    )
    for field in (
        "inventory_names",
        "casefold_names",
        "keep_names",
        "protected_names",
        "delete_names",
    ):
        _require_string_list(item[field], f"dry_run.{field}")
    _require_hash(item["candidate_sha256"], "dry_run.candidate_sha256")
    _require_hash(item["evidence_sha256"], "dry_run.evidence_sha256")


def _validate_policy(value: object) -> None:
    item = _require_object(
        value,
        frozenset({"environment_sha256", "enabled_epoch"}),
        "policy",
    )
    _require_hash(item["environment_sha256"], "policy.environment_sha256")
    _require_int(item["enabled_epoch"], "policy.enabled_epoch")


def _validate_observation(value: object) -> None:
    item = _require_object(
        value,
        frozenset(
            {"run_epoch", "journal_sha256", "local_sha256", "remote_sha256", "evidence_sha256"}
        ),
        "observation",
    )
    _require_int(item["run_epoch"], "observation.run_epoch")
    for field in ("journal_sha256", "local_sha256", "remote_sha256", "evidence_sha256"):
        _require_hash(item[field], f"observation.{field}")


def _validate_probe_transaction(value: object) -> None:
    item = _require_object(value, frozenset({"prefix", "objects"}), "active_transaction.probe")
    _require_string(item["prefix"], "active_transaction.probe.prefix")
    objects = item["objects"]
    if type(objects) is not list:
        raise OperationStateError("active_transaction.probe.objects must be a list")
    for index, value_item in enumerate(objects):
        label = f"active_transaction.probe.objects[{index}]"
        obj = _require_object(
            value_item,
            frozenset(
                {
                    "name",
                    "expected_sha256",
                    "expected_size",
                    "created",
                    "verified",
                    "cleaned",
                }
            ),
            label,
        )
        _require_string(obj["name"], f"{label}.name")
        _require_hash(obj["expected_sha256"], f"{label}.expected_sha256")
        _require_int(obj["expected_size"], f"{label}.expected_size")
        for field in ("created", "verified", "cleaned"):
            _require_bool(obj[field], f"{label}.{field}")


def _validate_active_transaction(value: object) -> None:
    item = _require_object(
        value,
        frozenset(
            {
                "kind",
                "prior_stable_phase",
                "prior_timer_enabled",
                "prior_timer_active",
                "guard",
                "started_epoch",
                "policy_environment_sha256",
                "probe",
            }
        ),
        "active_transaction",
    )
    kind = _require_string(item["kind"], "active_transaction.kind")
    if kind not in _TRANSACTION_KINDS:
        raise OperationStateError("active_transaction.kind is invalid")
    policy_environment = item["policy_environment_sha256"]
    if kind == "policy":
        _require_hash(
            policy_environment,
            "active_transaction.policy_environment_sha256",
        )
    elif policy_environment is not None:
        raise OperationStateError(
            "active_transaction.policy_environment_sha256 must be null outside policy"
        )
    prior = _require_string(item["prior_stable_phase"], "active_transaction.prior_stable_phase")
    if prior not in _PRIOR_STABLE_PHASES:
        raise OperationStateError("active_transaction.prior_stable_phase is invalid")
    _require_bool(item["prior_timer_enabled"], "active_transaction.prior_timer_enabled")
    _require_bool(item["prior_timer_active"], "active_transaction.prior_timer_active")
    guard = _require_object(
        item["guard"],
        frozenset(
            {
                "timer_stopped",
                "service_inactive_verified",
                "legacy_lock_acquired",
                "runtime_lock_acquired",
                "locks_released",
                "timer_restored",
            }
        ),
        "active_transaction.guard",
    )
    for field in guard:
        _require_bool(guard[field], f"active_transaction.guard.{field}")
    guard_dependencies = (
        ("service_inactive_verified", "timer_stopped"),
        ("legacy_lock_acquired", "service_inactive_verified"),
        ("runtime_lock_acquired", "legacy_lock_acquired"),
        ("locks_released", "runtime_lock_acquired"),
        ("timer_restored", "locks_released"),
    )
    for later, earlier in guard_dependencies:
        if guard[later] and not guard[earlier]:
            raise OperationStateError("active_transaction guard lifecycle is impossible")
    _require_int(item["started_epoch"], "active_transaction.started_epoch")
    if item["probe"] is not None:
        _validate_probe_transaction(item["probe"])
        probe = item["probe"]
        assert isinstance(probe, dict)
        objects = probe["objects"]
        assert isinstance(objects, list)
        for obj in objects:
            assert isinstance(obj, dict)
            if obj["verified"] and not obj["created"]:
                raise OperationStateError("active_transaction probe verification precedes creation")
            if obj["cleaned"] and not obj["verified"]:
                raise OperationStateError("active_transaction probe cleanup precedes verification")


def _validate_failure(value: object) -> None:
    item = _require_object(
        value,
        frozenset({"phase", "primary_error", "epoch", "evidence_sha256"}),
        "failure",
    )
    _require_string(item["phase"], "failure.phase")
    _require_string(item["primary_error"], "failure.primary_error")
    _require_int(item["epoch"], "failure.epoch")
    _require_hash(item["evidence_sha256"], "failure.evidence_sha256")


def _validate_secondary_error(value: object, label: str) -> None:
    item = _require_object(
        value,
        frozenset({"stage", "error", "epoch", "evidence_sha256"}),
        label,
    )
    _require_string(item["stage"], f"{label}.stage")
    _require_string(item["error"], f"{label}.error")
    _require_int(item["epoch"], f"{label}.epoch")
    _require_hash(item["evidence_sha256"], f"{label}.evidence_sha256")


def _validate_recovery(value: object) -> None:
    item = _require_object(
        value,
        frozenset(
            {
                "kind",
                "next_target_index",
                "current_target",
                "previous_sha256",
                "intended_sha256",
                "started_epoch",
                "completed_epoch",
                "evidence_sha256",
            }
        ),
        "recovery",
    )
    kind = _require_string(item["kind"], "recovery.kind")
    if kind not in _RECOVERY_KINDS:
        raise OperationStateError("recovery.kind is invalid")
    _require_int(item["next_target_index"], "recovery.next_target_index")
    if item["current_target"] is not None:
        _require_string(item["current_target"], "recovery.current_target", nonempty=True)
    _require_optional_hash(item["previous_sha256"], "recovery.previous_sha256")
    _require_optional_hash(item["intended_sha256"], "recovery.intended_sha256")
    _require_int(item["started_epoch"], "recovery.started_epoch")
    _require_optional_int(item["completed_epoch"], "recovery.completed_epoch")
    _require_hash(item["evidence_sha256"], "recovery.evidence_sha256")


def _validate_state_schema(state: object) -> None:
    item = _require_object(state, _TOP_LEVEL_KEYS, "operation state")
    if _require_int(item["schema_version"], "schema_version") != 1:
        raise OperationStateError("schema_version must equal 1")
    _require_string(item["operation_id"], "operation_id", nonempty=True)
    operation_dir = _require_string(item["operation_dir"], "operation_dir", nonempty=True)
    if not Path(operation_dir).is_absolute():
        raise OperationStateError("operation_dir must be absolute")
    phase = _require_string(item["phase"], "phase")
    if phase not in _PHASES:
        raise OperationStateError("phase is invalid")
    history = item["phase_history"]
    if type(history) is not list:
        raise OperationStateError("phase_history must be a list")
    for index, entry in enumerate(history):
        _validate_history_entry(entry, f"phase_history[{index}]")
    _validate_reviewed_source(item["reviewed_source"])
    if item["effective_config"] is not None:
        _validate_effective_config_receipt(item["effective_config"])
    if item["host_stage"] is not None:
        _validate_host_stage(item["host_stage"])
    if item["snapshot"] is not None:
        _validate_snapshot(item["snapshot"])
    if item["prior_runtime"] is not None:
        _validate_prior_runtime(item["prior_runtime"])
    if item["install"] is not None:
        _validate_install(item["install"])
    groups = item["rclone_evidence_groups"]
    if type(groups) is not list:
        raise OperationStateError("rclone_evidence_groups must be a list")
    for index, group in enumerate(groups):
        _validate_rclone_evidence_group(group, f"rclone_evidence_groups[{index}]")
    if item["probe"] is not None:
        _validate_probe_receipt(item["probe"])
    if item["dry_run"] is not None:
        _validate_dry_run(item["dry_run"])
    if item["policy"] is not None:
        _validate_policy(item["policy"])
    if item["observation"] is not None:
        _validate_observation(item["observation"])
    if item["active_transaction"] is not None:
        _validate_active_transaction(item["active_transaction"])
    if item["failure"] is not None:
        _validate_failure(item["failure"])
    secondary = item["secondary_errors"]
    if type(secondary) is not list:
        raise OperationStateError("secondary_errors must be a list")
    for index, error in enumerate(secondary):
        _validate_secondary_error(error, f"secondary_errors[{index}]")
    if item["recovery"] is not None:
        _validate_recovery(item["recovery"])


def _phase_index(phase: str) -> int:
    try:
        return _NORMAL_PHASE_ORDER.index(phase)
    except ValueError as exc:
        raise OperationStateError("phase receipt rules are incomplete for this recovery phase") from exc


def _require_receipt(state: dict[str, object], field: str, required: bool) -> None:
    value = state[field]
    if required and value is None:
        raise OperationStateError(f"{field} is required in the current phase")
    if not required and value is not None:
        raise OperationStateError(f"{field} must be null in the current phase")


def _validate_source_and_stage_assets(state: dict[str, object]) -> None:
    reviewed = state["reviewed_source"]
    assert isinstance(reviewed, dict)
    reviewed_hashes = reviewed["asset_hashes"]
    assert isinstance(reviewed_hashes, dict)
    if frozenset(reviewed_hashes) != _SOURCE_ASSETS:
        raise OperationStateError("reviewed_source.asset_hashes must have exact reviewed asset keys")
    host_stage = state["host_stage"]
    if host_stage is None:
        return
    assert isinstance(host_stage, dict)
    host_hashes = host_stage["asset_hashes"]
    assert isinstance(host_hashes, dict)
    if frozenset(host_hashes) != _SOURCE_ASSETS:
        raise OperationStateError("host_stage.asset_hashes must have exact reviewed asset keys")
    for source in _SOURCE_ASSETS:
        if host_hashes[source] != reviewed_hashes[source]:
            raise OperationStateError("host_stage asset provenance does not match reviewed_source")


def _validate_snapshot_semantics(state: dict[str, object]) -> None:
    snapshot = state["snapshot"]
    if snapshot is None:
        return
    assert isinstance(snapshot, dict)
    targets = snapshot["targets"]
    assert isinstance(targets, dict)
    if tuple(targets) != _TARGET_ORDER and frozenset(targets) != frozenset(_TARGET_ORDER):
        raise OperationStateError("snapshot.targets must contain the exact seven targets")
    if frozenset(targets) != frozenset(_TARGET_ORDER):
        raise OperationStateError("snapshot.targets must contain the exact seven targets")
    for target_name, target in targets.items():
        assert isinstance(target, dict)
        metadata = (target["sha256"], target["mode"], target["uid"], target["gid"])
        if target["present"] and any(value is None for value in metadata):
            raise OperationStateError(f"snapshot.targets[{target_name!r}] present metadata is incomplete")
        if not target["present"] and any(value is not None for value in metadata):
            raise OperationStateError(f"snapshot.targets[{target_name!r}] absent metadata must be null")
        if target["present"]:
            mode = target["mode"]
            assert isinstance(mode, int)
            if mode > 0o777 or mode & 0o022:
                raise OperationStateError(
                    f"snapshot.targets[{target_name!r}] mode is unsafe"
                )
    effective_config = state["effective_config"]
    assert isinstance(effective_config, dict)
    rclone_audit = snapshot["rclone_audit"]
    assert isinstance(rclone_audit, dict)
    if rclone_audit["path"] != effective_config["RCLONE_CONFIG"]:
        raise OperationStateError("snapshot rclone audit path differs from effective_config")
    if rclone_audit["mode"] != 0o600:
        raise OperationStateError("snapshot rclone audit source mode is unsafe")


def _validate_install_semantics(state: dict[str, object], installed_reached: bool) -> None:
    install = state["install"]
    if install is None:
        return
    assert isinstance(install, dict)
    index = install["next_target_index"]
    assert isinstance(index, int)
    if index > len(_TARGET_ORDER):
        raise OperationStateError("install.next_target_index exceeds the target order")
    cursor = (install["current_target"], install["previous_sha256"], install["intended_sha256"])
    if index == len(_TARGET_ORDER):
        if any(value is not None for value in cursor):
            raise OperationStateError("install cursor must clear after the final target")
    elif install["current_target"] != _TARGET_ORDER[index]:
        raise OperationStateError("install cursor is not coherent with the target order")
    elif install["intended_sha256"] is None:
        raise OperationStateError("install intended hash is required for the current target")
    else:
        target_name = _TARGET_ORDER[index]
        snapshot = state["snapshot"]
        host_stage = state["host_stage"]
        assert isinstance(snapshot, dict) and isinstance(host_stage, dict)
        snapshot_targets = snapshot["targets"]
        assert isinstance(snapshot_targets, dict)
        snapshot_target = snapshot_targets[target_name]
        assert isinstance(snapshot_target, dict)
        expected_previous = snapshot_target["sha256"] if snapshot_target["present"] else None
        if target_name == _TARGET_ORDER[-1]:
            expected_intended = host_stage["environment_sha256"]
        else:
            source = next(source for source, target in _SOURCE_TO_TARGET if target == target_name)
            staged_hashes = host_stage["asset_hashes"]
            assert isinstance(staged_hashes, dict)
            expected_intended = staged_hashes[source]
        if install["previous_sha256"] != expected_previous or install["intended_sha256"] != expected_intended:
            raise OperationStateError("install cursor hash tuple is not bound to snapshot and staged target")

    hashes = install["installed_hashes"]
    assert isinstance(hashes, dict)
    if not installed_reached:
        if hashes:
            raise OperationStateError("install.installed_hashes must be empty before installed")
        if install["completed_epoch"] is not None:
            raise OperationStateError("install.completed_epoch must be null before installed")
        return
    if index != len(_TARGET_ORDER) or any(value is not None for value in cursor):
        raise OperationStateError("installed requires a terminal cleared install cursor")
    if frozenset(hashes) != frozenset(_TARGET_ORDER):
        raise OperationStateError("install.installed_hashes must contain the exact seven targets")
    if install["completed_epoch"] is None:
        raise OperationStateError("install.completed_epoch is required after installed")
    reviewed = state["reviewed_source"]
    host_stage = state["host_stage"]
    assert isinstance(reviewed, dict) and isinstance(host_stage, dict)
    reviewed_hashes = reviewed["asset_hashes"]
    staged_hashes = host_stage["asset_hashes"]
    assert isinstance(reviewed_hashes, dict) and isinstance(staged_hashes, dict)
    for source, target in _SOURCE_TO_TARGET:
        if hashes[target] != reviewed_hashes[source] or hashes[target] != staged_hashes[source]:
            raise OperationStateError("installed hash provenance does not match reviewed and staged assets")
    if hashes[_TARGET_ORDER[-1]] != host_stage["environment_sha256"]:
        raise OperationStateError("installed environment hash must use host_stage.environment_sha256")


def _recovery_kind(state: dict[str, object]) -> str | None:
    recovery = state["recovery"]
    if recovery is None:
        return None
    assert isinstance(recovery, dict)
    return str(recovery["kind"])


def _active_expected_for_state(state: dict[str, object]) -> tuple[str, str] | None:
    phase = state["phase"]
    direct = {
        "probing": ("probe", "installed"),
        "dry_run_recording": ("dry_run", "probed"),
        "policy_enabling": ("policy", "dry_run_recorded"),
        "observing": ("observe", "policy_enabled"),
    }.get(phase)
    if direct is not None:
        return direct
    kind = _recovery_kind(state)
    if phase in {"recovery_required", "recovering_probe"} and kind == "probe":
        return ("probe", "installed")
    if phase in {"recovery_required", "recovering_policy"} and kind == "policy":
        return ("policy", "dry_run_recorded")
    if phase in {"recovery_required", "recovering_guard"} and kind == "guard":
        transaction = state["active_transaction"]
        if isinstance(transaction, dict):
            prior = transaction["prior_stable_phase"]
            if prior == "probed":
                return ("dry_run", "probed")
            if prior == "policy_enabled":
                return ("observe", "policy_enabled")
    return None


def _validate_active_transaction_for_phase(state: dict[str, object]) -> None:
    expected = _active_expected_for_state(state)
    transaction = state["active_transaction"]
    if expected is None:
        if transaction is not None:
            raise OperationStateError("active_transaction must be null in a stable phase")
        return
    if transaction is None:
        raise OperationStateError("active_transaction is required in the current phase")
    assert isinstance(transaction, dict)
    if (transaction["kind"], transaction["prior_stable_phase"]) != expected:
        raise OperationStateError("active_transaction does not match the current phase")
    if expected[0] == "probe" and transaction["probe"] is None:
        raise OperationStateError("active_transaction probe receipt is required while probing")
    if expected[0] != "probe" and transaction["probe"] is not None:
        raise OperationStateError("active_transaction probe receipt is invalid for this transaction")
    if expected[0] == "policy":
        host_stage = state["host_stage"]
        assert isinstance(host_stage, dict)
        if transaction["policy_environment_sha256"] == host_stage["environment_sha256"]:
            raise OperationStateError(
                "policy_environment enabled digest must differ from the disabled staged digest"
            )


def _validate_policy_environment_semantics(state: dict[str, object]) -> None:
    policy = state["policy"]
    if policy is None:
        return
    host_stage = state["host_stage"]
    assert isinstance(policy, dict) and isinstance(host_stage, dict)
    if policy["environment_sha256"] == host_stage["environment_sha256"]:
        raise OperationStateError(
            "policy environment enabled digest must differ from the disabled staged digest"
        )


def _receipt_baseline_phase(state: dict[str, object]) -> str:
    phase = str(state["phase"])
    if phase in _NORMAL_PHASE_ORDER:
        return phase
    kind = _recovery_kind(state)
    if phase == "rolled_back":
        history = state["phase_history"]
        assert isinstance(history, list)
        if any(entry["phase"] == "installed" for entry in history):
            rollback_index = max(
                index for index, entry in enumerate(history) if entry["phase"] == "manual_rollback"
            )
            return str(history[rollback_index - 1]["phase"])
        return "installing"
    if phase in {"recovering", "manual_rollback"}:
        if phase == "manual_rollback":
            history = state["phase_history"]
            assert isinstance(history, list)
            return str(history[-2]["phase"])
        return "installing"
    if phase in {"recovery_required", "recovering_policy", "recovering_probe", "recovering_guard"}:
        if kind == "install":
            return "installing"
        if kind == "manual_rollback":
            history = state["phase_history"]
            assert isinstance(history, list)
            manual_index = max(
                index for index, entry in enumerate(history) if entry["phase"] == "manual_rollback"
            )
            return str(history[manual_index - 1]["phase"])
        if kind == "policy":
            return "policy_enabling"
        if kind == "probe":
            return "probing"
        if kind == "guard":
            expected = _active_expected_for_state(state)
            if expected == ("dry_run", "probed"):
                return "dry_run_recording"
            if expected == ("observe", "policy_enabled"):
                return "observing"
    raise OperationStateError("recovery phase has no coherent receipt baseline")


def _validate_recovery_semantics(state: dict[str, object]) -> None:
    phase = str(state["phase"])
    recovery = state["recovery"]
    history = state["phase_history"]
    assert isinstance(history, list)
    latest_kind, latest_entry_epoch = _latest_recovery_attempt_from_history(history)
    if latest_kind is None and recovery is not None:
        raise OperationStateError("recovery must be null before the first recovery history")
    if latest_kind is not None and recovery is None:
        raise OperationStateError("the current/latest recovery receipt must be preserved")
    recovery_phase = phase in {
        "recovering",
        "recovering_policy",
        "recovering_probe",
        "recovering_guard",
        "manual_rollback",
        "recovery_required",
        "rolled_back",
    }
    if recovery_phase and recovery is None:
        raise OperationStateError("recovery receipt is required in a recovery phase")
    if recovery is None:
        return
    assert isinstance(recovery, dict)
    kind = str(recovery["kind"])
    if latest_kind is not None and kind != latest_kind:
        raise OperationStateError("recovery kind does not match the latest recovery attempt")
    if latest_entry_epoch is not None and recovery["started_epoch"] != latest_entry_epoch:
        raise OperationStateError("recovery start must equal the latest attempt entry epoch")
    expected_kinds = {
        "recovering": {"install"},
        "recovering_policy": {"policy"},
        "recovering_probe": {"probe"},
        "recovering_guard": {"guard"},
        "manual_rollback": {"manual_rollback"},
    }
    if phase in expected_kinds and kind not in expected_kinds[phase]:
        raise OperationStateError("recovery kind does not match the current recovery phase")
    if phase == "recovery_required" and kind not in _RECOVERY_KINDS:
        raise OperationStateError("recovery_required has an invalid recovery kind")
    if phase == "recovery_required":
        transaction = state["active_transaction"]
        if isinstance(transaction, dict):
            expected_kind = {
                "probe": "probe",
                "dry_run": "guard",
                "observe": "guard",
                "policy": "policy",
            }[str(transaction["kind"])]
            if kind != expected_kind:
                raise OperationStateError("recovery kind does not match the active transaction")
        else:
            history = state["phase_history"]
            assert isinstance(history, list)
            phases = [entry["phase"] for entry in history]
            expected_kind = "manual_rollback" if "manual_rollback" in phases else "install"
            if kind != expected_kind:
                raise OperationStateError("recovery kind does not match the interrupted recovery")
    index = recovery["next_target_index"]
    assert isinstance(index, int)
    cursor = (
        recovery["current_target"],
        recovery["previous_sha256"],
        recovery["intended_sha256"],
    )
    if kind in {"install", "manual_rollback"}:
        if index > len(_TARGET_ORDER):
            raise OperationStateError("recovery cursor exceeds the target order")
        if index == len(_TARGET_ORDER):
            if any(value is not None for value in cursor):
                raise OperationStateError("recovery cursor must clear after the final target")
        else:
            target_name = _TARGET_ORDER[index]
            if recovery["current_target"] != target_name:
                raise OperationStateError("recovery cursor is not coherent with the target order")
            snapshot = state["snapshot"]
            host_stage = state["host_stage"]
            install = state["install"]
            assert isinstance(snapshot, dict) and isinstance(host_stage, dict) and isinstance(install, dict)
            snapshot_targets = snapshot["targets"]
            assert isinstance(snapshot_targets, dict)
            snapshot_target = snapshot_targets[target_name]
            assert isinstance(snapshot_target, dict)
            restore_hash = snapshot_target["sha256"] if snapshot_target["present"] else None
            staged_hash = _target_staged_hash(state, target_name)
            installed_hashes = install["installed_hashes"]
            assert isinstance(installed_hashes, dict)
            installed_hash = installed_hashes.get(target_name, staged_hash)
            allowed_previous = (
                {installed_hash} if kind == "manual_rollback" else {restore_hash, staged_hash}
            )
            if recovery["intended_sha256"] != restore_hash:
                raise OperationStateError("recovery intended hash is not bound to snapshot provenance")
            if recovery["previous_sha256"] not in allowed_previous:
                raise OperationStateError("recovery previous hash is not bound to installed live provenance")
    elif kind == "policy":
        if index not in {0, 1}:
            raise OperationStateError("policy recovery cursor has an invalid index")
        if index == 0:
            if recovery["current_target"] != _TARGET_ORDER[-1]:
                raise OperationStateError("policy recovery must target the fixed environment path")
            expected_environment = _target_staged_hash(state, _TARGET_ORDER[-1])
            transaction = state["active_transaction"]
            assert isinstance(transaction, dict)
            enabled_environment = transaction["policy_environment_sha256"]
            assert isinstance(enabled_environment, str)
            if recovery["intended_sha256"] != expected_environment:
                raise OperationStateError("policy recovery intended hash lacks environment provenance")
            if recovery["previous_sha256"] not in {
                expected_environment,
                enabled_environment,
            }:
                raise OperationStateError("policy recovery previous hash lacks environment provenance")
        elif any(value is not None for value in cursor):
            raise OperationStateError("policy recovery cursor must clear after the environment target")
    else:
        if index != 0 or any(value is not None for value in cursor):
            raise OperationStateError("probe and guard recovery require a null file cursor")
    completed = recovery["completed_epoch"]
    if completed is not None:
        if completed < recovery["started_epoch"]:
            raise OperationStateError("recovery completion precedes recovery start")
        terminal = (
            index == len(_TARGET_ORDER)
            if kind in {"install", "manual_rollback"}
            else index == 1 if kind == "policy" else True
        )
        if not terminal:
            raise OperationStateError("recovery completion is premature")
    if phase in {"recovering", "recovering_policy", "recovering_probe", "recovering_guard", "manual_rollback", "recovery_required"} and completed is not None:
        raise OperationStateError("recovery completion is premature in an active recovery phase")
    if phase == "rolled_back" and completed is None:
        raise OperationStateError("rolled_back requires recovery completion")
    if phase in _NORMAL_PHASE_ORDER and latest_kind is not None and completed is None:
        raise OperationStateError("stable return requires recovery completion")


def _latest_recovery_attempt_from_history(
    history: list[object],
) -> tuple[str | None, int | None]:
    latest: str | None = None
    latest_epoch: int | None = None
    previous_phase: str | None = None
    for entry in history:
        assert isinstance(entry, dict)
        phase = str(entry["phase"])
        direct: str | None = None
        if phase == "recovering" and previous_phase == "installing":
            direct = "install"
        elif phase == "recovering_policy" and previous_phase == "policy_enabling":
            direct = "policy"
        elif phase == "manual_rollback" and previous_phase in {
            "installed",
            "probed",
            "dry_run_recorded",
            "policy_enabled",
            "observed",
        }:
            direct = "manual_rollback"
        if direct is not None:
            latest = direct
            latest_epoch = entry["epoch"]
        elif phase == "recovery_required" and previous_phase in {
            "probing",
            "dry_run_recording",
            "observing",
        }:
            latest = "probe" if previous_phase == "probing" else "guard"
            latest_epoch = entry["epoch"]
        previous_phase = phase
    return latest, latest_epoch


def _target_staged_hash(state: dict[str, object], target_name: str) -> str:
    host_stage = state["host_stage"]
    assert isinstance(host_stage, dict)
    if target_name == _TARGET_ORDER[-1]:
        value = host_stage["environment_sha256"]
        assert isinstance(value, str)
        return value
    source = next(source for source, target in _SOURCE_TO_TARGET if target == target_name)
    asset_hashes = host_stage["asset_hashes"]
    assert isinstance(asset_hashes, dict)
    value = asset_hashes[source]
    assert isinstance(value, str)
    return value


def _validate_receipt_phase_rules(state: dict[str, object]) -> None:
    _validate_recovery_semantics(state)
    baseline_phase = _receipt_baseline_phase(state)
    index = _phase_index(baseline_phase)
    _validate_source_and_stage_assets(state)
    _require_receipt(state, "effective_config", index >= _phase_index("staging_prepared"))
    _require_receipt(state, "host_stage", index >= _phase_index("staging_prepared"))
    _require_receipt(state, "snapshot", index >= _phase_index("snapshotted"))
    _require_receipt(state, "prior_runtime", index >= _phase_index("snapshotted"))
    _require_receipt(state, "install", index >= _phase_index("installing"))
    _require_receipt(state, "probe", index >= _phase_index("probed"))
    _require_receipt(state, "dry_run", index >= _phase_index("dry_run_recorded"))
    _require_receipt(state, "policy", index >= _phase_index("policy_enabled"))
    _require_receipt(state, "observation", index >= _phase_index("observed"))
    _validate_snapshot_semantics(state)
    _validate_install_semantics(state, index >= _phase_index("installed"))
    _validate_active_transaction_for_phase(state)
    _validate_policy_environment_semantics(state)


def _phase_epoch(state: dict[str, object], phase: str) -> int:
    history = state["phase_history"]
    assert isinstance(history, list)
    matches = [entry["epoch"] for entry in history if entry["phase"] == phase]
    if not matches:
        raise OperationStateError(f"phase_history is missing {phase}")
    return matches[-1]


def _validate_lifecycle_epochs(state: dict[str, object]) -> None:
    history = state["phase_history"]
    assert isinstance(history, list)
    epochs = [entry["epoch"] for entry in history]
    if epochs != sorted(epochs):
        raise OperationStateError("phase_history epochs must be nondecreasing")
    secondary = state["secondary_errors"]
    assert isinstance(secondary, list)
    secondary_epochs = [entry["epoch"] for entry in secondary]
    if secondary_epochs != sorted(secondary_epochs):
        raise OperationStateError("secondary_errors epochs must be nondecreasing")
    install = state["install"]
    if install is not None:
        assert isinstance(install, dict)
        completed = install["completed_epoch"]
        if completed is not None and install["started_epoch"] > completed:
            raise OperationStateError("install completion precedes install start")
    transaction = state["active_transaction"]
    if transaction is not None:
        assert isinstance(transaction, dict)
        entering_phase = {
            "probe": "probing",
            "dry_run": "dry_run_recording",
            "policy": "policy_enabling",
            "observe": "observing",
        }[str(transaction["kind"])]
        if transaction["started_epoch"] < _phase_epoch(state, entering_phase):
            raise OperationStateError("active_transaction start precedes its entering phase")
    policy = state["policy"]
    if policy is not None:
        assert isinstance(policy, dict) and isinstance(install, dict)
        if install["completed_epoch"] is None or policy["enabled_epoch"] < install["completed_epoch"]:
            raise OperationStateError("policy enablement precedes completed install")
        if policy["enabled_epoch"] < _phase_epoch(state, "policy_enabling"):
            raise OperationStateError("policy enablement precedes policy_enabling")
    observation = state["observation"]
    if observation is not None:
        assert isinstance(observation, dict) and isinstance(policy, dict)
        if observation["run_epoch"] <= policy["enabled_epoch"]:
            raise OperationStateError("observation is not newer than policy enablement")
        if observation["run_epoch"] < _phase_epoch(state, "observing"):
            raise OperationStateError("observation precedes observing phase")


def _history_transition_allowed(old: str, new: str, state: dict[str, object]) -> bool:
    normal = set(zip(_NORMAL_PHASE_ORDER, _NORMAL_PHASE_ORDER[1:]))
    if (old, new) in normal:
        return True
    if old == "installing" and new == "recovering":
        return True
    if old == "recovering" and new in {"rolled_back", "recovery_required"}:
        return True
    if old == "policy_enabling" and new == "recovering_policy":
        return True
    if old == "recovering_policy" and new in {"installed", "recovery_required"}:
        return True
    if old in {"probing", "dry_run_recording", "observing"} and new == "recovery_required":
        return True
    if old in {"installed", "probed", "dry_run_recorded", "policy_enabled", "observed"} and new == "manual_rollback":
        return True
    if old == "manual_rollback" and new in {"rolled_back", "recovery_required"}:
        return True
    if old == "recovering_probe" and new in {"installed", "recovery_required"}:
        return True
    if old == "recovering_guard" and new in {"probed", "policy_enabled", "recovery_required"}:
        return True
    if old == "recovery_required":
        return new in {
            "recovering",
            "recovering_policy",
            "manual_rollback",
            "recovering_probe",
            "recovering_guard",
        }
    return False


def _validate_phase_history_graph(state: dict[str, object]) -> None:
    history = state["phase_history"]
    assert isinstance(history, list)
    if not history or history[0]["phase"] != "source_verified":
        raise OperationStateError("phase_history must begin with source_verified")
    for old, new in zip(history, history[1:]):
        old_phase = str(old["phase"])
        new_phase = str(new["phase"])
        if old_phase == new_phase or not _history_transition_allowed(old_phase, new_phase, state):
            raise OperationStateError("phase_history contains a forbidden transition")
    _validate_recovery_history_bindings(history)


def _validate_recovery_history_bindings(history: list[object]) -> None:
    attempt_kind: str | None = None
    guard_prior: str | None = None
    resume_phases = {
        "install": "recovering",
        "policy": "recovering_policy",
        "manual_rollback": "manual_rollback",
        "probe": "recovering_probe",
        "guard": "recovering_guard",
    }
    stable_phases = {"installed", "probed", "dry_run_recorded", "policy_enabled", "observed"}

    for old_entry, new_entry in zip(history, history[1:]):
        assert isinstance(old_entry, dict) and isinstance(new_entry, dict)
        old_phase = str(old_entry["phase"])
        new_phase = str(new_entry["phase"])

        if old_phase == "installing" and new_phase == "recovering":
            attempt_kind = "install"
            guard_prior = None
        elif old_phase == "policy_enabling" and new_phase == "recovering_policy":
            attempt_kind = "policy"
            guard_prior = None
        elif old_phase in stable_phases and new_phase == "manual_rollback":
            attempt_kind = "manual_rollback"
            guard_prior = None
        elif old_phase == "probing" and new_phase == "recovery_required":
            attempt_kind = "probe"
            guard_prior = None
        elif old_phase == "dry_run_recording" and new_phase == "recovery_required":
            attempt_kind = "guard"
            guard_prior = "probed"
        elif old_phase == "observing" and new_phase == "recovery_required":
            attempt_kind = "guard"
            guard_prior = "policy_enabled"

        if old_phase == "recovery_required":
            expected_resume = resume_phases.get(attempt_kind)
            if expected_resume is None or new_phase != expected_resume:
                raise OperationStateError(
                    "phase history recovery resume does not match the recorded attempt kind"
                )

        if old_phase == "recovering_guard" and new_phase != "recovery_required":
            if attempt_kind != "guard" or guard_prior is None or new_phase != guard_prior:
                raise OperationStateError(
                    "phase history recovery guard return does not match its recorded prior stable phase"
                )


def _is_prefix(old: list[object], new: list[object]) -> bool:
    return len(new) >= len(old) and new[: len(old)] == old


def _validate_append_only_streams(
    previous: dict[str, object],
    current: dict[str, object],
) -> None:
    for field in ("rclone_evidence_groups", "secondary_errors"):
        old_stream = previous[field]
        new_stream = current[field]
        assert isinstance(old_stream, list) and isinstance(new_stream, list)
        if not _is_prefix(old_stream, new_stream):
            raise OperationStateError(f"{field} must remain append-only")
    if previous["failure"] is not None and current["failure"] != previous["failure"]:
        raise OperationStateError("the first primary failure is immutable")


def _validate_cursor_progress(old: dict[str, object], new: dict[str, object], label: str) -> None:
    old_index = old["next_target_index"]
    new_index = new["next_target_index"]
    assert isinstance(old_index, int) and isinstance(new_index, int)
    cursor_fields = ("current_target", "previous_sha256", "intended_sha256")
    if new_index == old_index:
        if any(new[field] != old[field] for field in cursor_fields):
            raise OperationStateError(f"{label} cursor changed at an unchanged index")
    elif new_index != old_index + 1:
        raise OperationStateError(f"{label} cursor must advance exactly one target")


def _validate_transaction_progress(old: dict[str, object], new: dict[str, object]) -> None:
    for field in (
        "kind",
        "prior_stable_phase",
        "prior_timer_enabled",
        "prior_timer_active",
        "started_epoch",
        "policy_environment_sha256",
    ):
        if new[field] != old[field]:
            raise OperationStateError("active_transaction identity is immutable")
    old_guard = old["guard"]
    new_guard = new["guard"]
    assert isinstance(old_guard, dict) and isinstance(new_guard, dict)
    guard_advances = 0
    for field in old_guard:
        if old_guard[field] and not new_guard[field]:
            raise OperationStateError("active_transaction guard booleans cannot regress")
        if not old_guard[field] and new_guard[field]:
            guard_advances += 1
    if guard_advances > 1:
        raise OperationStateError("active_transaction may advance at most one guard progress step per write")
    old_probe = old["probe"]
    new_probe = new["probe"]
    if old_probe is None or new_probe is None:
        if old_probe != new_probe:
            raise OperationStateError("active_transaction probe identity is immutable")
        return
    assert isinstance(old_probe, dict) and isinstance(new_probe, dict)
    if old_probe["prefix"] != new_probe["prefix"]:
        raise OperationStateError("active_transaction probe identity is immutable")
    old_objects = old_probe["objects"]
    new_objects = new_probe["objects"]
    assert isinstance(old_objects, list) and isinstance(new_objects, list)
    if len(old_objects) != len(new_objects):
        raise OperationStateError("active_transaction probe objects are immutable")
    probe_advances = 0
    for old_object, new_object in zip(old_objects, new_objects):
        for field in ("name", "expected_sha256", "expected_size"):
            if old_object[field] != new_object[field]:
                raise OperationStateError("active_transaction probe object identity is immutable")
        for field in ("created", "verified", "cleaned"):
            if old_object[field] and not new_object[field]:
                raise OperationStateError("active_transaction probe progress cannot regress")
            if not old_object[field] and new_object[field]:
                probe_advances += 1
    if probe_advances > 1:
        raise OperationStateError("active_transaction may advance at most one probe progress flag per write")
    if guard_advances + probe_advances > 1:
        raise OperationStateError("active_transaction may advance at most one durable progress step per write")


def _require_zero_transaction_entry(transaction: dict[str, object]) -> None:
    guard = transaction["guard"]
    assert isinstance(guard, dict)
    if any(guard.values()):
        raise OperationStateError("active_transaction entry requires zero initial progress")
    probe = transaction["probe"]
    if probe is None:
        return
    assert isinstance(probe, dict)
    objects = probe["objects"]
    assert isinstance(objects, list)
    for item in objects:
        assert isinstance(item, dict)
        if any(item[field] for field in ("created", "verified", "cleaned")):
            raise OperationStateError("active_transaction entry requires zero initial progress")


def _require_transaction_completion(
    transaction: dict[str, object],
    *,
    successful_probe: bool,
) -> None:
    guard = transaction["guard"]
    assert isinstance(guard, dict)
    if not all(guard.values()):
        raise OperationStateError("active_transaction guard must complete before stable return")
    probe = transaction["probe"]
    if probe is None:
        return
    assert isinstance(probe, dict)
    objects = probe["objects"]
    assert isinstance(objects, list)
    for item in objects:
        assert isinstance(item, dict)
        if successful_probe:
            if not (item["created"] and item["verified"] and item["cleaned"]):
                raise OperationStateError("probe object lifecycle must complete before probed")
        elif item["created"] and not item["cleaned"]:
            raise OperationStateError("created probe objects must be cleaned before recovery return")


def _validate_same_phase_progress(previous: dict[str, object], current: dict[str, object]) -> None:
    phase = str(current["phase"])
    allowed = {
        "installing",
        "recovering",
        "manual_rollback",
        "probing",
        "dry_run_recording",
        "policy_enabling",
        "observing",
        "recovery_required",
        "recovering_policy",
        "recovering_probe",
        "recovering_guard",
    }
    if phase not in allowed:
        raise OperationStateError("same-phase mutation is not allowed in a stable phase")
    mutable = {"failure", "secondary_errors", "rclone_evidence_groups"}
    if phase == "installing":
        mutable.add("install")
    if phase in {"recovering", "manual_rollback", "recovering_policy", "recovering_probe", "recovering_guard"}:
        mutable.add("recovery")
    if previous["active_transaction"] is not None:
        mutable.add("active_transaction")
    for field in _TOP_LEVEL_KEYS - mutable - {"phase_history"}:
        if current[field] != previous[field]:
            raise OperationStateError(f"same-phase mutation changed immutable field {field}")
    if phase == "installing":
        assert isinstance(previous["install"], dict) and isinstance(current["install"], dict)
        for field in ("started_epoch", "installed_hashes", "completed_epoch"):
            if current["install"][field] != previous["install"][field]:
                raise OperationStateError("install start and completion receipt are immutable during progress")
        _validate_cursor_progress(previous["install"], current["install"], "install")
    if "recovery" in mutable:
        old_recovery = previous["recovery"]
        new_recovery = current["recovery"]
        assert isinstance(old_recovery, dict) and isinstance(new_recovery, dict)
        for field in ("kind", "started_epoch", "evidence_sha256"):
            if old_recovery[field] != new_recovery[field]:
                raise OperationStateError("recovery attempt identity is immutable")
        _validate_cursor_progress(old_recovery, new_recovery, "recovery")
        if old_recovery["completed_epoch"] is not None and new_recovery["completed_epoch"] != old_recovery["completed_epoch"]:
            raise OperationStateError("recovery completion is immutable")
    if previous["active_transaction"] is not None:
        assert isinstance(previous["active_transaction"], dict)
        assert isinstance(current["active_transaction"], dict)
        _validate_transaction_progress(previous["active_transaction"], current["active_transaction"])


def _validate_state_transition(previous: dict[str, object], current: dict[str, object]) -> None:
    immutable_identity = ("schema_version", "operation_id", "operation_dir", "reviewed_source")
    for field in immutable_identity:
        if current[field] != previous[field]:
            raise OperationStateError(f"{field} cannot change across an operation")
    old_history = previous["phase_history"]
    new_history = current["phase_history"]
    assert isinstance(old_history, list) and isinstance(new_history, list)
    old_phase = str(previous["phase"])
    new_phase = str(current["phase"])
    if old_phase == new_phase:
        if new_history != old_history:
            raise OperationStateError("same-phase progress cannot append or alter history")
        if current == previous:
            return
        _validate_append_only_streams(previous, current)
        _validate_same_phase_progress(previous, current)
        return
    if len(new_history) != len(old_history) + 1 or new_history[:-1] != old_history:
        raise OperationStateError("phase change must append exactly one stable history entry")
    if not _history_transition_allowed(old_phase, new_phase, current):
        raise OperationStateError("forbidden phase transition")
    _validate_append_only_streams(previous, current)
    old_transaction = previous["active_transaction"]
    new_transaction = current["active_transaction"]
    transaction_start_edges = {
        ("installed", "probing"),
        ("probed", "dry_run_recording"),
        ("dry_run_recorded", "policy_enabling"),
        ("policy_enabled", "observing"),
    }
    transaction_completion_edges = {
        ("probing", "probed"),
        ("dry_run_recording", "dry_run_recorded"),
        ("policy_enabling", "policy_enabled"),
        ("observing", "observed"),
        ("recovering_policy", "installed"),
        ("recovering_probe", "installed"),
        ("recovering_guard", "probed"),
        ("recovering_guard", "policy_enabled"),
    }
    if isinstance(old_transaction, dict) and isinstance(new_transaction, dict):
        _validate_transaction_progress(old_transaction, new_transaction)
    elif old_transaction is None and isinstance(new_transaction, dict):
        if (old_phase, new_phase) not in transaction_start_edges:
            raise OperationStateError("active_transaction can start only at a guarded phase entry")
        _require_zero_transaction_entry(new_transaction)
    elif isinstance(old_transaction, dict) and new_transaction is None:
        if (old_phase, new_phase) not in transaction_completion_edges:
            raise OperationStateError("active_transaction cannot clear before a stable return")
        _require_transaction_completion(
            old_transaction,
            successful_probe=(old_phase, new_phase) == ("probing", "probed"),
        )
        if (old_phase, new_phase) == ("policy_enabling", "policy_enabled"):
            policy = current["policy"]
            assert isinstance(policy, dict)
            if policy["environment_sha256"] != old_transaction["policy_environment_sha256"]:
                raise OperationStateError(
                    "policy environment receipt must match the precommitted transaction digest"
                )
    if old_phase == "recovering_policy" and new_phase == "installed":
        for field in ("probe", "dry_run", "policy", "observation", "active_transaction"):
            if current[field] is not None:
                raise OperationStateError("policy recovery reset must clear all later receipts together")
        for field in (
            "reviewed_source",
            "effective_config",
            "host_stage",
            "snapshot",
            "prior_runtime",
            "install",
            "rclone_evidence_groups",
            "failure",
            "secondary_errors",
        ):
            if current[field] != previous[field]:
                raise OperationStateError(f"policy recovery reset must preserve {field}")
        old_recovery = previous["recovery"]
        new_recovery = current["recovery"]
        assert isinstance(old_recovery, dict) and isinstance(new_recovery, dict)
        _validate_cursor_progress(old_recovery, new_recovery, "recovery")
        if new_recovery["completed_epoch"] is None:
            raise OperationStateError("policy recovery reset requires recovery completion")
    for field in ("effective_config", "host_stage", "snapshot", "prior_runtime"):
        if previous[field] is not None and current[field] != previous[field]:
            raise OperationStateError(f"{field} is immutable once recorded")
    install_change_allowed = (
        (old_phase == "snapshotted" and new_phase == "installing")
        or (old_phase == "installing" and new_phase == "installed")
    )
    if current["install"] != previous["install"] and not install_change_allowed:
        raise OperationStateError("install receipt is frozen during recovery and rollback")
    if old_phase == "snapshotted" and new_phase == "installing":
        install = current["install"]
        assert isinstance(install, dict)
        if install["next_target_index"] != 0 or install["current_target"] != _TARGET_ORDER[0]:
            raise OperationStateError("installing entry requires index zero and the first target")
    if old_phase == "installing" and new_phase == "installed":
        prior_install = previous["install"]
        completed_install = current["install"]
        assert isinstance(prior_install, dict) and isinstance(completed_install, dict)
        prior_cursor = (
            prior_install["current_target"],
            prior_install["previous_sha256"],
            prior_install["intended_sha256"],
        )
        if prior_install["next_target_index"] != len(_TARGET_ORDER) or any(
            value is not None for value in prior_cursor
        ):
            raise OperationStateError("install prior cursor must be terminal before installed")
        if completed_install["started_epoch"] != prior_install["started_epoch"]:
            raise OperationStateError("install started_epoch is immutable at completion")
    receipt_creation_edges = {
        "probe": ("probing", "probed"),
        "dry_run": ("dry_run_recording", "dry_run_recorded"),
        "policy": ("policy_enabling", "policy_enabled"),
        "observation": ("observing", "observed"),
    }
    policy_reset = old_phase == "recovering_policy" and new_phase == "installed"
    for field, edge in receipt_creation_edges.items():
        if current[field] != previous[field] and (old_phase, new_phase) != edge and not policy_reset:
            raise OperationStateError(f"{field} receipt is immutable outside its completion transition")
    recovery_resume = old_phase == "recovery_required" and new_phase in {
        "recovering",
        "recovering_policy",
        "manual_rollback",
        "recovering_probe",
        "recovering_guard",
    }
    if recovery_resume:
        if current["recovery"] != previous["recovery"]:
            raise OperationStateError("recovery resume must preserve cursor, start, and evidence")
    recovery_start = (
        (old_phase == "installing" and new_phase == "recovering")
        or (old_phase == "policy_enabling" and new_phase == "recovering_policy")
        or (old_phase in {"probing", "dry_run_recording", "observing"} and new_phase == "recovery_required")
        or (new_phase == "manual_rollback" and old_phase in {"installed", "probed", "dry_run_recorded", "policy_enabled", "observed"})
    )
    old_recovery = previous["recovery"]
    new_recovery = current["recovery"]
    if recovery_start:
        assert isinstance(new_recovery, dict)
        if new_recovery["next_target_index"] != 0:
            raise OperationStateError("new recovery attempt must enter at index zero")
        entering_epoch = new_history[-1]["epoch"]
        if new_recovery["started_epoch"] != entering_epoch:
            raise OperationStateError("new recovery attempt start must equal its entry phase")
        if old_recovery is not None:
            assert isinstance(old_recovery, dict)
            if old_recovery["completed_epoch"] is None:
                raise OperationStateError("an incomplete recovery attempt cannot be replaced")
            if new_recovery["started_epoch"] < old_recovery["completed_epoch"]:
                raise OperationStateError("successive recovery attempt epochs must be nondecreasing")
    elif old_recovery != new_recovery:
        if not (isinstance(old_recovery, dict) and isinstance(new_recovery, dict)):
            raise OperationStateError("current/latest recovery receipt must remain preserved")
        for field in ("kind", "started_epoch", "evidence_sha256"):
            if old_recovery[field] != new_recovery[field]:
                raise OperationStateError("recovery attempt identity is immutable")
        _validate_cursor_progress(old_recovery, new_recovery, "recovery")
        old_completed = old_recovery["completed_epoch"]
        new_completed = new_recovery["completed_epoch"]
        if old_completed is not None and new_completed != old_completed:
            raise OperationStateError("completed recovery receipt is immutable")
        if old_completed is None and new_completed is not None and new_completed < new_recovery["started_epoch"]:
            raise OperationStateError("recovery completion precedes its start")


def validate_operation_state_for_context(
    state: dict[str, object],
    context: OperationsContext,
) -> None:
    validate_operation_state(state, context.paths.operation_dir)
    if context.paths != build_operation_paths(context.paths.operation_dir):
        raise OperationStateError("operations context paths do not match the fixed operation paths")
    if state["operation_id"] != context.operation_id:
        raise OperationStateError("operation_id does not match the operations context")
    if state["operation_dir"] != str(context.paths.operation_dir):
        raise OperationStateError("operation_dir does not match the operations context")
    reviewed = state["reviewed_source"]
    assert isinstance(reviewed, dict)
    bindings = {
        "commit": context.expected_commit,
        "archive_sha256": context.expected_archive_sha256,
        "manifest_sha256": context.expected_manifest_sha256,
    }
    for field, expected in bindings.items():
        if reviewed[field] != expected:
            raise OperationStateError(f"reviewed_source.{field} does not match the operations context")


def _validate_source_context(context: OperationsContext, source_dir: Path) -> None:
    expected_paths = build_operation_paths(context.paths.operation_dir)
    for field in (
        "operation_dir",
        "source_archive",
        "source_dir",
        "snapshot_dir",
        "staged_dir",
        "state_file",
    ):
        if str(getattr(context.paths, field)) != str(getattr(expected_paths, field)):
            raise OperationStateError("operations context paths do not match the fixed operation paths")
    if str(source_dir) != str(context.paths.source_dir):
        raise OperationStateError("source directory does not match the operations context")
    if context.operation_id != context.paths.operation_dir.name:
        raise OperationStateError("operation_id does not match the operation directory")
    if _GIT_COMMIT_RE.fullmatch(context.expected_commit) is None:
        raise OperationStateError("expected Git commit is not a lowercase 40-hex object ID")
    if _SHA256_RE.fullmatch(context.expected_archive_sha256) is None:
        raise OperationStateError("expected archive SHA-256 is invalid")
    if _SHA256_RE.fullmatch(context.expected_manifest_sha256) is None:
        raise OperationStateError("expected manifest SHA-256 is invalid")


def _validate_source_archive_metadata(metadata: os.stat_result, effective_uid: int) -> None:
    if not stat.S_ISREG(metadata.st_mode):
        raise OperationStateError("source archive is not a regular file")
    if metadata.st_uid != effective_uid:
        raise OperationStateError("source archive is not owned by the effective UID")
    if metadata.st_nlink != 1:
        raise OperationStateError("source archive must have a single link")
    if os.name == "posix" and stat.S_IMODE(metadata.st_mode) & 0o022:
        raise OperationStateError("source archive mode permits untrusted writes")
    if metadata.st_size <= 0 or metadata.st_size > _MAX_SOURCE_ARCHIVE_BYTES:
        raise OperationStateError("source archive size is invalid")


def _source_binding_metadata(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_nlink,
    )


@contextlib.contextmanager
def _open_source_archive(
    context: OperationsContext,
    directory_fd: int | None,
):
    descriptor: int | None = None
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        if directory_fd is None:
            named_before = context.paths.source_archive.lstat()
            _validate_source_archive_metadata(named_before, context.effective_uid)
            descriptor = os.open(context.paths.source_archive, flags)
            opened = os.fstat(descriptor)
            named_after = context.paths.source_archive.lstat()
        else:
            named_before = os.stat("source.tar", dir_fd=directory_fd, follow_symlinks=False)
            _validate_source_archive_metadata(named_before, context.effective_uid)
            descriptor = os.open("source.tar", flags, dir_fd=directory_fd)
            opened = os.fstat(descriptor)
            named_after = os.stat("source.tar", dir_fd=directory_fd, follow_symlinks=False)
    except OSError as exc:
        if descriptor is not None:
            os.close(descriptor)
        raise OperationStateError("source archive open failed") from exc
    try:
        _validate_source_archive_metadata(opened, context.effective_uid)
        if not _same_identity(named_before, opened) or not _same_identity(opened, named_after):
            raise OperationStateError("source archive path changed while opening")
        if _stable_file_metadata(named_before) != _stable_file_metadata(named_after):
            raise OperationStateError("source archive path metadata changed while opening")
        if _source_binding_metadata(opened) != _source_binding_metadata(named_after):
            raise OperationStateError("source archive metadata changed while opening")
        yield descriptor, opened
    finally:
        os.close(descriptor)


def _revalidate_source_archive(
    context: OperationsContext,
    directory_fd: int | None,
    descriptor: int,
    initial: os.stat_result,
) -> None:
    try:
        opened = os.fstat(descriptor)
        if directory_fd is None:
            named = context.paths.source_archive.lstat()
        else:
            named = os.stat("source.tar", dir_fd=directory_fd, follow_symlinks=False)
    except OSError as exc:
        raise OperationStateError("source archive revalidation failed") from exc
    _validate_source_archive_metadata(opened, context.effective_uid)
    _validate_source_archive_metadata(named, context.effective_uid)
    if _stable_file_metadata(opened) != _stable_file_metadata(initial):
        raise OperationStateError("source archive changed during verification")
    if not _same_identity(opened, named):
        raise OperationStateError("source archive path changed during verification")
    if _source_binding_metadata(opened) != _source_binding_metadata(named):
        raise OperationStateError("source archive metadata changed during verification")


def _hash_source_archive(descriptor: int) -> str:
    digest = hashlib.sha256()
    total = 0
    try:
        os.lseek(descriptor, 0, os.SEEK_SET)
        while True:
            chunk = os.read(descriptor, 64 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if total > _MAX_SOURCE_ARCHIVE_BYTES:
                raise OperationStateError("source archive exceeds the size limit")
            digest.update(chunk)
    except OSError as exc:
        raise OperationStateError("source archive hash failed") from exc
    return digest.hexdigest()


def _default_command_runner(
    argv: Sequence[str],
    pass_fds: tuple[int, ...],
) -> subprocess.CompletedProcess[str]:
    if pass_fds and os.name != "posix":
        raise OperationStateError("inherited file descriptors require POSIX")
    common: dict[str, object] = {
        "check": False,
        "shell": False,
        "text": True,
        "close_fds": True,
        "pass_fds": pass_fds,
        "env": {
            "LANG": "C",
            "LC_ALL": "C",
            "PATH": "/usr/sbin:/usr/bin:/sbin:/bin",
        },
    }
    if tuple(argv[:1]) == (_PG_RESTORE_EXECUTABLE,):
        completed = subprocess.run(
            list(argv),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            **common,
        )
        completed.stdout = ""
        completed.stderr = ""
        return completed
    return subprocess.run(list(argv), capture_output=True, **common)


def _verify_git_archive_commit(context: OperationsContext, descriptor: int) -> None:
    argv = (
        sys.executable,
        "-c",
        _INHERITED_FD_EXEC_SHIM,
        "git-stdin",
        str(descriptor),
        _GIT_EXECUTABLE,
        "git",
        "get-tar-commit-id",
    )
    try:
        os.lseek(descriptor, 0, os.SEEK_SET)
        completed = context.command_runner(argv, (descriptor,))
        os.lseek(descriptor, 0, os.SEEK_SET)
    except OperationStateError:
        raise
    except Exception as exc:
        raise OperationStateError("Git archive commit verification failed") from exc
    if type(completed) is not subprocess.CompletedProcess:
        raise OperationStateError("Git archive commit verifier returned an invalid result")
    if type(completed.returncode) is not int or completed.returncode != 0:
        raise OperationStateError("Git archive commit verification failed")
    if type(completed.stdout) is not str:
        raise OperationStateError("Git archive commit verifier returned invalid output")
    if completed.stdout != context.expected_commit + "\n":
        raise OperationStateError("Git archive commit does not match the expected commit")


def _read_exact_descriptor(descriptor: int, size: int, label: str) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        try:
            chunk = os.read(descriptor, min(remaining, 64 * 1024))
        except OSError as exc:
            raise OperationStateError(f"{label} read failed") from exc
        if not chunk:
            raise OperationStateError(f"{label} is truncated")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _tar_octal(field: bytes, label: str) -> int:
    if len(field) < 2 or field[-1:] != b"\0":
        raise OperationStateError(f"source archive {label} is not canonical octal")
    digits = field[:-1]
    if any(value < ord("0") or value > ord("7") for value in digits):
        raise OperationStateError(f"source archive {label} is not canonical octal")
    return int(digits, 8)


def _tar_checksum(field: bytes) -> int:
    if len(field) != 8 or field[-1:] != b"\0":
        raise OperationStateError("source archive checksum is not canonical octal")
    digits = field[:7]
    if any(value < ord("0") or value > ord("7") for value in digits):
        raise OperationStateError("source archive checksum is not canonical octal")
    return int(digits, 8)


def _tar_text_field(field: bytes, label: str) -> str:
    head, separator, tail = field.partition(b"\0")
    if separator and any(tail):
        raise OperationStateError(f"source archive {label} has nonzero padding")
    try:
        value = head.decode("ascii")
    except UnicodeDecodeError as exc:
        raise OperationStateError(f"source archive {label} is not ASCII") from exc
    return value


def _canonical_pax_commit_record(commit: str) -> bytes:
    body = f"comment={commit}\n".encode("ascii")
    length = len(body) + 2
    while True:
        record = str(length).encode("ascii") + b" " + body
        if len(record) == length:
            return record
        length = len(record)


def _expected_git_archive_mode(name: str, member_type: bytes) -> int:
    if member_type == tarfile.XGLTYPE:
        return 0o666
    normalized = name[:-1] if name.endswith("/") else name
    if member_type == tarfile.DIRTYPE or normalized in _EXECUTABLE_SOURCE_ASSETS:
        return 0o775
    return 0o664


def _validate_git_tar_header_metadata(
    block: bytes,
    name: str,
    member_type: bytes,
    expected_mtime: int | None,
) -> int:
    mode = _tar_octal(block[100:108], "member mode")
    uid = _tar_octal(block[108:116], "member UID")
    gid = _tar_octal(block[116:124], "member GID")
    mtime = _tar_octal(block[136:148], "member mtime")
    device_major = _tar_octal(block[329:337], "device major")
    device_minor = _tar_octal(block[337:345], "device minor")
    if mode != _expected_git_archive_mode(name, member_type):
        raise OperationStateError("source archive mode metadata is not canonical Git tar")
    if uid != 0 or gid != 0:
        raise OperationStateError("source archive owner metadata is not canonical Git tar")
    if block[265:297] != b"root".ljust(32, b"\0"):
        raise OperationStateError("source archive user metadata is not canonical Git tar")
    if block[297:329] != b"root".ljust(32, b"\0"):
        raise OperationStateError("source archive group metadata is not canonical Git tar")
    if device_major != 0 or device_minor != 0:
        raise OperationStateError("source archive device metadata is not canonical Git tar")
    if any(block[345:512]):
        raise OperationStateError("source archive extension metadata is not canonical Git tar")
    if expected_mtime is not None and mtime != expected_mtime:
        raise OperationStateError("source archive mtime metadata is not commit-bound")
    return mtime


def _verify_raw_git_archive(
    descriptor: int,
    expected_commit: str,
    expected_manifest_sha256: str,
) -> tuple[dict[str, str], bytes]:
    expected_raw_names = {
        *(name + "/" for name in _SOURCE_DIRECTORIES),
        *_SOURCE_FILES,
    }
    observed: set[str] = set()
    asset_hashes: dict[str, str] = {}
    manifest_bytes: bytes | None = None
    global_header_seen = False
    archive_mtime: int | None = None
    material_end = 0
    total = 0
    zero_blocks = 0
    try:
        os.lseek(descriptor, 0, os.SEEK_SET)
    except OSError as exc:
        raise OperationStateError("source archive seek failed") from exc
    while True:
        block = _read_exact_descriptor(descriptor, _TAR_BLOCK_BYTES, "source archive header")
        total += len(block)
        if not any(block):
            zero_blocks += 1
            if zero_blocks < 2:
                continue
            while True:
                try:
                    trailing = os.read(descriptor, 64 * 1024)
                except OSError as exc:
                    raise OperationStateError("source archive trailer read failed") from exc
                if not trailing:
                    break
                total += len(trailing)
                if total > _MAX_SOURCE_ARCHIVE_BYTES:
                    raise OperationStateError("source archive exceeds the size limit")
                if any(trailing):
                    raise OperationStateError("source archive has a trailing or concatenated payload")
            canonical_size = (
                (material_end + 2 * _TAR_BLOCK_BYTES + _GIT_TAR_RECORD_BYTES - 1)
                // _GIT_TAR_RECORD_BYTES
            ) * _GIT_TAR_RECORD_BYTES
            if total != canonical_size:
                raise OperationStateError("source archive padding exceeds the canonical Git tar record")
            break
        if zero_blocks:
            raise OperationStateError("source archive has only one end-of-archive block")
        checksum = _tar_checksum(block[148:156])
        checksum_block = bytearray(block)
        checksum_block[148:156] = b"        "
        if sum(checksum_block) != checksum:
            raise OperationStateError("source archive header checksum is invalid")
        if block[257:263] != b"ustar\0" or block[263:265] != b"00":
            raise OperationStateError("source archive header format is not canonical Git tar")
        name = _tar_text_field(block[0:100], "member name")
        linkname = _tar_text_field(block[157:257], "link name")
        member_type = block[156:157]
        size = _tar_octal(block[124:136], "member size")
        archive_mtime = _validate_git_tar_header_metadata(
            block,
            name,
            member_type,
            archive_mtime,
        )
        if size > _MAX_SOURCE_FILE_BYTES:
            raise OperationStateError("source archive member exceeds the size limit")
        padded_size = ((size + _TAR_BLOCK_BYTES - 1) // _TAR_BLOCK_BYTES) * _TAR_BLOCK_BYTES
        payload = _read_exact_descriptor(descriptor, padded_size, "source archive member")
        total += padded_size
        if total > _MAX_SOURCE_ARCHIVE_BYTES:
            raise OperationStateError("source archive exceeds the size limit")
        if any(payload[size:]):
            raise OperationStateError("source archive member padding is nonzero")
        contents = payload[:size]
        if not global_header_seen:
            if member_type != tarfile.XGLTYPE or name != "pax_global_header" or linkname:
                raise OperationStateError("source archive is missing the canonical Git commit header")
            if contents != _canonical_pax_commit_record(expected_commit):
                raise OperationStateError("source archive Git commit metadata is not canonical")
            global_header_seen = True
            material_end = total
            continue
        if member_type not in (tarfile.REGTYPE, tarfile.DIRTYPE):
            raise OperationStateError("source archive contains a forbidden member type")
        if linkname:
            raise OperationStateError("source archive contains a link target")
        if name in observed:
            raise OperationStateError("source archive contains duplicate member names")
        if name not in expected_raw_names:
            raise OperationStateError("source archive contains an unexpected or unsafe member name")
        if name.endswith("/"):
            if member_type != tarfile.DIRTYPE or size != 0:
                raise OperationStateError("source archive parent entry is not an exact directory")
        elif member_type != tarfile.REGTYPE:
            raise OperationStateError("source archive reviewed asset is not a regular file")
        else:
            digest = hashlib.sha256(contents).hexdigest()
            if name == _SOURCE_MANIFEST:
                manifest_bytes = contents
                if digest != expected_manifest_sha256:
                    raise OperationStateError(
                        "source archive manifest does not match the approved SHA-256"
                    )
            else:
                asset_hashes[name] = digest
        observed.add(name)
        material_end = total
    if not global_header_seen or observed != expected_raw_names or manifest_bytes is None:
        raise OperationStateError("source archive members do not match the reviewed source set")
    manifest_hashes = _parse_source_manifest(manifest_bytes)
    if asset_hashes != manifest_hashes:
        raise OperationStateError("source archive asset hashes do not match the strict manifest")
    return manifest_hashes, manifest_bytes


def _safe_manifest_path(path: str) -> bool:
    if not path or path.startswith("/") or "\\" in path or "\0" in path:
        return False
    components = path.split("/")
    return all(component not in ("", ".", "..") for component in components)


def _parse_source_manifest(raw: bytes) -> dict[str, str]:
    if not raw or len(raw) > _MAX_SOURCE_MANIFEST_BYTES:
        raise OperationStateError("source manifest size is invalid")
    if not raw.endswith(b"\n") or b"\r" in raw or b"\0" in raw:
        raise OperationStateError("source manifest must use exact LF records")
    lines = raw[:-1].split(b"\n")
    if not lines or any(not line for line in lines):
        raise OperationStateError("source manifest contains a blank record")
    records: dict[str, str] = {}
    for line in lines:
        if len(line) <= 66 or line[64:66] != b"  ":
            raise OperationStateError("source manifest record grammar is invalid")
        try:
            digest = line[:64].decode("ascii")
            path = line[66:].decode("ascii")
        except UnicodeDecodeError as exc:
            raise OperationStateError("source manifest records must be ASCII") from exc
        if _SHA256_RE.fullmatch(digest) is None:
            raise OperationStateError("source manifest digest is invalid")
        if not _safe_manifest_path(path):
            raise OperationStateError("source manifest path is unsafe")
        if path == _SOURCE_MANIFEST:
            raise OperationStateError("source manifest cannot contain a self-entry")
        if path in records:
            raise OperationStateError("source manifest paths must be unique")
        records[path] = digest
    if frozenset(records) != _SOURCE_ASSETS:
        raise OperationStateError("source manifest must contain the exact reviewed asset set")
    return records


def _validate_source_directory_metadata(
    metadata: os.stat_result,
    effective_uid: int,
    label: str,
) -> None:
    if not stat.S_ISDIR(metadata.st_mode):
        raise OperationStateError(f"{label} is not a real directory")
    if metadata.st_uid != effective_uid:
        raise OperationStateError(f"{label} is not owned by the effective UID")
    if os.name == "posix" and stat.S_IMODE(metadata.st_mode) & 0o7022:
        raise OperationStateError(f"{label} mode is unsafe")


def _validate_source_file_metadata(
    metadata: os.stat_result,
    effective_uid: int,
    label: str,
) -> None:
    if not stat.S_ISREG(metadata.st_mode):
        raise OperationStateError(f"{label} is not a regular file")
    if metadata.st_uid != effective_uid:
        raise OperationStateError(f"{label} is not owned by the effective UID")
    if metadata.st_nlink != 1:
        raise OperationStateError(f"{label} must have a single link")
    if os.name == "posix" and stat.S_IMODE(metadata.st_mode) & 0o7022:
        raise OperationStateError(f"{label} mode is unsafe")
    if metadata.st_size < 0 or metadata.st_size > _MAX_SOURCE_FILE_BYTES:
        raise OperationStateError(f"{label} size is invalid")


def _hash_open_source_file(
    descriptor: int,
    initial: os.stat_result,
    label: str,
    *,
    capture: bool,
) -> tuple[str, bytes | None]:
    digest = hashlib.sha256()
    contents = bytearray() if capture else None
    total = 0
    try:
        os.lseek(descriptor, 0, os.SEEK_SET)
        while True:
            chunk = os.read(descriptor, 64 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if total > _MAX_SOURCE_FILE_BYTES:
                raise OperationStateError(f"{label} exceeds the size limit")
            digest.update(chunk)
            if contents is not None:
                contents.extend(chunk)
        final = os.fstat(descriptor)
    except OSError as exc:
        raise OperationStateError(f"{label} read failed") from exc
    if total != initial.st_size or _stable_file_metadata(final) != _stable_file_metadata(initial):
        raise OperationStateError(f"{label} changed while reading")
    return digest.hexdigest(), bytes(contents) if contents is not None else None


def _validate_source_basename(name: str) -> None:
    if not name or name in (".", "..") or "/" in name or "\\" in name or "\0" in name:
        raise OperationStateError("source tree contains an unsafe entry name")


@dataclass
class _SourceDirectoryProof:
    relative: str
    descriptor: int | None
    parent_descriptor: int | None
    name: str
    path: Path | None
    metadata: os.stat_result


@dataclass
class _SourceFileProof:
    relative: str
    descriptor: int
    parent_descriptor: int | None
    name: str
    path: Path | None
    metadata: os.stat_result
    sha256: str
    contents: bytes | None


@dataclass
class _SourceTreeProof:
    context: OperationsContext
    directories: list[_SourceDirectoryProof]
    files: list[_SourceFileProof]
    asset_hashes: dict[str, str]
    manifest_bytes: bytes


def _close_source_tree_proof(proof: _SourceTreeProof) -> None:
    for item in reversed(proof.files):
        try:
            os.close(item.descriptor)
        except OSError:
            pass
    for item in reversed(proof.directories):
        if item.descriptor is not None:
            try:
                os.close(item.descriptor)
            except OSError:
                pass


def _expected_source_children(relative_parent: str) -> frozenset[str]:
    children: set[str] = set()
    for path in _SOURCE_DIRECTORIES | _SOURCE_FILES:
        parent, separator, name = path.rpartition("/")
        if (parent if separator else "") == relative_parent:
            children.add(name)
    return frozenset(children)


def _collect_source_directory(
    proof: _SourceTreeProof,
    directory_fd: int | None,
    directory_path: Path | None,
    relative_parent: str,
) -> None:
    expected_children = _expected_source_children(relative_parent)
    scan_target: int | Path
    scan_target = directory_fd if directory_fd is not None else directory_path  # type: ignore[assignment]
    if scan_target is None:
        raise OperationStateError("source directory proof is incomplete")
    try:
        iterator = os.scandir(scan_target)
    except OSError as exc:
        raise OperationStateError("source directory listing failed") from exc
    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    file_flags = (
        os.O_RDONLY
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_BINARY", 0)
    )
    seen: set[str] = set()
    try:
        with iterator:
            for entry in iterator:
                if len(seen) >= len(expected_children):
                    raise OperationStateError("source directory entries exceed the fixed child bound")
                name = entry.name
                _validate_source_basename(name)
                if name not in expected_children or name in seen:
                    raise OperationStateError("source tree contains an extra or duplicate entry")
                seen.add(name)
                relative = f"{relative_parent}/{name}" if relative_parent else name
                path = None if directory_path is None else directory_path / name
                try:
                    named = (
                        os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
                        if directory_fd is not None
                        else path.lstat()  # type: ignore[union-attr]
                    )
                except OSError as exc:
                    raise OperationStateError("source tree metadata read failed") from exc
                if relative in _SOURCE_DIRECTORIES:
                    _validate_source_directory_metadata(
                        named,
                        proof.context.effective_uid,
                        f"source directory {relative}",
                    )
                    if directory_fd is None:
                        proof.directories.append(
                            _SourceDirectoryProof(relative, None, None, name, path, named)
                        )
                        _collect_source_directory(proof, None, path, relative)
                    else:
                        try:
                            child_fd = os.open(name, directory_flags, dir_fd=directory_fd)
                        except OSError as exc:
                            raise OperationStateError("source directory open failed") from exc
                        try:
                            opened = os.fstat(child_fd)
                            if not _same_identity(named, opened):
                                raise OperationStateError("source directory changed while opening")
                            _validate_source_directory_metadata(
                                opened,
                                proof.context.effective_uid,
                                f"source directory {relative}",
                            )
                            proof.directories.append(
                                _SourceDirectoryProof(
                                    relative,
                                    child_fd,
                                    directory_fd,
                                    name,
                                    None,
                                    opened,
                                )
                            )
                        except BaseException:
                            try:
                                os.close(child_fd)
                            except OSError:
                                pass
                            raise
                        _collect_source_directory(proof, child_fd, None, relative)
                    continue
                _validate_source_file_metadata(
                    named,
                    proof.context.effective_uid,
                    f"source file {relative}",
                )
                try:
                    file_fd = (
                        os.open(name, file_flags, dir_fd=directory_fd)
                        if directory_fd is not None
                        else os.open(path, file_flags)  # type: ignore[arg-type]
                    )
                except OSError as exc:
                    raise OperationStateError("source file open failed") from exc
                try:
                    opened = os.fstat(file_fd)
                    if not _same_identity(named, opened):
                        raise OperationStateError("source file changed while opening")
                    _validate_source_file_metadata(
                        opened,
                        proof.context.effective_uid,
                        f"source file {relative}",
                    )
                    digest, contents = _hash_open_source_file(
                        file_fd,
                        opened,
                        f"source file {relative}",
                        capture=relative == _SOURCE_MANIFEST,
                    )
                except Exception:
                    os.close(file_fd)
                    raise
                proof.files.append(
                    _SourceFileProof(
                        relative,
                        file_fd,
                        directory_fd,
                        name,
                        path,
                        opened,
                        digest,
                        contents,
                    )
                )
    except OSError as exc:
        raise OperationStateError("source directory enumeration failed") from exc
    if seen != set(expected_children):
        raise OperationStateError("source directory is missing reviewed entries")


def _named_source_proof_metadata(
    parent_descriptor: int | None,
    name: str,
    path: Path | None,
) -> os.stat_result:
    try:
        if parent_descriptor is not None:
            return os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
        if path is None:
            raise OperationStateError("source proof path binding is missing")
        return path.lstat()
    except OSError as exc:
        raise OperationStateError("source proof path revalidation failed") from exc


def _validate_source_directory_proof(
    proof: _SourceTreeProof,
    item: _SourceDirectoryProof,
) -> None:
    opened = os.fstat(item.descriptor) if item.descriptor is not None else item.path.lstat()  # type: ignore[union-attr]
    named = _named_source_proof_metadata(
        item.parent_descriptor,
        item.name,
        item.path,
    )
    _validate_source_directory_metadata(
        opened,
        proof.context.effective_uid,
        f"source proof directory {item.relative or '.'}",
    )
    if _stable_file_metadata(opened) != _stable_file_metadata(item.metadata):
        raise OperationStateError("source proof directory changed after verification")
    if not _same_identity(opened, named):
        raise OperationStateError("source proof directory path changed after verification")
    if _source_binding_metadata(opened) != _source_binding_metadata(named):
        raise OperationStateError("source proof directory metadata changed after verification")


def _validate_source_file_proof(
    proof: _SourceTreeProof,
    item: _SourceFileProof,
    *,
    rehash: bool,
) -> None:
    opened = os.fstat(item.descriptor)
    named = _named_source_proof_metadata(
        item.parent_descriptor,
        item.name,
        item.path,
    )
    _validate_source_file_metadata(
        opened,
        proof.context.effective_uid,
        f"source proof file {item.relative}",
    )
    if _stable_file_metadata(opened) != _stable_file_metadata(item.metadata):
        raise OperationStateError("source proof file changed after verification")
    if not _same_identity(opened, named):
        raise OperationStateError("source proof file path changed after verification")
    if _source_binding_metadata(opened) != _source_binding_metadata(named):
        raise OperationStateError("source proof file metadata changed after verification")
    if rehash:
        digest, _ = _hash_open_source_file(
            item.descriptor,
            item.metadata,
            f"source proof file {item.relative}",
            capture=False,
        )
        if digest != item.sha256:
            raise OperationStateError("source proof file hash changed after verification")


def _revalidate_source_tree_proof(proof: _SourceTreeProof) -> None:
    for item in proof.directories:
        _validate_source_directory_proof(proof, item)
    for item in proof.files:
        _validate_source_file_proof(proof, item, rehash=True)
    for item in proof.directories:
        _validate_source_directory_proof(proof, item)
    for item in proof.files:
        _validate_source_file_proof(proof, item, rehash=False)


def _verify_extracted_source_tree(
    context: OperationsContext,
    directory_fd: int | None,
    archive_hashes: dict[str, str],
    archive_manifest: bytes,
) -> _SourceTreeProof:
    proof = _SourceTreeProof(context, [], [], {}, b"")
    try:
        if directory_fd is None:
            root_named = context.paths.source_dir.lstat()
            _validate_source_directory_metadata(
                root_named,
                context.effective_uid,
                "source directory",
            )
            proof.directories.append(
                _SourceDirectoryProof(
                    "",
                    None,
                    None,
                    context.paths.source_dir.name,
                    context.paths.source_dir,
                    root_named,
                )
            )
            _collect_source_directory(proof, None, context.paths.source_dir, "")
        else:
            directory_flags = (
                os.O_RDONLY
                | getattr(os, "O_DIRECTORY", 0)
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_CLOEXEC", 0)
            )
            root_named = os.stat("source", dir_fd=directory_fd, follow_symlinks=False)
            source_fd = os.open("source", directory_flags, dir_fd=directory_fd)
            try:
                root_opened = os.fstat(source_fd)
                if not _same_identity(root_named, root_opened):
                    raise OperationStateError("source directory changed while opening")
                _validate_source_directory_metadata(
                    root_opened,
                    context.effective_uid,
                    "source directory",
                )
                proof.directories.append(
                    _SourceDirectoryProof(
                        "",
                        source_fd,
                        directory_fd,
                        "source",
                        None,
                        root_opened,
                    )
                )
            except BaseException:
                try:
                    os.close(source_fd)
                except OSError:
                    pass
                raise
            _collect_source_directory(proof, source_fd, None, "")
        hashes = {item.relative: item.sha256 for item in proof.files}
        manifest_items = [item for item in proof.files if item.relative == _SOURCE_MANIFEST]
        if len(manifest_items) != 1 or manifest_items[0].contents is None:
            raise OperationStateError("source tree manifest is missing or ambiguous")
        extracted_manifest = manifest_items[0].contents
        if hashlib.sha256(extracted_manifest).hexdigest() != context.expected_manifest_sha256:
            raise OperationStateError("source tree manifest does not match the approved SHA-256")
        if extracted_manifest != archive_manifest:
            raise OperationStateError("source tree manifest differs from the reviewed archive manifest")
        extracted_manifest_hashes = _parse_source_manifest(extracted_manifest)
        extracted_asset_hashes = {
            name: digest for name, digest in hashes.items() if name != _SOURCE_MANIFEST
        }
        if extracted_manifest_hashes != archive_hashes or extracted_asset_hashes != archive_hashes:
            raise OperationStateError("source tree asset hashes do not match the reviewed manifest")
        proof.asset_hashes = extracted_asset_hashes
        proof.manifest_bytes = extracted_manifest
        return proof
    except Exception:
        _close_source_tree_proof(proof)
        raise


def _load_existing_verified_source_state(
    context: OperationsContext,
    directory_fd: int | None,
) -> dict[str, object] | None:
    try:
        if directory_fd is None:
            context.paths.state_file.lstat()
            raw = _read_state_file_fallback(context.paths.state_file, context.effective_uid)
        else:
            os.stat("operation-state.json", dir_fd=directory_fd, follow_symlinks=False)
            raw = _read_state_file_posix(directory_fd, context.effective_uid)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise OperationStateError("operation state existence check failed") from exc
    state = _decode_operation_state(raw)
    validate_operation_state_for_context(state, context)
    return state


def _source_verification_evidence(reviewed_source: dict[str, object]) -> bytes:
    canonical = json.dumps(
        reviewed_source,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return b"degen-source-verification-v1\n" + canonical + b"\n"


def _source_verified_state(
    context: OperationsContext,
    asset_hashes: dict[str, str],
) -> dict[str, object]:
    now = context.clock()
    if type(now) is not datetime or now.tzinfo is None or now.utcoffset() != timedelta(0):
        raise OperationStateError("operations clock must return an aware UTC datetime")
    epoch = int(now.astimezone(timezone.utc).timestamp())
    reviewed_source: dict[str, object] = {
        "commit": context.expected_commit,
        "archive_sha256": context.expected_archive_sha256,
        "manifest_sha256": context.expected_manifest_sha256,
        "asset_hashes": dict(asset_hashes),
    }
    evidence_sha256 = hashlib.sha256(_source_verification_evidence(reviewed_source)).hexdigest()
    return {
        "schema_version": 1,
        "operation_id": context.operation_id,
        "operation_dir": str(context.paths.operation_dir),
        "phase": "source_verified",
        "phase_history": [
            {
                "phase": "source_verified",
                "epoch": epoch,
                "evidence_sha256": evidence_sha256,
            }
        ],
        "reviewed_source": reviewed_source,
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


def _revalidate_source_receipt_proof(
    context: OperationsContext,
    directory_fd: int | None,
    operation_metadata: os.stat_result,
    archive_fd: int,
    archive_metadata: os.stat_result,
    source_proof: _SourceTreeProof,
) -> None:
    _revalidate_operation_dir_binding(
        context.paths.operation_dir,
        directory_fd,
        operation_metadata,
        context.effective_uid,
    )
    _revalidate_source_archive(
        context,
        directory_fd,
        archive_fd,
        archive_metadata,
    )
    if _hash_source_archive(archive_fd) != context.expected_archive_sha256:
        raise OperationStateError(
            "source archive SHA-256 changed before operation state replacement"
        )
    _revalidate_source_archive(
        context,
        directory_fd,
        archive_fd,
        archive_metadata,
    )
    _revalidate_source_tree_proof(source_proof)
    _revalidate_source_archive(
        context,
        directory_fd,
        archive_fd,
        archive_metadata,
    )
    if _hash_source_archive(archive_fd) != context.expected_archive_sha256:
        raise OperationStateError(
            "source archive SHA-256 changed before operation state replacement"
        )
    _revalidate_source_archive(
        context,
        directory_fd,
        archive_fd,
        archive_metadata,
    )
    _revalidate_operation_dir_binding(
        context.paths.operation_dir,
        directory_fd,
        operation_metadata,
        context.effective_uid,
    )


def verify_source_archive(
    context: OperationsContext,
    *,
    source_dir: Path,
) -> dict[str, str]:
    _validate_source_context(context, source_dir)
    source_proof: _SourceTreeProof | None = None
    try:
        with _open_validated_operation_dir(
            context.paths.operation_dir,
            context.effective_uid,
        ) as directory_fd:
            operation_metadata = (
                context.paths.operation_dir.lstat()
                if directory_fd is None
                else os.fstat(directory_fd)
            )
            with _open_source_archive(context, directory_fd) as (archive_fd, archive_metadata):
                archive_sha256 = _hash_source_archive(archive_fd)
                _revalidate_source_archive(
                    context,
                    directory_fd,
                    archive_fd,
                    archive_metadata,
                )
                if archive_sha256 != context.expected_archive_sha256:
                    raise OperationStateError(
                        "source archive SHA-256 does not match the approved digest"
                    )
                _verify_git_archive_commit(context, archive_fd)
                _revalidate_source_archive(
                    context,
                    directory_fd,
                    archive_fd,
                    archive_metadata,
                )
                archive_hashes, archive_manifest = _verify_raw_git_archive(
                    archive_fd,
                    context.expected_commit,
                    context.expected_manifest_sha256,
                )
                _revalidate_source_archive(
                    context,
                    directory_fd,
                    archive_fd,
                    archive_metadata,
                )
                source_proof = _verify_extracted_source_tree(
                    context,
                    directory_fd,
                    archive_hashes,
                    archive_manifest,
                )
                _revalidate_source_archive(
                    context,
                    directory_fd,
                    archive_fd,
                    archive_metadata,
                )
                _revalidate_source_receipt_proof(
                    context,
                    directory_fd,
                    operation_metadata,
                    archive_fd,
                    archive_metadata,
                    source_proof,
                )
                existing = _load_existing_verified_source_state(context, directory_fd)
                if existing is not None:
                    reviewed = existing["reviewed_source"]
                    history = existing["phase_history"]
                    assert isinstance(reviewed, dict) and isinstance(history, list)
                    expected_reviewed: dict[str, object] = {
                        "commit": context.expected_commit,
                        "archive_sha256": context.expected_archive_sha256,
                        "manifest_sha256": context.expected_manifest_sha256,
                        "asset_hashes": dict(source_proof.asset_hashes),
                    }
                    expected_evidence = hashlib.sha256(
                        _source_verification_evidence(expected_reviewed)
                    ).hexdigest()
                    if (
                        existing["phase"] != "source_verified"
                        or reviewed != expected_reviewed
                        or not history
                        or history[0]["evidence_sha256"] != expected_evidence
                    ):
                        raise OperationStateError(
                            "existing operation state evidence is not the identical source_verified receipt"
                        )
                    _revalidate_source_receipt_proof(
                        context,
                        directory_fd,
                        operation_metadata,
                        archive_fd,
                        archive_metadata,
                        source_proof,
                    )
                    return dict(source_proof.asset_hashes)
                state = _source_verified_state(context, source_proof.asset_hashes)
                validate_operation_state_for_context(state, context)

                def revalidate_receipt_proof() -> None:
                    _revalidate_source_receipt_proof(
                        context,
                        directory_fd,
                        operation_metadata,
                        archive_fd,
                        archive_metadata,
                        source_proof,
                    )

                revalidate_receipt_proof()
                _atomic_write_operation_state_internal(
                    context.paths.state_file,
                    state,
                    effective_uid=context.effective_uid,
                    pre_replace_validator=revalidate_receipt_proof,
                    operation_directory_binding=_OperationDirectoryBinding(
                        context.paths.operation_dir,
                        directory_fd,
                        operation_metadata,
                    ),
                )
                return dict(source_proof.asset_hashes)
    finally:
        if source_proof is not None:
            _close_source_tree_proof(source_proof)


@dataclass
class _VerifiedSourceMaterial:
    context: OperationsContext
    directory_fd: int | None
    operation_metadata: os.stat_result
    archive_fd: int
    archive_metadata: os.stat_result
    source_proof: _SourceTreeProof
    state: dict[str, object]


@contextlib.contextmanager
def _open_verified_source_material(context: OperationsContext):
    """Hold the complete source proof while a later receipt is prepared."""
    _validate_source_context(context, context.paths.source_dir)
    source_proof: _SourceTreeProof | None = None
    with _open_validated_operation_dir(
        context.paths.operation_dir,
        context.effective_uid,
    ) as directory_fd:
        operation_metadata = (
            context.paths.operation_dir.lstat()
            if directory_fd is None
            else os.fstat(directory_fd)
        )
        with _open_source_archive(context, directory_fd) as (archive_fd, archive_metadata):
            try:
                if _hash_source_archive(archive_fd) != context.expected_archive_sha256:
                    raise OperationStateError(
                        "source archive SHA-256 does not match the approved digest"
                    )
                _revalidate_source_archive(
                    context, directory_fd, archive_fd, archive_metadata
                )
                _verify_git_archive_commit(context, archive_fd)
                _revalidate_source_archive(
                    context, directory_fd, archive_fd, archive_metadata
                )
                archive_hashes, archive_manifest = _verify_raw_git_archive(
                    archive_fd,
                    context.expected_commit,
                    context.expected_manifest_sha256,
                )
                source_proof = _verify_extracted_source_tree(
                    context,
                    directory_fd,
                    archive_hashes,
                    archive_manifest,
                )
                _revalidate_source_receipt_proof(
                    context,
                    directory_fd,
                    operation_metadata,
                    archive_fd,
                    archive_metadata,
                    source_proof,
                )
                state = _load_existing_verified_source_state(context, directory_fd)
                if state is None or state["phase"] != "source_verified":
                    raise OperationStateError(
                        "prepare-staging requires strict source_verified operation state"
                    )
                reviewed = state["reviewed_source"]
                history = state["phase_history"]
                assert isinstance(reviewed, dict) and isinstance(history, list)
                expected_reviewed: dict[str, object] = {
                    "commit": context.expected_commit,
                    "archive_sha256": context.expected_archive_sha256,
                    "manifest_sha256": context.expected_manifest_sha256,
                    "asset_hashes": dict(source_proof.asset_hashes),
                }
                expected_evidence = hashlib.sha256(
                    _source_verification_evidence(expected_reviewed)
                ).hexdigest()
                if (
                    reviewed != expected_reviewed
                    or len(history) != 1
                    or history[0]["phase"] != "source_verified"
                    or history[0]["evidence_sha256"] != expected_evidence
                ):
                    raise OperationStateError(
                        "operation state is not the immutable source_verified receipt"
                    )
                yield _VerifiedSourceMaterial(
                    context,
                    directory_fd,
                    operation_metadata,
                    archive_fd,
                    archive_metadata,
                    source_proof,
                    state,
                )
            finally:
                if source_proof is not None:
                    _close_source_tree_proof(source_proof)


def _revalidate_verified_source_material(material: _VerifiedSourceMaterial) -> None:
    _revalidate_source_receipt_proof(
        material.context,
        material.directory_fd,
        material.operation_metadata,
        material.archive_fd,
        material.archive_metadata,
        material.source_proof,
    )


@contextlib.contextmanager
def _open_verified_later_material(
    context: OperationsContext,
    expected_phase: str,
):
    if expected_phase not in {"staging_prepared", "snapshotted"}:
        raise OperationStateError("later source proof phase is unsupported")
    _validate_source_context(context, context.paths.source_dir)
    source_proof: _SourceTreeProof | None = None
    with _open_validated_operation_dir(
        context.paths.operation_dir,
        context.effective_uid,
    ) as directory_fd:
        operation_metadata = (
            context.paths.operation_dir.lstat()
            if directory_fd is None
            else os.fstat(directory_fd)
        )
        with _open_source_archive(context, directory_fd) as (archive_fd, archive_metadata):
            try:
                if _hash_source_archive(archive_fd) != context.expected_archive_sha256:
                    raise OperationStateError(
                        "source archive SHA-256 does not match the approved digest"
                    )
                _revalidate_source_archive(
                    context, directory_fd, archive_fd, archive_metadata
                )
                _verify_git_archive_commit(context, archive_fd)
                _revalidate_source_archive(
                    context, directory_fd, archive_fd, archive_metadata
                )
                archive_hashes, archive_manifest = _verify_raw_git_archive(
                    archive_fd,
                    context.expected_commit,
                    context.expected_manifest_sha256,
                )
                source_proof = _verify_extracted_source_tree(
                    context,
                    directory_fd,
                    archive_hashes,
                    archive_manifest,
                )
                _revalidate_source_receipt_proof(
                    context,
                    directory_fd,
                    operation_metadata,
                    archive_fd,
                    archive_metadata,
                    source_proof,
                )
                state = _load_existing_verified_source_state(context, directory_fd)
                if state is None or state["phase"] != expected_phase:
                    raise OperationStateError(
                        f"snapshot requires strict {expected_phase} operation state"
                    )
                reviewed = state["reviewed_source"]
                effective_config = state["effective_config"]
                host_stage = state["host_stage"]
                history = state["phase_history"]
                assert isinstance(reviewed, dict)
                assert isinstance(effective_config, dict)
                assert isinstance(host_stage, dict)
                assert isinstance(history, list)
                expected_reviewed: dict[str, object] = {
                    "commit": context.expected_commit,
                    "archive_sha256": context.expected_archive_sha256,
                    "manifest_sha256": context.expected_manifest_sha256,
                    "asset_hashes": dict(source_proof.asset_hashes),
                }
                expected_source_evidence = hashlib.sha256(
                    _source_verification_evidence(expected_reviewed)
                ).hexdigest()
                expected_staging_evidence = hashlib.sha256(
                    _staging_evidence(effective_config, host_stage)
                ).hexdigest()
                expected_phases = ["source_verified", "staging_prepared"]
                if expected_phase == "snapshotted":
                    expected_phases.append("snapshotted")
                invalid = (
                    reviewed != expected_reviewed
                    or len(history) != len(expected_phases)
                    or [entry["phase"] for entry in history]
                    != expected_phases
                    or history[0]["evidence_sha256"] != expected_source_evidence
                    or history[1]["evidence_sha256"] != expected_staging_evidence
                )
                if expected_phase == "snapshotted":
                    snapshot = state["snapshot"]
                    prior_runtime = state["prior_runtime"]
                    assert isinstance(snapshot, dict)
                    assert isinstance(prior_runtime, dict)
                    expected_snapshot_evidence = hashlib.sha256(
                        _snapshot_evidence(snapshot, prior_runtime)
                    ).hexdigest()
                    invalid = invalid or (
                        history[2]["evidence_sha256"] != expected_snapshot_evidence
                    )
                if invalid:
                    raise OperationStateError(
                        f"operation state is not the immutable {expected_phase} receipt"
                    )
                yield _VerifiedSourceMaterial(
                    context,
                    directory_fd,
                    operation_metadata,
                    archive_fd,
                    archive_metadata,
                    source_proof,
                    state,
                )
            finally:
                if source_proof is not None:
                    _close_source_tree_proof(source_proof)


@contextlib.contextmanager
def _open_verified_staging_material(context: OperationsContext):
    """Hold source proofs for one strict staging_prepared operation state."""
    with _open_verified_later_material(context, "staging_prepared") as material:
        yield material


@contextlib.contextmanager
def _open_verified_snapshotted_material(context: OperationsContext):
    """Hold source proofs for one strict snapshotted operation state."""
    with _open_verified_later_material(context, "snapshotted") as material:
        yield material


def _capture_reviewed_asset_bytes(
    proof: _SourceTreeProof,
) -> dict[str, bytes]:
    captured: dict[str, bytes] = {}
    for item in proof.files:
        if item.relative not in _SOURCE_ASSETS:
            continue
        digest, contents = _hash_open_source_file(
            item.descriptor,
            item.metadata,
            f"source proof file {item.relative}",
            capture=True,
        )
        if contents is None or digest != proof.asset_hashes[item.relative]:
            raise OperationStateError("reviewed source asset changed while staging")
        captured[item.relative] = contents
    if frozenset(captured) != _SOURCE_ASSETS:
        raise OperationStateError("reviewed source asset capture is incomplete")
    _revalidate_source_tree_proof(proof)
    return captured


def _host_path(context: OperationsContext, logical_path: str) -> Path:
    if not isinstance(logical_path, str) or not logical_path.startswith("/"):
        raise OperationStateError("managed host path must be absolute")
    if "\\" in logical_path or "\0" in logical_path:
        raise OperationStateError("managed host path is unsafe")
    pure = PurePosixPath(logical_path)
    if str(pure) != logical_path or any(part in ("", ".", "..") for part in pure.parts[1:]):
        raise OperationStateError("managed host path is not canonical")
    if not context.host_root.is_absolute():
        raise OperationStateError("host_root must be absolute")
    try:
        resolved_root = context.host_root.resolve(strict=True)
    except OSError as exc:
        raise OperationStateError("host_root cannot be resolved safely") from exc
    lexical_root = Path(os.path.abspath(str(context.host_root)))
    if os.path.normcase(str(resolved_root)) != os.path.normcase(str(lexical_root)):
        raise OperationStateError("host_root contains a symlinked intermediate component")
    root_metadata = context.host_root.lstat()
    _validate_host_directory_metadata(
        root_metadata,
        context.effective_uid,
        "host_root",
    )
    return context.host_root.joinpath(*pure.parts[1:])


def _validate_host_directory_metadata(
    metadata: os.stat_result,
    effective_uid: int,
    label: str,
) -> None:
    if not stat.S_ISDIR(metadata.st_mode):
        raise OperationStateError(f"{label} is not a real directory")
    if metadata.st_uid != effective_uid:
        raise OperationStateError(f"{label} is not owned by the effective UID")
    if os.name == "posix" and stat.S_IMODE(metadata.st_mode) & 0o022:
        raise OperationStateError(f"{label} permits untrusted writes")


def _validate_host_file_metadata(
    metadata: os.stat_result,
    effective_uid: int,
    label: str,
    *,
    maximum_size: int,
    exact_mode: int = 0o600,
) -> None:
    if not stat.S_ISREG(metadata.st_mode):
        raise OperationStateError(f"{label} is not a regular file")
    if metadata.st_uid != effective_uid:
        raise OperationStateError(f"{label} is not owned by the effective UID")
    if metadata.st_nlink != 1:
        raise OperationStateError(f"{label} must have a single link")
    if os.name == "posix" and stat.S_IMODE(metadata.st_mode) != exact_mode:
        raise OperationStateError(f"{label} has an unsafe mode")
    if metadata.st_size < 0 or metadata.st_size > maximum_size:
        raise OperationStateError(f"{label} size is invalid")


@dataclass
class _HostRootProof:
    context: OperationsContext
    descriptors: list[int]
    names: list[str]
    metadata: list[os.stat_result]
    fallback_metadata: os.stat_result | None


def _revalidate_host_root_proof(proof: _HostRootProof) -> None:
    context = proof.context
    if not proof.descriptors:
        resolved = context.host_root.resolve(strict=True)
        lexical = Path(os.path.abspath(str(context.host_root)))
        if os.path.normcase(str(resolved)) != os.path.normcase(str(lexical)):
            raise OperationStateError("host_root contains a symlinked intermediate component")
        named = context.host_root.lstat()
        _validate_host_directory_metadata(named, context.effective_uid, "host_root")
        if proof.fallback_metadata is None or not _same_identity(
            named, proof.fallback_metadata
        ):
            raise OperationStateError("host_root binding changed during staging")
        return
    for index, descriptor in enumerate(proof.descriptors):
        opened = os.fstat(descriptor)
        if not stat.S_ISDIR(opened.st_mode):
            raise OperationStateError("host_root ancestor is not a directory")
        if not _same_identity(opened, proof.metadata[index]):
            raise OperationStateError("host_root ancestor identity changed")
        if index:
            named = os.stat(
                proof.names[index - 1],
                dir_fd=proof.descriptors[index - 1],
                follow_symlinks=False,
            )
            if not _same_identity(opened, named):
                raise OperationStateError("host_root ancestor binding changed")
    _validate_host_directory_metadata(
        os.fstat(proof.descriptors[-1]),
        context.effective_uid,
        "host_root",
    )


def _open_host_root_proof(context: OperationsContext) -> _HostRootProof:
    if os.name != "posix" or not _descriptor_primitives_available():
        _host_path(context, "/")
        metadata = context.host_root.lstat()
        proof = _HostRootProof(context, [], [], [], metadata)
        _revalidate_host_root_proof(proof)
        return proof
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW
    descriptors: list[int] = []
    names: list[str] = []
    metadata: list[os.stat_result] = []
    try:
        parts = context.host_root.parts
        root_fd = os.open(parts[0], flags)
        descriptors.append(root_fd)
        metadata.append(os.fstat(root_fd))
        for component in parts[1:]:
            _validate_source_basename(component)
            named = os.stat(
                component,
                dir_fd=descriptors[-1],
                follow_symlinks=False,
            )
            if not stat.S_ISDIR(named.st_mode):
                raise OperationStateError("host_root ancestor is not a real directory")
            child_fd = os.open(component, flags, dir_fd=descriptors[-1])
            opened = os.fstat(child_fd)
            if not _same_identity(named, opened):
                os.close(child_fd)
                raise OperationStateError("host_root ancestor changed while opening")
            names.append(component)
            descriptors.append(child_fd)
            metadata.append(opened)
        proof = _HostRootProof(context, descriptors, names, metadata, None)
        _revalidate_host_root_proof(proof)
        return proof
    except BaseException:
        for descriptor in reversed(descriptors):
            try:
                os.close(descriptor)
            except OSError:
                pass
        raise


def _close_host_root_proof(proof: _HostRootProof) -> None:
    for descriptor in reversed(proof.descriptors):
        try:
            os.close(descriptor)
        except OSError:
            pass
    proof.descriptors.clear()


@dataclass
class _HostDirectoryProof:
    context: OperationsContext
    host_root_proof: _HostRootProof
    logical_path: str
    path: Path
    descriptors: list[int]
    names: list[str]
    metadata: list[os.stat_result]

    @property
    def descriptor(self) -> int | None:
        return self.descriptors[-1] if self.descriptors else None


def _revalidate_host_directory_proof(proof: _HostDirectoryProof) -> None:
    context = proof.context
    _revalidate_host_root_proof(proof.host_root_proof)
    if not proof.descriptors:
        current = context.host_root
        for index, expected in enumerate(proof.metadata):
            named = current.lstat()
            _validate_host_directory_metadata(
                named, context.effective_uid, "host path directory"
            )
            if _stable_file_metadata(named) != _stable_file_metadata(expected):
                raise OperationStateError("host path directory changed during staging")
            if index < len(proof.names):
                current = current / proof.names[index]
        return
    root_opened = os.fstat(proof.descriptors[0])
    root_named = os.fstat(proof.host_root_proof.descriptors[-1])
    _validate_host_directory_metadata(
        root_opened, context.effective_uid, "host_root"
    )
    if not _same_identity(root_opened, root_named):
        raise OperationStateError("host_root path changed during staging")
    for index, descriptor in enumerate(proof.descriptors):
        opened = os.fstat(descriptor)
        expected = proof.metadata[index]
        _validate_host_directory_metadata(
            opened, context.effective_uid, "host path directory"
        )
        if _stable_file_metadata(opened) != _stable_file_metadata(expected):
            raise OperationStateError("host path directory changed during staging")
        if index:
            named = os.stat(
                proof.names[index - 1],
                dir_fd=proof.descriptors[index - 1],
                follow_symlinks=False,
            )
            if not _same_identity(opened, named):
                raise OperationStateError("host path binding changed during staging")


@contextlib.contextmanager
def _open_host_directory(context: OperationsContext, logical_path: str):
    path = _host_path(context, logical_path)
    components = list(PurePosixPath(logical_path).parts[1:])
    host_root_proof = _open_host_root_proof(context)
    if os.name != "posix" or not _descriptor_primitives_available():
        try:
            metadata: list[os.stat_result] = []
            current = context.host_root
            for component in (None, *components):
                if component is not None:
                    current = current / component
                named = current.lstat()
                _validate_host_directory_metadata(
                    named, context.effective_uid, "host path directory"
                )
                metadata.append(named)
            proof = _HostDirectoryProof(
                context,
                host_root_proof,
                logical_path,
                path,
                [],
                components,
                metadata,
            )
            _revalidate_host_directory_proof(proof)
            yield proof
            _revalidate_host_directory_proof(proof)
        finally:
            _close_host_root_proof(host_root_proof)
        return
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW
    descriptors: list[int] = []
    metadata = []
    try:
        root_fd = os.dup(host_root_proof.descriptors[-1])
        descriptors.append(root_fd)
        root_metadata = os.fstat(root_fd)
        _validate_host_directory_metadata(
            root_metadata, context.effective_uid, "host_root"
        )
        metadata.append(root_metadata)
        for component in components:
            _validate_source_basename(component)
            named = os.stat(component, dir_fd=descriptors[-1], follow_symlinks=False)
            _validate_host_directory_metadata(
                named, context.effective_uid, "host path directory"
            )
            child_fd = os.open(component, flags, dir_fd=descriptors[-1])
            opened = os.fstat(child_fd)
            if not _same_identity(named, opened):
                os.close(child_fd)
                raise OperationStateError("host path changed while opening")
            descriptors.append(child_fd)
            metadata.append(opened)
        proof = _HostDirectoryProof(
            context,
            host_root_proof,
            logical_path,
            path,
            descriptors,
            components,
            metadata,
        )
        _revalidate_host_directory_proof(proof)
        yield proof
        _revalidate_host_directory_proof(proof)
    except OSError as exc:
        raise OperationStateError("host directory access failed") from exc
    finally:
        for descriptor in reversed(descriptors):
            try:
                os.close(descriptor)
            except OSError:
                pass
        _close_host_root_proof(host_root_proof)


@dataclass
class _HostFileProof:
    directory: _HostDirectoryProof
    name: str
    path: Path
    descriptor: int
    metadata: os.stat_result
    label: str
    maximum_size: int
    exact_mode: int


def _revalidate_host_file_proof(proof: _HostFileProof) -> None:
    _revalidate_host_directory_proof(proof.directory)
    opened = os.fstat(proof.descriptor)
    if proof.directory.descriptor is None:
        named = proof.path.lstat()
    else:
        named = os.stat(
            proof.name,
            dir_fd=proof.directory.descriptor,
            follow_symlinks=False,
        )
    for metadata in (opened, named):
        _validate_host_file_metadata(
            metadata,
            proof.directory.context.effective_uid,
            proof.label,
            maximum_size=proof.maximum_size,
            exact_mode=proof.exact_mode,
        )
    if (
        _stable_file_metadata(opened) != _stable_file_metadata(proof.metadata)
        or not _same_identity(opened, named)
    ):
        raise OperationStateError(f"{proof.label} changed during staging")


@contextlib.contextmanager
def _open_host_file_from_directory(
    directory: _HostDirectoryProof,
    name: str,
    label: str,
    *,
    maximum_size: int,
    exact_mode: int = 0o600,
):
    _validate_source_basename(name)
    path = directory.path / name
    descriptor: int | None = None
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        named = (
            path.lstat()
            if directory.descriptor is None
            else os.stat(name, dir_fd=directory.descriptor, follow_symlinks=False)
        )
        _validate_host_file_metadata(
            named,
            directory.context.effective_uid,
            label,
            maximum_size=maximum_size,
            exact_mode=exact_mode,
        )
        descriptor = (
            os.open(path, flags)
            if directory.descriptor is None
            else os.open(name, flags, dir_fd=directory.descriptor)
        )
        opened = os.fstat(descriptor)
        if not _same_identity(named, opened):
            raise OperationStateError(f"{label} changed while opening")
        proof = _HostFileProof(
            directory,
            name,
            path,
            descriptor,
            opened,
            label,
            maximum_size,
            exact_mode,
        )
        _revalidate_host_file_proof(proof)
        yield proof
        _revalidate_host_file_proof(proof)
    except OSError as exc:
        raise OperationStateError(f"{label} access failed") from exc
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass


@contextlib.contextmanager
def _open_host_file(
    context: OperationsContext,
    logical_path: str,
    label: str,
    *,
    maximum_size: int,
    exact_mode: int = 0o600,
):
    pure = PurePosixPath(logical_path)
    parent = str(pure.parent)
    with _open_host_directory(context, parent) as directory:
        with _open_host_file_from_directory(
            directory,
            pure.name,
            label,
            maximum_size=maximum_size,
            exact_mode=exact_mode,
        ) as proof:
            yield proof


def _read_host_file(proof: _HostFileProof, maximum_size: int) -> bytes:
    chunks: list[bytes] = []
    total = 0
    try:
        os.lseek(proof.descriptor, 0, os.SEEK_SET)
        while True:
            chunk = os.read(proof.descriptor, min(64 * 1024, maximum_size + 1 - total))
            if not chunk:
                break
            total += len(chunk)
            if total > maximum_size:
                raise OperationStateError(f"{proof.label} exceeds the size limit")
            chunks.append(chunk)
    except OSError as exc:
        raise OperationStateError(f"{proof.label} read failed") from exc
    _revalidate_host_file_proof(proof)
    return b"".join(chunks)


def _hash_host_file(proof: _HostFileProof) -> str:
    digest = hashlib.sha256()
    total = 0
    try:
        os.lseek(proof.descriptor, 0, os.SEEK_SET)
        while True:
            chunk = os.read(proof.descriptor, 1024 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if total > proof.maximum_size:
                raise OperationStateError(f"{proof.label} exceeds the size limit")
            digest.update(chunk)
    except OSError as exc:
        raise OperationStateError(f"{proof.label} hash failed") from exc
    _revalidate_host_file_proof(proof)
    return digest.hexdigest()


@dataclass
class _SnapshotTargetProof:
    directory: _HostDirectoryProof
    logical_path: str
    name: str
    path: Path
    descriptor: int | None
    metadata: os.stat_result | None
    contents: bytes | None


def _validate_snapshot_source_metadata(
    metadata: os.stat_result,
    context: OperationsContext,
    label: str,
) -> None:
    if not stat.S_ISREG(metadata.st_mode):
        raise OperationStateError(f"{label} is not a regular file")
    if metadata.st_uid != context.effective_uid:
        raise OperationStateError(f"{label} is not owned by the effective UID")
    if metadata.st_nlink != 1:
        raise OperationStateError(f"{label} must have a single link")
    mode = stat.S_IMODE(metadata.st_mode)
    if os.name == "posix" and mode & 0o7022:
        raise OperationStateError(f"{label} has an unsafe mode")
    if metadata.st_size < 0 or metadata.st_size > _MAX_SNAPSHOT_FILE_BYTES:
        raise OperationStateError(f"{label} size is invalid")


def _snapshot_named_metadata(proof: _SnapshotTargetProof) -> os.stat_result:
    if proof.directory.descriptor is None:
        return proof.path.lstat()
    return os.stat(
        proof.name,
        dir_fd=proof.directory.descriptor,
        follow_symlinks=False,
    )


def _revalidate_snapshot_target_proof(proof: _SnapshotTargetProof) -> None:
    _revalidate_host_directory_proof(proof.directory)
    if proof.descriptor is None:
        try:
            _snapshot_named_metadata(proof)
        except FileNotFoundError:
            return
        except OSError as exc:
            raise OperationStateError("snapshot target absence cannot be revalidated") from exc
        raise OperationStateError("snapshot target appeared after absence was recorded")
    assert proof.metadata is not None and proof.contents is not None
    try:
        opened_before = os.fstat(proof.descriptor)
        named_before = _snapshot_named_metadata(proof)
        for metadata in (opened_before, named_before):
            _validate_snapshot_source_metadata(
                metadata,
                proof.directory.context,
                "snapshot target",
            )
        if (
            _stage_stable_metadata(opened_before)
            != _stage_stable_metadata(proof.metadata)
            or not _same_identity(opened_before, named_before)
        ):
            raise OperationStateError("snapshot target path changed after capture")
        os.lseek(proof.descriptor, 0, os.SEEK_SET)
        raw = _read_exact_descriptor(
            proof.descriptor,
            opened_before.st_size,
            "snapshot target",
        )
        opened_after = os.fstat(proof.descriptor)
        named_after = _snapshot_named_metadata(proof)
    except OSError as exc:
        raise OperationStateError("snapshot target cannot be revalidated") from exc
    if (
        raw != proof.contents
        or _stage_stable_metadata(opened_after)
        != _stage_stable_metadata(opened_before)
        or not _same_identity(opened_after, named_after)
    ):
        raise OperationStateError("snapshot target changed after capture")
    _revalidate_host_directory_proof(proof.directory)


def _capture_snapshot_target(
    directory: _HostDirectoryProof,
    logical_path: str,
) -> _SnapshotTargetProof:
    pure = PurePosixPath(logical_path)
    if str(pure.parent) != directory.logical_path:
        raise OperationStateError("snapshot target parent proof is mismatched")
    name = pure.name
    _validate_source_basename(name)
    path = directory.path / name
    descriptor: int | None = None
    try:
        try:
            named = (
                path.lstat()
                if directory.descriptor is None
                else os.stat(name, dir_fd=directory.descriptor, follow_symlinks=False)
            )
        except FileNotFoundError:
            proof = _SnapshotTargetProof(
                directory,
                logical_path,
                name,
                path,
                None,
                None,
                None,
            )
            _revalidate_snapshot_target_proof(proof)
            return proof
        _validate_snapshot_source_metadata(named, directory.context, "snapshot target")
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
        flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = (
            os.open(path, flags)
            if directory.descriptor is None
            else os.open(name, flags, dir_fd=directory.descriptor)
        )
        opened = os.fstat(descriptor)
        _validate_snapshot_source_metadata(opened, directory.context, "snapshot target")
        if not _same_identity(named, opened):
            raise OperationStateError("snapshot target changed while opening")
        _atomic_event_hook(
            "snapshot_target_opened",
            logical_path=logical_path,
            descriptor=descriptor,
        )
        contents = _read_exact_descriptor(
            descriptor,
            opened.st_size,
            "snapshot target",
        )
        proof = _SnapshotTargetProof(
            directory,
            logical_path,
            name,
            path,
            descriptor,
            opened,
            contents,
        )
        descriptor = None
        try:
            _revalidate_snapshot_target_proof(proof)
        except BaseException:
            _close_snapshot_target_proof(proof)
            raise
        return proof
    except OSError as exc:
        raise OperationStateError("snapshot target access failed") from exc
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass


def _close_snapshot_target_proof(proof: _SnapshotTargetProof) -> None:
    if proof.descriptor is not None:
        try:
            os.close(proof.descriptor)
        except OSError:
            pass
        proof.descriptor = None


@dataclass
class _SnapshotDirectoryProof:
    context: OperationsContext
    operation_directory_fd: int | None
    descriptor: int | None
    metadata: os.stat_result
    expected_bytes: dict[str, bytes]
    file_descriptors: dict[str, int]
    file_metadata: dict[str, os.stat_result]


def _validate_snapshot_directory_metadata(
    metadata: os.stat_result,
    context: OperationsContext,
) -> None:
    _validate_host_directory_metadata(metadata, context.effective_uid, "snapshot directory")
    if os.name == "posix" and stat.S_IMODE(metadata.st_mode) != 0o700:
        raise OperationStateError("snapshot directory must have mode 0700")


def _snapshot_file_metadata_is_safe(
    metadata: os.stat_result,
    context: OperationsContext,
) -> bool:
    return (
        stat.S_ISREG(metadata.st_mode)
        and metadata.st_uid == context.effective_uid
        and metadata.st_nlink == 1
        and (os.name != "posix" or stat.S_IMODE(metadata.st_mode) == 0o600)
        and 0 <= metadata.st_size <= _MAX_SNAPSHOT_FILE_BYTES
    )


def _snapshot_named_directory_metadata(proof: _SnapshotDirectoryProof) -> os.stat_result:
    if proof.operation_directory_fd is None:
        return proof.context.paths.snapshot_dir.lstat()
    return os.stat(
        "snapshot",
        dir_fd=proof.operation_directory_fd,
        follow_symlinks=False,
    )


def _snapshot_named_file_metadata(
    proof: _SnapshotDirectoryProof,
    name: str,
) -> os.stat_result:
    if proof.descriptor is None:
        return (proof.context.paths.snapshot_dir / name).lstat()
    return os.stat(name, dir_fd=proof.descriptor, follow_symlinks=False)


def _snapshot_inventory(
    proof: _SnapshotDirectoryProof,
) -> dict[str, os.stat_result]:
    try:
        iterator = os.scandir(
            proof.descriptor
            if proof.descriptor is not None
            else proof.context.paths.snapshot_dir
        )
    except OSError as exc:
        raise OperationStateError("snapshot inventory cannot be read") from exc
    files: dict[str, os.stat_result] = {}
    with iterator:
        for entry in iterator:
            if len(files) >= _MAX_SNAPSHOT_ARTIFACTS:
                raise OperationStateError("snapshot inventory exceeds its entry bound")
            _validate_source_basename(entry.name)
            metadata = _snapshot_named_file_metadata(proof, entry.name)
            if not _snapshot_file_metadata_is_safe(metadata, proof.context):
                raise OperationStateError("snapshot inventory contains an unsafe artifact")
            files[entry.name] = metadata
    return files


def _canonical_snapshot_manifest(artifacts: dict[str, bytes]) -> bytes:
    if len(artifacts) != 8 or _SNAPSHOT_MANIFEST_NAME in artifacts:
        raise OperationStateError("snapshot manifest requires exactly eight artifacts")
    names = sorted(artifacts)
    for name in names:
        _validate_source_basename(name)
    return b"".join(
        hashlib.sha256(artifacts[name]).hexdigest().encode("ascii")
        + b"  "
        + name.encode("ascii")
        + b"\n"
        for name in names
    )


def _parse_snapshot_manifest(
    raw: bytes,
    artifacts: dict[str, bytes],
) -> None:
    expected = _canonical_snapshot_manifest(artifacts)
    if raw != expected:
        raise OperationStateError("snapshot SHA256SUMS is not exact and canonical")
    try:
        lines = raw.decode("ascii", errors="strict").splitlines()
    except UnicodeDecodeError as exc:
        raise OperationStateError("snapshot SHA256SUMS is not ASCII") from exc
    names: list[str] = []
    for line in lines:
        if len(line) < 67 or line[64:66] != "  ":
            raise OperationStateError("snapshot SHA256SUMS record is malformed")
        digest = line[:64]
        name = line[66:]
        if _SHA256_RE.fullmatch(digest) is None:
            raise OperationStateError("snapshot SHA256SUMS digest is malformed")
        _validate_source_basename(name)
        names.append(name)
        if digest != hashlib.sha256(artifacts[name]).hexdigest():
            raise OperationStateError("snapshot SHA256SUMS digest does not match artifact")
    if names != sorted(artifacts) or len(names) != 8:
        raise OperationStateError("snapshot SHA256SUMS inventory is incomplete")


def _revalidate_snapshot_directory_proof(proof: _SnapshotDirectoryProof) -> None:
    named_directory = _snapshot_named_directory_metadata(proof)
    opened_directory = (
        os.fstat(proof.descriptor)
        if proof.descriptor is not None
        else named_directory
    )
    for metadata in (named_directory, opened_directory):
        _validate_snapshot_directory_metadata(metadata, proof.context)
    if (
        _stage_stable_metadata(opened_directory)
        != _stage_stable_metadata(proof.metadata)
        or not _same_identity(opened_directory, named_directory)
    ):
        raise OperationStateError("snapshot directory binding changed")
    current_files = _snapshot_inventory(proof)
    if set(current_files) != set(proof.expected_bytes):
        raise OperationStateError("snapshot artifact inventory changed")
    for name, expected in proof.expected_bytes.items():
        descriptor = proof.file_descriptors[name]
        initial = proof.file_metadata[name]
        opened_before = os.fstat(descriptor)
        named_before = current_files[name]
        if (
            not _snapshot_file_metadata_is_safe(opened_before, proof.context)
            or _stage_stable_metadata(opened_before)
            != _stage_stable_metadata(initial)
            or not _same_identity(opened_before, named_before)
        ):
            raise OperationStateError("snapshot artifact binding changed")
        try:
            os.lseek(descriptor, 0, os.SEEK_SET)
            raw = _read_exact_descriptor(
                descriptor,
                opened_before.st_size,
                "snapshot artifact",
            )
            opened_after = os.fstat(descriptor)
            named_after = _snapshot_named_file_metadata(proof, name)
        except OSError as exc:
            raise OperationStateError("snapshot artifact cannot be revalidated") from exc
        if (
            raw != expected
            or _stage_stable_metadata(opened_after)
            != _stage_stable_metadata(opened_before)
            or not _same_identity(opened_after, named_after)
        ):
            raise OperationStateError("snapshot artifact bytes changed")
    artifacts = {
        name: raw
        for name, raw in proof.expected_bytes.items()
        if name != _SNAPSHOT_MANIFEST_NAME
    }
    _parse_snapshot_manifest(
        proof.expected_bytes[_SNAPSHOT_MANIFEST_NAME],
        artifacts,
    )


def _open_snapshot_artifact_descriptor(
    proof: _SnapshotDirectoryProof,
    name: str,
    *,
    create: bool,
) -> int:
    _validate_source_basename(name)
    flags = (os.O_RDWR if create else os.O_RDONLY) | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    if create:
        flags |= os.O_CREAT | os.O_EXCL
    path = proof.context.paths.snapshot_dir / name
    return (
        os.open(name, flags, 0o600, dir_fd=proof.descriptor)
        if proof.descriptor is not None
        else os.open(path, flags, 0o600)
    )


def _open_snapshot_directory_proof(
    context: OperationsContext,
    operation_directory_fd: int | None,
    expected_bytes: dict[str, bytes],
    *,
    create: bool,
) -> _SnapshotDirectoryProof:
    if len(expected_bytes) != 9 or _SNAPSHOT_MANIFEST_NAME not in expected_bytes:
        raise OperationStateError("snapshot expected artifact inventory is invalid")
    descriptor: int | None = None
    file_descriptors: dict[str, int] = {}
    file_metadata: dict[str, os.stat_result] = {}
    use_descriptors = os.name == "posix" and operation_directory_fd is not None
    try:
        if create:
            try:
                (
                    os.stat(
                        "snapshot",
                        dir_fd=operation_directory_fd,
                        follow_symlinks=False,
                    )
                    if use_descriptors
                    else context.paths.snapshot_dir.lstat()
                )
            except FileNotFoundError:
                pass
            else:
                raise OperationStateError("preexisting snapshot path is forbidden")
            if use_descriptors:
                os.mkdir("snapshot", 0o700, dir_fd=operation_directory_fd)
            else:
                context.paths.snapshot_dir.mkdir(mode=0o700)
                if os.name == "posix":
                    context.paths.snapshot_dir.chmod(0o700)
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        if use_descriptors:
            descriptor = os.open("snapshot", flags, dir_fd=operation_directory_fd)
            if create:
                os.fchmod(descriptor, 0o700)
            metadata = os.fstat(descriptor)
            named = os.stat(
                "snapshot",
                dir_fd=operation_directory_fd,
                follow_symlinks=False,
            )
            if not _same_identity(metadata, named):
                raise OperationStateError("snapshot directory changed while opening")
        else:
            metadata = context.paths.snapshot_dir.lstat()
        _validate_snapshot_directory_metadata(metadata, context)
        proof = _SnapshotDirectoryProof(
            context,
            operation_directory_fd,
            descriptor,
            metadata,
            dict(expected_bytes),
            file_descriptors,
            file_metadata,
        )
        if not create:
            if set(_snapshot_inventory(proof)) != set(expected_bytes):
                raise OperationStateError("preexisting snapshot inventory is not exact")
        write_order = (
            [
                _RCLONE_AUDIT_NAME,
                *sorted(
                    set(expected_bytes)
                    - {_RCLONE_AUDIT_NAME, _SNAPSHOT_MANIFEST_NAME}
                ),
                _SNAPSHOT_MANIFEST_NAME,
            ]
            if create
            else sorted(expected_bytes)
        )
        for name in write_order:
            artifact_fd = _open_snapshot_artifact_descriptor(
                proof,
                name,
                create=create,
            )
            file_descriptors[name] = artifact_fd
            if create:
                if hasattr(os, "fchmod"):
                    os.fchmod(artifact_fd, 0o600)
                elif os.name == "posix":
                    raise OperationStateError("snapshot artifact chmod primitive is unavailable")
                else:
                    (context.paths.snapshot_dir / name).chmod(0o600)
                _write_all(artifact_fd, expected_bytes[name])
                os.fsync(artifact_fd)
            opened = os.fstat(artifact_fd)
            named = _snapshot_named_file_metadata(proof, name)
            if (
                not _snapshot_file_metadata_is_safe(opened, context)
                or not _same_identity(opened, named)
                or opened.st_size != len(expected_bytes[name])
            ):
                raise OperationStateError("snapshot artifact metadata is invalid")
            file_metadata[name] = opened
            if create:
                _atomic_event_hook("snapshot_artifact_written", name=name)
        if create:
            refreshed = (
                os.fstat(descriptor)
                if descriptor is not None
                else context.paths.snapshot_dir.lstat()
            )
            named_refreshed = _snapshot_named_directory_metadata(proof)
            if not _same_identity(refreshed, named_refreshed):
                raise OperationStateError("snapshot directory changed during artifact writes")
            _validate_snapshot_directory_metadata(refreshed, context)
            proof.metadata = refreshed
        _revalidate_snapshot_directory_proof(proof)
        return proof
    except BaseException as exc:
        for artifact_fd in file_descriptors.values():
            try:
                os.close(artifact_fd)
            except OSError:
                pass
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass
        if isinstance(exc, OperationStateError):
            raise
        raise OperationStateError("snapshot directory cannot be opened safely") from exc


def _fsync_snapshot_directory_proof(proof: _SnapshotDirectoryProof) -> None:
    _revalidate_snapshot_directory_proof(proof)
    for descriptor in proof.file_descriptors.values():
        os.fsync(descriptor)
    if proof.descriptor is not None:
        os.fsync(proof.descriptor)
    if proof.operation_directory_fd is not None:
        os.fsync(proof.operation_directory_fd)
    elif os.name == "posix":
        _fsync_parent_directory(proof.context.paths.operation_dir)
    _revalidate_snapshot_directory_proof(proof)


def _close_snapshot_directory_proof(proof: _SnapshotDirectoryProof) -> None:
    for descriptor in proof.file_descriptors.values():
        try:
            os.close(descriptor)
        except OSError:
            pass
    proof.file_descriptors.clear()
    if proof.descriptor is not None:
        try:
            os.close(proof.descriptor)
        except OSError:
            pass
        proof.descriptor = None


def _scrub_completed_process(completed: subprocess.CompletedProcess[str]) -> None:
    completed.args = ("[REDACTED]",)
    completed.stdout = "[REDACTED]"
    completed.stderr = "[REDACTED]"


def _checked_command(
    context: OperationsContext,
    argv: tuple[str, ...],
    pass_fds: tuple[int, ...],
    label: str,
    *,
    forbidden_values: tuple[str, ...] = (),
) -> subprocess.CompletedProcess[str]:
    try:
        completed = context.command_runner(argv, pass_fds)
    except Exception:
        raise OperationStateError(f"{label} failed") from None
    if type(completed) is not subprocess.CompletedProcess:
        raise OperationStateError(f"{label} returned an invalid result")
    fields = (repr(completed.args), completed.stdout, completed.stderr)
    invalid_types = type(completed.stdout) is not str or type(completed.stderr) is not str
    if invalid_types:
        _scrub_completed_process(completed)
        raise OperationStateError(f"{label} returned invalid output")
    assert isinstance(completed.stdout, str) and isinstance(completed.stderr, str)
    combined = "\n".join(str(value) for value in fields)
    leaked = any(value and value in combined for value in forbidden_values)
    leaked = leaked or _string_contains_secret(completed.stdout) or _string_contains_secret(
        completed.stderr
    )
    if leaked:
        _scrub_completed_process(completed)
        raise OperationStateError(f"{label} returned unsafe output")
    if tuple(str(value) for value in completed.args) != argv:
        _scrub_completed_process(completed)
        raise OperationStateError(f"{label} returned mismatched command evidence")
    if (
        len(completed.stdout.encode("utf-8")) > _MAX_COMMAND_OUTPUT_BYTES
        or len(completed.stderr.encode("utf-8")) > _MAX_COMMAND_OUTPUT_BYTES
    ):
        _scrub_completed_process(completed)
        raise OperationStateError(f"{label} output exceeds the size limit")
    if type(completed.returncode) is not int or completed.returncode != 0:
        completed.stdout = ""
        completed.stderr = ""
        raise OperationStateError(f"{label} failed")
    return completed


def _readonly_command_output(
    context: OperationsContext,
    argv: tuple[str, ...],
    label: str,
) -> str:
    completed = _checked_command(context, argv, (), label)
    assert isinstance(completed.stdout, str) and isinstance(completed.stderr, str)
    output = completed.stdout
    if completed.stderr:
        _scrub_completed_process(completed)
        raise OperationStateError(f"{label} returned unexpected stderr")
    _scrub_completed_process(completed)
    return output


def _parse_systemctl_show(
    raw: str,
    expected_keys: tuple[str, ...],
    label: str,
    *,
    allow_empty: frozenset[str] = frozenset(),
) -> dict[str, str]:
    if not raw.endswith("\n") or "\r" in raw:
        raise OperationStateError(f"{label} output is not canonical")
    values: dict[str, str] = {}
    for line in raw.splitlines():
        if line.count("=") != 1:
            raise OperationStateError(f"{label} output is malformed")
        key, value = line.split("=", 1)
        if (
            key in values
            or key not in expected_keys
            or (not value and key not in allow_empty)
        ):
            raise OperationStateError(f"{label} output has ambiguous properties")
        values[key] = value
    if tuple(values) != expected_keys and set(values) != set(expected_keys):
        raise OperationStateError(f"{label} output is incomplete")
    return values


def _systemctl_show_unit(
    context: OperationsContext,
    unit: str,
    *,
    user_machine: str | None = None,
) -> dict[str, str]:
    if _SYSTEMD_UNIT_RE.fullmatch(unit) is None:
        raise OperationStateError("systemd service unit is unsafe")
    properties = ("LoadState", "ActiveState", "SubState", "MainPID")
    prefix: tuple[str, ...] = (_SYSTEMCTL_EXECUTABLE,)
    if user_machine is not None:
        if _LOGIN_USER_RE.fullmatch(user_machine) is None:
            raise OperationStateError("systemd user owner is unsafe")
        prefix += ("--user", f"--machine={user_machine}@.host")
    argv = (
        *prefix,
        "show",
        unit,
        *(f"--property={value}" for value in properties),
        "--no-pager",
    )
    raw = _readonly_command_output(context, argv, "systemd unit query")
    return _parse_systemctl_show(raw, properties, "systemd unit query")


def _parse_positive_main_pid(
    values: dict[str, str],
    label: str,
    *,
    require_active: bool,
) -> int:
    try:
        pid = int(values["MainPID"], 10)
    except (KeyError, ValueError):
        raise OperationStateError(f"{label} MainPID is invalid") from None
    if str(pid) != values["MainPID"] or pid < 0:
        raise OperationStateError(f"{label} MainPID is invalid")
    if require_active:
        if (
            values.get("LoadState") != "loaded"
            or values.get("ActiveState") != "active"
            or values.get("SubState") != "running"
            or pid < 1
        ):
            raise OperationStateError(f"{label} is not one exact active process")
    elif (
        values.get("LoadState") != "loaded"
        or values.get("ActiveState") != "inactive"
        or values.get("SubState") != "dead"
        or pid != 0
    ):
        raise OperationStateError(f"{label} must be loaded, inactive, dead, and pidless")
    return pid


def _capture_prior_runtime(context: OperationsContext) -> dict[str, object]:
    timer_properties = (
        "UnitFileState",
        "ActiveState",
        "SubState",
        "LastTriggerUSec",
    )
    timer_raw = _readonly_command_output(
        context,
        (
            _SYSTEMCTL_EXECUTABLE,
            "show",
            "degen-prod-db-backup.timer",
            *(f"--property={value}" for value in timer_properties),
            "--no-pager",
        ),
        "backup timer query",
    )
    timer = _parse_systemctl_show(
        timer_raw,
        timer_properties,
        "backup timer query",
        allow_empty=frozenset({"LastTriggerUSec"}),
    )
    if timer["UnitFileState"] not in {"enabled", "disabled"}:
        raise OperationStateError("backup timer enablement state is ambiguous")
    if timer["ActiveState"] not in {"active", "inactive"}:
        raise OperationStateError("backup timer active state is ambiguous")
    expected_substate = "waiting" if timer["ActiveState"] == "active" else "dead"
    if timer["SubState"] != expected_substate:
        raise OperationStateError("backup timer substate is ambiguous")
    trigger_raw = timer["LastTriggerUSec"]
    trigger_epoch: int | None
    if trigger_raw in {"", "n/a", "never"}:
        trigger_epoch = None
    else:
        converted = _readonly_command_output(
            context,
            (_DATE_EXECUTABLE, "--date", trigger_raw, "+%s"),
            "backup timer trigger conversion",
        )
        if not converted.endswith("\n") or not converted[:-1].isdigit():
            raise OperationStateError("backup timer trigger epoch is invalid")
        trigger_epoch = int(converted[:-1], 10)

    postgres_raw = _readonly_command_output(
        context,
        (
            _SYSTEMCTL_EXECUTABLE,
            "list-units",
            "--type=service",
            "--state=running",
            "--no-legend",
            "--no-pager",
            "--plain",
            "postgresql*.service",
        ),
        "PostgreSQL unit discovery",
    )
    postgres_units: list[str] = []
    for line in postgres_raw.splitlines():
        fields = line.split()
        if (
            len(fields) < 4
            or _SYSTEMD_UNIT_RE.fullmatch(fields[0]) is None
            or not fields[0].startswith("postgresql")
            or fields[1:4] != ["loaded", "active", "running"]
        ):
            raise OperationStateError("PostgreSQL unit discovery output is ambiguous")
        postgres_units.append(fields[0])
    if len(postgres_units) != 1 or len(set(postgres_units)) != 1:
        raise OperationStateError("exactly one active PostgreSQL service is required")
    postgres_unit = postgres_units[0]
    postgres_pid = _parse_positive_main_pid(
        _systemctl_show_unit(context, postgres_unit),
        "PostgreSQL service",
        require_active=True,
    )
    web_pid = _parse_positive_main_pid(
        _systemctl_show_unit(context, "degen-web.service"),
        "web service",
        require_active=True,
    )
    worker_pid = _parse_positive_main_pid(
        _systemctl_show_unit(context, "degen-worker.service"),
        "worker service",
        require_active=True,
    )
    _parse_positive_main_pid(
        _systemctl_show_unit(context, "degen-prod-db-backup.service"),
        "backup service",
        require_active=False,
    )

    users_raw = _readonly_command_output(
        context,
        (_LOGINCTL_EXECUTABLE, "list-users", "--no-legend", "--no-pager"),
        "login user discovery",
    )
    users: list[tuple[str, str]] = []
    seen_uids: set[str] = set()
    seen_names: set[str] = set()
    for line in users_raw.splitlines():
        fields = line.split()
        if (
            len(fields) < 2
            or not fields[0].isdigit()
            or len(fields[0]) > 10
            or str(int(fields[0])) != fields[0]
            or int(fields[0]) < 0
            or _LOGIN_USER_RE.fullmatch(fields[1]) is None
            or fields[0] in seen_uids
            or fields[1] in seen_names
        ):
            raise OperationStateError("login user discovery output is ambiguous")
        seen_uids.add(fields[0])
        seen_names.add(fields[1])
        users.append((fields[0], fields[1]))
    bot_candidates: list[tuple[str, str, int]] = []
    for uid, username in users:
        values = _systemctl_show_unit(
            context,
            "degen-ops-discord-bot.service",
            user_machine=username,
        )
        if (
            values["LoadState"] == "not-found"
            and values["ActiveState"] == "inactive"
            and values["SubState"] == "dead"
            and values["MainPID"] == "0"
        ):
            continue
        bot_pid = _parse_positive_main_pid(
            values,
            "Discord bot service",
            require_active=True,
        )
        bot_candidates.append((uid, username, bot_pid))
    if len(bot_candidates) != 1:
        raise OperationStateError("Discord bot owning user or unit is ambiguous")
    bot_uid, bot_user, bot_pid = bot_candidates[0]
    pids = {
        f"postgresql:{postgres_unit}": postgres_pid,
        "system:degen-web.service": web_pid,
        "system:degen-worker.service": worker_pid,
        f"user:{bot_uid}:{bot_user}:degen-ops-discord-bot.service": bot_pid,
    }
    if len(set(pids.values())) != len(pids):
        raise OperationStateError("protected runtime PIDs are not unique")
    return {
        "timer_enabled": timer["UnitFileState"] == "enabled",
        "timer_active": timer["ActiveState"] == "active",
        "pids": pids,
        "preinstall_trigger_epoch": trigger_epoch,
    }


@dataclass
class _BackupPairProof:
    directory: _HostDirectoryProof
    dump: _HostFileProof
    sidecar: _HostFileProof
    dump_basename: str
    sidecar_basename: str
    timestamp: str
    prefix: str
    dump_sha256: str
    sidecar_bytes: bytes


def _revalidate_backup_pair(proof: _BackupPairProof, *, rehash: bool = True) -> None:
    inventory = _inventory_backup_directory(proof.directory)
    selected = _select_newest_complete_pair(inventory)
    if selected != (
        proof.dump_basename,
        proof.sidecar_basename,
        proof.timestamp,
        proof.prefix,
    ):
        raise OperationStateError("selected backup pair is no longer the unique newest pair")
    _revalidate_host_file_proof(proof.dump)
    _revalidate_host_file_proof(proof.sidecar)
    sidecar = _read_host_file(proof.sidecar, 256)
    if sidecar != proof.sidecar_bytes:
        raise OperationStateError("backup sidecar changed during staging")
    if rehash and _hash_host_file(proof.dump) != proof.dump_sha256:
        raise OperationStateError("backup dump changed during staging")
    _revalidate_host_file_proof(proof.dump)
    _revalidate_host_file_proof(proof.sidecar)
    final_inventory = _inventory_backup_directory(proof.directory)
    if _select_newest_complete_pair(final_inventory) != selected:
        raise OperationStateError("backup pair inventory changed during staging")
    if (
        not _same_identity(final_inventory[proof.dump_basename], proof.dump.metadata)
        or not _same_identity(
            final_inventory[proof.sidecar_basename], proof.sidecar.metadata
        )
    ):
        raise OperationStateError("selected backup pair path binding changed")


def _inventory_backup_directory(directory: _HostDirectoryProof) -> dict[str, os.stat_result]:
    scan_target: int | Path = (
        directory.descriptor if directory.descriptor is not None else directory.path
    )
    try:
        iterator = os.scandir(scan_target)
    except OSError as exc:
        raise OperationStateError("backup directory listing failed") from exc
    inventory: dict[str, os.stat_result] = {}
    folded: set[str] = set()
    try:
        with iterator:
            for entry in iterator:
                if len(inventory) >= _MAX_BACKUP_ENTRIES:
                    raise OperationStateError("backup directory exceeds the entry bound")
                name = entry.name
                _validate_source_basename(name)
                folded_name = name.casefold()
                if folded_name in folded:
                    raise OperationStateError("backup directory contains casefold-colliding names")
                folded.add(folded_name)
                if _BACKUP_NAME_RE.fullmatch(name) is None:
                    raise OperationStateError("backup directory contains an unsafe or ambiguous name")
                metadata = (
                    (directory.path / name).lstat()
                    if directory.descriptor is None
                    else os.stat(name, dir_fd=directory.descriptor, follow_symlinks=False)
                )
                maximum = 256 if name.endswith(".sha256") else _MAX_BACKUP_DUMP_BYTES
                _validate_host_file_metadata(
                    metadata,
                    directory.context.effective_uid,
                    "backup pair file",
                    maximum_size=maximum,
                )
                inventory[name] = metadata
    except OSError as exc:
        raise OperationStateError("backup directory enumeration failed") from exc
    _revalidate_host_directory_proof(directory)
    return inventory


def _select_newest_complete_pair(
    inventory: dict[str, os.stat_result],
) -> tuple[str, str, str, str]:
    groups: dict[str, set[str]] = {}
    parsed: dict[str, tuple[str, str]] = {}
    for name in inventory:
        match = _BACKUP_NAME_RE.fullmatch(name)
        assert match is not None
        dump_name = name[:-7] if name.endswith(".sha256") else name
        groups.setdefault(dump_name, set()).add(name)
        parsed[dump_name] = (match.group("stamp"), match.group("prefix"))
    if not groups:
        raise OperationStateError("no verified local backup pair exists")
    for dump_name, names in groups.items():
        expected = {dump_name, dump_name + ".sha256"}
        if names != expected:
            raise OperationStateError("backup directory contains an incomplete pair")
    ranked: list[tuple[datetime, str, str, str]] = []
    for dump_name, (stamp, prefix) in parsed.items():
        try:
            parsed_stamp = datetime.strptime(stamp, "%Y%m%dT%H%M%SZ")
        except ValueError as exc:
            raise OperationStateError("backup filename timestamp is invalid") from exc
        ranked.append((parsed_stamp, dump_name, stamp, prefix))
    newest_time = max(item[0] for item in ranked)
    newest = [item for item in ranked if item[0] == newest_time]
    if len(newest) != 1:
        raise OperationStateError("newest backup pair timestamp is ambiguous")
    _, dump_name, stamp, prefix = newest[0]
    return dump_name, dump_name + ".sha256", stamp, prefix


@contextlib.contextmanager
def _open_verified_backup_pair(
    context: OperationsContext,
    backup_dir: str,
):
    with _open_host_directory(context, backup_dir) as directory:
        inventory = _inventory_backup_directory(directory)
        dump_name, sidecar_name, timestamp, prefix = _select_newest_complete_pair(
            inventory
        )
        with _open_host_file_from_directory(
            directory,
            dump_name,
            "backup dump",
            maximum_size=_MAX_BACKUP_DUMP_BYTES,
        ) as dump:
            with _open_host_file_from_directory(
                directory,
                sidecar_name,
                "backup sidecar",
                maximum_size=256,
            ) as sidecar:
                sidecar_bytes = _read_host_file(sidecar, 256)
                expected_sidecar_prefix = b"  " + dump_name.encode("ascii") + b"\n"
                if (
                    len(sidecar_bytes) != 64 + len(expected_sidecar_prefix)
                    or sidecar_bytes[64:] != expected_sidecar_prefix
                ):
                    raise OperationStateError("backup sidecar record grammar is invalid")
                try:
                    recorded_sha256 = sidecar_bytes[:64].decode("ascii")
                except UnicodeDecodeError as exc:
                    raise OperationStateError("backup sidecar record is not ASCII") from exc
                if _SHA256_RE.fullmatch(recorded_sha256) is None:
                    raise OperationStateError("backup sidecar digest is invalid")
                dump_sha256 = _hash_host_file(dump)
                if dump_sha256 != recorded_sha256:
                    raise OperationStateError("backup dump SHA-256 does not match its sidecar")
                _revalidate_host_file_proof(dump)
                _revalidate_host_file_proof(sidecar)
                completed = _checked_command(
                    context,
                    (_PG_RESTORE_EXECUTABLE, "--list", str(dump.path)),
                    (),
                    "PostgreSQL archive verification",
                )
                if completed.stderr:
                    raise OperationStateError("PostgreSQL archive verification returned stderr")
                proof = _BackupPairProof(
                    directory,
                    dump,
                    sidecar,
                    dump_name,
                    sidecar_name,
                    timestamp,
                    prefix,
                    dump_sha256,
                    sidecar_bytes,
                )
                _revalidate_backup_pair(proof)
                yield proof
                _revalidate_backup_pair(proof)


def _parse_app_environment(raw: bytes) -> str:
    if not raw or len(raw) > _MAX_APP_ENV_BYTES:
        raise OperationStateError("application environment size is invalid")
    if b"\0" in raw or b"\r" in raw:
        raise OperationStateError("application environment contains unsafe bytes")
    try:
        text = raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise OperationStateError("application environment is not strict UTF-8") from exc
    values: dict[str, str] = {}
    for line in text.split("\n"):
        if not line or line.startswith("#"):
            continue
        match = _APP_ENV_ASSIGNMENT_RE.fullmatch(line)
        if match is None:
            raise OperationStateError("application environment syntax is unsupported")
        key = match.group("key")
        value = match.group("value")
        if key in values:
            raise OperationStateError("application environment contains a duplicate key")
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        elif value.startswith(("'", '"')) or value.endswith(("'", '"')):
            raise OperationStateError("application environment quoting is unsafe")
        if not value or any(ord(character) < 0x21 or ord(character) > 0x7E for character in value):
            raise OperationStateError("application environment value is unsafe")
        values[key] = value
    if "DATABASE_URL" not in values:
        raise OperationStateError("DATABASE_URL is missing from the application environment")
    database_url = values["DATABASE_URL"]
    if database_url.startswith("postgresql+psycopg://"):
        database_url = "postgresql://" + database_url[len("postgresql+psycopg://") :]
    elif not database_url.startswith(("postgresql://", "postgres://")):
        raise OperationStateError("DATABASE_URL uses an unsupported PostgreSQL URI scheme")
    encoded = database_url.encode("utf-8")
    if not encoded or len(encoded) > _MAX_DATABASE_URL_BYTES:
        raise OperationStateError("DATABASE_URL size is invalid")
    return database_url


def _write_secret_pipe(payload: bytes) -> tuple[int, int]:
    if len(payload) > _MAX_DATABASE_URL_BYTES:
        raise OperationStateError("database transport payload is oversized")
    if hasattr(os, "pipe2") and os.name == "posix":
        return os.pipe2(getattr(os, "O_CLOEXEC", 0))
    read_fd, write_fd = os.pipe()
    try:
        os.set_inheritable(read_fd, False)
        os.set_inheritable(write_fd, False)
    except BaseException:
        for descriptor in (read_fd, write_fd):
            try:
                os.close(descriptor)
            except OSError:
                pass
        raise
    return read_fd, write_fd


def _query_current_database(context: OperationsContext, database_url: str) -> str:
    payload = database_url.encode("utf-8")
    read_fd: int | None = None
    write_fd: int | None = None
    writer: threading.Thread | None = None
    writer_started = False
    writer_errors: list[BaseException] = []

    def write_payload(descriptor: int) -> None:
        try:
            offset = 0
            while offset < len(payload):
                written = os.write(descriptor, payload[offset:])
                if written <= 0:
                    raise OperationStateError(
                        "database transport write made no progress"
                    )
                offset += written
        except BaseException as exc:
            writer_errors.append(exc)
        finally:
            try:
                os.close(descriptor)
            except OSError as exc:
                writer_errors.append(exc)

    try:
        read_fd, write_fd = _write_secret_pipe(payload)
        writer = threading.Thread(
            target=write_payload,
            args=(write_fd,),
            name="degen-pgdatabase-fd-writer",
            daemon=True,
        )
        writer.start()
        writer_started = True
        write_fd = None
        argv = (
            sys.executable,
            "-c",
            _INHERITED_FD_EXEC_SHIM,
            "pgdatabase",
            str(read_fd),
            _PSQL_EXECUTABLE,
            "psql",
            "--no-psqlrc",
            "--tuples-only",
            "--no-align",
            "--command",
            "SELECT current_database();",
        )
        completed = _checked_command(
            context,
            argv,
            (read_fd,),
            "PostgreSQL identity query",
            forbidden_values=(database_url,),
        )
    except OperationStateError:
        raise
    except Exception:
        raise OperationStateError("PostgreSQL identity query failed") from None
    finally:
        for descriptor in (read_fd, write_fd):
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except OSError:
                    pass
        if writer is not None and writer_started:
            writer.join(timeout=5)
            if writer.is_alive():
                raise OperationStateError("database transport writer did not terminate")
    if writer_errors:
        raise OperationStateError("database transport write failed")
    if completed.stderr or not completed.stdout.endswith("\n"):
        raise OperationStateError("PostgreSQL identity query returned invalid output")
    value = completed.stdout[:-1]
    if "\n" in value or "\r" in value or _SAFE_LABEL_RE.fullmatch(value) is None:
        raise OperationStateError("PostgreSQL identity label is unsafe")
    return value


def _query_hostname(context: OperationsContext) -> str:
    completed = _checked_command(
        context,
        (_HOSTNAME_EXECUTABLE, "-s"),
        (),
        "hostname query",
    )
    if completed.stderr or not completed.stdout.endswith("\n"):
        raise OperationStateError("hostname query returned invalid output")
    value = completed.stdout[:-1]
    if "\n" in value or "\r" in value or _SAFE_LABEL_RE.fullmatch(value) is None:
        raise OperationStateError("hostname label is unsafe")
    return value


def _load_verified_environment_helper(raw: bytes) -> types.ModuleType:
    if not raw or len(raw) > _MAX_SOURCE_FILE_BYTES:
        raise OperationStateError("verified environment helper size is invalid")
    module_name = "_degen_manifest_verified_backup_environment_helper"
    module = types.ModuleType(module_name)
    module.__file__ = "<manifest-verified-degen-prod-db-backup-env.py>"
    previous = sys.modules.get(module_name)
    try:
        code = compile(raw, module.__file__, "exec", dont_inherit=True)
        sys.modules[module_name] = module
        exec(code, module.__dict__)
    except Exception:
        raise OperationStateError("manifest-verified environment helper failed to load") from None
    finally:
        if previous is None:
            sys.modules.pop(module_name, None)
        else:
            sys.modules[module_name] = previous
    defaults = getattr(module, "MANAGED_DEFAULTS", None)
    managed_keys = getattr(module, "MANAGED_KEYS", None)
    if (
        type(defaults) is not dict
        or not isinstance(managed_keys, frozenset)
        or not callable(getattr(module, "parse_simple_environment", None))
        or not callable(getattr(module, "validate_effective_configuration", None))
        or not callable(getattr(module, "render_managed_environment", None))
        or not callable(getattr(module, "_render_bytes", None))
    ):
        raise OperationStateError("manifest-verified environment helper API is invalid")
    if managed_keys != frozenset((*defaults, "BACKUP_PREFIX")):
        raise OperationStateError("manifest-verified environment helper managed keys are invalid")
    return module


def _parse_live_managed_environment(
    helper: types.ModuleType,
    proof: _HostFileProof,
    raw: bytes,
    effective_uid: int,
) -> tuple[object, dict[str, str]]:
    try:
        parsed = helper.parse_simple_environment(proof.path)
        if parsed.raw_bytes != raw:
            raise OperationStateError("live managed environment changed while parsing")
        if not _same_identity(parsed.source_metadata, proof.metadata):
            raise OperationStateError("live managed environment path changed while parsing")
        effective = helper.validate_effective_configuration(
            parsed.values,
            effective_uid=effective_uid,
        )
    except OperationStateError:
        raise
    except Exception:
        raise OperationStateError("live managed environment is invalid") from None
    if type(effective) is not dict or any(
        type(key) is not str or type(value) is not str
        for key, value in effective.items()
    ):
        raise OperationStateError("live managed configuration result is invalid")
    if effective.get("REMOTE_PRUNE_ENABLED") != "0":
        raise OperationStateError(
            "live remote prune policy is enabled and cannot be silently reversed"
        )
    return parsed, effective


def _unsafe_environment_payload(raw: bytes) -> bool:
    try:
        text = raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        return True
    return any(
        pattern.search(text) is not None
        for pattern in (
            _DATABASE_URL_RE,
            _URL_USERINFO_RE,
            _SECRET_ASSIGNMENT_RE,
            _PRIVATE_KEY_RE,
            _RCLONE_CONTENT_RE,
            _BEARER_RE,
            _TOKEN_PREFIX_RE,
        )
    )


def _write_exclusive_staged_file(
    path: Path,
    data: bytes,
    mode: int,
    *,
    effective_uid: int,
    directories: _StageDirectoryProof,
) -> os.stat_result:
    if len(data) > _MAX_SOURCE_FILE_BYTES:
        raise OperationStateError("staged file exceeds the size limit")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor: int | None = None
    parent_relative = path.parent.relative_to(directories.context.paths.staged_dir).as_posix()
    if parent_relative == ".":
        parent_relative = "."
    basename = path.name
    try:
        _revalidate_stage_directories(directories)
        parent_fd = directories.descriptors.get(parent_relative)
        descriptor = (
            os.open(basename, flags, mode, dir_fd=parent_fd)
            if parent_fd is not None
            else os.open(path, flags, mode)
        )
        if hasattr(os, "fchmod"):
            os.fchmod(descriptor, mode)
        _write_all(descriptor, data)
        os.fsync(descriptor)
        opened = os.fstat(descriptor)
        named = (
            os.stat(basename, dir_fd=parent_fd, follow_symlinks=False)
            if parent_fd is not None
            else path.lstat()
        )
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != effective_uid
            or opened.st_nlink != 1
            or not _same_identity(opened, named)
            or opened.st_size != len(data)
        ):
            raise OperationStateError("staged file binding is invalid")
        if os.name == "posix" and stat.S_IMODE(opened.st_mode) != mode:
            raise OperationStateError("staged file mode is invalid")
        _refresh_stage_directory_metadata(directories, parent_relative)
        _revalidate_stage_directories(directories)
        return opened
    except OSError as exc:
        raise OperationStateError("staged file creation failed") from exc
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass


def _stage_relative_paths() -> tuple[set[str], set[str]]:
    files = {f"reviewed/{source}" for source in _SOURCE_ASSETS}
    files.update(
        {
            "host/etc/degen/prod-db-backup.env",
            "host-stage-manifest.json",
        }
    )
    directories = {"."}
    for relative in files:
        parent = PurePosixPath(relative).parent
        while str(parent) not in ("", "."):
            directories.add(str(parent))
            parent = parent.parent
    return files, directories


@dataclass
class _StageDirectoryProof:
    context: OperationsContext
    operation_directory_fd: int | None
    descriptors: dict[str, int]
    metadata: dict[str, os.stat_result]


def _stage_stable_metadata(metadata: os.stat_result) -> tuple[int, ...]:
    values = _stable_file_metadata(metadata)
    if os.name == "posix":
        return values
    # Windows fstat/path-stat can disagree on ctime by one filesystem tick
    # merely from opening a file. mtime still detects content rewrites.
    return values[:5] + values[6:]


def _stage_directory_path(context: OperationsContext, relative: str) -> Path:
    return (
        context.paths.staged_dir
        if relative == "."
        else context.paths.staged_dir.joinpath(*relative.split("/"))
    )


def _validate_stage_directory_metadata(
    metadata: os.stat_result,
    context: OperationsContext,
) -> None:
    _validate_host_directory_metadata(metadata, context.effective_uid, "staged directory")
    if os.name == "posix" and stat.S_IMODE(metadata.st_mode) != 0o700:
        raise OperationStateError("staged directory must have mode 0700")


def _revalidate_stage_directories(proof: _StageDirectoryProof) -> None:
    context = proof.context
    for relative, initial in proof.metadata.items():
        descriptor = proof.descriptors.get(relative)
        if relative == ".":
            if descriptor is not None and proof.operation_directory_fd is not None:
                named = os.stat(
                    "staged",
                    dir_fd=proof.operation_directory_fd,
                    follow_symlinks=False,
                )
            else:
                named = context.paths.staged_dir.lstat()
        else:
            parent = PurePosixPath(relative).parent.as_posix()
            if parent == ".":
                parent = "."
            parent_fd = proof.descriptors.get(parent)
            name = PurePosixPath(relative).name
            named = (
                os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
                if parent_fd is not None
                else _stage_directory_path(context, relative).lstat()
            )
        opened = os.fstat(descriptor) if descriptor is not None else named
        _validate_stage_directory_metadata(opened, context)
        _validate_stage_directory_metadata(named, context)
        if (
            _stage_stable_metadata(opened) != _stage_stable_metadata(initial)
            or not _same_identity(opened, named)
        ):
            raise OperationStateError("staged directory binding changed")


def _refresh_stage_directory_metadata(
    proof: _StageDirectoryProof,
    relative: str,
) -> None:
    descriptor = proof.descriptors.get(relative)
    path = _stage_directory_path(proof.context, relative)
    if descriptor is not None:
        opened = os.fstat(descriptor)
        if relative == ".":
            assert proof.operation_directory_fd is not None
            named = os.stat(
                "staged",
                dir_fd=proof.operation_directory_fd,
                follow_symlinks=False,
            )
        else:
            pure = PurePosixPath(relative)
            parent = pure.parent.as_posix()
            if parent == ".":
                parent = "."
            named = os.stat(
                pure.name,
                dir_fd=proof.descriptors[parent],
                follow_symlinks=False,
            )
    else:
        opened = path.lstat()
        named = path.lstat()
    _validate_stage_directory_metadata(opened, proof.context)
    if not _same_identity(opened, named):
        raise OperationStateError("staged directory binding changed during mutation")
    proof.metadata[relative] = opened


def _close_stage_directories(proof: _StageDirectoryProof) -> None:
    for descriptor in reversed(list(proof.descriptors.values())):
        try:
            os.close(descriptor)
        except OSError:
            pass
    proof.descriptors.clear()


def _open_stage_directories(
    context: OperationsContext,
    operation_directory_fd: int | None,
    *,
    create: bool,
) -> _StageDirectoryProof:
    _, expected_directories = _stage_relative_paths()
    ordered = sorted(
        expected_directories,
        key=lambda value: (value.count("/"), value),
    )
    descriptors: dict[str, int] = {}
    metadata: dict[str, os.stat_result] = {}
    use_descriptors = os.name == "posix" and operation_directory_fd is not None
    try:
        if use_descriptors:
            flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW
            if create:
                os.mkdir("staged", 0o700, dir_fd=operation_directory_fd)
            root_fd = os.open("staged", flags, dir_fd=operation_directory_fd)
            descriptors["."] = root_fd
            root_metadata = os.fstat(root_fd)
            named_root = os.stat(
                "staged", dir_fd=operation_directory_fd, follow_symlinks=False
            )
            if not _same_identity(root_metadata, named_root):
                raise OperationStateError("staged root changed while opening")
            if create:
                os.fchmod(root_fd, 0o700)
            root_metadata = os.fstat(root_fd)
            _validate_stage_directory_metadata(root_metadata, context)
            metadata["."] = root_metadata
            for relative in (item for item in ordered if item != "."):
                pure = PurePosixPath(relative)
                parent = pure.parent.as_posix()
                if parent == ".":
                    parent = "."
                parent_fd = descriptors[parent]
                if create:
                    os.mkdir(pure.name, 0o700, dir_fd=parent_fd)
                child_fd = os.open(pure.name, flags, dir_fd=parent_fd)
                descriptors[relative] = child_fd
                opened = os.fstat(child_fd)
                named = os.stat(
                    pure.name, dir_fd=parent_fd, follow_symlinks=False
                )
                if not _same_identity(opened, named):
                    raise OperationStateError("staged directory changed while opening")
                if create:
                    os.fchmod(child_fd, 0o700)
                opened = os.fstat(child_fd)
                _validate_stage_directory_metadata(opened, context)
                metadata[relative] = opened
        else:
            if create:
                context.paths.staged_dir.mkdir(mode=0o700)
                if os.name == "posix":
                    context.paths.staged_dir.chmod(0o700)
            for relative in ordered:
                path = _stage_directory_path(context, relative)
                if create and relative != ".":
                    path.mkdir(mode=0o700)
                    if os.name == "posix":
                        path.chmod(0o700)
                opened = path.lstat()
                _validate_stage_directory_metadata(opened, context)
                metadata[relative] = opened
        proof = _StageDirectoryProof(
            context,
            operation_directory_fd,
            descriptors,
            metadata,
        )
        for relative in ordered:
            _refresh_stage_directory_metadata(proof, relative)
        _revalidate_stage_directories(proof)
        return proof
    except BaseException as exc:
        for descriptor in reversed(list(descriptors.values())):
            try:
                os.close(descriptor)
            except OSError:
                pass
        if isinstance(exc, OperationStateError):
            raise
        raise OperationStateError("staged directory cannot be created safely") from exc


def _inventory_stage(
    context: OperationsContext,
    stage_directories: _StageDirectoryProof,
) -> tuple[dict[str, os.stat_result], dict[str, os.stat_result]]:
    _revalidate_stage_directories(stage_directories)
    if stage_directories.descriptors:
        files: dict[str, os.stat_result] = {}
        directories = dict(stage_directories.metadata)
        for relative_parent, descriptor in stage_directories.descriptors.items():
            try:
                iterator = os.scandir(descriptor)
            except OSError as exc:
                raise OperationStateError("staged directory inventory failed") from exc
            with iterator:
                for entry in iterator:
                    if len(files) + len(directories) > 128:
                        raise OperationStateError("staged directory exceeds the entry bound")
                    _validate_source_basename(entry.name)
                    relative = (
                        entry.name
                        if relative_parent == "."
                        else f"{relative_parent}/{entry.name}"
                    )
                    metadata = os.stat(
                        entry.name,
                        dir_fd=descriptor,
                        follow_symlinks=False,
                    )
                    if stat.S_ISDIR(metadata.st_mode):
                        if relative not in stage_directories.metadata:
                            directories[relative] = metadata
                    elif stat.S_ISREG(metadata.st_mode):
                        if metadata.st_uid != context.effective_uid or metadata.st_nlink != 1:
                            raise OperationStateError(
                                "staged file ownership or link count is unsafe"
                            )
                        files[relative] = metadata
                    else:
                        raise OperationStateError(
                            "staged inventory contains a link or special file"
                        )
        _revalidate_stage_directories(stage_directories)
        return files, directories
    root = context.paths.staged_dir
    try:
        root_metadata = root.lstat()
    except OSError as exc:
        raise OperationStateError("staged directory is unavailable") from exc
    _validate_host_directory_metadata(
        root_metadata, context.effective_uid, "staged directory"
    )
    if os.name == "posix" and stat.S_IMODE(root_metadata.st_mode) != 0o700:
        raise OperationStateError("staged directory must have mode 0700")
    files: dict[str, os.stat_result] = {}
    directories: dict[str, os.stat_result] = {".": root_metadata}

    def walk(path: Path, relative_parent: str) -> None:
        try:
            iterator = os.scandir(path)
        except OSError as exc:
            raise OperationStateError("staged directory inventory failed") from exc
        with iterator:
            for entry in iterator:
                if len(files) + len(directories) > 128:
                    raise OperationStateError("staged directory exceeds the entry bound")
                _validate_source_basename(entry.name)
                relative = (
                    f"{relative_parent}/{entry.name}" if relative_parent else entry.name
                )
                metadata = (path / entry.name).lstat()
                if stat.S_ISDIR(metadata.st_mode):
                    _validate_host_directory_metadata(
                        metadata, context.effective_uid, "staged directory"
                    )
                    if os.name == "posix" and stat.S_IMODE(metadata.st_mode) != 0o700:
                        raise OperationStateError("staged subdirectory must have mode 0700")
                    directories[relative] = metadata
                    walk(path / entry.name, relative)
                elif stat.S_ISREG(metadata.st_mode):
                    if metadata.st_uid != context.effective_uid or metadata.st_nlink != 1:
                        raise OperationStateError("staged file ownership or link count is unsafe")
                    files[relative] = metadata
                else:
                    raise OperationStateError("staged inventory contains a link or special file")

    walk(root, "")
    _revalidate_stage_directories(stage_directories)
    return files, directories


@dataclass
class _HostStageProof:
    context: OperationsContext
    stage_directories: _StageDirectoryProof
    expected_bytes: dict[str, bytes]
    expected_modes: dict[str, int]
    file_descriptors: dict[str, int]
    file_metadata: dict[str, os.stat_result]
    directory_metadata: dict[str, os.stat_result]


def _decode_strict_manifest(raw: bytes) -> dict[str, object]:
    if not raw or len(raw) > _MAX_STAGED_MANIFEST_BYTES or not raw.endswith(b"\n"):
        raise OperationStateError("host-stage manifest encoding is invalid")
    try:
        value = json.loads(
            raw,
            object_pairs_hook=_reject_duplicate_pairs,
            parse_constant=_reject_nonfinite,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise OperationStateError("host-stage manifest is invalid") from exc
    if type(value) is not dict:
        raise OperationStateError("host-stage manifest must be an object")
    canonical = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii") + b"\n"
    if canonical != raw:
        raise OperationStateError("host-stage manifest is not canonical")
    return value


def _capture_host_stage_proof(
    context: OperationsContext,
    stage_directories: _StageDirectoryProof,
    expected_bytes: dict[str, bytes],
    expected_modes: dict[str, int],
    expected_manifest: dict[str, object],
) -> _HostStageProof:
    expected_files, expected_directories = _stage_relative_paths()
    if set(expected_bytes) != expected_files or set(expected_modes) != expected_files:
        raise OperationStateError("host-stage expected inventory is incomplete")
    files, directories = _inventory_stage(context, stage_directories)
    if set(files) != expected_files or set(directories) != expected_directories:
        raise OperationStateError("host-stage inventory contains missing or extra paths")
    descriptors: dict[str, int] = {}
    flags = (os.O_RDONLY if os.name == "posix" else os.O_RDWR) | getattr(
        os, "O_BINARY", 0
    )
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        for relative in sorted(expected_files):
            path = context.paths.staged_dir.joinpath(*relative.split("/"))
            pure = PurePosixPath(relative)
            parent = pure.parent.as_posix()
            if parent == ".":
                parent = "."
            parent_fd = stage_directories.descriptors.get(parent)
            descriptor = (
                os.open(pure.name, flags, dir_fd=parent_fd)
                if parent_fd is not None
                else os.open(path, flags)
            )
            descriptors[relative] = descriptor
            opened = os.fstat(descriptor)
            metadata = files[relative]
            if not _same_identity(opened, metadata):
                raise OperationStateError("host-stage file changed while proof opened")
            if os.name == "posix" and stat.S_IMODE(metadata.st_mode) != expected_modes[relative]:
                raise OperationStateError("host-stage file mode does not match its manifest")
        proof = _HostStageProof(
            context,
            stage_directories,
            dict(expected_bytes),
            dict(expected_modes),
            descriptors,
            files,
            directories,
        )
        _revalidate_host_stage_proof(proof, expected_manifest)
        return proof
    except BaseException:
        for descriptor in descriptors.values():
            try:
                os.close(descriptor)
            except OSError:
                pass
        raise


def _revalidate_host_stage_proof(
    proof: _HostStageProof,
    expected_manifest: dict[str, object],
) -> None:
    expected_files, expected_directories = _stage_relative_paths()
    current_files, current_directories = _inventory_stage(
        proof.context,
        proof.stage_directories,
    )
    if set(current_files) != expected_files or set(current_directories) != expected_directories:
        raise OperationStateError("host-stage inventory changed after verification")
    for relative, initial in proof.file_metadata.items():
        descriptor = proof.file_descriptors[relative]
        opened_before = os.fstat(descriptor)
        current = current_files[relative]
        if (
            not _same_identity(initial, opened_before)
            or not _same_identity(opened_before, current)
            or _stage_stable_metadata(initial)
            != _stage_stable_metadata(opened_before)
            or _stage_stable_metadata(opened_before) != _stage_stable_metadata(current)
        ):
            raise OperationStateError("host-stage file path changed after verification")
        maximum = (
            _MAX_STAGED_MANIFEST_BYTES
            if relative == "host-stage-manifest.json"
            else _MAX_SOURCE_FILE_BYTES
        )
        try:
            os.lseek(descriptor, 0, os.SEEK_SET)
            chunks: list[bytes] = []
            total = 0
            while True:
                chunk = os.read(descriptor, min(64 * 1024, maximum + 1 - total))
                if not chunk:
                    break
                total += len(chunk)
                if total > maximum:
                    raise OperationStateError("host-stage file exceeds the size limit")
                chunks.append(chunk)
            opened_after = os.fstat(descriptor)
        except OSError as exc:
            raise OperationStateError("host-stage held file read failed") from exc
        if _stage_stable_metadata(opened_after) != _stage_stable_metadata(opened_before):
            raise OperationStateError("host-stage held file changed while reading")
        raw = b"".join(chunks)
        if raw != proof.expected_bytes[relative]:
            raise OperationStateError("host-stage file bytes changed after verification")
        if relative == "host-stage-manifest.json" and _decode_strict_manifest(raw) != expected_manifest:
            raise OperationStateError("host-stage manifest contents changed")
    for relative, initial in proof.directory_metadata.items():
        current = current_directories[relative]
        if (
            not _same_identity(initial, current)
            or _stage_stable_metadata(initial)
            != _stage_stable_metadata(current)
        ):
            raise OperationStateError("host-stage directory path changed after verification")


def _close_host_stage_proof(proof: _HostStageProof) -> None:
    for descriptor in proof.file_descriptors.values():
        try:
            os.close(descriptor)
        except OSError:
            pass
    proof.file_descriptors.clear()
    _close_stage_directories(proof.stage_directories)


def _fsync_host_stage_proof(proof: _HostStageProof, manifest: dict[str, object]) -> None:
    _revalidate_host_stage_proof(proof, manifest)
    for descriptor in proof.file_descriptors.values():
        try:
            os.fsync(descriptor)
        except OSError as exc:
            raise OperationStateError("host-stage held file fsync failed") from exc
    _fsync_stage_directories(proof.context, proof.stage_directories)
    _revalidate_host_stage_proof(proof, manifest)


def _read_stage_file_once(
    context: OperationsContext,
    stage_directories: _StageDirectoryProof,
    relative: str,
    *,
    maximum_size: int,
    exact_mode: int,
) -> bytes:
    pure = PurePosixPath(relative)
    parent = pure.parent.as_posix()
    if parent == ".":
        parent = "."
    parent_fd = stage_directories.descriptors.get(parent)
    path = context.paths.staged_dir.joinpath(*pure.parts)
    descriptor: int | None = None
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        named = (
            os.stat(pure.name, dir_fd=parent_fd, follow_symlinks=False)
            if parent_fd is not None
            else path.lstat()
        )
        if (
            not stat.S_ISREG(named.st_mode)
            or named.st_uid != context.effective_uid
            or named.st_nlink != 1
            or (os.name == "posix" and stat.S_IMODE(named.st_mode) != exact_mode)
            or named.st_size < 0
            or named.st_size > maximum_size
        ):
            raise OperationStateError("host-stage file metadata is invalid")
        descriptor = (
            os.open(pure.name, flags, dir_fd=parent_fd)
            if parent_fd is not None
            else os.open(path, flags)
        )
        opened_before = os.fstat(descriptor)
        if not _same_identity(named, opened_before):
            raise OperationStateError("host-stage file changed while opening")
        raw = _read_exact_descriptor(
            descriptor,
            opened_before.st_size,
            "host-stage file",
        )
        opened_after = os.fstat(descriptor)
        named_after = (
            os.stat(pure.name, dir_fd=parent_fd, follow_symlinks=False)
            if parent_fd is not None
            else path.lstat()
        )
        if (
            _stage_stable_metadata(opened_before)
            != _stage_stable_metadata(opened_after)
            or not _same_identity(opened_after, named_after)
        ):
            raise OperationStateError("host-stage file changed while reading")
        _revalidate_stage_directories(stage_directories)
        return raw
    except OSError as exc:
        raise OperationStateError("host-stage file cannot be read safely") from exc
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass


def _validate_existing_stage_manifest(
    context: OperationsContext,
    state: dict[str, object],
    manifest: dict[str, object],
    asset_bytes: dict[str, bytes],
    environment_sha256: str,
) -> None:
    root = _require_object(
        manifest,
        frozenset(
            {
                "schema_version",
                "operation",
                "selected_pair",
                "reviewed_assets",
                "host_environment",
            }
        ),
        "host-stage manifest",
    )
    if root["schema_version"] != 1:
        raise OperationStateError("host-stage manifest schema is invalid")
    operation = _require_object(
        root["operation"],
        frozenset(
            {
                "archive_sha256",
                "commit",
                "manifest_sha256",
                "operation_dir",
                "operation_id",
            }
        ),
        "host-stage manifest operation",
    )
    expected_operation = {
        "archive_sha256": context.expected_archive_sha256,
        "commit": context.expected_commit,
        "manifest_sha256": context.expected_manifest_sha256,
        "operation_dir": str(context.paths.operation_dir),
        "operation_id": context.operation_id,
    }
    if operation != expected_operation:
        raise OperationStateError("host-stage manifest operation binding is invalid")
    selected = _require_object(
        root["selected_pair"],
        frozenset({"dump_basename", "dump_sha256"}),
        "host-stage manifest selected pair",
    )
    dump_basename = _require_string(
        selected["dump_basename"],
        "host-stage manifest dump basename",
        nonempty=True,
    )
    _require_hash(selected["dump_sha256"], "host-stage manifest dump sha256")
    match = _BACKUP_NAME_RE.fullmatch(dump_basename)
    effective_config = state["effective_config"]
    assert isinstance(effective_config, dict)
    if (
        match is None
        or match.group("sidecar") is not None
        or match.group("prefix") != effective_config["BACKUP_PREFIX"]
    ):
        raise OperationStateError("host-stage manifest selected pair is invalid")
    target_by_source = dict(_SOURCE_TO_TARGET)
    expected_reviewed = [
        {
            "mode": 0o755 if source in _EXECUTABLE_SOURCE_ASSETS else 0o644,
            "sha256": hashlib.sha256(asset_bytes[source]).hexdigest(),
            "source": source,
            "staged_path": f"reviewed/{source}",
            "target": target_by_source.get(source),
        }
        for source in sorted(_SOURCE_ASSETS)
    ]
    if root["reviewed_assets"] != expected_reviewed:
        raise OperationStateError("host-stage manifest reviewed assets are invalid")
    expected_environment = {
        "mode": 0o600,
        "sha256": environment_sha256,
        "staged_path": "host/etc/degen/prod-db-backup.env",
        "target": "/etc/degen/prod-db-backup.env",
    }
    if root["host_environment"] != expected_environment:
        raise OperationStateError("host-stage manifest environment binding is invalid")


def _open_existing_host_stage_proof(
    context: OperationsContext,
    material: _VerifiedSourceMaterial,
) -> tuple[_HostStageProof, dict[str, object]]:
    stage_directories = _open_stage_directories(
        context,
        material.directory_fd,
        create=False,
    )
    try:
        asset_bytes = _capture_reviewed_asset_bytes(material.source_proof)
        environment_raw = _read_stage_file_once(
            context,
            stage_directories,
            "host/etc/degen/prod-db-backup.env",
            maximum_size=_MAX_APP_ENV_BYTES,
            exact_mode=0o600,
        )
        if _unsafe_environment_payload(environment_raw):
            raise OperationStateError("host-stage environment contains secret-like content")
        environment_sha256 = hashlib.sha256(environment_raw).hexdigest()
        manifest_raw = _read_stage_file_once(
            context,
            stage_directories,
            "host-stage-manifest.json",
            maximum_size=_MAX_STAGED_MANIFEST_BYTES,
            exact_mode=0o600,
        )
        manifest = _decode_strict_manifest(manifest_raw)
        _validate_existing_stage_manifest(
            context,
            material.state,
            manifest,
            asset_bytes,
            environment_sha256,
        )
        expected_bytes = {
            **{
                f"reviewed/{source}": contents
                for source, contents in asset_bytes.items()
            },
            "host/etc/degen/prod-db-backup.env": environment_raw,
            "host-stage-manifest.json": manifest_raw,
        }
        expected_modes = {
            **{
                f"reviewed/{source}": (
                    0o755 if source in _EXECUTABLE_SOURCE_ASSETS else 0o644
                )
                for source in _SOURCE_ASSETS
            },
            "host/etc/degen/prod-db-backup.env": 0o600,
            "host-stage-manifest.json": 0o600,
        }
        proof = _capture_host_stage_proof(
            context,
            stage_directories,
            expected_bytes,
            expected_modes,
            manifest,
        )
        host_stage = material.state["host_stage"]
        assert isinstance(host_stage, dict)
        expected_receipt = {
            "manifest_sha256": hashlib.sha256(manifest_raw).hexdigest(),
            "asset_hashes": {
                source: hashlib.sha256(contents).hexdigest()
                for source, contents in asset_bytes.items()
            },
            "environment_sha256": environment_sha256,
        }
        if host_stage != expected_receipt:
            _close_host_stage_proof(proof)
            raise OperationStateError("host-stage receipt does not match held stage bytes")
        return proof, manifest
    except BaseException:
        _close_stage_directories(stage_directories)
        raise


def _host_stage_manifest(
    context: OperationsContext,
    asset_hashes: dict[str, str],
    environment_sha256: str,
    pair: _BackupPairProof,
) -> dict[str, object]:
    target_by_source = dict(_SOURCE_TO_TARGET)
    reviewed_assets: list[dict[str, object]] = []
    for source in sorted(_SOURCE_ASSETS):
        reviewed_assets.append(
            {
                "mode": 0o755 if source in _EXECUTABLE_SOURCE_ASSETS else 0o644,
                "sha256": asset_hashes[source],
                "source": source,
                "staged_path": f"reviewed/{source}",
                "target": target_by_source.get(source),
            }
        )
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
            "dump_basename": pair.dump_basename,
            "dump_sha256": pair.dump_sha256,
        },
        "reviewed_assets": reviewed_assets,
        "host_environment": {
            "mode": 0o600,
            "sha256": environment_sha256,
            "staged_path": "host/etc/degen/prod-db-backup.env",
            "target": "/etc/degen/prod-db-backup.env",
        },
    }


def _canonical_host_stage_manifest(manifest: dict[str, object]) -> bytes:
    raw = json.dumps(
        manifest,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii") + b"\n"
    if len(raw) > _MAX_STAGED_MANIFEST_BYTES:
        raise OperationStateError("host-stage manifest exceeds the size limit")
    return raw


def _validate_rendered_environment(
    helper: types.ModuleType,
    path: Path,
    effective_config: dict[str, str],
    effective_uid: int,
) -> bytes:
    try:
        parsed = helper.parse_simple_environment(path)
        rendered_effective = helper.validate_effective_configuration(
            parsed.values,
            effective_uid=effective_uid,
        )
    except Exception:
        raise OperationStateError("rendered managed environment is invalid") from None
    if rendered_effective != effective_config:
        raise OperationStateError("rendered managed environment does not match effective config")
    if parsed.values.get("REMOTE_PRUNE_ENABLED") != "0":
        raise OperationStateError("rendered managed environment did not disable remote prune")
    raw = parsed.raw_bytes
    if _unsafe_environment_payload(raw):
        raise OperationStateError("rendered managed environment contains secret-like content")
    return raw


def _render_expected_environment_for_existing_stage(
    helper: types.ModuleType,
    parsed_live_environment: object,
    effective_config: dict[str, str],
) -> bytes:
    try:
        renderer = helper._render_bytes
        raw = renderer(parsed_live_environment, effective_config)
    except Exception:
        raise OperationStateError("managed environment byte render failed") from None
    if type(raw) is not bytes or not raw:
        raise OperationStateError("managed environment byte render returned invalid data")
    if _unsafe_environment_payload(raw):
        raise OperationStateError("managed environment byte render contains secret-like content")
    if raw.count(b"REMOTE_PRUNE_ENABLED=0\n") != 1:
        raise OperationStateError("managed environment byte render did not disable prune exactly once")
    return raw


def _fsync_stage_directories(
    context: OperationsContext,
    stage_directories: _StageDirectoryProof,
) -> None:
    if os.name != "posix":
        return
    _, directories = _stage_relative_paths()
    for relative in sorted(
        directories,
        key=lambda value: (value.count("/"), value),
        reverse=True,
    ):
        descriptor = stage_directories.descriptors.get(relative)
        if descriptor is None:
            path = _stage_directory_path(context, relative)
            flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW
            descriptor = os.open(path, flags)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        else:
            os.fsync(descriptor)
    if stage_directories.operation_directory_fd is not None:
        os.fsync(stage_directories.operation_directory_fd)
    else:
        _fsync_parent_directory(context.paths.operation_dir)
    _revalidate_stage_directories(stage_directories)


def _prepare_or_resume_stage(
    context: OperationsContext,
    helper: types.ModuleType,
    parsed_live_environment: object,
    effective_config: dict[str, str],
    asset_bytes: dict[str, bytes],
    pair: _BackupPairProof,
    operation_directory_fd: int | None,
) -> tuple[_HostStageProof, dict[str, object], bytes, dict[str, object]]:
    asset_hashes = {
        source: hashlib.sha256(contents).hexdigest()
        for source, contents in asset_bytes.items()
    }
    if frozenset(asset_hashes) != _SOURCE_ASSETS:
        raise OperationStateError("reviewed stage asset set is incomplete")
    environment_bytes = _render_expected_environment_for_existing_stage(
        helper,
        parsed_live_environment,
        effective_config,
    )
    try:
        existing_metadata = (
            os.stat(
                "staged",
                dir_fd=operation_directory_fd,
                follow_symlinks=False,
            )
            if operation_directory_fd is not None
            else context.paths.staged_dir.lstat()
        )
    except FileNotFoundError:
        existing_metadata = None
    except OSError as exc:
        raise OperationStateError("staged path metadata cannot be read") from exc
    existing = existing_metadata is not None
    if existing and not stat.S_ISDIR(existing_metadata.st_mode):
        raise OperationStateError("preexisting staged path is a symlink or non-directory")
    stage_directories: _StageDirectoryProof | None = None
    try:
        stage_directories = _open_stage_directories(
            context,
            operation_directory_fd,
            create=not existing,
        )
        if existing:
            files, directories = _inventory_stage(context, stage_directories)
            expected_files, expected_directories = _stage_relative_paths()
            if set(files) != expected_files or set(directories) != expected_directories:
                raise OperationStateError("preexisting staged residue is not exact")
        if not existing:
            for source in sorted(_SOURCE_ASSETS):
                destination = context.paths.staged_dir / "reviewed" / Path(source)
                mode = 0o755 if source in _EXECUTABLE_SOURCE_ASSETS else 0o644
                _write_exclusive_staged_file(
                    destination,
                    asset_bytes[source],
                    mode,
                    effective_uid=context.effective_uid,
                    directories=stage_directories,
                )
            environment_path = (
                context.paths.staged_dir / "host/etc/degen/prod-db-backup.env"
            )
            _write_exclusive_staged_file(
                environment_path,
                environment_bytes,
                0o600,
                effective_uid=context.effective_uid,
                directories=stage_directories,
            )
        environment_path = (
            context.paths.staged_dir / "host/etc/degen/prod-db-backup.env"
        )
        if _validate_rendered_environment(
            helper,
            environment_path,
            effective_config,
            context.effective_uid,
        ) != environment_bytes:
            raise OperationStateError(
                "staged managed environment differs from verified helper rendering"
            )
        environment_sha256 = hashlib.sha256(environment_bytes).hexdigest()
        manifest = _host_stage_manifest(
            context,
            asset_hashes,
            environment_sha256,
            pair,
        )
        manifest_bytes = _canonical_host_stage_manifest(manifest)
        if not existing:
            _write_exclusive_staged_file(
                context.paths.staged_dir / "host-stage-manifest.json",
                manifest_bytes,
                0o600,
                effective_uid=context.effective_uid,
                directories=stage_directories,
            )
        expected_bytes = {
            **{
                f"reviewed/{source}": contents
                for source, contents in asset_bytes.items()
            },
            "host/etc/degen/prod-db-backup.env": environment_bytes,
            "host-stage-manifest.json": manifest_bytes,
        }
        expected_modes = {
            **{
                f"reviewed/{source}": (
                    0o755 if source in _EXECUTABLE_SOURCE_ASSETS else 0o644
                )
                for source in _SOURCE_ASSETS
            },
            "host/etc/degen/prod-db-backup.env": 0o600,
            "host-stage-manifest.json": 0o600,
        }
        proof = _capture_host_stage_proof(
            context,
            stage_directories,
            expected_bytes,
            expected_modes,
            manifest,
        )
        try:
            _fsync_host_stage_proof(proof, manifest)
        except BaseException:
            _close_host_stage_proof(proof)
            raise
        host_stage: dict[str, object] = {
            "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
            "asset_hashes": dict(asset_hashes),
            "environment_sha256": environment_sha256,
        }
        return proof, manifest, manifest_bytes, host_stage
    except BaseException:
        if stage_directories is not None:
            _close_stage_directories(stage_directories)
        raise


def _staging_evidence(
    effective_config: dict[str, str],
    host_stage: dict[str, object],
) -> bytes:
    payload = {
        "effective_config": effective_config,
        "host_stage": host_stage,
    }
    canonical = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return b"degen-host-staging-v1\n" + canonical + b"\n"


def _staging_prepared_state(
    context: OperationsContext,
    previous: dict[str, object],
    effective_config: dict[str, str],
    host_stage: dict[str, object],
) -> dict[str, object]:
    now = context.clock()
    if type(now) is not datetime or now.tzinfo is None or now.utcoffset() != timedelta(0):
        raise OperationStateError("operations clock must return an aware UTC datetime")
    state = copy.deepcopy(previous)
    state["phase"] = "staging_prepared"
    state["effective_config"] = dict(effective_config)
    state["host_stage"] = copy.deepcopy(host_stage)
    history = state["phase_history"]
    assert isinstance(history, list)
    history.append(
        {
            "phase": "staging_prepared",
            "epoch": int(now.astimezone(timezone.utc).timestamp()),
            "evidence_sha256": hashlib.sha256(
                _staging_evidence(effective_config, host_stage)
            ).hexdigest(),
        }
    )
    validate_operation_state(state, context.paths.operation_dir, previous)
    validate_operation_state_for_context(state, context)
    return state


def _commit_staging_prepared_receipt(
    context: OperationsContext,
    material: _VerifiedSourceMaterial,
    initial_state: dict[str, object],
    effective_config: dict[str, str],
    host_stage: dict[str, object],
    stage_proof: _HostStageProof,
    stage_manifest: dict[str, object],
    pair: _BackupPairProof,
    managed_environment: _HostFileProof,
    managed_raw: bytes,
    app_environment: _HostFileProof,
    app_environment_raw: bytes,
    database_url: str,
    database_name: str,
    hostname: str,
) -> dict[str, object]:
    try:
        state = _staging_prepared_state(
            context,
            initial_state,
            effective_config,
            host_stage,
        )

        def revalidate_receipt_proof() -> None:
            _revalidate_verified_source_material(material)
            _revalidate_backup_pair(pair)
            _revalidate_host_file_proof(managed_environment)
            if _read_host_file(managed_environment, _MAX_APP_ENV_BYTES) != managed_raw:
                raise OperationStateError(
                    "live managed environment changed before state replacement"
                )
            _revalidate_host_file_proof(app_environment)
            if (
                _read_host_file(app_environment, _MAX_APP_ENV_BYTES)
                != app_environment_raw
            ):
                raise OperationStateError(
                    "application environment changed before state replacement"
                )
            _revalidate_host_stage_proof(stage_proof, stage_manifest)
            fresh_database = _query_current_database(context, database_url)
            fresh_hostname = _query_hostname(context)
            if (
                fresh_database != database_name
                or fresh_hostname != hostname
                or f"{fresh_database}_{fresh_hostname}_" != pair.prefix
            ):
                raise OperationStateError(
                    "live database or hostname identity changed before state replacement"
                )
            _revalidate_backup_pair(pair)
            _revalidate_verified_source_material(material)
            _revalidate_host_stage_proof(stage_proof, stage_manifest)

        _atomic_write_operation_state_internal(
            context.paths.state_file,
            state,
            effective_uid=context.effective_uid,
            pre_replace_validator=revalidate_receipt_proof,
            operation_directory_binding=_OperationDirectoryBinding(
                context.paths.operation_dir,
                material.directory_fd,
                material.operation_metadata,
            ),
        )
        result: dict[str, object] = {
            "effective_config": dict(effective_config),
            "host_stage": copy.deepcopy(host_stage),
        }
        _reject_residual_secrets(result)
        return result
    finally:
        _close_host_stage_proof(stage_proof)


def prepare_host_staging(context: OperationsContext) -> dict[str, object]:
    """Verify live host readiness and persist one staging_prepared receipt."""
    _validate_source_context(context, context.paths.source_dir)
    initial_state = load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    validate_operation_state_for_context(initial_state, context)
    if initial_state["phase"] != "source_verified":
        raise OperationStateError("prepare-staging requires source_verified state")
    with _open_verified_source_material(context) as material:
        if material.state != initial_state:
            raise OperationStateError("operation state changed before source revalidation")
        asset_bytes = _capture_reviewed_asset_bytes(material.source_proof)
        helper = _load_verified_environment_helper(
            asset_bytes["deploy/linux/degen-prod-db-backup-env.py"]
        )
        with _open_host_file(
            context,
            "/etc/degen/prod-db-backup.env",
            "live managed environment",
            maximum_size=_MAX_APP_ENV_BYTES,
        ) as managed_environment:
            managed_raw = _read_host_file(managed_environment, _MAX_APP_ENV_BYTES)
            _parsed_managed, base_effective = _parse_live_managed_environment(
                helper,
                managed_environment,
                managed_raw,
                context.effective_uid,
            )
            backup_dir = base_effective.get("BACKUP_DIR")
            app_env_file = base_effective.get("APP_ENV_FILE")
            if type(backup_dir) is not str or type(app_env_file) is not str:
                raise OperationStateError("managed host paths are missing")
            with _open_verified_backup_pair(context, backup_dir) as pair:
                with _open_host_file(
                    context,
                    app_env_file,
                    "application environment",
                    maximum_size=_MAX_APP_ENV_BYTES,
                ) as app_environment:
                    app_environment_raw = _read_host_file(
                        app_environment, _MAX_APP_ENV_BYTES
                    )
                    database_url = _parse_app_environment(app_environment_raw)
                    database_name = _query_current_database(context, database_url)
                    hostname = _query_hostname(context)
                    expected_prefix = f"{database_name}_{hostname}_"
                    configured_prefix = base_effective.get("BACKUP_PREFIX")
                    if pair.prefix != expected_prefix:
                        raise OperationStateError(
                            "filename-derived backup prefix does not match live identity"
                        )
                    if configured_prefix is not None and configured_prefix != expected_prefix:
                        raise OperationStateError(
                            "configured backup prefix does not match live identity"
                        )
                    requested = dict(base_effective)
                    requested["BACKUP_PREFIX"] = expected_prefix
                    requested["REMOTE_PRUNE_ENABLED"] = "0"
                    try:
                        effective_config = helper.validate_effective_configuration(
                            requested,
                            effective_uid=context.effective_uid,
                        )
                    except Exception:
                        raise OperationStateError(
                            "effective managed configuration is invalid"
                        ) from None
                    if type(effective_config) is not dict:
                        raise OperationStateError(
                            "effective managed configuration result is invalid"
                        )
                    _revalidate_verified_source_material(material)
                    _revalidate_backup_pair(pair)
                    _revalidate_host_file_proof(managed_environment)
                    if _read_host_file(
                        managed_environment, _MAX_APP_ENV_BYTES
                    ) != managed_raw:
                        raise OperationStateError("live managed environment changed")
                    _revalidate_host_file_proof(app_environment)
                    if _read_host_file(app_environment, _MAX_APP_ENV_BYTES) != app_environment_raw:
                        raise OperationStateError("application environment changed")
                    stage_proof, stage_manifest, _manifest_bytes, host_stage = (
                        _prepare_or_resume_stage(
                            context,
                            helper,
                            _parsed_managed,
                            effective_config,
                            asset_bytes,
                            pair,
                            material.directory_fd,
                        )
                    )
                    return _commit_staging_prepared_receipt(
                        context,
                        material,
                        initial_state,
                        effective_config,
                        host_stage,
                        stage_proof,
                        stage_manifest,
                        pair,
                        managed_environment,
                        managed_raw,
                        app_environment,
                        app_environment_raw,
                        database_url,
                        database_name,
                        hostname,
                    )


def _snapshot_target_receipt(proof: _SnapshotTargetProof) -> dict[str, object]:
    if proof.descriptor is None:
        return {
            "present": False,
            "sha256": None,
            "mode": None,
            "uid": None,
            "gid": None,
        }
    assert proof.metadata is not None and proof.contents is not None
    return {
        "present": True,
        "sha256": hashlib.sha256(proof.contents).hexdigest(),
        "mode": (
            stat.S_IMODE(proof.metadata.st_mode)
            if os.name == "posix"
            else 0o600
        ),
        "uid": proof.metadata.st_uid,
        "gid": proof.metadata.st_gid,
    }


def _snapshot_artifacts_and_receipt(
    targets: dict[str, _SnapshotTargetProof],
    rclone: _HostFileProof,
    rclone_raw: bytes,
) -> tuple[dict[str, bytes], dict[str, object]]:
    if tuple(targets) != _TARGET_ORDER:
        raise OperationStateError("snapshot target capture order is invalid")
    if len(set(_SNAPSHOT_TARGET_NAMES.values())) != len(_TARGET_ORDER):
        raise OperationStateError("snapshot target basenames collide")
    artifacts: dict[str, bytes] = {}
    target_receipts: dict[str, dict[str, object]] = {}
    for logical_path, proof in targets.items():
        base = _SNAPSHOT_TARGET_NAMES[logical_path]
        receipt = _snapshot_target_receipt(proof)
        target_receipts[logical_path] = receipt
        if proof.contents is None:
            name = f"{base}.absent"
            contents = f"ABSENT {logical_path}\n".encode("ascii")
        else:
            name = base
            contents = proof.contents
        if name in artifacts:
            raise OperationStateError("snapshot artifact basenames collide")
        artifacts[name] = contents
    if _RCLONE_AUDIT_NAME in artifacts:
        raise OperationStateError("rclone audit artifact basename collides")
    artifacts[_RCLONE_AUDIT_NAME] = rclone_raw
    manifest_raw = _canonical_snapshot_manifest(artifacts)
    expected_bytes = {**artifacts, _SNAPSHOT_MANIFEST_NAME: manifest_raw}
    metadata = rclone.metadata
    snapshot: dict[str, object] = {
        "manifest_sha256": hashlib.sha256(manifest_raw).hexdigest(),
        "targets": target_receipts,
        "rclone_audit": {
            "path": _RCLONE_CONFIG_PATH,
            "sha256": hashlib.sha256(rclone_raw).hexdigest(),
            "inode": metadata.st_ino,
            "uid": metadata.st_uid,
            "gid": metadata.st_gid,
            "mode": stat.S_IMODE(metadata.st_mode) if os.name == "posix" else 0o600,
            "size": metadata.st_size,
            "mtime_ns": metadata.st_mtime_ns,
        },
    }
    return expected_bytes, snapshot


def _snapshot_evidence(
    snapshot: dict[str, object],
    prior_runtime: dict[str, object],
) -> bytes:
    canonical = json.dumps(
        {"prior_runtime": prior_runtime, "snapshot": snapshot},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return b"degen-host-snapshot-v1\n" + canonical + b"\n"


def _snapshotted_state(
    context: OperationsContext,
    previous: dict[str, object],
    snapshot: dict[str, object],
    prior_runtime: dict[str, object],
) -> dict[str, object]:
    now = context.clock()
    if type(now) is not datetime or now.tzinfo is None or now.utcoffset() != timedelta(0):
        raise OperationStateError("operations clock must return an aware UTC datetime")
    state = copy.deepcopy(previous)
    state["phase"] = "snapshotted"
    state["snapshot"] = copy.deepcopy(snapshot)
    state["prior_runtime"] = copy.deepcopy(prior_runtime)
    history = state["phase_history"]
    assert isinstance(history, list)
    history.append(
        {
            "phase": "snapshotted",
            "epoch": int(now.astimezone(timezone.utc).timestamp()),
            "evidence_sha256": hashlib.sha256(
                _snapshot_evidence(snapshot, prior_runtime)
            ).hexdigest(),
        }
    )
    validate_operation_state(state, context.paths.operation_dir, previous)
    validate_operation_state_for_context(state, context)
    return state


def _revalidate_snapshot_inputs(
    material: _VerifiedSourceMaterial,
    stage_proof: _HostStageProof,
    stage_manifest: dict[str, object],
    targets: dict[str, _SnapshotTargetProof],
    rclone: _HostFileProof,
    rclone_raw: bytes,
    snapshot_proof: _SnapshotDirectoryProof,
) -> None:
    _revalidate_verified_source_material(material)
    _revalidate_host_stage_proof(stage_proof, stage_manifest)
    for proof in targets.values():
        _revalidate_snapshot_target_proof(proof)
    _revalidate_host_file_proof(rclone)
    if _read_host_file(rclone, _MAX_SNAPSHOT_FILE_BYTES) != rclone_raw:
        raise OperationStateError("rclone audit source changed after capture")
    _revalidate_snapshot_directory_proof(snapshot_proof)
    _revalidate_host_stage_proof(stage_proof, stage_manifest)
    _revalidate_verified_source_material(material)


def _snapshot_result(
    snapshot: dict[str, object],
    prior_runtime: dict[str, object],
) -> dict[str, object]:
    result = {
        "snapshot": copy.deepcopy(snapshot),
        "prior_runtime": copy.deepcopy(prior_runtime),
    }
    _reject_residual_secrets(result)
    return result


def snapshot_host_state(context: OperationsContext) -> dict[str, object]:
    """Capture immutable rollback artifacts and prior runtime without mutation."""
    _validate_source_context(context, context.paths.source_dir)
    initial_state = load_operation_state(
        context.paths.state_file,
        effective_uid=context.effective_uid,
    )
    validate_operation_state_for_context(initial_state, context)
    phase = initial_state["phase"]
    if phase not in {"staging_prepared", "snapshotted"}:
        raise OperationStateError(
            "snapshot requires strict staging_prepared or snapshotted state"
        )
    material_context = (
        _open_verified_staging_material(context)
        if phase == "staging_prepared"
        else _open_verified_snapshotted_material(context)
    )
    with material_context as material:
        if material.state != initial_state:
            raise OperationStateError("operation state changed before snapshot proof")
        stage_proof, stage_manifest = _open_existing_host_stage_proof(
            context,
            material,
        )
        snapshot_proof: _SnapshotDirectoryProof | None = None
        target_proofs: dict[str, _SnapshotTargetProof] = {}
        try:
            with contextlib.ExitStack() as stack:
                for logical_path in _TARGET_ORDER:
                    parent = str(PurePosixPath(logical_path).parent)
                    directory = stack.enter_context(
                        _open_host_directory(context, parent)
                    )
                    proof = _capture_snapshot_target(directory, logical_path)
                    target_proofs[logical_path] = proof
                effective_config = initial_state["effective_config"]
                assert isinstance(effective_config, dict)
                if effective_config["RCLONE_CONFIG"] != _RCLONE_CONFIG_PATH:
                    raise OperationStateError("rclone config path is not the approved host path")
                rclone = stack.enter_context(
                    _open_host_file(
                        context,
                        _RCLONE_CONFIG_PATH,
                        "rclone audit source",
                        maximum_size=_MAX_SNAPSHOT_FILE_BYTES,
                        exact_mode=0o600,
                    )
                )
                rclone_raw = _read_host_file(rclone, _MAX_SNAPSHOT_FILE_BYTES)
                if not rclone_raw:
                    raise OperationStateError("rclone audit source is empty")
                _revalidate_verified_source_material(material)
                _revalidate_host_stage_proof(stage_proof, stage_manifest)
                for proof in target_proofs.values():
                    _revalidate_snapshot_target_proof(proof)
                _revalidate_host_file_proof(rclone)
                prior_runtime = _capture_prior_runtime(context)
                expected_bytes, snapshot = _snapshot_artifacts_and_receipt(
                    target_proofs,
                    rclone,
                    rclone_raw,
                )
                if phase == "snapshotted":
                    if (
                        initial_state["snapshot"] != snapshot
                        or initial_state["prior_runtime"] != prior_runtime
                    ):
                        raise OperationStateError(
                            "snapshotted state differs from current verified evidence"
                        )
                    snapshot_proof = _open_snapshot_directory_proof(
                        context,
                        material.directory_fd,
                        expected_bytes,
                        create=False,
                    )
                    _revalidate_snapshot_inputs(
                        material,
                        stage_proof,
                        stage_manifest,
                        target_proofs,
                        rclone,
                        rclone_raw,
                        snapshot_proof,
                    )
                    if _capture_prior_runtime(context) != prior_runtime:
                        raise OperationStateError(
                            "protected runtime changed during snapshot verification"
                        )
                    _revalidate_snapshot_inputs(
                        material,
                        stage_proof,
                        stage_manifest,
                        target_proofs,
                        rclone,
                        rclone_raw,
                        snapshot_proof,
                    )
                    return _snapshot_result(snapshot, prior_runtime)

                snapshot_proof = _open_snapshot_directory_proof(
                    context,
                    material.directory_fd,
                    expected_bytes,
                    create=True,
                )
                _fsync_snapshot_directory_proof(snapshot_proof)
                state = _snapshotted_state(
                    context,
                    initial_state,
                    snapshot,
                    prior_runtime,
                )

                def revalidate_receipt_proof() -> None:
                    assert snapshot_proof is not None
                    _revalidate_snapshot_inputs(
                        material,
                        stage_proof,
                        stage_manifest,
                        target_proofs,
                        rclone,
                        rclone_raw,
                        snapshot_proof,
                    )
                    if _capture_prior_runtime(context) != prior_runtime:
                        raise OperationStateError(
                            "protected runtime changed before snapshot state replacement"
                        )
                    _revalidate_snapshot_inputs(
                        material,
                        stage_proof,
                        stage_manifest,
                        target_proofs,
                        rclone,
                        rclone_raw,
                        snapshot_proof,
                    )

                revalidate_receipt_proof()
                _atomic_write_operation_state_internal(
                    context.paths.state_file,
                    state,
                    effective_uid=context.effective_uid,
                    pre_replace_validator=revalidate_receipt_proof,
                    operation_directory_binding=_OperationDirectoryBinding(
                        context.paths.operation_dir,
                        material.directory_fd,
                        material.operation_metadata,
                    ),
                )
                return _snapshot_result(snapshot, prior_runtime)
        finally:
            if snapshot_proof is not None:
                _close_snapshot_directory_proof(snapshot_proof)
            for proof in target_proofs.values():
                _close_snapshot_target_proof(proof)
            _close_host_stage_proof(stage_proof)


def sanitize_error_text(value: object) -> str:
    try:
        text = str(value)
    except Exception:
        text = type(value).__name__
    if _RCLONE_CONTENT_RE.search(text):
        text = "[REDACTED]"
    text = _PRIVATE_KEY_RE.sub("[REDACTED]", text)
    text = _SECRET_ASSIGNMENT_RE.sub("[REDACTED]", text)
    text = _URL_USERINFO_RE.sub("[REDACTED]", text)
    text = _DATABASE_URL_RE.sub("[REDACTED]", text)
    text = _BEARER_RE.sub("[REDACTED]", text)
    text = _TOKEN_PREFIX_RE.sub("[REDACTED]", text)
    text = "".join(" " if ord(character) < 32 or ord(character) == 127 else character for character in text)
    text = " ".join(text.split())
    if not text:
        text = type(value).__name__ if not isinstance(value, str) else "operation failed"
    return text[:512]


def _string_contains_secret(value: str) -> bool:
    return any(
        pattern.search(value) is not None
        for pattern in (
            _DATABASE_URL_RE,
            _URL_USERINFO_RE,
            _SECRET_ASSIGNMENT_RE,
            _BEARER_RE,
            _TOKEN_PREFIX_RE,
            _PRIVATE_KEY_RE,
            _ENV_CONTENT_RE,
            _RCLONE_CONTENT_RE,
        )
    )


def _reject_residual_secrets(value: object, path: str = "state") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if _SECRET_KEY_RE.search(key):
                raise OperationStateError("secret-like key is forbidden in operation state")
            _reject_residual_secrets(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _reject_residual_secrets(item, f"{path}[{index}]")
    elif isinstance(value, str) and _string_contains_secret(value):
        raise OperationStateError("secret-like value is forbidden in operation state")


def _effective_uid() -> int:
    getter = getattr(os, "geteuid", None)
    if getter is None:
        return -1
    return int(getter())


class _StoreOnce(argparse.Action):
    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: object,
        option_string: str | None = None,
    ) -> None:
        if getattr(namespace, self.dest, None) is not None:
            parser.error(f"argument {option_string} may not be repeated")
        setattr(namespace, self.dest, values)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    subparsers = parser.add_subparsers(dest="command", required=True)
    show_state = subparsers.add_parser("show-state", allow_abbrev=False)
    show_state.add_argument("--operation-dir", required=True)
    verify_source = subparsers.add_parser("verify-source", allow_abbrev=False)
    verify_source.add_argument("--operation-dir", required=True, action=_StoreOnce)
    verify_source.add_argument("--archive", required=True, action=_StoreOnce)
    verify_source.add_argument("--expected-commit", required=True, action=_StoreOnce)
    verify_source.add_argument(
        "--expected-archive-sha256",
        required=True,
        action=_StoreOnce,
    )
    verify_source.add_argument(
        "--expected-manifest-sha256",
        required=True,
        action=_StoreOnce,
    )
    prepare_staging = subparsers.add_parser("prepare-staging", allow_abbrev=False)
    prepare_staging.add_argument("--operation-dir", required=True, action=_StoreOnce)
    snapshot = subparsers.add_parser("snapshot", allow_abbrev=False)
    snapshot.add_argument("--operation-dir", required=True, action=_StoreOnce)
    return parser


def _is_production_operation_dir(raw_path: str) -> bool:
    if _PRODUCTION_OPERATION_RE.fullmatch(raw_path) is None:
        return False
    try:
        datetime.strptime(raw_path.rsplit("/", 1)[-1], "%Y%m%dT%H%M%SZ")
    except ValueError:
        return False
    return True


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    effective_uid = _effective_uid()
    if effective_uid != 0:
        print("error: root privileges are required", file=sys.stderr)
        return 1
    if not _is_production_operation_dir(args.operation_dir):
        print("error: invalid production operation directory", file=sys.stderr)
        return 1
    try:
        _require_posix_descriptor_primitives()
        operation_dir = _PRODUCTION_HOST_ROOT / args.operation_dir.lstrip("/")
        if args.command == "show-state":
            state = load_operation_state(
                operation_dir / "operation-state.json",
                effective_uid=effective_uid,
            )
            validate_operation_state(state, operation_dir)
            sys.stdout.write(_canonical_state_bytes(state).decode("utf-8"))
        elif args.command == "verify-source":
            if args.archive != f"{args.operation_dir}/source.tar":
                raise OperationStateError("archive must equal the fixed operation source.tar path")
            paths = build_operation_paths(operation_dir)
            context = OperationsContext(
                operation_id=operation_dir.name,
                paths=paths,
                effective_uid=effective_uid,
                command_runner=_default_command_runner,
                clock=lambda: datetime.now(timezone.utc),
                expected_commit=args.expected_commit,
                expected_archive_sha256=args.expected_archive_sha256,
                expected_manifest_sha256=args.expected_manifest_sha256,
                host_root=_PRODUCTION_HOST_ROOT,
            )
            verify_source_archive(context, source_dir=paths.source_dir)
        elif args.command == "prepare-staging":
            paths = build_operation_paths(operation_dir)
            state = load_operation_state(
                paths.state_file,
                effective_uid=effective_uid,
            )
            if state["phase"] != "source_verified":
                raise OperationStateError(
                    "prepare-staging requires strict source_verified operation state"
                )
            reviewed = state["reviewed_source"]
            assert isinstance(reviewed, dict)
            context = OperationsContext(
                operation_id=operation_dir.name,
                paths=paths,
                effective_uid=effective_uid,
                command_runner=_default_command_runner,
                clock=lambda: datetime.now(timezone.utc),
                expected_commit=str(reviewed["commit"]),
                expected_archive_sha256=str(reviewed["archive_sha256"]),
                expected_manifest_sha256=str(reviewed["manifest_sha256"]),
                host_root=_PRODUCTION_HOST_ROOT,
            )
            validate_operation_state_for_context(state, context)
            prepare_host_staging(context)
        elif args.command == "snapshot":
            paths = build_operation_paths(operation_dir)
            state = load_operation_state(
                paths.state_file,
                effective_uid=effective_uid,
            )
            if state["phase"] not in {"staging_prepared", "snapshotted"}:
                raise OperationStateError(
                    "snapshot requires strict staging_prepared or snapshotted state"
                )
            reviewed = state["reviewed_source"]
            assert isinstance(reviewed, dict)
            context = OperationsContext(
                operation_id=operation_dir.name,
                paths=paths,
                effective_uid=effective_uid,
                command_runner=_default_command_runner,
                clock=lambda: datetime.now(timezone.utc),
                expected_commit=str(reviewed["commit"]),
                expected_archive_sha256=str(reviewed["archive_sha256"]),
                expected_manifest_sha256=str(reviewed["manifest_sha256"]),
                host_root=_PRODUCTION_HOST_ROOT,
            )
            validate_operation_state_for_context(state, context)
            snapshot_host_state(context)
        else:
            raise OperationStateError("unsupported command")
    except Exception as exc:
        message = sanitize_error_text(exc)
        if _string_contains_secret(message):
            message = "operation state is invalid"
        print(f"error: {message}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
